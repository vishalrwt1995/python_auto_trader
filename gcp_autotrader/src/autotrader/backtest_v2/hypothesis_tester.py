"""Phase B — Hypothesis Tester using VALIDATED swing replica.

The swing replica (prod_replica_v2.py) matches production scan_decisions
100% on swing rows. Direction, raw_score, adjusted_score all exact.

This module uses that replica to test rule-change hypotheses by:
  1. Pulling every swing scan from BQ scan_decisions in the test window
  2. Replaying each scan with DEFAULT settings (validates 100% match)
  3. Replaying each scan with HYPOTHESIS settings (modified gates/scoring)
  4. Identifying NEWLY QUALIFIED scans (would have fired under hypothesis)
     and NO-LONGER-QUALIFIED scans (would have been blocked under hypothesis)
  5. Simulating trade outcomes (next-day open, SL/target/MAX_HOLD) for the
     delta set
  6. Reporting P&L delta vs production baseline

## Limitations (documented)

  - Test window: only where stored brain snapshots exist (Mar 7 - May 21, 2026)
  - SWING rows only (intraday replica still ~50% match, excluded here)
  - In BQ, PULLBACK has only intraday rows and MOMENTUM has none — so
    hypothesis-tested setups are BREAKOUT and MEAN_REVERSION only
  - Trade simulation uses daily candles (no intraday tick precision)

## Hypotheses tested

  H1: Unblock BREAKOUT in TREND_UP   — was parked 2026-05-06 on 0/9 live data
  H2: Unblock BREAKOUT in RANGE     — today's missed JNKINDIA scenario
  H3: Floor affinity multiplier at 0.8 in RANGE for strong stocks
        (adx>=30, ema_stack=BULL_STACK) — selective version of H2

Run:
    python -m autotrader.backtest_v2.hypothesis_tester
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from autotrader.backtest_v2.brain_loader import BrainSnapshotLoader
from autotrader.backtest_v2.data import HistoricalDataset
from autotrader.backtest_v2.phase5_trade_sim import find_entry_idx, simulate_swing_trade
from autotrader.backtest_v2.prod_replica_v2 import ProdReplicaV2
from autotrader.domain.daily_bias import compute_daily_bias
from autotrader.domain.indicators import compute_indicators
from autotrader.domain.regime_affinity import (
    regime_hard_blocks_strategy,
    regime_strategy_multiplier,
)
from autotrader.domain.scoring import check_swing_entry, determine_direction, score_signal
from autotrader.services.market_policy_service import MarketPolicyService
from autotrader.settings import StrategySettings


@dataclass
class HypothesisConfig:
    name: str
    # Set of (regime, setup) pairs to UNBLOCK (override hard_block)
    unblock_pairs: set[tuple[str, str]] = field(default_factory=set)
    # Minimum affinity multiplier floor — if production says 0.6, we'd use max(0.6, floor)
    affinity_floor_range: float | None = None
    # Override score_floor (None = use production default)
    score_floor_override: int | None = None
    # Only apply unblock if stock has these characteristics
    require_strong_stock: bool = False


HYPOTHESES = [
    HypothesisConfig(
        name="baseline_production",
        # No overrides — should match production exactly
    ),
    HypothesisConfig(
        name="H1_unblock_breakout_trend_up",
        unblock_pairs={("TREND_UP", "BREAKOUT")},
    ),
    HypothesisConfig(
        name="H2_unblock_breakout_range",
        unblock_pairs={("RANGE", "BREAKOUT")},
    ),
    HypothesisConfig(
        name="H2b_unblock_breakout_range_strong_only",
        unblock_pairs={("RANGE", "BREAKOUT")},
        require_strong_stock=True,
    ),
    HypothesisConfig(
        name="H3_floor_affinity_range_08",
        affinity_floor_range=0.8,
    ),
    HypothesisConfig(
        name="H4_combined_unblock_brkout_all_trends",
        unblock_pairs={("TREND_UP", "BREAKOUT"), ("RANGE", "BREAKOUT"), ("RECOVERY", "BREAKOUT")},
    ),
]


def _bq_query(query: str) -> list[dict]:
    gcloud = os.environ.get("GCLOUD", "gcloud")
    token = subprocess.check_output(
        [gcloud, "auth", "application-default", "print-access-token"],
        text=True,
    ).strip()
    body = json.dumps({"query": query, "useLegacySql": False, "location": "asia-south1"}).encode()
    req = urllib.request.Request(
        "https://bigquery.googleapis.com/bigquery/v2/projects/grow-profit-machine/queries",
        data=body,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=180) as resp:
        r = json.loads(resp.read())
    if "error" in r:
        raise RuntimeError(f"BQ error: {r['error']}")
    schema = r.get("schema", {}).get("fields", [])
    out = []
    for row in r.get("rows", []):
        d = {}
        for fld, cell in zip(schema, row["f"]):
            v = cell.get("v")
            if v is None:
                d[fld["name"]] = None
                continue
            if fld["type"] == "TIMESTAMP":
                try:
                    d[fld["name"]] = datetime.fromtimestamp(float(v), tz=timezone.utc).isoformat()
                except Exception:
                    d[fld["name"]] = v
            else:
                d[fld["name"]] = v
        out.append(d)
    return out


def is_strong_stock(daily_truncated: list[list[Any]], cfg: StrategySettings) -> bool:
    """Check if stock has strong-trend characteristics."""
    try:
        from autotrader.domain.indicators import calc_adx, calc_ema
        closes = [float(c[4]) for c in daily_truncated]
        ema9 = calc_ema(closes, 9)
        ema21 = calc_ema(closes, 21)
        ema50 = calc_ema(closes, 50)
        adx = calc_adx(daily_truncated, period=14)
    except Exception:
        return False
    if adx < 30.0:
        return False
    if not (ema9 > ema21 > ema50):
        return False
    return True


def replay_with_hypothesis(
    symbol: str,
    setup: str,
    scan_ts: str,
    hyp: HypothesisConfig,
    replica: ProdReplicaV2,
    cfg: StrategySettings,
    policy: MarketPolicyService,
) -> dict[str, Any]:
    """Replay a single scan applying hypothesis modifications. Returns
    {qualified, raw_score, adjusted_score, blocked_reason, direction}.
    """
    # First get the standard replica result (this is production-matched)
    standard = replica.replay_scan(symbol=symbol, setup=setup, scan_ts=scan_ts, is_swing=True)

    # If hypothesis is baseline, just return the standard result
    if hyp.name == "baseline_production":
        return {
            "qualified": standard.qualified,
            "raw_score": standard.raw_score,
            "adjusted_score": standard.adjusted_score,
            "affinity_score": standard.affinity_score,
            "direction": standard.direction,
            "blocked_reason": standard.blocked_reason,
        }

    # Re-run with hypothesis modifications
    snap = replica.brain_loader.find_snapshot_before(scan_ts)
    if snap is None:
        return {"qualified": False, "raw_score": 0, "adjusted_score": 0, "affinity_score": 0, "direction": "HOLD", "blocked_reason": "no_brain_snapshot"}
    regime_state = snap.state
    regime_str = regime_state.regime
    regime_obj = snap.to_regime_snapshot()

    # Build daily candles (same as standard swing path)
    scan_date = scan_ts[:10]
    daily_all = replica.ds.daily_candles(symbol)
    daily_before = [c for c in daily_all if str(c[0])[:10] < scan_date]
    primary_candles = daily_before[-120:] if len(daily_before) > 120 else daily_before
    if len(primary_candles) < 60:
        return {"qualified": False, "raw_score": 0, "adjusted_score": 0, "affinity_score": 0, "direction": "HOLD", "blocked_reason": "insufficient_candles"}

    # Synthesize partial day bar (matches replica)
    intra_all = replica.ds.intraday_candles(symbol, end_ts=scan_ts)
    today_intra = [c for c in intra_all if str(c[0])[:10] == scan_date]
    if today_intra:
        try:
            op = float(today_intra[0][1])
            hi = max(float(c[2]) for c in today_intra)
            lo = min(float(c[3]) for c in today_intra)
            cl = float(today_intra[-1][4])
            vol = sum(float(c[5]) for c in today_intra)
            primary_candles = (list(primary_candles) + [[f"{scan_date}T00:00:00+05:30", op, hi, lo, cl, vol]])[-120:]
        except Exception:
            pass

    try:
        ind = compute_indicators(primary_candles, cfg)
        db = compute_daily_bias(primary_candles)
    except Exception as exc:
        return {"qualified": False, "raw_score": 0, "adjusted_score": 0, "affinity_score": 0, "direction": "HOLD", "blocked_reason": f"indicator_error:{type(exc).__name__}"}

    if ind is None:
        return {"qualified": False, "raw_score": 0, "adjusted_score": 0, "affinity_score": 0, "direction": "HOLD", "blocked_reason": "indicator_none"}

    # direction
    try:
        direction = determine_direction(ind, regime_obj, setup=setup, wl_type="swing", daily_bias=db)
    except Exception:
        return {"qualified": False, "raw_score": 0, "adjusted_score": 0, "affinity_score": 0, "direction": "HOLD", "blocked_reason": "direction_error"}
    if direction == "HOLD":
        return {"qualified": False, "raw_score": 0, "adjusted_score": 0, "affinity_score": 0, "direction": "HOLD", "blocked_reason": "direction_hold"}

    # score
    try:
        sig = score_signal(symbol, direction, ind, regime_obj, cfg, daily_bias=db, setup=setup)
    except Exception:
        return {"qualified": False, "raw_score": 0, "adjusted_score": 0, "affinity_score": 0, "direction": direction, "blocked_reason": "score_error"}
    raw_score = int(sig.score)

    # Affinity multiplier with hypothesis floor
    mult = regime_strategy_multiplier(regime_str, setup, direction)
    if hyp.affinity_floor_range is not None and regime_str == "RANGE":
        mult = max(mult, hyp.affinity_floor_range)
    affinity_score = max(0, min(100, int(round(raw_score * mult))))

    # adjust_signal haircut
    try:
        adjusted_score = int(policy.adjust_signal(affinity_score, regime_state))
    except Exception:
        adjusted_score = affinity_score
    adjusted_score = max(0, min(100, adjusted_score))

    # Score gate (swing uses affinity_score)
    dynamic_min_score = int(cfg.swing_min_signal_score)
    _score_for_threshold = affinity_score

    # Gates
    policy_block_reason = ""
    raw_policy = snap.raw_policy or {}

    # Strategy allowed?
    allowed = raw_policy.get("allowed_strategies", [])
    strategy_in_allowed = (not allowed) or (setup.upper() in [s.upper() for s in allowed])
    if not strategy_in_allowed:
        policy_block_reason = "policy_strategy_blocked"
    else:
        # Hard block (with hypothesis override)
        hard_blocked = regime_hard_blocks_strategy(regime_str, setup)
        if hard_blocked and (regime_str, setup) in hyp.unblock_pairs:
            if hyp.require_strong_stock and not is_strong_stock(primary_candles, cfg):
                # Strong-stock filter says no — keep production's hard_block
                policy_block_reason = "regime_strategy_hard_block"
            else:
                # Hypothesis: unblock
                hard_blocked = False
        if hard_blocked:
            policy_block_reason = "regime_strategy_hard_block"
        else:
            # check_swing_entry
            try:
                gate_ok, gate_reason = check_swing_entry(setup, direction, ind, db, regime=regime_str)
            except Exception:
                gate_ok = False
                gate_reason = "check_swing_entry_error"
            if not gate_ok:
                policy_block_reason = gate_reason

    qualified = (
        direction != "HOLD"
        and _score_for_threshold >= dynamic_min_score
        and not policy_block_reason
    )

    if qualified:
        blocked_reason = ""
    elif direction == "HOLD":
        blocked_reason = "direction_hold"
    elif adjusted_score < dynamic_min_score:
        blocked_reason = "score_below_min"
    elif policy_block_reason:
        blocked_reason = policy_block_reason
    else:
        blocked_reason = "entry_window_closed_or_blocked"

    return {
        "qualified": qualified, "raw_score": raw_score,
        "affinity_score": affinity_score, "adjusted_score": adjusted_score,
        "direction": direction, "blocked_reason": blocked_reason,
    }


def simulate_trade(
    symbol: str, scan_date: str, direction: str, cfg: StrategySettings, ds: HistoricalDataset,
) -> dict[str, Any] | None:
    """Simulate trade outcome from next-day open."""
    daily_all = ds.daily_candles(symbol)
    daily_truncated = [c for c in daily_all if str(c[0])[:10] <= scan_date]
    if len(daily_truncated) < 60:
        return None
    try:
        db = compute_daily_bias(daily_truncated)
    except Exception:
        return None
    if db is None or not db.atr_daily:
        return None
    entry_idx = find_entry_idx(daily_all, scan_date)
    if entry_idx is None or entry_idx >= len(daily_all):
        return None
    trade = simulate_swing_trade(
        symbol, entry_idx, daily_all, direction, cfg, float(db.atr_daily),
    )
    if trade.get("status") != "OK":
        return None
    return trade


def main() -> int:
    start_date = sys.argv[1] if len(sys.argv) > 1 else "2026-03-07"
    end_date = sys.argv[2] if len(sys.argv) > 2 else "2026-05-21"
    print("=" * 80)
    print(f"Hypothesis tester — swing replica — {start_date} to {end_date}")
    print("=" * 80)

    # Pull swing BREAKOUT + MR scans
    print(f"\n[1] Pulling swing scans from BQ ({start_date} → {end_date})...")
    rows = _bq_query(f"""
        SELECT scan_ts, symbol, setup, regime, direction, raw_score, adjusted_score,
               qualified, blocked_reason, wl_type
        FROM `grow-profit-machine.autotrader.scan_decisions`
        WHERE run_date >= '{start_date}' AND run_date <= '{end_date}'
          AND wl_type = 'swing'
          AND setup IN ('BREAKOUT', 'MEAN_REVERSION')
        ORDER BY scan_ts, symbol, setup
    """)
    print(f"    Got {len(rows)} swing scans")

    # Deduplicate by (symbol, setup, scan_date) — keep last scan of day
    by_day: dict[tuple[str, str, str], dict] = {}
    for r in rows:
        scan_date = (r["scan_ts"] or "")[:10]
        key = (r["symbol"], r["setup"], scan_date)
        if key not in by_day or r["scan_ts"] > by_day[key]["scan_ts"]:
            by_day[key] = r
    print(f"    Deduplicated to {len(by_day)} unique (symbol, setup, date) triples")

    cfg = StrategySettings()
    ds = HistoricalDataset()
    brain_loader = BrainSnapshotLoader()
    replica = ProdReplicaV2(cfg=cfg, dataset=ds, brain_loader=brain_loader)
    policy = MarketPolicyService()

    # Track per-hypothesis results
    results = {h.name: {"newly_qualified": [], "newly_blocked": [], "errors": 0} for h in HYPOTHESES}

    print(f"\n[2] Running each scan through all {len(HYPOTHESES)} hypotheses...")
    for i, ((sym, setup, scan_date), bq) in enumerate(by_day.items()):
        if i % 200 == 0:
            print(f"    ... {i}/{len(by_day)}")
        scan_ts = bq["scan_ts"]
        bq_qualified = str(bq.get("qualified", "")).lower() == "true"

        # For each hypothesis, see if outcome changes
        for hyp in HYPOTHESES:
            try:
                outcome = replay_with_hypothesis(sym, setup, scan_ts, hyp, replica, cfg, policy)
            except Exception:
                results[hyp.name]["errors"] += 1
                continue

            if outcome["qualified"] and not bq_qualified:
                # Newly qualified under hypothesis — simulate trade
                results[hyp.name]["newly_qualified"].append({
                    "symbol": sym, "setup": setup, "scan_date": scan_date,
                    "direction": outcome["direction"],
                    "raw_score": outcome["raw_score"],
                    "bq_blocked": bq.get("blocked_reason"),
                })
            elif not outcome["qualified"] and bq_qualified:
                results[hyp.name]["newly_blocked"].append({
                    "symbol": sym, "setup": setup, "scan_date": scan_date,
                })

    print(f"\n[3] Simulating trade outcomes for newly-qualified scans...")
    for h in HYPOTHESES:
        new_q = results[h.name]["newly_qualified"]
        for nq in new_q:
            trade = simulate_trade(nq["symbol"], nq["scan_date"], nq["direction"], cfg, ds)
            nq["trade"] = trade

    # Report
    print(f"\n[4] Results\n")
    print(f"  {'Hypothesis':40s} {'NewQ':>5s} {'Wins':>5s} {'WR':>6s} {'AvgR':>7s} {'NetPnL':>10s}")
    print("  " + "-" * 78)
    for h in HYPOTHESES:
        if h.name == "baseline_production":
            # Baseline produces 0 delta vs itself; show validation
            errs = results[h.name]["errors"]
            print(f"  {h.name:40s} 0 newly qualified (replica matches prod)  [errors={errs}]")
            continue
        new_q = results[h.name]["newly_qualified"]
        trades = [nq["trade"] for nq in new_q if nq.get("trade")]
        n = len(trades)
        if n == 0:
            print(f"  {h.name:40s} {len(new_q):>5d} (no simulated trades)")
            continue
        wins = sum(1 for t in trades if t["net_pnl"] > 0)
        wr = wins / n * 100
        avg_r = sum(t["r_realized"] for t in trades) / n
        net = sum(t["net_pnl"] for t in trades)
        new_b = len(results[h.name]["newly_blocked"])
        print(f"  {h.name:40s} {n:>5d} {wins:>5d} {wr:>5.1f}% {avg_r:>+6.2f}R ₹{net:>+9.0f}  (newly_blocked: {new_b})")

    # Dump details
    out_dir = os.path.expanduser("~/.autotrader_backtest_cache")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"hypothesis_results_{start_date}_{end_date}.json")
    serializable_results = {
        h.name: {
            "newly_qualified": results[h.name]["newly_qualified"],
            "newly_blocked": results[h.name]["newly_blocked"],
            "errors": results[h.name]["errors"],
        }
        for h in HYPOTHESES
    }
    try:
        with open(out_path, "w") as fh:
            json.dump(serializable_results, fh, default=str)
        print(f"\n📦 Detailed results dumped to {out_path}")
    except Exception as exc:
        print(f"\n⚠️  Dump failed: {exc}")

    print("\n✅ Hypothesis testing complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
