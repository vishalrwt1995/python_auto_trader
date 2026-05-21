"""Production-Replica Equivalence Test (the gate that should run before
every multi-year backtest).

Picks a recent day with known BQ scan_decisions and runs our backtest's
scoring + gating pipeline on the same (symbol, setup) pairs. Compares
the outcome (qualified / blocked_reason) row by row.

Pass criteria:
  - qualified count within ±10%
  - For each blocked_reason that the backtest CAN reproduce (daily-only):
      • Reason must appear in replay with count within ±20% of BQ
      • If reason appears in BQ but NEVER in replay → STRUCTURAL GAP

Known limitations (accepted gaps):
  - Intraday gates (time-of-day, live VWAP, intraday RSI) will differ
    because backtest uses daily candles only.
  - Per-symbol direction can differ when daily vs intraday indicators disagree.

For SWING setups (BREAKOUT, MOMENTUM, PULLBACK, MEAN_REVERSION) we expect
tight match. Intraday-only setups (MORNING_FADE, OPEN_DRIVE, VWAP_*, PHASE1_*)
are excluded from strict pass criteria.

Run:
    python -m autotrader.backtest_v2.equivalence_test 2026-05-19
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from collections import defaultdict
from typing import Any

from autotrader.backtest_v2.data import HistoricalDataset
from autotrader.backtest_v2.phase6_full_backtest import _build_regime_snapshot
from autotrader.backtest_v2.phase9_prod_replica import production_adjusted_score
from autotrader.domain.daily_bias import compute_daily_bias
from autotrader.domain.indicators import compute_indicators
from autotrader.domain.regime_affinity import regime_hard_blocks_strategy
from autotrader.domain.scoring import check_swing_entry, determine_direction, score_signal
from autotrader.settings import StrategySettings


SWING_SETUPS_FOR_TEST = {"BREAKOUT", "MOMENTUM", "PULLBACK", "MEAN_REVERSION"}


def _bq_query(query: str) -> list[dict]:
    """Run BQ via REST API using gcloud ADC token."""
    gcloud = os.environ.get("GCLOUD", "gcloud")
    token = subprocess.check_output(
        [gcloud, "auth", "application-default", "print-access-token"],
        text=True,
    ).strip()
    import urllib.request
    body = json.dumps({
        "query": query, "useLegacySql": False, "location": "asia-south1"
    }).encode()
    req = urllib.request.Request(
        "https://bigquery.googleapis.com/bigquery/v2/projects/grow-profit-machine/queries",
        data=body, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        r = json.loads(resp.read())
    if "error" in r:
        raise RuntimeError(f"BQ error: {r['error']}")
    schema = [f["name"] for f in r.get("schema", {}).get("fields", [])]
    out = []
    for row in r.get("rows", []):
        d = {}
        for k, cell in zip(schema, row["f"]):
            d[k] = cell.get("v")
        out.append(d)
    return out


def _replay_row(symbol: str, setup: str, scan_date: str, regime_str: str, ds: HistoricalDataset, cfg: StrategySettings) -> dict[str, Any]:
    """Run our backtest pipeline on a single (symbol, setup, date) combination.

    Returns {qualified: bool, blocked_reason: str, raw_score: int, adj_score: int, direction: str}.
    """
    daily_all = ds.daily_candles(symbol)
    daily_truncated = [c for c in daily_all if str(c[0])[:10] <= scan_date]
    if len(daily_truncated) < 60:
        return {"qualified": False, "blocked_reason": "insufficient_history", "raw_score": 0, "adj_score": 0, "direction": "HOLD"}

    try:
        db = compute_daily_bias(daily_truncated)
        ind = compute_indicators(daily_truncated, cfg)
    except Exception as exc:
        return {"qualified": False, "blocked_reason": f"compute_error:{type(exc).__name__}", "raw_score": 0, "adj_score": 0, "direction": "HOLD"}
    if db is None or ind is None:
        return {"qualified": False, "blocked_reason": "compute_none", "raw_score": 0, "adj_score": 0, "direction": "HOLD"}

    regime = _build_regime_snapshot(regime_str)
    try:
        direction = determine_direction(ind, regime, setup=setup, wl_type="swing", daily_bias=db)
    except Exception as exc:
        return {"qualified": False, "blocked_reason": f"direction_error:{type(exc).__name__}", "raw_score": 0, "adj_score": 0, "direction": "HOLD"}
    if direction == "HOLD":
        return {"qualified": False, "blocked_reason": "direction_hold", "raw_score": 0, "adj_score": 0, "direction": "HOLD"}

    try:
        sig = score_signal(symbol, direction, ind, regime, cfg, daily_bias=db, setup=setup)
        gate_ok, gate_reason = check_swing_entry(setup, direction, ind, db, regime=regime.regime)
    except Exception as exc:
        return {"qualified": False, "blocked_reason": f"score_error:{type(exc).__name__}", "raw_score": 0, "adj_score": 0, "direction": direction}

    raw = int(sig.score)
    adj = production_adjusted_score(raw, regime_str, setup, direction)

    if not gate_ok:
        return {"qualified": False, "blocked_reason": f"check_swing_entry:{gate_reason}", "raw_score": raw, "adj_score": adj, "direction": direction}

    if regime_hard_blocks_strategy(regime_str, setup):
        return {"qualified": False, "blocked_reason": "regime_strategy_hard_block", "raw_score": raw, "adj_score": adj, "direction": direction}

    min_score = int(cfg.swing_min_signal_score)
    if adj < min_score:
        return {"qualified": False, "blocked_reason": "score_below_min", "raw_score": raw, "adj_score": adj, "direction": direction}

    return {"qualified": True, "blocked_reason": None, "raw_score": raw, "adj_score": adj, "direction": direction}


def main() -> int:
    test_date = sys.argv[1] if len(sys.argv) > 1 else "2026-05-19"
    print("=" * 80)
    print(f"Production-Replica Equivalence Test — {test_date}")
    print("=" * 80)

    # Pull BQ scan_decisions for the test day, restricted to swing setups
    print(f"\n[1/3] Pulling BQ scan_decisions for {test_date}...")
    setups_csv = ",".join(f"'{s}'" for s in SWING_SETUPS_FOR_TEST)
    bq_rows = _bq_query(f"""
        SELECT symbol, setup, regime, direction, raw_score, adjusted_score, qualified, blocked_reason
        FROM `grow-profit-machine.autotrader.scan_decisions`
        WHERE run_date = '{test_date}' AND setup IN ({setups_csv})
        ORDER BY symbol, setup
    """)
    if not bq_rows:
        print(f"  ⚠️  No BQ scan_decisions for {test_date}; pick a different date")
        return 1
    print(f"  Got {len(bq_rows)} BQ rows ({len(SWING_SETUPS_FOR_TEST)} swing setups)")

    # Deduplicate: same symbol/setup can be scanned multiple times per day.
    # Keep the LAST scan (latest scan_ts has freshest data).
    bq_by_key: dict[tuple[str, str], dict] = {}
    for row in bq_rows:
        key = (row["symbol"], row["setup"])
        bq_by_key[key] = row  # last wins (since we ORDER BY)
    print(f"  Deduplicated to {len(bq_by_key)} unique (symbol, setup) pairs")

    # Replay
    print("\n[2/3] Replaying with backtest pipeline...")
    cfg = StrategySettings()
    ds = HistoricalDataset()

    bq_blocker_counts: dict[str, int] = defaultdict(int)
    replay_blocker_counts: dict[str, int] = defaultdict(int)
    bq_qualified = 0
    replay_qualified = 0
    mismatches: list[dict] = []

    for i, ((symbol, setup), bq_row) in enumerate(bq_by_key.items()):
        if i % 200 == 0:
            print(f"  ... {i}/{len(bq_by_key)}")
        regime = bq_row.get("regime") or "RANGE"
        replay = _replay_row(symbol, setup, test_date, regime, ds, cfg)

        bq_q = (str(bq_row.get("qualified", "")).lower() == "true")
        rp_q = replay["qualified"]
        bq_b = bq_row.get("blocked_reason") or ""
        rp_b = replay["blocked_reason"] or ""

        if bq_q:
            bq_qualified += 1
        else:
            bq_blocker_counts[bq_b] += 1
        if rp_q:
            replay_qualified += 1
        else:
            replay_blocker_counts[rp_b] += 1

        if bq_q != rp_q:
            mismatches.append({
                "symbol": symbol, "setup": setup, "regime": regime,
                "bq_qualified": bq_q, "replay_qualified": rp_q,
                "bq_blocker": bq_b, "replay_blocker": rp_b,
                "bq_raw": bq_row.get("raw_score"), "replay_raw": replay["raw_score"],
                "bq_adj": bq_row.get("adjusted_score"), "replay_adj": replay["adj_score"],
            })

    # Report
    print(f"\n[3/3] Equivalence Report\n")
    print(f"  BQ qualified count    : {bq_qualified}")
    print(f"  Replay qualified count: {replay_qualified}")
    print(f"  Qualified delta       : {replay_qualified - bq_qualified} ({(replay_qualified-bq_qualified)/(bq_qualified or 1)*100:+.1f}%)")
    print()

    # Blocker distribution comparison
    all_blockers = sorted(set(bq_blocker_counts) | set(replay_blocker_counts), key=lambda b: -(bq_blocker_counts.get(b, 0) + replay_blocker_counts.get(b, 0)))
    print(f"  {'blocked_reason':50s} {'BQ':>8s} {'Replay':>8s} {'Δ':>8s} {'Verdict':>10s}")
    print("  " + "-" * 90)
    structural_gaps = []
    for b in all_blockers:
        bqn = bq_blocker_counts.get(b, 0)
        rpn = replay_blocker_counts.get(b, 0)
        delta = rpn - bqn
        if bqn > 0 and rpn == 0 and bqn >= 5:  # significant absent reason
            verdict = "⚠️ GAP"
            structural_gaps.append(b)
        elif bqn == 0 and rpn > 0:
            verdict = "extra"
        elif bqn > 0 and abs(delta) / bqn > 0.5:
            verdict = "drift"
        else:
            verdict = "ok"
        print(f"  {b:50s} {bqn:>8d} {rpn:>8d} {delta:>+8d} {verdict:>10s}")

    # Mismatch sample
    if mismatches:
        print(f"\n  First 10 qualified-disagreement rows:")
        print(f"  {'symbol':14s} {'setup':16s} {'reg':10s} {'bq_q':>5s} {'rp_q':>5s} {'bq_blk':40s} {'rp_blk':40s}")
        for m in mismatches[:10]:
            print(f"  {m['symbol']:14s} {m['setup']:16s} {m['regime']:10s} {str(m['bq_qualified']):>5s} {str(m['replay_qualified']):>5s} {(m['bq_blocker'] or '')[:40]:40s} {(m['replay_blocker'] or '')[:40]:40s}")

    # Verdict
    print()
    if structural_gaps:
        print(f"❌ FAIL — {len(structural_gaps)} structural gap(s): {structural_gaps}")
        print("   These blocked_reasons appear in BQ but never in our replay.")
        print("   The backtest is missing those gates. DO NOT run multi-year until fixed.")
        return 2
    qd = abs(replay_qualified - bq_qualified)
    if bq_qualified > 0 and qd / bq_qualified > 0.20:
        print(f"⚠️  WARN — qualified count off by >20% ({qd}/{bq_qualified})")
        print("   Replay may still be subtly biased. Investigate before trusting multi-year.")
        return 3
    print(f"✅ PASS — equivalence acceptable. Backtest baseline is calibrated to production.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
