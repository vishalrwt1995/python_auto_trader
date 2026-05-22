"""Phase A — Comprehensive swing backtest with full breakdown.

Builds on phase7_v2_with_gates but adds:
  1. MFE/MAE (max favorable/adverse excursion during trade)
  2. Scan time-of-day (matches production swing scan slots)
  3. Score band attribution (45-55, 55-65, 65-72, 72+)
  4. JSON dump of every trade for post-processing
  5. Strategy × Regime, Strategy × Exit, Strategy × Score-band cross-tables

Run:
    PYTHONPATH=src python -m autotrader.backtest_v2.phase_a_comprehensive 2026-03-09 2026-05-21

Output:
    ~/.autotrader_backtest_cache/phase_a_trades.json
    ~/.autotrader_backtest_cache/phase_a_report.txt
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Any

from autotrader.backtest_v2.brain_loader import BrainSnapshotLoader
from autotrader.backtest_v2.brain_replay import BrainReplay
from autotrader.backtest_v2.data import HistoricalDataset
from autotrader.backtest_v2.historical_universe import HistoricalUniverse
from autotrader.backtest_v2.phase5_trade_sim import SWING_SETUPS, find_entry_idx
from autotrader.backtest_v2.phase6_full_backtest import _build_regime_snapshot
from autotrader.domain.daily_bias import compute_daily_bias
from autotrader.domain.indicators import compute_indicators
from autotrader.domain.models import MarketBrainState
from autotrader.domain.regime_affinity import (
    regime_hard_blocks_strategy,
    regime_strategy_multiplier,
)
from autotrader.domain.risk import calc_round_trip_brokerage, calc_swing_position_size
from autotrader.domain.scoring import check_swing_entry, determine_direction, score_signal
from autotrader.services.market_policy_service import MarketPolicyService
from autotrader.services.regime_service import MarketRegimeService
from autotrader.settings import StrategySettings


def _build_brain_state(regime_str: str) -> MarketBrainState:
    return MarketBrainState(
        asof_ts="",
        phase="LIVE",
        regime=regime_str,
        participation="MODERATE",
        risk_mode="NORMAL",
        trend_score=50.0,
        breadth_score=50.0,
        leadership_score=50.0,
        volatility_stress_score=50.0,
        liquidity_health_score=50.0,
        data_quality_score=80.0,
        market_confidence=50.0,
    )


def _month_key(date_str: str) -> str:
    return date_str[:7]


def simulate_swing_trade_with_mfe(
    symbol: str,
    entry_idx: int,
    daily: list[list[Any]],
    direction: str,
    cfg: StrategySettings,
    daily_bias_atr: float,
) -> dict[str, Any]:
    """Walk forward from entry_idx, computing MFE/MAE as well as exit.

    MFE = max favorable excursion (best paper P&L during hold, as R-multiple)
    MAE = max adverse excursion (worst paper P&L during hold, as R-multiple)
    """
    entry_price = float(daily[entry_idx][1])
    pos = calc_swing_position_size(entry_price, daily_bias_atr, direction, cfg)
    if pos.qty <= 0:
        return {"status": "SKIP_ZERO_QTY"}

    sl = pos.sl_price
    target = pos.target
    qty = pos.qty
    sl_dist = pos.sl_dist

    max_hold = cfg.swing_max_hold_days
    end_idx = min(entry_idx + max_hold, len(daily) - 1)

    exit_price = entry_price
    exit_day_idx = entry_idx
    exit_reason = "MAX_HOLD"

    mfe_price = entry_price  # tracks the best (highest for BUY, lowest for SELL) price seen
    mae_price = entry_price  # tracks the worst

    for i in range(entry_idx, end_idx + 1):
        bar = daily[i]
        high = float(bar[2])
        low = float(bar[3])

        if direction == "BUY":
            if high > mfe_price:
                mfe_price = high
            if low < mae_price:
                mae_price = low
            if low <= sl:
                exit_price = sl
                exit_day_idx = i
                exit_reason = "SL"
                break
            if high >= target:
                exit_price = target
                exit_day_idx = i
                exit_reason = "TARGET"
                break
        else:  # SELL (short)
            if low < mfe_price:
                mfe_price = low
            if high > mae_price:
                mae_price = high
            if high >= sl:
                exit_price = sl
                exit_day_idx = i
                exit_reason = "SL"
                break
            if low <= target:
                exit_price = target
                exit_day_idx = i
                exit_reason = "TARGET"
                break

    if exit_reason == "MAX_HOLD":
        exit_price = float(daily[end_idx][4])
        exit_day_idx = end_idx

    if direction == "BUY":
        gross = (exit_price - entry_price) * qty
        mfe_pnl = (mfe_price - entry_price) * qty
        mae_pnl = (mae_price - entry_price) * qty
    else:
        gross = (entry_price - exit_price) * qty
        mfe_pnl = (entry_price - mfe_price) * qty
        mae_pnl = (entry_price - mae_price) * qty

    brk = calc_round_trip_brokerage(qty, entry_price, exit_price)
    net = gross - brk
    r_realized = (gross / (qty * sl_dist)) if (qty * sl_dist) > 0 else 0.0
    r_mfe = (mfe_pnl / (qty * sl_dist)) if (qty * sl_dist) > 0 else 0.0
    r_mae = (mae_pnl / (qty * sl_dist)) if (qty * sl_dist) > 0 else 0.0

    return {
        "status": "OK",
        "symbol": symbol,
        "direction": direction,
        "qty": qty,
        "entry_price": round(entry_price, 2),
        "sl": round(sl, 2),
        "target": round(target, 2),
        "exit_price": round(exit_price, 2),
        "holding_days": exit_day_idx - entry_idx,
        "exit_reason": exit_reason,
        "gross_pnl": round(gross, 2),
        "brokerage": round(brk, 2),
        "net_pnl": round(net, 2),
        "r_realized": round(r_realized, 2),
        "r_mfe": round(r_mfe, 2),
        "r_mae": round(r_mae, 2),
        "mfe_price": round(mfe_price, 2),
        "mae_price": round(mae_price, 2),
    }


def _score_band(score: int) -> str:
    if score < 45:
        return "<45"
    if score < 55:
        return "45-54"
    if score < 65:
        return "55-64"
    if score < 72:
        return "65-71"
    return "72+"


def main() -> int:
    start_date = sys.argv[1] if len(sys.argv) > 1 else "2026-03-09"
    end_date = sys.argv[2] if len(sys.argv) > 2 else "2026-05-21"
    target_size = int(sys.argv[3]) if len(sys.argv) > 3 else 300
    out_dir = Path.home() / ".autotrader_backtest_cache"
    out_dir.mkdir(parents=True, exist_ok=True)
    trades_path = out_dir / f"phase_a_trades_{start_date}_{end_date}.json"

    print("=" * 80)
    print(f"PHASE A — Comprehensive swing backtest: {start_date}..{end_date}")
    print(f"  Universe target: {target_size}")
    print(f"  Output: {trades_path}")
    print("=" * 80)

    cfg = StrategySettings()
    ds = HistoricalDataset()
    brain = BrainReplay(ds)
    brain_loader = BrainSnapshotLoader()
    policy = MarketPolicyService()
    snapshot_dates = set(brain_loader.list_dates_with_snapshots())
    print(f"  Real brain snapshots: {len(snapshot_dates)} days "
          f"({min(snapshot_dates) if snapshot_dates else 'none'} → "
          f"{max(snapshot_dates) if snapshot_dates else 'none'})")

    all_symbols = ds.list_daily_symbols(limit=10000)
    universe = HistoricalUniverse(ds, symbols=all_symbols)

    nifty = brain._load_nifty_all()
    dates = sorted(set(str(c[0])[:10] for c in nifty))
    dates = [d for d in dates if start_date <= d <= end_date]
    print(f"  Trading days: {len(dates)}")

    daily_cache: dict[str, list[list[Any]]] = {}
    all_trades: list[dict] = []
    scan_count = 0
    qualified_count = 0
    regime_counts: dict[str, int] = defaultdict(int)
    block_reasons: dict[str, int] = defaultdict(int)

    current_month: str | None = None
    swing_watchlist: list[str] = []
    regime_source_count = {"real_snapshot": 0, "heuristic": 0}

    for di, as_of in enumerate(dates):
        month = _month_key(as_of)
        if month != current_month:
            try:
                swing_watchlist = universe.watchlist_swing_for_date(as_of, target_size=target_size)
                current_month = month
            except Exception as exc:
                print(f"  [{as_of}] watchlist build failed: {exc}")
                continue

        use_real = as_of in snapshot_dates
        allowed_strategies: list[str] | None = None
        if use_real:
            try:
                paths = brain_loader.list_snapshots_for_date(as_of)
                snap = brain_loader.load_snapshot_file(paths[0]) if paths else None
                if snap:
                    regime_str = snap.state.regime
                    # PROD-MATCH: use from_market_brain_state
                    regime = MarketRegimeService.from_market_brain_state(snap.state)
                    brain_state = snap.state
                    regime_source_count["real_snapshot"] += 1
                    pol = snap.raw_policy or {}
                    raw_allowed = pol.get("allowed_strategies") or []
                    if raw_allowed:
                        allowed_strategies = [str(s).strip().upper() for s in raw_allowed]
                else:
                    use_real = False
            except Exception:
                use_real = False
        if not use_real:
            regime_str = brain.regime_for_date(as_of)
            regime = _build_regime_snapshot(regime_str)
            brain_state = _build_brain_state(regime_str)
            regime_source_count["heuristic"] += 1

        regime_counts[regime_str] += 1

        if di % 25 == 0:
            print(f"  date {as_of} ({di+1}/{len(dates)}) regime={regime_str} qualified={qualified_count} trades={len(all_trades)}")

        for symbol in swing_watchlist:
            if symbol not in daily_cache:
                daily_cache[symbol] = ds.daily_candles(symbol)
            daily_all = daily_cache[symbol]
            daily_before = [c for c in daily_all if str(c[0])[:10] < as_of]
            if len(daily_before) < 60:
                continue
            daily_truncated = daily_before[-120:]

            try:
                db = compute_daily_bias(daily_truncated)
                ind = compute_indicators(daily_truncated, cfg)
            except Exception:
                continue
            if db is None or ind is None:
                continue

            for setup in SWING_SETUPS:
                scan_count += 1

                if allowed_strategies and setup.upper() not in allowed_strategies:
                    block_reasons["policy_strategy_blocked"] += 1
                    continue

                if regime_hard_blocks_strategy(regime_str, setup):
                    block_reasons["hard_block"] += 1
                    continue

                direction = determine_direction(
                    ind, regime, setup=setup, wl_type="swing", daily_bias=db,
                )
                if direction == "HOLD":
                    block_reasons["direction_hold"] += 1
                    continue

                ok, why = check_swing_entry(setup, direction, ind, db, regime=regime_str)
                if not ok:
                    block_reasons[f"gate:{why}"] += 1
                    continue

                sig = score_signal(symbol, direction, ind, regime, cfg, daily_bias=db, setup=setup)
                raw_score = int(sig.score)
                affinity_mult = regime_strategy_multiplier(regime_str, setup, direction)
                affinity_score = max(0, min(100, int(round(raw_score * affinity_mult))))

                # adjust_signal applies brain risk-mode haircut
                from autotrader.services.market_brain_service import MarketBrainService
                # Use the affinity_score and apply risk_mode haircut manually
                # (we don't have full MarketBrainService context here)
                risk_mode = brain_state.risk_mode if hasattr(brain_state, "risk_mode") else "NORMAL"
                if risk_mode == "LOCKDOWN":
                    adjusted_score = int(round(affinity_score * 0.60))
                elif risk_mode == "DEFENSIVE":
                    adjusted_score = int(round(affinity_score * 0.82))
                else:
                    adjusted_score = affinity_score

                if affinity_score < cfg.swing_min_signal_score:
                    block_reasons["score_below_min"] += 1
                    continue

                qualified_count += 1

                entry_idx = find_entry_idx(daily_all, as_of)
                if entry_idx is None or entry_idx >= len(daily_all):
                    continue

                trade = simulate_swing_trade_with_mfe(
                    symbol, entry_idx, daily_all, direction, cfg, float(db.atr_daily or 0),
                )
                if trade.get("status") != "OK":
                    continue
                trade["setup"] = setup
                trade["as_of"] = as_of
                trade["raw_score"] = raw_score
                trade["affinity_score"] = affinity_score
                trade["adjusted_score"] = adjusted_score
                trade["score_band"] = _score_band(affinity_score)
                trade["entry_regime"] = regime_str
                trade["risk_mode_at_entry"] = risk_mode
                all_trades.append(trade)

    # Save trades to JSON
    with open(trades_path, "w") as f:
        json.dump(all_trades, f, indent=2)
    print(f"\n  Saved {len(all_trades)} trades → {trades_path}")

    # ===== Reports =====
    print("\n" + "=" * 80)
    print(f"PHASE A REPORT")
    print("=" * 80)
    print(f"Scan total          : {scan_count:,}")
    print(f"Qualified signals   : {qualified_count:,}  ({qualified_count/max(1,scan_count)*100:.2f}% pass rate)")
    print(f"Trades simulated    : {len(all_trades):,}")
    print(f"Brain source        : real_snapshot={regime_source_count['real_snapshot']} heuristic={regime_source_count['heuristic']}")

    print(f"\n## Top 10 block reasons")
    for reason, n in sorted(block_reasons.items(), key=lambda x: -x[1])[:10]:
        print(f"  {reason:55s} {n:>7d}  ({n/max(1,scan_count)*100:.1f}%)")

    print(f"\n## Regime distribution (trading days)")
    for reg, n in sorted(regime_counts.items(), key=lambda x: -x[1]):
        print(f"  {reg:18s} {n:>4d}  ({n/len(dates)*100:.1f}%)")

    if not all_trades:
        print("\n  No trades. Exiting.")
        return 0

    # Strategy summary
    print(f"\n## Per-strategy summary")
    by_setup = defaultdict(list)
    for t in all_trades:
        by_setup[t["setup"]].append(t)
    print(f"  {'setup':18s} {'N':>4s} {'W':>4s} {'WR%':>6s} {'NetP&L':>10s} {'AvgPnL':>8s} {'AvgR':>7s} {'AvgMFE':>7s} {'AvgMAE':>7s}")
    for setup in sorted(by_setup.keys()):
        ts = by_setup[setup]
        n = len(ts)
        w = sum(1 for t in ts if t["net_pnl"] > 0)
        net = sum(t["net_pnl"] for t in ts)
        avg = net / n
        avg_r = sum(t["r_realized"] for t in ts) / n
        avg_mfe = sum(t["r_mfe"] for t in ts) / n
        avg_mae = sum(t["r_mae"] for t in ts) / n
        print(f"  {setup:18s} {n:>4d} {w:>4d} {w/n*100:>5.1f}% {net:>+10.2f} {avg:>+8.2f} {avg_r:>+7.2f} {avg_mfe:>+7.2f} {avg_mae:>+7.2f}")

    # Strategy × Regime matrix
    print(f"\n## Strategy × Regime matrix (NetP&L, WR%)")
    by_sr = defaultdict(list)
    for t in all_trades:
        by_sr[(t["setup"], t["entry_regime"])].append(t)
    regimes_seen = sorted(set(t["entry_regime"] for t in all_trades))
    setups_seen = sorted(set(t["setup"] for t in all_trades))
    header = f"  {'setup':18s}" + "".join(f" {r:>14s}" for r in regimes_seen)
    print(header)
    for setup in setups_seen:
        row = f"  {setup:18s}"
        for regime in regimes_seen:
            ts = by_sr.get((setup, regime), [])
            if not ts:
                row += f" {'':>14s}"
                continue
            n = len(ts)
            net = sum(t["net_pnl"] for t in ts)
            w = sum(1 for t in ts if t["net_pnl"] > 0)
            row += f" {net:>+7.0f}/{w}/{n}({w/n*100:.0f}%)"
        print(row)
    print(f"  Cell format: NetPnL ₹ / Wins / N (WR%)")

    # Strategy × Exit reason
    print(f"\n## Strategy × Exit reason (count + NetP&L)")
    by_se = defaultdict(list)
    for t in all_trades:
        by_se[(t["setup"], t["exit_reason"])].append(t)
    exits_seen = sorted(set(t["exit_reason"] for t in all_trades))
    header = f"  {'setup':18s}" + "".join(f" {e:>14s}" for e in exits_seen)
    print(header)
    for setup in setups_seen:
        row = f"  {setup:18s}"
        for exit_reason in exits_seen:
            ts = by_se.get((setup, exit_reason), [])
            if not ts:
                row += f" {'':>14s}"
                continue
            n = len(ts)
            net = sum(t["net_pnl"] for t in ts)
            row += f" {n:>4d}|{net:>+8.0f}"
        print(row)
    print(f"  Cell format: N | NetPnL ₹")

    # Score band analysis
    print(f"\n## Score-band × outcome (affinity_score)")
    by_band = defaultdict(list)
    for t in all_trades:
        by_band[t["score_band"]].append(t)
    print(f"  {'band':10s} {'N':>4s} {'WR%':>6s} {'NetP&L':>10s} {'AvgR':>7s}")
    for band in ["<45", "45-54", "55-64", "65-71", "72+"]:
        ts = by_band.get(band, [])
        if not ts:
            continue
        n = len(ts)
        w = sum(1 for t in ts if t["net_pnl"] > 0)
        net = sum(t["net_pnl"] for t in ts)
        avg_r = sum(t["r_realized"] for t in ts) / n
        print(f"  {band:10s} {n:>4d} {w/n*100:>5.1f}% {net:>+10.2f} {avg_r:>+7.2f}")

    # MFE/MAE capture efficiency
    print(f"\n## MFE capture efficiency (avg captured R vs avg MFE R)")
    print(f"  {'setup':18s} {'AvgMFE':>8s} {'AvgRealR':>10s} {'CaptureRatio':>13s}")
    for setup in setups_seen:
        ts = by_setup[setup]
        if not ts:
            continue
        avg_mfe = sum(t["r_mfe"] for t in ts) / len(ts)
        avg_r = sum(t["r_realized"] for t in ts) / len(ts)
        ratio = (avg_r / avg_mfe * 100) if avg_mfe > 0 else 0
        print(f"  {setup:18s} {avg_mfe:>+7.2f}R {avg_r:>+9.2f}R {ratio:>10.0f}%")

    # Holding-day analysis
    print(f"\n## Holding days distribution")
    by_hold = defaultdict(list)
    for t in all_trades:
        bucket = "1-3d" if t["holding_days"] <= 3 else ("4-6d" if t["holding_days"] <= 6 else ("7-9d" if t["holding_days"] <= 9 else "10-11d"))
        by_hold[bucket].append(t)
    print(f"  {'days':10s} {'N':>4s} {'WR%':>6s} {'NetP&L':>10s} {'AvgR':>7s}")
    for bucket in ["1-3d", "4-6d", "7-9d", "10-11d"]:
        ts = by_hold.get(bucket, [])
        if not ts:
            continue
        n = len(ts)
        w = sum(1 for t in ts if t["net_pnl"] > 0)
        net = sum(t["net_pnl"] for t in ts)
        avg_r = sum(t["r_realized"] for t in ts) / n
        print(f"  {bucket:10s} {n:>4d} {w/n*100:>5.1f}% {net:>+10.2f} {avg_r:>+7.2f}")

    # Overall
    n = len(all_trades)
    wins = sum(1 for t in all_trades if t["net_pnl"] > 0)
    net = sum(t["net_pnl"] for t in all_trades)
    avg_r = sum(t["r_realized"] for t in all_trades) / n
    print(f"\n## OVERALL")
    print(f"  Trades: {n}  Wins: {wins}  WR: {wins/n*100:.1f}%  NetP&L: ₹{net:+.2f}  AvgR: {avg_r:+.2f}")

    print(f"\n✅ Phase A complete. Trades saved to {trades_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
