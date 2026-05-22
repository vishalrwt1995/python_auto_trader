"""Phase E — Validate exit-rule variants with proper bar-by-bar simulation.

Re-runs the SAME entries Phase A produced, but with each exit variant from
exit_variants.py. Compares results exactly.

Output:
  ~/.autotrader_backtest_cache/phase_e_results_<window>.json
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from autotrader.backtest_v2.brain_loader import BrainSnapshotLoader
from autotrader.backtest_v2.brain_replay import BrainReplay
from autotrader.backtest_v2.data import HistoricalDataset
from autotrader.backtest_v2.exit_variants import VARIANTS
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
from autotrader.domain.scoring import check_swing_entry, determine_direction, score_signal
from autotrader.services.market_policy_service import MarketPolicyService
from autotrader.services.regime_service import MarketRegimeService
from autotrader.settings import StrategySettings


def _build_brain_state(regime_str: str) -> MarketBrainState:
    return MarketBrainState(
        asof_ts="", phase="LIVE", regime=regime_str,
        participation="MODERATE", risk_mode="NORMAL",
        trend_score=50.0, breadth_score=50.0, leadership_score=50.0,
        volatility_stress_score=50.0, liquidity_health_score=50.0,
        data_quality_score=80.0, market_confidence=50.0,
    )


def _month_key(date_str: str) -> str:
    return date_str[:7]


def main() -> int:
    start_date = sys.argv[1] if len(sys.argv) > 1 else "2026-03-09"
    end_date = sys.argv[2] if len(sys.argv) > 2 else "2026-05-21"
    target_size = int(sys.argv[3]) if len(sys.argv) > 3 else 300

    print("=" * 80)
    print(f"PHASE E — Exit-rule validation: {start_date}..{end_date}")
    print(f"  Variants tested: {list(VARIANTS.keys())}")
    print("=" * 80)

    cfg = StrategySettings()
    ds = HistoricalDataset()
    brain = BrainReplay(ds)
    brain_loader = BrainSnapshotLoader()
    snapshot_dates = set(brain_loader.list_dates_with_snapshots())
    all_symbols = ds.list_daily_symbols(limit=10000)
    universe = HistoricalUniverse(ds, symbols=all_symbols)
    nifty = brain._load_nifty_all()
    dates = sorted(set(str(c[0])[:10] for c in nifty))
    dates = [d for d in dates if start_date <= d <= end_date]
    print(f"  Trading days: {len(dates)}")

    daily_cache: dict[str, list[list[Any]]] = {}
    # Per-variant trade lists
    variant_trades: dict[str, list[dict]] = {v: [] for v in VARIANTS}
    current_month: str | None = None
    swing_watchlist: list[str] = []

    qualified_count = 0
    for di, as_of in enumerate(dates):
        month = _month_key(as_of)
        if month != current_month:
            try:
                swing_watchlist = universe.watchlist_swing_for_date(as_of, target_size=target_size)
                current_month = month
            except Exception:
                continue

        use_real = as_of in snapshot_dates
        allowed_strategies: list[str] | None = None
        if use_real:
            try:
                paths = brain_loader.list_snapshots_for_date(as_of)
                snap = brain_loader.load_snapshot_file(paths[0]) if paths else None
                if snap:
                    regime_str = snap.state.regime
                    regime = MarketRegimeService.from_market_brain_state(snap.state)
                    brain_state = snap.state
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

        if di % 25 == 0:
            print(f"  date {as_of} ({di+1}/{len(dates)}) qualified={qualified_count}")

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
                if allowed_strategies and setup.upper() not in allowed_strategies:
                    continue
                if regime_hard_blocks_strategy(regime_str, setup):
                    continue
                direction = determine_direction(
                    ind, regime, setup=setup, wl_type="swing", daily_bias=db,
                )
                if direction == "HOLD":
                    continue
                ok, why = check_swing_entry(setup, direction, ind, db, regime=regime_str)
                if not ok:
                    continue
                sig = score_signal(symbol, direction, ind, regime, cfg, daily_bias=db, setup=setup)
                raw_score = int(sig.score)
                affinity_mult = regime_strategy_multiplier(regime_str, setup, direction)
                affinity_score = max(0, min(100, int(round(raw_score * affinity_mult))))
                if affinity_score < cfg.swing_min_signal_score:
                    continue

                qualified_count += 1
                entry_idx = find_entry_idx(daily_all, as_of)
                if entry_idx is None or entry_idx >= len(daily_all):
                    continue

                # Run EVERY variant on the same entry
                for variant_name, variant_fn in VARIANTS.items():
                    trade = variant_fn(symbol, entry_idx, daily_all, direction, cfg, float(db.atr_daily or 0))
                    if trade.get("status") != "OK":
                        continue
                    trade["setup"] = setup
                    trade["as_of"] = as_of
                    trade["affinity_score"] = affinity_score
                    trade["entry_regime"] = regime_str
                    trade["variant"] = variant_name
                    variant_trades[variant_name].append(trade)

    # Save per-variant trades
    out_dir = Path.home() / ".autotrader_backtest_cache"
    for v_name, trades in variant_trades.items():
        path = out_dir / f"phase_e_{v_name}_{start_date}_{end_date}.json"
        path.write_text(json.dumps(trades, indent=2))

    print(f"\n  Saved trades for {len(variant_trades)} variants to {out_dir}/phase_e_*.json")

    # Reports
    print("\n" + "=" * 80)
    print("PHASE E RESULTS — Variant comparison")
    print("=" * 80)
    print(f"\n  {'variant':18s} {'N':>5s} {'W':>5s} {'WR%':>6s} {'NetP&L':>12s} {'AvgPnL':>9s} {'AvgR':>7s}")
    base_net = None
    for v_name in ["V0_baseline", "V1_half_r_full", "V2_half_r_50pct", "V3_trail_be"]:
        trades = variant_trades[v_name]
        if not trades:
            continue
        n = len(trades)
        w = sum(1 for t in trades if t["net_pnl"] > 0)
        net = sum(t["net_pnl"] for t in trades)
        avg_r = sum(t["r_realized"] for t in trades) / n
        marker = ""
        if base_net is None:
            base_net = net
        else:
            delta = net - base_net
            marker = f"  Δ={delta:+.0f}"
        print(f"  {v_name:18s} {n:>5d} {w:>5d} {w/n*100:>5.1f}% {net:>+12.2f} {net/n:>+9.2f} {avg_r:>+7.3f}{marker}")

    # Per-setup breakdown for the best variant
    print(f"\n  Per-setup breakdown by variant")
    setups = sorted(set(t["setup"] for trades in variant_trades.values() for t in trades))
    print(f"  {'setup':18s} ", end="")
    for v_name in ["V0_baseline", "V1_half_r_full", "V2_half_r_50pct", "V3_trail_be"]:
        print(f"{v_name:>22s}  ", end="")
    print()
    for setup in setups:
        print(f"  {setup:18s} ", end="")
        for v_name in ["V0_baseline", "V1_half_r_full", "V2_half_r_50pct", "V3_trail_be"]:
            ts = [t for t in variant_trades[v_name] if t["setup"] == setup]
            if not ts:
                print(f"{'-':>22s}  ", end="")
                continue
            n = len(ts)
            net = sum(t["net_pnl"] for t in ts)
            wr = sum(1 for t in ts if t["net_pnl"] > 0) / n * 100
            print(f"{net:>+8.0f}({wr:.0f}% N={n}) ", end="")
        print()

    # Exit-reason distribution under V1 (the simplest fix)
    if variant_trades["V1_half_r_full"]:
        print(f"\n  Exit reasons under V1_half_r_full:")
        v1_exits = defaultdict(list)
        for t in variant_trades["V1_half_r_full"]:
            v1_exits[t["exit_reason"]].append(t)
        for er, ts in sorted(v1_exits.items(), key=lambda x: -len(x[1])):
            n = len(ts)
            net = sum(t["net_pnl"] for t in ts)
            print(f"    {er:12s} N={n:>4d}  net=₹{net:+.2f}  avgR={sum(t['r_realized'] for t in ts)/n:+.3f}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
