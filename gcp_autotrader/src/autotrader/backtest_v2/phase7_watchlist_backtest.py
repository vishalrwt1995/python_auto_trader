"""Phase 7 — Multi-year backtest with REAL historical watchlist mechanism.

Key difference from Phase 6:
  - Universe + watchlist rebuilt per-date using production pure functions
    from `universe_v2.py` (compute_tradability_stats + classify_eligibility +
    assign_turnover_rank_and_bucket)
  - Scan ONLY watchlist members each date — same mechanism as live
  - Universe rebuilt monthly (universe membership changes slowly; daily
    rebuild would 30x the cost without changing decisions materially)

This is the closest we can get to "what would live actually have done"
without point-in-time Firestore snapshots.

Run:
    python -m autotrader.backtest_v2.phase7_watchlist_backtest 2024-01-01 2025-12-31
"""
from __future__ import annotations

import sys
from collections import defaultdict
from typing import Any

from autotrader.backtest_v2.brain_replay import BrainReplay
from autotrader.backtest_v2.data import HistoricalDataset
from autotrader.backtest_v2.historical_universe import HistoricalUniverse
from autotrader.backtest_v2.phase5_trade_sim import (
    SWING_SETUPS,
    find_entry_idx,
    simulate_swing_trade,
)
from autotrader.backtest_v2.phase6_full_backtest import _build_regime_snapshot
from autotrader.domain.daily_bias import compute_daily_bias
from autotrader.domain.indicators import compute_indicators
from autotrader.domain.scoring import check_swing_entry, determine_direction, score_signal
from autotrader.settings import StrategySettings


# Rebuild universe at the start of each month (universe membership is
# relatively stable; daily rebuild adds compute without changing decisions
# materially).
def _month_key(date_str: str) -> str:
    return date_str[:7]


def main() -> int:
    start_date = sys.argv[1] if len(sys.argv) > 1 else "2024-01-01"
    end_date = sys.argv[2] if len(sys.argv) > 2 else "2025-12-31"
    target_size = int(sys.argv[3]) if len(sys.argv) > 3 else 300

    print("=" * 70)
    print(f"Phase 7 — Watchlist-driven backtest: {start_date}..{end_date}")
    print(f"  Target watchlist size: {target_size}")
    print("=" * 70)

    cfg = StrategySettings()
    ds = HistoricalDataset()
    brain = BrainReplay(ds)

    # Discover all symbols with daily cache (cap to keep run time reasonable;
    # 800 covers most liquid + mid caps with comfortable headroom over
    # target_size=300).
    all_symbols = ds.list_daily_symbols(limit=10000)
    print(f"  Universe pool: {len(all_symbols)} symbols")

    universe = HistoricalUniverse(ds, symbols=all_symbols)

    # Trading-day list from NIFTY index
    nifty = brain._load_nifty_all()
    dates = sorted(set(str(c[0])[:10] for c in nifty))
    dates = [d for d in dates if start_date <= d <= end_date]
    print(f"  Trading days: {len(dates)}")

    daily_cache: dict[str, list[list[Any]]] = {}

    all_trades: list[dict] = []
    scan_count = 0
    qualified_count = 0
    regime_counts: dict[str, int] = defaultdict(int)
    watchlist_size_by_month: dict[str, int] = {}

    # Per-month watchlist computed on first day of that month
    current_month: str | None = None
    swing_watchlist: list[str] = []

    for di, as_of in enumerate(dates):
        month = _month_key(as_of)
        if month != current_month:
            # Rebuild watchlist at start of this month
            try:
                # Build watchlist using the FIRST date of the new month —
                # this represents the universe selection live would have
                # done on that month's first trading day.
                swing_watchlist = universe.watchlist_swing_for_date(as_of, target_size=target_size)
                watchlist_size_by_month[month] = len(swing_watchlist)
                current_month = month
                print(f"  [{as_of}] month rollover — watchlist size {len(swing_watchlist)}")
            except Exception as exc:
                print(f"  [{as_of}] watchlist build failed: {exc}")
                continue

        regime_str = brain.regime_for_date(as_of)
        regime_counts[regime_str] += 1
        regime = _build_regime_snapshot(regime_str)

        if di % 50 == 0:
            print(f"  date {as_of} ({di+1}/{len(dates)}) regime={regime_str} qualified_so_far={qualified_count} trades={len(all_trades)}")

        # Scan only watchlist members
        for symbol in swing_watchlist:
            if symbol not in daily_cache:
                daily_cache[symbol] = ds.daily_candles(symbol)
            daily_all = daily_cache[symbol]
            daily_truncated = [c for c in daily_all if str(c[0])[:10] <= as_of]
            if len(daily_truncated) < 60:
                continue

            try:
                db = compute_daily_bias(daily_truncated)
                ind = compute_indicators(daily_truncated, cfg)
            except Exception:
                continue
            if db is None or ind is None:
                continue

            for setup in SWING_SETUPS:
                scan_count += 1
                try:
                    direction = determine_direction(ind, regime, setup=setup, wl_type="swing", daily_bias=db)
                    sig = score_signal(symbol, direction, ind, regime, cfg, daily_bias=db, setup=setup)
                    gate_ok, _ = check_swing_entry(setup, direction, ind, db, regime=regime.regime)
                except Exception:
                    continue
                if direction == "HOLD" or not gate_ok or sig.score < cfg.swing_min_signal_score:
                    continue

                qualified_count += 1
                entry_idx = find_entry_idx(daily_all, as_of)
                if entry_idx is None or entry_idx >= len(daily_all):
                    continue
                trade = simulate_swing_trade(
                    symbol, entry_idx, daily_all, direction, cfg, float(db.atr_daily or 0),
                )
                if trade.get("status") != "OK":
                    continue
                trade["setup"] = setup
                trade["as_of"] = as_of
                trade["score"] = sig.score
                trade["entry_regime"] = regime_str
                all_trades.append(trade)

    # ===== Reports =====
    print()
    print(f"Scan total      : {scan_count:,}")
    print(f"Qualified signals: {qualified_count:,}")
    print(f"Trades simulated : {len(all_trades):,}")
    print()
    print(f"Watchlist size by month (median across {len(watchlist_size_by_month)} months):")
    sizes = sorted(watchlist_size_by_month.values())
    if sizes:
        median = sizes[len(sizes) // 2]
        print(f"  min={min(sizes)} median={median} max={max(sizes)}")
    print()
    print("Regime distribution (trading days):")
    total_days = sum(regime_counts.values())
    for r in sorted(regime_counts.keys()):
        n = regime_counts[r]
        print(f"  {r:14s} {n:>5d}  ({n/total_days*100:.1f}%)")

    if not all_trades:
        print("\n⚠️  No trades simulated")
        return 1

    # Per-setup
    print()
    print("PER SETUP:")
    print(f"  {'Setup':18s} {'N':>5s} {'Wins':>5s} {'WR':>6s} {'AvgR':>7s} {'NetPnL':>10s} {'AvgPnL':>9s}")
    by_setup: dict[str, list[dict]] = defaultdict(list)
    for t in all_trades:
        by_setup[t["setup"]].append(t)
    for setup, trades in sorted(by_setup.items(), key=lambda x: -len(x[1])):
        n = len(trades)
        wins = sum(1 for t in trades if t["net_pnl"] > 0)
        wr = wins / n * 100 if n else 0
        avg_r = sum(t["r_realized"] for t in trades) / n if n else 0
        net = sum(t["net_pnl"] for t in trades)
        avg_pnl = net / n if n else 0
        print(f"  {setup:18s} {n:>5d} {wins:>5d} {wr:>5.1f}% {avg_r:>+6.2f}R ₹{net:>+9.0f} ₹{avg_pnl:>+8.0f}")

    # Per-regime
    print()
    print("PER REGIME (at entry):")
    by_regime: dict[str, list[dict]] = defaultdict(list)
    for t in all_trades:
        by_regime[t["entry_regime"]].append(t)
    print(f"  {'Regime':14s} {'N':>5s} {'WR':>6s} {'AvgR':>7s} {'NetPnL':>10s}")
    for reg, trades in sorted(by_regime.items(), key=lambda x: -len(x[1])):
        n = len(trades)
        wins = sum(1 for t in trades if t["net_pnl"] > 0)
        wr = wins / n * 100 if n else 0
        avg_r = sum(t["r_realized"] for t in trades) / n if n else 0
        net = sum(t["net_pnl"] for t in trades)
        print(f"  {reg:14s} {n:>5d} {wr:>5.1f}% {avg_r:>+6.2f}R ₹{net:>+9.0f}")

    # Total
    n = len(all_trades)
    wins = sum(1 for t in all_trades if t["net_pnl"] > 0)
    wr = wins / n * 100
    avg_r = sum(t["r_realized"] for t in all_trades) / n
    net = sum(t["net_pnl"] for t in all_trades)
    print()
    print(f"  {'TOTAL':18s} {n:>5d} {wins:>5d} {wr:>5.1f}% {avg_r:>+6.2f}R ₹{net:>+9.0f}")

    # Per-year
    print()
    by_year: dict[str, dict] = defaultdict(lambda: {"trades": 0, "net": 0.0, "wins": 0})
    for t in all_trades:
        y = str(t["as_of"])[:4]
        by_year[y]["trades"] += 1
        by_year[y]["net"] += t["net_pnl"]
        if t["net_pnl"] > 0:
            by_year[y]["wins"] += 1
    print("PER YEAR:")
    print(f"  {'Year':6s} {'N':>5s} {'WR':>6s} {'NetPnL':>10s}")
    for y in sorted(by_year.keys()):
        d = by_year[y]
        wr = d["wins"] / d["trades"] * 100 if d["trades"] else 0
        print(f"  {y:6s} {d['trades']:>5d} {wr:>5.1f}% ₹{d['net']:>+9.0f}")

    print()
    print("✅ Phase 7 complete — backtest using REAL historical watchlist mechanism")
    return 0


if __name__ == "__main__":
    sys.exit(main())
