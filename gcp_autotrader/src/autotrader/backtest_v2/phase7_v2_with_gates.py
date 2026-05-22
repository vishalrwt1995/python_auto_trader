"""Phase 7 v2 — Long-range backtest WITH PRODUCTION GATES applied.

Fixes the critical gap in original Phase 7: missing production gates meant
the simulation overcounted trades in regimes where production hard-blocks
(CHOP especially). This version applies:

  1. regime_hard_blocks_strategy() — block setup-regime mismatches
  2. regime_strategy_multiplier() — apply affinity to raw score
  3. MarketPolicyService.adjust_signal() — brain risk-mode haircut
  4. Score threshold uses _affinity_score for swing (matches production
     trading_service.py:1375 — pre-haircut score for swing)
  5. check_swing_entry — already covered (uses imported production code)

Uses BrainReplay v1 (NIFTY-derived) for regime when stored snapshots don't
exist (pre-March 7, 2026). For dates where snapshots exist, could use them
directly — but for simplicity here we use the heuristic for all dates to
keep the multi-year run consistent.

Run:
    python -m autotrader.backtest_v2.phase7_v2_with_gates 2026-05-01 2026-05-21
    python -m autotrader.backtest_v2.phase7_v2_with_gates 2024-01-01 2025-12-31
"""
from __future__ import annotations

import sys
from collections import defaultdict
from typing import Any

from autotrader.backtest_v2.brain_loader import BrainSnapshotLoader
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
from autotrader.domain.models import MarketBrainState
from autotrader.domain.regime_affinity import (
    regime_hard_blocks_strategy,
    regime_strategy_multiplier,
)
from autotrader.domain.scoring import check_swing_entry, determine_direction, score_signal
from autotrader.services.market_policy_service import MarketPolicyService
from autotrader.settings import StrategySettings


def _build_brain_state(regime_str: str) -> MarketBrainState:
    """Construct a minimal MarketBrainState for adjust_signal().

    For 2024-2025 we don't have stored snapshots — use NORMAL risk_mode
    and default scores. The adjust_signal multiplier becomes 1.0 in
    NORMAL + non-CHOP/PANIC. In CHOP/PANIC it adds the 0.88× haircut.
    This is the simplest approximation that mirrors production's typical
    behavior outside of explicit DEFENSIVE/LOCKDOWN periods.
    """
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


def main() -> int:
    start_date = sys.argv[1] if len(sys.argv) > 1 else "2024-01-01"
    end_date = sys.argv[2] if len(sys.argv) > 2 else "2025-12-31"
    target_size = int(sys.argv[3]) if len(sys.argv) > 3 else 300

    print("=" * 75)
    print(f"Phase 7 v2 — backtest WITH production gates: {start_date}..{end_date}")
    print(f"  Target watchlist size: {target_size}")
    print(f"  Gates: hard_block + affinity_multiplier + adjust_signal + check_swing_entry")
    print("=" * 75)

    cfg = StrategySettings()
    ds = HistoricalDataset()
    brain = BrainReplay(ds)               # NIFTY heuristic fallback for old dates
    brain_loader = BrainSnapshotLoader()  # Real stored brain snapshots when available
    policy = MarketPolicyService()
    # Pre-load which dates have real brain snapshots (faster lookup)
    snapshot_dates = set(brain_loader.list_dates_with_snapshots())
    print(f"  Real brain snapshots available for: {len(snapshot_dates)} days "
          f"({min(snapshot_dates) if snapshot_dates else 'none'} → "
          f"{max(snapshot_dates) if snapshot_dates else 'none'})")

    all_symbols = ds.list_daily_symbols(limit=10000)
    print(f"  Universe pool: {len(all_symbols)} symbols")
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

    # Track WHY trades were filtered (for diagnostic)
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
                print(f"  [{as_of}] month rollover — watchlist {len(swing_watchlist)}")
            except Exception as exc:
                print(f"  [{as_of}] watchlist build failed: {exc}")
                continue

        # HYBRID BRAIN: use real stored snapshot when available, heuristic
        # for older dates. Real snapshot has full breadth + leadership +
        # flow data — far more accurate than NIFTY-only heuristic.
        use_real = as_of in snapshot_dates
        allowed_strategies: list[str] | None = None  # MarketPolicy gate (from snapshot)
        if use_real:
            # Find a representative snapshot for this date (use first/midday)
            try:
                paths = brain_loader.list_snapshots_for_date(as_of)
                # Pick the first snapshot of the day for consistency
                snap = brain_loader.load_snapshot_file(paths[0]) if paths else None
                if snap:
                    regime_str = snap.state.regime
                    regime = snap.to_regime_snapshot()
                    brain_state = snap.state
                    regime_source_count["real_snapshot"] += 1
                    # MarketPolicy.allowed_strategies from snapshot
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

        if di % 50 == 0:
            print(f"  date {as_of} ({di+1}/{len(dates)}) regime={regime_str} qualified={qualified_count} trades={len(all_trades)}")

        for symbol in swing_watchlist:
            if symbol not in daily_cache:
                daily_cache[symbol] = ds.daily_candles(symbol)
            daily_all = daily_cache[symbol]
            # Use daily candles BEFORE as_of (no look-ahead)
            daily_before = [c for c in daily_all if str(c[0])[:10] < as_of]
            if len(daily_before) < 60:
                continue
            # Match production lookback (120 days)
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

                # GATE 0a: MarketPolicy.allowed_strategies (from brain snapshot)
                # Production checks setup against the day's MarketPolicy.
                if allowed_strategies and setup.upper() not in allowed_strategies:
                    block_reasons["policy_strategy_blocked"] += 1
                    continue

                # GATE 0b: regime_hard_blocks_strategy (production's check)
                if regime_hard_blocks_strategy(regime_str, setup):
                    block_reasons["hard_block"] += 1
                    continue

                # 1. determine_direction (includes my Fix A for MR direction)
                try:
                    direction = determine_direction(ind, regime, setup=setup, wl_type="swing", daily_bias=db)
                except Exception:
                    continue
                if direction == "HOLD":
                    block_reasons["direction_hold"] += 1
                    continue

                # 2. check_swing_entry (production's swing gate — includes my Fix B)
                try:
                    gate_ok, gate_reason = check_swing_entry(setup, direction, ind, db, regime=regime_str)
                except Exception:
                    continue
                if not gate_ok:
                    block_reasons[f"check_swing_entry:{gate_reason}"] += 1
                    continue

                # 3. score_signal → raw_score
                try:
                    sig = score_signal(symbol, direction, ind, regime, cfg, daily_bias=db, setup=setup)
                except Exception:
                    continue
                raw_score = int(sig.score)

                # 4. Apply regime-strategy affinity multiplier
                affinity_mult = regime_strategy_multiplier(regime_str, setup, direction)
                affinity_score = max(0, min(100, int(round(raw_score * affinity_mult))))

                # 5. Apply brain adjust_signal haircut (for stored adjusted_score)
                try:
                    adjusted_score = int(policy.adjust_signal(affinity_score, brain_state))
                except Exception:
                    adjusted_score = affinity_score
                adjusted_score = max(0, min(100, adjusted_score))

                # 6. Score threshold — production uses _affinity_score for swing
                if affinity_score < cfg.swing_min_signal_score:
                    block_reasons["score_below_min"] += 1
                    continue

                qualified_count += 1

                # 7. Simulate the trade
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
                trade["raw_score"] = raw_score
                trade["affinity_score"] = affinity_score
                trade["adjusted_score"] = adjusted_score
                trade["entry_regime"] = regime_str
                all_trades.append(trade)

    # ===== Reports =====
    print()
    print(f"Scan total       : {scan_count:,}")
    print(f"Qualified signals: {qualified_count:,}")
    print(f"Trades simulated : {len(all_trades):,}")
    print(f"Brain source     : real_snapshot={regime_source_count['real_snapshot']} heuristic={regime_source_count['heuristic']}")
    print()
    print(f"Block reasons (top 10):")
    for reason, n in sorted(block_reasons.items(), key=lambda x: -x[1])[:10]:
        print(f"  {reason:50s} {n:>7d}")
    print()
    print(f"Regime distribution (trading days):")
    total_days = sum(regime_counts.values())
    for r in sorted(regime_counts.keys()):
        n = regime_counts[r]
        print(f"  {r:14s} {n:>5d}  ({n/total_days*100:.1f}%)")

    if not all_trades:
        print("\n⚠️  No trades simulated")
        return 1

    print()
    print(f"PER SETUP:")
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

    print()
    print(f"PER REGIME (at entry):")
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

    n = len(all_trades)
    wins = sum(1 for t in all_trades if t["net_pnl"] > 0)
    wr = wins / n * 100
    avg_r = sum(t["r_realized"] for t in all_trades) / n
    net = sum(t["net_pnl"] for t in all_trades)
    print()
    print(f"  {'TOTAL':18s} {n:>5d} {wins:>5d} {wr:>5.1f}% {avg_r:>+6.2f}R ₹{net:>+9.0f}")

    # Per year
    print()
    by_year: dict[str, dict] = defaultdict(lambda: {"trades": 0, "net": 0.0, "wins": 0})
    for t in all_trades:
        y = str(t["as_of"])[:4]
        by_year[y]["trades"] += 1
        by_year[y]["net"] += t["net_pnl"]
        if t["net_pnl"] > 0:
            by_year[y]["wins"] += 1
    print(f"PER YEAR:")
    print(f"  {'Year':6s} {'N':>5s} {'WR':>6s} {'NetPnL':>10s}")
    for y in sorted(by_year.keys()):
        d = by_year[y]
        wr_y = d["wins"] / d["trades"] * 100 if d["trades"] else 0
        print(f"  {y:6s} {d['trades']:>5d} {wr_y:>5.1f}% ₹{d['net']:>+9.0f}")

    print()
    print("✅ Phase 7 v2 complete — backtest with production gates applied")
    return 0


if __name__ == "__main__":
    sys.exit(main())
