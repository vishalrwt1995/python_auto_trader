"""Phase 8 — Optimized backtest with intelligent regime + setup filters.

Builds on Phase 7 (watchlist-driven) with toggleable optimization filters
evaluated SIMULTANEOUSLY in a single pass — clean attribution in one run.

## Filters

  F1 (chop_block):   Skip trend setups (BREAKOUT/MOMENTUM/PULLBACK) when
                     NIFTY regime = CHOP. Single biggest leak in Phase 7
                     (-₹77k across 5,151 CHOP trades).

  F2 (stock_uptrend): Only take trend setups if the STOCK itself is in an
                     uptrend (ADX(14) ≥ 20 AND EMA9 > EMA21 > EMA50). A
                     stock can trend while NIFTY chops.

  F3a (stage2):      PULLBACK only fires for stage-2 stocks (above 200-SMA,
                     within 25% of 52w high, up 20%+ from 52w low). Filters
                     out "falling knife" pullbacks — Minervini playbook.

  F3b (vol_contract): PULLBACK requires volume contraction (last 3d avg
                     vol < prior 5d avg vol). Real pullbacks come on dropping
                     volume; rising volume = distribution.

  F3c (weekly):       PULLBACK requires weekly EMA9 > EMA21 (higher-TF gate).

  F4 (score_floor):  Score floor escalates with regime: CHOP ≥ 80, RANGE ≥ 70,
                     TREND_UP uses default (55). Forces A+ setups only in
                     hostile regimes.

  F5 (setup_matrix): Strict per-regime setup allowlist:
                     TREND_UP → MOMENTUM, BREAKOUT, PULLBACK
                     RANGE    → MEAN_REVERSION, BREAKOUT
                     CHOP     → MEAN_REVERSION only

## Configs tested (all in one pass)

  baseline          : no filters (== Phase 7 sanity check)
  chop_block        : F1 only
  pullback_strict   : F3a + F3b + F3c
  stock_regime      : F2 only
  score_escalation  : F4 only
  setup_matrix      : F5 only
  smart_stack       : F1 + F2 + F3a + F3b + F4 (the intelligent combo)
  max_filters       : ALL filters on

## Engineering

  ONE scan loop — for each qualified trade, evaluate ALL filter combos and
  tag the trade with which configs accept it. Trade simulation runs ONCE
  (it's filter-independent — same entry → same exit). At end, slice
  trades-by-config to report per-config aggregates.

  Total scan cost ≈ Phase 7 (~60-90 min). Output: 8× the analytical signal.

Run:
    python -m autotrader.backtest_v2.phase8_optimized_backtest 2024-01-01 2025-12-31
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
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
from autotrader.domain.indicators import calc_adx, calc_ema, compute_indicators
from autotrader.domain.scoring import check_swing_entry, determine_direction, score_signal
from autotrader.settings import StrategySettings


TREND_SETUPS = {"BREAKOUT", "MOMENTUM", "PULLBACK"}


# ---------- Filter helpers (point-in-time correct) ----------

def is_stock_in_uptrend(daily_candles: list[list]) -> bool:
    """F2: Stock-level uptrend = ADX(14) ≥ 20 AND EMA9 > EMA21 > EMA50."""
    if len(daily_candles) < 60:
        return False
    closes = [float(c[4]) for c in daily_candles]
    try:
        ema9 = calc_ema(closes, 9)
        ema21 = calc_ema(closes, 21)
        ema50 = calc_ema(closes, 50)
        adx = calc_adx(daily_candles, period=14)
    except Exception:
        return False
    return ema9 > ema21 > ema50 and adx >= 20.0


def passes_stage2(daily_candles: list[list]) -> bool:
    """F3a: Stage-2 = above 200-SMA, within 25% of 52w high, up 20%+ from 52w low."""
    if len(daily_candles) < 252:
        return False
    closes = [float(c[4]) for c in daily_candles]
    last = closes[-1]
    sma200 = sum(closes[-200:]) / 200
    window = daily_candles[-252:]
    high52 = max(float(c[2]) for c in window)
    low52 = min(float(c[3]) for c in window)
    if low52 <= 0:
        return False
    above_sma = last > sma200
    near_high = last >= high52 * 0.75
    above_low = last >= low52 * 1.20
    return above_sma and near_high and above_low


def passes_volume_contraction(daily_candles: list[list]) -> bool:
    """F3b: Last 3 days avg volume < prior 5 days avg volume."""
    if len(daily_candles) < 8:
        return False
    vols = [float(c[5]) for c in daily_candles[-8:]]
    last3 = sum(vols[-3:]) / 3.0
    prior5 = sum(vols[:5]) / 5.0
    return last3 < prior5


def weekly_bias_bullish(daily_candles: list[list]) -> bool:
    """F3c: Weekly EMA9 > EMA21 stack (resample daily → weekly closes)."""
    if len(daily_candles) < 105:  # need ~21 weeks
        return False
    week_groups: dict[str, list[tuple[str, float]]] = {}
    for c in daily_candles:
        try:
            day = str(c[0])[:10]
            dt = datetime.fromisoformat(day)
            yr, wk, _ = dt.isocalendar()
            key = f"{yr}-W{wk:02d}"
            week_groups.setdefault(key, []).append((day, float(c[4])))
        except Exception:
            continue
    # Last close in each week, ordered by week
    weekly_closes: list[float] = []
    for key in sorted(week_groups.keys()):
        days = sorted(week_groups[key], key=lambda x: x[0])
        weekly_closes.append(days[-1][1])
    if len(weekly_closes) < 21:
        return False
    try:
        ema9 = calc_ema(weekly_closes, 9)
        ema21 = calc_ema(weekly_closes, 21)
    except Exception:
        return False
    return ema9 > ema21


def score_floor_for_regime(regime: str, default: int) -> int:
    """F4: regime-escalated score floor."""
    if regime == "CHOP":
        return max(default, 80)
    if regime == "RANGE":
        return max(default, 70)
    if regime == "PANIC":
        return max(default, 85)
    return default


# Allowed setups per regime (F5)
_SETUP_MATRIX = {
    "TREND_UP": {"MOMENTUM", "BREAKOUT", "PULLBACK"},
    "TREND_DOWN": {"BREAKOUT", "MOMENTUM"},  # short setups (we score in direction-agnostic way)
    "RANGE": {"MEAN_REVERSION", "BREAKOUT"},
    "CHOP": {"MEAN_REVERSION"},
    "PANIC": set(),    # don't trade
    "RECOVERY": {"MOMENTUM", "BREAKOUT"},
}


def setup_allowed_in_regime(setup: str, regime: str) -> bool:
    """F5: enforce strict per-regime allowlist."""
    allowed = _SETUP_MATRIX.get(regime, {"MOMENTUM", "BREAKOUT", "PULLBACK", "MEAN_REVERSION"})
    return setup in allowed


# ---------- Per-trade config evaluation ----------

def evaluate_configs(
    setup: str,
    regime: str,
    score: int,
    default_score_floor: int,
    daily_truncated: list[list],
) -> dict[str, bool]:
    """Return dict[config_name → bool] = does this trade pass each config's filters?"""
    is_trend_setup = setup in TREND_SETUPS
    is_pullback = setup == "PULLBACK"

    # Pre-compute filter results (each filter computed at most once per trade)
    f1_chop_block = not (regime == "CHOP" and is_trend_setup)
    f2_stock_uptrend = (not is_trend_setup) or is_stock_in_uptrend(daily_truncated)
    f3a_stage2 = (not is_pullback) or passes_stage2(daily_truncated)
    f3b_vol = (not is_pullback) or passes_volume_contraction(daily_truncated)
    f3c_weekly = (not is_pullback) or weekly_bias_bullish(daily_truncated)
    f4_score = score >= score_floor_for_regime(regime, default_score_floor)
    f5_matrix = setup_allowed_in_regime(setup, regime)

    return {
        "baseline":         True,
        "chop_block":       f1_chop_block,
        "stock_regime":     f2_stock_uptrend,
        "pullback_strict":  f3a_stage2 and f3b_vol and f3c_weekly,
        "score_escalation": f4_score,
        "setup_matrix":     f5_matrix,
        "smart_stack":      f1_chop_block and f2_stock_uptrend and (f3a_stage2 and f3b_vol) and f4_score,
        "max_filters":      f1_chop_block and f2_stock_uptrend and f3a_stage2 and f3b_vol and f3c_weekly and f4_score and f5_matrix,
    }


CONFIG_ORDER = [
    "baseline",
    "chop_block",
    "stock_regime",
    "pullback_strict",
    "score_escalation",
    "setup_matrix",
    "smart_stack",
    "max_filters",
]


# ---------- Reporting ----------

def _report_block(name: str, trades: list[dict], baseline_net: float | None = None) -> None:
    n = len(trades)
    if n == 0:
        print(f"\n=== {name} ===  NO TRADES")
        return
    wins = sum(1 for t in trades if t["net_pnl"] > 0)
    wr = wins / n * 100
    avg_r = sum(t["r_realized"] for t in trades) / n
    net = sum(t["net_pnl"] for t in trades)
    avg_pnl = net / n
    delta = ""
    if baseline_net is not None:
        delta = f"  (Δ vs baseline: ₹{net - baseline_net:+,.0f})"
    print(f"\n=== {name} ===  N={n:,}  WR={wr:.1f}%  AvgR={avg_r:+.3f}R  Net=₹{net:+,.0f}  AvgPnL=₹{avg_pnl:+.1f}{delta}")

    # Per-setup
    by_setup: dict[str, list[dict]] = defaultdict(list)
    for t in trades:
        by_setup[t["setup"]].append(t)
    print(f"  {'Setup':18s} {'N':>5s} {'WR':>6s} {'AvgR':>7s} {'NetPnL':>10s}")
    for setup, st in sorted(by_setup.items(), key=lambda x: -sum(t["net_pnl"] for t in x[1])):
        sn = len(st)
        sw = sum(1 for t in st if t["net_pnl"] > 0)
        swr = sw / sn * 100 if sn else 0
        sar = sum(t["r_realized"] for t in st) / sn if sn else 0
        sn_net = sum(t["net_pnl"] for t in st)
        print(f"  {setup:18s} {sn:>5d} {swr:>5.1f}% {sar:>+6.2f}R ₹{sn_net:>+9.0f}")

    # Per-regime
    by_regime: dict[str, list[dict]] = defaultdict(list)
    for t in trades:
        by_regime[t["entry_regime"]].append(t)
    print(f"  {'Regime':14s} {'N':>5s} {'WR':>6s} {'AvgR':>7s} {'NetPnL':>10s}")
    for reg, rt in sorted(by_regime.items(), key=lambda x: -sum(t["net_pnl"] for t in x[1])):
        rn = len(rt)
        rw = sum(1 for t in rt if t["net_pnl"] > 0)
        rwr = rw / rn * 100 if rn else 0
        rar = sum(t["r_realized"] for t in rt) / rn if rn else 0
        rn_net = sum(t["net_pnl"] for t in rt)
        print(f"  {reg:14s} {rn:>5d} {rwr:>5.1f}% {rar:>+6.2f}R ₹{rn_net:>+9.0f}")


def _summary_table(all_trades: list[dict]) -> None:
    """Side-by-side comparison of all configs."""
    print("\n" + "=" * 90)
    print("SUMMARY — all configs side by side")
    print("=" * 90)
    print(f"  {'Config':22s} {'N':>6s} {'WR':>7s} {'AvgR':>8s} {'Net':>12s} {'AvgPnL':>10s}")
    base_net = None
    for cfg_name in CONFIG_ORDER:
        trades = [t for t in all_trades if cfg_name in t["config_tags"]]
        n = len(trades)
        if n == 0:
            print(f"  {cfg_name:22s} {n:>6d}   --       --             --         --")
            continue
        wins = sum(1 for t in trades if t["net_pnl"] > 0)
        wr = wins / n * 100
        avg_r = sum(t["r_realized"] for t in trades) / n
        net = sum(t["net_pnl"] for t in trades)
        avg_pnl = net / n
        if cfg_name == "baseline":
            base_net = net
        marker = ""
        if base_net is not None and cfg_name != "baseline":
            d = net - base_net
            marker = f"  ({d:+,.0f})"
        print(f"  {cfg_name:22s} {n:>6d} {wr:>6.1f}% {avg_r:>+7.3f}R ₹{net:>+10,.0f} ₹{avg_pnl:>+8.1f}{marker}")


def main() -> int:
    start_date = sys.argv[1] if len(sys.argv) > 1 else "2024-01-01"
    end_date = sys.argv[2] if len(sys.argv) > 2 else "2025-12-31"
    target_size = int(sys.argv[3]) if len(sys.argv) > 3 else 300

    print("=" * 70)
    print(f"Phase 8 — Optimized backtest: {start_date}..{end_date}")
    print(f"  Target watchlist size: {target_size}")
    print(f"  Configs evaluated in one pass: {len(CONFIG_ORDER)}")
    print("=" * 70)

    cfg = StrategySettings()
    default_score_floor = int(cfg.swing_min_signal_score)
    print(f"  Default score floor: {default_score_floor}")

    ds = HistoricalDataset()
    brain = BrainReplay(ds)

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

    current_month: str | None = None
    swing_watchlist: list[str] = []

    for di, as_of in enumerate(dates):
        month = as_of[:7]
        if month != current_month:
            try:
                swing_watchlist = universe.watchlist_swing_for_date(as_of, target_size=target_size)
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
                if direction == "HOLD" or not gate_ok or sig.score < default_score_floor:
                    continue

                # Trade qualifies under baseline (Phase 7 settings). Now
                # check which other configs ALSO accept it.
                qualified_count += 1
                config_pass = evaluate_configs(
                    setup=setup,
                    regime=regime_str,
                    score=int(sig.score),
                    default_score_floor=default_score_floor,
                    daily_truncated=daily_truncated,
                )
                # Only simulate if AT LEAST baseline accepts it (always true here)
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
                trade["score"] = int(sig.score)
                trade["entry_regime"] = regime_str
                trade["config_tags"] = [name for name, ok in config_pass.items() if ok]
                all_trades.append(trade)

    # ===== Reports =====
    print()
    print(f"Scan total      : {scan_count:,}")
    print(f"Qualified (base): {qualified_count:,}")
    print(f"Trades simulated: {len(all_trades):,}")
    print()
    print("Regime distribution (trading days):")
    total_days = sum(regime_counts.values())
    for r in sorted(regime_counts.keys()):
        n = regime_counts[r]
        print(f"  {r:14s} {n:>5d}  ({n/total_days*100:.1f}%)")

    if not all_trades:
        print("\n⚠️  No trades simulated")
        return 1

    # Headline summary table
    _summary_table(all_trades)

    # Detailed per-config breakdowns
    baseline_net = sum(t["net_pnl"] for t in all_trades if "baseline" in t["config_tags"])
    for cfg_name in CONFIG_ORDER:
        trades = [t for t in all_trades if cfg_name in t["config_tags"]]
        _report_block(cfg_name, trades, baseline_net=baseline_net)

    # Dump trades for post-hoc analysis
    out_dir = Path.home() / ".autotrader_backtest_cache"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"phase8_trades_{start_date}_{end_date}.json"
    try:
        with open(out_path, "w") as fh:
            json.dump(all_trades, fh, default=str)
        print(f"\n📦 Trades dumped to {out_path}")
    except Exception as exc:
        print(f"\n⚠️  Trade dump failed: {exc}")

    print("\n✅ Phase 8 complete — multi-config attribution in one pass")
    return 0


if __name__ == "__main__":
    sys.exit(main())
