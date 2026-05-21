"""Phase 9 — TRUE production replica + targeted RANGE-BREAKOUT unblock tests.

Critical correction from Phase 7/8:
  - Phase 7/8 baseline did NOT call `regime_hard_blocks_strategy` (production gate)
  - Phase 7/8 baseline did NOT apply the affinity multiplier to the score gate
  - Result: baseline was BIASED — overcount CHOP trades, undercount TREND_UP trades

Phase 9 fixes both gaps. Then tests targeted hypotheses on top of the true
production replica.

## Configs

  prod_replica:
    Apply EXACT production logic — `regime_hard_blocks_strategy()` AND
    `regime_strategy_multiplier()` on score gate. This IS what live actually
    does and is the new TRUE baseline.

  unblock_range_breakout:
    prod_replica + remove RANGE hard-block on BREAKOUT. Today's hypothesis:
    JNKINDIA/E2E type moves would have been caught.

  selective_unblock:
    prod_replica + UNBLOCK BREAKOUT in RANGE only when
        adx >= 30 AND supertrend == "UP" AND ema_state == "BULL_STACK"
    The smart version — selective rather than universal.

  unblock_range_breakout_high_vol:
    prod_replica + UNBLOCK BREAKOUT in RANGE only when vol_ratio >= 2.0.
    Tests volume-confirmation as the discriminator.

  prod_replica_plus_floor80:
    prod_replica + raise score floor to 80 in RANGE. Tests if quality
    threshold alone catches the alpha.

Run:
    python -m autotrader.backtest_v2.phase9_prod_replica 2024-01-01 2025-12-31 300
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
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
from autotrader.domain.indicators import compute_indicators
from autotrader.domain.regime_affinity import (
    regime_hard_blocks_strategy,
    regime_strategy_multiplier,
)
from autotrader.domain.scoring import check_swing_entry, determine_direction, score_signal
from autotrader.settings import StrategySettings


# ---------- Helper: compute production-adjusted score ----------

def production_adjusted_score(raw_score: int, regime: str, setup: str, direction: str) -> int:
    """Replicate production's adjusted_score computation.

    See trading_service.py:980 — the actual formula is roughly:
        adjusted = clip(raw * affinity_mult, 0, 100)
    """
    mult = regime_strategy_multiplier(regime, setup, direction)
    adjusted = max(0, min(100, int(round(raw_score * mult))))
    return adjusted


# ---------- Stock-level signal classification (used by selective_unblock) ----------

def stock_has_strong_breakout_signal(
    daily_truncated: list[list[Any]],
    indicators: Any,
) -> bool:
    """Selective unblock criterion — only override RANGE hard-block when
    the stock itself shows a strong trend.

    Heuristic (parallels what we saw today on JNKINDIA, E2E, BALAMINES):
      - ADX(14) >= 30 (strong directional move)
      - Supertrend = UP
      - EMA9 > EMA21 > EMA50 (bull stack)
    """
    if len(daily_truncated) < 60:
        return False
    closes = [float(c[4]) for c in daily_truncated]
    try:
        from autotrader.domain.indicators import calc_adx, calc_ema
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
    # Check supertrend via indicators object if available
    try:
        if hasattr(indicators, "supertrend"):
            st = str(indicators.supertrend or "").upper()
            if st and st != "UP":
                return False
    except Exception:
        pass
    return True


def stock_has_high_volume(daily_truncated: list[list[Any]]) -> bool:
    """High-volume confirmation — last bar volume >= 2x avg of prior 20."""
    if len(daily_truncated) < 21:
        return False
    last_vol = float(daily_truncated[-1][5])
    prior_avg = sum(float(c[5]) for c in daily_truncated[-21:-1]) / 20.0
    if prior_avg <= 0:
        return False
    return (last_vol / prior_avg) >= 2.0


# ---------- Per-trade config evaluation ----------

CONFIG_ORDER = [
    "prod_replica",
    "unblock_range_breakout",
    "selective_unblock",
    "unblock_range_breakout_high_vol",
    "prod_replica_plus_floor80",
]


def evaluate_configs(
    setup: str,
    regime: str,
    direction: str,
    raw_score: int,
    adjusted_score: int,
    daily_truncated: list[list[Any]],
    indicators: Any,
    default_score_floor: int,
) -> dict[str, bool]:
    """For each config, return True if this trade would qualify."""
    is_range = (regime == "RANGE")
    is_breakout = (setup == "BREAKOUT")

    # prod_replica: production hard_block + affinity-adjusted score gate
    prod_block = regime_hard_blocks_strategy(regime, setup)
    prod_score_ok = adjusted_score >= default_score_floor
    prod_passes = (not prod_block) and prod_score_ok

    # unblock_range_breakout: same as prod, except RANGE BREAKOUT is NOT hard-blocked
    if is_range and is_breakout:
        unblock_passes = prod_score_ok  # no hard_block, but score still must pass
    else:
        unblock_passes = prod_passes

    # selective_unblock: unblock RANGE BREAKOUT only if stock signal is strong
    if is_range and is_breakout:
        strong = stock_has_strong_breakout_signal(daily_truncated, indicators)
        selective_passes = strong and prod_score_ok
    else:
        selective_passes = prod_passes

    # unblock_range_breakout_high_vol: unblock RANGE BREAKOUT only with volume spike
    if is_range and is_breakout:
        high_vol = stock_has_high_volume(daily_truncated)
        high_vol_passes = high_vol and prod_score_ok
    else:
        high_vol_passes = prod_passes

    # prod_replica_plus_floor80: prod + raise score floor to 80 in RANGE
    if is_range:
        floor80_passes = (not prod_block) and adjusted_score >= 80
    else:
        floor80_passes = prod_passes

    return {
        "prod_replica":                    prod_passes,
        "unblock_range_breakout":          unblock_passes,
        "selective_unblock":               selective_passes,
        "unblock_range_breakout_high_vol": high_vol_passes,
        "prod_replica_plus_floor80":       floor80_passes,
    }


# ---------- Reporting ----------

def _summary_table(all_trades: list[dict]) -> None:
    print("\n" + "=" * 92)
    print("SUMMARY — all configs side by side")
    print("=" * 92)
    print(f"  {'Config':32s} {'N':>6s} {'WR':>7s} {'AvgR':>8s} {'Net':>12s} {'AvgPnL':>10s}")
    base_net = None
    for cfg_name in CONFIG_ORDER:
        trades = [t for t in all_trades if cfg_name in t["config_tags"]]
        n = len(trades)
        if n == 0:
            print(f"  {cfg_name:32s} {n:>6d}   --       --             --         --")
            continue
        wins = sum(1 for t in trades if t["net_pnl"] > 0)
        wr = wins / n * 100
        avg_r = sum(t["r_realized"] for t in trades) / n
        net = sum(t["net_pnl"] for t in trades)
        avg_pnl = net / n
        if cfg_name == "prod_replica":
            base_net = net
        marker = ""
        if base_net is not None and cfg_name != "prod_replica":
            d = net - base_net
            marker = f"  ({d:+,.0f})"
        print(f"  {cfg_name:32s} {n:>6d} {wr:>6.1f}% {avg_r:>+7.3f}R ₹{net:>+10,.0f} ₹{avg_pnl:>+8.1f}{marker}")


def _detail_block(name: str, trades: list[dict], baseline_net: float | None = None) -> None:
    n = len(trades)
    if n == 0:
        print(f"\n=== {name} ===  NO TRADES")
        return
    wins = sum(1 for t in trades if t["net_pnl"] > 0)
    wr = wins / n * 100
    avg_r = sum(t["r_realized"] for t in trades) / n
    net = sum(t["net_pnl"] for t in trades)
    delta = ""
    if baseline_net is not None:
        delta = f"  (Δ vs prod_replica: ₹{net - baseline_net:+,.0f})"
    print(f"\n=== {name} ===  N={n:,}  WR={wr:.1f}%  AvgR={avg_r:+.3f}R  Net=₹{net:+,.0f}{delta}")

    # Per setup
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

    # Per regime
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


def main() -> int:
    start_date = sys.argv[1] if len(sys.argv) > 1 else "2024-01-01"
    end_date = sys.argv[2] if len(sys.argv) > 2 else "2025-12-31"
    target_size = int(sys.argv[3]) if len(sys.argv) > 3 else 300

    print("=" * 70)
    print(f"Phase 9 — Production-replica backtest: {start_date}..{end_date}")
    print(f"  Target watchlist: {target_size}")
    print(f"  Configs: {len(CONFIG_ORDER)}")
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
    qualified_any_count = 0  # at least one config accepts
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
            print(f"  date {as_of} ({di+1}/{len(dates)}) regime={regime_str} qualified_any={qualified_any_count} trades={len(all_trades)}")

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
                if direction == "HOLD" or not gate_ok:
                    continue

                # Compute production-adjusted score
                raw = int(sig.score)
                adj = production_adjusted_score(raw, regime_str, setup, direction)

                # Evaluate ALL configs
                config_pass = evaluate_configs(
                    setup=setup,
                    regime=regime_str,
                    direction=direction,
                    raw_score=raw,
                    adjusted_score=adj,
                    daily_truncated=daily_truncated,
                    indicators=ind,
                    default_score_floor=default_score_floor,
                )

                # If no config accepts this trade, skip (don't waste simulation)
                if not any(config_pass.values()):
                    continue

                qualified_any_count += 1
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
                trade["raw_score"] = raw
                trade["adjusted_score"] = adj
                trade["entry_regime"] = regime_str
                trade["config_tags"] = [name for name, ok in config_pass.items() if ok]
                all_trades.append(trade)

    # ===== Reports =====
    print()
    print(f"Scan total          : {scan_count:,}")
    print(f"Qualified (any cfg) : {qualified_any_count:,}")
    print(f"Trades simulated    : {len(all_trades):,}")
    print()
    print("Regime distribution (trading days):")
    total_days = sum(regime_counts.values())
    for r in sorted(regime_counts.keys()):
        n = regime_counts[r]
        print(f"  {r:14s} {n:>5d}  ({n/total_days*100:.1f}%)")

    if not all_trades:
        print("\n⚠️  No trades simulated")
        return 1

    _summary_table(all_trades)

    baseline_net = sum(t["net_pnl"] for t in all_trades if "prod_replica" in t["config_tags"])
    for cfg_name in CONFIG_ORDER:
        trades = [t for t in all_trades if cfg_name in t["config_tags"]]
        _detail_block(cfg_name, trades, baseline_net=baseline_net)

    # Dump
    out_dir = Path.home() / ".autotrader_backtest_cache"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"phase9_trades_{start_date}_{end_date}.json"
    try:
        with open(out_path, "w") as fh:
            json.dump(all_trades, fh, default=str)
        print(f"\n📦 Trades dumped to {out_path}")
    except Exception as exc:
        print(f"\n⚠️  Trade dump failed: {exc}")

    print("\n✅ Phase 9 complete — true prod replica + targeted unblock tests")
    return 0


if __name__ == "__main__":
    sys.exit(main())
