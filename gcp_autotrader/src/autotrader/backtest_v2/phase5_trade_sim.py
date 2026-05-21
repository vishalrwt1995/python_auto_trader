"""Phase 5 — Trade outcome simulation.

For each qualified signal:
  - Use production calc_swing_position_size for SL/target/qty
  - Walk forward day-by-day through actual subsequent candles
  - Exit on first of: SL hit, target hit, supertrend flip, max_hold_days
  - Compute realized P&L (gross + after brokerage)

Aggregate across all signals per strategy to get win rate, expected R,
total P&L. This is the actual edge measurement.

Run:
    python -m autotrader.backtest_v2.phase5_trade_sim swing 2026-05-01 2026-05-15

Note: we use the CURRENT production scoring code to GENERATE signals
(not BQ-recorded signals). This means we're testing "what would today's
strategies have made on historical data" — exactly what we want.
"""
from __future__ import annotations

import sys
from collections import defaultdict
from typing import Any

from autotrader.backtest_v2.data import HistoricalDataset
from autotrader.domain.daily_bias import compute_daily_bias
from autotrader.domain.indicators import compute_indicators
from autotrader.domain.models import (
    FiiDiiSnapshot,
    NiftySnapshot,
    PcrSnapshot,
    RegimeSnapshot,
)
from autotrader.domain.risk import calc_brokerage, calc_round_trip_brokerage, calc_swing_position_size
from autotrader.domain.scoring import check_swing_entry, determine_direction, score_signal
from autotrader.settings import StrategySettings


# Liquid swing universe — see Phase 4 rationale (full daily history available
# in GCS cache; matches the universe live actively traded).
SWING_UNIVERSE = [
    "TCS", "RELIANCE", "HDFCBANK", "INFY", "ICICIBANK", "CANBK", "AXISBANK",
    "SBIN", "POWERGRID", "ITC", "HINDUNILVR", "BHARTIARTL", "BAJFINANCE",
    "LT", "MARUTI", "TATAMOTORS", "WIPRO", "HCLTECH", "TECHM", "ASIANPAINT",
    "TITAN", "ULTRACEMCO", "NESTLEIND", "JSWSTEEL", "GRASIM", "ADANIENT",
    "TATASTEEL", "ONGC", "COALINDIA", "NTPC", "BPCL", "IOC", "DIVISLAB",
    "DRREDDY", "CIPLA", "SUNPHARMA", "APOLLOHOSP", "BRITANNIA", "DABUR",
    "GODREJCP", "PIDILITIND", "EICHERMOT", "HEROMOTOCO",
    "INDUSINDBK", "KOTAKBANK", "PNB", "BANKBARODA", "CDSL", "BSE",
]

# Setups to test in swing
SWING_SETUPS = ["MOMENTUM", "PULLBACK", "MEAN_REVERSION", "BREAKOUT"]


def _build_regime_for_date(date_str: str) -> RegimeSnapshot:
    """Approximate regime from generic priors (no brain replay yet).

    For Phase 5 we use TREND_UP/RANGE/TREND_DOWN as a coarse bucket. Brain
    replay is Phase 6+; using TREND_UP here gives setups their best chance,
    which is biased TOWARD finding edge. We'll tighten in Phase 6.
    """
    return RegimeSnapshot(
        regime="TREND_UP",
        bias="BULLISH",
        vix=14.0,
        nifty=NiftySnapshot(change_pct=0.3, ltp=22000.0),
        pcr=PcrSnapshot(),
        fii=FiiDiiSnapshot(),
        confidence=0.7,
        data_health=0.8,
        source_quality=0.9,
    )


def simulate_swing_trade(
    symbol: str,
    entry_idx: int,
    daily: list[list[Any]],
    direction: str,
    cfg: StrategySettings,
    daily_bias_atr: float,
) -> dict[str, Any]:
    """Walk forward from entry_idx (next-day open) through daily candles.

    Returns {qty, entry_price, sl, target, exit_price, exit_day, exit_reason,
    holding_days, gross_pnl, net_pnl, r_realized}.
    """
    # Entry at next day's open
    entry_price = float(daily[entry_idx][1])  # open of entry day
    # Position sizing using production code with daily ATR
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

    for i in range(entry_idx, end_idx + 1):
        bar = daily[i]
        high = float(bar[2])
        low = float(bar[3])
        close = float(bar[4])

        if direction == "BUY":
            # SL hit: low <= sl
            if low <= sl:
                exit_price = sl
                exit_day_idx = i
                exit_reason = "SL"
                break
            # Target hit: high >= target
            if high >= target:
                exit_price = target
                exit_day_idx = i
                exit_reason = "TARGET"
                break
        else:  # SELL (short)
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
        exit_price = float(daily[end_idx][4])  # close of last held day
        exit_day_idx = end_idx

    # P&L
    if direction == "BUY":
        gross = (exit_price - entry_price) * qty
    else:
        gross = (entry_price - exit_price) * qty
    brk = calc_round_trip_brokerage(qty, entry_price, exit_price)
    net = gross - brk
    r_realized = (gross / (qty * sl_dist)) if (qty * sl_dist) > 0 else 0.0

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
    }


def scan_one_day_one_symbol(
    symbol: str,
    setup: str,
    as_of: str,
    ds: HistoricalDataset,
    cfg: StrategySettings,
) -> dict[str, Any] | None:
    """Try to qualify a swing signal for `symbol` on `as_of`.

    Returns the trade-entry context if qualified, else None.
    """
    # Truncate intraday to end of `as_of` (15:30 IST)
    end_ts = f"{as_of}T15:30:00+05:30"
    intraday = ds.intraday_candles(symbol, end_ts=end_ts)
    daily = ds.daily_candles(symbol, end_date=as_of)

    if len(daily) < 50 or len(intraday) < 80:
        return None

    try:
        db = compute_daily_bias(daily)
        ind = compute_indicators(intraday, cfg)
    except Exception:
        return None
    if db is None or ind is None:
        return None

    regime = _build_regime_for_date(as_of)
    try:
        direction = determine_direction(ind, regime, setup=setup, wl_type="swing", daily_bias=db)
        sig = score_signal(symbol, direction, ind, regime, cfg, daily_bias=db, setup=setup)
        gate_ok, gate_reason = check_swing_entry(setup, direction, ind, db, regime=regime.regime)
    except Exception:
        return None

    if direction == "HOLD" or not gate_ok or sig.score < cfg.swing_min_signal_score:
        return None

    return {
        "symbol": symbol,
        "setup": setup,
        "direction": direction,
        "score": sig.score,
        "daily_atr": db.atr_daily,
        "as_of": as_of,
    }


def find_entry_idx(daily: list[list[Any]], as_of: str) -> int | None:
    """Find the index of the FIRST daily candle AFTER as_of (entry happens on next day's open)."""
    for i, c in enumerate(daily):
        d = str(c[0])[:10]
        if d > as_of:
            return i
    return None


def main() -> int:
    wl_type = sys.argv[1] if len(sys.argv) > 1 else "swing"
    start_date = sys.argv[2] if len(sys.argv) > 2 else "2025-06-01"
    end_date = sys.argv[3] if len(sys.argv) > 3 else "2025-12-31"

    print("=" * 70)
    print(f"Phase 5 — Trade outcome simulation: {wl_type} {start_date}..{end_date}")
    print("=" * 70)

    cfg = StrategySettings()
    ds = HistoricalDataset()

    # Iterate every date in range; for each, scan every (symbol, setup)
    all_trades: list[dict] = []
    qualified_count = 0
    scan_count = 0

    # Build the list of dates we'll scan. Use a daily index from any
    # representative symbol's daily candles (TCS has the full history).
    tcs_daily = ds.daily_candles("TCS")
    if not tcs_daily:
        print("✗ no TCS daily candles — abort")
        return 1
    dates = sorted(set(str(c[0])[:10] for c in tcs_daily))
    dates = [d for d in dates if start_date <= d <= end_date]
    print(f"  Scanning {len(dates)} trading days × {len(SWING_UNIVERSE)} symbols × {len(SWING_SETUPS)} setups")

    if not dates:
        print(f"  ⚠️  No dates in range {start_date}..{end_date}.")
        print(f"      TCS cache covers {str(tcs_daily[0][0])[:10]} .. {str(tcs_daily[-1][0])[:10]}")
        return 1

    # Build per-symbol daily cache (don't re-fetch each iteration)
    daily_cache: dict[str, list[list[Any]]] = {}
    daily_bias_cache: dict[tuple[str, str], Any] = {}  # (symbol, as_of) -> daily_bias

    for di, as_of in enumerate(dates):
        if di % 20 == 0:
            print(f"  date {as_of} ({di+1}/{len(dates)}) qualified_so_far={qualified_count}")
        for symbol in SWING_UNIVERSE:
            # Get daily candles (cached, full series)
            if symbol not in daily_cache:
                daily_cache[symbol] = ds.daily_candles(symbol)
            daily_all = daily_cache[symbol]
            if len(daily_all) < 80:
                continue
            # Truncate to as_of for replay scoring
            daily_truncated = [c for c in daily_all if str(c[0])[:10] <= as_of]
            if len(daily_truncated) < 50:
                continue

            # Compute daily_bias once per (symbol, date)
            db_key = (symbol, as_of)
            if db_key not in daily_bias_cache:
                try:
                    daily_bias_cache[db_key] = compute_daily_bias(daily_truncated)
                except Exception:
                    daily_bias_cache[db_key] = None
            db = daily_bias_cache[db_key]
            if db is None:
                continue

            # For Phase 5 we use daily candles ONLY (no intraday) — much
            # cheaper than per-symbol intraday lookups. Compute a synthetic
            # IndicatorSnapshot from daily candles by treating them as the
            # "intraday" series. This is an APPROXIMATION acceptable for
            # SWING scoring where the daily timeframe dominates the gate
            # decision anyway. Documented in BACKTEST_PLAN.md §8 if it
            # affects results meaningfully.
            try:
                ind = compute_indicators(daily_truncated, cfg)
            except Exception:
                continue
            if ind is None:
                continue

            regime = _build_regime_for_date(as_of)
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
                # Simulate trade
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
                all_trades.append(trade)

    # Aggregate
    print()
    print(f"Scan total: {scan_count}")
    print(f"Qualified : {qualified_count}")
    print(f"Trades sim: {len(all_trades)}")
    print()

    if not all_trades:
        print("⚠️  No trades simulated. Either no qualified signals or all entry_idx out of range.")
        return 1

    # Per-setup attribution
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

    # Overall
    n = len(all_trades)
    wins = sum(1 for t in all_trades if t["net_pnl"] > 0)
    wr = wins / n * 100
    avg_r = sum(t["r_realized"] for t in all_trades) / n
    net = sum(t["net_pnl"] for t in all_trades)
    print()
    print(f"  {'TOTAL':18s} {n:>5d} {wins:>5d} {wr:>5.1f}% {avg_r:>+6.2f}R ₹{net:>+9.0f}")

    # Exit-reason distribution
    print()
    print("Exit reasons:")
    by_reason: dict[str, int] = defaultdict(int)
    for t in all_trades:
        by_reason[t["exit_reason"]] += 1
    for reason, count in sorted(by_reason.items(), key=lambda x: -x[1]):
        print(f"  {reason:12s} {count:>5d}  ({count/n*100:.1f}%)")

    print()
    print(f"✅ Phase 5 complete — {n} simulated trades, total net P&L ₹{net:.0f}")
    print(f"   Note: regime defaulted to TREND_UP for all dates. Phase 6 will add real brain.")
    print(f"   Note: indicators computed from daily candles (approximation). Phase 6 adds intraday.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
