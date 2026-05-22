"""Exit-rule variants for swing trades, simulated bar-by-bar.

Each variant returns the same trade-dict shape as simulate_swing_trade_with_mfe.

Variants:
  V0_baseline:     SL + TARGET + MAX_HOLD (current production)
  V1_half_r_full:  Exit ALL at 0.5R if reached (single take-profit)
  V2_half_r_50:    Exit 50% at 0.5R, hold remaining 50% to TARGET/SL/MAX_HOLD
  V3_trail_be:     After 0.5R MFE, move SL to entry (breakeven trail)
  V4_tiered:       50% at 0.5R, 25% at 1.0R, 25% to MAX_HOLD/TARGET
  V5_half_r_atr:   Same as V1 but trigger at 0.5 × ATR_daily (not 0.5 × sl_dist)

Within-bar ordering:
  For BUY: if SL is in range AND half_r is in range, assume SL hit first (conservative).
  For SELL: same — SL hit first.
  This matches typical worst-case backtest practice.
"""
from __future__ import annotations

from typing import Any

from autotrader.domain.risk import calc_round_trip_brokerage, calc_swing_position_size
from autotrader.settings import StrategySettings


def _v0_baseline(symbol, entry_idx, daily, direction, cfg, atr) -> dict[str, Any]:
    """Current production behavior — SL/TARGET/MAX_HOLD only."""
    entry_price = float(daily[entry_idx][1])
    pos = calc_swing_position_size(entry_price, atr, direction, cfg)
    if pos.qty <= 0:
        return {"status": "SKIP_ZERO_QTY"}

    sl, target, qty, sl_dist = pos.sl_price, pos.target, pos.qty, pos.sl_dist
    max_hold = cfg.swing_max_hold_days
    end_idx = min(entry_idx + max_hold, len(daily) - 1)

    exit_price = entry_price
    exit_day_idx = entry_idx
    exit_reason = "MAX_HOLD"
    mfe_price = entry_price
    mae_price = entry_price

    for i in range(entry_idx, end_idx + 1):
        bar = daily[i]
        high, low = float(bar[2]), float(bar[3])

        if direction == "BUY":
            mfe_price = max(mfe_price, high)
            mae_price = min(mae_price, low)
            if low <= sl:
                exit_price, exit_day_idx, exit_reason = sl, i, "SL"
                break
            if high >= target:
                exit_price, exit_day_idx, exit_reason = target, i, "TARGET"
                break
        else:
            mfe_price = min(mfe_price, low)
            mae_price = max(mae_price, high)
            if high >= sl:
                exit_price, exit_day_idx, exit_reason = sl, i, "SL"
                break
            if low <= target:
                exit_price, exit_day_idx, exit_reason = target, i, "TARGET"
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
    r_realized = gross / (qty * sl_dist) if (qty * sl_dist) > 0 else 0.0
    r_mfe = mfe_pnl / (qty * sl_dist) if (qty * sl_dist) > 0 else 0.0
    r_mae = mae_pnl / (qty * sl_dist) if (qty * sl_dist) > 0 else 0.0

    return _trade_dict(symbol, direction, qty, entry_price, sl, target, exit_price,
                       exit_day_idx - entry_idx, exit_reason, gross, brk, net,
                       r_realized, r_mfe, r_mae, mfe_price, mae_price)


def _v1_half_r_full(symbol, entry_idx, daily, direction, cfg, atr) -> dict[str, Any]:
    """Exit ALL at 0.5R if half-R level reached. Otherwise standard SL/TARGET/MAX_HOLD."""
    entry_price = float(daily[entry_idx][1])
    pos = calc_swing_position_size(entry_price, atr, direction, cfg)
    if pos.qty <= 0:
        return {"status": "SKIP_ZERO_QTY"}

    sl, target, qty, sl_dist = pos.sl_price, pos.target, pos.qty, pos.sl_dist
    half_r_level = entry_price + 0.5 * sl_dist if direction == "BUY" else entry_price - 0.5 * sl_dist

    max_hold = cfg.swing_max_hold_days
    end_idx = min(entry_idx + max_hold, len(daily) - 1)

    exit_price = entry_price
    exit_day_idx = entry_idx
    exit_reason = "MAX_HOLD"
    mfe_price = entry_price
    mae_price = entry_price

    for i in range(entry_idx, end_idx + 1):
        bar = daily[i]
        high, low = float(bar[2]), float(bar[3])

        if direction == "BUY":
            mfe_price = max(mfe_price, high)
            mae_price = min(mae_price, low)
            # Conservative: SL check first
            if low <= sl:
                exit_price, exit_day_idx, exit_reason = sl, i, "SL"
                break
            # Half-R check (between SL and TARGET)
            if high >= half_r_level:
                exit_price, exit_day_idx, exit_reason = half_r_level, i, "HALF_R"
                break
            if high >= target:
                exit_price, exit_day_idx, exit_reason = target, i, "TARGET"
                break
        else:
            mfe_price = min(mfe_price, low)
            mae_price = max(mae_price, high)
            if high >= sl:
                exit_price, exit_day_idx, exit_reason = sl, i, "SL"
                break
            if low <= half_r_level:
                exit_price, exit_day_idx, exit_reason = half_r_level, i, "HALF_R"
                break
            if low <= target:
                exit_price, exit_day_idx, exit_reason = target, i, "TARGET"
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
    r_realized = gross / (qty * sl_dist) if (qty * sl_dist) > 0 else 0.0
    r_mfe = mfe_pnl / (qty * sl_dist) if (qty * sl_dist) > 0 else 0.0
    r_mae = mae_pnl / (qty * sl_dist) if (qty * sl_dist) > 0 else 0.0

    return _trade_dict(symbol, direction, qty, entry_price, sl, target, exit_price,
                       exit_day_idx - entry_idx, exit_reason, gross, brk, net,
                       r_realized, r_mfe, r_mae, mfe_price, mae_price)


def _v2_half_r_50pct(symbol, entry_idx, daily, direction, cfg, atr) -> dict[str, Any]:
    """Scale out 50% at 0.5R, hold remaining 50% to SL/TARGET/MAX_HOLD."""
    entry_price = float(daily[entry_idx][1])
    pos = calc_swing_position_size(entry_price, atr, direction, cfg)
    if pos.qty <= 0:
        return {"status": "SKIP_ZERO_QTY"}

    sl, target, qty, sl_dist = pos.sl_price, pos.target, pos.qty, pos.sl_dist
    half_r_level = entry_price + 0.5 * sl_dist if direction == "BUY" else entry_price - 0.5 * sl_dist

    qty_half = max(1, qty // 2)
    qty_rest = qty - qty_half

    max_hold = cfg.swing_max_hold_days
    end_idx = min(entry_idx + max_hold, len(daily) - 1)

    half_filled = False
    half_fill_price = 0.0
    half_fill_day = entry_idx
    rest_exit_price = entry_price
    rest_exit_day = entry_idx
    rest_exit_reason = "MAX_HOLD"
    mfe_price = entry_price
    mae_price = entry_price

    for i in range(entry_idx, end_idx + 1):
        bar = daily[i]
        high, low = float(bar[2]), float(bar[3])

        if direction == "BUY":
            mfe_price = max(mfe_price, high)
            mae_price = min(mae_price, low)
            # SL check on remaining position (or full if not filled half yet)
            if low <= sl:
                rest_exit_price, rest_exit_day, rest_exit_reason = sl, i, "SL"
                break
            # Half-R fills the first half
            if (not half_filled) and high >= half_r_level:
                half_filled = True
                half_fill_price = half_r_level
                half_fill_day = i
                # Continue holding the rest
            if high >= target:
                rest_exit_price, rest_exit_day, rest_exit_reason = target, i, "TARGET"
                break
        else:
            mfe_price = min(mfe_price, low)
            mae_price = max(mae_price, high)
            if high >= sl:
                rest_exit_price, rest_exit_day, rest_exit_reason = sl, i, "SL"
                break
            if (not half_filled) and low <= half_r_level:
                half_filled = True
                half_fill_price = half_r_level
                half_fill_day = i
            if low <= target:
                rest_exit_price, rest_exit_day, rest_exit_reason = target, i, "TARGET"
                break

    if rest_exit_reason == "MAX_HOLD":
        rest_exit_price = float(daily[end_idx][4])
        rest_exit_day = end_idx

    # P&L from both legs
    if direction == "BUY":
        gross_half = (half_fill_price - entry_price) * qty_half if half_filled else 0.0
        gross_rest = (rest_exit_price - entry_price) * qty_rest
        mfe_pnl = (mfe_price - entry_price) * qty
        mae_pnl = (mae_price - entry_price) * qty
    else:
        gross_half = (entry_price - half_fill_price) * qty_half if half_filled else 0.0
        gross_rest = (entry_price - rest_exit_price) * qty_rest
        mfe_pnl = (entry_price - mfe_price) * qty
        mae_pnl = (entry_price - mae_price) * qty
    gross = gross_half + gross_rest
    # Brokerage on both legs (approximate as one big brokerage call)
    brk_half = calc_round_trip_brokerage(qty_half, entry_price, half_fill_price) if half_filled else 0.0
    brk_rest = calc_round_trip_brokerage(qty_rest, entry_price, rest_exit_price)
    brk = brk_half + brk_rest
    net = gross - brk
    r_realized = gross / (qty * sl_dist) if (qty * sl_dist) > 0 else 0.0
    r_mfe = mfe_pnl / (qty * sl_dist) if (qty * sl_dist) > 0 else 0.0
    r_mae = mae_pnl / (qty * sl_dist) if (qty * sl_dist) > 0 else 0.0

    exit_reason = f"HALF_R+{rest_exit_reason}" if half_filled else rest_exit_reason
    exit_day_idx = rest_exit_day  # report the last exit day
    exit_price_summary = (gross_half + gross_rest) / qty + entry_price if direction == "BUY" else entry_price - (gross_half + gross_rest) / qty

    return _trade_dict(symbol, direction, qty, entry_price, sl, target, exit_price_summary,
                       exit_day_idx - entry_idx, exit_reason, gross, brk, net,
                       r_realized, r_mfe, r_mae, mfe_price, mae_price,
                       half_filled=half_filled, half_fill_price=half_fill_price)


def _v3_trail_be(symbol, entry_idx, daily, direction, cfg, atr) -> dict[str, Any]:
    """After 0.5R MFE, move SL to entry (breakeven). Don't take half off."""
    entry_price = float(daily[entry_idx][1])
    pos = calc_swing_position_size(entry_price, atr, direction, cfg)
    if pos.qty <= 0:
        return {"status": "SKIP_ZERO_QTY"}

    sl, target, qty, sl_dist = pos.sl_price, pos.target, pos.qty, pos.sl_dist
    half_r_level = entry_price + 0.5 * sl_dist if direction == "BUY" else entry_price - 0.5 * sl_dist
    breakeven_armed = False

    max_hold = cfg.swing_max_hold_days
    end_idx = min(entry_idx + max_hold, len(daily) - 1)

    exit_price = entry_price
    exit_day_idx = entry_idx
    exit_reason = "MAX_HOLD"
    mfe_price = entry_price
    mae_price = entry_price
    effective_sl = sl

    for i in range(entry_idx, end_idx + 1):
        bar = daily[i]
        high, low = float(bar[2]), float(bar[3])

        if direction == "BUY":
            mfe_price = max(mfe_price, high)
            mae_price = min(mae_price, low)
            # Check if breakeven trigger hit in this bar BEFORE checking exits
            if (not breakeven_armed) and high >= half_r_level:
                breakeven_armed = True
                effective_sl = entry_price  # SL now at entry
            if low <= effective_sl:
                exit_price = effective_sl
                exit_day_idx, exit_reason = i, ("BE_STOP" if breakeven_armed else "SL")
                break
            if high >= target:
                exit_price, exit_day_idx, exit_reason = target, i, "TARGET"
                break
        else:
            mfe_price = min(mfe_price, low)
            mae_price = max(mae_price, high)
            if (not breakeven_armed) and low <= half_r_level:
                breakeven_armed = True
                effective_sl = entry_price
            if high >= effective_sl:
                exit_price = effective_sl
                exit_day_idx, exit_reason = i, ("BE_STOP" if breakeven_armed else "SL")
                break
            if low <= target:
                exit_price, exit_day_idx, exit_reason = target, i, "TARGET"
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
    r_realized = gross / (qty * sl_dist) if (qty * sl_dist) > 0 else 0.0
    r_mfe = mfe_pnl / (qty * sl_dist) if (qty * sl_dist) > 0 else 0.0
    r_mae = mae_pnl / (qty * sl_dist) if (qty * sl_dist) > 0 else 0.0

    return _trade_dict(symbol, direction, qty, entry_price, sl, target, exit_price,
                       exit_day_idx - entry_idx, exit_reason, gross, brk, net,
                       r_realized, r_mfe, r_mae, mfe_price, mae_price)


def _trade_dict(symbol, direction, qty, entry_price, sl, target, exit_price,
                holding_days, exit_reason, gross, brk, net,
                r_realized, r_mfe, r_mae, mfe_price, mae_price, **extras):
    base = {
        "status": "OK",
        "symbol": symbol, "direction": direction, "qty": qty,
        "entry_price": round(entry_price, 2),
        "sl": round(sl, 2), "target": round(target, 2),
        "exit_price": round(exit_price, 2),
        "holding_days": holding_days,
        "exit_reason": exit_reason,
        "gross_pnl": round(gross, 2),
        "brokerage": round(brk, 2),
        "net_pnl": round(net, 2),
        "r_realized": round(r_realized, 3),
        "r_mfe": round(r_mfe, 3),
        "r_mae": round(r_mae, 3),
        "mfe_price": round(mfe_price, 2),
        "mae_price": round(mae_price, 2),
    }
    base.update(extras)
    return base


VARIANTS = {
    "V0_baseline": _v0_baseline,
    "V1_half_r_full": _v1_half_r_full,
    "V2_half_r_50pct": _v2_half_r_50pct,
    "V3_trail_be": _v3_trail_be,
}
