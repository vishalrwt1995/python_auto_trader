from __future__ import annotations

from autotrader.domain.models import PositionSizing
from autotrader.settings import StrategySettings


def calc_swing_position_size(
    entry_price: float,
    atr: float,
    direction: str,
    cfg: StrategySettings,
    *,
    atr_mult_override: float | None = None,
) -> PositionSizing:
    """Position sizing for swing (CNC) trades — wider SL, higher R:R."""
    sl_mult = atr_mult_override if atr_mult_override is not None else cfg.swing_atr_sl_mult
    sl_dist = max(atr * sl_mult, entry_price * 0.01)  # floor at 1% for swing
    sl_price = entry_price - sl_dist if direction == "BUY" else entry_price + sl_dist
    target = entry_price + sl_dist * cfg.swing_rr if direction == "BUY" else entry_price - sl_dist * cfg.swing_rr

    raw_qty = int(cfg.swing_risk_per_trade // sl_dist) if sl_dist > 0 else 0
    qty = min(raw_qty, int((cfg.capital * 0.20) // max(entry_price, 1)))  # 20% max capital per swing
    # Skip swing trade only if even 1 share exceeds 1.5× risk budget.
    if qty < 1 and sl_dist > cfg.swing_risk_per_trade * 1.5:
        qty = 0
    else:
        qty = max(1, qty)
    brokerage = calc_brokerage(qty, entry_price)
    max_loss = round(qty * sl_dist + brokerage, 2)
    max_gain = round(qty * sl_dist * cfg.swing_rr - brokerage, 2)
    return PositionSizing(
        qty=qty,
        sl_price=sl_price,
        target=target,
        sl_dist=sl_dist,
        entry_price=entry_price,
        max_loss=max_loss,
        max_gain=max_gain,
        brokerage=brokerage,
    )


def calc_brokerage_leg(qty: int, price: float) -> float:
    """Brokerage + taxes for a single leg (entry OR exit).

    Indian retail cost model (Upstox, CNC/MIS equity):
    - Brokerage: ₹20 flat OR 0.05% of turnover, whichever is lower
    - STT: 0.025% on SELL side only for intraday; 0.1% each leg for delivery.
      Here we charge 0.025% every leg as a conservative mid-point estimate
      (real STT is asymmetric but the difference is <0.01% on most trades).
    - NSE txn + GST on (brk + nse) + SEBI + stamp duty.
    """
    turnover = qty * price
    if turnover <= 0:
        return 0.0
    brk = min(20.0, turnover * 0.0005)
    stt = turnover * 0.00025
    nse = turnover * 0.0000322
    gst = (brk + nse) * 0.18
    sebi = turnover * 0.000001
    stamp = turnover * 0.000015  # buy-side only in reality; included as conservative
    return round(brk + stt + nse + gst + sebi + stamp, 2)


def calc_brokerage(qty: int, price: float) -> float:
    """Round-trip brokerage using `price` for both legs (sizing estimate only).

    Used at position-sizing time when exit price is unknown. For realized
    P&L, call calc_round_trip_brokerage(qty, entry_price, exit_price).
    """
    return round(calc_brokerage_leg(qty, price) * 2, 2)


def calc_round_trip_brokerage(qty: int, entry_price: float, exit_price: float) -> float:
    """Exact round-trip brokerage using the real entry AND exit prices.

    Subtract this from gross P&L to get net realized P&L.
    """
    return round(calc_brokerage_leg(qty, entry_price) + calc_brokerage_leg(qty, exit_price), 2)


def calc_position_size(
    entry_price: float,
    atr: float,
    direction: str,
    cfg: StrategySettings,
    *,
    atr_mult_override: float | None = None,
    rr_override: float | None = None,
) -> PositionSizing:
    """Calculate position sizing with optional regime-aware ATR multiplier.

    Args:
        atr_mult_override: When provided, replaces cfg.atr_sl_mult. Used by
            trading_service to scale SL width by regime — tighter in PANIC/
            LOCKDOWN (ATR already inflated 3-4x), wider in AGGRESSIVE TREND_UP
            (give momentum trades room to breathe).
        rr_override: When provided, replaces cfg.rr_intraday. Batch 4.1
            (2026-04-22): MEAN_REVERSION / VWAP_REVERSAL pass the wider
            cfg.rr_intraday_reversion (2.0) because fade setups need
            meaningful excursion. Trend setups use the default 1.25R.
    """
    sl_mult = atr_mult_override if atr_mult_override is not None else cfg.atr_sl_mult
    sl_dist = max(atr * sl_mult, entry_price * 0.005)
    sl_price = entry_price - sl_dist if direction == "BUY" else entry_price + sl_dist
    rr = rr_override if rr_override is not None else cfg.rr_intraday
    target = entry_price + sl_dist * rr if direction == "BUY" else entry_price - sl_dist * rr

    raw_qty = int(cfg.risk_per_trade // sl_dist) if sl_dist > 0 else 0
    qty = min(raw_qty, int((cfg.capital * 0.15) // max(entry_price, 1)))
    # Skip trade only if even 1 share would exceed 1.5× risk budget (SL too wide).
    # Previously this was 2× which forced qty=1 for stocks with SL between 1-2× risk
    # budget, turning them into trades that risked ₹2,000–4,000 for a ₹2,000 budget.
    if qty < 1 and sl_dist > cfg.risk_per_trade * 1.5:
        qty = 0
    else:
        qty = max(1, qty)
    brokerage = calc_brokerage(qty, entry_price)
    max_loss = round(qty * sl_dist + brokerage, 2)
    max_gain = round(qty * sl_dist * rr - brokerage, 2)
    return PositionSizing(
        qty=qty,
        sl_price=sl_price,
        target=target,
        sl_dist=sl_dist,
        entry_price=entry_price,
        max_loss=max_loss,
        max_gain=max_gain,
        brokerage=brokerage,
    )

