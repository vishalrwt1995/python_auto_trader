from __future__ import annotations

from autotrader.domain.models import PositionSizing
from autotrader.settings import StrategySettings


def calc_brokerage(qty: int, price: float) -> float:
    turnover = qty * price
    brk = min(20.0, turnover * 0.0005)
    stt = turnover * 0.00025
    nse = turnover * 0.0000322
    gst = (brk + nse) * 0.18
    sebi = turnover * 0.000001
    return round((brk + stt + nse + gst + sebi) * 2, 2)


def calc_position_size(
    entry_price: float,
    atr: float,
    direction: str,
    cfg: StrategySettings,
    *,
    atr_mult_override: float | None = None,
) -> PositionSizing:
    """Calculate position sizing with optional regime-aware ATR multiplier.

    Args:
        atr_mult_override: When provided, replaces cfg.atr_sl_mult. Used by
            trading_service to scale SL width by regime — tighter in PANIC/
            LOCKDOWN (ATR already inflated 3-4x), wider in AGGRESSIVE TREND_UP
            (give momentum trades room to breathe).
    """
    sl_mult = atr_mult_override if atr_mult_override is not None else cfg.atr_sl_mult
    sl_dist = max(atr * sl_mult, entry_price * 0.005)
    sl_price = entry_price - sl_dist if direction == "BUY" else entry_price + sl_dist
    target = entry_price + sl_dist * cfg.rr_intraday if direction == "BUY" else entry_price - sl_dist * cfg.rr_intraday

    qty = int(cfg.risk_per_trade // sl_dist) if sl_dist > 0 else 1
    qty = min(qty, int((cfg.capital * 0.15) // max(entry_price, 1)))
    qty = max(1, qty)
    brokerage = calc_brokerage(qty, entry_price)
    max_loss = round(qty * sl_dist + brokerage, 2)
    max_gain = round(qty * sl_dist * cfg.rr_intraday - brokerage, 2)
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

