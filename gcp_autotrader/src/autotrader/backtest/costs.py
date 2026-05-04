"""Indian-market round-trip cost calculator.

Computes realistic frictions for a backtest fill: brokerage, STT (Securities
Transaction Tax), exchange transaction charges, SEBI charges, GST on
brokerage+exchange, stamp duty, DP charges (delivery only).

Defaults match Zerodha-style discount-broker fees as of 2026; the user can
override via `CostConfig` to model their actual broker.

References:
    https://zerodha.com/charges  (rates change annually — verify before use)

Conventions
-----------
* All numbers returned are in INR (₹), absolute, never percentage strings.
* `compute()` returns the *fill-side* cost (one leg). For a round-trip,
  call it twice — once for the entry leg, once for the exit leg.
* Intraday vs delivery is decided by `is_swing` on the position, not by
  product code on the order — backtests don't have product codes.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CostConfig:
    """Per-side cost rates. All "rate" fields are decimal fractions
    (0.001 = 0.1%, NOT 0.1). Rupee values are flat fees."""

    # ── Brokerage ──────────────────────────────────────────────────────
    # Zerodha-style: ₹20 per executed order or 0.03% (whichever lower)
    # for intraday. Delivery: ₹0 brokerage. Capped at ₹20.
    brokerage_intraday_pct: float = 0.0003     # 0.03%
    brokerage_intraday_cap: float = 20.0       # ₹ per order
    brokerage_delivery_pct: float = 0.0        # delivery is free at most discount brokers

    # ── STT (paid only on the SELL leg) ────────────────────────────────
    # Intraday: 0.025% on sell side only.
    # Delivery: 0.1% on both buy and sell.
    stt_intraday_sell_pct: float = 0.00025
    stt_delivery_pct: float = 0.001

    # ── Exchange transaction charges (NSE) ─────────────────────────────
    # NSE: 0.00297% on both sides (was 0.00345% pre-2024). Verify yearly.
    exchange_pct: float = 0.0000297

    # ── SEBI turnover charges ──────────────────────────────────────────
    sebi_pct: float = 0.000001                 # ₹10 per crore = 0.0001%

    # ── GST (on brokerage + exchange + SEBI) ───────────────────────────
    gst_pct: float = 0.18                      # 18%

    # ── Stamp duty (paid only on BUY leg) ──────────────────────────────
    # Intraday: 0.003% on buy. Delivery: 0.015% on buy.
    stamp_intraday_buy_pct: float = 0.00003
    stamp_delivery_buy_pct: float = 0.00015

    # ── DP charges (delivery only, charged on SELL leg) ────────────────
    # ₹13.5 + GST per scrip per day, regardless of qty.
    dp_charge_flat: float = 13.5
    dp_gst_pct: float = 0.18                   # GST on DP charge separately


def compute_leg_cost(
    *,
    side: str,
    qty: int,
    price: float,
    is_swing: bool,
    cfg: CostConfig | None = None,
) -> float:
    """Compute cost (₹) for ONE leg (one fill).

    Parameters
    ----------
    side : "BUY" or "SELL" — affects STT and stamp duty
    qty : shares
    price : ₹/share
    is_swing : True ⇒ delivery (CNC) charges; False ⇒ intraday (MIS)
    cfg : optional override; defaults to module-level CostConfig()

    Returns
    -------
    float — total cost for this leg in ₹.
    """
    cfg = cfg or CostConfig()
    if qty <= 0 or price <= 0:
        return 0.0

    notional = qty * price
    side_u = side.upper()

    # 1. Brokerage
    if is_swing:
        brokerage = notional * cfg.brokerage_delivery_pct
    else:
        brokerage = min(cfg.brokerage_intraday_cap, notional * cfg.brokerage_intraday_pct)

    # 2. STT
    if is_swing:
        stt = notional * cfg.stt_delivery_pct
    else:
        stt = notional * cfg.stt_intraday_sell_pct if side_u == "SELL" else 0.0

    # 3. Exchange + SEBI (both sides)
    exchange = notional * cfg.exchange_pct
    sebi = notional * cfg.sebi_pct

    # 4. GST on (brokerage + exchange + SEBI)
    gst = (brokerage + exchange + sebi) * cfg.gst_pct

    # 5. Stamp duty (BUY only)
    if side_u == "BUY":
        stamp_pct = cfg.stamp_delivery_buy_pct if is_swing else cfg.stamp_intraday_buy_pct
        stamp = notional * stamp_pct
    else:
        stamp = 0.0

    # 6. DP charges (delivery, SELL only — flat fee)
    if is_swing and side_u == "SELL":
        dp = cfg.dp_charge_flat * (1 + cfg.dp_gst_pct)
    else:
        dp = 0.0

    return round(brokerage + stt + exchange + sebi + gst + stamp + dp, 2)


def compute_round_trip_cost(
    *,
    qty: int,
    entry_price: float,
    exit_price: float,
    is_swing: bool,
    cfg: CostConfig | None = None,
) -> float:
    """Total ₹ cost of buying and selling one position.

    For longs: BUY at entry, SELL at exit.
    For shorts: SELL at entry, BUY at exit.
    The math is symmetric — STT-on-sell applies to the selling leg
    regardless of which leg opens the position.
    """
    # Long-side accounting: cost is the same either way for a balanced round
    # trip (BUY + SELL each at their respective price). The simulator passes
    # the actual fill prices.
    cost_buy = compute_leg_cost(
        side="BUY", qty=qty, price=entry_price, is_swing=is_swing, cfg=cfg,
    )
    cost_sell = compute_leg_cost(
        side="SELL", qty=qty, price=exit_price, is_swing=is_swing, cfg=cfg,
    )
    return round(cost_buy + cost_sell, 2)


__all__ = ["CostConfig", "compute_leg_cost", "compute_round_trip_cost"]
