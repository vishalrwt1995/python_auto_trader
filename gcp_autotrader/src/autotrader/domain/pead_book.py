"""PEAD/EVENT channel portfolio book — pure sizing + slot + daily-breaker logic.

Shared by the live PEAD trading service AND the backtest, so live sizing/slotting
can't drift from the validated config (the swing_signals / swing_exit / pead_signals
fidelity discipline). All functions are pure and side-effect-free.

Validated book (audit 2026-06-19, deep OOS 2010-2026, net of Upstox cost):
  * 5 slots, filled best-surprise-first each reaction day.
  * per-trade risk = 1.5% of channel capital; per-position notional cap = 20%.
  * stop distance = max(ATR14 × 2.5, entry × 1%); exit = daily 1R-trail armed at
    +1.75R over a 40-day max-hold (domain/swing_exit geometry, PEAD params).
  * daily loss/profit circuit breaker = 3% / 6% of channel capital (halts new entries).
"""
from __future__ import annotations

from typing import Any, Sequence

# Floor so a degenerate ATR can't create an absurd-size position (matches backtest).
MIN_SL_FRAC: float = 0.01


def sl_distance(atr: float, entry: float, atr_mult: float, min_frac: float = MIN_SL_FRAC) -> float:
    """Stop distance per share = max(ATR × atr_mult, entry × min_frac).

    The min_frac floor mirrors the validated backtest (``max(atr*2.5, entry*0.01)``)
    so a tiny ATR can't blow up position size.
    """
    return max(atr * atr_mult, entry * min_frac)


def position_size(entry: float, sl_dist: float, risk: float, notional_cap: float) -> int:
    """Integer share qty = min(risk / sl_dist, notional_cap / entry), floored.

    ``risk`` = rupees risked per trade (1.5% of channel capital); ``notional_cap`` =
    max position notional (20% of channel capital). Returns 0 when nothing fits
    (caller skips). Identical to the validated backtest's ``book_cap`` sizing.
    """
    if entry <= 0 or sl_dist <= 0 or risk <= 0 or notional_cap <= 0:
        return 0
    return max(0, min(int(risk / sl_dist), int(notional_cap / entry)))


def daily_breaker_tripped(realized_today: float, loss_limit: float, profit_limit: float) -> bool:
    """True when the channel's realized P&L for the day has hit its loss or profit
    circuit breaker — no new entries until tomorrow.

    ``loss_limit`` is a NEGATIVE rupee figure (e.g. −3% × capital); ``profit_limit``
    is positive (e.g. +6% × capital). Matches the per-channel breaker in the
    validated walk (``dr <= lh or dr >= ph``). Fail-closed: a None/NaN realized
    value trips the breaker.
    """
    if realized_today is None or realized_today != realized_today:   # None or NaN
        return True
    return realized_today <= loss_limit or realized_today >= profit_limit


def select_for_slots(candidates: Sequence[dict[str, Any]], open_count: int,
                     max_slots: int) -> list[dict[str, Any]]:
    """Pick the best-surprise candidates that fit the remaining slots.

    ``candidates`` are this reaction day's qualifying PEAD candidates (each a dict
    with a ``surprise`` key, as emitted by ``pead_signal_service.build_candidates``);
    ``open_count`` is how many PEAD positions are already open. Returns the top
    ``free`` by surprise (descending), where ``free = max(0, max_slots - open_count)``.
    Pure: does not place orders.
    """
    free = max(0, max_slots - open_count)
    if free <= 0:
        return []
    ranked = sorted(candidates, key=lambda c: -float(c.get("surprise", 0.0)))
    return ranked[:free]
