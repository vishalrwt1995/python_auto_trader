"""Swing trailing-exit geometry — the single source of truth for the daily 1R
trailing stop, shared by the production reconciliation service and the backtest
fidelity test.

Replaces the retired "V2" swing exit (50% scale-out at 0.5R + 2R fixed target +
10-day max-hold) with the backtest-validated **daily 1R trailing** policy:

  - NO partial scale-out, NO fixed target — ride the full position.
  - Track the running peak (highest high for a long, lowest low for a short)
    across the daily bars since entry.
  - ARM the trail once the peak reaches ``activate_R`` (default 1R = one
    ``sl_dist``) of favourable excursion.  At the arm point the stop jumps to
    breakeven (entry), then ratchets to ``peak - trail_R*sl_dist`` (1R below the
    peak) as the peak advances.  The stop only ever moves in the favourable
    direction.
  - The stop is a RESTING level: it triggers intraday when a bar's low (long) /
    high (short) pierces it.  Production splits this across two services — the
    daily reconciliation job RATCHETS the level premarket using the peak through
    the prior bar; ``ws_monitor`` enforces the resting stop intraday.  Because
    the ratchet uses the prior bar's peak, the trail can only be HIT on a later
    bar (optimistic on same-bar reversals) — identical to the backtest.
  - ``max_hold`` is counted in TRADING days (bar offsets) → exit at the close.

Fidelity: :func:`simulate_exit` is a byte-faithful port of the trail-no-partial
branch of ``backtest_v2/exit_lab.simulate``.  ``tests/test_swing_exit.py`` asserts
the two produce identical exits over the resolved backtest entries, so the
production trail provably reproduces the +39,310/yr backtest geometry.

Backtest evidence (account-level, ₹1L, 5-slot): the V2 exit on the faithful pool
nets −56,820/yr; swapping in this trailing exit is the load-bearing exit change
behind the validated fixed system.  Do NOT reintroduce a breakeven-after-partial
or a near 2R fixed target — both were tested and lose (see git history / docs).
"""
from __future__ import annotations

from typing import Sequence

# Backtest-validated defaults (MOM_trail1.0_np_20d). Trail and activation are
# expressed in R (multiples of sl_dist) so they are price- and ATR-independent.
DEFAULT_TRAIL_R: float = 1.0
DEFAULT_ACTIVATE_R: float = 1.0
DEFAULT_MAX_HOLD_DAYS: int = 20


def _gap_fill(open_px: float, level: float, favorable: bool, is_buy: bool) -> float:
    """Gap-aware fill price for a level that is crossed on a bar.

    ``favorable=True`` for a level on the profit side (gap THROUGH it fills at the
    better ``open``); ``favorable=False`` for a stop (gap through it fills at the
    worse ``open``).  Mirrors ``exit_lab._gap_fill``.
    """
    if is_buy:
        if favorable:
            return open_px if open_px > level else level
        return open_px if open_px < level else level
    else:
        if favorable:
            return open_px if open_px < level else level
        return open_px if open_px > level else level


def trailed_stop(
    entry: float,
    is_buy: bool,
    sl_dist: float,
    peak_price: float,
    base_sl: float,
    *,
    trail_R: float = DEFAULT_TRAIL_R,
    activate_R: float = DEFAULT_ACTIVATE_R,
) -> tuple[float, bool]:
    """Return ``(stop_price, armed)`` for the current running ``peak_price``.

    ``base_sl`` is the original entry stop (``entry -/+ sl_dist``).  Once the peak
    reaches ``activate_R`` the stop is ``max/min(base_sl, peak -/+ trail_R*sl_dist)``.
    Before that it stays at ``base_sl``.  Because ``peak_price`` is monotonic, the
    returned stop is monotonic in the favourable direction — callers still ratchet
    against the persisted stop for safety.
    """
    if sl_dist <= 0:
        return base_sl, False
    if is_buy:
        armed = peak_price >= entry + activate_R * sl_dist
        if armed:
            return max(base_sl, peak_price - trail_R * sl_dist), True
        return base_sl, False
    else:
        armed = peak_price <= entry - activate_R * sl_dist
        if armed:
            return min(base_sl, peak_price + trail_R * sl_dist), True
        return base_sl, False


def simulate_exit(
    bars: Sequence[Sequence[float]],
    ei: int,
    is_buy: bool,
    sl_dist: float,
    max_hold: int = DEFAULT_MAX_HOLD_DAYS,
    *,
    trail_R: float = DEFAULT_TRAIL_R,
    activate_R: float = DEFAULT_ACTIVATE_R,
) -> tuple[int, float, str]:
    """Walk daily ``bars`` from entry index ``ei`` under the trailing policy.

    Returns ``(day_offset, exit_price, reason)`` where ``reason`` is one of
    ``"SL"`` / ``"TRAIL"`` / ``"MAX_HOLD"``.  ``bars`` rows are
    ``(date, open, high, low, close, ...)``.  This is the reference geometry; the
    reconciliation service computes the same stop via :func:`trailed_stop` and
    relies on ``ws_monitor`` for the intraday pierce, so the two stay in lockstep.
    """
    entry = float(bars[ei][1])
    sl0 = entry - sl_dist if is_buy else entry + sl_dist
    end = min(ei + max_hold, len(bars) - 1)
    peak = entry  # running peak price; peak_R starts at 0 in the backtest
    for j in range(ei, end + 1):
        o = float(bars[j][1]); h = float(bars[j][2]); l = float(bars[j][3]); c = float(bars[j][4])
        cur_sl, armed = trailed_stop(
            entry, is_buy, sl_dist, peak, sl0, trail_R=trail_R, activate_R=activate_R
        )
        # Resting-stop pierce (checked with the peak through the PRIOR bar).
        if (is_buy and l <= cur_sl) or (not is_buy and h >= cur_sl):
            moved = (cur_sl > sl0) if is_buy else (cur_sl < sl0)
            reason = "TRAIL" if (armed and moved) else "SL"
            return j - ei, _gap_fill(o, cur_sl, False, is_buy), reason
        # Update the peak at bar end → trail can only be hit on a later bar.
        peak = max(peak, h) if is_buy else min(peak, l)
    return end - ei, float(bars[end][4]), "MAX_HOLD"
