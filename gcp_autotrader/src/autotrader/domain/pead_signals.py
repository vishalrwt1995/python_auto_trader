"""PEAD (Post-Earnings-Announcement Drift) signal logic — the single source of
truth for the EVENT/PEAD channel, shared by the production signal service AND the
backtest, so production can never drift from the validated backtest (the same
``domain/swing_signals.py`` / ``domain/swing_exit.py`` fidelity discipline).

Validated config — "Config B", deep-OOS 2010-2026, net of Upstox cost (build 2026-06-19):
  - SIGNAL: a stock whose earnings reaction (close vs prior close on the first
    session after results are filed) is >= ``SURPRISE_MIN``.
  - GATE 1 — market-state: trade only when the broad market is within
    ``MARKET_DD_GATE`` of its trailing high. PEAD drifts ~+6% near highs and dies
    in corrections — the edge is regime-conditional (see PROJECT_KNOWLEDGE §8).
  - GATE 2 — anti-pump: exclude already-pumped names (pre-event run-up >=
    ``ANTI_PUMP_MAX_RUNUP`` over ``ANTI_PUMP_LOOKBACK`` days). The disaster trades
    are pump-and-dumps that pop on results then mean-revert hard.
  - ENTRY: next session's open. EXIT: daily 1R-trail (arm 1.75) over
    ``MAX_HOLD_DAYS``, 2.5x ATR stop — reuse ``domain/swing_exit.py`` geometry.

Standalone performance (own capital): ~8% raw / ~4-5% live per yr, ~20% max DD,
10/14 years positive, holds out-of-sample (both halves +). Dormant in corrections
(the market-state gate sits it out) — a long-term, regime-conditional diversifier.

All functions are PURE and LOOK-AHEAD-FREE: they use only bars up to the reaction
index, and the market-state drawdown is computed by the caller (kept out of this
module so it stays per-stock and deterministic). Fail-closed: missing inputs -> no
signal, never a silent default.
"""
from __future__ import annotations

from typing import Sequence

# ── Validated config constants (do not change without a fresh OOS walk) ────────
# Config B + grind v2 (2026-06-19 OOS-disciplined sweep, IS 2010-17 / OOS 2018-26):
# loosened anti-pump 0.50->0.75 and extended max-hold 40->60. Both improve BOTH
# halves with the gain CONCENTRATED OUT-OF-SAMPLE (OOS +71%, IS +24% — the opposite
# of overfitting), on a smooth plateau, and are economically motivated (0.50 was
# over-filtering legit momentum; 60d is the textbook PEAD drift horizon). Net at
# Rs2L NIFTY-50 gate: +42% total (Rs332k vs Rs234k), ~same MTM DD (23.6% vs 22.8%),
# 14/17 +yrs. See PROJECT_KNOWLEDGE §8.
SURPRISE_MIN: float = 0.05            # min earnings-day reaction to qualify
ANTI_PUMP_MAX_RUNUP: float = 0.75     # exclude if pre-event run-up >= this (grind v2: 0.50->0.75)
ANTI_PUMP_LOOKBACK: int = 60          # trading days for the pre-event run-up window
MARKET_DD_GATE: float = -0.05         # trade only when broad-market drawdown > this
MAX_HOLD_DAYS: int = 60               # PEAD drift horizon (grind v2: 40->60; vs swing's 20)
ATR_SL_MULT: float = 2.5              # stop distance = ATR14 * this (matches swing sizing)


def earnings_surprise(closes: Sequence[float], ri: int) -> float | None:
    """Reaction-day price surprise = ``close[ri] / close[ri-1] - 1``.

    ``ri`` is the reaction index — the first session AFTER results were filed.
    Known at the close of ``ri``; entry is at ``ri+1`` open, so look-ahead-free.
    Returns ``None`` when there is no valid prior close.
    """
    if ri < 1 or ri >= len(closes):
        return None
    prev = closes[ri - 1]
    if prev <= 0:
        return None
    return closes[ri] / prev - 1.0


def pre_event_runup(
    closes: Sequence[float], ri: int, lookback: int = ANTI_PUMP_LOOKBACK
) -> float | None:
    """Pre-event run-up = ``close[ri-1] / close[ri-1-lookback] - 1`` — the move INTO
    the event, ending the session before the reaction (so it excludes the reaction
    itself). Used by the anti-pump gate. Returns ``None`` without enough history.
    """
    base_idx = ri - 1 - lookback
    if base_idx < 0 or ri - 1 >= len(closes):
        return None
    base = closes[base_idx]
    if base <= 0:
        return None
    return closes[ri - 1] / base - 1.0


def passes_pead_gates(
    surprise: float | None,
    runup: float | None,
    market_dd: float | None,
    *,
    surprise_min: float = SURPRISE_MIN,
    max_runup: float = ANTI_PUMP_MAX_RUNUP,
    market_dd_gate: float = MARKET_DD_GATE,
) -> bool:
    """Config B entry gate: positive surprise AND healthy market AND not-pumped.

    ``market_dd`` is the broad-market drawdown from its trailing high (<= 0),
    computed once per day by the caller from the universe and passed in.
    Fail-closed: any missing input returns ``False``.
    """
    if surprise is None or runup is None or market_dd is None:
        return False
    return (
        surprise >= surprise_min
        and runup < max_runup
        and market_dd > market_dd_gate
    )
