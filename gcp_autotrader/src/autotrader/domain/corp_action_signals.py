"""Corporate-action (bonus / split) pre-meeting-drift signal logic — single source of
truth for the corp-action sub-strategy of the EVENT/PEAD channel, shared by the
production signal service AND the backtest, so production can never drift from the
validated backtest (same ``domain/pead_signals.py`` fidelity discipline).

Validated config — deep-OOS 2010-2026, market-adjusted, net of Upstox cost
(build 2026-06-20; see ``docs/EVENT_CHANNEL_CORP_ACTION_PLAN.md``):
  - SIGNAL: a **bonus or split** board-meeting *intimation* (NSE
    ``/api/corporate-board-meetings`` — ``bm_timestamp`` is the intimation, ``bm_date``
    the meeting). The stock drifts up INTO the meeting then sells the news; we capture
    only the pre-meeting run-up (long), the tradeable no-look-ahead slice.
  - GATE 1 — first-time: the company's FIRST bonus/split in history (serial repeats are
    noise: +1.78% vs +0.42%). Tracked by the caller (rolling per (symbol, type)).
  - GATE 2 — uptrend: ``close >= UPTREND_MIN * 252-day low`` (healthy long-term uptrend;
    +2.32% vs +0.36% near-low).
  - GATE 3 — anti-pump: pre-entry 20d **market-adjusted** run-up < ``ANTI_PUMP_MAX_RUNUP``
    (already-pumped names give back the move). The market-adjustment reference MUST match
    the backtest (equal-weight universe) — caller computes it, passes the scalar.
  - GATE 4 — lead: >= ``LEAD_MIN_DAYS`` trading days from intimation to meeting.
  - ENTRY: smart — intimation-day CLOSE if intimated before ``SMART_ENTRY_HOUR`` IST,
    else next session OPEN (the news is already public; no look-ahead either way).
  - EXIT: hard exit at the **meeting-day close** (~3-day hold). NOT PEAD's ATR-trail —
    the run-up peaks at the meeting and reverses after. Owned by the reconciliation
    service via the position's stored meeting date (this module is entry-only).
  Liquidity (20d avg turnover >= ``TURNOVER_MIN``) + price floor applied by the caller.

Economics (liquid >= Rs10cr): ~+2.48% net/event, robust IS+OOS, ~5/yr. As a sub-strategy
sharing the EVENT/PEAD pool it adds ~+2.4-3.4%/yr **uncorrelated** with PEAD and lifts the
channel ~+41% on the same Rs2L (corp fills PEAD's idle slots). Buyback REJECTED (negative);
merger/delist/fundraise/dividend are duds on the long side.

All functions are PURE and LOOK-AHEAD-FREE (only bars up to the entry index; the
market-adjustment is computed by the caller and passed in). Fail-closed: any missing
input -> no signal, never a silent default.
"""
from __future__ import annotations

from typing import Sequence

# ── Validated config constants (do not change without a fresh OOS walk) ─────────
EVENT_TYPES: tuple[str, ...] = ("bonus", "split")   # the only tradeable long corp-action edges
UPTREND_MIN: float = 1.40           # close >= this * 252d low (>=40% above the 52-week low)
ANTI_PUMP_MAX_RUNUP: float = 0.06   # exclude if 20d market-adjusted run-up >= this (~tertile cutoff)
RUNUP_LOOKBACK: int = 20            # trading days for the pre-entry run-up window
LEAD_MIN_DAYS: int = 4              # min trading days intimation -> meeting
LOW_WINDOW: int = 252              # trading days for the 52-week low
TURNOVER_MIN: float = 1e8           # 20d avg turnover floor (Rs10cr — liquid, realistic cost)
PRICE_MIN: float = 30.0             # entry price floor
SMART_ENTRY_HOUR: int = 14          # intimation before this IST hour -> same-day close entry
MAX_LEAD_DAYS: int = 15             # sanity cap; ignore stale/rescheduled meetings beyond this


def dist_above_52w_low(
    closes: Sequence[float], lows: Sequence[float], idx: int, window: int = LOW_WINDOW
) -> float | None:
    """``close[idx] / min(low[idx-window : idx])`` — the entry close relative to the
    trailing 52-week low. A value of 1.40 means 40% above the low (an uptrend). Uses
    only bars strictly before ``idx`` for the low, plus the entry close. Returns
    ``None`` without enough history or on bad data.
    """
    if idx <= 0 or idx >= len(closes):
        return None
    start = idx - window
    if start < 0:
        start = 0
    lo = min(lows[start:idx]) if idx > start else None
    if not lo or lo <= 0 or closes[idx] <= 0:
        return None
    return closes[idx] / lo


def pre_entry_runup_raw(
    closes: Sequence[float], idx: int, lookback: int = RUNUP_LOOKBACK
) -> float | None:
    """Raw pre-entry run-up = ``close[idx-1] / close[idx-1-lookback] - 1`` — the move
    over the ``lookback`` sessions ending the day before entry. The caller subtracts
    the market return over the same window (equal-weight universe, matching the
    backtest) before applying the anti-pump gate. Returns ``None`` without history.
    """
    base_idx = idx - 1 - lookback
    if base_idx < 0 or idx - 1 >= len(closes):
        return None
    base = closes[base_idx]
    if base <= 0:
        return None
    return closes[idx - 1] / base - 1.0


def entry_is_same_day_close(intim_hour: int | None) -> bool:
    """Smart-entry timing: ``True`` -> enter at the intimation-day CLOSE (intimated
    during market hours, before ``SMART_ENTRY_HOUR``); ``False`` -> next session OPEN.
    No look-ahead either way: the intimation is public the moment it is filed.
    """
    return bool(intim_hour) and 0 < int(intim_hour) < SMART_ENTRY_HOUR


def passes_corp_gates(
    event_type: str | None,
    is_first_time: bool | None,
    dist_low_ratio: float | None,
    runup_market_adj: float | None,
    lead_days: int | None,
    *,
    uptrend_min: float = UPTREND_MIN,
    max_runup: float = ANTI_PUMP_MAX_RUNUP,
    lead_min: int = LEAD_MIN_DAYS,
    max_lead: int = MAX_LEAD_DAYS,
) -> bool:
    """The locked corp-action entry gate: bonus/split AND first-time AND uptrend AND
    not-pumped AND adequate (not stale) intimation lead.

    ``dist_low_ratio`` = ``dist_above_52w_low(...)`` (>= 1.0). ``runup_market_adj`` =
    raw 20d run-up minus the market's return over the same window (caller-computed).
    ``lead_days`` = trading days from intimation to meeting. Fail-closed.
    """
    if event_type not in EVENT_TYPES:
        return False
    if not is_first_time:
        return False
    if dist_low_ratio is None or runup_market_adj is None or lead_days is None:
        return False
    return (
        dist_low_ratio >= uptrend_min
        and runup_market_adj < max_runup
        and lead_min <= lead_days <= max_lead
    )
