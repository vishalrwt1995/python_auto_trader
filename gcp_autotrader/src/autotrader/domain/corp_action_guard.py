"""PURE corporate-action guard for stop-loss exits (all channels).

Why this module exists. HEG (insider channel) demerged 2026-09-01 with a 2026-09-07 record
date -- NSE's own feed labels the row ``subject="Demerger"``. The price mechanically adjusted
down ~64% (₹729 -> ₹260) to reflect value carved into the new listed entity; nothing about the
shareholder's economic position was actually destroyed. The live exit path read the adjusted
price as a stop-loss breach and exited the position for a ₹12,286 "loss" that isn't real. No
channel, and no part of the ten independent SL-comparison sites this codebase has, had any
concept of a corporate action on a held position.

This module does NOT try to auto-adjust the stop/entry price for the new baseline -- a bonus or
split has an exact, mechanically computable ratio, but a demerger's price carve-out does not (the
market decides what each half is worth, which this system cannot know in advance). Instead it
answers one narrower, safer question: "is `symbol` inside the blast radius of a recent corporate
action, close enough that an automatic SL exit should be PAUSED for a human to look at it?" A
paused position still shows up in the daily audit; a silently-executed phantom exit does not.

Window: ``[rec_date - 1, rec_date + 5]`` calendar days. The -1 covers an ex-date landing the day
before the record date (as it does for most dividends); the +5 is a deliberate buffer for the
position's own check to be a few sessions late (weekend gaps, a lagging ingest) without needing a
trading-day calendar here -- unlike ``reaction_dates.py``, backtest parity is not the concern, so
plain calendar days are enough and keep this dependency-free. The window is a stopgap, not a fix:
once it lapses the position's stop is still set against the PRE-action price, so anything still
open after +5 days needs a human to look at it (that's the point -- five days is "long enough that
the paused position will surface in the next session's audit" by design, not an attempt to auto-
resolve it).
"""
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
import datetime as dt

__all__ = ["GuardResult", "is_guard_active"]

_WINDOW_BEFORE_DAYS = 1
_WINDOW_AFTER_DAYS = 5


@dataclass(frozen=True)
class GuardResult:
    """``active`` is False (never None) on every fail-closed path -- callers can always
    branch on it directly. ``action`` carries the matched row only when active, for logging."""
    active: bool
    action: Mapping[str, object] | None = None


def is_guard_active(symbol: str, as_of: str, actions: Iterable[Mapping[str, object]]) -> GuardResult:
    """PURE: does `symbol` have a corporate action whose window covers `as_of`?

    ``actions`` is the full list of corp-action rows available (NOT pre-filtered by symbol --
    this does the symbol match itself so callers can pass a shared, unfiltered lookup). Each row
    is expected to carry ``symbol`` and ``rec_date`` (``ex_date`` as a fallback when ``rec_date``
    is absent -- NSE's feed sometimes carries only one of the two). Rows this symbol doesn't
    match, or whose date fields don't parse, are skipped rather than raising -- a malformed
    calendar row must never crash or block real exits.

    Fail-closed on bad input: an unparseable ``as_of`` or empty ``symbol`` returns inactive. This
    mirrors ``reaction_dates.classify_pending_dates``'s posture, but the failure DIRECTION here is
    the opposite and intentional -- when this function cannot make a confident call, the correct
    default is to let the stop-loss fire (inactive), not to suspend it. A guard that fails toward
    "hold anyway" is a worse bug than a corporate action it occasionally misses.
    """
    try:
        asof_date = dt.date.fromisoformat(str(as_of))
    except (TypeError, ValueError):
        return GuardResult(active=False)
    sym = str(symbol or "").strip().upper()
    if not sym:
        return GuardResult(active=False)

    for action in actions or ():
        if str(action.get("symbol", "")).strip().upper() != sym:
            continue
        raw_date = action.get("rec_date") or action.get("ex_date")
        if not raw_date:
            continue
        try:
            rec_date = dt.date.fromisoformat(str(raw_date))
        except (TypeError, ValueError):
            continue
        window_start = rec_date - dt.timedelta(days=_WINDOW_BEFORE_DAYS)
        window_end = rec_date + dt.timedelta(days=_WINDOW_AFTER_DAYS)
        if window_start <= asof_date <= window_end:
            return GuardResult(active=True, action=action)
    return GuardResult(active=False)
