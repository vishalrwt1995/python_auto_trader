"""PURE reaction-date coverage for the event channels (PLEDGE / INSIDER).

Why this module exists. Both channels used to read exactly ONE reaction date per scan --
``MAX(date)`` from ``nse_insider_daily`` -- and never revisited it. Two failure modes followed,
both observed live in the 2026-07-20 -> 2026-08-13 window:

  * a weekend/holiday-dated filing was skipped forever, because by the next scan ``MAX(date)``
    had already advanced past it (Sat 2026-07-25, RAMCOIND promoter pledge-revoke);
  * a row that reached BQ after its own date's scan was never re-read (2026-07-31, EMBDL -- the
    08-03 09:12 scan logged ``rows=0`` and the 08-03 19:30 ingest wrote the row hours later).

Two of six qualifying promoter revoke legs were lost that way.

PARITY RULE -- the reason this is not just "read more dates". The backtest harness iterates every
date and enters on the first trading day AFTER the reaction date, so a Saturday filing is a Monday
entry. Processing a date on that first trading day therefore reproduces the backtest exactly.
Processing it any later would fill at a price the backtest never validated, so those dates are
reported STALE and deliberately dropped rather than entered late: losing a signal is recoverable,
silently breaking backtest/live parity is not.
"""
from collections.abc import Iterable, Sequence
import datetime as dt

__all__ = ["classify_pending_dates", "trading_days_between"]


def trading_days_between(start: str, end: str, holidays: Iterable[str] = ()) -> list[str]:
    """PURE: ascending ISO trading days in ``[start, end]`` -- weekdays minus NSE holidays.

    Pass ``time_utils.NSE_TRADING_HOLIDAYS``. Fail-closed: ``[]`` on unparseable bounds.
    """
    hol = {str(h) for h in (holidays or ())}
    try:
        day, last = dt.date.fromisoformat(str(start)), dt.date.fromisoformat(str(end))
    except (TypeError, ValueError):
        return []
    out: list[str] = []
    while day <= last:
        iso = day.isoformat()
        if day.weekday() < 5 and iso not in hol:
            out.append(iso)
        day += dt.timedelta(days=1)
    return out


def _first_trading_day_after(day: str, trading_days: Sequence[str]) -> str:
    """PURE: earliest trading day strictly after ``day``; ``""`` when the calendar cannot reach it."""
    for td in trading_days:
        if td > day:
            return td
    return ""


def classify_pending_dates(
    available: Iterable[str],
    completed: Iterable[str],
    scan_date: str,
    trading_days: Iterable[str],
) -> tuple[list[str], list[str]]:
    """PURE: split un-processed reaction dates into ``(to_process, stale)`` for ``scan_date``.

    A reaction date ``T`` is *owed* on the first trading day after ``T`` -- the day the backtest
    enters it. Hence:

    * ``first_trading_day_after(T) == scan_date`` -> **to_process** (ascending; parity-exact)
    * ``first_trading_day_after(T) <  scan_date`` -> **stale** (window missed; must NOT be entered)
    * ``first_trading_day_after(T) >  scan_date`` -> neither (not due yet)

    ``T >= scan_date`` is never owed -- the 19:30 ingest has not finished collecting that day.
    Already-completed dates are excluded first, which is what makes repeated scans on the same day
    idempotent (the one way a catch-up loop could otherwise double-enter a symbol). Dates the
    supplied calendar cannot resolve are dropped rather than guessed.
    """
    asof = str(scan_date or "")
    done = {str(c) for c in (completed or ())}
    tds = sorted({str(t) for t in (trading_days or ())})
    to_process: list[str] = []
    stale: list[str] = []
    for raw in sorted({str(a) for a in (available or ()) if a}):
        if raw in done or not asof or raw >= asof:
            continue
        nxt = _first_trading_day_after(raw, tds)
        if not nxt:
            continue
        if nxt == asof:
            to_process.append(raw)
        elif nxt < asof:
            stale.append(raw)
    return to_process, stale
