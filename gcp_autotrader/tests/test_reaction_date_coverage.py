"""Failing-first tests for the PLEDGE/INSIDER reaction-date coverage leak (found 2026-08-14).

LIVE EVIDENCE (BQ ``nse_insider_daily`` + Cloud Run logs, window 2026-07-20 -> 2026-08-13):
six qualifying promoter pledge-revoke legs existed across five dates. The live scan reads exactly
ONE date per run -- ``pledge_signal_service.latest_reaction_date()`` = ``MAX(date)`` -- and never
revisits it, so two signals were lost outright:

  * 2026-07-25 (Sat) RAMCOIND -- NO run ever read ``date=2026-07-25``; by the next scan
    ``MAX(date)`` had already advanced to 07-27, so the date was skipped permanently.
  * 2026-07-31 (Fri) EMBDL    -- the 08-03 scan logged ``pledge_fetch_rows date=2026-07-31 rows=0``
    yet that row is in BQ today: it landed with the 08-03 19:30 ingest, AFTER that morning's
    09:12 scan. Never re-read.

The three dates that WERE read behaved correctly and must keep doing so: 07-23 produced
``revokes=1 candidates=1`` (RAMCOIND passed, then met a closed macro gate), while 07-30
(GEOJITFSL) and 08-12 (MASTERTR x2) were rejected by the validated turnover / 200DMA filters.

PARITY IS THE BINDING CONSTRAINT. No backtest or grind script references ``latest_reaction_date``
or ``fetch_revoke_rows`` -- the harness iterates EVERY date and enters on the first trading day
after the reaction date. So a Saturday-dated filing is a Monday entry in the backtest. Catching up
on the first trading day after T therefore reproduces the backtest exactly; entering any later
does not, and would silently break parity. ``classify_pending_dates`` must hand back a date for
processing only when the scan day IS that first trading day, and mark anything older STALE rather
than entering it late.

Test split is deliberate:
  * ``classify_pending_dates`` tests FAIL until the function exists -- that is the point; they
    prove the suite detects the leak rather than merely describing it.
  * ``aggregate_revokes`` parity tests PASS today and must keep passing -- they pin the real BQ
    rows so the fix cannot alter strategy behaviour.
"""
import datetime as dt

import pytest

from autotrader.domain.pledge_signals import aggregate_revokes

# --- the real NSE calendar for the window (no NSE holidays fall inside it) -------------------
def _weekdays(start: str, end: str) -> list[str]:
    d, last, out = dt.date.fromisoformat(start), dt.date.fromisoformat(end), []
    while d <= last:
        if d.weekday() < 5:                     # Mon-Fri
            out.append(d.isoformat())
        d += dt.timedelta(days=1)
    return out


TRADING_DAYS = _weekdays("2026-07-15", "2026-08-21")

# Dates present in nse_insider_daily across the window, as the live fetch log shows them read.
AVAILABLE = ["2026-07-23", "2026-07-24", "2026-07-25", "2026-07-30", "2026-07-31",
             "2026-08-03", "2026-08-12", "2026-08-13"]


def _classify(*args, **kwargs):
    """Imported lazily so the parity tests below still run while the function is unimplemented."""
    from autotrader.domain.reaction_dates import classify_pending_dates
    return classify_pending_dates(*args, **kwargs)


# =============================== coverage: currently FAILING ===============================

def test_saturday_dated_filing_is_recovered_on_monday():
    """THE RAMCOIND MISS. A Sat-dated (2026-07-25) filing must process on Mon 2026-07-27 --
    the first trading day after it, which is exactly where the backtest enters it."""
    to_process, stale = _classify(
        available=AVAILABLE, completed=["2026-07-23", "2026-07-24"],
        scan_date="2026-07-27", trading_days=TRADING_DAYS)
    assert "2026-07-25" in to_process
    assert "2026-07-25" not in stale


def test_friday_and_saturday_dates_both_process_on_the_same_monday():
    """07-24 (Fri) and 07-25 (Sat) share Mon 07-27 as their next trading day, so a single Monday
    scan owes BOTH. The single-MAX(date) reader can only ever return one of them."""
    to_process, _ = _classify(
        available=AVAILABLE, completed=["2026-07-23"],
        scan_date="2026-07-27", trading_days=TRADING_DAYS)
    assert to_process == ["2026-07-24", "2026-07-25"]        # oldest-first


def test_completed_date_is_never_reprocessed():
    """IDEMPOTENCY -- the one way this fix could place duplicate orders. A date already processed
    must never come back, even while a neighbouring date is still pending."""
    to_process, stale = _classify(
        available=AVAILABLE, completed=["2026-07-23", "2026-07-24", "2026-07-25"],
        scan_date="2026-07-27", trading_days=TRADING_DAYS)
    assert to_process == []
    assert "2026-07-23" not in stale


def test_overdue_date_is_marked_stale_not_entered_late():
    """PARITY GUARD. 07-31's validated entry day is Mon 08-03. Scanning on 08-05 is too late --
    entering then would fill at a price the backtest never validated, so it must be reported
    stale, never processed."""
    to_process, stale = _classify(
        available=AVAILABLE, completed=[], scan_date="2026-08-05", trading_days=TRADING_DAYS)
    assert "2026-07-31" in stale
    assert "2026-07-31" not in to_process


def test_ordinary_next_trading_day_case_processes():
    """The path that already works today (MASTERTR): 08-12 -> scanned 08-13. Must not regress."""
    to_process, stale = _classify(
        available=AVAILABLE, completed=AVAILABLE[:-2],
        scan_date="2026-08-13", trading_days=TRADING_DAYS)
    assert to_process == ["2026-08-12"]
    assert stale == []


def test_date_not_yet_due_is_neither_processed_nor_stale():
    """08-13's entry day is 08-14, so a scan ON 08-13 must leave it alone -- not process it early,
    not write it off as stale."""
    to_process, stale = _classify(
        available=AVAILABLE, completed=AVAILABLE[:-1],
        scan_date="2026-08-13", trading_days=TRADING_DAYS)
    assert "2026-08-13" not in to_process
    assert "2026-08-13" not in stale


def test_scan_date_itself_is_never_a_reaction_date():
    """A reaction date must be strictly before the scan day; same-day would enter on data the
    19:30 ingest has not finished collecting."""
    to_process, _ = _classify(
        available=["2026-08-13"], completed=[], scan_date="2026-08-13",
        trading_days=TRADING_DAYS)
    assert to_process == []


def test_empty_available_is_fail_closed():
    """No data must yield no work -- never a bare MAX() fallback that guesses a date."""
    assert _classify(available=[], completed=[], scan_date="2026-08-13",
                     trading_days=TRADING_DAYS) == ([], [])


# ===================== parity: PASSING today, must keep passing =====================
# Rows below are the real BQ rows for each date (transaction_type/person_category verbatim).

def test_parity_0723_ramcoind_promoter_group_leg_aggregates():
    """07-23 produced revokes=1 in prod. Pinned so the fix cannot change it."""
    out = aggregate_revokes([
        {"symbol": "RAMCOIND", "person_category": "Promoter Group",
         "transaction_type": "Pledge Revoke", "shares": 255000.0}])
    assert list(out) == ["RAMCOIND"]
    assert out["RAMCOIND"]["n_revokes"] == 1


def test_parity_0812_mastertr_two_legs_aggregate_to_one_symbol():
    """08-12 read rows=2 and logged revokes=1 -- two legs, one symbol."""
    leg = {"symbol": "MASTERTR", "person_category": "Promoter and Director",
           "transaction_type": "Pledge Revoke", "shares": 7500000.0}
    out = aggregate_revokes([dict(leg), dict(leg)])
    assert list(out) == ["MASTERTR"]
    assert out["MASTERTR"]["n_revokes"] == 2


def test_parity_0730_promoter_kept_connected_person_rejected():
    """Both rows are dated 07-30; only the promoter leg may survive."""
    out = aggregate_revokes([
        {"symbol": "GEOJITFSL", "person_category": "Promoter and Director",
         "transaction_type": "Pledge Revoke", "shares": 7500000.0},
        {"symbol": "CCL", "person_category": "Connected Person",
         "transaction_type": "Pledge Revoke", "shares": 15000.0}])
    assert list(out) == ["GEOJITFSL"]


@pytest.mark.parametrize("symbol,category,shares", [
    ("BLUESTONE", "Director", 5016087.0),      # 2026-08-13
    ("DMART", "KMP", 13112.0),                 # 2026-07-17
])
def test_parity_non_promoter_revokes_yield_nothing(symbol, category, shares):
    """The promoter filter works and must stay working -- Director/KMP legs are not signals."""
    assert aggregate_revokes([
        {"symbol": symbol, "person_category": category,
         "transaction_type": "Pledge Revoke", "shares": shares}]) == {}
