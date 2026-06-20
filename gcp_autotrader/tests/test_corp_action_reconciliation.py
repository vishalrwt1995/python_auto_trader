"""Unit tests for corp_action_reconciliation_service.evaluate_corp_position — the pure
hard meeting-day exit (+ wide protective-SL backstop). No trailing."""
from autotrader.services import corp_action_reconciliation_service as rec


def _bars(rows):  # rows = [(date, close)]
    return [[d, c, c, c, c, 1e6] for d, c in rows]


def _eval(bars, meeting="2026-01-10", sl=85.0):
    return rec.evaluate_corp_position(entry_price=100.0, side="BUY", sl_price=sl,
                                      meeting_date=meeting, candles=bars)


def test_meeting_exit_on_meeting_day():
    ev = _eval(_bars([("2026-01-08", 100.0), ("2026-01-10", 105.0)]))
    assert ev["exit_reason"] == "MEETING_EXIT" and ev["last_close"] == 105.0


def test_meeting_exit_when_passed():
    ev = _eval(_bars([("2026-01-08", 100.0), ("2026-01-12", 106.0)]))
    assert ev["exit_reason"] == "MEETING_EXIT"


def test_protective_sl_breach_before_meeting():
    ev = _eval(_bars([("2026-01-05", 100.0), ("2026-01-06", 80.0)]))  # 80 < 85, pre-meeting
    assert ev["exit_reason"] == "PROTECTIVE_SL"


def test_hold_before_meeting_above_sl():
    ev = _eval(_bars([("2026-01-05", 100.0), ("2026-01-06", 98.0)]))
    assert ev["exit_reason"] is None


def test_meeting_exit_takes_precedence_over_sl():
    # on/after meeting AND below sl -> still books MEETING_EXIT (the primary exit)
    ev = _eval(_bars([("2026-01-08", 100.0), ("2026-01-10", 80.0)]))
    assert ev["exit_reason"] == "MEETING_EXIT"


def test_no_meeting_date_holds():
    ev = _eval(_bars([("2026-01-05", 100.0), ("2026-01-06", 98.0)]), meeting="")
    assert ev["exit_reason"] is None
