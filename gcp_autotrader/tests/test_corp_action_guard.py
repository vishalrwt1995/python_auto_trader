"""Tests for domain.corp_action_guard -- see that module's docstring for the HEG incident
this exists to prevent a repeat of."""
from autotrader.domain.corp_action_guard import GuardResult, is_guard_active

HEG_DEMERGER = {
    "symbol": "HEG",
    "isin": "INE545A01016",
    "subject": "Demerger",
    "ex_date": "2026-09-07",
    "rec_date": "2026-09-07",
}


def test_heg_regression_the_guard_would_have_caught_it():
    """The actual incident, as a fixture: entry 679.63 / sl 607.49, exited at ltp=260.00 on
    2026-09-07 -- exactly the corporate action's own record date. This must be active."""
    result = is_guard_active("HEG", "2026-09-07", [HEG_DEMERGER])
    assert result.active is True
    assert result.action["subject"] == "Demerger"


def test_active_on_the_day_before_record_date():
    result = is_guard_active("HEG", "2026-09-06", [HEG_DEMERGER])
    assert result.active is True


def test_active_five_days_after_record_date():
    result = is_guard_active("HEG", "2026-09-12", [HEG_DEMERGER])
    assert result.active is True


def test_inactive_two_days_before_record_date():
    result = is_guard_active("HEG", "2026-09-05", [HEG_DEMERGER])
    assert result.active is False


def test_inactive_six_days_after_record_date():
    result = is_guard_active("HEG", "2026-09-13", [HEG_DEMERGER])
    assert result.active is False


def test_inactive_for_a_different_symbol():
    result = is_guard_active("TCS", "2026-09-07", [HEG_DEMERGER])
    assert result.active is False


def test_symbol_match_is_case_insensitive():
    result = is_guard_active("heg", "2026-09-07", [HEG_DEMERGER])
    assert result.active is True


def test_empty_actions_list_is_inactive():
    result = is_guard_active("HEG", "2026-09-07", [])
    assert result.active is False


def test_empty_symbol_is_inactive():
    result = is_guard_active("", "2026-09-07", [HEG_DEMERGER])
    assert result.active is False


def test_unparseable_as_of_date_fails_closed_to_inactive():
    result = is_guard_active("HEG", "not-a-date", [HEG_DEMERGER])
    assert result.active is False


def test_malformed_rec_date_on_the_row_is_skipped_not_raised():
    bad_row = {**HEG_DEMERGER, "rec_date": "07-Sep-2026"}  # NSE's raw string format, unparsed
    result = is_guard_active("HEG", "2026-09-07", [bad_row])
    assert result.active is False


def test_falls_back_to_ex_date_when_rec_date_is_missing():
    row = {"symbol": "FOO", "subject": "Bonus", "ex_date": "2026-09-07"}
    result = is_guard_active("FOO", "2026-09-07", [row])
    assert result.active is True


def test_picks_the_matching_row_among_several_for_the_same_symbol():
    old_action = {"symbol": "HEG", "subject": "Dividend", "rec_date": "2026-01-15"}
    result = is_guard_active("HEG", "2026-09-07", [old_action, HEG_DEMERGER])
    assert result.active is True
    assert result.action["subject"] == "Demerger"


def test_unrelated_symbols_in_the_list_do_not_interfere():
    other = {"symbol": "TCS", "subject": "Dividend", "rec_date": "2026-09-07"}
    result = is_guard_active("HEG", "2026-09-07", [other, HEG_DEMERGER])
    assert result.active is True
    assert result.action["symbol"] == "HEG"


def test_guard_result_default_action_is_none():
    result = GuardResult(active=False)
    assert result.action is None
