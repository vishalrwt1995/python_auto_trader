"""Guards the overnight SL-only classification in ws_monitor: swing + the EVENT channel
(pead + corp_action) must be treated as overnight SL-only holds (FSM = pure SL-enforcer,
EOD watchdog must NOT square them off); true intraday must NOT. Regression for the
2026-06-20 fix that added pead/corp_action (previously only "swing" was exempt)."""
from autotrader.services.ws_monitor_service import _is_overnight_sl_only


def test_includes_swing_and_event_channel():
    assert _is_overnight_sl_only({"wl_type": "swing"}) is True
    assert _is_overnight_sl_only({"wl_type": "pead"}) is True
    assert _is_overnight_sl_only({"wl_type": "corp_action"}) is True


def test_excludes_intraday():
    assert _is_overnight_sl_only({"wl_type": "intraday"}) is False
    assert _is_overnight_sl_only({"wl_type": ""}) is False   # defaults to intraday
    assert _is_overnight_sl_only({}) is False                # missing -> intraday


def test_case_and_whitespace_insensitive():
    assert _is_overnight_sl_only({"wl_type": "CORP_ACTION"}) is True
    assert _is_overnight_sl_only({"wl_type": " Pead "}) is True
    assert _is_overnight_sl_only({"wl_type": "SWING"}) is True
