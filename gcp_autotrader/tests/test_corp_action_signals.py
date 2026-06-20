"""Unit tests for domain/corp_action_signals.py — the corp-action (bonus/split)
pre-meeting selection gate. Fast + self-contained (synthetic fixtures). The heavyweight
fidelity-replay over the real archive is a separate scratch run (scripts/redesign /
~/.autotrader_backtest_cache), gated before deploy."""
import pytest

from autotrader.domain import corp_action_signals as ca


def test_dist_above_52w_low_basic():
    closes = [100.0] * 300
    lows = [50.0] * 300
    closes[260] = 75.0
    # min(low[8:260]) = 50 -> 75/50 = 1.5
    assert ca.dist_above_52w_low(closes, lows, 260) == pytest.approx(1.5)


def test_dist_above_52w_low_short_history_clamps_to_start():
    closes = [10.0, 12.0, 9.0, 15.0]
    lows = [8.0, 9.0, 7.0, 11.0]
    # idx=3, window clamps to 0; min(low[0:3]) = 7 -> 15/7
    assert ca.dist_above_52w_low(closes, lows, 3) == pytest.approx(15.0 / 7.0)


def test_dist_above_52w_low_bad_data_returns_none():
    assert ca.dist_above_52w_low([0.0, 0.0], [0.0, 0.0], 1) is None
    assert ca.dist_above_52w_low([10.0], [5.0], 0) is None  # idx <= 0


def test_pre_entry_runup_raw():
    closes = [100.0] * 30
    closes[8] = 110.0  # base = close[29-1-20] = close[8]
    assert ca.pre_entry_runup_raw(closes, 29, lookback=20) == pytest.approx(100.0 / 110.0 - 1.0)


def test_pre_entry_runup_raw_no_history_returns_none():
    assert ca.pre_entry_runup_raw([100.0] * 5, 3, lookback=20) is None


def test_entry_is_same_day_close():
    assert ca.entry_is_same_day_close(10) is True
    assert ca.entry_is_same_day_close(13) is True
    assert ca.entry_is_same_day_close(14) is False  # not strictly before 14:00
    assert ca.entry_is_same_day_close(18) is False
    assert ca.entry_is_same_day_close(0) is False
    assert ca.entry_is_same_day_close(None) is False


def test_passes_corp_gates_happy_path():
    assert ca.passes_corp_gates("bonus", True, 1.5, 0.0, 5) is True
    assert ca.passes_corp_gates("split", True, 1.40, 0.059, 4) is True  # exact boundaries


def test_passes_corp_gates_rejects_wrong_event_type():
    assert ca.passes_corp_gates("buyback", True, 1.5, 0.0, 5) is False
    assert ca.passes_corp_gates("dividend", True, 1.5, 0.0, 5) is False
    assert ca.passes_corp_gates("merger", True, 1.5, 0.0, 5) is False


def test_passes_corp_gates_rejects_serial_repeat():
    assert ca.passes_corp_gates("bonus", False, 1.5, 0.0, 5) is False


def test_passes_corp_gates_rejects_not_uptrend():
    assert ca.passes_corp_gates("bonus", True, 1.39, 0.0, 5) is False  # below 1.40


def test_passes_corp_gates_rejects_pumped():
    assert ca.passes_corp_gates("bonus", True, 1.5, 0.06, 5) is False  # >= 0.06
    assert ca.passes_corp_gates("bonus", True, 1.5, 0.10, 5) is False


def test_passes_corp_gates_rejects_bad_lead():
    assert ca.passes_corp_gates("bonus", True, 1.5, 0.0, 3) is False   # too short
    assert ca.passes_corp_gates("bonus", True, 1.5, 0.0, 16) is False  # stale / rescheduled


def test_passes_corp_gates_fail_closed_on_none():
    assert ca.passes_corp_gates(None, True, 1.5, 0.0, 5) is False
    assert ca.passes_corp_gates("bonus", None, 1.5, 0.0, 5) is False
    assert ca.passes_corp_gates("bonus", True, None, 0.0, 5) is False
    assert ca.passes_corp_gates("bonus", True, 1.5, None, 5) is False
    assert ca.passes_corp_gates("bonus", True, 1.5, 0.0, None) is False
