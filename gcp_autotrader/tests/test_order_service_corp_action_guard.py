"""Integration tests for the corp-action guard wired into OrderService.place_exit_order.

Built after HEG (insider) was stopped out for a "loss" that was actually a demerger price
adjustment -- see domain/corp_action_guard.py for the full incident. These tests exercise the
REAL place_exit_order body (not a mock of it) against hand-rolled fakes, following this
codebase's existing convention (e.g. tests/test_instrument_keys.py) rather than a shared fixture
framework -- there is no project-wide conftest.py.
"""
from types import SimpleNamespace

from autotrader.services.order_service import OrderService

HEG_DEMERGER_ROW = {
    "symbol": "HEG", "isin": "INE545A01016", "subject": "Demerger",
    "ex_date": "2026-09-07", "rec_date": "2026-09-07",
}


class FakeState:
    def __init__(self, position: dict, calendar_rows: list[dict] | None = None, get_json_raises: bool = False):
        self._position = dict(position)
        self._calendar_rows = calendar_rows
        self._get_json_raises = get_json_raises
        self.update_calls: list[tuple[str, dict]] = []

    def get_position(self, tag):
        return dict(self._position)

    def get_json(self, collection, key):
        if self._get_json_raises:
            raise RuntimeError("firestore unavailable")
        if self._calendar_rows is None:
            return None
        return {"rows": self._calendar_rows}

    def update_position(self, tag, updates):
        self.update_calls.append((tag, updates))
        self._position.update(updates)

    def delete_paper_gtt(self, tag):
        pass


class FakeBQ:
    def __init__(self):
        self.inserted = []

    def insert_trade(self, row):
        self.inserted.append(row)


class FakeUpstox:
    def __init__(self, ltp: float):
        self._ltp = ltp

    def get_quote(self, instrument_key):
        return SimpleNamespace(ltp=self._ltp)


def _settings(*, guard_enabled: bool = True):
    return SimpleNamespace(
        strategy=SimpleNamespace(corp_action_guard_enabled=guard_enabled, paper_sl_slippage_pct=0.0),
        runtime=SimpleNamespace(paper_trade=True, use_attribution_log_v1=False),
    )


def _heg_position(**overrides):
    pos = {
        "symbol": "HEG", "side": "BUY", "qty": 29, "status": "OPEN",
        "entry_price": 679.63, "sl_price": 607.49, "product": "D", "paper": True,
        "entry_ts": "2026-08-05T09:10:06+05:30",
    }
    pos.update(overrides)
    return pos


def _make_service(position, *, calendar_rows=None, get_json_raises=False, guard_enabled=True, ltp=260.0):
    state = FakeState(position, calendar_rows=calendar_rows, get_json_raises=get_json_raises)
    return OrderService(
        settings=_settings(guard_enabled=guard_enabled),
        state=state,
        upstox=FakeUpstox(ltp),
        bq=FakeBQ(),
    ), state


def test_heg_regression_sl_hit_is_suppressed_by_an_active_demerger():
    svc, state = _make_service(_heg_position(), calendar_rows=[HEG_DEMERGER_ROW])
    result = svc.place_exit_order(position_tag="TAG1", instrument_key="NSE_EQ|INE545A01016", exit_reason="SL_HIT")
    assert result == {"skipped": "corp_action_guard", "tag": "TAG1", "action": HEG_DEMERGER_ROW}
    assert state.update_calls == []  # position must NOT have been closed


def test_sl_hit_proceeds_normally_when_no_corp_action_exists():
    svc, state = _make_service(_heg_position(), calendar_rows=[])
    result = svc.place_exit_order(position_tag="TAG2", instrument_key="NSE_EQ|INE545A01016", exit_reason="SL_HIT")
    assert result.get("skipped") != "corp_action_guard"
    assert state.update_calls  # the position WAS closed


def test_sl_hit_proceeds_when_calendar_cache_is_empty_doc():
    svc, state = _make_service(_heg_position(), calendar_rows=None)
    result = svc.place_exit_order(position_tag="TAG3", instrument_key="NSE_EQ|INE545A01016", exit_reason="SL_HIT")
    assert result.get("skipped") != "corp_action_guard"


def test_target_hit_is_never_gated_even_with_an_active_corp_action():
    svc, state = _make_service(_heg_position(), calendar_rows=[HEG_DEMERGER_ROW])
    result = svc.place_exit_order(position_tag="TAG4", instrument_key="NSE_EQ|INE545A01016", exit_reason="TARGET_HIT")
    assert result.get("skipped") != "corp_action_guard"
    assert state.update_calls


def test_manual_exit_is_never_gated_even_with_an_active_corp_action():
    svc, state = _make_service(_heg_position(), calendar_rows=[HEG_DEMERGER_ROW])
    result = svc.place_exit_order(position_tag="TAG5", instrument_key="NSE_EQ|INE545A01016", exit_reason="MANUAL_EXIT")
    assert result.get("skipped") != "corp_action_guard"


def test_max_hold_is_never_gated_even_with_an_active_corp_action():
    svc, state = _make_service(_heg_position(), calendar_rows=[HEG_DEMERGER_ROW])
    result = svc.place_exit_order(position_tag="TAG6", instrument_key="NSE_EQ|INE545A01016", exit_reason="MAX_HOLD")
    assert result.get("skipped") != "corp_action_guard"


def test_protective_sl_reason_is_gated_like_sl_hit():
    svc, state = _make_service(_heg_position(), calendar_rows=[HEG_DEMERGER_ROW])
    result = svc.place_exit_order(position_tag="TAG7", instrument_key="NSE_EQ|INE545A01016", exit_reason="PROTECTIVE_SL")
    assert result == {"skipped": "corp_action_guard", "tag": "TAG7", "action": HEG_DEMERGER_ROW}


def test_sl_breach_daily_reason_is_gated_like_sl_hit():
    svc, state = _make_service(_heg_position(), calendar_rows=[HEG_DEMERGER_ROW])
    result = svc.place_exit_order(position_tag="TAG8", instrument_key="NSE_EQ|INE545A01016", exit_reason="SWING_SL_BREACH_DAILY")
    assert result == {"skipped": "corp_action_guard", "tag": "TAG8", "action": HEG_DEMERGER_ROW}


def test_fails_open_when_the_calendar_lookup_raises():
    """The whole point of fail-open: a broken guard must never become a way to hold a real
    crash too long. Even with the exact HEG scenario, a lookup error must still let the exit
    proceed."""
    svc, state = _make_service(_heg_position(), get_json_raises=True)
    result = svc.place_exit_order(position_tag="TAG9", instrument_key="NSE_EQ|INE545A01016", exit_reason="SL_HIT")
    assert result.get("skipped") != "corp_action_guard"
    assert state.update_calls


def test_kill_switch_disables_the_guard_entirely():
    svc, state = _make_service(_heg_position(), calendar_rows=[HEG_DEMERGER_ROW], guard_enabled=False)
    result = svc.place_exit_order(position_tag="TAG10", instrument_key="NSE_EQ|INE545A01016", exit_reason="SL_HIT")
    assert result.get("skipped") != "corp_action_guard"
    assert state.update_calls


def test_guard_does_not_fire_for_a_different_symbol_sharing_the_same_calendar():
    svc, state = _make_service(_heg_position(symbol="TCS"), calendar_rows=[HEG_DEMERGER_ROW])
    result = svc.place_exit_order(position_tag="TAG11", instrument_key="NSE_EQ|SOME_OTHER", exit_reason="SL_HIT")
    assert result.get("skipped") != "corp_action_guard"
