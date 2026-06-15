"""Exit FSM swing-awareness (2026-06 swing-config, PR3).

The live intraday exit path is domain/exit_fsm.transition (USE_EXIT_FSM_V1=true).
For SWING positions it must be a pure SL-enforcer: stay INITIAL, never move the
stop, never fire a non-SL exit (no breakeven, no 2R target, no ATR runner trail).
swing_reconciliation_service owns the daily 1R trail; the FSM only enforces the
trailed SL intraday. Intraday behaviour must be unchanged. (Root-cause fix: PR1
edited the legacy _on_quote handler, but the FSM is the active path.)
"""
from __future__ import annotations

from autotrader.domain.exit_fsm import ExitState, FsmConfig, PositionView, TickEvent, transition

CFG = FsmConfig()


def _swing(**kw):
    base = dict(tag="T", side="BUY", entry_price=100.0, atr=4.0, sl_dist=10.0,
                is_swing=True, entry_epoch=1000.0, target=0.0,
                state=ExitState.INITIAL, best_price=100.0, peak_mfe_r=0.0,
                current_sl=90.0, confirm_started_epoch=0.0)
    base.update(kw)
    return PositionView(**base)


def _tick(ltp, ts=1000.0):
    return TickEvent(ltp=ltp, ts=ts)


# ── SWING: pure SL-enforcer ──────────────────────────────────────────────────

def test_swing_big_profit_stays_initial_no_sl_change():
    # +2R MFE would CONFIRM/RUNNER an intraday trade; swing must ignore it.
    out = transition(_swing(), _tick(120.0), CFG)
    assert out.next_state == ExitState.INITIAL
    assert out.sl_changed is False
    assert out.exit_reason == ""


def test_swing_ignores_fixed_target():
    # target crossed → intraday would TARGET_HIT; swing must not.
    out = transition(_swing(target=110.0), _tick(115.0), CFG)
    assert out.next_state == ExitState.INITIAL
    assert out.exit_reason == ""


def test_swing_never_moves_to_breakeven_at_0_8R():
    # sustained +0.8R is the CONFIRMED→breakeven trigger for intraday.
    p = _swing(confirm_started_epoch=900.0)  # armed long ago → debounce satisfied
    out = transition(p, _tick(108.0, ts=1000.0), CFG)
    assert out.next_state == ExitState.INITIAL
    assert out.sl_changed is False


def test_swing_sl_hit_still_exits_long():
    out = transition(_swing(current_sl=90.0), _tick(89.0), CFG)
    assert out.next_state == ExitState.TERMINAL
    assert out.exit_reason == "SL_HIT"


def test_swing_sl_hit_still_exits_short():
    p = _swing(side="SELL", current_sl=110.0)
    out = transition(p, _tick(111.0), CFG)
    assert out.next_state == ExitState.TERMINAL
    assert out.exit_reason == "SL_HIT"


# ── INTRADAY regression: FSM behaviour unchanged ─────────────────────────────

def test_intraday_still_arms_confirm_at_0_8R():
    p = _swing(is_swing=False)  # same numbers, intraday
    out = transition(p, _tick(108.0), CFG)  # +0.8R, first touch
    assert "confirm_arming" in out.events
    assert out.next_state == ExitState.INITIAL


def test_intraday_target_still_fires():
    p = _swing(is_swing=False, target=110.0)
    out = transition(p, _tick(111.0), CFG)
    assert out.next_state == ExitState.TERMINAL
    assert out.exit_reason == "TARGET_HIT"


def test_intraday_confirms_to_breakeven_after_debounce():
    p = _swing(is_swing=False, confirm_started_epoch=900.0)
    out = transition(p, _tick(108.0, ts=1000.0), CFG)
    assert out.next_state == ExitState.CONFIRMED
    assert out.sl_changed is True
