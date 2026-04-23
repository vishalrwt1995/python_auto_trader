"""Tests for M1 — 5-state exit state machine.

The FSM is a pure function of (PositionView, TickEvent, FsmConfig), so
every transition is directly assertable. These tests cover:

  * INITIAL → CONFIRMED only after the debounce elapses (prevents the
    whipsaw-breakeven trap where a 1-bar spike through 0.8R tightens the
    stop to breakeven and the immediate retest kicks us out).
  * INITIAL → CONFIRMED is not premature — a brief breach that reverts
    aborts the arming.
  * CONFIRMED → RUNNER at 2.0R.
  * CONFIRMED → LOSING when MFE pulls back ≥ 50% of peak before hitting
    2.0R.
  * RUNNER trailing ratchet — stop only moves in the favourable direction.
  * SL_HIT fires from any state and goes TERMINAL.
  * Flat timeout fires only when intraday + price inside 0.3 × ATR band.
  * SELL-side symmetry for each transition.

The tests intentionally drive the FSM directly (no ws_monitor scaffolding)
so a regression in the pure logic is caught even when the ws_monitor
wire-up is flag-gated off.
"""
from __future__ import annotations

from autotrader.domain.exit_fsm import (
    ExitState,
    FsmConfig,
    PositionView,
    TickEvent,
    transition,
)


def _fresh_view(side: str = "BUY", entry: float = 100.0, atr: float = 2.0, sl_dist: float = 2.0) -> PositionView:
    return PositionView(
        tag="TEST",
        side=side,
        entry_price=entry,
        atr=atr,
        sl_dist=sl_dist,
        is_swing=False,
        entry_epoch=1_000.0,
        state=ExitState.INITIAL,
        best_price=entry,
        peak_mfe_r=0.0,
        current_sl=entry - sl_dist,
        confirm_started_epoch=0.0,
    )


def _cfg() -> FsmConfig:
    return FsmConfig()


# ──────────────────────────────────────────────────────────────────────────
# INITIAL → CONFIRMED debounce
# ──────────────────────────────────────────────────────────────────────────


def test_initial_arms_confirm_on_first_breach_but_does_not_move_stop():
    v = _fresh_view()
    # Price rises to 1R (< confirm_mfe_r=0.8R would be true here — confirm
    # at 0.8R means ltp = entry + 0.8 × sl_dist = 101.6; use 102 to breach).
    out = transition(v, TickEvent(ltp=102.0, ts=1_000.0), _cfg())
    assert out.next_state == ExitState.INITIAL
    assert not out.sl_changed
    assert "confirm_arming" in out.events


def test_initial_to_confirmed_after_debounce():
    v = _fresh_view()
    v.confirm_started_epoch = 1_000.0   # arming already recorded
    # ts after debounce
    out = transition(v, TickEvent(ltp=102.0, ts=1_016.0), _cfg())
    assert out.next_state == ExitState.CONFIRMED
    assert out.sl_changed
    # give_back_r=0.3 → new SL = entry - 0.3 × sl_dist = 99.40 for BUY
    assert abs(out.new_sl - 99.40) < 1e-6


def test_initial_aborts_confirm_if_mfe_reverts_before_debounce():
    v = _fresh_view()
    v.confirm_started_epoch = 1_000.0
    # Price pulls back below 0.8R before debounce window elapsed.
    out = transition(v, TickEvent(ltp=100.5, ts=1_005.0), _cfg())
    assert out.next_state == ExitState.INITIAL
    assert "confirm_aborted" in out.events


def test_initial_sl_hit_goes_terminal():
    v = _fresh_view()
    out = transition(v, TickEvent(ltp=97.0, ts=1_001.0), _cfg())
    assert out.next_state == ExitState.TERMINAL
    assert out.exit_reason == "SL_HIT"


# ──────────────────────────────────────────────────────────────────────────
# CONFIRMED → RUNNER / LOSING
# ──────────────────────────────────────────────────────────────────────────


def test_confirmed_to_runner_at_2r():
    v = _fresh_view()
    v.state = ExitState.CONFIRMED
    v.current_sl = 99.40
    v.best_price = 104.0  # 2R
    v.peak_mfe_r = 2.0
    out = transition(v, TickEvent(ltp=104.0, ts=1_200.0), _cfg())
    assert out.next_state == ExitState.RUNNER
    assert out.sl_changed
    # Runner trail: best - 2 × atr = 104 - 4 = 100.0
    assert abs(out.new_sl - 100.0) < 1e-6


def test_confirmed_to_losing_on_50pct_pullback_from_peak():
    v = _fresh_view()
    v.state = ExitState.CONFIRMED
    v.current_sl = 99.40
    v.best_price = 102.8   # peak 1.4R
    v.peak_mfe_r = 1.4
    # LTP pulls back to 0.6R (peak was 1.4R → 57% pullback > 50%)
    out = transition(v, TickEvent(ltp=101.2, ts=1_200.0), _cfg())
    assert out.next_state == ExitState.LOSING
    # Tighten to 1 × ATR from LTP: 101.2 - 2 = 99.2; but since old SL (99.40)
    # is tighter for a BUY, we keep the tighter of the two.
    assert out.new_sl == 99.40


# ──────────────────────────────────────────────────────────────────────────
# RUNNER trailing
# ──────────────────────────────────────────────────────────────────────────


def test_runner_trail_ratchets_up_only():
    v = _fresh_view()
    v.state = ExitState.RUNNER
    v.best_price = 108.0
    v.peak_mfe_r = 4.0
    v.current_sl = 100.0   # from earlier ratchet
    # New high 110 → trail candidate = 110 - 4 = 106; that beats 100 so ratchet.
    out = transition(v, TickEvent(ltp=110.0, ts=1_500.0), _cfg())
    assert out.next_state == ExitState.RUNNER
    assert out.sl_changed
    assert abs(out.new_sl - 106.0) < 1e-6


def test_runner_does_not_loosen_sl_on_pullback():
    v = _fresh_view()
    v.state = ExitState.RUNNER
    v.best_price = 110.0
    v.peak_mfe_r = 5.0
    v.current_sl = 106.0
    # Price dips to 108 — trail candidate from local best (still 110) = 106.
    out = transition(v, TickEvent(ltp=108.0, ts=1_500.0), _cfg())
    # Not changed (candidate == sl, not greater).
    assert not out.sl_changed


def test_runner_sl_hit_goes_terminal():
    v = _fresh_view()
    v.state = ExitState.RUNNER
    v.best_price = 110.0
    v.peak_mfe_r = 5.0
    v.current_sl = 106.0
    out = transition(v, TickEvent(ltp=105.9, ts=1_500.0), _cfg())
    assert out.next_state == ExitState.TERMINAL
    assert out.exit_reason == "SL_HIT"


# ──────────────────────────────────────────────────────────────────────────
# Flat timeout
# ──────────────────────────────────────────────────────────────────────────


def test_flat_timeout_fires_in_confirmed_when_intraday_and_flat():
    v = _fresh_view()
    v.state = ExitState.CONFIRMED
    v.current_sl = 99.40
    # 2 hours elapsed, price within 0.3 × ATR band.
    out = transition(v, TickEvent(ltp=100.1, ts=v.entry_epoch + 120 * 60 + 1), _cfg())
    assert out.next_state == ExitState.TERMINAL
    assert out.exit_reason == "FLAT_TIMEOUT"


def test_flat_timeout_does_not_fire_for_swing():
    v = _fresh_view()
    v.is_swing = True
    v.state = ExitState.CONFIRMED
    v.current_sl = 99.40
    out = transition(v, TickEvent(ltp=100.1, ts=v.entry_epoch + 120 * 60 + 1), _cfg())
    assert out.next_state == ExitState.CONFIRMED


# ──────────────────────────────────────────────────────────────────────────
# SELL side symmetry
# ──────────────────────────────────────────────────────────────────────────


def test_sell_side_confirm_moves_sl_above_entry():
    v = _fresh_view(side="SELL", entry=100.0, atr=2.0, sl_dist=2.0)
    v.current_sl = 102.0
    v.confirm_started_epoch = 1_000.0
    # SELL MFE rises as price falls. LTP 98.4 = -0.8R.
    out = transition(v, TickEvent(ltp=98.4, ts=1_016.0), _cfg())
    assert out.next_state == ExitState.CONFIRMED
    # give_back_r=0.3 → new SL = entry + 0.3 × sl_dist = 100.6
    assert abs(out.new_sl - 100.6) < 1e-6


def test_sell_side_sl_hit_goes_terminal():
    v = _fresh_view(side="SELL", entry=100.0)
    v.current_sl = 102.0
    out = transition(v, TickEvent(ltp=102.5, ts=1_001.0), _cfg())
    assert out.next_state == ExitState.TERMINAL
    assert out.exit_reason == "SL_HIT"


# ──────────────────────────────────────────────────────────────────────────
# Determinism / replay — same inputs produce same outputs
# ──────────────────────────────────────────────────────────────────────────


def test_fsm_is_deterministic_across_repeated_calls():
    """FSM has no hidden state — two identical inputs must produce the same output."""
    v1 = _fresh_view()
    v2 = _fresh_view()
    tick = TickEvent(ltp=102.0, ts=1_000.0)
    o1 = transition(v1, tick, _cfg())
    o2 = transition(v2, tick, _cfg())
    assert o1.next_state == o2.next_state
    assert o1.sl_changed == o2.sl_changed
    assert o1.mfe_r_now == o2.mfe_r_now
    assert o1.events == o2.events
