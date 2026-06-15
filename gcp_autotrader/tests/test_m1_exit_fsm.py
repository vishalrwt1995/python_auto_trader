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
    # 2026-06 swing-config: the FSM is a pure SL-enforcer for swing — no
    # flat-timeout, no confirm/breakeven, no target. A flat swing past the
    # intraday timeout window stays INITIAL and exits only via the
    # reconcile-managed SL (the SL_HIT check sits above the swing guard).
    v = _fresh_view()
    v.is_swing = True
    out = transition(v, TickEvent(ltp=100.1, ts=v.entry_epoch + 120 * 60 + 1), _cfg())
    assert out.next_state == ExitState.INITIAL
    assert out.exit_reason == ""  # specifically NOT FLAT_TIMEOUT


def test_flat_timeout_fires_in_initial_state():
    """FLAT_TIMEOUT should fire from INITIAL too — covers positions that
    never reached CONFIRMED but are stuck flat past the timeout. This was
    silently broken: the check used to live in the CONFIRMED-only branch
    and was therefore unreachable for any position that never confirmed.
    """
    v = _fresh_view()
    v.state = ExitState.INITIAL
    out = transition(v, TickEvent(ltp=100.1, ts=v.entry_epoch + 120 * 60 + 1), _cfg())
    assert out.next_state == ExitState.TERMINAL
    assert out.exit_reason == "FLAT_TIMEOUT"
    assert "flat_timeout_from_initial" in out.events


def test_flat_timeout_fires_in_losing_state():
    """FLAT_TIMEOUT should also fire from LOSING — a position that briefly
    reached CONFIRMED, pulled back enough to drop into LOSING, then drifted
    flat for 2 hours should be exited rather than left to waste capital.
    """
    v = _fresh_view()
    v.state = ExitState.LOSING
    v.current_sl = 99.40
    v.peak_mfe_r = 1.0
    out = transition(v, TickEvent(ltp=100.05, ts=v.entry_epoch + 120 * 60 + 1), _cfg())
    assert out.next_state == ExitState.TERMINAL
    assert out.exit_reason == "FLAT_TIMEOUT"


def test_flat_timeout_does_not_fire_when_entry_epoch_zero():
    """Defensive: when caller forgot to populate entry_epoch (legacy path
    with a position written before entry_epoch was tracked), don't claim
    the position has been flat forever — skip the check rather than
    closing every untracked position immediately on the first tick.
    """
    v = _fresh_view()
    v.state = ExitState.INITIAL
    v.entry_epoch = 0.0
    out = transition(v, TickEvent(ltp=100.1, ts=1_700_000_000.0), _cfg())
    assert out.next_state != ExitState.TERMINAL or out.exit_reason != "FLAT_TIMEOUT"


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


# ──────────────────────────────────────────────────────────────────────────
# TARGET_HIT — bug fix 2026-04-29 (AEROFLEX regression)
#
# Without these the FSM only exits on SL or RUNNER trail; a peak that
# briefly exceeds target then pulls back ≥ 50% (still under 2R) used to
# transition CONFIRMED→LOSING and tighten SL to breakeven, missing the
# planned profit entirely.
# ──────────────────────────────────────────────────────────────────────────


def test_target_hit_from_initial_buys_terminal():
    """Spike straight to target on a single tick → TARGET_HIT, not arming."""
    v = _fresh_view()
    v.target = 103.0   # < 2R (would be 104) but valid planned target
    out = transition(v, TickEvent(ltp=103.5, ts=1_000.0), _cfg())
    assert out.next_state == ExitState.TERMINAL
    assert out.exit_reason == "TARGET_HIT"
    assert "target_hit_from_initial" in out.events


def test_target_hit_from_confirmed_terminal():
    """In CONFIRMED, an LTP that re-touches target after pulling back exits."""
    v = _fresh_view()
    v.state = ExitState.CONFIRMED
    v.current_sl = 99.40
    v.best_price = 102.5
    v.peak_mfe_r = 1.25
    v.target = 102.4
    out = transition(v, TickEvent(ltp=102.6, ts=1_200.0), _cfg())
    assert out.next_state == ExitState.TERMINAL
    assert out.exit_reason == "TARGET_HIT"
    assert "target_hit_from_confirmed" in out.events


def test_aeroflex_regression_target_hit_beats_losing_transition():
    """Repro of the actual production miss.

    Entry 100, sl_dist 2 (so 1R = 2). Target at 1.13R (= 102.26). Peak
    pushed to 1.62R (103.24) but current LTP pulls back to 0.8R (101.6),
    which is a 50% pullback from peak — the LOSING gate would normally
    fire here. But the path to peak crossed target, so the next tick that
    is at/above target must exit with TARGET_HIT, not slide to LOSING.

    We check the case where the *current* tick is at target (the realistic
    in-bar scenario where the FSM sees an at-target tick before any
    pullback can be observed).
    """
    v = _fresh_view(entry=100.0, atr=2.0, sl_dist=2.0)
    v.state = ExitState.CONFIRMED
    v.current_sl = 99.40
    v.best_price = 103.24
    v.peak_mfe_r = 1.62
    v.target = 102.26
    # Tick at the target level — TARGET_HIT must win over the LOSING check.
    out = transition(v, TickEvent(ltp=102.30, ts=1_300.0), _cfg())
    assert out.next_state == ExitState.TERMINAL
    assert out.exit_reason == "TARGET_HIT"


def test_sl_hit_still_wins_over_target_in_gap_scenario():
    """If both SL and target would trigger on the same tick (unusual gap),
    SL_HIT is emitted because risk-cap takes priority over profit lock."""
    v = _fresh_view()
    v.state = ExitState.CONFIRMED
    v.current_sl = 99.40
    v.target = 103.0
    # Construct a (degenerate) tick that pretends to satisfy both — we
    # exercise the guard ordering by setting LTP to a value below SL; the
    # SL branch must short-circuit before the target branch evaluates.
    out = transition(v, TickEvent(ltp=99.0, ts=1_200.0), _cfg())
    assert out.next_state == ExitState.TERMINAL
    assert out.exit_reason == "SL_HIT"


def test_sell_target_hit_when_ltp_falls_to_target():
    v = _fresh_view(side="SELL", entry=100.0, atr=2.0, sl_dist=2.0)
    v.current_sl = 102.0
    v.target = 98.0   # SELL target sits below entry
    out = transition(v, TickEvent(ltp=97.8, ts=1_001.0), _cfg())
    assert out.next_state == ExitState.TERMINAL
    assert out.exit_reason == "TARGET_HIT"


def test_target_zero_disables_check_backcompat():
    """Positions written before the target field carry target=0.0 and must
    fall through to legacy MFE-only behaviour (no spurious TARGET_HIT)."""
    v = _fresh_view()
    v.target = 0.0   # explicit
    out = transition(v, TickEvent(ltp=999.0, ts=1_000.0), _cfg())
    # 999 is way above any realistic price but target=0 disables the check.
    # We expect arming behaviour (or at most CONFIRMED on the next tick).
    assert out.exit_reason != "TARGET_HIT"


def test_runner_does_not_re_fire_target_hit():
    """RUNNER means we already chose to let the winner run past target.
    A re-touch of target in RUNNER must NOT exit — only the trailing SL
    governs RUNNER exits. (If it fired here, every winner would close at
    target and the RUNNER state would have no purpose.)"""
    v = _fresh_view()
    v.state = ExitState.RUNNER
    v.best_price = 110.0
    v.peak_mfe_r = 5.0
    v.current_sl = 106.0
    v.target = 103.0   # already passed long ago
    out = transition(v, TickEvent(ltp=108.0, ts=1_500.0), _cfg())
    assert out.next_state == ExitState.RUNNER
    assert out.exit_reason != "TARGET_HIT"
