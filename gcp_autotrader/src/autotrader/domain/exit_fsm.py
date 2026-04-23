"""M1 — 5-state exit state machine.

Replaces the 10-rank exit precedence in ws_monitor with a deterministic
state machine that every position walks through:

    INITIAL ──(MFE ≥ 0.8R for ≥ debounce_s)──► CONFIRMED
            ──(ltp ≤ sl)──► TERMINAL          (SL hit, no confirm yet)

    CONFIRMED ──(MFE ≥ 2.0R)──► RUNNER
              ──(ltp ≤ sl OR MFE drawdown ≥ 50% from peak)──► LOSING
              ──(FLAT timeout elapsed)──► TERMINAL

    RUNNER    ──(ltp ≤ trailing_sl)──► TERMINAL
              ──(EOD intraday / regime flip swing)──► TERMINAL

    LOSING    ──(ltp ≤ tightened_sl)──► TERMINAL

    TERMINAL  (absorbing — no further transitions)

Design goals:
  1. Pure data structure + pure transition function — no I/O, no logging
     from this module. ws_monitor owns the side effects (Firestore writes,
     order placement).
  2. Deterministic / replayable — given the same input stream, the state
     machine produces the same output. This is the contract the M7 replay
     tests verify against.
  3. Flag-gated rollout — settings.runtime.use_exit_fsm_v1 switches the
     monitor between the legacy precedence and this FSM. Defaults to
     legacy so deploys are a no-op until we flip the flag.
  4. Debounce on the INITIAL→CONFIRMED edge: price must sustain MFE ≥
     0.8R for ≥ 15s (`confirm_debounce_s`) before the stop is moved. This
     is what prevents the whipsaw-breakeven trap that eats the P&L in the
     legacy path (price pokes 0.8R, we tighten to entry, price retests
     entry, we stop out at breakeven, price continues in our favour).
  5. At CONFIRMED, the SL moves to (entry - 0.3R) for BUY / (entry + 0.3R)
     for SELL — not strict breakeven. A small give-back is allowed so the
     normal intraday retest of the breakout level doesn't stop us out.

No side effects are performed in this module. Consumers build an FsmInput
from the latest tick + position state, call `transition(...)`, and
interpret the returned FsmOutput to decide whether to place orders and
what to persist.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class ExitState(str, Enum):
    INITIAL = "INITIAL"
    CONFIRMED = "CONFIRMED"
    RUNNER = "RUNNER"
    LOSING = "LOSING"
    TERMINAL = "TERMINAL"


@dataclass
class FsmConfig:
    """Tunables for the exit FSM. All defaults match DESIGN.md §6."""
    # R-multiple at which we consider the position confirmed and move the stop.
    confirm_mfe_r: float = 0.8
    # Seconds price must sustain confirm_mfe_r before the stop moves (debounce).
    confirm_debounce_s: float = 15.0
    # New SL once confirmed: entry -/+ give_back_r × sl_dist.
    confirm_sl_give_back_r: float = 0.3
    # R-multiple at which we graduate to RUNNER and widen the trail.
    runner_mfe_r: float = 2.0
    # RUNNER trailing-stop distance in ATR multiples (from best_price).
    runner_trail_atr_mult: float = 2.0
    # CONFIRMED→LOSING transition: if MFE has receded this fraction from its
    # peak *and* peak MFE was below runner_mfe_r, we tighten to a 1R stop from
    # the LTP (prevents a quick winner turning into a full-R loss).
    losing_pullback_frac: float = 0.5
    # Tightened SL distance in ATR multiples once LOSING.
    losing_tighten_atr_mult: float = 1.0
    # Intraday-only flat timeout in seconds. Set to 0 to disable.
    flat_timeout_s: int = 120 * 60
    # Price must stay within this ATR fraction of entry to count as "flat".
    flat_atr_fraction: float = 0.3


@dataclass
class PositionView:
    """Minimal view of a position the FSM needs. ws_monitor hydrates this."""
    tag: str
    side: str                 # "BUY" or "SELL"
    entry_price: float
    atr: float
    sl_dist: float            # abs(entry - initial_sl) — never mutated
    is_swing: bool
    entry_epoch: float        # time.time() at entry
    # Mutable state carried across ticks (stored on the position doc).
    state: ExitState = ExitState.INITIAL
    best_price: float = 0.0
    peak_mfe_r: float = 0.0
    current_sl: float = 0.0
    # Epoch of the first tick that breached confirm_mfe_r. 0 until breach.
    confirm_started_epoch: float = 0.0


@dataclass
class TickEvent:
    ltp: float
    ts: float          # epoch seconds
    # Current market regime (e.g. "CHOP"). Used by swing-only regime tighten.
    regime: str = ""
    entry_regime: str = ""


@dataclass
class FsmOutput:
    """What the FSM decided this tick. Consumer performs the side effects."""
    next_state: ExitState
    new_sl: float = 0.0
    sl_changed: bool = False
    exit_reason: str = ""
    mfe_r_now: float = 0.0
    # Derived events useful for logging / attribution — optional flags.
    events: list[str] = field(default_factory=list)


# ──────────────────────────────────────────────────────────────────────────
# Pure helpers
# ──────────────────────────────────────────────────────────────────────────


def _mfe_r(entry: float, ltp: float, sl_dist: float, side: str) -> float:
    if sl_dist <= 0:
        return 0.0
    direction = 1.0 if side.upper() == "BUY" else -1.0
    return round((ltp - entry) * direction / sl_dist, 4)


def _crossed_sl(ltp: float, sl: float, side: str) -> bool:
    if sl <= 0:
        return False
    if side.upper() == "BUY":
        return ltp <= sl
    return ltp >= sl


# ──────────────────────────────────────────────────────────────────────────
# Transition
# ──────────────────────────────────────────────────────────────────────────


def transition(pos: PositionView, tick: TickEvent, cfg: FsmConfig) -> FsmOutput:
    """Pure transition. Caller applies FsmOutput to position + orders.

    Contract:
      * Caller must track pos.best_price / pos.peak_mfe_r / pos.state /
        pos.current_sl across calls (FSM does not mutate the input).
      * Caller must call transition() on EVERY tick; debounce uses ts
        differences.
    """
    state = pos.state
    side = pos.side.upper()
    entry = pos.entry_price
    atr = pos.atr
    sl = pos.current_sl
    best = pos.best_price or entry
    sl_dist = pos.sl_dist
    ltp = tick.ltp
    events: list[str] = []

    # Update running stats (best + peak MFE) on a local copy.
    if side == "BUY":
        best = max(best, ltp)
    else:
        best = min(best, ltp)
    # mfe_r_now represents the best-to-date excursion (peak); current_r is
    # the live R-from-entry at this tick, which can pull back from peak.
    mfe_r_now = _mfe_r(entry, best, sl_dist, side)
    current_r = _mfe_r(entry, ltp, sl_dist, side)
    peak_mfe_r = max(pos.peak_mfe_r, mfe_r_now)

    # Absorbing terminal — nothing to do.
    if state == ExitState.TERMINAL:
        return FsmOutput(next_state=state, mfe_r_now=mfe_r_now)

    # ─── SL hit — fires in any state ────────────────────────────────────
    if _crossed_sl(ltp, sl, side):
        events.append("sl_hit_from_" + state.value.lower())
        return FsmOutput(
            next_state=ExitState.TERMINAL,
            exit_reason="SL_HIT",
            new_sl=sl,
            mfe_r_now=mfe_r_now,
            events=events,
        )

    # ─── INITIAL → CONFIRMED ────────────────────────────────────────────
    if state == ExitState.INITIAL:
        # Debounce tracking: record first epoch MFE crossed the confirm level.
        if mfe_r_now >= cfg.confirm_mfe_r:
            if pos.confirm_started_epoch == 0.0:
                events.append("confirm_arming")
                return FsmOutput(
                    next_state=ExitState.INITIAL,
                    mfe_r_now=mfe_r_now,
                    events=events,
                )
            elapsed = tick.ts - pos.confirm_started_epoch
            if elapsed >= cfg.confirm_debounce_s:
                # Move SL to entry +/- give_back × sl_dist.
                direction = 1.0 if side == "BUY" else -1.0
                new_sl = round(entry - direction * sl_dist * cfg.confirm_sl_give_back_r, 2)
                events.append("confirmed_after_debounce")
                return FsmOutput(
                    next_state=ExitState.CONFIRMED,
                    new_sl=new_sl,
                    sl_changed=True,
                    mfe_r_now=mfe_r_now,
                    events=events,
                )
            events.append("confirm_debouncing")
            return FsmOutput(
                next_state=ExitState.INITIAL, mfe_r_now=mfe_r_now, events=events,
            )
        # MFE fell back below confirm threshold before debounce expired —
        # caller should reset confirm_started_epoch to 0.
        if pos.confirm_started_epoch > 0.0:
            events.append("confirm_aborted")
        return FsmOutput(
            next_state=ExitState.INITIAL, mfe_r_now=mfe_r_now, events=events,
        )

    # ─── CONFIRMED → RUNNER | LOSING ────────────────────────────────────
    if state == ExitState.CONFIRMED:
        if mfe_r_now >= cfg.runner_mfe_r:
            # Graduate to runner — stop trails 2× ATR from best.
            if atr > 0:
                direction = 1.0 if side == "BUY" else -1.0
                new_sl = round(best - direction * atr * cfg.runner_trail_atr_mult, 2)
                events.append("runner_graduation")
                return FsmOutput(
                    next_state=ExitState.RUNNER,
                    new_sl=new_sl,
                    sl_changed=new_sl != sl,
                    mfe_r_now=mfe_r_now,
                    events=events,
                )
            # No ATR — graduate but don't move the SL (legacy fallback).
            return FsmOutput(
                next_state=ExitState.RUNNER, mfe_r_now=mfe_r_now, events=events,
            )
        # Check for excessive pullback: peak MFE reached confirm but the
        # live price has receded ≥ losing_pullback_frac of that peak —
        # not runner material.
        if (
            peak_mfe_r >= cfg.confirm_mfe_r
            and peak_mfe_r < cfg.runner_mfe_r
            and peak_mfe_r > 0
            and current_r < peak_mfe_r * (1 - cfg.losing_pullback_frac)
            and atr > 0
        ):
            # Tighten to 1R from current LTP, but never loosen.
            direction = 1.0 if side == "BUY" else -1.0
            tight_sl = round(ltp - direction * atr * cfg.losing_tighten_atr_mult, 2)
            new_sl = max(tight_sl, sl) if side == "BUY" else min(tight_sl, sl) if sl > 0 else tight_sl
            events.append("losing_transition")
            return FsmOutput(
                next_state=ExitState.LOSING,
                new_sl=new_sl,
                sl_changed=new_sl != sl,
                mfe_r_now=mfe_r_now,
                events=events,
            )
        # Flat timeout on intraday — deterministic exit if we're stuck.
        if (
            not pos.is_swing
            and cfg.flat_timeout_s > 0
            and atr > 0
            and (tick.ts - pos.entry_epoch) >= cfg.flat_timeout_s
            and abs(ltp - entry) < atr * cfg.flat_atr_fraction
        ):
            events.append("flat_timeout")
            return FsmOutput(
                next_state=ExitState.TERMINAL,
                exit_reason="FLAT_TIMEOUT",
                mfe_r_now=mfe_r_now,
                events=events,
            )
        return FsmOutput(
            next_state=ExitState.CONFIRMED, mfe_r_now=mfe_r_now, events=events,
        )

    # ─── RUNNER: only the trailing stop changes ─────────────────────────
    if state == ExitState.RUNNER:
        if atr > 0:
            direction = 1.0 if side == "BUY" else -1.0
            candidate = round(best - direction * atr * cfg.runner_trail_atr_mult, 2)
            # Monotone ratchet — never loosen the trail.
            if side == "BUY":
                if candidate > sl:
                    events.append("runner_trail_ratchet")
                    return FsmOutput(
                        next_state=ExitState.RUNNER,
                        new_sl=candidate,
                        sl_changed=True,
                        mfe_r_now=mfe_r_now,
                        events=events,
                    )
            else:
                if candidate < sl or sl == 0:
                    events.append("runner_trail_ratchet")
                    return FsmOutput(
                        next_state=ExitState.RUNNER,
                        new_sl=candidate,
                        sl_changed=True,
                        mfe_r_now=mfe_r_now,
                        events=events,
                    )
        return FsmOutput(
            next_state=ExitState.RUNNER, mfe_r_now=mfe_r_now, events=events,
        )

    # ─── LOSING: stop has been tightened; no further transitions except SL ──
    if state == ExitState.LOSING:
        return FsmOutput(
            next_state=ExitState.LOSING, mfe_r_now=mfe_r_now, events=events,
        )

    # Defensive — should be unreachable.
    return FsmOutput(next_state=state, mfe_r_now=mfe_r_now)


__all__ = [
    "ExitState",
    "FsmConfig",
    "PositionView",
    "TickEvent",
    "FsmOutput",
    "transition",
]
