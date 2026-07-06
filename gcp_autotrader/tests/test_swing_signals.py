"""Tests for the pure swing-edge predicates (domain/swing_signals.py) and their
wiring into the production scanner (services/universe_service.build_watchlist).

Two edges, validated on the 2010-2026 deep held-out OOS (walk-forward, robust
across five split boundaries — docs/SWING_EDGE_AUDIT_HANDOFF.md):
  #3       MEAN_REVERSION emits ONLY above its 200-day SMA (dip-in-uptrend).
  #7-soft  MOMENTUM gets a near-52wk-high ranking tilt added to wl_score.

Layers (mirrors test_swing_exit.py):
  1. Hand-worked unit tests pinning the gate/tilt math — including explicit
     off-by-one guards, because a one-bar slice error silently deletes the edge.
  2. Structural guards that the prod scanner actually consumes the shared
     predicates (gate at candidate construction, tilt on the MOMENTUM wl_score,
     and the winner-takes-all fallback honouring the gate). Same grep idiom as
     test_swing_multi_emission.py — a full build_watchlist simulation is
     fixture-heavy and brittle; the numeric proof lives in the fidelity-replay.
"""
from __future__ import annotations

import inspect
import re

from autotrader.domain import swing_signals as S
from autotrader.domain.swing_signals import (
    mean_reversion_above_200sma,
    near_high_tilt,
)
from autotrader.services import universe_service as us_mod


# ── #3  mean_reversion_above_200sma ──────────────────────────────────────────

def test_mr_gate_above_sma_passes():
    # 200 priors at 100, as-of close 150 -> SMA 100, 150 > 100 -> True
    assert mean_reversion_above_200sma([100.0] * 200 + [150.0]) is True


def test_mr_gate_below_sma_blocks_falling_knife():
    # as-of close 90 < SMA 100 -> the falling-knife case the gate exists to block
    assert mean_reversion_above_200sma([100.0] * 200 + [90.0]) is False


def test_mr_gate_equal_is_blocked_strict_gt():
    # close == SMA -> strict ">" -> blocked
    assert mean_reversion_above_200sma([100.0] * 200 + [100.0]) is False


def test_mr_gate_insufficient_history_fail_closed():
    # < 201 closes -> cannot form the window -> False (MR simply not emitted),
    # matching the backtest's `if j < 200: continue`.
    assert mean_reversion_above_200sma([100.0] * 200) is False        # exactly 200
    assert mean_reversion_above_200sma([100.0] * 199 + [150.0]) is False
    assert mean_reversion_above_200sma([]) is False


def test_mr_gate_201_closes_is_the_boundary():
    # 201 closes is the minimum: 200 priors + the as-of close.
    assert mean_reversion_above_200sma([100.0] * 200 + [101.0]) is True


def test_mr_gate_nonpositive_sma_blocked():
    # degenerate all-zero history -> sma200 == 0 -> blocked (not > 0)
    assert mean_reversion_above_200sma([0.0] * 201) is False


def test_mr_gate_excludes_as_of_close_from_its_own_sma():
    """Off-by-one guard: the SMA must be the 200 closes ENDING THE BAR BEFORE the
    as-of close (closes[-201:-1] == backtest range(j-200, j)), NOT a 200-window
    that includes the as-of close (closes[-200:]).

    Fixture: oldest prior = 50, next 199 priors = 100, as-of = 99.8.
      correct  SMA over [50] + [100]*199           = 19950/200 = 99.75 -> 99.8 > 99.75 -> True
      buggy    SMA over [100]*199 + [99.8] (as-of)  = 19999.8/200 = 99.999 -> 99.8 > 99.999 -> False
    A regression to the close-inclusive window flips this True -> False.
    """
    closes = [50.0] + [100.0] * 199 + [99.8]
    assert len(closes) == 201
    assert mean_reversion_above_200sma(closes) is True


# ── #7-soft  near_high_tilt ──────────────────────────────────────────────────

def test_tilt_basic_within_15pct():
    # 253 bars, peak high 100, as-of close 95 -> hi52 0.95 -> (0.95-0.85)*100 = 10
    tilt = near_high_tilt([90.0] * 252 + [95.0], [100.0] * 253)
    assert abs(tilt - 10.0) < 1e-9


def test_tilt_zero_below_floor():
    # hi52 0.50 < 0.85 -> floored to 0 (laggard gets no slot bonus)
    assert near_high_tilt([50.0] * 253, [100.0] * 253) == 0.0


def test_tilt_zero_exactly_at_floor():
    # hi52 == 0.85 -> max(0, 0) -> 0
    assert near_high_tilt([85.0] * 253, [100.0] * 253) < 1e-9


def test_tilt_at_new_52wk_high():
    # close == high (fresh high) -> hi52 1.0 -> (1.0-0.85)*100 = 15 (max bonus)
    assert abs(near_high_tilt([100.0] * 253, [100.0] * 253) - 15.0) < 1e-9


def test_tilt_insufficient_history_no_tilt():
    # < 253 bars -> 0.0 (no tilt; the momentum trade still happens unboosted),
    # matching the backtest's `h is None` path.
    assert near_high_tilt([100.0] * 252, [100.0] * 252) == 0.0


def test_tilt_weight_scales_linearly():
    closes, highs = [90.0] * 252 + [95.0], [100.0] * 253
    assert abs(near_high_tilt(closes, highs, weight=2.0) - 20.0) < 1e-9
    assert near_high_tilt(closes, highs, weight=0.0) == 0.0


def test_tilt_nonpositive_high_no_tilt():
    assert near_high_tilt([100.0] * 253, [0.0] * 253) == 0.0


def test_tilt_includes_full_253_bar_window():
    """Off-by-one guard: the 52-week high is the max over the 253 bars ENDING AT
    the as-of bar (highs[-253:] == backtest bars[j-252:j+1]), inclusive of the
    oldest bar in the window. A regression to highs[-252:] drops the oldest bar.

    Fixture: oldest high = 200 (the true peak), next 252 highs = 100, as-of close
    = 100.
      correct  max over 253 bars = 200 -> hi52 100/200 = 0.50 -> tilt 0
      buggy    max over highs[-252:] = 100 -> hi52 100/100 = 1.0 -> tilt 15
    A dropped-oldest-bar regression flips tilt 0 -> 15.
    """
    highs = [200.0] + [100.0] * 252
    closes = [100.0] * 253
    assert len(highs) == 253
    assert near_high_tilt(closes, highs) == 0.0


def test_window_constants_match_backtest():
    # Pin the windows so a constant edit can't silently shift the slice.
    assert S.SMA200_WINDOW == 200
    assert S.HIGH252_WINDOW == 252
    assert S.NEAR_HIGH_FLOOR == 0.85
    assert S.DEFAULT_TILT_WEIGHT == 1.0


# ── Structural wiring guards (build_watchlist consumes the predicates) ───────

def test_universe_imports_swing_signals():
    src = inspect.getsource(us_mod)
    assert re.search(r"from autotrader\.domain import swing_signals", src), (
        "universe_service must import the shared swing_signals module so prod "
        "and the fidelity-replay run the SAME gate/tilt code."
    )
    assert "swing_signals.mean_reversion_above_200sma(" in src
    assert "swing_signals.near_high_tilt(" in src


def test_mean_reversion_not_emitted_as_candidate():
    """MEAN_REVERSION was REMOVED from the swing roster 2026-07-03 (gross-negative
    every year 2015-2026, no validated edge). It must NOT be appended to the long
    candidate slate at all — neither unconditionally nor under a gate. The regime
    allowlist also blocks it, but not emitting keeps it off the watchlist entirely."""
    src = inspect.getsource(us_mod)
    # No active MEAN_REVERSION candidate append (commented-out lines are fine).
    for line in src.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            continue  # ignore the retained-for-history commented lines
        assert '_long_candidates.append(("MEAN_REVERSION"' not in stripped.replace(" ", ""), (
            "MEAN_REVERSION must not be an active long candidate (removed 2026-07-03)"
        )


def test_momentum_tilt_applied_to_wl_score():
    """The near-high tilt must be added to wl_score for MOMENTUM rows only
    (swing-edge #7-soft), since trading_service fills the 5-slot book by
    wl_score descending."""
    src = inspect.getsource(us_mod)
    assert 'r.get("momNearHighTilt")' in src
    assert re.search(
        r'_mom_tilt\s+if\s+_label\s*==\s*"MOMENTUM"\s+else\s+0\.0',
        src,
    ), "the tilt must apply to the MOMENTUM candidate's wl_score and no other"


def test_fallback_excludes_mean_reversion():
    """The winner-takes-all fallback (fires when nothing clears the score floor)
    must NOT be able to pick MEAN_REVERSION — it's removed from the roster
    (2026-07-03). The fallback's _setup_scores must contain only tradeable
    setups (PULLBACK / MOMENTUM), never MEAN_REVERSION."""
    src = inspect.getsource(us_mod)
    for line in src.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        assert '_setup_scores["MEAN_REVERSION"]' not in stripped.replace(" ", ""), (
            "fallback must not assign a MEAN_REVERSION score (removed 2026-07-03)"
        )
