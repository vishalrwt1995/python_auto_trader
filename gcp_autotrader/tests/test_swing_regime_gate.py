"""Tests for the swing-config per-setup regime gate.

2026-07-03 re-grind (full 2015-2026, IS/OOS-validated) config:
  MOMENTUM       → {TREND_UP, RANGE}  (RANGE added — validated cell, +₹335k standalone)
  PULLBACK       → {TREND_UP}         (PB×RANGE failed: IS-negative)
  MEAN_REVERSION → (none)             (REMOVED — gross-negative every year, no edge)
RANGE_ROTATING folds to RANGE via core4_regime() upstream, so it's covered.
Other setups (BREAKOUT, shorts, intraday) pass through this gate and are
governed by _HARD_BLOCKS instead.
"""
from __future__ import annotations

from autotrader.domain.regime_affinity import swing_setup_allowed_in_regime

_MOM_BLOCKED = ("RANGE_ROTATING", "PANIC", "TREND_DOWN", "RECOVERY")  # RANGE_ROTATING folds→RANGE before this gate
_PB_BLOCKED = ("RANGE", "RANGE_ROTATING", "PANIC", "TREND_DOWN", "RECOVERY")
_MR_ALL = ("TREND_UP", "RANGE", "RANGE_ROTATING", "RECOVERY", "PANIC", "TREND_DOWN")


def test_momentum_in_uptrend_and_range():
    assert swing_setup_allowed_in_regime("MOMENTUM", "TREND_UP")
    assert swing_setup_allowed_in_regime("MOMENTUM", "RANGE")   # 2026-07-03: RANGE cell added
    for r in _MOM_BLOCKED:
        assert not swing_setup_allowed_in_regime("MOMENTUM", r), r


def test_pullback_only_in_uptrend_bucket():
    assert swing_setup_allowed_in_regime("PULLBACK", "TREND_UP")
    for r in _PB_BLOCKED:
        assert not swing_setup_allowed_in_regime("PULLBACK", r), r


def test_mean_reversion_removed_blocked_everywhere():
    # MR removed from the roster 2026-07-03 — empty allowlist blocks all regimes.
    for r in _MR_ALL:
        assert not swing_setup_allowed_in_regime("MEAN_REVERSION", r), r


def test_other_setups_pass_through():
    # BREAKOUT / shorts / intraday labels are NOT gated here (handled by _HARD_BLOCKS).
    assert swing_setup_allowed_in_regime("BREAKOUT", "RANGE")
    assert swing_setup_allowed_in_regime("SHORT_BREAKDOWN", "PANIC")
    assert swing_setup_allowed_in_regime("AUTO", "RANGE")
    assert swing_setup_allowed_in_regime("", "TREND_UP")


def test_case_insensitive():
    assert swing_setup_allowed_in_regime("momentum", "trend_up")
    assert swing_setup_allowed_in_regime("momentum", "range")   # RANGE now allowed
    assert not swing_setup_allowed_in_regime("pullback", "range")
