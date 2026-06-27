"""Tests for the 2026-06 swing-config per-setup regime gate.

The validated config trades three long cells:
  MOMENTUM / PULLBACK → uptrend bucket {TREND_UP}
  MEAN_REVERSION       → range/recovery bucket {RANGE, RANGE_ROTATING, RECOVERY}
Phase 4 (2026-06-27): RECOVERY added for MR — post-PANIC oversold snaps are
the sweet spot for mean reversion (affinity dict had RECOVERY → MR 0.7× already).
Other setups (BREAKOUT, shorts, intraday) pass through this gate and are
governed by _HARD_BLOCKS instead.
"""
from __future__ import annotations

from autotrader.domain.regime_affinity import swing_setup_allowed_in_regime

_TREND_BLOCKED = ("RANGE", "RANGE_ROTATING", "PANIC", "TREND_DOWN", "RECOVERY")
_MR_BLOCKED = ("TREND_UP", "PANIC", "TREND_DOWN")


def test_momentum_only_in_uptrend_bucket():
    assert swing_setup_allowed_in_regime("MOMENTUM", "TREND_UP")
    for r in _TREND_BLOCKED:
        assert not swing_setup_allowed_in_regime("MOMENTUM", r), r


def test_pullback_only_in_uptrend_bucket():
    assert swing_setup_allowed_in_regime("PULLBACK", "TREND_UP")
    for r in _TREND_BLOCKED:
        assert not swing_setup_allowed_in_regime("PULLBACK", r), r


def test_mean_reversion_in_range_and_recovery_bucket():
    for r in ("RANGE", "RANGE_ROTATING", "RECOVERY"):
        assert swing_setup_allowed_in_regime("MEAN_REVERSION", r), r
    for r in _MR_BLOCKED:
        assert not swing_setup_allowed_in_regime("MEAN_REVERSION", r), r


def test_other_setups_pass_through():
    # BREAKOUT / shorts / intraday labels are NOT gated here (handled by _HARD_BLOCKS).
    assert swing_setup_allowed_in_regime("BREAKOUT", "RANGE")
    assert swing_setup_allowed_in_regime("SHORT_BREAKDOWN", "PANIC")
    assert swing_setup_allowed_in_regime("AUTO", "RANGE")
    assert swing_setup_allowed_in_regime("", "TREND_UP")


def test_case_insensitive():
    assert swing_setup_allowed_in_regime("momentum", "trend_up")
    assert not swing_setup_allowed_in_regime("momentum", "range")
