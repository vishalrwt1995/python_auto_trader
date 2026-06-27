"""Tests for the 2026-06 swing-config per-setup regime gate.

The validated config trades exactly three long cells, each only in its regime bucket:
  MOMENTUM / PULLBACK → uptrend bucket {TREND_UP}
  MEAN_REVERSION       → range bucket  {RANGE, RANGE_ROTATING}
Every other regime trades none of the three. Other setups (BREAKOUT, shorts,
intraday) pass through this gate and are governed by _HARD_BLOCKS instead.
"""
from __future__ import annotations

from autotrader.domain.regime_affinity import swing_setup_allowed_in_regime

_BLOCKED_FOR_LONGS = ("PANIC", "TREND_DOWN", "RECOVERY")


def test_momentum_only_in_uptrend_bucket():
    assert swing_setup_allowed_in_regime("MOMENTUM", "TREND_UP")
    for r in ("RANGE", "RANGE_ROTATING", *_BLOCKED_FOR_LONGS):
        assert not swing_setup_allowed_in_regime("MOMENTUM", r), r


def test_pullback_only_in_uptrend_bucket():
    assert swing_setup_allowed_in_regime("PULLBACK", "TREND_UP")
    for r in ("RANGE", "RANGE_ROTATING", *_BLOCKED_FOR_LONGS):
        assert not swing_setup_allowed_in_regime("PULLBACK", r), r


def test_mean_reversion_only_in_range_bucket():
    assert swing_setup_allowed_in_regime("MEAN_REVERSION", "RANGE")
    assert swing_setup_allowed_in_regime("MEAN_REVERSION", "RANGE_ROTATING")
    for r in ("TREND_UP", *_BLOCKED_FOR_LONGS):
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
