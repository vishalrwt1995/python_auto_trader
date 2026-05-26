"""Tests for Phase D — tactical_trend + EARLY_TREND_UP/DOWN regimes.

Validates:
  - tactical_trend_score field exists on MarketBrainState
  - EARLY_TREND_UP / EARLY_TREND_DOWN are in MarketRegimeV2 type
  - regime_affinity has entries for new regimes
  - hard_blocks correctly include/exclude per regime
  - affinity multipliers are within sensible bounds
"""
from __future__ import annotations

from autotrader.domain.models import MarketBrainState
from autotrader.domain.regime_affinity import (
    _AFFINITY,
    _HARD_BLOCKS,
    regime_hard_blocks_strategy,
    regime_strategy_multiplier,
)


def test_tactical_trend_score_field_exists():
    """MarketBrainState must accept tactical_trend_score; defaults to 50.0 (neutral)."""
    state = MarketBrainState(asof_ts="2026-05-26T15:00:00+05:30")
    assert hasattr(state, "tactical_trend_score")
    assert state.tactical_trend_score == 50.0
    # Can override
    state2 = MarketBrainState(asof_ts="2026-05-26T15:00:00+05:30", tactical_trend_score=85.0)
    assert state2.tactical_trend_score == 85.0


def test_early_trend_regimes_in_hard_blocks():
    """EARLY_TREND_UP and EARLY_TREND_DOWN must have hard_block entries."""
    assert "EARLY_TREND_UP" in _HARD_BLOCKS
    assert "EARLY_TREND_DOWN" in _HARD_BLOCKS


def test_early_trend_regimes_in_affinity():
    """EARLY_TREND_UP and EARLY_TREND_DOWN must have affinity entries."""
    assert "EARLY_TREND_UP" in _AFFINITY
    assert "EARLY_TREND_DOWN" in _AFFINITY


def test_early_trend_up_blocks_shorts_and_breakout():
    """EARLY_TREND_UP must block all short setups + BREAKOUT (mirrors TREND_UP)."""
    blocked = _HARD_BLOCKS["EARLY_TREND_UP"]
    assert "SHORT_BREAKDOWN" in blocked
    assert "SHORT_PULLBACK" in blocked
    assert "BREAKOUT" in blocked  # still blocked pending VCP pattern detector
    assert "MORNING_FADE" in blocked
    # MOMENTUM and PULLBACK must be ALLOWED (they're the primary edge here)
    assert "MOMENTUM" not in blocked
    assert "PULLBACK" not in blocked
    assert "MEAN_REVERSION" not in blocked


def test_early_trend_down_blocks_breakout_not_shorts():
    """EARLY_TREND_DOWN must allow shorts + MORNING_FADE; block BREAKOUT only."""
    blocked = _HARD_BLOCKS["EARLY_TREND_DOWN"]
    assert "BREAKOUT" in blocked
    # Short setups must NOT be blocked (we want shorts in down-trend)
    assert "SHORT_BREAKDOWN" not in blocked
    assert "SHORT_PULLBACK" not in blocked
    # MORNING_FADE allowed (Batch H rule — fade pops in down-market)
    assert "MORNING_FADE" not in blocked


def test_early_trend_up_affinity_favours_momentum_and_pullback():
    """EARLY_TREND_UP should boost MOMENTUM and PULLBACK above 1.0."""
    assert regime_strategy_multiplier("EARLY_TREND_UP", "MOMENTUM", "BUY") >= 1.0
    assert regime_strategy_multiplier("EARLY_TREND_UP", "PULLBACK", "BUY") >= 1.0
    # SHORT_BREAKDOWN should be suppressed (counter-direction)
    assert regime_strategy_multiplier("EARLY_TREND_UP", "SHORT_BREAKDOWN", "SELL") <= 0.5


def test_early_trend_down_affinity_favours_shorts():
    """EARLY_TREND_DOWN should boost SHORT_BREAKDOWN/SHORT_PULLBACK above 1.0."""
    assert regime_strategy_multiplier("EARLY_TREND_DOWN", "SHORT_BREAKDOWN", "SELL") >= 1.0
    assert regime_strategy_multiplier("EARLY_TREND_DOWN", "SHORT_PULLBACK", "SELL") >= 1.0
    # MOMENTUM (chasing strength) suppressed
    assert regime_strategy_multiplier("EARLY_TREND_DOWN", "MOMENTUM", "BUY") <= 0.5


def test_existing_regimes_still_work():
    """Backward compat: existing regimes must keep their semantics."""
    # TREND_UP still blocks BREAKOUT (regression guard from earlier test)
    assert regime_hard_blocks_strategy("TREND_UP", "BREAKOUT")
    # RANGE blocks SHORT_BREAKDOWN
    assert regime_hard_blocks_strategy("RANGE", "SHORT_BREAKDOWN")
    # PANIC blocks BREAKOUT
    assert regime_hard_blocks_strategy("PANIC", "BREAKOUT")
    # MOMENTUM is allowed in TREND_UP
    assert not regime_hard_blocks_strategy("TREND_UP", "MOMENTUM")


def test_affinity_within_bounds():
    """All affinity multipliers must be within [0.2, 1.4] (the clip bounds)."""
    for regime, strategies in _AFFINITY.items():
        for strategy, mult in strategies.items():
            assert 0.2 <= mult <= 1.4, (
                f"affinity[{regime}][{strategy}] = {mult} outside [0.2, 1.4]"
            )


def _call_tactical_trend(regime_ctx):
    """Helper: invoke _compute_tactical_trend with a minimal mock self.

    The method uses self._norm and self._clip which are @staticmethod on
    the real class. Set them as staticmethods on the proxy too so the
    self.X(...) call doesn't try to bind self.
    """
    from autotrader.services.market_brain_service import MarketBrainService

    class _Proxy:
        # Wrap as staticmethod so 'self._norm(x, lo, hi)' doesn't bind self
        _norm = staticmethod(MarketBrainService._norm)  # type: ignore[attr-defined]
        _clip = staticmethod(MarketBrainService._clip)  # type: ignore[attr-defined]

    return MarketBrainService._compute_tactical_trend(_Proxy(), regime_ctx)  # type: ignore[arg-type]


def test_compute_tactical_trend_with_neutral_data():
    """_compute_tactical_trend should return 50.0 when ema20/ret10 are missing."""
    assert _call_tactical_trend({"daily": {}}) == 50.0
    assert _call_tactical_trend({"daily": {"close": 24000.0}}) == 50.0  # no ema20


def test_compute_tactical_trend_with_bullish_data():
    """_compute_tactical_trend should return >70 when fast EMA above slow + positive return."""
    result = _call_tactical_trend({
        "daily": {
            "close": 24500.0,
            "ema20": 24400.0,    # close above ema20
            "ema50": 24000.0,    # ema20 above ema50 (bullish spread ~1.67%)
            "ret10": 2.5,        # +2.5% over 10 days
        }
    })
    assert result > 70.0, f"strongly bullish setup should score >70, got {result}"


def test_compute_tactical_trend_with_bearish_data():
    """_compute_tactical_trend should return <30 when fast EMA below slow + negative return."""
    result = _call_tactical_trend({
        "daily": {
            "close": 23500.0,
            "ema20": 23800.0,    # close below ema20
            "ema50": 24200.0,    # ema20 below ema50 (bearish spread ~-1.65%)
            "ret10": -2.5,       # -2.5% over 10 days
        }
    })
    assert result < 30.0, f"strongly bearish setup should score <30, got {result}"
