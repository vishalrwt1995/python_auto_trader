"""Tests for the 2026-04-22 P0 profitability fixes.

Covers:
- Brokerage round-trip helpers (risk.calc_brokerage_leg, calc_round_trip_brokerage)
- Strategy kill-switch (StrategySettings.disabled_strategies filters VWAP_REVERSAL)
- Position-sizing haircut uses min() of quality factors, not product()
"""
from __future__ import annotations

from autotrader.domain.models import MarketBrainState, PositionSizing
from autotrader.domain.risk import (
    calc_brokerage,
    calc_brokerage_leg,
    calc_round_trip_brokerage,
)
from autotrader.services.market_policy_service import MarketPolicyService
from autotrader.settings import StrategySettings


# ─── P0-1: Brokerage round-trip ──────────────────────────────────────────


def test_brokerage_leg_nonzero_for_real_trade():
    # 100 shares at ₹500 = ₹50,000 turnover
    # Expected ~20 + 12.5 + 1.61 + ~6 GST + 0.05 + 0.75 = ~40ish
    brk = calc_brokerage_leg(100, 500.0)
    assert 30.0 < brk < 60.0, f"leg brokerage out of range: {brk}"


def test_brokerage_leg_zero_for_zero_qty():
    assert calc_brokerage_leg(0, 500.0) == 0.0
    assert calc_brokerage_leg(100, 0.0) == 0.0


def test_round_trip_equals_two_legs_when_price_equal():
    # When entry == exit, round-trip equals 2× single-leg
    leg = calc_brokerage_leg(100, 500.0)
    rt_same = calc_round_trip_brokerage(100, 500.0, 500.0)
    assert abs(rt_same - (leg * 2)) < 0.02


def test_round_trip_different_prices_asymmetric():
    # Exit price higher → exit leg brokerage higher
    rt = calc_round_trip_brokerage(100, 500.0, 550.0)
    expected = calc_brokerage_leg(100, 500.0) + calc_brokerage_leg(100, 550.0)
    assert abs(rt - expected) < 0.02


def test_legacy_calc_brokerage_still_works():
    # Back-compat wrapper used at sizing time when exit unknown
    v = calc_brokerage(100, 500.0)
    assert v > 0
    # Should equal 2× single-leg at same price (within rounding)
    assert abs(v - (calc_brokerage_leg(100, 500.0) * 2)) < 0.02


def test_brokerage_eats_small_trade_edge():
    # Canary: on a ₹2000 position with a +0.5% move, brokerage should
    # already be a significant fraction of the gross P&L. This is the
    # empirical observation motivating P0-3 (no multiplicative haircut).
    qty, entry, exit = 10, 200.0, 201.0  # +₹10 gross
    gross = (exit - entry) * qty  # ₹10
    rt = calc_round_trip_brokerage(qty, entry, exit)
    # Real cost should be >10% of this tiny gross — that's the problem small
    # positions face. Asserting the cost ratio is meaningful (>5% here).
    assert rt / gross > 0.05, f"expected cost drag >5% on tiny trade, got {rt/gross:.2%}"


# ─── P0-2 / P0-4: Strategy kill-switch ───────────────────────────────────


def test_disabled_strategies_default_contains_vwap_reversal():
    cfg = StrategySettings()
    assert "VWAP_REVERSAL" in cfg.disabled_strategies, (
        "VWAP_REVERSAL must be in the default kill-list after the 2026-04-22 "
        "live P&L review (23% win rate, -0.61% avg, 12/13 EOD-closed)."
    )


def test_disabled_strategies_is_tuple_immutable():
    cfg = StrategySettings()
    # Frozen dataclass + tuple = safe default
    assert isinstance(cfg.disabled_strategies, tuple)


# ─── P0-3: min() haircut, not product() ──────────────────────────────────


def _brain_state(risk_mode: str = "NORMAL", size_multiplier: float = 1.0) -> MarketBrainState:
    """Build a minimal MarketBrainState for sizing tests."""
    return MarketBrainState(
        asof_ts="2026-04-22T10:00:00+05:30",
        phase="LIVE",
        regime="TREND_UP",
        sub_regime_v2="BASELINE",
        structure_state="ORDERLY",
        recovery_state="NONE",
        event_state="NONE",
        participation="MODERATE",
        risk_mode=risk_mode,
        intraday_state="MID",
        swing_permission="ENABLED",
        size_multiplier=size_multiplier,
        max_positions_multiplier=1.0,
        allowed_strategies=["BREAKOUT", "PULLBACK", "MEAN_REVERSION"],
        long_bias=0.5,
        short_bias=0.5,
        market_confidence=70.0,
        policy_confidence=70.0,
        data_quality_score=80.0,
    )


def _base_sizing(qty: int = 100) -> PositionSizing:
    return PositionSizing(
        qty=qty,
        sl_price=95.0,
        target=110.0,
        sl_dist=5.0,
        entry_price=100.0,
        max_loss=500.0,
        max_gain=1000.0,
        brokerage=50.0,
    )


def test_sizing_uses_min_not_product():
    """When all 3 quality factors are 0.8, min() gives 0.8× not 0.512×."""
    svc = MarketPolicyService()
    state = _brain_state(risk_mode="NORMAL", size_multiplier=1.0)
    sizing = _base_sizing(qty=100)
    out = svc.size_position_with_market_brain(
        sizing, state, StrategySettings(),
        setup_confidence_multiplier=0.8,
        liquidity_multiplier=0.8,
        data_quality_multiplier=0.8,
    )
    # With min(), expected qty ≈ 100 × 1.0 × 0.8 = 80
    # With product(), qty would be 100 × 1.0 × 0.512 = 51
    assert out.qty >= 75, f"expected ≥75 shares with min() haircut, got {out.qty}"
    assert out.qty <= 85


def test_sizing_worst_factor_still_caps():
    """The worst single factor still caps size — one bad input shouldn't be ignored."""
    svc = MarketPolicyService()
    state = _brain_state(risk_mode="NORMAL", size_multiplier=1.0)
    sizing = _base_sizing(qty=100)
    # Liquidity terrible (0.4), others perfect (1.0)
    out = svc.size_position_with_market_brain(
        sizing, state, StrategySettings(),
        setup_confidence_multiplier=1.0,
        liquidity_multiplier=0.4,
        data_quality_multiplier=1.0,
    )
    # min() = 0.4, so qty ≈ 40
    assert 35 <= out.qty <= 45, f"worst-factor cap failed: {out.qty}"


def test_sizing_size_multiplier_still_applies_globally():
    """risk_mode's size_multiplier is a global multiplier — should still scale output."""
    svc = MarketPolicyService()
    state_normal = _brain_state(risk_mode="NORMAL", size_multiplier=1.0)
    state_defensive = _brain_state(risk_mode="DEFENSIVE", size_multiplier=0.5)
    sizing = _base_sizing(qty=100)
    out_normal = svc.size_position_with_market_brain(
        sizing, state_normal, StrategySettings(),
        setup_confidence_multiplier=1.0,
        liquidity_multiplier=1.0,
        data_quality_multiplier=1.0,
    )
    out_def = svc.size_position_with_market_brain(
        sizing, state_defensive, StrategySettings(),
        setup_confidence_multiplier=1.0,
        liquidity_multiplier=1.0,
        data_quality_multiplier=1.0,
    )
    # Defensive should be roughly half of normal
    assert out_def.qty < out_normal.qty
    assert 45 <= out_def.qty <= 55, f"defensive sizing wrong: {out_def.qty}"


def test_sizing_zero_qty_preserved():
    """When risk.py set qty=0 (SL too wide), don't inflate it back."""
    svc = MarketPolicyService()
    state = _brain_state()
    sizing = _base_sizing(qty=0)
    out = svc.size_position_with_market_brain(
        sizing, state, StrategySettings(),
    )
    assert out.qty == 0, "qty=0 skip-flag from risk.py must be preserved"
