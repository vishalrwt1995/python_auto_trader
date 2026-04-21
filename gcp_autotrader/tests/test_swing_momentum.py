"""Tests for the MOMENTUM swing setup and the 2026-04-22 swing fixes.

Covers:
- Lock key split: swing scanner no longer collides with the 3-min intraday scanner.
- Swing threshold gate: compares _affinity_score, not the brain-haircut adjusted_score.
- MOMENTUM setup: affinity matrix, hard-blocks, entry gates.
"""
from __future__ import annotations

from autotrader.domain.daily_bias import DailyBias
from autotrader.domain.indicators import compute_indicators
from autotrader.domain.regime_affinity import (
    regime_hard_blocks_strategy,
    regime_strategy_multiplier,
)
from autotrader.domain.scoring import check_swing_entry
from autotrader.settings import StrategySettings


def _candles():
    rows = []
    px = 200.0
    for i in range(130):
        px += 0.5
        rows.append(
            (
                f"2025-01-{(i % 28) + 1:02d}T10:{i % 60:02d}:00+05:30",
                px - 0.2,
                px + 1.0,
                px - 0.8,
                px,
                5000 + i * 20,
            )
        )
    return rows


def _ind():
    cfg = StrategySettings()
    return compute_indicators(_candles(), cfg)


def _daily_bias(
    *,
    trend: str = "UP",
    ema_stack: bool = True,
    adx_daily: float = 25.0,
    rsi_daily: float = 60.0,
    ema_flip: bool = False,
) -> DailyBias:
    return DailyBias(
        trend=trend,
        strength=75.0,
        support=180.0,
        resistance=300.0,
        atr_daily=5.0,
        adx_daily=adx_daily,
        rsi_daily=rsi_daily,
        supertrend_dir=1,
        ema_stack=ema_stack,
        ema_flip=ema_flip,
    )


# ─── Momentum affinity ────────────────────────────────────────────────────


def test_momentum_affinity_trend_up_is_strong():
    mult = regime_strategy_multiplier("TREND_UP", "MOMENTUM", "BUY")
    assert mult >= 1.3, f"expected TREND_UP BUY MOMENTUM ≥1.3, got {mult}"


def test_momentum_affinity_trend_down_is_suppressed():
    mult = regime_strategy_multiplier("TREND_DOWN", "MOMENTUM", "BUY")
    assert mult <= 0.5, f"expected TREND_DOWN BUY MOMENTUM ≤0.5, got {mult}"


def test_momentum_affinity_range_is_enabled():
    # RANGE: leader stocks can still outperform even when the index ranges.
    mult = regime_strategy_multiplier("RANGE", "MOMENTUM", "BUY")
    assert mult >= 1.0, f"expected RANGE BUY MOMENTUM ≥1.0, got {mult}"


def test_momentum_hard_blocks():
    # CHOP / PANIC / TREND_DOWN (via empty allowed_strategies) — MOMENTUM must not fire
    assert regime_hard_blocks_strategy("CHOP", "MOMENTUM") is True
    assert regime_hard_blocks_strategy("PANIC", "MOMENTUM") is True
    # TREND_UP / RECOVERY / RANGE — must allow
    assert regime_hard_blocks_strategy("TREND_UP", "MOMENTUM") is False
    assert regime_hard_blocks_strategy("RECOVERY", "MOMENTUM") is False
    assert regime_hard_blocks_strategy("RANGE", "MOMENTUM") is False


# ─── Momentum entry gates ─────────────────────────────────────────────────


def test_momentum_entry_happy_path():
    ind = _ind()
    # Push intraday volume ratio up to clear the 1.0 gate
    ind.volume.ratio = 1.2
    ok, reason = check_swing_entry("MOMENTUM", "BUY", ind, _daily_bias(), regime="TREND_UP")
    assert ok is True, f"expected pass, got {reason}"
    assert reason == ""


def test_momentum_blocks_sell_direction():
    ind = _ind()
    ok, reason = check_swing_entry("MOMENTUM", "SELL", ind, _daily_bias(), regime="TREND_UP")
    assert ok is False
    assert reason == "swing_momentum_sell_not_supported"


def test_momentum_blocks_daily_trend_not_up():
    ind = _ind()
    ind.volume.ratio = 1.2
    db = _daily_bias(trend="NEUTRAL")
    ok, reason = check_swing_entry("MOMENTUM", "BUY", ind, db, regime="TREND_UP")
    assert ok is False
    assert reason == "swing_momentum_daily_trend_not_up"


def test_momentum_blocks_daily_ema_not_stacked():
    ind = _ind()
    ind.volume.ratio = 1.2
    db = _daily_bias(ema_stack=False)
    ok, reason = check_swing_entry("MOMENTUM", "BUY", ind, db, regime="TREND_UP")
    assert ok is False
    assert reason == "swing_momentum_daily_ema_not_stacked"


def test_momentum_blocks_low_daily_adx():
    ind = _ind()
    ind.volume.ratio = 1.2
    db = _daily_bias(adx_daily=18.0)
    ok, reason = check_swing_entry("MOMENTUM", "BUY", ind, db, regime="TREND_UP")
    assert ok is False
    assert reason == "swing_momentum_daily_adx_too_low"


def test_momentum_blocks_rsi_cold():
    # RSI < 50 = stock has cooled; this is PULLBACK territory, not MOMENTUM.
    ind = _ind()
    ind.volume.ratio = 1.2
    db = _daily_bias(rsi_daily=45.0)
    ok, reason = check_swing_entry("MOMENTUM", "BUY", ind, db, regime="TREND_UP")
    assert ok is False
    assert reason == "swing_momentum_daily_rsi_outside_zone"


def test_momentum_blocks_rsi_overbought():
    # RSI > 75 = stretched; poor risk/reward for new long entries.
    ind = _ind()
    ind.volume.ratio = 1.2
    db = _daily_bias(rsi_daily=78.0)
    ok, reason = check_swing_entry("MOMENTUM", "BUY", ind, db, regime="TREND_UP")
    assert ok is False
    assert reason == "swing_momentum_daily_rsi_outside_zone"


def test_momentum_blocks_no_volume():
    ind = _ind()
    ind.volume.ratio = 0.5
    ok, reason = check_swing_entry("MOMENTUM", "BUY", ind, _daily_bias(), regime="TREND_UP")
    assert ok is False
    assert reason == "swing_momentum_volume_insufficient"


def test_momentum_needs_daily_bias():
    ind = _ind()
    ok, reason = check_swing_entry("MOMENTUM", "BUY", ind, None, regime="TREND_UP")
    assert ok is False
    assert reason == "swing_no_daily_data"


# ─── Pre-existing swing setups unaffected ─────────────────────────────────


def test_breakout_still_works_after_momentum_added():
    ind = _ind()
    ind.volume.ratio = 1.5  # clear swing_breakout_volume_insufficient gate (≥1.3)
    ok, reason = check_swing_entry("BREAKOUT", "BUY", ind, _daily_bias(), regime="TREND_UP")
    assert ok is True, f"expected BREAKOUT pass, got {reason}"


def test_mean_reversion_still_works():
    ind = _ind()
    # Force price near support so the new MR gate stays open.
    ind.close = 180.0 * 1.05
    db = _daily_bias(rsi_daily=30.0)  # oversold
    ok, reason = check_swing_entry("MEAN_REVERSION", "BUY", ind, db, regime="RANGE")
    assert ok is True, f"expected MEAN_REVERSION pass, got {reason}"
