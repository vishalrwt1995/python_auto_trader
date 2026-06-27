"""Fix1 (2026-06): swing MR RSI threshold tightened to ≤ 35, SELL disabled.

Covers both the direction function (determine_direction) and the entry gate
(check_swing_entry) to ensure both layers enforce the new threshold consistently.
"""
from __future__ import annotations

from dataclasses import replace

import pytest

from autotrader.domain.daily_bias import DailyBias
from autotrader.domain.indicators import compute_indicators
from autotrader.domain.models import RegimeSnapshot
from autotrader.domain.scoring import check_swing_entry, determine_direction
from autotrader.settings import StrategySettings


def _candles(n: int = 130):
    rows, px = [], 200.0
    for i in range(n):
        px += 0.5
        rows.append((
            f"2025-01-{(i % 28) + 1:02d}T10:{i % 60:02d}:00+05:30",
            px - 0.2, px + 1.0, px - 0.8, px, 5000 + i * 20,
        ))
    return rows


def _ind():
    return compute_indicators(_candles(), StrategySettings())


def _daily_bias(**overrides) -> DailyBias:
    base = DailyBias(
        trend="DOWN", strength=30.0, support=0.0, resistance=0.0,
        atr_daily=5.0, adx_daily=20.0, rsi_daily=30.0,
        supertrend_dir=-1, ema_stack=False, ema_flip=True,
    )
    return replace(base, **overrides)


def _regime(r: str = "RANGE") -> RegimeSnapshot:
    return RegimeSnapshot(regime=r, bias="NEUTRAL", vix=15.0)


# ─── check_swing_entry: SELL always blocked ───────────────────────────────


def test_mr_sell_blocked_in_range():
    ok, reason = check_swing_entry(
        "MEAN_REVERSION", "SELL", _ind(), _daily_bias(rsi_daily=65.0), regime="RANGE",
    )
    assert not ok
    assert reason == "swing_mr_sell_disabled"


def test_mr_sell_blocked_in_trend_up():
    ok, reason = check_swing_entry(
        "MEAN_REVERSION", "SELL", _ind(), _daily_bias(rsi_daily=70.0), regime="TREND_UP",
    )
    assert not ok
    assert reason == "swing_mr_sell_disabled"


def test_vwap_reversal_sell_blocked():
    ok, reason = check_swing_entry(
        "VWAP_REVERSAL", "SELL", _ind(), _daily_bias(rsi_daily=65.0), regime="RANGE",
    )
    assert not ok
    assert reason == "swing_mr_sell_disabled"


# ─── check_swing_entry: BUY threshold is exactly RSI ≤ 35 ────────────────


def test_mr_buy_passes_at_rsi_35():
    ok, reason = check_swing_entry(
        "MEAN_REVERSION", "BUY", _ind(), _daily_bias(rsi_daily=35.0), regime="RANGE",
    )
    assert ok, f"RSI=35 BUY should pass; got {reason}"


def test_mr_buy_blocked_at_rsi_36():
    ok, reason = check_swing_entry(
        "MEAN_REVERSION", "BUY", _ind(), _daily_bias(rsi_daily=36.0), regime="RANGE",
    )
    assert not ok
    assert reason == "swing_mr_daily_rsi_not_oversold"


def test_mr_buy_blocked_at_rsi_44_in_range():
    """Previously RSI ≤ 45 was allowed in RANGE; Fix1 tightens to ≤ 35 universally."""
    ok, reason = check_swing_entry(
        "MEAN_REVERSION", "BUY", _ind(), _daily_bias(rsi_daily=44.0), regime="RANGE",
    )
    assert not ok
    assert reason == "swing_mr_daily_rsi_not_oversold"


def test_mr_buy_blocked_at_rsi_40_non_range():
    ok, reason = check_swing_entry(
        "MEAN_REVERSION", "BUY", _ind(), _daily_bias(rsi_daily=40.0), regime="TREND_UP",
    )
    assert not ok
    assert reason == "swing_mr_daily_rsi_not_oversold"


def test_mr_buy_passes_at_rsi_30_range():
    ok, reason = check_swing_entry(
        "MEAN_REVERSION", "BUY", _ind(), _daily_bias(rsi_daily=30.0), regime="RANGE",
    )
    assert ok, f"RSI=30 BUY in RANGE should pass; got {reason}"


def test_mr_buy_passes_at_rsi_30_trend_up():
    ok, reason = check_swing_entry(
        "MEAN_REVERSION", "BUY", _ind(), _daily_bias(rsi_daily=30.0), regime="TREND_UP",
    )
    assert ok, f"RSI=30 BUY in TREND_UP should pass; got {reason}"


# ─── determine_direction: swing MR path ──────────────────────────────────


def test_direction_swing_mr_buy_at_rsi_35():
    ind = _ind()
    d = determine_direction(
        ind, _regime("RANGE"), setup="MEAN_REVERSION", wl_type="swing",
        daily_bias=_daily_bias(rsi_daily=35.0),
    )
    assert d == "BUY"


def test_direction_swing_mr_hold_at_rsi_36():
    ind = _ind()
    d = determine_direction(
        ind, _regime("RANGE"), setup="MEAN_REVERSION", wl_type="swing",
        daily_bias=_daily_bias(rsi_daily=36.0),
    )
    assert d == "HOLD"


def test_direction_swing_mr_hold_at_rsi_44_range():
    """RSI 44 in RANGE previously returned BUY; Fix1 must return HOLD."""
    ind = _ind()
    d = determine_direction(
        ind, _regime("RANGE"), setup="MEAN_REVERSION", wl_type="swing",
        daily_bias=_daily_bias(rsi_daily=44.0),
    )
    assert d == "HOLD"


def test_direction_swing_mr_no_sell_at_rsi_60_range():
    """RSI 60 in RANGE previously returned SELL; Fix1 must return HOLD."""
    ind = _ind()
    d = determine_direction(
        ind, _regime("RANGE"), setup="MEAN_REVERSION", wl_type="swing",
        daily_bias=_daily_bias(rsi_daily=60.0),
    )
    assert d == "HOLD"


def test_direction_swing_mr_no_sell_at_rsi_70_trend_up():
    ind = _ind()
    d = determine_direction(
        ind, _regime("TREND_UP"), setup="MEAN_REVERSION", wl_type="swing",
        daily_bias=_daily_bias(rsi_daily=70.0),
    )
    assert d == "HOLD"


def test_direction_intraday_mr_unchanged_buy():
    """Intraday MR path must be unaffected: RSI 42 in RANGE → BUY."""
    ind = _ind()
    ind.rsi = type("RSI", (), {"curr": 42.0, "prev": 44.0})()
    d = determine_direction(
        ind, _regime("RANGE"), setup="MEAN_REVERSION", wl_type="intraday",
        daily_bias=None,
    )
    assert d == "BUY"


def test_direction_intraday_mr_unchanged_sell():
    """Intraday MR SELL path must be unaffected: RSI 59 in RANGE → SELL."""
    ind = _ind()
    ind.rsi = type("RSI", (), {"curr": 59.0, "prev": 57.0})()
    d = determine_direction(
        ind, _regime("RANGE"), setup="MEAN_REVERSION", wl_type="intraday",
        daily_bias=None,
    )
    assert d == "SELL"
