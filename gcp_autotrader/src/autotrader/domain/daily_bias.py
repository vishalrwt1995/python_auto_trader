"""Daily bias computation for multi-timeframe confirmation.

Computes a daily-timeframe trend bias from daily candles, used by both
swing (primary signal source) and intraday (confirmation overlay that
rewards/penalises alignment with the higher-timeframe trend).
"""
from __future__ import annotations

from dataclasses import dataclass

from autotrader.domain.indicators import (
    calc_adx,
    calc_atr,
    calc_ema,
    calc_rsi,
    calc_supertrend,
    normalize_candles,
)
from autotrader.domain.models import Candle


@dataclass
class DailyBias:
    trend: str  # "UP", "DOWN", "NEUTRAL"
    strength: float  # 0-100, composite trend strength
    support: float  # nearest daily swing low
    resistance: float  # nearest daily swing high
    atr_daily: float  # daily ATR for swing SL calibration
    adx_daily: float  # daily ADX
    rsi_daily: float  # daily RSI
    supertrend_dir: int  # 1 = up, -1 = down
    ema_stack: bool  # EMA 9 > 21 > 50 on daily
    ema_flip: bool  # EMA 9 < 21 < 50 on daily


def _find_swing_levels(candles: list[Candle], lookback: int = 20) -> tuple[float, float]:
    """Find recent swing high (resistance) and swing low (support)."""
    if len(candles) < 5:
        return 0.0, 0.0
    recent = candles[-lookback:] if len(candles) >= lookback else candles
    highs = [c[2] for c in recent]
    lows = [c[3] for c in recent]
    return min(lows), max(highs)


def compute_daily_bias(daily_candles: list[list]) -> DailyBias | None:
    """Compute daily-timeframe bias from daily OHLCV candles.

    Returns None if insufficient data (< 50 daily candles).
    """
    candles = normalize_candles(daily_candles)
    if len(candles) < 50:
        return None

    closes = [c[4] for c in candles]
    n = len(closes) - 1

    # EMAs
    ema9 = calc_ema(closes, 9)
    ema21 = calc_ema(closes, 21)
    ema50 = calc_ema(closes, 50)

    ema_stack = ema9[n] > ema21[n] > ema50[n]
    ema_flip = ema9[n] < ema21[n] < ema50[n]

    # SuperTrend on daily
    _, st_dirs = calc_supertrend(candles, 10, 3.0)
    st_dir = st_dirs[n]

    # ADX and RSI on daily
    adx = calc_adx(candles, 14)
    rsi_vals = calc_rsi(closes, 14)
    rsi = rsi_vals[-1] if rsi_vals else 50.0

    # ATR for swing SL calibration
    atr = calc_atr(candles, 14)

    # Swing levels
    support, resistance = _find_swing_levels(candles, 20)

    # Compute trend direction
    bull_signals = 0
    bear_signals = 0

    # EMA stack
    if ema_stack:
        bull_signals += 3
    elif ema_flip:
        bear_signals += 3
    elif ema9[n] > ema21[n]:
        bull_signals += 1
    elif ema9[n] < ema21[n]:
        bear_signals += 1

    # SuperTrend
    if st_dir == 1:
        bull_signals += 2
    else:
        bear_signals += 2

    # Price vs EMA50
    if closes[n] > ema50[n]:
        bull_signals += 1
    else:
        bear_signals += 1

    # RSI tilt
    if rsi > 55:
        bull_signals += 1
    elif rsi < 45:
        bear_signals += 1

    # Determine trend
    if bull_signals >= bear_signals + 2:
        trend = "UP"
    elif bear_signals >= bull_signals + 2:
        trend = "DOWN"
    else:
        trend = "NEUTRAL"

    # Compute strength (0-100)
    # Factors: ADX (trend strength), EMA spread, RSI distance from 50
    ema_spread_pct = abs(ema9[n] - ema50[n]) / ema50[n] * 100 if ema50[n] > 0 else 0
    rsi_dist = abs(rsi - 50)  # 0-50 scale

    strength = 0.0
    strength += min(40, adx)  # ADX contributes up to 40
    strength += min(30, ema_spread_pct * 5)  # EMA spread contributes up to 30
    strength += min(30, rsi_dist * 0.6)  # RSI distance contributes up to 30
    strength = max(0, min(100, strength))

    return DailyBias(
        trend=trend,
        strength=round(strength, 1),
        support=round(support, 2),
        resistance=round(resistance, 2),
        atr_daily=round(atr, 4),
        adx_daily=round(adx, 2),
        rsi_daily=round(rsi, 1),
        supertrend_dir=st_dir,
        ema_stack=ema_stack,
        ema_flip=ema_flip,
    )
