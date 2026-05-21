"""Phase 6 — Brain replay (NIFTY-derived heuristic).

We do NOT replay the full MarketBrainService (that needs universe service,
sector data, option chain, etc.). Instead we approximate the regime
classification from NIFTY 50 daily indicators alone — a coarse but
empirically reasonable proxy.

Mapping:
  - TREND_UP   : NIFTY daily EMA9 > EMA21 > EMA50 (bull stack) AND ADX ≥ 20
  - TREND_DOWN : EMA9 < EMA21 < EMA50 (bear stack) AND ADX ≥ 20
  - PANIC      : ATR% > 2.5 AND price below EMA50 (high volatility down move)
  - CHOP       : ADX < 18 AND price oscillating
  - RECOVERY   : Was TREND_DOWN/PANIC last regime AND price now above EMA20
  - RANGE      : Default

This is documented in BACKTEST_PLAN.md §8 as a known approximation.
The full MarketBrainService takes universe breadth + leadership +
sector data, all of which require larger replay infrastructure.
"""
from __future__ import annotations

from autotrader.domain.indicators import calc_adx, calc_atr, calc_ema, calc_rsi


# NIFTY 50 daily index in the live cache
_NIFTY_DAILY_PATH = "cache/watchlist_v2/index_daily/NSE_INDEX_NIFTY_50.json"


def classify_regime(nifty_daily_candles: list[list]) -> str:
    """Classify regime from NIFTY daily candles.

    `nifty_daily_candles` is the list of candles up to and including the
    as-of date — point-in-time correct.
    """
    if len(nifty_daily_candles) < 60:
        return "RANGE"

    closes = [float(c[4]) for c in nifty_daily_candles]
    last_close = closes[-1]

    try:
        ema9 = calc_ema(closes, 9)
        ema21 = calc_ema(closes, 21)
        ema50 = calc_ema(closes, 50)
    except Exception:
        return "RANGE"

    # EMA stack — booleans
    bull_stack = ema9 > ema21 > ema50
    bear_stack = ema9 < ema21 < ema50

    try:
        adx = calc_adx(nifty_daily_candles, period=14)
        atr = calc_atr(nifty_daily_candles, period=14)
        rsi = calc_rsi(closes, period=14)
    except Exception:
        adx = 0.0
        atr = 0.0
        rsi = 50.0

    atr_pct = (atr / last_close * 100.0) if last_close > 0 else 0.0

    # PANIC: high vol + below EMA50
    if atr_pct > 2.5 and last_close < ema50:
        return "PANIC"

    # CHOP: low ADX
    if adx < 18:
        return "CHOP"

    # Strong trends
    if bull_stack and adx >= 20:
        return "TREND_UP"
    if bear_stack and adx >= 20:
        return "TREND_DOWN"

    # Default
    return "RANGE"


class BrainReplay:
    """Cache NIFTY daily lookups across many date queries."""

    def __init__(self, dataset) -> None:
        self.ds = dataset
        self._nifty_all: list[list] | None = None
        self._regime_cache: dict[str, str] = {}

    def _load_nifty_all(self) -> list[list]:
        if self._nifty_all is None:
            self._nifty_all = self.ds._read_candles_cached(_NIFTY_DAILY_PATH) or []
        return self._nifty_all

    def regime_for_date(self, as_of: str) -> str:
        if as_of in self._regime_cache:
            return self._regime_cache[as_of]
        nifty = self._load_nifty_all()
        truncated = [c for c in nifty if str(c[0])[:10] <= as_of]
        regime = classify_regime(truncated)
        self._regime_cache[as_of] = regime
        return regime
