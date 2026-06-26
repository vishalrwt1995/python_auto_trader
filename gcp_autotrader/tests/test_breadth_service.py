"""Tests for MarketBreadthService.compute_breadth_snapshot — aboveEma200Pct."""
from __future__ import annotations

from datetime import timedelta
from typing import Any

from autotrader.services.market_breadth_service import MarketBreadthService
from autotrader.time_utils import IST, now_ist


def _daily_candles(n: int, start_px: float, step: float) -> list[list[object]]:
    now_i = now_ist().astimezone(IST)
    d = now_i.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=n + 10)
    out: list[list[object]] = []
    px = start_px
    while len(out) < n:
        if d.weekday() < 5:
            o = px
            c = px + step
            h = max(o, c) + 0.4
            lo = min(o, c) - 0.4
            out.append([d.isoformat(), o, h, lo, c, 1_000_000.0])
            px = c
        d += timedelta(days=1)
    return out


_ROW = {
    "enabled": True, "fresh": True,
    "eligibleSwing": True, "eligibleIntraday": False,
    "turnoverRank60D": 1, "liquidityBucket": "A", "sector": "IT",
}


def test_above_ema200_pct_computed_correctly():
    """50% of EMA200-eligible stocks above EMA200 → aboveEma200Pct == 50.0."""
    svc = MarketBreadthService(liquidity_turnover_rank_max=500, min_bars=30)
    lcd = (now_ist() - timedelta(days=1)).strftime("%Y-%m-%d")
    rows = [dict(_ROW, symbol=s) for s in ("UP", "DOWN", "SHORT")]

    def _fetch(row: dict[str, Any], _lcd: str) -> list[list[object]]:
        if row["symbol"] == "UP":
            return _daily_candles(n=250, start_px=50.0, step=0.5)   # uptrend → above EMA200
        if row["symbol"] == "DOWN":
            return _daily_candles(n=250, start_px=200.0, step=-0.5) # downtrend → below EMA200
        return _daily_candles(n=100, start_px=100.0, step=0.3)      # <201 bars → excluded

    out = svc.compute_breadth_snapshot(universe_rows=rows, expected_lcd=lcd,
                                        daily_candle_fetcher=_fetch)
    assert out["aboveEma200Pct"] == 50.0
    assert "aboveEma200Pct" in out


def test_above_ema200_pct_zero_when_no_200bar_stocks():
    """Falls back to 0.0 (blocking/conservative) when no stock has 200+ daily bars."""
    svc = MarketBreadthService(liquidity_turnover_rank_max=500, min_bars=30)
    lcd = (now_ist() - timedelta(days=1)).strftime("%Y-%m-%d")
    rows = [dict(_ROW, symbol="X")]

    out = svc.compute_breadth_snapshot(universe_rows=rows, expected_lcd=lcd,
                                        daily_candle_fetcher=lambda r, l: _daily_candles(100, 100.0, 0.3))
    assert out["aboveEma200Pct"] == 0.0


def test_above_ema200_pct_in_empty_universe_early_return():
    """The early-return path (no qualified rows) must also include aboveEma200Pct=0.0."""
    svc = MarketBreadthService(liquidity_turnover_rank_max=500, min_bars=30)
    lcd = (now_ist() - timedelta(days=1)).strftime("%Y-%m-%d")
    out = svc.compute_breadth_snapshot(universe_rows=[], expected_lcd=lcd,
                                        daily_candle_fetcher=lambda r, l: [])
    assert "aboveEma200Pct" in out
    assert out["aboveEma200Pct"] == 0.0
