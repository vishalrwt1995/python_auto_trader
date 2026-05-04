"""Tests for `TradingService._prefetch_candles_parallel` — the perf fix
that hoisted per-symbol candle fetches out of the scan loop and parallelised
them with a ThreadPoolExecutor.

The behavioural invariants we care about:
  - all symbols' candles end up in the returned dict,
  - per-symbol failures are isolated (other symbols still complete),
  - the underlying `_fetch_candles` is called exactly once per symbol
    with the right (symbol, exchange, segment, instrument_key, timeframe,
    lookback_days) parameters,
  - empty subset → empty dict (no executor spin-up).
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from unittest.mock import MagicMock

from autotrader.services.trading_service import TradingService


@dataclass
class _W:
    symbol: str
    exchange: str = "NSE"
    segment: str = "CASH"


def _make_svc() -> TradingService:
    """Build a minimally-wired TradingService — only `_fetch_candles` is
    exercised by the prefetch path, so the rest are MagicMocks."""
    return TradingService(
        settings=MagicMock(),
        state=MagicMock(),
        gcs=MagicMock(),
        upstox=MagicMock(),
        regime_service=MagicMock(),
        market_brain_service=MagicMock(),
        order_service=MagicMock(),
        log_sink=MagicMock(),
    )


def test_prefetch_returns_one_entry_per_symbol():
    svc = _make_svc()

    def fake_fetch(symbol, exchange, segment, *, instrument_key, timeframe, lookback_days):
        return [[symbol, timeframe, 100.0]]

    svc._fetch_candles = fake_fetch  # type: ignore[assignment]

    subset = [_W("AAA"), _W("BBB"), _W("CCC")]
    key_map = {"AAA": "ik-aaa", "BBB": "ik-bbb", "CCC": "ik-ccc"}

    out = svc._prefetch_candles_parallel(
        subset, key_map, timeframe="15m", lookback_days=8, max_workers=4,
    )

    assert set(out.keys()) == {"AAA", "BBB", "CCC"}
    assert out["AAA"] == [["AAA", "15m", 100.0]]
    assert out["BBB"] == [["BBB", "15m", 100.0]]
    assert out["CCC"] == [["CCC", "15m", 100.0]]


def test_prefetch_isolates_per_symbol_failures():
    """If one symbol's fetch raises, the others must still complete."""
    svc = _make_svc()

    def fake_fetch(symbol, exchange, segment, *, instrument_key, timeframe, lookback_days):
        if symbol == "BBB":
            raise RuntimeError("upstox 500")
        return [[symbol, timeframe]]

    svc._fetch_candles = fake_fetch  # type: ignore[assignment]

    subset = [_W("AAA"), _W("BBB"), _W("CCC")]
    out = svc._prefetch_candles_parallel(
        subset, {"AAA": "ik", "BBB": "ik", "CCC": "ik"},
        timeframe="15m", lookback_days=8, max_workers=4,
    )

    assert "AAA" in out and "CCC" in out
    assert "BBB" not in out  # failed symbol is absent — caller treats as no candles


def test_prefetch_passes_through_timeframe_and_lookback():
    svc = _make_svc()
    captured: list[dict] = []

    def fake_fetch(symbol, exchange, segment, *, instrument_key, timeframe, lookback_days):
        captured.append({
            "symbol": symbol, "exchange": exchange, "segment": segment,
            "instrument_key": instrument_key, "timeframe": timeframe,
            "lookback_days": lookback_days,
        })
        return [[1]]

    svc._fetch_candles = fake_fetch  # type: ignore[assignment]

    out = svc._prefetch_candles_parallel(
        [_W("XYZ", exchange="NSE", segment="CASH")],
        {"XYZ": "NSE_EQ|INE000"},
        timeframe="1d", lookback_days=120,
    )

    assert "XYZ" in out
    assert len(captured) == 1
    assert captured[0]["timeframe"] == "1d"
    assert captured[0]["lookback_days"] == 120
    assert captured[0]["instrument_key"] == "NSE_EQ|INE000"
    assert captured[0]["exchange"] == "NSE"


def test_prefetch_empty_subset_returns_empty_dict():
    svc = _make_svc()
    svc._fetch_candles = MagicMock(side_effect=AssertionError("should not be called"))  # type: ignore[assignment]

    out = svc._prefetch_candles_parallel([], {}, timeframe="15m", lookback_days=8)
    assert out == {}


def test_prefetch_actually_runs_concurrently():
    """Sanity check the parallelism — N slow fetches should finish in
    ~max(per-fetch) wall-clock, not N×per-fetch. We use a barrier rather
    than sleep to stay deterministic."""
    svc = _make_svc()
    subset = [_W(f"S{i}") for i in range(8)]
    key_map = {w.symbol: f"ik-{w.symbol}" for w in subset}
    barrier = threading.Barrier(len(subset), timeout=2.0)

    def fake_fetch(symbol, exchange, segment, *, instrument_key, timeframe, lookback_days):
        # If they ran sequentially, only one thread would be at the barrier
        # at a time and the second arrival would block forever — the timeout
        # would raise threading.BrokenBarrierError.
        barrier.wait()
        return [[symbol]]

    svc._fetch_candles = fake_fetch  # type: ignore[assignment]

    out = svc._prefetch_candles_parallel(
        subset, key_map, timeframe="15m", lookback_days=8, max_workers=len(subset),
    )
    assert len(out) == len(subset)
