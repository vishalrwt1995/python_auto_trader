"""Unit tests for the GCS candle loader.

Stubs `GoogleCloudStorageStore.read_candles` so the tests are hermetic.
This is the loader behind `RunSpec.candle_source='gcs'` (the default), so a
silent regression here would cause the backtester to no-op against an
empty universe — guard the path resolution + filtering logic explicitly.
"""
from __future__ import annotations

from typing import Any

import pytest

from autotrader.backtest import data as bt_data
from autotrader.backtest.data import (
    _aggregate_5m_to_15m,
    _filter_and_box,
    _gcs_path_for,
    load_candles_bulk_gcs,
)
from autotrader.backtest.types import Bar


# ── Path resolution ──────────────────────────────────────────────────────


def test_path_1d_uses_candles_prefix():
    """1d uses the same `cache/candles/{tf}/...` path as 5m/15m.

    2026-05-07 fix: previously 1d read from `cache/score_1d/`, but that
    is the LEGACY score-cache writer's output and the live system stopped
    refreshing it on 2026-02-27. The fresh canonical writer is
    `cache/candles/1d/` (uniform with 5m/15m), updated daily by the
    candle finalize cron. Verified: candles/1d had bars through
    2026-05-06; score_1d's last bar was 2026-02-26 (70 days stale).
    """
    assert _gcs_path_for("1d", "RELIANCE", "NSE", "CASH") == \
        "cache/candles/1d/NSE/CASH/RELIANCE.json"


def test_path_5m_uses_candles_prefix():
    """5m and 15m go to `cache/candles/{tf}/...`."""
    assert _gcs_path_for("5m", "sbin", "nse", "cash") == \
        "cache/candles/5m/NSE/CASH/SBIN.json"
    assert _gcs_path_for("15m", "INFY", "NSE", "CASH") == \
        "cache/candles/15m/NSE/CASH/INFY.json"


def test_path_unknown_timeframe_raises():
    with pytest.raises(ValueError, match="unsupported timeframe"):
        _gcs_path_for("1h", "X", "NSE", "CASH")


# ── Filter + box (raw → Bar) ─────────────────────────────────────────────


def _row(ts: str, o: float = 100.0, h: float = 101.0, lo: float = 99.0,
         c: float = 100.5, v: float = 1000.0) -> list[Any]:
    return [ts, o, h, lo, c, v]


def test_filter_and_box_keeps_in_window():
    raw = [
        _row("2026-04-15T09:15:00+05:30"),
        _row("2026-04-16T09:15:00+05:30"),
        _row("2026-04-20T15:25:00+05:30"),
        _row("2026-04-25T09:15:00+05:30"),
    ]
    bars = _filter_and_box(raw, "X", "5m", "2026-04-16", "2026-04-20")
    assert [b.ts[:10] for b in bars] == ["2026-04-16", "2026-04-20"]


def test_filter_and_box_inclusive_bounds():
    raw = [
        _row("2026-04-16T00:00:00+05:30"),  # since boundary
        _row("2026-04-20T23:59:59+05:30"),  # until boundary
    ]
    bars = _filter_and_box(raw, "X", "1d", "2026-04-16", "2026-04-20")
    assert len(bars) == 2


def test_filter_and_box_drops_malformed_rows():
    raw = [
        ["only_ts"],                                     # too short
        _row("2026-04-16T09:15:00+05:30"),               # good
        ["2026-04-16T09:15:00+05:30", "x", 1, 1, 1, 1],  # non-numeric open
        None,                                             # not a list
    ]
    bars = _filter_and_box(raw, "X", "5m", "2026-04-16", "2026-04-16")
    assert len(bars) == 1
    assert bars[0].open == 100.0


def test_filter_and_box_sorts_by_ts():
    raw = [
        _row("2026-04-16T15:00:00+05:30"),
        _row("2026-04-16T09:15:00+05:30"),
        _row("2026-04-16T11:30:00+05:30"),
    ]
    bars = _filter_and_box(raw, "X", "5m", "2026-04-16", "2026-04-16")
    assert [b.ts for b in bars] == sorted([b.ts for b in bars])


# ── 5m → 15m aggregation fallback ────────────────────────────────────────


def test_aggregate_5m_to_15m_buckets_correctly():
    """Three 5m bars in one 15m window aggregate to one bar with first-open,
    max-high, min-low, last-close, sum-volume."""
    bars = [
        Bar(symbol="X", ts="2026-04-16T09:15:00+05:30",
            open=100.0, high=101.0, low=99.5, close=100.5, volume=1000, timeframe="5m"),
        Bar(symbol="X", ts="2026-04-16T09:20:00+05:30",
            open=100.5, high=102.0, low=100.0, close=101.5, volume=2000, timeframe="5m"),
        Bar(symbol="X", ts="2026-04-16T09:25:00+05:30",
            open=101.5, high=101.8, low=99.0, close=100.0, volume=1500, timeframe="5m"),
    ]
    out = _aggregate_5m_to_15m(bars)
    assert len(out) == 1
    bar = out[0]
    assert bar.ts == "2026-04-16T09:15:00+05:30"
    assert bar.open == 100.0       # first
    assert bar.high == 102.0        # max
    assert bar.low == 99.0          # min
    assert bar.close == 100.0       # last
    assert bar.volume == 4500       # sum
    assert bar.timeframe == "15m"


def test_aggregate_5m_to_15m_splits_buckets_by_15min():
    bars = [
        Bar(symbol="X", ts="2026-04-16T09:15:00+05:30",
            open=100.0, high=101.0, low=99.0, close=100.5, volume=1000, timeframe="5m"),
        Bar(symbol="X", ts="2026-04-16T09:30:00+05:30",
            open=100.5, high=102.0, low=100.0, close=101.5, volume=2000, timeframe="5m"),
    ]
    out = _aggregate_5m_to_15m(bars)
    # Two distinct 15m buckets: 09:15 and 09:30
    assert len(out) == 2
    assert {b.ts[11:16] for b in out} == {"09:15", "09:30"}


# ── load_candles_bulk_gcs (with stubbed read_candles) ────────────────────


class _StubGCS:
    """Stand-in for GoogleCloudStorageStore — returns data from a path map."""

    def __init__(self, path_to_rows: dict[str, list[list[Any]]]) -> None:
        self._map = path_to_rows

    def read_candles(self, path: str) -> list[list[Any]]:
        return self._map.get(path, [])


def test_load_candles_bulk_gcs_dispatches_paths_per_symbol(monkeypatch):
    """Each symbol resolves to its own GCS path; rows outside [since, until]
    are filtered out; bars come back keyed by symbol."""
    rows_by_path = {
        "cache/candles/5m/NSE/CASH/RELIANCE.json": [
            _row("2026-04-15T09:15:00+05:30", o=2900.0),  # before since
            _row("2026-04-16T09:15:00+05:30", o=2901.0),
            _row("2026-04-20T15:25:00+05:30", o=2950.0),
        ],
        "cache/candles/5m/NSE/CASH/SBIN.json": [
            _row("2026-04-16T09:15:00+05:30", o=750.0),
            _row("2026-04-25T09:15:00+05:30", o=755.0),  # after until
        ],
    }
    stub = _StubGCS(rows_by_path)
    monkeypatch.setattr(
        "autotrader.adapters.gcs_store.GoogleCloudStorageStore",
        lambda **kw: stub,
    )

    out = load_candles_bulk_gcs(
        symbols=["RELIANCE", "SBIN"],
        timeframe="5m",
        since="2026-04-16",
        until="2026-04-20",
        concurrency=2,
    )
    assert set(out.keys()) == {"RELIANCE", "SBIN"}
    assert [b.ts[:10] for b in out["RELIANCE"]] == ["2026-04-16", "2026-04-20"]
    assert [b.ts[:10] for b in out["SBIN"]] == ["2026-04-16"]
    assert out["RELIANCE"][0].timeframe == "5m"


def test_load_candles_bulk_gcs_empty_symbol_list_returns_empty():
    assert load_candles_bulk_gcs(
        symbols=[], timeframe="5m", since="2026-04-16", until="2026-04-20",
    ) == {}


def test_load_candles_bulk_gcs_skips_symbol_with_no_data(monkeypatch):
    """A symbol whose GCS file is empty/missing is dropped from the result
    (not present with an empty list)."""
    rows_by_path = {
        "cache/candles/5m/NSE/CASH/HASBARS.json": [
            _row("2026-04-16T09:15:00+05:30"),
        ],
        # NOBARS — intentionally omitted
    }
    stub = _StubGCS(rows_by_path)
    monkeypatch.setattr(
        "autotrader.adapters.gcs_store.GoogleCloudStorageStore",
        lambda **kw: stub,
    )

    out = load_candles_bulk_gcs(
        symbols=["HASBARS", "NOBARS"], timeframe="5m",
        since="2026-04-16", until="2026-04-20",
    )
    assert "HASBARS" in out
    assert "NOBARS" not in out


def test_load_candles_bulk_gcs_uses_candles_path_for_1d(monkeypatch):
    """1d reads from cache/candles/1d/ (same convention as 5m/15m).

    2026-05-07 fix: was previously cache/score_1d/, but that path is the
    legacy score-cache writer and stopped refreshing 2026-02-27. The
    canonical fresh path is cache/candles/1d/. Reverting this test to the
    old path silently re-introduces 70-day-stale daily candles into every
    backtest run.
    """
    rows_by_path = {
        "cache/candles/1d/NSE/CASH/RELIANCE.json": [
            _row("2026-04-16T00:00:00+05:30", o=2900.0),
        ],
    }
    stub = _StubGCS(rows_by_path)
    monkeypatch.setattr(
        "autotrader.adapters.gcs_store.GoogleCloudStorageStore",
        lambda **kw: stub,
    )

    out = load_candles_bulk_gcs(
        symbols=["RELIANCE"], timeframe="1d",
        since="2026-04-16", until="2026-04-20",
    )
    assert "RELIANCE" in out
    assert out["RELIANCE"][0].timeframe == "1d"


def test_load_candles_bulk_gcs_15m_falls_back_to_5m_aggregation(monkeypatch):
    """When the 15m cache file is empty, the loader aggregates from 5m
    so callers get bars instead of an empty result."""
    rows_by_path = {
        # 15m cache: empty (file missing in real life)
        "cache/candles/15m/NSE/CASH/X.json": [],
        # 5m cache: three bars in one 15m bucket
        "cache/candles/5m/NSE/CASH/X.json": [
            _row("2026-04-16T09:15:00+05:30", o=100.0, h=101.0, lo=99.0, c=100.5, v=1000),
            _row("2026-04-16T09:20:00+05:30", o=100.5, h=102.0, lo=100.0, c=101.5, v=2000),
            _row("2026-04-16T09:25:00+05:30", o=101.5, h=101.8, lo=99.0, c=100.0, v=1500),
        ],
    }
    stub = _StubGCS(rows_by_path)
    monkeypatch.setattr(
        "autotrader.adapters.gcs_store.GoogleCloudStorageStore",
        lambda **kw: stub,
    )

    out = load_candles_bulk_gcs(
        symbols=["X"], timeframe="15m",
        since="2026-04-16", until="2026-04-16",
    )
    assert "X" in out
    assert len(out["X"]) == 1
    bar = out["X"][0]
    assert bar.timeframe == "15m"
    assert bar.high == 102.0
    assert bar.volume == 4500


def test_load_candles_bulk_gcs_15m_aggregation_disabled(monkeypatch):
    """When `aggregate_15m_from_5m=False`, missing 15m → empty (no fallback)."""
    rows_by_path = {
        "cache/candles/15m/NSE/CASH/X.json": [],
        "cache/candles/5m/NSE/CASH/X.json": [
            _row("2026-04-16T09:15:00+05:30"),
        ],
    }
    stub = _StubGCS(rows_by_path)
    monkeypatch.setattr(
        "autotrader.adapters.gcs_store.GoogleCloudStorageStore",
        lambda **kw: stub,
    )

    out = load_candles_bulk_gcs(
        symbols=["X"], timeframe="15m",
        since="2026-04-16", until="2026-04-16",
        aggregate_15m_from_5m=False,
    )
    assert "X" not in out


def test_load_candles_bulk_gcs_unsupported_timeframe_raises():
    with pytest.raises(ValueError, match="unsupported timeframe"):
        load_candles_bulk_gcs(
            symbols=["X"], timeframe="1h",
            since="2026-04-16", until="2026-04-16",
        )
