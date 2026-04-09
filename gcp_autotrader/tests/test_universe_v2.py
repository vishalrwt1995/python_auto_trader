from __future__ import annotations

from datetime import date, datetime

import pytest

from autotrader.adapters.gcs_store import GoogleCloudStorageStore
from autotrader.adapters.upstox_client import UpstoxApiError
from autotrader.services.universe_service import UniverseService
from autotrader.services.universe_v2 import (
    ModeThresholds,
    CanonicalListing,
    TradabilityStats,
    UniverseControls,
    assign_turnover_rank_and_bucket,
    canonical_id_from_fields,
    choose_primary_listing,
    classify_eligibility,
    compute_tradability_stats,
)
from autotrader.settings import StrategySettings
from autotrader.time_utils import IST
from tests.fixtures_universe_v2 import (
    synthetic_candles_constant_tr,
    synthetic_candles_fixed_gap,
    synthetic_candles_linear_volume,
    synthetic_instrument_snapshot,
)


def _controls(mode: str = "BALANCED") -> UniverseControls:
    return UniverseControls(
        mode=mode,
        min_bars_hard=90,
        min_price_hard=20.0,
        max_gap_risk_hard=0.10,
        max_atr_pct_hard=0.20,
        stale_days_max=5,
        mode_thresholds={
            "CONSERVATIVE": ModeThresholds(
                swing_topn_turnover_60d=500,
                intraday_topn_turnover_60d=250,
                min_bars_swing=252,
                min_bars_intraday=320,
                min_price_mode=50.0,
                max_atr_pct_swing=0.08,
                max_atr_pct_intraday=0.06,
                max_gap_risk_mode=0.04,
            ),
            "BALANCED": ModeThresholds(
                swing_topn_turnover_60d=1000,
                intraday_topn_turnover_60d=500,
                min_bars_swing=180,
                min_bars_intraday=252,
                min_price_mode=30.0,
                max_atr_pct_swing=0.12,
                max_atr_pct_intraday=0.09,
                max_gap_risk_mode=0.06,
            ),
            "AGGRESSIVE": ModeThresholds(
                swing_topn_turnover_60d=1500,
                intraday_topn_turnover_60d=800,
                min_bars_swing=120,
                min_bars_intraday=180,
                min_price_mode=20.0,
                max_atr_pct_swing=0.16,
                max_atr_pct_intraday=0.12,
                max_gap_risk_mode=0.08,
            ),
        },
    )


def _make_calendar_service(*, holiday_rows=None, by_date_rows=None, fail_all=False, gcs_json=None):
    class FakeGcs:
        def __init__(self):
            self._obj = dict(gcs_json or {})

        def read_json(self, path, default=None):
            return self._obj.get(path, default)

        def write_json(self, path, data):
            self._obj[path] = data

    class FakeUpstox:
        def get_market_holidays(self, date=None):
            if fail_all:
                raise UpstoxApiError("forced_holiday_api_failure")
            if date is not None:
                return list((by_date_rows or {}).get(str(date), []))
            return list(holiday_rows or [])

    return UniverseService(FakeGcs(), FakeUpstox(), StrategySettings())


def test_expected_lcd_normal_weekday_holiday_aware():
    svc = _make_calendar_service(holiday_rows=[])
    now = datetime(2026, 3, 4, 8, 0, tzinfo=IST)  # Wednesday
    ctx = svc._expected_lcd_context(now)
    assert ctx["expectedLCD"] == "2026-03-03"
    assert ctx["todayTradingDay"] is True
    assert ctx["method"] == "holiday-aware"


def test_expected_lcd_weekend_holiday_aware():
    svc = _make_calendar_service(holiday_rows=[])
    now = datetime(2026, 3, 1, 8, 0, tzinfo=IST)  # Sunday
    ctx = svc._expected_lcd_context(now)
    assert ctx["expectedLCD"] == "2026-02-27"
    assert ctx["todayTradingDay"] is False
    assert ctx["method"] == "holiday-aware"


def test_expected_lcd_single_weekday_holiday():
    rows = [
        {
            "date": "2026-01-26",
            "holiday_type": "TRADING_HOLIDAY",
            "closed_exchanges": ["NSE", "BSE"],
        }
    ]
    svc = _make_calendar_service(holiday_rows=rows)
    now = datetime(2026, 1, 27, 8, 0, tzinfo=IST)
    ctx = svc._expected_lcd_context(now)
    assert ctx["expectedLCD"] == "2026-01-23"
    candles = [["2026-01-23T00:00:00+05:30", 100.0, 101.0, 99.0, 100.0, 10000.0]]
    assert svc._daily_cache_is_current(candles, now=now) is True


def test_expected_lcd_consecutive_holidays():
    rows = [
        {"date": "2026-03-02", "holiday_type": "TRADING_HOLIDAY", "closed_exchanges": ["NSE"]},
        {"date": "2026-03-03", "holiday_type": "TRADING_HOLIDAY", "closed_exchanges": ["NSE"]},
    ]
    svc = _make_calendar_service(holiday_rows=rows)
    now = datetime(2026, 3, 4, 8, 0, tzinfo=IST)
    ctx = svc._expected_lcd_context(now)
    assert ctx["expectedLCD"] == "2026-02-27"


def test_expected_lcd_uses_stored_holiday_calendar_when_available():
    cache_path = UniverseService._holiday_calendar_cache_path(2026)
    svc = _make_calendar_service(
        fail_all=True,
        gcs_json={
            cache_path: {
                "year": 2026,
                "dates": ["2026-03-02"],
                "source": "seeded",
            }
        },
    )
    now = datetime(2026, 3, 3, 8, 0, tzinfo=IST)
    ctx = svc._expected_lcd_context(now)
    assert ctx["expectedLCD"] == "2026-02-27"
    assert ctx["method"] == "holiday-aware"


def test_year_boundary_holiday_uses_stored_calendar_first():
    cache_path_2025 = UniverseService._holiday_calendar_cache_path(2025)
    svc = _make_calendar_service(
        holiday_rows=[],
        gcs_json={
            cache_path_2025: {
                "year": 2025,
                "dates": ["2025-12-31"],
                "source": "seeded",
            }
        },
    )
    now = datetime(2026, 1, 1, 8, 0, tzinfo=IST)
    ctx = svc._expected_lcd_context(now)
    assert ctx["expectedLCD"] == "2025-12-30"
    assert ctx["method"] == "holiday-aware"


def test_expected_lcd_year_boundary_with_previous_year_holiday_probe():
    by_date_rows = {
        "2025-12-31": [
            {
                "date": "2025-12-31",
                "holiday_type": "TRADING_HOLIDAY",
                "closed_exchanges": ["NSE"],
            }
        ]
    }
    svc = _make_calendar_service(holiday_rows=[], by_date_rows=by_date_rows)
    now = datetime(2026, 1, 1, 9, 0, tzinfo=IST)
    ctx = svc._expected_lcd_context(now)
    assert ctx["expectedLCD"] == "2025-12-30"


def test_expected_lcd_fallback_weekend_when_holiday_api_fails():
    svc = _make_calendar_service(fail_all=True)
    now = datetime(2026, 1, 27, 8, 0, tzinfo=IST)
    ctx = svc._expected_lcd_context(now)
    # Weekend-only fallback does not know Jan-26 holiday.
    assert ctx["expectedLCD"] == "2026-01-26"
    assert ctx["method"] == "fallback-weekend"


def test_canonical_dedupe_prefers_nse_primary():
    rows = []
    for r in synthetic_instrument_snapshot():
        canonical = canonical_id_from_fields(str(r.get("isin") or ""), str(r.get("exchange") or ""), str(r.get("trading_symbol") or ""))
        rows.append(
            {
                "canonical_id": canonical,
                "symbol": str(r.get("trading_symbol")),
                "exchange": str(r.get("exchange")),
                "instrument_key": str(r.get("instrument_key")),
                "source_segment": str(r.get("segment")),
                "security_type": str(r.get("security_type")),
                "isin": str(r.get("isin")),
                "name": str(r.get("name")),
            }
        )

    abc_rows = [r for r in rows if r["canonical_id"] == "INE000A01001"]
    listing = choose_primary_listing(abc_rows)
    assert listing is not None
    assert listing.primary_exchange == "NSE"
    assert listing.primary_instrument_key == "NSE_EQ|ABC"
    assert listing.secondary_exchange == "BSE"
    assert listing.secondary_instrument_key == "BSE_EQ|ABC"


def test_turnover_median_60d_computation():
    candles = synthetic_candles_linear_volume(n=120, close=100.0)
    stats = compute_tradability_stats(candles)
    assert stats.bars_1d == 120
    assert stats.price_last == 100.0
    # Last 60 volumes are 61..120; median is 90.5 and turnover uses close*volume.
    assert stats.turnover_med_60d == pytest.approx(9050.0, rel=1e-6)


def test_atr_pct_14d_computation():
    candles = synthetic_candles_constant_tr(n=40, close=100.0)
    stats = compute_tradability_stats(candles)
    assert stats.atr_14 == pytest.approx(2.0, rel=1e-6)
    assert stats.atr_pct_14d == pytest.approx(0.02, rel=1e-6)


def test_gap_risk_60d_computation():
    candles = synthetic_candles_fixed_gap(n=80, close=100.0, gap=0.01)
    stats = compute_tradability_stats(candles)
    assert stats.gap_risk_60d == pytest.approx(0.01, rel=1e-6)


def test_eligibility_mode_switch_changes_outcome():
    stats = TradabilityStats(
        bars_1d=260,
        price_last=120.0,
        turnover_med_60d=1_000_000.0,
        atr_14=8.0,
        atr_pct_14d=0.065,
        gap_risk_60d=0.03,
        turnover_rank_60d=700,
        liquidity_bucket="B",
    )
    balanced = classify_eligibility(stats=stats, data_quality_flag="FRESH", stale_days=0, controls=_controls("BALANCED"))
    aggressive = classify_eligibility(stats=stats, data_quality_flag="FRESH", stale_days=0, controls=_controls("AGGRESSIVE"))
    conservative = classify_eligibility(stats=stats, data_quality_flag="FRESH", stale_days=0, controls=_controls("CONSERVATIVE"))

    assert conservative.eligible_swing is False
    assert conservative.disable_reason == "SWING_TOPN_FAIL"
    assert balanced.eligible_swing is True and balanced.eligible_intraday is False
    assert balanced.disable_reason == "INTRADAY_TOPN_FAIL"
    assert aggressive.eligible_swing is True and aggressive.eligible_intraday is True


def test_eligibility_hard_failures_are_deterministic():
    stats = TradabilityStats(
        bars_1d=300,
        price_last=10.0,
        turnover_med_60d=500_000.0,
        atr_14=5.0,
        atr_pct_14d=0.05,
        gap_risk_60d=0.02,
        turnover_rank_60d=50,
        liquidity_bucket="A",
    )
    out = classify_eligibility(stats=stats, data_quality_flag="FRESH", stale_days=0, controls=_controls("BALANCED"))
    assert out.eligible_swing is False and out.eligible_intraday is False
    assert out.disable_reason == "PRICE_LT_MIN_HARD"


def test_raw_snapshot_fallback_does_not_write_latest_on_empty_decode():
    class FakeGcs:
        def __init__(self):
            self.writes: list[tuple[str, bytes | str]] = []

        @staticmethod
        def upstox_raw_universe_versioned_path(run_date: str, run_stamp: str | None = None) -> str:  # pragma: no cover - signature parity
            return "raw/versioned.gz"

        @staticmethod
        def upstox_raw_universe_latest_path() -> str:
            return "raw/latest.gz"

        @staticmethod
        def upstox_raw_universe_latest_meta_path() -> str:
            return "raw/meta.json"

        def write_bytes(self, path: str, data: bytes, content_type: str = "application/octet-stream") -> None:
            self.writes.append((path, data))

        def write_json(self, path: str, data: object) -> None:
            self.writes.append((path, str(data)))

    class FakeUpstox:
        class settings:
            instruments_complete_url = "https://example.invalid/complete.json.gz"

        @staticmethod
        def fetch_instruments_complete_gz() -> bytes:
            return b"{}"

        @staticmethod
        def decode_instruments_gz_json(blob: bytes) -> list[dict[str, object]]:
            return []

    svc = UniverseService(FakeGcs(), FakeUpstox(), StrategySettings())
    with pytest.raises(RuntimeError):
        svc.refresh_raw_universe_from_upstox()
    assert svc.gcs.writes == []


def _make_universe_row(symbol: str, exchange: str, segment: str, enabled: str, notes: str, instrument_key: str, canonical_id: str) -> list[Any]:
    """Build a sheet-style row using _UNIVERSE_COL positions (0-indexed: col-1)."""
    row: list[Any] = [""] * 38
    row[1] = symbol       # Symbol col 2 → index 1
    row[2] = exchange     # Exchange col 3 → index 2
    row[3] = segment      # Segment col 4 → index 3
    row[8] = enabled      # Enabled col 9 → index 8
    row[10] = notes       # Notes col 11 → index 10
    row[15] = instrument_key  # Instrument Key col 16 → index 15
    row[18] = canonical_id    # Canonical ID col 19 → index 18
    return row


def test_stale_candle_handling_marks_row_stale():
    universe_row = _make_universe_row("ABC", "NSE", "CASH", "Y", "isin=INE000A01001", "NSE_EQ|ABC", "INE000A01001")

    class FakeGcs:
        def __init__(self):
            self.path = GoogleCloudStorageStore.score_cache_1d_path_by_instrument_key("NSE_EQ|ABC", "NSE", "CASH")
            self.cache = {
                self.path: [["2024-12-31T00:00:00+05:30", 100.0, 101.0, 99.0, 100.0, 10000.0]],
            }

        @staticmethod
        def score_cache_1d_path(symbol: str, exchange: str, segment: str) -> str:
            return GoogleCloudStorageStore.score_cache_1d_path(symbol, exchange, segment)

        @staticmethod
        def score_cache_1d_path_by_instrument_key(instrument_key: str, exchange: str, segment: str) -> str:
            return GoogleCloudStorageStore.score_cache_1d_path_by_instrument_key(instrument_key, exchange, segment)

        def read_candles(self, path: str) -> list[list[object]]:
            return list(self.cache.get(path, []))

        def write_candles(self, path: str, candles: list[list[object]]) -> None:
            self.cache[path] = [list(c) for c in candles]

        def merge_candles(self, path: str, candles: list[list[object]]) -> list[list[object]]:
            existing = self.cache.get(path, [])
            by_ts = {str(c[0]): list(c) for c in existing}
            for c in candles:
                by_ts[str(c[0])] = list(c)
            merged = [by_ts[k] for k in sorted(by_ts.keys())]
            self.cache[path] = merged
            return merged

    class FakeUpstox:
        pass

    svc = UniverseService(FakeGcs(), FakeUpstox(), StrategySettings())
    svc._load_universe_rows_from_firestore = lambda: [list(universe_row)]  # type: ignore[method-assign]
    out = svc._update_universe_v2_cache_and_stats(api_cap=0, run_full_backfill=False)
    q = out["qualityByCanonical"]["INE000A01001"]
    assert q["data_quality_flag"] == "STALE"
    assert out["summary"]["stale"] == 1


def test_universe_v2_cache_recompute_can_skip_history_index_write():
    universe_row = _make_universe_row("ABC", "NSE", "CASH", "Y", "isin=INE000A01001", "NSE_EQ|ABC", "INE000A01001")

    class FakeGcs:
        def __init__(self):
            self.path = GoogleCloudStorageStore.score_cache_1d_path_by_instrument_key("NSE_EQ|ABC", "NSE", "CASH")
            self.cache = {
                self.path: [["2024-12-31T00:00:00+05:30", 100.0, 101.0, 99.0, 100.0, 10000.0]],
            }

        @staticmethod
        def score_cache_1d_path(symbol: str, exchange: str, segment: str) -> str:
            return GoogleCloudStorageStore.score_cache_1d_path(symbol, exchange, segment)

        @staticmethod
        def score_cache_1d_path_by_instrument_key(instrument_key: str, exchange: str, segment: str) -> str:
            return GoogleCloudStorageStore.score_cache_1d_path_by_instrument_key(instrument_key, exchange, segment)

        def read_candles(self, path: str) -> list[list[object]]:
            return list(self.cache.get(path, []))

        def write_candles(self, path: str, candles: list[list[object]]) -> None:
            self.cache[path] = [list(c) for c in candles]

        def merge_candles(self, path: str, candles: list[list[object]]) -> list[list[object]]:
            existing = self.cache.get(path, [])
            by_ts = {str(c[0]): list(c) for c in existing}
            for c in candles:
                by_ts[str(c[0])] = list(c)
            merged = [by_ts[k] for k in sorted(by_ts.keys())]
            self.cache[path] = merged
            return merged

    class FakeUpstox:
        pass

    svc = UniverseService(FakeGcs(), FakeUpstox(), StrategySettings())
    svc._load_universe_rows_from_firestore = lambda: [list(universe_row)]  # type: ignore[method-assign]
    out = svc._update_universe_v2_cache_and_stats(api_cap=0, run_full_backfill=False, write_history_index=False)
    assert out["summary"]["stale"] == 1


def test_prefetch_stale_retry_terminalizes_api_cap_blocked_source():
    class FakeGcs:
        pass

    class FakeUpstox:
        pass

    svc = UniverseService(FakeGcs(), FakeUpstox(), StrategySettings())
    candles = [["2026-02-27T00:00:00+05:30", 100.0, 101.0, 99.0, 100.0, 10000.0]]
    prev_row = {
        "status": "STALE_READY",
        "expectedlcd": "2026-02-28",
        "last_candle_time": "2026-02-27 00:00:00",
        "src": "gcs_score_cache_1d_stale_api_cap_blocked",
    }
    assert svc._prefetch_should_skip_stale_retry(prev_row, candles, expected_lcd="2026-02-28") is True


def test_prefetch_stale_retry_allows_retry_after_terminal_skip_status():
    class FakeGcs:
        pass

    class FakeUpstox:
        pass

    svc = UniverseService(FakeGcs(), FakeUpstox(), StrategySettings())
    candles = [["2026-02-27T00:00:00+05:30", 100.0, 101.0, 99.0, 100.0, 10000.0]]
    prev_row = {
        "status": "STALE_SKIPPED",
        "expectedlcd": "2026-02-28",
        "last_candle_time": "2026-02-27 00:00:00",
        "src": "gcs_score_cache_1d_stale_terminal",
    }
    assert svc._prefetch_should_skip_stale_retry(prev_row, candles, expected_lcd="2026-02-28") is False


def test_prefetch_missing_retry_terminalizes_api_cap_blocked_source():
    class FakeGcs:
        pass

    class FakeUpstox:
        pass

    svc = UniverseService(FakeGcs(), FakeUpstox(), StrategySettings())
    candles: list[list[object]] = []
    prev_row = {
        "status": "MISSING",
        "expectedlcd": "2026-02-28",
        "last_candle_time": "",
        "src": "api_cap_blocked",
    }
    assert svc._prefetch_should_skip_missing_retry(prev_row, candles, expected_lcd="2026-02-28") is True


def test_intraday_prefetch_stale_retry_terminalizes_empty_fetch_source():
    class FakeGcs:
        pass

    class FakeUpstox:
        pass

    svc = UniverseService(FakeGcs(), FakeUpstox(), StrategySettings())
    candles = [["2026-02-27T15:25:00+05:30", 100.0, 101.0, 99.0, 100.0, 10000.0]]
    prev_row = {
        "status": "STALE_READY",
        "expectedlcd": "2026-02-28",
        "last_candle_time": "2026-02-27 15:25:00",
        "src": "upstox_api_5m_empty",
    }
    assert svc._prefetch_intraday_should_skip_stale_retry(prev_row, candles, expected_lcd="2026-02-28") is True


def test_intraday_prefetch_stale_retry_allows_retry_after_terminal_skip_status():
    class FakeGcs:
        pass

    class FakeUpstox:
        pass

    svc = UniverseService(FakeGcs(), FakeUpstox(), StrategySettings())
    candles = [["2026-02-27T15:25:00+05:30", 100.0, 101.0, 99.0, 100.0, 10000.0]]
    prev_row = {
        "status": "STALE_SKIPPED",
        "expectedlcd": "2026-02-28",
        "last_candle_time": "2026-02-27 15:25:00",
        "src": "gcs_intraday_5m_stale_terminal",
    }
    assert svc._prefetch_intraday_should_skip_stale_retry(prev_row, candles, expected_lcd="2026-02-28") is False


def test_intraday_prefetch_missing_retry_terminalizes_empty_source():
    class FakeGcs:
        pass

    class FakeUpstox:
        pass

    svc = UniverseService(FakeGcs(), FakeUpstox(), StrategySettings())
    candles: list[list[object]] = []
    prev_row = {
        "status": "MISSING",
        "expectedlcd": "2026-02-28",
        "last_candle_time": "",
        "src": "upstox_api_5m_empty",
    }
    assert svc._prefetch_intraday_should_skip_missing_retry(prev_row, candles, expected_lcd="2026-02-28") is True


def test_intraday_windowed_fetch_uses_30_day_chunks():
    class FakeGcs:
        pass

    class FakeUpstox:
        def __init__(self):
            self.calls: list[tuple[str, str]] = []

        def get_historical_candles_v3_intraday_range(
            self,
            instrument_key: str,
            *,
            from_date: str,
            to_date: str,
            unit: str = "minutes",
            interval: int = 5,
        ) -> list[list[object]]:
            self.calls.append((from_date, to_date))
            return []

    upstox = FakeUpstox()
    svc = UniverseService(FakeGcs(), upstox, StrategySettings())
    _, calls = svc._fetch_intraday_5m_windowed_between(
        "NSE_EQ|TEST",
        from_date=date(2026, 1, 1),
        to_date=date(2026, 2, 28),
    )
    assert calls == 2
    assert upstox.calls == [("2026-01-01", "2026-01-30"), ("2026-01-31", "2026-02-28")]


def test_universe_v2_fetch_scope_limits_api_to_target_symbols():
    universe_rows = [
        _make_universe_row("AAA", "NSE", "CASH", "Y", "isin=INE000A01001", "NSE_EQ|AAA", "INE000A01001"),
        _make_universe_row("BBB", "NSE", "CASH", "Y", "isin=INE000B01002", "NSE_EQ|BBB", "INE000B01002"),
    ]

    class FakeGcs:
        def __init__(self):
            p_aaa = GoogleCloudStorageStore.score_cache_1d_path_by_instrument_key("NSE_EQ|AAA", "NSE", "CASH")
            p_bbb = GoogleCloudStorageStore.score_cache_1d_path_by_instrument_key("NSE_EQ|BBB", "NSE", "CASH")
            stale = [["2024-12-31T00:00:00+05:30", 100.0, 101.0, 99.0, 100.0, 10000.0]]
            self.cache = {
                p_aaa: [list(stale[0])],
                p_bbb: [list(stale[0])],
            }

        @staticmethod
        def score_cache_1d_path(symbol: str, exchange: str, segment: str) -> str:
            return GoogleCloudStorageStore.score_cache_1d_path(symbol, exchange, segment)

        @staticmethod
        def score_cache_1d_path_by_instrument_key(instrument_key: str, exchange: str, segment: str) -> str:
            return GoogleCloudStorageStore.score_cache_1d_path_by_instrument_key(instrument_key, exchange, segment)

        def read_candles(self, path: str) -> list[list[object]]:
            return list(self.cache.get(path, []))

        def write_candles(self, path: str, candles: list[list[object]]) -> None:
            self.cache[path] = [list(c) for c in candles]

        def merge_candles(self, path: str, candles: list[list[object]]) -> list[list[object]]:
            existing = self.cache.get(path, [])
            by_ts = {str(c[0]): list(c) for c in existing}
            for c in candles:
                by_ts[str(c[0])] = list(c)
            merged = [by_ts[k] for k in sorted(by_ts.keys())]
            self.cache[path] = merged
            return merged

    class FakeUpstox:
        pass

    svc = UniverseService(FakeGcs(), FakeUpstox(), StrategySettings())
    svc._load_universe_rows_from_firestore = lambda: [list(r) for r in universe_rows]  # type: ignore[method-assign]
    calls: list[tuple[str, str]] = []

    svc._fetch_daily_candles_incremental = lambda key, cached, lookback_days: calls.append(("inc", key)) or []  # type: ignore[method-assign]
    svc._fetch_daily_candles_windowed_between = lambda key, start, end: calls.append(("win", key)) or []  # type: ignore[method-assign]
    svc._fetch_daily_candles_backfill_older = lambda key, cached, lookback_days: calls.append(("old", key)) or []  # type: ignore[method-assign]

    out = svc._update_universe_v2_cache_and_stats(
        api_cap=10,
        run_full_backfill=True,
        fetch_only_symbols=["AAA"],
    )
    assert calls
    assert all("NSE_EQ|AAA" in key for _, key in calls)
    assert out["summary"]["fetchScopeSymbols"] == 1


def test_turnover_rank_assignment_bucket_quartiles():
    stats_by_symbol = {
        "A": TradabilityStats(turnover_med_60d=400.0),
        "B": TradabilityStats(turnover_med_60d=300.0),
        "C": TradabilityStats(turnover_med_60d=200.0),
        "D": TradabilityStats(turnover_med_60d=100.0),
    }
    assign_turnover_rank_and_bucket(stats_by_symbol)
    assert stats_by_symbol["A"].turnover_rank_60d == 1 and stats_by_symbol["A"].liquidity_bucket == "A"
    assert stats_by_symbol["B"].turnover_rank_60d == 2 and stats_by_symbol["B"].liquidity_bucket == "B"
    assert stats_by_symbol["C"].turnover_rank_60d == 3 and stats_by_symbol["C"].liquidity_bucket == "C"
    assert stats_by_symbol["D"].turnover_rank_60d == 4 and stats_by_symbol["D"].liquidity_bucket == "D"


def test_instrument_key_cache_path_prevents_symbol_collision():
    p1 = GoogleCloudStorageStore.score_cache_1d_path_by_instrument_key("NSE_EQ|INE732I01013", "NSE", "CASH")
    p2 = GoogleCloudStorageStore.score_cache_1d_path_by_instrument_key("NSE_EQ|INE732I01021", "NSE", "CASH")
    assert p1 != p2


def test_disabled_row_is_hard_disqualified():
    stats = TradabilityStats(
        bars_1d=300,
        price_last=120.0,
        turnover_med_60d=2_000_000.0,
        atr_14=4.0,
        atr_pct_14d=0.03,
        gap_risk_60d=0.02,
        turnover_rank_60d=10,
        liquidity_bucket="A",
    )
    out = classify_eligibility(
        stats=stats,
        data_quality_flag="FRESH",
        stale_days=0,
        controls=_controls("BALANCED"),
        enabled=False,
    )
    assert out.eligible_swing is False and out.eligible_intraday is False
    assert out.disable_reason == "ROW_DISABLED"


def test_symbol_exchange_conflict_resolution_prefers_existing_instrument_key():
    class FakeGcs:
        pass

    class FakeUpstox:
        pass

    svc = UniverseService(FakeGcs(), FakeUpstox(), StrategySettings())
    svc._probe_instrument_key_liveness = lambda key: (2, "live")  # type: ignore[method-assign]
    masters = [
        CanonicalListing(
            canonical_id="INE000A01001",
            symbol="ANGELONE",
            primary_exchange="NSE",
            primary_instrument_key="NSE_EQ|INE732I01013",
            primary_source_segment="NSE_EQ",
            security_type="SM",
            isin="INE732I01013",
            name="ANGELONE LTD",
        ),
        CanonicalListing(
            canonical_id="INE000A01002",
            symbol="ANGELONE",
            primary_exchange="NSE",
            primary_instrument_key="NSE_EQ|INE732I01021",
            primary_source_segment="NSE_EQ",
            security_type="SM",
            isin="INE732I01021",
            name="ANGELONE LTD",
        ),
    ]
    deduped, conflicts = svc._dedupe_master_by_symbol_exchange(
        masters,
        preferred_by_symbol_exchange={("ANGELONE", "NSE"): "NSE_EQ|INE732I01021"},
    )
    assert conflicts == 1
    assert len(deduped) == 1
    assert deduped[0].primary_instrument_key == "NSE_EQ|INE732I01021"


def test_sync_sector_mapping_to_universe_updates_targeted_symbols_only():
    class FakeState:
        def __init__(self):
            self.universe_docs = [
                {"symbol": "AAA", "exchange": "NSE", "sector": "UNKNOWN", "sector_source": "", "sector_updated_at": ""},
                {"symbol": "BBB", "exchange": "NSE", "sector": "UNKNOWN", "sector_source": "", "sector_updated_at": ""},
            ]
            self.updates: dict[str, dict] = {}

        def list_universe(self, limit: int = 3000) -> list[dict]:
            return list(self.universe_docs)

        def update_universe_row(self, symbol: str, fields: dict) -> None:
            self.updates[symbol] = dict(fields)

    state = FakeState()
    svc = UniverseService(object(), object(), StrategySettings())
    svc.state = state  # type: ignore[assignment]
    mapping = {
        ("AAA", "NSE"): {
            "sector": "IT",
            "source": "nse_quote_equity",
            "updatedAt": "2026-03-05 09:00:00",
        }
    }
    out = svc._sync_sector_mapping_to_universe(mapping, only_symbols={"AAA"})
    assert out["targeted"] == 1
    assert out["updated"] == 1
    assert "AAA" in state.updates
    assert state.updates["AAA"]["sector"] == "IT"
    assert state.updates["AAA"]["sector_source"] == "nse_quote_equity"
    assert state.updates["AAA"]["sector_updated_at"] == "2026-03-05 09:00:00"
    assert "BBB" not in state.updates


def test_sector_mapping_coverage_metrics_match_final_merged_mapping():
    class FakeState:
        @staticmethod
        def list_sector_mapping(limit: int = 3000) -> list[dict]:
            return [
                {
                    "symbol": "AAA",
                    "exchange": "NSE",
                    "macroSector": "FINANCIALS",
                    "sector": "BANKING",
                    "industry": "BANKS",
                    "basicIndustry": "PRIVATE BANKS",
                    "source": "firestore_seed",
                    "updatedAt": "2026-03-05 09:00:00",
                }
            ]

    class FakeGcs:
        @staticmethod
        def read_json(path: str, default=None):
            del path
            return [
                {
                    "symbol": "bbb",
                    "exchange": "nse",
                    "macro_sector": "INDUSTRIALS",
                    "sector": "CAPITAL GOODS",
                    "industry": "MACHINERY",
                    "basic_industry": "HEAVY MACHINERY",
                    "source": "gcs_seed",
                    "updated_at": "2026-03-05 09:01:00",
                }
            ]

        @staticmethod
        def write_json(path: str, data: object) -> None:
            del path, data

    class FakeUpstox:
        pass

    svc = UniverseService(FakeGcs(), FakeUpstox(), StrategySettings())
    svc.state = FakeState()  # type: ignore[assignment]
    universe_rows = [
        {"symbol": "AAA", "exchange": "NSE", "enabled": True, "fresh": True, "eligibleSwing": True, "eligibleIntraday": False, "sector": "UNKNOWN"},
        {"symbol": "BBB", "exchange": "NSE", "enabled": True, "fresh": True, "eligibleSwing": False, "eligibleIntraday": True, "sector": "UNKNOWN"},
        {"symbol": "CCC", "exchange": "NSE", "enabled": True, "fresh": True, "eligibleSwing": True, "eligibleIntraday": False, "sector": "ENERGY", "sectorSource": "universe_seed"},
        {"symbol": "DDD", "exchange": "NSE", "enabled": True, "fresh": True, "eligibleSwing": True, "eligibleIntraday": False, "sector": "UNKNOWN"},
        {"symbol": "ZZZ", "exchange": "NSE", "enabled": True, "fresh": True, "eligibleSwing": False, "eligibleIntraday": False, "sector": "IT"},
    ]

    mapping, coverage_pct, _source_origin, metrics = svc._load_sector_mapping_dataset(universe_rows, include_meta=True)
    assert ("AAA", "NSE") in mapping
    assert ("BBB", "NSE") in mapping
    assert ("CCC", "NSE") in mapping
    assert metrics["eligible_universe_count"] == 4
    assert metrics["mapped_count"] == 3
    assert metrics["unmapped_count"] == 1
    assert metrics["mapped_count"] + metrics["unmapped_count"] == metrics["eligible_universe_count"]
    assert metrics["coverage_pct"] == pytest.approx(75.0, abs=0.1)
    assert coverage_pct == pytest.approx(metrics["coverage_pct"], abs=0.1)
    assert metrics["source_breakdown_counts"]["firestore"] == 1
    assert metrics["source_breakdown_counts"]["gcs"] == 1
    assert metrics["source_breakdown_counts"]["universe_fallback"] == 1
    assert metrics["source_breakdown_counts"]["unknown"] == 0


def test_sector_mapping_coverage_metrics_falls_back_to_enabled_scope_when_eligible_is_empty():
    svc = UniverseService(object(), object(), StrategySettings())
    universe_rows = [
        {"symbol": "AAA", "exchange": "NSE", "enabled": True, "fresh": False, "eligibleSwing": False, "eligibleIntraday": False},
        {"symbol": "BBB", "exchange": "NSE", "enabled": True, "fresh": False, "eligibleSwing": False, "eligibleIntraday": False},
        {"symbol": "CCC", "exchange": "NSE", "enabled": False, "fresh": True, "eligibleSwing": True, "eligibleIntraday": True},
    ]
    mapping = {
        ("AAA", "NSE"): {"sector": "IT"},
    }
    source_origin = {
        ("AAA", "NSE"): "sheet",
    }

    metrics = svc._sector_mapping_coverage_metrics(universe_rows, mapping, source_origin)
    assert metrics["coverage_scope"] == "enabled_fallback"
    # Top-level backward-compatible counts reflect effective scope in fallback mode.
    assert metrics["eligible_universe_count"] == 2
    assert metrics["mapped_count"] == 1
    assert metrics["unmapped_count"] == 1
    assert metrics["mapped_count"] + metrics["unmapped_count"] == metrics["eligible_universe_count"]
    assert metrics["coverage_pct"] == pytest.approx(50.0, abs=0.1)
    # Raw diagnostics preserve why fallback was used.
    assert metrics["eligible_universe_count_raw"] == 0
    assert metrics["enabled_universe_count"] == 2
    assert metrics["eligible_coverage_pct_raw"] == pytest.approx(0.0, abs=0.1)
    assert metrics["source_breakdown_counts"]["sheet"] == 1
