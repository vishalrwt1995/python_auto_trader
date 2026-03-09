from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from autotrader.adapters.sheets_repository import GoogleSheetsRepository, SheetNames
from autotrader.domain.models import MarketBrainState, MarketPolicy
from autotrader.services.universe_service import UniverseService
from autotrader.services.universe_v2 import ModeThresholds, UniverseControls
from autotrader.settings import StrategySettings
from autotrader.time_utils import IST, now_ist


def _controls() -> UniverseControls:
    return UniverseControls(
        mode="BALANCED",
        min_bars_hard=90,
        min_price_hard=20.0,
        max_gap_risk_hard=0.10,
        max_atr_pct_hard=0.20,
        stale_days_max=5,
        mode_thresholds={
            "BALANCED": ModeThresholds(
                swing_topn_turnover_60d=1000,
                intraday_topn_turnover_60d=500,
                min_bars_swing=180,
                min_bars_intraday=252,
                min_price_mode=30.0,
                max_atr_pct_swing=0.12,
                max_atr_pct_intraday=0.09,
                max_gap_risk_mode=0.06,
            )
        },
    )


def _daily_candles(n: int = 280, start: datetime | None = None, step: float = 0.35) -> list[list[object]]:
    start_i = start or datetime(2025, 1, 1, tzinfo=IST)
    out: list[list[object]] = []
    px = 100.0
    for i in range(n):
        d = start_i + timedelta(days=i)
        while d.weekday() >= 5:
            d += timedelta(days=1)
        o = px
        c = px + step
        h = max(o, c) + 0.4
        l = min(o, c) - 0.4
        v = 1_000_000 + (i * 2000)
        out.append([d.strftime("%Y-%m-%dT00:00:00+05:30"), o, h, l, c, v])
        px = c
    return out


def _intraday_5m_today(count: int = 18, *, expanding: bool = True) -> list[list[object]]:
    now_i = now_ist().astimezone(IST)
    d = now_i.replace(hour=9, minute=15, second=0, microsecond=0)
    out: list[list[object]] = []
    px = 100.0
    for i in range(count):
        ts = d + timedelta(minutes=5 * i)
        amp = (0.15 + (0.04 * i)) if expanding else 0.25
        o = px
        c = px + 0.18
        h = max(o, c) + amp
        l = min(o, c) - amp
        v = 10000 + (i * 300)
        out.append([ts.isoformat(), o, h, l, c, v])
        px = c
    return out


class _FakeSheets:
    def __init__(self):
        self.swing_rows: list[list[object]] = []
        self.intraday_rows: list[list[object]] = []
        self.sector_rows: list[list[object]] = []

    def replace_watchlist_swing_v2(self, rows: list[list[object]]) -> None:
        self.swing_rows = list(rows)

    def replace_watchlist_intraday_v2(self, rows: list[list[object]]) -> None:
        self.intraday_rows = list(rows)

    def replace_sector_mapping(self, rows: list[list[object]]) -> None:
        self.sector_rows = list(rows)


class _FakeSheetsSector(_FakeSheets):
    def __init__(self, sector_rows: list[list[str]]):
        super().__init__()
        self._sector_rows = sector_rows

    def ensure_sheet_headers_append(self, sheet_name: str, required_headers: list[str], header_row: int = 3) -> dict[str, int]:
        del header_row
        if sheet_name == SheetNames.SECTOR_MAPPING:
            return {h: i + 1 for i, h in enumerate(required_headers)}
        return {}

    def read_sheet_rows(self, sheet_name: str, start_row: int = 4) -> list[list[str]]:
        del start_row
        if sheet_name == SheetNames.SECTOR_MAPPING:
            return [list(r) for r in self._sector_rows]
        return []


class _MarketBrainStub:
    def __init__(self, state: MarketBrainState, policy: MarketPolicy):
        self._state = state
        self._policy = policy
        self.state = None

    def derive_market_policy(self, state: MarketBrainState) -> MarketPolicy:
        del state
        return self._policy

    def watchlist_regime_payload(self, state: MarketBrainState) -> dict[str, Any]:
        del state
        return {
            "regimeDaily": "RISK_OFF",
            "regimeIntraday": "CHOPPY",
            "source": {"dailySource": "cache_only", "intradaySource": "premarket_skip"},
        }

    def adjust_watchlist_rows(self, rows: list[dict[str, Any]], policy: MarketPolicy, *, section: str) -> list[dict[str, Any]]:
        if section == "swing" and str(policy.swing_permission).upper() == "DISABLED":
            return []
        return list(rows)


def _intraday_5m_history_with_today(days: int, *, today_bars: int = 18) -> list[list[object]]:
    now_i = now_ist().astimezone(IST)
    today = now_i.date()
    offsets = [5 * i for i in range(today_bars)]
    bars: list[list[object]] = []
    prev_days: list[datetime] = []
    d = today - timedelta(days=1)
    while len(prev_days) < days:
        if d.weekday() < 5:
            prev_days.append(datetime(d.year, d.month, d.day, 9, 15, tzinfo=IST))
        d -= timedelta(days=1)
    prev_days.sort()
    px = 100.0
    for base in prev_days:
        for j, off in enumerate(offsets):
            ts = base + timedelta(minutes=off)
            o = px
            c = px + 0.05
            h = max(o, c) + 0.08
            l = min(o, c) - 0.08
            v = 7000 + (j * 100)
            bars.append([ts.isoformat(), o, h, l, c, v])
            px = c
    today_base = datetime(today.year, today.month, today.day, 9, 15, tzinfo=IST)
    for j, off in enumerate(offsets):
        ts = today_base + timedelta(minutes=off)
        o = px
        c = px + 0.09
        h = max(o, c) + 0.09
        l = min(o, c) - 0.09
        v = 8500 + (j * 120)
        bars.append([ts.isoformat(), o, h, l, c, v])
        px = c
    return bars


def test_watchlist_v2_regime_classification():
    svc = UniverseService(_FakeSheets(), object(), object(), StrategySettings())
    svc._fetch_index_daily_proxy = lambda expected_lcd, allow_live_api=True: (_daily_candles(), "NSE_INDEX|Nifty 50", "cache")  # type: ignore[method-assign]
    svc._fetch_index_intraday_proxy = lambda timeframe, now_i, allow_live_api=True: (_intraday_5m_today(), "NSE_INDEX|Nifty 50", "cache")  # type: ignore[method-assign]
    now_i = now_ist()
    reg = svc._build_watchlist_v2_regime(timeframe="5m", expected_lcd=(now_i - timedelta(days=1)).strftime("%Y-%m-%d"), now_i=now_i)
    assert reg["regimeDaily"] == "TREND"
    assert reg["regimeIntraday"] == "TRENDY"


def test_watchlist_v2_regime_premarket_skips_index_intraday_api():
    svc = UniverseService(_FakeSheets(), object(), object(), StrategySettings())
    calls: list[bool] = []

    def _daily(expected_lcd: str, *, allow_live_api: bool = True):  # type: ignore[no-untyped-def]
        calls.append(bool(allow_live_api))
        return _daily_candles(), "NSE_INDEX|Nifty 50", "cache"

    def _intra(**kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("intraday proxy should not be called in premarket")

    svc._fetch_index_daily_proxy = _daily  # type: ignore[method-assign]
    svc._fetch_index_intraday_proxy = _intra  # type: ignore[method-assign]
    now_i = now_ist()
    reg = svc._build_watchlist_v2_regime(
        timeframe="5m",
        expected_lcd=(now_i - timedelta(days=1)).strftime("%Y-%m-%d"),
        now_i=now_i,
        premarket=True,
    )
    assert calls == [False]
    assert reg["regimeIntraday"] == "CHOPPY"
    assert reg["source"]["intradaySource"] == "premarket_skip"


def test_watchlist_v2_phase2_window_allows_final_block_with_completed_bars():
    svc = UniverseService(_FakeSheets(), object(), object(), StrategySettings())
    now_i = now_ist().astimezone(IST).replace(hour=14, minute=50, second=0, microsecond=0)
    assert svc._phase2_window_open(now_i, premarket=False, run_block="INTRA_FINAL") is True
    assert svc._phase2_window_open(now_i, premarket=True, run_block="INTRA_FINAL") is False


def test_watchlist_v2_score_bounds_zero_to_hundred():
    sheets = _FakeSheets()
    svc = UniverseService(sheets, object(), object(), StrategySettings())
    now_i = now_ist()
    expected_lcd = svc._expected_latest_daily_candle_date(now_i).strftime("%Y-%m-%d")

    candidates = [
        {
            "symbol": "AAA",
            "exchange": "NSE",
            "segment": "CASH",
            "enabled": True,
            "instrumentKey": "NSE_EQ|AAA",
            "eligibleSwing": True,
            "eligibleIntraday": True,
            "turnoverRank60D": 10,
            "liquidityBucket": "A",
            "atrPct14D": 0.02,
            "gapRisk60D": 0.01,
            "priceLast": 120.0,
            "bars1D": 260,
            "last1DDate": expected_lcd,
            "fresh": True,
            "disableReason": "",
        },
        {
            "symbol": "BBB",
            "exchange": "NSE",
            "segment": "CASH",
            "enabled": True,
            "instrumentKey": "NSE_EQ|BBB",
            "eligibleSwing": True,
            "eligibleIntraday": True,
            "turnoverRank60D": 30,
            "liquidityBucket": "A",
            "atrPct14D": 0.025,
            "gapRisk60D": 0.015,
            "priceLast": 180.0,
            "bars1D": 260,
            "last1DDate": expected_lcd,
            "fresh": True,
            "disableReason": "",
        },
    ]

    svc._build_universe_v2_controls = lambda: _controls()  # type: ignore[method-assign]
    svc._build_watchlist_v2_regime = lambda timeframe, expected_lcd, now_i, premarket=False: {  # type: ignore[method-assign]
        "regimeDaily": "TREND",
        "regimeIntraday": "CHOPPY",
        "source": {},
    }
    svc._watchlist_v2_candidates = lambda expected_lcd: list(candidates)  # type: ignore[method-assign]
    svc._watchlist_daily_candles = lambda row, expected_lcd: _daily_candles()  # type: ignore[method-assign]
    svc._watchlist_intraday_candles = lambda row, timeframe, now_i: _intraday_5m_today()  # type: ignore[method-assign]

    out = svc.build_watchlist(None, target_size=20, premarket=True, intraday_timeframe="5m")
    assert out["ready"] is True
    assert sheets.swing_rows
    assert sheets.intraday_rows
    for r in sheets.swing_rows:
        assert 0.0 <= float(r[6]) <= 100.0
    for r in sheets.intraday_rows:
        assert 0.0 <= float(r[7]) <= 100.0


def test_watchlist_v2_intraday_run_does_not_overwrite_swing_sheet():
    sheets = _FakeSheets()
    sheets.swing_rows = [["KEEP_SWING_ROW"]]
    svc = UniverseService(sheets, object(), object(), StrategySettings())
    now_i = now_ist()
    expected_lcd = svc._expected_latest_daily_candle_date(now_i).strftime("%Y-%m-%d")

    candidates = [
        {
            "symbol": "AAA",
            "exchange": "NSE",
            "segment": "CASH",
            "enabled": True,
            "instrumentKey": "NSE_EQ|AAA",
            "eligibleSwing": True,
            "eligibleIntraday": True,
            "turnoverRank60D": 10,
            "liquidityBucket": "A",
            "atrPct14D": 0.02,
            "gapRisk60D": 0.01,
            "priceLast": 120.0,
            "bars1D": 260,
            "last1DDate": expected_lcd,
            "fresh": True,
            "disableReason": "",
            "decisionPresent": True,
        }
    ]
    svc._build_universe_v2_controls = lambda: _controls()  # type: ignore[method-assign]
    svc._build_watchlist_v2_regime = lambda timeframe, expected_lcd, now_i, premarket=False: {  # type: ignore[method-assign]
        "regimeDaily": "RANGE",
        "regimeIntraday": "CHOPPY",
        "source": {},
    }
    svc._watchlist_v2_candidates = lambda expected_lcd: list(candidates)  # type: ignore[method-assign]
    svc._watchlist_daily_candles = lambda row, expected_lcd: _daily_candles()  # type: ignore[method-assign]
    svc._watchlist_intraday_candles = lambda row, timeframe, now_i: _intraday_5m_today()  # type: ignore[method-assign]

    out = svc.build_watchlist(None, target_size=20, premarket=False, intraday_timeframe="5m")
    assert out["ready"] is True
    assert out["swingSelected"] == 0
    assert out["swingComputed"] >= 1
    assert sheets.swing_rows == [["KEEP_SWING_ROW"]]
    assert sheets.intraday_rows


def test_watchlist_v2_phase2_first_merge():
    phase2 = [
        {"symbol": "P2A", "score": 99.0, "source": "PHASE2_INPLAY"},
        {"symbol": "P2B", "score": 97.0, "source": "PHASE2_INPLAY"},
    ]
    phase1 = [
        {"symbol": "P1A", "score": 95.0, "source": "PHASE1_DAILY_FALLBACK"},
        {"symbol": "P2A", "score": 94.0, "source": "PHASE1_DAILY_FALLBACK"},
        {"symbol": "P1B", "score": 93.0, "source": "PHASE1_DAILY_FALLBACK"},
    ]
    merged = UniverseService._merge_intraday_v2(phase2, phase1, target=3)
    assert [x["symbol"] for x in merged] == ["P2A", "P2B", "P1A"]


def test_watchlist_v2_phase2_eligibility_fails_and_forces_fallback():
    sheets = _FakeSheets()
    svc = UniverseService(sheets, object(), object(), StrategySettings())
    now_i = now_ist()
    expected_lcd = svc._expected_latest_daily_candle_date(now_i).strftime("%Y-%m-%d")
    candidate = {
        "symbol": "AAA",
        "exchange": "NSE",
        "segment": "CASH",
        "enabled": True,
        "instrumentKey": "NSE_EQ|AAA",
        "eligibleSwing": True,
        "eligibleIntraday": True,
        "turnoverRank60D": 10,
        "turnoverMed60D": 12_000_000.0,
        "liquidityBucket": "A",
        "atr14": 1.4,
        "atrPct14D": 0.018,
        "gapRisk60D": 0.012,
        "priceLast": 120.0,
        "bars1D": 260,
        "last1DDate": expected_lcd,
        "fresh": True,
        "disableReason": "",
        "decisionPresent": True,
        "sector": "FINANCIALS",
        "sectorSource": "nse",
        "sectorUpdatedAt": "",
    }
    svc._build_universe_v2_controls = lambda: _controls()  # type: ignore[method-assign]
    svc._build_watchlist_v2_regime = lambda timeframe, expected_lcd, now_i, premarket=False: {  # type: ignore[method-assign]
        "regimeDaily": "RANGE",
        "regimeIntraday": "CHOPPY",
        "source": {},
    }
    svc._watchlist_v2_candidates = lambda expected_lcd: [dict(candidate)]  # type: ignore[method-assign]
    svc._watchlist_daily_candles = lambda row, expected_lcd: _daily_candles()  # type: ignore[method-assign]
    svc._watchlist_intraday_candles = lambda row, timeframe, now_i: _intraday_5m_history_with_today(20)  # type: ignore[method-assign]

    out = svc.build_watchlist(None, target_size=20, premarket=False, intraday_timeframe="5m")
    assert out["ready"] is True
    assert len(sheets.intraday_rows) == 1
    row = sheets.intraday_rows[0]
    assert row[8] == "PHASE1_DAILY_FALLBACK"
    assert row[26] == "N"
    assert row[28] == "LOW_SLOT_COVERAGE"


def test_watchlist_v2_phase2_eligibility_fails_when_today_bars_missing():
    svc = UniverseService(_FakeSheets(), object(), object(), StrategySettings())
    bars = _intraday_5m_history_with_today(60, today_bars=0)
    chk = svc._phase2_eligibility(bars=bars, now_i=now_ist(), interval_min=5)
    assert chk["eligible"] is False
    assert chk["reason"] == "STALE_INTRADAY_CACHE"


def test_watchlist_v2_phase2_eligibility_passes_with_complete_baseline():
    sheets = _FakeSheets()
    svc = UniverseService(sheets, object(), object(), StrategySettings())
    now_i = now_ist()
    expected_lcd = svc._expected_latest_daily_candle_date(now_i).strftime("%Y-%m-%d")
    candidate = {
        "symbol": "AAA",
        "exchange": "NSE",
        "segment": "CASH",
        "enabled": True,
        "instrumentKey": "NSE_EQ|AAA",
        "eligibleSwing": True,
        "eligibleIntraday": True,
        "turnoverRank60D": 10,
        "turnoverMed60D": 12_000_000.0,
        "liquidityBucket": "A",
        "atr14": 1.4,
        "atrPct14D": 0.018,
        "gapRisk60D": 0.012,
        "priceLast": 120.0,
        "bars1D": 260,
        "last1DDate": expected_lcd,
        "fresh": True,
        "disableReason": "",
        "decisionPresent": True,
        "sector": "FINANCIALS",
        "sectorSource": "nse",
        "sectorUpdatedAt": "",
    }
    svc._build_universe_v2_controls = lambda: _controls()  # type: ignore[method-assign]
    svc._build_watchlist_v2_regime = lambda timeframe, expected_lcd, now_i, premarket=False: {  # type: ignore[method-assign]
        "regimeDaily": "RANGE",
        "regimeIntraday": "CHOPPY",
        "source": {},
    }
    svc._watchlist_v2_candidates = lambda expected_lcd: [dict(candidate)]  # type: ignore[method-assign]
    svc._watchlist_daily_candles = lambda row, expected_lcd: _daily_candles()  # type: ignore[method-assign]
    svc._watchlist_intraday_candles = lambda row, timeframe, now_i: _intraday_5m_history_with_today(60)  # type: ignore[method-assign]

    out = svc.build_watchlist(None, target_size=20, premarket=False, intraday_timeframe="5m")
    assert out["ready"] is True
    assert len(sheets.intraday_rows) == 1
    row = sheets.intraday_rows[0]
    assert row[8] == "PHASE2_INPLAY"
    assert row[26] == "Y"
    assert float(row[27]) >= 75.0


def test_watchlist_v2_phase2_diagnostics_present_when_phase2_runs():
    sheets = _FakeSheets()
    svc = UniverseService(sheets, object(), object(), StrategySettings())
    now_i = now_ist()
    expected_lcd = svc._expected_latest_daily_candle_date(now_i).strftime("%Y-%m-%d")
    candidate = {
        "symbol": "AAA",
        "exchange": "NSE",
        "segment": "CASH",
        "enabled": True,
        "instrumentKey": "NSE_EQ|AAA",
        "eligibleSwing": True,
        "eligibleIntraday": True,
        "turnoverRank60D": 10,
        "turnoverMed60D": 12_000_000.0,
        "liquidityBucket": "A",
        "atr14": 1.4,
        "atrPct14D": 0.018,
        "gapRisk60D": 0.012,
        "priceLast": 120.0,
        "bars1D": 260,
        "last1DDate": expected_lcd,
        "fresh": True,
        "disableReason": "",
        "decisionPresent": True,
        "sector": "FINANCIALS",
        "sectorSource": "nse",
        "sectorUpdatedAt": "",
    }
    svc._build_universe_v2_controls = lambda: _controls()  # type: ignore[method-assign]
    svc._run_time_block = lambda now_i, premarket=False: "INTRA_5M"  # type: ignore[method-assign]
    svc._build_watchlist_v2_regime = lambda timeframe, expected_lcd, now_i, premarket=False: {  # type: ignore[method-assign]
        "regimeDaily": "RANGE",
        "regimeIntraday": "CHOPPY",
        "source": {},
    }
    svc._watchlist_v2_candidates = lambda expected_lcd: [dict(candidate)]  # type: ignore[method-assign]
    svc._watchlist_daily_candles = lambda row, expected_lcd: _daily_candles()  # type: ignore[method-assign]
    svc._watchlist_intraday_candles = lambda row, timeframe, now_i: _intraday_5m_history_with_today(20)  # type: ignore[method-assign]

    out = svc.build_watchlist(None, target_size=20, premarket=False, intraday_timeframe="5m")
    stats = out["intradayPhaseStats"]
    assert stats["phase2BranchEntered"] is True
    assert stats["phase2BranchCompleted"] is True
    assert int(stats["phase2CandidatesSeen"]) >= 1
    assert "phase2RejectionSummary" in stats
    summary = stats["phase2RejectionSummary"]
    assert isinstance(summary, dict)
    assert "LOW_SLOT_COVERAGE" in summary


def test_watchlist_v2_writes_explicit_swing_disabled_state():
    sheets = _FakeSheets()
    svc = UniverseService(sheets, object(), object(), StrategySettings())
    now_i = now_ist()
    expected_lcd = svc._expected_latest_daily_candle_date(now_i).strftime("%Y-%m-%d")
    candidate = {
        "symbol": "AAA",
        "exchange": "NSE",
        "segment": "CASH",
        "enabled": True,
        "instrumentKey": "NSE_EQ|AAA",
        "eligibleSwing": True,
        "eligibleIntraday": True,
        "turnoverRank60D": 10,
        "turnoverMed60D": 12_000_000.0,
        "liquidityBucket": "A",
        "atr14": 1.4,
        "atrPct14D": 0.018,
        "gapRisk60D": 0.012,
        "priceLast": 120.0,
        "bars1D": 260,
        "last1DDate": expected_lcd,
        "fresh": True,
        "disableReason": "",
        "decisionPresent": True,
        "sector": "FINANCIALS",
        "sectorSource": "nse",
        "sectorUpdatedAt": "",
    }
    state = MarketBrainState(
        asof_ts=now_i.isoformat(),
        phase="PREMARKET",
        regime="PANIC",
        participation="WEAK",
        risk_mode="LOCKDOWN",
        intraday_state="PREOPEN",
        swing_permission="DISABLED",
    )
    policy = MarketPolicy(
        regime="PANIC",
        risk_mode="LOCKDOWN",
        swing_permission="DISABLED",
        intraday_phase2_enabled=False,
        breakout_enabled=False,
        open_drive_enabled=False,
        long_enabled=False,
        short_enabled=True,
        watchlist_target_multiplier=0.4,
        watchlist_min_score_boost=18,
        liquidity_bucket_floor="A",
        reasons=["regime=PANIC", "riskMode=LOCKDOWN"],
    )
    svc.market_brain_service = _MarketBrainStub(state, policy)  # type: ignore[assignment]
    svc._build_universe_v2_controls = lambda: _controls()  # type: ignore[method-assign]
    svc._build_watchlist_v2_regime = lambda timeframe, expected_lcd, now_i, premarket=False: {  # type: ignore[method-assign]
        "regimeDaily": "RISK_OFF",
        "regimeIntraday": "CHOPPY",
        "source": {},
    }
    svc._watchlist_v2_candidates = lambda expected_lcd: [dict(candidate)]  # type: ignore[method-assign]
    svc._watchlist_daily_candles = lambda row, expected_lcd: _daily_candles()  # type: ignore[method-assign]
    svc._watchlist_intraday_candles = lambda row, timeframe, now_i: _intraday_5m_history_with_today(60)  # type: ignore[method-assign]

    out = svc.build_watchlist(state, target_size=20, premarket=True, intraday_timeframe="5m")
    assert out["swingSelected"] == 0
    assert len(sheets.swing_rows) == 1
    assert sheets.swing_rows[0][17] == "DISABLED_BY_MARKET_BRAIN"
    assert "PANIC_LOCKDOWN" in str(sheets.swing_rows[0][18])


def test_watchlist_v2_correlation_guard_blocks_high_corr_duplicate():
    svc = UniverseService(_FakeSheets(), object(), object(), StrategySettings())
    rets = {f"2026-01-{i:02d}": (0.004 + (i * 0.0002)) for i in range(1, 31)}
    selected = svc._select_with_diversification_and_corr(
        [
            {
                "symbol": "AAA",
                "score": 99.0,
                "sector": "IT",
                "liquidityBucket": "A",
                "atrPct14D": 0.02,
                "gapRisk60D": 0.01,
                "returnsByDate": dict(rets),
            },
            {
                "symbol": "BBB",
                "score": 98.0,
                "sector": "BANK",
                "liquidityBucket": "A",
                "atrPct14D": 0.02,
                "gapRisk60D": 0.01,
                "returnsByDate": dict(rets),
            },
        ],
        target=2,
        sector_coverage_pct=100.0,
    )
    assert [r["symbol"] for r in selected] == ["AAA"]


def test_watchlist_v2_sector_mapping_join_and_coverage_computed():
    sheets = _FakeSheetsSector(
        [
            ["AAA", "NSE", "ECONOMY", "FINANCIALS", "BANKS", "PRIVATE BANKS", "nse", "2026-03-01 09:00:00"],
        ]
    )
    svc = UniverseService(sheets, object(), object(), StrategySettings())
    universe_rows: list[dict[str, Any]] = [
        {"symbol": "AAA", "exchange": "NSE", "enabled": True, "fresh": True, "eligibleSwing": True, "eligibleIntraday": False, "sector": "UNKNOWN"},
        {"symbol": "BBB", "exchange": "NSE", "enabled": True, "fresh": True, "eligibleSwing": True, "eligibleIntraday": False, "sector": "UNKNOWN"},
    ]
    mapping, coverage = svc._load_sector_mapping_dataset(universe_rows)
    assert ("AAA", "NSE") in mapping
    assert mapping[("AAA", "NSE")]["sector"] == "FINANCIALS"
    assert coverage == 50.0


def test_watchlist_v2_no_lookahead_daily_filter():
    candles = [
        ["2026-03-01T00:00:00+05:30", 100, 101, 99, 100, 1000],
        ["2026-03-02T00:00:00+05:30", 100, 102, 99, 101, 1200],
        ["2026-03-03T00:00:00+05:30", 101, 103, 100, 102, 1300],
    ]
    out = UniverseService._daily_no_lookahead(candles, "2026-03-02")
    assert len(out) == 2
    assert out[-1][0].startswith("2026-03-02")


def test_watchlist_v2_sheet_auto_creation_paths():
    state = {
        SheetNames.CONFIG: 1,
        SheetNames.WATCHLIST: 2,
    }
    writes: list[dict[str, object]] = []

    class _Exec:
        def __init__(self, body):
            self.body = body

        def execute(self):
            requests = list((self.body or {}).get("requests") or [])
            for req in requests:
                if "addSheet" in req:
                    title = str(req["addSheet"]["properties"]["title"])
                    if title not in state:
                        state[title] = max(state.values()) + 1
                elif "deleteSheet" in req:
                    sid = int(req["deleteSheet"]["sheetId"])
                    for k in list(state.keys()):
                        if int(state[k]) == sid:
                            del state[k]
                elif "updateSheetProperties" in req:
                    props = req["updateSheetProperties"]["properties"]
                    sid = int(props.get("sheetId", -1))
                    new_title = str(props.get("title") or "")
                    if new_title:
                        for k in list(state.keys()):
                            if int(state[k]) == sid:
                                del state[k]
                                state[new_title] = sid
                                break
            return {}

    class _Spreadsheets:
        def batchUpdate(self, spreadsheetId, body):
            return _Exec(body)

    class _Svc:
        def spreadsheets(self):
            return _Spreadsheets()

    repo = GoogleSheetsRepository("dummy")
    repo._service = _Svc()
    repo._sheet_meta = lambda: dict(state)  # type: ignore[method-assign]
    repo.batch_update_values = lambda data: writes.extend(data)  # type: ignore[method-assign]
    repo.ensure_core_sheets()

    assert SheetNames.WATCHLIST_SWING_V2 in state
    assert SheetNames.WATCHLIST_INTRADAY_V2 in state
    assert SheetNames.WATCHLIST not in state
    assert any(SheetNames.WATCHLIST_SWING_V2 in str(x.get("range", "")) for x in writes)
    assert any(SheetNames.WATCHLIST_INTRADAY_V2 in str(x.get("range", "")) for x in writes)


def test_watchlist_v2_require_full_coverage_blocks_when_unclassified_enabled_rows_exist():
    sheets = _FakeSheets()
    svc = UniverseService(sheets, object(), object(), StrategySettings())
    now_i = now_ist()
    expected_lcd = svc._expected_latest_daily_candle_date(now_i).strftime("%Y-%m-%d")

    svc._build_universe_v2_controls = lambda: _controls()  # type: ignore[method-assign]
    svc._watchlist_v2_candidates = lambda expected_lcd: [  # type: ignore[method-assign]
        {
            "symbol": "AAA",
            "exchange": "NSE",
            "segment": "CASH",
            "enabled": True,
            "instrumentKey": "NSE_EQ|AAA",
            "eligibleSwing": True,
            "eligibleIntraday": True,
            "turnoverRank60D": 10,
            "liquidityBucket": "A",
            "atrPct14D": 0.02,
            "gapRisk60D": 0.01,
            "priceLast": 120.0,
            "bars1D": 260,
            "last1DDate": expected_lcd,
            "fresh": True,
            "disableReason": "",
            "decisionPresent": True,
        },
        {
            "symbol": "BBB",
            "exchange": "NSE",
            "segment": "CASH",
            "enabled": True,
            "instrumentKey": "NSE_EQ|BBB",
            "eligibleSwing": False,
            "eligibleIntraday": False,
            "turnoverRank60D": None,
            "liquidityBucket": "",
            "atrPct14D": 0.0,
            "gapRisk60D": 0.0,
            "priceLast": 0.0,
            "bars1D": 0,
            "last1DDate": "",
            "fresh": False,
            "disableReason": "",
            "decisionPresent": False,
        },
    ]

    out = svc.build_watchlist(None, target_size=20, require_full_coverage=True, premarket=True)
    assert out["ready"] is False
    assert out["reason"] == "score_coverage_incomplete"
    assert out["selected"] == 0
    assert out["coverage"]["full"] is False
    assert sheets.swing_rows == []
    assert sheets.intraday_rows == []


def test_watchlist_v2_require_today_scored_blocks_when_today_coverage_incomplete():
    sheets = _FakeSheets()
    svc = UniverseService(sheets, object(), object(), StrategySettings())
    now_i = now_ist()
    expected_lcd = svc._expected_latest_daily_candle_date(now_i).strftime("%Y-%m-%d")

    svc._build_universe_v2_controls = lambda: _controls()  # type: ignore[method-assign]
    svc._watchlist_v2_candidates = lambda expected_lcd: [  # type: ignore[method-assign]
        {
            "symbol": "AAA",
            "exchange": "NSE",
            "segment": "CASH",
            "enabled": True,
            "instrumentKey": "NSE_EQ|AAA",
            "eligibleSwing": True,
            "eligibleIntraday": True,
            "turnoverRank60D": 10,
            "liquidityBucket": "A",
            "atrPct14D": 0.02,
            "gapRisk60D": 0.01,
            "priceLast": 120.0,
            "bars1D": 260,
            "last1DDate": expected_lcd,
            "fresh": True,
            "disableReason": "",
            "decisionPresent": True,
        },
        {
            "symbol": "BBB",
            "exchange": "NSE",
            "segment": "CASH",
            "enabled": True,
            "instrumentKey": "NSE_EQ|BBB",
            "eligibleSwing": False,
            "eligibleIntraday": False,
            "turnoverRank60D": 12,
            "liquidityBucket": "A",
            "atrPct14D": 0.03,
            "gapRisk60D": 0.02,
            "priceLast": 100.0,
            "bars1D": 260,
            "last1DDate": "2000-01-01",
            "fresh": False,
            "disableReason": "STALE_1D_CANDLE",
            "decisionPresent": True,
        },
    ]

    out = svc.build_watchlist(None, target_size=20, require_full_coverage=True, require_today_scored=True, premarket=True)
    assert out["ready"] is False
    assert out["reason"] == "today_score_coverage_incomplete"
    assert out["selected"] == 0
    assert out["coverage"]["todayFull"] is False
    assert sheets.swing_rows == []
    assert sheets.intraday_rows == []
