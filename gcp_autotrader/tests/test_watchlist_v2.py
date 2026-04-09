from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

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


class _FakeState:
    """Captures save_watchlist() calls for assertion in tests."""

    def __init__(self, sector_mapping: list[dict] | None = None):
        self._sector_mapping = sector_mapping or []
        self.watchlist_payload: dict | None = None

    def save_watchlist(self, payload: dict) -> None:
        self.watchlist_payload = payload

    def list_sector_mapping(self, limit: int = 3000) -> list[dict]:
        return list(self._sector_mapping)

    @property
    def intraday_rows(self) -> list[dict]:
        if self.watchlist_payload is None:
            return []
        return [r for r in self.watchlist_payload.get("rows", []) if r.get("wlType") == "intraday"]

    @property
    def swing_rows(self) -> list[dict]:
        if self.watchlist_payload is None:
            return []
        return [r for r in self.watchlist_payload.get("rows", []) if r.get("wlType") == "swing"]


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
    svc = UniverseService(object(), object(), StrategySettings())
    svc._fetch_index_daily_proxy = lambda expected_lcd, allow_live_api=True: (_daily_candles(), "NSE_INDEX|Nifty 50", "cache")  # type: ignore[method-assign]
    svc._fetch_index_intraday_proxy = lambda timeframe, now_i, allow_live_api=True: (_intraday_5m_today(), "NSE_INDEX|Nifty 50", "cache")  # type: ignore[method-assign]
    now_i = now_ist()
    reg = svc._build_watchlist_v2_regime(timeframe="5m", expected_lcd=(now_i - timedelta(days=1)).strftime("%Y-%m-%d"), now_i=now_i)
    assert reg["regimeDaily"] == "TREND"
    assert reg["regimeIntraday"] == "TRENDY"


def test_watchlist_v2_regime_premarket_skips_index_intraday_api():
    svc = UniverseService(object(), object(), StrategySettings())
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
    svc = UniverseService(object(), object(), StrategySettings())
    now_i = now_ist().astimezone(IST).replace(hour=14, minute=50, second=0, microsecond=0)
    assert svc._phase2_window_open(now_i, premarket=False, run_block="INTRA_FINAL") is True
    assert svc._phase2_window_open(now_i, premarket=True, run_block="INTRA_FINAL") is False


def test_watchlist_v2_score_bounds_zero_to_hundred():
    state = _FakeState()
    svc = UniverseService(object(), object(), StrategySettings())
    svc.state = state  # type: ignore[assignment]
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
    assert state.swing_rows
    assert state.intraday_rows
    for r in state.swing_rows:
        assert 0.0 <= float(r["score"]) <= 100.0
    for r in state.intraday_rows:
        assert 0.0 <= float(r["score"]) <= 100.0


def test_watchlist_v2_intraday_run_does_not_report_swing_selected():
    """When premarket=False, swingSelected in response must be 0 (swing output suppressed)."""
    svc = UniverseService(object(), object(), StrategySettings())
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
    # Intraday-only run: swingSelected reported as 0, swingComputed shows candidates were scored
    assert out["swingSelected"] == 0
    assert out["swingComputed"] >= 1
    assert out["intradaySelected"] >= 1


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
    state = _FakeState()
    svc = UniverseService(object(), object(), StrategySettings())
    svc.state = state  # type: ignore[assignment]
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
    assert len(state.intraday_rows) == 1
    row = state.intraday_rows[0]
    assert row["source"] == "PHASE1_DAILY_FALLBACK"
    assert row["phase2eligibility"] == "N"
    # Phase2 was entered but fell back; rejection summary should record LOW_SLOT_COVERAGE
    phase2_summary = out["intradayPhaseStats"]["phase2RejectionSummary"]
    assert phase2_summary.get("LOW_SLOT_COVERAGE", 0) >= 1


def test_watchlist_v2_phase2_eligibility_fails_when_today_bars_missing():
    svc = UniverseService(object(), object(), StrategySettings())
    bars = _intraday_5m_history_with_today(60, today_bars=0)
    chk = svc._phase2_eligibility(bars=bars, now_i=now_ist(), interval_min=5)
    assert chk["eligible"] is False
    assert chk["reason"] == "STALE_INTRADAY_CACHE"


def test_watchlist_v2_phase2_eligibility_passes_with_complete_baseline():
    state = _FakeState()
    svc = UniverseService(object(), object(), StrategySettings())
    svc.state = state  # type: ignore[assignment]
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
    assert len(state.intraday_rows) == 1
    row = state.intraday_rows[0]
    assert row["source"] == "PHASE2_INPLAY"
    assert row["phase2eligibility"] == "Y"
    # Phase2 ran with sufficient history; quality score should be positive
    assert out["intradayPhaseStats"]["phase2QualityScore"] >= 0.0


def test_watchlist_v2_phase2_diagnostics_present_when_phase2_runs():
    svc = UniverseService(object(), object(), StrategySettings())
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
    """When MarketBrain disables swing (PANIC/LOCKDOWN), swingSelected must be 0."""
    svc = UniverseService(object(), object(), StrategySettings())
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
    market_state = MarketBrainState(
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
    svc.market_brain_service = _MarketBrainStub(market_state, policy)  # type: ignore[assignment]
    svc._build_universe_v2_controls = lambda: _controls()  # type: ignore[method-assign]
    svc._build_watchlist_v2_regime = lambda timeframe, expected_lcd, now_i, premarket=False: {  # type: ignore[method-assign]
        "regimeDaily": "RISK_OFF",
        "regimeIntraday": "CHOPPY",
        "source": {},
    }
    svc._watchlist_v2_candidates = lambda expected_lcd: [dict(candidate)]  # type: ignore[method-assign]
    svc._watchlist_daily_candles = lambda row, expected_lcd: _daily_candles()  # type: ignore[method-assign]
    svc._watchlist_intraday_candles = lambda row, timeframe, now_i: _intraday_5m_history_with_today(60)  # type: ignore[method-assign]

    out = svc.build_watchlist(market_state, target_size=20, premarket=True, intraday_timeframe="5m")
    # Swing must be suppressed when MarketBrain policy is DISABLED
    assert out["swingSelected"] == 0
    assert out["marketPolicy"]["swing_permission"] == "DISABLED"


def test_watchlist_v2_correlation_guard_blocks_high_corr_duplicate():
    svc = UniverseService(object(), object(), StrategySettings())
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
    state = _FakeState(
        sector_mapping=[
            {
                "symbol": "AAA",
                "exchange": "NSE",
                "macroSector": "ECONOMY",
                "sector": "FINANCIALS",
                "industry": "BANKS",
                "basicIndustry": "PRIVATE BANKS",
                "source": "nse",
                "updatedAt": "2026-03-01 09:00:00",
            }
        ]
    )
    svc = UniverseService(object(), object(), StrategySettings())
    svc.state = state  # type: ignore[assignment]
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


def test_watchlist_v2_require_full_coverage_blocks_when_unclassified_enabled_rows_exist():
    svc = UniverseService(object(), object(), StrategySettings())
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
    assert out["selected"] == 0


def test_watchlist_v2_require_today_scored_blocks_when_today_coverage_incomplete():
    svc = UniverseService(object(), object(), StrategySettings())
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
    assert out["selected"] == 0
