from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from autotrader.domain.models import MarketBrainState, MarketPolicy, PositionSizing, RegimeSnapshot, UniverseRow, WatchlistRow
from autotrader.services.market_breadth_service import MarketBreadthService
from autotrader.services.market_brain_service import MarketBrainService
from autotrader.services.market_policy_service import MarketPolicyService
from autotrader.services.trading_service import TradingService
from autotrader.services.universe_service import UniverseService
from autotrader.services.universe_v2 import ModeThresholds, UniverseControls
from autotrader.settings import AppSettings, GcpSettings, GrowwSettings, RuntimeSettings, StrategySettings, UpstoxSettings
from autotrader.time_utils import IST, now_ist


def _daily_candles(n: int = 260, start_px: float = 100.0, step: float = 0.25) -> list[list[object]]:
    now_i = now_ist().astimezone(IST)
    d = now_i.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=n + 10)
    out: list[list[object]] = []
    px = start_px
    while len(out) < n:
        if d.weekday() < 5:
            o = px
            c = px + step
            h = max(o, c) + 0.4
            l = min(o, c) - 0.4
            v = 1_000_000 + len(out) * 1000
            out.append([d.isoformat(), o, h, l, c, v])
            px = c
        d += timedelta(days=1)
    return out


def _intraday_today_5m(n: int = 30, step: float = 0.08) -> list[list[object]]:
    now_i = now_ist().astimezone(IST)
    base = now_i.replace(hour=9, minute=15, second=0, microsecond=0)
    out: list[list[object]] = []
    px = 100.0
    for i in range(n):
        ts = base + timedelta(minutes=5 * i)
        o = px
        c = px + step
        h = max(o, c) + 0.06 + (i * 0.003)
        l = min(o, c) - 0.06
        v = 10_000 + i * 120
        out.append([ts.isoformat(), o, h, l, c, v])
        px = c
    return out


class _FakeState:
    def __init__(self):
        self._rows: dict[tuple[str, str], dict[str, Any]] = {}
        self._locks: set[str] = set()
        self._runtime: dict[str, str] = {}

    def get_json(self, collection: str, key: str) -> dict[str, Any] | None:
        return self._rows.get((collection, key))

    def set_json(self, collection: str, key: str, payload: dict[str, Any], merge: bool = True) -> None:
        del merge
        self._rows[(collection, key)] = dict(payload)

    def try_acquire_lock(self, name: str, ttl_seconds: int = 30):  # type: ignore[no-untyped-def]
        del ttl_seconds
        if name in self._locks:
            return None
        self._locks.add(name)
        return SimpleNamespace(name=name, owner="test")

    def release_lock(self, lease):  # type: ignore[no-untyped-def]
        self._locks.discard(getattr(lease, "name", ""))

    def get_runtime_prop(self, key: str, default: str = "") -> str:
        return self._runtime.get(key, default)

    def set_runtime_prop(self, key: str, value: str) -> None:
        self._runtime[key] = value


class _FakeGCS:
    def __init__(self):
        self._rows: dict[str, Any] = {}

    def write_json(self, path: str, data: Any) -> None:
        self._rows[path] = data

    def read_json(self, path: str, default: Any = None) -> Any:
        return self._rows.get(path, default)


class _FakeRegimeService:
    def __init__(self, vix: float = 16.0, chop_risk: float = 30.0, gap_pct: float = 0.4):
        self.vix = vix
        self.chop_risk = chop_risk
        self.gap_pct = gap_pct

    def get_market_regime(self) -> RegimeSnapshot:
        reg = RegimeSnapshot(regime="RANGE", bias="NEUTRAL", vix=float(self.vix))
        reg.nifty_structure.chop_risk = float(self.chop_risk)
        reg.nifty_structure.gap_pct = float(self.gap_pct)
        return reg


class _FakeUniverseForBrain:
    def __init__(self, rows: list[dict[str, Any]], regime_ctx: dict[str, Any], expected_lcd: str | None = None):
        self._rows = rows
        self._regime_ctx = regime_ctx
        self._expected_lcd = expected_lcd

    def _expected_latest_daily_candle_date(self, asof_i):  # type: ignore[no-untyped-def]
        if self._expected_lcd:
            y, m, d = [int(x) for x in self._expected_lcd.split("-")]
            return asof_i.replace(year=y, month=m, day=d).date()
        return (asof_i - timedelta(days=1)).date()

    def _watchlist_v2_candidates(self, expected_lcd: str) -> list[dict[str, Any]]:
        del expected_lcd
        return [dict(r) for r in self._rows]

    def _watchlist_daily_candles(self, row: dict[str, Any], expected_lcd: str) -> list[list[object]]:
        del expected_lcd
        return [list(x) for x in row.get("dailyCandles", _daily_candles())]

    def _watchlist_intraday_candles(self, row: dict[str, Any], timeframe: str, now_i):  # type: ignore[no-untyped-def]
        del timeframe, now_i
        return [list(x) for x in row.get("intradayCandles", _intraday_today_5m())]

    def _build_watchlist_v2_regime(self, timeframe: str, expected_lcd: str, now_i, premarket: bool = False):  # type: ignore[no-untyped-def]
        del timeframe, expected_lcd, now_i
        ctx = dict(self._regime_ctx)
        source = dict(ctx.get("source") or {})
        if premarket:
            source["intradaySource"] = "premarket_skip"
        ctx["source"] = source
        return ctx


def _make_brain_service(*, regime_ctx: dict[str, Any], rows: list[dict[str, Any]] | None = None, vix: float = 15.0) -> MarketBrainService:
    rows = rows or [
        {
            "symbol": "AAA",
            "exchange": "NSE",
            "enabled": True,
            "fresh": True,
            "eligibleSwing": True,
            "eligibleIntraday": True,
            "turnoverRank60D": 20,
            "liquidityBucket": "A",
            "turnoverMed60D": 2e8,
            "decisionPresent": True,
            "dailyCandles": _daily_candles(),
            "intradayCandles": _intraday_today_5m(),
        }
    ]
    return MarketBrainService(
        regime_service=_FakeRegimeService(vix=vix),
        universe_service=_FakeUniverseForBrain(rows=rows, regime_ctx=regime_ctx),
        gcs=_FakeGCS(),
        state=_FakeState(),
    )


def _baseline_regime_ctx() -> dict[str, Any]:
    return {
        "daily": {
            "close": 220.0,
            "ema50": 210.0,
            "ema200": 180.0,
            "atr14": 2.5,
            "atrPct": 0.011,
            "atrMedian252": 0.010,
            "trendUp": True,
            "trendDown": False,
            "highVol": False,
            "lowVol": False,
        },
        "intraday": {"vwapSlope": 0.0015, "rangeExpansion30m": 1.35, "bars": 24},
        "source": {
            "dailyKey": "NSE_INDEX|Nifty 50",
            "dailySource": "cache_only",
            "intradayKey": "",
            "intradaySource": "premarket_skip",
        },
    }


def test_market_brain_v2_strong_trend_maps_to_trend_up_aggressive(monkeypatch: pytest.MonkeyPatch):
    svc = _make_brain_service(regime_ctx=_baseline_regime_ctx(), vix=13.0)
    monkeypatch.setattr(svc, "compute_breadth_snapshot", lambda expected_lcd, rows: {"score": 78.0, "processedCount": 200})  # type: ignore[misc]
    monkeypatch.setattr(svc, "compute_leadership_snapshot", lambda expected_lcd, rows, now_i: {"score": 74.0, "leadersProcessed": 80})  # type: ignore[misc]
    monkeypatch.setattr(svc, "_compute_liquidity_health", lambda rows: (76.0, {}))  # type: ignore[misc]
    monkeypatch.setattr(svc, "_compute_volatility_stress", lambda regime_ctx, live_regime: (28.0, {}))  # type: ignore[misc]
    monkeypatch.setattr(svc, "_compute_data_quality", lambda rows, breadth, leadership, regime_ctx: (82.0, {}))  # type: ignore[misc]
    state = svc.build_post_open_market_brain(now_ist().isoformat())
    assert state.regime == "TREND_UP"
    assert state.risk_mode == "AGGRESSIVE"


def test_market_brain_v2_weak_trend_maps_to_trend_down(monkeypatch: pytest.MonkeyPatch):
    ctx = _baseline_regime_ctx()
    ctx["daily"]["trendUp"] = False
    ctx["daily"]["trendDown"] = True
    ctx["daily"]["close"] = 150.0
    ctx["daily"]["ema50"] = 170.0
    ctx["daily"]["ema200"] = 190.0
    svc = _make_brain_service(regime_ctx=ctx, vix=19.0)
    monkeypatch.setattr(svc, "compute_breadth_snapshot", lambda expected_lcd, rows: {"score": 32.0, "processedCount": 140})  # type: ignore[misc]
    monkeypatch.setattr(svc, "compute_leadership_snapshot", lambda expected_lcd, rows, now_i: {"score": 34.0, "leadersProcessed": 70})  # type: ignore[misc]
    monkeypatch.setattr(svc, "_compute_liquidity_health", lambda rows: (52.0, {}))  # type: ignore[misc]
    monkeypatch.setattr(svc, "_compute_volatility_stress", lambda regime_ctx, live_regime: (64.0, {}))  # type: ignore[misc]
    monkeypatch.setattr(svc, "_compute_data_quality", lambda rows, breadth, leadership, regime_ctx: (78.0, {}))  # type: ignore[misc]
    state = svc.build_post_open_market_brain(now_ist().isoformat())
    assert state.regime == "TREND_DOWN"


def test_market_brain_v2_chop_environment_maps_to_chop(monkeypatch: pytest.MonkeyPatch):
    svc = _make_brain_service(regime_ctx=_baseline_regime_ctx(), vix=21.0)
    monkeypatch.setattr(svc, "_compute_trend_score", lambda regime_ctx: 44.0)  # type: ignore[misc]
    monkeypatch.setattr(svc, "compute_breadth_snapshot", lambda expected_lcd, rows: {"score": 50.0, "processedCount": 120})  # type: ignore[misc]
    monkeypatch.setattr(svc, "compute_leadership_snapshot", lambda expected_lcd, rows, now_i: {"score": 35.0, "leadersProcessed": 70})  # type: ignore[misc]
    monkeypatch.setattr(svc, "_compute_liquidity_health", lambda rows: (60.0, {}))  # type: ignore[misc]
    monkeypatch.setattr(svc, "_compute_volatility_stress", lambda regime_ctx, live_regime: (70.0, {}))  # type: ignore[misc]
    monkeypatch.setattr(svc, "_compute_data_quality", lambda rows, breadth, leadership, regime_ctx: (72.0, {}))  # type: ignore[misc]
    state = svc.build_post_open_market_brain(now_ist().isoformat())
    assert state.regime == "CHOP"
    assert state.risk_mode == "DEFENSIVE"


def test_market_brain_v2_panic_maps_to_lockdown(monkeypatch: pytest.MonkeyPatch):
    svc = _make_brain_service(regime_ctx=_baseline_regime_ctx(), vix=26.0)
    monkeypatch.setattr(svc, "_compute_trend_score", lambda regime_ctx: 30.0)  # type: ignore[misc]
    monkeypatch.setattr(svc, "compute_breadth_snapshot", lambda expected_lcd, rows: {"score": 12.0, "processedCount": 90})  # type: ignore[misc]
    monkeypatch.setattr(svc, "compute_leadership_snapshot", lambda expected_lcd, rows, now_i: {"score": 22.0, "leadersProcessed": 55})  # type: ignore[misc]
    monkeypatch.setattr(svc, "_compute_liquidity_health", lambda rows: (38.0, {}))  # type: ignore[misc]
    monkeypatch.setattr(svc, "_compute_volatility_stress", lambda regime_ctx, live_regime: (90.0, {}))  # type: ignore[misc]
    monkeypatch.setattr(svc, "_compute_data_quality", lambda rows, breadth, leadership, regime_ctx: (60.0, {}))  # type: ignore[misc]
    state = svc.build_post_open_market_brain(now_ist().isoformat())
    assert state.regime == "PANIC"
    assert state.risk_mode == "LOCKDOWN"


def test_market_brain_v2_recovery_after_panic(monkeypatch: pytest.MonkeyPatch):
    svc = _make_brain_service(regime_ctx=_baseline_regime_ctx(), vix=17.0)
    prev = MarketBrainState(
        asof_ts=(now_ist() - timedelta(minutes=20)).isoformat(),
        phase="LIVE",
        regime="PANIC",
        participation="WEAK",
        risk_mode="LOCKDOWN",
        intraday_state="EVENT_RISK",
        long_bias=0.2,
        short_bias=0.8,
        size_multiplier=0.3,
        max_positions_multiplier=0.3,
        swing_permission="DISABLED",
        allowed_strategies=["MEAN_REVERSION"],
        reasons=["prev panic"],
        trend_score=20.0,
        breadth_score=12.0,
        leadership_score=20.0,
        volatility_stress_score=92.0,
        liquidity_health_score=35.0,
        data_quality_score=55.0,
    )
    svc.persist_market_brain_state(prev, context={"regimeContext": {"source": {"dailySource": "cache_only"}}}, policy=svc.derive_market_policy(prev))
    monkeypatch.setattr(svc, "_compute_trend_score", lambda regime_ctx: 58.0)  # type: ignore[misc]
    monkeypatch.setattr(svc, "compute_breadth_snapshot", lambda expected_lcd, rows: {"score": 55.0, "processedCount": 120})  # type: ignore[misc]
    monkeypatch.setattr(svc, "compute_leadership_snapshot", lambda expected_lcd, rows, now_i: {"score": 57.0, "leadersProcessed": 80})  # type: ignore[misc]
    monkeypatch.setattr(svc, "_compute_liquidity_health", lambda rows: (61.0, {}))  # type: ignore[misc]
    monkeypatch.setattr(svc, "_compute_volatility_stress", lambda regime_ctx, live_regime: (48.0, {}))  # type: ignore[misc]
    monkeypatch.setattr(svc, "_compute_data_quality", lambda rows, breadth, leadership, regime_ctx: (74.0, {}))  # type: ignore[misc]
    state = svc.build_post_open_market_brain(now_ist().isoformat())
    assert state.regime == "RECOVERY"


def test_market_brain_v2_stale_data_forces_defensive(monkeypatch: pytest.MonkeyPatch):
    svc = _make_brain_service(regime_ctx=_baseline_regime_ctx(), vix=14.0)
    monkeypatch.setattr(svc, "compute_breadth_snapshot", lambda expected_lcd, rows: {"score": 74.0, "processedCount": 200})  # type: ignore[misc]
    monkeypatch.setattr(svc, "compute_leadership_snapshot", lambda expected_lcd, rows, now_i: {"score": 72.0, "leadersProcessed": 100})  # type: ignore[misc]
    monkeypatch.setattr(svc, "_compute_liquidity_health", lambda rows: (76.0, {}))  # type: ignore[misc]
    monkeypatch.setattr(svc, "_compute_volatility_stress", lambda regime_ctx, live_regime: (42.0, {}))  # type: ignore[misc]
    monkeypatch.setattr(svc, "_compute_data_quality", lambda rows, breadth, leadership, regime_ctx: (42.0, {}))  # type: ignore[misc]
    state = svc.build_post_open_market_brain(now_ist().isoformat())
    assert state.risk_mode == "DEFENSIVE"
    assert state.risk_mode != "AGGRESSIVE"


def test_market_breadth_uses_liquidity_qualified_universe_only():
    svc = MarketBreadthService(liquidity_turnover_rank_max=500, min_bars=30)
    lcd = (now_ist() - timedelta(days=1)).strftime("%Y-%m-%d")
    rows = [
        {
            "symbol": "AAA",
            "enabled": True,
            "fresh": True,
            "eligibleSwing": True,
            "eligibleIntraday": False,
            "turnoverRank60D": 10,
            "liquidityBucket": "A",
            "sector": "IT",
        },
        {
            "symbol": "BAD",
            "enabled": False,
            "fresh": True,
            "eligibleSwing": True,
            "eligibleIntraday": True,
            "turnoverRank60D": 1,
            "liquidityBucket": "A",
            "sector": "BANK",
        },
    ]

    def _fetch(row: dict[str, Any], expected_lcd: str) -> list[list[object]]:
        del expected_lcd
        if row["symbol"] == "AAA":
            return _daily_candles(n=80, start_px=100.0, step=0.4)
        return _daily_candles(n=80, start_px=200.0, step=-0.5)

    out = svc.compute_breadth_snapshot(universe_rows=rows, expected_lcd=lcd, daily_candle_fetcher=_fetch)
    assert out["qualifiedCount"] == 1
    assert out["processedCount"] == 1


def test_market_policy_size_and_max_positions_multipliers():
    policy_svc = MarketPolicyService()
    state = MarketBrainState(
        asof_ts=now_ist().isoformat(),
        regime="TREND_UP",
        risk_mode="AGGRESSIVE",
        size_multiplier=1.2,
        max_positions_multiplier=1.3,
    )
    pos = PositionSizing(qty=100, sl_price=95.0, target=110.0, sl_dist=5.0, entry_price=100.0, max_loss=500.0, max_gain=750.0, brokerage=25.0)
    resized = policy_svc.size_position_with_market_brain(pos, state, StrategySettings(), setup_confidence_multiplier=1.0, liquidity_multiplier=1.0, data_quality_multiplier=1.0)
    assert resized.qty > pos.qty
    assert policy_svc.max_positions_limit(4, state) == 5


def test_market_brain_premarket_no_lookahead_and_post_open_live_allowed():
    pre_ctx = _baseline_regime_ctx()
    pre_ctx["source"]["dailySource"] = "cache_only"
    pre_ctx["source"]["intradaySource"] = "premarket_skip"
    svc = _make_brain_service(regime_ctx=pre_ctx, vix=15.0)
    pre_state = svc.build_premarket_market_brain(now_ist().isoformat())
    assert pre_state.phase == "PREMARKET"
    assert svc.validate_no_lookahead_market_brain(pre_state) is True

    post_ctx = _baseline_regime_ctx()
    post_ctx["source"]["dailySource"] = "upstox_api"
    post_ctx["source"]["intradaySource"] = "upstox_api"
    svc2 = _make_brain_service(regime_ctx=post_ctx, vix=16.0)
    post_state = svc2.build_post_open_market_brain(now_ist().isoformat())
    assert post_state.phase in {"POST_OPEN", "LIVE", "EOD"}
    assert svc2.validate_no_lookahead_market_brain(post_state) is True


def test_watchlist_and_scanner_use_same_canonical_state(monkeypatch: pytest.MonkeyPatch):
    fixed_state = MarketBrainState(
        asof_ts=now_ist().isoformat(),
        phase="LIVE",
        regime="RANGE",
        participation="MODERATE",
        risk_mode="NORMAL",
        intraday_state="CHOP_DAY",
        long_bias=0.55,
        short_bias=0.45,
        size_multiplier=1.0,
        max_positions_multiplier=1.0,
        swing_permission="ENABLED",
        allowed_strategies=["MEAN_REVERSION", "VWAP_REVERSAL"],
        reasons=["test"],
        trend_score=52.0,
        breadth_score=54.0,
        leadership_score=50.0,
        volatility_stress_score=48.0,
        liquidity_health_score=62.0,
        data_quality_score=70.0,
    )
    fixed_policy = MarketPolicyService().derive_market_policy(fixed_state)

    class _FakeMB:
        def __init__(self):
            self.policy_service = MarketPolicyService()

        def build_post_open_market_brain(self, asof_ts: str) -> MarketBrainState:
            del asof_ts
            return fixed_state

        def derive_market_policy(self, state: MarketBrainState) -> MarketPolicy:
            assert state is fixed_state
            return fixed_policy

        def watchlist_regime_payload(self, state: MarketBrainState) -> dict[str, Any]:
            assert state is fixed_state
            return {
                "regimeDaily": "RANGE",
                "regimeIntraday": "CHOPPY",
                "daily": {},
                "intraday": {},
                "source": {"dailyKey": "NSE_INDEX|Nifty 50", "dailySource": "cache_only", "intradayKey": "", "intradaySource": "premarket_skip"},
            }

        def adjust_watchlist_rows(self, rows: list[dict[str, Any]], policy: MarketPolicy, *, section: str) -> list[dict[str, Any]]:
            del policy, section
            return rows

        def align_legacy_regime(self, regime: RegimeSnapshot, state: MarketBrainState) -> RegimeSnapshot:
            assert state is fixed_state
            return regime

        def adjust_signal(self, signal_score: int, state: MarketBrainState) -> int:
            assert state is fixed_state
            return signal_score

        def size_position_with_market_brain(self, position_sizing: PositionSizing, state: MarketBrainState, cfg: StrategySettings, **kwargs):  # type: ignore[no-untyped-def]
            del cfg, kwargs
            assert state is fixed_state
            return position_sizing

    class _WatchlistSheets:
        def __init__(self):
            self.swing_rows: list[list[object]] = []
            self.intraday_rows: list[list[object]] = []

        def replace_watchlist_swing_v2(self, rows: list[list[object]]) -> None:
            self.swing_rows = list(rows)

        def replace_watchlist_intraday_v2(self, rows: list[list[object]]) -> None:
            self.intraday_rows = list(rows)

        def ensure_sheet_headers_append(self, sheet_name: str, required_headers: list[str], header_row: int = 3) -> dict[str, int]:
            del sheet_name, required_headers, header_row
            return {}

        def read_sheet_rows(self, sheet_name: str, start_row: int = 4) -> list[list[str]]:
            del sheet_name, start_row
            return []

    watchlist_sheets = _WatchlistSheets()
    uni = UniverseService(watchlist_sheets, object(), object(), StrategySettings())
    uni.set_market_brain_service(_FakeMB())
    expected_lcd = (now_ist() - timedelta(days=1)).strftime("%Y-%m-%d")
    candidate = {
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
    controls = UniverseControls(
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
    uni._build_universe_v2_controls = lambda: controls  # type: ignore[method-assign]
    uni._watchlist_v2_candidates = lambda expected_lcd: [dict(candidate)]  # type: ignore[method-assign]
    uni._watchlist_daily_candles = lambda row, expected_lcd: _daily_candles()  # type: ignore[method-assign]
    uni._watchlist_intraday_candles = lambda row, timeframe, now_i: _intraday_today_5m()  # type: ignore[method-assign]
    uni._load_sector_mapping_dataset = lambda rows, include_meta=False: ({}, 0.0) if not include_meta else ({}, 0.0, {}, {"eligible_universe_count": 1, "mapped_count": 0, "unmapped_count": 1, "coverage_pct": 0.0, "source_breakdown_counts": {"sheet": 0, "gcs": 0, "universe_fallback": 0, "unknown": 1}})  # type: ignore[method-assign]
    uni._select_with_diversification_and_corr = lambda rows, target, sector_coverage_pct, seed=None: list(rows)[:target]  # type: ignore[method-assign]
    uni._phase2_eligibility = lambda bars, now_i, interval_min: {"eligible": True, "phase2BaselineCoveragePct": 100.0, "baselineMedianVolume": 10000.0}  # type: ignore[method-assign]
    uni._watchlist_volume_shock = lambda bars, now_i, baseline_override=0.0: (1.4, 0.9)  # type: ignore[method-assign]
    uni._watchlist_orb_signal = lambda today_bars, now_i: ("UP_BREAK", 0.8)  # type: ignore[method-assign]
    uni._watchlist_reversal_signal = lambda bars, regime_intraday, now_i: (0.2, 0.1)  # type: ignore[method-assign]

    watchlist_out = uni.build_watchlist(None, target_size=5, premarket=False, intraday_timeframe="5m")
    assert watchlist_out["marketBrainState"]["regime"] == fixed_state.regime

    class _TradeSheets:
        def __init__(self):
            self.scan_rows: list[list[Any]] = []
            self.signal_rows: list[list[Any]] = []

        def write_market_brain(self, regime: RegimeSnapshot) -> None:
            del regime

        def write_market_brain_v2(self, state: MarketBrainState, policy: MarketPolicy) -> None:
            assert state is fixed_state
            assert policy.regime == fixed_state.regime

        def read_watchlist(self) -> list[WatchlistRow]:
            return [WatchlistRow(symbol="AAA", exchange="NSE", segment="CASH", product="CNC", strategy="AUTO")]

        def read_universe_rows(self) -> list[UniverseRow]:
            return [UniverseRow(row_number=4, symbol="AAA", exchange="NSE", segment="CASH", instrument_key="NSE_EQ|AAA")]

        def replace_scan_rows(self, rows: list[list[Any]]) -> None:
            self.scan_rows = list(rows)

        def append_signals(self, rows: list[list[Any]]) -> None:
            self.signal_rows = list(rows)

    class _OrderSvc:
        def reconcile_pending_entries(self, max_items: int = 15) -> dict[str, int]:
            del max_items
            return {"filled": 0, "failed": 0}

        def place_entry_order(self, **kwargs):  # type: ignore[no-untyped-def]
            return {"ok": True, **kwargs}

    class _Sink:
        def action(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            return None

        def decision(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            return None

        def log(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            return None

        def flush_all(self) -> None:
            return None

    settings = AppSettings(
        gcp=GcpSettings(project_id="p", region="r", spreadsheet_id="s", bucket_name="b"),
        upstox=UpstoxSettings(
            api_v2_host="https://api.upstox.com/v2",
            api_v3_host="https://api.upstox.com/v3",
            client_id_secret_name="a",
            client_secret_secret_name="b",
            access_token_secret_name="c",
            access_token_expiry_secret_name="d",
        ),
        groww=GrowwSettings(
            api_host="https://api.groww.in",
            api_key_secret_name="a",
            api_secret_secret_name="b",
            access_token_secret_name="c",
            access_token_expiry_secret_name="d",
        ),
        runtime=RuntimeSettings(paper_trade=True, job_trigger_token="tok", log_level="INFO"),
        strategy=StrategySettings(min_signal_score=10, max_positions=4),
    )
    trade = TradingService(
        settings=settings,
        sheets=_TradeSheets(),
        state=_FakeState(),
        gcs=object(),
        groww=object(),
        upstox=object(),
        regime_service=_FakeRegimeService(),
        market_brain_service=_FakeMB(),
        order_service=_OrderSvc(),
        log_sink=_Sink(),
    )
    monkeypatch.setattr("autotrader.services.trading_service.is_market_open_ist", lambda: True)
    monkeypatch.setattr("autotrader.services.trading_service.is_entry_window_open_ist", lambda: True)
    monkeypatch.setattr("autotrader.services.trading_service.compute_indicators", lambda candles, cfg: SimpleNamespace(  # type: ignore[misc]
        close=100.0,
        prev_close=99.0,
        volume=SimpleNamespace(curr=10000, ratio=1.2),
        ema_stack=True,
        ema_flip=False,
        macd=SimpleNamespace(crossed="BUY", hist=0.5),
        rsi=SimpleNamespace(curr=58.0),
        supertrend=SimpleNamespace(dir=1),
        atr=2.0,
        open=99.5,
        vwap=99.8,
    ))
    monkeypatch.setattr("autotrader.services.trading_service.determine_direction", lambda ind, regime: "BUY")
    monkeypatch.setattr("autotrader.services.trading_service.score_signal", lambda symbol, direction, ind, regime, cfg: SimpleNamespace(score=82, breakdown=SimpleNamespace(options=20, technical=40, volume=15)))
    monkeypatch.setattr("autotrader.services.trading_service.calc_position_size", lambda entry_price, atr, direction, cfg: PositionSizing(qty=10, sl_price=98.0, target=104.0, sl_dist=2.0, entry_price=100.0, max_loss=20.0, max_gain=40.0, brokerage=1.0))
    monkeypatch.setattr(trade, "_fetch_candles", lambda symbol, exchange, segment, instrument_key="", timeframe="15m", lookback_days=8: [[1, 2, 3, 4, 5, 6]] * 120)

    scan_out = trade.run_scan_once(force=True)
    assert scan_out["marketBrainRegime"] == watchlist_out["marketBrainState"]["regime"]


def test_market_brain_v2_data_quality_penalizes_live_phase2_absence_and_stale_writers(monkeypatch: pytest.MonkeyPatch):
    svc = _make_brain_service(regime_ctx=_baseline_regime_ctx(), vix=16.0)
    now_i = now_ist().astimezone(IST)
    svc.state.set_runtime_prop("runtime:watchlist_last_run_ts", (now_i - timedelta(minutes=65)).isoformat())
    svc.state.set_runtime_prop("runtime:signals_last_write_ts", (now_i - timedelta(minutes=135)).isoformat())
    svc.state.set_runtime_prop("runtime:watchlist_last_phase2_eligible_count", "0")
    svc.state.set_runtime_prop("runtime:watchlist_last_phase2_branch_entered", "Y")
    monkeypatch.setattr(svc, "_phase_from_clock", lambda now_i: "LIVE")  # type: ignore[misc]

    score, ctx = svc._compute_data_quality(
        rows=[
            {"fresh": True, "decisionPresent": True},
            {"fresh": True, "decisionPresent": True},
            {"fresh": False, "decisionPresent": True},
        ],
        breadth={"processedCount": 160},
        leadership={"leadersProcessed": 72},
        regime_ctx={"intraday": {"bars": 26}},
    )
    assert float(ctx["intradayPhase2Penalty"]) > 0.0
    assert float(ctx["staleWriterPenalty"]) > 0.0
    assert float(ctx["pipelineAlignmentPenalty"]) > 0.0
    assert float(score) < float(ctx["baseQualityScore"])


def test_market_brain_v2_liquidity_health_refinement_avoids_easy_saturation():
    svc = _make_brain_service(regime_ctx=_baseline_regime_ctx(), vix=15.0)
    rows = []
    for i in range(30):
        rows.append(
            {
                "fresh": True,
                "eligibleIntraday": True,
                "eligibleSwing": True,
                "turnoverRank60D": i + 1,
                "liquidityBucket": "A",
                "turnoverMed60D": float(9.0e8 - (i * 2.5e7)),
            }
        )
    score, ctx = svc._compute_liquidity_health(rows)
    assert 0.0 <= score <= 100.0
    assert score < 99.0
    assert "candidateTurnoverPercentiles" in ctx
    assert "top5LiquidityConcentrationPct" in ctx
    assert "liquidityDistributionEntropy" in ctx
