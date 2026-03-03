from __future__ import annotations

from datetime import datetime, timedelta

from autotrader.adapters.sheets_repository import GoogleSheetsRepository, SheetNames
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

    def replace_watchlist_swing_v2(self, rows: list[list[object]]) -> None:
        self.swing_rows = list(rows)

    def replace_watchlist_intraday_v2(self, rows: list[list[object]]) -> None:
        self.intraday_rows = list(rows)


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


def test_watchlist_v2_score_bounds_zero_to_hundred():
    sheets = _FakeSheets()
    svc = UniverseService(sheets, object(), object(), StrategySettings())
    now_i = now_ist()
    expected_lcd = (now_i - timedelta(days=1)).strftime("%Y-%m-%d")

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

    out = svc.build_watchlist(None, target_size=20, premarket=False, intraday_timeframe="5m")
    assert out["ready"] is True
    assert sheets.swing_rows
    assert sheets.intraday_rows
    for r in sheets.swing_rows:
        assert 0.0 <= float(r[6]) <= 100.0
    for r in sheets.intraday_rows:
        assert 0.0 <= float(r[7]) <= 100.0


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
    expected_lcd = (now_i - timedelta(days=1)).strftime("%Y-%m-%d")

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
    expected_lcd = (now_i - timedelta(days=1)).strftime("%Y-%m-%d")

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
