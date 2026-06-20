"""Unit tests for corp_action_signal_service.build_candidates — the pure corp-action
selection core (entry timing, gates, eq-weight market-adjusted anti-pump). Synthetic
fixtures; the live NSE fetch is the thin I/O wrapper (smoke-tested separately)."""
from autotrader.services import corp_action_signal_service as svc

N = 90
DD = [f"{i:04d}" for i in range(N)]   # sortable synthetic dates


def _bars(c79=100.0, c59=100.0, low=50.0, vol=2e6):
    return {"BONUSCO": [[DD[i], 100.0, 100.0, low,
                         (c79 if i == 79 else c59 if i == 59 else 100.0), vol] for i in range(N)]}


def _event(typ="bonus", intim_i=79, meeting_i=85, hour=10, lead=6):
    return [{"symbol": "BONUSCO", "type": typ, "intim_dt": DD[intim_i],
             "meeting_dt": DD[meeting_i], "intim_hour": hour, "lead": lead}]


def _run(candles=None, events=None, market=None, prior=None, last=DD[79]):
    # last_session = the intimation session DD[79]; entry (next open) is DD[80]
    return svc.build_candidates(last, events or _event(), candles or _bars(),
                                market if market is not None else {}, prior or set())


def test_happy_path_emits_candidate():
    out = _run()
    assert len(out) == 1
    c = out[0]
    assert c["symbol"] == "BONUSCO" and c["event_type"] == "bonus"
    assert c["channel"] == "pead" and c["wl_type"] == "corp_action" and c["strategy"] == "CORP_ACTION"
    assert c["meeting_date"] == DD[85]
    assert c["dist_low"] == 2.0  # 100 / 50


def test_rejects_serial_repeat():
    assert _run(prior={("BONUSCO", "bonus")}) == []


def test_rejects_stale_intimation():
    # last_session != the intimation's session -> entry already passed -> no candidate
    assert _run(last=DD[80]) == []


def test_rejects_non_bonus_split():
    assert _run(events=_event(typ="buyback")) == []
    assert _run(events=_event(typ="dividend")) == []


def test_rejects_not_uptrend():
    # close 60 / low 50 = 1.2 < 1.40
    assert _run(candles=_bars(c79=60.0, c59=60.0)) == []


def test_rejects_pumped_raw():
    # raw run-up 130/100-1 = 0.30 >= 0.06, no market offset
    assert _run(candles=_bars(c79=130.0, c59=100.0)) == []


def test_eqweight_adjustment_rescues_market_driven_runup():
    # stock +10% but market +8% -> excess +2% < 0.06 -> passes (anti-pump uses EXCESS)
    mkt = {DD[59]: 1.0, DD[79]: 1.08}
    out = _run(candles=_bars(c79=110.0, c59=100.0), market=mkt)
    assert len(out) == 1


def test_eqweight_adjustment_keeps_true_pump_blocked():
    # stock +10%, market flat -> excess +10% >= 0.06 -> blocked
    mkt = {DD[59]: 1.0, DD[79]: 1.0}
    assert _run(candles=_bars(c79=110.0, c59=100.0), market=mkt) == []


def test_rejects_illiquid():
    assert _run(candles=_bars(vol=1e3)) == []


def test_rejects_bad_lead():
    assert _run(events=_event(lead=3)) == []     # too short
    assert _run(events=_event(lead=16)) == []    # stale / rescheduled


def test_ranking_by_uptrend_desc():
    # vary the 52w LOW (not the close, which would trip anti-pump): both flat closes ->
    # raw run-up 0; A low=50 -> dist 2.0, B low=40 -> dist 2.5 -> B ranks first
    candles = {
        "A": [[DD[i], 100.0, 100.0, 50.0, 100.0, 2e6] for i in range(N)],
        "B": [[DD[i], 100.0, 100.0, 40.0, 100.0, 2e6] for i in range(N)],
    }
    events = [
        {"symbol": "A", "type": "bonus", "intim_dt": DD[79], "meeting_dt": DD[85], "intim_hour": 10, "lead": 6},
        {"symbol": "B", "type": "split", "intim_dt": DD[79], "meeting_dt": DD[85], "intim_hour": 10, "lead": 6},
    ]
    out = svc.build_candidates(DD[79], events, candles, {}, set())
    assert [c["symbol"] for c in out] == ["B", "A"]  # B further above its low -> ranked first


def test_build_market_level_basic():
    # needs >= MARKET_MIN_STOCKS (50) per day and >= MARKET_MIN_HISTORY (300) bars/name
    md = [f"{i:04d}" for i in range(310)]
    uni = {f"S{j}": [[md[i], 100.0, 100.0, 100.0, 100.0, 1e6] for i in range(310)] for j in range(60)}
    lvl = svc.build_market_level(uni, md[309])
    assert lvl                                   # non-empty eq-weight index built
    assert abs(next(iter(lvl.values())) - 1.0) < 1e-6   # all-flat universe -> level ~1.0


def test_build_market_level_too_few_stocks_empty():
    md = [f"{i:04d}" for i in range(310)]
    uni = {f"S{j}": [[md[i], 100.0, 100.0, 100.0, 100.0, 1e6] for i in range(310)] for j in range(10)}
    assert svc.build_market_level(uni, md[309]) == {}   # < 50 stocks/day -> no valid index point
