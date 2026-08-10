"""Phase 0 dashboard per-channel endpoints — unit tests for the pure rollup.

Covers `build_channel_overview` (the cockpit data) + `_position_channel`
routing. The endpoints themselves are thin I/O wrappers (smoke-verified live).
"""
from __future__ import annotations

from autotrader.web.dashboard_api import (
    build_channel_overview,
    _position_channel,
    _CHANNELS,
)

CAP = {"swing": 500000.0, "intraday": 100000.0, "pead": 200000.0,
       "gap_fade": 100000.0, "core": 300000.0}
MAXP = {"swing": 5, "intraday": 3, "pead": 5, "gap_fade": 3, "core": None}


def _cap(ch): return CAP.get(ch, 0.0)
def _maxp(ch): return MAXP.get(ch)


def test_position_channel_routing():
    assert _position_channel({"channel": "core"}) == "core"
    assert _position_channel({"channel": "GAP_FADE"}) == "gap_fade"   # normalized
    assert _position_channel({"wl_type": "swing"}) == "swing"          # falls back to wl_type
    assert _position_channel({"channel": "pead", "wl_type": "corp_action"}) == "pead"  # channel wins
    assert _position_channel({}) == "intraday"                         # legacy default


def test_overview_rollup_and_totals():
    positions = ([{"channel": "core", "symbol": f"C{i}"} for i in range(30)]
                 + [{"channel": "swing", "symbol": s} for s in ("SAIL", "CROMPTON", "JAYNECOIND")])
    pnl = {"swing": -200.0}
    risk = {"swing": 4500.0, "core": 0.0}
    out = build_channel_overview(_CHANNELS, positions, pnl, risk, _cap, _maxp, 0.03, 0.06)
    rows = {r["channel"]: r for r in out["channels"]}

    assert set(rows) == set(_CHANNELS)
    assert rows["core"]["open_positions"] == 30
    assert rows["core"]["capital"] == 300000.0 and rows["core"]["enabled"] is True
    assert rows["core"]["max_positions"] is None              # CORE is a basket, not slot-capped
    assert rows["swing"]["open_positions"] == 3
    assert rows["swing"]["today_pnl"] == -200.0 and rows["swing"]["open_risk"] == 4500.0
    assert rows["swing"]["max_positions"] == 5
    assert "SAIL" in rows["swing"]["open_symbols"]
    # totals
    assert out["totals"]["capital"] == 1200000.0             # Rs12L across 5 funded channels
    assert out["totals"]["open_positions"] == 33


def test_breaker_loss_and_profit_limits():
    # swing capital 500000 -> loss_limit -3% = -15000, profit_limit 6% = 30000
    loss = build_channel_overview(["swing"], [], {"swing": -15000.0}, {}, _cap, _maxp, 0.03, 0.06)["channels"][0]
    assert loss["daily_loss_limit"] == -15000.0 and loss["daily_profit_limit"] == 30000.0
    assert loss["breaker_tripped"] is True and loss["breaker_reason"] == "daily_loss_limit_hit"

    profit = build_channel_overview(["swing"], [], {"swing": 30000.0}, {}, _cap, _maxp, 0.03, 0.06)["channels"][0]
    assert profit["breaker_tripped"] is True and profit["breaker_reason"] == "daily_profit_target_hit"

    calm = build_channel_overview(["swing"], [], {"swing": -5000.0}, {}, _cap, _maxp, 0.03, 0.06)["channels"][0]
    assert calm["breaker_tripped"] is False and calm["breaker_reason"] is None


def test_unfunded_channel_disabled_and_never_trips():
    out = build_channel_overview(["corp_action"], [], {"corp_action": -99999.0}, {},
                                 lambda ch: 0.0, _maxp, 0.03, 0.06)["channels"][0]
    assert out["enabled"] is False
    assert out["breaker_tripped"] is False         # capital 0 -> no breaker
    assert out["daily_loss_limit"] == 0.0


def test_overall_pnl_realized_plus_unrealized():
    # delivery: 3 closed (realized net -2657 over 3 trades, 2 wins) + 2 open marked +1898 unrealized
    positions = [{"channel": "delivery", "symbol": "KIMS", "entry_price": 800.0, "qty": 50},
                 {"channel": "delivery", "symbol": "TIMKEN", "entry_price": 3000.0, "qty": 12}]
    realized = {"delivery": {"realized": -2657.0, "closed": 5, "wins": 3},
                "core": {"realized": 0.0, "closed": 0, "wins": 0}}
    unreal = {"delivery": 1898.0}
    out = build_channel_overview(["delivery", "core"], positions, {}, {}, _cap, _maxp, 0.03, 0.06,
                                 realized_by_channel=realized, unrealized_by_channel=unreal)
    d = {r["channel"]: r for r in out["channels"]}["delivery"]
    assert d["realized_pnl"] == -2657.0
    assert d["unrealized_pnl"] == 1898.0
    assert d["overall_pnl"] == -759.0                     # realized + unrealized
    assert d["closed_trades"] == 5 and d["win_rate"] == 60.0
    assert d["open_value"] == 800.0 * 50 + 3000.0 * 12    # cost basis of the 2 open
    # core: no closed trades -> win_rate None, overall == unrealized (0 here)
    core = {r["channel"]: r for r in out["channels"]}["core"]
    assert core["win_rate"] is None and core["closed_trades"] == 0
    # totals carry the new aggregates
    assert out["totals"]["overall_pnl"] == -759.0
    assert out["totals"]["realized_pnl"] == -2657.0 and out["totals"]["unrealized_pnl"] == 1898.0


def test_today_move_mtm_by_channel():
    # today's MTM move (open book marked last vs prev close): delivery +500, momentum -200;
    # realized-from-exits stays 0 (hold book, nothing closed today) -> today_move is the real daily P&L
    positions = [{"channel": "delivery", "symbol": "KIMS", "entry_price": 800.0, "qty": 50},
                 {"channel": "momentum", "symbol": "LUPIN", "entry_price": 2000.0, "qty": 10}]
    move = {"delivery": 500.0, "momentum": -200.0}
    out = build_channel_overview(["delivery", "momentum"], positions, {}, {}, _cap, _maxp, 0.03, 0.06,
                                 daily_move_by_channel=move)
    d = {r["channel"]: r for r in out["channels"]}
    assert d["delivery"]["today_move"] == 500.0
    assert d["momentum"]["today_move"] == -200.0
    assert out["totals"]["today_move"] == 300.0                    # summed across channels


def test_today_move_defaults_zero_when_omitted():
    # back-compat: no daily_move_by_channel -> today_move is 0 everywhere (never KeyError)
    out = build_channel_overview(["delivery"],
                                 [{"channel": "delivery", "symbol": "KIMS", "entry_price": 800.0, "qty": 50}],
                                 {}, {}, _cap, _maxp, 0.03, 0.06)
    assert out["channels"][0]["today_move"] == 0.0
    assert out["totals"]["today_move"] == 0.0


# ── candles_1d retirement: GCS is the PRIMARY 1d source (2026-08-07) ──────────
def test_gcs_candles_1d_parses_and_windows():
    """`_gcs_candles_1d` (now primary for 1d) returns BQ-shaped rows and honours the
    date window. Guards the retirement of the cold BQ candles_1d table: the old
    BQ-primary order silently truncated 1d charts at ~2026-04 because its GCS fallback
    only fired on a completely empty result, and the table still holds 1.19M old rows."""
    from autotrader.web.dashboard_api import _gcs_candles_1d

    class _GCS:
        """Legacy by-symbol file is FROZEN (last bar 2026-02-26, as in prod); the
        instrument-key file is FRESH. Reading legacy first would serve a stale chart."""
        @staticmethod
        def score_cache_1d_path_by_instrument_key(ik, exch, seg):
            safe = ik.replace("|", "_")
            return f"cache/score_1d_by_instrument/{exch}/{seg}/{safe}.json"

        def read_candles(self, path):
            if path == "cache/score_1d_by_instrument/NSE/CASH/NSE_EQ_INE062A01020.json":
                return [
                    ["2026-07-01T00:00:00+05:30", 10.0, 11.0, 9.0, 10.5, 1000.0],
                    ["2026-08-03T00:00:00+05:30", 20.0, 21.0, 19.0, 20.5, 2000.0],
                    ["2026-08-06T00:00:00+05:30", 30.0, 31.0, 29.0, 30.5, 3000.0],  # fresh
                ]
            if path == "cache/score_1d/NSE/CASH/SBIN.json":
                return [["2026-02-26T00:00:00+05:30", 1.0, 1.0, 1.0, 1.0, 1.0]]  # STALE
            return []

    class _State:
        def get_universe_row(self, sym):
            return {"instrument_key": "NSE_EQ|INE062A01020", "exchange": "NSE", "segment": "CASH"}

    class _C:
        gcs = _GCS()
        state = _State()

    rows = _gcs_candles_1d(_C(), "sbin", "2026-08-01", "2026-08-06")
    assert [r["time"] for r in rows] == ["2026-08-03", "2026-08-06"]   # window applied
    assert rows[-1]["close"] == 30.5                                   # recent bar present
    assert set(rows[0]) == {"time", "open", "high", "low", "close", "volume"}  # BQ-shaped
    # unknown symbol -> empty (every candidate path misses), never raises
    class _NoRow(_C):
        class state:  # type: ignore[misc]
            @staticmethod
            def get_universe_row(sym): return None
    assert _gcs_candles_1d(_NoRow(), "NOSUCHSYM", "2026-08-01", "2026-08-06") == []


def test_gcs_candles_1d_prefers_instrument_key_over_frozen_legacy():
    """Path ORDER is the fix: the legacy by-symbol score_1d files froze when the live job
    migrated to instrument-key keying (prod 2026-08-07: SBIN legacy last bar 2026-02-26 vs
    2026-08-06 by-ik). Reading legacy first would serve a ~5-month-stale chart."""
    from autotrader.web.dashboard_api import _gcs_candles_1d
    rows = _gcs_candles_1d(_c_for_order_test(), "SBIN", "2026-01-01", "2026-08-06")
    assert [r["time"] for r in rows][-1] == "2026-08-06"      # fresh ik file won
    assert "2026-02-26" not in [r["time"] for r in rows]      # frozen legacy NOT used
    # if the ik lookup fails, it still degrades to legacy rather than returning nothing
    class _Broken(_c_for_order_test().__class__):
        class state:  # type: ignore[misc]
            @staticmethod
            def get_universe_row(sym): raise RuntimeError("firestore down")
    got = _gcs_candles_1d(_Broken(), "SBIN", "2026-01-01", "2026-08-06")
    assert [r["time"] for r in got] == ["2026-02-26"]         # graceful legacy fallback


def _c_for_order_test():
    """Container stub: fresh instrument-key file + FROZEN legacy by-symbol file."""
    class _GCS:
        @staticmethod
        def score_cache_1d_path_by_instrument_key(ik, exch, seg):
            return f"cache/score_1d_by_instrument/{exch}/{seg}/{ik.replace('|', '_')}.json"

        def read_candles(self, path):
            if path == "cache/score_1d_by_instrument/NSE/CASH/NSE_EQ_INE062A01020.json":
                return [["2026-08-06T00:00:00+05:30", 30.0, 31.0, 29.0, 30.5, 3000.0]]
            if path == "cache/score_1d/NSE/CASH/SBIN.json":
                return [["2026-02-26T00:00:00+05:30", 1.0, 1.0, 1.0, 1.0, 1.0]]
            return []

    class _State:
        @staticmethod
        def get_universe_row(sym):
            return {"instrument_key": "NSE_EQ|INE062A01020", "exchange": "NSE", "segment": "CASH"}

    class _C:
        gcs = _GCS()
        state = _State()
    return _C()


# ── forward-test epoch (fixed 2026-07-27) ─────────────────────────────────────
def test_forward_test_start_is_fixed_and_env_overridable():
    """The epoch is a real setting, not a magic literal scattered around."""
    from autotrader.settings import StrategySettings
    assert StrategySettings().forward_test_start == "2026-07-27"


def test_realized_stats_entry_based_filter_excludes_old_positions_closing_late():
    """THE attribution rule. A pre-cutoff position that EXITS inside the forward window
    must NOT count — this is why the filter is entry-based, not exit-based. At the real
    cutoff 34 such positions were open (core 30, delivery 4)."""
    from autotrader.adapters.firestore_state import FirestoreStateStore

    rows = [
        # old-logic: entered before cutoff, exits AFTER it -> must be excluded
        {"status": "CLOSED", "channel": "core", "entry_ts": "2026-06-23T09:20:00+05:30",
         "exit_ts": "2026-08-05T15:20:00+05:30", "net_pnl": -5000.0},
        {"status": "CLOSED", "channel": "delivery", "entry_ts": "2026-07-20T09:30:00+05:30",
         "exit_ts": "2026-08-10T15:20:00+05:30", "net_pnl": -2491.0},
        # forward-test: entered on/after cutoff -> counts
        {"status": "CLOSED", "channel": "delivery", "entry_ts": "2026-07-27T09:30:00+05:30",
         "exit_ts": "2026-08-20T15:20:00+05:30", "net_pnl": 800.0},
        {"status": "CLOSED", "channel": "delivery", "entry_ts": "2026-08-03T09:30:00+05:30",
         "exit_ts": "2026-08-21T15:20:00+05:30", "net_pnl": -300.0},
        # undated legacy row -> fail-closed, excluded
        {"status": "CLOSED", "channel": "delivery", "net_pnl": 99999.0},
        {"status": "OPEN", "channel": "momentum", "entry_ts": "2026-08-01T09:35:00+05:30"},
    ]

    class _Doc:
        def __init__(self, d): self._d = d
        def to_dict(self): return self._d

    class _Coll:
        def stream(self): return [_Doc(r) for r in rows]

    class _DB:
        def collection(self, name): return _Coll()

    st = FirestoreStateStore.__new__(FirestoreStateStore)   # bypass __init__ (no real client)
    st._db = lambda: _DB()                               # type: ignore[method-assign]

    fwd = st.get_realized_stats_by_channel(since_entry="2026-07-27")
    assert "core" not in fwd                                     # old core exit excluded
    assert fwd["delivery"]["closed"] == 2                        # only the 2 in-epoch
    assert fwd["delivery"]["realized"] == 500.0                  # 800 - 300
    assert fwd["delivery"]["wins"] == 1
    assert 99999.0 not in (fwd["delivery"]["realized"],)         # undated row not counted

    alltime = st.get_realized_stats_by_channel()                 # no filter = everything
    assert alltime["core"]["closed"] == 1
    assert alltime["delivery"]["closed"] == 4                    # incl. old + undated


# ── data-quality exclusion (tagged 2026-08-07) ────────────────────────────────
def test_is_invalid_helper_matches_only_tagged_rows():
    from autotrader.adapters.firestore_state import _is_invalid
    assert _is_invalid({"data_quality": "INVALID"}) is True
    assert _is_invalid({"data_quality": "invalid"}) is True      # case-insensitive
    assert _is_invalid({"data_quality": " INVALID "}) is True    # whitespace-tolerant
    assert _is_invalid({}) is False                              # untagged = valid
    assert _is_invalid({"data_quality": None}) is False          # fail-open on null
    assert _is_invalid({"data_quality": ""}) is False
    assert _is_invalid(None) is False                            # never raises


def test_realized_stats_excludes_data_quality_invalid():
    """Tagged rows (EOD-squareoff bug / commissioning artifacts) must not reach any
    P&L or win-rate aggregate, even when they fall inside the requested window."""
    from autotrader.adapters.firestore_state import FirestoreStateStore
    rows = [
        # tagged bug exit: real pnl, but must be ignored
        {"status": "CLOSED", "channel": "delivery", "entry_ts": "2026-07-15T14:30:00+05:30",
         "exit_ts": "2026-07-15T15:20:00+05:30", "net_pnl": -1162.87,
         "data_quality": "INVALID", "invalid_reason": "EOD_SQUAREOFF_BUG"},
        # tagged commissioning artifact
        {"status": "CLOSED", "channel": "core", "entry_ts": "2026-06-21T09:20:00+05:30",
         "exit_ts": "2026-06-22T15:20:00+05:30", "net_pnl": 0.0,
         "data_quality": "INVALID", "invalid_reason": "CORE_COMMISSIONING_RESET"},
        # genuine trade -> counts
        {"status": "CLOSED", "channel": "delivery", "entry_ts": "2026-07-20T09:30:00+05:30",
         "exit_ts": "2026-07-24T15:20:00+05:30", "net_pnl": -2491.0},
    ]

    class _Doc:
        def __init__(self, d): self._d = d
        def to_dict(self): return self._d

    class _Coll:
        def stream(self): return [_Doc(r) for r in rows]

    class _DB:
        def collection(self, name): return _Coll()

    st = FirestoreStateStore.__new__(FirestoreStateStore)
    st._db = lambda: _DB()                                   # type: ignore[method-assign]

    out = st.get_realized_stats_by_channel()
    assert "core" not in out                                 # only had a tagged row
    assert out["delivery"]["closed"] == 1                    # the tagged bug exit dropped
    assert out["delivery"]["realized"] == -2491.0
    assert out["delivery"]["wins"] == 0
    # today's per-channel P&L must also skip tagged rows
    today = st.get_today_realized_pnl_by_channel("2026-07-15")
    assert today.get("delivery", 0.0) == 0.0                 # the only 07-15 exit was tagged


def test_bq_valid_trade_guard_is_null_safe():
    """SQL `!=` drops NULLs, so the guard must use IFNULL or every untagged row
    (121 of 155 in prod) would silently vanish from the dashboard."""
    from autotrader.web.dashboard_api import _BQ_VALID_TRADE
    assert "IFNULL" in _BQ_VALID_TRADE and "'INVALID'" in _BQ_VALID_TRADE


def test_cards_lead_with_all_time_and_carry_forward_epoch_secondary():
    """Regression for the 2026-08-10 mistake: the epoch was briefly the PRIMARY card
    number, which blanked every card to ₹0/0 trades and hid the whole trade history.
    All-time clean must lead; the epoch rides along as fwd_*."""
    positions = [{"channel": "swing", "symbol": "X", "entry_price": 100.0, "qty": 10}]
    alltime = {"swing": {"realized": -6618.0, "closed": 22, "wins": 8}}
    fwd = {"swing": {"realized": 0.0, "closed": 0, "wins": 0}}     # nothing closed in epoch
    out = build_channel_overview(["swing"], positions, {}, {}, _cap, _maxp, 0.03, 0.06,
                                 realized_by_channel=alltime, forward_by_channel=fwd)
    r = out["channels"][0]
    assert r["realized_pnl"] == -6618.0 and r["closed_trades"] == 22   # history VISIBLE
    assert r["win_rate"] == 36.4
    assert r["fwd_realized_pnl"] == 0.0 and r["fwd_closed_trades"] == 0
    assert out["totals"]["realized_pnl"] == -6618.0
    assert out["totals"]["fwd_realized_pnl"] == 0.0
    assert out["totals"]["fwd_closed_trades"] == 0
    # omitting forward_by_channel must not break anything (back-compat)
    b = build_channel_overview(["swing"], positions, {}, {}, _cap, _maxp, 0.03, 0.06,
                               realized_by_channel=alltime)
    assert b["channels"][0]["fwd_realized_pnl"] == 0.0
