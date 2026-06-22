"""Regression tests for the 2026-06-22 daily-analysis fixes:

1. max_trades_day is PER-CHANNEL — the CORE quarterly rebalance's bulk
   buy-and-hold entries must NOT consume the swing/intraday daily-trade budget
   and silently halt those scanners (`get_today_trade_count(channels=...)`).
2. GAP_FADE daily-breaker realized-PnL helper uses real state methods
   (no more `pead_trading_service._channel_realized_today` / `list_closed_positions_today`,
   neither of which exists) — fail-closed on read error.
3. Upstox v3 `market-quote/ohlc` nests OHLC under `live_ohlc` — the quote parser
   must read it, else the GAP_FADE open-snapshot reads open=0 and drops every
   symbol (the live `snapshot=0` we saw on the first armed fire).
"""
from __future__ import annotations

import math
import types

import pytest


# ── helpers ──────────────────────────────────────────────────────────────
class _Doc:
    def __init__(self, d): self._d = d
    def to_dict(self): return self._d


class _Coll:
    def __init__(self, docs): self._docs = docs
    def stream(self): return iter(self._docs)


class _DB:
    def __init__(self, docs): self._docs = docs
    def collection(self, name): return _Coll(self._docs)


def _count(docs, today, channels=None):
    """Invoke the real FirestoreStateStore.get_today_trade_count against fake docs."""
    from autotrader.adapters.firestore_state import FirestoreStateStore
    fake = types.SimpleNamespace(_db=lambda: _DB([_Doc(d) for d in docs]))
    return FirestoreStateStore.get_today_trade_count(fake, today, channels=channels)


# ── 1. per-channel max_trades_day ──────────────────────────────────────────
def test_core_rebalance_does_not_halt_swing_intraday():
    """The exact incident: 30 CORE buy-and-hold entries stamped today must not
    count against the swing/intraday cap (was 30 >= max_trades_day=5 -> halt)."""
    docs = [{"channel": "core", "entry_ts": f"2026-06-22T03:0{i % 10}:00", "status": "OPEN"}
            for i in range(30)]
    # Legacy un-scoped behaviour saw all 30 (would trip a cap of 5).
    assert _count(docs, "2026-06-22", channels=None) == 30
    # Per-channel: swing and intraday see ZERO of the CORE entries -> no halt.
    assert _count(docs, "2026-06-22", channels={"intraday"}) == 0
    assert _count(docs, "2026-06-22", channels={"swing"}) == 0
    assert _count(docs, "2026-06-22", channels={"swing", "intraday"}) == 0


def test_per_channel_counts_only_own_entries():
    docs = [
        {"channel": "swing", "entry_ts": "2026-06-22T09:30:00", "status": "OPEN"},
        {"channel": "swing", "entry_ts": "2026-06-22T11:00:00", "status": "CLOSED"},   # opened+closed today still counts
        {"channel": "intraday", "entry_ts": "2026-06-22T09:45:00", "status": "OPEN"},
        {"channel": "core", "entry_ts": "2026-06-22T03:00:00", "status": "OPEN"},
        {"channel": "gap_fade", "entry_ts": "2026-06-22T09:16:00", "status": "OPEN"},
        {"channel": "swing", "entry_ts": "2026-06-19T09:30:00", "status": "CLOSED"},   # not today
    ]
    assert _count(docs, "2026-06-22", channels={"swing"}) == 2
    assert _count(docs, "2026-06-22", channels={"intraday"}) == 1
    assert _count(docs, "2026-06-22", channels={"swing", "intraday"}) == 3
    assert _count(docs, "2026-06-22", channels=None) == 5            # all-today, all channels


def test_channel_routing_falls_back_to_wl_type_then_intraday():
    docs = [
        {"wl_type": "swing", "entry_ts": "2026-06-22T09:30:00"},     # no channel -> wl_type
        {"entry_ts": "2026-06-22T09:45:00"},                          # no channel/wl_type -> legacy 'intraday'
    ]
    assert _count(docs, "2026-06-22", channels={"swing"}) == 1
    assert _count(docs, "2026-06-22", channels={"intraday"}) == 1


# ── 2. gap-fade daily-breaker realized PnL ─────────────────────────────────
def test_gap_fade_realized_today_sums_only_channel_closed_today():
    from autotrader.services import gap_fade_trading_service as gf

    class _State:
        def list_all_positions(self, limit=500):
            return [
                {"channel": "gap_fade", "status": "CLOSED", "exit_ts": "2026-06-22T15:25:00", "pnl": 120.0},
                {"channel": "gap_fade", "status": "CLOSED", "exit_ts": "2026-06-22T15:25:00", "pnl": -50.0},
                {"channel": "gap_fade", "status": "OPEN", "exit_ts": "", "pnl": 0.0},                 # open -> excluded
                {"channel": "swing", "status": "CLOSED", "exit_ts": "2026-06-22T15:25:00", "pnl": 999.0},  # other channel
                {"channel": "gap_fade", "status": "CLOSED", "exit_ts": "2026-06-19T15:25:00", "pnl": 7.0},  # not today
            ]

    val = gf._channel_realized_today(_State(), "2026-06-22")
    assert val == pytest.approx(70.0)       # 120 - 50, gap_fade closed today only
    assert not math.isnan(val)              # the old code returned nan (missing methods)


def test_gap_fade_realized_today_fail_closed_on_read_error():
    from autotrader.services import gap_fade_trading_service as gf

    class _BadState:
        def list_all_positions(self, limit=500):
            raise RuntimeError("firestore down")

    assert math.isnan(gf._channel_realized_today(_BadState(), "2026-06-22"))


# ── 3. Upstox v3 OHLC (live_ohlc) parsing -> snapshot>0 ────────────────────
def test_v3_ohlc_live_ohlc_is_parsed():
    """Real v3 market-quote/ohlc row shape (2026-06-22 live probe)."""
    from autotrader.adapters.upstox_client import UpstoxClient
    row = {"last_price": 1328.9, "instrument_token": "NSE_EQ|INE002A01018", "prev_ohlc": None,
           "live_ohlc": {"open": 1316.7, "high": 1344.9, "low": 1314.1, "close": 1328.9, "volume": 9252798}}
    q = UpstoxClient._extract_quote_from_row(row)
    assert q.open == pytest.approx(1316.7)
    assert q.high == pytest.approx(1344.9)
    assert q.low == pytest.approx(1314.1)
    assert q.ltp == pytest.approx(1328.9)


def test_v2_ohlc_key_still_parsed():
    """Regression guard: the v2 `ohlc` shape must keep working."""
    from autotrader.adapters.upstox_client import UpstoxClient
    row = {"last_price": 1328.9, "instrument_token": "NSE_EQ|INE002A01018",
           "ohlc": {"open": 1316.7, "high": 1344.9, "low": 1314.1, "close": 1300.0}}
    q = UpstoxClient._extract_quote_from_row(row)
    assert q.open == pytest.approx(1316.7)
    assert q.high == pytest.approx(1344.9)
    assert q.low == pytest.approx(1314.1)


def test_gap_fade_open_snapshot_populates_from_v3():
    """End-to-end: with the v3 live_ohlc shape, fetch_open_snapshot returns a
    non-empty snapshot (the bug produced snapshot=0)."""
    from autotrader.adapters.upstox_client import UpstoxClient
    from autotrader.services import gap_fade_signal_service as gfs

    ik = "NSE_EQ|INE002A01018"
    keymap = {"RELIANCE": ik}

    class _Up:
        def get_ohlc_v3(self, iks):
            return {ik: UpstoxClient._extract_quote_from_row(
                {"last_price": 1328.9, "instrument_token": ik,
                 "live_ohlc": {"open": 1316.7, "high": 1344.9, "low": 1314.1, "close": 1328.9}})}

        def get_ltp_v3(self, iks):
            return {ik: UpstoxClient._extract_quote_from_row(
                {"last_price": 1328.9, "instrument_token": ik, "cp": 1309.5})}

    snap = gfs.fetch_open_snapshot(keymap, _Up(), {"RELIANCE": 1e9})
    assert "RELIANCE" in snap, "snapshot=0 regression — v3 open not parsed"
    assert snap["RELIANCE"]["open"] == pytest.approx(1316.7)
    assert snap["RELIANCE"]["prev_close"] == pytest.approx(1309.5)
