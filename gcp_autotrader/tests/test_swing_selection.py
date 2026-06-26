"""Tests for the 2026-06 swing-config selection changes (PR2c/PR2d).

Covers:
  - WatchlistRow new fields (wl_score / rs_vs_mkt / breadth_pct) + safe defaults
  - _read_watchlist_with_fallback: swing rows re-ranked by wl_score desc
    (the slot-fill order), intraday rows untouched
  - swing_setup_group / SWING_RANGE_GROUP_CAP (reserve-2-trend)
  - structural guards: the rs/breadth/reserve policy blocks and the per-setup
    multi-emit slates exist in the source (same guard style as
    test_ws_monitor_swing_exit.py — fails loudly if the gating regresses)
"""
from __future__ import annotations

import inspect
from unittest.mock import MagicMock

from autotrader.domain.models import WatchlistRow
from autotrader.domain.regime_affinity import (
    SWING_RANGE_GROUP_CAP,
    swing_setup_group,
)
from autotrader.services.trading_service import TradingService


# ── WatchlistRow fields ──────────────────────────────────────────────────────

def test_watchlistrow_new_fields_default_to_zero():
    row = WatchlistRow(symbol="X")
    assert row.wl_score == 0.0
    assert row.rs_vs_mkt == 0.0
    assert row.breadth_pct == 0.0


# ── wl_score ranking in the watchlist read ──────────────────────────────────

def _svc_with_doc(rows: list[dict]) -> TradingService:
    svc = TradingService.__new__(TradingService)
    svc.state = MagicMock()
    svc.state.get_watchlist.return_value = {"rows": rows}
    return svc


def test_swing_rows_sorted_by_wl_score_desc():
    doc_rows = [
        {"symbol": "AAA", "wlType": "swing", "setuplabel": "MOMENTUM", "wl_score": 40.0},
        {"symbol": "BBB", "wlType": "swing", "setuplabel": "MEAN_REVERSION", "wl_score": 70.0},
        {"symbol": "CCC", "wlType": "swing", "setuplabel": "PULLBACK", "wl_score": 55.0},
    ]
    rows = _svc_with_doc(doc_rows)._read_watchlist_with_fallback()
    assert [r.symbol for r in rows] == ["BBB", "CCC", "AAA"]
    assert [r.wl_score for r in rows] == [70.0, 55.0, 40.0]


def test_intraday_rows_keep_position_and_order():
    doc_rows = [
        {"symbol": "I1", "wlType": "intraday", "setuplabel": "VWAP_TREND"},
        {"symbol": "S1", "wlType": "swing", "setuplabel": "MOMENTUM", "wl_score": 10.0},
        {"symbol": "I2", "wlType": "intraday", "setuplabel": "VWAP_REVERSAL"},
        {"symbol": "S2", "wlType": "swing", "setuplabel": "MEAN_REVERSION", "wl_score": 90.0},
    ]
    rows = _svc_with_doc(doc_rows)._read_watchlist_with_fallback()
    # intraday first in original order, then swing by wl_score desc
    assert [r.symbol for r in rows] == ["I1", "I2", "S2", "S1"]


def test_legacy_doc_without_new_fields_reads_zero():
    doc_rows = [{"symbol": "OLD", "wlType": "swing", "setuplabel": "MEAN_REVERSION"}]
    rows = _svc_with_doc(doc_rows)._read_watchlist_with_fallback()
    assert rows[0].wl_score == 0.0
    assert rows[0].rs_vs_mkt == 0.0
    assert rows[0].breadth_pct == 0.0


# ── reserve-2-trend group helper ─────────────────────────────────────────────

def test_swing_setup_group_assignment():
    assert swing_setup_group("MEAN_REVERSION") == "RANGE"
    assert swing_setup_group("MOMENTUM") == "TREND"
    assert swing_setup_group("PULLBACK") == "TREND"
    assert swing_setup_group("mean_reversion") == "RANGE"
    assert SWING_RANGE_GROUP_CAP == 3


# ── structural guards (source inspection) ────────────────────────────────────

def test_trading_service_has_new_swing_policy_blocks():
    from autotrader.services import trading_service as ts_mod
    src = inspect.getsource(ts_mod)
    assert "swing_setup_regime_gate" in src
    assert "swing_rs_below_market" in src
    assert "swing_breadth_below_60" in src
    assert "swing_range_slots_full" in src
    # 2026-06: breadth EMA200 gate + pb_slot reservation
    assert "swing_breadth_ema200_below_70" in src
    assert "swing_pb_slot_reserved" in src
    # the in-scan RANGE-group counter must be incremented on qualification
    assert "_open_swing_range_count += 1" in src


def test_universe_service_multi_emit_slates():
    from autotrader.services import universe_service as us_mod
    src = inspect.getsource(us_mod)
    assert '_MULTI_EMIT_SETUPS = ("MOMENTUM", "PULLBACK", "MEAN_REVERSION")' in src, (
        "per-setup multi-emit slates removed — single _select_rows() hides "
        "PULLBACK/MEAN_REVERSION behind MOMENTUM (backtest-validated change)"
    )
    # regime-weighted wl_score must be persisted on swing rows + Firestore write
    assert '"wl_score": float(round(final_score, 2))' in src
    assert '"wl_score": float(round(float(r.get("wl_score") or 0.0), 2))' in src
    # shorts stay disabled
    assert "_ALLOW_SHORT_SETUPS = False" in src
