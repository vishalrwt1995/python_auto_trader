"""GF-2 tests for gap_fade_signal_service.build_candidates (the pure selection core).
The live fetch (fetch_open_snapshot) is thin I/O, exercised via injected snapshots."""
from __future__ import annotations

import pytest

from autotrader.services import gap_fade_signal_service as svc
from autotrader.domain import gap_fade_signals as gf


def _q(open_, prev_close, high=None, low=None, turnover=2e8):
    return {"open": open_, "prev_close": prev_close,
            "high": high if high is not None else open_ * 1.01,
            "low": low if low is not None else open_ * 0.99,
            "turnover_20d": turnover}


def test_happy_short_candidate():
    # +6% gap-up, liquid, has range -> one SELL candidate
    snap = {"ABC": _q(106.0, 100.0)}
    out = svc.build_candidates(snap)
    assert len(out) == 1
    c = out[0]
    assert c["symbol"] == "ABC" and c["side"] == "SELL"
    assert c["channel"] == "gap_fade" and c["wl_type"] == "gap_fade" and c["strategy"] == "GAP_FADE"
    assert c["gap"] == pytest.approx(0.06)
    assert c["ref_open"] == pytest.approx(106.0)
    assert c["stop_price"] == pytest.approx(106.0 * (1 + gf.INITIAL_STOP_PCT))   # buy-stop above


def test_gap_too_small_excluded():
    assert svc.build_candidates({"ABC": _q(104.0, 100.0)}) == []        # +4% < 5%


def test_illiquid_excluded():
    assert svc.build_candidates({"ABC": _q(106.0, 100.0, turnover=1e7)}) == []


def test_locked_limit_excluded():
    # no intraday range (high==low) -> circuit-locked, unshortable
    assert svc.build_candidates({"ABC": _q(106.0, 100.0, high=106.0, low=106.0)}) == []


def test_price_floor_excluded():
    # +8% gap but penny price below floor
    assert svc.build_candidates({"PENNY": _q(8.64, 8.0)}) == []


def test_ranked_by_gap_desc_and_topk():
    snap = {
        "G6": _q(106.0, 100.0),    # +6%
        "G9": _q(109.0, 100.0),    # +9%
        "G7": _q(107.0, 100.0),    # +7%
        "G5h": _q(105.5, 100.0),   # +5.5%
    }
    out = svc.build_candidates(snap, max_positions=2)
    assert [c["symbol"] for c in out] == ["G9", "G7"]                   # largest gaps, capped at K=2


def test_empty_and_bad_snapshot():
    assert svc.build_candidates({}) == []
    assert svc.build_candidates({"X": {}}) == []
    assert svc.build_candidates({"X": _q(0.0, 100.0)}) == []            # bad open


def test_scan_with_injected_snapshot():
    snap = {"ABC": _q(106.0, 100.0), "DEF": _q(103.0, 100.0)}           # DEF +3% filtered
    out = svc.scan(["ABC", "DEF"], snapshot=snap)
    assert [c["symbol"] for c in out] == ["ABC"]


def test_fetch_open_snapshot_fail_closed_without_client():
    # no client / no get_ohlc_quotes -> empty, no crash
    class Dummy:
        pass
    assert svc.fetch_open_snapshot(["ABC"], Dummy(), {}) == {}
