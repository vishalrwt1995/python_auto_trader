"""EOD position-reconcile must exempt overnight SL-only channels (2026-06-22 fix).

Background: `OrderService.reconcile_open_positions` (the /jobs/eod-position-reconcile
path, 15:25/27/29 IST) used to skip ONLY `wl_type == "swing"`. That EOD-squared the
CORE buy-and-hold book on 2026-06-22 (30 positions closed EOD_CLOSE at 15:25) — and
would have squared PEAD/corp_action too. The fix broadens the exemption to mirror
ws_monitor._OVERNIGHT_SL_ONLY_WL = {swing, pead, corp_action, core}. gap_fade is
intentionally NOT exempt — it's an intraday MIS short covered at the EOD squareoff.

These tests drive the REAL `reconcile_open_positions` with a fake OrderService (paper
mode, LTP available) and assert which channels close vs persist.
"""
from __future__ import annotations

import inspect

from autotrader.services.order_service import OrderService
from autotrader.services import order_service as os_mod


class _Quote:
    def __init__(self, ltp: float):
        self.ltp = ltp


class _FakeUpstox:
    def get_quote(self, ik: str) -> _Quote:
        return _Quote(100.0)


class _FakeState:
    def __init__(self, positions):
        self._positions = positions
    def list_open_positions(self):
        return self._positions
    def get_json(self, collection, key):
        return {}


class _Runtime:
    paper_trade = True


class _Settings:
    runtime = _Runtime()


def _make_order_service(positions):
    svc = OrderService.__new__(OrderService)
    svc.state = _FakeState(positions)
    svc.upstox = _FakeUpstox()
    svc.settings = _Settings()
    closed: list[tuple[str, str]] = []

    def _close(position_tag, exit_price, exit_reason):  # mirrors _close_position_firestore sig
        closed.append((str(position_tag), str(exit_reason)))

    svc._close_position_firestore = _close  # type: ignore[assignment]
    svc._closed_calls = closed  # type: ignore[attr-defined]
    return svc


def _pos(tag, wl_type):
    return {
        "position_tag": tag, "symbol": tag, "wl_type": wl_type, "status": "OPEN",
        "order_id": "", "entry_price": 100.0, "instrument_key": f"NSE_EQ|{tag}",
    }


def _closed_tags(svc):
    return {t for t, _ in svc._closed_calls}  # type: ignore[attr-defined]


def test_eod_recon_skips_core():
    svc = _make_order_service([_pos("CORE1", "core")])
    out = OrderService.reconcile_open_positions(svc, force_close=True)
    assert _closed_tags(svc) == set()           # CORE buy-and-hold must persist
    assert out["closed"] == 0 and out["remaining"] == 1


def test_eod_recon_skips_all_overnight_channels():
    svc = _make_order_service([
        _pos("S", "swing"), _pos("P", "pead"), _pos("C", "corp_action"), _pos("CO", "core"),
    ])
    OrderService.reconcile_open_positions(svc, force_close=True)
    assert _closed_tags(svc) == set()           # none of the overnight channels close at EOD


def test_eod_recon_closes_intraday_and_gap_fade():
    svc = _make_order_service([_pos("I", "intraday"), _pos("G", "gap_fade")])
    OrderService.reconcile_open_positions(svc, force_close=True)
    assert _closed_tags(svc) == {"I", "G"}      # intraday MIS + gap-fade short DO square at EOD
    assert all(r == "EOD_CLOSE" for _, r in svc._closed_calls)  # type: ignore[attr-defined]


def test_eod_recon_mixed_book():
    svc = _make_order_service([_pos("I", "intraday"), _pos("S", "swing"), _pos("CO", "core")])
    OrderService.reconcile_open_positions(svc, force_close=True)
    assert _closed_tags(svc) == {"I"}           # only intraday squares; swing + core persist


def test_exemption_set_matches_ws_monitor():
    """Drift guard: the EOD-recon exemption must stay in sync with ws_monitor's set."""
    src = inspect.getsource(os_mod)
    assert 'in ("swing", "pead", "corp_action", "core")' in src, (
        "reconcile_open_positions overnight-skip set drifted from ws_monitor._OVERNIGHT_SL_ONLY_WL"
    )
