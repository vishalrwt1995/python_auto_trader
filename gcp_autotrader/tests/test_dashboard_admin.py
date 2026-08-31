"""Admin action wiring (web/dashboard_api.py).

Regression for a real prod 500: post_admin_exit_position called
`c.order_service.place_exit_order(...)` (attribute access) instead of
`c.order_service().place_exit_order(...)` -- container.order_service is a
lazy-init factory METHOD (container.py:142), called with `()` at every one
of its other 14+ use sites. `'function' object has no attribute
'place_exit_order'`. The endpoint had zero test coverage and nobody had
exercised it until the first real manual exit attempt (2026-08-31, MON100).
"""
from __future__ import annotations

from autotrader.web import dashboard_api as da


class _FakeOrderService:
    def __init__(self):
        self.calls = []

    def place_exit_order(self, **kw):
        self.calls.append(kw)
        return {"exit_price": 336.81, "pnl": 1002.0}


class _FakeState:
    def get_position(self, tag):
        return {"symbol": "MON100"} if tag == "TAG1" else None

    def get_json(self, collection, key):
        return {"instrument_key": "NSE_EQ|INF247L01AP3"}


class _FakeContainer:
    def __init__(self):
        self.state = _FakeState()
        self._order_service = _FakeOrderService()

    def order_service(self):          # a METHOD, matching the real container
        return self._order_service


def test_exit_position_calls_order_service_correctly(monkeypatch):
    fake = _FakeContainer()
    monkeypatch.setattr(da, "get_container", lambda: fake)
    out = da.post_admin_exit_position({"position_tag": "TAG1"}, admin={"email": "t@x.com"})
    assert out["status"] == "ok"
    assert fake._order_service.calls == [{
        "position_tag": "TAG1", "instrument_key": "NSE_EQ|INF247L01AP3",
        "exit_reason": "MANUAL_EXIT",
    }]
