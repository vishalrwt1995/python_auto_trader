from __future__ import annotations

from autotrader.adapters.upstox_client import UpstoxApiError, UpstoxClient


def test_get_intraday_candles_v3_uses_documented_endpoint_first():
    client = object.__new__(UpstoxClient)
    calls: list[str] = []

    def _fake_request(method, endpoint, **kwargs):  # type: ignore[no-untyped-def]
        calls.append(str(endpoint))
        if endpoint == "historical-candle/intraday/NSE_INDEX%7CNifty%2050/minutes/5":
            return {"candles": [["2026-03-03T09:15:00+05:30", 1, 2, 0.5, 1.5, 1000]]}
        raise UpstoxApiError(f"unexpected endpoint {endpoint}")

    client._request = _fake_request  # type: ignore[method-assign]
    out = UpstoxClient.get_intraday_candles_v3(client, "NSE_INDEX|Nifty 50", unit="minutes", interval=5)

    assert calls
    assert calls[0] == "historical-candle/intraday/NSE_INDEX%7CNifty%2050/minutes/5"
    assert len(out) == 1
    assert out[0][0] == "2026-03-03T09:15:00+05:30"


def test_get_intraday_candles_v3_falls_back_to_interval_only_when_needed():
    client = object.__new__(UpstoxClient)
    calls: list[str] = []

    def _fake_request(method, endpoint, **kwargs):  # type: ignore[no-untyped-def]
        calls.append(str(endpoint))
        if endpoint == "historical-candle/intraday/NSE_EQ%7CINE123/5":
            return {"candles": [["2026-03-03T09:20:00+05:30", 10, 11, 9, 10.5, 500]]}
        raise UpstoxApiError(f"HTTP 400 [{endpoint}]: invalid input")

    client._request = _fake_request  # type: ignore[method-assign]
    out = UpstoxClient.get_intraday_candles_v3(client, "NSE_EQ|INE123", unit="minutes", interval=5)

    assert any(x == "historical-candle/intraday/NSE_EQ%7CINE123/5" for x in calls)
    assert len(out) == 1
    assert out[0][4] == 10.5
