"""Auth-split tests — verify the long-lived Upstox Analytics token routing.

Goal: morning cron jobs (06:15 / 07:05 / 07:40 / 09:00 IST) hit Upstox
market-data endpoints BEFORE the user renews the daily access token at
~09:35 IST. Historically this caused every scan to fail with
"Upstox access token missing/expired" until manual renewal.

The fix splits auth into two modes:
  - read  → long-lived Analytics token (1-year expiry, market-data scope)
  - write → daily access token (orders / portfolio / funds)

These tests pin that behaviour so a future refactor doesn't silently
revert the split and reintroduce the morning-failure class.
"""
from __future__ import annotations

from dataclasses import replace

import httpx
import pytest

from autotrader.adapters.upstox_client import UpstoxClient
from autotrader.settings import UpstoxSettings


DAILY_TOKEN = "daily-token-expires-at-0330-ist"
ANALYTICS_TOKEN = "analytics-token-valid-for-one-year"


class _FakeSecrets:
    """Minimal SecretManagerStore stand-in."""

    def __init__(self, secrets: dict[str, str]):
        self._s = dict(secrets)

    def get_secret(self, name: str) -> str:  # noqa: D401
        return self._s.get(name, "")

    def add_secret_version(self, name: str, value: str) -> None:
        self._s[name] = value


def _settings(*, analytics_secret_name: str = "") -> UpstoxSettings:
    return UpstoxSettings(
        api_v2_host="https://api.upstox.com/v2",
        api_v3_host="https://api.upstox.com/v3",
        client_id_secret_name="client-id-secret",
        client_secret_secret_name="client-secret-secret",
        access_token_secret_name="access-token-secret",
        access_token_expiry_secret_name="access-token-expiry-secret",
        analytics_token_secret_name=analytics_secret_name,
    )


def _client(*, settings: UpstoxSettings, with_analytics: bool) -> tuple[UpstoxClient, list[httpx.Request]]:
    """Build an UpstoxClient whose HTTP layer records every request."""
    captured: list[httpx.Request] = []

    def _handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json={"status": "success", "data": {"ok": True}})

    secrets_map: dict[str, str] = {
        "client-id-secret": "CLIENT_ID",
        "client-secret-secret": "CLIENT_SECRET",
        "access-token-secret": DAILY_TOKEN,
        # Expiry far in the future so ensure_access_token returns the cached daily token.
        "access-token-expiry-secret": "2099-01-01T00:00:00+00:00",
    }
    if with_analytics and settings.analytics_token_secret_name:
        secrets_map[settings.analytics_token_secret_name] = ANALYTICS_TOKEN

    secrets = _FakeSecrets(secrets_map)
    c = UpstoxClient(settings, secrets, timeout_sec=5.0)
    # Swap the httpx client for one wired to our handler.
    c.http.close()
    c.http = httpx.Client(transport=httpx.MockTransport(_handler), timeout=5.0)
    return c, captured


def _bearer(req: httpx.Request) -> str:
    auth = req.headers.get("Authorization", "")
    assert auth.startswith("Bearer "), f"missing bearer: {auth!r}"
    return auth[len("Bearer "):]


# ──────────────────────────────────────────────────────────────────────────
# Core routing rules
# ──────────────────────────────────────────────────────────────────────────


def test_read_call_uses_analytics_token_when_configured():
    s = _settings(analytics_secret_name="upstox-analytics-token")
    c, captured = _client(settings=s, with_analytics=True)

    c._request("GET", "market-quote/ltp", params={"instrument_key": "NSE_EQ|INE123"},
               auth=True, version="v3", content_type=None)

    assert captured, "no HTTP request captured"
    assert _bearer(captured[-1]) == ANALYTICS_TOKEN


def test_read_call_falls_back_to_daily_when_analytics_unset():
    """Deployments without the Analytics secret keep legacy behaviour."""
    s = _settings(analytics_secret_name="")
    c, captured = _client(settings=s, with_analytics=False)

    c._request("GET", "market-quote/ltp", auth=True, version="v3", content_type=None)

    assert _bearer(captured[-1]) == DAILY_TOKEN


def test_read_call_falls_back_to_daily_when_analytics_secret_empty():
    """Analytics name configured but secret empty → fall back, don't crash."""
    s = _settings(analytics_secret_name="upstox-analytics-token")
    c, captured = _client(settings=s, with_analytics=False)  # secret name set, not populated

    c._request("GET", "market-quote/ltp", auth=True, version="v3", content_type=None)

    assert _bearer(captured[-1]) == DAILY_TOKEN


def test_write_call_always_uses_daily_token_even_when_analytics_configured():
    """Order endpoints MUST use the daily token — analytics token lacks
    order-placement scope (Upstox returns UDAPI100067)."""
    s = _settings(analytics_secret_name="upstox-analytics-token")
    c, captured = _client(settings=s, with_analytics=True)

    c._request("POST", "order/place", json_body={"quantity": 1},
               auth=True, auth_mode="write", version="v2")

    assert _bearer(captured[-1]) == DAILY_TOKEN
    assert _bearer(captured[-1]) != ANALYTICS_TOKEN


# ──────────────────────────────────────────────────────────────────────────
# Public method call sites — guard against a future refactor removing
# auth_mode="write" from order helpers.
# ──────────────────────────────────────────────────────────────────────────


def test_place_order_uses_daily_token():
    s = _settings(analytics_secret_name="upstox-analytics-token")
    c, captured = _client(settings=s, with_analytics=True)

    c.place_order({"quantity": 1, "instrument_token": "NSE_EQ|INE123",
                   "transaction_type": "BUY", "order_type": "MARKET",
                   "product": "I", "validity": "DAY", "price": 0})

    assert _bearer(captured[-1]) == DAILY_TOKEN


def test_place_bracket_order_uses_daily_token():
    s = _settings(analytics_secret_name="upstox-analytics-token")
    c, captured = _client(settings=s, with_analytics=True)

    c.place_bracket_order(
        instrument_token="NSE_EQ|INE123",
        transaction_type="BUY",
        quantity=1,
        stop_loss=1.0,
        square_off=2.0,
    )

    assert _bearer(captured[-1]) == DAILY_TOKEN


def test_cancel_order_uses_daily_token():
    s = _settings(analytics_secret_name="upstox-analytics-token")
    c, captured = _client(settings=s, with_analytics=True)

    c.cancel_order("ORDER-123")

    assert _bearer(captured[-1]) == DAILY_TOKEN


def test_list_orders_uses_daily_token():
    s = _settings(analytics_secret_name="upstox-analytics-token")
    c, captured = _client(settings=s, with_analytics=True)

    c.list_orders()

    assert _bearer(captured[-1]) == DAILY_TOKEN


def test_get_order_details_uses_daily_token():
    s = _settings(analytics_secret_name="upstox-analytics-token")
    c, captured = _client(settings=s, with_analytics=True)

    c.get_order_details("ORDER-123")

    assert _bearer(captured[-1]) == DAILY_TOKEN


def test_place_gtt_order_uses_daily_token():
    s = _settings(analytics_secret_name="upstox-analytics-token")
    c, captured = _client(settings=s, with_analytics=True)

    c.place_gtt_order(
        instrument_token="NSE_EQ|INE123",
        transaction_type="SELL",
        quantity=1,
        trigger_price=100.0,
    )

    assert _bearer(captured[-1]) == DAILY_TOKEN


def test_delete_gtt_order_uses_daily_token():
    s = _settings(analytics_secret_name="upstox-analytics-token")
    c, captured = _client(settings=s, with_analytics=True)

    c.delete_gtt_order("GTT-123")

    assert _bearer(captured[-1]) == DAILY_TOKEN


# ──────────────────────────────────────────────────────────────────────────
# Market-data helpers must stay on the read path (Analytics token when set).
# This is the morning-cron failure class we are guarding against.
# ──────────────────────────────────────────────────────────────────────────


def test_get_historical_candles_uses_analytics_token():
    s = _settings(analytics_secret_name="upstox-analytics-token")
    c, captured = _client(settings=s, with_analytics=True)

    c.get_historical_candles_v3_days("NSE_EQ|INE123", to_date="2026-04-24",
                                     from_date="2026-04-01", interval_days=1)

    assert _bearer(captured[-1]) == ANALYTICS_TOKEN


def test_get_ltp_uses_analytics_token():
    s = _settings(analytics_secret_name="upstox-analytics-token")
    c, captured = _client(settings=s, with_analytics=True)

    c.get_ltp_v3(["NSE_EQ|INE123"])

    assert _bearer(captured[-1]) == ANALYTICS_TOKEN


def test_get_option_chain_uses_analytics_token():
    s = _settings(analytics_secret_name="upstox-analytics-token")
    c, captured = _client(settings=s, with_analytics=True)

    c.get_option_chain("NSE_INDEX|Nifty 50", "2026-05-01")

    assert _bearer(captured[-1]) == ANALYTICS_TOKEN


def test_market_holidays_uses_analytics_token():
    s = _settings(analytics_secret_name="upstox-analytics-token")
    c, captured = _client(settings=s, with_analytics=True)

    c.get_market_holidays("2026-04-24")

    assert _bearer(captured[-1]) == ANALYTICS_TOKEN


# ──────────────────────────────────────────────────────────────────────────
# Morning-cron resilience scenario — this is the regression test for the
# actual production incident on 2026-04-24.
# ──────────────────────────────────────────────────────────────────────────


def test_morning_cron_scenario_stale_daily_token_still_reads_candles():
    """Simulates the 06:15 IST job:
    - Daily access token has already expired (03:30 IST rotation passed)
    - User has not yet renewed it (they do so ~09:35 IST)
    - Analytics token is valid (1-year expiry)

    The read call MUST succeed because it routes through the analytics
    token. This is the core value of the auth split.
    """
    s = _settings(analytics_secret_name="upstox-analytics-token")

    captured: list[httpx.Request] = []

    def _handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        # If the handler sees the stale daily token we'd get 401 in prod.
        # Assert the split prevented that by checking the bearer.
        auth = request.headers.get("Authorization", "")
        if auth == f"Bearer {DAILY_TOKEN}":
            return httpx.Response(401, json={"errors": [{"errorCode": "UDAPI100050",
                                                         "message": "Token expired"}]})
        return httpx.Response(200, json={"status": "success", "data": {"candles": []}})

    secrets = _FakeSecrets({
        "client-id-secret": "CLIENT_ID",
        "client-secret-secret": "CLIENT_SECRET",
        # Stale daily token — expired yesterday.
        "access-token-secret": DAILY_TOKEN,
        "access-token-expiry-secret": "2020-01-01T00:00:00+00:00",
        "upstox-analytics-token": ANALYTICS_TOKEN,
    })
    c = UpstoxClient(s, secrets, timeout_sec=5.0)
    c.http.close()
    c.http = httpx.Client(transport=httpx.MockTransport(_handler), timeout=5.0)

    # Should NOT raise — analytics token carries the read call.
    out = c.get_historical_candles_v3_days(
        "NSE_EQ|INE123", to_date="2026-04-24", from_date="2026-04-01", interval_days=1,
    )

    assert out == []
    assert _bearer(captured[-1]) == ANALYTICS_TOKEN
