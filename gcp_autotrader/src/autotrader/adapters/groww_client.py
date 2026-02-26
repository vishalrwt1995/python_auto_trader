from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from urllib.parse import urlencode

import httpx

from autotrader.adapters.secrets_manager import SecretManagerStore
from autotrader.domain.models import Quote
from autotrader.settings import GrowwSettings
from autotrader.time_utils import now_utc, parse_any_ts

logger = logging.getLogger(__name__)


GROWW_INSTRUMENT_CSV_URLS = [
    "https://growwapi-assets.groww.in/instruments/instrument.csv",
    "https://assets.groww.in/instruments/instrument.csv",
]


@dataclass
class GrowwCredentials:
    api_key: str
    api_secret: str
    access_token: str = ""
    access_token_expiry: str = ""


class LocalRateLimiter:
    """Process-local limiter. Use Cloud Tasks/PubSub for distributed limits."""

    def __init__(self, rps: int):
        self.min_interval = 1.0 / max(1, rps)
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            gap = now - self._last
            if gap < self.min_interval:
                time.sleep(self.min_interval - gap)
            self._last = time.monotonic()


class GrowwApiError(RuntimeError):
    pass


class GrowwClient:
    def __init__(self, settings: GrowwSettings, secrets: SecretManagerStore, timeout_sec: float = 20.0):
        self.settings = settings
        self.secrets = secrets
        self.http = httpx.Client(timeout=timeout_sec, follow_redirects=True)
        self.limiter = LocalRateLimiter(settings.requests_per_second)
        self._cached_credentials: GrowwCredentials | None = None
        self._cached_token_expiry_ts: float | None = None

    def _read_credentials(self) -> GrowwCredentials:
        if self._cached_credentials is not None:
            if self._cached_token_expiry_ts is None or (self._cached_token_expiry_ts - time.time()) > 300:
                return self._cached_credentials
        api_key = (self.secrets.get_secret(self.settings.api_key_secret_name) or "").strip()
        api_secret = (self.secrets.get_secret(self.settings.api_secret_secret_name) or "").strip()
        access_token = (self.secrets.get_secret(self.settings.access_token_secret_name) or "").strip()
        expiry = (self.secrets.get_secret(self.settings.access_token_expiry_secret_name) or "").strip()
        if not api_key or not api_secret:
            raise GrowwApiError("Missing Groww API credentials in Secret Manager")
        self._cached_credentials = GrowwCredentials(api_key=api_key, api_secret=api_secret, access_token=access_token, access_token_expiry=expiry)
        dt = parse_any_ts(expiry)
        self._cached_token_expiry_ts = dt.timestamp() if dt is not None else None
        return self._cached_credentials

    @staticmethod
    def _checksum(secret: str, timestamp: str) -> str:
        return hashlib.sha256((secret + timestamp).encode("utf-8")).hexdigest()

    def ensure_access_token(self) -> str:
        creds = self._read_credentials()
        if creds.access_token and creds.access_token_expiry:
            dt = parse_any_ts(creds.access_token_expiry)
            if dt is not None and (dt - now_utc()) > timedelta(minutes=5):
                return creds.access_token
        return self.refresh_access_token()

    def refresh_access_token(self) -> str:
        creds = self._read_credentials()
        ts = str(int(time.time()))
        checksum = self._checksum(creds.api_secret, ts)
        url = f"{self.settings.api_host}/v1/token/api/access"
        headers = {
            "Authorization": f"Bearer {creds.api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-API-VERSION": "1.0",
        }
        body = {"key_type": "approval", "checksum": checksum, "timestamp": ts}
        self.limiter.wait()
        resp = self.http.post(url, headers=headers, json=body)
        if resp.status_code < 200 or resp.status_code >= 300:
            raise GrowwApiError(f"Token refresh failed ({resp.status_code}): {resp.text[:300]}")
        data = resp.json()
        token = str(data.get("token") or data.get("access_token") or "").strip()
        expiry = str(data.get("expiry") or data.get("expires_at") or "").strip()
        if not token:
            raise GrowwApiError(f"Token refresh response missing token: {data}")
        self.secrets.add_secret_version(self.settings.access_token_secret_name, token)
        if expiry:
            self.secrets.add_secret_version(self.settings.access_token_expiry_secret_name, expiry)
        self._cached_credentials = GrowwCredentials(
            api_key=creds.api_key,
            api_secret=creds.api_secret,
            access_token=token,
            access_token_expiry=expiry,
        )
        dt = parse_any_ts(expiry)
        self._cached_token_expiry_ts = dt.timestamp() if dt else None
        logger.info("Groww access token refreshed")
        return token

    def _headers(self) -> dict[str, str]:
        token = self.ensure_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "X-API-VERSION": "1.0",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    @staticmethod
    def _parse_payload(text: str, endpoint: str) -> Any:
        stripped = (text or "").strip()
        if not stripped:
            return None
        try:
            data = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise GrowwApiError(f"Non-JSON response for {endpoint}: {stripped[:200]}") from exc
        if isinstance(data, dict):
            status = str(data.get("status", "")).upper()
            if status == "SUCCESS":
                return data.get("payload", {})
            if any(k in data for k in ("token", "access_token", "groww_order_id", "candles", "last_price", "data")):
                return data
            msg = data.get("message") or (data.get("error") or {}).get("message") or str(data)[:250]
            raise GrowwApiError(f"Groww error [{endpoint}]: {msg}")
        return data

    def _request(self, method: str, endpoint: str, *, params: dict[str, Any] | None = None, json_body: dict[str, Any] | None = None, auth: bool = True) -> Any:
        url = endpoint if endpoint.startswith("http") else f"{self.settings.api_host.rstrip('/')}/v1/{endpoint.lstrip('/')}"
        last_exc: Exception | None = None
        for attempt in range(1, self.settings.max_retries + 1):
            try:
                self.limiter.wait()
                resp = self.http.request(
                    method.upper(),
                    url,
                    params=params,
                    json=json_body,
                    headers=(self._headers() if auth else {"Accept": "application/json"}),
                )
                if resp.status_code in (401, 403) and auth:
                    logger.warning("Groww auth %s on %s (attempt %s), refreshing token", resp.status_code, endpoint, attempt)
                    self.refresh_access_token()
                    continue
                if resp.status_code == 429 or 500 <= resp.status_code <= 599:
                    logger.warning("Transient Groww HTTP %s on %s (attempt %s)", resp.status_code, endpoint, attempt)
                    time.sleep(0.5 * attempt)
                    continue
                if resp.status_code < 200 or resp.status_code >= 300:
                    raise GrowwApiError(f"HTTP {resp.status_code} [{endpoint}]: {resp.text[:300]}")
                return self._parse_payload(resp.text, endpoint)
            except (httpx.TimeoutException, httpx.TransportError, GrowwApiError) as exc:
                last_exc = exc
                if isinstance(exc, GrowwApiError) and "HTTP 4" in str(exc) and "429" not in str(exc):
                    raise
                if attempt >= self.settings.max_retries:
                    break
                time.sleep(0.4 * attempt)
        raise GrowwApiError(f"Groww request failed [{endpoint}]: {last_exc}")

    def get_quote(self, symbol: str, exchange: str = "NSE", segment: str = "CASH") -> Quote:
        payload = self._request(
            "GET",
            "live-data/quote",
            params={"exchange": exchange, "segment": segment, "trading_symbol": symbol},
        )
        ohlc = payload.get("ohlc") if isinstance(payload, dict) else None
        if isinstance(ohlc, str):
            ohlc_map = {}
            for key in ("open", "high", "low", "close"):
                try:
                    import re

                    m = re.search(rf"{key}\s*:\s*([\d.]+)", ohlc)
                    ohlc_map[key] = float(m.group(1)) if m else 0.0
                except Exception:
                    ohlc_map[key] = 0.0
            ohlc = ohlc_map
        ohlc = ohlc or {}
        ltp = float(payload.get("last_price") or payload.get("ltp") or 0.0)
        return Quote(
            ltp=ltp,
            open=float(ohlc.get("open") or 0.0),
            high=float(ohlc.get("high") or 0.0),
            low=float(ohlc.get("low") or 0.0),
            close=float(ohlc.get("close") or ltp or 0.0),
            volume=float(payload.get("volume") or 0.0),
            change_pct=float(payload.get("day_change_perc") or 0.0),
            change=float(payload.get("day_change") or 0.0),
            bid=float((((payload.get("depth") or {}).get("buy") or [{}])[0] or {}).get("price") or payload.get("bid_price") or 0.0),
            ask=float((((payload.get("depth") or {}).get("sell") or [{}])[0] or {}).get("price") or payload.get("offer_price") or 0.0),
        )

    @staticmethod
    def _groww_symbol(exchange: str, symbol: str) -> str:
        return f"{exchange.upper()}-{symbol.upper()}"

    @staticmethod
    def _candle_interval_str(tf: str) -> str:
        s = str(tf).lower().strip()
        mapping = {
            "1m": "1minute",
            "2m": "2minute",
            "3m": "3minute",
            "5m": "5minute",
            "10m": "10minute",
            "15m": "15minute",
            "30m": "30minute",
            "60m": "60minute",
            "1h": "60minute",
            "240m": "240minute",
            "4h": "240minute",
            "1d": "1day",
            "1w": "1week",
            "1mo": "1month",
        }
        return mapping.get(s, "15minute")

    def get_candles_range(self, symbol: str, exchange: str, segment: str, timeframe: str, start_ist: str, end_ist: str) -> list[list[Any]]:
        groww_symbol = self._groww_symbol(exchange, symbol)
        payload = self._request(
            "GET",
            "historical/candles",
            params={
                "exchange": exchange,
                "segment": segment,
                "groww_symbol": groww_symbol,
                "start_time": start_ist,
                "end_time": end_ist,
                "candle_interval": self._candle_interval_str(timeframe),
            },
        )
        candles = []
        if isinstance(payload, dict):
            candles = payload.get("candles") or ((payload.get("payload") or {}).get("candles") if isinstance(payload.get("payload"), dict) else []) or []
        elif isinstance(payload, list):
            candles = payload
        return [c for c in candles if isinstance(c, list) and len(c) >= 6]

    def create_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        out = self._request("POST", "order/create", json_body=payload)
        return out if isinstance(out, dict) else {"raw": out}

    def list_orders(self) -> list[dict[str, Any]]:
        out = self._request("GET", "orders")
        if isinstance(out, list):
            return [o for o in out if isinstance(o, dict)]
        if isinstance(out, dict):
            for key in ("orders", "order_list", "items", "results", "data"):
                val = out.get(key)
                if isinstance(val, list):
                    return [o for o in val if isinstance(o, dict)]
                if isinstance(val, dict) and isinstance(val.get("orders"), list):
                    return [o for o in val["orders"] if isinstance(o, dict)]
        return []

    def create_smart_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        out = self._request("POST", "order-advance/create", json_body=payload)
        return out if isinstance(out, dict) else {"raw": out}

    def cancel_smart_order(self, segment: str, smart_order_type: str, smart_order_id: str) -> dict[str, Any]:
        endpoint = f"order-advance/cancel/{segment}/{smart_order_type}/{smart_order_id}"
        out = self._request("POST", endpoint, json_body={})
        return out if isinstance(out, dict) else {"raw": out}

    def fetch_instruments_csv(self) -> str:
        for url in GROWW_INSTRUMENT_CSV_URLS:
            try:
                resp = self.http.get(url, headers={"Accept": "text/csv"})
                if resp.status_code == 200 and resp.text.strip():
                    return resp.text
            except Exception:
                logger.debug("Instrument CSV fetch failed from %s", url, exc_info=True)
        raise GrowwApiError("Instrument CSV fetch failed from all configured URLs")

    def close(self) -> None:
        self.http.close()

