from __future__ import annotations

import gzip
import json
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote, unquote_plus

import httpx

from autotrader.adapters.secrets_manager import SecretManagerStore
from autotrader.domain.models import Quote
from autotrader.settings import UpstoxSettings
from autotrader.time_utils import IST, now_ist, now_utc, parse_any_ts

logger = logging.getLogger(__name__)


@dataclass
class UpstoxCredentials:
    client_id: str
    client_secret: str
    access_token: str = ""
    access_token_expiry: str = ""


class UpstoxApiError(RuntimeError):
    pass


class MultiWindowRateLimiter:
    """Process-local limiter for 1s / 60s / 1800s windows."""

    def __init__(self, *, per_second: int, per_minute: int, per_30min: int):
        self.per_second = max(1, per_second)
        self.per_minute = max(1, per_minute)
        self.per_30min = max(1, per_30min)
        self._lock = threading.Lock()
        self._sec = deque()
        self._min = deque()
        self._half_hr = deque()

    @staticmethod
    def _trim(q: deque[float], cutoff: float) -> None:
        while q and q[0] < cutoff:
            q.popleft()

    def wait(self) -> None:
        with self._lock:
            while True:
                now = time.monotonic()
                self._trim(self._sec, now - 1.0)
                self._trim(self._min, now - 60.0)
                self._trim(self._half_hr, now - 1800.0)

                if (
                    len(self._sec) < self.per_second
                    and len(self._min) < self.per_minute
                    and len(self._half_hr) < self.per_30min
                ):
                    self._sec.append(now)
                    self._min.append(now)
                    self._half_hr.append(now)
                    return

                waits: list[float] = []
                if len(self._sec) >= self.per_second:
                    waits.append(max(0.01, 1.0 - (now - self._sec[0])))
                if len(self._min) >= self.per_minute:
                    waits.append(max(0.05, 60.0 - (now - self._min[0])))
                if len(self._half_hr) >= self.per_30min:
                    waits.append(max(0.05, 1800.0 - (now - self._half_hr[0])))
                time.sleep(min(waits) if waits else 0.05)


class UpstoxClient:
    def __init__(self, settings: UpstoxSettings, secrets: SecretManagerStore, timeout_sec: float = 30.0):
        self.settings = settings
        self.secrets = secrets
        self.http = httpx.Client(timeout=timeout_sec, follow_redirects=True)
        self.limiter = MultiWindowRateLimiter(
            per_second=settings.requests_per_second,
            per_minute=settings.max_per_minute,
            per_30min=settings.max_per_30min,
        )
        self._cached_credentials: UpstoxCredentials | None = None
        self._cached_token_expiry_ts: float | None = None

    def _read_credentials(self) -> UpstoxCredentials:
        if self._cached_credentials is not None:
            if self._cached_token_expiry_ts is None or (self._cached_token_expiry_ts - time.time()) > 300:
                return self._cached_credentials
        client_id = (self.secrets.get_secret(self.settings.client_id_secret_name) or "").strip()
        client_secret = (self.secrets.get_secret(self.settings.client_secret_secret_name) or "").strip()
        access_token = (self.secrets.get_secret(self.settings.access_token_secret_name) or "").strip()
        expiry = (self.secrets.get_secret(self.settings.access_token_expiry_secret_name) or "").strip()
        if not client_id or not client_secret:
            raise UpstoxApiError("Missing Upstox client credentials in Secret Manager")
        self._cached_credentials = UpstoxCredentials(
            client_id=client_id,
            client_secret=client_secret,
            access_token=access_token,
            access_token_expiry=expiry,
        )
        dt = parse_any_ts(expiry)
        self._cached_token_expiry_ts = dt.timestamp() if dt is not None else None
        return self._cached_credentials

    @staticmethod
    def _fallback_expiry_ist() -> str:
        # Upstox access tokens are valid until ~03:30 AM on the next day.
        now_i = now_ist()
        next_day = now_i.date() + timedelta(days=1)
        dt = datetime(next_day.year, next_day.month, next_day.day, 3, 30, 0, tzinfo=IST)
        return dt.isoformat()

    def ensure_access_token(self) -> str:
        creds = self._read_credentials()
        if creds.access_token:
            dt = parse_any_ts(creds.access_token_expiry)
            if dt is not None and (dt - now_utc()) > timedelta(minutes=10):
                return creds.access_token
            if dt is None:
                # If expiry is not stored, still use token optimistically; 401 will trigger error.
                return creds.access_token
        if self.settings.auth_code_secret_name:
            code = (self.secrets.get_secret(self.settings.auth_code_secret_name) or "").strip()
            if code and code.upper() != "INIT":
                return self.exchange_auth_code(code)
        raise UpstoxApiError(
            "Upstox access token missing/expired. Upstox official API does not provide a refresh token flow. "
            "Generate a new auth code and store it in Secret Manager (UPSTOX_AUTH_CODE_SECRET_NAME), then retry."
        )

    def exchange_auth_code(self, auth_code: str) -> str:
        creds = self._read_credentials()
        if not self.settings.redirect_uri:
            raise UpstoxApiError("UPSTOX_REDIRECT_URI is required for auth code exchange")
        url = f"{self.settings.api_v2_host}/login/authorization/token"
        data = {
            "code": auth_code,
            "client_id": creds.client_id,
            "client_secret": creds.client_secret,
            "redirect_uri": self.settings.redirect_uri,
            "grant_type": "authorization_code",
        }
        headers = {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded", "Api-Version": "2.0"}
        self.limiter.wait()
        resp = self.http.post(url, data=data, headers=headers)
        if resp.status_code < 200 or resp.status_code >= 300:
            raise UpstoxApiError(f"Upstox auth code exchange failed ({resp.status_code}): {resp.text[:400]}")
        payload = self._parse_payload(resp.text, "login/authorization/token")
        access_token = str(payload.get("access_token") or "").strip()
        expires_at = str(payload.get("expires_at") or "").strip() or self._fallback_expiry_ist()
        if not access_token:
            raise UpstoxApiError(f"Upstox auth code exchange response missing access_token: {payload}")
        self.secrets.add_secret_version(self.settings.access_token_secret_name, access_token)
        self.secrets.add_secret_version(self.settings.access_token_expiry_secret_name, expires_at)
        self._cached_credentials = UpstoxCredentials(
            client_id=creds.client_id,
            client_secret=creds.client_secret,
            access_token=access_token,
            access_token_expiry=expires_at,
        )
        dt = parse_any_ts(expires_at)
        self._cached_token_expiry_ts = dt.timestamp() if dt else None
        logger.info("Upstox access token exchanged and stored (expires_at=%s)", expires_at)
        return access_token

    def request_access_token_v3(self) -> dict[str, Any]:
        """Request a fresh Upstox access-token authorization via notifier flow."""
        creds = self._read_credentials()
        endpoint = f"login/auth/token/request/{quote(creds.client_id, safe='')}"
        data = self._request(
            "POST",
            endpoint,
            json_body={"client_secret": creds.client_secret},
            auth=False,
            version="v3",
        )
        if not isinstance(data, dict):
            return {"ok": True, "raw": data}
        return {
            "ok": True,
            "status": str(data.get("status") or "success"),
            "message": str(data.get("message") or ""),
            "authorization_expiry": str(data.get("authorization_expiry") or ""),
            "notifier_url": str(data.get("notifier_url") or ""),
        }

    @staticmethod
    def _normalize_expiry_for_storage(expires_at: Any) -> str:
        dt = parse_any_ts(expires_at)
        if dt is None:
            return str(expires_at or "").strip() or UpstoxClient._fallback_expiry_ist()
        return dt.astimezone(timezone.utc).isoformat()

    def ingest_notifier_payload(self, payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict):
            return {"accepted": False, "reason": "payload_not_object"}

        data = payload.get("data")
        if isinstance(data, dict):
            msg = data
        else:
            msg = payload

        message_type = str(msg.get("message_type") or payload.get("message_type") or "").strip().lower()
        if message_type and message_type != "access_token":
            return {"accepted": True, "messageType": message_type, "stored": False}

        access_token = str(msg.get("access_token") or "").strip()
        expires_at_raw = msg.get("expires_at")
        client_id = str(msg.get("client_id") or "").strip()
        if not access_token:
            return {"accepted": False, "reason": "missing_access_token", "messageType": message_type or "unknown"}

        try:
            creds = self._read_credentials()
        except Exception:
            creds = None
        if creds is not None and client_id and client_id != creds.client_id:
            raise UpstoxApiError("Upstox notifier client_id mismatch")

        expires_at = self._normalize_expiry_for_storage(expires_at_raw)
        self.secrets.add_secret_version(self.settings.access_token_secret_name, access_token)
        self.secrets.add_secret_version(self.settings.access_token_expiry_secret_name, expires_at)

        if creds is not None:
            self._cached_credentials = UpstoxCredentials(
                client_id=creds.client_id,
                client_secret=creds.client_secret,
                access_token=access_token,
                access_token_expiry=expires_at,
            )
        dt = parse_any_ts(expires_at)
        self._cached_token_expiry_ts = dt.timestamp() if dt else None
        logger.info("Upstox notifier stored access token (expires_at=%s)", expires_at)
        return {
            "accepted": True,
            "messageType": message_type or "access_token",
            "stored": True,
            "expires_at": expires_at,
            "client_id_match": (not client_id) or (creds is not None and client_id == creds.client_id),
        }

    @staticmethod
    def _parse_payload(text: str, endpoint: str) -> Any:
        stripped = (text or "").strip()
        if not stripped:
            return None
        try:
            data = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise UpstoxApiError(f"Non-JSON response for {endpoint}: {stripped[:300]}") from exc

        if isinstance(data, dict):
            status = str(data.get("status", "")).lower()
            if status in {"success", "ok"} and "data" in data:
                return data.get("data")
            if "access_token" in data:
                return data
            errors = data.get("errors")
            if errors:
                raise UpstoxApiError(f"Upstox error [{endpoint}]: {errors}")
            if "data" in data:
                return data.get("data")
        return data

    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        auth: bool = True,
        version: str = "v2",
        content_type: str | None = "application/json",
    ) -> Any:
        base = self.settings.api_v3_host if version == "v3" else self.settings.api_v2_host
        url = endpoint if endpoint.startswith("http") else f"{base.rstrip('/')}/{endpoint.lstrip('/')}"
        last_exc: Exception | None = None
        for attempt in range(1, self.settings.max_retries + 1):
            try:
                self.limiter.wait()
                headers: dict[str, str] = {"Accept": "application/json", "Api-Version": "2.0"}
                if auth:
                    headers["Authorization"] = f"Bearer {self.ensure_access_token()}"
                if content_type:
                    headers["Content-Type"] = content_type
                resp = self.http.request(method.upper(), url, params=params, json=json_body, headers=headers)
                if resp.status_code in (401, 403):
                    raise UpstoxApiError(f"HTTP {resp.status_code} [{endpoint}]: token invalid/expired")
                if resp.status_code == 429 or 500 <= resp.status_code <= 599:
                    time.sleep(0.5 * attempt)
                    continue
                if resp.status_code < 200 or resp.status_code >= 300:
                    raise UpstoxApiError(f"HTTP {resp.status_code} [{endpoint}]: {resp.text[:400]}")
                return self._parse_payload(resp.text, endpoint)
            except (httpx.TimeoutException, httpx.TransportError, UpstoxApiError) as exc:
                last_exc = exc
                if isinstance(exc, UpstoxApiError) and "HTTP 4" in str(exc) and "429" not in str(exc):
                    raise
                if attempt >= self.settings.max_retries:
                    break
                time.sleep(0.4 * attempt)
        raise UpstoxApiError(f"Upstox request failed [{endpoint}]: {last_exc}")

    def fetch_instruments_complete_gz(self) -> bytes:
        self.limiter.wait()
        resp = self.http.get(self.settings.instruments_complete_url, headers={"Accept": "application/gzip"})
        if resp.status_code < 200 or resp.status_code >= 300:
            raise UpstoxApiError(f"Upstox instruments download failed ({resp.status_code}): {resp.text[:200]}")
        return resp.content

    @staticmethod
    def decode_instruments_gz_json(blob: bytes) -> list[dict[str, Any]]:
        try:
            raw = gzip.decompress(blob)
        except OSError:
            raw = blob
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise UpstoxApiError("Unable to parse Upstox instruments JSON") from exc
        if isinstance(parsed, list):
            return [r for r in parsed if isinstance(r, dict)]
        if isinstance(parsed, dict):
            data = parsed.get("data")
            if isinstance(data, list):
                return [r for r in data if isinstance(r, dict)]
        raise UpstoxApiError(f"Unexpected Upstox instruments payload shape: {type(parsed).__name__}")

    @staticmethod
    def _enc_instrument_key(key: str) -> str:
        return quote(str(key).strip(), safe="")

    @staticmethod
    def _norm_instrument_key(key: str) -> str:
        return unquote_plus(str(key or "")).strip().upper().replace(":", "|")

    @staticmethod
    def _safe_float(v: Any) -> float:
        try:
            return float(v)
        except Exception:
            return 0.0

    @classmethod
    def _extract_quote_from_row(cls, row: dict[str, Any]) -> Quote:
        ltpc = row.get("ltpc") if isinstance(row.get("ltpc"), dict) else {}
        ohlc = row.get("ohlc") if isinstance(row.get("ohlc"), dict) else {}
        market_data = row.get("market_data") if isinstance(row.get("market_data"), dict) else {}
        ff = row.get("ff") if isinstance(row.get("ff"), dict) else {}
        ff_market = ff.get("marketFF") if isinstance(ff.get("marketFF"), dict) else {}
        ff_ltpc = ff_market.get("ltpc") if isinstance(ff_market.get("ltpc"), dict) else {}
        ff_ohlc = ff_market.get("ohlc") if isinstance(ff_market.get("ohlc"), dict) else {}
        ff_index = ff.get("indexFF") if isinstance(ff.get("indexFF"), dict) else {}
        ff_index_ltpc = ff_index.get("ltpc") if isinstance(ff_index.get("ltpc"), dict) else {}

        ltp = (
            cls._safe_float(row.get("ltp"))
            or cls._safe_float(row.get("last_price"))
            or cls._safe_float(ltpc.get("ltp"))
            or cls._safe_float(market_data.get("ltp"))
            or cls._safe_float(ff_ltpc.get("ltp"))
        )
        prev_close = (
            cls._safe_float(row.get("cp"))
            or cls._safe_float(row.get("close_price"))
            or cls._safe_float(row.get("close"))
            or cls._safe_float(ltpc.get("cp"))
            or cls._safe_float(ohlc.get("close"))
            or cls._safe_float(market_data.get("close"))
            or cls._safe_float(ff_ltpc.get("cp"))
            or cls._safe_float(ff_ohlc.get("close"))
        )
        change_pct = (
            cls._safe_float(row.get("change_pct"))
            or cls._safe_float(row.get("change_percentage"))
            or cls._safe_float(ltpc.get("change_percentage"))
            or cls._safe_float(market_data.get("change_percentage"))
            or cls._safe_float(ff_ltpc.get("change_percentage"))
        )
        open_px = (
            cls._safe_float(row.get("open"))
            or cls._safe_float(ohlc.get("open"))
            or cls._safe_float(market_data.get("open"))
            or cls._safe_float(ff_ohlc.get("open"))
        )
        high_px = (
            cls._safe_float(row.get("high"))
            or cls._safe_float(ohlc.get("high"))
            or cls._safe_float(market_data.get("high"))
            or cls._safe_float(ff_ohlc.get("high"))
        )
        low_px = (
            cls._safe_float(row.get("low"))
            or cls._safe_float(ohlc.get("low"))
            or cls._safe_float(market_data.get("low"))
            or cls._safe_float(ff_ohlc.get("low"))
        )
        if not change_pct and ltp and prev_close:
            change_pct = ((ltp - prev_close) / prev_close) * 100.0
        ts_raw = (
            row.get("timestamp")
            or row.get("last_trade_time")
            or row.get("ltt")
            or ltpc.get("ltt")
            or market_data.get("ltt")
            or ff_ltpc.get("ltt")
            or ff_index_ltpc.get("ltt")
        )
        ts = ""
        if ts_raw is not None:
            try:
                parsed = parse_any_ts(ts_raw)
                if parsed is not None:
                    ts = parsed.astimezone(timezone.utc).isoformat()
                else:
                    ts = str(ts_raw).strip()
            except Exception:
                ts = str(ts_raw).strip()
        return Quote(
            ltp=float(ltp or 0.0),
            open=float(open_px or 0.0),
            high=float(high_px or 0.0),
            low=float(low_px or 0.0),
            close=float(prev_close or ltp or 0.0),
            change_pct=float(change_pct or 0.0),
            ts=ts,
        )

    def get_historical_candles_v3_days(
        self,
        instrument_key: str,
        to_date: str,
        from_date: str | None = None,
        interval_days: int = 1,
    ) -> list[list[Any]]:
        ik = self._enc_instrument_key(instrument_key)
        endpoint = f"historical-candle/{ik}/days/{int(interval_days)}/{to_date}"
        if from_date:
            endpoint = f"{endpoint}/{from_date}"
        data = self._request("GET", endpoint, auth=True, version="v3", content_type=None)
        candles = []
        if isinstance(data, dict):
            candles = data.get("candles") or []
        elif isinstance(data, list):
            candles = data
        return [c[:6] for c in candles if isinstance(c, list) and len(c) >= 6]

    def get_intraday_candles_v3(
        self,
        instrument_key: str,
        unit: str = "minutes",
        interval: int = 15,
    ) -> list[list[Any]]:
        ik = self._enc_instrument_key(instrument_key)
        # Upstox v3 path expects interval first, then unit: .../{interval}/{unit}
        # Keep a compatibility fallback for older assumptions to avoid hard failures.
        base = "historical-candle/intra-day"
        norm_unit = str(unit or "minutes").strip().lower()
        if norm_unit == "minute":
            norm_unit = "minutes"
        attempts = [
            f"{base}/{ik}/{int(interval)}/{norm_unit}",
            f"{base}/{ik}/{norm_unit}/{int(interval)}",
        ]
        data: Any = {}
        last_exc: Exception | None = None
        for endpoint in attempts:
            try:
                data = self._request("GET", endpoint, auth=True, version="v3", content_type=None)
                break
            except Exception as exc:
                last_exc = exc
                data = {}
        if not data and last_exc is not None:
            raise last_exc
        candles = []
        if isinstance(data, dict):
            candles = data.get("candles") or []
        elif isinstance(data, list):
            candles = data
        return [c[:6] for c in candles if isinstance(c, list) and len(c) >= 6]

    def get_ltp_v3(self, instrument_keys: list[str]) -> dict[str, Quote]:
        keys = [k.strip() for k in instrument_keys if str(k).strip()]
        if not keys:
            return {}
        data = self._request(
            "GET",
            "market-quote/ltp",
            params={"instrument_key": ",".join(keys)},
            auth=True,
            version="v3",
            content_type=None,
        )
        out: dict[str, Quote] = {}
        if not isinstance(data, dict):
            return out
        payload = data
        # Be permissive to evolving response shapes; _parse_payload usually unwraps `data`, but not always.
        if "data" in payload and isinstance(payload.get("data"), dict):
            payload = payload.get("data") or {}
        elif "quotes" in payload and isinstance(payload.get("quotes"), dict):
            payload = payload.get("quotes") or {}
        for key, row in payload.items():
            if not isinstance(row, dict):
                continue
            q = self._extract_quote_from_row(row)
            row_key = str(
                row.get("instrument_key")
                or row.get("instrument_token")
                or row.get("instrumentKey")
                or key
            ).strip()
            out[row_key] = q
            # Preserve original dictionary key as alias if it differs.
            if row_key != str(key):
                out[str(key)] = q
        return out

    def get_quote(self, instrument_key: str) -> Quote:
        q = self.get_ltp_v3([instrument_key])
        if instrument_key in q:
            return q[instrument_key]
        want = self._norm_instrument_key(instrument_key)
        for k, v in q.items():
            if self._norm_instrument_key(k) == want:
                return v
        # If only one quote came back but key alias differs, return it instead of zeroing regime inputs.
        if len(q) == 1:
            only = next(iter(q.values()))
            logger.debug("Upstox quote key mismatch requested=%s returned=%s", instrument_key, list(q.keys()))
            return only
        return Quote(ltp=0.0)

    def get_expiries(self, instrument_key: str) -> list[str]:
        ik = str(instrument_key or "").strip()
        if not ik:
            return []
        data = self._request(
            "GET",
            "expired-instruments/expiries",
            params={"instrument_key": ik},
            auth=True,
            version="v2",
            content_type=None,
        )
        out: list[str] = []
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get("expiries") or data.get("expiry_dates") or data.get("data") or []
        else:
            items = []
        for item in items:
            if isinstance(item, str):
                s = item.strip()
            elif isinstance(item, dict):
                s = str(item.get("expiry") or item.get("expiry_date") or item.get("date") or "").strip()
            else:
                s = ""
            if s:
                out.append(s)
        return out

    def get_option_chain(self, instrument_key: str, expiry_date: str) -> list[dict[str, Any]]:
        ik = str(instrument_key or "").strip()
        ex = str(expiry_date or "").strip()
        if not ik or not ex:
            return []
        data = self._request(
            "GET",
            "option/chain",
            params={"instrument_key": ik, "expiry_date": ex},
            auth=True,
            version="v2",
            content_type=None,
        )
        if isinstance(data, list):
            return [r for r in data if isinstance(r, dict)]
        if isinstance(data, dict):
            rows = data.get("option_chain") or data.get("data") or data.get("items") or []
            if isinstance(rows, list):
                return [r for r in rows if isinstance(r, dict)]
        return []

    def get_option_contracts(self, instrument_key: str, expiry_date: str | None = None) -> list[dict[str, Any]]:
        ik = str(instrument_key or "").strip()
        if not ik:
            return []
        params: dict[str, Any] = {"instrument_key": ik}
        ex = str(expiry_date or "").strip()
        if ex:
            params["expiry_date"] = ex
        data = self._request(
            "GET",
            "option/contract",
            params=params,
            auth=True,
            version="v2",
            content_type=None,
        )
        if isinstance(data, list):
            return [r for r in data if isinstance(r, dict)]
        if isinstance(data, dict):
            rows = data.get("contracts") or data.get("data") or data.get("items") or []
            if isinstance(rows, list):
                return [r for r in rows if isinstance(r, dict)]
        return []

    def close(self) -> None:
        self.http.close()
