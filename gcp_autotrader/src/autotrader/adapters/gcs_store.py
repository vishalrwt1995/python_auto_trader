from __future__ import annotations

import json
import re
from typing import Any

from autotrader.domain.indicators import normalize_candles


class GoogleCloudStorageStore:
    def __init__(self, bucket_name: str, *, pool_maxsize: int = 64):
        """`pool_maxsize` sizes the urllib3 connection pool that backs the
        underlying requests session. The default of 10 chokes when the
        backtester bulk-loads 30+ symbols concurrently — every request
        beyond pool size eats a fresh TLS handshake. 64 covers the GCS
        loader's default concurrency=32 with headroom."""
        self.bucket_name = bucket_name
        self._pool_maxsize = max(10, int(pool_maxsize))
        self._client = None
        self._bucket = None

    def _get_bucket(self):
        if self._bucket is not None:
            return self._bucket
        from google.cloud import storage

        # Build the client; then resize its HTTP adapters to avoid
        # "Connection pool is full, discarding connection" warnings under
        # high concurrency. The storage client uses an AuthorizedSession
        # internally — patch its mounted adapters.
        client = storage.Client()
        try:
            from requests.adapters import HTTPAdapter
            session = client._http  # AuthorizedSession (subclass of Session)
            adapter = HTTPAdapter(
                pool_connections=self._pool_maxsize,
                pool_maxsize=self._pool_maxsize,
            )
            session.mount("https://", adapter)
            session.mount("http://", adapter)
        except Exception:
            # Best-effort: an outdated google-cloud-storage that doesn't
            # expose `_http` will still work — just with the old warnings.
            pass
        self._client = client
        self._bucket = client.bucket(self.bucket_name)
        return self._bucket

    def exists(self, path: str) -> bool:
        blob = self._get_bucket().blob(path)
        return blob.exists()

    def read_text(self, path: str) -> str | None:
        blob = self._get_bucket().blob(path)
        if not blob.exists():
            return None
        return blob.download_as_text()

    def read_bytes(self, path: str) -> bytes | None:
        blob = self._get_bucket().blob(path)
        if not blob.exists():
            return None
        return blob.download_as_bytes()

    def write_text(self, path: str, data: str, content_type: str = "text/plain") -> None:
        blob = self._get_bucket().blob(path)
        blob.upload_from_string(data, content_type=content_type)

    def write_bytes(self, path: str, data: bytes, content_type: str = "application/octet-stream") -> None:
        blob = self._get_bucket().blob(path)
        blob.upload_from_string(data, content_type=content_type)

    def read_json(self, path: str, default: Any = None) -> Any:
        txt = self.read_text(path)
        if txt is None:
            return default
        try:
            return json.loads(txt)
        except Exception:
            return default

    def write_json(self, path: str, data: Any) -> None:
        self.write_text(path, json.dumps(data, separators=(",", ":"), ensure_ascii=False), content_type="application/json")

    def list_paths(self, prefix: str) -> list[str]:
        bucket = self._get_bucket()
        return [b.name for b in bucket.list_blobs(prefix=prefix)]

    @staticmethod
    def candle_cache_path(symbol: str, exchange: str, segment: str, timeframe: str) -> str:
        return f"cache/candles/{timeframe.lower()}/{exchange.upper()}/{segment.upper()}/{symbol.upper()}.json"

    @staticmethod
    def history_path(symbol: str, exchange: str, segment: str, timeframe: str) -> str:
        return f"history/{timeframe.lower()}/{exchange.upper()}/{segment.upper()}/{symbol.upper()}.json"

    @staticmethod
    def score_cache_1d_path(symbol: str, exchange: str, segment: str) -> str:
        return f"cache/score_1d/{exchange.upper()}/{segment.upper()}/{symbol.upper()}.json"

    @staticmethod
    def score_cache_1d_path_by_instrument_key(instrument_key: str, exchange: str, segment: str) -> str:
        raw = str(instrument_key or "").strip().upper()
        if not raw:
            return GoogleCloudStorageStore.score_cache_1d_path("UNKNOWN", exchange, segment)
        # Upstox recommends instrument_key as the stable identifier; use a sanitized path-safe key.
        safe = re.sub(r"[^A-Z0-9._-]+", "_", raw)
        safe = re.sub(r"_+", "_", safe).strip("_")
        return f"cache/score_1d_by_instrument/{exchange.upper()}/{segment.upper()}/{safe}.json"

    @staticmethod
    def upstox_raw_universe_versioned_path(run_date: str, run_stamp: str | None = None) -> str:
        if run_stamp:
            return f"raw/upstox/universe/{run_date}/{run_stamp}/complete.json.gz"
        return f"raw/upstox/universe/{run_date}/complete.json.gz"

    @staticmethod
    def upstox_raw_universe_latest_path() -> str:
        return "raw/upstox/universe/latest/complete.json.gz"

    @staticmethod
    def upstox_raw_universe_latest_meta_path() -> str:
        return "raw/upstox/universe/latest/meta.json"

    def read_candles(self, path: str) -> list[list[Any]]:
        data = self.read_json(path, default=[])
        return data if isinstance(data, list) else []

    def write_candles(self, path: str, candles: list[list[Any]] | list[tuple[Any, ...]]) -> None:
        self.write_json(path, list(candles))

    def merge_candles(self, path: str, candles: list[list[Any]] | list[tuple[Any, ...]]) -> list[list[Any]]:
        existing = self.read_candles(path)
        by_ts: dict[str, list[Any]] = {}
        for c in normalize_candles(existing):
            by_ts[str(c[0])] = list(c)
        for c in normalize_candles(candles):
            by_ts[str(c[0])] = list(c)
        merged = [by_ts[k] for k in sorted(by_ts.keys())]
        self.write_candles(path, merged)
        return merged
