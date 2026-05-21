"""Historical data accessor for the backtest engine.

Wraps the production GCS store so the backtest reads from EXACTLY the same
data cache the live system uses. No reimplementation, no parallel BQ
queries.

Path layout (per `cache/score_1d/{exchange}/{segment}/{symbol}.json`):
  - daily candles per symbol — same files the live system reads
  - intraday candles likewise under `cache/score_5m/...`
  - special instruments (NIFTY, VIX) under their own keys

Phase 1 scope: just expose the methods the backtest needs.  We do NOT
reimplement candle parsing — `gcs.read_candles(path)` returns the
already-normalized `list[Candle]` format that production indicators expect.

Phase 7 addition: local-disk mirror cache. Multi-year backtests over
500+ symbols hammer GCS with 1000s of small reads; on macOS this hits
ephemeral-port exhaustion. We mirror each GCS file to `~/.autotrader_backtest_cache/`
on first read and serve from disk thereafter.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import date as _date_cls
from pathlib import Path
from typing import Any

from autotrader.adapters.gcs_store import GoogleCloudStorageStore


# Production layout for daily candles.
# IMPORTANT discovery 2026-05-21: there are TWO daily candle sources in GCS:
#   - cache/score_1d/  — long history (2000+) but FROZEN at 2026-02-26
#   - cache/candles/1d/ — fresh up to today, but only ~6 months back
# Production live scoring reads from cache/candles/1d/ (fresh).
# For backtests post-Nov 2025 we use the fresh source; for older dates
# (before Nov 2025) we fall back to score_1d.
_DAILY_PATH_FRESH = "cache/candles/1d/{exchange}/{segment}/{symbol}.json"
_DAILY_PATH_HIST = "cache/score_1d/{exchange}/{segment}/{symbol}.json"
_DAILY_PATH_TMPL = _DAILY_PATH_HIST  # legacy default for old code paths


@dataclass
class HistoricalDataset:
    """Read-only accessor for cached daily/intraday candles.

    Construct with the production GCS bucket name (default matches the
    live deployment). Methods return the same `list[Candle]` format
    used by `compute_indicators` and `compute_daily_bias`.

    Local-disk mirror cache avoids hammering GCS on multi-symbol backtest
    loops (port exhaustion on macOS). Set `local_cache_dir=None` to disable.
    """

    bucket_name: str = "grow-profit-machine-autotrader-data"
    local_cache_dir: str | None = None

    def __post_init__(self) -> None:
        self.gcs = GoogleCloudStorageStore(self.bucket_name)
        if self.local_cache_dir is None:
            self.local_cache_dir = str(Path.home() / ".autotrader_backtest_cache")
        Path(self.local_cache_dir).mkdir(parents=True, exist_ok=True)

    def _read_candles_cached(self, path: str) -> list[list[Any]]:
        """Read candles via local-disk mirror to avoid repeated GCS hits."""
        local_fname = path.replace("/", "__")
        local_path = os.path.join(str(self.local_cache_dir), local_fname)
        if os.path.exists(local_path):
            try:
                with open(local_path) as fh:
                    return json.load(fh)
            except Exception:
                pass  # fall through to re-download
        # Critical: must call the production GCS path here, NOT self._read_candles_cached
        candles = self.gcs.read_candles(path) or []
        try:
            with open(local_path, "w") as fh:
                json.dump(candles, fh)
        except Exception:
            pass
        return candles

    # -------- Daily --------

    def daily_path(self, symbol: str, exchange: str = "NSE", segment: str = "CASH", source: str = "auto") -> str:
        """Return daily candle path for `symbol`.

        source:
          - "fresh" → cache/candles/1d/  (fresh up to today, only ~6mo history)
          - "hist"  → cache/score_1d/    (long history but frozen Feb 26, 2026)
          - "auto"  → fresh if available, else fall back to hist
        """
        tmpl = _DAILY_PATH_FRESH if source == "fresh" else _DAILY_PATH_HIST
        return tmpl.format(
            symbol=str(symbol).strip().upper(),
            exchange=str(exchange).strip().upper(),
            segment=str(segment).strip().upper(),
        )

    def daily_candles(
        self,
        symbol: str,
        end_date: _date_cls | str | None = None,
        exchange: str = "NSE",
        segment: str = "CASH",
    ) -> list[list[Any]]:
        """Return daily candles for `symbol`, optionally truncated to <= end_date.

        Merges both fresh + historical sources to get the longest series
        possible. Fresh source is preferred for dates after Nov 2025;
        historical source supplements older dates. Production live scoring
        reads from the fresh source (cache/candles/1d/).

        Returns empty list if both sources are missing.

        Candle format: [ts_iso, open, high, low, close, volume, ...]
        """
        # Try fresh first
        fresh = self._read_candles_cached(self.daily_path(symbol, exchange, segment, source="fresh")) or []
        hist = self._read_candles_cached(self.daily_path(symbol, exchange, segment, source="hist")) or []

        # Merge — historical for old dates, fresh for new dates
        # Both lists are sorted by ts. Find the cutover where fresh starts.
        if fresh and hist:
            fresh_start = str(fresh[0][0])[:10]
            merged = [c for c in hist if str(c[0])[:10] < fresh_start] + fresh
        elif fresh:
            merged = fresh
        else:
            merged = hist

        candles = merged
        if end_date is None:
            return candles
        end_str = str(end_date)
        # Truncate to entries strictly <= end_date (point-in-time correctness;
        # don't leak future data into a backtest at this date).
        return [c for c in candles if str(c[0])[:10] <= end_str]

    def has_daily(self, symbol: str, exchange: str = "NSE", segment: str = "CASH") -> bool:
        """Quick existence check without downloading."""
        return self.gcs.exists(self.daily_path(symbol, exchange, segment))

    # -------- Intraday --------

    def intraday_path(
        self, symbol: str, exchange: str = "NSE", segment: str = "CASH", timeframe: str = "5m"
    ) -> str:
        # Phase 1 discovery: live caches intraday under `cache/candles/{tf}/`
        # (not `cache/score_{tf}/` which is daily-only). Verified by listing
        # GCS — 2631 5m files and 603 15m files at this path.
        return f"cache/candles/{timeframe}/{exchange.upper()}/{segment.upper()}/{symbol.upper()}.json"

    def intraday_candles(
        self,
        symbol: str,
        end_date: _date_cls | str | None = None,
        exchange: str = "NSE",
        segment: str = "CASH",
        timeframe: str = "5m",
        end_ts: str | None = None,
    ) -> list[list[Any]]:
        """Intraday candles up to `end_ts` (preferred) or `end_date` (date-only).

        For accurate replay, callers should pass `end_ts` (full ISO timestamp).
        A scan at 09:22 IST sees only the 09:15-09:20 bar; without time-
        precision truncation, the replay sees the entire day's intraday and
        computes very different RSI/EMA/MACD values.

        `end_date` is kept for back-compat — truncates to date only.
        """
        path = self.intraday_path(symbol, exchange, segment, timeframe)
        candles = self._read_candles_cached(path) or []
        if end_ts is not None:
            cutoff = str(end_ts)
            return [c for c in candles if str(c[0]) <= cutoff]
        if end_date is None:
            return candles
        end_str = str(end_date)
        return [c for c in candles if str(c[0])[:10] <= end_str]

    # -------- Universe enumeration --------

    def list_daily_symbols(self, exchange: str = "NSE", segment: str = "CASH", limit: int = 100) -> list[str]:
        """List symbols that have a daily candle cache file."""
        prefix = f"cache/score_1d/{exchange.upper()}/{segment.upper()}/"
        paths = self.gcs.list_paths(prefix)
        out: list[str] = []
        for p in paths:
            name = p.rsplit("/", 1)[-1]
            if name.endswith(".json"):
                out.append(name[:-5])
            if len(out) >= limit:
                break
        return out
