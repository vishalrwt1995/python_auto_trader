from __future__ import annotations

import json
import logging
import math
import time
from dataclasses import dataclass
from datetime import date as date_cls, datetime, timedelta
from typing import Any

from autotrader.adapters.gcs_store import GoogleCloudStorageStore
from autotrader.adapters.sheets_repository import GoogleSheetsRepository, SheetNames
from autotrader.adapters.upstox_client import UpstoxClient
from autotrader.domain.indicators import compute_indicators
from autotrader.domain.models import RegimeSnapshot, UniverseRow
from autotrader.domain.scoring import compute_universe_score_breakdown, format_universe_score_calc_short
from autotrader.settings import StrategySettings
from autotrader.time_utils import IST, now_ist, now_ist_str, parse_any_ts, today_ist

logger = logging.getLogger(__name__)


@dataclass
class UniversePipelineResult:
    synced: int = 0
    scored: int = 0
    selected: int = 0
    coverage_pct: float = 0.0


class UniverseService:
    def __init__(
        self,
        sheets: GoogleSheetsRepository,
        gcs: GoogleCloudStorageStore,
        upstox: UpstoxClient,
        cfg: StrategySettings,
    ):
        self.sheets = sheets
        self.gcs = gcs
        self.upstox = upstox
        self.cfg = cfg

    @staticmethod
    def _flag_any(v: Any, default: bool = True) -> bool:
        if v is None:
            return default
        s = str(v).strip().lower()
        if s == "":
            return default
        if s in {"1", "true", "y", "yes", "active", "enabled"}:
            return True
        if s in {"0", "false", "n", "no", "inactive", "disabled"}:
            return False
        return default

    @staticmethod
    def _to_upper(row: dict[str, Any], *keys: str) -> str:
        for k in keys:
            v = row.get(k)
            if v is not None and str(v).strip():
                return str(v).strip().upper()
        return ""

    @staticmethod
    def _parse_pipe_kv(note: str) -> dict[str, str]:
        out: dict[str, str] = {}
        for part in str(note or "").split("|"):
            if "=" not in part:
                continue
            k, v = part.split("=", 1)
            k = k.strip().lower()
            v = v.strip()
            if k:
                out[k] = v
        return out

    @staticmethod
    def _score_calc_skip_short(code: str, detail: str = "") -> str:
        c = str(code or "").strip().upper()[:10] or "SKIP"
        d = str(detail or "").strip()[:24]
        return f"{c}:{d}" if d else c

    def _read_score_cache_index_snapshot(self) -> dict[tuple[str, str, str], dict[str, str]]:
        out: dict[tuple[str, str, str], dict[str, str]] = {}
        try:
            rows = self.sheets.read_sheet_rows(SheetNames.SCORE_CACHE_1D, 4)
        except Exception:
            logger.debug("Unable to read score-cache index snapshot", exc_info=True)
            return out
        for row in rows:
            if len(row) < 8:
                continue
            symbol = row[0].strip().upper() if len(row) > 0 else ""
            exchange = row[1].strip().upper() if len(row) > 1 else "NSE"
            segment = row[2].strip().upper() if len(row) > 2 else "CASH"
            if not symbol:
                continue
            notes = self._parse_pipe_kv(row[12] if len(row) > 12 else "")
            out[(symbol, exchange, segment)] = {
                "status": (row[7].strip().upper() if len(row) > 7 else ""),
                "last_candle_time": (row[5].strip() if len(row) > 5 else ""),
                "src": notes.get("src", ""),
                "expectedlcd": notes.get("expectedlcd", ""),
                "current": notes.get("current", ""),
                "terminal": notes.get("terminal", ""),
            }
        return out

    def _last_candle_text(self, candles: list[list[object]]) -> str:
        ts = self._last_candle_ts(candles)
        if ts is None:
            return ""
        return ts.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _last_candle_sig(candles: list[list[object]]) -> str:
        if not candles:
            return ""
        for row in reversed(candles):
            if not row or len(row) < 6:
                continue
            vals = row[:6]
            return "|".join(str(v) for v in vals)
        return ""

    @staticmethod
    def _is_provisional_source(source: str) -> bool:
        return "provisional" in str(source or "").strip().lower()

    @staticmethod
    def _daily_ts_for_ist_date(d: date_cls) -> str:
        return f"{d.isoformat()}T00:00:00+05:30"

    @staticmethod
    def _parse_iso_date(text: str) -> date_cls | None:
        s = str(text or "").strip()
        if not s:
            return None
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
        except ValueError:
            return None

    def _build_provisional_daily_from_intraday(
        self,
        instrument_key: str,
        *,
        target_date: date_cls,
        min_last_time: tuple[int, int] = (15, 25),
    ) -> list[object] | None:
        if not instrument_key:
            return None
        try:
            # 15m bars are sufficient for EOD provisional OHLCV and much lighter than 1m.
            intra = self.upstox.get_intraday_candles_v3(instrument_key, unit="minutes", interval=15)
        except Exception:
            logger.debug("intraday provisional fetch failed instrument=%s", instrument_key, exc_info=True)
            return None
        rows: list[list[object]] = []
        for c in intra:
            if not isinstance(c, list) or len(c) < 6:
                continue
            ts = parse_any_ts(c[0])
            if ts is None:
                continue
            ts_i = ts.astimezone(IST)
            if ts_i.date() != target_date:
                continue
            rows.append([ts_i, c])
        if not rows:
            return None
        rows.sort(key=lambda x: x[0])
        last_ts = rows[-1][0]
        if (last_ts.hour, last_ts.minute) < min_last_time:
            # Intraday feed may still be incomplete near close; do not create provisional yet.
            return None
        first = rows[0][1]
        last = rows[-1][1]
        try:
            o = float(first[1]); h = max(float(r[1][2]) for r in rows); l = min(float(r[1][3]) for r in rows)
            c = float(last[4]); v = sum(float(r[1][5]) for r in rows)
        except Exception:
            logger.debug("intraday provisional aggregation failed instrument=%s", instrument_key, exc_info=True)
            return None
        return [self._daily_ts_for_ist_date(target_date), o, h, l, c, v]

    def _prefetch_should_skip_stale_retry(
        self,
        prev_row: dict[str, str] | None,
        candles: list[list[object]],
        *,
        expected_lcd: str,
    ) -> bool:
        if not prev_row:
            return False
        prev_status = (prev_row.get("status") or "").upper()
        if prev_status not in {"STALE_READY", "STALE_SKIPPED"}:
            return False
        prev_expected = (prev_row.get("expectedlcd") or "").strip()
        if prev_expected and prev_expected != expected_lcd:
            return False
        current_last = self._last_candle_text(candles)
        if not current_last or (prev_row.get("last_candle_time") or "") != current_last:
            return False
        prev_src = (prev_row.get("src") or "").strip().lower()
        if prev_status == "STALE_SKIPPED":
            return True
        return prev_src in {"upstox_api_incremental", "gcs_score_cache_1d_stale_fetch_empty"}

    def _score_cache_index_row(
        self,
        u: UniverseRow,
        *,
        path: str,
        candles: list[list[object]],
        source: str,
        api_calls: int,
        min_bars: int,
        expected_lcd: str,
        updated_at: str,
    ) -> list[object]:
        last_ts = self._last_candle_ts(candles)
        last_candle = last_ts.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S") if last_ts is not None else ""
        bars = len(candles)
        is_current = self._daily_cache_is_current(candles) if bars else False
        terminal = ""
        if source == "gcs_score_cache_1d_insufficient_history_final":
            status = "INSUFFICIENT_HISTORY_FINAL"
            terminal = "INSUFFICIENT_HISTORY"
        elif source == "gcs_score_cache_1d_stale_terminal":
            status = "STALE_SKIPPED"
            terminal = "STALE_CACHE"
        elif bars == 0:
            status = "MISSING"
        elif bars < min_bars:
            status = "INSUFFICIENT_HISTORY"
        elif is_current:
            status = "FRESH_READY"
        else:
            status = "STALE_READY"

        last_error = ""
        if source in {
            "api_cap_blocked",
            "gcs_score_cache_1d_stale_api_cap_blocked",
            "gcs_score_cache_1d_stale_fetch_empty",
            "cache_only_missing",
            "missing_instrument_key",
            "empty",
        }:
            last_error = source

        file_name = path.rsplit("/", 1)[-1]
        isin = self._parse_pipe_kv(u.notes).get("isin", "")
        notes = f"Src={source}|ExpectedLCD={expected_lcd}|Current={'Y' if is_current else 'N'}"
        if self._is_provisional_source(source):
            notes += "|Provisional=Y"
        if terminal:
            notes += f"|Terminal={terminal}"
        return [
            u.symbol,
            u.exchange,
            u.segment,
            "Y" if str(u.enabled).upper() == "Y" else "N",
            bars,
            last_candle,
            updated_at,
            status,
            api_calls,
            last_error,
            file_name,
            isin,
            notes,
            path,
        ]

    def refresh_raw_universe_from_upstox(self) -> dict[str, object]:
        blob = self.upstox.fetch_instruments_complete_gz()
        rows = self.upstox.decode_instruments_gz_json(blob)
        run_date = today_ist()
        ver_path = self.gcs.upstox_raw_universe_versioned_path(run_date)
        latest_path = self.gcs.upstox_raw_universe_latest_path()
        meta_path = self.gcs.upstox_raw_universe_latest_meta_path()
        self.gcs.write_bytes(ver_path, blob, content_type="application/gzip")
        self.gcs.write_bytes(latest_path, blob, content_type="application/gzip")
        meta = {
            "provider": "UPSTOX",
            "runDate": run_date,
            "fetchedAt": now_ist_str(),
            "path": ver_path,
            "latestPath": latest_path,
            "itemCount": len(rows),
            "sourceUrl": self.upstox.settings.instruments_complete_url,
        }
        self.gcs.write_json(meta_path, meta)
        logger.info("raw_universe_refresh complete runDate=%s itemCount=%s", run_date, len(rows))
        return meta

    def _load_latest_upstox_raw_universe(self) -> tuple[list[dict[str, object]], dict[str, object]]:
        meta = self.gcs.read_json(self.gcs.upstox_raw_universe_latest_meta_path(), default={}) or {}
        blob = self.gcs.read_bytes(self.gcs.upstox_raw_universe_latest_path())
        if not blob:
            raise RuntimeError("Upstox raw universe snapshot not found in GCS. Run raw-universe-refresh first.")
        rows = self.upstox.decode_instruments_gz_json(blob)
        return rows, (meta if isinstance(meta, dict) else {})

    def build_trading_universe_from_upstox_raw(self, limit: int = 0, *, replace: bool = False) -> dict[str, int | str]:
        raw_rows, meta = self._load_latest_upstox_raw_universe()
        existing_count, existing_symbols = (0, set()) if replace else self.sheets.read_universe_row_count_and_symbols()

        dedup: dict[str, dict[str, object]] = {}
        pref_exchange: dict[str, str] = {}
        seen_rows = 0
        eligible_rows = 0
        for raw in raw_rows:
            if not isinstance(raw, dict):
                continue
            seen_rows += 1
            if limit and seen_rows > limit:
                break

            seg = self._to_upper(raw, "segment", "exchange_segment")
            exchange = self._to_upper(raw, "exchange")
            if not exchange and "_" in seg:
                exchange = seg.split("_", 1)[0]
            symbol = self._to_upper(raw, "trading_symbol", "tradingsymbol", "symbol")
            instrument_key = str(raw.get("instrument_key") or raw.get("instrumentKey") or "").strip()
            instrument_type = self._to_upper(raw, "instrument_type", "instrumentType", "instrument")
            security_type = self._to_upper(raw, "security_type", "securityType")
            isin = self._to_upper(raw, "isin")
            name = str(raw.get("name") or raw.get("company_name") or raw.get("companyName") or symbol).strip()

            if not symbol or not instrument_key:
                continue
            if seg not in {"NSE_EQ", "BSE_EQ"}:
                continue
            if exchange not in {"NSE", "BSE"}:
                continue
            if instrument_type and instrument_type not in {"EQ", "EQUITY"}:
                continue
            if not self._flag_any(raw.get("is_enabled"), True):
                continue
            if self._flag_any(raw.get("is_delisted"), False):
                continue
            if self._flag_any(raw.get("is_suspended"), False):
                continue
            if self._flag_any(raw.get("suspended"), False):
                continue

            eligible_rows += 1
            key = isin or symbol
            cand = {
                "symbol": symbol,
                "exchange": exchange,
                "segment": "CASH",
                "instrument_key": instrument_key,
                "source_segment": seg,
                "security_type": security_type or "UNKNOWN",
                "isin": isin,
                "name": name,
                "raw_json": raw,
            }
            if key not in dedup:
                dedup[key] = cand
                pref_exchange[key] = exchange
            else:
                if pref_exchange.get(key) != "NSE" and exchange == "NSE":
                    dedup[key] = cand
                    pref_exchange[key] = exchange

        out_rows: list[list[object]] = []
        next_idx = existing_count + 1
        for d in sorted(dedup.values(), key=lambda r: (str(r["symbol"]), str(r["exchange"]))):
            sym = str(d["symbol"])
            if sym in existing_symbols:
                continue
            notes = f"isin={d['isin']}|name={d['name']}|source=upstox_bod"
            out_rows.append([
                next_idx,
                d["symbol"],
                d["exchange"],
                "CASH",
                "BOTH",
                "AUTO",
                "UNKNOWN",
                1.0,
                "Y",
                0,
                notes,
                0,
                0,
                0,
                "",
                "",
                "",
                "",
                json.dumps(d["raw_json"], ensure_ascii=False, separators=(",", ":")),
                "",
                "",
                "UPSTOX",
                d["instrument_key"],
                d["source_segment"],
                d["security_type"],
                "",
            ])
            next_idx += 1
        if replace:
            self.sheets.replace_universe_rows(out_rows)
        else:
            self.sheets.append_universe_rows(out_rows)
        out = {
            "rows": len(out_rows) if replace else (existing_count + len(out_rows)),
            "appended": len(out_rows),
            "replaced": 1 if replace else 0,
            "rawSeen": seen_rows,
            "rawEligible": eligible_rows,
            "rawSnapshotDate": str(meta.get("runDate") or ""),
        }
        logger.info(
            "universe_build_from_raw complete appended=%s totalRows=%s replace=%s rawSeen=%s rawEligible=%s snapshotDate=%s",
            out["appended"], out["rows"], bool(replace), seen_rows, eligible_rows, out["rawSnapshotDate"],
        )
        return out

    def sync_universe_from_groww_instruments(self, limit: int = 0) -> int:
        # Backward-compatible endpoint name; now uses Upstox raw snapshot as source of truth.
        if not self.gcs.exists(self.gcs.upstox_raw_universe_latest_path()):
            self.refresh_raw_universe_from_upstox()
        out = self.build_trading_universe_from_upstox_raw(limit=limit, replace=False)
        return int(out.get("rows", 0))

    def _fetch_daily_candles_windowed(self, instrument_key: str, lookback_days: int) -> list[list[object]]:
        end = now_ist()
        start = end - timedelta(days=max(3650, lookback_days))
        return self._fetch_daily_candles_windowed_between(instrument_key, start, end)

    def _fetch_daily_candles_windowed_between(
        self,
        instrument_key: str,
        start: datetime,
        end: datetime,
    ) -> list[list[object]]:
        if end <= start:
            return []
        cursor = start
        seen: dict[str, list[object]] = {}
        while cursor < end:
            # Upstox v3 supports 1-day candles with up to 1 decade per request.
            win_end = min(cursor + timedelta(days=3650), end)
            from_str = cursor.strftime("%Y-%m-%d")
            to_str = win_end.strftime("%Y-%m-%d")
            part = self.upstox.get_historical_candles_v3_days(instrument_key, to_date=to_str, from_date=from_str, interval_days=1)
            for c in part:
                if len(c) >= 6:
                    seen[str(c[0])] = c[:6]
            cursor = win_end + timedelta(seconds=1)
            time.sleep(0.02)
        return [seen[k] for k in sorted(seen.keys())]

    @staticmethod
    def _last_candle_ts(candles: list[list[object]]) -> datetime | None:
        for row in reversed(candles):
            if not row:
                continue
            ts = parse_any_ts(row[0])
            if ts is not None:
                return ts
        return None

    @staticmethod
    def _first_candle_ts(candles: list[list[object]]) -> datetime | None:
        for row in candles:
            if not row:
                continue
            ts = parse_any_ts(row[0])
            if ts is not None:
                return ts
        return None

    @staticmethod
    def _prev_weekday(d: date_cls) -> date_cls:
        out = d
        while out.weekday() >= 5:
            out -= timedelta(days=1)
        return out

    def _expected_latest_daily_candle_date(self, now: datetime | None = None) -> date_cls:
        now = now or now_ist()
        now_i = now.astimezone(IST)
        # By design we start cache prefetch after 18:00 IST, so expect today's daily bar after that.
        if now_i.weekday() < 5 and (now_i.hour, now_i.minute) >= (18, 0):
            return now_i.date()
        return self._prev_weekday(now_i.date() - timedelta(days=1))

    def _daily_cache_is_current(self, candles: list[list[object]], now: datetime | None = None) -> bool:
        last_ts = self._last_candle_ts(candles)
        if last_ts is None:
            return False
        expected = self._expected_latest_daily_candle_date(now)
        last_date = last_ts.astimezone(IST).date()
        return last_date >= expected

    def _daily_cache_has_lookback(self, candles: list[list[object]], lookback_days: int, now: datetime | None = None) -> bool:
        first_ts = self._first_candle_ts(candles)
        if first_ts is None:
            return False
        now_i = (now or now_ist()).astimezone(IST)
        horizon = now_i - timedelta(days=max(3650, lookback_days))
        # Small tolerance for market holidays/weekends and provider availability.
        return first_ts.astimezone(IST) <= (horizon + timedelta(days=14))

    def _fetch_daily_candles_incremental(
        self,
        instrument_key: str,
        cached: list[list[object]],
        lookback_days: int,
    ) -> list[list[object]]:
        last_ts = self._last_candle_ts(cached)
        if last_ts is None:
            return self._fetch_daily_candles_windowed(instrument_key, lookback_days)
        end = now_ist()
        horizon_start = end - timedelta(days=max(3650, lookback_days))
        start = last_ts.astimezone(end.tzinfo) - timedelta(days=7)
        if start < horizon_start:
            start = horizon_start
        if start >= end:
            start = end - timedelta(days=7)
        return self._fetch_daily_candles_windowed_between(instrument_key, start, end)

    def _fetch_daily_candles_backfill_older(
        self,
        instrument_key: str,
        cached: list[list[object]],
        lookback_days: int,
    ) -> list[list[object]]:
        first_ts = self._first_candle_ts(cached)
        if first_ts is None:
            return self._fetch_daily_candles_windowed(instrument_key, lookback_days)
        end = now_ist()
        horizon_start = end - timedelta(days=max(3650, lookback_days))
        # Stop if cached history already covers requested horizon.
        if first_ts.astimezone(IST) <= (horizon_start.astimezone(IST) + timedelta(days=14)):
            return []
        older_end = first_ts.astimezone(end.tzinfo) + timedelta(days=1)
        if older_end <= horizon_start:
            return []
        return self._fetch_daily_candles_windowed_between(instrument_key, horizon_start, older_end)

    def _daily_score_candles(
        self,
        symbol: str,
        exchange: str,
        segment: str,
        instrument_key: str,
        lookback_days: int,
        min_bars: int,
        allow_api: bool,
        *,
        cache_only: bool = False,
        allow_provisional_intraday: bool = False,
        expected_lcd: str = "",
        refresh_provisional_current: bool = False,
    ) -> tuple[list[list[object]], str, int]:
        path = self.gcs.score_cache_1d_path(symbol, exchange, segment)
        cached = self.gcs.read_candles(path)
        has_required_lookback = self._daily_cache_has_lookback(cached, lookback_days) if cached else False
        if len(cached) >= min_bars:
            if self._daily_cache_is_current(cached):
                if has_required_lookback and not refresh_provisional_current:
                    return cached, "gcs_score_cache_1d", 0
                if cache_only:
                    if refresh_provisional_current:
                        return cached, "gcs_score_cache_1d_provisional", 0
                    return cached, "gcs_score_cache_1d_partial_history", 0
                if not allow_api:
                    if refresh_provisional_current:
                        return cached, "gcs_score_cache_1d_provisional_api_cap_blocked", 0
                    return cached, "gcs_score_cache_1d_partial_history_api_cap_blocked", 0
                if refresh_provisional_current:
                    api = self._fetch_daily_candles_incremental(instrument_key, cached, lookback_days)
                    if api:
                        merged = self.gcs.merge_candles(path, api)
                        return merged, "upstox_api_incremental", 1
                    if allow_provisional_intraday and expected_lcd:
                        exp_date = self._parse_iso_date(expected_lcd)
                        if exp_date is not None:
                            prov = self._build_provisional_daily_from_intraday(instrument_key, target_date=exp_date)
                            if prov:
                                merged = self.gcs.merge_candles(path, [prov])
                                return merged, "upstox_api_intraday_provisional_daily", 2
                    return cached, "gcs_score_cache_1d_provisional", 1
                api = self._fetch_daily_candles_backfill_older(instrument_key, cached, lookback_days)
                if api:
                    merged = self.gcs.merge_candles(path, api)
                    return merged, "upstox_api_backfill_older", 1
                return cached, "gcs_score_cache_1d_partial_history_fetch_empty", 1
            if cache_only:
                return cached, "gcs_score_cache_1d_stale", 0
            if not allow_api:
                return cached, "gcs_score_cache_1d_stale_api_cap_blocked", 0
            api = self._fetch_daily_candles_incremental(instrument_key, cached, lookback_days)
            if api:
                merged = self.gcs.merge_candles(path, api)
                return merged, "upstox_api_incremental", 1
            if allow_provisional_intraday and expected_lcd:
                exp_date = self._parse_iso_date(expected_lcd)
                if exp_date is not None:
                    prov = self._build_provisional_daily_from_intraday(instrument_key, target_date=exp_date)
                    if prov:
                        merged = self.gcs.merge_candles(path, [prov])
                        return merged, "upstox_api_intraday_provisional_daily", 2
            return cached, "gcs_score_cache_1d_stale_fetch_empty", 1
        if not allow_api:
            if cache_only and cached:
                if self._daily_cache_is_current(cached):
                    return cached, "gcs_score_cache_1d_insufficient_history_final", 0
                return cached, "gcs_score_cache_1d_insufficient_history_stale", 0
            return cached, "api_cap_blocked", 0
        if cache_only:
            return cached, "cache_only_missing", 0
        if not instrument_key:
            return cached, "missing_instrument_key", 0
        api = self._fetch_daily_candles_windowed(instrument_key, lookback_days)
        if api:
            merged = self.gcs.merge_candles(path, api)
            return merged, "upstox_api", 1
        return cached, "empty", 1

    def prefetch_score_cache_batch(
        self,
        *,
        lookback_days: int = 700,
        min_bars: int = 320,
        api_cap: int = 300,
        allow_provisional_intraday: bool = False,
        retry_stale_terminal_today: bool = False,
        priority_symbols: list[str] | None = None,
    ) -> dict[str, int | float | bool]:
        rows = self.sheets.read_universe_rows()
        if priority_symbols:
            pset = {str(s).strip().upper() for s in priority_symbols if str(s).strip()}
            if pset:
                prioritized = [u for u in rows if u.symbol in pset]
                others = [u for u in rows if u.symbol not in pset]
                rows = prioritized + others
        scanned = 0
        fetches = 0
        ready = 0
        fresh = 0
        terminal_insufficient = 0
        terminal_stale = 0
        provisional_ready = 0
        pending = 0
        updated = 0
        retried_no_change = 0
        expected = self._expected_latest_daily_candle_date().strftime("%Y-%m-%d")
        updated_at = now_ist_str()
        score_cache_index_rows: list[list[object]] = []
        prev_index = self._read_score_cache_index_snapshot()

        for u in rows:
            scanned += 1
            path = self.gcs.score_cache_1d_path(u.symbol, u.exchange, u.segment)
            cached = self.gcs.read_candles(path)
            before_bars = len(cached)
            before_last_text = self._last_candle_text(cached)
            before_last_sig = self._last_candle_sig(cached)
            before_ready = len(cached) >= min_bars
            before_fresh = before_ready and self._daily_cache_is_current(cached)
            prev_row = prev_index.get((u.symbol, u.exchange, u.segment))
            before_provisional = before_fresh and self._is_provisional_source((prev_row or {}).get("src", ""))
            if before_fresh and not before_provisional:
                ready += 1
                fresh += 1
                score_cache_index_rows.append(
                    self._score_cache_index_row(
                        u,
                        path=path,
                        candles=cached,
                        source="gcs_score_cache_1d",
                        api_calls=0,
                        min_bars=min_bars,
                        expected_lcd=expected,
                        updated_at=updated_at,
                    )
                )
                continue
            if before_ready:
                ready += 1
            if 0 < len(cached) < min_bars and self._daily_cache_is_current(cached):
                candles, source, api_calls = cached, "gcs_score_cache_1d_insufficient_history_final", 0
            elif (
                before_ready
                and not allow_provisional_intraday
                and not retry_stale_terminal_today
                and self._prefetch_should_skip_stale_retry(prev_row, cached, expected_lcd=expected)
            ):
                candles, source, api_calls = cached, "gcs_score_cache_1d_stale_terminal", 0
            else:
                candles, source, api_calls = self._daily_score_candles(
                    u.symbol,
                    u.exchange,
                    u.segment,
                    u.instrument_key,
                    lookback_days,
                    min_bars,
                    allow_api=(fetches < api_cap),
                    cache_only=False,
                    allow_provisional_intraday=allow_provisional_intraday,
                    expected_lcd=expected,
                    refresh_provisional_current=before_provisional,
                )
            fetches += api_calls
            after_ready = len(candles) >= min_bars
            after_fresh = after_ready and self._daily_cache_is_current(candles)
            after_last_sig = self._last_candle_sig(candles)
            if source == "upstox_api" and (0 < len(candles) < min_bars) and self._daily_cache_is_current(candles):
                # Terminal for the day: provider returned the max available history and it is still below threshold.
                source = "gcs_score_cache_1d_insufficient_history_final"
            if before_provisional and after_fresh and source in {"upstox_api_incremental", "gcs_score_cache_1d_provisional"}:
                # Preserve provisional marker when official daily 1D has not replaced the intraday-derived candle yet.
                if after_last_sig == before_last_sig:
                    source = "gcs_score_cache_1d_provisional"
            if api_calls > 0:
                after_last_text = self._last_candle_text(candles)
                if len(candles) != before_bars or after_last_text != before_last_text or after_last_sig != before_last_sig:
                    updated += 1
                else:
                    retried_no_change += 1
            if after_ready and after_fresh and not before_fresh:
                fresh += 1
            if after_ready and not before_ready:
                ready += 1
            if after_ready and after_fresh and self._is_provisional_source(source):
                provisional_ready += 1
            if source == "gcs_score_cache_1d_insufficient_history_final":
                terminal_insufficient += 1
            elif source == "gcs_score_cache_1d_stale_terminal":
                terminal_stale += 1
            if source in {"api_cap_blocked", "gcs_score_cache_1d_stale_api_cap_blocked"} and fetches >= api_cap:
                # Continue scanning to count readiness but don't force more API beyond cap.
                pass

            score_cache_index_rows.append(
                self._score_cache_index_row(
                    u,
                    path=path,
                    candles=candles,
                    source=source,
                    api_calls=api_calls,
                    min_bars=min_bars,
                    expected_lcd=expected,
                    updated_at=updated_at,
                )
            )

        # Refresh the visible 1D score-cache index sheet so manual backfill progress can be tracked easily.
        self.sheets.replace_score_cache_1d_index(score_cache_index_rows)

        total = len(rows)
        complete = min(total, fresh + terminal_insufficient + terminal_stale)
        pending = max(0, total - complete)
        out = {
            "scanned": scanned,
            "fetches": fetches,
            "updated": updated,
            "retriedNoChange": retried_no_change,
            "ready": ready,
            "freshReady": fresh,
            "provisionalReady": provisional_ready,
            "terminalInsufficientHistory": terminal_insufficient,
            "terminalStaleSkipped": terminal_stale,
            "prefillDone": complete,
            "prefillComplete": pending == 0,
            "prefillCoveragePct": round((complete * 100.0 / total), 2) if total else 0.0,
            "staleOrMissing": pending,
            "total": total,
            "freshCoveragePct": round((fresh * 100.0 / total), 2) if total else 0.0,
            "provisionalCoveragePct": round((provisional_ready * 100.0 / total), 2) if total else 0.0,
            "expectedLatestDailyCandleDate": expected,
        }
        logger.info(
            "prefetch_score_cache_batch complete scanned=%s fetches=%s updated=%s retriedNoChange=%s freshReady=%s terminalIH=%s terminalStale=%s prefillDone=%s/%s pending=%s expectedLCD=%s",
            scanned, fetches, updated, retried_no_change, fresh, terminal_insufficient, terminal_stale, complete, total, pending, expected,
        )
        return out

    @staticmethod
    def _auto_strategy(ind) -> str:
        if ind.macd.crossed in {"BUY", "SELL"}:
            return "EMA_CROSS"
        if 45 <= ind.rsi.curr <= 65:
            return "RSI_EMA"
        return "ALL"

    @staticmethod
    def _resolve_product(u: UniverseRow, score: int, regime: RegimeSnapshot) -> str:
        if u.allowed_product in {"MIS", "CNC"}:
            return u.allowed_product
        if regime.regime == "RANGE":
            return "MIS"
        if regime.regime == "AVOID":
            return "CNC"
        if u.beta >= 1.25 and score >= 60:
            return "MIS"
        return "CNC"

    def score_universe_batch(
        self,
        regime: RegimeSnapshot,
        *,
        lookback_days: int = 700,
        min_bars: int = 320,
        api_cap: int = 120,
        fresh_hours: int = 18,
        sheet_write_batch_size: int = 200,
        cache_only: bool = False,
        require_fresh_cache: bool = False,
    ) -> dict[str, Any]:
        candidates = self.sheets.read_universe_rows()
        cache_index = self._read_score_cache_index_snapshot()
        updates: list[tuple[int, list[object]]] = []
        scored = 0
        scanned = 0
        fetches = 0
        skip_counts = {"freshWindow": 0, "insufficientHistory": 0, "staleCache": 0, "indicatorNone": 0}
        scored_zero = 0
        now = now_ist()
        expected_lcd = self._expected_latest_daily_candle_date(now).strftime("%Y-%m-%d")

        for u in candidates:
            scanned += 1
            idx_meta = cache_index.get((u.symbol, u.exchange, u.segment)) or {}
            idx_src = str(idx_meta.get("src") or "")
            current_cache_provisional = self._is_provisional_source(idx_src)
            prev_score_kv = self._parse_pipe_kv(u.last_note)
            prev_score_provisional = str(prev_score_kv.get("prov", "")).strip().upper() in {"Y", "1", "TRUE"}
            if u.last_scanned and fresh_hours > 0:
                ts = parse_any_ts(u.last_scanned)
                if ts is not None:
                    age_hours = (now.astimezone(ts.tzinfo) - ts).total_seconds() / 3600.0
                    if age_hours <= fresh_hours and not (prev_score_provisional and not current_cache_provisional):
                        skip_counts["freshWindow"] += 1
                        continue

            candles, source, api_calls = self._daily_score_candles(
                u.symbol,
                u.exchange,
                u.segment,
                u.instrument_key,
                lookback_days,
                min_bars,
                allow_api=(fetches < api_cap),
                cache_only=cache_only,
            )
            fetches += api_calls
            if len(candles) < min_bars:
                last_candle_ts = self._last_candle_ts(candles)
                lcd = last_candle_ts.astimezone(now.tzinfo).strftime("%Y-%m-%d") if last_candle_ts is not None else "NA"
                # Mark true ineligible rows (some history but not enough bars) as processed so coverage can complete.
                if len(candles) > 0:
                    skip_counts["insufficientHistory"] += 1
                    note = f"Skip=INSUFFICIENT_HISTORY|Bars={len(candles)}|Need={min_bars}|LCD={lcd}|Src={source}"
                    updates.append(
                        (
                            u.row_number,
                            [
                                u.score if math.isfinite(u.score) else 0,
                                u.last_rsi if math.isfinite(u.last_rsi) else 0,
                                u.last_vol_ratio if math.isfinite(u.last_vol_ratio) else 0,
                                now_ist_str(),
                                u.last_product or "",
                                u.last_strategy or "",
                                note,
                                self._score_calc_skip_short("IH", f"{len(candles)}/{min_bars}"),
                            ],
                        )
                    )
                    if len(updates) >= max(1, int(sheet_write_batch_size)):
                        self.sheets.update_universe_score_columns(updates)
                        updates.clear()
                continue
            if require_fresh_cache and not self._daily_cache_is_current(candles):
                skip_counts["staleCache"] += 1
                last_candle_ts = self._last_candle_ts(candles)
                lcd = last_candle_ts.astimezone(now.tzinfo).strftime("%Y-%m-%d") if last_candle_ts is not None else "NA"
                note = f"Skip=STALE_CACHE|Bars={len(candles)}|ExpectedLCD={expected_lcd}|LCD={lcd}|Src={source}"
                updates.append(
                    (
                        u.row_number,
                        [
                            0,
                            0,
                            0,
                            now_ist_str(),
                            "",
                            "",
                            note,
                            self._score_calc_skip_short("STL", lcd),
                        ],
                    )
                )
                if len(updates) >= max(1, int(sheet_write_batch_size)):
                    self.sheets.update_universe_score_columns(updates)
                    updates.clear()
                continue
            ind = compute_indicators(candles, self.cfg)
            if ind is None:
                skip_counts["indicatorNone"] += 1
                last_candle_ts = self._last_candle_ts(candles)
                lcd = last_candle_ts.astimezone(now.tzinfo).strftime("%Y-%m-%d") if last_candle_ts is not None else "NA"
                note = f"Skip=INDICATOR_NONE|Bars={len(candles)}|LCD={lcd}|Src={source}"
                updates.append(
                    (
                        u.row_number,
                        [
                            u.score if math.isfinite(u.score) else 0,
                            u.last_rsi if math.isfinite(u.last_rsi) else 0,
                            u.last_vol_ratio if math.isfinite(u.last_vol_ratio) else 0,
                            now_ist_str(),
                            u.last_product or "",
                            u.last_strategy or "",
                            note,
                            self._score_calc_skip_short("IND0", lcd),
                        ],
                    )
                )
                if len(updates) >= max(1, int(sheet_write_batch_size)):
                    self.sheets.update_universe_score_columns(updates)
                    updates.clear()
                continue
            raw_score, score_parts = compute_universe_score_breakdown(ind)
            priority_bonus = min(5.0, float(u.priority or 0.0))
            base_score = min(100, int(round(raw_score + priority_bonus)))
            product = self._resolve_product(u, base_score, regime)
            strategy = u.strategy_pref if u.strategy_pref != "AUTO" else self._auto_strategy(ind)
            last_candle_ts = self._last_candle_ts(candles)
            lcd = last_candle_ts.astimezone(now.tzinfo).strftime("%Y-%m-%d") if last_candle_ts is not None else "NA"
            note = (
                f"Score={base_score}|Reg={regime.regime}|Bias={regime.bias}|RSI={ind.rsi.curr:.1f}|"
                f"VR={ind.volume.ratio:.2f}|LCD={lcd}|Src={source}|Prov={'Y' if current_cache_provisional else 'N'}"
            )
            calc_short = format_universe_score_calc_short(base_score, score_parts, priority_bonus=priority_bonus)
            updates.append((u.row_number, [base_score, round(ind.rsi.curr, 2), round(ind.volume.ratio, 3), now_ist_str(), product, strategy, note, calc_short]))
            scored += 1
            if base_score == 0:
                scored_zero += 1

            if len(updates) >= max(1, int(sheet_write_batch_size)):
                self.sheets.update_universe_score_columns(updates)
                updates.clear()

            if fetches >= api_cap and scored > 0:
                # Continue only with cached rows; if more api needed they will be deferred by scheduler.
                pass

        if updates:
            self.sheets.update_universe_score_columns(updates)

        cov = self.universe_score_coverage()
        out = {
            "scanned": scanned,
            "scored": scored,
            "scoredZero": scored_zero,
            "fetches": fetches,
            "coveragePct": cov["coveragePct"],
            "total": cov["total"],
            "todayCoveragePct": cov["todayCoveragePct"],
            "cacheOnly": cache_only,
            "skipCounts": skip_counts,
        }
        logger.info(
            "score_universe_batch complete scanned=%s scored=%s scoredZero=%s fetches=%s cacheOnly=%s requireFresh=%s skips=%s coverage=%.2f todayCoverage=%.2f",
            scanned,
            scored,
            scored_zero,
            fetches,
            cache_only,
            require_fresh_cache,
            skip_counts,
            float(cov["coveragePct"] or 0.0),
            float(cov["todayCoveragePct"] or 0.0),
        )
        return out

    def universe_score_coverage(self) -> dict[str, float | int | bool]:
        rows = self.sheets.read_universe_rows()
        total = len(rows)
        scored_any = 0
        scored_today = 0
        today = today_ist()
        for r in rows:
            if parse_any_ts(r.last_scanned):
                scored_any += 1
                ts = parse_any_ts(r.last_scanned)
                if ts and ts.astimezone(now_ist().tzinfo).strftime("%Y-%m-%d") == today:
                    scored_today += 1
        return {
            "total": total,
            "scored": scored_any,
            "missing": max(0, total - scored_any),
            "coveragePct": round((scored_any * 100.0 / total), 2) if total else 0.0,
            "todayScored": scored_today,
            "todayCoveragePct": round((scored_today * 100.0 / total), 2) if total else 0.0,
            "full": total > 0 and scored_any >= total,
            "todayFull": total > 0 and scored_today >= total,
        }

    @staticmethod
    def _select_diversified(scored: list[dict[str, object]], limit: int, mis_target: int, cnc_target: int) -> list[dict[str, object]]:
        picked: list[dict[str, object]] = []
        seen: set[str] = set()
        product_count = {"MIS": 0, "CNC": 0}
        ordered = sorted(scored, key=lambda s: (-float(s["score"]), -float(s.get("beta", 0.0)), str(s["symbol"])))

        def can_pick(item: dict[str, object], relaxed: bool) -> bool:
            sym = str(item["symbol"])
            if sym in seen:
                return False
            p = "MIS" if str(item.get("product", "CNC")) == "MIS" else "CNC"
            if not relaxed:
                if p == "MIS" and product_count["MIS"] >= mis_target:
                    return False
                if p == "CNC" and product_count["CNC"] >= cnc_target:
                    return False
            return True

        for pass_no in range(2):
            relaxed = pass_no > 0
            for item in ordered:
                if len(picked) >= limit:
                    break
                if not can_pick(item, relaxed):
                    continue
                sym = str(item["symbol"])
                seen.add(sym)
                picked.append(item)
                product_count["MIS" if str(item.get("product", "CNC")) == "MIS" else "CNC"] += 1
        for item in ordered:
            if len(picked) >= limit:
                break
            sym = str(item["symbol"])
            if sym in seen:
                continue
            seen.add(sym)
            picked.append(item)
        return picked[:limit]

    def build_watchlist(
        self,
        regime: RegimeSnapshot,
        target_size: int = 200,
        *,
        min_score: int = 1,
        require_today_scored: bool = False,
        require_full_coverage: bool = False,
    ) -> dict[str, object]:
        rows = self.sheets.read_universe_rows()
        cov = self.universe_score_coverage()
        coverage_key = "todayFull" if require_today_scored else "full"
        if require_full_coverage and not bool(cov.get(coverage_key)):
            reason = "today_score_coverage_incomplete" if require_today_scored else "score_coverage_incomplete"
            logger.info(
                "build_watchlist blocked reason=%s coverageKey=%s total=%s scored=%s todayScored=%s",
                reason, coverage_key, cov.get("total"), cov.get("scored"), cov.get("todayScored"),
            )
            return {"selected": 0, "coverage": cov, "ready": False, "reason": reason}
        pool = []
        today = today_ist()
        for u in rows:
            if str(u.last_note or "").startswith("Skip="):
                continue
            if not (math.isfinite(u.score) and u.score > 0):
                continue
            if require_today_scored:
                ts = parse_any_ts(u.last_scanned)
                if ts is None or ts.astimezone(IST).strftime("%Y-%m-%d") != today:
                    continue
            score = max(0, min(100, int(round(u.score))))
            if score < max(1, int(min_score)):
                continue
            pool.append(
                {
                    "symbol": u.symbol,
                    "exchange": u.exchange,
                    "segment": u.segment,
                    "product": self._resolve_product(u, score, regime),
                    "strategy": u.last_strategy or (u.strategy_pref if u.strategy_pref else "AUTO"),
                    "sector": u.sector or "UNKNOWN",
                    "beta": u.beta,
                    "score": score,
                    "note": u.last_note or f"Score={score}|Reg={regime.regime}|Bias={regime.bias}",
                }
            )
        n = max(8, int(target_size))
        selected = self._select_diversified(pool, n, mis_target=n // 2, cnc_target=n - (n // 2))
        sheet_rows = []
        for i, s in enumerate(selected, start=1):
            sheet_rows.append([i, s["symbol"], s["exchange"], s["segment"], s["product"], s["strategy"], s["sector"], s["beta"], "Y", s["note"]])
        self.sheets.replace_watchlist(sheet_rows)
        cov = self.universe_score_coverage()
        out = {"selected": len(selected), "coverage": cov, "ready": True, "eligiblePool": len(pool)}
        logger.info(
            "build_watchlist complete selected=%s eligiblePool=%s target=%s minScore=%s requireToday=%s requireFull=%s",
            len(selected), len(pool), n, int(min_score), require_today_scored, require_full_coverage,
        )
        return out

    def run_premarket_pipeline(self, regime: RegimeSnapshot, target_size: int = 300) -> UniversePipelineResult:
        score_out = self.score_universe_batch(regime, api_cap=120, fresh_hours=12, sheet_write_batch_size=200)
        wl_out = self.build_watchlist(regime, target_size=target_size)
        cov = wl_out["coverage"]
        return UniversePipelineResult(
            synced=0,
            scored=int(score_out["scored"]),
            selected=int(wl_out["selected"]),
            coverage_pct=float(cov["todayCoveragePct"]),
        )
