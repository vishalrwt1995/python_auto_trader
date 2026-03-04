from __future__ import annotations

import hashlib
import json
import logging
import math
import statistics
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date as date_cls, datetime, timedelta
from typing import Any

from autotrader.adapters.gcs_store import GoogleCloudStorageStore
from autotrader.adapters.sheets_repository import GoogleSheetsRepository, SheetNames
from autotrader.adapters.upstox_client import UpstoxApiError, UpstoxClient
from autotrader.domain.indicators import calc_atr, compute_indicators
from autotrader.domain.models import RegimeSnapshot, UniverseRow
from autotrader.domain.scoring import compute_universe_score_breakdown, format_universe_score_calc_short
from autotrader.services.universe_v2 import (
    UNIVERSE_V2_HEADERS,
    CanonicalListing,
    ModeThresholds,
    TradabilityStats,
    UniverseControls,
    assign_turnover_rank_and_bucket,
    canonical_id_from_fields,
    choose_primary_listing,
    classify_eligibility,
    compute_tradability_stats,
)
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
    UNIVERSE_V2_CONFIG_DEFAULTS: dict[str, str] = {
        "UNIVERSE_MODE": "BALANCED",
        "UNIVERSE_MIN_BARS_HARD": "90",
        "UNIVERSE_MIN_PRICE_HARD": "20",
        "UNIVERSE_MAX_GAP_RISK_HARD": "0.10",
        "UNIVERSE_MAX_ATR_PCT_HARD": "0.20",
        "UNIVERSE_STALE_DAYS_MAX": "5",
        "UNIVERSE_CONSERVATIVE_SWING_TOPN_TURNOVER_60D": "500",
        "UNIVERSE_CONSERVATIVE_INTRADAY_TOPN_TURNOVER_60D": "250",
        "UNIVERSE_CONSERVATIVE_MIN_BARS_SWING": "252",
        "UNIVERSE_CONSERVATIVE_MIN_BARS_INTRADAY": "320",
        "UNIVERSE_CONSERVATIVE_MIN_PRICE_MODE": "50",
        "UNIVERSE_CONSERVATIVE_MAX_ATR_PCT_SWING": "0.08",
        "UNIVERSE_CONSERVATIVE_MAX_ATR_PCT_INTRADAY": "0.06",
        "UNIVERSE_CONSERVATIVE_MAX_GAP_RISK_MODE": "0.04",
        "UNIVERSE_BALANCED_SWING_TOPN_TURNOVER_60D": "1000",
        "UNIVERSE_BALANCED_INTRADAY_TOPN_TURNOVER_60D": "500",
        "UNIVERSE_BALANCED_MIN_BARS_SWING": "180",
        "UNIVERSE_BALANCED_MIN_BARS_INTRADAY": "252",
        "UNIVERSE_BALANCED_MIN_PRICE_MODE": "30",
        "UNIVERSE_BALANCED_MAX_ATR_PCT_SWING": "0.12",
        "UNIVERSE_BALANCED_MAX_ATR_PCT_INTRADAY": "0.09",
        "UNIVERSE_BALANCED_MAX_GAP_RISK_MODE": "0.06",
        "UNIVERSE_AGGRESSIVE_SWING_TOPN_TURNOVER_60D": "1500",
        "UNIVERSE_AGGRESSIVE_INTRADAY_TOPN_TURNOVER_60D": "800",
        "UNIVERSE_AGGRESSIVE_MIN_BARS_SWING": "120",
        "UNIVERSE_AGGRESSIVE_MIN_BARS_INTRADAY": "180",
        "UNIVERSE_AGGRESSIVE_MIN_PRICE_MODE": "20",
        "UNIVERSE_AGGRESSIVE_MAX_ATR_PCT_SWING": "0.16",
        "UNIVERSE_AGGRESSIVE_MAX_ATR_PCT_INTRADAY": "0.12",
        "UNIVERSE_AGGRESSIVE_MAX_GAP_RISK_MODE": "0.08",
    }

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
        # Trading-calendar caches for holiday-aware ExpectedLCD.
        self._holiday_dates_by_year: dict[int, set[date_cls]] = {}
        self._holiday_year_loaded_ok: set[int] = set()
        self._holiday_date_probe_cache: dict[str, bool] = {}
        self._holiday_api_fallback_day: str | None = None
        self._expected_lcd_ctx_by_day: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _cfg_int(cfg: dict[str, str], key: str, default: int) -> int:
        try:
            return int(str(cfg.get(key, default)).strip())
        except Exception:
            return default

    @staticmethod
    def _cfg_float(cfg: dict[str, str], key: str, default: float) -> float:
        try:
            return float(str(cfg.get(key, default)).strip())
        except Exception:
            return default

    @staticmethod
    def _history_horizon_start_ist() -> datetime:
        return datetime(2000, 1, 1, tzinfo=IST)

    @staticmethod
    def _extract_isin_from_notes(notes: str) -> str:
        kv = UniverseService._parse_pipe_kv(notes)
        return str(kv.get("isin", "")).strip().upper()

    def _is_weekend(self, d: date_cls) -> bool:
        return d.weekday() >= 5

    @staticmethod
    def _holiday_calendar_cache_path(year: int) -> str:
        return f"cache/calendars/nse_holidays/{int(year)}.json"

    def _load_holiday_calendar_from_store(self, year: int) -> set[date_cls]:
        try:
            payload = self.gcs.read_json(self._holiday_calendar_cache_path(year), default={})
        except Exception:
            return set()
        if isinstance(payload, dict):
            items = payload.get("dates") or payload.get("holidays") or []
        elif isinstance(payload, list):
            items = payload
        else:
            items = []
        out: set[date_cls] = set()
        if not isinstance(items, list):
            return out
        for item in items:
            d = self._parse_iso_date(str(item))
            if d is not None and d.year == year:
                out.add(d)
        return out

    def _save_holiday_calendar_to_store(self, year: int, dates: set[date_cls], *, source: str) -> None:
        data = {
            "year": int(year),
            "dates": sorted(d.isoformat() for d in dates),
            "source": str(source),
            "updatedAt": now_ist().isoformat(),
        }
        try:
            self.gcs.write_json(self._holiday_calendar_cache_path(year), data)
        except Exception:
            logger.debug("holiday calendar cache write failed year=%s", year, exc_info=True)

    def _extract_holiday_date(self, row: dict[str, Any]) -> date_cls | None:
        # Upstox payload fields observed across variants.
        for key in ("date", "holiday_date", "holidayDate"):
            d = self._parse_iso_date(str(row.get(key) or ""))
            if d is not None:
                return d
        return None

    def _exchange_token_set(self, v: Any) -> set[str]:
        out: set[str] = set()
        if isinstance(v, list):
            items = v
        elif isinstance(v, dict):
            items = [v]
        elif isinstance(v, str):
            items = [v]
        else:
            items = []
        for item in items:
            if isinstance(item, dict):
                s = str(item.get("exchange") or item.get("segment") or item.get("name") or "").strip().upper()
            else:
                s = str(item).strip().upper()
            if s:
                out.add(s.replace(":", "|"))
        return out

    def _row_closes_nse(self, row: dict[str, Any]) -> bool:
        # Prefer explicit closed/open exchange fields when available.
        closed = self._exchange_token_set(row.get("closed_exchanges"))
        opened = self._exchange_token_set(row.get("open_exchanges"))
        holiday_type = str(row.get("holiday_type") or row.get("holidayType") or "").strip().upper()

        has_nse_closed = any("NSE" in token for token in closed)
        has_nse_open = any("NSE" in token for token in opened)
        if has_nse_closed and not has_nse_open:
            return True
        if has_nse_open:
            return False
        # Fallback heuristic: trading holiday rows without explicit open NSE are considered NSE-closed.
        return holiday_type == "TRADING_HOLIDAY"

    def _load_holiday_calendar_for_year(self, year: int, *, run_day: date_cls) -> tuple[set[date_cls], bool]:
        if year in self._holiday_year_loaded_ok:
            return self._holiday_dates_by_year.get(year, set()), True
        stored_dates = self._load_holiday_calendar_from_store(year)
        if stored_dates:
            self._holiday_dates_by_year[year] = stored_dates
            self._holiday_year_loaded_ok.add(year)
            return stored_dates, True
        if self._holiday_api_fallback_day == run_day.isoformat():
            return set(), False
        try:
            rows = self.upstox.get_market_holidays()
            dates = {
                d
                for r in rows
                if isinstance(r, dict)
                for d in [self._extract_holiday_date(r)]
                if d is not None and d.year == year and self._row_closes_nse(r)
            }
            if dates:
                self._save_holiday_calendar_to_store(year, dates, source="upstox_api")
            self._holiday_dates_by_year[year] = dates
            self._holiday_year_loaded_ok.add(year)
            return dates, True
        except Exception as exc:
            self._holiday_api_fallback_day = run_day.isoformat()
            logger.warning(
                "expected_lcd holiday calendar fetch failed year=%s runDay=%s fallback=weekend_only error=%s",
                year,
                run_day.isoformat(),
                type(exc).__name__,
            )
            return set(), False

    def _is_exchange_holiday(self, d: date_cls, *, run_year: int, run_day: date_cls) -> bool:
        if self._holiday_api_fallback_day == run_day.isoformat():
            return False
        if d.year == run_year:
            dates, ok = self._load_holiday_calendar_for_year(run_year, run_day=run_day)
            if not ok:
                return False
            return d in dates
        # Year-boundary: prefer stored calendar for that year; otherwise probe specific date.
        stored = self._load_holiday_calendar_from_store(d.year)
        if stored:
            return d in stored
        key = d.isoformat()
        if key in self._holiday_date_probe_cache:
            return self._holiday_date_probe_cache[key]
        try:
            rows = self.upstox.get_market_holidays(date=key)
            is_holiday = any(
                isinstance(r, dict)
                and self._extract_holiday_date(r) == d
                and self._row_closes_nse(r)
                for r in rows
            )
            if is_holiday:
                merged = set(stored)
                merged.add(d)
                self._save_holiday_calendar_to_store(d.year, merged, source="upstox_api_probe")
            self._holiday_date_probe_cache[key] = bool(is_holiday)
            return bool(is_holiday)
        except Exception as exc:
            self._holiday_api_fallback_day = run_day.isoformat()
            logger.warning(
                "expected_lcd holiday date probe failed date=%s runDay=%s fallback=weekend_only error=%s",
                key,
                run_day.isoformat(),
                type(exc).__name__,
            )
            self._holiday_date_probe_cache[key] = False
            return False

    def _is_trading_day(self, d: date_cls, *, run_year: int, run_day: date_cls) -> bool:
        if self._is_weekend(d):
            return False
        if self._is_exchange_holiday(d, run_year=run_year, run_day=run_day):
            return False
        return True

    def _expected_lcd_context(self, now: datetime | None = None) -> dict[str, Any]:
        now_i = (now or now_ist()).astimezone(IST)
        today = now_i.date()
        cache_key = today.isoformat()
        cached = self._expected_lcd_ctx_by_day.get(cache_key)
        if cached is not None:
            return cached

        run_year = today.year
        # Try loading current-year calendar once; failures auto-switch fallback mode.
        self._load_holiday_calendar_for_year(run_year, run_day=today)

        cursor = today - timedelta(days=1)
        expected = None
        for _ in range(370):
            if self._is_trading_day(cursor, run_year=run_year, run_day=today):
                expected = cursor
                break
            cursor -= timedelta(days=1)
        if expected is None:
            # Safety fallback should never happen.
            expected = self._prev_weekday(today - timedelta(days=1))
        today_trading = self._is_trading_day(today, run_year=run_year, run_day=today)
        method = "fallback-weekend" if self._holiday_api_fallback_day == today.isoformat() else "holiday-aware"
        ctx = {
            "today": today.isoformat(),
            "todayTradingDay": bool(today_trading),
            "marketClosedToday": not bool(today_trading),
            "expectedLCD": expected.isoformat(),
            "method": method,
        }
        self._expected_lcd_ctx_by_day[cache_key] = ctx
        logger.info(
            "expected_lcd resolved expected=%s today=%s todayTradingDay=%s marketClosedToday=%s method=%s",
            ctx["expectedLCD"],
            ctx["today"],
            ctx["todayTradingDay"],
            ctx["marketClosedToday"],
            ctx["method"],
        )
        return ctx

    def _expected_last_completed_daily_date(self, now: datetime | None = None) -> date_cls:
        return self._parse_iso_date(self._expected_lcd_context(now).get("expectedLCD", "")) or self._prev_weekday((now or now_ist()).astimezone(IST).date() - timedelta(days=1))

    def _build_universe_v2_controls(self) -> UniverseControls:
        self.sheets.ensure_config_defaults(self.UNIVERSE_V2_CONFIG_DEFAULTS)
        cfg_map = self.sheets.read_config_label_map()
        merged = dict(self.UNIVERSE_V2_CONFIG_DEFAULTS)
        merged.update({k: str(v) for k, v in cfg_map.items()})
        mode = str(merged.get("UNIVERSE_MODE", "BALANCED")).strip().upper()
        if mode not in {"CONSERVATIVE", "BALANCED", "AGGRESSIVE"}:
            mode = "BALANCED"

        def _mode(name: str, defaults: ModeThresholds) -> ModeThresholds:
            p = f"UNIVERSE_{name}_"
            return ModeThresholds(
                swing_topn_turnover_60d=self._cfg_int(merged, f"{p}SWING_TOPN_TURNOVER_60D", defaults.swing_topn_turnover_60d),
                intraday_topn_turnover_60d=self._cfg_int(merged, f"{p}INTRADAY_TOPN_TURNOVER_60D", defaults.intraday_topn_turnover_60d),
                min_bars_swing=self._cfg_int(merged, f"{p}MIN_BARS_SWING", defaults.min_bars_swing),
                min_bars_intraday=self._cfg_int(merged, f"{p}MIN_BARS_INTRADAY", defaults.min_bars_intraday),
                min_price_mode=self._cfg_float(merged, f"{p}MIN_PRICE_MODE", defaults.min_price_mode),
                max_atr_pct_swing=self._cfg_float(merged, f"{p}MAX_ATR_PCT_SWING", defaults.max_atr_pct_swing),
                max_atr_pct_intraday=self._cfg_float(merged, f"{p}MAX_ATR_PCT_INTRADAY", defaults.max_atr_pct_intraday),
                max_gap_risk_mode=self._cfg_float(merged, f"{p}MAX_GAP_RISK_MODE", defaults.max_gap_risk_mode),
            )

        return UniverseControls(
            mode=mode,
            min_bars_hard=self._cfg_int(merged, "UNIVERSE_MIN_BARS_HARD", 90),
            min_price_hard=self._cfg_float(merged, "UNIVERSE_MIN_PRICE_HARD", 20.0),
            max_gap_risk_hard=self._cfg_float(merged, "UNIVERSE_MAX_GAP_RISK_HARD", 0.10),
            max_atr_pct_hard=self._cfg_float(merged, "UNIVERSE_MAX_ATR_PCT_HARD", 0.20),
            stale_days_max=self._cfg_int(merged, "UNIVERSE_STALE_DAYS_MAX", 5),
            mode_thresholds={
                "CONSERVATIVE": _mode(
                    "CONSERVATIVE",
                    ModeThresholds(
                        swing_topn_turnover_60d=500,
                        intraday_topn_turnover_60d=250,
                        min_bars_swing=252,
                        min_bars_intraday=320,
                        min_price_mode=50.0,
                        max_atr_pct_swing=0.08,
                        max_atr_pct_intraday=0.06,
                        max_gap_risk_mode=0.04,
                    ),
                ),
                "BALANCED": _mode(
                    "BALANCED",
                    ModeThresholds(
                        swing_topn_turnover_60d=1000,
                        intraday_topn_turnover_60d=500,
                        min_bars_swing=180,
                        min_bars_intraday=252,
                        min_price_mode=30.0,
                        max_atr_pct_swing=0.12,
                        max_atr_pct_intraday=0.09,
                        max_gap_risk_mode=0.06,
                    ),
                ),
                "AGGRESSIVE": _mode(
                    "AGGRESSIVE",
                    ModeThresholds(
                        swing_topn_turnover_60d=1500,
                        intraday_topn_turnover_60d=800,
                        min_bars_swing=120,
                        min_bars_intraday=180,
                        min_price_mode=20.0,
                        max_atr_pct_swing=0.16,
                        max_atr_pct_intraday=0.12,
                        max_gap_risk_mode=0.08,
                    ),
                ),
            },
        )

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

    @staticmethod
    def _is_invalid_instrument_key_error(exc: Exception) -> bool:
        msg = str(exc or "").lower()
        return "udapi100011" in msg or "invalid instrument key" in msg

    @staticmethod
    def _error_text_short(exc: Exception, max_len: int = 220) -> str:
        msg = str(exc or "").replace("\n", " ").strip()
        if len(msg) <= max_len:
            return msg
        return msg[: max(0, max_len - 3)].rstrip() + "..."

    def _score_cache_paths(self, symbol: str, exchange: str, segment: str, instrument_key: str) -> tuple[str, str]:
        legacy_path = self.gcs.score_cache_1d_path(symbol, exchange, segment)
        if not instrument_key:
            return legacy_path, legacy_path
        ik_path = self.gcs.score_cache_1d_path_by_instrument_key(instrument_key, exchange, segment)
        return ik_path, legacy_path

    def _read_score_cache_with_migration(
        self,
        symbol: str,
        exchange: str,
        segment: str,
        instrument_key: str,
    ) -> tuple[str, list[list[object]]]:
        path, legacy_path = self._score_cache_paths(symbol, exchange, segment, instrument_key)
        candles = self.gcs.read_candles(path)
        if not candles and path != legacy_path:
            legacy = self.gcs.read_candles(legacy_path)
            if legacy:
                self.gcs.write_candles(path, legacy)
                candles = legacy
        return path, candles

    def _probe_instrument_key_liveness(self, instrument_key: str) -> tuple[int, str]:
        """Return (score, note) for selecting symbol-conflict winners.

        score:
        - 2 => key appears live (request succeeded)
        - 1 => unknown/transient error (keep candidate viable)
        - 0 => invalid key
        """
        if not instrument_key:
            return 0, "missing_key"
        try:
            to_date = now_ist().strftime("%Y-%m-%d")
            from_date = (now_ist() - timedelta(days=10)).strftime("%Y-%m-%d")
            # Lightweight validity probe only for symbol conflicts.
            self.upstox.get_historical_candles_v3_days(instrument_key, to_date=to_date, from_date=from_date, interval_days=1)
            return 2, "live_or_accessible"
        except UpstoxApiError as exc:
            if self._is_invalid_instrument_key_error(exc):
                return 0, "invalid_key"
            return 1, "transient_error"
        except Exception:
            return 1, "transient_error"

    def _dedupe_master_by_symbol_exchange(
        self,
        masters: list[CanonicalListing],
        *,
        preferred_by_symbol_exchange: dict[tuple[str, str], str],
    ) -> tuple[list[CanonicalListing], int]:
        grouped: dict[tuple[str, str], list[CanonicalListing]] = {}
        for m in masters:
            key = (str(m.symbol).upper(), str(m.primary_exchange).upper())
            grouped.setdefault(key, []).append(m)

        deduped: list[CanonicalListing] = []
        conflicts = 0
        probe_cache: dict[str, tuple[int, str]] = {}
        for key, arr in grouped.items():
            if len(arr) == 1:
                deduped.append(arr[0])
                continue
            conflicts += 1
            preferred_key = preferred_by_symbol_exchange.get(key, "").strip().upper()
            best: CanonicalListing | None = None
            best_score = -10_000
            for c in arr:
                ik = str(c.primary_instrument_key or "").strip().upper()
                if ik not in probe_cache:
                    probe_cache[ik] = self._probe_instrument_key_liveness(ik)
                live_score, _ = probe_cache[ik]
                score = 0
                if preferred_key and ik == preferred_key:
                    score += 1000
                score += live_score * 100
                # Prefer real ISIN canonicals over synthetic markers.
                if "::DUPROW::" not in str(c.canonical_id):
                    score += 20
                if str(c.isin or "").strip():
                    score += 10
                # Deterministic tie-break.
                score += len(str(c.primary_instrument_key or ""))
                if score > best_score:
                    best_score = score
                    best = c
            if best is not None:
                deduped.append(best)
        deduped.sort(key=lambda m: (str(m.symbol), str(m.primary_exchange)))
        return deduped, conflicts

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

    def _read_score_cache_5m_index_snapshot(self) -> dict[tuple[str, str, str], dict[str, str]]:
        out: dict[tuple[str, str, str], dict[str, str]] = {}
        try:
            rows = self.sheets.read_sheet_rows(SheetNames.SCORE_CACHE_5M, 4)
        except Exception:
            logger.debug("Unable to read score-cache 5m index snapshot", exc_info=True)
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
            # Do not freeze stale symbols for the entire day. Allow retry on subsequent runs.
            # This lets same-day provider catch-up recover freshness without waiting for next expectedLCD.
            return prev_src in {"upstox_api_error", "prefetch_unexpected_error"}
        # Terminalize on second pass for known no-progress sources so batch jobs do not loop forever:
        # - stale fetch returned empty
        # - api cap prevented fetch in prior pass
        # - prior symbol-level API failure left cache stale for this expected date
        return prev_src in {
            "upstox_api_incremental",
            "gcs_score_cache_1d_stale_fetch_empty",
            "gcs_score_cache_1d_stale_api_cap_blocked",
            "upstox_api_error",
            "prefetch_unexpected_error",
        }

    def _prefetch_should_skip_invalid_key_retry(
        self,
        prev_row: dict[str, str] | None,
        candles: list[list[object]],
    ) -> bool:
        if not prev_row:
            return False
        if (prev_row.get("status") or "").upper() != "INVALID_KEY_SKIPPED":
            return False
        current_last = self._last_candle_text(candles)
        prev_last = (prev_row.get("last_candle_time") or "").strip()
        if not current_last and not prev_last:
            return True
        return bool(current_last) and current_last == prev_last

    def _prefetch_should_skip_missing_retry(
        self,
        prev_row: dict[str, str] | None,
        candles: list[list[object]],
        *,
        expected_lcd: str,
    ) -> bool:
        if not prev_row:
            return False
        prev_status = (prev_row.get("status") or "").upper()
        if prev_status not in {"MISSING", "MISSING_SKIPPED"}:
            return False
        prev_expected = (prev_row.get("expectedlcd") or "").strip()
        if prev_expected and prev_expected != expected_lcd:
            return False
        current_last = self._last_candle_text(candles)
        prev_last = (prev_row.get("last_candle_time") or "").strip()
        if current_last != prev_last:
            return False
        prev_src = (prev_row.get("src") or "").strip().lower()
        if prev_status == "MISSING_SKIPPED":
            return True
        return prev_src in {
            "empty",
            "cache_only_missing",
            "missing_instrument_key",
            "api_cap_blocked",
            "upstox_api_error",
            "prefetch_unexpected_error",
        }

    def _prefetch_intraday_should_skip_stale_retry(
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
            # Same behavior as 1D: avoid per-day freeze so symbols can recover when source catches up.
            return prev_src in {"upstox_api_error", "intraday_5m_unexpected_error"}
        return prev_src in {
            "upstox_api_5m_range",
            "upstox_api_5m_empty",
            "api_cap_blocked",
            "upstox_api_error",
            "intraday_5m_unexpected_error",
            "gcs_intraday_5m_stale_terminal",
        }

    def _prefetch_intraday_should_skip_missing_retry(
        self,
        prev_row: dict[str, str] | None,
        candles: list[list[object]],
        *,
        expected_lcd: str,
    ) -> bool:
        if not prev_row:
            return False
        prev_status = (prev_row.get("status") or "").upper()
        if prev_status not in {"MISSING", "MISSING_SKIPPED"}:
            return False
        prev_expected = (prev_row.get("expectedlcd") or "").strip()
        if prev_expected and prev_expected != expected_lcd:
            return False
        current_last = self._last_candle_text(candles)
        prev_last = (prev_row.get("last_candle_time") or "").strip()
        if current_last != prev_last:
            return False
        prev_src = (prev_row.get("src") or "").strip().lower()
        if prev_status == "MISSING_SKIPPED":
            return True
        return prev_src in {
            "upstox_api_5m_empty",
            "api_cap_blocked",
            "upstox_api_error",
            "intraday_5m_unexpected_error",
            "gcs_intraday_5m_missing_terminal",
        }

    def _prefetch_intraday_should_skip_insufficient_retry(
        self,
        prev_row: dict[str, str] | None,
        candles: list[list[object]],
        *,
        expected_lcd: str,
    ) -> bool:
        if not prev_row:
            return False
        prev_status = (prev_row.get("status") or "").upper()
        if prev_status != "INSUFFICIENT_HISTORY_FINAL":
            return False
        prev_expected = (prev_row.get("expectedlcd") or "").strip()
        if prev_expected and prev_expected != expected_lcd:
            return False
        current_last = self._last_candle_text(candles)
        prev_last = (prev_row.get("last_candle_time") or "").strip()
        return current_last == prev_last

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
        last_error: str = "",
    ) -> list[object]:
        first_ts = self._first_candle_ts(candles)
        last_ts = self._last_candle_ts(candles)
        first_candle_date = first_ts.astimezone(IST).date().isoformat() if first_ts is not None else ""
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
        elif source == "invalid_instrument_key_terminal":
            status = "INVALID_KEY_SKIPPED"
            terminal = "INVALID_INSTRUMENT_KEY"
        elif source == "gcs_score_cache_1d_missing_terminal":
            status = "MISSING_SKIPPED"
            terminal = "MISSING_CACHE"
        elif bars == 0:
            status = "MISSING"
        elif bars < min_bars:
            status = "INSUFFICIENT_HISTORY"
        elif is_current:
            status = "FRESH_READY"
        else:
            status = "STALE_READY"

        derived_last_error = ""
        if source in {
            "api_cap_blocked",
            "gcs_score_cache_1d_stale_api_cap_blocked",
            "gcs_score_cache_1d_stale_fetch_empty",
            "cache_only_missing",
            "missing_instrument_key",
            "empty",
            "invalid_instrument_key_terminal",
            "gcs_score_cache_1d_missing_terminal",
            "upstox_api_error",
            "prefetch_unexpected_error",
        }:
            derived_last_error = source
        if last_error:
            derived_last_error = last_error

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
            derived_last_error,
            file_name,
            isin,
            notes,
            path,
            first_candle_date,
        ]

    def refresh_raw_universe_from_upstox(self) -> dict[str, object]:
        blob = self.upstox.fetch_instruments_complete_gz()
        rows = self.upstox.decode_instruments_gz_json(blob)
        if not rows:
            raise RuntimeError("Upstox raw universe decode produced 0 rows; latest pointer unchanged")
        run_date = today_ist()
        run_stamp = now_ist().strftime("%Y%m%dT%H%M%S")
        ver_path = self.gcs.upstox_raw_universe_versioned_path(run_date, run_stamp=run_stamp)
        latest_path = self.gcs.upstox_raw_universe_latest_path()
        meta_path = self.gcs.upstox_raw_universe_latest_meta_path()
        signature = hashlib.sha256(blob).hexdigest()
        self.gcs.write_bytes(ver_path, blob, content_type="application/gzip")
        self.gcs.write_bytes(latest_path, blob, content_type="application/gzip")
        meta = {
            "provider": "UPSTOX",
            "runDate": run_date,
            "fetchedAt": now_ist_str(),
            "runStamp": run_stamp,
            "path": ver_path,
            "latestPath": latest_path,
            "itemCount": len(rows),
            "rowCount": len(rows),
            "signature": signature,
            "sourceUrl": self.upstox.settings.instruments_complete_url,
            "snapshotVersion": "",
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

    def build_trading_universe_from_upstox_raw(self, limit: int = 0, *, replace: bool = False) -> dict[str, Any]:
        raw_rows, meta = self._load_latest_upstox_raw_universe()
        header_map = self.sheets.ensure_sheet_headers_append(SheetNames.UNIVERSE, UNIVERSE_V2_HEADERS, header_row=3)
        col_canonical = int(header_map.get("Canonical ID", 27))
        col_primary_exchange = int(header_map.get("Primary Exchange", 28))
        col_secondary_exchange = int(header_map.get("Secondary Exchange", 29))
        col_secondary_key = int(header_map.get("Secondary Instrument Key", 30))
        col_symbol = int(header_map.get("Symbol", 2))
        col_exchange = int(header_map.get("Exchange", 3))
        col_enabled = int(header_map.get("Enabled", 9))
        col_notes = int(header_map.get("Notes", 11))
        col_provider = int(header_map.get("Data Provider", 22))
        col_instrument_key = int(header_map.get("Instrument Key", 23))
        col_source_segment = int(header_map.get("Source Segment", 24))
        col_security_type = int(header_map.get("Security Type", 25))
        min_cols = max(26, max(header_map.values()) if header_map else 26)

        existing_rows = [] if replace else self.sheets.read_sheet_rows(SheetNames.UNIVERSE, 4)
        for row in existing_rows:
            if len(row) < min_cols:
                row.extend([""] * (min_cols - len(row)))

        existing_by_canonical: dict[str, int] = {}
        preferred_by_symbol_exchange: dict[tuple[str, str], str] = {}
        existing_symbols_count = 0
        duplicate_existing_canonical = 0
        for i, row in enumerate(existing_rows):
            sym = row[col_symbol - 1].strip().upper() if len(row) >= col_symbol else ""
            if not sym:
                continue
            existing_symbols_count += 1
            exch = row[col_exchange - 1].strip().upper() if len(row) >= col_exchange else "NSE"
            notes = row[col_notes - 1] if len(row) >= col_notes else ""
            canonical = row[col_canonical - 1].strip().upper() if len(row) >= col_canonical else ""
            if canonical.startswith("DUPLICATE::"):
                # Heal old duplicate markers from previous versions.
                parts = canonical.split("::")
                if len(parts) >= 2 and parts[1].strip():
                    canonical = parts[1].strip().upper()
            if not canonical:
                canonical = canonical_id_from_fields(self._extract_isin_from_notes(notes), exch, sym)
            row[col_canonical - 1] = canonical
            if canonical in existing_by_canonical:
                duplicate_existing_canonical += 1
                # Keep sheet rows (no deletion), but explicitly disable duplicate canonical rows.
                row[col_enabled - 1] = "N"
                row[col_canonical - 1] = f"{canonical}::DUPROW::{i + 4}"
                note_text = row[col_notes - 1].strip() if len(row) >= col_notes else ""
                dedupe_note = f"duplicate_canonical={canonical}|primary_row={existing_by_canonical[canonical] + 4}"
                if dedupe_note not in note_text:
                    row[col_notes - 1] = f"{note_text}|{dedupe_note}".strip("|")
                continue
            existing_by_canonical[canonical] = i
            ik = row[col_instrument_key - 1].strip().upper() if len(row) >= col_instrument_key else ""
            if ik and str(row[col_enabled - 1]).strip().upper() == "Y":
                preferred_by_symbol_exchange.setdefault((sym, exch), ik)

        grouped: dict[str, list[dict[str, Any]]] = {}
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
            canonical = canonical_id_from_fields(isin, exchange, symbol)
            grouped.setdefault(canonical, []).append(
                {
                    "canonical_id": canonical,
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
            )

        masters: list[CanonicalListing] = []
        raw_json_by_canonical: dict[str, dict[str, Any]] = {}
        for canonical, rows_for_key in grouped.items():
            listing = choose_primary_listing(rows_for_key)
            if listing is None:
                continue
            masters.append(listing)
            primary_row = next((r for r in rows_for_key if str(r.get("exchange", "")).upper() == str(listing.primary_exchange).upper()), rows_for_key[0])
            raw_json_by_canonical[canonical] = dict(primary_row.get("raw_json") or {})
        masters.sort(key=lambda m: (str(m.symbol), str(m.primary_exchange)))
        masters, symbol_conflicts_resolved = self._dedupe_master_by_symbol_exchange(
            masters,
            preferred_by_symbol_exchange=preferred_by_symbol_exchange,
        )

        updated_existing = 0
        appended_rows: list[list[Any]] = []
        appended_symbols: list[str] = []
        next_idx = existing_symbols_count + 1
        for m in masters:
            canonical = str(m.canonical_id).strip().upper()
            existing_idx = existing_by_canonical.get(canonical)
            if existing_idx is not None:
                row = existing_rows[existing_idx]
                prev = list(row)
                row[col_symbol - 1] = str(m.symbol).upper()
                row[col_exchange - 1] = str(m.primary_exchange).upper()
                row[col_provider - 1] = "UPSTOX"
                row[col_instrument_key - 1] = str(m.primary_instrument_key)
                row[col_source_segment - 1] = str(m.primary_source_segment).upper()
                row[col_security_type - 1] = str(m.security_type).upper() or "UNKNOWN"
                row[col_canonical - 1] = canonical
                row[col_primary_exchange - 1] = str(m.primary_exchange).upper()
                row[col_secondary_exchange - 1] = str(m.secondary_exchange).upper()
                row[col_secondary_key - 1] = str(m.secondary_instrument_key)
                if row[col_notes - 1].strip() == "":
                    row[col_notes - 1] = f"isin={m.isin}|name={m.name}|source=upstox_bod"
                if row != prev:
                    updated_existing += 1
                continue

            raw_json = raw_json_by_canonical.get(canonical) or {}
            row = [""] * min_cols
            row[0] = next_idx
            row[col_symbol - 1] = str(m.symbol).upper()
            row[col_exchange - 1] = str(m.primary_exchange).upper()
            row[3] = "CASH"
            row[4] = "BOTH"
            row[5] = "AUTO"
            row[6] = "UNKNOWN"
            row[7] = 1.0
            row[8] = "Y"
            row[9] = 0
            row[col_notes - 1] = f"isin={m.isin}|name={m.name}|source=upstox_bod"
            row[11] = 0
            row[12] = 0
            row[13] = 0
            row[14] = ""
            row[15] = ""
            row[16] = ""
            row[17] = ""
            row[18] = json.dumps(raw_json, ensure_ascii=False, separators=(",", ":"))
            row[19] = ""
            row[20] = ""
            row[col_provider - 1] = "UPSTOX"
            row[col_instrument_key - 1] = str(m.primary_instrument_key)
            row[col_source_segment - 1] = str(m.primary_source_segment).upper()
            row[col_security_type - 1] = str(m.security_type).upper() or "UNKNOWN"
            row[25] = ""
            row[col_canonical - 1] = canonical
            row[col_primary_exchange - 1] = str(m.primary_exchange).upper()
            row[col_secondary_exchange - 1] = str(m.secondary_exchange).upper()
            row[col_secondary_key - 1] = str(m.secondary_instrument_key)
            appended_rows.append(row)
            appended_symbols.append(str(m.symbol).upper())
            existing_by_canonical[canonical] = len(existing_rows) + len(appended_rows) - 1
            next_idx += 1

        if replace:
            rows_for_replace = []
            seq = 1
            for r in appended_rows:
                r[0] = seq
                seq += 1
                rows_for_replace.append(r)
            self.sheets.replace_universe_rows(rows_for_replace)
            total_rows = len(rows_for_replace)
            appended = len(rows_for_replace)
            updated_existing = 0
        else:
            if existing_rows:
                last_col = self.sheets.col_to_a1(min_cols)
                write_rows = [row[:min_cols] for row in existing_rows]
                self.sheets.update_values(f"'{SheetNames.UNIVERSE}'!A4:{last_col}{3 + len(write_rows)}", write_rows)
            if appended_rows:
                self.sheets.append_universe_rows([row[:min_cols] for row in appended_rows])
            total_rows = existing_symbols_count + len(appended_rows)
            appended = len(appended_rows)

        out = {
            "rows": total_rows,
            "appended": appended,
            "appendedSymbols": [] if replace else appended_symbols,
            "updatedCanonical": updated_existing,
            "replaced": 1 if replace else 0,
            "rawSeen": seen_rows,
            "rawEligible": eligible_rows,
            "rawSnapshotDate": str(meta.get("runDate") or ""),
            "duplicateCanonicalInExisting": duplicate_existing_canonical,
            "symbolConflictsResolved": symbol_conflicts_resolved,
        }
        logger.info(
            "universe_build_from_raw complete appended=%s updatedCanonical=%s totalRows=%s replace=%s rawSeen=%s rawEligible=%s snapshotDate=%s duplicateCanonicalExisting=%s symbolConflictsResolved=%s",
            out["appended"], out["updatedCanonical"], out["rows"], bool(replace), seen_rows, eligible_rows, out["rawSnapshotDate"], duplicate_existing_canonical, symbol_conflicts_resolved,
        )
        return out

    def sync_universe_from_groww_instruments(self, limit: int = 0) -> int:
        # Backward-compatible endpoint name; now uses Upstox raw snapshot as source of truth.
        if not self.gcs.exists(self.gcs.upstox_raw_universe_latest_path()):
            self.refresh_raw_universe_from_upstox()
        out = self.build_trading_universe_from_upstox_raw(limit=limit, replace=False)
        return int(out.get("rows", 0))

    def _history_index_row_v2(
        self,
        *,
        symbol: str,
        exchange: str,
        segment: str,
        enabled: str,
        candles: list[list[object]],
        path: str,
        status: str,
        api_calls: int,
        last_error: str,
        expected_lcd: str,
        source: str,
    ) -> list[object]:
        first_ts = self._first_candle_ts(candles)
        last_ts = self._last_candle_ts(candles)
        bars = len(candles)
        first_candle_date = first_ts.astimezone(IST).date().isoformat() if first_ts else ""
        last_candle = last_ts.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S") if last_ts else ""
        file_name = path.rsplit("/", 1)[-1]
        notes = f"Src={source}|ExpectedLCD={expected_lcd}|Current={'Y' if status == 'FRESH_READY' else 'N'}"
        return [
            symbol,
            exchange,
            segment,
            "Y" if str(enabled).strip().upper() == "Y" else "N",
            bars,
            last_candle,
            now_ist_str(),
            status,
            int(api_calls),
            str(last_error or ""),
            file_name,
            "",
            notes,
            path,
            first_candle_date,
        ]

    def _intraday_index_row_5m(
        self,
        *,
        symbol: str,
        exchange: str,
        segment: str,
        enabled: str,
        candles: list[list[object]],
        path: str,
        status: str,
        api_calls: int,
        last_error: str,
        expected_lcd: str,
        source: str,
    ) -> list[object]:
        first_ts = self._first_candle_ts(candles)
        last_ts = self._last_candle_ts(candles)
        bars = len(candles)
        first_candle_date = first_ts.astimezone(IST).date().isoformat() if first_ts else ""
        last_candle = last_ts.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S") if last_ts else ""
        file_name = path.rsplit("/", 1)[-1]
        notes = f"Src={source}|ExpectedLCD={expected_lcd}|TF=5m"
        return [
            symbol,
            exchange,
            segment,
            "Y" if str(enabled).strip().upper() == "Y" else "N",
            bars,
            last_candle,
            now_ist_str(),
            status,
            int(api_calls),
            str(last_error or ""),
            file_name,
            "",
            notes,
            path,
            first_candle_date,
        ]

    def _trading_days_back_from_expected(self, *, expected_lcd: date_cls, trading_days: int) -> list[date_cls]:
        n = max(1, int(trading_days))
        out: list[date_cls] = []
        run_day_text = str(today_ist() or "")
        run_day = self._parse_iso_date(run_day_text) or expected_lcd
        cursor = expected_lcd
        for _ in range(max(500, n * 8)):
            if self._is_trading_day(cursor, run_year=run_day.year, run_day=run_day):
                out.append(cursor)
                if len(out) >= n:
                    break
            cursor -= timedelta(days=1)
        out.sort()
        return out

    def _fetch_intraday_5m_windowed_between(
        self,
        instrument_key: str,
        *,
        from_date: date_cls,
        to_date: date_cls,
        max_chunk_days: int = 30,
    ) -> tuple[list[list[object]], int]:
        if to_date < from_date:
            return [], 0
        calls = 0
        seen: dict[str, list[object]] = {}
        cursor = from_date
        chunk_days = max(1, int(max_chunk_days))
        while cursor <= to_date:
            win_end = min(to_date, cursor + timedelta(days=chunk_days - 1))
            part = self.upstox.get_historical_candles_v3_intraday_range(
                instrument_key,
                from_date=cursor.isoformat(),
                to_date=win_end.isoformat(),
                unit="minutes",
                interval=5,
            )
            calls += 1
            for c in part:
                if not isinstance(c, list) or len(c) < 6:
                    continue
                ts = parse_any_ts(c[0])
                if ts is None:
                    continue
                seen[ts.astimezone(IST).isoformat()] = c[:6]
            cursor = win_end + timedelta(days=1)
            time.sleep(0.02)
        merged = [seen[k] for k in sorted(seen.keys())]
        return merged, calls

    def prefetch_intraday_cache_5m_batch(
        self,
        *,
        lookback_trading_days: int = 60,
        api_cap: int = 1200,
        only_symbols: list[str] | None = None,
        refresh_last_day_only: bool = False,
        retry_stale_terminal_today: bool = False,
    ) -> dict[str, Any]:
        header_map = self.sheets.ensure_sheet_headers_append(SheetNames.UNIVERSE, UNIVERSE_V2_HEADERS, header_row=3)
        rows = self.sheets.read_sheet_rows(SheetNames.UNIVERSE, 4)
        col_symbol = int(header_map.get("Symbol", 2))
        col_exchange = int(header_map.get("Exchange", 3))
        col_segment = int(header_map.get("Segment", 4))
        col_enabled = int(header_map.get("Enabled", 9))
        col_instrument_key = int(header_map.get("Instrument Key", 23))

        effective_lookback_days = 1 if refresh_last_day_only else max(1, int(lookback_trading_days))
        expected_ctx = self._expected_lcd_context()
        expected_lcd = str(expected_ctx.get("expectedLCD") or "")
        expected = self._parse_iso_date(expected_lcd) or self._expected_last_completed_daily_date()
        td_window = self._trading_days_back_from_expected(expected_lcd=expected, trading_days=effective_lookback_days)
        window_start = td_window[0] if td_window else expected
        only_set = {str(s).strip().upper() for s in (only_symbols or []) if str(s).strip()}
        filter_enabled = bool(only_symbols is not None)

        scanned = 0
        fetches = 0
        updated = 0
        fresh_ready = 0
        terminal_insufficient = 0
        terminal_stale = 0
        terminal_missing = 0
        terminal_disabled = 0
        stale_pending = 0
        invalid = 0
        errors = 0
        pending_api_cap = 0
        index_rows: list[list[object]] = []
        retried_no_change = 0
        history_extended = 0
        prev_index = self._read_score_cache_5m_index_snapshot()

        for row in rows:
            symbol = row[col_symbol - 1].strip().upper() if len(row) >= col_symbol else ""
            if not symbol:
                continue
            if filter_enabled and symbol not in only_set:
                continue
            scanned += 1
            exchange = row[col_exchange - 1].strip().upper() if len(row) >= col_exchange else "NSE"
            segment = row[col_segment - 1].strip().upper() if len(row) >= col_segment else "CASH"
            enabled = row[col_enabled - 1].strip().upper() if len(row) >= col_enabled else "Y"
            instrument_key = row[col_instrument_key - 1].strip() if len(row) >= col_instrument_key else ""
            path = self.gcs.candle_cache_path(symbol, exchange, segment, "5m")
            cached = self.gcs.read_candles(path)
            cached_sorted = self._candles_sorted_unique(cached)
            before_sig = self._last_candle_sig(cached_sorted)
            before_bars = len(cached_sorted)
            source = "cache"
            row_error = ""
            api_calls_row = 0
            candles = cached_sorted
            cap_blocked = False
            prev_row = prev_index.get((symbol, exchange, segment))

            def _dates_in_window(cs: list[list[object]]) -> set[date_cls]:
                out_dates: set[date_cls] = set()
                for c in cs:
                    ts = parse_any_ts(c[0])
                    if ts is None:
                        continue
                    d = ts.astimezone(IST).date()
                    if window_start <= d <= expected:
                        out_dates.add(d)
                return out_dates

            try:
                if enabled != "Y":
                    status = "DISABLED_SKIPPED"
                    terminal_disabled += 1
                elif not instrument_key:
                    status = "INVALID_KEY_SKIPPED"
                    invalid += 1
                elif (
                    not retry_stale_terminal_today
                    and self._prefetch_should_skip_invalid_key_retry(prev_row, candles)
                ):
                    status = "INVALID_KEY_SKIPPED"
                    invalid += 1
                    source = "invalid_instrument_key_terminal"
                elif (
                    not retry_stale_terminal_today
                    and self._prefetch_intraday_should_skip_missing_retry(prev_row, candles, expected_lcd=expected_lcd)
                ):
                    status = "MISSING_SKIPPED"
                    terminal_missing += 1
                    source = "gcs_intraday_5m_missing_terminal"
                elif (
                    not retry_stale_terminal_today
                    and self._prefetch_intraday_should_skip_stale_retry(prev_row, candles, expected_lcd=expected_lcd)
                ):
                    status = "STALE_SKIPPED"
                    terminal_stale += 1
                    source = "gcs_intraday_5m_stale_terminal"
                elif (
                    not retry_stale_terminal_today
                    and self._prefetch_intraday_should_skip_insufficient_retry(prev_row, candles, expected_lcd=expected_lcd)
                ):
                    status = "INSUFFICIENT_HISTORY_FINAL"
                    terminal_insufficient += 1
                    source = "gcs_intraday_5m_insufficient_terminal"
                else:
                    dates_have = _dates_in_window(candles)
                    last_ts = self._last_candle_ts(candles)
                    last_date = last_ts.astimezone(IST).date() if last_ts else None
                    need_last_day = last_date is None or last_date < expected
                    need_depth = len(dates_have) < len(td_window)
                    should_fetch = need_last_day if refresh_last_day_only else (need_last_day or need_depth)

                    if should_fetch and fetches < max(0, int(api_cap)):
                        if refresh_last_day_only:
                            fetch_from = expected
                        elif need_depth:
                            fetch_from = window_start
                        elif last_date is None:
                            fetch_from = window_start
                        else:
                            fetch_from = max(window_start, last_date - timedelta(days=1))
                        est_calls = max(1, ((expected - fetch_from).days // 30) + 1)
                        if fetches + est_calls > max(0, int(api_cap)):
                            source = "api_cap_blocked"
                            cap_blocked = True
                        else:
                            fetched, calls = self._fetch_intraday_5m_windowed_between(
                                instrument_key,
                                from_date=fetch_from,
                                to_date=expected,
                                max_chunk_days=30,
                            )
                            fetches += calls
                            api_calls_row += calls
                            source = "upstox_api_5m_range"
                            if fetched:
                                candles = self.gcs.merge_candles(path, fetched)
                                candles = self._candles_sorted_unique(candles)
                            else:
                                source = "upstox_api_5m_empty"
                    elif should_fetch and fetches >= max(0, int(api_cap)):
                        source = "api_cap_blocked"
                        cap_blocked = True

                    dates_have = _dates_in_window(candles)
                    last_ts = self._last_candle_ts(candles)
                    last_date = last_ts.astimezone(IST).date() if last_ts else None
                    if cap_blocked and not candles:
                        status = "MISSING_PENDING"
                        pending_api_cap += 1
                    elif cap_blocked and (last_date is None or last_date < expected):
                        status = "STALE_PENDING"
                        stale_pending += 1
                        pending_api_cap += 1
                    elif cap_blocked and len(dates_have) < len(td_window):
                        status = "INSUFFICIENT_HISTORY_PENDING"
                        pending_api_cap += 1
                    elif not candles:
                        status = "MISSING_SKIPPED"
                        terminal_missing += 1
                    elif last_date is None or last_date < expected:
                        status = "STALE_SKIPPED"
                        terminal_stale += 1
                    elif len(dates_have) < len(td_window):
                        status = "INSUFFICIENT_HISTORY_FINAL"
                        terminal_insufficient += 1
                    else:
                        status = "FRESH_READY"
                        fresh_ready += 1
            except UpstoxApiError as exc:
                row_error = self._error_text_short(exc, max_len=180)
                if self._is_invalid_instrument_key_error(exc):
                    status = "INVALID_KEY_SKIPPED"
                    invalid += 1
                    source = "invalid_instrument_key_terminal"
                else:
                    status = "FETCH_ERROR"
                    source = "upstox_api_error"
                    errors += 1
                logger.warning(
                    "intraday_5m_cache_fetch_failed symbol=%s exchange=%s segment=%s instrument_key=%s error=%s",
                    symbol,
                    exchange,
                    segment,
                    instrument_key,
                    row_error,
                )
            except Exception as exc:
                row_error = self._error_text_short(exc, max_len=180)
                status = "FETCH_ERROR"
                source = "intraday_5m_unexpected_error"
                errors += 1
                logger.warning(
                    "intraday_5m_cache_unexpected_error symbol=%s exchange=%s segment=%s instrument_key=%s error=%s",
                    symbol,
                    exchange,
                    segment,
                    instrument_key,
                    row_error,
                )

            after_sig = self._last_candle_sig(candles)
            if after_sig != before_sig:
                updated += 1
            if len(candles) > before_bars:
                history_extended += 1
            if api_calls_row > 0 and after_sig == before_sig and len(candles) == before_bars:
                retried_no_change += 1
            index_rows.append(
                self._intraday_index_row_5m(
                    symbol=symbol,
                    exchange=exchange,
                    segment=segment,
                    enabled=enabled,
                    candles=candles,
                    path=path,
                    status=status,
                    api_calls=api_calls_row,
                    last_error=row_error,
                    expected_lcd=expected_lcd,
                    source=source,
                )
            )

        sheet_write_error = ""
        try:
            self.sheets.replace_score_cache_5m_index(index_rows)
        except Exception as exc:
            # Do not fail the whole backfill run on transient Sheets transport errors.
            # Cache files are already written to GCS and should keep progressing.
            sheet_write_error = self._error_text_short(exc, max_len=220)
            logger.warning("intraday_5m_index_write_failed error=%s", sheet_write_error)
        total = len(index_rows)
        complete = min(total, fresh_ready + terminal_insufficient + terminal_stale + terminal_missing + terminal_disabled + invalid)
        pending = max(0, total - complete)
        out = {
            "scanned": scanned,
            "fetches": fetches,
            "updated": updated,
            "historyExtended": history_extended,
            "retriedNoChange": retried_no_change,
            "freshReady": fresh_ready,
            "terminalInsufficientHistory": terminal_insufficient,
            "terminalStaleSkipped": terminal_stale,
            "terminalMissingSkipped": terminal_missing,
            "terminalDisabledSkipped": terminal_disabled,
            "terminalInvalidInstrumentKey": invalid,
            "apiCapBlocked": pending_api_cap,
            "staleOrMissing": pending,
            "stale": stale_pending,
            "errors": errors,
            "prefillDone": complete,
            "prefillComplete": pending == 0,
            "prefillCoveragePct": round((complete * 100.0 / total), 2) if total else 0.0,
            "total": total,
            "expectedLatestDailyCandleDate": expected_lcd,
            "expectedLcdMethod": str(expected_ctx.get("method") or "fallback-weekend"),
            "todayTradingDay": bool(expected_ctx.get("todayTradingDay", False)),
            "marketClosedToday": bool(expected_ctx.get("marketClosedToday", True)),
            "lookbackTradingDays": int(effective_lookback_days),
            "requestedLookbackTradingDays": int(lookback_trading_days),
            "refreshLastDayOnly": bool(refresh_last_day_only),
        }
        if sheet_write_error:
            out["sheetWriteError"] = sheet_write_error
        logger.info(
            "prefetch_intraday_cache_5m_batch complete scanned=%s fetches=%s updated=%s historyExtended=%s retriedNoChange=%s freshReady=%s terminalIH=%s terminalStale=%s terminalMissing=%s terminalDisabled=%s invalid=%s apiCapBlocked=%s stalePending=%s errors=%s prefillDone=%s/%s expectedLCD=%s lookbackTD=%s refreshLastDayOnly=%s",
            scanned,
            fetches,
            updated,
            history_extended,
            retried_no_change,
            fresh_ready,
            terminal_insufficient,
            terminal_stale,
            terminal_missing,
            terminal_disabled,
            invalid,
            pending_api_cap,
            stale_pending,
            errors,
            out["prefillDone"],
            total,
            expected_lcd,
            int(effective_lookback_days),
            bool(refresh_last_day_only),
        )
        return out

    def _update_universe_v2_cache_and_stats(
        self,
        *,
        api_cap: int = 600,
        run_full_backfill: bool = True,
        priority_symbols: list[str] | None = None,
        fetch_only_symbols: list[str] | None = None,
        write_history_index: bool = True,
    ) -> dict[str, Any]:
        header_map = self.sheets.ensure_sheet_headers_append(SheetNames.UNIVERSE, UNIVERSE_V2_HEADERS, header_row=3)
        rows = self.sheets.read_sheet_rows(SheetNames.UNIVERSE, 4)
        min_cols = max(26, max(header_map.values()) if header_map else 26)
        for r in rows:
            if len(r) < min_cols:
                r.extend([""] * (min_cols - len(r)))

        col_symbol = int(header_map.get("Symbol", 2))
        col_exchange = int(header_map.get("Exchange", 3))
        col_segment = int(header_map.get("Segment", 4))
        col_enabled = int(header_map.get("Enabled", 9))
        col_notes = int(header_map.get("Notes", 11))
        col_raw_json = int(header_map.get("Raw CSV (JSON)", 19))
        col_instrument_key = int(header_map.get("Instrument Key", 23))
        col_canonical = int(header_map.get("Canonical ID", 27))
        fetch_only_set: set[str] | None = None
        if fetch_only_symbols is not None:
            fetch_only_set = {str(s).strip().upper() for s in fetch_only_symbols if str(s).strip()}
        if priority_symbols:
            pset = {str(s).strip().upper() for s in priority_symbols if str(s).strip()}
            if pset:
                prioritized = [r for r in rows if len(r) >= col_symbol and str(r[col_symbol - 1]).strip().upper() in pset]
                others = [r for r in rows if len(r) < col_symbol or str(r[col_symbol - 1]).strip().upper() not in pset]
                rows = prioritized + others

        expected_ctx = self._expected_lcd_context()
        expected_lcd = str(expected_ctx.get("expectedLCD") or "")
        expected = self._parse_iso_date(expected_lcd) or self._expected_last_completed_daily_date()
        horizon_start = self._history_horizon_start_ist()
        horizon_floor = horizon_start.date()

        scanned = 0
        fetches = 0
        updated = 0
        stale = 0
        missing = 0
        invalid = 0
        error_count = 0
        index_rows: list[list[object]] = []
        stats_by_canonical: dict[str, TradabilityStats] = {}
        quality_by_canonical: dict[str, dict[str, Any]] = {}

        for row in rows:
            symbol = row[col_symbol - 1].strip().upper() if len(row) >= col_symbol else ""
            if not symbol:
                continue
            scanned += 1
            exchange = row[col_exchange - 1].strip().upper() if len(row) >= col_exchange else "NSE"
            segment = row[col_segment - 1].strip().upper() if len(row) >= col_segment else "CASH"
            enabled = row[col_enabled - 1] if len(row) >= col_enabled else "Y"
            notes = row[col_notes - 1] if len(row) >= col_notes else ""
            raw_json_text = row[col_raw_json - 1] if len(row) >= col_raw_json else ""
            instrument_key = row[col_instrument_key - 1].strip() if len(row) >= col_instrument_key else ""
            canonical = row[col_canonical - 1].strip().upper() if len(row) >= col_canonical else ""
            if not canonical:
                canonical = canonical_id_from_fields(self._extract_isin_from_notes(notes), exchange, symbol)
                row[col_canonical - 1] = canonical

            path, cached = self._read_score_cache_with_migration(symbol, exchange, segment, instrument_key)
            before_sig = self._last_candle_sig(cached)
            last_error = ""
            source = "cache"
            api_calls = 0
            candles = cached
            suspended_or_delisted = False
            if raw_json_text.strip():
                try:
                    raw_obj = json.loads(raw_json_text)
                    if isinstance(raw_obj, dict):
                        suspended_or_delisted = (
                            self._flag_any(raw_obj.get("is_delisted"), False)
                            or self._flag_any(raw_obj.get("is_suspended"), False)
                            or self._flag_any(raw_obj.get("suspended"), False)
                        )
                except Exception:
                    suspended_or_delisted = False

            try:
                if str(enabled).strip().upper() != "Y":
                    status = "DISABLED"
                    quality = "DISABLED"
                elif not instrument_key:
                    status = "INVALID_KEY_SKIPPED"
                    quality = "INVALID_KEY"
                    invalid += 1
                else:
                    allow_fetch_for_symbol = fetch_only_set is None or symbol in fetch_only_set
                    did_fetch = False
                    if allow_fetch_for_symbol and len(candles) == 0 and fetches < api_cap:
                        api = self._fetch_daily_candles_windowed_between(instrument_key, horizon_start, now_ist())
                        fetches += 1
                        api_calls += 1
                        did_fetch = True
                        source = "upstox_api_full_history"
                        if api:
                            candles = self.gcs.merge_candles(path, api)
                    elif allow_fetch_for_symbol and len(candles) > 0 and fetches < api_cap:
                        api = self._fetch_daily_candles_incremental(instrument_key, candles, lookback_days=9500)
                        fetches += 1
                        api_calls += 1
                        did_fetch = True
                        source = "upstox_api_incremental"
                        if api:
                            candles = self.gcs.merge_candles(path, api)

                    if allow_fetch_for_symbol and run_full_backfill and len(candles) > 0 and fetches < api_cap:
                        first_ts = self._first_candle_ts(candles)
                        if first_ts is None or first_ts.astimezone(IST).date() > (horizon_floor + timedelta(days=14)):
                            older = self._fetch_daily_candles_backfill_older(instrument_key, candles, lookback_days=9500)
                            fetches += 1
                            api_calls += 1
                            did_fetch = True
                            source = "upstox_api_backfill_older"
                            if older:
                                candles = self.gcs.merge_candles(path, older)

                    if allow_fetch_for_symbol and not did_fetch and fetches >= api_cap:
                        source = "api_cap_blocked"
                    elif not allow_fetch_for_symbol:
                        source = "fetch_scope_skipped"

                    last_ts = self._last_candle_ts(candles)
                    if len(candles) == 0 or last_ts is None:
                        status = "MISSING"
                        quality = "MISSING"
                        missing += 1
                    else:
                        last_date = last_ts.astimezone(IST).date()
                        stale_days = max(0, (expected - last_date).days)
                        if stale_days > 0:
                            status = "STALE_READY"
                            quality = "STALE"
                            stale += 1
                        else:
                            status = "FRESH_READY"
                            quality = "FRESH"
            except UpstoxApiError as exc:
                if self._is_invalid_instrument_key_error(exc):
                    status = "INVALID_KEY_SKIPPED"
                    quality = "INVALID_KEY"
                    invalid += 1
                else:
                    status = "FETCH_ERROR"
                    quality = "STALE" if len(cached) > 0 else "MISSING"
                    error_count += 1
                last_error = self._error_text_short(exc, max_len=180)
                logger.warning(
                    "universe_v2_candle_fetch_failed symbol=%s exchange=%s segment=%s instrument_key=%s error=%s",
                    symbol,
                    exchange,
                    segment,
                    instrument_key,
                    last_error,
                )
            except Exception as exc:
                status = "FETCH_ERROR"
                quality = "STALE" if len(cached) > 0 else "MISSING"
                error_count += 1
                last_error = self._error_text_short(exc, max_len=180)
                logger.warning(
                    "universe_v2_candle_fetch_unexpected_error symbol=%s exchange=%s segment=%s instrument_key=%s error=%s",
                    symbol,
                    exchange,
                    segment,
                    instrument_key,
                    last_error,
                )

            after_sig = self._last_candle_sig(candles)
            if after_sig != before_sig:
                updated += 1

            stats = compute_tradability_stats(candles)
            last_ts = self._last_candle_ts(candles)
            last_date = last_ts.astimezone(IST).date().isoformat() if last_ts is not None else ""
            stale_days = max(0, (expected - last_ts.astimezone(IST).date()).days) if last_ts is not None else 9999
            stats_by_canonical[canonical] = stats
            quality_by_canonical[canonical] = {
                "data_quality_flag": quality,
                "stale_days": int(stale_days),
                "last_1d_date": last_date,
                "status": status,
                "suspended_or_delisted": bool(suspended_or_delisted),
            }
            index_rows.append(
                self._history_index_row_v2(
                    symbol=symbol,
                    exchange=exchange,
                    segment=segment,
                    enabled=enabled,
                    candles=candles,
                    path=path,
                    status=status,
                    api_calls=api_calls,
                    last_error=last_error,
                    expected_lcd=expected_lcd,
                    source=source,
                )
            )

        if write_history_index:
            self.sheets.replace_score_cache_1d_index(index_rows)
        assign_turnover_rank_and_bucket(stats_by_canonical)
        return {
            "statsByCanonical": stats_by_canonical,
            "qualityByCanonical": quality_by_canonical,
            "summary": {
                "scanned": scanned,
                "fetches": fetches,
                "updated": updated,
                "stale": stale,
                "missing": missing,
                "invalidKey": invalid,
                "errors": error_count,
                "expectedLatestDailyCandleDate": expected_lcd,
                "expectedLcdMethod": str(expected_ctx.get("method") or "fallback-weekend"),
                "todayTradingDay": bool(expected_ctx.get("todayTradingDay", False)),
                "marketClosedToday": bool(expected_ctx.get("marketClosedToday", True)),
                "fetchScopeSymbols": len(fetch_only_set) if fetch_only_set is not None else -1,
            },
        }

    def _write_universe_v2_columns(
        self,
        *,
        controls: UniverseControls,
        stats_by_canonical: dict[str, TradabilityStats],
        quality_by_canonical: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        header_map = self.sheets.ensure_sheet_headers_append(SheetNames.UNIVERSE, UNIVERSE_V2_HEADERS, header_row=3)
        rows = self.sheets.read_sheet_rows(SheetNames.UNIVERSE, 4)
        col_symbol = int(header_map.get("Symbol", 2))
        col_exchange = int(header_map.get("Exchange", 3))
        col_enabled = int(header_map.get("Enabled", 9))
        col_notes = int(header_map.get("Notes", 11))
        col_canonical = int(header_map.get("Canonical ID", 27))
        col_primary_exchange = int(header_map.get("Primary Exchange", 28))
        col_secondary_exchange = int(header_map.get("Secondary Exchange", 29))
        col_secondary_key = int(header_map.get("Secondary Instrument Key", 30))

        updated_at = now_ist_str()
        eligible_swing_count = 0
        eligible_intraday_count = 0
        disabled_count = 0
        stale_count = 0
        disable_reasons: dict[str, int] = {}
        total_master_count = 0

        header_to_colvals: dict[str, list[list[Any]]] = {h: [] for h in UNIVERSE_V2_HEADERS}
        for row in rows:
            symbol = row[col_symbol - 1].strip().upper() if len(row) >= col_symbol else ""
            if not symbol:
                for h in UNIVERSE_V2_HEADERS:
                    header_to_colvals[h].append([""])
                continue
            exchange = row[col_exchange - 1].strip().upper() if len(row) >= col_exchange else "NSE"
            enabled = row[col_enabled - 1].strip().upper() if len(row) >= col_enabled else "Y"
            notes = row[col_notes - 1] if len(row) >= col_notes else ""
            canonical = row[col_canonical - 1].strip().upper() if len(row) >= col_canonical else ""
            if not canonical:
                canonical = canonical_id_from_fields(self._extract_isin_from_notes(notes), exchange, symbol)

            total_master_count += 1
            stats = stats_by_canonical.get(canonical, TradabilityStats())
            quality = quality_by_canonical.get(
                canonical,
                {
                    "data_quality_flag": "MISSING",
                    "stale_days": 9999,
                    "last_1d_date": "",
                    "status": "MISSING",
                    "suspended_or_delisted": False,
                },
            )
            data_quality_flag = str(quality.get("data_quality_flag") or "MISSING").strip().upper()
            stale_days = int(quality.get("stale_days") or 0)
            suspended_or_delisted = bool(quality.get("suspended_or_delisted"))
            if data_quality_flag == "STALE":
                stale_count += 1

            eligibility = classify_eligibility(
                stats=stats,
                data_quality_flag=data_quality_flag,
                stale_days=stale_days,
                controls=controls,
                suspended_or_delisted=suspended_or_delisted,
                enabled=(enabled == "Y"),
            )
            if eligibility.eligible_swing:
                eligible_swing_count += 1
            if eligibility.eligible_intraday:
                eligible_intraday_count += 1
            if not eligibility.eligible_swing:
                disabled_count += 1
                reason = str(eligibility.disable_reason or "UNKNOWN_DISABLE_REASON")
                disable_reasons[reason] = disable_reasons.get(reason, 0) + 1

            values = {
                "Canonical ID": canonical,
                "Primary Exchange": (
                    row[col_primary_exchange - 1].strip().upper()
                    if len(row) >= col_primary_exchange and row[col_primary_exchange - 1].strip()
                    else exchange
                ),
                "Secondary Exchange": row[col_secondary_exchange - 1].strip().upper() if len(row) >= col_secondary_exchange else "",
                "Secondary Instrument Key": row[col_secondary_key - 1].strip() if len(row) >= col_secondary_key else "",
                "Bars 1D": int(stats.bars_1d),
                "Last 1D Date": str(quality.get("last_1d_date") or ""),
                "Price Last": round(float(stats.price_last), 4) if math.isfinite(float(stats.price_last)) else 0.0,
                "Turnover Med 60D": round(float(stats.turnover_med_60d), 4) if math.isfinite(float(stats.turnover_med_60d)) else 0.0,
                "ATR 14": round(float(stats.atr_14), 6) if math.isfinite(float(stats.atr_14)) else 0.0,
                "ATR Pct 14D": round(float(stats.atr_pct_14d), 6) if math.isfinite(float(stats.atr_pct_14d)) else 0.0,
                "Gap Risk 60D": round(float(stats.gap_risk_60d), 6) if math.isfinite(float(stats.gap_risk_60d)) else 0.0,
                "Turnover Rank 60D": int(stats.turnover_rank_60d) if stats.turnover_rank_60d is not None else "",
                "Liquidity Bucket": str(stats.liquidity_bucket or ""),
                "Data Quality Flag": data_quality_flag,
                "Stale Days": int(stale_days),
                "Eligible Swing": "Y" if eligibility.eligible_swing else "N",
                "Eligible Intraday": "Y" if eligibility.eligible_intraday else "N",
                "Disable Reason": str(eligibility.disable_reason or ""),
                "Universe Mode": str(controls.mode),
                "Universe V2 Updated At": updated_at,
            }
            for h in UNIVERSE_V2_HEADERS:
                header_to_colvals[h].append([values[h]])

        if rows:
            end_row = 3 + len(rows)
            for h in UNIVERSE_V2_HEADERS:
                col = int(header_map[h])
                col_a1 = self.sheets.col_to_a1(col)
                self.sheets.update_values(
                    f"'{SheetNames.UNIVERSE}'!{col_a1}4:{col_a1}{end_row}",
                    header_to_colvals[h],
                )

        top_disable = sorted(disable_reasons.items(), key=lambda kv: (-kv[1], kv[0]))[:5]
        return {
            "totalMasterCount": total_master_count,
            "eligibleSwingCount": eligible_swing_count,
            "eligibleIntradayCount": eligible_intraday_count,
            "disabledCount": disabled_count,
            "staleCount": stale_count,
            "topDisableReasons": [{"reason": k, "count": v} for k, v in top_disable],
            "mode": controls.mode,
        }

    def run_universe_v2_pipeline(
        self,
        *,
        build_limit: int = 0,
        replace: bool = False,
        candle_api_cap: int = 600,
        run_full_backfill: bool = True,
        write_v2_eligibility: bool = False,
    ) -> dict[str, Any]:
        controls = self._build_universe_v2_controls()
        try:
            raw_out = self.refresh_raw_universe_from_upstox()
        except Exception as exc:
            # Raw fallback contract: keep last-good snapshot pointer and exit without mutating universe/cache sheets.
            return {
                "raw": {
                    "ok": False,
                    "errorType": type(exc).__name__,
                    "error": self._error_text_short(exc, max_len=240),
                    "latestPointerUnchanged": True,
                },
                "build": {"skipped": "raw_snapshot_failed"},
                "cache": {"skipped": "raw_snapshot_failed"},
                "eligibility": {"skipped": "raw_snapshot_failed"},
            }
        build_out = self.build_trading_universe_from_upstox_raw(limit=max(0, build_limit), replace=replace)
        appended_symbols = build_out.get("appendedSymbols") if isinstance(build_out, dict) else None
        fetch_only_symbols: list[str] | None
        if replace:
            # Replace mode is explicit full rebuild; allow full fetch scope.
            fetch_only_symbols = None
        elif isinstance(appended_symbols, list):
            # Daily mode: reduce overlap by fetching candles only for newly appended symbols.
            fetch_only_symbols = list(appended_symbols)
        else:
            fetch_only_symbols = []
        cache_out = self._update_universe_v2_cache_and_stats(
            api_cap=max(0, candle_api_cap),
            run_full_backfill=bool(run_full_backfill),
            priority_symbols=(list(appended_symbols) if isinstance(appended_symbols, list) else None),
            fetch_only_symbols=fetch_only_symbols,
        )
        if write_v2_eligibility:
            stats_out = self._write_universe_v2_columns(
                controls=controls,
                stats_by_canonical=cache_out["statsByCanonical"],
                quality_by_canonical=cache_out["qualityByCanonical"],
            )
        else:
            stats_out = {"skipped": "deferred_to_score_refresh"}
        return {
            "raw": raw_out,
            "build": build_out,
            "cache": cache_out["summary"],
            "eligibility": stats_out,
        }

    def recompute_universe_v2_from_cache(self) -> dict[str, Any]:
        controls = self._build_universe_v2_controls()
        cache_out = self._update_universe_v2_cache_and_stats(
            api_cap=0,
            run_full_backfill=False,
            fetch_only_symbols=None,
            write_history_index=False,
        )
        stats_out = self._write_universe_v2_columns(
            controls=controls,
            stats_by_canonical=cache_out["statsByCanonical"],
            quality_by_canonical=cache_out["qualityByCanonical"],
        )
        return {
            "cache": cache_out["summary"],
            "eligibility": stats_out,
        }

    def audit_universe_v2_integrity(self) -> dict[str, Any]:
        header_map = self.sheets.ensure_sheet_headers_append(SheetNames.UNIVERSE, UNIVERSE_V2_HEADERS, header_row=3)
        rows = self.sheets.read_sheet_rows(SheetNames.UNIVERSE, 4)
        col_symbol = int(header_map.get("Symbol", 2))
        col_exchange = int(header_map.get("Exchange", 3))
        col_segment = int(header_map.get("Segment", 4))
        col_canonical = int(header_map.get("Canonical ID", 27))
        col_instrument_key = int(header_map.get("Instrument Key", 23))

        universe_rows: list[dict[str, str]] = []
        for rnum, row in enumerate(rows, start=4):
            sym = row[col_symbol - 1].strip().upper() if len(row) >= col_symbol else ""
            if not sym:
                continue
            universe_rows.append(
                {
                    "row": str(rnum),
                    "symbol": sym,
                    "exchange": (row[col_exchange - 1].strip().upper() if len(row) >= col_exchange else "NSE"),
                    "segment": (row[col_segment - 1].strip().upper() if len(row) >= col_segment else "CASH"),
                    "canonical": (row[col_canonical - 1].strip().upper() if len(row) >= col_canonical else ""),
                    "instrument_key": (row[col_instrument_key - 1].strip() if len(row) >= col_instrument_key else ""),
                }
            )

        canon_ctr = Counter(x["canonical"] for x in universe_rows if x["canonical"])
        sym_exch_ctr = Counter((x["symbol"], x["exchange"]) for x in universe_rows)
        hist_rows = self.sheets.read_sheet_rows(SheetNames.SCORE_CACHE_1D, 4)
        hist_key_ctr = Counter()
        hist_path_ctr = Counter()
        history_first_date_known = 0
        history_first_date_year_2000 = 0
        history_status_counts: Counter[str] = Counter()
        history_terminal_or_fresh_count = 0
        history_last_date_known = 0
        history_last_date_min: date_cls | None = None
        history_last_date_max: date_cls | None = None
        for row in hist_rows:
            if len(row) < 3:
                continue
            sym = row[0].strip().upper() if len(row) > 0 else ""
            if not sym:
                continue
            exch = row[1].strip().upper() if len(row) > 1 else "NSE"
            seg = row[2].strip().upper() if len(row) > 2 else "CASH"
            path = row[13].strip() if len(row) > 13 else ""
            first_candle_date = row[14].strip() if len(row) > 14 else ""
            status = row[7].strip().upper() if len(row) > 7 else ""
            last_candle_time = row[5].strip() if len(row) > 5 else ""
            hist_key_ctr[(sym, exch, seg)] += 1
            if path:
                hist_path_ctr[path] += 1
            if status:
                history_status_counts[status] += 1
            if status in {
                "FRESH_READY",
                "INSUFFICIENT_HISTORY_FINAL",
                "STALE_SKIPPED",
                "INVALID_KEY_SKIPPED",
                "MISSING_SKIPPED",
            }:
                history_terminal_or_fresh_count += 1
            if first_candle_date:
                history_first_date_known += 1
                parsed = self._parse_iso_date(first_candle_date)
                if parsed is not None and parsed.year == 2000:
                    history_first_date_year_2000 += 1
            if last_candle_time:
                parsed_last = self._parse_iso_date(last_candle_time)
                if parsed_last is not None:
                    history_last_date_known += 1
                    if history_last_date_min is None or parsed_last < history_last_date_min:
                        history_last_date_min = parsed_last
                    if history_last_date_max is None or parsed_last > history_last_date_max:
                        history_last_date_max = parsed_last

        canon_dups = sorted([(k, v) for k, v in canon_ctr.items() if v > 1], key=lambda kv: (-kv[1], kv[0]))
        sym_dups = sorted([(k, v) for k, v in sym_exch_ctr.items() if v > 1], key=lambda kv: (-kv[1], kv[0]))
        hist_key_dups = sorted([(k, v) for k, v in hist_key_ctr.items() if v > 1], key=lambda kv: (-kv[1], kv[0]))
        hist_path_dups = sorted([(k, v) for k, v in hist_path_ctr.items() if v > 1], key=lambda kv: (-kv[1], kv[0]))

        return {
            "universeRows": len(universe_rows),
            "historyRows": sum(hist_key_ctr.values()),
            "universeDuplicateCanonicalCount": len(canon_dups),
            "universeDuplicateSymbolExchangeCount": len(sym_dups),
            "historyDuplicateSymbolExchangeSegmentCount": len(hist_key_dups),
            "historyDuplicatePathCount": len(hist_path_dups),
            "historyFirstCandleDateKnownCount": history_first_date_known,
            "historyFirstCandleYear2000Count": history_first_date_year_2000,
            "historyStatusCounts": dict(sorted(history_status_counts.items())),
            "historyTerminalOrFreshCount": history_terminal_or_fresh_count,
            "historyPendingOrErrorCount": max(0, sum(hist_key_ctr.values()) - history_terminal_or_fresh_count),
            "historyLastCandleDateKnownCount": history_last_date_known,
            "historyLastCandleDateMin": history_last_date_min.isoformat() if history_last_date_min else "",
            "historyLastCandleDateMax": history_last_date_max.isoformat() if history_last_date_max else "",
            "samples": {
                "universeDuplicateCanonical": [{"canonical": k, "count": v} for k, v in canon_dups[:10]],
                "universeDuplicateSymbolExchange": [{"symbol": k[0], "exchange": k[1], "count": v} for k, v in sym_dups[:10]],
                "historyDuplicateSymbolExchangeSegment": [
                    {"symbol": k[0], "exchange": k[1], "segment": k[2], "count": v} for k, v in hist_key_dups[:10]
                ],
                "historyDuplicatePath": [{"path": k, "count": v} for k, v in hist_path_dups[:10]],
            },
        }

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
        # Holiday-aware ExpectedLCD: most recent completed trading day strictly before today.
        return self._expected_last_completed_daily_date(now)

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

    def _fetch_daily_candles_expected_lcd_bounded(
        self,
        instrument_key: str,
        *,
        expected_lcd: str,
        lookback_days: int,
    ) -> list[list[object]]:
        expected = self._parse_iso_date(expected_lcd)
        if expected is None:
            return []
        end = datetime(expected.year, expected.month, expected.day, tzinfo=IST) + timedelta(days=1)
        start = end - timedelta(days=max(3650, lookback_days))
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
        path, cached = self._read_score_cache_with_migration(symbol, exchange, segment, instrument_key)
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
        terminal_invalid_key = 0
        terminal_missing = 0
        provisional_ready = 0
        pending = 0
        updated = 0
        retried_no_change = 0
        expected_ctx = self._expected_lcd_context()
        expected = str(expected_ctx.get("expectedLCD") or self._expected_latest_daily_candle_date().strftime("%Y-%m-%d"))
        updated_at = now_ist_str()
        score_cache_index_rows: list[list[object]] = []
        prev_index = self._read_score_cache_index_snapshot()

        for u in rows:
            scanned += 1
            path, cached = self._read_score_cache_with_migration(u.symbol, u.exchange, u.segment, u.instrument_key)
            before_bars = len(cached)
            before_last_text = self._last_candle_text(cached)
            before_last_sig = self._last_candle_sig(cached)
            before_ready = len(cached) >= min_bars
            before_fresh = before_ready and self._daily_cache_is_current(cached)
            prev_row = prev_index.get((u.symbol, u.exchange, u.segment))
            before_provisional = before_fresh and self._is_provisional_source((prev_row or {}).get("src", ""))
            row_last_error = ""
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
                        last_error=row_last_error,
                    )
                )
                continue
            if before_ready:
                ready += 1
            if 0 < len(cached) < min_bars and self._daily_cache_is_current(cached):
                candles, source, api_calls = cached, "gcs_score_cache_1d_insufficient_history_final", 0
            elif (
                self._prefetch_should_skip_invalid_key_retry(prev_row, cached)
            ):
                candles, source, api_calls = cached, "invalid_instrument_key_terminal", 0
            elif (
                not before_ready
                and not retry_stale_terminal_today
                and self._prefetch_should_skip_missing_retry(prev_row, cached, expected_lcd=expected)
            ):
                candles, source, api_calls = cached, "gcs_score_cache_1d_missing_terminal", 0
            elif (
                before_ready
                and not allow_provisional_intraday
                and not retry_stale_terminal_today
                and self._prefetch_should_skip_stale_retry(prev_row, cached, expected_lcd=expected)
            ):
                candles, source, api_calls = cached, "gcs_score_cache_1d_stale_terminal", 0
            else:
                try:
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
                except UpstoxApiError as exc:
                    row_last_error = self._error_text_short(exc)
                    if self._is_invalid_instrument_key_error(exc):
                        candles, source, api_calls = cached, "invalid_instrument_key_terminal", 1
                        logger.warning(
                            "prefetch_score_cache_batch skip invalid instrument key symbol=%s exchange=%s segment=%s instrument_key=%s error=%s",
                            u.symbol,
                            u.exchange,
                            u.segment,
                            u.instrument_key,
                            row_last_error,
                        )
                    else:
                        # Keep full-universe progress resilient to one-symbol API failures.
                        candles, source, api_calls = cached, "upstox_api_error", 1
                        logger.warning(
                            "prefetch_score_cache_batch symbol fetch error symbol=%s exchange=%s segment=%s instrument_key=%s error=%s",
                            u.symbol,
                            u.exchange,
                            u.segment,
                            u.instrument_key,
                            row_last_error,
                        )
                except Exception as exc:
                    row_last_error = self._error_text_short(exc)
                    candles, source, api_calls = cached, "prefetch_unexpected_error", 0
                    logger.warning(
                        "prefetch_score_cache_batch unexpected symbol error symbol=%s exchange=%s segment=%s instrument_key=%s error=%s",
                        u.symbol,
                        u.exchange,
                        u.segment,
                        u.instrument_key,
                        row_last_error,
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
            elif source == "invalid_instrument_key_terminal":
                terminal_invalid_key += 1
            elif source == "gcs_score_cache_1d_missing_terminal":
                terminal_missing += 1
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
                    last_error=row_last_error,
                )
            )

        # Refresh the visible 1D score-cache index sheet so manual backfill progress can be tracked easily.
        self.sheets.replace_score_cache_1d_index(score_cache_index_rows)

        total = len(rows)
        complete = min(total, fresh + terminal_insufficient + terminal_stale + terminal_invalid_key + terminal_missing)
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
            "terminalInvalidInstrumentKey": terminal_invalid_key,
            "terminalMissingSkipped": terminal_missing,
            "prefillDone": complete,
            "prefillComplete": pending == 0,
            "prefillCoveragePct": round((complete * 100.0 / total), 2) if total else 0.0,
            "staleOrMissing": pending,
            "total": total,
            "freshCoveragePct": round((fresh * 100.0 / total), 2) if total else 0.0,
            "provisionalCoveragePct": round((provisional_ready * 100.0 / total), 2) if total else 0.0,
            "expectedLatestDailyCandleDate": expected,
            "expectedLcdMethod": str(expected_ctx.get("method") or "fallback-weekend"),
            "todayTradingDay": bool(expected_ctx.get("todayTradingDay", False)),
            "marketClosedToday": bool(expected_ctx.get("marketClosedToday", True)),
        }
        logger.info(
            "prefetch_score_cache_batch complete scanned=%s fetches=%s updated=%s retriedNoChange=%s freshReady=%s terminalIH=%s terminalStale=%s terminalInvalidKey=%s terminalMissing=%s prefillDone=%s/%s pending=%s expectedLCD=%s method=%s todayTradingDay=%s",
            scanned,
            fetches,
            updated,
            retried_no_change,
            fresh,
            terminal_insufficient,
            terminal_stale,
            terminal_invalid_key,
            terminal_missing,
            complete,
            total,
            pending,
            expected,
            out.get("expectedLcdMethod"),
            out.get("todayTradingDay"),
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
        skip_counts = {"freshWindow": 0, "insufficientHistory": 0, "staleCache": 0, "indicatorNone": 0, "invalidInstrumentKey": 0}
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

            try:
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
            except UpstoxApiError as exc:
                if not self._is_invalid_instrument_key_error(exc):
                    raise
                skip_counts["invalidInstrumentKey"] += 1
                api_calls = 1
                source = "invalid_instrument_key_terminal"
                _, candles = self._read_score_cache_with_migration(u.symbol, u.exchange, u.segment, u.instrument_key)
                short_err = self._error_text_short(exc, max_len=96)
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
                            f"Skip=INVALID_INSTRUMENT_KEY|Bars={len(candles)}|Src={source}|Err={short_err}",
                            self._score_calc_skip_short("INVKEY", u.instrument_key),
                        ],
                    )
                )
                logger.warning(
                    "score_universe_batch skip invalid instrument key symbol=%s exchange=%s segment=%s instrument_key=%s error=%s",
                    u.symbol,
                    u.exchange,
                    u.segment,
                    u.instrument_key,
                    short_err,
                )
                if len(updates) >= max(1, int(sheet_write_batch_size)):
                    self.sheets.update_universe_score_columns(updates)
                    updates.clear()
                fetches += api_calls
                continue
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
    def _clip01(v: float) -> float:
        if not math.isfinite(float(v)):
            return 0.0
        return max(0.0, min(1.0, float(v)))

    @staticmethod
    def _norm_minmax_clip(v: float, lo: float, hi: float) -> float:
        if not math.isfinite(float(v)):
            return 0.0
        if hi <= lo:
            return 0.0
        return UniverseService._clip01((float(v) - float(lo)) / (float(hi) - float(lo)))

    @staticmethod
    def _ema_last(values: list[float], period: int) -> float:
        if not values:
            return 0.0
        if period <= 1:
            return float(values[-1])
        alpha = 2.0 / (float(period) + 1.0)
        ema = float(values[0])
        for x in values[1:]:
            ema = (alpha * float(x)) + ((1.0 - alpha) * ema)
        return float(ema)

    @staticmethod
    def _safe_key_fragment(key: str) -> str:
        raw = str(key or "").strip().upper()
        if not raw:
            return "UNKNOWN"
        out = []
        for ch in raw:
            if ch.isalnum():
                out.append(ch)
            else:
                out.append("_")
        collapsed = "".join(out)
        while "__" in collapsed:
            collapsed = collapsed.replace("__", "_")
        return collapsed.strip("_") or "UNKNOWN"

    @staticmethod
    def _candles_sorted_unique(candles: list[list[object]]) -> list[list[object]]:
        by_ts: dict[str, list[object]] = {}
        for c in candles:
            if not c or len(c) < 6:
                continue
            ts = parse_any_ts(c[0])
            if ts is None:
                continue
            by_ts[ts.astimezone(IST).isoformat()] = [ts.astimezone(IST).isoformat(), c[1], c[2], c[3], c[4], c[5]]
        return [by_ts[k] for k in sorted(by_ts.keys())]

    @staticmethod
    def _daily_no_lookahead(candles: list[list[object]], expected_lcd: str) -> list[list[object]]:
        expected = UniverseService._parse_iso_date(expected_lcd)
        if expected is None:
            return UniverseService._candles_sorted_unique(candles)
        out: list[list[object]] = []
        for c in UniverseService._candles_sorted_unique(candles):
            ts = parse_any_ts(c[0])
            if ts is None:
                continue
            if ts.astimezone(IST).date() <= expected:
                out.append(c)
        return out

    @staticmethod
    def _completed_intraday_bars(candles: list[list[object]], now_i: datetime, interval_min: int) -> list[list[object]]:
        if interval_min <= 0:
            interval_min = 5
        cutoff = now_i.astimezone(IST) - timedelta(minutes=int(interval_min))
        out: list[list[object]] = []
        for c in UniverseService._candles_sorted_unique(candles):
            ts = parse_any_ts(c[0])
            if ts is None:
                continue
            if ts.astimezone(IST) <= cutoff:
                out.append(c)
        return out

    @staticmethod
    def _rolling_atr_pct_series(candles: list[list[object]], period: int = 14) -> list[float]:
        norm = UniverseService._candles_sorted_unique(candles)
        if len(norm) < 2 or period <= 0:
            return []
        tr: list[float] = []
        for i in range(1, len(norm)):
            h = float(norm[i][2] or 0.0)
            l = float(norm[i][3] or 0.0)
            prev_close = float(norm[i - 1][4] or 0.0)
            tr.append(max(h - l, abs(h - prev_close), abs(l - prev_close)))
        if len(tr) < period:
            return []
        out: list[float] = []
        atr = sum(tr[:period]) / float(period)
        close = float(norm[period][4] or 0.0)
        if close > 0:
            out.append(float(atr / close))
        for i in range(period, len(tr)):
            atr = ((atr * float(period - 1)) + tr[i]) / float(period)
            close = float(norm[i + 1][4] or 0.0)
            if close > 0:
                out.append(float(atr / close))
        return out

    @staticmethod
    def _weighted_median(values: list[float], weights: list[float]) -> float:
        if not values or len(values) != len(weights):
            return 0.0
        pairs = sorted([(float(v), max(0.0, float(w))) for v, w in zip(values, weights)], key=lambda x: x[0])
        total = sum(w for _, w in pairs)
        if total <= 0:
            return 0.0
        run = 0.0
        half = total / 2.0
        for v, w in pairs:
            run += w
            if run >= half:
                return float(v)
        return float(pairs[-1][0])

    @staticmethod
    def _run_time_block(now_i: datetime, *, premarket: bool) -> str:
        if premarket:
            return "PREMARKET"
        t = now_i.astimezone(IST).time()
        hm = t.hour * 60 + t.minute
        if 9 * 60 + 30 <= hm <= 10 * 60 + 30:
            return "INTRA_5M"
        if 10 * 60 + 45 <= hm <= 13 * 60:
            return "INTRA_15M"
        if hm == (14 * 60 + 45):
            return "INTRA_FINAL"
        return "INTRA_ADHOC"

    def _watchlist_index_daily_cache_path(self, instrument_key: str) -> str:
        safe = self._safe_key_fragment(instrument_key)
        return f"cache/watchlist_v2/index_daily/{safe}.json"

    def _watchlist_index_intraday_cache_path(self, instrument_key: str, timeframe: str) -> str:
        safe = self._safe_key_fragment(instrument_key)
        tf = str(timeframe or "5m").strip().lower()
        return f"cache/watchlist_v2/index_intraday/{tf}/{safe}.json"

    def _fetch_index_daily_proxy(self, expected_lcd: str, *, allow_live_api: bool = True) -> tuple[list[list[object]], str, str]:
        keys = [
            str(self.upstox.settings.nifty50_instrument_key or "").strip(),
            "NSE_INDEX|Nifty 500",
            "NSE_INDEX|Nifty Bank",
        ]
        best_candles: list[list[object]] = []
        best_key = ""
        best_source = "cache_fallback"
        for idx, key in enumerate([k for k in keys if k]):
            path = self._watchlist_index_daily_cache_path(key)
            cached = self.gcs.read_candles(path)
            merged = cached
            source = "cache_only"
            if allow_live_api:
                try:
                    api = self._fetch_daily_candles_incremental(key, cached, lookback_days=9500)
                    if api:
                        merged = self.gcs.merge_candles(path, api)
                        source = "upstox_api"
                except Exception:
                    source = "cache_fallback_error"
                    logger.warning("watchlist_v2 index daily fetch failed key=%s", key, exc_info=True)
            else:
                # Premarket deterministic mode: never do live incremental refresh.
                # If cache is missing or behind ExpectedLCD, sync deterministic history
                # bounded to ExpectedLCD.
                exp = self._parse_iso_date(expected_lcd)
                cached_last: date_cls | None = None
                if cached:
                    for row in reversed(cached):
                        if not row:
                            continue
                        ts = parse_any_ts(row[0])
                        if ts is None:
                            continue
                        cached_last = ts.astimezone(IST).date()
                        break
                needs_sync = (not cached) or (exp is not None and (cached_last is None or cached_last < exp))
                if needs_sync:
                    try:
                        api = self._fetch_daily_candles_expected_lcd_bounded(
                            key,
                            expected_lcd=expected_lcd,
                            lookback_days=9500,
                        )
                        if api:
                            merged = self.gcs.merge_candles(path, api)
                            source = "upstox_api_expectedlcd_sync"
                        else:
                            source = "cache_missing_expectedlcd_fetch_empty"
                    except Exception:
                        source = "cache_fallback_error"
                        logger.warning("watchlist_v2 index daily expectedlcd fetch failed key=%s", key, exc_info=True)
            filtered = self._daily_no_lookahead(merged, expected_lcd)
            if len(filtered) > len(best_candles):
                best_candles = filtered
                best_key = key
                best_source = source if idx == 0 else f"fallback_{source}"
            if idx == 0 and len(filtered) >= 220:
                return filtered, key, source
        return best_candles, best_key, best_source

    def _fetch_index_intraday_proxy(self, *, timeframe: str, now_i: datetime, allow_live_api: bool = True) -> tuple[list[list[object]], str, str]:
        keys = [
            str(self.upstox.settings.nifty50_instrument_key or "").strip(),
            "NSE_INDEX|Nifty 500",
            "NSE_INDEX|Nifty Bank",
        ]
        interval = 5 if str(timeframe).strip().lower() == "5m" else 15
        best_candles: list[list[object]] = []
        best_key = ""
        best_source = "cache_fallback"
        for idx, key in enumerate([k for k in keys if k]):
            path = self._watchlist_index_intraday_cache_path(key, timeframe)
            cached = self.gcs.read_candles(path)
            merged = cached
            source = "cache_only"
            if allow_live_api:
                try:
                    api = self.upstox.get_intraday_candles_v3(key, unit="minutes", interval=interval)
                    if api:
                        merged = self.gcs.merge_candles(path, api)
                        source = "upstox_api"
                except Exception:
                    source = "cache_fallback_error"
                    logger.warning("watchlist_v2 index intraday fetch failed key=%s", key, exc_info=True)
            completed = self._completed_intraday_bars(merged, now_i, interval_min=interval)
            if len(completed) > len(best_candles):
                best_candles = completed
                best_key = key
                best_source = source if idx == 0 else f"fallback_{source}"
            if idx == 0 and len(completed) >= 12:
                return completed, key, source
        return best_candles, best_key, best_source

    def _build_watchlist_v2_regime(
        self,
        *,
        timeframe: str,
        expected_lcd: str,
        now_i: datetime,
        premarket: bool = False,
    ) -> dict[str, Any]:
        daily, daily_key, daily_source = self._fetch_index_daily_proxy(
            expected_lcd,
            allow_live_api=not bool(premarket),
        )
        if premarket:
            intra: list[list[object]] = []
            intra_key = ""
            intra_source = "premarket_skip"
        else:
            intra, intra_key, intra_source = self._fetch_index_intraday_proxy(
                timeframe=timeframe,
                now_i=now_i,
                allow_live_api=True,
            )

        closes = [float(c[4] or 0.0) for c in daily if len(c) >= 6]
        close = float(closes[-1]) if closes else 0.0
        ema50 = self._ema_last(closes, 50)
        ema200 = self._ema_last(closes, 200)
        atr14 = float(calc_atr(daily[-260:], period=14) or 0.0) if len(daily) >= 20 else 0.0
        atr_pct = float((atr14 / close) if close > 0 else 0.0)
        atr_pct_series = self._rolling_atr_pct_series(daily[-320:], period=14)
        atr_median_252 = float(statistics.median(atr_pct_series[-252:])) if atr_pct_series else 0.0

        trend_up = bool(close > ema200 and ema50 > ema200)
        trend_down = bool(close < ema200 and ema50 < ema200)
        high_vol = bool(atr_median_252 > 0 and atr_pct > (1.3 * atr_median_252))
        low_vol = bool(atr_median_252 > 0 and atr_pct < (0.8 * atr_median_252))

        if trend_up and not high_vol:
            regime_daily = "TREND"
        elif (not trend_up) and (not trend_down):
            regime_daily = "RANGE"
        else:
            regime_daily = "RISK_OFF"

        today = now_i.astimezone(IST).date()
        today_intra = [c for c in intra if (parse_any_ts(c[0]) or now_i).astimezone(IST).date() == today]
        vwap_series: list[float] = []
        cum_pv = 0.0
        cum_vol = 0.0
        for c in today_intra:
            h = float(c[2] or 0.0)
            l = float(c[3] or 0.0)
            cl = float(c[4] or 0.0)
            vol = float(c[5] or 0.0)
            tp = (h + l + cl) / 3.0
            cum_pv += tp * max(0.0, vol)
            cum_vol += max(0.0, vol)
            vwap_series.append((cum_pv / cum_vol) if cum_vol > 0 else cl)

        if len(vwap_series) >= 4:
            base_vwap = max(1e-9, abs(vwap_series[-4]))
            vwap_slope = (vwap_series[-1] - vwap_series[-4]) / base_vwap
        else:
            vwap_slope = 0.0

        if len(today_intra) >= 12:
            last6 = today_intra[-6:]
            prev6 = today_intra[-12:-6]
            range_last = max(float(c[2] or 0.0) for c in last6) - min(float(c[3] or 0.0) for c in last6)
            range_prev = max(float(c[2] or 0.0) for c in prev6) - min(float(c[3] or 0.0) for c in prev6)
        elif len(today_intra) >= 6:
            last6 = today_intra[-6:]
            range_last = max(float(c[2] or 0.0) for c in last6) - min(float(c[3] or 0.0) for c in last6)
            range_prev = range_last
        else:
            range_last = 0.0
            range_prev = 0.0
        range_expansion = float(range_last / max(1e-9, range_prev))

        strong_slope = abs(vwap_slope) >= 0.0008
        persistent_expansion = range_expansion >= 1.15 and range_last > 0
        regime_intraday = "TRENDY" if (strong_slope and persistent_expansion) else "CHOPPY"

        return {
            "regimeDaily": regime_daily,
            "regimeIntraday": regime_intraday,
            "daily": {
                "close": close,
                "ema50": ema50,
                "ema200": ema200,
                "atr14": atr14,
                "atrPct": atr_pct,
                "atrMedian252": atr_median_252,
                "trendUp": trend_up,
                "trendDown": trend_down,
                "highVol": high_vol,
                "lowVol": low_vol,
            },
            "intraday": {
                "vwapSlope": float(vwap_slope),
                "rangeExpansion30m": float(range_expansion),
                "bars": len(today_intra),
            },
            "source": {
                "dailyKey": daily_key,
                "dailySource": daily_source,
                "intradayKey": intra_key,
                "intradaySource": intra_source,
            },
        }

    def _watchlist_v2_candidates(self, expected_lcd: str) -> list[dict[str, Any]]:
        header_map = self.sheets.ensure_sheet_headers_append(SheetNames.UNIVERSE, UNIVERSE_V2_HEADERS, header_row=3)
        rows = self.sheets.read_sheet_rows(SheetNames.UNIVERSE, 4)
        col = {k: int(v) for k, v in header_map.items()}
        out: list[dict[str, Any]] = []
        for row in rows:
            symbol = row[col["Symbol"] - 1].strip().upper() if len(row) >= col["Symbol"] else ""
            if not symbol:
                continue
            exchange = row[col["Exchange"] - 1].strip().upper() if len(row) >= col["Exchange"] else "NSE"
            segment = row[col["Segment"] - 1].strip().upper() if len(row) >= col["Segment"] else "CASH"
            enabled = row[col["Enabled"] - 1].strip().upper() if len(row) >= col["Enabled"] else "Y"
            instrument_key = row[col["Instrument Key"] - 1].strip() if len(row) >= col["Instrument Key"] else ""
            eligible_swing = row[col["Eligible Swing"] - 1].strip().upper() == "Y" if len(row) >= col["Eligible Swing"] else False
            eligible_intraday = row[col["Eligible Intraday"] - 1].strip().upper() == "Y" if len(row) >= col["Eligible Intraday"] else False
            turnover_rank = row[col["Turnover Rank 60D"] - 1].strip() if len(row) >= col["Turnover Rank 60D"] else ""
            liquidity_bucket = row[col["Liquidity Bucket"] - 1].strip().upper() if len(row) >= col["Liquidity Bucket"] else ""
            atr_pct = row[col["ATR Pct 14D"] - 1].strip() if len(row) >= col["ATR Pct 14D"] else ""
            gap_risk = row[col["Gap Risk 60D"] - 1].strip() if len(row) >= col["Gap Risk 60D"] else ""
            price_last = row[col["Price Last"] - 1].strip() if len(row) >= col["Price Last"] else ""
            bars_1d = row[col["Bars 1D"] - 1].strip() if len(row) >= col["Bars 1D"] else ""
            last_1d_date = row[col["Last 1D Date"] - 1].strip() if len(row) >= col["Last 1D Date"] else ""
            disable_reason = row[col["Disable Reason"] - 1].strip() if len(row) >= col["Disable Reason"] else ""
            eligible_swing_raw = row[col["Eligible Swing"] - 1].strip() if len(row) >= col["Eligible Swing"] else ""
            eligible_intraday_raw = row[col["Eligible Intraday"] - 1].strip() if len(row) >= col["Eligible Intraday"] else ""
            decision_present = bool(eligible_swing_raw) or bool(eligible_intraday_raw) or bool(disable_reason)
            fresh = bool(last_1d_date and last_1d_date >= expected_lcd)
            out.append(
                {
                    "symbol": symbol,
                    "exchange": exchange,
                    "segment": segment,
                    "enabled": enabled == "Y",
                    "instrumentKey": instrument_key,
                    "eligibleSwing": eligible_swing,
                    "eligibleIntraday": eligible_intraday,
                    "turnoverRank60D": int(float(turnover_rank)) if turnover_rank else None,
                    "liquidityBucket": liquidity_bucket,
                    "atrPct14D": float(atr_pct) if atr_pct else 0.0,
                    "gapRisk60D": float(gap_risk) if gap_risk else 0.0,
                    "priceLast": float(price_last) if price_last else 0.0,
                    "bars1D": int(float(bars_1d)) if bars_1d else 0,
                    "last1DDate": last_1d_date,
                    "fresh": fresh,
                    "disableReason": disable_reason,
                    "decisionPresent": decision_present,
                }
            )
        return out

    def _watchlist_daily_candles(self, row: dict[str, Any], expected_lcd: str) -> list[list[object]]:
        _, candles = self._read_score_cache_with_migration(
            str(row.get("symbol") or ""),
            str(row.get("exchange") or "NSE"),
            str(row.get("segment") or "CASH"),
            str(row.get("instrumentKey") or ""),
        )
        return self._daily_no_lookahead(candles, expected_lcd)

    def _watchlist_intraday_candles(
        self,
        row: dict[str, Any],
        *,
        timeframe: str,
        now_i: datetime,
    ) -> list[list[object]]:
        symbol = str(row.get("symbol") or "").upper()
        exchange = str(row.get("exchange") or "NSE").upper()
        segment = str(row.get("segment") or "CASH").upper()
        key = str(row.get("instrumentKey") or "").strip()
        tf = str(timeframe or "5m").strip().lower()
        interval = 5 if tf == "5m" else 15
        path = self.gcs.candle_cache_path(symbol, exchange, segment, tf)
        cached = self.gcs.read_candles(path)
        merged = cached
        if key:
            try:
                api = self.upstox.get_intraday_candles_v3(key, unit="minutes", interval=interval)
                if api:
                    merged = self.gcs.merge_candles(path, api)
            except Exception:
                logger.warning("watchlist_v2 symbol intraday fetch failed symbol=%s key=%s", symbol, key, exc_info=True)
        return self._completed_intraday_bars(merged, now_i, interval_min=interval)

    def _watchlist_volume_shock(self, bars: list[list[object]], now_i: datetime) -> tuple[float, float]:
        if not bars:
            return 1.0, 1.0
        today = now_i.astimezone(IST).date()
        ts_last = parse_any_ts(bars[-1][0])
        if ts_last is None:
            return 1.0, 1.0
        slot = ts_last.astimezone(IST).strftime("%H:%M")
        current_vol = float(bars[-1][5] or 0.0)
        per_day: dict[str, float] = {}
        for c in bars:
            ts = parse_any_ts(c[0])
            if ts is None:
                continue
            ti = ts.astimezone(IST)
            if ti.date() >= today:
                continue
            if ti.strftime("%H:%M") != slot:
                continue
            per_day[ti.date().isoformat()] = float(c[5] or 0.0)
        ordered_days = sorted(per_day.keys(), reverse=True)[:60]
        vals: list[float] = []
        weights: list[float] = []
        for i, d in enumerate(ordered_days):
            vals.append(float(per_day[d]))
            weights.append(2.0 if i < 20 else 1.0)
        baseline = self._weighted_median(vals, weights) if vals else 0.0
        if baseline <= 0:
            fallback_vals = [float(c[5] or 0.0) for c in bars[:-1] if float(c[5] or 0.0) > 0]
            baseline = float(statistics.median(fallback_vals)) if fallback_vals else 0.0
        if baseline <= 0:
            return 1.0, 1.0
        ratio = float(current_vol / baseline) if baseline > 0 else 1.0
        component = self._norm_minmax_clip(ratio, 0.5, 3.0)
        return ratio, component

    def _watchlist_orb_signal(self, bars: list[list[object]], now_i: datetime) -> tuple[str, float]:
        today = now_i.astimezone(IST).date()
        today_bars = []
        for c in bars:
            ts = parse_any_ts(c[0])
            if ts is None:
                continue
            ti = ts.astimezone(IST)
            if ti.date() != today:
                continue
            today_bars.append((ti, c))
        if not today_bars:
            return "NONE", 0.0
        first_15 = []
        for ti, c in today_bars:
            hm = ti.hour * 60 + ti.minute
            if 9 * 60 + 15 <= hm <= 9 * 60 + 30:
                first_15.append(c)
        if len(first_15) < 3:
            return "NONE", 0.0
        orb_high = max(float(c[2] or 0.0) for c in first_15)
        orb_low = min(float(c[3] or 0.0) for c in first_15)
        close_now = float(today_bars[-1][1][4] or 0.0)
        if close_now > orb_high:
            return "UP_BREAK", 1.0
        if close_now < orb_low:
            return "DOWN_BREAK", 1.0
        return "NONE", 0.0

    def _watchlist_reversal_signal(self, bars: list[list[object]], regime_intraday: str, now_i: datetime) -> tuple[float, float]:
        if regime_intraday != "CHOPPY" or len(bars) < 3:
            return 0.0, 0.0
        today = []
        now_date = now_i.astimezone(IST).date()
        for c in bars:
            ts = parse_any_ts(c[0])
            if ts is None:
                continue
            if ts.astimezone(IST).date() == now_date:
                today.append(c)
        series = today if len(today) >= 3 else bars[-30:]
        if len(series) < 3:
            return 0.0, 0.0
        last = series[-1]
        prev = series[-2]
        vwap_series: list[float] = []
        cum_pv = 0.0
        cum_v = 0.0
        for c in series:
            h = float(c[2] or 0.0)
            l = float(c[3] or 0.0)
            cl = float(c[4] or 0.0)
            v = float(c[5] or 0.0)
            tp = (h + l + cl) / 3.0
            cum_pv += tp * max(0.0, v)
            cum_v += max(0.0, v)
            vwap_series.append((cum_pv / cum_v) if cum_v > 0 else cl)
        close_now = float(last[4] or 0.0)
        vwap_now = float(vwap_series[-1] if vwap_series else close_now)
        atr = float(calc_atr(series[-30:], period=14) or 0.0) if len(series) >= 15 else max(0.01, abs(float(last[2] or 0.0) - float(last[3] or 0.0)))
        extension_raw = abs(close_now - vwap_now) / max(1e-6, atr)
        extension_component = self._norm_minmax_clip(extension_raw, 0.2, 2.0)

        lo = float(last[3] or 0.0)
        hi = float(last[2] or 0.0)
        op = float(last[1] or 0.0)
        cl = float(last[4] or 0.0)
        op_prev = float(prev[1] or 0.0)
        cl_prev = float(prev[4] or 0.0)
        rng = max(1e-6, hi - lo)
        body = abs(cl - op)
        last_dir = 1 if cl > op else (-1 if cl < op else 0)
        prev_dir = 1 if cl_prev > op_prev else (-1 if cl_prev < op_prev else 0)
        reversal_signal = 1.0 if (extension_component >= 0.3 and body / rng >= 0.4 and last_dir != 0 and prev_dir != 0 and last_dir != prev_dir) else 0.0
        return extension_component, reversal_signal

    @staticmethod
    def _merge_intraday_v2(
        phase2: list[dict[str, Any]],
        phase1: list[dict[str, Any]],
        target: int,
    ) -> list[dict[str, Any]]:
        ordered_phase2 = sorted(phase2, key=lambda x: (-float(x.get("score", 0.0)), str(x.get("symbol", ""))))
        ordered_phase1 = sorted(phase1, key=lambda x: (-float(x.get("score", 0.0)), str(x.get("symbol", ""))))
        picked: list[dict[str, Any]] = []
        seen: set[str] = set()
        for r in ordered_phase2:
            if len(picked) >= target:
                break
            sym = str(r.get("symbol", ""))
            if not sym or sym in seen:
                continue
            seen.add(sym)
            picked.append(r)
        for r in ordered_phase1:
            if len(picked) >= target:
                break
            sym = str(r.get("symbol", ""))
            if not sym or sym in seen:
                continue
            seen.add(sym)
            picked.append(r)
        return picked[:target]

    def build_watchlist(
        self,
        regime: RegimeSnapshot | None,
        target_size: int = 150,
        *,
        min_score: int = 1,
        require_today_scored: bool = False,
        require_full_coverage: bool = False,
        premarket: bool = False,
        intraday_timeframe: str = "5m",
    ) -> dict[str, object]:
        del regime
        now_i = now_ist()
        expected_lcd = self._expected_latest_daily_candle_date(now_i).strftime("%Y-%m-%d")
        run_ts = now_ist_str()
        run_date = now_i.astimezone(IST).strftime("%Y-%m-%d")
        run_block = self._run_time_block(now_i, premarket=premarket)
        timeframe = "5m" if str(intraday_timeframe or "").strip().lower() != "15m" else "15m"

        controls = self._build_universe_v2_controls()
        mode_thresholds = controls.active_thresholds()

        all_rows = self._watchlist_v2_candidates(expected_lcd)
        enabled_rows = [r for r in all_rows if bool(r.get("enabled"))]
        classified_rows = [r for r in enabled_rows if bool(r.get("decisionPresent"))]
        today_classified_rows = [r for r in classified_rows if bool(r.get("fresh"))]
        total_enabled = len(enabled_rows)
        classified = len(classified_rows)
        today_classified = len(today_classified_rows)
        coverage_v2 = {
            "total": total_enabled,
            "scored": classified,
            "todayScored": today_classified,
            "missing": max(0, total_enabled - classified),
            "todayMissing": max(0, total_enabled - today_classified),
            "coveragePct": round((classified * 100.0 / total_enabled), 2) if total_enabled else 100.0,
            "todayCoveragePct": round((today_classified * 100.0 / total_enabled), 2) if total_enabled else 100.0,
            "full": classified >= total_enabled,
            "todayFull": today_classified >= total_enabled,
        }
        if require_full_coverage:
            coverage_key = "todayFull" if require_today_scored else "full"
            if not bool(coverage_v2.get(coverage_key)):
                reason = "today_score_coverage_incomplete" if require_today_scored else "score_coverage_incomplete"
                logger.info(
                    "build_watchlist_v2 blocked reason=%s coverageKey=%s total=%s scored=%s todayScored=%s",
                    reason,
                    coverage_key,
                    coverage_v2.get("total"),
                    coverage_v2.get("scored"),
                    coverage_v2.get("todayScored"),
                )
                return {
                    "selected": 0,
                    "swingSelected": 0,
                    "intradaySelected": 0,
                    "coverage": {
                        **coverage_v2,
                        "totalUniverseRows": len(all_rows),
                        "swingCandidates": 0,
                        "intradayCandidates": 0,
                        "phase1Candidates": 0,
                        "phase2Candidates": 0,
                        "runTimeBlock": run_block,
                        "timeframe": timeframe,
                        "expectedLCD": expected_lcd,
                    },
                    "ready": False,
                    "reason": reason,
                    "eligiblePool": 0,
                    "regimeV2": {},
                }

        regime_v2 = self._build_watchlist_v2_regime(
            timeframe=timeframe,
            expected_lcd=expected_lcd,
            now_i=now_i,
            premarket=bool(premarket),
        )
        swing_candidates = [r for r in all_rows if r.get("enabled") and r.get("eligibleSwing") and r.get("fresh")]
        intraday_candidates = [
            r
            for r in all_rows
            if r.get("enabled")
            and r.get("eligibleIntraday")
            and r.get("fresh")
            and (r.get("turnoverRank60D") is not None)
            and int(r.get("turnoverRank60D") or 0) <= int(mode_thresholds.intraday_topn_turnover_60d)
        ]

        swing_work: list[dict[str, Any]] = []
        ret60_values: list[float] = []
        for r in swing_candidates:
            daily = self._watchlist_daily_candles(r, expected_lcd)
            if len(daily) < 60:
                continue
            closes = [float(c[4] or 0.0) for c in daily]
            vols = [float(c[5] or 0.0) for c in daily]
            high_20 = max(float(c[2] or 0.0) for c in daily[-20:])
            low_20 = min(float(c[3] or 0.0) for c in daily[-20:])
            close = float(closes[-1] if closes else 0.0)
            ret_20 = ((close / closes[-21]) - 1.0) if len(closes) >= 21 and closes[-21] > 0 else 0.0
            ret_60 = ((close / closes[-61]) - 1.0) if len(closes) >= 61 and closes[-61] > 0 else ret_20
            ret60_values.append(ret_60)
            vol_med20 = float(statistics.median(vols[-20:])) if vols[-20:] else 0.0
            vol_ratio = float((vols[-1] / vol_med20) if vol_med20 > 0 else 1.0)
            ema20 = self._ema_last(closes, 20)
            ema50 = self._ema_last(closes, 50)
            ema200 = self._ema_last(closes, 200)
            ema50_prev = self._ema_last(closes[:-20], 50) if len(closes) > 70 else ema50
            atr14 = float(calc_atr(daily[-260:], period=14) or 0.0) if len(daily) >= 20 else 0.0
            swing_work.append(
                {
                    **r,
                    "close": close,
                    "ret20": ret_20,
                    "ret60": ret_60,
                    "volRatio": vol_ratio,
                    "ema20": ema20,
                    "ema50": ema50,
                    "ema200": ema200,
                    "ema50Prev": ema50_prev,
                    "atr14": atr14,
                    "high20": high_20,
                    "low20": low_20,
                }
            )

        ret_mean = float(statistics.mean(ret60_values)) if ret60_values else 0.0
        ret_std = float(statistics.pstdev(ret60_values)) if len(ret60_values) > 1 else 0.0

        swing_scored: list[dict[str, Any]] = []
        for r in swing_work:
            close = float(r.get("close") or 0.0)
            if close <= 0:
                continue
            z = ((float(r.get("ret60") or 0.0) - ret_mean) / ret_std) if ret_std > 1e-9 else 0.0
            rs_component = self._norm_minmax_clip(max(-3.0, min(3.0, z)), -3.0, 3.0)
            high20 = float(r.get("high20") or 0.0)
            low20 = float(r.get("low20") or 0.0)
            dist_to_high = ((high20 - close) / high20) if high20 > 0 else 0.0
            breakout_component = max(0.0, 1.0 - (dist_to_high * 5.0))
            volume_component = min(2.0, float(r.get("volRatio") or 0.0)) / 2.0
            trend_component = 1.0 if (close > float(r.get("ema50") or 0.0) > float(r.get("ema200") or 0.0)) else 0.0
            breakout = self._clip01((0.35 * rs_component) + (0.30 * breakout_component) + (0.20 * volume_component) + (0.15 * trend_component)) * 100.0

            atr14 = float(r.get("atr14") or 0.0)
            ema20 = float(r.get("ema20") or 0.0)
            ema50 = float(r.get("ema50") or 0.0)
            ema200 = float(r.get("ema200") or 0.0)
            ema50_prev = float(r.get("ema50Prev") or ema50)
            atr_pct = float((atr14 / close) if close > 0 else 0.0)
            if ema50 > ema200 and atr14 > 0:
                pullback_depth = (ema20 - close) / atr14
                pullback_component = self._clip01(pullback_depth / 2.0)
                slope_per_bar = (ema50 - ema50_prev) / 20.0
                denom = max(1e-6, atr_pct)
                trend_strength = self._clip01(slope_per_bar / (denom * close))
            else:
                pullback_component = 0.0
                trend_strength = 0.0
            volume_contraction = 1.0 if float(r.get("volRatio") or 0.0) < 1.0 else 0.0
            pullback = self._clip01((0.40 * trend_strength) + (0.40 * pullback_component) + (0.20 * volume_contraction)) * 100.0

            range_den = max(1e-6, high20 - low20)
            range_pos = (close - low20) / range_den
            mr_component = self._clip01(1.0 - range_pos)
            vol_sanity = 1.0 if 0.01 <= float(r.get("atrPct14D") or 0.0) <= 0.035 else 0.0
            mean_rev = self._clip01((0.60 * mr_component) + (0.40 * vol_sanity)) * 100.0

            if regime_v2["regimeDaily"] == "TREND":
                final_score = (0.60 * breakout) + (0.30 * pullback) + (0.10 * mean_rev)
            elif regime_v2["regimeDaily"] == "RANGE":
                final_score = (0.60 * mean_rev) + (0.25 * pullback) + (0.15 * breakout)
            else:
                final_score = (0.50 * breakout) + (0.20 * pullback) + (0.30 * mean_rev)

            setup_scores = {
                "BREAKOUT": breakout,
                "PULLBACK": pullback,
                "MEAN_REVERSION": mean_rev,
            }
            setup_label = max(setup_scores.items(), key=lambda kv: kv[1])[0]
            swing_scored.append(
                {
                    **r,
                    "score": float(round(max(0.0, min(100.0, final_score)), 2)),
                    "setupLabel": setup_label,
                    "breakout": breakout,
                    "pullback": pullback,
                    "meanRev": mean_rev,
                }
            )

        swing_scored = [r for r in swing_scored if float(r.get("score") or 0.0) >= float(max(1, int(min_score)))]
        if regime_v2["regimeDaily"] == "RISK_OFF":
            swing_scored = [r for r in swing_scored if str(r.get("liquidityBucket") or "").upper() == "A"]
            swing_target = max(1, math.floor(int(target_size or 150) * 0.7))
        else:
            swing_target = max(1, int(target_size or 150))
        swing_target = min(swing_target, 150)
        swing_selected = sorted(swing_scored, key=lambda x: (-float(x["score"]), str(x["symbol"])))[:swing_target]

        intraday_phase1: list[dict[str, Any]] = []
        for r in intraday_candidates:
            daily = self._watchlist_daily_candles(r, expected_lcd)
            if len(daily) < 21:
                continue
            closes = [float(c[4] or 0.0) for c in daily]
            close = float(closes[-1] if closes else 0.0)
            if close <= 0:
                continue
            ret20 = ((close / closes[-21]) - 1.0) if closes[-21] > 0 else 0.0
            momentum_component = self._norm_minmax_clip(ret20, -0.20, 0.20)
            rank = int(r.get("turnoverRank60D") or mode_thresholds.intraday_topn_turnover_60d)
            liquidity_component = 1.0 - self._norm_minmax_clip(float(rank), 1.0, float(max(2, mode_thresholds.intraday_topn_turnover_60d)))
            vol_sanity_component = 1.0 - self._norm_minmax_clip(float(r.get("atrPct14D") or 0.0), 0.01, 0.12)
            phase1_score = self._clip01((0.40 * momentum_component) + (0.30 * liquidity_component) + (0.30 * vol_sanity_component)) * 100.0
            intraday_phase1.append(
                {
                    **r,
                    "score": float(round(phase1_score, 2)),
                    "setupLabel": "PHASE1_MOMENTUM",
                    "momentumComponent": momentum_component,
                    "liquidityComponent": liquidity_component,
                    "volSanityComponent": vol_sanity_component,
                }
            )

        intraday_phase1 = [r for r in intraday_phase1 if float(r.get("score") or 0.0) >= float(max(1, int(min_score)))]

        intraday_phase2: list[dict[str, Any]] = []
        if not premarket:
            for r in intraday_phase1:
                bars = self._watchlist_intraday_candles(r, timeframe=timeframe, now_i=now_i)
                today_bars = []
                for c in bars:
                    ts = parse_any_ts(c[0])
                    if ts is None:
                        continue
                    if ts.astimezone(IST).date() == now_i.astimezone(IST).date():
                        today_bars.append(c)
                if len(today_bars) < 4:
                    continue

                vwap_series: list[float] = []
                cum_pv = 0.0
                cum_v = 0.0
                for c in today_bars:
                    h = float(c[2] or 0.0)
                    l = float(c[3] or 0.0)
                    cl = float(c[4] or 0.0)
                    v = float(c[5] or 0.0)
                    tp = (h + l + cl) / 3.0
                    cum_pv += tp * max(0.0, v)
                    cum_v += max(0.0, v)
                    vwap_series.append((cum_pv / cum_v) if cum_v > 0 else cl)
                close_now = float(today_bars[-1][4] or 0.0)
                vwap_now = float(vwap_series[-1] if vwap_series else close_now)

                base_vwap = max(1e-9, abs(vwap_series[-4])) if len(vwap_series) >= 4 else max(1e-9, abs(vwap_now))
                vwap_slope_raw = abs((vwap_series[-1] - vwap_series[-4]) / base_vwap) if len(vwap_series) >= 4 else 0.0
                vwap_slope_component = self._norm_minmax_clip(vwap_slope_raw, 0.0, 0.005)

                vol_ratio, volume_shock_component = self._watchlist_volume_shock(bars, now_i)
                orb_label, orb_component = self._watchlist_orb_signal(today_bars, now_i)
                trend_score = self._clip01((0.40 * vwap_slope_component) + (0.35 * volume_shock_component) + (0.25 * orb_component))

                ext_component, reversal_signal = self._watchlist_reversal_signal(today_bars, regime_v2["regimeIntraday"], now_i)
                reversal_score = self._clip01((0.50 * ext_component) + (0.30 * reversal_signal) + (0.20 * float(r.get("liquidityComponent") or 0.0)))
                if regime_v2["regimeIntraday"] != "CHOPPY":
                    reversal_score = 0.0

                phase2_score = max(trend_score, reversal_score) * 100.0
                setup_label = "VWAP_TREND" if trend_score >= reversal_score else "VWAP_REVERSAL"
                intraday_phase2.append(
                    {
                        **r,
                        "score": float(round(phase2_score, 2)),
                        "source": "PHASE2_INPLAY",
                        "setupLabel": setup_label,
                        "vwapBias": ("ABOVE" if close_now > vwap_now else ("BELOW" if close_now < vwap_now else "FLAT")),
                        "volumeShock": float(round(vol_ratio, 4)),
                        "orbSignal": orb_label,
                        "reversalSignal": float(round(reversal_signal, 4)),
                        "confidence": float(round(phase2_score, 2)),
                    }
                )

        intraday_target = min(150, max(1, int(target_size or 150)))
        if premarket:
            intraday_selected = sorted(intraday_phase1, key=lambda x: (-float(x["score"]), str(x["symbol"])))[:intraday_target]
            for r in intraday_selected:
                r["source"] = "PHASE1_DAILY_FALLBACK"
                r["vwapBias"] = "N/A"
                r["volumeShock"] = 1.0
                r["orbSignal"] = "N/A"
                r["reversalSignal"] = 0.0
                r["confidence"] = float(r.get("score") or 0.0)
        else:
            intraday_selected = self._merge_intraday_v2(intraday_phase2, [{**r, "source": "PHASE1_DAILY_FALLBACK", "vwapBias": "N/A", "volumeShock": 1.0, "orbSignal": "N/A", "reversalSignal": 0.0, "confidence": float(r.get("score") or 0.0)} for r in intraday_phase1], intraday_target)

        swing_rows: list[list[object]] = []
        for i, r in enumerate(swing_selected, start=1):
            swing_rows.append(
                [
                    run_ts,
                    run_date,
                    i,
                    r.get("symbol", ""),
                    r.get("instrumentKey", ""),
                    r.get("exchange", "NSE"),
                    float(round(float(r.get("score") or 0.0), 2)),
                    r.get("setupLabel", ""),
                    regime_v2["regimeDaily"],
                    regime_v2["regimeIntraday"],
                    r.get("turnoverRank60D", ""),
                    r.get("liquidityBucket", ""),
                    float(round(float(r.get("atrPct14D") or 0.0), 6)),
                    float(round(float(r.get("gapRisk60D") or 0.0), 6)),
                    float(round(float(r.get("priceLast") or 0.0), 4)),
                    r.get("last1DDate", ""),
                    "Y",
                    "SELECTED_SWING_V2",
                    f"B={round(float(r.get('breakout') or 0.0),2)}|P={round(float(r.get('pullback') or 0.0),2)}|MR={round(float(r.get('meanRev') or 0.0),2)}",
                ]
            )

        intraday_rows: list[list[object]] = []
        for i, r in enumerate(intraday_selected, start=1):
            intraday_rows.append(
                [
                    run_ts,
                    run_date,
                    run_block,
                    i,
                    r.get("symbol", ""),
                    r.get("instrumentKey", ""),
                    r.get("exchange", "NSE"),
                    float(round(float(r.get("score") or 0.0), 2)),
                    r.get("source", "PHASE1_DAILY_FALLBACK"),
                    r.get("setupLabel", ""),
                    regime_v2["regimeDaily"],
                    regime_v2["regimeIntraday"],
                    r.get("vwapBias", "N/A"),
                    float(round(float(r.get("volumeShock") or 0.0), 4)),
                    r.get("orbSignal", "N/A"),
                    float(round(float(r.get("reversalSignal") or 0.0), 4)),
                    float(round(float(r.get("confidence") or 0.0), 2)),
                    r.get("turnoverRank60D", ""),
                    r.get("liquidityBucket", ""),
                    float(round(float(r.get("atrPct14D") or 0.0), 6)),
                    float(round(float(r.get("gapRisk60D") or 0.0), 6)),
                    float(round(float(r.get("priceLast") or 0.0), 4)),
                    r.get("last1DDate", ""),
                    "Y",
                    ("SELECTED_PHASE2_INPLAY" if str(r.get("source") or "").upper() == "PHASE2_INPLAY" else "SELECTED_PHASE1_FALLBACK"),
                    f"m={round(float(r.get('momentumComponent') or 0.0),3)}|liq={round(float(r.get('liquidityComponent') or 0.0),3)}|vol={round(float(r.get('volSanityComponent') or 0.0),3)}",
                ]
            )

        swing_written = bool(premarket)
        if swing_written:
            self.sheets.replace_watchlist_swing_v2(swing_rows)
        else:
            logger.info(
                "build_watchlist_v2 swing write skipped premarket=%s runBlock=%s",
                premarket,
                run_block,
            )
        self.sheets.replace_watchlist_intraday_v2(intraday_rows)

        out = {
            "selected": len(intraday_rows),
            "swingSelected": len(swing_rows) if swing_written else 0,
            "swingComputed": len(swing_rows),
            "intradaySelected": len(intraday_rows),
            "coverage": {
                **coverage_v2,
                "totalUniverseRows": len(all_rows),
                "swingCandidates": len(swing_candidates),
                "intradayCandidates": len(intraday_candidates),
                "phase1Candidates": len(intraday_phase1),
                "phase2Candidates": len(intraday_phase2),
                "runTimeBlock": run_block,
                "timeframe": timeframe,
                "expectedLCD": expected_lcd,
            },
            "ready": bool(len(swing_rows) > 0 or len(intraday_rows) > 0),
            "eligiblePool": len(intraday_candidates),
            "regimeV2": regime_v2,
        }
        logger.info(
            "build_watchlist_v2 complete swingSelected=%s intradaySelected=%s swingCandidates=%s intradayCandidates=%s phase1=%s phase2=%s runBlock=%s regimeDaily=%s regimeIntraday=%s expectedLCD=%s",
            len(swing_rows),
            len(intraday_rows),
            len(swing_candidates),
            len(intraday_candidates),
            len(intraday_phase1),
            len(intraday_phase2),
            run_block,
            regime_v2["regimeDaily"],
            regime_v2["regimeIntraday"],
            expected_lcd,
        )
        return out

    def run_premarket_pipeline(self, regime: RegimeSnapshot, target_size: int = 150) -> UniversePipelineResult:
        score_out = self.score_universe_batch(regime, api_cap=120, fresh_hours=12, sheet_write_batch_size=200)
        wl_out = self.build_watchlist(regime, target_size=target_size, premarket=True)
        cov = wl_out.get("coverage", {}) if isinstance(wl_out, dict) else {}
        return UniversePipelineResult(
            synced=0,
            scored=int(score_out["scored"]),
            selected=int(wl_out.get("selected", 0)),
            coverage_pct=float(cov.get("intradayCandidates", 0) or 0.0),
        )
