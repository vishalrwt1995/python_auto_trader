from __future__ import annotations

import logging
import random
import ssl
import time
from dataclasses import dataclass
from typing import Any, Iterable

from autotrader.domain.models import RegimeSnapshot, UniverseRow, WatchlistRow
from autotrader.time_utils import now_ist_str

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SheetLayout:
    title: str
    tab_name: str
    headers: list[str]


class SheetNames:
    CONFIG = "⚙️ Config"
    WATCHLIST = "📋 Watchlist"
    WATCHLIST_SWING_V2 = "Watchlist_Swing_V2"
    WATCHLIST_INTRADAY_V2 = "Watchlist_Intraday_V2"
    UNIVERSE = "🧾 Universe Instruments"
    CANDLE_CACHE = "🗄️ Candle Cache"
    SCORE_CACHE_1D = "History Candle 1D"
    SCORE_CACHE_5M = "History Candle 5m"
    SCORE_CACHE_1D_DATA = "📗 Score Cache 1D Data"
    BACKFILL = "📚 History Backfill"
    DECISIONS = "🧠 Decision Log"
    ACTIONS = "🧩 Project Log"
    MARKET = "🧠 Market Brain"
    SCAN = "📡 Live Scanner"
    SIGNALS = "🎯 Signals"
    ORDERS = "📦 Orders"
    POSITIONS = "💼 Positions"
    PNL = "💰 P&L Tracker"
    RISK = "🛡️ Risk Monitor"
    LOGS = "📝 Logs"


LEGACY_SHEET_NAMES: dict[str, str] = {
    "📘 Score Cache 1D": SheetNames.SCORE_CACHE_1D,
}


SHEET_LAYOUTS: dict[str, SheetLayout] = {
    SheetNames.WATCHLIST_SWING_V2: SheetLayout(
        title="Watchlist Swing V2 - regime-aware deterministic swing selections",
        tab_name=SheetNames.WATCHLIST_SWING_V2,
        headers=[
            "RunTS",
            "RunDate",
            "Rank",
            "Symbol",
            "InstrumentKey",
            "Exchange",
            "SwingScoreV2",
            "SetupLabel",
            "RegimeDaily",
            "RegimeIntraday",
            "TurnoverRank60D",
            "LiquidityBucket",
            "ATRPct14D",
            "GapRisk60D",
            "PriceLast",
            "Last1DDate",
            "Enabled",
            "Reason",
            "Notes",
        ],
    ),
    SheetNames.WATCHLIST_INTRADAY_V2: SheetLayout(
        title="Watchlist Intraday V2 - phase2 in-play + phase1 fallback",
        tab_name=SheetNames.WATCHLIST_INTRADAY_V2,
        headers=[
            "RunTS",
            "RunDate",
            "RunTimeBlock",
            "Rank",
            "Symbol",
            "InstrumentKey",
            "Exchange",
            "IntradayScoreV2",
            "Source",
            "SetupLabel",
            "RegimeDaily",
            "RegimeIntraday",
            "VWAPBias",
            "VolumeShock",
            "ORBSignal",
            "ReversalSignal",
            "Confidence",
            "TurnoverRank60D",
            "LiquidityBucket",
            "ATRPct14D",
            "GapRisk60D",
            "PriceLast",
            "Last1DDate",
            "Enabled",
            "Reason",
            "Notes",
        ],
    ),
    SheetNames.UNIVERSE: SheetLayout(
        title="Universe Instruments - Master list for smart watchlist generation",
        tab_name=SheetNames.UNIVERSE,
        headers=[
            "#", "Symbol", "Exchange", "Segment", "Allowed Product", "Strategy", "Sector", "Beta", "Enabled",
            "Priority", "Notes", "Score", "RSI", "Vol Ratio", "Last Scanned", "Last Product", "Last Strategy",
            "Last Note", "Raw CSV (JSON)", "Sector Source", "Sector Updated At",
            "Data Provider", "Instrument Key", "Source Segment", "Security Type", "Sc Calc",
        ],
    ),
    SheetNames.CANDLE_CACHE: SheetLayout(
        title="Candle Cache - persistent historical OHLCV",
        tab_name=SheetNames.CANDLE_CACHE,
        headers=["Symbol", "Exchange", "Segment", "Timeframe", "Timestamp", "Open", "High", "Low", "Close", "Volume", "Source", "Fetched At", "Raw Candle (JSON)"],
    ),
    SheetNames.SCORE_CACHE_1D: SheetLayout(
        title="Score Cache 1D - universe daily candle cache index",
        tab_name=SheetNames.SCORE_CACHE_1D,
        headers=["Symbol", "Exchange", "Segment", "Enabled", "Bars", "Last Candle Time", "Updated At", "Status", "API Calls (Run)", "Last Error", "File Name", "ISIN", "Notes", "GCS Path", "First Candle Date"],
    ),
    SheetNames.SCORE_CACHE_5M: SheetLayout(
        title="Score Cache 5m - intraday candle cache index",
        tab_name=SheetNames.SCORE_CACHE_5M,
        headers=["Symbol", "Exchange", "Segment", "Enabled", "Bars", "Last Candle Time", "Updated At", "Status", "API Calls (Run)", "Last Error", "File Name", "ISIN", "Notes", "GCS Path", "First Candle Date"],
    ),
    SheetNames.SCORE_CACHE_1D_DATA: SheetLayout(
        title="Score Cache 1D Data - sheet-backed JSON blobs for Stage-A scoring",
        tab_name=SheetNames.SCORE_CACHE_1D_DATA,
        headers=["Key", "Symbol", "Exchange", "Segment", "Bars", "Last Candle Time", "Updated At", "Blob JSON", "Blob Chars"],
    ),
    SheetNames.BACKFILL: SheetLayout(
        title="History Backfill Queue - status of universe history creation",
        tab_name=SheetNames.BACKFILL,
        headers=["Symbol", "Exchange", "Segment", "Timeframe", "Enabled", "Last Candle Time", "Bars Saved", "Status", "Attempts", "Last Error", "Updated At", "File Name", "ISIN", "Sector"],
    ),
    SheetNames.DECISIONS: SheetLayout(
        title="Decision Log - every trading decision",
        tab_name=SheetNames.DECISIONS,
        headers=["Timestamp", "Stage", "Symbol", "Decision", "Reason", "Context", "Run Date"],
    ),
    SheetNames.ACTIONS: SheetLayout(
        title="Project Log - technical action trace",
        tab_name=SheetNames.ACTIONS,
        headers=["Timestamp", "Module", "Action", "Status", "Message", "Context", "Run Date", "Exec ID"],
    ),
}


class GoogleSheetsRepository:
    def __init__(self, spreadsheet_id: str):
        self.spreadsheet_id = spreadsheet_id
        self._service = None

    def _svc(self):
        if self._service is not None:
            return self._service
        from google.auth import default
        from googleapiclient.discovery import build

        creds, _ = default(scopes=["https://www.googleapis.com/auth/spreadsheets"])
        self._service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        return self._service

    def _values(self):
        return self._svc().spreadsheets().values()

    @staticmethod
    def _is_retryable_sheets_error(exc: Exception) -> bool:
        try:
            from googleapiclient.errors import HttpError

            if isinstance(exc, HttpError):
                status = int(getattr(getattr(exc, "resp", None), "status", 0) or 0)
                if status in {408, 409, 425, 429, 500, 502, 503, 504}:
                    return True
        except Exception:
            pass

        if isinstance(exc, (BrokenPipeError, TimeoutError, ConnectionResetError, ConnectionAbortedError, ssl.SSLError)):
            return True
        msg = str(exc).lower()
        return any(
            token in msg
            for token in (
                "broken pipe",
                "unexpected eof",
                "timed out",
                "connection reset",
                "temporarily unavailable",
                "service unavailable",
                "internal error encountered",
                "quota exceeded",
                "rate limit exceeded",
            )
        )

    def _execute_with_retry(self, request: Any, *, op: str, retries: int = 6) -> Any:
        for attempt in range(1, max(1, int(retries)) + 1):
            try:
                return request.execute()
            except Exception as exc:
                retryable = self._is_retryable_sheets_error(exc)
                if (not retryable) or attempt >= retries:
                    raise
                sleep_s = min(8.0, 0.5 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.25)
                logger.warning(
                    "sheets_retry op=%s attempt=%s/%s sleepSec=%.2f error=%s",
                    op,
                    attempt,
                    retries,
                    sleep_s,
                    exc,
                )
                time.sleep(sleep_s)

    @staticmethod
    def col_to_a1(col_num_1_based: int) -> str:
        n = int(col_num_1_based)
        if n <= 0:
            raise ValueError("col_num_1_based must be >= 1")
        out = ""
        while n > 0:
            n, rem = divmod(n - 1, 26)
            out = chr(65 + rem) + out
        return out

    def _sheet_meta(self) -> dict[str, int]:
        meta = self._execute_with_retry(
            self._svc().spreadsheets().get(spreadsheetId=self.spreadsheet_id),
            op="sheet_meta_get",
        )
        out: dict[str, int] = {}
        for sh in meta.get("sheets", []):
            p = sh.get("properties", {})
            title = str(p.get("title", ""))
            sid = p.get("sheetId")
            if title and isinstance(sid, int):
                out[title] = sid
        return out

    def _sheet_grid_meta(self) -> dict[str, dict[str, int]]:
        meta = self._execute_with_retry(
            self._svc().spreadsheets().get(spreadsheetId=self.spreadsheet_id),
            op="sheet_grid_meta_get",
        )
        out: dict[str, dict[str, int]] = {}
        for sh in meta.get("sheets", []):
            p = sh.get("properties", {}) or {}
            title = str(p.get("title", "") or "")
            sid = p.get("sheetId")
            gp = p.get("gridProperties", {}) or {}
            if title and isinstance(sid, int):
                out[title] = {
                    "sheetId": sid,
                    "rowCount": int(gp.get("rowCount") or 0),
                    "columnCount": int(gp.get("columnCount") or 0),
                }
        return out

    def ensure_sheet_grid_min(self, sheet_name: str, *, min_rows: int = 0, min_cols: int = 0) -> None:
        if min_rows <= 0 and min_cols <= 0:
            return
        meta = self._sheet_grid_meta().get(sheet_name)
        if not meta:
            return
        row_count = int(meta.get("rowCount") or 0)
        col_count = int(meta.get("columnCount") or 0)
        target_rows = max(row_count, int(min_rows or 0))
        target_cols = max(col_count, int(min_cols or 0))
        if target_rows == row_count and target_cols == col_count:
            return
        self._execute_with_retry(
            self._svc().spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={
                    "requests": [
                        {
                            "updateSheetProperties": {
                                "properties": {
                                    "sheetId": int(meta["sheetId"]),
                                    "gridProperties": {
                                        "rowCount": target_rows,
                                        "columnCount": target_cols,
                                    },
                                },
                                "fields": "gridProperties(rowCount,columnCount)",
                            }
                        }
                    ]
                },
            ),
            op="sheet_grid_resize",
        )

    def get_values(self, a1_range: str) -> list[list[Any]]:
        res = self._execute_with_retry(
            self._values().get(spreadsheetId=self.spreadsheet_id, range=a1_range),
            op="values_get",
        )
        return res.get("values", [])

    def update_values(self, a1_range: str, values: list[list[Any]]) -> None:
        self._execute_with_retry(
            self._values().update(
                spreadsheetId=self.spreadsheet_id,
                range=a1_range,
                valueInputOption="USER_ENTERED",
                body={"values": values},
            ),
            op="values_update",
        )

    def batch_update_values(self, data: list[dict[str, Any]]) -> None:
        if not data:
            return
        self._execute_with_retry(
            self._values().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={"valueInputOption": "USER_ENTERED", "data": data},
            ),
            op="values_batch_update",
        )

    def append_values(self, a1_range: str, values: list[list[Any]], *, value_input_option: str = "USER_ENTERED") -> None:
        if not values:
            return
        self._execute_with_retry(
            self._values().append(
                spreadsheetId=self.spreadsheet_id,
                range=a1_range,
                valueInputOption=value_input_option,
                insertDataOption="INSERT_ROWS",
                body={"values": values},
            ),
            op="values_append",
        )

    def clear_range(self, a1_range: str) -> None:
        self._execute_with_retry(
            self._values().clear(spreadsheetId=self.spreadsheet_id, range=a1_range, body={}),
            op="values_clear",
        )

    def ensure_core_sheets(self) -> None:
        existing = self._sheet_meta()
        # One-time sheet renames for evolving schema/tab naming.
        rename_requests: list[dict[str, Any]] = []
        for old_name, new_name in LEGACY_SHEET_NAMES.items():
            if old_name in existing and new_name not in existing:
                rename_requests.append(
                    {
                        "updateSheetProperties": {
                            "properties": {"sheetId": existing[old_name], "title": new_name},
                            "fields": "title",
                        }
                    }
                )
        if rename_requests:
            self._execute_with_retry(
                self._svc().spreadsheets().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body={"requests": rename_requests},
                ),
                op="sheet_rename_batch",
            )
            existing = self._sheet_meta()

        requests: list[dict[str, Any]] = []
        for name in [
            SheetNames.CONFIG, SheetNames.WATCHLIST_SWING_V2, SheetNames.WATCHLIST_INTRADAY_V2, SheetNames.MARKET, SheetNames.SCAN, SheetNames.SIGNALS,
            SheetNames.ORDERS, SheetNames.POSITIONS, SheetNames.PNL, SheetNames.RISK,
            SheetNames.UNIVERSE, SheetNames.SCORE_CACHE_1D, SheetNames.SCORE_CACHE_5M, SheetNames.SCORE_CACHE_1D_DATA,
            SheetNames.DECISIONS, SheetNames.ACTIONS,
        ]:
            if name not in existing:
                requests.append({"addSheet": {"properties": {"title": name}}})
        if requests:
            self._execute_with_retry(
                self._svc().spreadsheets().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body={"requests": requests},
                ),
                op="sheet_add_batch",
            )
            existing = self._sheet_meta()

        # Watchlist V2 migration: delete legacy watchlist tab once V2 tabs are in place.
        if SheetNames.WATCHLIST in existing:
            try:
                self._execute_with_retry(
                    self._svc().spreadsheets().batchUpdate(
                        spreadsheetId=self.spreadsheet_id,
                        body={
                            "requests": [
                                {"deleteSheet": {"sheetId": int(existing[SheetNames.WATCHLIST])}},
                            ]
                        },
                    ),
                    op="sheet_delete_legacy_watchlist",
                )
                existing = self._sheet_meta()
            except Exception:
                logger.debug("Unable to delete legacy watchlist tab", exc_info=True)

        value_updates: list[dict[str, Any]] = []
        for name, layout in SHEET_LAYOUTS.items():
            if name not in existing:
                continue
            title_row = [layout.title] + [""] * (len(layout.headers) - 1)
            value_updates.append({"range": f"'{name}'!A1", "values": [title_row]})
            value_updates.append({"range": f"'{name}'!A3", "values": [layout.headers]})
        if value_updates:
            self.batch_update_values(value_updates)

        # Hide score cache data tab (best-effort)
        sid = existing.get(SheetNames.SCORE_CACHE_1D_DATA)
        if sid is not None:
            try:
                self._execute_with_retry(
                    self._svc().spreadsheets().batchUpdate(
                        spreadsheetId=self.spreadsheet_id,
                        body={
                            "requests": [
                                {
                                    "updateSheetProperties": {
                                        "properties": {"sheetId": sid, "hidden": True},
                                        "fields": "hidden",
                                    }
                                }
                            ]
                        },
                    ),
                    op="sheet_hide_score_cache_data",
                )
            except Exception:
                logger.debug("Unable to hide score cache data tab", exc_info=True)

    def read_config_label_map(self) -> dict[str, str]:
        rows = self.get_values(f"'{SheetNames.CONFIG}'!A:B")
        out: dict[str, str] = {}
        for row in rows:
            if not row:
                continue
            label = str(row[0]).strip() if len(row) > 0 else ""
            if not label:
                continue
            val = str(row[1]).strip() if len(row) > 1 and row[1] is not None else ""
            out[label] = val
        return out

    def read_sheet_rows(self, sheet_name: str, start_row: int = 4) -> list[list[str]]:
        rows = self.get_values(f"'{sheet_name}'!A{start_row}:ZZ")
        return [[str(c) if c is not None else "" for c in row] for row in rows]

    def read_sheet_headers(self, sheet_name: str, header_row: int = 3) -> list[str]:
        rows = self.get_values(f"'{sheet_name}'!A{header_row}:ZZ{header_row}")
        if not rows:
            return []
        return [str(c).strip() if c is not None else "" for c in rows[0]]

    def ensure_sheet_headers_append(
        self,
        sheet_name: str,
        required_headers: list[str],
        *,
        header_row: int = 3,
    ) -> dict[str, int]:
        headers = self.read_sheet_headers(sheet_name, header_row=header_row)
        existing_map: dict[str, int] = {}
        for i, h in enumerate(headers, start=1):
            key = str(h).strip()
            if key and key not in existing_map:
                existing_map[key] = i

        missing = [h for h in required_headers if str(h).strip() and str(h).strip() not in existing_map]
        if missing:
            start_col = len(headers) + 1 if headers else 1
            end_col = start_col + len(missing) - 1
            # Some sheets are still created with 26 columns; expand before writing beyond Z.
            self.ensure_sheet_grid_min(sheet_name, min_rows=max(1000, header_row), min_cols=end_col)
            start_a1 = self.col_to_a1(start_col)
            self.update_values(f"'{sheet_name}'!{start_a1}{header_row}", [missing])
            headers = self.read_sheet_headers(sheet_name, header_row=header_row)
            existing_map.clear()
            for i, h in enumerate(headers, start=1):
                key = str(h).strip()
                if key and key not in existing_map:
                    existing_map[key] = i
        return existing_map

    def ensure_config_defaults(self, defaults: dict[str, str]) -> int:
        existing = self.read_config_label_map()
        to_append: list[list[Any]] = []
        for k, v in defaults.items():
            key = str(k).strip()
            if not key:
                continue
            if key in existing:
                continue
            to_append.append([key, str(v)])
        if to_append:
            self.append_values(f"'{SheetNames.CONFIG}'!A1", to_append)
        return len(to_append)

    def append_rows(self, sheet_name: str, rows: list[list[Any]]) -> None:
        self.append_values(f"'{sheet_name}'!A4", rows)

    def read_watchlist(self) -> list[WatchlistRow]:
        rows = self.read_sheet_rows(SheetNames.WATCHLIST_INTRADAY_V2, 4)
        out: list[WatchlistRow] = []
        for row in rows:
            if len(row) < 24:
                continue
            if not row[4].strip():
                continue
            if row[23].strip().upper() != "Y":
                continue
            out.append(
                WatchlistRow(
                    symbol=row[4].strip().upper(),
                    exchange=(row[6].strip().upper() or "NSE"),
                    segment="CASH",
                    product="MIS",
                    strategy="AUTO",
                    sector="UNKNOWN",
                    beta=1.0,
                    enabled=True,
                    note=row[25].strip() if len(row) > 25 else "",
                )
            )
        return out

    def replace_watchlist(self, rows: list[list[Any]]) -> None:
        # Legacy alias retained for compatibility: writes into intraday V2 tab.
        self.clear_range(f"'{SheetNames.WATCHLIST_INTRADAY_V2}'!A4:ZZ")
        if rows:
            self.update_values(f"'{SheetNames.WATCHLIST_INTRADAY_V2}'!A4", rows)

    def replace_watchlist_swing_v2(self, rows: list[list[Any]]) -> None:
        self.clear_range(f"'{SheetNames.WATCHLIST_SWING_V2}'!A4:ZZ")
        if rows:
            self.update_values(f"'{SheetNames.WATCHLIST_SWING_V2}'!A4", rows)

    def replace_watchlist_intraday_v2(self, rows: list[list[Any]]) -> None:
        self.clear_range(f"'{SheetNames.WATCHLIST_INTRADAY_V2}'!A4:ZZ")
        if rows:
            self.update_values(f"'{SheetNames.WATCHLIST_INTRADAY_V2}'!A4", rows)

    def replace_score_cache_1d_index(self, rows: list[list[Any]], *, chunk_size: int = 500) -> None:
        self.ensure_sheet_headers_append(
            SheetNames.SCORE_CACHE_1D,
            SHEET_LAYOUTS[SheetNames.SCORE_CACHE_1D].headers,
            header_row=3,
        )
        target_tabs = [SheetNames.SCORE_CACHE_1D]
        # Some workbooks still contain a duplicate visual tab with emoji prefix.
        # Mirror writes so operators checking either tab see the same cache-index state.
        try:
            existing_tabs = self._sheet_meta()
            if "📚 History Candle 1D" in existing_tabs and "📚 History Candle 1D" not in target_tabs:
                self.ensure_sheet_headers_append(
                    "📚 History Candle 1D",
                    SHEET_LAYOUTS[SheetNames.SCORE_CACHE_1D].headers,
                    header_row=3,
                )
                target_tabs.append("📚 History Candle 1D")
        except Exception:
            logger.debug("score_cache_1d_index mirror tab detection failed", exc_info=True)

        for tab in target_tabs:
            self.clear_range(f"'{tab}'!A4:Z")
        if not rows:
            return
        # Use append so Sheets can grow beyond the default 1000-row grid without manual resizing.
        for i in range(0, len(rows), max(1, int(chunk_size))):
            chunk = rows[i : i + max(1, int(chunk_size))]
            # RAW preserves candle timestamps as text (prevents Sheets from showing date serials like 46077).
            for tab in target_tabs:
                self.append_values(f"'{tab}'!A4", chunk, value_input_option="RAW")

    def replace_score_cache_5m_index(self, rows: list[list[Any]], *, chunk_size: int = 500) -> None:
        self.ensure_sheet_headers_append(
            SheetNames.SCORE_CACHE_5M,
            SHEET_LAYOUTS[SheetNames.SCORE_CACHE_5M].headers,
            header_row=3,
        )
        self.clear_range(f"'{SheetNames.SCORE_CACHE_5M}'!A4:Z")
        if not rows:
            return
        for i in range(0, len(rows), max(1, int(chunk_size))):
            chunk = rows[i : i + max(1, int(chunk_size))]
            self.append_values(f"'{SheetNames.SCORE_CACHE_5M}'!A4", chunk, value_input_option="RAW")

    def replace_scan_rows(self, rows: list[list[Any]]) -> None:
        self.clear_range(f"'{SheetNames.SCAN}'!A4:Z")
        if rows:
            self.update_values(f"'{SheetNames.SCAN}'!A4", rows)

    def write_market_brain(self, regime: RegimeSnapshot) -> None:
        """Best-effort write to the Market Brain dashboard cells (B4:B63).

        This mirrors the original Apps Script `MarketBrain.gs` dashboard layout so the
        same sheet remains useful while the Python pipeline drives regime computation.
        """
        vix = float(getattr(regime, "vix", 0.0) or 0.0)
        # Market Brain v2 uses rows up to 63 and columns up to G; many legacy sheets are smaller.
        self.ensure_sheet_grid_min(SheetNames.MARKET, min_rows=70, min_cols=7)
        nifty = getattr(regime, "nifty", None)
        pcr = getattr(regime, "pcr", None)
        fii = getattr(regime, "fii", None)

        nifty_ltp = float(getattr(nifty, "ltp", 0.0) or 0.0)
        nifty_change = float(getattr(nifty, "change_pct", 0.0) or 0.0)
        nifty_open = float(getattr(nifty, "open", 0.0) or 0.0)
        nifty_high = float(getattr(nifty, "high", 0.0) or 0.0)
        nifty_low = float(getattr(nifty, "low", 0.0) or 0.0)
        nifty_age_sec = float(getattr(nifty, "age_sec", 0.0) or 0.0)

        pcr_val = float(getattr(pcr, "pcr", 1.0) or 1.0)
        pcr_max_pain = float(getattr(pcr, "max_pain", 0.0) or 0.0)
        pcr_call_oi = float(getattr(pcr, "call_oi", 0.0) or 0.0)
        pcr_put_oi = float(getattr(pcr, "put_oi", 0.0) or 0.0)
        pcr_weighted = float(getattr(pcr, "pcr_weighted", pcr_val) or pcr_val)
        pcr_near = float(getattr(pcr, "pcr_near", pcr_val) or pcr_val)
        pcr_next = float(getattr(pcr, "pcr_next", pcr_val) or pcr_val)
        pcr_monthly = float(getattr(pcr, "pcr_monthly", pcr_val) or pcr_val)
        pcr_term_slope = float(getattr(pcr, "pcr_term_slope", 0.0) or 0.0)
        pcr_oi_change_pcr = float(getattr(pcr, "oi_change_pcr", 1.0) or 1.0)
        pcr_oi_conc = float(getattr(pcr, "oi_concentration", 0.0) or 0.0)
        pcr_call_wall = float(getattr(pcr, "call_wall", 0.0) or 0.0)
        pcr_put_wall = float(getattr(pcr, "put_wall", 0.0) or 0.0)
        pcr_call_wall_dist = float(getattr(pcr, "call_wall_dist_pct", 0.0) or 0.0)
        pcr_put_wall_dist = float(getattr(pcr, "put_wall_dist_pct", 0.0) or 0.0)
        pcr_max_pain_dist = float(getattr(pcr, "max_pain_dist_pct", 0.0) or 0.0)
        pcr_exp_near = str(getattr(pcr, "expiry_near", "") or "")
        pcr_exp_next = str(getattr(pcr, "expiry_next", "") or "")
        pcr_exp_month = str(getattr(pcr, "expiry_monthly", "") or "")
        pcr_exp_used = int(getattr(pcr, "expiries_used", 0) or 0)
        pcr_conf = float(getattr(pcr, "confidence", 0.0) or 0.0)
        fii_val = float(getattr(fii, "fii", 0.0) or 0.0)
        dii_val = float(getattr(fii, "dii", 0.0) or 0.0)
        fii_freshness = float(getattr(fii, "freshness_score", 0.0) or 0.0)
        fii_as_of = str(getattr(fii, "as_of_date", "") or "")

        freshness = getattr(regime, "freshness", None)
        f_session = str(getattr(freshness, "session_phase", "UNKNOWN") or "UNKNOWN")
        f_score = float(getattr(freshness, "score", 0.0) or 0.0)
        f_nifty_age = float(getattr(freshness, "nifty_age_sec", nifty_age_sec) or nifty_age_sec)
        f_vix_age = float(getattr(freshness, "vix_age_sec", 0.0) or 0.0)
        f_pcr_age = float(getattr(freshness, "pcr_age_sec", 0.0) or 0.0)
        f_fii_age = float(getattr(freshness, "fii_age_hours", 0.0) or 0.0)

        struct = getattr(regime, "nifty_structure", None)
        s_regime = str(getattr(struct, "structure_regime", "UNKNOWN") or "UNKNOWN")
        s_trend = float(getattr(struct, "trend_strength", 0.0) or 0.0)
        s_chop = float(getattr(struct, "chop_risk", 0.0) or 0.0)
        s_adx = float(getattr(struct, "adx", 0.0) or 0.0)
        s_atr_pct = float(getattr(struct, "atr_pct", 0.0) or 0.0)
        s_ema_spread = float(getattr(struct, "ema_spread_pct", 0.0) or 0.0)
        s_vwap_gap = float(getattr(struct, "vwap_gap_pct", 0.0) or 0.0)
        s_gap = float(getattr(struct, "gap_pct", 0.0) or 0.0)
        s_orb = str(getattr(struct, "opening_range_break", "NONE") or "NONE")
        s_last_ts = str(getattr(struct, "last_candle_ts", "") or "")
        s_timeframe = str(getattr(struct, "timeframe", "") or "")

        regime_conf = float(getattr(regime, "confidence", 0.0) or 0.0)
        data_health = float(getattr(regime, "data_health", 0.0) or 0.0)
        source_quality = float(getattr(regime, "source_quality", 0.0) or 0.0)
        sub_regime = str(getattr(regime, "sub_regime", "UNKNOWN") or "UNKNOWN")
        rationale = str(getattr(regime, "rationale", "") or "")

        def _age_display(v: float) -> str:
            try:
                x = float(v)
            except Exception:
                return ""
            if x < 0:
                x = 0.0
            if x < 1.0:
                return "<1s"
            return f"{round(x, 1)}"

        def _hours_age_display(v: float) -> str:
            try:
                x = float(v)
            except Exception:
                return ""
            if x < 0:
                x = 0.0
            if x < 1.0:
                return "<1h"
            return f"{round(x, 2)}"

        intraday_structure = s_timeframe == "15m"
        s_vwap_gap_display: Any = round(s_vwap_gap, 3) if intraday_structure else "N/A"
        s_orb_display: Any = s_orb if intraday_structure else "N/A"

        # Use strategy defaults if config labels are absent; this is just a sheet display helper.
        vix_trend_max = 15.0
        vix_safe_max = 20.0
        nifty_trend_pct = 0.3
        pcr_bull_min = 0.8
        pcr_bear_max = 1.2
        try:
            cfg = self.read_config_label_map()
            # Best-effort parsing from sheet config labels if present.
            vix_trend_max = float(cfg.get("VIX_TREND_MAX", vix_trend_max))
            vix_safe_max = float(cfg.get("VIX_SAFE_MAX", vix_safe_max))
            nifty_trend_pct = float(cfg.get("NIFTY_TREND_PCT", nifty_trend_pct))
            pcr_bull_min = float(cfg.get("PCR_BULL_MIN", pcr_bull_min))
            pcr_bear_max = float(cfg.get("PCR_BEAR_MAX", pcr_bear_max))
        except Exception:
            logger.debug("Unable to read config label map for market brain thresholds", exc_info=True)

        vix_status = "✅ SAFE" if vix < vix_trend_max else ("⚠️ CAUTION" if vix < vix_safe_max else "🛑 DANGER")
        nifty_dir = "BULLISH" if nifty_change > nifty_trend_pct else ("BEARISH" if nifty_change < -nifty_trend_pct else "NEUTRAL")
        pcr_dir = "BULLISH" if pcr_val >= pcr_bull_min else ("BEARISH" if pcr_val <= pcr_bear_max else "NEUTRAL")

        # Market Brain "Sentiment Score" (0..100): long-side favorability gauge for PM visibility.
        sentiment = 50.0
        regime_name = str(getattr(regime, "regime", "RANGE") or "RANGE").upper()
        bias_name = str(getattr(regime, "bias", "NEUTRAL") or "NEUTRAL").upper()
        sentiment += 12.0 if regime_name == "TREND" else (-18.0 if regime_name == "AVOID" else 0.0)
        sentiment += 14.0 if bias_name == "BULLISH" else (-14.0 if bias_name == "BEARISH" else 0.0)
        if vix <= vix_trend_max:
            sentiment += 8.0
        elif vix <= vix_safe_max:
            sentiment -= 2.0
        else:
            sentiment -= 12.0
        # Nifty move contribution capped to avoid dominating the score.
        sentiment += max(-10.0, min(10.0, nifty_change * 20.0))
        if bias_name == "BULLISH":
            sentiment += 4.0 if pcr_val >= pcr_bull_min else -4.0
        elif bias_name == "BEARISH":
            sentiment += 4.0 if pcr_val <= pcr_bear_max else -4.0
        else:
            sentiment += 2.0 if pcr_bull_min <= pcr_val <= pcr_bear_max else -2.0
        if fii_val > 0:
            sentiment += 4.0
        elif fii_val < 0:
            sentiment -= 4.0
        sentiment = max(0.0, min(100.0, sentiment))

        data = [
            # Left panel labels (legacy Market Brain dashboard)
            {"range": f"'{SheetNames.MARKET}'!A4", "values": [[
                "India VIX",
            ]]},
            {"range": f"'{SheetNames.MARKET}'!C4:D4", "values": [["What it means", "How calculated / source"]]},
            {"range": f"'{SheetNames.MARKET}'!A5", "values": [["VIX Status"]]},
            {"range": f"'{SheetNames.MARKET}'!A6", "values": [["Nifty 50 LTP"]]},
            {"range": f"'{SheetNames.MARKET}'!A7", "values": [["Nifty Change %"]]},
            {"range": f"'{SheetNames.MARKET}'!A8", "values": [["Nifty Bias"]]},
            {"range": f"'{SheetNames.MARKET}'!A9", "values": [["Nifty Open"]]},
            {"range": f"'{SheetNames.MARKET}'!A10", "values": [["Nifty High"]]},
            {"range": f"'{SheetNames.MARKET}'!A11", "values": [["Nifty Low"]]},
            {"range": f"'{SheetNames.MARKET}'!A12", "values": [["PCR"]]},
            {"range": f"'{SheetNames.MARKET}'!A13", "values": [["PCR Bias"]]},
            {"range": f"'{SheetNames.MARKET}'!A14", "values": [["Max Pain"]]},
            {"range": f"'{SheetNames.MARKET}'!A15", "values": [["Call OI"]]},
            {"range": f"'{SheetNames.MARKET}'!A16", "values": [["Put OI"]]},
            {"range": f"'{SheetNames.MARKET}'!A17", "values": [["FII Net"]]},
            {"range": f"'{SheetNames.MARKET}'!A18", "values": [["DII Net"]]},
            {"range": f"'{SheetNames.MARKET}'!A19", "values": [["Net Flow (FII+DII)"]]},
            {"range": f"'{SheetNames.MARKET}'!A20", "values": [["Market Regime"]]},
            {"range": f"'{SheetNames.MARKET}'!A21", "values": [["Market Bias"]]},
            {"range": f"'{SheetNames.MARKET}'!A22", "values": [["Sentiment Score (0-100)"]]},
            {"range": f"'{SheetNames.MARKET}'!A23", "values": [["Updated At (IST)"]]},
            {"range": f"'{SheetNames.MARKET}'!A24", "values": [["Data Source"]]},
            {"range": f"'{SheetNames.MARKET}'!A25", "values": [["Session Phase"]]},
            {"range": f"'{SheetNames.MARKET}'!A26", "values": [["Regime Confidence (0-100)"]]},
            {"range": f"'{SheetNames.MARKET}'!A27", "values": [["Freshness Score (0-100)"]]},
            {"range": f"'{SheetNames.MARKET}'!A28", "values": [["Data Health (0-100)"]]},
            {"range": f"'{SheetNames.MARKET}'!A29", "values": [["Source Quality (0-100)"]]},
            {"range": f"'{SheetNames.MARKET}'!A30", "values": [["Sub Regime"]]},
            {"range": f"'{SheetNames.MARKET}'!A31", "values": [["Nifty Structure Regime"]]},
            {"range": f"'{SheetNames.MARKET}'!A32", "values": [["Nifty Trend Strength"]]},
            {"range": f"'{SheetNames.MARKET}'!A33", "values": [["Nifty Chop Risk"]]},
            {"range": f"'{SheetNames.MARKET}'!A34", "values": [["Nifty ADX (15m)"]]},
            {"range": f"'{SheetNames.MARKET}'!A35", "values": [["Nifty ATR % (15m)"]]},
            {"range": f"'{SheetNames.MARKET}'!A36", "values": [["Nifty EMA Spread %"]]},
            {"range": f"'{SheetNames.MARKET}'!A37", "values": [["Nifty VWAP Gap %"]]},
            {"range": f"'{SheetNames.MARKET}'!A38", "values": [["Nifty Gap %"]]},
            {"range": f"'{SheetNames.MARKET}'!A39", "values": [["Nifty OR Break (15m)"]]},
            {"range": f"'{SheetNames.MARKET}'!A40", "values": [["Nifty Quote Age (sec)"]]},
            {"range": f"'{SheetNames.MARKET}'!A41", "values": [["VIX Quote Age (sec)"]]},
            {"range": f"'{SheetNames.MARKET}'!A42", "values": [["PCR Age (sec)"]]},
            {"range": f"'{SheetNames.MARKET}'!A43", "values": [["FII Data Age (hrs)"]]},
            {"range": f"'{SheetNames.MARKET}'!A44", "values": [["FII/DII Freshness (0-100)"]]},
            {"range": f"'{SheetNames.MARKET}'!A45", "values": [["Regime Rationale"]]},
            {"range": f"'{SheetNames.MARKET}'!A46", "values": [["PCR Weighted"]]},
            {"range": f"'{SheetNames.MARKET}'!A47", "values": [["PCR Near Exp"]]},
            {"range": f"'{SheetNames.MARKET}'!A48", "values": [["PCR Next Exp"]]},
            {"range": f"'{SheetNames.MARKET}'!A49", "values": [["PCR Monthly"]]},
            {"range": f"'{SheetNames.MARKET}'!A50", "values": [["PCR Term Slope"]]},
            {"range": f"'{SheetNames.MARKET}'!A51", "values": [["PCR OI Change Ratio"]]},
            {"range": f"'{SheetNames.MARKET}'!A52", "values": [["OI Concentration (Top3)"]]},
            {"range": f"'{SheetNames.MARKET}'!A53", "values": [["Spot vs Max Pain %"]]},
            {"range": f"'{SheetNames.MARKET}'!A54", "values": [["Call Wall"]]},
            {"range": f"'{SheetNames.MARKET}'!A55", "values": [["Put Wall"]]},
            {"range": f"'{SheetNames.MARKET}'!A56", "values": [["Call Wall Dist %"]]},
            {"range": f"'{SheetNames.MARKET}'!A57", "values": [["Put Wall Dist %"]]},
            {"range": f"'{SheetNames.MARKET}'!A58", "values": [["PCR Expiry Near"]]},
            {"range": f"'{SheetNames.MARKET}'!A59", "values": [["PCR Expiry Next"]]},
            {"range": f"'{SheetNames.MARKET}'!A60", "values": [["PCR Expiry Monthly"]]},
            {"range": f"'{SheetNames.MARKET}'!A61", "values": [["PCR Expiries Used"]]},
            {"range": f"'{SheetNames.MARKET}'!A62", "values": [["PCR Confidence (0-100)"]]},
            {"range": f"'{SheetNames.MARKET}'!A63", "values": [["Nifty Structure Candle Time"]]},

            # Definitions for each A-row metric (PM-friendly, self-explanatory dashboard)
            {"range": f"'{SheetNames.MARKET}'!C4:D4", "values": [["Meaning", "Calculation / Source"]]},
            {"range": f"'{SheetNames.MARKET}'!C5:D5", "values": [["Volatility risk zone", "Derived from B4 using VIX_TREND_MAX/VIX_SAFE_MAX thresholds"]]},
            {"range": f"'{SheetNames.MARKET}'!C6:D6", "values": [["Current Nifty index level", "Upstox Nifty quote LTP"]]},
            {"range": f"'{SheetNames.MARKET}'!C7:D7", "values": [["Nifty day move vs previous close", "Upstox change% (fallback computed from LTP and prev close)"]]},
            {"range": f"'{SheetNames.MARKET}'!C8:D8", "values": [["Short market direction from Nifty move", "BULLISH/BEARISH/NEUTRAL using NIFTY_TREND_PCT"]]},
            {"range": f"'{SheetNames.MARKET}'!C9:D9", "values": [["Session open level", "Upstox quote OHLC if present; else derived from intraday/daily candles"]]},
            {"range": f"'{SheetNames.MARKET}'!C10:D10", "values": [["Session high level", "Upstox quote OHLC if present; else derived from intraday/daily candles"]]},
            {"range": f"'{SheetNames.MARKET}'!C11:D11", "values": [["Session low level", "Upstox quote OHLC if present; else derived from intraday/daily candles"]]},
            {"range": f"'{SheetNames.MARKET}'!C12:D12", "values": [["Put/Call sentiment ratio", "Aggregate Put OI / Call OI from Upstox option chain (nearest expiry)"]]},
            {"range": f"'{SheetNames.MARKET}'!C13:D13", "values": [["PCR interpretation", "BULLISH/BEARISH/NEUTRAL using PCR_BULL_MIN/PCR_BEAR_MAX"]]},
            {"range": f"'{SheetNames.MARKET}'!C14:D14", "values": [["Max pain proxy strike", "Strike with highest total OI (Call OI + Put OI) from option chain"]]},
            {"range": f"'{SheetNames.MARKET}'!C15:D15", "values": [["Total call open interest", "Sum of call OI across option-chain strikes (nearest expiry)"]]},
            {"range": f"'{SheetNames.MARKET}'!C16:D16", "values": [["Total put open interest", "Sum of put OI across option-chain strikes (nearest expiry)"]]},
            {"range": f"'{SheetNames.MARKET}'!C17:D17", "values": [["FII net flow", "Best-effort NSE fiidiiTradeReact latest row (fallback 0 if unavailable)"]]},
            {"range": f"'{SheetNames.MARKET}'!C18:D18", "values": [["DII net flow", "Best-effort NSE fiidiiTradeReact latest row (fallback 0 if unavailable)"]]},
            {"range": f"'{SheetNames.MARKET}'!C19:D19", "values": [["Combined institutional flow", "FII Net + DII Net"]]},
            {"range": f"'{SheetNames.MARKET}'!C20:D20", "values": [["Trading environment type", "TREND/RANGE/AVOID from VIX + Nifty move + PCR contradiction checks"]]},
            {"range": f"'{SheetNames.MARKET}'!C21:D21", "values": [["Directional bias used by algo", "Bias derived from Nifty change vs threshold"]]},
            {"range": f"'{SheetNames.MARKET}'!C22:D22", "values": [["PM dashboard sentiment (0-100)", "Composite of regime, bias, VIX zone, Nifty move, PCR alignment, FII sign"]]},
            {"range": f"'{SheetNames.MARKET}'!C23:D23", "values": [["Last Market Brain refresh time", "Written whenever regime is fetched by score/watchlist/scan jobs"]]},
            {"range": f"'{SheetNames.MARKET}'!C24:D24", "values": [["Actual source used per metric", "e.g. nifty=upstox;vix=upstox|yahoo;pcr=upstox_option_chain|fallback;fii=nse|fallback"]]},
            {"range": f"'{SheetNames.MARKET}'!C25:D25", "values": [["Market session classification", "Derived from IST time window (pre-open/opening/regular/post-close/off-hours)"]]},
            {"range": f"'{SheetNames.MARKET}'!C26:D26", "values": [["Confidence in current regime classification", "Composite of signal agreement, freshness, source quality, stability and structure strength"]]},
            {"range": f"'{SheetNames.MARKET}'!C27:D27", "values": [["Freshness quality of market inputs", "Weighted recency score of Nifty/VIX/PCR/FII data (session-aware)"]]},
            {"range": f"'{SheetNames.MARKET}'!C28:D28", "values": [["Overall data health", "Composite of freshness, source quality and metric completeness"]]},
            {"range": f"'{SheetNames.MARKET}'!C29:D29", "values": [["Source reliability score", "Weighted source quality: Upstox > NSE/Yahoo > cache/fallback"]]},
            {"range": f"'{SheetNames.MARKET}'!C30:D30", "values": [["Refined regime subtype", "More descriptive state (e.g., TREND_UP, RANGE_COMPRESSION, VOLATILE_RISK_OFF)"]]},
            {"range": f"'{SheetNames.MARKET}'!C31:D31", "values": [["Intraday Nifty structure state", "Derived from 15m EMA/SuperTrend/VWAP/ADX/ORB features"]]},
            {"range": f"'{SheetNames.MARKET}'!C32:D32", "values": [["Intraday trend strength (0-100)", "Strength score from ADX, EMA spread, VWAP gap, MACD, OR break"]]},
            {"range": f"'{SheetNames.MARKET}'!C33:D33", "values": [["Intraday chop risk (0-100)", "Higher when ADX low, EMA spread small, VWAP gap small, RSI around 50"]]},
            {"range": f"'{SheetNames.MARKET}'!C34:D34", "values": [["Average Directional Index", "ADX(14) on Nifty 15m candles (trend strength indicator)"]]},
            {"range": f"'{SheetNames.MARKET}'!C35:D35", "values": [["Realized intraday volatility %", "ATR(14) / current price on Nifty 15m candles"]]},
            {"range": f"'{SheetNames.MARKET}'!C36:D36", "values": [["EMA spread %", "Abs(EMA fast - EMA slow) / price on Nifty 15m"]]},
            {"range": f"'{SheetNames.MARKET}'!C37:D37", "values": [["VWAP gap %", "Current Nifty vs intraday VWAP on 15m candles"]]},
            {"range": f"'{SheetNames.MARKET}'!C38:D38", "values": [["Opening gap %", "Today's first 15m open vs previous session last close"]]},
            {"range": f"'{SheetNames.MARKET}'!C39:D39", "values": [["Opening range breakout state", "Current price vs first 15m range high/low (UP_BREAK/DOWN_BREAK/INSIDE)"]]},
            {"range": f"'{SheetNames.MARKET}'!C40:D40", "values": [["Nifty quote recency", "Age of Nifty quote used by Market Brain"]]},
            {"range": f"'{SheetNames.MARKET}'!C41:D41", "values": [["VIX quote recency", "Age of VIX value used by Market Brain"]]},
            {"range": f"'{SheetNames.MARKET}'!C42:D42", "values": [["PCR recency", "Age of PCR snapshot fetch"]]},
            {"range": f"'{SheetNames.MARKET}'!C43:D43", "values": [["FII/DII data age", "Hours since latest FII/DII trade date (not live-tick data)"]]},
            {"range": f"'{SheetNames.MARKET}'!C44:D44", "values": [["FII/DII freshness quality", "Freshness score from trade date + source (NSE/cache/fallback)"]]},
            {"range": f"'{SheetNames.MARKET}'!C45:D45", "values": [["Machine-readable regime explanation", "Compact rationale string with key metrics used in classification"]]},
            {"range": f"'{SheetNames.MARKET}'!C46:D46", "values": [["Weighted PCR across expiries", "OI/time weighted PCR using near/next/monthly expiries"]]},
            {"range": f"'{SheetNames.MARKET}'!C47:D47", "values": [["Nearest expiry PCR", "Aggregate put OI / call OI for nearest expiry"]]},
            {"range": f"'{SheetNames.MARKET}'!C48:D48", "values": [["Next expiry PCR", "Aggregate PCR for next expiry"]]},
            {"range": f"'{SheetNames.MARKET}'!C49:D49", "values": [["Monthly expiry PCR", "Aggregate PCR for monthly expiry"]]},
            {"range": f"'{SheetNames.MARKET}'!C50:D50", "values": [["PCR term slope", "PCR(next expiry) - PCR(near expiry)"]]},
            {"range": f"'{SheetNames.MARKET}'!C51:D51", "values": [["OI change PCR", "Positive Put OI change / Positive Call OI change (nearest expiry)"]]},
            {"range": f"'{SheetNames.MARKET}'!C52:D52", "values": [["OI concentration", "Top-3 strikes total OI share (nearest expiry)"]]},
            {"range": f"'{SheetNames.MARKET}'!C53:D53", "values": [["Spot vs max pain distance", "(Max Pain - Nifty Spot) / Nifty Spot %"]]},
            {"range": f"'{SheetNames.MARKET}'!C54:D54", "values": [["Call wall strike", "Strike with highest call OI (nearest expiry)"]]},
            {"range": f"'{SheetNames.MARKET}'!C55:D55", "values": [["Put wall strike", "Strike with highest put OI (nearest expiry)"]]},
            {"range": f"'{SheetNames.MARKET}'!C56:D56", "values": [["Call wall distance %", "(Call Wall - Nifty Spot) / Nifty Spot %"]]},
            {"range": f"'{SheetNames.MARKET}'!C57:D57", "values": [["Put wall distance %", "(Put Wall - Nifty Spot) / Nifty Spot %"]]},
            {"range": f"'{SheetNames.MARKET}'!C58:D58", "values": [["Nearest expiry used", "Nearest future option expiry used for PCR analytics"]]},
            {"range": f"'{SheetNames.MARKET}'!C59:D59", "values": [["Next expiry used", "Second expiry used for PCR term structure"]]},
            {"range": f"'{SheetNames.MARKET}'!C60:D60", "values": [["Monthly expiry used", "Monthly expiry used for PCR term structure"]]},
            {"range": f"'{SheetNames.MARKET}'!C61:D61", "values": [["Expiries considered", "Count of expiries successfully fetched for PCR analytics"]]},
            {"range": f"'{SheetNames.MARKET}'!C62:D62", "values": [["PCR analytics confidence", "Confidence score based on OI coverage and expiry availability"]]},
            {"range": f"'{SheetNames.MARKET}'!C63:D63", "values": [["Latest Nifty structure candle time", "Timestamp of latest 15m candle used for Nifty structure signals"]]},

            {"range": f"'{SheetNames.MARKET}'!B4", "values": [[round(vix, 2)]]},
            {"range": f"'{SheetNames.MARKET}'!B5", "values": [[vix_status]]},
            {"range": f"'{SheetNames.MARKET}'!B6", "values": [[round(nifty_ltp, 2)]]},
            {"range": f"'{SheetNames.MARKET}'!B7", "values": [[f"'{nifty_change:.2f}%"]]},
            {"range": f"'{SheetNames.MARKET}'!B8", "values": [[nifty_dir]]},
            {"range": f"'{SheetNames.MARKET}'!B9", "values": [[round(nifty_open, 2) if nifty_open else ""]]},
            {"range": f"'{SheetNames.MARKET}'!B10", "values": [[round(nifty_high, 2) if nifty_high else ""]]},
            {"range": f"'{SheetNames.MARKET}'!B11", "values": [[round(nifty_low, 2) if nifty_low else ""]]},
            {"range": f"'{SheetNames.MARKET}'!B12", "values": [[round(pcr_val, 2)]]},
            {"range": f"'{SheetNames.MARKET}'!B13", "values": [[pcr_dir]]},
            {"range": f"'{SheetNames.MARKET}'!B14", "values": [[round(pcr_max_pain, 2) if pcr_max_pain else 0]]},
            {"range": f"'{SheetNames.MARKET}'!B15", "values": [[round(pcr_call_oi, 0) if pcr_call_oi else 0]]},
            {"range": f"'{SheetNames.MARKET}'!B16", "values": [[round(pcr_put_oi, 0) if pcr_put_oi else 0]]},
            {"range": f"'{SheetNames.MARKET}'!B17", "values": [[round(fii_val, 2)]]},
            {"range": f"'{SheetNames.MARKET}'!B18", "values": [[round(dii_val, 2)]]},
            {"range": f"'{SheetNames.MARKET}'!B19", "values": [[round(fii_val + dii_val, 2)]]},
            {"range": f"'{SheetNames.MARKET}'!B20", "values": [[str(getattr(regime, 'regime', 'RANGE'))]]},
            {"range": f"'{SheetNames.MARKET}'!B21", "values": [[str(getattr(regime, 'bias', 'NEUTRAL'))]]},
            {"range": f"'{SheetNames.MARKET}'!B22", "values": [[round(sentiment, 1)]]},
            {"range": f"'{SheetNames.MARKET}'!B23", "values": [[now_ist_str()]]},
            {"range": f"'{SheetNames.MARKET}'!B24", "values": [[str(getattr(regime, 'source', 'computed'))]]},
            {"range": f"'{SheetNames.MARKET}'!B25", "values": [[f_session]]},
            {"range": f"'{SheetNames.MARKET}'!B26", "values": [[round(regime_conf, 1)]]},
            {"range": f"'{SheetNames.MARKET}'!B27", "values": [[round(f_score, 1)]]},
            {"range": f"'{SheetNames.MARKET}'!B28", "values": [[round(data_health, 1)]]},
            {"range": f"'{SheetNames.MARKET}'!B29", "values": [[round(source_quality, 1)]]},
            {"range": f"'{SheetNames.MARKET}'!B30", "values": [[sub_regime]]},
            {"range": f"'{SheetNames.MARKET}'!B31", "values": [[s_regime]]},
            {"range": f"'{SheetNames.MARKET}'!B32", "values": [[round(s_trend, 1)]]},
            {"range": f"'{SheetNames.MARKET}'!B33", "values": [[round(s_chop, 1)]]},
            {"range": f"'{SheetNames.MARKET}'!B34", "values": [[round(s_adx, 2) if s_adx else 0]]},
            {"range": f"'{SheetNames.MARKET}'!B35", "values": [[round(s_atr_pct, 3) if s_atr_pct else 0]]},
            {"range": f"'{SheetNames.MARKET}'!B36", "values": [[round(s_ema_spread, 3) if s_ema_spread else 0]]},
            {"range": f"'{SheetNames.MARKET}'!B37", "values": [[s_vwap_gap_display]]},
            {"range": f"'{SheetNames.MARKET}'!B38", "values": [[round(s_gap, 3) if s_gap else 0]]},
            {"range": f"'{SheetNames.MARKET}'!B39", "values": [[s_orb_display]]},
            {"range": f"'{SheetNames.MARKET}'!B40", "values": [[_age_display(f_nifty_age)]]},
            {"range": f"'{SheetNames.MARKET}'!B41", "values": [[_age_display(f_vix_age)]]},
            {"range": f"'{SheetNames.MARKET}'!B42", "values": [[_age_display(f_pcr_age)]]},
            {"range": f"'{SheetNames.MARKET}'!B43", "values": [[_hours_age_display(f_fii_age)]]},
            {"range": f"'{SheetNames.MARKET}'!B44", "values": [[round(fii_freshness, 1)]]},
            {"range": f"'{SheetNames.MARKET}'!B45", "values": [[rationale]]},
            {"range": f"'{SheetNames.MARKET}'!B46", "values": [[round(pcr_weighted, 3)]]},
            {"range": f"'{SheetNames.MARKET}'!B47", "values": [[round(pcr_near, 3)]]},
            {"range": f"'{SheetNames.MARKET}'!B48", "values": [[round(pcr_next, 3)]]},
            {"range": f"'{SheetNames.MARKET}'!B49", "values": [[round(pcr_monthly, 3)]]},
            {"range": f"'{SheetNames.MARKET}'!B50", "values": [[round(pcr_term_slope, 3)]]},
            {"range": f"'{SheetNames.MARKET}'!B51", "values": [[round(pcr_oi_change_pcr, 3)]]},
            {"range": f"'{SheetNames.MARKET}'!B52", "values": [[round(pcr_oi_conc, 4)]]},
            {"range": f"'{SheetNames.MARKET}'!B53", "values": [[round(pcr_max_pain_dist, 3)]]},
            {"range": f"'{SheetNames.MARKET}'!B54", "values": [[round(pcr_call_wall, 2) if pcr_call_wall else 0]]},
            {"range": f"'{SheetNames.MARKET}'!B55", "values": [[round(pcr_put_wall, 2) if pcr_put_wall else 0]]},
            {"range": f"'{SheetNames.MARKET}'!B56", "values": [[round(pcr_call_wall_dist, 3)]]},
            {"range": f"'{SheetNames.MARKET}'!B57", "values": [[round(pcr_put_wall_dist, 3)]]},
            # Prefix apostrophe so Sheets keeps expiry labels as text (avoids serial date numbers in exports).
            {"range": f"'{SheetNames.MARKET}'!B58", "values": [[f"'{pcr_exp_near}" if pcr_exp_near else ""]]},
            {"range": f"'{SheetNames.MARKET}'!B59", "values": [[f"'{pcr_exp_next}" if pcr_exp_next else ""]]},
            {"range": f"'{SheetNames.MARKET}'!B60", "values": [[f"'{pcr_exp_month}" if pcr_exp_month else ""]]},
            {"range": f"'{SheetNames.MARKET}'!B61", "values": [[pcr_exp_used]]},
            {"range": f"'{SheetNames.MARKET}'!B62", "values": [[round(pcr_conf, 1)]]},
            {"range": f"'{SheetNames.MARKET}'!B63", "values": [[s_last_ts]]},

            # Right panel legend (what the intraday signal score means)
            {"range": f"'{SheetNames.MARKET}'!E4:G4", "values": [["Layer", "Max Pts", "Meaning"]]},
            {"range": f"'{SheetNames.MARKET}'!E5:G5", "values": [["Regime", "25", "Nifty/VIX/FII context alignment"]]},
            {"range": f"'{SheetNames.MARKET}'!E6:G6", "values": [["Options", "20", "PCR + Max Pain alignment"]]},
            {"range": f"'{SheetNames.MARKET}'!E7:G7", "values": [["Technical", "40", "ST/VWAP/EMA/RSI/MACD + patterns"]]},
            {"range": f"'{SheetNames.MARKET}'!E8:G8", "values": [["Volume", "15", "Vol ratio + OBV confirmation"]]},
            {"range": f"'{SheetNames.MARKET}'!E9:G9", "values": [["Penalty", "Neg", "VIX/chop/doji/overstretch deductions"]]},
            {"range": f"'{SheetNames.MARKET}'!E10:G10", "values": [["Direction", "Gate", "BUY / SELL / HOLD from intraday indicators"]]},
            {"range": f"'{SheetNames.MARKET}'!E11:G11", "values": [["Score Range", "0-100", "Final signal score after penalties and clamp"]]},
            {"range": f"'{SheetNames.MARKET}'!E12:G12", "values": [["Use", "-", "Regime dashboard for score/watchlist/scanner decisions"]]},
            {"range": f"'{SheetNames.MARKET}'!E13:G13", "values": [["Regime v2", "-", "Confidence/freshness-aware regime + sub-regime classification"]]},
            {"range": f"'{SheetNames.MARKET}'!E14:G14", "values": [["Freshness", "-", "Source recency model (Nifty/VIX/PCR/FII) used in confidence"]]},
            {"range": f"'{SheetNames.MARKET}'!E15:G15", "values": [["Nifty Structure", "-", "EMA/ATR/ADX/VWAP/ORB intraday state for regime strength/chop"]]},
            {"range": f"'{SheetNames.MARKET}'!E16:G16", "values": [["PCR Structure", "-", "Multi-expiry weighted PCR + OI walls + concentration + max pain distance"]]},
            {"range": f"'{SheetNames.MARKET}'!E17:G17", "values": [["Flow Layer", "-", "FII/DII net + freshness/source quality (NSE/cache/fallback)"]]},
            {"range": f"'{SheetNames.MARKET}'!E18:G18", "values": [["Data Health", "-", "Composite completeness + freshness + source quality for safe decisioning"]]},
        ]
        self.batch_update_values(data)

    def read_universe_rows(self) -> list[UniverseRow]:
        rows = self.read_sheet_rows(SheetNames.UNIVERSE, 4)
        out: list[UniverseRow] = []
        for idx, row in enumerate(rows, start=4):
            if len(row) < 9 or not row[1].strip():
                continue
            if row[8].strip().upper() != "Y":
                continue
            out.append(
                UniverseRow(
                    row_number=idx,
                    symbol=row[1].strip().upper(),
                    exchange=(row[2].strip().upper() or "NSE"),
                    segment=(row[3].strip().upper() or "CASH"),
                    allowed_product=(row[4].strip().upper() or "BOTH"),
                    strategy_pref=(row[5].strip().upper() or "AUTO"),
                    sector=(row[6].strip() or "UNKNOWN"),
                    beta=float(row[7]) if row[7] else 1.0,
                    enabled=row[8].strip().upper(),
                    priority=float(row[9]) if len(row) > 9 and row[9] else 0.0,
                    notes=row[10] if len(row) > 10 else "",
                    score=float(row[11]) if len(row) > 11 and row[11] else 0.0,
                    last_rsi=float(row[12]) if len(row) > 12 and row[12] else 0.0,
                    last_vol_ratio=float(row[13]) if len(row) > 13 and row[13] else 0.0,
                    last_scanned=row[14] if len(row) > 14 else "",
                    last_product=(row[15].strip().upper() if len(row) > 15 else ""),
                    last_strategy=(row[16].strip().upper() if len(row) > 16 else ""),
                    last_note=row[17] if len(row) > 17 else "",
                    provider=(row[21].strip().upper() if len(row) > 21 else ""),
                    instrument_key=(row[22].strip() if len(row) > 22 else ""),
                    source_segment=(row[23].strip().upper() if len(row) > 23 else ""),
                    security_type=(row[24].strip().upper() if len(row) > 24 else ""),
                    score_calc=(row[25].strip() if len(row) > 25 else ""),
                )
            )
        return out

    def replace_universe_rows(self, rows: list[list[Any]]) -> None:
        self.clear_range(f"'{SheetNames.UNIVERSE}'!A4:ZZ")
        if rows:
            max_cols = max((len(r) for r in rows), default=26)
            self.ensure_sheet_grid_min(SheetNames.UNIVERSE, min_rows=max(1000, 4 + len(rows) + 5), min_cols=max(26, max_cols))
            self.update_values(f"'{SheetNames.UNIVERSE}'!A4", rows)

    def append_universe_rows(self, rows: list[list[Any]]) -> None:
        if rows:
            max_cols = max((len(r) for r in rows), default=26)
            self.ensure_sheet_grid_min(SheetNames.UNIVERSE, min_rows=1000, min_cols=max(26, max_cols))
            self.append_values(f"'{SheetNames.UNIVERSE}'!A4", rows)

    def read_universe_row_count_and_symbols(self) -> tuple[int, set[str]]:
        rows = self.read_sheet_rows(SheetNames.UNIVERSE, 4)
        count = 0
        symbols: set[str] = set()
        for row in rows:
            if len(row) < 2:
                continue
            symbol = row[1].strip().upper()
            if not symbol:
                continue
            count += 1
            symbols.add(symbol)
        return count, symbols

    def update_universe_score_columns(self, updates: list[tuple[int, list[Any]]]) -> int:
        if not updates:
            return 0
        data = []
        for row_num, vals in updates:
            if len(vals) >= 8:
                data.append({"range": f"'{SheetNames.UNIVERSE}'!L{row_num}:R{row_num}", "values": [vals[:7]]})
                data.append({"range": f"'{SheetNames.UNIVERSE}'!Z{row_num}", "values": [[vals[7]]]})
            else:
                data.append({"range": f"'{SheetNames.UNIVERSE}'!L{row_num}:R{row_num}", "values": [vals]})
        self.batch_update_values(data)
        return len(data)

    def append_decisions(self, rows: list[list[Any]]) -> None:
        self.append_rows(SheetNames.DECISIONS, rows)

    def append_actions(self, rows: list[list[Any]]) -> None:
        self.append_rows(SheetNames.ACTIONS, rows)

    def append_signals(self, rows: list[list[Any]]) -> None:
        self.append_rows(SheetNames.SIGNALS, rows)

    def append_orders(self, rows: list[list[Any]]) -> None:
        self.append_rows(SheetNames.ORDERS, rows)

    def append_positions(self, rows: list[list[Any]]) -> None:
        self.append_rows(SheetNames.POSITIONS, rows)

    def append_logs(self, rows: list[list[Any]]) -> None:
        # Legacy logs tab is deprecated. Mirror into Project Log shape so runtime visibility stays centralized.
        if not rows:
            return
        mapped: list[list[Any]] = []
        for row in rows:
            ts = row[0] if len(row) > 0 else ""
            level = str(row[1]) if len(row) > 1 else "INFO"
            fn = str(row[2]) if len(row) > 2 else "legacy"
            msg = str(row[3]) if len(row) > 3 else ""
            run_date = row[4] if len(row) > 4 else ""
            mapped.append([ts, "RuntimeLog", fn, level.upper(), msg, '{"kind":"legacy_log"}', run_date, ""])
        self.append_rows(SheetNames.ACTIONS, mapped)
