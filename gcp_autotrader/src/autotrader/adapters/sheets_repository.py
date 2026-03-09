from __future__ import annotations

import logging
import random
import ssl
import time
from dataclasses import dataclass
from typing import Any, Iterable

from autotrader.domain.models import MarketBrainState, MarketPolicy, RegimeSnapshot, UniverseRow, WatchlistRow
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
    SECTOR_MAPPING = "🗂️ Sector Mapping"
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

UNIVERSE_V1_DEPRECATED_HEADERS: tuple[str, ...] = (
    "Score",
    "RSI",
    "Vol Ratio",
    "Last Scanned",
    "Last Product",
    "Last Strategy",
    "Last Note",
    "Sc Calc",
)


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
            "MacroSector",
            "Sector",
            "Industry",
            "BasicIndustry",
            "SectorMapSource",
            "MaxCorrToSelected",
            "TurnoverMed60D",
            "ATR14",
            "CanonicalRegime",
            "RiskMode",
            "StructureState",
            "Participation",
            "SubRegimeV2",
            "Phase2Status",
            "RunDegradedFlag",
            "PolicyConfidence",
            "MarketConfidence",
            "BreadthConfidence",
            "LeadershipConfidence",
            "Phase2Confidence",
            "RunIntegrityConfidence",
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
            "Phase2Eligibility",
            "Phase2BaselineCoveragePct",
            "FallbackReason",
            "MacroSector",
            "Sector",
            "Industry",
            "BasicIndustry",
            "SectorMapSource",
            "MaxCorrToSelected",
            "TurnoverMed60D",
            "ATR14",
            "CanonicalRegime",
            "RiskMode",
            "StructureState",
            "Participation",
            "SubRegimeV2",
            "Phase2Status",
            "RunDegradedFlag",
            "PolicyConfidence",
            "MarketConfidence",
            "BreadthConfidence",
            "LeadershipConfidence",
            "Phase2Confidence",
            "RunIntegrityConfidence",
        ],
    ),
    SheetNames.SECTOR_MAPPING: SheetLayout(
        title="Sector Mapping - NSE symbol to 4-tier sector taxonomy",
        tab_name=SheetNames.SECTOR_MAPPING,
        headers=[
            "Symbol",
            "Exchange",
            "MacroSector",
            "Sector",
            "Industry",
            "BasicIndustry",
            "Source",
            "UpdatedAt",
        ],
    ),
    SheetNames.UNIVERSE: SheetLayout(
        title="Universe Instruments - Master list for smart watchlist generation",
        tab_name=SheetNames.UNIVERSE,
        headers=[
            "#", "Symbol", "Exchange", "Segment", "Allowed Product", "Strategy", "Sector", "Beta", "Enabled",
            "Priority", "Notes", "Raw CSV (JSON)", "Sector Source", "Sector Updated At",
            "Data Provider", "Instrument Key", "Source Segment", "Security Type",
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

    def _remove_columns_by_headers(self, sheet_name: str, headers_to_remove: Iterable[str], *, header_row: int = 3) -> int:
        targets = {str(h).strip() for h in headers_to_remove if str(h).strip()}
        if not targets:
            return 0
        grid = self._sheet_grid_meta().get(sheet_name)
        if not grid:
            return 0
        headers = self.read_sheet_headers(sheet_name, header_row=header_row)
        remove_idxs = [i for i, h in enumerate(headers, start=1) if str(h).strip() in targets]
        if not remove_idxs:
            return 0
        sheet_id = int(grid.get("sheetId") or 0)
        requests: list[dict[str, Any]] = []
        for idx in sorted(remove_idxs, reverse=True):
            requests.append(
                {
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": idx - 1,
                            "endIndex": idx,
                        }
                    }
                }
            )
        self._execute_with_retry(
            self._svc().spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={"requests": requests},
            ),
            op="sheet_delete_columns_by_header",
        )
        return len(remove_idxs)

    def get_values(self, a1_range: str) -> list[list[Any]]:
        res = self._execute_with_retry(
            self._values().get(spreadsheetId=self.spreadsheet_id, range=a1_range),
            op="values_get",
        )
        return res.get("values", [])

    def update_values(self, a1_range: str, values: list[list[Any]], *, value_input_option: str = "USER_ENTERED") -> None:
        self._execute_with_retry(
            self._values().update(
                spreadsheetId=self.spreadsheet_id,
                range=a1_range,
                valueInputOption=value_input_option,
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
            SheetNames.UNIVERSE, SheetNames.SECTOR_MAPPING, SheetNames.SCORE_CACHE_1D, SheetNames.SCORE_CACHE_5M, SheetNames.SCORE_CACHE_1D_DATA,
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

        if SheetNames.UNIVERSE in existing:
            try:
                self._remove_columns_by_headers(
                    SheetNames.UNIVERSE,
                    UNIVERSE_V1_DEPRECATED_HEADERS,
                    header_row=3,
                )
            except Exception:
                logger.debug("Unable to remove deprecated universe v1 score columns", exc_info=True)

        value_updates: list[dict[str, Any]] = []
        for name, layout in SHEET_LAYOUTS.items():
            if name not in existing:
                continue
            try:
                self.clear_range(f"'{name}'!A1:ZZ1")
                self.clear_range(f"'{name}'!A3:ZZ3")
            except Exception:
                logger.debug("Unable to clear title/header rows for sheet=%s", name, exc_info=True)
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
            self.update_values(f"'{SheetNames.WATCHLIST_INTRADAY_V2}'!A4", rows, value_input_option="RAW")

    def replace_watchlist_swing_v2(self, rows: list[list[Any]]) -> None:
        self.clear_range(f"'{SheetNames.WATCHLIST_SWING_V2}'!A4:ZZ")
        if rows:
            self.update_values(f"'{SheetNames.WATCHLIST_SWING_V2}'!A4", rows, value_input_option="RAW")

    def replace_watchlist_intraday_v2(self, rows: list[list[Any]]) -> None:
        self.clear_range(f"'{SheetNames.WATCHLIST_INTRADAY_V2}'!A4:ZZ")
        if rows:
            self.update_values(f"'{SheetNames.WATCHLIST_INTRADAY_V2}'!A4", rows, value_input_option="RAW")

    def replace_sector_mapping(self, rows: list[list[Any]]) -> None:
        self.clear_range(f"'{SheetNames.SECTOR_MAPPING}'!A4:ZZ")
        if rows:
            self.update_values(f"'{SheetNames.SECTOR_MAPPING}'!A4", rows)

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

    @staticmethod
    def _market_brain_bool(value: bool) -> str:
        return "Y" if bool(value) else "N"

    @staticmethod
    def _market_brain_reason_value(reasons: list[str], key: str) -> str:
        prefix = f"{str(key).strip()}="
        for item in reasons:
            token = str(item or "").strip()
            if token.startswith(prefix):
                return token.split("=", 1)[1].strip()
        return ""

    def _write_market_brain_table(self, *, title: str, rows: list[list[Any]]) -> None:
        body = [
            [title],
            ["GeneratedAtIST", now_ist_str()],
            [],
            ["Section", "Metric", "Value", "UsedByV2", "Notes"],
            *rows,
        ]
        self.ensure_sheet_grid_min(SheetNames.MARKET, min_rows=max(140, len(body) + 12), min_cols=5)
        self.clear_range(f"'{SheetNames.MARKET}'!A1:ZZ")
        self.update_values(f"'{SheetNames.MARKET}'!A1", body, value_input_option="RAW")

    def write_market_brain(self, regime: RegimeSnapshot) -> None:
        del regime
        self._write_market_brain_table(
            title="Market Brain V2 (Canonical Policy Engine)",
            rows=[
                [
                    "Compatibility",
                    "LegacyWriter",
                    "DEPRECATED",
                    "NO",
                    "Legacy market-brain v1 writer removed. Use write_market_brain_v2(state, policy).",
                ]
            ],
        )

    def write_market_brain_v2(self, state: MarketBrainState, policy: MarketPolicy) -> None:
        reasons = [str(x).strip() for x in (state.reasons or []) if str(x).strip()]
        reason_blob = " | ".join(reasons)
        policy_reasons = [str(x).strip() for x in (policy.reasons or []) if str(x).strip()]

        rows = [
            ["Canonical State", "AsOfTS", str(state.asof_ts or ""), "YES", "Single source-of-truth timestamp (IST ISO)"],
            ["Canonical State", "Phase", str(state.phase or ""), "YES", "PREMARKET/POST_OPEN/LIVE/EOD"],
            ["Canonical State", "Regime", str(state.regime or ""), "YES", "TREND_UP/TREND_DOWN/RANGE/CHOP/PANIC/RECOVERY"],
            ["Canonical State", "Participation", str(state.participation or ""), "YES", "STRONG/MODERATE/WEAK"],
            ["Canonical State", "RiskMode", str(state.risk_mode or ""), "YES", "AGGRESSIVE/NORMAL/DEFENSIVE/LOCKDOWN"],
            ["Canonical State", "IntradayState", str(state.intraday_state or ""), "YES", "PREOPEN/OPEN_DRIVE/OPEN_FADE/TREND_DAY/CHOP_DAY/EVENT_RISK"],
            ["Canonical State", "SubRegimeV2", str(state.sub_regime_v2 or ""), "YES", "Secondary market state (deterministic)"],
            ["Canonical State", "StructureState", str(state.structure_state or ""), "YES", "Structure classifier (narrow/mature/chop/etc.)"],
            ["Canonical State", "RecoveryState", str(state.recovery_state or ""), "YES", "Recovery lifecycle state"],
            ["Canonical State", "EventState", str(state.event_state or ""), "YES", "Deterministic event-risk context"],
            ["Canonical State", "LongBias", round(float(state.long_bias or 0.0), 4), "YES", "Portfolio long bias"],
            ["Canonical State", "ShortBias", round(float(state.short_bias or 0.0), 4), "YES", "Portfolio short bias"],
            ["Canonical State", "SizeMultiplier", round(float(state.size_multiplier or 0.0), 4), "YES", "Risk-per-trade multiplier"],
            ["Canonical State", "MaxPositionsMultiplier", round(float(state.max_positions_multiplier or 0.0), 4), "YES", "Concurrency multiplier"],
            ["Canonical State", "SwingPermission", str(state.swing_permission or ""), "YES", "ENABLED/REDUCED/DISABLED"],
            ["Canonical State", "RunDegradedFlag", self._market_brain_bool(bool(state.run_degraded_flag)), "YES", "Y when run intelligence is degraded"],
            ["Canonical Scores", "TrendScore", round(float(state.trend_score or 0.0), 2), "YES", "Index trend quality"],
            ["Canonical Scores", "BreadthScore", round(float(state.breadth_score or 0.0), 2), "YES", "Liquidity-qualified breadth quality"],
            ["Canonical Scores", "LeadershipScore", round(float(state.leadership_score or 0.0), 2), "YES", "Leader follow-through quality"],
            ["Canonical Scores", "VolatilityStressScore", round(float(state.volatility_stress_score or 0.0), 2), "YES", "Stress/risk pressure"],
            ["Canonical Scores", "LiquidityHealthScore", round(float(state.liquidity_health_score or 0.0), 2), "YES", "Opportunity quality by liquidity"],
            ["Canonical Scores", "DataQualityScore", round(float(state.data_quality_score or 0.0), 2), "YES", "Freshness/completeness guard"],
            ["Canonical Scores", "RiskAppetite", self._market_brain_reason_value(reasons, "appetite"), "YES", "Derived weighted appetite score"],
            ["Confidence", "MarketConfidence", round(float(state.market_confidence or 0.0), 2), "YES", "Market regime confidence"],
            ["Confidence", "BreadthConfidence", round(float(state.breadth_confidence or 0.0), 2), "YES", "Breadth confidence"],
            ["Confidence", "LeadershipConfidence", round(float(state.leadership_confidence or 0.0), 2), "YES", "Leadership confidence"],
            ["Confidence", "Phase2Confidence", round(float(state.phase2_confidence or 0.0), 2), "YES", "Intraday phase2 confidence"],
            ["Confidence", "PolicyConfidence", round(float(state.policy_confidence or 0.0), 2), "YES", "Policy confidence"],
            ["Confidence", "RunIntegrityConfidence", round(float(state.run_integrity_confidence or 0.0), 2), "YES", "Pipeline/run integrity confidence"],
            ["Canonical Policy", "PolicyRegime", str(policy.regime or ""), "YES", "Policy mapped from canonical state"],
            ["Canonical Policy", "PolicyRiskMode", str(policy.risk_mode or ""), "YES", "Policy mapped from canonical state"],
            ["Canonical Policy", "AllowedStrategies", "|".join(str(x) for x in (policy.allowed_strategies or [])), "YES", "Strategy families currently allowed"],
            ["Canonical Policy", "BreakoutEnabled", self._market_brain_bool(bool(policy.breakout_enabled)), "YES", "Breakout gating"],
            ["Canonical Policy", "OpenDriveEnabled", self._market_brain_bool(bool(policy.open_drive_enabled)), "YES", "Open-drive gating"],
            ["Canonical Policy", "IntradayPhase2Enabled", self._market_brain_bool(bool(policy.intraday_phase2_enabled)), "YES", "Phase2 gating"],
            ["Canonical Policy", "LongEnabled", self._market_brain_bool(bool(policy.long_enabled)), "YES", "Long side permission"],
            ["Canonical Policy", "ShortEnabled", self._market_brain_bool(bool(policy.short_enabled)), "YES", "Short side permission"],
            ["Canonical Policy", "SwingPermission", str(policy.swing_permission or ""), "YES", "Policy-level swing participation"],
            ["Canonical Policy", "SizeMultiplier", round(float(policy.size_multiplier or 0.0), 4), "YES", "Policy risk-per-trade multiplier"],
            ["Canonical Policy", "MaxPositionsMultiplier", round(float(policy.max_positions_multiplier or 0.0), 4), "YES", "Policy concurrency multiplier"],
            ["Canonical Policy", "WatchlistTargetMultiplier", round(float(policy.watchlist_target_multiplier or 0.0), 4), "YES", "Target size scaler for watchlist"],
            ["Canonical Policy", "WatchlistMinScoreBoost", int(policy.watchlist_min_score_boost or 0), "YES", "Minimum-score tightening"],
            ["Canonical Policy", "LiquidityBucketFloor", str(policy.liquidity_bucket_floor or ""), "YES", "Minimum liquidity bucket allowed"],
            ["Canonical Policy", "DynamicSectorCapShare", round(float(policy.dynamic_sector_cap_share or 0.0), 4), "YES", "Regime-aware diversification cap"],
            ["Canonical Policy", "CorrelationThreshold", round(float(policy.correlation_threshold or 0.0), 4), "YES", "Regime-aware correlation guard threshold"],
            ["Canonical Policy", "PolicyConfidence", round(float(policy.policy_confidence or 0.0), 2), "YES", "Policy confidence propagated to watchlist/scanner"],
            ["Canonical Diagnostics", "StateReasons", reason_blob, "YES", "Machine-readable state diagnostics"],
            ["Canonical Diagnostics", "PolicyReasons", " | ".join(policy_reasons), "YES", "Machine-readable policy diagnostics"],
            ["Canonical Diagnostics", "TrendReason", self._market_brain_reason_value(reasons, "trend"), "YES", "Trend score reason token"],
            ["Canonical Diagnostics", "BreadthReason", self._market_brain_reason_value(reasons, "breadth"), "YES", "Breadth score reason token"],
            ["Canonical Diagnostics", "LeadershipReason", self._market_brain_reason_value(reasons, "leadership"), "YES", "Leadership score reason token"],
            ["Canonical Diagnostics", "StressReason", self._market_brain_reason_value(reasons, "stress"), "YES", "Stress score reason token"],
            ["Canonical Diagnostics", "LiquidityReason", self._market_brain_reason_value(reasons, "liq"), "YES", "Liquidity score reason token"],
            ["Canonical Diagnostics", "DataQualityReason", self._market_brain_reason_value(reasons, "dataQ"), "YES", "Data-quality reason token"],
            ["Compatibility", "LegacyRegimeSheetUsed", "NO", "NO", "Legacy B4:G63 dashboard removed; this V2 table is canonical"],
        ]
        self._write_market_brain_table(
            title="Market Brain V2 (Canonical Policy Engine)",
            rows=rows,
        )

    def read_universe_rows(self) -> list[UniverseRow]:
        header_map = self.read_sheet_headers(SheetNames.UNIVERSE, header_row=3)
        h2i: dict[str, int] = {}
        for i, h in enumerate(header_map, start=1):
            key = str(h).strip()
            if key and key not in h2i:
                h2i[key] = i

        def _col(name: str, default: int) -> int:
            return int(h2i.get(name, default))

        col_symbol = _col("Symbol", 2)
        col_exchange = _col("Exchange", 3)
        col_segment = _col("Segment", 4)
        col_allowed_product = _col("Allowed Product", 5)
        col_strategy = _col("Strategy", 6)
        col_sector = _col("Sector", 7)
        col_beta = _col("Beta", 8)
        col_enabled = _col("Enabled", 9)
        col_priority = _col("Priority", 10)
        col_notes = _col("Notes", 11)
        col_provider = _col("Data Provider", 15)
        col_instrument_key = _col("Instrument Key", 16)
        col_source_segment = _col("Source Segment", 17)
        col_security_type = _col("Security Type", 18)

        rows = self.read_sheet_rows(SheetNames.UNIVERSE, 4)
        out: list[UniverseRow] = []
        for idx, row in enumerate(rows, start=4):
            if len(row) < col_symbol or not row[col_symbol - 1].strip():
                continue
            if len(row) < col_enabled or row[col_enabled - 1].strip().upper() != "Y":
                continue
            out.append(
                UniverseRow(
                    row_number=idx,
                    symbol=row[col_symbol - 1].strip().upper(),
                    exchange=(row[col_exchange - 1].strip().upper() if len(row) >= col_exchange else "NSE") or "NSE",
                    segment=(row[col_segment - 1].strip().upper() if len(row) >= col_segment else "CASH") or "CASH",
                    allowed_product=(row[col_allowed_product - 1].strip().upper() if len(row) >= col_allowed_product else "BOTH") or "BOTH",
                    strategy_pref=(row[col_strategy - 1].strip().upper() if len(row) >= col_strategy else "AUTO") or "AUTO",
                    sector=(row[col_sector - 1].strip() if len(row) >= col_sector else "") or "UNKNOWN",
                    beta=float(row[col_beta - 1]) if len(row) >= col_beta and row[col_beta - 1] else 1.0,
                    enabled=(row[col_enabled - 1].strip().upper() if len(row) >= col_enabled else "Y"),
                    priority=float(row[col_priority - 1]) if len(row) >= col_priority and row[col_priority - 1] else 0.0,
                    notes=row[col_notes - 1] if len(row) >= col_notes else "",
                    provider=(row[col_provider - 1].strip().upper() if len(row) >= col_provider else ""),
                    instrument_key=(row[col_instrument_key - 1].strip() if len(row) >= col_instrument_key else ""),
                    source_segment=(row[col_source_segment - 1].strip().upper() if len(row) >= col_source_segment else ""),
                    security_type=(row[col_security_type - 1].strip().upper() if len(row) >= col_security_type else ""),
                )
            )
        return out

    def replace_universe_rows(self, rows: list[list[Any]]) -> None:
        self.clear_range(f"'{SheetNames.UNIVERSE}'!A4:ZZ")
        if rows:
            base_cols = len(SHEET_LAYOUTS[SheetNames.UNIVERSE].headers)
            max_cols = max((len(r) for r in rows), default=base_cols)
            self.ensure_sheet_grid_min(SheetNames.UNIVERSE, min_rows=max(1000, 4 + len(rows) + 5), min_cols=max(base_cols, max_cols))
            self.update_values(f"'{SheetNames.UNIVERSE}'!A4", rows)

    def append_universe_rows(self, rows: list[list[Any]]) -> None:
        if rows:
            base_cols = len(SHEET_LAYOUTS[SheetNames.UNIVERSE].headers)
            max_cols = max((len(r) for r in rows), default=base_cols)
            self.ensure_sheet_grid_min(SheetNames.UNIVERSE, min_rows=1000, min_cols=max(base_cols, max_cols))
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
