from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any

from autotrader.adapters.sheets_repository import GoogleSheetsRepository, SheetNames
from autotrader.time_utils import now_ist_str, today_ist

logger = logging.getLogger(__name__)


@dataclass
class LogSink:
    sheets: GoogleSheetsRepository
    exec_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12].upper())
    decision_buffer: list[list[Any]] = field(default_factory=list)
    action_buffer: list[list[Any]] = field(default_factory=list)
    log_buffer: list[list[Any]] = field(default_factory=list)
    context_char_limit: int = 45000

    def _ctx_json(self, ctx: dict[str, Any] | None) -> str:
        payload = json.dumps(ctx or {}, separators=(",", ":"), default=str)
        if len(payload) <= int(self.context_char_limit):
            return payload
        trunc = "...<truncated>"
        keep = max(0, int(self.context_char_limit) - len(trunc))
        return payload[:keep] + trunc

    def decision(self, stage: str, symbol: str, decision: str, reason: str, ctx: dict[str, Any] | None = None) -> None:
        ctx_json = self._ctx_json(ctx)
        logger.info(
            "decision stage=%s symbol=%s decision=%s reason=%s ctx=%s execId=%s",
            stage,
            symbol,
            decision,
            reason,
            ctx_json,
            self.exec_id,
        )
        self.decision_buffer.append([
            now_ist_str(), str(stage), str(symbol), str(decision), str(reason),
            ctx_json, today_ist(),
        ])
        if len(self.decision_buffer) >= 20:
            self.flush_decisions()

    def action(self, module: str, action: str, status: str, message: str = "", ctx: dict[str, Any] | None = None) -> None:
        ctx_json = self._ctx_json(ctx)
        logger.info(
            "action module=%s action=%s status=%s message=%s ctx=%s execId=%s",
            module,
            action,
            status,
            message,
            ctx_json,
            self.exec_id,
        )
        self.action_buffer.append([
            now_ist_str(), module, action, status, message,
            ctx_json, today_ist(), self.exec_id,
        ])
        if len(self.action_buffer) >= 20:
            self.flush_actions()

    def log(self, level: str, fn: str, msg: str) -> None:
        py_level = getattr(logging, level.upper(), logging.INFO)
        logger.log(py_level, "%s: %s", fn, msg)
        # Consolidate operational logs into Project Log so scheduling/runtime issues are visible in one place.
        self.action("RuntimeLog", str(fn), str(level).upper(), str(msg), {"kind": "log"})

    def flush_decisions(self) -> None:
        if not self.decision_buffer:
            return
        try:
            self.sheets.append_rows(SheetNames.DECISIONS, self.decision_buffer)
        except Exception:
            logger.exception("log_sink_flush_decisions_failed execId=%s rows=%s", self.exec_id, len(self.decision_buffer))
            return
        self.decision_buffer.clear()

    def flush_actions(self) -> None:
        if not self.action_buffer:
            return
        try:
            self.sheets.append_rows(SheetNames.ACTIONS, self.action_buffer)
        except Exception:
            logger.exception("log_sink_flush_actions_failed execId=%s rows=%s", self.exec_id, len(self.action_buffer))
            return
        self.action_buffer.clear()

    def flush_logs(self) -> None:
        if not self.log_buffer:
            return
        # Migrate any legacy buffered log rows into Project Log shape instead of writing to the Logs tab.
        for row in self.log_buffer:
            ts = row[0] if len(row) > 0 else now_ist_str()
            level = row[1] if len(row) > 1 else "INFO"
            fn = row[2] if len(row) > 2 else "unknown"
            msg = row[3] if len(row) > 3 else ""
            run_date = row[4] if len(row) > 4 else today_ist()
            self.action_buffer.append([ts, "RuntimeLog", str(fn), str(level).upper(), str(msg), '{"kind":"log_legacy"}', run_date, self.exec_id])
        self.log_buffer.clear()
        if len(self.action_buffer) >= 20:
            self.flush_actions()

    def flush_all(self) -> None:
        try:
            self.flush_decisions()
            self.flush_logs()
            self.flush_actions()
        except Exception:
            logger.exception("log_sink_flush_all_failed execId=%s", self.exec_id)
