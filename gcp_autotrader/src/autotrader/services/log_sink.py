from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any

from autotrader.time_utils import now_ist_str, today_ist

logger = logging.getLogger(__name__)


# Module-level default BQ adapter, set by Container at boot. LogSink instances
# created without an explicit `bq=` (the common case across jobs.py / web/api.py)
# fall back to this so audit-log persistence is wired up project-wide without
# touching every construction site.
#
# Discovered 2026-05-07: `flush_actions()` was clearing its buffer without
# persisting, which left the BQ `audit_log` table empty since launch. Without
# the persistence path, dashboards reading `audit_log` showed 0 rows even
# though jobs were calling `sink.action()` constantly.
_DEFAULT_BQ: Any = None


def set_default_bq(bq: Any) -> None:
    """Wire a BQ adapter that LogSink instances will use by default."""
    global _DEFAULT_BQ
    _DEFAULT_BQ = bq


@dataclass
class LogSink:
    exec_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12].upper())
    decision_buffer: list[list[Any]] = field(default_factory=list)
    action_buffer: list[list[Any]] = field(default_factory=list)
    log_buffer: list[list[Any]] = field(default_factory=list)
    context_char_limit: int = 45000
    # Optional BQ adapter for persisting actions to the `audit_log` table.
    # When None, falls back to the module-level _DEFAULT_BQ (set by Container).
    # Tests construct LogSink() with no bq and never wire _DEFAULT_BQ, so the
    # writes silently no-op — matching the legacy behaviour for unit tests.
    bq: Any = None

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

    def _resolve_bq(self) -> Any:
        return self.bq if self.bq is not None else _DEFAULT_BQ

    def flush_decisions(self) -> None:
        # Decisions are written to the `scan_decisions` table by
        # trading_service.run_scan_once() directly via insert_scan_decisions_batch.
        # The decision_buffer here is a UI/log artefact only — flushing means
        # discarding it to bound memory.
        self.decision_buffer.clear()

    def flush_actions(self) -> None:
        """Persist any buffered actions to the BQ audit_log table, then clear.

        Buffer rows are produced by `action()` as positional lists matching:
            [log_ts, module, action, status, message, ctx_json, run_date, exec_id]
        We translate to dicts here and call insert_audit_log_batch. _insert
        already swallows errors and logs them, so this never raises into the
        caller's code path.
        """
        if not self.action_buffer:
            return
        bq = self._resolve_bq()
        if bq is not None:
            try:
                rows = [
                    {
                        "log_ts": entry[0],
                        "module": entry[1],
                        "action": entry[2],
                        "status": entry[3],
                        "message": entry[4],
                        "context": entry[5],
                        "run_date": entry[6],
                        "exec_id": entry[7],
                    }
                    for entry in self.action_buffer
                ]
                bq.insert_audit_log_batch(rows)
            except Exception:
                # Defensive: bq.insert_audit_log_batch already swallows + logs
                # internally, but if buffer entries are malformed we don't want
                # this to take down the job that was just trying to log.
                logger.exception("audit_log_flush_failed buffer_size=%d", len(self.action_buffer))
        self.action_buffer.clear()

    def flush_logs(self) -> None:
        self.log_buffer.clear()

    def flush_all(self) -> None:
        # Order: actions first (the ones that need persisting), then drop the
        # rest. flush_actions handles the BQ write; the others are local-only.
        self.flush_actions()
        self.decision_buffer.clear()
        self.log_buffer.clear()
