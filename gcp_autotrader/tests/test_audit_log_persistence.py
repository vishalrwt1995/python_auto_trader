"""Tests for the 2026-05-07 audit_log persistence fix.

Background: `LogSink.flush_actions()` was originally a no-op that just
cleared the buffer. As a result, the BQ `audit_log` table stayed empty
since launch — every job called `sink.action(...)` but nothing
persisted, so the dashboard's audit-trail view showed 0 rows.

These tests guard against the regression by verifying:
  1. flush_actions() routes the buffered rows through bq.insert_audit_log_batch
  2. flush_actions() is a no-op when the buffer is empty (no spurious calls)
  3. The auto-flush at 20 buffer entries triggers a BQ write
  4. flush_all() also persists actions (not just decisions/logs)
  5. The module-level default-BQ injection works for `LogSink()` with no args
  6. The buffer-row → dict translation matches the audit_log schema field names
"""
from __future__ import annotations

from typing import Any, List
from unittest.mock import MagicMock

from autotrader.services import log_sink as log_sink_mod
from autotrader.services.log_sink import LogSink, set_default_bq


class _FakeBQ:
    """Minimal BQ stand-in that records insert_audit_log_batch calls."""

    def __init__(self) -> None:
        self.batches: List[List[dict[str, Any]]] = []

    def insert_audit_log_batch(self, rows: list[dict[str, Any]]) -> None:
        # Defensive copy so test assertions see what was sent at the moment
        # of the call, not the buffer state after subsequent mutations.
        self.batches.append([dict(r) for r in rows])


def _reset_default_bq() -> None:
    """Restore module-level default to None between tests."""
    set_default_bq(None)


def test_flush_actions_writes_to_bq():
    bq = _FakeBQ()
    sink = LogSink(bq=bq)
    sink.action("Universe", "sync", "START", "", {"limit": 100})
    sink.action("Universe", "sync", "DONE", "synced", {"rows": 50})

    assert len(bq.batches) == 0, "must not write until flush"
    sink.flush_actions()
    assert len(bq.batches) == 1, "expected exactly one BQ batch"
    assert len(bq.batches[0]) == 2, "expected both buffered actions"
    assert sink.action_buffer == [], "buffer must be cleared post-flush"


def test_flush_actions_empty_buffer_is_noop():
    bq = _FakeBQ()
    sink = LogSink(bq=bq)
    sink.flush_actions()
    assert bq.batches == [], "empty flush must not call BQ"


def test_auto_flush_at_threshold_writes_to_bq():
    bq = _FakeBQ()
    sink = LogSink(bq=bq)
    # action() auto-flushes when buffer reaches 20.
    for i in range(20):
        sink.action("M", f"a{i}", "OK", "", None)
    assert len(bq.batches) == 1, "20-entry threshold must auto-flush"
    assert len(bq.batches[0]) == 20
    assert sink.action_buffer == []


def test_flush_all_persists_actions():
    bq = _FakeBQ()
    sink = LogSink(bq=bq)
    sink.action("Universe", "sync", "DONE", "ok", None)
    sink.flush_all()
    assert len(bq.batches) == 1, "flush_all must persist actions, not just clear"
    assert sink.action_buffer == []


def test_module_default_bq_used_when_no_explicit_arg():
    """Critical for jobs.py / web/api.py which call LogSink() with no args."""
    bq = _FakeBQ()
    set_default_bq(bq)
    try:
        sink = LogSink()  # no explicit bq=
        assert sink.bq is None, "explicit field stays None — fallback is via _resolve_bq"
        sink.action("Job", "run", "DONE", "ok", None)
        sink.flush_actions()
        assert len(bq.batches) == 1, (
            "LogSink() without explicit bq must fall back to module default — "
            "this is the wiring that makes 32 call sites in jobs.py / api.py "
            "actually persist."
        )
    finally:
        _reset_default_bq()


def test_no_bq_no_writes_no_crash():
    """When neither explicit bq nor default is set, flush must silently no-op."""
    _reset_default_bq()
    sink = LogSink()
    sink.action("M", "a", "OK", "", None)
    sink.flush_actions()  # must not raise even though nowhere to write
    assert sink.action_buffer == []


def test_buffer_row_to_dict_schema_matches_audit_log():
    """The buffer-row → dict translation must produce exactly the field names
    in the BQ audit_log schema (log_ts, run_date, module, action, status,
    message, context, exec_id). Drift here = silent column-mismatch insert
    failures in BQ that look like 0-row writes from the caller's perspective.
    """
    bq = _FakeBQ()
    sink = LogSink(bq=bq, exec_id="DEADBEEF1234")
    sink.action("Universe", "sync", "DONE", "synced", {"rows": 5})
    sink.flush_actions()

    assert len(bq.batches) == 1 and len(bq.batches[0]) == 1
    row = bq.batches[0][0]
    expected_keys = {"log_ts", "run_date", "module", "action", "status", "message", "context", "exec_id"}
    assert set(row.keys()) == expected_keys, (
        f"audit_log row keys drifted from BQ schema. Got {set(row.keys())}, expected {expected_keys}. "
        "If you renamed a column in bq_setup.py:audit_log, update LogSink.flush_actions to match."
    )
    # Spot-check field values — particularly that we didn't swap module<->action
    assert row["module"] == "Universe"
    assert row["action"] == "sync"
    assert row["status"] == "DONE"
    assert row["message"] == "synced"
    assert row["exec_id"] == "DEADBEEF1234"
    # context is the JSON-serialised ctx string (not a dict)
    assert isinstance(row["context"], str)
    assert '"rows":5' in row["context"]


def test_bq_exception_does_not_propagate():
    """If insert_audit_log_batch raises, the caller's job must continue.

    The existing _insert in BigQueryClient already swallows errors, but the
    contract here is stricter: even if someone passes a buggy bq stub,
    flush_actions must not crash the surrounding job.
    """
    class _RaisingBQ:
        def insert_audit_log_batch(self, rows):
            raise RuntimeError("simulated BQ outage")

    sink = LogSink(bq=_RaisingBQ())
    sink.action("M", "a", "OK", "", None)
    # If this raises, caller jobs would crash on every flush during a BQ
    # outage. flush_actions must catch + log, not propagate.
    sink.flush_actions()
    assert sink.action_buffer == [], "buffer must still be cleared even on BQ failure"


def test_default_bq_can_be_overridden_by_explicit_arg():
    """An explicit `bq=` should take precedence over the module default."""
    default_bq = _FakeBQ()
    explicit_bq = _FakeBQ()
    set_default_bq(default_bq)
    try:
        sink = LogSink(bq=explicit_bq)
        sink.action("M", "a", "OK", "", None)
        sink.flush_actions()
        assert len(explicit_bq.batches) == 1, "explicit bq must receive the write"
        assert len(default_bq.batches) == 0, "module default must NOT be used when explicit bq is set"
    finally:
        _reset_default_bq()
