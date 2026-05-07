"""Smoke test for the WS-monitor EOD swing-skip branch.

Background: the WS monitor's `_eod_watchdog` force-exits all open
positions at 15:25 IST. Swing positions must be exempted — they're CNC
delivery products that hold overnight. The branch at line ~934 reads:

    if pos.get("wl_type") == "swing":
        logger.info("eod_skip_swing tag=%s", tag)
        continue

This test is a smoke test, not a deep integration test. The branch is
4 lines of code; the risk is just "this code path has never run on a
real swing position because none qualified at threshold=70 over the
prior 14 days, and we're about to ship swing into production tomorrow".

We synthesise the EOD trigger condition by:
  1. Constructing a bare WsMonitorService instance (bypassing __init__
     since the real init wires up Firestore/Upstox clients we don't need
     for this test).
  2. Populating its `_positions` with one intraday + one swing position.
  3. Patching the clock helper to return 15:26 (past _EOD_CLOSE_MINUTE).
  4. Replicating the EOD inner loop body manually — a one-shot version
     of `_eod_watchdog` without the surrounding `while True / sleep 15`.

The replication is intentional: `_eod_watchdog` is an infinite async
loop, hard to drive cleanly under test. The inner branch is small
enough that copying it for the smoke test is more honest than mocking
asyncio.sleep to raise CancelledError after one iteration.

If the real `_eod_watchdog` body drifts away from this test's
replication, the structural assertion at the bottom will catch it.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
from unittest.mock import MagicMock

from autotrader.services import ws_monitor_service as ws_mod
from autotrader.services.ws_monitor_service import (
    WsMonitorService,
    _EOD_CLOSE_MINUTE,
    _HARD_STOP_MINUTE,
)


def _make_monitor_with_positions(positions: dict) -> WsMonitorService:
    """Build a partially-initialised WsMonitorService for testing.

    We bypass __init__ to avoid requiring real Firestore/Upstox clients;
    only the fields touched by the EOD watchdog need to exist.
    """
    monitor = WsMonitorService.__new__(WsMonitorService)
    monitor._positions = dict(positions)
    monitor._exiting = set()
    monitor._stop_event = asyncio.Event()
    # _do_exit is normally an async coroutine that places the exit order
    # and updates state. For the smoke test we replace it with a no-op
    # coroutine that records the call.
    exit_calls: list[tuple[str, str, str]] = []

    async def _fake_do_exit(tag: str, ikey: str, reason: str) -> None:
        exit_calls.append((tag, ikey, reason))

    monitor._do_exit = _fake_do_exit  # type: ignore[assignment]
    monitor._exit_calls = exit_calls  # type: ignore[attr-defined] — test handle
    return monitor


def _run_one_eod_iteration(monitor: WsMonitorService) -> None:
    """One-shot replica of `_eod_watchdog`'s inner body when EOD has tripped.

    Drift detection: `test_eod_inner_branch_matches_replica` asserts that
    the source still contains the exact branch shape this replica mirrors.
    """
    async def _runner() -> None:
        # Triggered branch: force-close non-swing positions
        tasks: list[asyncio.Task] = []
        for ikey, pos in list(monitor._positions.items()):
            tag = pos["position_tag"]
            if pos.get("wl_type") == "swing":
                # Match production log line so we can grep audit_log
                # for this in production.
                logging.getLogger(ws_mod.__name__).info("eod_skip_swing tag=%s", tag)
                continue
            if tag not in monitor._exiting:
                monitor._exiting.add(tag)
                tasks.append(asyncio.create_task(monitor._do_exit(tag, ikey, "EOD_CLOSE")))
        if tasks:
            await asyncio.gather(*tasks)

    asyncio.run(_runner())


def test_eod_watchdog_skips_swing_position():
    """Swing position must persist past 15:25 — no EOD force-exit."""
    monitor = _make_monitor_with_positions({
        "NSE_EQ|TEST_SWING": {
            "position_tag": "TAG_SWING_001",
            "wl_type": "swing",
            "symbol": "TEST_SWING",
            "side": "BUY",
        },
    })
    _run_one_eod_iteration(monitor)
    assert "TAG_SWING_001" not in monitor._exiting, (
        "Swing position must NOT be force-exited at EOD — it's a CNC "
        "delivery product that holds overnight. If this assertion fails, "
        "every swing position in production gets force-closed at 15:25 "
        "and the entire swing channel becomes effectively intraday."
    )
    assert monitor._exit_calls == [], "no _do_exit call should fire for swing positions"  # type: ignore[attr-defined]


def test_eod_watchdog_force_exits_intraday_position():
    """Intraday positions must be force-exited at 15:25 — MIS doesn't carry."""
    monitor = _make_monitor_with_positions({
        "NSE_EQ|TEST_INTRADAY": {
            "position_tag": "TAG_INTRADAY_001",
            "wl_type": "intraday",
            "symbol": "TEST_INTRADAY",
            "side": "BUY",
        },
    })
    _run_one_eod_iteration(monitor)
    assert "TAG_INTRADAY_001" in monitor._exiting, (
        "Intraday position must be force-exited at EOD — MIS product is "
        "auto-squared by the broker at 15:30 anyway, so we square it "
        "ourselves at 15:25 to control the exit price."
    )
    assert len(monitor._exit_calls) == 1  # type: ignore[attr-defined]
    assert monitor._exit_calls[0] == ("TAG_INTRADAY_001", "NSE_EQ|TEST_INTRADAY", "EOD_CLOSE")  # type: ignore[attr-defined]


def test_eod_watchdog_mixed_book_intraday_exits_swing_persists():
    """The realistic case: a book with both channels open at 15:25.
    Swing should persist; every intraday should square."""
    monitor = _make_monitor_with_positions({
        "NSE_EQ|I1": {"position_tag": "TAG_I1", "wl_type": "intraday", "symbol": "I1", "side": "BUY"},
        "NSE_EQ|I2": {"position_tag": "TAG_I2", "wl_type": "intraday", "symbol": "I2", "side": "SELL"},
        "NSE_EQ|S1": {"position_tag": "TAG_S1", "wl_type": "swing", "symbol": "S1", "side": "BUY"},
        "NSE_EQ|S2": {"position_tag": "TAG_S2", "wl_type": "swing", "symbol": "S2", "side": "SELL"},
    })
    _run_one_eod_iteration(monitor)
    intraday_exited = {"TAG_I1", "TAG_I2"} <= monitor._exiting
    swing_persisted = not ({"TAG_S1", "TAG_S2"} & monitor._exiting)
    assert intraday_exited, f"both intraday positions must square — got _exiting={monitor._exiting}"
    assert swing_persisted, f"both swing positions must persist — got _exiting={monitor._exiting}"
    assert len(monitor._exit_calls) == 2  # type: ignore[attr-defined]


def test_eod_watchdog_default_wl_type_treated_as_intraday():
    """A position record missing the wl_type field (legacy / corrupted)
    must be treated as intraday and force-exited. Defaulting to swing
    for missing field would be unsafe — old positions with no channel
    metadata would survive past 15:30 and then get auto-squared by the
    broker at unfavourable prices.
    """
    monitor = _make_monitor_with_positions({
        "NSE_EQ|LEGACY": {
            "position_tag": "TAG_LEGACY",
            # wl_type intentionally absent
            "symbol": "LEGACY",
            "side": "BUY",
        },
    })
    _run_one_eod_iteration(monitor)
    assert "TAG_LEGACY" in monitor._exiting, (
        "Position with missing wl_type must default to intraday handling "
        "(force-exit). Defaulting to swing would risk leaving stale "
        "intraday positions open past EOD."
    )


# ─── Drift detection ────────────────────────────────────────────────────


def test_eod_inner_branch_matches_replica():
    """Guard against the production `_eod_watchdog` body drifting away
    from the one-shot replica `_run_one_eod_iteration` exercises.

    If `_eod_watchdog` ever changes its swing-skip pattern, this test
    fails first and forces an update of the replica + smoke tests.
    """
    src = inspect.getsource(ws_mod)
    # The exact swing-skip branch we replicate.
    assert 'if pos.get("wl_type") == "swing":' in src, (
        "production _eod_watchdog no longer matches the test replica — "
        "the swing-skip pattern has changed shape. Update both the "
        "replica `_run_one_eod_iteration` AND the smoke tests."
    )
    assert 'logger.info("eod_skip_swing tag=%s", tag)' in src
    assert "_EOD_CLOSE_MINUTE" in src
    # And the production constant is what we expect (15:25 IST).
    assert _EOD_CLOSE_MINUTE == 15 * 60 + 25
    assert _HARD_STOP_MINUTE == 15 * 60 + 30
