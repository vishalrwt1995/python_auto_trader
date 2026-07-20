"""Smoke test for the WS-monitor EOD overnight-skip branch.

Background: the WS monitor's `_eod_watchdog` force-exits all open positions at 15:25 IST.
OVERNIGHT SL-only holds must be exempted — CNC delivery products that hold overnight:
swing AND the EVENT channel (pead earnings-drift + corp_action bonus/split). The branch
at ~line 960 reads:

    if _is_overnight_sl_only(pos):          # wl_type in {swing, pead, corp_action}
        logger.info("eod_skip_overnight tag=%s wl_type=%s", tag, pos.get("wl_type"))
        continue

2026-06-20: broadened from swing-only to the overnight set. Previously pead/corp_action
would have been EOD-squared (and intraday-managed) — wrong for a multi-day hold. No live
pead/corp position existed yet, so swing/intraday behaviour is unchanged.

This is a smoke test, not a deep integration test. We synthesise the EOD trigger and
replicate the inner loop body; the drift assertion at the bottom catches production drift.
"""
from __future__ import annotations

import asyncio
import inspect
import logging

from autotrader.services import ws_monitor_service as ws_mod
from autotrader.services.ws_monitor_service import (
    WsMonitorService,
    _EOD_CLOSE_MINUTE,
    _HARD_STOP_MINUTE,
)


def _make_monitor_with_positions(positions: dict) -> WsMonitorService:
    """Build a partially-initialised WsMonitorService (bypass __init__; only the fields
    the EOD watchdog touches need to exist)."""
    monitor = WsMonitorService.__new__(WsMonitorService)
    monitor._positions = dict(positions)
    monitor._exiting = set()
    monitor._stop_event = asyncio.Event()
    exit_calls: list[tuple[str, str, str]] = []

    async def _fake_do_exit(tag: str, ikey: str, reason: str) -> None:
        exit_calls.append((tag, ikey, reason))

    monitor._do_exit = _fake_do_exit  # type: ignore[assignment]
    monitor._exit_calls = exit_calls  # type: ignore[attr-defined]
    return monitor


def _run_one_eod_iteration(monitor: WsMonitorService) -> None:
    """One-shot replica of `_eod_watchdog`'s inner body when EOD has tripped. Uses the
    production `_is_overnight_sl_only` classifier so the smoke tests exercise the real
    decision. `test_eod_inner_branch_matches_replica` guards against production drift."""
    async def _runner() -> None:
        tasks: list[asyncio.Task] = []
        for ikey, pos in list(monitor._positions.items()):
            tag = pos["position_tag"]
            if ws_mod._is_overnight_sl_only(pos):
                logging.getLogger(ws_mod.__name__).info(
                    "eod_skip_overnight tag=%s wl_type=%s", tag, pos.get("wl_type"))
                continue
            if tag not in monitor._exiting:
                monitor._exiting.add(tag)
                tasks.append(asyncio.create_task(monitor._do_exit(tag, ikey, "EOD_CLOSE")))
        if tasks:
            await asyncio.gather(*tasks)

    asyncio.run(_runner())


def _pos(tag, wl_type=None, side="BUY"):
    d = {"position_tag": tag, "symbol": tag, "side": side}
    if wl_type is not None:
        d["wl_type"] = wl_type
    return d


def test_eod_watchdog_skips_swing_position():
    monitor = _make_monitor_with_positions({"NSE_EQ|S": _pos("TAG_SWING", "swing")})
    _run_one_eod_iteration(monitor)
    assert "TAG_SWING" not in monitor._exiting
    assert monitor._exit_calls == []  # type: ignore[attr-defined]


def test_eod_watchdog_skips_pead_position():
    """EVENT/PEAD positions hold for weeks — must NOT be EOD-squared (2026-06-20 fix)."""
    monitor = _make_monitor_with_positions({"NSE_EQ|P": _pos("TAG_PEAD", "pead")})
    _run_one_eod_iteration(monitor)
    assert "TAG_PEAD" not in monitor._exiting
    assert monitor._exit_calls == []  # type: ignore[attr-defined]


def test_eod_watchdog_skips_corp_action_position():
    """Corp-action positions hold to the meeting (~3 days) — must NOT be EOD-squared."""
    monitor = _make_monitor_with_positions({"NSE_EQ|C": _pos("TAG_CORP", "corp_action")})
    _run_one_eod_iteration(monitor)
    assert "TAG_CORP" not in monitor._exiting
    assert monitor._exit_calls == []  # type: ignore[attr-defined]


def test_eod_watchdog_force_exits_intraday_position():
    monitor = _make_monitor_with_positions({"NSE_EQ|I": _pos("TAG_INTRADAY", "intraday")})
    _run_one_eod_iteration(monitor)
    assert "TAG_INTRADAY" in monitor._exiting
    assert monitor._exit_calls == [("TAG_INTRADAY", "NSE_EQ|I", "EOD_CLOSE")]  # type: ignore[attr-defined]


def test_eod_watchdog_mixed_book():
    """Realistic 15:25 book: intraday squares; swing + pead + corp persist."""
    monitor = _make_monitor_with_positions({
        "NSE_EQ|I1": _pos("TAG_I1", "intraday"),
        "NSE_EQ|S1": _pos("TAG_S1", "swing"),
        "NSE_EQ|P1": _pos("TAG_P1", "pead"),
        "NSE_EQ|C1": _pos("TAG_C1", "corp_action"),
    })
    _run_one_eod_iteration(monitor)
    assert "TAG_I1" in monitor._exiting
    assert not ({"TAG_S1", "TAG_P1", "TAG_C1"} & monitor._exiting)  # all overnight persist
    assert len(monitor._exit_calls) == 1  # type: ignore[attr-defined]


def test_eod_watchdog_default_wl_type_treated_as_intraday():
    """Missing wl_type must default to intraday (force-exit) — never to an overnight hold."""
    monitor = _make_monitor_with_positions({"NSE_EQ|L": _pos("TAG_LEGACY", wl_type=None)})
    _run_one_eod_iteration(monitor)
    assert "TAG_LEGACY" in monitor._exiting


# ─── Drift detection ────────────────────────────────────────────────────


def test_eod_inner_branch_matches_replica():
    """Guard against the production `_eod_watchdog` body drifting from the replica."""
    src = inspect.getsource(ws_mod)
    assert "if _is_overnight_sl_only(pos):" in src, (
        "production _eod_watchdog no longer matches the test replica — the overnight-skip "
        "pattern changed. Update both `_run_one_eod_iteration` AND the smoke tests."
    )
    assert 'logger.info("eod_skip_overnight tag=%s wl_type=%s", tag, pos.get("wl_type"))' in src
    assert "_OVERNIGHT_SL_ONLY_WL = frozenset({\"swing\", \"pead\", \"corp_action\", \"core\", \"momentum\", \"delivery\", \"insider\"})" in src
    assert "_EOD_CLOSE_MINUTE" in src
    assert _EOD_CLOSE_MINUTE == 15 * 60 + 25
    assert _HARD_STOP_MINUTE == 15 * 60 + 30
