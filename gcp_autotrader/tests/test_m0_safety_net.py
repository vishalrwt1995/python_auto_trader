"""Tests for M0 — Safety net (redesign).

Covers the M0 milestone items committed to redesign/audit-and-design:

M0.1 Kill-switch primitive in FirestoreStateStore + scan-loop gate
     (fail-closed: read errors treat as ACTIVE so a DB outage halts trading).

M0.2 Fail-closed risk-cap reads — the four previously silent-continue paths
     in trading_service.run_scan_once (daily PnL, daily trade count, earnings
     blackout, re-entry cooldown) now SKIP the scan on read error rather than
     assuming "no cap in effect".

M0.3 Exit-price fallback chain — live exit retries get_quote 3x with 500ms
     backoff, falls back to ws_last_tick, and aborts (position stays OPEN)
     rather than writing a corrupt ₹0 P&L row via the old entry_price
     fallback.

M0.4 GTT placement assertion — live swing entry retries GTT placement 3x
     and emergency-exits the position if all attempts fail, rather than
     silently holding a CNC position with no broker-level SL.

M0.5 Paper GTT via Firestore — paper entries write a paper_gtts Firestore
     row; ws_monitor's 60s reconciler is the failover path for stops that
     the tick stream would otherwise miss.

M0.6 MFE/MAE capture — position schema carries max-favorable /
     max-adverse excursion (in R) updated live by the ws_monitor tick
     handler, plus a breakeven_sl_fired flag for later attribution.

Mode stickiness — positions record paper/live at entry time; all exit /
GTT paths honour the recorded mode rather than the current runtime flag,
so a paper→live flip mid-day can't route a paper position's exit through
the real broker (or vice versa).
"""
from __future__ import annotations

import inspect

from autotrader.adapters import firestore_state as fs_mod
from autotrader.services import order_service as os_mod
from autotrader.services import trading_service as ts_mod
from autotrader.services import ws_monitor_service as ws_mod


# ──────────────────────────────────────────────────────────────────────────
# M0.1 Kill-switch
# ──────────────────────────────────────────────────────────────────────────


def test_firestore_state_has_kill_switch_primitives():
    """FirestoreStateStore must expose get_kill_switch / set_kill_switch."""
    src = inspect.getsource(fs_mod)
    assert "def get_kill_switch" in src, "get_kill_switch helper missing"
    assert "def set_kill_switch" in src, "set_kill_switch helper missing"
    # Fail-closed semantics: the docstring and implementation must make the
    # read-error-means-active contract explicit.
    assert "fail_closed" in src.lower() or "fail-closed" in src.lower(), (
        "Kill-switch must document fail-closed semantics (read error => ACTIVE)."
    )


def test_kill_switch_checked_in_run_scan_once():
    """run_scan_once must gate on kill-switch as the first action after lock-acquire."""
    src = inspect.getsource(ts_mod)
    assert "get_kill_switch" in src, (
        "trading_service.run_scan_once missing kill-switch check. "
        "M0.1 requires a fail-closed gate before market-open / risk-cap reads."
    )
    assert "kill_switch_active" in src, (
        "Missing kill_switch_active skip reason string — dashboards need "
        "a stable identifier to alert on."
    )


# ──────────────────────────────────────────────────────────────────────────
# M0.2 Fail-closed risk-cap reads
# ──────────────────────────────────────────────────────────────────────────


def test_daily_pnl_read_failure_skips_scan():
    """Prior code defaulted _today_pnl=0.0 on read failure — now it SKIPs."""
    src = inspect.getsource(ts_mod)
    assert "daily_pnl_read_failed" in src, (
        "M0.2 daily-PnL read failure must SKIP the scan (fail-closed), not "
        "default to 0.0 and bypass max_daily_loss."
    )


def test_trade_count_read_failure_skips_scan():
    src = inspect.getsource(ts_mod)
    assert "trade_count_read_failed" in src, (
        "M0.2 trade-count read failure must SKIP the scan (fail-closed)."
    )


def test_earnings_blackout_read_failure_skips_scan():
    src = inspect.getsource(ts_mod)
    assert "earnings_blackout_read_failed" in src, (
        "M0.2 earnings-blackout read failure must SKIP rather than silently "
        "allow entries through an earnings window."
    )


def test_reentry_cooldown_read_failure_skips_scan():
    src = inspect.getsource(ts_mod)
    assert "reentry_cooldown_read_failed" in src, (
        "M0.2 re-entry cooldown read failure must SKIP rather than silently "
        "allow re-entries into freshly-stopped names."
    )


# ──────────────────────────────────────────────────────────────────────────
# M0.3 Exit-price fallback chain
# ──────────────────────────────────────────────────────────────────────────


def test_exit_price_chain_retries_quote_and_uses_ws_last_tick():
    """Exit price chain must retry quote then fall back to ws_last_tick."""
    src = inspect.getsource(os_mod)
    assert "live_exit_quote_retry" in src, (
        "M0.3 exit quote must retry (3x with backoff); single-shot fallback "
        "was the regression that let transient 429s write ₹0 P&L rows."
    )
    assert 'get_json("ws_last_tick"' in src, (
        "M0.3 exit must consult ws_last_tick/{instrument_key} as a second-"
        "chance fallback when the broker quote endpoint is unreachable."
    )


def test_exit_price_unknown_aborts_close():
    """When no price is available, the close must abort (not write corrupt pnl)."""
    src = inspect.getsource(os_mod)
    assert "exit_price_unknown" in src, (
        "M0.3 must abort and leave the position OPEN when no fill price is "
        "available — writing ₹0 P&L corrupts daily aggregation."
    )
    # The old entry_price fallback must be gone.
    assert "_NO_FILL_PRICE" not in src, (
        "Old entry_price fallback (tagged _NO_FILL_PRICE) must be removed — "
        "it silently booked ₹0 P&L and hid the real loss."
    )


# ──────────────────────────────────────────────────────────────────────────
# M0.4 GTT placement assertion
# ──────────────────────────────────────────────────────────────────────────


def test_live_gtt_place_has_bounded_retries():
    src = inspect.getsource(os_mod)
    assert "_place_live_gtt_with_retries" in src, (
        "M0.4: live GTT placement must use a bounded retry helper."
    )
    assert "max_attempts" in src, (
        "Retry loop must expose max_attempts so tests / runtime can tune it."
    )


def test_live_swing_no_gtt_triggers_emergency_exit():
    src = inspect.getsource(os_mod)
    assert "live_swing_no_gtt_sl_emergency_exit" in src, (
        "M0.4 assertion: if GTT placement fails on a live swing, we must "
        "emergency-exit the position rather than hold it unprotected."
    )
    assert "needs_manual_gtt" in src, (
        "Failed GTT placements must set needs_manual_gtt=True so operators "
        "and the premarket reconciler can surface them."
    )


# ──────────────────────────────────────────────────────────────────────────
# M0.5 Paper GTT
# ──────────────────────────────────────────────────────────────────────────


def test_firestore_state_has_paper_gtt_helpers():
    src = inspect.getsource(fs_mod)
    for fn in ("save_paper_gtt", "delete_paper_gtt", "list_paper_gtts", "get_paper_gtt"):
        assert f"def {fn}" in src, f"Paper-GTT helper {fn} missing on FirestoreStateStore"


def test_order_service_writes_paper_gtt_on_entry():
    src = inspect.getsource(os_mod)
    assert "save_paper_gtt" in src, (
        "M0.5: order_service must write paper_gtts on paper entry so "
        "ws_monitor's 60s reconciler can fire the stop if the tick stream "
        "stalls."
    )


def test_ws_monitor_has_paper_gtt_reconciler():
    src = inspect.getsource(ws_mod)
    assert "_paper_gtt_reconciler" in src, (
        "M0.5 ws_monitor must run a 60s reconciler that polls paper_gtts and "
        "fires stops that the tick stream missed."
    )


# ──────────────────────────────────────────────────────────────────────────
# M0.6 MFE/MAE capture
# ──────────────────────────────────────────────────────────────────────────


def test_position_schema_has_mfe_mae_fields():
    src = inspect.getsource(os_mod)
    for field in (
        "max_favorable_excursion_r",
        "max_adverse_excursion_r",
        "max_favorable_excursion_price",
        "max_adverse_excursion_price",
        "breakeven_sl_fired",
        "breakeven_sl_trigger_mfe_r",
    ):
        assert field in src, (
            f"Position schema must declare {field!r} (M0.6 — drives M6 "
            "AttributionLog and backtest edge scoring)."
        )


def test_ws_monitor_tracks_mfe_mae_per_tick():
    src = inspect.getsource(ws_mod)
    assert "mfe_price" in src and "mae_price" in src, (
        "ws_monitor tick handler must track favorable + adverse excursion."
    )
    assert "max_favorable_excursion_r" in src, (
        "MFE/MAE must be persisted to Firestore in R-units, not just as raw "
        "prices — R-multiples are what the edge-scoring path consumes."
    )


def test_ws_monitor_writes_ws_last_tick():
    """ws_last_tick must be written so order_service.py can fall back to it."""
    src = inspect.getsource(ws_mod)
    assert '"ws_last_tick"' in src, (
        "ws_monitor must persist last tick so order_service's exit-price "
        "fallback chain (M0.3) has a second-chance source when the quote "
        "endpoint is unreachable."
    )


# ──────────────────────────────────────────────────────────────────────────
# Mode stickiness — paper/live switch safety
# ──────────────────────────────────────────────────────────────────────────


def test_position_records_paper_flag_at_entry():
    src = inspect.getsource(os_mod)
    assert '"paper": bool(self.settings.runtime.paper_trade)' in src, (
        "Every position doc must record its mode at entry so a runtime flip "
        "can't re-route an in-flight exit through the wrong channel."
    )


def test_exit_honours_positions_recorded_mode():
    src = inspect.getsource(os_mod)
    # The mode-sticky read pattern: pos.get('paper', runtime-flag-fallback)
    assert 'pos.get("paper", self.settings.runtime.paper_trade)' in src, (
        "Exit path must read position.paper (falling back to runtime) so a "
        "paper→live flip mid-day doesn't send a paper exit to the real broker."
    )
