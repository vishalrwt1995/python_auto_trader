"""Tests for Batch 2 — Safety Rails (2026-04-22).

Three structural protections against the most common failure modes
surfaced in the 04-16/04-20/04-21 post-mortems:

2.1 Re-entry cooldown — scanner must suppress symbols whose last
    position closed within `reentry_cooldown_minutes` (default 30).

2.2 Entry-window cutoff tightened 14:00 → 13:30. With FLAT_TIMEOUT at
    120 min and EOD force-close at 15:25, any entry after 13:25 is
    pre-committed to an EOD_CLOSE exit at whatever price the market
    gives — not a clean SL/target/timeout.

2.3 Regime-change SL tighten in ws_monitor_service gated to intraday
    only. Applying an intraday-regime-flip 0.8× ATR tighten to a
    multi-day swing position (sized on daily ATR 2.5×) guarantees
    stop-out on the next intraday squeeze.
"""
from __future__ import annotations

import inspect
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from autotrader.services import trading_service as ts_mod
from autotrader.services import ws_monitor_service as ws_mod
from autotrader.settings import StrategySettings
from autotrader import time_utils


IST = timezone(timedelta(hours=5, minutes=30))


# ─── 2.1: re-entry cooldown ────────────────────────────────────────────


def test_reentry_cooldown_setting_present():
    """StrategySettings must expose a reentry_cooldown_minutes knob."""
    s = StrategySettings()
    assert hasattr(s, "reentry_cooldown_minutes"), (
        "reentry_cooldown_minutes setting missing — Batch 2.1 expected a "
        "tunable cooldown knob on StrategySettings."
    )
    assert isinstance(s.reentry_cooldown_minutes, int)
    assert s.reentry_cooldown_minutes >= 5, (
        f"cooldown too short ({s.reentry_cooldown_minutes} min) — a cooldown "
        "shorter than one intraday candle defeats the purpose. Default >= 15."
    )


def test_reentry_cooldown_enforced_in_scan_loop():
    """The scan loop must call get_recently_exited_symbols and set the
    policy_block_reason to 'reentry_cooldown' for matching symbols."""
    src = inspect.getsource(ts_mod)
    assert "get_recently_exited_symbols" in src, (
        "scan loop does not call state.get_recently_exited_symbols — cooldown "
        "not wired. Batch 2.1 (2026-04-22)."
    )
    assert '"reentry_cooldown"' in src or "'reentry_cooldown'" in src, (
        "reentry_cooldown policy_block_reason string missing — dashboards rely "
        "on this reason to surface the cooldown activity."
    )
    assert "cfg.reentry_cooldown_minutes" in src, (
        "cooldown must read from cfg (honours runtime overrides), not from "
        "settings.strategy.reentry_cooldown_minutes directly."
    )


def test_firestore_state_exposes_recently_exited_symbols():
    """Adapter must provide the lookup the scanner uses."""
    from autotrader.adapters import firestore_state as fs_mod
    src = inspect.getsource(fs_mod)
    assert "def get_recently_exited_symbols" in src, (
        "firestore_state.get_recently_exited_symbols() missing."
    )
    # Must only count CLOSED positions — OPEN positions are caught elsewhere
    # (portfolio_strategy_concentrated) and shouldn't be double-counted.
    assert '"CLOSED"' in src, (
        "recently-exited filter must check status=CLOSED — OPEN positions are "
        "handled by different guard (portfolio concentration)."
    )


# ─── 2.2: entry-window cutoff 14:00 → 13:30 ────────────────────────────


def test_entry_window_cutoff_is_13_30():
    """Cut-off must be 13:30 IST (810 minutes)."""
    src = inspect.getsource(time_utils)
    assert "ist_minutes() <= 810" in src, (
        "entry window cutoff is no longer 13:30 (810 min) — Batch 2.2. "
        "If this needs to shift again, update the test WITH a post-mortem "
        "reference, don't revert silently."
    )
    assert "ist_minutes() <= 840" not in src, (
        "14:00 (840 min) cutoff still present — did Batch 2.2 get reverted?"
    )


def _fake_now(h: int, m: int, weekday: int = 2):
    """Build a datetime for a weekday (Wed=2) at h:m IST."""
    # Pick a Wednesday: 2026-04-22 is a Wednesday.
    return datetime(2026, 4, 22, h, m, 0, tzinfo=IST)


def test_entry_window_open_at_13_29():
    """13:29 IST must still allow entries."""
    with patch.object(time_utils, "now_ist", return_value=_fake_now(13, 29)):
        assert time_utils.is_entry_window_open_ist() is True


def test_entry_window_closed_at_13_31():
    """13:31 IST must block new entries (one minute past cutoff)."""
    with patch.object(time_utils, "now_ist", return_value=_fake_now(13, 31)):
        assert time_utils.is_entry_window_open_ist() is False


def test_entry_window_closed_at_14_00():
    """14:00 IST must block — old cutoff should no longer allow entries."""
    with patch.object(time_utils, "now_ist", return_value=_fake_now(14, 0)):
        assert time_utils.is_entry_window_open_ist() is False


def test_market_open_still_15_30():
    """Regression: tightening the entry cutoff must NOT change market-open
    window. Exits, stops, and target checks run until 15:30."""
    with patch.object(time_utils, "now_ist", return_value=_fake_now(15, 0)):
        assert time_utils.is_market_open_ist() is True
    with patch.object(time_utils, "now_ist", return_value=_fake_now(15, 31)):
        assert time_utils.is_market_open_ist() is False


# ─── 2.3: regime-tighten gated to non-swing ────────────────────────────


def test_regime_tighten_skips_swing_positions():
    """ws_monitor_service must not apply regime-flip SL tightening to swing.

    Structural check: the precondition must include a swing exclusion.
    """
    src = inspect.getsource(ws_mod)
    # The new condition must reference wl_type and the swing exclusion
    assert "_pos_is_swing" in src or 'wl_type"' in src, (
        "ws_monitor regime-tighten block does not check wl_type — a swing "
        "position would get its daily-ATR SL tightened by intraday regime "
        "flips, guaranteeing stop-out. Batch 2.3 (2026-04-22)."
    )
    # The condition must include `not _pos_is_swing` (or equivalent) inside
    # the gating `if` for regime-tighten.
    import re
    # Grab the regime-tighten block (between "Regime-change tighten" and the
    # one-shot comment "Persist immediately").
    m = re.search(
        r"#\s*──\s*Regime-change tighten.*?Persist immediately",
        src, re.DOTALL,
    )
    assert m is not None, "regime-tighten block not found in ws_monitor_service"
    block = m.group(0)
    assert "not _pos_is_swing" in block or '!= "swing"' in block, (
        "regime-tighten gating clause missing swing exclusion — Batch 2.3 "
        "expected `and not _pos_is_swing` (or equivalent) in the precondition."
    )


def test_regime_tighten_comment_explains_swing_rationale():
    """The fix must carry a comment documenting why swing is excluded —
    so a future reader doesn't 'clean up' the exclusion thinking it's dead.
    """
    src = inspect.getsource(ws_mod)
    assert "daily ATR" in src or "daily-ATR" in src, (
        "regime-tighten swing-exclusion lacks the daily-ATR rationale comment. "
        "Without this context the guard looks accidental and gets removed."
    )
