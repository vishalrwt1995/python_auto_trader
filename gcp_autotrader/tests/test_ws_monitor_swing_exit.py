"""Tests for the swing exit gating in ws_monitor_service (2026-06).

The retired "V2" swing exit (50%-at-0.5R scale-out + 2R fixed target + post-
breakeven ATR trail) was replaced by a daily 1R trailing stop managed in
swing_reconciliation_service (see domain/swing_exit + tests/test_swing_exit.py).

For SWING positions, ws_monitor's ONLY intraday job is now the resting-SL check:
the 0.5R partial is removed, and the target-passed switch, the intraday ATR-trail
and the 2R target-exit are all gated to intraday (`not is_swing`). Intraday
behaviour is unchanged. These are structural guards (source inspection, matching
the prior test style) that fail loudly if the gating regresses.
"""
from __future__ import annotations

import inspect

from autotrader.services import ws_monitor_service as ws_mod


def test_swing_0_5R_partial_removed():
    """The retired swing 0.5R scale-out must be gone — swing rides full size."""
    src = inspect.getsource(ws_mod)
    assert "SWING_PARTIAL_0_5R" not in src, "retired swing 0.5R partial is back"
    assert "if is_swing and sl_dist > 0 and original_qty >= 2:" not in src, (
        "retired swing partial guard is back — swing must ride full size and exit "
        "via the daily 1R trail + resting SL only"
    )


def test_swing_target_passed_switch_gated_to_intraday():
    src = inspect.getsource(ws_mod)
    assert 'if not is_swing and not pos.get("target_passed") and target > 0:' in src, (
        "target-passed trailing switch must be gated to intraday (not is_swing) — "
        "swing has no fixed target under the trailing policy"
    )


def test_swing_intraday_atr_trail_gated_to_intraday():
    src = inspect.getsource(ws_mod)
    assert 'if not is_swing and pos.get("sl_moved") and atr > 0:' in src, (
        "intraday ATR-trail must be gated to intraday — the swing trail is managed "
        "daily in swing_reconciliation_service; an intraday ATR-trail would fight it"
    )


def test_swing_target_exit_gated_to_intraday():
    src = inspect.getsource(ws_mod)
    assert 'elif not is_swing and not pos.get("target_passed") and target > 0 and ltp >= target:' in src
    assert 'elif not is_swing and not pos.get("target_passed") and target > 0 and ltp <= target:' in src


def test_breakeven_still_skips_swing():
    """Unchanged invariant — breakeven was already intraday-only (Phase E V3)."""
    src = inspect.getsource(ws_mod)
    assert 'if not is_swing and not pos.get("sl_moved") and entry_price > 0 and atr > 0:' in src


def test_intraday_partials_unchanged():
    """The intraday 3-stage / qty==2 partial blocks stay (guarded `not is_swing`)."""
    src = inspect.getsource(ws_mod)
    assert "and not is_swing:" in src
    assert "PARTIAL_1R" in src
    assert "PARTIAL_1_5R" in src
    assert "PARTIAL_1R_QTY2" in src
