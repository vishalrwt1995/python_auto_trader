"""Tests for M2 — Edge registry + Playbook hard-block + Thesis shape.

These tests exercise the three pure domain modules directly, and do a
structural check on `trading_service.py` to confirm the flag-gated
wire-up is in place (without having to boot the whole service).

The intent is to fail loudly if:
  * An edge is accidentally un-registered by a future refactor.
  * A new scanner setup ships without a matching Edge entry (the
    playbook would silently pass it through if the gate were missing).
  * The playbook is removed / ungated from trading_service.
  * The Thesis shape drifts so attribution can no longer join.
"""
from __future__ import annotations

import inspect

import pytest

from autotrader.domain import edge as edge_mod
from autotrader.domain.edge import Edge
from autotrader.domain.playbook import check_playbook, matching_edges
from autotrader.domain.thesis import Thesis, build_thesis


# ──────────────────────────────────────────────────────────────────────────
# Edge registry
# ──────────────────────────────────────────────────────────────────────────


def test_registry_has_default_edges_for_every_scanner_setup():
    """Every setup the scanner currently emits must have at least one Edge."""
    names = {e.name for e in edge_mod.all_edges()}
    # Direction-discriminated pairs; aliases (SHORT_BREAKDOWN etc.) are
    # normalized by the playbook, so we only need the canonical setups here.
    expected = {
        "breakout_long", "breakout_short",
        "pullback_long", "pullback_short",
        "mean_reversion_long", "mean_reversion_short",
        "open_drive_long", "open_drive_short",
        "momentum_long", "momentum_short",
    }
    assert expected.issubset(names), f"missing default edges: {expected - names}"


def test_register_and_get_are_versioned():
    edge_mod.reset_for_tests()
    edge_mod.register(Edge(name="foo", version="v1", setup="BREAKOUT",
                           direction="LONG", allowed_regimes=("TREND_UP",)))
    edge_mod.register(Edge(name="foo", version="v2", setup="BREAKOUT",
                           direction="LONG", allowed_regimes=("TREND_UP", "RECOVERY")))
    assert edge_mod.get("foo", "v1").allowed_regimes == ("TREND_UP",)
    latest = edge_mod.get("foo", "latest")
    assert latest is not None and latest.version == "v2"
    # Re-register defaults so later tests aren't polluted.
    edge_mod.reset_for_tests()
    edge_mod._register_defaults()


def test_edge_is_frozen():
    """Edge should be immutable — mutation must raise."""
    e = edge_mod.get("breakout_long", "v1")
    assert e is not None
    with pytest.raises(Exception):
        e.name = "something_else"  # type: ignore[misc]


# ──────────────────────────────────────────────────────────────────────────
# Playbook — hard-block semantics
# ──────────────────────────────────────────────────────────────────────────


def test_playbook_allows_breakout_long_in_trend_up():
    ok, reason = check_playbook("BREAKOUT", "BUY", "TREND_UP", "NORMAL")
    assert ok, reason
    assert reason == ""


def test_playbook_blocks_breakout_long_in_trend_down():
    """The whole premise of M2: a (setup, direction, regime) tuple that
    no edge allows must be HARD-BLOCKED, not passed through."""
    ok, reason = check_playbook("BREAKOUT", "BUY", "TREND_DOWN", "NORMAL")
    assert not ok
    assert reason == "playbook_regime_not_allowed"


def test_playbook_blocks_unknown_setup_fail_closed():
    ok, reason = check_playbook("SOMETHING_NEW", "BUY", "TREND_UP", "NORMAL")
    assert not ok
    assert reason == "playbook_no_edge_registered"


def test_playbook_hold_direction_is_noop():
    ok, reason = check_playbook("BREAKOUT", "HOLD", "TREND_UP", "NORMAL")
    assert ok and reason == ""


def test_playbook_normalizes_scanner_aliases():
    """SHORT_BREAKDOWN is the scanner alias for BREAKOUT / SELL — the
    playbook must recognize it (otherwise every short breakdown would
    fail the 'no_edge_registered' check)."""
    ok, reason = check_playbook("SHORT_BREAKDOWN", "SELL", "TREND_DOWN", "NORMAL")
    assert ok, reason


def test_playbook_blocks_lockdown_risk_mode_by_default():
    """LOCKDOWN is opt-in per edge — default edges don't list it, so
    any signal in LOCKDOWN must be blocked."""
    ok, reason = check_playbook("BREAKOUT", "BUY", "TREND_UP", "LOCKDOWN")
    assert not ok
    assert reason == "playbook_risk_mode_not_allowed"


def test_matching_edges_returns_direction_appropriate_edges():
    buys = matching_edges("BREAKOUT", "BUY")
    sells = matching_edges("BREAKOUT", "SELL")
    assert any(e.name == "breakout_long" for e in buys)
    assert not any(e.name == "breakout_short" for e in buys)
    assert any(e.name == "breakout_short" for e in sells)


def test_playbook_mean_reversion_allowed_in_range_and_chop():
    assert check_playbook("MEAN_REVERSION", "BUY", "RANGE", "NORMAL")[0]
    assert check_playbook("MEAN_REVERSION", "BUY", "CHOP", "NORMAL")[0]


def test_playbook_mean_reversion_blocked_in_trend_down_for_long():
    """MR longs should NOT fire in a screaming down-trend — you're
    fading the dominant move and it chews the trade up."""
    ok, reason = check_playbook("MEAN_REVERSION", "BUY", "TREND_DOWN", "NORMAL")
    assert not ok and reason == "playbook_regime_not_allowed"


# ──────────────────────────────────────────────────────────────────────────
# Thesis shape + build helper
# ──────────────────────────────────────────────────────────────────────────


def test_thesis_is_frozen():
    t = build_thesis(
        setup="BREAKOUT", direction="BUY", entry_price=100.0, sl_price=98.0,
        regime="TREND_UP", risk_mode="NORMAL", ts_epoch=1_000.0,
    )
    with pytest.raises(Exception):
        t.entry_price = 101.0  # type: ignore[misc]


def test_build_thesis_fills_defaults_and_roundtrips():
    t = build_thesis(
        setup="BREAKOUT", direction="BUY", entry_price=100.0, sl_price=98.0,
        regime="TREND_UP", risk_mode="NORMAL", ts_epoch=1_000.0,
    )
    assert t.expected_r > 0, "expected_r must default to a non-zero prior"
    assert t.expected_hold_minutes > 0, "expected hold must default non-zero"
    assert t.invalidation_price == 98.0, "invalidation defaults to SL when not given"
    # Dict round-trip for Firestore compatibility.
    d = t.to_dict()
    t2 = Thesis.from_dict(d)
    assert t2 == t


def test_build_thesis_honours_swing_hold_default():
    intraday = build_thesis(
        setup="BREAKOUT", direction="BUY", entry_price=100.0, sl_price=98.0,
        regime="TREND_UP", risk_mode="NORMAL", ts_epoch=1.0, is_swing=False,
    )
    swing = build_thesis(
        setup="BREAKOUT", direction="BUY", entry_price=100.0, sl_price=98.0,
        regime="TREND_UP", risk_mode="NORMAL", ts_epoch=1.0, is_swing=True,
    )
    assert swing.expected_hold_minutes > intraday.expected_hold_minutes


# ──────────────────────────────────────────────────────────────────────────
# Structural: the Playbook wire-up in trading_service must exist and be
# gated behind use_playbook_v1.
# ──────────────────────────────────────────────────────────────────────────


def test_trading_service_wires_playbook_behind_flag():
    from autotrader.services import trading_service
    src = inspect.getsource(trading_service)
    assert "check_playbook(" in src, "trading_service must call check_playbook"
    assert "use_playbook_v1" in src, "playbook call must be gated behind USE_PLAYBOOK_V1"
    # The wire-up should sit near the existing strategy-entry gate, not
    # at entry time — guarantees the hard-block happens before position sizing.
    idx_pb = src.index("check_playbook(")
    idx_strategy = src.index("check_strategy_entry(")
    assert idx_pb > idx_strategy, "playbook check must come AFTER strategy-entry check"
