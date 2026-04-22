"""Tests for Batch 5 — Capability Expansion (2026-04-22).

Pre-Batch-5 audit: Phase 2 (intraday in-play scanner) emitted only
VWAP_TREND and VWAP_REVERSAL as setup labels. Because disabled_strategies
contains "VWAP_REVERSAL" in the default kill-switch set, the Phase 2
scanner was effectively producing exactly ONE setup type in production —
that's a single bet, not a scanner.

Batch 5 diversifies the label pool so the same microstructure signals we
already compute (orb_label, volume_shock_component, vwap_slope_component,
ext_component, reversal_signal) actually drive the routing:

  - OPEN_DRIVE   : early session + ORB up-break + strong volume thrust
  - BREAKOUT     : ORB up-break + above VWAP + moderate volume (any time)
  - MEAN_REVERSION: CHOPPY regime + reversal signal + overstretched below VWAP
  - PULLBACK     : above VWAP + rising VWAP slope + moderate volume
  - VWAP_TREND / VWAP_REVERSAL: catch-all fallback based on VWAP side

check_strategy_entry() in domain.scoring already has gates for all five
new labels (or passes-through for OPEN_DRIVE), so the downstream contract
is already satisfied — Batch 5 is purely additive in the scanner.
"""
from __future__ import annotations

import inspect

from autotrader.services import universe_service as us_mod


# ─── All five labels must appear in the Phase 2 block ──────────────────


def _phase2_block() -> str:
    """Return the Phase 2 in-play scoring/labelling block as a single string."""
    src = inspect.getsource(us_mod)
    # Anchor on the Batch 5 comment which sits immediately above the new tree.
    marker = "Batch 5"
    assert marker in src, (
        "Batch 5 comment marker not found in universe_service — the decision "
        "tree edit must be anchored with a 'Batch 5' comment so future reviewers "
        "can find it."
    )
    idx = src.index(marker)
    # Grab a generous window after the marker (the tree is ~60 lines).
    return src[idx : idx + 4000]


def test_phase2_emits_open_drive():
    assert '"OPEN_DRIVE"' in _phase2_block(), (
        "Phase 2 must emit OPEN_DRIVE for early-session breakouts with a "
        "volume thrust — Batch 5 introduced this label to separate the "
        "highest-conviction morning moves from generic BREAKOUT."
    )


def test_phase2_emits_breakout():
    assert '"BREAKOUT"' in _phase2_block(), (
        "Phase 2 must emit BREAKOUT on ORB up-break with volume confirmation."
    )


def test_phase2_emits_pullback():
    assert '"PULLBACK"' in _phase2_block(), (
        "Phase 2 must emit PULLBACK when price is above VWAP with a rising "
        "VWAP slope — healthy-uptrend continuation."
    )


def test_phase2_emits_mean_reversion():
    assert '"MEAN_REVERSION"' in _phase2_block(), (
        "Phase 2 must emit MEAN_REVERSION when in CHOPPY regime with an "
        "overstretched-below-VWAP setup and a reversal signal."
    )


def test_phase2_fallback_labels_retained():
    blk = _phase2_block()
    assert '"VWAP_TREND"' in blk, (
        "Phase 2 must retain VWAP_TREND as a catch-all fallback above VWAP."
    )
    assert '"VWAP_REVERSAL"' in blk, (
        "Phase 2 must retain VWAP_REVERSAL as a catch-all fallback below VWAP."
    )


# ─── Structural gates on the new decision tree ─────────────────────────


def test_open_drive_gated_on_early_session():
    """OPEN_DRIVE must require an early-session time check — that's the whole
    point of the label. If it fires at 2pm, it's not opening drive."""
    blk = _phase2_block()
    # The implementation computes a tuple (hour, minute) and compares to (10, 45).
    assert "(10, 45)" in blk or "10, 45)" in blk, (
        "OPEN_DRIVE branch doesn't check for early session — expected a "
        "(hour, minute) < (10, 45) style guard."
    )


def test_open_drive_requires_orb_up_break():
    blk = _phase2_block()
    # Find the OPEN_DRIVE branch and verify it references UP_BREAK nearby.
    idx = blk.index('"OPEN_DRIVE"')
    # Look in the ~400 chars leading up to the assignment (the if condition).
    window = blk[max(0, idx - 400) : idx]
    assert "UP_BREAK" in window, (
        "OPEN_DRIVE branch must require orb_label == 'UP_BREAK' — an opening "
        "drive without a break is just… gapping."
    )


def test_breakout_requires_orb_up_break():
    blk = _phase2_block()
    idx = blk.index('"BREAKOUT"')
    window = blk[max(0, idx - 400) : idx]
    assert "UP_BREAK" in window, (
        "BREAKOUT branch must require orb_label == 'UP_BREAK'."
    )


def test_mean_reversion_gated_on_choppy_regime():
    """MEAN_REVERSION must be gated on CHOPPY regime — fading a trending
    market is how accounts die."""
    blk = _phase2_block()
    idx = blk.index('"MEAN_REVERSION"')
    window = blk[max(0, idx - 500) : idx]
    # Either the choppy flag variable or the literal "CHOPPY" should appear.
    assert ("_choppy_regime" in window) or ("CHOPPY" in window), (
        "MEAN_REVERSION branch must be gated on the CHOPPY regime — fading a "
        "trending regime is negative expectancy."
    )


def test_mean_reversion_requires_reversal_signal():
    blk = _phase2_block()
    idx = blk.index('"MEAN_REVERSION"')
    window = blk[max(0, idx - 500) : idx]
    assert ("_reversal_fired" in window) or ("reversal_signal" in window), (
        "MEAN_REVERSION branch must consume the reversal_signal we already "
        "compute — otherwise we're ignoring our own oscillator."
    )


def test_pullback_requires_above_vwap_and_rising_slope():
    blk = _phase2_block()
    idx = blk.index('"PULLBACK"')
    window = blk[max(0, idx - 500) : idx]
    assert ("_above_vwap" in window) or ("close_now > vwap_now" in window), (
        "PULLBACK branch must require price above VWAP — you can't pull back "
        "into an uptrend from below the VWAP anchor."
    )
    assert ("_rising_vwap" in window) or ("vwap_slope_component" in window), (
        "PULLBACK branch must require a rising VWAP slope to confirm the "
        "uptrend is intact."
    )


def test_decision_tree_has_more_than_two_branches():
    """Sanity: the number of setup_label = "..." assignments in the new block
    must be > 3 — otherwise we regressed to the 2-label world."""
    blk = _phase2_block()
    count = blk.count("setup_label = \"")
    assert count >= 6, (
        f"Phase 2 decision tree has only {count} setup_label assignments — "
        "Batch 5 expected at least 6 (OPEN_DRIVE, BREAKOUT, MEAN_REVERSION, "
        "PULLBACK, VWAP_TREND, VWAP_REVERSAL + tiebreaker fallback)."
    )
