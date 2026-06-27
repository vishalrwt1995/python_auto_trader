"""Swing CORE-4 regime fold (2026-06-24, updated 2026-06-27).

`regime_affinity.core4_regime` folds RANGE_ROTATING → RANGE before the swing
gates so live matches the backtest. EARLY_TREND_UP/DOWN have been removed from
the regime set entirely (2026-06-27); the fold now only covers RANGE_ROTATING.

These tests lock the mapping, parity with the backtest CORE_MAP, and that
trading_service wires the fold correctly.
"""
from __future__ import annotations

import inspect

from autotrader.domain.playbook import check_playbook
from autotrader.domain.regime_affinity import _CORE4_FOLD, core4_regime
from autotrader.services import trading_service as ts_mod


# ── core4_regime mapping ─────────────────────────────────────────────────
def test_core4_regime_folds_range_rotating_to_range():
    assert core4_regime("RANGE_ROTATING") == "RANGE"


def test_core4_regime_identity_on_base_regimes():
    for r in ("TREND_UP", "TREND_DOWN", "RANGE", "PANIC", "RECOVERY"):
        assert core4_regime(r) == r


def test_core4_regime_case_insensitive_and_safe():
    assert core4_regime("range_rotating") == "RANGE"
    assert core4_regime("") == ""
    assert core4_regime(None) == ""  # type: ignore[arg-type]


def test_core4_fold_matches_backtest_core_map():
    """Drift guard: the live fold MUST equal the backtest's CORE_MAP exactly."""
    from autotrader.backtest_v2.brain_reconstruct import CORE_MAP
    assert _CORE4_FOLD == CORE_MAP


# ── MR is allowed in RANGE_ROTATING (directly and via fold) ──────────────
def test_mean_reversion_allowed_in_range_rotating():
    # edge.py explicitly lists RANGE_ROTATING so the playbook allows it directly.
    raw_ok, _ = check_playbook(setup="MEAN_REVERSION", direction="BUY", regime="RANGE_ROTATING")
    assert raw_ok is True
    # Folded to RANGE -> also allowed, same result.
    folded_ok, _ = check_playbook(setup="MEAN_REVERSION", direction="BUY", regime=core4_regime("RANGE_ROTATING"))
    range_ok, _ = check_playbook(setup="MEAN_REVERSION", direction="BUY", regime="RANGE")
    assert folded_ok == range_ok


def test_fold_is_identity_for_base_regimes_in_playbook():
    # On core-4 regimes the fold is identity -> playbook behaviour unchanged.
    for setup, reg in (("MEAN_REVERSION", "RANGE"), ("MOMENTUM", "TREND_UP"), ("PULLBACK", "TREND_UP")):
        a, _ = check_playbook(setup=setup, direction="BUY", regime=reg)
        b, _ = check_playbook(setup=setup, direction="BUY", regime=core4_regime(reg))
        assert a == b


# ── wiring guard: trading_service must fold for SWING before the gates ────
def test_trading_service_wires_core4_fold_for_swing():
    src = inspect.getsource(ts_mod)
    assert "core4_regime" in src, "core4_regime not imported/used in trading_service"
    assert (
        "_gate_regime = core4_regime(_raw_brain_regime) if _is_swing else _raw_brain_regime" in src
    ), "swing gate regime is no longer the CORE-4 folded value — the fix may have regressed"
    assert "_brain_regime = _gate_regime" in src, (
        "_brain_regime must route through the folded _gate_regime (feeds hard_blocks/"
        "swing_setup_allowed/check_swing_entry/check_playbook)"
    )
