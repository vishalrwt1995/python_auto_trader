"""Swing CORE-4 regime fold (2026-06-24).

Production swing diverged from the validated backtest because `trading_service`
fed the brain's REFINED regime (RANGE_ROTATING, EARLY_TREND_UP) into the gates,
while every validated backtest gates on CORE-4 folded regimes
(`brain_reconstruct.CORE_MAP`). The intended, core-4-validated playbook then
vetoed validated swing cells on refined-regime days (the RANGE_ROTATING drought).

`regime_affinity.core4_regime` folds refined->base before the SWING gates so live
matches the backtest. These tests lock the mapping, its parity with the backtest's
CORE_MAP, the playbook-unblock effect, and that trading_service actually wires it.
"""
from __future__ import annotations

import inspect

from autotrader.domain.playbook import check_playbook
from autotrader.domain.regime_affinity import _CORE4_FOLD, core4_regime
from autotrader.services import trading_service as ts_mod


# ── core4_regime mapping ─────────────────────────────────────────────────
def test_core4_regime_folds_refined_to_base():
    assert core4_regime("RANGE_ROTATING") == "RANGE"
    assert core4_regime("EARLY_TREND_UP") == "TREND_UP"
    assert core4_regime("EARLY_TREND_DOWN") == "TREND_DOWN"


def test_core4_regime_identity_on_base_regimes():
    for r in ("TREND_UP", "TREND_DOWN", "RANGE", "CHOP", "PANIC", "RECOVERY"):
        assert core4_regime(r) == r


def test_core4_regime_case_insensitive_and_safe():
    assert core4_regime("range_rotating") == "RANGE"
    assert core4_regime("") == ""
    assert core4_regime(None) == ""  # type: ignore[arg-type]


def test_core4_fold_matches_backtest_core_map():
    """Drift guard: the live fold MUST equal the backtest's CORE_MAP exactly."""
    from autotrader.backtest_v2.brain_reconstruct import CORE_MAP
    assert _CORE4_FOLD == CORE_MAP


# ── the fold makes the playbook stop vetoing the validated cells ──────────
def test_fold_unblocks_mean_reversion_in_range_rotating():
    # Without the fold, the playbook vetoes MR in RANGE_ROTATING (edge.py predates it).
    raw_ok, _ = check_playbook(setup="MEAN_REVERSION", direction="BUY", regime="RANGE_ROTATING")
    assert raw_ok is False
    # Folded to RANGE -> allowed, exactly as a real RANGE day.
    folded_ok, _ = check_playbook(setup="MEAN_REVERSION", direction="BUY", regime=core4_regime("RANGE_ROTATING"))
    range_ok, _ = check_playbook(setup="MEAN_REVERSION", direction="BUY", regime="RANGE")
    assert folded_ok is True
    assert folded_ok == range_ok


def test_fold_unblocks_momentum_pullback_in_early_trend_up():
    for setup in ("MOMENTUM", "PULLBACK"):
        raw_ok, _ = check_playbook(setup=setup, direction="BUY", regime="EARLY_TREND_UP")
        assert raw_ok is False, f"{setup} unexpectedly allowed without fold"
        folded_ok, _ = check_playbook(setup=setup, direction="BUY", regime=core4_regime("EARLY_TREND_UP"))
        tup_ok, _ = check_playbook(setup=setup, direction="BUY", regime="TREND_UP")
        assert folded_ok is True and folded_ok == tup_ok


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
