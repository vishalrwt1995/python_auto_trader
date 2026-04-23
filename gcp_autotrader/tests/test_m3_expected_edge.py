"""Tests for M3 — Priors loader + expected_edge_R gate + wire-up.

Covers:
  * Priors load from the shipped config/priors/priors_v1.json file.
  * Stale prior (n < min_sample_size) must NOT block — that would cap
    every new edge at zero before it accumulates evidence.
  * Established prior with edge ≤ 0 MUST block.
  * Established prior with edge > 0 MUST pass.
  * Direction aliases (BUY/LONG, SELL/SHORT) normalize correctly.
  * The trading_service wire-up sits in the entry-gate chain and is
    gated behind USE_EXPECTED_EDGE_R_V1.
  * The backtest harness aggregates realized R correctly.
"""
from __future__ import annotations

import inspect
import json
import tempfile
from pathlib import Path

from autotrader.domain import priors as priors_mod
from autotrader.domain.expected_edge import evaluate
from autotrader.domain.priors import Prior, get_prior


# ──────────────────────────────────────────────────────────────────────────
# Prior math
# ──────────────────────────────────────────────────────────────────────────


def test_prior_expected_edge_math():
    p = Prior(win_rate=0.45, avg_win_r=1.60, avg_loss_r=-1.00, n=100)
    # 0.45 * 1.60 + 0.55 * -1.00 = 0.72 - 0.55 = 0.17
    assert abs(p.expected_edge_r - 0.17) < 1e-6


def test_prior_expected_edge_zero_edge():
    p = Prior(win_rate=0.40, avg_win_r=1.50, avg_loss_r=-1.00, n=100)
    # 0.40 * 1.50 + 0.60 * -1.00 = 0.60 - 0.60 = 0.00
    assert abs(p.expected_edge_r - 0.0) < 1e-6


def test_prior_expected_edge_negative():
    p = Prior(win_rate=0.30, avg_win_r=1.20, avg_loss_r=-1.00, n=100)
    # 0.30 * 1.20 + 0.70 * -1.00 = 0.36 - 0.70 = -0.34
    assert p.expected_edge_r < 0


# ──────────────────────────────────────────────────────────────────────────
# Priors loader + default fallback
# ──────────────────────────────────────────────────────────────────────────


def test_get_prior_reads_shipped_file():
    priors_mod.reset_for_tests()
    # Forces a load from the shipped path.
    p = get_prior("TREND_UP", "BREAKOUT", "LONG")
    assert p.win_rate > 0


def test_get_prior_falls_back_to_default_on_miss():
    priors_mod.reset_for_tests()
    p = get_prior("NONSENSE_REGIME", "NONSENSE_SETUP", "LONG")
    # Seed _default: win_rate 0.40, avg 1.5 / -1.0 → edge exactly 0.0
    assert abs(p.expected_edge_r - 0.0) < 1e-6


def test_direction_normalization_long_and_buy_equivalent():
    priors_mod.reset_for_tests()
    a = get_prior("TREND_UP", "BREAKOUT", "LONG")
    b = get_prior("TREND_UP", "BREAKOUT", "BUY")
    assert a == b


def test_load_uses_custom_path(tmp_path: Path):
    payload = {
        "version": 1,
        "min_sample_size": 50,
        "_default": {"win_rate": 0.5, "avg_win_r": 2.0, "avg_loss_r": -1.0, "n": 0},
        "TREND_UP:BREAKOUT:LONG": {"win_rate": 0.6, "avg_win_r": 2.0, "avg_loss_r": -1.0, "n": 75},
    }
    fp = tmp_path / "priors.json"
    fp.write_text(json.dumps(payload))
    priors_mod.load(fp)
    assert priors_mod.min_sample_size() == 50
    p = get_prior("TREND_UP", "BREAKOUT", "LONG")
    assert p.n == 75
    assert abs(p.expected_edge_r - (0.6 * 2.0 + 0.4 * -1.0)) < 1e-6
    priors_mod.reset_for_tests()


# ──────────────────────────────────────────────────────────────────────────
# ExpectedEdge gate semantics
# ──────────────────────────────────────────────────────────────────────────


def test_evaluate_allows_when_prior_sample_below_floor(tmp_path: Path):
    """Stale guard: n < min_sample_size must NOT block."""
    payload = {
        "version": 1, "min_sample_size": 30,
        "_default": {"win_rate": 0.25, "avg_win_r": 1.0, "avg_loss_r": -1.0, "n": 0},
        "TREND_UP:BREAKOUT:LONG": {
            "win_rate": 0.25, "avg_win_r": 1.0, "avg_loss_r": -1.0, "n": 5,  # low n
        },
    }
    fp = tmp_path / "priors.json"
    fp.write_text(json.dumps(payload))
    priors_mod.load(fp)
    # edge is -0.50, but n (5) < floor (30) → allowed
    res = evaluate("TREND_UP", "BREAKOUT", "LONG")
    assert res.allowed
    priors_mod.reset_for_tests()


def test_evaluate_blocks_when_established_prior_is_negative(tmp_path: Path):
    payload = {
        "version": 1, "min_sample_size": 30,
        "_default": {"win_rate": 0.40, "avg_win_r": 1.5, "avg_loss_r": -1.0, "n": 0},
        "TREND_UP:BREAKOUT:LONG": {
            "win_rate": 0.25, "avg_win_r": 1.0, "avg_loss_r": -1.0, "n": 100,
        },
    }
    fp = tmp_path / "priors.json"
    fp.write_text(json.dumps(payload))
    priors_mod.load(fp)
    res = evaluate("TREND_UP", "BREAKOUT", "LONG")
    assert not res.allowed
    assert res.reason == "expected_edge_non_positive"
    assert res.expected_edge_r < 0
    priors_mod.reset_for_tests()


def test_evaluate_allows_when_established_prior_is_positive(tmp_path: Path):
    payload = {
        "version": 1, "min_sample_size": 30,
        "_default": {"win_rate": 0.40, "avg_win_r": 1.5, "avg_loss_r": -1.0, "n": 0},
        "TREND_UP:BREAKOUT:LONG": {
            "win_rate": 0.55, "avg_win_r": 1.5, "avg_loss_r": -1.0, "n": 200,
        },
    }
    fp = tmp_path / "priors.json"
    fp.write_text(json.dumps(payload))
    priors_mod.load(fp)
    res = evaluate("TREND_UP", "BREAKOUT", "LONG")
    assert res.allowed
    assert res.expected_edge_r > 0
    priors_mod.reset_for_tests()


# ──────────────────────────────────────────────────────────────────────────
# Wire-up: trading_service must call evaluate_expected_edge behind the flag
# ──────────────────────────────────────────────────────────────────────────


def test_trading_service_wires_expected_edge_behind_flag():
    from autotrader.services import trading_service
    src = inspect.getsource(trading_service)
    assert "evaluate_expected_edge(" in src
    assert "use_expected_edge_r_v1" in src


# ──────────────────────────────────────────────────────────────────────────
# Backtest harness: summarization logic (no BQ needed)
# ──────────────────────────────────────────────────────────────────────────


def test_compute_priors_summarize_from_trade_rows():
    # Import the script as a module so we can unit-test its summarizer.
    import importlib.util
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "redesign" / "compute_priors_from_bq.py"
    spec = importlib.util.spec_from_file_location("compute_priors_from_bq", script_path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]

    # Simulate 3 wins (1R, 1.5R, 2R) + 2 losses (-1R each) for the same bucket.
    # entry 100, sl 98 → sl_dist 2, per-share R = pnl/qty / 2.
    rows = [
        {"regime": "TREND_UP", "setup": "BREAKOUT", "side": "BUY",
         "entry_price": 100.0, "sl_price": 98.0, "qty": 1, "net_pnl": 2.0, "pnl": 2.0, "exit_reason": "TARGET"},
        {"regime": "TREND_UP", "setup": "BREAKOUT", "side": "BUY",
         "entry_price": 100.0, "sl_price": 98.0, "qty": 1, "net_pnl": 3.0, "pnl": 3.0, "exit_reason": "TARGET"},
        {"regime": "TREND_UP", "setup": "BREAKOUT", "side": "BUY",
         "entry_price": 100.0, "sl_price": 98.0, "qty": 1, "net_pnl": 4.0, "pnl": 4.0, "exit_reason": "TARGET"},
        {"regime": "TREND_UP", "setup": "BREAKOUT", "side": "BUY",
         "entry_price": 100.0, "sl_price": 98.0, "qty": 1, "net_pnl": -2.0, "pnl": -2.0, "exit_reason": "SL"},
        {"regime": "TREND_UP", "setup": "BREAKOUT", "side": "BUY",
         "entry_price": 100.0, "sl_price": 98.0, "qty": 1, "net_pnl": -2.0, "pnl": -2.0, "exit_reason": "SL"},
    ]
    out = mod._summarize(rows)
    key = "TREND_UP:BREAKOUT:LONG"
    assert key in out
    assert out[key]["n"] == 5
    assert abs(out[key]["win_rate"] - 0.6) < 1e-6
    # Average win R = (1+1.5+2)/3 = 1.5
    assert abs(out[key]["avg_win_r"] - 1.5) < 1e-6
    # Average loss R = -1.0 (both losses were clean 1R losses)
    assert abs(out[key]["avg_loss_r"] - (-1.0)) < 1e-6
