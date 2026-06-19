"""Unit tests for domain/pead_signals.py — the EVENT/PEAD channel signal logic.

Covers the reaction-day surprise, the pre-event run-up (anti-pump input), and the
Config B gate, including off-by-one / look-ahead guards, boundary conditions, and
fail-closed behaviour on missing inputs.
"""
from autotrader.domain.pead_signals import (
    earnings_surprise,
    pre_event_runup,
    passes_pead_gates,
    SURPRISE_MIN,
    ANTI_PUMP_MAX_RUNUP,
    MARKET_DD_GATE,
)


# ── earnings_surprise ─────────────────────────────────────────────────────────
def test_surprise_positive():
    assert abs(earnings_surprise([100.0, 100.0, 110.0], 2) - 0.10) < 1e-9


def test_surprise_negative():
    assert abs(earnings_surprise([100.0, 100.0, 95.0], 2) - (-0.05)) < 1e-9


def test_surprise_uses_prior_close_only():
    # surprise at ri must use close[ri-1], not anything later -> look-ahead-free
    closes = [50.0, 80.0, 88.0, 200.0]
    assert abs(earnings_surprise(closes, 2) - (88.0 / 80.0 - 1.0)) < 1e-9


def test_surprise_guards():
    assert earnings_surprise([100.0], 0) is None          # ri < 1
    assert earnings_surprise([0.0, 110.0], 1) is None      # prior close <= 0
    assert earnings_surprise([100.0, 110.0], 5) is None    # ri out of range


# ── pre_event_runup ───────────────────────────────────────────────────────────
def test_runup_basic():
    closes = [0.0] * 100
    closes[1] = 100.0      # base = close[ri-1-lookback] = close[1]
    closes[61] = 150.0     # close[ri-1] = close[61]; ri-1=61, lookback=60 -> ri=62
    assert abs(pre_event_runup(closes, 62, 60) - 0.5) < 1e-9


def test_runup_excludes_reaction_day():
    # run-up ends at ri-1 (the close BEFORE the reaction), never includes close[ri]
    closes = [0.0] * 70
    closes[5] = 100.0
    closes[65] = 120.0     # ri-1=65 -> ri=66, base idx = 65-60 = 5
    closes[66] = 999.0     # the reaction day must NOT affect run-up
    assert abs(pre_event_runup(closes, 66, 60) - 0.2) < 1e-9


def test_runup_guards():
    assert pre_event_runup([100.0, 110.0], 5, 60) is None   # not enough history
    closes = [0.0] * 100
    closes[61] = 150.0                                       # base close[1] == 0
    assert pre_event_runup(closes, 62, 60) is None


# ── passes_pead_gates ─────────────────────────────────────────────────────────
def test_gate_all_pass():
    # surprise >= 5%, run-up < 50%, market within 5% of highs
    assert passes_pead_gates(0.08, 0.20, -0.02) is True


def test_gate_surprise_too_small():
    assert passes_pead_gates(0.03, 0.20, -0.02) is False


def test_gate_pumped_name_excluded():
    assert passes_pead_gates(0.08, 0.80, -0.02) is False    # run-up 80% >= 50%


def test_gate_market_in_correction_excluded():
    assert passes_pead_gates(0.08, 0.20, -0.10) is False    # dd -10% <= -5% gate


def test_gate_fail_closed_on_none():
    assert passes_pead_gates(None, 0.20, -0.02) is False
    assert passes_pead_gates(0.08, None, -0.02) is False
    assert passes_pead_gates(0.08, 0.20, None) is False


def test_gate_boundaries():
    # surprise exactly at the floor qualifies (>=); healthy market
    assert passes_pead_gates(SURPRISE_MIN, 0.0, -0.001) is True
    # run-up exactly at the cap is excluded (strict <)
    assert passes_pead_gates(0.08, ANTI_PUMP_MAX_RUNUP, -0.02) is False
    # market dd exactly at the gate is excluded (strict >)
    assert passes_pead_gates(0.08, 0.20, MARKET_DD_GATE) is False
