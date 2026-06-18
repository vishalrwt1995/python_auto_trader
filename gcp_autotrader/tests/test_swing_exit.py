"""Tests for the pure swing trailing-exit geometry (domain/swing_exit.py).

Two layers:
  1. Hand-worked unit tests for trailed_stop() and simulate_exit() — both BUY and
     SELL — pinning the arm-at-1R / trail-1R / max-hold / gap-fill behaviour.
  2. A fidelity check asserting simulate_exit() reproduces the backtest
     (backtest_v2/exit_lab.simulate, MOM_trail1.0_np_20d) EXACTLY over the cached
     resolved entries. This is the gate that proves the production trail matches
     the +39,310/yr validated geometry. It self-skips when the backtest cache is
     absent (e.g. CI without the 130MB pickle).
"""
from __future__ import annotations

import os

import pytest

from autotrader.domain.swing_exit import DEFAULT_ACTIVATE_R, simulate_exit, trailed_stop


def _bars(rows):
    """rows of (open, high, low, close) -> bars of (date, o, h, l, c)."""
    return [(f"2026-01-{i + 1:02d}", o, h, l, c) for i, (o, h, l, c) in enumerate(rows)]


# ── trailed_stop ────────────────────────────────────────────────────────────

def test_trailed_stop_not_armed_below_1R():
    # peak only +0.5R -> stay at original stop, not armed
    stop, armed = trailed_stop(100.0, True, 10.0, peak_price=105.0, base_sl=90.0)
    assert stop == 90.0 and armed is False


def test_trailed_stop_arms_at_exactly_1R_to_breakeven():
    # peak == entry + 1R -> arms, stop jumps to breakeven (entry)
    stop, armed = trailed_stop(100.0, True, 10.0, peak_price=110.0, base_sl=90.0, activate_R=1.0)
    assert stop == 100.0 and armed is True  # explicit 1.0R arm (default is now 1.75R)


def test_trailed_stop_ratchets_with_peak():
    stop, armed = trailed_stop(100.0, True, 10.0, peak_price=115.0, base_sl=90.0, activate_R=1.0)
    assert stop == 105.0 and armed is True  # 115 - 1R(10); explicit 1.0R arm


def test_trailed_stop_sell_side():
    # short: entry 100, peak (low) 90 == entry-1R -> arms, stop to breakeven 100
    stop, armed = trailed_stop(100.0, False, 10.0, peak_price=90.0, base_sl=110.0, activate_R=1.0)
    assert stop == 100.0 and armed is True
    # deeper: low 85 -> stop 95
    stop2, armed2 = trailed_stop(100.0, False, 10.0, peak_price=85.0, base_sl=110.0, activate_R=1.0)
    assert stop2 == 95.0 and armed2 is True


def test_trailed_stop_zero_sl_dist_guard():
    stop, armed = trailed_stop(100.0, True, 0.0, peak_price=200.0, base_sl=90.0)
    assert stop == 90.0 and armed is False


# ── simulate_exit (BUY) ───────────────────────────────────────────────────────

def test_simulate_clean_sl_hit():
    # never arms; bar1 low pierces original stop 90
    bars = _bars([(100, 102, 98, 101), (95, 96, 89, 90)])
    assert simulate_exit(bars, 0, True, 10.0, 20) == (1, 90.0, "SL")


def test_simulate_trail_exit():
    # bar0 peak 112 (>=+1R) arms; bar1 stop = 112-10 = 102, low 101 pierces it
    bars = _bars([(100, 112, 99, 110), (108, 109, 101, 103)])
    assert simulate_exit(bars, 0, True, 10.0, 20, activate_R=1.0) == (1, 102.0, "TRAIL")


def test_simulate_max_hold():
    # never arms, never stops; exit at close of bar offset == max_hold
    bars = _bars([(100, 105, 98, 104), (104, 106, 99, 105), (104, 107, 100, 106)])
    assert simulate_exit(bars, 0, True, 10.0, max_hold=2) == (2, 106.0, "MAX_HOLD")


def test_simulate_gap_through_stop_fills_at_open():
    # bar1 gaps below the stop -> fills at the (worse) open, not the stop level
    bars = _bars([(100, 102, 99, 101), (85, 86, 80, 82)])
    assert simulate_exit(bars, 0, True, 10.0, 20) == (1, 85.0, "SL")


# ── simulate_exit (SELL / short) ──────────────────────────────────────────────

def test_simulate_short_trail_exit():
    # short entry 100, sl0 110; bar0 low 88 arms (<= entry-1R); bar1 stop 88+10=98,
    # high 99 pierces it -> TRAIL exit at 98
    bars = _bars([(100, 101, 88, 90), (92, 99, 90, 95)])
    assert simulate_exit(bars, 0, False, 10.0, 20, activate_R=1.0) == (1, 98.0, "TRAIL")


def test_simulate_short_clean_sl_hit():
    # short never arms; bar1 high pierces original stop 110
    bars = _bars([(100, 102, 98, 101), (105, 111, 104, 109)])
    assert simulate_exit(bars, 0, False, 10.0, 20) == (1, 110.0, "SL")


# ── new default: arm at 1.75R (raised from 1R, 2026-06-18) ────────────────────

def test_default_activate_r_is_1_75():
    assert DEFAULT_ACTIVATE_R == 1.75


def test_default_arms_later_rides_longer():
    # peak 112 = +1.2R: armed at 1.0R but NOT at the 1.75R default -> rides to close
    bars = _bars([(100, 112, 99, 110), (108, 109, 101, 103)])
    assert simulate_exit(bars, 0, True, 10.0, 20) == (1, 103.0, "MAX_HOLD")
    # same bars, explicit 1.0R arm -> the old behaviour (trails out at 102)
    assert simulate_exit(bars, 0, True, 10.0, 20, activate_R=1.0) == (1, 102.0, "TRAIL")


def test_default_arms_above_1_75R():
    # peak 120 = +2.0R >= 1.75R default -> arms; stop 120-10=110, bar1 low 109 pierces
    bars = _bars([(100, 120, 99, 118), (115, 116, 109, 112)])
    assert simulate_exit(bars, 0, True, 10.0, 20) == (1, 110.0, "TRAIL")


# ── fidelity vs backtest exit_lab ─────────────────────────────────────────────

_CACHE = os.path.expanduser("~/.autotrader_backtest_cache")
_POOL = os.path.join(_CACHE, "s2_shorts_trades.json")
_HAS_CACHE = os.path.exists(os.path.join(_CACHE, "candles_daily_all.pkl")) and os.path.exists(_POOL)


@pytest.mark.skipif(not _HAS_CACHE, reason="backtest cache (candles_daily_all.pkl / pool) not present")
def test_fidelity_matches_exit_lab():
    """simulate_exit must equal exit_lab.simulate (trail-no-partial) for every
    resolved entry — proves prod trail == backtest geometry."""
    from autotrader.backtest_v2 import exit_lab

    resolved = exit_lab.load_resolved(_POOL)
    pol = exit_lab.POLICIES["MOM_trail1.0_np_20d"]
    n = mism = buys = sells = 0
    for t, bars, ei in resolved:
        sld = float(t.get("sl_dist") or 0.0)
        if sld <= 0:
            continue
        is_buy = t.get("direction", "BUY") == "BUY"
        legs = exit_lab.simulate(bars, ei, is_buy, sld, pol)
        _frac, price, off, reason = legs[-1]  # single leg for no-partial policy
        m_off, m_price, m_reason = simulate_exit(bars, ei, is_buy, sld, pol["max_hold"],
                                                 trail_R=pol["trail_R"], activate_R=pol["activate_R"])
        n += 1
        buys += is_buy
        sells += (not is_buy)
        if (off, reason) != (m_off, m_reason) or abs(price - m_price) > 1e-6:
            mism += 1
    assert n > 1000, f"expected a large resolved sample, got {n}"
    assert buys > 0 and sells > 0, f"want both directions exercised (buys={buys} sells={sells})"
    assert mism == 0, f"{mism}/{n} exits diverge from exit_lab.simulate"
