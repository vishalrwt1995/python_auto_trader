"""GF-1 tests for domain/gap_fade_signals.py — unit coverage of the pure gates/economics,
plus a fidelity replay that drives the pure module over the deep daily pool and reproduces
the validated backtest with the canonical PRICE_MIN=30 floor (docs/GAP_FADE_CHANNEL_PLAN.md:
OOS 2018-26 = +₹99,525 per ₹1L at notional 0.33×cap, 6/9 years positive — the no-floor
9/9/+112,885 leaned on penny F&O names that are partly a historical-price artifact). The
replay skips when the local backtest cache is absent (CI), like other backtest checks."""
from __future__ import annotations

import os
import gzip
import json
import pickle
import collections

import pytest

from autotrader.domain import gap_fade_signals as gf

CACHE = os.path.expanduser("~/.autotrader_backtest_cache")
DEEP = os.path.join(CACHE, "candles_daily_deep.pkl")
MASTER = os.path.join(CACHE, "nse_master.json.gz")


# ── gap_pct ─────────────────────────────────────────────────────────────────────
def test_gap_pct_basic():
    assert gf.gap_pct(106.0, 100.0) == pytest.approx(0.06)
    assert gf.gap_pct(100.0, 100.0) == pytest.approx(0.0)


@pytest.mark.parametrize("o,pc", [(None, 100.0), (100.0, None), (100.0, 0.0), (-1.0, 100.0), (100.0, -5.0)])
def test_gap_pct_bad_input_is_none(o, pc):
    assert gf.gap_pct(o, pc) is None


# ── is_locked_limit ───────────────────────────────────────────────────────────────
def test_is_locked_limit():
    assert gf.is_locked_limit(110.0, 100.0) is False      # has range
    assert gf.is_locked_limit(100.0, 100.0) is True       # single print / locked
    assert gf.is_locked_limit(99.0, 100.0) is True        # degenerate
    assert gf.is_locked_limit(None, 100.0) is True        # fail-closed
    assert gf.is_locked_limit(0.0, 0.0) is True


# ── passes_gap_gates ──────────────────────────────────────────────────────────────
def _ok_kwargs(**over):
    base = dict(gap=0.06, turnover_20d=2e8, high=108.0, low=101.0, price=105.0, is_fno=True)
    base.update(over)
    return base


def test_passes_gap_gates_happy():
    assert gf.passes_gap_gates(**_ok_kwargs()) is True


def test_passes_gap_gates_each_gate_blocks():
    assert gf.passes_gap_gates(**_ok_kwargs(gap=0.04)) is False          # gap too small
    assert gf.passes_gap_gates(**_ok_kwargs(gap=gf.GAP_MIN)) is False     # strictly greater
    assert gf.passes_gap_gates(**_ok_kwargs(turnover_20d=1e7)) is False   # illiquid
    assert gf.passes_gap_gates(**_ok_kwargs(price=10.0)) is False         # below price floor
    assert gf.passes_gap_gates(**_ok_kwargs(is_fno=False)) is False       # not F&O / not shortable
    assert gf.passes_gap_gates(**_ok_kwargs(high=105.0, low=105.0)) is False  # locked-limit
    assert gf.passes_gap_gates(**_ok_kwargs(gap=None)) is False           # fail-closed


# ── short_stop_price ──────────────────────────────────────────────────────────────
def test_short_stop_price_is_above_entry():
    assert gf.short_stop_price(100.0) == pytest.approx(103.0)   # 3% above (a short's stop)
    assert gf.short_stop_price(100.0, stop_pct=0.05) == pytest.approx(105.0)
    assert gf.short_stop_price(0.0) is None


# ── position_qty ──────────────────────────────────────────────────────────────────
def test_position_qty_floor_and_pct():
    # 0.20 × 100000 / 50 = 400 shares
    assert gf.position_qty(50.0, 100000.0) == 400
    # 0.33 × 100000 / 300 = 110 (floor)
    assert gf.position_qty(300.0, 100000.0, notional_cap_pct=0.33) == 110
    assert gf.position_qty(0.0, 100000.0) == 0
    assert gf.position_qty(50.0, 0.0) == 0


# ── fade_net (the canonical economics) ────────────────────────────────────────────
def test_fade_net_stopped():
    # high pierces the 3% stop -> capped loss = -(0.03 + 0.0027 + 0.0025 + 0.0015)
    n = gf.fade_net(entry=100.0, day_high=104.0, day_close=101.0)
    assert n == pytest.approx(-(0.03 + gf.MIS_COST + gf.SLIPPAGE + gf.STOP_SLIPPAGE))


def test_fade_net_cover_at_close_profit():
    # not stopped (high 102 < 103), closes down at 97 -> fade profit
    n = gf.fade_net(entry=100.0, day_high=102.0, day_close=97.0)
    assert n == pytest.approx(0.03 - gf.MIS_COST - gf.SLIPPAGE)   # -(97/100-1) = +0.03


def test_fade_net_cover_at_close_loss():
    # not stopped, but closes UP at 102 -> the fade lost (short closes higher)
    n = gf.fade_net(entry=100.0, day_high=102.5, day_close=102.0)
    assert n == pytest.approx(-(0.02) - gf.MIS_COST - gf.SLIPPAGE)


def test_fade_net_stop_takes_precedence_over_close():
    # high hits stop even though it closes down -> stop loss booked (path-conservative)
    n_stopped = gf.fade_net(entry=100.0, day_high=103.5, day_close=95.0)
    assert n_stopped == pytest.approx(-(0.03 + gf.MIS_COST + gf.SLIPPAGE + gf.STOP_SLIPPAGE))


@pytest.mark.parametrize("e,h,c", [(0.0, 100.0, 95.0), (None, 1.0, 1.0), (100.0, None, 95.0), (100.0, 100.0, None)])
def test_fade_net_bad_input_is_none(e, h, c):
    assert gf.fade_net(e, h, c) is None


# ── Fidelity replay: pure module reproduces the validated backtest ────────────────
@pytest.mark.skipif(not (os.path.exists(DEEP) and os.path.exists(MASTER)),
                    reason="backtest cache (deep daily + F&O master) not present")
def test_fidelity_replay_reproduces_backtest_oos():
    """Drive the pure gates (incl. PRICE_MIN=30) + fade_net over the deep daily pool with the
    canonical backtest config (F&O, gap>5%, price≥30, 3% stop, K=3 slots, notional 0.33×cap,
    −3%/day breaker) and confirm OOS 2018-26 == +₹99,525 per ₹1L, 6/9 years positive, no
    large down year (GAP_FADE_CHANNEL_PLAN.md)."""
    INDEX = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50", "NIFTYIT"}
    master = json.load(gzip.open(MASTER))
    fno = {d.get("underlying_symbol") or d.get("asset_symbol") for d in master
           if d.get("segment") == "NSE_FO" and d.get("instrument_type") == "FUT"
           and (d.get("underlying_symbol") or d.get("asset_symbol")) not in INDEX
           and (d.get("underlying_type") or "").upper() != "INDEX"}
    candles = pickle.load(open(DEEP, "rb"))

    byday = collections.defaultdict(list)
    for sym, b in candles.items():
        if sym not in fno or len(b) < 60:
            continue
        op = [x[1] for x in b]; hi = [x[2] for x in b]; lo = [x[3] for x in b]
        cl = [x[4] for x in b]; vo = [x[5] for x in b]; bd = [x[0] for x in b]
        cumcv = [0.0]
        for k in range(len(b)):
            cumcv.append(cumcv[-1] + cl[k] * vo[k])
        for i in range(21, len(b)):
            g = gf.gap_pct(op[i], cl[i - 1])
            tov = (cumcv[i] - cumcv[i - 20]) / 20.0
            if not gf.passes_gap_gates(g, tov, hi[i], lo[i], op[i], sym in fno):
                continue
            if not (-0.5 < cl[i] / op[i] - 1.0 < 0.5):    # sanity clip (matches backtest)
                continue
            byday[bd[i]].append((g, op[i], hi[i], cl[i]))

    cap, notl, lh, K = 100000.0, 0.33 * 100000.0, -0.03 * 100000.0, gf.MAX_POSITIONS
    yr = collections.defaultdict(float)
    for day, lst in byday.items():
        dr = 0.0
        for g, o, h, c in sorted(lst, reverse=True)[:K]:
            if dr <= lh:
                break
            pnl = notl * gf.fade_net(o, h, c)
            dr += pnl
            yr[day[:4]] += pnl

    oos_years = sorted(y for y in yr if y >= "2018")
    oos_total = sum(yr[y] for y in oos_years)
    pos = sum(1 for y in oos_years if yr[y] > 0)
    detail = ", ".join(f"{y}={yr[y]:,.0f}" for y in oos_years)
    assert len(oos_years) == 9, f"expected 9 OOS years, got {oos_years}"
    assert oos_total == pytest.approx(99525.0, rel=0.01), f"OOS total ₹{oos_total:,.0f} != 99,525 ({detail})"
    assert pos >= 6, f"expected >=6/9 positive OOS years, got {pos}/9 ({detail})"
    assert all(yr[y] > -2500 for y in oos_years), f"no large down year allowed ({detail})"
