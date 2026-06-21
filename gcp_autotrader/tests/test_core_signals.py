"""CORE-1 tests for domain/core_signals.py — unit coverage of the pure selection, plus a
fidelity replay driving the pure module over the deep daily pool to reproduce the validated
blend backtest (mom+low-vol rank-blend, large-cap top-30, quarterly hold ≈ CAGR ~11% /
maxDD ~-35% / Calmar ~0.32). Replay skips without the local backtest cache (CI)."""
from __future__ import annotations

import os
import statistics
import collections
import pickle

import pytest

from autotrader.domain import core_signals as cs

DEEP = os.path.expanduser("~/.autotrader_backtest_cache/candles_daily_deep.pkl")


# ── pure units ────────────────────────────────────────────────────────────────
def test_momentum_score():
    closes = [100.0] * 300
    closes[300 - cs.MOM_LOOKBACK] = 100.0
    closes[300 - cs.MOM_SKIP] = 120.0
    assert cs.momentum_score(closes, 300) == pytest.approx(0.20)
    assert cs.momentum_score([1.0] * 10, 5) is None         # short history


def test_realized_vol():
    rets = [0.0] * 100 + [0.01, -0.01] * 30
    v = cs.realized_vol(rets, 160, window=60)
    assert v is not None and v > 0
    assert cs.realized_vol([0.0] * 10, 5, window=60) is None


def test_universe_gates():
    assert cs.passes_universe_gates(100.0, 2e8, True) is True
    assert cs.passes_universe_gates(10.0, 2e8, True) is False      # price floor
    assert cs.passes_universe_gates(100.0, 1e7, True) is False     # illiquid
    assert cs.passes_universe_gates(100.0, 2e8, False) is False    # no history


def test_rank_blend_select_picks_high_mom_low_vol():
    # 40 names; the blend should favor high-momentum + low-vol; topn small for the test
    cands = []
    for i in range(40):
        cands.append({"symbol": f"S{i}", "momentum": i / 100.0, "vol": 0.02 + i / 1000.0,
                      "turnover": 1e9})   # higher i = higher momentum BUT higher vol
    sel = cs.rank_blend_select(cands, topn=5, universe_top=40)
    assert len(sel) == 5
    # blend balances mom (favors high i) and low-vol (favors low i) -> mid-to-high, not the extremes
    assert all(s.startswith("S") for s in sel)


def test_rank_blend_large_cap_filter():
    # 120 names, only top-100 by turnover eligible
    cands = [{"symbol": f"S{i}", "momentum": 0.5, "vol": 0.03, "turnover": float(i)} for i in range(120)]
    sel = cs.rank_blend_select(cands, topn=30, universe_top=100)
    assert len(sel) == 30
    assert "S0" not in sel and "S19" not in sel               # lowest-turnover excluded


def test_rank_blend_too_few():
    assert cs.rank_blend_select([{"symbol": "A", "momentum": 0.1, "vol": 0.02, "turnover": 1e9}], topn=30) == []


def test_target_weights_and_qty():
    w = cs.target_weights(["A", "B", "C", "D"])
    assert w == {"A": 0.25, "B": 0.25, "C": 0.25, "D": 0.25}
    assert cs.target_weights([]) == {}
    assert cs.position_qty(100.0, 300000.0, 1 / 30) == int((1 / 30) * 300000 // 100)   # 100 sh
    assert cs.position_qty(0.0, 300000.0, 0.1) == 0


# ── fidelity replay: pure module reproduces the validated blend backtest ─────────
@pytest.mark.skipif(not os.path.exists(DEEP), reason="backtest cache (deep daily) not present")
def test_fidelity_replay_reproduces_blend_core():
    candles = pickle.load(open(DEEP, "rb"))
    sym = {}
    allset = set()
    for s, b in candles.items():
        if len(b) < 300:
            continue
        bd = [x[0] for x in b]; cl = [x[4] for x in b]; vo = [x[5] for x in b]
        rets = [0.0] + [cl[i] / cl[i - 1] - 1.0 if cl[i - 1] > 0 else 0.0 for i in range(1, len(cl))]
        cumcv = [0.0]
        for k in range(len(b)):
            cumcv.append(cumcv[-1] + cl[k] * vo[k])
        sym[s] = {"cl": cl, "rets": rets, "cumcv": cumcv, "idx": {d: i for i, d in enumerate(bd)}}
        allset.update(bd)
    cal = sorted(allset)
    qends = [cal[i] for i in range(len(cal)) if cal[i][5:7] in ("03", "06", "09", "12")
             and (i + 1 == len(cal) or cal[i + 1][5:7] != cal[i][5:7])]

    def candidates(d):
        out = []
        for s, v in sym.items():
            i = v["idx"].get(d)
            if i is None:
                continue
            tov = (v["cumcv"][i] - v["cumcv"][i - 20]) / 20.0 if i >= 20 else 0.0
            if not cs.passes_universe_gates(v["cl"][i], tov, i >= cs.MOM_LOOKBACK):
                continue
            mom = cs.momentum_score(v["cl"], i); vol = cs.realized_vol(v["rets"], i)
            if mom is None or vol is None:
                continue
            out.append({"symbol": s, "momentum": mom, "vol": vol, "turnover": tov})
        return out

    eq = 1.0; peak = 1.0; mdd = 0.0; basket = []; prev = set(); qi = 0; DELIV_RT = 0.0058
    for di, d in enumerate(cal):
        if qi < len(qends) and d == qends[qi]:
            nb = cs.rank_blend_select(candidates(d))
            if nb:
                turn = len(set(nb) ^ prev) / (2 * max(len(nb), 1)) if prev else 1.0
                eq *= (1 - turn * DELIV_RT); basket = nb; prev = set(nb)
            qi += 1
        if di > 0 and basket:
            rs = [sym[s]["cl"][sym[s]["idx"][d]] / sym[s]["cl"][sym[s]["idx"][cal[di - 1]]] - 1.0
                  for s in basket if d in sym[s]["idx"] and cal[di - 1] in sym[s]["idx"]
                  and sym[s]["cl"][sym[s]["idx"][cal[di - 1]]] > 0]
            if rs:
                eq *= (1 + statistics.fmean(rs))
        peak = max(peak, eq); mdd = min(mdd, eq / peak - 1.0)
    cagr = (eq ** (1 / (len(cal) / 252)) - 1) * 100
    calmar = cagr / abs(mdd * 100)
    assert 9.5 <= cagr <= 13.5, f"CAGR {cagr:.1f}% off validated ~11%"
    assert -42 <= mdd * 100 <= -28, f"maxDD {mdd*100:.0f}% off validated ~-35%"
    assert calmar >= 0.28, f"Calmar {calmar:.2f} below validated ~0.32"
