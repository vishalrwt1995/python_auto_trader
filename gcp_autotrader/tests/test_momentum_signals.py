"""Unit + fidelity tests for the Momentum x Low-Vol channel domain logic (domain/momentum_signals).
The fidelity replay (opt-in, needs the backtest cache) proves rank_blend_select reproduces the
validated selection math the grind scripts used."""
from __future__ import annotations

import os
import pickle

import pytest

from autotrader.domain import momentum_signals as ms


# ── pure factor functions ────────────────────────────────────────────────────
def test_momentum_score_12_1():
    closes = [100.0] * 300
    idx = 260
    closes[idx - 252] = 100.0
    closes[idx - 21] = 120.0
    assert abs(ms.momentum_score(closes, idx) - 0.20) < 1e-9


def test_momentum_score_short_history_none():
    assert ms.momentum_score([100.0] * 100, 50) is None


def test_realized_vol():
    rets = [0.0] * 300
    for k in range(134, 260):
        rets[k] = 0.01 if k % 2 else -0.01
    v = ms.realized_vol(rets, 260, 126)
    assert v is not None and v > 0
    assert ms.realized_vol([0.0] * 50, 30, 126) is None


def test_median_turnover():
    closes = [10.0] * 100
    vols = [1e7] * 100                     # turnover = 1e8 per day
    assert abs(ms.median_turnover(closes, vols, 80, 60) - 1e8) < 1.0
    assert ms.median_turnover(closes, vols, 30, 60) is None


def test_universe_gates():
    assert ms.passes_universe_gates(50.0, 2e8, True)
    assert not ms.passes_universe_gates(20.0, 2e8, True)     # price floor
    assert not ms.passes_universe_gates(50.0, 5e7, True)     # < Rs10cr turnover
    assert not ms.passes_universe_gates(50.0, 2e8, False)    # no history


def test_nifty_regime_overlay():
    assert ms.nifty_regime_ok(list(range(1, 201)), 100)      # rising -> hold
    assert not ms.nifty_regime_ok(list(range(200, 0, -1)), 100)   # falling -> cash
    assert not ms.nifty_regime_ok([1.0, 2.0, 3.0], 100)      # insufficient -> fail-closed (cash)


def test_stops_and_target():
    assert ms.catastrophe_stop(100.0) == 40.0
    assert ms.unreachable_target(100.0) == 1100.0


def test_weights_and_qty():
    w = ms.target_weights(["A", "B", "C", "D"])
    assert all(abs(v - 0.25) < 1e-9 for v in w.values())
    assert ms.target_weights([]) == {}
    assert ms.position_qty(100.0, 200000.0, 0.05) == 100
    assert ms.position_qty(0.0, 200000.0, 0.05) == 0


# ── rank_blend_select: regime, ranking, buffer ───────────────────────────────
def test_regime_gate_goes_to_cash():
    cands = [{"symbol": f"S{i}", "momentum": 0.1, "vol": 0.02, "turnover": 2e8} for i in range(30)]
    assert ms.rank_blend_select(cands, regime_ok=False) == []


def test_picks_high_momentum_low_vol_first():
    cands = [{"symbol": f"S{i}", "momentum": i * 0.01, "vol": 0.05 - i * 0.001, "turnover": 2e8}
             for i in range(25)]
    sel = ms.rank_blend_select(cands, topn=5, buffer_mult=1.0)
    assert sel[0] == "S24"                                    # best on BOTH momentum and vol
    assert len(sel) == 5


def test_insufficient_candidates_empty():
    cands = [{"symbol": f"S{i}", "momentum": 0.1, "vol": 0.02, "turnover": 2e8} for i in range(10)]
    assert ms.rank_blend_select(cands, topn=20) == []


def test_buffer_keeps_stayer_in_band_drops_outside():
    # blended order is S0(best)..S39(worst): momentum descends, vol ascends together.
    cands = [{"symbol": f"S{i}", "momentum": -i * 0.01, "vol": 0.02 + i * 0.0001, "turnover": 2e8}
             for i in range(40)]
    no_buf = ms.rank_blend_select(cands, topn=20, buffer_mult=1.0)
    assert "S22" not in no_buf                                 # rank 22 not in plain top-20
    # buffer 1.5 -> band = top-30; a held name at rank 22 is KEPT (rides), bumping the #20
    with_buf = ms.rank_blend_select(cands, prev_holds=["S22"], topn=20, buffer_mult=1.5)
    assert "S22" in with_buf and len(with_buf) == 20
    # a held name OUTSIDE the band (rank 35 > 30) is NOT kept
    outside = ms.rank_blend_select(cands, prev_holds=["S35"], topn=20, buffer_mult=1.5)
    assert "S35" not in outside


# ── fidelity replay on the real survivorship-safe cache (opt-in) ─────────────
CACHE = os.path.expanduser("~/.autotrader_backtest_cache/pead_full_bars_2014.pkl")


@pytest.mark.skipif(not os.path.exists(CACHE), reason="backtest cache not present")
def test_fidelity_selection_matches_independent_rank_blend():
    """Build candidates from the real full universe at a fixed date via the domain helpers,
    then assert rank_blend_select == an independent inline rank-blend on the same inputs.
    Proves the domain implements the exact validated selection (no drift)."""
    bars = pickle.load(open(CACHE, "rb"))
    asof = "2023-06-30"
    cands = []
    for s, b in bars.items():
        if not b or len(b) < 300:
            continue
        d = [x[0] for x in b]; c = [float(x[4]) for x in b]; v = [float(x[5]) for x in b]
        i = max((k for k in range(len(d)) if d[k] <= asof), default=-1)
        if i < 252:
            continue
        rets = [0.0] + [c[k] / c[k - 1] - 1.0 if c[k - 1] > 0 else 0.0 for k in range(1, len(c))]
        tov = ms.median_turnover(c, v, i)
        if not ms.passes_universe_gates(c[i], tov, i >= 252):
            continue
        mom = ms.momentum_score(c, i); vol = ms.realized_vol(rets, i)
        if mom is None or vol is None:
            continue
        cands.append({"symbol": s.upper(), "momentum": mom, "vol": vol, "turnover": tov})

    assert len(cands) >= 100, "expected a broad >=Rs10cr universe from the survivorship-safe pool"
    sel = ms.rank_blend_select(cands, topn=20, buffer_mult=1.0, regime_ok=True)
    mr = {c["symbol"]: r for r, c in enumerate(sorted(cands, key=lambda c: -c["momentum"]))}
    vr = {c["symbol"]: r for r, c in enumerate(sorted(cands, key=lambda c: c["vol"]))}
    indep = [c["symbol"] for c in sorted(cands, key=lambda c: mr[c["symbol"]] + vr[c["symbol"]])][:20]
    assert sel == indep
    assert len(sel) == 20
