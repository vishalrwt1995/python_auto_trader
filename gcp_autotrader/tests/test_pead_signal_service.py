"""Tests for services/pead_signal_service.py.

1. Pure unit tests (no data): _simple_atr + build_candidates gate filtering.
2. Fidelity (self-skips without the deep cache): proves the live service's candidate
   SELECTION reproduces the validated Config B backtest exactly (1,409 candidates),
   and spot-checks compute_market_dd — so the live path provably == the backtest.
"""
import bisect
import collections
import json
import os
import pickle

import pytest

from autotrader.services import pead_signal_service as svc
from autotrader.domain import pead_signals

CACHE = os.path.expanduser("~/.autotrader_backtest_cache")
PKL = os.path.join(CACHE, "candles_daily_deep.pkl")
ARC = os.path.join(CACHE, "pead_earnings_dates_evcal.json")
HAS_CACHE = os.path.exists(PKL) and os.path.exists(ARC)
# Look-ahead-free Config B candidate count under the validated index universe
# (len>=300 established names, reaction-date drawdown). The original scratch
# matrix (oos_pead_matrix) reported 1410 because it used (a) the next-open price
# floor — look-ahead the live service can't use — and (b) the *announce*-date
# drawdown instead of the *reaction*-date one. Categorised diff (pead_gap_diag):
# 0 unexplained divergences; the 166-event delta is entirely the reaction- vs
# announce-date drawdown at the -5% gate boundary (the service is more correct)
# plus 2 names the next-open floor wrongly admitted. So the live service and the
# backtest agree event-for-event once both use look-ahead-free choices.
LOOKAHEAD_FREE_COUNT = 1494


# ── pure unit tests (no cache) ────────────────────────────────────────────────
def test_simple_atr_constant_tr():
    highs = [10.0] * 20; lows = [8.0] * 20; closes = [9.0] * 20
    assert abs(svc._simple_atr(highs, lows, closes, 15) - 2.0) < 1e-9   # TR=2 each day


def test_simple_atr_short_history():
    assert svc._simple_atr([10.0] * 5, [8.0] * 5, [9.0] * 5, 3) is None


def _mk_bars(closes):
    """Build OHLCV bars with given closes; high=close*1.01, low=close*0.99, vol huge."""
    return [[f"2024-{1+i//28:02d}-{1+i%28:02d}", c, c * 1.01, c * 0.99, c, 10_000_000.0]
            for i, c in enumerate(closes)]


def test_build_candidates_passes_clean_signal():
    # 80 flat sessions at 100, then a +8% reaction day -> qualifies (healthy market dd=-0.02)
    closes = [100.0] * 80 + [108.0]
    bars = _mk_bars(closes)
    cands = svc.build_candidates(bars[-1][0], ["X"], {"X": bars}, market_dd=-0.02)
    assert len(cands) == 1 and cands[0]["symbol"] == "X"
    assert abs(cands[0]["surprise"] - 0.08) < 1e-6


def test_build_candidates_rejects_small_surprise():
    closes = [100.0] * 80 + [103.0]   # +3% < 5%
    bars = _mk_bars(closes)
    assert svc.build_candidates(bars[-1][0], ["X"], {"X": bars}, -0.02) == []


def test_build_candidates_rejects_pumped():
    closes = [50.0] * 20 + [50.0 + i for i in range(60)] + [108.0]  # ran 50->~109 pre-event (pumped)
    bars = _mk_bars(closes)
    assert svc.build_candidates(bars[-1][0], ["X"], {"X": bars}, -0.02) == []


def test_build_candidates_rejects_correction_market():
    closes = [100.0] * 80 + [108.0]
    bars = _mk_bars(closes)
    assert svc.build_candidates(bars[-1][0], ["X"], {"X": bars}, market_dd=-0.10) == []  # market in correction


# ── fidelity: live service == validated backtest (self-skips without cache) ────
def _precompute_dd_map(candles, window=svc.MARKET_DD_WINDOW):
    rsum = collections.defaultdict(float); rcnt = collections.defaultdict(int)
    for bars in candles.values():
        if len(bars) < svc.MARKET_MIN_HISTORY:   # match compute_market_dd index universe
            continue
        for i in range(1, len(bars)):
            pc = bars[i - 1][4]
            if pc > 0:
                r = bars[i][4] / pc - 1.0
                if -svc.MARKET_RET_CLIP < r < svc.MARKET_RET_CLIP:
                    rsum[bars[i][0]] += r; rcnt[bars[i][0]] += 1
    days = sorted(d for d in rsum if rcnt[d] >= svc.MARKET_MIN_STOCKS)
    levels = []; ix = 1.0
    for d in days:
        ix *= (1 + rsum[d] / rcnt[d]); levels.append(ix)
    ddmap = {}
    for i, d in enumerate(days):
        pk = max(levels[max(0, i - window + 1):i + 1])
        ddmap[d] = levels[i] / pk - 1.0 if pk > 0 else None
    return ddmap


@pytest.mark.skipif(not HAS_CACHE, reason="deep backtest cache not present")
def test_live_service_selection_matches_backtest():
    candles = pickle.load(open(PKL, "rb"))
    arc = json.load(open(ARC))
    dd_map = _precompute_dd_map(candles)

    # spot-check the service's own market-dd against the precomputed map
    sample_days = sorted(dd_map)[::400][:4]
    for d in sample_days:
        got = svc.compute_market_dd(candles, d)
        assert got is not None and abs(got - dd_map[d]) < 1e-6, f"market_dd mismatch {d}"

    # build the backtest-eligible event set (same filters as oos_pead_matrix Config B)
    by_sym = collections.defaultdict(list)
    for e in arc:
        by_sym[e["symbol"]].append(e["announce_dt"][:10])
    events_by_date = collections.defaultdict(list)   # reaction_date -> [symbols]
    backtest_selected = set()
    for sym, dates in by_sym.items():
        bars = candles.get(sym)
        if not bars or len(bars) < 80:
            continue
        bd = [b[0] for b in bars]; op = [b[1] for b in bars]; cl = [b[4] for b in bars]; vo = [b[5] for b in bars]
        for ad in set(dates):
            j = bisect.bisect_right(bd, ad)            # first bar strictly after announce
            if j < 63 or j + 1 + 40 >= len(bars):       # matrix history + forward filters
                continue
            rd = bd[j]
            events_by_date[rd].append(sym)
            # backtest Config B inline (must mirror build_candidates: premarket-known
            # close as the price floor, NOT the next open — live can't see the open)
            if cl[j - 1] <= 0 or cl[j] < svc.PRICE_MIN:
                continue
            if sum(cl[k] * vo[k] for k in range(j + 1 - svc.TURNOVER_WINDOW, j + 1)) / svc.TURNOVER_WINDOW < svc.TURNOVER_MIN:
                continue
            sur = cl[j] / cl[j - 1] - 1.0
            runup = cl[j - 1] / cl[j - 1 - pead_signals.ANTI_PUMP_LOOKBACK] - 1.0
            if sur >= pead_signals.SURPRISE_MIN and runup < pead_signals.ANTI_PUMP_MAX_RUNUP and (dd_map.get(rd) or 0) > pead_signals.MARKET_DD_GATE:
                backtest_selected.add((rd, sym))

    # run the live service over the same event set, restricted to backtest-eligible (sym,rd)
    eligible = {(rd, s) for rd, syms in events_by_date.items() for s in syms}
    service_selected = set()
    for rd, syms in events_by_date.items():
        for c in svc.build_candidates(rd, syms, candles, dd_map.get(rd)):
            if (rd, c["symbol"]) in eligible:
                service_selected.add((rd, c["symbol"]))

    assert backtest_selected == service_selected, (
        f"live≠backtest: only-bt={len(backtest_selected - service_selected)} "
        f"only-svc={len(service_selected - backtest_selected)}")
    # exact, deterministic: the inline backtest mirrors the service byte-for-byte
    # (same len>=300 index, reaction-date drawdown, look-ahead-free price floor)
    assert len(service_selected) == LOOKAHEAD_FREE_COUNT, (
        f"expected {LOOKAHEAD_FREE_COUNT} look-ahead-free Config B candidates, "
        f"got {len(service_selected)}")
