"""CORE edge grind — parameterized sweep on the faithful engine, benchmarked vs Nifty50.

ISOLATION: reads cached data + imports prod GATE functions (momentum_score / realized_vol /
passes_universe_gates) read-only. All VARIATIONS (top-N, pure-mom vs blend, overlay, freq) are
LOCAL to this script — prod's rank_blend_select/settings are NOT edited. New file, no writes.

Method (relative frontier): EW daily-rebalanced portfolio return per variant, cost on churn,
optional regime risk-overlay (cash on risk-off days). IS 2016-2021 / OOS 2022-2026 split.
Benchmarks: Nifty50 buy-hold (real nifty_close) + EW-top-100 (own the large-cap universe).
The WINNER is separately reality-checked with the full ₹3L integer-share engine (core_faithful).

NOTE: frontier is gross of overlay toggle-cost (PANIC is ~23% of days -> real overlay churn cost
is high) -> overlay Calmar here is OPTIMISTIC; treat as upper bound, reality-check before trusting.
"""
from __future__ import annotations
import os, sys, pickle, json, statistics
from bisect import bisect_right
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
from autotrader.domain.core_signals import (
    momentum_score, realized_vol, passes_universe_gates, MOM_LOOKBACK, UNIVERSE_TOP,
)
C = os.path.expanduser("~/.autotrader_backtest_cache")
raw = pickle.load(open(f"{C}/swing_adj_bars_2015.pkl", "rb"))
mkt = json.load(open(f"{C}/market_inputs_2015.json"))
rg = json.load(open(f"{C}/regime_faithful_2015.json"))
def reg_of(d):
    v = rg.get(d); return (v.get("regime") if isinstance(v, dict) else v) if v else None

sym = {}
for s, bars in raw.items():
    if not bars or len(bars) < MOM_LOOKBACK + 5: continue
    dates = [b[0] for b in bars]; closes = [float(b[4]) for b in bars]; vols = [float(b[5]) for b in bars]
    rets = [0.0] + [(closes[i]/closes[i-1]-1.0) if closes[i-1] > 0 else 0.0 for i in range(1, len(closes))]
    tov = []; _rs = 0.0; _w = []
    for i in range(len(closes)):
        x = closes[i]*vols[i]; _w.append(x); _rs += x
        if len(_w) > 20: _rs -= _w.pop(0)
        tov.append(_rs/len(_w))
    sym[s] = {"dates": dates, "closes": closes, "rets": rets, "tov": tov,
              "rbd": {dates[i]: rets[i] for i in range(len(dates))}}
all_dates = sorted({d for v in sym.values() for d in v["dates"]})
print(f"loaded {len(sym)} symbols, {len(all_dates)} trading days")

from bisect import bisect_left
def first_ge(iso):
    k = bisect_left(all_dates, iso); return all_dates[k] if k < len(all_dates) else None
def q_dates():  return sorted({first_ge(f"{y}-{m:02d}-01") for y in range(2016,2027) for m in (1,4,7,10)} - {None})
def m_dates():  return sorted({first_ge(f"{y}-{m:02d}-01") for y in range(2016,2027) for m in range(1,13)} - {None})

def i_at(s, d):
    ds = sym[s]["dates"]; i = bisect_right(ds, d)-1; return i if i >= 0 else None

# candidates per rebalance date (shared across modes/topn) — gated, with mom/vol/tov
_cand_cache = {}
def candidates(d):
    if d in _cand_cache: return _cand_cache[d]
    out = []
    for s, v in sym.items():
        i = i_at(s, d)
        if i is None or i - MOM_LOOKBACK < 0: continue
        price = v["closes"][i]; tov = v["tov"][i]
        if not passes_universe_gates(price, tov, has_history=True): continue
        mom = momentum_score(v["closes"], i); vol = realized_vol(v["rets"], i)
        if mom is None or vol is None: continue
        out.append({"symbol": s, "momentum": mom, "vol": vol, "turnover": tov})
    _cand_cache[d] = out; return out

def select(cands, topn, mode):
    elig = [c for c in cands if c["vol"] and c["vol"] > 0]
    if len(elig) < topn: return []
    large = sorted(elig, key=lambda c: -c["turnover"])[:UNIVERSE_TOP]     # top-100 by turnover (prod)
    if mode == "pure_mom":
        rank = sorted(large, key=lambda c: -c["momentum"])
    elif mode == "pure_lowvol":
        rank = sorted(large, key=lambda c: c["vol"])
    else:  # blend (prod)
        mr = {c["symbol"]: r for r, c in enumerate(sorted(large, key=lambda c: -c["momentum"]))}
        vr = {c["symbol"]: r for r, c in enumerate(sorted(large, key=lambda c: c["vol"]))}
        rank = sorted(large, key=lambda c: mr[c["symbol"]] + vr[c["symbol"]])
    return [c["symbol"] for c in rank[:topn]]

RISK_OFF = {"PANIC", "TREND_DOWN"}
COST_RT = 0.006  # ~0.6% delivery round-trip (churn) — consistent across variants

def sim(mode, topn, freq, overlay, toggle_rate=0.0):
    """toggle_rate = one-way cost (portfolio value) applied on each risk-on<->risk-off switch
    (sell whole book on exit / buy whole book on re-entry). 0.0 = gross (grind default)."""
    rebs = q_dates() if freq == "q" else m_dates()
    rebs = [d for d in rebs if candidates(d) and len(candidates(d)) >= topn]
    eq = [(all_dates[bisect_left(all_dates, rebs[0])], 1.0)]
    val = 1.0; prev = set(); invested = True; toggles = 0
    for k, d in enumerate(rebs):
        basket = set(select(candidates(d), topn, mode))
        if not basket: continue
        if invested:                                     # rebalance churn only when holding
            churn = len(basket ^ prev)/(2*topn) if prev else 1.0
            val *= (1 - churn*COST_RT)
        prev = basket
        d_nx = rebs[k+1] if k+1 < len(rebs) else all_dates[-1]
        lo = bisect_left(all_dates, d); hi = bisect_left(all_dates, d_nx)
        for gi in range(lo, hi):
            day = all_dates[gi]
            risk_off = overlay and reg_of(day) in RISK_OFF
            if risk_off and invested:                    # exit to cash (sell whole book)
                val *= (1 - toggle_rate); invested = False; toggles += 1
            elif (not risk_off) and not invested:        # re-enter (buy whole book)
                val *= (1 - toggle_rate); invested = True; toggles += 1
            if invested and not risk_off:
                rs = [sym[s]["rbd"][day] for s in basket if day in sym[s]["rbd"]]
                if rs: val *= (1 + sum(rs)/len(rs))
            eq.append((day, val))
    return eq, toggles

def metrics(eq, lo=None, hi=None):
    pts = [(d, v) for d, v in eq if (lo is None or d >= lo) and (hi is None or d <= hi)]
    vs = [v for _, v in pts]; ds = [d for d, _ in pts]
    yrs = (date.fromisoformat(ds[-1]) - date.fromisoformat(ds[0])).days/365.25
    cagr = (vs[-1]/vs[0])**(1/yrs)-1
    pk = -1e18; mdd = 0.0
    for v in vs: pk = max(pk, v); mdd = min(mdd, v/pk-1)
    return cagr, mdd, (cagr/abs(mdd) if mdd else float('nan'))

# benchmark: Nifty50 buy-hold (real nifty_close)
nif = [(d, mkt[d]["nifty_close"]) for d in all_dates if d in mkt and mkt[d].get("nifty_close")]
nif = [(d, v) for d, v in nif if d >= all_dates[bisect_left(all_dates, q_dates()[0])]]
def bench_metrics(series):
    vs = [v for _, v in series]; ds = [d for d, _ in series]
    yrs = (date.fromisoformat(ds[-1]) - date.fromisoformat(ds[0])).days/365.25
    cagr = (vs[-1]/vs[0])**(1/yrs)-1; pk=-1e18; mdd=0.0
    for v in vs: pk=max(pk,v); mdd=min(mdd, v/pk-1)
    return cagr, mdd, cagr/abs(mdd)

IS_HI, OOS_LO = "2021-12-31", "2022-01-01"
def show(tag, eq, toggles=None):
    c, m, cal = metrics(eq)
    isc = metrics(eq, hi=IS_HI)[0]; oosc = metrics(eq, lo=OOS_LO)[0]
    tg = f"  toggles={toggles}" if toggles is not None else ""
    print(f"{tag:34} {c*100:5.1f}% {m*100:6.1f}% {cal:6.2f} | IS {isc*100:5.1f}% OOS {oosc*100:5.1f}%{tg}")

nc, nd, ncal = bench_metrics(nif)
print(f"\n{'strategy':34} {'CAGR':>6} {'maxDD':>7} {'Calmar':>6} |")
print("-"*92)
print(f"{'NIFTY50 buy-hold':34} {nc*100:5.1f}% {nd*100:6.1f}% {ncal:6.2f} |")
print("-"*92)
# how fragmented are the risk-off days? (drives toggle count)
w0 = all_dates[bisect_left(all_dates, q_dates()[0])]
roff = [reg_of(d) in RISK_OFF for d in all_dates if d >= w0]
stretches = (1 if roff and roff[0] else 0) + sum(1 for i in range(1, len(roff)) if roff[i] and not roff[i-1])
print(f"risk-off (PANIC/TREND_DOWN): {sum(roff)}/{len(roff)} days ({sum(roff)/len(roff)*100:.0f}%) in {stretches} separate stretches\n")

for mode, topn in (("blend", 30), ("pure_mom", 20)):
    base, _ = sim(mode, topn, "q", False)
    gross, tg = sim(mode, topn, "q", True, toggle_rate=0.0)
    net, tn = sim(mode, topn, "q", True, toggle_rate=0.003)
    print(f"# {mode} top{topn}")
    show("   no overlay", base)
    show("   overlay GROSS (0 toggle cost)", gross, tg)
    show("   overlay NET (0.3%/toggle)", net, tn)
    print()
print("Verdict: if 'overlay NET' Calmar stays well above 'no overlay' AND above Nifty (0.29) -> REAL edge.")
print("If NET collapses toward 'no overlay' -> cost-blind (risk-off days too scattered to trade).")
