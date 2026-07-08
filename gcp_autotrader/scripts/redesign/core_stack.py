"""CORE best-config grind — compound engine x selection x concentration.

Uses the validated COMPOUND deployment (size off current NAV, keep stayers) on the faithful
integer-share engine (₹3L, full-Upstox cost, catastrophe stop), sweeping:
  selection: blend (prod) / pure_mom / pure_lowvol
  concentration: top-10 / 15 / 20 / 30
Reports CAGR / maxDD / Calmar, IS 2016-21 / OOS 2022-26, vs Nifty50. Finds the best HONEST
CORE. Isolated new script, prod GATE functions imported read-only, variations local. Free.
"""
from __future__ import annotations
import os, sys, pickle, json
from bisect import bisect_left, bisect_right
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
from autotrader.domain.core_signals import (
    momentum_score, realized_vol, passes_universe_gates, MOM_LOOKBACK, UNIVERSE_TOP,
    CATASTROPHE_STOP_PCT, MAX_WEIGHT_MULT,
)
from autotrader.backtest.costs import CostConfig, compute_round_trip_cost

C = os.path.expanduser("~/.autotrader_backtest_cache")
raw = pickle.load(open(f"{C}/swing_adj_bars_2015.pkl", "rb"))
mkt = json.load(open(f"{C}/market_inputs_2015.json"))
CAPITAL = 300000.0
_UP = CostConfig.upstox()
def rt_cost(q, e, x): return compute_round_trip_cost(qty=int(q), entry_price=e, exit_price=x, is_swing=True, cfg=_UP) if q > 0 else 0.0

sym = {}
for s, bars in raw.items():
    if not bars or len(bars) < MOM_LOOKBACK + 5: continue
    dates = [b[0] for b in bars]; lows = [float(b[3]) for b in bars]; closes = [float(b[4]) for b in bars]; vols = [float(b[5]) for b in bars]
    rets = [0.0] + [(closes[i]/closes[i-1]-1.0) if closes[i-1] > 0 else 0.0 for i in range(1, len(closes))]
    tov = []; _rs = 0.0; _w = []
    for i in range(len(closes)):
        x = closes[i]*vols[i]; _w.append(x); _rs += x
        if len(_w) > 20: _rs -= _w.pop(0)
        tov.append(_rs/len(_w))
    sym[s] = {"dates": dates, "lows": lows, "closes": closes, "rets": rets, "tov": tov, "dmap": {dates[i]: i for i in range(len(dates))}}
all_dates = sorted({d for v in sym.values() for d in v["dates"]})
def first_ge(iso):
    k = bisect_left(all_dates, iso); return all_dates[k] if k < len(all_dates) else None
rebal_dates = [d for d in sorted({first_ge(f"{y}-{m:02d}-01") for y in range(2016,2027) for m in (1,4,7,10)} - {None}) if d]
gidx = {d: i for i, d in enumerate(all_dates)}

def i_le(s, d):
    j = bisect_right(sym[s]["dates"], d) - 1; return j if j >= 0 else None
_cc = {}
def cand_at(d):
    if d in _cc: return _cc[d]
    out = []
    for s, v in sym.items():
        i = i_le(s, d)
        if i is None or i - MOM_LOOKBACK < 0: continue
        if not passes_universe_gates(v["closes"][i], v["tov"][i], has_history=True): continue
        mom = momentum_score(v["closes"], i); vol = realized_vol(v["rets"], i)
        if mom is None or vol is None: continue
        out.append({"symbol": s, "momentum": mom, "vol": vol, "turnover": v["tov"][i]})
    _cc[d] = out; return out

def select(cands, topn, mode):
    elig = [c for c in cands if c["vol"] and c["vol"] > 0]
    if len(elig) < topn: return []
    large = sorted(elig, key=lambda c: -c["turnover"])[:UNIVERSE_TOP]
    if mode == "pure_mom":     rank = sorted(large, key=lambda c: -c["momentum"])
    elif mode == "pure_lowvol": rank = sorted(large, key=lambda c: c["vol"])
    else:
        mr = {c["symbol"]: r for r, c in enumerate(sorted(large, key=lambda c: -c["momentum"]))}
        vr = {c["symbol"]: r for r, c in enumerate(sorted(large, key=lambda c: c["vol"]))}
        rank = sorted(large, key=lambda c: mr[c["symbol"]] + vr[c["symbol"]])
    return [c["symbol"] for c in rank[:topn]]

# precompute baskets for all variants + aligned prices for the union
variants = [(mo, tn) for mo in ("blend", "pure_mom", "pure_lowvol") for tn in (10, 15, 20, 30)]
bask = {(mo, tn): {d: select(cand_at(d), tn, mo) for d in rebal_dates} for mo, tn in variants}
union = set()
for v in bask.values():
    for b in v.values(): union |= set(b)
cff, lff = {}, {}
for s in union:
    dm = sym[s]["dmap"]; cl = sym[s]["closes"]; lo = sym[s]["lows"]; ca = []; la = []; last = None
    for gd in all_dates:
        if gd in dm: i = dm[gd]; last = cl[i]; la.append(lo[i])
        else: la.append(None)
        ca.append(last)
    cff[s] = ca; lff[s] = la
print(f"{len(sym)} syms, {len(rebal_dates)} rebalances, {len(union)} names, {len(variants)} variants")

def run(basket_by_date, topn):
    cash = CAPITAL; book = {}; equity = []
    for k, d in enumerate(rebal_dates):
        basket = basket_by_date[d]
        if not basket: continue
        gi = gidx[d]; bset = set(basket)
        price = {s: cff[s][gi] for s in set(book) | bset if cff[s][gi]}
        NAV = cash + sum(book[s]["qty"]*price.get(s, book[s]["entry"]) for s in book)
        for s in [x for x in book if x not in bset]:
            q = book[s]["qty"]; px = price.get(s, book[s]["entry"]); cash += q*px - rt_cost(q, book[s]["entry"], px); del book[s]
        slice_amt = NAV/topn; cap_amt = slice_amt*MAX_WEIGHT_MULT
        cand = [(s, price[s]) for s in basket if s not in book and s in price]
        qy = {s: 0 for s, _ in cand}
        for s, px in cand:
            base = int(slice_amt//px)
            if base >= 1 and base*px <= cash and base*px <= cap_amt: qy[s] = base; cash -= base*px
        while True:
            best = bestpx = bestr = None
            for s, px in cand:
                if px > cash or qy[s]*px+px > cap_amt: continue
                r = (qy[s]*px)/slice_amt
                if bestr is None or r < bestr: best, bestpx, bestr = s, px, r
            if best is None: break
            qy[best] += 1; cash -= bestpx
        for s, px in cand:
            if qy[s] >= 1: book[s] = {"qty": qy[s], "entry": px}
        gj = gidx[rebal_dates[k+1]] if k+1 < len(rebal_dates) else len(all_dates)-1
        for g in (range(gi, gj+1) if k+1 == len(rebal_dates) else range(gi, gj)):
            for s in list(book.keys()):
                low = lff[s][g]; stop = book[s]["entry"]*(1-CATASTROPHE_STOP_PCT)
                if low is not None and low <= stop:
                    q = book[s]["qty"]; cash += q*stop - rt_cost(q, book[s]["entry"], stop); del book[s]
            equity.append((all_dates[g], cash + sum(b["qty"]*(cff[s][g] or b["entry"]) for s, b in book.items())))
    def met(pts):
        vs = [v for _, v in pts]; ds = [d for d, _ in pts]
        y = (date.fromisoformat(ds[-1])-date.fromisoformat(ds[0])).days/365.25
        cg = (vs[-1]/vs[0])**(1/y)-1; pk=-1e18; md=0.0
        for v in vs: pk=max(pk,v); md=min(md, v/pk-1)
        return cg, md
    c, m = met(equity); isc, _ = met([p for p in equity if p[0] <= "2021-12-31"]); oos, _ = met([p for p in equity if p[0] >= "2022-01-01"])
    return c, m, (c/abs(m) if m else 0), isc, oos

nv = [mkt[d]["nifty_close"] for d in all_dates if d in mkt and mkt[d].get("nifty_close") and d >= rebal_dates[0]]
ny = (date.fromisoformat([d for d in all_dates if d in mkt and d >= rebal_dates[0]][-1]) - date.fromisoformat(rebal_dates[0])).days/365.25
ncg = (nv[-1]/nv[0])**(1/ny)-1; npk=-1e18; nmd=0.0
for v in nv: npk=max(npk,v); nmd=min(nmd, v/npk-1)

print(f"\n{'config (compound, ₹3L)':26} {'CAGR':>6} {'maxDD':>7} {'Calmar':>6} | {'IS':>6} {'OOS':>6}")
print("-"*72)
print(f"{'NIFTY50 buy-hold':26} {ncg*100:5.1f}% {nmd*100:6.1f}% {ncg/abs(nmd):6.2f} |")
print("-"*72)
rows = []
for mo, tn in variants:
    c, m, cal, isc, oos = run(bask[(mo, tn)], tn)
    rows.append((cal, c, m, isc, oos, f"{mo} top{tn}"))
for cal, c, m, isc, oos, tag in sorted(rows, reverse=True):
    star = " *" if (c > ncg and cal > ncg/abs(nmd)) else ""
    print(f"{tag:26} {c*100:5.1f}% {m*100:6.1f}% {cal:6.2f} | {isc*100:5.1f}% {oos*100:5.1f}%{star}")
print("\n* = beats Nifty on BOTH CAGR and Calmar. Sorted by Calmar (risk-adjusted).")
