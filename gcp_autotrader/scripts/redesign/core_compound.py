"""CORE compounding test — does redeploying idle cash (size off current NAV) lift CAGR?

Two variants of the faithful engine, same top-30 basket / cost / catastrophe stop:
  FIXED    = prod: plan_core_rebalance sizes new buys off FIXED channel_capital -> gains pile
             up as idle cash (~30% observed).
  COMPOUND = size new buys off CURRENT NAV (slice = NAV/30), deploying accumulated cash, while
             KEEPING stayers (winners still run, no forced trim). Same cap + residual sweep.
The delta = the pure deployment/compounding lever. Isolated, read-only, cached data (free).
"""
from __future__ import annotations
import os, sys, pickle, json
from bisect import bisect_left, bisect_right
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
from autotrader.domain.core_signals import (
    momentum_score, realized_vol, passes_universe_gates, rank_blend_select,
    MOM_LOOKBACK, CATASTROPHE_STOP_PCT, TOPN, MAX_WEIGHT_MULT,
)
from autotrader.services.core_trading_service import plan_core_rebalance
from autotrader.backtest.costs import CostConfig, compute_round_trip_cost

C = os.path.expanduser("~/.autotrader_backtest_cache")
raw = pickle.load(open(f"{C}/swing_adj_bars_2015.pkl", "rb"))
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

def i_le(s, d):
    j = bisect_right(sym[s]["dates"], d) - 1; return j if j >= 0 else None
def cand_at(d):
    out = []
    for s, v in sym.items():
        i = i_le(s, d)
        if i is None or i - MOM_LOOKBACK < 0: continue
        if not passes_universe_gates(v["closes"][i], v["tov"][i], has_history=True): continue
        mom = momentum_score(v["closes"], i); vol = realized_vol(v["rets"], i)
        if mom is None or vol is None: continue
        out.append({"symbol": s, "momentum": mom, "vol": vol, "turnover": v["tov"][i]})
    return out
baskets = {d: rank_blend_select(cand_at(d)) for d in rebal_dates}
union = set().union(*[set(b) for b in baskets.values()])
cff, lff = {}, {}
for s in union:
    dm = sym[s]["dmap"]; cl = sym[s]["closes"]; lo = sym[s]["lows"]; ca = []; la = []; last = None
    for gd in all_dates:
        if gd in dm: i = dm[gd]; last = cl[i]; la.append(lo[i])
        else: la.append(None)
        ca.append(last)
    cff[s] = ca; lff[s] = la
gidx = {d: i for i, d in enumerate(all_dates)}
print(f"{len(sym)} syms, {len(rebal_dates)} rebalances, {len(union)} basket names")

def run(capital, compound):
    cash = capital; book = {}; equity = []; tcost = 0.0
    for k, d in enumerate(rebal_dates):
        basket = baskets[d]
        if not basket: continue
        gi = gidx[d]; bset = set(basket)
        price = {s: cff[s][gi] for s in set(book) | bset if cff[s][gi]}
        if compound:
            NAV = cash + sum(book[s]["qty"]*price.get(s, book[s]["entry"]) for s in book)
            for s in [x for x in book if x not in bset]:            # sell dropped
                q = book[s]["qty"]; px = price.get(s, book[s]["entry"]); c = rt_cost(q, book[s]["entry"], px); tcost += c; cash += q*px - c; del book[s]
            slice_amt = NAV/TOPN; cap_amt = slice_amt*MAX_WEIGHT_MULT
            cand = [(s, price[s]) for s in basket if s not in book and s in price]
            qy = {s: 0 for s, _ in cand}
            for s, px in cand:                                       # pass1 EW base
                base = int(slice_amt//px)
                if base >= 1 and base*px <= cash and base*px <= cap_amt: qy[s] = base; cash -= base*px
            while True:                                              # pass2 residual sweep
                best = bestpx = bestr = None
                for s, px in cand:
                    if px > cash or qy[s]*px+px > cap_amt: continue
                    r = (qy[s]*px)/slice_amt
                    if bestr is None or r < bestr: best, bestpx, bestr = s, px, r
                if best is None: break
                qy[best] += 1; cash -= bestpx
            for s, px in cand:
                if qy[s] >= 1: book[s] = {"qty": qy[s], "entry": px}
        else:                                                        # FIXED (prod)
            target = [{"symbol": s, "ref_price": price[s], "instrument_key": s} for s in basket if s in price]
            holdings = [{"symbol": s, "qty": b["qty"], "entry_price": b["entry"], "instrument_key": s} for s, b in book.items()]
            plan = plan_core_rebalance(target, holdings, capital, cfg=None)
            for sell in plan["sells"]:
                s = sell["symbol"]; q = book[s]["qty"]; px = price.get(s, book[s]["entry"]); c = rt_cost(q, book[s]["entry"], px); tcost += c; cash += q*px - c; del book[s]
            for buy in plan["buys"]:
                s = buy["symbol"]; q = int(buy["qty"]); px = float(buy["entry_price"]); cash -= q*px; book[s] = {"qty": q, "entry": px}
        gj = gidx[rebal_dates[k+1]] if k+1 < len(rebal_dates) else len(all_dates)-1
        rng = range(gi, gj+1) if k+1 == len(rebal_dates) else range(gi, gj)
        for g in rng:
            for s in list(book.keys()):
                low = lff[s][g]; stop = book[s]["entry"]*(1-CATASTROPHE_STOP_PCT)
                if low is not None and low <= stop:
                    q = book[s]["qty"]; c = rt_cost(q, book[s]["entry"], stop); tcost += c; cash += q*stop - c; del book[s]
            mtm = cash + sum(b["qty"]*(cff[s][g] or b["entry"]) for s, b in book.items())
            equity.append((all_dates[g], mtm, cash))
    g = len(all_dates)-1
    oc = sum(rt_cost(b["qty"], b["entry"], cff[s][g] or b["entry"]) for s, b in book.items())
    equity[-1] = (equity[-1][0], equity[-1][1]-oc, equity[-1][2]); tcost += oc
    vs = [v for _, v, _ in equity]; ds = [d for d, _, _ in equity]
    yrs = (date.fromisoformat(ds[-1])-date.fromisoformat(ds[0])).days/365.25
    cagr = (vs[-1]/capital)**(1/yrs)-1; pk = -1e18; mdd = 0.0
    for v in vs: pk = max(pk, v); mdd = min(mdd, v/pk-1)
    avg_cash = sum(c/v for _, v, c in equity if v > 0)/len(equity)
    isc = None
    is_pts = [v for d, v, _ in equity if d <= "2021-12-31"]; oos_pts = [v for d, v, _ in equity if d >= "2022-01-01"]
    def cg(pts, y): return (pts[-1]/pts[0])**(1/y)-1 if len(pts) > 1 else 0
    return cagr, mdd, cagr/abs(mdd), tcost/capital, avg_cash, cg(is_pts, 6.0), cg(oos_pts, 4.5)

print(f"\n{'variant':28} {'CAGR':>6} {'maxDD':>7} {'Calmar':>6} {'cost%':>6} {'idle$':>6} | {'IS':>6} {'OOS':>6}")
print("-"*84)
for cap in (300000.0, 500000.0):
    for compound in (False, True):
        c, m, cal, cost, ac, isc, oos = run(cap, compound)
        tag = f"Rs{int(cap/100000)}L {'COMPOUND' if compound else 'fixed(prod)'}"
        print(f"{tag:28} {c*100:5.1f}% {m*100:6.1f}% {cal:6.2f} {cost*100:5.1f}% {ac*100:5.1f}% | {isc*100:5.1f}% {oos*100:5.1f}%")
print("\nNifty50 benchmark: 11.0% / -38% / 0.29")
print("If COMPOUND lifts CAGR toward ~13% with idle$ -> ~0, the deployment lever is real.")
