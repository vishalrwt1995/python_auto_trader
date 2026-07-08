"""CORE capital sensitivity — REAL prod engine at each capital level.

Drives the faithful engine (prod rank_blend_select + prod plan_core_rebalance integer-share
sizing with the per-name price cap + residual sweep + full-Upstox per-episode cost + catastrophe
stop) across capital levels, to quantify how much the small ₹3L allocation costs CORE.

At low capital the cap (=1.5*capital/30) excludes expensive winners (MRF/PAGEIND) and fixed
DP/STT costs are a bigger % ; both ease as capital grows. Selection is capital-independent ->
baskets computed once. Isolated new script, read-only, cached data (free). vs Nifty50 benchmark.
"""
from __future__ import annotations
import os, sys, pickle, json
from bisect import bisect_left
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
from autotrader.domain.core_signals import (
    momentum_score, realized_vol, passes_universe_gates, rank_blend_select,
    MOM_LOOKBACK, CATASTROPHE_STOP_PCT,
)
from autotrader.services.core_trading_service import plan_core_rebalance
from autotrader.backtest.costs import CostConfig, compute_round_trip_cost

C = os.path.expanduser("~/.autotrader_backtest_cache")
raw = pickle.load(open(f"{C}/swing_adj_bars_2015.pkl", "rb"))
mkt = json.load(open(f"{C}/market_inputs_2015.json"))
_UP = CostConfig.upstox()
def rt_cost(qty, entry, exit_):
    return compute_round_trip_cost(qty=int(qty), entry_price=entry, exit_price=exit_, is_swing=True, cfg=_UP) if qty > 0 else 0.0

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
rebal_dates = sorted({first_ge(f"{y}-{m:02d}-01") for y in range(2016, 2027) for m in (1,4,7,10)} - {None})
rebal_dates = [d for d in rebal_dates if d and d <= all_dates[-1]]

def i_le(s, d):
    ds = sym[s]["dates"]; i = bisect_left(ds, d) - (0 if (bisect_left(ds, d) < len(ds) and ds[bisect_left(ds, d)] == d) else 1)
    # simpler: last index with date <= d
    from bisect import bisect_right
    j = bisect_right(ds, d) - 1
    return j if j >= 0 else None
def cand_at(d):
    out = []
    for s, v in sym.items():
        i = i_le(s, d)
        if i is None or i - MOM_LOOKBACK < 0: continue
        price = v["closes"][i]; tov = v["tov"][i]
        if not passes_universe_gates(price, tov, has_history=True): continue
        mom = momentum_score(v["closes"], i); vol = realized_vol(v["rets"], i)
        if mom is None or vol is None: continue
        out.append({"symbol": s, "momentum": mom, "vol": vol, "turnover": tov})
    return out
baskets = {d: rank_blend_select(cand_at(d)) for d in rebal_dates}
print(f"{len(sym)} syms, {len(rebal_dates)} rebalances, baskets precomputed")

# aligned forward-filled close (MTM) + same-day low (catastrophe) for union of basket names
union = set().union(*[set(b) for b in baskets.values()])
cff, lff = {}, {}
for s in union:
    dm = sym[s]["dmap"]; cl = sym[s]["closes"]; lo = sym[s]["lows"]
    ca = []; la = []; last = None
    for gd in all_dates:
        if gd in dm:
            i = dm[gd]; last = cl[i]; la.append(lo[i])
        else:
            la.append(None)
        ca.append(last)
    cff[s] = ca; lff[s] = la
gidx = {d: i for i, d in enumerate(all_dates)}
print(f"aligned {len(union)} basket names")

def run(capital):
    cash = capital; book = {}; equity = []; tcost = 0.0; excluded = {}
    for k, d in enumerate(rebal_dates):
        basket = baskets[d]
        if not basket: continue
        gi = gidx[d]
        target = [{"symbol": s, "ref_price": cff[s][gi], "instrument_key": s} for s in basket if cff[s][gi]]
        holdings = [{"symbol": s, "qty": b["qty"], "entry_price": b["entry"], "instrument_key": s} for s, b in book.items()]
        plan = plan_core_rebalance(target, holdings, capital, cfg=None)
        bought = {b["symbol"] for b in plan["buys"]}
        for sell in plan["sells"]:
            s = sell["symbol"]; q = book[s]["qty"]; px = cff[s][gi] or book[s]["entry"]
            c = rt_cost(q, book[s]["entry"], px); tcost += c; cash += q*px - c; del book[s]
        for buy in plan["buys"]:
            s = buy["symbol"]; q = int(buy["qty"]); px = float(buy["entry_price"])
            cash -= q*px; book[s] = {"qty": q, "entry": px}
        for t in target:
            if t["symbol"] not in book and t["symbol"] not in bought:
                excluded[t["symbol"]] = t["ref_price"]
        gj = gidx[rebal_dates[k+1]] if k+1 < len(rebal_dates) else len(all_dates)-1
        for g in range(gi, gj+1 if k+1 == len(rebal_dates) else gj):
            for s in list(book.keys()):                              # catastrophe stop
                low = lff[s][g]; stop = book[s]["entry"]*(1-CATASTROPHE_STOP_PCT)
                if low is not None and low <= stop:
                    q = book[s]["qty"]; c = rt_cost(q, book[s]["entry"], stop); tcost += c
                    cash += q*stop - c; del book[s]
            mtm = cash + sum(b["qty"]*(cff[s][g] or b["entry"]) for s, b in book.items())
            equity.append((all_dates[g], mtm, cash))
    # terminal net of closing open positions
    g = len(all_dates)-1
    oc = sum(rt_cost(b["qty"], b["entry"], cff[s][g] or b["entry"]) for s, b in book.items())
    equity[-1] = (equity[-1][0], equity[-1][1]-oc, equity[-1][2]); tcost += oc
    vs = [v for _, v, _ in equity]; ds = [d for d, _, _ in equity]
    yrs = (date.fromisoformat(ds[-1]) - date.fromisoformat(ds[0])).days/365.25
    cagr = (vs[-1]/capital)**(1/yrs)-1; pk = -1e18; mdd = 0.0
    for v in vs: pk = max(pk, v); mdd = min(mdd, v/pk-1)
    avg_cash = sum(c/v for _, v, c in equity if v > 0)/len(equity)
    return cagr, mdd, cagr/abs(mdd), tcost, len(excluded), excluded, avg_cash

# Nifty benchmark
nif = [(d, mkt[d]["nifty_close"]) for d in all_dates if d in mkt and mkt[d].get("nifty_close") and d >= rebal_dates[0]]
nv = [v for _, v in nif]; nyrs = (date.fromisoformat(nif[-1][0])-date.fromisoformat(nif[0][0])).days/365.25
ncagr = (nv[-1]/nv[0])**(1/nyrs)-1; npk=-1e18; nmdd=0.0
for v in nv: npk=max(npk,v); nmdd=min(nmdd, v/npk-1)

print(f"\n{'capital':>9} {'CAGR':>6} {'maxDD':>7} {'Calmar':>6} {'lifetime cost':>14} {'names excl':>10} {'avg cash':>8}")
print("-"*72)
print(f"{'NIFTY50':>9} {ncagr*100:5.1f}% {nmdd*100:6.1f}% {ncagr/abs(nmdd):6.2f}")
print("-"*72)
last_excl = None
for cap in (300000, 500000, 1000000, 2500000, 5000000, 10000000, 50000000):
    cagr, mdd, cal, tc, nex, exd, acash = run(float(cap))
    print(f"{'Rs'+format(int(cap),',')[:-4]+'L' if cap>=100000 else cap:>9} {cagr*100:5.1f}% {mdd*100:6.1f}% {cal:6.2f} {'Rs'+format(int(tc),','):>13} ({tc/cap*100:4.1f}%) {nex:>6} {acash*100:6.1f}%")
    last_excl = exd
print(f"\nnames CORE can't hold at low capital (excluded by price-cap, sample): {sorted(set(k for k in (last_excl or {})))[:1]}")
# show which names get excluded at Rs3L specifically
c3 = run(300000.0)[5]
print(f"excluded @Rs3L ({len(c3)} names): {sorted(f'{k}(Rs{int(v)})' for k,v in c3.items())[:10]}")
