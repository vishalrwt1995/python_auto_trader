"""CORE compounding fix — hardening validation before proposing a prod change.

Confirms the compounding uplift (fixed->NAV sizing) is robust, not window-driven or cost-fragile:
  1. per-year: fixed vs compound each calendar year (is the uplift consistent?)
  2. walk-forward windows (multiple start/end splits)
  3. cost-stress: compound at 1x / 2x / 3x the round-trip cost
Same faithful engine (prod rank_blend_select + integer shares + cap + catastrophe stop), ₹3L,
blend top30. Isolated, read-only, cached (free).
"""
from __future__ import annotations
import os, sys, pickle
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
CAPITAL = 300000.0
_UP = CostConfig.upstox()
def rt_cost(q, e, x, mult=1.0): return mult*compute_round_trip_cost(qty=int(q), entry_price=e, exit_price=x, is_swing=True, cfg=_UP) if q > 0 else 0.0

sym = {}
for s, bars in raw.items():
    if not bars or len(bars) < MOM_LOOKBACK + 5: continue
    dates=[b[0] for b in bars]; lows=[float(b[3]) for b in bars]; closes=[float(b[4]) for b in bars]; vols=[float(b[5]) for b in bars]
    rets=[0.0]+[(closes[i]/closes[i-1]-1.0) if closes[i-1]>0 else 0.0 for i in range(1,len(closes))]
    tov=[]; _rs=0.0; _w=[]
    for i in range(len(closes)):
        x=closes[i]*vols[i]; _w.append(x); _rs+=x
        if len(_w)>20: _rs-=_w.pop(0)
        tov.append(_rs/len(_w))
    sym[s]={"dates":dates,"lows":lows,"closes":closes,"rets":rets,"tov":tov,"dmap":{dates[i]:i for i in range(len(dates))}}
all_dates=sorted({d for v in sym.values() for d in v["dates"]}); gidx={d:i for i,d in enumerate(all_dates)}
def first_ge(iso):
    k=bisect_left(all_dates,iso); return all_dates[k] if k<len(all_dates) else None
rebal_dates=[d for d in sorted({first_ge(f"{y}-{m:02d}-01") for y in range(2016,2027) for m in (1,4,7,10)}-{None}) if d]
def i_le(s,d):
    j=bisect_right(sym[s]["dates"],d)-1; return j if j>=0 else None
def cand_at(d):
    out=[]
    for s,v in sym.items():
        i=i_le(s,d)
        if i is None or i-MOM_LOOKBACK<0: continue
        if not passes_universe_gates(v["closes"][i],v["tov"][i],has_history=True): continue
        mom=momentum_score(v["closes"],i); vol=realized_vol(v["rets"],i)
        if mom is None or vol is None: continue
        out.append({"symbol":s,"momentum":mom,"vol":vol,"turnover":v["tov"][i]})
    return out
baskets={d:rank_blend_select(cand_at(d)) for d in rebal_dates}
union=set().union(*[set(b) for b in baskets.values()])
cff,lff={},{}
for s in union:
    dm=sym[s]["dmap"]; cl=sym[s]["closes"]; lo=sym[s]["lows"]; ca=[]; la=[]; last=None
    for gd in all_dates:
        if gd in dm: i=dm[gd]; last=cl[i]; la.append(lo[i])
        else: la.append(None)
        ca.append(last)
    cff[s]=ca; lff[s]=la
print(f"{len(sym)} syms, {len(rebal_dates)} rebalances")

def run(compound, cost_mult=1.0):
    cash=CAPITAL; book={}; equity=[]
    for k,d in enumerate(rebal_dates):
        basket=baskets[d]
        if not basket: continue
        gi=gidx[d]; bset=set(basket); price={s:cff[s][gi] for s in set(book)|bset if cff[s][gi]}
        if compound:
            NAV=cash+sum(book[s]["qty"]*price.get(s,book[s]["entry"]) for s in book)
            for s in [x for x in book if x not in bset]:
                q=book[s]["qty"]; px=price.get(s,book[s]["entry"]); cash+=q*px-rt_cost(q,book[s]["entry"],px,cost_mult); del book[s]
            slice_amt=NAV/TOPN; cap_amt=slice_amt*MAX_WEIGHT_MULT
            cand=[(s,price[s]) for s in basket if s not in book and s in price]; qy={s:0 for s,_ in cand}
            for s,px in cand:
                base=int(slice_amt//px)
                if base>=1 and base*px<=cash and base*px<=cap_amt: qy[s]=base; cash-=base*px
            while True:
                best=bpx=br=None
                for s,px in cand:
                    if px>cash or qy[s]*px+px>cap_amt: continue
                    r=(qy[s]*px)/slice_amt
                    if br is None or r<br: best,bpx,br=s,px,r
                if best is None: break
                qy[best]+=1; cash-=bpx
            for s,px in cand:
                if qy[s]>=1: book[s]={"qty":qy[s],"entry":px}
        else:
            target=[{"symbol":s,"ref_price":price[s],"instrument_key":s} for s in basket if s in price]
            holdings=[{"symbol":s,"qty":b["qty"],"entry_price":b["entry"],"instrument_key":s} for s,b in book.items()]
            plan=plan_core_rebalance(target,holdings,CAPITAL,cfg=None)
            for sell in plan["sells"]:
                s=sell["symbol"]; q=book[s]["qty"]; px=price.get(s,book[s]["entry"]); cash+=q*px-rt_cost(q,book[s]["entry"],px,cost_mult); del book[s]
            for buy in plan["buys"]:
                s=buy["symbol"]; q=int(buy["qty"]); px=float(buy["entry_price"]); cash-=q*px; book[s]={"qty":q,"entry":px}
        gj=gidx[rebal_dates[k+1]] if k+1<len(rebal_dates) else len(all_dates)-1
        for g in (range(gi,gj+1) if k+1==len(rebal_dates) else range(gi,gj)):
            for s in list(book.keys()):
                low=lff[s][g]; stop=book[s]["entry"]*(1-CATASTROPHE_STOP_PCT)
                if low is not None and low<=stop:
                    q=book[s]["qty"]; cash+=q*stop-rt_cost(q,book[s]["entry"],stop,cost_mult); del book[s]
            equity.append((all_dates[g], cash+sum(b["qty"]*(cff[s][g] or b["entry"]) for s,b in book.items())))
    return equity
def cagr_dd(eq, lo=None, hi=None):
    pts=[(d,v) for d,v in eq if (lo is None or d>=lo) and (hi is None or d<=hi)]
    vs=[v for _,v in pts]; ds=[d for d,_ in pts]
    y=(date.fromisoformat(ds[-1])-date.fromisoformat(ds[0])).days/365.25
    cg=(vs[-1]/vs[0])**(1/y)-1; pk=-1e18; md=0.0
    for v in vs: pk=max(pk,v); md=min(md,v/pk-1)
    return cg, md

fx=run(False); cp=run(True)
print("\n=== 1. PER-YEAR: fixed vs compound (uplift should be +in up-years, -in down-years, net +) ===")
print(f"{'year':6} {'fixed':>7} {'compound':>9} {'uplift':>7}")
yrs=sorted({d[:4] for d,_ in fx})
def yr_ret(eq,y):
    pts=[v for d,v in eq if d[:4]==y]; return (pts[-1]/pts[0]-1) if len(pts)>1 else 0.0
wins=0
for y in yrs:
    f=yr_ret(fx,y); c=yr_ret(cp,y); u=c-f; wins+= (1 if u>0 else 0)
    print(f"{y:6} {f*100:6.1f}% {c*100:8.1f}% {u*100:+6.1f}%")
print(f"compound beat fixed in {wins}/{len(yrs)} years")
print("\n=== 2. WALK-FORWARD WINDOWS (CAGR fixed vs compound) ===")
for lo,hi in [(None,"2019-12-31"),("2019-01-01",None),("2016-01-01","2020-12-31"),("2021-01-01",None),(None,None)]:
    ff,_=cagr_dd(fx,lo,hi); cc2,dd2=cagr_dd(cp,lo,hi)
    print(f"  {(lo or 'start')[:7]}..{(hi or 'end')[:7]:8}  fixed {ff*100:5.1f}%  compound {cc2*100:5.1f}%  (uplift {(cc2-ff)*100:+.1f}%)")
print("\n=== 3. COST-STRESS (compound, cost x1/x2/x3) ===")
for m in (1.0,2.0,3.0):
    e=run(True,m); c,d=cagr_dd(e)
    print(f"  cost x{m:.0f}:  CAGR {c*100:5.1f}%  maxDD {d*100:6.1f}%")
cf,_=cagr_dd(fx); print(f"\n(fixed baseline CAGR {cf*100:.1f}%. Compound survives if x2/x3 still beat it.)")
