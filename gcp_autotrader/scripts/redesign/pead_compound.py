"""PEAD compounding test on the validated run-up>=0 edge. Size risk = compound_pct x rolling
REALIZED equity (look-ahead-free: only closed trades count) instead of fixed Rs3k. Also test a
liquidity cap (position <= liq_pct% of 60d median turnover) to get the FILLABLE number — thin
event names make uncapped compounding an un-fillable fiction (the swing 14% lesson).
READ-ONLY, local, zero cost. Liquid (deployable) universe."""
from __future__ import annotations
import os, sys, json, pickle
from bisect import bisect_left
from datetime import date
from statistics import mean, median

sys.path.insert(0, "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.domain.pead_signals import earnings_surprise, pre_event_runup, ANTI_PUMP_LOOKBACK, ATR_SL_MULT
from autotrader.domain.swing_exit import simulate_exit
from autotrader.backtest.costs import compute_leg_cost, CostConfig

C = os.path.expanduser("~/.autotrader_backtest_cache")
UPSTOX = CostConfig.upstox(); SLIP = 0.001; CAP0 = 200_000.0
ev = json.load(open(f"{C}/pead_nse_result_dates_2012_2026.json"))["events"]
bars = pickle.load(open(f"{C}/swing_adj_bars_2015.pkl", "rb"))
mkt = json.load(open(f"{C}/market_inputs_2015.json"))
mdl = sorted(d for d in mkt if mkt[d].get("nifty_close"))
pk=-1e18; ddd={}
for d in mdl:
    v=float(mkt[d]["nifty_close"]); pk=max(pk,v); ddd[d]=v/pk-1.0
def mdd_at(d):
    i=bisect_left(mdl,d)-1; return ddd[mdl[i]] if i>=0 else None
def atr14(hi,lo,cl):
    tr=[hi[0]-lo[0]]
    for i in range(1,len(cl)): tr.append(max(hi[i]-lo[i],abs(hi[i]-cl[i-1]),abs(lo[i]-cl[i-1])))
    out=[None]*len(cl); s=0.0
    for i in range(len(tr)):
        s+=tr[i]
        if i>=14: s-=tr[i-14]
        if i>=13: out[i]=s/14.0
    return out
SYM={}
for s,b in bars.items():
    if not b or len(b)<ANTI_PUMP_LOOKBACK+20: continue
    d=[x[0] for x in b]; o=[float(x[1]) for x in b]; hi=[float(x[2]) for x in b]
    lo=[float(x[3]) for x in b]; cl=[float(x[4]) for x in b]; vol=[float(x[5]) for x in b]
    SYM[s]={"d":d,"o":o,"cl":cl,"vol":vol,"bars":b,"atr":atr14(hi,lo,cl)}
pool=[]
for e in ev:
    sy=SYM.get(e["symbol"])
    if sy is None or e["date"]<"2015-01-01": continue
    dl=sy["d"]; ri=bisect_left(dl,e["date"])
    if ri>=len(dl) or ri<ANTI_PUMP_LOOKBACK+1 or ri+1>=len(sy["cl"]): continue
    sp=earnings_surprise(sy["cl"],ri); ru=pre_event_runup(sy["cl"],ri); m=mdd_at(dl[ri]); atr=sy["atr"][ri]
    if sp is None or ru is None or m is None or not atr or atr<=0: continue
    if not(sp>=0.05 and ru<0.75 and m>-0.05 and ru>=0.0): continue   # <-- run-up>=0 edge baked in
    tov=median([sy["cl"][j]*sy["vol"][j] for j in range(ri-60,ri)]) if ri>=60 else 0.0
    pool.append({"sym":e["symbol"],"ei":ri+1,"entry_d":dl[ri+1],"atr":atr,"tov":tov})
pool.sort(key=lambda c:c["entry_d"])

def run(compound_pct=None, fixed_risk=3000.0, liq_pct=None, slots=5):
    free=[""]*slots; tr=[]; pend=[]; realized=0.0
    for c in pool:
        # settle closed trades into realized equity (look-ahead-free)
        keep=[]
        for xd,nt in pend:
            if xd<=c["entry_d"]: realized+=nt
            else: keep.append((xd,nt))
        pend=keep
        equity=CAP0+realized
        risk=(compound_pct/100.0*equity) if compound_pct else fixed_risk
        sy=SYM[c["sym"]]; ei=c["ei"]; ed=c["entry_d"]
        k=next((j for j in range(slots) if free[j]<=ed),None)
        if k is None: continue
        epx=sy["o"][ei]; sl=ATR_SL_MULT*c["atr"]; qty=int(risk//sl)
        if qty<1 or epx<=0: continue
        if qty*epx>equity/slots: qty=int((equity/slots)//epx)          # per-slot notional (also compounds)
        if liq_pct and c["tov"]>0:                                     # liquidity cap
            qty=min(qty,int((liq_pct/100.0*c["tov"])//epx))
        if qty<1: continue
        off,xpx,_=simulate_exit(sy["bars"],ei,True,sl,60,trail_R=1.0,activate_R=1.75)
        xi=min(ei+off,len(sy["bars"])-1); free[k]=sy["d"][xi]
        ef=epx*(1+SLIP); xf=xpx*(1-SLIP); g=(xf-ef)*qty
        cost=(compute_leg_cost(side="BUY",qty=qty,price=ef,is_swing=True,cfg=UPSTOX)
              +compute_leg_cost(side="SELL",qty=qty,price=xf,is_swing=True,cfg=UPSTOX))
        net=g-cost; tr.append({"entry_d":ed,"exit_d":sy["d"][xi],"net":net}); pend.append((sy["d"][xi],net))
    return tr
def met(tr,lo=None,hi=None):
    t=[x for x in tr if (lo is None or x["exit_d"]>=lo) and (hi is None or x["entry_d"]<=hi)]
    if not t: return None
    byd={}
    for x in t: byd[x["exit_d"]]=byd.get(x["exit_d"],0.0)+x["net"]
    eq=CAP0; cur=[CAP0]
    for d in sorted(byd): eq+=byd[d]; cur.append(eq)
    p=-1e18; m=0.0
    for v in cur: p=max(p,v); m=min(m,v/p-1)
    y=(date.fromisoformat(t[-1]["exit_d"])-date.fromisoformat(t[0]["entry_d"])).days/365.25
    cg=((cur[-1]/CAP0)**(1/y)-1) if y>0 and cur[-1]>0 else 0.0
    return dict(n=len(t),net=sum(x["net"] for x in t),cagr=cg,mdd=m,calmar=(cg/abs(m)) if m else 0.0,final=cur[-1])

CONFIGS=[
    ("FIXED Rs3k (validated edge)", dict(fixed_risk=3000.0)),
    ("compound 1.5%/eq (=Rs3k@start)", dict(compound_pct=1.5)),
    ("compound 2.0%/eq", dict(compound_pct=2.0)),
    ("compound 2.5%/eq", dict(compound_pct=2.5)),
    ("compound 1.5% + 1% liq-cap (FILLABLE)", dict(compound_pct=1.5, liq_pct=1.0)),
    ("compound 2.0% + 1% liq-cap (FILLABLE)", dict(compound_pct=2.0, liq_pct=1.0)),
    ("compound 2.0% + 0.5% liq-cap", dict(compound_pct=2.0, liq_pct=0.5)),
]
print(f"pool (run-up>=0 edge): {len(pool)}\n")
print(f"{'config':40} {'FULL n/CAGR/DD/Cal':28} {'IS Cal':7} {'OOS Cal':8} finalRs")
print("="*100)
for name,kw in CONFIGS:
    tr=run(**kw); f,i,o=met(tr),met(tr,hi="2020-12-31"),met(tr,lo="2021-01-01")
    if not f: print(f"{name:40} (none)"); continue
    print(f"{name:40} n={f['n']:3} {f['cagr']*100:4.1f}%/{f['mdd']*100:6.1f}%/{f['calmar']:.2f}   "
          f"{(i['calmar'] if i else 0):.2f}    {(o['calmar'] if o else 0):.2f}   {f['final']/1e5:.1f}L")
print("\nRead: compounding lifts CAGR but DD grows ~in step (Calmar ~flat = leverage, not edge).")
print("Liq-cap shows the FILLABLE version; the gap to uncapped = un-fillable thin-name fiction.")
