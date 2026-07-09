"""PEAD survivorship check — re-run the run-up-floor edge on the FULL universe (incl.
delisted names, pead_full_bars_2014.pkl from bt_bhavcopy_adj) vs the liquid local pkl.
If the edge holds here it's survivorship-robust → shippable. READ-ONLY, local, zero cost."""
from __future__ import annotations
import os, sys, json, pickle
from bisect import bisect_left
from datetime import date
from statistics import mean

sys.path.insert(0, "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.domain.pead_signals import earnings_surprise, pre_event_runup, ANTI_PUMP_LOOKBACK, ATR_SL_MULT
from autotrader.domain.swing_exit import simulate_exit
from autotrader.backtest.costs import compute_leg_cost, CostConfig

C = os.path.expanduser("~/.autotrader_backtest_cache")
UPSTOX = CostConfig.upstox(); SLIP = 0.001; CAPITAL = 200_000.0
ev = json.load(open(f"{C}/pead_nse_result_dates_2012_2026.json"))["events"]
bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))   # FULL universe incl. delisted
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
evsyms={e["symbol"] for e in ev}
print(f"FULL universe: {len(SYM)} syms | event-symbol coverage: {len(evsyms & set(SYM))}/{len(evsyms)} "
      f"({100*len(evsyms & set(SYM))//len(evsyms)}%)  [liquid pkl was 70%]")

pool=[]
for e in ev:
    sy=SYM.get(e["symbol"])
    if sy is None or e["date"]<"2015-01-01": continue
    dl=sy["d"]; ri=bisect_left(dl,e["date"])
    if ri>=len(dl) or ri<ANTI_PUMP_LOOKBACK+1 or ri+1>=len(sy["cl"]): continue
    sp=earnings_surprise(sy["cl"],ri); ru=pre_event_runup(sy["cl"],ri); m=mdd_at(dl[ri]); atr=sy["atr"][ri]
    if sp is None or ru is None or m is None or not atr or atr<=0: continue
    if not(sp>=0.05 and ru<0.75 and m>-0.05): continue
    pool.append({"sym":e["symbol"],"ei":ri+1,"entry_d":dl[ri+1],"sp":sp,"ru":ru,"atr":atr})
pool.sort(key=lambda c:c["entry_d"])

def run(flt, slots=5, risk=3000.0, max_hold=60):
    free=[""]*slots; tr=[]
    for c in pool:
        if not flt(c): continue
        sy=SYM[c["sym"]]; ei=c["ei"]; ed=c["entry_d"]
        k=next((j for j in range(slots) if free[j]<=ed),None)
        if k is None: continue
        epx=sy["o"][ei]; sl=ATR_SL_MULT*c["atr"]; qty=int(risk//sl)
        if qty<1 or epx<=0: continue
        if qty*epx>CAPITAL/slots: qty=int((CAPITAL/slots)//epx)
        if qty<1: continue
        off,xpx,_=simulate_exit(sy["bars"],ei,True,sl,max_hold,trail_R=1.0,activate_R=1.75)
        xi=min(ei+off,len(sy["bars"])-1); free[k]=sy["d"][xi]
        ef=epx*(1+SLIP); xf=xpx*(1-SLIP); g=(xf-ef)*qty
        cost=(compute_leg_cost(side="BUY",qty=qty,price=ef,is_swing=True,cfg=UPSTOX)
              +compute_leg_cost(side="SELL",qty=qty,price=xf,is_swing=True,cfg=UPSTOX))
        tr.append({"entry_d":ed,"exit_d":sy["d"][xi],"net":g-cost})
    return tr
def met(tr,lo=None,hi=None):
    t=[x for x in tr if (lo is None or x["exit_d"]>=lo) and (hi is None or x["entry_d"]<=hi)]
    if not t: return None
    byd={}
    for x in t: byd[x["exit_d"]]=byd.get(x["exit_d"],0.0)+x["net"]
    eq=CAPITAL; cur=[CAPITAL]
    for d in sorted(byd): eq+=byd[d]; cur.append(eq)
    p=-1e18; m=0.0
    for v in cur: p=max(p,v); m=min(m,v/p-1)
    y=(date.fromisoformat(t[-1]["exit_d"])-date.fromisoformat(t[0]["entry_d"])).days/365.25
    cg=((cur[-1]/CAPITAL)**(1/y)-1) if y>0 and cur[-1]>0 else 0.0
    return dict(n=len(t),net=sum(x["net"] for x in t),cagr=cg,mdd=m,calmar=(cg/abs(m)) if m else 0.0)

print(f"\npool (full universe, base-gated): {len(pool)}")
print(f"{'filter':26} {'FULL n/CAGR/DD/Cal':30} {'IS Cal':8} {'OOS Cal':8}  +yrs")
print("="*86)
FILT=[("BASELINE", lambda c:True),
      ("drop negative run-up", lambda c:c["ru"]>=0.0),
      ("run-up >= 15%", lambda c:c["ru"]>=0.15),
      ("run-up 15-35%", lambda c:0.15<=c["ru"]<0.35)]
for name,flt in FILT:
    tr=run(flt); f,i,o=met(tr),met(tr,hi="2020-12-31"),met(tr,lo="2021-01-01")
    if not f: print(f"{name:26} (none)"); continue
    yr={}
    for x in tr: yr[x["exit_d"][:4]]=yr.get(x["exit_d"][:4],0.0)+x["net"]
    pos=sum(1 for v in yr.values() if v>0)
    print(f"{name:26} n={f['n']:4} {f['cagr']*100:4.1f}%/{f['mdd']*100:6.1f}%/{f['calmar']:.2f}   "
          f"{(i['calmar'] if i else 0):.2f}    {(o['calmar'] if o else 0):.2f}    {pos}/{len(yr)}")
print("\nHOLDS if the run-up floor still lifts Calmar + both halves on the FULL (delisted-incl) universe.")
