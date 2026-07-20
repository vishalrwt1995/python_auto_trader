"""Insider GOD-MODE Phase 2 -- signal-quality selection, on the two Phase-1 frontier regime
bases: b200>50 (best Calmar 0.61 / DD -22%) and Nifty>100DMA (best CAGR +21% / DD -38%). Sweeps
category / cluster / first-buy / value / holding-delta / dip-vs-strength and combos, reporting
FULL+IS+OOS Calmar so we lift CAGR+Calmar ROBUSTLY (both halves) without over-filtering to a
fragile subset. Loads the enriched cache from insider_god.py. READ-ONLY, single-process, cached."""
import os
for _v in ("OMP_NUM_THREADS","OPENBLAS_NUM_THREADS","MKL_NUM_THREADS","NUMEXPR_NUM_THREADS","VECLIB_MAXIMUM_THREADS"):
    os.environ[_v]="4"
import sys, json, pickle
from bisect import bisect_left
from collections import defaultdict
sys.path.insert(0,"/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.backtest.costs import compute_leg_cost, CostConfig

C=os.path.expanduser("~/.autotrader_backtest_cache")
UPSTOX=CostConfig.upstox(); CAP0,SLIP,IS_END=200_000.0,0.001,"2020-12-31"
obj=pickle.load(open(os.path.join(C,"insider_cands_enriched.pkl"),"rb")); SYM,cands=obj["SYM"],obj["cands"]
mkt=json.load(open(f"{C}/market_inputs_2015.json"))
md=sorted(x for x in mkt if mkt[x].get("nifty_close")); nc=[float(mkt[x]["nifty_close"]) for x in md]
ma100=[None]*len(nc); run=0.0
for i in range(len(nc)):
    run+=nc[i]
    if i>=100: run-=nc[i-100]
    if i>=99: ma100[i]=run/100.0
def nifty100(c):
    i=bisect_left(md,c["ed"])-1
    return i<0 or ma100[i] is None or nc[i]>ma100[i]

def seg(points,start_eq,years):
    if not points or start_eq<=0: return 0.0,0.0
    peak=start_eq; mdd=0.0; last=start_eq
    for _,eq in points:
        peak=max(peak,eq); mdd=min(mdd,eq/peak-1.0); last=eq
    return ((last/start_eq)**(1/years)-1 if years>0 else 0.0), mdd

def walk(cfg):
    hold=cfg["hold"];slots=cfg["slots"];tmin=cfg.get("turn_min",10e7);stop_mult=cfg.get("stop_mult",2.5)
    risk=cfg.get("risk_pct",0.015);sect_cap=cfg.get("sect_cap",0);rf=cfg.get("regime");sf=cfg.get("select")
    equity=CAP0;free=[""]*slots;ssec=[None]*slots;openp=[];closed=[]
    for c in cands:
        if c["turn"]<tmin: continue
        if rf and not rf(c): continue
        if sf and not sf(c): continue
        ed=c["ed"];still=[]
        for xd,pnl,sl in openp:
            if xd<=ed: equity+=pnl;closed.append((xd,pnl))
            else: still.append((xd,pnl,sl))
        openp=still
        for k in range(slots):
            if free[k] and free[k]<=ed: free[k]="";ssec[k]=None
        if sect_cap and c["sec"]!="?" and sum(1 for k in range(slots) if ssec[k]==c["sec"])>=sect_cap: continue
        slot=next((k for k in range(slots) if not free[k]),None)
        if slot is None: continue
        S=SYM[c["sym"]];ref=c["ref"];epx=S["o"][ref]
        if epx<=0: continue
        qty=int((risk*equity)//c["sl"])
        if qty<1: continue
        if qty*epx>equity/slots: qty=int((equity/slots)//epx)
        if qty<1: continue
        xi=min(ref+hold,len(S["c"])-1);xpx=S["c"][xi]
        if stop_mult:
            stop=epx-(stop_mult/2.5)*c["sl"]
            for k in range(ref+1,xi+1):
                if S["l"][k]<=stop: xpx=stop;xi=k;break
        xd=S["d"][xi];ef=epx*(1+SLIP);xf=xpx*(1-SLIP)
        pnl=(xf-ef)*qty-(compute_leg_cost(side="BUY",qty=qty,price=ef,is_swing=True,cfg=UPSTOX)
                         +compute_leg_cost(side="SELL",qty=qty,price=xf,is_swing=True,cfg=UPSTOX))
        free[slot]=xd;ssec[slot]=c["sec"];openp.append((xd,pnl,slot))
    for xd,pnl,sl in openp: equity+=pnl;closed.append((xd,pnl))
    if len(closed)<10: return None
    closed.sort();curve=[];eq=CAP0
    for xd,pnl in closed: eq+=pnl;curve.append((xd,eq))
    y0=int(closed[0][0][:4]);y1=int(closed[-1][0][:4])
    isp=[p for p in curve if p[0]<=IS_END];oosp=[p for p in curve if p[0]>IS_END]
    eqis=isp[-1][1] if isp else CAP0
    fc,fdd=seg(curve,CAP0,y1-y0+1);ic,idd=seg(isp,CAP0,max(1,2020-y0+1));oc,odd=seg(oosp,eqis,max(1,y1-2021+1))
    yr=defaultdict(float)
    for xd,pnl in closed: yr[xd[:4]]+=pnl
    return dict(cagr=fc,dd=fdd,cal=fc/abs(fdd) if fdd<0 else 0,n=len(closed),
                wr=100*sum(1 for _,p in closed if p>0)/len(closed),
                is_cal=ic/abs(idd) if idd<0 else 0,is_cagr=ic,oos_cal=oc/abs(odd) if odd<0 else 0,oos_cagr=oc,
                worst=min(yr.values()))
def show(tag,r):
    if not r: print(f"  {tag:32} -- thin"); return
    print(f"  {tag:32} CAGR{r['cagr']*100:+6.1f}% DD{r['dd']*100:6.1f}% Cal{r['cal']:5.2f} | "
          f"IS{r['is_cal']:5.2f}({r['is_cagr']*100:+5.1f}%) OOS{r['oos_cal']:5.2f}({r['oos_cagr']*100:+5.1f}%) "
          f"n{r['n']:>4} WR{r['wr']:4.0f}% wYr{r['worst']/1000:+.0f}k", flush=True)

SEL={
 "all informed": None,
 "promoter only": lambda c:c["cat"]=="promoter",
 "director+kmp only": lambda c:c["cat"] in ("director","kmp/rel"),
 "exclude promoter": lambda c:c["cat"]!="promoter",
 "cluster n>=2": lambda c:c["n"]>=2,
 "first-buy only": lambda c:c["first"],
 "value >25L": lambda c:c["val"]>=25e5,
 "value >1cr": lambda c:c["val"]>=1e7,
 "value >5cr": lambda c:c["val"]>=5e7,
 "hold-delta >0.1pp": lambda c:c["dpct"]>0.1,
 "hold-delta >0.5pp": lambda c:c["dpct"]>0.5,
 "buying dip (pr20<-5%)": lambda c:c["pr20"]<-0.05,
 "buying strength (pr20>0)": lambda c:c["pr20"]>0,
 "first & cluster": lambda c:c["first"] and c["n"]>=2,
 "first & value>25L": lambda c:c["first"] and c["val"]>=25e5,
 "first & (dir/kmp)": lambda c:c["first"] and c["cat"] in ("director","kmp/rel"),
 "conviction(dir/kmp|n>=2|>1cr)": lambda c:c["cat"] in ("director","kmp/rel") or c["n"]>=2 or c["val"]>=1e7,
 "first & conviction": lambda c:c["first"] and (c["cat"] in ("director","kmp/rel") or c["n"]>=2 or c["val"]>=1e7),
}
for base_name, base_reg in [("b200>50", lambda c:c["b200"]>50), ("Nifty>100DMA", nifty100)]:
    print(f"\n=== PHASE 2 SELECTION on base [{base_name}] (h60 s8 turn>10cr) ===", flush=True)
    for name,fn in SEL.items():
        show(name, walk(dict(hold=60,slots=8,turn_min=10e7,stop_mult=2.5,risk_pct=0.015,regime=base_reg,select=fn)))
print("\nREAD: keep selections that lift CAGR AND Calmar in BOTH halves w/ enough n (not a fragile", flush=True)
print("high-filter subset). Best base+selection carries to P3/P4 (exit/hold + sizing + DD-governor).", flush=True)
