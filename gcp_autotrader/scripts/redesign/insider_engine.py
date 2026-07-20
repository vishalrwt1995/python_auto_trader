"""Shared insider GOD-MODE engine -- loads the enriched candidate cache + nifty MAs and exposes
seg()/walk(cfg)/show() + gate helpers, so each phase script (p3/p4/p5) imports one correct walk
engine instead of duplicating it. READ-ONLY; import-time loads cached data only (zero GCP cost)."""
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
_o=pickle.load(open(os.path.join(C,"insider_cands_enriched.pkl"),"rb")); SYM,cands=_o["SYM"],_o["cands"]
_mkt=json.load(open(f"{C}/market_inputs_2015.json"))
_md=sorted(x for x in _mkt if _mkt[x].get("nifty_close")); _nc=[float(_mkt[x]["nifty_close"]) for x in _md]
def _ma(n):
    out=[None]*len(_nc); run=0.0
    for i in range(len(_nc)):
        run+=_nc[i]
        if i>=n: run-=_nc[i-n]
        if i>=n-1: out[i]=run/n
    return out
_MA={n:_ma(n) for n in (50,100,200)}
def nifty_gate(n):
    def f(c):
        i=bisect_left(_md,c["ed"])-1
        return i<0 or _MA[n][i] is None or _nc[i]>_MA[n][i]
    return f

def seg(points,start_eq,years):
    if not points or start_eq<=0: return 0.0,0.0
    peak=start_eq; mdd=0.0; last=start_eq
    for _,eq in points:
        peak=max(peak,eq); mdd=min(mdd,eq/peak-1.0); last=eq
    return ((last/start_eq)**(1/years)-1 if years>0 else 0.0), mdd

def walk(cfg):
    hold=cfg["hold"];slots=cfg["slots"];tmin=cfg.get("turn_min",10e7);stop_mult=cfg.get("stop_mult",2.5)
    risk=cfg.get("risk_pct",0.015);sect_cap=cfg.get("sect_cap",0);rf=cfg.get("regime");sf=cfg.get("select")
    dd_halt=cfg.get("dd_halt",0.0)
    equity=CAP0;free=[""]*slots;ssec=[None]*slots;openp=[];closed=[];peak_live=CAP0
    for c in cands:
        if c["turn"]<tmin: continue
        if rf and not rf(c): continue
        if sf and not sf(c): continue
        ed=c["ed"];still=[]
        for xd,pnl,sl in openp:
            if xd<=ed: equity+=pnl;peak_live=max(peak_live,equity);closed.append((xd,pnl))
            else: still.append((xd,pnl,sl))
        openp=still
        for k in range(slots):
            if free[k] and free[k]<=ed: free[k]="";ssec[k]=None
        if dd_halt>0 and equity<(1-dd_halt)*peak_live: continue
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
    return dict(cagr=fc,dd=fdd,cal=fc/abs(fdd) if fdd<0 else 0,n=len(closed),eq=eq,
                wr=100*sum(1 for _,p in closed if p>0)/len(closed),
                is_cal=ic/abs(idd) if idd<0 else 0,is_cagr=ic,oos_cal=oc/abs(odd) if odd<0 else 0,oos_cagr=oc,
                worst=min(yr.values()),yr=dict(yr))

def show(tag,r):
    if not r: print(f"  {tag:34} -- thin"); return
    print(f"  {tag:34} CAGR{r['cagr']*100:+6.1f}% DD{r['dd']*100:6.1f}% Cal{r['cal']:5.2f} | "
          f"IS{r['is_cal']:5.2f}({r['is_cagr']*100:+5.1f}%) OOS{r['oos_cal']:5.2f}({r['oos_cagr']*100:+5.1f}%) "
          f"n{r['n']:>4} WR{r['wr']:4.0f}% wYr{r['worst']/1000:+.0f}k", flush=True)

# common selection/regime lambdas
G_B200_50=lambda c:c["b200"]>50
G_CLUSTER=lambda c:c["n"]>=2
