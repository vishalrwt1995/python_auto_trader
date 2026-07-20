"""Insider-buy walk v2 -- the edge is real (both-half +avgR, additive, high-capacity) but v1 DD
was brutal (Calmar 0.32) because v1 had NO liquidity floor (illiquid microcaps drive the DD).
v2 adds the two proven DD-killers: a 20d-turnover floor (like delivery/momentum) + a Nifty-MA
regime overlay (like momentum). Grid: turnover {10,25cr} x regime {none,>100DMA,>200DMA} x hold
{40,60} at slots=8. Fixed-hold + 2.5ATR stop, risk-sized, compounding, real costs, IS/OOS. Goal:
Calmar>=0.6 while keeping CAGR>>FD. Survivorship-safe. READ-ONLY, single-process, cached only."""
import os
for _v in ("OMP_NUM_THREADS","OPENBLAS_NUM_THREADS","MKL_NUM_THREADS","NUMEXPR_NUM_THREADS","VECLIB_MAXIMUM_THREADS"):
    os.environ[_v]="4"
import sys, json, glob, pickle, statistics
from bisect import bisect_right, bisect_left
from datetime import datetime
from collections import defaultdict
sys.path.insert(0,"/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.backtest.costs import compute_leg_cost, CostConfig

C=os.path.expanduser("~/.autotrader_backtest_cache"); PIT=os.path.join(C,"insider_pit")
UPSTOX=CostConfig.upstox()
CAP0,RISK_PCT,SLIP,ATR_MULT,IS_END=200_000.0,0.015,0.001,2.5,"2020-12-31"

def atr14(h,l,c):
    tr=[h[0]-l[0]]
    for i in range(1,len(c)): tr.append(max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1])))
    out=[None]*len(c); s=0.0
    for i in range(len(tr)):
        s+=tr[i]
        if i>=14: s-=tr[i-14]
        if i>=13: out[i]=s/14.0
    return out

print("loading bars + nifty + PIT ...", flush=True)
bars=pickle.load(open(f"{C}/pead_full_bars_2014.pkl","rb"))
SYM={}
for s,b in bars.items():
    if len(b)<70: continue
    d=[x[0] for x in b];o=[x[1] for x in b];h=[x[2] for x in b];l=[x[3] for x in b];c=[x[4] for x in b];v=[x[5] for x in b]
    turn=[None]*len(c); run=0.0
    for i in range(len(c)):
        if i>=1: run+=c[i-1]*v[i-1]
        if i>=21: run-=c[i-21]*v[i-21]
        if i>=21: turn[i]=run/20.0
    SYM[s]={"d":d,"o":o,"h":h,"l":l,"c":c,"atr":atr14(h,l,c),"turn":turn}
mkt=json.load(open(f"{C}/market_inputs_2015.json"))
md=sorted(x for x in mkt if mkt[x].get("nifty_close")); nc=[float(mkt[x]["nifty_close"]) for x in md]
def ma(n):
    out=[None]*len(nc); run=0.0
    for i in range(len(nc)):
        run+=nc[i]
        if i>=n: run-=nc[i-n]
        if i>=n-1: out[i]=run/n
    return out
ma100,ma200=ma(100),ma(200)
def regime_ok(d,mode):
    if mode=="none": return True
    i=bisect_left(md,d)-1
    if i<0: return True
    m=ma100[i] if mode=="100" else ma200[i]
    return m is None or nc[i]>m

recs=[]
for fn in sorted(glob.glob(os.path.join(PIT,"*.json"))):
    try: recs.extend(json.load(open(fn)))
    except Exception: pass
INFORMED=("promoter","director","key managerial","immediate relative","promoter group")
def fnum(x):
    try: return float(str(x).replace(",",""))
    except Exception: return None
def dd_of(r):
    s=str(r.get("date","")).split()[0]
    try: return datetime.strptime(s,"%d-%b-%Y").strftime("%Y-%m-%d")
    except Exception: return None
agg=defaultdict(float)
for r in recs:
    if "buy" not in str(r.get("tdpTransactionType","")).lower(): continue
    if not any(k in str(r.get("personCategory","")).lower() for k in INFORMED): continue
    mode=str(r.get("acqMode","")).lower()
    if "market" not in mode or "off" in mode: continue
    val=fnum(r.get("secVal")) or fnum(r.get("buyValue")) or 0.0
    if val<500000: continue
    dd=dd_of(r)
    if dd is None or r.get("symbol") not in SYM: continue
    agg[(r["symbol"],dd)]+=val

base=[]
for (sym,dd) in agg:
    S=SYM[sym]; ref=bisect_right(S["d"],dd)
    if ref>=len(S["c"]) or ref<1 or S["atr"][ref-1] is None or S["atr"][ref-1]<=0: continue
    if S["o"][ref]<30 or S["turn"][ref] is None: continue
    base.append((S["d"][ref],sym,ref,ATR_MULT*S["atr"][ref-1],S["turn"][ref]))
base.sort()
print(f"  {len(base):,} candidates (pre-liquidity-filter)\n", flush=True)

def walk(hold,slots,turn_min,regime):
    equity=CAP0; free=[""]*slots; openpos=[]; trades=[]; peak=equity; maxdd=0.0
    for entry_d,sym,ei,sl_dist,tv in base:
        if tv<turn_min: continue
        if not regime_ok(entry_d,regime): continue
        still=[]
        for xd,pnl in openpos:
            if xd<=entry_d: equity+=pnl; peak=max(peak,equity); maxdd=min(maxdd,equity/peak-1.0)
            else: still.append((xd,pnl))
        openpos=still
        for k in range(slots):
            if free[k] and free[k]<=entry_d: free[k]=""
        slot=next((k for k in range(slots) if not free[k]),None)
        if slot is None: continue
        S=SYM[sym]; entry_px=S["o"][ei]
        if entry_px<=0: continue
        qty=int((RISK_PCT*equity)//sl_dist)
        if qty<1: continue
        if qty*entry_px>equity/slots: qty=int((equity/slots)//entry_px)
        if qty<1: continue
        xi=min(ei+hold,len(S["c"])-1); exit_px=S["c"][xi]; stop=entry_px-sl_dist
        for k in range(ei+1,xi+1):
            if S["l"][k]<=stop: exit_px=stop; xi=k; break
        xd=S["d"][xi]; ef=entry_px*(1+SLIP); xf=exit_px*(1-SLIP)
        pnl=(xf-ef)*qty-(compute_leg_cost(side="BUY",qty=qty,price=ef,is_swing=True,cfg=UPSTOX)
                         +compute_leg_cost(side="SELL",qty=qty,price=xf,is_swing=True,cfg=UPSTOX))
        free[slot]=xd; openpos.append((xd,pnl))
        trades.append({"ed":entry_d,"xd":xd,"pnl":pnl,"R":pnl/(sl_dist*qty)})
    for xd,pnl in openpos:
        equity+=pnl; peak=max(peak,equity); maxdd=min(maxdd,equity/peak-1.0)
    if not trades: return None
    span=int(max(t["xd"] for t in trades)[:4])-int(min(t["ed"] for t in trades)[:4])+1
    cagr=(equity/CAP0)**(1/span)-1; wr=100*sum(1 for t in trades if t["pnl"]>0)/len(trades)
    isr=[t["R"] for t in trades if t["ed"]<=IS_END]; oosr=[t["R"] for t in trades if t["ed"]>IS_END]
    return dict(cagr=cagr,maxdd=maxdd,calmar=cagr/abs(maxdd) if maxdd<0 else 0,n=len(trades),span=span,
                wr=wr,isR=statistics.mean(isr) if isr else 0,oosR=statistics.mean(oosr) if oosr else 0)

print("=== v2 GRID: liquidity floor + regime overlay (slots=8, fixed-hold+2.5ATR stop) ===", flush=True)
print(f"  {'config':28} {'CAGR':>7} {'maxDD':>7} {'Calmar':>7} {'trades':>7} {'WR':>6}  IS_R/OOS_R", flush=True)
for turn_min in (10e7,25e7):
    for regime in ("none","100","200"):
        for hold in (40,60):
            r=walk(hold,8,turn_min,regime)
            if not r: continue
            tag=f"turn>{int(turn_min/1e7)}cr reg{regime} h{hold}"
            flag=" <<" if r["calmar"]>=0.6 else ""
            print(f"  {tag:28} {r['cagr']*100:+6.1f}% {r['maxdd']*100:6.1f}% {r['calmar']:7.2f} "
                  f"{r['n']:>7} {r['wr']:5.1f}%  {r['isR']:+.3f}/{r['oosR']:+.3f}{flag}", flush=True)
print("\n  READ: does liquidity+regime lift Calmar to >=0.6 while keeping CAGR>>FD both halves?", flush=True)
