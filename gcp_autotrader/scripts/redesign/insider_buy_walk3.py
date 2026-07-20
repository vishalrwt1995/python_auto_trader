"""Insider-buy walk v3 -- v2 got Calmar to 0.51 (turn>10cr, Nifty>100DMA, h60, CAGR +22.6%,
DD -44%). Last non-overfit DD lever: SECTOR diversification (insider buying clusters by sector;
a max-N-per-sector cap should cut concentrated blowups without touching the signal). Grid at the
best base (turn>10cr, Nifty>100DMA): hold {40,60} x slots {8,10,12} x sector_cap {none,2,3}.
Also reports per-year net to see WHERE the DD lives. Survivorship-safe. READ-ONLY, single-process,
cached only (zero GCP cost)."""
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
CAP0,RISK_PCT,SLIP,ATR_MULT,TURN_MIN,IS_END=200_000.0,0.015,0.001,2.5,10e7,"2020-12-31"

def atr14(h,l,c):
    tr=[h[0]-l[0]]
    for i in range(1,len(c)): tr.append(max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1])))
    out=[None]*len(c); s=0.0
    for i in range(len(tr)):
        s+=tr[i]
        if i>=14: s-=tr[i-14]
        if i>=13: out[i]=s/14.0
    return out

print("loading bars + nifty + sectors + PIT ...", flush=True)
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
ma100=[None]*len(nc); run=0.0
for i in range(len(nc)):
    run+=nc[i]
    if i>=100: run-=nc[i-100]
    if i>=99: ma100[i]=run/100.0
def reg_ok(d):
    i=bisect_left(md,d)-1
    return i<0 or ma100[i] is None or nc[i]>ma100[i]
sect_raw=json.load(open(f"{C}/sector_map.json")); SYM2SEC={}
for _v2 in sect_raw.values():
    if isinstance(_v2,dict) and _v2.get("sym") and _v2.get("sector"):
        SYM2SEC[str(_v2["sym"]).strip().upper()]=_v2["sector"]

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
    if S["o"][ref]<30 or S["turn"][ref] is None or S["turn"][ref]<TURN_MIN: continue
    if not reg_ok(S["d"][ref]): continue
    base.append((S["d"][ref],sym,ref,ATR_MULT*S["atr"][ref-1],SYM2SEC.get(sym.upper(),"?")))
base.sort()
print(f"  {len(base):,} candidates (turn>10cr + Nifty>100DMA)\n", flush=True)

def walk(hold,slots,sect_cap,peryear=False):
    equity=CAP0; free=[""]*slots; slot_sec=[None]*slots; openpos=[]; trades=[]; peak=equity; maxdd=0.0
    yr=defaultdict(float)
    for entry_d,sym,ei,sl_dist,sec in base:
        still=[]
        for xd,pnl,sl in openpos:
            if xd<=entry_d: equity+=pnl; peak=max(peak,equity); maxdd=min(maxdd,equity/peak-1.0)
            else: still.append((xd,pnl,sl))
        openpos=still
        for k in range(slots):
            if free[k] and free[k]<=entry_d: free[k]=""; slot_sec[k]=None
        if sect_cap and sec!="?" and sum(1 for k in range(slots) if slot_sec[k]==sec)>=sect_cap: continue
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
        free[slot]=xd; slot_sec[slot]=sec; openpos.append((xd,pnl,slot)); yr[xd[:4]]+=pnl
        trades.append({"ed":entry_d,"xd":xd,"pnl":pnl,"R":pnl/(sl_dist*qty)})
    for xd,pnl,sl in openpos:
        equity+=pnl; peak=max(peak,equity); maxdd=min(maxdd,equity/peak-1.0)
    if not trades: return None
    span=int(max(t["xd"] for t in trades)[:4])-int(min(t["ed"] for t in trades)[:4])+1
    cagr=(equity/CAP0)**(1/span)-1; wr=100*sum(1 for t in trades if t["pnl"]>0)/len(trades)
    isr=[t["R"] for t in trades if t["ed"]<=IS_END]; oosr=[t["R"] for t in trades if t["ed"]>IS_END]
    d=dict(cagr=cagr,maxdd=maxdd,calmar=cagr/abs(maxdd) if maxdd<0 else 0,n=len(trades),span=span,
           wr=wr,isR=statistics.mean(isr) if isr else 0,oosR=statistics.mean(oosr) if oosr else 0,yr=dict(yr))
    return d

print("=== v3 GRID: + sector cap (base turn>10cr + Nifty>100DMA) ===", flush=True)
print(f"  {'config':30} {'CAGR':>7} {'maxDD':>7} {'Calmar':>7} {'trades':>7} {'WR':>6}  IS_R/OOS_R", flush=True)
best=None
for hold in (40,60):
    for slots in (8,10,12):
        for sc in (0,2,3):
            r=walk(hold,slots,sc)
            if not r: continue
            tag=f"h{hold} s{slots} sect{'∞' if sc==0 else sc}"
            flag=" <<" if r["calmar"]>=0.6 else ""
            print(f"  {tag:30} {r['cagr']*100:+6.1f}% {r['maxdd']*100:6.1f}% {r['calmar']:7.2f} "
                  f"{r['n']:>7} {r['wr']:5.1f}%  {r['isR']:+.3f}/{r['oosR']:+.3f}{flag}", flush=True)
            if best is None or r["calmar"]>best[1]["calmar"]: best=(tag,r)
tag,r=best
print(f"\n=== BEST by Calmar: {tag} | CAGR {r['cagr']*100:+.1f}% DD {r['maxdd']*100:.1f}% Calmar {r['calmar']:.2f} ===", flush=True)
print("  per-year net Rs (where the DD lives):", flush=True)
for y in sorted(r["yr"]): print(f"    {y}: {r['yr'][y]:>+12,.0f}")
