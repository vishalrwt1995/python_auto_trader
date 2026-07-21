"""Is the h60 Calmar peak structural or a lucky compounding path? Run the hold sensitivity SEPARATELY
on IS(<=2020) and OOS(>=2021) trade sets (slots=10, gated + px>200DMA). If ~60d wins (or the 50-70
neighborhood is uniformly strong) in BOTH halves independently, it's structural -> trust it. If h60 is
best only in the full sample, it's fragile -> quote the robust neighborhood, not the peak. READ-ONLY."""
import os
for _v in ("OMP_NUM_THREADS","OPENBLAS_NUM_THREADS","MKL_NUM_THREADS","NUMEXPR_NUM_THREADS","VECLIB_MAXIMUM_THREADS"):
    os.environ[_v]="4"
import sys, json, glob, pickle
from bisect import bisect_right, bisect_left
from datetime import datetime
sys.path.insert(0,"/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.backtest.costs import compute_leg_cost, CostConfig
C=os.path.expanduser("~/.autotrader_backtest_cache"); PIT=os.path.join(C,"insider_pit")
UPSTOX=CostConfig.upstox(); CAP0,SLIP,IS_END=200_000.0,0.001,"2020-12-31"
TURN_MIN,PRICE_MIN,ATR_MULT,RISK_PCT,B200_MIN,SLOTS=10e7,30.0,2.5,0.015,50.0,10
def sma(c,n):
    out=[None]*len(c);s=0.0
    for i in range(len(c)):
        s+=c[i]
        if i>=n:s-=c[i-n]
        if i>=n-1:out[i]=s/n
    return out
def atr14(h,l,c):
    tr=[h[0]-l[0]]
    for i in range(1,len(c)):tr.append(max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1])))
    out=[None]*len(c);s=0.0
    for i in range(len(tr)):
        s+=tr[i]
        if i>=14:s-=tr[i-14]
        if i>=13:out[i]=s/14.0
    return out
def dd_of(r):
    s=str(r.get("date","")).split()[0] if r.get("date") else ""
    try:return datetime.strptime(s,"%d-%b-%Y").strftime("%Y-%m-%d")
    except Exception:return None
bars=pickle.load(open(f"{C}/pead_full_bars_2014.pkl","rb"))
SYM={}
for s,b in bars.items():
    if len(b)<210:continue
    d=[x[0] for x in b];o=[x[1] for x in b];h=[x[2] for x in b];l=[x[3] for x in b];c=[x[4] for x in b];v=[x[5] for x in b]
    turn=[None]*len(c);run=0.0
    for i in range(len(c)):
        if i>=1:run+=c[i-1]*v[i-1]
        if i>=21:run-=c[i-21]*v[i-21]
        if i>=21:turn[i]=run/20.0
    SYM[s]={"d":d,"o":o,"c":c,"atr":atr14(h,l,c),"turn":turn,"s200":sma(c,200)}
b200h=pickle.load(open(f"{C}/swing_b200_history.pkl","rb"));bdd=sorted(b200h.keys())
mkt=json.load(open(f"{C}/market_inputs_2015.json"));md=sorted(x for x in mkt if mkt[x].get("nifty_close"));nc=[float(mkt[x]["nifty_close"]) for x in md]
ma=[None]*len(nc);run=0.0
for i in range(len(nc)):
    run+=nc[i]
    if i>=100:run-=nc[i-100]
    if i>=99:ma[i]=run/100.0
def nifty_ok(dt):
    i=bisect_left(md,dt)-1;return i<0 or ma[i] is None or nc[i]>ma[i]
def b200_at(dt):
    i=bisect_right(bdd,dt)-1;return b200h[bdd[i]] if i>=0 else 0.0
recs=[]
for fn in sorted(glob.glob(os.path.join(PIT,"*.json"))):
    try:recs.extend(json.load(open(fn)))
    except Exception:pass
allc=[]
for r in recs:
    if "revoke" not in str(r.get("tdpTransactionType","")).lower():continue
    if "promoter" not in str(r.get("personCategory","")).lower():continue
    sym=str(r.get("symbol") or "").strip().upper();dd=dd_of(r);S=SYM.get(sym)
    if not S or not dd:continue
    ref=bisect_right(S["d"],dd)
    if ref>=len(S["c"]) or ref<1 or S["atr"][ref-1] is None or S["atr"][ref-1]<=0:continue
    if S["turn"][ref] is None or S["turn"][ref]<TURN_MIN or S["o"][ref]<PRICE_MIN:continue
    ed=S["d"][ref]
    if b200_at(ed)<=B200_MIN or not nifty_ok(ed):continue
    if not (S["s200"][ref] is not None and S["c"][ref]>S["s200"][ref]):continue
    allc.append({"ed":ed,"sym":sym,"ref":ref,"sl":ATR_MULT*S["atr"][ref-1]})
allc.sort(key=lambda x:x["ed"])
def walk(cands,hold):
    if len(cands)<20:return None
    equity=CAP0;free=[""]*SLOTS;openp=[];closed=[]
    for c in cands:
        ed=c["ed"];still=[]
        for xd,pnl in openp:
            if xd<=ed:equity+=pnl;closed.append((xd,pnl))
            else:still.append((xd,pnl))
        openp=still
        for k in range(SLOTS):
            if free[k] and free[k]<=ed:free[k]=""
        slot=next((k for k in range(SLOTS) if not free[k]),None)
        if slot is None:continue
        S=SYM[c["sym"]];ref=c["ref"];epx=S["o"][ref]
        if epx<=0:continue
        qty=int((RISK_PCT*equity)//c["sl"])
        if qty<1:continue
        if qty*epx>equity/SLOTS:qty=int((equity/SLOTS)//epx)
        if qty<1:continue
        xi=min(ref+hold,len(S["c"])-1);xpx=S["c"][xi];stop=epx-c["sl"]
        for k in range(ref+1,xi+1):
            if k<len(S["c"]) and S["c"][k]<=stop:xpx=stop;xi=k;break
        xd=S["d"][xi];ef=epx*(1+SLIP);xf=xpx*(1-SLIP)
        pnl=(xf-ef)*qty-(compute_leg_cost(side="BUY",qty=qty,price=ef,is_swing=True,cfg=UPSTOX)+compute_leg_cost(side="SELL",qty=qty,price=xf,is_swing=True,cfg=UPSTOX))
        free[slot]=xd;openp.append((xd,pnl))
    for xd,pnl in openp:equity+=pnl;closed.append((xd,pnl))
    closed.sort();eq=CAP0;peak=CAP0;mdd=0.0
    for xd,pnl in closed:eq+=pnl;peak=max(peak,eq);mdd=min(mdd,eq/peak-1)
    span=max(1,int(closed[-1][0][:4])-int(closed[0][0][:4])+1);cagr=(eq/CAP0)**(1/span)-1
    return dict(cagr=cagr,mdd=mdd,cal=cagr/abs(mdd) if mdd<0 else 0,n=len(closed))
ISc=[c for c in allc if c["ed"]<=IS_END]; OOSc=[c for c in allc if c["ed"]>IS_END]
print(f"IS candidates {len(ISc)} | OOS candidates {len(OOSc)}\n",flush=True)
print(f"  {'hold':6}{'IS Calmar':>12}{'IS CAGR':>10}{'OOS Calmar':>13}{'OOS CAGR':>11}",flush=True)
for hold in (40,50,55,60,65,70,80,90):
    a=walk(ISc,hold);z=walk(OOSc,hold)
    print(f"  h{hold:<5}{a['cal']:>12.2f}{a['cagr']*100:>9.1f}%{z['cal']:>13.2f}{z['cagr']*100:>10.1f}%",flush=True)
print("\nREAD: if the 55-65 band is strong in BOTH IS and OOS columns -> structural (trust ~60d).",flush=True)
print("If h60 spikes in only one column -> fragile peak; quote the robust band, not 1.84.",flush=True)
