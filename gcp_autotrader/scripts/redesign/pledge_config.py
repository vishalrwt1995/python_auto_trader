"""Pledge BEST-CONFIG grind — the edge (Calmar) is ~1.0-1.2 robust; CAGR is driven by capital
DEPLOYMENT. With 60d holds + ~25 signals/yr, the equity/slots cap leaves ~40% idle. This sweeps a
realistic NO-LEVERAGE sizing model (position = min(risk%%*eq/stop, cap%%*eq, eq-committed)) over
risk%% x cap%% to trace the CAGR-vs-DD frontier, reporting FULL + IS + OOS independently + avg
deployment. Goal: the robust config (strong in BOTH halves) that best converts the fixed edge into
CAGR at an acceptable DD. Fixed: 200DMA filter, hold=60, stop 2.5ATR. READ-ONLY, cached only."""
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
TURN_MIN,PRICE_MIN,ATR_MULT,B200_MIN,HOLD,MAXSLOTS=10e7,30.0,2.5,50.0,60,20
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

def walk(cands,risk_pct,cap_pct):
    equity=CAP0;openp=[];closed=[];depl=[]      # openp: (exit_date, pnl, notional)
    for c in cands:
        ed=c["ed"];still=[]
        for xd,pnl,notl in openp:
            if xd<=ed:equity+=pnl;closed.append((xd,pnl))
            else:still.append((xd,pnl,notl))
        openp=still
        committed=sum(n for _,_,n in openp)
        if len(openp)>=MAXSLOTS:continue
        S=SYM[c["sym"]];ref=c["ref"];epx=S["o"][ref]
        if epx<=0:continue
        room=equity-committed
        if room<=epx:continue
        cap_amt=min(risk_pct*equity/c["sl"]*epx, cap_pct*equity, room)   # value cap
        qty=int(cap_amt//epx)
        if qty<1:continue
        depl.append((committed+qty*epx)/equity)
        xi=min(ref+HOLD,len(S["c"])-1);xpx=S["c"][xi];stop=epx-c["sl"]
        for k in range(ref+1,xi+1):
            if k<len(S["c"]) and S["c"][k]<=stop:xpx=stop;xi=k;break
        xd=S["d"][xi];ef=epx*(1+SLIP);xf=xpx*(1-SLIP)
        pnl=(xf-ef)*qty-(compute_leg_cost(side="BUY",qty=qty,price=ef,is_swing=True,cfg=UPSTOX)+compute_leg_cost(side="SELL",qty=qty,price=xf,is_swing=True,cfg=UPSTOX))
        openp.append((xd,pnl,qty*epx))
    for xd,pnl,notl in openp:equity+=pnl;closed.append((xd,pnl))
    if len(closed)<20:return None
    closed.sort();eq=CAP0;peak=CAP0;mdd=0.0
    for xd,pnl in closed:eq+=pnl;peak=max(peak,eq);mdd=min(mdd,eq/peak-1)
    span=max(1,int(closed[-1][0][:4])-int(closed[0][0][:4])+1);cagr=(eq/CAP0)**(1/span)-1
    return dict(cagr=cagr,mdd=mdd,cal=cagr/abs(mdd) if mdd<0 else 0,eq=eq,n=len(closed),span=span,
                depl=sum(depl)/len(depl) if depl else 0)
def full(risk_pct,cap_pct):
    a=walk([c for c in allc if c["ed"]<=IS_END],risk_pct,cap_pct)
    z=walk([c for c in allc if c["ed"]>IS_END],risk_pct,cap_pct)
    f=walk(allc,risk_pct,cap_pct)
    return f,a,z
print("=== SIZING / DEPLOYMENT FRONTIER (200DMA, hold60, stop2.5, no-leverage) ===",flush=True)
print(f"  {'risk%/cap%':13}{'deploy':>7}{'FULL CAGR':>11}{'DD':>7}{'Cal':>6}{'IS CAGR':>9}{'IS Cal':>7}{'OOS CAGR':>10}{'OOS Cal':>8}",flush=True)
rows=[]
for risk_pct in (0.015,0.025,0.04):
    for cap_pct in (0.10,0.15,0.20,0.33):
        f,a,z=full(risk_pct,cap_pct)
        if not f:continue
        rows.append((risk_pct,cap_pct,f,a,z))
        rob=" <<" if (a['cal']>=0.8 and z['cal']>=0.8) else ""
        print(f"  {risk_pct*100:>3.1f}/{cap_pct*100:>4.0f}    {f['depl']*100:>5.0f}% {f['cagr']*100:>+9.1f}%{f['mdd']*100:>6.1f}%{f['cal']:>6.2f}{a['cagr']*100:>+8.1f}%{a['cal']:>7.2f}{z['cagr']*100:>+9.1f}%{z['cal']:>8.2f}{rob}",flush=True)
# robust picks: max FULL CAGR subject to both-half Calmar>=0.8
robust=[r for r in rows if r[3]['cal']>=0.8 and r[4]['cal']>=0.8]
robust.sort(key=lambda r:-r[2]['cagr'])
print("\n=== ROBUST configs (IS Cal>=0.8 AND OOS Cal>=0.8), ranked by CAGR ===",flush=True)
for risk_pct,cap_pct,f,a,z in robust[:5]:
    print(f"  risk{risk_pct*100:.1f}% cap{cap_pct*100:.0f}%: CAGR {f['cagr']*100:+.1f}% DD {f['mdd']*100:.1f}% Cal {f['cal']:.2f} | deploy {f['depl']*100:.0f}% | {f['n']/f['span']:.0f} tr/yr | final Rs.{f['eq']:,.0f}",flush=True)
print("\nREAD: Calmar is ~flat across the frontier => extra CAGR is LEVERAGE (DD rises proportionally),",flush=True)
print("NOT free edge. If deployment lifts Calmar too, that's a real efficiency gain. Pick your DD tolerance.",flush=True)
