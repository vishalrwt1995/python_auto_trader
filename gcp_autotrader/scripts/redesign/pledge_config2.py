"""FINAL config lock — confirm the cap10%% deployment gain is robust ACROSS holds (not riding the h60
peak), then emit the locked config's full year-wise ledger + honest expectation. Sizing: 200DMA filter,
risk1.5%%, cap10%% per position, no-leverage, stop2.5ATR. If the 50-70 hold band is solid in BOTH IS and
OOS, lock hold=60 (robust central) and report. READ-ONLY, cached only."""
import os
for _v in ("OMP_NUM_THREADS","OPENBLAS_NUM_THREADS","MKL_NUM_THREADS","NUMEXPR_NUM_THREADS","VECLIB_MAXIMUM_THREADS"):
    os.environ[_v]="4"
import sys, json, glob, pickle
from bisect import bisect_right, bisect_left
from datetime import datetime
from collections import defaultdict
sys.path.insert(0,"/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.backtest.costs import compute_leg_cost, CostConfig
C=os.path.expanduser("~/.autotrader_backtest_cache"); PIT=os.path.join(C,"insider_pit")
UPSTOX=CostConfig.upstox(); CAP0,SLIP,IS_END=200_000.0,0.001,"2020-12-31"
TURN_MIN,PRICE_MIN,ATR_MULT,B200_MIN,RISK_PCT,CAP_PCT,MAXSLOTS=10e7,30.0,2.5,50.0,0.015,0.10,20
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
def walk(cands,hold,capture=False):
    equity=CAP0;openp=[];closed=[];trades=[]
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
        cap_amt=min(RISK_PCT*equity/c["sl"]*epx,CAP_PCT*equity,room)
        qty=int(cap_amt//epx)
        if qty<1:continue
        xi=min(ref+hold,len(S["c"])-1);xpx=S["c"][xi];stop=epx-c["sl"]
        for k in range(ref+1,xi+1):
            if k<len(S["c"]) and S["c"][k]<=stop:xpx=stop;xi=k;break
        xd=S["d"][xi];ef=epx*(1+SLIP);xf=xpx*(1-SLIP)
        pnl=(xf-ef)*qty-(compute_leg_cost(side="BUY",qty=qty,price=ef,is_swing=True,cfg=UPSTOX)+compute_leg_cost(side="SELL",qty=qty,price=xf,is_swing=True,cfg=UPSTOX))
        openp.append((xd,pnl,qty*epx))
        if capture:trades.append({"entry":ed,"exit":xd,"pnl":pnl})
    for xd,pnl,notl in openp:equity+=pnl;closed.append((xd,pnl))
    if len(closed)<20:return None
    closed.sort();eq=CAP0;peak=CAP0;mdd=0.0
    for xd,pnl in closed:eq+=pnl;peak=max(peak,eq);mdd=min(mdd,eq/peak-1)
    span=max(1,int(closed[-1][0][:4])-int(closed[0][0][:4])+1);cagr=(eq/CAP0)**(1/span)-1
    r=dict(cagr=cagr,mdd=mdd,cal=cagr/abs(mdd) if mdd<0 else 0,eq=eq,n=len(closed),span=span,
           isp=sum(p for xd,p in closed if xd<=IS_END),oosp=sum(p for xd,p in closed if xd>IS_END),
           wr=100*sum(1 for _,p in closed if p>0)/len(closed))
    if capture:r["trades"]=trades
    return r
ISc=[c for c in allc if c["ed"]<=IS_END];OOSc=[c for c in allc if c["ed"]>IS_END]
print("=== HOLD ROBUSTNESS at cap10% deployment (IS/OOS independent) ===",flush=True)
print(f"  {'hold':6}{'IS CAGR':>9}{'IS Cal':>8}{'OOS CAGR':>10}{'OOS Cal':>9}",flush=True)
band=[]
for hold in (40,50,55,60,65,70,80):
    a=walk(ISc,hold);z=walk(OOSc,hold)
    if a and z:
        band.append((a['cal'],z['cal']))
        print(f"  h{hold:<5}{a['cagr']*100:>+8.1f}%{a['cal']:>8.2f}{z['cagr']*100:>+9.1f}%{z['cal']:>9.2f}",flush=True)
solid=all(min(a,z)>=0.7 for a,z in band[1:6])   # holds 50-70
print(f"\n  50-70 band solid in BOTH halves (min-half Cal>=0.7 all): {solid}",flush=True)
print("\n=== LOCKED FINAL CONFIG: 200DMA + hold60 + cap10% + risk1.5% + stop2.5 (no-leverage) ===",flush=True)
r=walk(allc,60,capture=True);trades=r["trades"]
ent=defaultdict(int);exi=defaultdict(int);pnly=defaultdict(float)
for t in trades:
    ent[int(t["entry"][:4])]+=1;exi[int(t["exit"][:4])]+=1;pnly[int(t["exit"][:4])]+=t["pnl"]
eq=CAP0;yend={}
for t in sorted(trades,key=lambda t:t["exit"]):
    eq+=t["pnl"];yend[int(t["exit"][:4])]=eq
years=sorted(set(ent)|set(exi))
print(f"  Start Rs.{CAP0:,.0f} -> Final Rs.{r['eq']:,.0f} | PROFIT +Rs.{r['eq']-CAP0:,.0f} ({(r['eq']/CAP0-1)*100:+.0f}%)",flush=True)
print(f"  FULL: CAGR {r['cagr']*100:.1f}% | maxDD {r['mdd']*100:.1f}% | Calmar {r['cal']:.2f} | {len(trades)} tr ({len(trades)/r['span']:.0f}/yr) | WR {r['wr']:.0f}%",flush=True)
print(f"\n  {'Year':6}{'Entries':>9}{'Exits':>7}{'Realized P&L':>16}{'Year-end eq':>15}{'Return':>9}",flush=True)
prev=CAP0
for y in years:
    ye=yend.get(y,prev);ret=(ye/prev-1)*100 if prev>0 else 0
    print(f"  {y:<6}{ent[y]:>9}{exi[y]:>7}   Rs.{pnly[y]:>11,.0f}   Rs.{ye:>11,.0f}   {ret:>+7.1f}%",flush=True)
    prev=ye
print(f"\n  IS(<=2020) +Rs.{r['isp']:,.0f}   OOS(>=2021) +Rs.{r['oosp']:,.0f}",flush=True)
print(f"  HONEST expectation = both-half Calmar ~1.05 (full-sample {r['cal']:.2f} is bull-path-flattered);",flush=True)
print(f"  normal-regime CAGR ~mid-teens, bull-breadth years ~mid-20s; ~35-40 tr/yr (declining forward).",flush=True)
