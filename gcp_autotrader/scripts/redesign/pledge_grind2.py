"""Pledge grind round 2 — lock the ONE robust improver (px>200DMA uptrend confirmation) and (a) confirm
it's a plateau not a peak across hold x slots, (b) emit the improved config's full year-wise ledger.
Honest label: the 200DMA lift blends cleaner-signal + momentum-beta (both real/harvestable); the magnitude
filters were killed for failing OOS. Survivorship-safe, real Upstox cost, IS/OOS. READ-ONLY, cached only."""
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
TURN_MIN,PRICE_MIN,ATR_MULT,RISK_PCT,B200_MIN=10e7,30.0,2.5,0.015,50.0
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
print("loading ...",flush=True)
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
cands=[]
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
    if not (S["s200"][ref] is not None and S["c"][ref]>S["s200"][ref]):continue   # px>200DMA lock
    cands.append({"ed":ed,"sym":sym,"ref":ref,"sl":ATR_MULT*S["atr"][ref-1]})
cands.sort(key=lambda x:x["ed"])
print(f"  {len(cands)} gated + px>200DMA candidates\n",flush=True)
def walk(hold,slots,capture=False):
    equity=CAP0;free=[""]*slots;openp=[];closed=[];trades=[]
    for c in cands:
        ed=c["ed"];still=[]
        for xd,pnl in openp:
            if xd<=ed:equity+=pnl;closed.append((xd,pnl))
            else:still.append((xd,pnl))
        openp=still
        for k in range(slots):
            if free[k] and free[k]<=ed:free[k]=""
        slot=next((k for k in range(slots) if not free[k]),None)
        if slot is None:continue
        S=SYM[c["sym"]];ref=c["ref"];epx=S["o"][ref]
        if epx<=0:continue
        qty=int((RISK_PCT*equity)//c["sl"])
        if qty<1:continue
        if qty*epx>equity/slots:qty=int((equity/slots)//epx)
        if qty<1:continue
        xi=min(ref+hold,len(S["c"])-1);xpx=S["c"][xi];stop=epx-c["sl"]
        for k in range(ref+1,xi+1):
            if k<len(S["c"]) and S["c"][k]<=stop:xpx=stop;xi=k;break
        xd=S["d"][xi];ef=epx*(1+SLIP);xf=xpx*(1-SLIP)
        pnl=(xf-ef)*qty-(compute_leg_cost(side="BUY",qty=qty,price=ef,is_swing=True,cfg=UPSTOX)+compute_leg_cost(side="SELL",qty=qty,price=xf,is_swing=True,cfg=UPSTOX))
        free[slot]=xd;openp.append((xd,pnl))
        if capture:trades.append({"entry":ed,"exit":xd,"pnl":pnl,"notional":qty*epx})
    for xd,pnl in openp:equity+=pnl;closed.append((xd,pnl))
    closed.sort();eq=CAP0;peak=CAP0;mdd=0.0
    for xd,pnl in closed:eq+=pnl;peak=max(peak,eq);mdd=min(mdd,eq/peak-1)
    span=int(closed[-1][0][:4])-int(closed[0][0][:4])+1;cagr=(eq/CAP0)**(1/span)-1
    isp=sum(p for xd,p in closed if xd<=IS_END);oosp=sum(p for xd,p in closed if xd>IS_END)
    r=dict(cagr=cagr,mdd=mdd,cal=cagr/abs(mdd),n=len(closed),span=span,eq=eq,isp=isp,oosp=oosp,wr=100*sum(1 for _,p in closed if p>0)/len(closed))
    if capture:r["trades"]=trades
    return r
print("=== PLATEAU CHECK: hold x slots (gated + px>200DMA) ===",flush=True)
print(f"  {'hold/slots':12}{'CAGR':>7}{'maxDD':>7}{'Calmar':>8}{'trades':>8}{'IS/OOS':>8}",flush=True)
best=None
for hold in (40,50,60,70,90):
    for slots in (5,8,10):
        r=walk(hold,slots)
        flag=" <<" if r["cal"]>=1.4 else ""
        print(f"  h{hold} s{slots:<8}{r['cagr']*100:+6.1f}%{r['mdd']*100:6.1f}%{r['cal']:8.2f}{r['n']:>8}   {'+' if r['isp']>0 else '-'}{'+' if r['oosp']>0 else '-'}{flag}",flush=True)
        if best is None or r["cal"]>best[1]["cal"]:best=((hold,slots),r)
(bh,bs),_=best
print(f"\n=== LOCKED: h{bh} s{bs} — full year-wise ledger ===",flush=True)
r=walk(bh,bs,capture=True);trades=r["trades"]
ent=defaultdict(int);exi=defaultdict(int);pnly=defaultdict(float)
for t in trades:
    ent[int(t["entry"][:4])]+=1;exi[int(t["exit"][:4])]+=1;pnly[int(t["exit"][:4])]+=t["pnl"]
eq=CAP0;yend={}
for t in sorted(trades,key=lambda t:t["exit"]):
    eq+=t["pnl"];yend[int(t["exit"][:4])]=eq
years=sorted(set(ent)|set(exi));wins=sum(1 for t in trades if t["pnl"]>0)
print(f"  Start Rs.{CAP0:,.0f} -> Final Rs.{r['eq']:,.0f} | PROFIT +Rs.{r['eq']-CAP0:,.0f} ({(r['eq']/CAP0-1)*100:+.0f}%)",flush=True)
print(f"  CAGR {r['cagr']*100:.1f}% | maxDD {r['mdd']*100:.1f}% | Calmar {r['cal']:.2f} | {len(trades)} trades ({len(trades)/r['span']:.1f}/yr) | WR {r['wr']:.0f}%",flush=True)
print(f"\n  {'Year':6}{'Entries':>9}{'Exits':>7}{'Realized P&L':>16}{'Year-end eq':>15}{'Return':>9}",flush=True)
prev=CAP0
for y in years:
    ye=yend.get(y,prev);ret=(ye/prev-1)*100 if prev>0 else 0
    print(f"  {y:<6}{ent[y]:>9}{exi[y]:>7}   Rs.{pnly[y]:>11,.0f}   Rs.{ye:>11,.0f}   {ret:>+7.1f}%",flush=True)
    prev=ye
print(f"\n  IS(<=2020) +Rs.{r['isp']:,.0f}   OOS(>=2021) +Rs.{r['oosp']:,.0f}",flush=True)
