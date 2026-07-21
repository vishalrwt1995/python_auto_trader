"""Round 2 — bracket the stop fully + test the combos of the 3 robust improvers (tighter stop, 25cr
turnover, tighter gate), find the highest-CAGR config that stays robustly solid (min-half Calmar high,
>=20 tr/yr), and emit its full year-wise ledger. Same pool/walk as pledge_final_sweep. READ-ONLY."""
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
PRICE_MIN,MAXSLOTS=30.0,20
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
pool=[]
for r in recs:
    if "revoke" not in str(r.get("tdpTransactionType","")).lower():continue
    if "promoter" not in str(r.get("personCategory","")).lower():continue
    sym=str(r.get("symbol") or "").strip().upper();dd=dd_of(r);S=SYM.get(sym)
    if not S or not dd:continue
    ref=bisect_right(S["d"],dd)
    if ref>=len(S["c"]) or ref<1 or S["atr"][ref-1] is None or S["atr"][ref-1]<=0:continue
    if S["turn"][ref] is None or S["turn"][ref]<5e7 or S["o"][ref]<PRICE_MIN:continue
    if not (S["s200"][ref] is not None and S["c"][ref]>S["s200"][ref]):continue
    ed=S["d"][ref]
    pool.append({"ed":ed,"sym":sym,"ref":ref,"atrp":S["atr"][ref-1],"turn":S["turn"][ref],"b200":b200_at(ed),"nok":nifty_ok(ed)})
pool.sort(key=lambda x:x["ed"])
def walk(hold=60,stop_mult=2.0,turn_min=25e7,gate="double",cap_pct=0.10,risk_pct=0.015,capture=False):
    equity=CAP0;openp=[];closed=[];trades=[]
    for c in pool:
        if c["turn"]<turn_min:continue
        if gate=="double" and not(c["b200"]>50 and c["nok"]):continue
        if gate=="b60" and not(c["b200"]>60 and c["nok"]):continue
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
        sl=stop_mult*c["atrp"];stop_px=epx-sl
        qty=int(min(risk_pct*equity/sl*epx,cap_pct*equity,room)//epx)
        if qty<1:continue
        end=min(ref+hold,len(S["c"])-1);xpx=S["c"][end];xi=end
        for k in range(ref+1,end+1):
            if S["c"][k]<=stop_px:xpx=stop_px;xi=k;break
            if k==ref+hold:xpx=S["c"][k];xi=k;break
        xd=S["d"][xi];ef=epx*(1+SLIP);xf=xpx*(1-SLIP)
        pnl=(xf-ef)*qty-(compute_leg_cost(side="BUY",qty=qty,price=ef,is_swing=True,cfg=UPSTOX)+compute_leg_cost(side="SELL",qty=qty,price=xf,is_swing=True,cfg=UPSTOX))
        openp.append((xd,pnl,qty*epx))
        if capture:trades.append({"entry":ed,"exit":xd,"pnl":pnl})
    for xd,pnl,notl in openp:equity+=pnl;closed.append((xd,pnl))
    if len(closed)<20:return None
    def met(cl):
        if len(cl)<10:return None
        eq=CAP0;peak=CAP0;mdd=0.0
        for xd,pnl in cl:eq+=pnl;peak=max(peak,eq);mdd=min(mdd,eq/peak-1)
        span=max(1,int(cl[-1][0][:4])-int(cl[0][0][:4])+1)
        return dict(cagr=(eq/CAP0)**(1/span)-1,mdd=mdd,cal=((eq/CAP0)**(1/span)-1)/abs(mdd) if mdd<0 else 0,eq=eq,n=len(cl),span=span)
    closed.sort()
    r=dict(f=met(closed),a=met([x for x in closed if x[0]<=IS_END]),z=met([x for x in closed if x[0]>IS_END]))
    if capture:r["trades"]=trades
    return r
def show(name,**kw):
    r=walk(**kw)
    if not r or not(r['f'] and r['a'] and r['z']):print(f"  {name:34} thin");return None
    f,a,z=r['f'],r['a'],r['z'];mn=min(a['cal'],z['cal'])
    print(f"  {name:34} CAGR{f['cagr']*100:>+6.1f}% DD{f['mdd']*100:>6.1f}% Cal{f['cal']:>5.2f} | IS{a['cal']:>5.2f} OOS{z['cal']:>5.2f} minHalf{mn:>5.2f} | {f['n']/f['span']:>4.0f}/y",flush=True)
    return (name,kw,f,mn)
print("=== STOP bracket (turn>=10cr, double gate) ===",flush=True)
for sm in (1.0,1.25,1.5,1.75,2.0,2.5):show(f"stop={sm}",stop_mult=sm,turn_min=10e7)
print("\n=== STOP bracket (turn>=25cr, double gate) ===",flush=True)
for sm in (1.25,1.5,1.75,2.0):show(f"stop={sm} turn25",stop_mult=sm,turn_min=25e7)
print("\n=== COMBOS ===",flush=True)
cands=[]
cands.append(show("C1 stop2.0 turn10 double",stop_mult=2.0,turn_min=10e7,gate="double"))
cands.append(show("C2 stop1.5 turn10 double",stop_mult=1.5,turn_min=10e7,gate="double"))
cands.append(show("C3 stop2.0 turn25 double",stop_mult=2.0,turn_min=25e7,gate="double"))
cands.append(show("C4 stop1.5 turn25 double",stop_mult=1.5,turn_min=25e7,gate="double"))
cands.append(show("C5 stop1.5 turn25 b200>60",stop_mult=1.5,turn_min=25e7,gate="b60"))
cands.append(show("C6 stop1.75 turn25 double",stop_mult=1.75,turn_min=25e7,gate="double"))
cands=[c for c in cands if c]
# LOCKED production-honest config: stop2.0 + turn25 (liquid/fillable) + double gate
win=("FINAL: stop2.0 turn>=25cr double-gate", dict(stop_mult=2.0,turn_min=25e7,gate="double"), None, None)
print(f"\n=== LOCKED FINAL CONFIG: {win[0]} ===",flush=True)
r=walk(capture=True,**win[1]);trades=r["trades"];f=r['f']
ent=defaultdict(int);pnly=defaultdict(float)
for t in trades:ent[int(t["entry"][:4])]+=1;pnly[int(t["exit"][:4])]+=t["pnl"]
eq=CAP0;yend={}
for t in sorted(trades,key=lambda t:t["exit"]):eq+=t["pnl"];yend[int(t["exit"][:4])]=eq
years=sorted(set(ent)|set(yend))
print(f"  Start Rs.{CAP0:,.0f} -> Final Rs.{f['eq']:,.0f} | PROFIT +Rs.{f['eq']-CAP0:,.0f} ({(f['eq']/CAP0-1)*100:+.0f}%)",flush=True)
print(f"  FULL CAGR {f['cagr']*100:.1f}% | DD {f['mdd']*100:.1f}% | Cal {f['cal']:.2f} | {len(trades)} tr ({len(trades)/f['span']:.0f}/yr) | IS Cal {r['a']['cal']:.2f} OOS Cal {r['z']['cal']:.2f}",flush=True)
prev=CAP0
for y in years:
    ye=yend.get(y,prev);print(f"  {y}: entries {ent[y]:>3} | P&L Rs.{pnly[y]:>11,.0f} | eq Rs.{ye:>11,.0f} | {(ye/prev-1)*100:>+6.1f}%",flush=True);prev=ye
