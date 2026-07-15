"""DELIVERY-ACCUMULATION enhancement grind — beat the slippage wall (god-mode exhaustive).

v1 = 9.5%/Cal0.64 at 0.1% slip but decays to 5.3%/0.29 at realistic 0.25% (thin per-trade x
120/yr churn). Target: a config that HOLDS at 0.25% slip both-halves. Untested levers:
  A) LIQUIDITY tier (>=10/25/50/100cr) — more-liquid names => lower real slippage
  B) delivery PERSISTENCE (sustained accumulation K-of-5 days) — higher conviction, fewer trades
  C) longer HOLD (capture bigger drift => slippage a smaller fraction)
  D) oversold-accumulation (delivery + RSI<=35) — capitulation buying
  E) best combos
Precomputes the candidate pool ONCE (features), then sweeps configs fast. ALL tested at 0.25% slip.
Survivorship-safe. Reuses read-only prod primitives. READ-ONLY, local, single-process, thread-capped."""
import os
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "4"
import sys, pickle
from statistics import mean
from datetime import date
from collections import defaultdict
sys.path.insert(0, "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.domain.swing_exit import simulate_exit
from autotrader.backtest.costs import compute_leg_cost, CostConfig

GC = os.path.expanduser("~/.autotrader_grind_cache"); BC = os.path.expanduser("~/.autotrader_backtest_cache")
UPSTOX = CostConfig.upstox()
CAPITAL, RISK, SLOTS, ATR_MULT = 200_000.0, 3_000.0, 5, 2.5
TRAIL_R, ACTIVATE_R = 1.0, 1.75
SLIP = 0.0025   # realistic — the bar v1 failed

deliv = pickle.load(open(f"{GC}/delivery.pkl", "rb"))
bars = pickle.load(open(f"{BC}/pead_full_bars_2014.pkl", "rb"))
def atr14(b):
    h=[x[2] for x in b]; l=[x[3] for x in b]; c=[x[4] for x in b]; tr=[h[0]-l[0]]
    for i in range(1,len(c)): tr.append(max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1])))
    o=[None]*len(c); s=0.0
    for i in range(len(tr)):
        s+=tr[i]
        if i>=14: s-=tr[i-14]
        if i>=13: o[i]=s/14.0
    return o
def rsi14(c):
    o=[None]*len(c)
    if len(c)<15: return o
    g=l=0.0
    for i in range(1,15): d=c[i]-c[i-1]; g+=max(d,0.0); l+=max(-d,0.0)
    g/=14; l/=14; o[14]=100-100/(1+g/(l or 1e-9))
    for i in range(15,len(c)):
        d=c[i]-c[i-1]; g=(g*13+max(d,0.0))/14; l=(l*13+max(-d,0.0))/14
        o[i]=100-100/(1+g/(l or 1e-9))
    return o
SYM={}
for sym,b in bars.items():
    if not b or len(b)<60: continue
    SYM[sym]={"b":b,"d":[x[0] for x in b],"o":[x[1] for x in b],"c":[x[4] for x in b],
              "v":[x[5] for x in b],"atr":atr14(b),"rsi":rsi14([x[4] for x in b]),"bd":{x[0]:i for i,x in enumerate(b)}}

# ---- precompute candidate POOL once (all liquid delivery-days + features) ----
print("building candidate pool ...", flush=True)
POOL=[]
for sym,dl in deliv.items():
    S=SYM.get(sym)
    if S is None: continue
    c,v,bd=S["c"],S["v"],S["bd"]
    pcts=[p for (_,p,_,_) in dl]
    for k,(d,pct,qty,ttl) in enumerate(dl):
        if d not in bd or k<5: continue
        i=bd[d]
        if i<20 or i+1>=len(c) or c[i]<30.0: continue
        turn=mean(c[j]*v[j] for j in range(i-20,i))
        if turn<1e8: continue
        a=S["atr"][i]
        if not a or a<=0: continue
        persist5=sum(1 for p in pcts[k-4:k+1] if p>=75)
        ret5=c[i]/c[i-5]-1.0 if c[i-5]>0 else 0.0
        hi20=max(c[i-19:i+1]); disthi=(hi20-c[i])/hi20 if hi20>0 else 0.0
        rsi=S["rsi"][i] or 50.0
        POOL.append((d,sym,i,a,pct,persist5,turn,ret5,disthi,rsi))
POOL.sort()
print(f"  pool: {len(POOL):,} liquid delivery-days\n", flush=True)

def walk(cands, max_hold):
    free=[""]*SLOTS; tr=[]
    for (d,sym,i,a) in cands:
        slot=next((k for k in range(SLOTS) if free[k]<=d),None)
        if slot is None: continue
        S=SYM[sym]; ei=i+1; epx=S["o"][ei]; sl=ATR_MULT*a; qty=int(RISK//sl)
        if qty<1 or epx<=0: continue
        if qty*epx>CAPITAL/SLOTS: qty=int((CAPITAL/SLOTS)//epx)
        if qty<1: continue
        off,xpx,_=simulate_exit(S["b"],ei,True,sl,max_hold,trail_R=TRAIL_R,activate_R=ACTIVATE_R)
        xi=min(ei+off,len(S["b"])-1); free[slot]=S["d"][xi]
        ef=epx*(1+SLIP); xf=xpx*(1-SLIP); gross=(xf-ef)*qty
        cost=(compute_leg_cost(side="BUY",qty=qty,price=ef,is_swing=True,cfg=UPSTOX)
              +compute_leg_cost(side="SELL",qty=qty,price=xf,is_swing=True,cfg=UPSTOX))
        tr.append({"ed":d,"xd":S["d"][xi],"net":gross-cost})
    return tr
def met(tr,lo=None,hi=None):
    t=[x for x in tr if (lo is None or x["xd"]>=lo) and (hi is None or x["ed"]<=hi)]
    if not t: return None
    days=sorted({x["xd"] for x in t}); eq=CAPITAL; cur=[CAPITAL]; byd=defaultdict(float)
    for x in t: byd[x["xd"]]+=x["net"]
    for dd in days: eq+=byd[dd]; cur.append(eq)
    pk=-1e18; m=0.0
    for vv in cur: pk=max(pk,vv); m=min(m,vv/pk-1)
    y=(date.fromisoformat(t[-1]["xd"])-date.fromisoformat(t[0]["ed"])).days/365.25
    cg=((cur[-1]/CAPITAL)**(1/y)-1)*100 if y>0 and cur[-1]>0 else 0.0
    return dict(n=len(t),cagr=cg,mdd=m*100,cal=(cg/100)/abs(m) if m else 0,net=sum(x['net'] for x in t))
def run(lbl, filt, max_hold):
    cands=[(d,s,i,a) for (d,s,i,a,pct,per,turn,r5,dh,rsi) in POOL if filt(pct,per,turn,r5,dh,rsi)]
    tr=walk(cands,max_hold); f=met(tr); I=met(tr,hi="2022-12-31"); O=met(tr,lo="2023-01-01")
    if not f: print(f"  {lbl:44} (0)"); return
    win=I and O and I["net"]>0 and O["net"]>0 and f["cal"]>=0.40
    print(f"  {lbl:44} n={f['n']:>4}({f['n']/6:>3.0f}/y) CAGR={f['cagr']:>5.1f}% Cal={f['cal']:>4.2f} "
          f"DD={f['mdd']:>6.1f}% | IS{I['cagr'] if I else 0:>5.1f} OOS{O['cagr'] if O else 0:>5.1f}{'  <== ROBUST' if win else ''}",flush=True)

print(f"=== ALL @ 0.25% slip (bar: beat v1's 5.3%/0.29; ROBUST = both+ & Cal>=0.40) ===", flush=True)
print("--- A) LIQUIDITY tier (v1 signal deliv>=75 & ret5<=0, hold10) ---", flush=True)
for tmin,lbl in [(1e8,">=10cr"),(2.5e8,">=25cr"),(5e8,">=50cr"),(1e9,">=100cr")]:
    run(f"deliv>=75 & ret5<=0 & turn{lbl} h10", lambda p,pe,t,r,dh,rs,tm=tmin: p>=75 and r<=0 and t>=tm, 10)
print("--- B) PERSISTENCE (sustained delivery) + ret5<=0, >=50cr ---", flush=True)
for pm in (2,3,4):
    run(f"persist5>={pm} & ret5<=0 & >=50cr h10", lambda p,pe,t,r,dh,rs,pmm=pm: pe>=pmm and r<=0 and t>=5e8, 10)
print("--- C) HOLD sweep (deliv>=75 & ret5<=0, >=50cr) ---", flush=True)
for h in (15,20,30):
    run(f"deliv>=75 & ret5<=0 & >=50cr h{h}", lambda p,pe,t,r,dh,rs: p>=75 and r<=0 and t>=5e8, h)
print("--- D) oversold-accumulation (delivery + RSI) >=50cr ---", flush=True)
run("deliv>=75 & rsi<=40 & >=50cr h10", lambda p,pe,t,r,dh,rs: p>=75 and rs<=40 and t>=5e8, 10)
run("deliv>=75 & rsi<=35 & >=50cr h15", lambda p,pe,t,r,dh,rs: p>=75 and rs<=35 and t>=5e8, 15)
print("--- E) best combos ---", flush=True)
run("persist5>=3 & ret5<=0 & rsi<=45 & >=50cr h15", lambda p,pe,t,r,dh,rs: pe>=3 and r<=0 and rs<=45 and t>=5e8, 15)
run("persist5>=3 & disthi>=0.05 & >=50cr h15", lambda p,pe,t,r,dh,rs: pe>=3 and dh>=0.05 and t>=5e8, 15)
print("\nAny ROBUST config = slippage-durable delivery edge. Else the edge is genuinely thin/fragile.", flush=True)
