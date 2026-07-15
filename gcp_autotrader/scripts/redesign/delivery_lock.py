"""DELIVERY final-config validation — the CORRECTED config after the alpha/beta grind.

Config: deliv>=75 (NO dip filter), hold 20d, 25-50cr band, 5 slots, ATR2.5 arm1.75 trail1.0, size-aware
fills. Turnover = trailing-20d EXCLUDING signal day (consistent with the delivery_* / swing_final family).
Reports: LOCKED @ Rs5L and @ Rs2L with per-year net + return + n (is 13% broad or concentrated?), IS/OOS,
and a perturbation grid (deliv 72/75/78, band, hold 18/20/22, slots 5/7) to confirm it's not a knife-edge.
Survivorship-safe (pead_full_bars incl delisted). READ-ONLY, single-process, thread-capped."""
import os
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "4"
import sys, pickle
from math import sqrt
from statistics import mean
from datetime import date
from collections import defaultdict
sys.path.insert(0, "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.domain.swing_exit import simulate_exit
from autotrader.backtest.costs import compute_leg_cost, CostConfig

GC = os.path.expanduser("~/.autotrader_grind_cache"); BC = os.path.expanduser("~/.autotrader_backtest_cache")
UPSTOX = CostConfig.upstox()
HALF_SPREAD, IMPACT, MAX_PART = 0.0005, 0.01, 0.02
ATR_MULT, ARM, TRAIL = 2.5, 1.75, 1.0

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
SYM={}
for sym,b in bars.items():
    if not b or len(b)<60: continue
    SYM[sym]={"b":b,"d":[x[0] for x in b],"o":[x[1] for x in b],"c":[x[4] for x in b],
              "v":[x[5] for x in b],"atr":atr14(b),"bd":{x[0]:i for i,x in enumerate(b)}}

# pool: deliv>=75 (NO dip), all bands; band+deliv applied at walk time for perturbation
print("building pool ...", flush=True)
POOL=[]
for sym,dl in deliv.items():
    S=SYM.get(sym)
    if S is None: continue
    c,v,bd=S["c"],S["v"],S["bd"]
    for (d,pct,qty,ttl) in dl:
        if d not in bd or pct<70: continue
        i=bd[d]
        if i<20 or i+1>=len(c) or c[i]<30.0: continue
        turn=mean(c[j]*v[j] for j in range(i-20,i))
        if turn<=0: continue
        a=S["atr"][i]
        if not a or a<=0: continue
        POOL.append((d,sym,i,a,turn,pct))
POOL.sort()
print(f"  pool: {len(POOL):,} deliv>=70 signal-days\n", flush=True)

def walk(capital, risk, slots, tlo, thi, dmin, hold):
    free=[""]*slots; tr=[]
    for (d,sym,i,a,turn,pct) in POOL:
        if pct<dmin or turn<tlo or turn>=thi: continue
        slot=next((k for k in range(slots) if free[k]<=d),None)
        if slot is None: continue
        S=SYM[sym]; ei=i+1; epx=S["o"][ei]; sl=ATR_MULT*a
        if epx<=0 or sl<=0: continue
        qty=min(int(risk//sl), int((capital/slots)//epx), int((MAX_PART*turn)//epx))
        if qty<1: continue
        part=(qty*epx)/turn; slip=HALF_SPREAD+IMPACT*sqrt(part)
        off,xpx,_=simulate_exit(S["b"],ei,True,sl,hold,trail_R=TRAIL,activate_R=ARM)
        xi=min(ei+off,len(S["b"])-1); free[slot]=S["d"][xi]
        ef=epx*(1+slip); xf=xpx*(1-slip); gross=(xf-ef)*qty
        cost=(compute_leg_cost(side="BUY",qty=qty,price=ef,is_swing=True,cfg=UPSTOX)
              +compute_leg_cost(side="SELL",qty=qty,price=xf,is_swing=True,cfg=UPSTOX))
        tr.append({"ed":d,"xd":S["d"][xi],"net":gross-cost})
    return tr
def met(tr,capital,lo=None,hi=None):
    t=[x for x in tr if (lo is None or x["xd"]>=lo) and (hi is None or x["ed"]<=hi)]
    if not t: return None
    days=sorted({x["xd"] for x in t}); eq=capital; cur=[capital]; byd=defaultdict(float)
    for x in t: byd[x["xd"]]+=x["net"]
    for dd in days: eq+=byd[dd]; cur.append(eq)
    pk=-1e18; m=0.0
    for vv in cur: pk=max(pk,vv); m=min(m,vv/pk-1)
    y=(date.fromisoformat(t[-1]["xd"])-date.fromisoformat(t[0]["ed"])).days/365.25
    cg=((cur[-1]/capital)**(1/y)-1)*100 if y>0 and cur[-1]>0 else 0.0
    return dict(n=len(t),cagr=cg,mdd=m*100,cal=(cg/100)/abs(m) if m else 0,
                net=sum(x['net'] for x in t),peryr=sum(x['net'] for x in t)/y if y>0 else 0)

def locked(capital, risk, lbl):
    tr=walk(capital,risk,5,2.5e8,5e8,75,20)
    f=met(tr,capital); I=met(tr,capital,hi="2022-12-31"); O=met(tr,capital,lo="2023-01-01")
    print(f"=== LOCKED {lbl}: deliv>=75, 25-50cr, hold20, 5slot ===", flush=True)
    print(f"  FULL  CAGR={f['cagr']:.1f}%  Cal={f['cal']:.2f}  DD={f['mdd']:.1f}%  Rs{f['peryr']/1000:.1f}k/y  n={f['n']}", flush=True)
    print(f"  IS    CAGR={I['cagr']:.1f}%  Rs{I['net']/1000:.0f}k (n{I['n']})   |   OOS  CAGR={O['cagr']:.1f}%  Rs{O['net']/1000:.0f}k (n{O['n']})", flush=True)
    by=defaultdict(lambda:[0,0.0])
    for x in tr: by[x["xd"][:4]][0]+=1; by[x["xd"][:4]][1]+=x["net"]
    pos=sum(1 for y,(n,v) in by.items() if v>0)
    print("  by exit-year: "+"  ".join(f"{y}:{v/1000:+.0f}k(n{n})" for y,(n,v) in sorted(by.items())), flush=True)
    print(f"  positive years: {pos}/{len(by)}\n", flush=True)

locked(500_000, 7_500, "@Rs5L")
locked(200_000, 3_000, "@Rs2L")

print("=== PERTURBATION @Rs5L (robust = positive both halves, no knife-edge) ===", flush=True)
def pert(lbl, slots, tlo, thi, dmin, hold):
    tr=walk(500_000,7_500,slots,tlo,thi,dmin,hold)
    f=met(tr,500_000); I=met(tr,500_000,hi="2022-12-31"); O=met(tr,500_000,lo="2023-01-01")
    if not f: print(f"  {lbl:28} (0)"); return
    ok=I and O and I["net"]>0 and O["net"]>0
    print(f"  {lbl:28} CAGR={f['cagr']:>5.1f}% Cal={f['cal']:>4.2f} DD={f['mdd']:>6.1f}% | IS{I['cagr']:>5.1f} OOS{O['cagr']:>5.1f}{'  ok' if ok else '  <FAIL>'}", flush=True)
print("-- deliv threshold --", flush=True)
for dm in (70,72,75,78,80): pert(f"deliv>={dm}",5,2.5e8,5e8,dm,20)
print("-- band --", flush=True)
for tlo,thi,bl in [(2e8,5e8,"20-50cr"),(2.5e8,5e8,"25-50cr"),(2.5e8,6e8,"25-60cr"),(3e8,6e8,"30-60cr")]: pert(f"band {bl}",5,tlo,thi,75,20)
print("-- hold --", flush=True)
for h in (15,18,20,22,25): pert(f"hold{h}",5,2.5e8,5e8,75,h)
print("-- slots --", flush=True)
for sl in (3,5,7): pert(f"slots{sl}",sl,2.5e8,5e8,75,20)
print("\nBroad positive-years + robust perturbation neighborhood = trustworthy lock. Else tighten.", flush=True)
