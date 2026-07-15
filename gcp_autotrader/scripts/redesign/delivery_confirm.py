"""DELIVERY 25-50cr find — CONFIRM it's real, not a band-specific fluke (grind hard).

Size-aware re-grind surfaced: deliv>=75 & ret5<=0, hold10, 25-50cr turnover band, Rs2L = 8.9% CAGR /
Cal0.51 / IS+9.9 / OOS+9.8 with realistic fills (part 0.01%). Before calling it a KEEP, stress it:
  1) band-edge perturbation (does the edge survive moving 25-50cr?)
  2) per-year P&L (concentrated in one year = fragile)
  3) threshold sensitivity (deliv 70/75/80 x ret5 cutoff)
  4) hold x slots
  5) capital scaling (participation is tiny -> how much can it hold before slippage bites?)
Survivorship-safe. Size-aware slip = 5bps + 0.01*sqrt(participation). READ-ONLY, thread-capped."""
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
ATR_MULT = 2.5; TRAIL_R, ACTIVATE_R = 1.0, 1.75
HALF_SPREAD = 0.0005; IMPACT = 0.01; MAX_PART = 0.02

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
# superset pool: deliv>=70 & ret5<=0, store pct+ret5+turn to filter later
print("building superset pool ...", flush=True)
POOL=[]
for sym,dl in deliv.items():
    S=SYM.get(sym)
    if S is None: continue
    c,v,bd=S["c"],S["v"],S["bd"]
    for (d,pct,qty,ttl) in dl:
        if d not in bd: continue
        i=bd[d]
        if i<20 or i+1>=len(c) or c[i]<30.0: continue
        turn=mean(c[j]*v[j] for j in range(i-20,i))
        if turn<=0: continue
        ret5=c[i]/c[i-5]-1.0 if c[i-5]>0 else 0.0
        if pct<70 or ret5>0: continue
        a=S["atr"][i]
        if not a or a<=0: continue
        POOL.append((d,sym,i,a,turn,pct,ret5))
POOL.sort()
print(f"  superset pool: {len(POOL):,}\n", flush=True)

def walk(capital, risk, tlo, thi, hold, slots, dmin, r5max):
    free=[""]*slots; tr=[]
    for (d,sym,i,a,turn,pct,ret5) in POOL:
        if turn<tlo or turn>=thi or pct<dmin or ret5>r5max: continue
        slot=next((k for k in range(slots) if free[k]<=d),None)
        if slot is None: continue
        S=SYM[sym]; ei=i+1; epx=S["o"][ei]; sl=ATR_MULT*a
        if epx<=0 or sl<=0: continue
        qty=min(int(risk//sl), int((capital/slots)//epx), int((MAX_PART*turn)//epx))
        if qty<1: continue
        part=(qty*epx)/turn; slip=HALF_SPREAD+IMPACT*sqrt(part)
        off,xpx,_=simulate_exit(S["b"],ei,True,sl,hold,trail_R=TRAIL_R,activate_R=ACTIVATE_R)
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
def run(lbl, capital=200_000, risk=3000, tlo=2.5e8, thi=5e8, hold=10, slots=5, dmin=75, r5max=0.0, yearly=False):
    tr=walk(capital,risk,tlo,thi,hold,slots,dmin,r5max)
    f=met(tr,capital); I=met(tr,capital,hi="2022-12-31"); O=met(tr,capital,lo="2023-01-01")
    if not f: print(f"  {lbl:32} (0)"); return
    win=I and O and I["net"]>0 and O["net"]>0
    print(f"  {lbl:32} n={f['n']:>4} CAGR={f['cagr']:>5.1f}% Cal={f['cal']:>4.2f} DD={f['mdd']:>6.1f}% "
          f"Rs{f['peryr']/1000:>5.1f}k/y | IS{I['cagr'] if I else 0:>5.1f} OOS{O['cagr'] if O else 0:>5.1f}{'  <==REAL' if win else ''}",flush=True)
    if yearly:
        by=defaultdict(float)
        for x in tr: by[x["xd"][:4]]+=x["net"]
        print("      by-yr: "+" ".join(f"{y}:{int(v/1000)}k" for y,v in sorted(by.items())),flush=True)

print("=== 1) BAND-EDGE perturbation (Rs2L, deliv>=75, ret5<=0, hold10) ===", flush=True)
for tlo,thi,lbl in [(2e8,5e8,"20-50cr"),(2.5e8,4.5e8,"25-45cr"),(2.5e8,5e8,"25-50cr*"),(2.5e8,6e8,"25-60cr"),(3e8,5e8,"30-50cr"),(2e8,6e8,"20-60cr")]:
    run(lbl, tlo=tlo, thi=thi)
print("=== 2) PER-YEAR (25-50cr* base) ===", flush=True)
run("25-50cr* yearly", yearly=True)
print("=== 3) THRESHOLDS (25-50cr, Rs2L, hold10) ===", flush=True)
for dm in (70,75,80):
    for r5 in (0.0,-0.02):
        run(f"deliv>={dm} ret5<={r5}", dmin=dm, r5max=r5)
print("=== 4) HOLD x SLOTS (25-50cr, deliv>=75, ret5<=0) ===", flush=True)
for h in (8,10,15):
    for sl in (3,5,7):
        run(f"hold{h} slots{sl}", hold=h, slots=sl)
print("=== 5) CAPITAL SCALING (25-50cr, risk=1.5% cap) — capacity frontier ===", flush=True)
for cap in (200_000,300_000,500_000,1_000_000):
    run(f"capital Rs{cap//1000}k", capital=cap, risk=int(cap*0.015))
print("\nREAL = positive BOTH halves. Robust = survives band-perturbation + not 1-yr-concentrated.", flush=True)
