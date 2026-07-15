"""DELIVERY-ACCUMULATION final validation — the make-or-break tests on the v1 winner.

Winner: deliv_pct>=75 & ret5<=0, hold-10, momentum-trail (simulate_exit) = 9.5%/Cal 0.64, both
halves, survivorship-safe, ~120 trades/yr. Two decisive gates for a HIGH-CHURN edge:
  1. SLIPPAGE STRESS (0.10 / 0.25 / 0.50% per leg) — 120/yr is cost-heavy; does the edge survive
     realistic slippage, or is it a cost mirage (the gapdown lesson)?
  2. PER-YEAR — is 9.5% broad or carried by 1-2 years?
Also a light churn-reducer probe (deliv>=80, hold15) to see if Calmar improves. Survivorship-safe.
Reuses read-only prod primitives. READ-ONLY, local, single-process, thread-capped."""
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
CAPITAL, RISK, SLOTS = 200_000.0, 3_000.0, 5
PRICE_MIN, TURN_MIN, ATR_MULT = 30.0, 1e8, 2.5
TRAIL_R, ACTIVATE_R = 1.0, 1.75

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

def build(deliv_min, dip_thr):
    cands=[]
    for sym,dl in deliv.items():
        S=SYM.get(sym)
        if S is None: continue
        c,bd,v=S["c"],S["bd"],S["v"]
        for (d,pct,qty,ttl) in dl:
            if pct<deliv_min or d not in bd: continue
            i=bd[d]
            if i<20 or i+1>=len(c) or c[i]<PRICE_MIN: continue
            if mean(c[j]*v[j] for j in range(i-20,i))<TURN_MIN: continue
            if dip_thr is not None and not (c[i]/c[i-5]-1.0)<=dip_thr: continue
            a=S["atr"][i]
            if not a or a<=0: continue
            cands.append((d,sym,i,a))
    cands.sort(); return cands

def walk(cands, max_hold, slip):
    free=[""]*SLOTS; tr=[]
    for (d,sym,i,a) in cands:
        slot=next((k for k in range(SLOTS) if free[k]<=d),None)
        if slot is None: continue
        S=SYM[sym]; ei=i+1
        epx=S["o"][ei]; sl=ATR_MULT*a; qty=int(RISK//sl)
        if qty<1 or epx<=0: continue
        if qty*epx>CAPITAL/SLOTS: qty=int((CAPITAL/SLOTS)//epx)
        if qty<1: continue
        off,xpx,_=simulate_exit(S["b"],ei,True,sl,max_hold,trail_R=TRAIL_R,activate_R=ACTIVATE_R)
        xi=min(ei+off,len(S["b"])-1); free[slot]=S["d"][xi]
        ef=epx*(1+slip); xf=xpx*(1-slip); gross=(xf-ef)*qty
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
    return dict(n=len(t),net=sum(x["net"] for x in t),cagr=cg,mdd=m*100,cal=(cg/100)/abs(m) if m else 0)

WIN = build(75, 0.0)   # v1 winner
print("=== 1) SLIPPAGE STRESS — v1 winner (deliv>=75 & ret5<=0, hold10) ===", flush=True)
for slip in (0.001, 0.0025, 0.005):
    tr=walk(WIN,10,slip); f=met(tr); i=met(tr,hi="2022-12-31"); o=met(tr,lo="2023-01-01")
    print(f"  slip {slip*100:.2f}%/leg  CAGR={f['cagr']:>5.1f}%  Cal={f['cal']:>4.2f}  DD={f['mdd']:>6.1f}%  "
          f"| IS {i['cagr'] if i else 0:>5.1f}%  OOS {o['cagr'] if o else 0:>5.1f}%", flush=True)

print("\n=== 2) PER-YEAR (v1 winner @ 0.10% slip) — broad or carried? ===", flush=True)
tr=walk(WIN,10,0.001); by=defaultdict(lambda:[0,0.0])
for x in tr: by[x["xd"][:4]][0]+=1; by[x["xd"][:4]][1]+=x["net"]
tot=sum(v[1] for v in by.values())
for y,v in sorted(by.items()):
    print(f"  {y}: n={v[0]:>4}  net={v[1]:>9,.0f}  ({100*v[1]/tot:>4.0f}% of total)", flush=True)
mx=max(by.items(), key=lambda kv: kv[1][1])
print(f"  -> largest year = {mx[0]} ({100*mx[1][1]/tot:.0f}% of total); >50% => concentrated", flush=True)

print("\n=== 3) churn-reducer probe (higher deliv / longer hold @ 0.25% slip) ===", flush=True)
for dmin,hold,lbl in [(80,10,"deliv>=80 h10"),(75,15,"deliv>=75 h15"),(85,15,"deliv>=85 h15")]:
    tr=walk(build(dmin,0.0),hold,0.0025); f=met(tr); i=met(tr,hi="2022-12-31"); o=met(tr,lo="2023-01-01")
    if f: print(f"  {lbl:16} n={f['n']:>4}({f['n']/6:>3.0f}/yr) CAGR={f['cagr']:>5.1f}% Cal={f['cal']:>4.2f} "
                f"DD={f['mdd']:>6.1f}% | IS{i['cagr'] if i else 0:>5.1f} OOS{o['cagr'] if o else 0:>5.1f}", flush=True)

print("\nVERDICT gate: survives ~0.25% slip (both halves +) AND not >50% one-year = real keeper.", flush=True)
