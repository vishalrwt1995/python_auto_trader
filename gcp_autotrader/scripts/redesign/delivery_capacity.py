"""DELIVERY re-grind with SIZE-AWARE execution (capacity model) — the honest re-test.

The earlier KILL used a FLAT slippage (0.25/0.5% every trade regardless of size). That is too blunt:
slippage is size-dependent. A small position in a thin name fills at ~half-spread, not 25bps. This
re-grinds the delivery edge with a realistic impact model and small-channel sizing, restricted to the
thin (Rs5-25cr) names where the signal actually lives, and reports IS/OOS + realized avg slip so we can
see the fills are real.

Impact model (per leg):  slip_frac = HALF_SPREAD + IMPACT_COEF * sqrt(order_value / daily_turnover)
  - HALF_SPREAD ~ 5bps (tick+spread on a Rs10cr+ name)
  - IMPACT_COEF swept optimistic/base/conservative
Position capped by: risk-size AND channel-slot AND max-participation (never > MAX_PART of daily turnover).
Survivorship-safe (pead_full_bars incl. delisted). Reuses read-only prod primitives. READ-ONLY, thread-capped."""
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
SLOTS, ATR_MULT = 5, 2.5
TRAIL_R, ACTIVATE_R = 1.0, 1.75
HALF_SPREAD = 0.0005          # 5bps each side
MAX_PART = 0.02               # never take > 2% of a day's turnover (realistic capacity cap)

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

# candidate pool once: delivery>=75 & ret5<=0 (best signal from prior grind), with turnover
print("building pool ...", flush=True)
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
        if pct<75 or ret5>0: continue
        a=S["atr"][i]
        if not a or a<=0: continue
        POOL.append((d,sym,i,a,turn))
POOL.sort()
print(f"  pool: {len(POOL):,} signal-days (deliv>=75 & ret5<=0)\n", flush=True)

def walk(capital, risk, turn_lo, turn_hi, impact, max_hold=10):
    free=[""]*SLOTS; tr=[]; slips=[]; parts=[]
    for (d,sym,i,a,turn) in POOL:
        if turn<turn_lo or turn>=turn_hi: continue
        slot=next((k for k in range(SLOTS) if free[k]<=d),None)
        if slot is None: continue
        S=SYM[sym]; ei=i+1; epx=S["o"][ei]; sl=ATR_MULT*a
        if epx<=0 or sl<=0: continue
        qty=min(int(risk//sl), int((capital/SLOTS)//epx), int((MAX_PART*turn)//epx))
        if qty<1: continue
        ov=qty*epx; part=ov/turn; slip=HALF_SPREAD+impact*sqrt(part)
        off,xpx,_=simulate_exit(S["b"],ei,True,sl,max_hold,trail_R=TRAIL_R,activate_R=ACTIVATE_R)
        xi=min(ei+off,len(S["b"])-1); free[slot]=S["d"][xi]
        ef=epx*(1+slip); xf=xpx*(1-slip); gross=(xf-ef)*qty
        cost=(compute_leg_cost(side="BUY",qty=qty,price=ef,is_swing=True,cfg=UPSTOX)
              +compute_leg_cost(side="SELL",qty=qty,price=xf,is_swing=True,cfg=UPSTOX))
        tr.append({"ed":d,"xd":S["d"][xi],"net":gross-cost}); slips.append(slip); parts.append(part)
    return tr, (mean(slips)*100 if slips else 0), (mean(parts)*100 if parts else 0)
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
def run(lbl, capital, risk, turn_lo, turn_hi, impact):
    tr,avslip,avpart=walk(capital,risk,turn_lo,turn_hi,impact)
    f=met(tr,capital); I=met(tr,capital,hi="2022-12-31"); O=met(tr,capital,lo="2023-01-01")
    if not f: print(f"  {lbl:34} (0)"); return
    win=I and O and I["net"]>0 and O["net"]>0
    print(f"  {lbl:34} n={f['n']:>4}({f['n']/6:>3.0f}/y) CAGR={f['cagr']:>5.1f}% Cal={f['cal']:>4.2f} "
          f"DD={f['mdd']:>6.1f}% Rs{f['peryr']/1000:>5.1f}k/y | IS{I['cagr'] if I else 0:>5.1f} OOS{O['cagr'] if O else 0:>5.1f} "
          f"| slip{avslip:.2f}% part{avpart:.2f}%{'  <==REAL both+' if win else ''}",flush=True)

print("=== DELIVERY size-aware capacity re-grind (deliv>=75 & ret5<=0, hold10, ATR-trail) ===", flush=True)
print("--- BASE impact (coef=0.01): capital x turnover-band ---", flush=True)
for cap,risk in [(50_000,750),(100_000,1500),(200_000,3000)]:
    print(f"  capital Rs{cap//1000}k, risk Rs{risk}:", flush=True)
    for lo,hi,tl in [(2e7,5e7,"2-5cr"),(5e7,1e8,"5-10cr"),(1e8,2.5e8,"10-25cr"),(2.5e8,5e8,"25-50cr"),(5e8,1e18,"50cr+")]:
        run(f"    turn {tl}", cap, risk, lo, hi, 0.01)
print("--- STRESS the best band (10-25cr, Rs1L) across impact assumptions ---", flush=True)
for coef,lbl in [(0.005,"optimistic"),(0.01,"base"),(0.02,"conservative"),(0.04,"harsh")]:
    run(f"  10-25cr impact={lbl}", 100_000, 1500, 1e8, 2.5e8, coef)
print("--- widen: all thin (2-25cr), Rs1L, base ---", flush=True)
run("  2-25cr all-thin Rs1L base", 100_000, 1500, 2e7, 2.5e8, 0.01)
print("\nREAL = positive BOTH halves with realistic fills (low avg participation). Rs/y = absolute profit.", flush=True)
