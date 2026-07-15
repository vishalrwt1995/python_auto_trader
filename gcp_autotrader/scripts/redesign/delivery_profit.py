"""DELIVERY profit-grind — push past 10.3% CAGR / 0.65 Calmar WITHOUT overfitting.

Levers (all judged IS(<=2022)/OOS(>=2023) + size-aware fills @ Rs5L, ROBUST = both halves+ AND beats
baseline Calmar):
  A) SLOTS 3/5/7 x RANKING (date / delivery-% / dip-depth / persistence) — better selection under contention
  B) SELECTIVITY filters (deliv>=80/85, persist>=3, rsi<=40, deeper dip) — concentrate on higher-expectancy
  C) EXIT geometry (hold 8/10/12/15; arm/trail variants)
  D) BAND (25-50 locked vs 20-50 / 25-60 / 30-60)
  E) best combo
Precomputes the feature pool ONCE. Size-aware slip = half_spread + impact*sqrt(participation), 2% cap.
Survivorship-safe (pead_full_bars incl delisted). Reuses read-only prod exit. READ-ONLY, thread-capped."""
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
CAPITAL, RISK = 500_000.0, 7_500.0
HALF_SPREAD, IMPACT, MAX_PART = 0.0005, 0.01, 0.02
ATR_MULT_D, TRAIL_D, ARM_D, HOLD_D = 2.5, 1.0, 1.75, 10

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
    for i in range(1,15): dd=c[i]-c[i-1]; g+=max(dd,0.0); l+=max(-dd,0.0)
    g/=14; l/=14; o[14]=100-100/(1+g/(l or 1e-9))
    for i in range(15,len(c)):
        dd=c[i]-c[i-1]; g=(g*13+max(dd,0.0))/14; l=(l*13+max(-dd,0.0))/14
        o[i]=100-100/(1+g/(l or 1e-9))
    return o
SYM={}
for sym,b in bars.items():
    if not b or len(b)<60: continue
    SYM[sym]={"b":b,"d":[x[0] for x in b],"o":[x[1] for x in b],"c":[x[4] for x in b],
              "v":[x[5] for x in b],"atr":atr14(b),"rsi":rsi14([x[4] for x in b]),"bd":{x[0]:i for i,x in enumerate(b)}}

# ── feature pool (deliv>=75 & ret5<=0, computed once; band applied at walk time) ──
print("building feature pool ...", flush=True)
POOL=[]   # (date, sym, i, atr, turn, deliv, persist5, ret5, ret1, rsi, disthi, distlo, volr)
for sym,dl in deliv.items():
    S=SYM.get(sym)
    if S is None: continue
    c,v,bd=S["c"],S["v"],S["bd"]
    pcts=[p for (_,p,_,_) in dl]
    for k,(d,pct,qty,ttl) in enumerate(dl):
        if d not in bd or pct<75: continue
        i=bd[d]
        if i<20 or i+1>=len(c) or c[i]<30.0: continue
        ret5=c[i]/c[i-5]-1.0 if c[i-5]>0 else 0.0
        if ret5>0: continue
        turn=mean(c[j]*v[j] for j in range(i-20,i))
        if turn<=0: continue
        a=S["atr"][i]
        if not a or a<=0: continue
        persist5=sum(1 for p in pcts[max(0,k-4):k+1] if p>=75)
        ret1=c[i]/c[i-1]-1.0 if c[i-1]>0 else 0.0
        rsi=S["rsi"][i] or 50.0
        hi20=max(c[i-19:i+1]); lo20=min(c[i-19:i+1])
        disthi=(hi20-c[i])/hi20 if hi20>0 else 0.0; distlo=(c[i]-lo20)/lo20 if lo20>0 else 0.0
        vm=mean(v[i-20:i]) if i>=20 else v[i]; volr=v[i]/vm if vm>0 else 1.0
        POOL.append((d,sym,i,a,turn,pct,persist5,ret5,ret1,rsi,disthi,distlo,volr))
print(f"  pool: {len(POOL):,} signal-days (deliv>=75 & ret5<=0, all bands)\n", flush=True)

def walk(flt, rankfn, slots, tlo, thi, hold, atrm, arm, trail):
    cands=[r for r in POOL if tlo<=r[4]<thi and flt(r)]
    # within-day ranking: sort by (date asc, rank desc) so best fills first under contention
    cands.sort(key=lambda r:(r[0], -rankfn(r)))
    free=[""]*slots; tr=[]
    for r in cands:
        d,sym,i,a,turn=r[0],r[1],r[2],r[3],r[4]
        slot=next((k for k in range(slots) if free[k]<=d),None)
        if slot is None: continue
        S=SYM[sym]; ei=i+1; epx=S["o"][ei]; sl=atrm*a
        if epx<=0 or sl<=0: continue
        qty=min(int(RISK//sl), int((CAPITAL/slots)//epx), int((MAX_PART*turn)//epx))
        if qty<1: continue
        part=(qty*epx)/turn; slip=HALF_SPREAD+IMPACT*sqrt(part)
        off,xpx,_=simulate_exit(S["b"],ei,True,sl,hold,trail_R=trail,activate_R=arm)
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
    return dict(n=len(t),cagr=cg,mdd=m*100,cal=(cg/100)/abs(m) if m else 0,
                net=sum(x['net'] for x in t),peryr=sum(x['net'] for x in t)/y if y>0 else 0)
BASE_CAL=[0.0]
def run(lbl, flt, rankfn, slots, tlo, thi, hold, atrm=ATR_MULT_D, arm=ARM_D, trail=TRAIL_D):
    tr=walk(flt,rankfn,slots,tlo,thi,hold,atrm,arm,trail)
    f=met(tr); I=met(tr,hi="2022-12-31"); O=met(tr,lo="2023-01-01")
    if not f: print(f"  {lbl:40} (0)"); return None
    robust=I and O and I["net"]>0 and O["net"]>0 and f["cal"]>=BASE_CAL[0]
    print(f"  {lbl:40} n={f['n']:>4}({f['n']/6:>3.0f}/y) CAGR={f['cagr']:>5.1f}% Cal={f['cal']:>4.2f} "
          f"DD={f['mdd']:>6.1f}% Rs{f['peryr']/1000:>5.1f}k | IS{I['cagr'] if I else 0:>5.1f} OOS{O['cagr'] if O else 0:>5.1f}"
          f"{'  <==ROBUST+' if robust else ''}",flush=True)
    return f

ALL=lambda r: True
R_DATE=lambda r: 0; R_DELIV=lambda r: r[5]; R_DIP=lambda r: -r[7]; R_PERSIST=lambda r: r[6]
print("=== BASELINE (locked: deliv>=75&ret5<=0, 25-50cr, 5slot, hold10, date-order) @Rs5L ===", flush=True)
b=run("baseline", ALL, R_DATE, 5, 2.5e8, 5e8, 10); BASE_CAL[0]=b["cal"] if b else 0.6
print(f"  (ROBUST bar = both-halves+ AND Calmar >= {BASE_CAL[0]:.2f})\n", flush=True)

print("--- A) SLOTS x RANKING (25-50cr, hold10) ---", flush=True)
for slots in (3,5,7):
    for rk,rl in [(R_DATE,"date"),(R_DELIV,"deliv%"),(R_DIP,"dip"),(R_PERSIST,"persist")]:
        run(f"slots{slots} rank={rl}", ALL, rk, slots, 2.5e8, 5e8, 10)
print("--- B) SELECTIVITY filters (5slot, best-rank=dip, 25-50cr, hold10) ---", flush=True)
run("deliv>=80", lambda r:r[5]>=80, R_DIP, 5, 2.5e8, 5e8, 10)
run("deliv>=85", lambda r:r[5]>=85, R_DIP, 5, 2.5e8, 5e8, 10)
run("persist5>=3", lambda r:r[6]>=3, R_DIP, 5, 2.5e8, 5e8, 10)
run("rsi<=40 (oversold)", lambda r:r[9]<=40, R_DIP, 5, 2.5e8, 5e8, 10)
run("dip ret5<=-3%", lambda r:r[7]<=-0.03, R_DIP, 5, 2.5e8, 5e8, 10)
run("dip ret5<=-5%", lambda r:r[7]<=-0.05, R_DIP, 5, 2.5e8, 5e8, 10)
run("volr>=1.5 (vol confirm)", lambda r:r[12]>=1.5, R_DIP, 5, 2.5e8, 5e8, 10)
run("distlo<=5% (near low)", lambda r:r[11]<=0.05, R_DIP, 5, 2.5e8, 5e8, 10)
print("--- C) EXIT geometry (5slot, dip-rank, 25-50cr) ---", flush=True)
for h in (8,10,12,15): run(f"hold{h}", ALL, R_DIP, 5, 2.5e8, 5e8, h)
run("arm1.0 trail0.5 hold10", ALL, R_DIP, 5, 2.5e8, 5e8, 10, arm=1.0, trail=0.5)
run("arm1.5 trail1.0 hold12", ALL, R_DIP, 5, 2.5e8, 5e8, 12, arm=1.5, trail=1.0)
run("atr3.0 hold12", ALL, R_DIP, 5, 2.5e8, 5e8, 12, atrm=3.0)
print("--- D) BAND (5slot, dip-rank, hold10) ---", flush=True)
for tlo,thi,bl in [(2e8,5e8,"20-50cr"),(2.5e8,5e8,"25-50cr"),(2.5e8,6e8,"25-60cr"),(3e8,6e8,"30-60cr")]:
    run(f"band {bl}", ALL, R_DIP, 5, tlo, thi, 10)
print("--- E) best combos (chase both-halves + Calmar) ---", flush=True)
run("deliv>=80 & persist>=3, dip, hold12", lambda r:r[5]>=80 and r[6]>=3, R_DIP, 5, 2.5e8, 5e8, 12)
run("deliv>=80, dip, slots7, hold12", lambda r:r[5]>=80, R_DIP, 7, 2.5e8, 5e8, 12)
run("persist>=2 & dip<=-2%, dip, hold12", lambda r:r[6]>=2 and r[7]<=-0.02, R_DIP, 5, 2.5e8, 5e8, 12)
print("\nROBUST+ beats the locked config on Calmar AND holds both halves. Anything else = don't ship it.", flush=True)
