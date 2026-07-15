"""SHORT-SQUEEZE engine-test — does any squeeze-long config survive the portfolio walk?

Diagnostic already showed: short-intensity flips sign IS->OOS and is NEGATIVE both halves in liquid
names. This is the empirical nail: run the best-defensible squeeze-long configs through slots + Upstox
cost + slippage + ATR exit, IS(<=2020)/OOS(>=2021) + per-year. If the CSV signal is dead, the walk
confirms it. Survivorship-safe (pead_full_bars). Reuses read-only prod primitives. READ-ONLY, thread-capped."""
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

SH = pickle.load(open(f"{GC}/shorts.pkl", "rb"))
bars = pickle.load(open(f"{BC}/pead_full_bars_2014.pkl", "rb"))
short = defaultdict(int)
for (d, sym, qty) in SH:
    if qty > 0: short[(sym, d)] += qty
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

# candidate pool: short-events with features
POOL=[]
for (sym,d),sq in short.items():
    S=SYM.get(sym)
    if S is None or d not in S["bd"]: continue
    i=S["bd"][d]; c=S["c"]; v=S["v"]
    if i<20 or i+1>=len(c) or c[i]<30.0 or v[i]<=0: continue
    turn=mean(c[j]*v[j] for j in range(i-20,i))
    if turn<1e8: continue
    a=S["atr"][i]
    if not a or a<=0: continue
    sint=sq/v[i]; ret5=c[i]/c[i-5]-1.0 if c[i-5]>0 else 0.0; rsi=S["rsi"][i] or 50.0
    POOL.append((d,sym,i,a,sint,ret5,turn,rsi))
POOL.sort()
print(f"pool: {len(POOL):,} short-events\n", flush=True)

def walk(cands, max_hold, slip):
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
    return dict(n=len(t),cagr=cg,mdd=m*100,cal=(cg/100)/abs(m) if m else 0,net=sum(x['net'] for x in t))
def run(lbl, filt, hold, slip, yearly=False):
    cands=[(d,s,i,a) for (d,s,i,a,sint,r5,turn,rsi) in POOL if filt(sint,r5,turn,rsi)]
    tr=walk(cands,hold,slip); f=met(tr); I=met(tr,hi="2020-12-31"); O=met(tr,lo="2021-01-01")
    if not f: print(f"  {lbl:42} (0)"); return
    win=I and O and I["net"]>0 and O["net"]>0
    print(f"  {lbl:42} n={f['n']:>4}({f['n']/12:>3.0f}/y) CAGR={f['cagr']:>5.1f}% Cal={f['cal']:>4.2f} "
          f"DD={f['mdd']:>6.1f}% | IS{I['cagr'] if I else 0:>5.1f} OOS{O['cagr'] if O else 0:>5.1f}{'  <==both+' if win else ''}",flush=True)
    if yearly:
        by=defaultdict(float)
        for x in tr: by[x["xd"][:4]]+=x["net"]
        print("      by-yr: "+" ".join(f"{y}:{int(v/1000)}k" for y,v in sorted(by.items())),flush=True)

print("=== SHORT-SQUEEZE-long engine-test — @0.10% then @0.25% slip ===", flush=True)
for slip in (0.001, 0.0025):
    print(f"--- slip {slip*100:.2f}%/leg ---", flush=True)
    run("sint>=2%, hold10", lambda si,r,t,rs: si>=0.02, 10, slip, slip==0.001)
    run("sint>=5%, hold10", lambda si,r,t,rs: si>=0.05, 10, slip)
    run("sint>=5% & ret5<=0 (falling), hold10", lambda si,r,t,rs: si>=0.05 and r<=0, 10, slip)
    run("sint>=5% & rsi<=45 (oversold), hold10", lambda si,r,t,rs: si>=0.05 and rs<=45, 10, slip)
    run("sint>=5% & >=50cr liquid, hold10", lambda si,r,t,rs: si>=0.05 and t>=5e8, 10, slip)
    run("sint>=2% & ret5<=0, hold20", lambda si,r,t,rs: si>=0.02 and r<=0, 20, slip)
print("\nKEEP = both-halves+ AND survives 0.25% slip. (Diag already flags this dead — this is the nail.)", flush=True)
