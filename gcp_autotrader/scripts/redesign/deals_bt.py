"""BLOCK-DEAL-FOLLOW engine-test — does the block-deal buy edge survive the portfolio walk?

Diagnostic: BLOCK buy-deals drift (IS +1.77 / OOS +1.44 fwd10) while bulk/pool are negative. Now
engine-test: slots + Upstox cost + slippage + ATR exit, IS(<=2020)/OOS(>=2021) + per-year, with the
delivery lesson baked in -> SLIPPAGE STRESS (0.10 / 0.25%) + LIQUIDITY tiers from the start.
Survivorship-safe (pead_full_bars). Reuses read-only prod primitives. READ-ONLY, thread-capped."""
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

D = pickle.load(open(f"{GC}/deals.pkl", "rb"))
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
# block BUY deals aggregated per (sym,date)
blk=defaultdict(float)
for (d,sym,bs,qty,px) in D["block"]:
    if bs=="BUY" and qty>0 and px>0: blk[(sym,d)]+=qty*px

def build(ratio_min, turn_min):
    cands=[]
    for (sym,d),bval in blk.items():
        S=SYM.get(sym)
        if S is None or d not in S["bd"]: continue
        i=S["bd"][d]; c=S["c"]; v=S["v"]
        if i<20 or i+1>=len(c) or c[i]<30.0: continue
        turn=mean(c[j]*v[j] for j in range(i-20,i))
        if turn<turn_min or turn<=0: continue
        if bval/turn < ratio_min: continue
        a=S["atr"][i]
        if not a or a<=0: continue
        cands.append((d,sym,i,a))
    cands.sort(); return cands

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
def run(lbl, ratio_min, turn_min, hold, slip, yearly=False):
    tr=walk(build(ratio_min,turn_min),hold,slip); f=met(tr); I=met(tr,hi="2020-12-31"); O=met(tr,lo="2021-01-01")
    if not f: print(f"  {lbl:40} (0)"); return
    win=I and O and I["net"]>0 and O["net"]>0
    print(f"  {lbl:40} n={f['n']:>4}({f['n']/12:>3.0f}/y) CAGR={f['cagr']:>5.1f}% Cal={f['cal']:>4.2f} "
          f"DD={f['mdd']:>6.1f}% | IS{I['cagr'] if I else 0:>5.1f} OOS{O['cagr'] if O else 0:>5.1f}{'  <==both+' if win else ''}",flush=True)
    if yearly:
        by=defaultdict(float)
        for x in tr: by[x["xd"][:4]]+=x["net"]
        print("      by-yr: "+" ".join(f"{y}:{int(v/1000)}k" for y,v in sorted(by.items())),flush=True)

print("=== BLOCK-deal-follow engine-test (12yr 2014-26) — @0.10% then @0.25% slip ===", flush=True)
for slip in (0.001, 0.0025):
    print(f"--- slip {slip*100:.2f}%/leg ---", flush=True)
    run(f"block-buy, hold10", 0.0, 1e8, 10, slip, slip==0.001)
    run(f"block-buy, hold20", 0.0, 1e8, 20, slip)
    run(f"block-buy ratio>=1x, hold10", 1.0, 1e8, 10, slip)
    run(f"block-buy ratio>=3x, hold10", 3.0, 1e8, 10, slip)
    run(f"block-buy >=50cr liq, hold10", 0.0, 5e8, 10, slip)
    run(f"block-buy ratio>=1x >=50cr, hold15", 1.0, 5e8, 15, slip)
print("\nKEEP = both-halves+ AND survives 0.25% slip (block deals ~liquid => should be more fillable).", flush=True)
