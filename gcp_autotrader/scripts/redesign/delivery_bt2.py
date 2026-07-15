"""DELIVERY-ACCUMULATION engine-test v2 — exhaustive combinations to strengthen the edge.

v1 found a real-but-weak base (deliv>=75 & ret5<=0 = 9.5%/Cal 0.64, both halves, survivorship-safe)
+ killed the deep-dip mirage. v2 grinds the untested levers:
  - delivery-QTY-spike signal (clean in the CSV diagnostic; never engine-tested)
  - MEAN-REVERSION exit (fixed ATR target / ATR stop / short hold) vs the momentum trail —
    a MR accumulation signal likely wants to take the bounce, not trail it
  - combos (deliv-pct + qty-spike + mild dip)
Per-year printed for the survivors (concentration check). Survivorship-safe (pead_full_bars).
Reuses read-only prod primitives only. READ-ONLY, local, single-process, thread-capped."""
import os
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "4"
import sys, pickle
from statistics import mean
from datetime import date
from collections import defaultdict
sys.path.insert(0, "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.backtest.costs import compute_leg_cost, CostConfig

GC = os.path.expanduser("~/.autotrader_grind_cache"); BC = os.path.expanduser("~/.autotrader_backtest_cache")
UPSTOX = CostConfig.upstox(); SLIP = 0.001
CAPITAL, RISK, SLOTS = 200_000.0, 3_000.0, 5
PRICE_MIN, TURN_MIN, ATR_MULT = 30.0, 1e8, 2.5

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
    SYM[sym]={"b":b,"d":[x[0] for x in b],"o":[x[1] for x in b],"h":[x[2] for x in b],"l":[x[3] for x in b],
              "c":[x[4] for x in b],"v":[x[5] for x in b],"atr":atr14(b),"bd":{x[0]:i for i,x in enumerate(b)}}

def build(deliv_min=0.0, qspk_min=0.0, dip_thr=None):
    cands=[]
    for sym,dl in deliv.items():
        S=SYM.get(sym)
        if S is None: continue
        c,bd,v=S["c"],S["bd"],S["v"]
        ql=[q for (_,_,q,_) in dl]
        for k,(d,pct,qty,ttl) in enumerate(dl):
            if pct<deliv_min or d not in bd or k<20: continue
            i=bd[d]
            if i<20 or i+1>=len(c) or c[i]<PRICE_MIN: continue
            if mean(c[j]*v[j] for j in range(i-20,i))<TURN_MIN: continue
            qm=mean(ql[k-20:k]) if k>=20 else 0
            if qspk_min and not (qm>0 and qty/qm>=qspk_min): continue
            if dip_thr is not None and not (c[i]/c[i-5]-1.0)<=dip_thr: continue
            a=S["atr"][i]
            if not a or a<=0: continue
            cands.append((d,sym,i,a))
    cands.sort(); return cands

def walk(cands, target_atr, stop_atr, max_hold):
    """mean-reversion exit: first of +target_atr*ATR / -stop_atr*ATR / max_hold-day close."""
    free=[""]*SLOTS; tr=[]
    for (d,sym,i,a) in cands:
        slot=next((k for k in range(SLOTS) if free[k]<=d),None)
        if slot is None: continue
        S=SYM[sym]; ei=i+1
        epx=S["o"][ei]; sl=ATR_MULT*a; qty=int(RISK//sl)
        if qty<1 or epx<=0: continue
        if qty*epx>CAPITAL/SLOTS: qty=int((CAPITAL/SLOTS)//epx)
        if qty<1: continue
        tgt=epx+target_atr*a; stp=epx-stop_atr*a; xpx=None; xi=ei
        for j in range(ei, min(ei+max_hold+1, len(S["b"]))):
            xi=j
            if S["h"][j]>=tgt: xpx=tgt; break
            if S["l"][j]<=stp: xpx=stp; break
        if xpx is None: xpx=S["c"][xi]
        free[slot]=S["d"][xi]
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
    return dict(n=len(t),net=sum(x["net"] for x in t),cagr=cg,mdd=m*100,cal=(cg/100)/abs(m) if m else 0,
                wr=100*sum(1 for x in t if x["net"]>0)/len(t))

def run(lbl, cands, target_atr, stop_atr, max_hold, yearly=False):
    tr=walk(cands,target_atr,stop_atr,max_hold)
    f=met(tr); i=met(tr,hi="2022-12-31"); o=met(tr,lo="2023-01-01")
    if not f: print(f"  {lbl:40} (no trades)"); return
    win=i and o and i["net"]>0 and o["net"]>0
    print(f"  {lbl:40} n={f['n']:>4}({f['n']/6:>4.0f}/yr) CAGR={f['cagr']:>5.1f}% Cal={f['cal']:>4.2f} "
          f"DD={f['mdd']:>6.1f}% WR={f['wr']:>3.0f}% | IS{i['cagr'] if i else 0:>5.1f} OOS{o['cagr'] if o else 0:>5.1f}{'  <==both+' if win else ''}",flush=True)
    if yearly:
        by=defaultdict(float)
        for x in tr: by[x["xd"][:4]]+=x["net"]
        print("      by-yr net: "+" ".join(f"{y}:{int(v/1000)}k" for y,v in sorted(by.items())),flush=True)

print("=== v2: delivery-QTY-spike signal (never engine-tested) + MR exit (tgt/stop ATR) ===",flush=True)
C_q = build(deliv_min=75, qspk_min=1.5)
run("deliv>=75 & qty>=1.5x | MRexit 2/1.5 h10", C_q, 2.0,1.5,10, True)
run("deliv>=75 & qty>=1.5x | MRexit 3/1.5 h10", C_q, 3.0,1.5,10)
run("deliv>=75 & qty>=1.5x | MRexit 1.5/1 h5", C_q, 1.5,1.0,5)
print("--- deliv-pct + qty-spike + mild dip ---",flush=True)
C_qd = build(deliv_min=75, qspk_min=1.5, dip_thr=0.0)
run("deliv>=75 & qty>=1.5x & ret5<=0 | 2/1.5 h10", C_qd, 2.0,1.5,10, True)
print("--- v1 winner (deliv>=75 & ret5<=0) under MR exit vs its 9.5%/0.64 momentum-trail ---",flush=True)
C_d = build(deliv_min=75, dip_thr=0.0)
run("deliv>=75 & ret5<=0 | MRexit 2/1.5 h10", C_d, 2.0,1.5,10, True)
run("deliv>=75 & ret5<=0 | MRexit 1.5/1 h5", C_d, 1.5,1.0,5)
print("\nGoal: beat v1 (9.5%/Cal 0.64) with a cleaner Calmar + both-halves + un-concentrated by-year.",flush=True)
