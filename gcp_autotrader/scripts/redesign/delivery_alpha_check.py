"""DELIVERY alpha-vs-beta isolation — is the delivery-% signal REAL, or just mid-cap-dip + beta?

The profit-grind showed CAGR scaling monotonically with hold length (hold10 4.3% -> hold15 13.7%) in
a rising 2020-26 mid-cap tape. That is the SIGNATURE OF BETA, not necessarily delivery alpha. Decisive
test: run identical 5-slot / size-aware / Rs5L walks over the SAME 25-50cr band for three entry signals:
  FULL   = deliv>=75 & ret5<=0   (the channel)
  DIP    = ret5<=0               (any mid-cap dip, NO delivery filter — the MR+beta control)
  DELIV  = deliv>=75             (delivery high, NO dip filter)
If FULL >> DIP both halves at every hold, delivery-% is real alpha. If FULL ~= DIP, it's decoration.
DIP pool deterministically 1-in-4 subsampled (unbiased vs forward return) to bound memory; 5 slots stay
full regardless. Survivorship-safe. READ-ONLY, single-process, thread-capped."""
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
CAPITAL, RISK, SLOTS = 500_000.0, 7_500.0, 5
HALF_SPREAD, IMPACT, MAX_PART = 0.0005, 0.01, 0.02
ATR_MULT, ARM, TRAIL = 2.5, 1.75, 1.0
TLO, THI = 2.5e8, 5e8   # 25-50cr band

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
    c=[x[4] for x in b]; v=[x[5] for x in b]
    turn=[0.0]*len(c); rs=0.0; w=[]
    for i in range(len(c)):
        x=c[i]*v[i]; w.append(x); rs+=x
        if len(w)>20: rs-=w.pop(0)
        turn[i]=rs/len(w)
    SYM[sym]={"b":b,"d":[x[0] for x in b],"o":[x[1] for x in b],"c":c,"turn":turn,
              "atr":atr14(b),"bd":{x[0]:i for i,x in enumerate(b)}}
deliv_days={sym:{d for (d,pct,_,_) in dl if pct>=75} for sym,dl in deliv.items()}

# build the candidate pools
FULL=[]; DIP=[]; DELIV=[]; ALLC=[]
for sym,S in SYM.items():
    c,turn,bd=S["c"],S["turn"],S["bd"]
    dd=deliv_days.get(sym,set())
    for i in range(21,len(c)-1):
        if not (TLO<=turn[i]<THI) or c[i]<30.0: continue
        a=S["atr"][i]
        if not a or a<=0: continue
        d=S["d"][i]; ret5=c[i]/c[i-5]-1.0 if c[i-5]>0 else 0.0
        is_dip=ret5<=0; is_del=d in dd
        cand=(d,sym,i,a,turn[i])
        if is_del and is_dip: FULL.append(cand)
        if is_del: DELIV.append(cand)
        if is_dip and ((i+len(sym))%4==0): DIP.append(cand)      # 1-in-4 unbiased subsample
        if (i+len(sym))%8==0: ALLC.append(cand)                  # any band-day = pure beta control
for P in (FULL,DIP,DELIV,ALLC): P.sort()
print(f"pools: FULL(deliv&dip)={len(FULL):,} | DIP(subsampled)={len(DIP):,} | DELIV(deliv only)={len(DELIV):,} | ALL(beta)={len(ALLC):,}\n", flush=True)

def walk(P, hold):
    free=[""]*SLOTS; tr=[]
    for (d,sym,i,a,turn) in P:
        slot=next((k for k in range(SLOTS) if free[k]<=d),None)
        if slot is None: continue
        S=SYM[sym]; ei=i+1; epx=S["o"][ei]; sl=ATR_MULT*a
        if epx<=0 or sl<=0: continue
        qty=min(int(RISK//sl), int((CAPITAL/SLOTS)//epx), int((MAX_PART*turn)//epx))
        if qty<1: continue
        part=(qty*epx)/turn; slip=HALF_SPREAD+IMPACT*sqrt(part)
        off,xpx,_=simulate_exit(S["b"],ei,True,sl,hold,trail_R=TRAIL,activate_R=ARM)
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
    return dict(n=len(t),cagr=cg,mdd=m*100,cal=(cg/100)/abs(m) if m else 0,peryr=sum(x['net'] for x in t)/y if y>0 else 0)
def show(lbl,P,hold):
    tr=walk(P,hold); f=met(tr); I=met(tr,hi="2022-12-31"); O=met(tr,lo="2023-01-01")
    if not f: print(f"  {lbl:22} (0)"); return
    print(f"  {lbl:22} n={f['n']:>4}({f['n']/6:>3.0f}/y) CAGR={f['cagr']:>5.1f}% Cal={f['cal']:>4.2f} "
          f"DD={f['mdd']:>6.1f}% Rs{f['peryr']/1000:>5.1f}k | IS{I['cagr'] if I else 0:>5.1f} OOS{O['cagr'] if O else 0:>5.1f}",flush=True)

for hold in (10,12,15,20):
    print(f"--- hold {hold} (25-50cr, 5slot, Rs5L, size-aware) ---", flush=True)
    show("DELIV only", DELIV, hold)
    show("FULL deliv&dip", FULL, hold)
    show("DIP only (control)", DIP, hold)
    show("ALL any-day (beta)", ALLC, hold)
print("\nVERDICT: delivery-% is real alpha ONLY if DELIV beats ALL(beta) AND DIP BOTH halves at every hold.", flush=True)
print("If DELIV ~= ALL -> it's just mid-cap beta; delivery-% adds nothing -> honest KILL.", flush=True)
