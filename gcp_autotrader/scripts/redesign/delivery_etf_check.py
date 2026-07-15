"""DELIVERY ETF-contamination check — is the 12.8% real stock alpha, or ETF-beta?

The backtest universe (pead_full_bars) INCLUDES all ETFs (NIFTYBEES, GOLDBEES, MON100, LIQUID...). ETFs have
structurally ~100% delivery-% every day (no churn) → the deliv>=75 signal selects them trivially, and they
drift with their index (beta). If ETFs drove the backtest, the 'edge' is contaminated. Re-run the locked
config (deliv>=75, 25-50cr, hold20, 5slot, Rs5L, size-aware) four ways and compare + report ETF trade share:
  ALL              = current backtest (incl ETFs)
  ex-ETF-name      = drop name-pattern ETFs (BEES / ETF / curated)
  ex-structural    = drop symbols whose MEDIAN deliv-% >= 88 (ETFs + non-churning instruments)
  ex-both          = the clean stock universe
Survivorship-safe. READ-ONLY, thread-capped."""
import os
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "4"
import sys, pickle
from math import sqrt
from statistics import mean, median
from datetime import date
from collections import defaultdict
sys.path.insert(0, "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.domain.swing_exit import simulate_exit
from autotrader.backtest.costs import compute_leg_cost, CostConfig

GC = os.path.expanduser("~/.autotrader_grind_cache"); BC = os.path.expanduser("~/.autotrader_backtest_cache")
UPSTOX = CostConfig.upstox()
CAPITAL, RISK, SLOTS = 500_000.0, 7_500.0, 5
HALF_SPREAD, IMPACT, MAX_PART = 0.0005, 0.01, 0.02
ATR_MULT, ARM, TRAIL, HOLD = 2.5, 1.75, 1.0, 20
TLO, THI = 2.5e8, 5e8

deliv = pickle.load(open(f"{GC}/delivery.pkl", "rb"))
bars = pickle.load(open(f"{BC}/pead_full_bars_2014.pkl", "rb"))

# ETF name heuristic (NSE): BEES suffix / ETF substring / curated tickers w/o those tokens
_CURATED = {"MON100","MOM100","MOM50","ICICIB22","CPSEETF","LIQUID","LIQUIDCASE","LIQUIDADD","LIQUIDBEES",
            "NASDAQ","N100","HNGSNGBEES","MAFANG","MASPTOP50","SETFNIF50","SETFNIFBK","SETFGOLD","QGOLDHALF",
            "GOLDSHARE","GOLDBEES","SILVERBEES","GOLDIAM" and None,"HDFCMFGETF","UTINIFTETF","AXISGOLD",
            "AXISNIFTY","KOTAKGOLD","KOTAKNIFTY","EBBETF0430","EBBETF0431","EBBETF0433","LICNETFN50",
            "LICNETFGSC","LICNMID100","IVZINGOLD","BSLGOLDETF","QNIFTY","JUNIORBEES","PSUBNKBEES","INFRABEES"}
_CURATED.discard(None)
def is_etf_name(s):
    return s.endswith("BEES") or "ETF" in s or "IETF" in s or s in _CURATED

# structural: median delivery-% across all the symbol's observed days
med_deliv = {}
for sym, dl in deliv.items():
    pcts = [p for (_, p, _, _) in dl if p is not None]
    if pcts: med_deliv[sym] = median(pcts)

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

POOL=[]
for sym,dl in deliv.items():
    S=SYM.get(sym)
    if S is None: continue
    c,v,bd=S["c"],S["v"],S["bd"]
    for (d,pct,qty,ttl) in dl:
        if d not in bd or pct<75: continue
        i=bd[d]
        if i<20 or i+1>=len(c) or c[i]<30.0: continue
        turn=mean(c[j]*v[j] for j in range(i-20,i))
        if turn<TLO or turn>=THI: continue
        a=S["atr"][i]
        if not a or a<=0: continue
        POOL.append((d,sym,i,a,turn))
POOL.sort()

def walk(exclude):
    free=[""]*SLOTS; tr=[]
    for (d,sym,i,a,turn) in POOL:
        if exclude(sym): continue
        slot=next((k for k in range(SLOTS) if free[k]<=d),None)
        if slot is None: continue
        S=SYM[sym]; ei=i+1; epx=S["o"][ei]; sl=ATR_MULT*a
        if epx<=0 or sl<=0: continue
        qty=min(int(RISK//sl), int((CAPITAL/SLOTS)//epx), int((MAX_PART*turn)//epx))
        if qty<1: continue
        part=(qty*epx)/turn; slip=HALF_SPREAD+IMPACT*sqrt(part)
        off,xpx,_=simulate_exit(S["b"],ei,True,sl,HOLD,trail_R=TRAIL,activate_R=ARM)
        xi=min(ei+off,len(S["b"])-1); free[slot]=S["d"][xi]
        ef=epx*(1+slip); xf=xpx*(1-slip); gross=(xf-ef)*qty
        cost=(compute_leg_cost(side="BUY",qty=qty,price=ef,is_swing=True,cfg=UPSTOX)
              +compute_leg_cost(side="SELL",qty=qty,price=xf,is_swing=True,cfg=UPSTOX))
        tr.append({"ed":d,"xd":S["d"][xi],"net":gross-cost,"sym":sym,"etf":is_etf_name(sym)})
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
    return dict(n=len(t),cagr=cg,mdd=m*100,cal=(cg/100)/abs(m) if m else 0,net=sum(x['net'] for x in t),
                peryr=sum(x['net'] for x in t)/y if y>0 else 0)
def show(lbl, exclude):
    tr=walk(exclude); f=met(tr); I=met(tr,hi="2022-12-31"); O=met(tr,lo="2023-01-01")
    if not f: print(f"  {lbl:16} (0)"); return
    print(f"  {lbl:16} n={f['n']:>4} CAGR={f['cagr']:>5.1f}% Cal={f['cal']:>4.2f} DD={f['mdd']:>6.1f}% "
          f"Rs{f['peryr']/1000:>5.1f}k | IS{I['cagr'] if I else 0:>5.1f} OOS{O['cagr'] if O else 0:>5.1f}",flush=True)
    return tr

# how much of the ALL run is ETFs?
etf_names = sorted({s for (_,s,_,_,_) in POOL if is_etf_name(s)})
print(f"universe: {len(SYM)} syms | ETF-name in pool: {len({s for (_,s,_,_,_) in POOL if is_etf_name(s)})} | "
      f"median-deliv>=88 syms in pool: {len({s for (_,s,_,_,_) in POOL if med_deliv.get(s,0)>=88})}\n", flush=True)
print("=== locked config (deliv>=75, 25-50cr, hold20, 5slot, Rs5L) — universe variants ===", flush=True)
trA = show("ALL (incl ETF)", lambda s: False)
show("ex-ETF-name", is_etf_name)
show("ex-structural>=88", lambda s: med_deliv.get(s,0)>=88)
trC = show("ex-both (STOCKS)", lambda s: is_etf_name(s) or med_deliv.get(s,0)>=88)

# ETF share of the ALL run
etf_net = sum(x["net"] for x in trA if x["etf"]); tot = sum(x["net"] for x in trA)
etf_n = sum(1 for x in trA if x["etf"])
print(f"\nETF share of ALL run: {etf_n}/{len(trA)} trades ({100*etf_n/len(trA):.0f}%), "
      f"Rs{etf_net/1000:.0f}k of Rs{tot/1000:.0f}k net ({100*etf_net/tot if tot else 0:.0f}%)", flush=True)
if trC:
    by=defaultdict(float)
    for x in trC: by[x["xd"][:4]]+=x["net"]
    print("STOCKS-only by-year: "+"  ".join(f"{y}:{v/1000:+.0f}k" for y,v in sorted(by.items())), flush=True)
print("\nVERDICT: if ex-both (STOCKS) holds ~both-halves+ near the headline → real stock alpha, exclude ETFs live.", flush=True)
print("If it collapses → the edge was ETF-beta; the channel is NOT what we validated.", flush=True)
