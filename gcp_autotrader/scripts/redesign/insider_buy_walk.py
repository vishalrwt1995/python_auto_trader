"""Insider-buy account walk (decisive test) -- does the strong per-signal drift survive a real
5-slot compounding account with fixed-hold exit + real Upstox costs? Grid over hold {20,40,60} x
slots {5,10,15} x selection {all, director/KMP-or-cluster}. Entry = next trading day after the
DISCLOSURE date (public, no look-ahead). Fixed-hold + 2.5xATR protective disaster stop; risk-
sized (1.5% equity). IS(<=2020)/OOS(>=2021). PLUS additivity: overlap of entries with top-20
12-1 momentum (is it just momentum?). Survivorship-safe. READ-ONLY, single-process, cached only
(zero GCP cost). Imports prod cost model read-only."""
import os
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "4"
import sys, json, glob, pickle, statistics
from bisect import bisect_right
from datetime import datetime
sys.path.insert(0, "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.backtest.costs import compute_leg_cost, CostConfig

C = os.path.expanduser("~/.autotrader_backtest_cache")
PIT = os.path.join(C, "insider_pit")
UPSTOX = CostConfig.upstox()
CAP0, RISK_PCT, SLIP, ATR_MULT, IS_END = 200_000.0, 0.015, 0.001, 2.5, "2020-12-31"

def atr14(h, l, c):
    tr=[h[0]-l[0]]
    for i in range(1,len(c)): tr.append(max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1])))
    out=[None]*len(c); s=0.0
    for i in range(len(tr)):
        s+=tr[i]
        if i>=14: s-=tr[i-14]
        if i>=13: out[i]=s/14.0
    return out

print("loading bars + PIT ...", flush=True)
bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
SYM = {}
for s,b in bars.items():
    if len(b)<70: continue
    d=[x[0] for x in b];o=[x[1] for x in b];h=[x[2] for x in b];l=[x[3] for x in b];c=[x[4] for x in b]
    SYM[s]={"d":d,"o":o,"h":h,"l":l,"c":c,"atr":atr14(h,l,c),"idx":{dt:i for i,dt in enumerate(d)}}

recs=[]
for fn in sorted(glob.glob(os.path.join(PIT,"*.json"))):
    try: recs.extend(json.load(open(fn)))
    except Exception: pass

INFORMED=("promoter","director","key managerial","immediate relative","promoter group")
def fnum(x):
    try: return float(str(x).replace(",",""))
    except Exception: return None
def dd_of(r):
    s=str(r.get("date","")).split()[0]
    try: return datetime.strptime(s,"%d-%b-%Y").strftime("%Y-%m-%d")
    except Exception: return None
from collections import defaultdict
agg=defaultdict(lambda:{"val":0.0,"n":0,"cats":set()})
for r in recs:
    if "buy" not in str(r.get("tdpTransactionType","")).lower(): continue
    if not any(k in str(r.get("personCategory","")).lower() for k in INFORMED): continue
    mode=str(r.get("acqMode","")).lower()
    if "market" not in mode or "off" in mode: continue
    val=fnum(r.get("secVal")) or fnum(r.get("buyValue")) or 0.0
    if val<500000: continue
    dd=dd_of(r)
    if dd is None or r.get("symbol") not in SYM: continue
    a=agg[(r["symbol"],dd)]; a["val"]+=val; a["n"]+=1; a["cats"].add(str(r.get("personCategory")).lower())

cands=[]
for (sym,dd),a in agg.items():
    S=SYM[sym]; ref=bisect_right(S["d"],dd)
    if ref>=len(S["c"]) or ref<1 or S["atr"][ref-1] is None or S["atr"][ref-1]<=0: continue
    if S["o"][ref]<30: continue
    quality = any(("director" in c or "managerial" in c or "relative" in c) for c in a["cats"]) or a["n"]>=2
    cands.append((S["d"][ref],sym,ref,ATR_MULT*S["atr"][ref-1],quality))
cands.sort()
print(f"  {len(cands):,} insider-buy entry candidates\n", flush=True)

def walk(hold, slots, quality_only):
    equity=CAP0; free=[""]*slots; openpos=[]; trades=[]; peak=equity; maxdd=0.0
    for entry_d,sym,ei,sl_dist,q in cands:
        if quality_only and not q: continue
        still=[]
        for xd,pnl in openpos:
            if xd<=entry_d: equity+=pnl; peak=max(peak,equity); maxdd=min(maxdd,equity/peak-1.0)
            else: still.append((xd,pnl))
        openpos=still
        for k in range(slots):
            if free[k] and free[k]<=entry_d: free[k]=""
        slot=next((k for k in range(slots) if not free[k]),None)
        if slot is None: continue
        S=SYM[sym]; entry_px=S["o"][ei]
        if entry_px<=0: continue
        qty=int((RISK_PCT*equity)//sl_dist)
        if qty<1: continue
        if qty*entry_px>equity/slots: qty=int((equity/slots)//entry_px)
        if qty<1: continue
        xi=min(ei+hold,len(S["c"])-1); exit_px=S["c"][xi]; stop=entry_px-sl_dist
        for k in range(ei+1,xi+1):
            if S["l"][k]<=stop: exit_px=stop; xi=k; break
        xd=S["d"][xi]; ef=entry_px*(1+SLIP); xf=exit_px*(1-SLIP)
        pnl=(xf-ef)*qty-(compute_leg_cost(side="BUY",qty=qty,price=ef,is_swing=True,cfg=UPSTOX)
                         +compute_leg_cost(side="SELL",qty=qty,price=xf,is_swing=True,cfg=UPSTOX))
        free[slot]=xd; openpos.append((xd,pnl))
        trades.append({"ed":entry_d,"xd":xd,"sym":sym,"pnl":pnl,"R":pnl/(sl_dist*qty)})
    for xd,pnl in openpos:
        equity+=pnl; peak=max(peak,equity); maxdd=min(maxdd,equity/peak-1.0)
    if not trades: return None
    span=int(max(t["xd"] for t in trades)[:4])-int(min(t["ed"] for t in trades)[:4])+1
    cagr=(equity/CAP0)**(1/span)-1; wr=100*sum(1 for t in trades if t["pnl"]>0)/len(trades)
    isr=[t["R"] for t in trades if t["ed"]<=IS_END]; oosr=[t["R"] for t in trades if t["ed"]>IS_END]
    return {"eq":equity,"cagr":cagr,"maxdd":maxdd,"calmar":cagr/abs(maxdd) if maxdd<0 else 0,
            "n":len(trades),"span":span,"wr":wr,
            "isR":statistics.mean(isr) if isr else 0,"oosR":statistics.mean(oosr) if oosr else 0,"trades":trades}

print("=== ACCOUNT WALK GRID (fixed-hold + 2.5ATR stop, compounding, real costs) ===", flush=True)
print(f"  {'config':30} {'CAGR':>7} {'maxDD':>7} {'Calmar':>7} {'trades':>7} {'WR':>6}  IS_R / OOS_R", flush=True)
best=None
for quality_only in (False, True):
    for slots in (5, 10, 15):
        for hold in (20, 40, 60):
            r=walk(hold,slots,quality_only)
            if not r: continue
            tag=f"{'quality' if quality_only else 'all'} h{hold} s{slots}"
            print(f"  {tag:30} {r['cagr']*100:+6.1f}% {r['maxdd']*100:6.1f}% {r['calmar']:7.2f} "
                  f"{r['n']:>7} {r['wr']:5.1f}%  {r['isR']:+.3f}/{r['oosR']:+.3f}", flush=True)
            if best is None or r["calmar"]>best[1]["calmar"]: best=(tag,r)

tag,r=best
print(f"\n=== BEST by Calmar: {tag} -> CAGR {r['cagr']*100:+.1f}% maxDD {r['maxdd']*100:.1f}% Calmar {r['calmar']:.2f} "
      f"({r['n']} trades ~{r['n']//r['span']}/yr) ===", flush=True)
# additivity vs momentum
def mom_overlap(trades, K=20):
    cache={}; hits=tot=0
    for t in trades:
        d,s=t["ed"],t["sym"]
        if d not in cache:
            uni=[]
            for sym,S in SYM.items():
                j=S["idx"].get(d)
                if j is None or j<273 or S["c"][j-273]<=0: continue
                uni.append((sym,S["c"][j-21]/S["c"][j-273]-1.0))
            uni.sort(key=lambda x:-x[1]); cache[d]={sym for sym,_ in uni[:K]}
        tot+=1; hits+=1 if s in cache[d] else 0
    return 100.0*hits/tot if tot else 0
print(f"  entries also in top-20 12-1 momentum: {mom_overlap(r['trades'],20):.1f}%  (low => additive, distinct signal)", flush=True)
print("\n  READ: clears bar if Calmar>=0.6 + CAGR>>FD both-half-positive R + low momentum overlap.", flush=True)
