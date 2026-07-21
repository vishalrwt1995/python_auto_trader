"""Promoter-PLEDGE diagnostic (channel #8 candidate #1) — does a promoter pledge RELEASE
(revoke = deleveraging → bullish) predict forward outperformance, and does pledge CREATION
(raising debt vs shares = stress → bearish) predict the opposite? A two-sided validation: a real
signal should show release>baseline AND creation<baseline in BOTH halves. Uses the cached insider
PIT pull (341k rows incl. Pledge/Pledge Revoke/Pledge Invoke) — survivorship-safe join to
pead_full_bars_2014. Entry = NEXT trading day after the public disclosure date (no look-ahead).
Cheap-first fwd10/20/60 NET (minus 0.7% cost+slip), IS(<=2020)/OOS(>=2021) vs the known ALL-liquid
baselines. READ-ONLY, single-process, cached only (zero GCP cost). Touches no prod module."""
import os
for _v in ("OMP_NUM_THREADS","OPENBLAS_NUM_THREADS","MKL_NUM_THREADS","NUMEXPR_NUM_THREADS","VECLIB_MAXIMUM_THREADS"):
    os.environ[_v]="4"
import json, glob, pickle, statistics
from bisect import bisect_right
from datetime import datetime
from collections import Counter

C=os.path.expanduser("~/.autotrader_backtest_cache"); PIT=os.path.join(C,"insider_pit")
COST, IS_END = 0.007, "2020-12-31"
TURN_MIN, PRICE_MIN = 10e7, 30.0     # fillable names only (like insider/delivery)
BASE={"f10":(-0.39,0.03),"f20":(-0.10,0.56),"f60":(1.46,3.30)}   # ALL-liquid baseline, same cost/universe

print("loading PIT + survivorship-safe bars ...", flush=True)
recs=[]
for fn in sorted(glob.glob(os.path.join(PIT,"*.json"))):
    try: recs.extend(json.load(open(fn)))
    except Exception: pass
bars=pickle.load(open(f"{C}/pead_full_bars_2014.pkl","rb"))
SYM={}
for s,b in bars.items():
    if len(b)<70: continue
    d=[x[0] for x in b]; c=[x[4] for x in b]; v=[x[5] for x in b]
    turn=[None]*len(c); run=0.0
    for i in range(len(c)):
        if i>=1: run+=c[i-1]*v[i-1]
        if i>=21: run-=c[i-21]*v[i-21]
        if i>=21: turn[i]=run/20.0
    SYM[s]={"d":d,"c":c,"turn":turn}
print(f"  {len(recs):,} PIT rows | {len(SYM):,} symbols\n", flush=True)

print("=== pledge transaction types present ===", flush=True)
print(" ", dict(Counter(str(r.get('tdpTransactionType','?')) for r in recs if 'pledge' in str(r.get('tdpTransactionType','')).lower()).most_common()), flush=True)

def dd_of(r):
    s=str(r.get("date","")).split()[0] if r.get("date") else ""
    try: return datetime.strptime(s,"%d-%b-%Y").strftime("%Y-%m-%d")
    except Exception: return None
def is_promoter(cat): return "promoter" in str(cat or "").lower()

# build event rows per (kind): release / creation / invoke
rows={"release":[], "creation":[], "invoke":[]}
for r in recs:
    t=str(r.get("tdpTransactionType","")).lower()
    if "pledge" not in t: continue
    if not is_promoter(r.get("personCategory")): continue
    kind = "release" if "revoke" in t or "revok" in t else ("invoke" if "invoc" in t or "invoke" in t else "creation")
    sym=str(r.get("symbol") or "").strip().upper(); dd=dd_of(r)
    S=SYM.get(sym)
    if not S or not dd: continue
    ref=bisect_right(S["d"], dd)                      # next trading day after disclosure
    if ref>=len(S["c"]) or ref<1: continue
    if S["turn"][ref] is None or S["turn"][ref]<TURN_MIN or S["c"][ref]<PRICE_MIN: continue
    def fwd(k): return (S["c"][ref+k]/S["c"][ref]-1.0-COST) if ref+k<len(S["c"]) and S["c"][ref]>0 else None
    rows[kind].append({"dd":dd,"f10":fwd(10),"f20":fwd(20),"f60":fwd(60)})

def stat(pool,key):
    v=[r[key] for r in pool if r[key] is not None]
    if not v: return "   n/a   "
    return f"avg={statistics.mean(v)*100:+5.2f}% med={statistics.median(v)*100:+5.2f}% WR={100*sum(1 for x in v if x>0)/len(v):4.1f}% n={len(v)}"
def report(lbl,pool):
    a=[r for r in pool if r["dd"]<=IS_END]; z=[r for r in pool if r["dd"]>IS_END]
    print(f"  {lbl} (n={len(pool):,})", flush=True)
    for k in ("f10","f20","f60"):
        b=BASE[k]; print(f"      {k}  IS: {stat(a,k)}   OOS: {stat(z,k)}   [base {b[0]:+.2f}/{b[1]:+.2f}]", flush=True)

print("\n=== forward NET return after PROMOTER pledge events (fillable names) ===", flush=True)
print("\n>>> PLEDGE RELEASE / REVOKE (bullish thesis: deleveraging) — the tradeable LONG signal", flush=True)
report("release", rows["release"])
print("\n>>> PLEDGE CREATION (bearish thesis: raising debt vs shares)", flush=True)
report("creation", rows["creation"])
print("\n>>> PLEDGE INVOCATION (lender-forced sale — most bearish)", flush=True)
report("invoke", rows["invoke"])
print("\nREAD: a real 2-sided signal = RELEASE robustly BEATS baseline both halves AND CREATION/INVOKE", flush=True)
print("robustly LAGS. If release~baseline -> no long edge (kill). If both directions confirm -> full walk.", flush=True)
