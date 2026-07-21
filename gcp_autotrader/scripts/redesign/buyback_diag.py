"""BUYBACK diagnostic (channel candidate #2) — does entering AFTER a buyback board-meeting predict
forward outperformance (the undervaluation / cash-return thesis)? This is a DIFFERENT thesis from the
corp_action pre-meeting run-up (which validated bonus/split, NOT buyback). Cheap-first on the cached
98 buyback events (pead_nse_result_dates 'Results / Buyback' purpose) — SEVERELY thin + biased slice,
so this is a rule-in/out screen only: a strong signal justifies pulling the full nse_corp_actions set
(BQ, gated); a flat/negative one kills it cheaply. Entry = NEXT trading day after the meeting date
(no timestamp cached → next-day is the realistic post-announcement entry). fwd10/20/60 NET vs the
all-liquid baseline, IS(<=2020)/OOS(>=2021). READ-ONLY, cached only (zero GCP cost)."""
import os
for _v in ("OMP_NUM_THREADS","OPENBLAS_NUM_THREADS","MKL_NUM_THREADS","NUMEXPR_NUM_THREADS","VECLIB_MAXIMUM_THREADS"):
    os.environ[_v]="4"
import json, pickle, statistics
from bisect import bisect_right

C=os.path.expanduser("~/.autotrader_backtest_cache")
COST, IS_END = 0.007, "2020-12-31"
TURN_MIN, PRICE_MIN = 10e7, 30.0
BASE={"f10":(-0.39,0.03),"f20":(-0.10,0.56),"f60":(1.46,3.30)}   # all-liquid baseline, same cost/universe

d=json.load(open(f"{C}/pead_nse_result_dates_2012_2026.json"))
evs=d["events"] if isinstance(d,dict) and "events" in d else d
bb=[e for e in evs if any(k in str(e.get("purpose","")).lower() for k in ("buy back","buyback"))]
bars=pickle.load(open(f"{C}/pead_full_bars_2014.pkl","rb"))
SYM={}
for s,b in bars.items():
    if len(b)<70: continue
    dd=[x[0] for x in b]; c=[x[4] for x in b]; v=[x[5] for x in b]
    turn=[None]*len(c); run=0.0
    for i in range(len(c)):
        if i>=1: run+=c[i-1]*v[i-1]
        if i>=21: run-=c[i-21]*v[i-21]
        if i>=21: turn[i]=run/20.0
    SYM[s]={"d":dd,"c":c,"turn":turn}
print(f"{len(bb)} buyback events | {len(SYM)} symbols with bars\n", flush=True)

rows=[]; nofill=0; nobar=0
for e in bb:
    sym=str(e.get("symbol") or "").strip().upper(); dt=str(e.get("date") or "")[:10]
    S=SYM.get(sym)
    if not S or not dt: nobar+=1; continue
    ref=bisect_right(S["d"], dt)                 # next trading day after the meeting date
    if ref>=len(S["c"]) or ref<1: nobar+=1; continue
    if S["turn"][ref] is None or S["turn"][ref]<TURN_MIN or S["c"][ref]<PRICE_MIN: nofill+=1; continue
    def fwd(k): return (S["c"][ref+k]/S["c"][ref]-1.0-COST) if ref+k<len(S["c"]) and S["c"][ref]>0 else None
    rows.append({"dt":dt,"sym":sym,"f10":fwd(10),"f20":fwd(20),"f60":fwd(60)})
print(f"fillable (turn>=10cr, price>=30): {len(rows)} | dropped: no-bar {nobar}, illiquid {nofill}\n", flush=True)

def stat(pool,key):
    v=[r[key] for r in pool if r[key] is not None]
    if not v: return "   n/a   "
    return f"avg={statistics.mean(v)*100:+5.2f}% med={statistics.median(v)*100:+5.2f}% WR={100*sum(1 for x in v if x>0)/len(v):4.1f}% n={len(v)}"
a=[r for r in rows if r["dt"]<=IS_END]; z=[r for r in rows if r["dt"]>IS_END]
print(f"=== forward NET return after buyback board-meeting (fillable), IS n={len(a)} / OOS n={len(z)} ===", flush=True)
for k in ("f10","f20","f60"):
    b=BASE[k]; print(f"  {k}  IS: {stat(a,k)}   OOS: {stat(z,k)}   [base {b[0]:+.2f}/{b[1]:+.2f}]", flush=True)
print("\nREAD: buyback n is TINY (~7/yr raw, fewer fillable). This is a rule-in/out screen only —", flush=True)
print("strong+consistent beat both halves -> justify a full nse_corp_actions BQ pull; flat/neg -> kill cheap.", flush=True)
