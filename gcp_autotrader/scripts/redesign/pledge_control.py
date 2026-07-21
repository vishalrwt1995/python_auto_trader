"""Pledge SIGNAL-vs-TIMING control (final honesty gate). The gated walk only buys in good regimes,
so part of +19% could just be 'long liquid mid-caps in a good tape.' This strips that out: for each
pledge-release entry date, compute the SAME-DAY cross-sectional mean f60 of ALL liquid names (what a
random gated buy that day returned), and measure pledge's EXCESS over it. Date-matched => removes BOTH
the macro-gate and the 'pledge fires in good months' confound. If excess is robustly +ve IS & OOS, the
pledge signal itself is real alpha. If excess ~ 0, it was just market timing (kill). READ-ONLY, cached."""
import os
for _v in ("OMP_NUM_THREADS","OPENBLAS_NUM_THREADS","MKL_NUM_THREADS","NUMEXPR_NUM_THREADS","VECLIB_MAXIMUM_THREADS"):
    os.environ[_v]="4"
import json, glob, pickle, statistics
from bisect import bisect_right
from datetime import datetime
from collections import defaultdict

C=os.path.expanduser("~/.autotrader_backtest_cache"); PIT=os.path.join(C,"insider_pit")
COST,IS_END,TURN_MIN,PRICE_MIN=0.007,"2020-12-31",10e7,30.0
def dd_of(r):
    s=str(r.get("date","")).split()[0] if r.get("date") else ""
    try: return datetime.strptime(s,"%d-%b-%Y").strftime("%Y-%m-%d")
    except Exception: return None

print("loading + indexing bars by date ...", flush=True)
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
    idx={dd:i for i,dd in enumerate(d)}
    SYM[s]={"d":d,"c":c,"turn":turn,"idx":idx}

# cross-sectional gated-market f60 per DATE: mean f60 over all liquid names entering that date
def market_f60(date):
    vals=[]
    for s,S in SYM.items():
        i=S["idx"].get(date)
        if i is None or i+60>=len(S["c"]): continue
        if S["turn"][i] is None or S["turn"][i]<TURN_MIN or S["c"][i]<PRICE_MIN or S["c"][i]<=0: continue
        vals.append(S["c"][i+60]/S["c"][i]-1.0-COST)
    return statistics.mean(vals) if vals else None

recs=[]
for fn in sorted(glob.glob(os.path.join(PIT,"*.json"))):
    try: recs.extend(json.load(open(fn)))
    except Exception: pass

# pledge-release entries -> (entry_date, f60); cache market f60 by date
mkt_cache={}
pool=[]
for r in recs:
    if "revoke" not in str(r.get("tdpTransactionType","")).lower(): continue
    if "promoter" not in str(r.get("personCategory","")).lower(): continue
    sym=str(r.get("symbol") or "").upper(); dd=dd_of(r); S=SYM.get(sym)
    if not S or not dd: continue
    ref=bisect_right(S["d"],dd)
    if ref>=len(S["c"]) or ref<1 or S["turn"][ref] is None or S["turn"][ref]<TURN_MIN or S["c"][ref]<PRICE_MIN: continue
    if ref+60>=len(S["c"]) or S["c"][ref]<=0: continue
    ed=S["d"][ref]; pf=S["c"][ref+60]/S["c"][ref]-1.0-COST
    if ed not in mkt_cache: mkt_cache[ed]=market_f60(ed)
    mf=mkt_cache[ed]
    if mf is None: continue
    pool.append({"ed":ed,"pf":pf,"mf":mf,"ex":pf-mf})
print(f"  {len(pool)} date-matched pledge entries\n", flush=True)

def rep(lbl,sub):
    pf=[p["pf"] for p in sub]; mf=[p["mf"] for p in sub]; ex=[p["ex"] for p in sub]
    if not sub: print(f"  {lbl}: n/a"); return
    print(f"  {lbl:6} n={len(sub):4}  pledge f60={statistics.mean(pf)*100:+6.2f}%  market f60={statistics.mean(mf)*100:+6.2f}%  EXCESS={statistics.mean(ex)*100:+6.2f}%  (excess WR={100*sum(1 for x in ex if x>0)/len(ex):.1f}%)", flush=True)

print("=== pledge-release f60 vs SAME-DAY liquid-universe f60 (date-matched; strips gate+timing) ===", flush=True)
rep("ALL", pool)
rep("IS", [p for p in pool if p["ed"]<=IS_END])
rep("OOS",[p for p in pool if p["ed"]>IS_END])
print("\nREAD: robust +ve EXCESS in BOTH IS & OOS => the pledge signal itself is alpha (not just tape).", flush=True)
