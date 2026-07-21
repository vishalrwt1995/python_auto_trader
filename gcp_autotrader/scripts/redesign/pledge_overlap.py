"""Pledge-release DISTINCTNESS + ADDITIVITY test (the decision gate). The 82% symbol-ever-overlap
with insider-buys is a weak measure (any active mid-cap had an insider buy sometime in 11yr). The
real questions: (1) TEMPORAL overlap — do pledge-release entries land near an actual insider event
in time? (2) ADDITIVITY — does the forward edge SURVIVE on pledge-releases that have NO insider buy
within +/-30d? If the no-insider subset keeps the edge, pledge-release is NEW alpha, not a duplicate.
Gated population (b200>50 & Nifty>100DMA) to match the channel. READ-ONLY, cached only."""
import os
for _v in ("OMP_NUM_THREADS","OPENBLAS_NUM_THREADS","MKL_NUM_THREADS","NUMEXPR_NUM_THREADS","VECLIB_MAXIMUM_THREADS"):
    os.environ[_v]="4"
import json, glob, pickle, statistics
from bisect import bisect_right, bisect_left
from datetime import datetime
from collections import defaultdict

C=os.path.expanduser("~/.autotrader_backtest_cache"); PIT=os.path.join(C,"insider_pit")
COST,IS_END,TURN_MIN,PRICE_MIN,B200_MIN=0.007,"2020-12-31",10e7,30.0,50.0
def dd_of(r):
    s=str(r.get("date","")).split()[0] if r.get("date") else ""
    try: return datetime.strptime(s,"%d-%b-%Y").strftime("%Y-%m-%d")
    except Exception: return None
def dnum(s):
    try: return datetime.strptime(s,"%Y-%m-%d")
    except Exception: return None

print("loading ...", flush=True)
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
b200h=pickle.load(open(f"{C}/swing_b200_history.pkl","rb")); bdd=sorted(b200h.keys())
mkt=json.load(open(f"{C}/market_inputs_2015.json")); md=sorted(x for x in mkt if mkt[x].get("nifty_close")); nc=[float(mkt[x]["nifty_close"]) for x in md]
ma=[None]*len(nc); run=0.0
for i in range(len(nc)):
    run+=nc[i]
    if i>=100: run-=nc[i-100]
    if i>=99: ma[i]=run/100.0
def nifty_ok(dt):
    i=bisect_left(md,dt)-1; return i<0 or ma[i] is None or nc[i]>ma[i]
def b200_at(dt):
    i=bisect_right(bdd,dt)-1; return b200h[bdd[i]] if i>=0 else 0.0

recs=[]
for fn in sorted(glob.glob(os.path.join(PIT,"*.json"))):
    try: recs.extend(json.load(open(fn)))
    except Exception: pass

# insider open-market BUY dates per symbol; and same-day CLUSTER dates (>=2 buys) per symbol
buy_dates=defaultdict(list); day_ct=defaultdict(int)
for r in recs:
    if "buy" not in str(r.get("tdpTransactionType","")).lower(): continue
    if not any(k in str(r.get("personCategory","")).lower() for k in ("promoter","director","managerial","relative")): continue
    m=str(r.get("acqMode","")).lower()
    if "market" not in m or "off" in m: continue
    sym=str(r.get("symbol") or "").upper(); dd=dd_of(r)
    if sym and dd: buy_dates[sym].append(dd); day_ct[(sym,dd)]+=1
cluster_dates=defaultdict(list)
for (sym,dd),ct in day_ct.items():
    if ct>=2: cluster_dates[sym].append(dd)
for d in buy_dates.values(): d.sort()
for d in cluster_dates.values(): d.sort()

def near(dates_for_sym, ed, win):
    e=dnum(ed)
    if not e: return False
    lo=bisect_left(dates_for_sym, (e).strftime("%Y-%m-%d"))  # crude; refine w/ day math below
    for ds in dates_for_sym:
        dq=dnum(ds)
        if dq and abs((dq-e).days)<=win: return True
    return False

# gated pledge-release population + forward return, split by insider proximity
pool=[]
for r in recs:
    if "revoke" not in str(r.get("tdpTransactionType","")).lower(): continue
    if "promoter" not in str(r.get("personCategory","")).lower(): continue
    sym=str(r.get("symbol") or "").upper(); dd=dd_of(r); S=SYM.get(sym)
    if not S or not dd: continue
    ref=bisect_right(S["d"],dd)
    if ref>=len(S["c"]) or ref<1: continue
    if S["turn"][ref] is None or S["turn"][ref]<TURN_MIN or S["c"][ref]<PRICE_MIN: continue
    ed=S["d"][ref]
    if b200_at(ed)<=B200_MIN or not nifty_ok(ed): continue      # gated pop
    f60=(S["c"][ref+60]/S["c"][ref]-1.0-COST) if ref+60<len(S["c"]) and S["c"][ref]>0 else None
    has_buy=near(buy_dates.get(sym,[]), ed, 30)
    has_clu=near(cluster_dates.get(sym,[]), ed, 45)
    pool.append({"ed":ed,"f60":f60,"has_buy":has_buy,"has_clu":has_clu})

n=len(pool)
pct_buy=100*sum(1 for p in pool if p["has_buy"])/n
pct_clu=100*sum(1 for p in pool if p["has_clu"])/n
print(f"\n=== gated pledge-release population: {n} events ===", flush=True)
print(f"  temporal overlap w/ ANY insider buy (+/-30d):     {pct_buy:5.1f}%", flush=True)
print(f"  temporal overlap w/ insider CLUSTER (+/-45d):      {pct_clu:5.1f}%   (<- the actual insider channel trigger)", flush=True)

def fwd_stat(sub,lbl):
    a=[p["f60"] for p in sub if p["f60"] is not None and p["ed"]<=IS_END]
    z=[p["f60"] for p in sub if p["f60"] is not None and p["ed"]>IS_END]
    def m(v): return f"avg={statistics.mean(v)*100:+5.2f}% WR={100*sum(1 for x in v if x>0)/len(v):4.1f}% n={len(v)}" if v else "n/a"
    print(f"  {lbl:34} IS: {m(a)}   OOS: {m(z)}", flush=True)

print(f"\n=== ADDITIVITY: does the f60 edge survive with NO nearby insider buy? (base f60 IS+1.46/OOS+3.30) ===", flush=True)
fwd_stat(pool, "ALL gated pledge-release")
fwd_stat([p for p in pool if not p["has_buy"]], "NO insider buy +/-30d (pure pledge)")
fwd_stat([p for p in pool if p["has_buy"]], "WITH insider buy +/-30d (overlap)")
print("\nREAD: if 'pure pledge' keeps a strong IS+OOS edge, pledge-release is ADDITIVE alpha (build it).", flush=True)
print("If the edge lives only in the 'with insider buy' subset, it's an insider duplicate (kill/merge).", flush=True)
