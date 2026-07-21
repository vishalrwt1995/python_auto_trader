"""SHORT-INTEREST proper deep grind (the one killed dataset that only got triage). Triage killed it on
(a) IS-positive configs flipping OOS and (b) the 2024-26 NSE short-reporting explosion making IS(sparse)
vs OOS(dense) near-different datasets. Fix BOTH with COVERAGE-INVARIANT normalization:
- short_ratio = short_qty / same-day volume (intensity; invariant to how many stocks are reported)
- short_z    = (short_qty − stock's trailing-20d mean)/std (per-stock spike; works even when sparse)
- cross-sectional daily percentile (rank within the day's reported set; invariant to set size)
Most-plausible real use = HIGH short-selling → underperformance → an AVOID FILTER for the long channels
(stock-only long can't short). Also test covering/contrarian. Forward NET f20/f60, next-day entry (short
data is EOD → act next day, no look-ahead), fillable (turn>=10cr, px>=30), IS(<=2020)/OOS(>=2021), with
honest per-half event counts. READ-ONLY, single-process, cached."""
import os, json, pickle, statistics
from bisect import bisect_left
from collections import defaultdict
C=os.path.expanduser("~/.autotrader_backtest_cache")
S="/private/tmp/claude-501/-Users-apple-Projects-Migrated-Auto-Trading-Python-GCP/439e48e8-a413-4a1d-9d0a-530e53a5e277/scratchpad"
COST, IS_END, TURN_MIN, PRICE_MIN = 0.007, "2020-12-31", 10e7, 30.0

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
    SYM[s]={"d":d,"c":c,"v":v,"turn":turn,"idx":{dd:i for i,dd in enumerate(d)}}
short=json.load(open(f"{S}/short_selling.json"))
# per symbol: {barindex: short_qty}
sh=defaultdict(dict)
for r in short:
    sym=r["symbol"]; Sd=SYM.get(sym)
    if not Sd: continue
    i=Sd["idx"].get(r["date"][:10])
    if i is not None:
        try: sh[sym][i]=float(r["qty"])
        except: pass

def fwd(sym,i,k):
    Sd=SYM[sym]
    return (Sd["c"][i+1+k]/Sd["c"][i+1]-1.0-COST) if i+1+k<len(Sd["c"]) and Sd["c"][i+1]>0 else None
def fillable(sym,i):
    Sd=SYM[sym]
    return Sd["turn"][i] is not None and Sd["turn"][i]>=TURN_MIN and Sd["c"][i]>=PRICE_MIN

# build events with coverage-invariant features
events=[]
for sym,dq in sh.items():
    Sd=SYM[sym]; idxs=sorted(dq)
    for j,i in enumerate(idxs):
        if i+22>=len(Sd["c"]) or not fillable(sym,i): continue
        vol=Sd["v"][i]
        if vol<=0: continue
        ratio=dq[i]/vol
        if j>=10:
            hist=[dq[idxs[k]] for k in range(max(0,j-20),j)]
            mu=statistics.mean(hist); sd=statistics.pstdev(hist) or 1.0
            z=(dq[i]-mu)/sd
        else: z=None
        f20=fwd(sym,i,20); f60=fwd(sym,i,60)
        if f20 is None: continue
        events.append({"dt":Sd["d"][i],"ratio":ratio,"z":z,"f20":f20,"f60":f60})
def half(evs,lo,hi): return [e for e in evs if lo<e["dt"]<=hi]
IS=half(events,"0000",IS_END); OOS=half(events,IS_END,"9999")
print(f"{len(events)} fillable short-events | IS n={len(IS)} / OOS n={len(OOS)}  (note the IS-sparse/OOS-dense coverage imbalance)\n", flush=True)

def m(vals,k):
    v=[e[k] for e in vals if e[k] is not None]
    return f"avg={statistics.mean(v)*100:+5.2f}% med={statistics.median(v)*100:+5.2f}% WR={100*sum(1 for x in v if x>0)/len(v):4.0f}% n={len(v)}" if v else "n/a"

# (1) cross-sectional daily short_ratio quintiles (coverage-invariant)
print("=== (1) cross-sectional daily short_ratio quintile -> fwd20 (Q1=least-shorted .. Q5=most) ===", flush=True)
def xsec(evs, lbl):
    byd=defaultdict(list)
    for e in evs: byd[e["dt"]].append(e)
    q={1:[],2:[],3:[],4:[],5:[]}
    days=0
    for dt,es in byd.items():
        if len(es)<10: continue
        days+=1; es=sorted(es,key=lambda x:x["ratio"]); n=len(es)
        for qi in range(5):
            for e in es[qi*n//5:(qi+1)*n//5]: q[qi+1].append(e["f20"])
    print(f"  {lbl} ({days} days w/ >=10 names):", flush=True)
    for qi in (1,2,3,4,5):
        vv=q[qi]; print(f"     Q{qi} {('avg=%+.2f%% WR=%.0f%% n=%d'%(statistics.mean(vv)*100,100*sum(1 for x in vv if x>0)/len(vv),len(vv))) if vv else 'n/a'}", flush=True)
    if q[1] and q[5]: print(f"     -> Q1(low)-Q5(high) spread = {(statistics.mean(q[1])-statistics.mean(q[5]))*100:+.2f}%  (positive = avoid-high-short works)", flush=True)
xsec(IS,"IS "); xsec(OOS,"OOS")

# (2) per-stock short_z buckets (works in sparse IS too)
print("\n=== (2) per-stock short_z -> fwd20 (spike z>1 vs collapse/covering z<-1) ===", flush=True)
for lbl,evs in [("IS ",IS),("OOS",OOS)]:
    sp=[e for e in evs if e["z"] is not None and e["z"]>1]; cv=[e for e in evs if e["z"] is not None and e["z"]<-1]
    print(f"  {lbl} short-SPIKE(z>1): {m(sp,'f20')}   |  short-COLLAPSE(z<-1): {m(cv,'f20')}", flush=True)

# (3) FILTER value: does excluding the most-shorted names lift a long book? (both halves)
print("\n=== (3) AVOID-FILTER: all fillable-short names vs excluding top-quintile short_ratio (fwd20) ===", flush=True)
for lbl,evs in [("IS ",IS),("OOS",OOS)]:
    if len(evs)<20: print(f"  {lbl} too thin (n={len(evs)})"); continue
    thr=sorted(e["ratio"] for e in evs)[int(len(evs)*0.8)]
    allm=m(evs,'f20'); keptm=m([e for e in evs if e["ratio"]<thr],'f20')
    print(f"  {lbl} all: {allm}   |  excl. top-20% short: {keptm}", flush=True)
print("\nREAD: robust edge only if a direction holds in BOTH IS and OOS on the coverage-invariant features.", flush=True)
print("If IS is too thin to conclude, say so; if high-short robustly underperforms both halves -> real AVOID filter.", flush=True)
