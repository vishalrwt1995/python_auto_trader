"""Promoter-Pledge SELECTION grind — can we lift the thin ~1.9%/60d edge with better selection?
Tests single filters + combos on the gated pledge-release pool, reporting BOTH the practical account
outcome (CAGR/DD/Calmar, IS & OOS both +) AND the pure date-matched cross-sectional excess (the honest
'is it real signal' yardstick). Features: trend (px>50/200DMA), prior 20d momentum, release size (secVal),
release-% of promoter holding (secAcq/afterAcqSharesNo), promoter holding %, and revoke-cluster (>=2 in 90d).
A filter is kept only if it lifts risk-adjusted return robustly in BOTH halves AND keeps >=~10 trades/yr.
Survivorship-safe, real Upstox cost. READ-ONLY, single-process, cached only."""
import os
for _v in ("OMP_NUM_THREADS","OPENBLAS_NUM_THREADS","MKL_NUM_THREADS","NUMEXPR_NUM_THREADS","VECLIB_MAXIMUM_THREADS"):
    os.environ[_v]="4"
import sys, json, glob, pickle, statistics
from bisect import bisect_right, bisect_left
from datetime import datetime, timedelta
from collections import defaultdict
sys.path.insert(0,"/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.backtest.costs import compute_leg_cost, CostConfig

C=os.path.expanduser("~/.autotrader_backtest_cache"); PIT=os.path.join(C,"insider_pit")
UPSTOX=CostConfig.upstox(); CAP0,SLIP,IS_END=200_000.0,0.001,"2020-12-31"
TURN_MIN,PRICE_MIN,ATR_MULT,RISK_PCT,B200_MIN,COST=10e7,30.0,2.5,0.015,50.0,0.007
HOLD,SLOTS=60,10

def sma(c,n):
    out=[None]*len(c); s=0.0
    for i in range(len(c)):
        s+=c[i]
        if i>=n: s-=c[i-n]
        if i>=n-1: out[i]=s/n
    return out
def atr14(h,l,c):
    tr=[h[0]-l[0]]
    for i in range(1,len(c)): tr.append(max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1])))
    out=[None]*len(c); s=0.0
    for i in range(len(tr)):
        s+=tr[i]
        if i>=14: s-=tr[i-14]
        if i>=13: out[i]=s/14.0
    return out
def fnum(x):
    try: return float(str(x).replace(",",""))
    except Exception: return None
def dd_of(r):
    s=str(r.get("date","")).split()[0] if r.get("date") else ""
    try: return datetime.strptime(s,"%d-%b-%Y").strftime("%Y-%m-%d")
    except Exception: return None

print("loading + featurizing ...", flush=True)
bars=pickle.load(open(f"{C}/pead_full_bars_2014.pkl","rb"))
SYM={}
for s,b in bars.items():
    if len(b)<210: continue
    d=[x[0] for x in b];o=[x[1] for x in b];h=[x[2] for x in b];l=[x[3] for x in b];c=[x[4] for x in b];v=[x[5] for x in b]
    turn=[None]*len(c); run=0.0
    for i in range(len(c)):
        if i>=1: run+=c[i-1]*v[i-1]
        if i>=21: run-=c[i-21]*v[i-21]
        if i>=21: turn[i]=run/20.0
    SYM[s]={"d":d,"o":o,"c":c,"atr":atr14(h,l,c),"turn":turn,"s50":sma(c,50),"s200":sma(c,200),"idx":{dd:i for i,dd in enumerate(d)}}
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
# revoke disclosure dates per symbol (for cluster)
rev_dd=defaultdict(list)
for r in recs:
    if "revoke" in str(r.get("tdpTransactionType","")).lower() and "promoter" in str(r.get("personCategory","")).lower():
        dd=dd_of(r);
        if dd: rev_dd[str(r.get("symbol") or "").upper()].append(dd)
for v in rev_dd.values(): v.sort()

# gated candidate pool with features
cands=[]
for r in recs:
    if "revoke" not in str(r.get("tdpTransactionType","")).lower(): continue
    if "promoter" not in str(r.get("personCategory","")).lower(): continue
    sym=str(r.get("symbol") or "").strip().upper(); dd=dd_of(r); S=SYM.get(sym)
    if not S or not dd: continue
    ref=bisect_right(S["d"],dd)
    if ref>=len(S["c"]) or ref<1 or S["atr"][ref-1] is None or S["atr"][ref-1]<=0: continue
    if S["turn"][ref] is None or S["turn"][ref]<TURN_MIN or S["o"][ref]<PRICE_MIN: continue
    ed=S["d"][ref]
    if b200_at(ed)<=B200_MIN or not nifty_ok(ed): continue        # gated pool
    c0=S["c"][ref]
    relval=fnum(r.get("secVal")) or (fnum(r.get("secAcq")) or 0)*S["o"][ref]
    hold_no=fnum(r.get("afterAcqSharesNo")) or 0
    relpct=(fnum(r.get("secAcq")) or 0)/hold_no*100 if hold_no>0 else 0
    holdpct=fnum(r.get("afterAcqSharesPer")) or 0
    # cluster: >=2 revokes for this symbol within prior 90 calendar days (incl this)
    e=datetime.strptime(ed,"%Y-%m-%d"); lo=(e-timedelta(days=90)).strftime("%Y-%m-%d")
    ncl=sum(1 for x in rev_dd[sym] if lo<=x<=dd)
    f60=(S["c"][ref+60]/c0-1.0-COST) if ref+60<len(S["c"]) and c0>0 else None
    cands.append({"ed":ed,"sym":sym,"ref":ref,"sl":ATR_MULT*S["atr"][ref-1],"relval":relval,
                  "above50":(S["s50"][ref] is not None and c0>S["s50"][ref]),
                  "above200":(S["s200"][ref] is not None and c0>S["s200"][ref]),
                  "ret20":(c0/S["c"][ref-21]-1) if ref>=21 and S["c"][ref-21]>0 else 0.0,
                  "relpct":relpct,"holdpct":holdpct,"cluster":ncl>=2,"f60":f60})
cands.sort(key=lambda x:(x["ed"], -x["relval"]))
print(f"  {len(cands)} gated candidates\n", flush=True)

# date-matched market f60 (once)
mktf={}
def market_f60(date):
    if date in mktf: return mktf[date]
    vals=[]
    for s,S in SYM.items():
        i=S["idx"].get(date)
        if i is None or i+60>=len(S["c"]) or S["turn"][i] is None or S["turn"][i]<TURN_MIN or S["c"][i]<PRICE_MIN or S["c"][i]<=0: continue
        vals.append(S["c"][i+60]/S["c"][i]-1.0-COST)
    mktf[date]=statistics.mean(vals) if vals else None
    return mktf[date]

def evaluate(name,pred):
    sub=[c for c in cands if pred(c)]
    if len(sub)<30:
        print(f"  {name:34} n={len(sub):<4} (too few)"); return
    equity=CAP0; free=[""]*SLOTS; openp=[]; closed=[]
    for c in sub:
        ed=c["ed"]; still=[]
        for xd,pnl in openp:
            if xd<=ed: equity+=pnl; closed.append((xd,pnl))
            else: still.append((xd,pnl))
        openp=still
        for k in range(SLOTS):
            if free[k] and free[k]<=ed: free[k]=""
        slot=next((k for k in range(SLOTS) if not free[k]),None)
        if slot is None: continue
        S=SYM[c["sym"]];ref=c["ref"];epx=S["o"][ref]
        if epx<=0: continue
        qty=int((RISK_PCT*equity)//c["sl"])
        if qty<1: continue
        if qty*epx>equity/SLOTS: qty=int((equity/SLOTS)//epx)
        if qty<1: continue
        xi=min(ref+HOLD,len(S["c"])-1); xpx=S["c"][xi]; stop=epx-c["sl"]
        for k in range(ref+1,xi+1):
            if k<len(S["c"]) and S["c"][k]<=stop: xpx=stop; xi=k; break
        xd=S["d"][xi]; ef=epx*(1+SLIP); xf=xpx*(1-SLIP)
        pnl=(xf-ef)*qty-(compute_leg_cost(side="BUY",qty=qty,price=ef,is_swing=True,cfg=UPSTOX)+compute_leg_cost(side="SELL",qty=qty,price=xf,is_swing=True,cfg=UPSTOX))
        free[slot]=xd; openp.append((xd,pnl))
    for xd,pnl in openp: equity+=pnl; closed.append((xd,pnl))
    closed.sort(); eq=CAP0;peak=CAP0;mdd=0.0
    for xd,pnl in closed: eq+=pnl;peak=max(peak,eq);mdd=min(mdd,eq/peak-1)
    span=int(closed[-1][0][:4])-int(closed[0][0][:4])+1
    cagr=(eq/CAP0)**(1/span)-1; wr=100*sum(1 for _,p in closed if p>0)/len(closed)
    isp=sum(p for xd,p in closed if xd<=IS_END); oosp=sum(p for xd,p in closed if xd>IS_END)
    # pure excess vs same-day liquid universe
    ex=[c["f60"]-market_f60(c["ed"]) for c in sub if c["f60"] is not None and market_f60(c["ed"]) is not None]
    exi=[c["f60"]-market_f60(c["ed"]) for c in sub if c["f60"] is not None and market_f60(c["ed"]) is not None and c["ed"]<=IS_END]
    exo=[c["f60"]-market_f60(c["ed"]) for c in sub if c["f60"] is not None and market_f60(c["ed"]) is not None and c["ed"]>IS_END]
    exm=statistics.mean(ex)*100 if ex else 0; exim=statistics.mean(exi)*100 if exi else 0; exom=statistics.mean(exo)*100 if exo else 0
    print(f"  {name:34} n={len(sub):<4} {len(sub)/span:4.0f}/y  CAGR{cagr*100:+5.1f}% DD{mdd*100:5.1f}% Cal{cagr/abs(mdd):4.2f} WR{wr:4.0f}%  IS/OOS{'+' if isp>0 else '-'}{'+' if oosp>0 else '-'}  excess{exm:+4.1f}%(IS{exim:+4.1f}/OOS{exom:+4.1f})", flush=True)

print("=== SINGLE FILTERS (baseline first) ===", flush=True)
evaluate("F0 baseline (all gated)", lambda c: True)
evaluate("F1 px>50DMA (no falling knife)", lambda c: c["above50"])
evaluate("F2 px>200DMA (uptrend)", lambda c: c["above200"])
evaluate("F3 ret20>=0 (momentum floor)", lambda c: c["ret20"]>=0)
evaluate("F4 relval>=Rs1cr", lambda c: c["relval"]>=1e7)
evaluate("F5 relval>=Rs10cr", lambda c: c["relval"]>=1e8)
evaluate("F6 release>=25% of holding", lambda c: c["relpct"]>=25)
evaluate("F7 release>=50% of holding", lambda c: c["relpct"]>=50)
evaluate("F8 promoter holding>=40%", lambda c: c["holdpct"]>=40)
evaluate("F9 revoke-cluster(>=2 in 90d)", lambda c: c["cluster"])
print("\n=== COMBOS (stack the winners) ===", flush=True)
evaluate("px>50DMA + relval>=1cr", lambda c: c["above50"] and c["relval"]>=1e7)
evaluate("px>50DMA + ret20>=0", lambda c: c["above50"] and c["ret20"]>=0)
evaluate("px>200DMA + release>=25%", lambda c: c["above200"] and c["relpct"]>=25)
evaluate("px>50DMA + release>=25%", lambda c: c["above50"] and c["relpct"]>=25)
evaluate("px>50DMA+relval>=1cr+ret20>=0", lambda c: c["above50"] and c["relval"]>=1e7 and c["ret20"]>=0)
print("\nREAD: keep a filter only if it lifts Calmar robustly, IS/OOS both +, >=~10/y, AND excess stays/rises.", flush=True)
print("A filter that lifts CAGR but not excess is adding momentum BETA (fine, but label it honestly).", flush=True)
