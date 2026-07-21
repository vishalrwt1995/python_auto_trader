"""Promoter pledge-RELEASE account walk (decisive test) — does the strong per-signal edge survive
a 5/10-slot compounding account with fixed-hold exit + real Upstox costs? Grid over hold {40,60,90}
x slots {5,10} x macro-gate {none, double b200>50 & Nifty>100DMA}. Entry = next trading day after
the public pledge-revoke disclosure; fixed-hold + 2.5xATR protective stop; risk 1.5% equity; slot
priority = larger release (shares x close). IS(<=2020)/OOS(>=2021). PLUS overlap vs the insider-buy
signal (are pledge-release names just the insider-buy names?). Survivorship-safe (pead_full_bars_2014).
READ-ONLY, single-process, cached only (zero GCP cost). Imports prod cost model read-only."""
import os
for _v in ("OMP_NUM_THREADS","OPENBLAS_NUM_THREADS","MKL_NUM_THREADS","NUMEXPR_NUM_THREADS","VECLIB_MAXIMUM_THREADS"):
    os.environ[_v]="4"
import sys, json, glob, pickle, statistics
from bisect import bisect_right, bisect_left
from datetime import datetime
from collections import defaultdict
sys.path.insert(0,"/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.backtest.costs import compute_leg_cost, CostConfig

C=os.path.expanduser("~/.autotrader_backtest_cache"); PIT=os.path.join(C,"insider_pit")
UPSTOX=CostConfig.upstox(); CAP0,SLIP,IS_END=200_000.0,0.001,"2020-12-31"
TURN_MIN,PRICE_MIN,ATR_MULT,RISK_PCT,B200_MIN=10e7,30.0,2.5,0.015,50.0

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

print("loading bars + regime + nifty + PIT ...", flush=True)
bars=pickle.load(open(f"{C}/pead_full_bars_2014.pkl","rb"))
SYM={}
for s,b in bars.items():
    if len(b)<70: continue
    d=[x[0] for x in b];o=[x[1] for x in b];h=[x[2] for x in b];l=[x[3] for x in b];c=[x[4] for x in b];v=[x[5] for x in b]
    turn=[None]*len(c); run=0.0
    for i in range(len(c)):
        if i>=1: run+=c[i-1]*v[i-1]
        if i>=21: run-=c[i-21]*v[i-21]
        if i>=21: turn[i]=run/20.0
    SYM[s]={"d":d,"o":o,"c":c,"atr":atr14(h,l,c),"turn":turn}
b200h=pickle.load(open(f"{C}/swing_b200_history.pkl","rb")); bdd=sorted(b200h.keys())
mkt=json.load(open(f"{C}/market_inputs_2015.json")); md=sorted(x for x in mkt if mkt[x].get("nifty_close")); nc=[float(mkt[x]["nifty_close"]) for x in md]
ma100=[None]*len(nc); run=0.0
for i in range(len(nc)):
    run+=nc[i]
    if i>=100: run-=nc[i-100]
    if i>=99: ma100[i]=run/100.0
def nifty_ok(dt):
    i=bisect_left(md,dt)-1;  return i<0 or ma100[i] is None or nc[i]>ma100[i]
def b200_at(dt):
    i=bisect_right(bdd,dt)-1; return b200h[bdd[i]] if i>=0 else 0.0

recs=[]
for fn in sorted(glob.glob(os.path.join(PIT,"*.json"))):
    try: recs.extend(json.load(open(fn)))
    except Exception: pass
# insider-buy name-days (for overlap check)
buy_syms=set()
for r in recs:
    if "buy" in str(r.get("tdpTransactionType","")).lower() and any(k in str(r.get("personCategory","")).lower() for k in ("promoter","director","managerial","relative")):
        m=str(r.get("acqMode","")).lower()
        if "market" in m and "off" not in m: buy_syms.add(str(r.get("symbol") or "").upper())

cands=[]
for r in recs:
    if "revoke" not in str(r.get("tdpTransactionType","")).lower(): continue
    if "promoter" not in str(r.get("personCategory","")).lower(): continue
    sym=str(r.get("symbol") or "").strip().upper(); dd=dd_of(r); S=SYM.get(sym)
    if not S or not dd: continue
    ref=bisect_right(S["d"],dd)
    if ref>=len(S["c"]) or ref<1 or S["atr"][ref-1] is None or S["atr"][ref-1]<=0: continue
    if S["turn"][ref] is None or S["turn"][ref]<TURN_MIN or S["o"][ref]<PRICE_MIN: continue
    ed=S["d"][ref]; shares=fnum(r.get("secAcq")) or 0.0
    cands.append({"ed":ed,"sym":sym,"ref":ref,"sl":ATR_MULT*S["atr"][ref-1],"turn":S["turn"][ref],
                  "relval":shares*S["o"][ref],"b200":b200_at(ed),"nifty_ok":nifty_ok(ed)})
cands.sort(key=lambda x:(x["ed"], -x["relval"]))   # date, then larger release first (slot priority)
print(f"  {len(cands):,} fillable promoter pledge-release candidates\n", flush=True)

def walk(hold,slots,use_gate):
    equity=CAP0; free=[""]*slots; openp=[]; closed=[]
    for c in cands:
        if use_gate and (c["b200"]<=B200_MIN or not c["nifty_ok"]): continue
        ed=c["ed"]; still=[]
        for xd,pnl in openp:
            if xd<=ed: equity+=pnl; closed.append((xd,pnl))
            else: still.append((xd,pnl))
        openp=still
        for k in range(slots):
            if free[k] and free[k]<=ed: free[k]=""
        slot=next((k for k in range(slots) if not free[k]),None)
        if slot is None: continue
        S=SYM[c["sym"]];ref=c["ref"];epx=S["o"][ref]
        if epx<=0: continue
        qty=int((RISK_PCT*equity)//c["sl"])
        if qty<1: continue
        if qty*epx>equity/slots: qty=int((equity/slots)//epx)
        if qty<1: continue
        xi=min(ref+hold,len(S["c"])-1); xpx=S["c"][xi]; stop=epx-c["sl"]
        for k in range(ref+1,xi+1):
            if k<len(S["c"]) and S["c"][k]<=stop: xpx=stop; xi=k; break
        xd=S["d"][xi]; ef=epx*(1+SLIP); xf=xpx*(1-SLIP)
        pnl=(xf-ef)*qty-(compute_leg_cost(side="BUY",qty=qty,price=ef,is_swing=True,cfg=UPSTOX)
                         +compute_leg_cost(side="SELL",qty=qty,price=xf,is_swing=True,cfg=UPSTOX))
        free[slot]=xd; openp.append((xd,pnl))
    for xd,pnl in openp: equity+=pnl; closed.append((xd,pnl))
    if not closed: return None
    closed.sort(); curve=[]; eq=CAP0; peak=CAP0; mdd=0.0
    for xd,pnl in closed: eq+=pnl; peak=max(peak,eq); mdd=min(mdd,eq/peak-1.0); curve.append((xd,eq))
    y0=int(closed[0][0][:4]); y1=int(closed[-1][0][:4]); span=y1-y0+1
    cagr=(eq/CAP0)**(1/span)-1; wr=100*sum(1 for _,p in closed if p>0)/len(closed)
    isr=[p for xd,p in closed if xd<=IS_END]; oosr=[p for xd,p in closed if xd>IS_END]
    return dict(cagr=cagr,mdd=mdd,calmar=cagr/abs(mdd) if mdd<0 else 0,n=len(closed),span=span,wr=wr,
                is_pos=sum(isr)>0, oos_pos=sum(oosr)>0)

print("=== ACCOUNT WALK GRID (fixed-hold + 2.5ATR stop, compounding, real costs) ===", flush=True)
print(f"  {'config':22} {'CAGR':>7} {'maxDD':>7} {'Calmar':>7} {'trades':>7} {'WR':>6}  IS+/OOS+", flush=True)
best=None
for gate in (False,True):
    for hold in (40,60,90):
        for slots in (5,10):
            r=walk(hold,slots,gate)
            if not r: continue
            tag=f"{'gated' if gate else 'nogate'} h{hold} s{slots}"
            flag=" <<" if r["calmar"]>=0.6 else ""
            print(f"  {tag:22} {r['cagr']*100:+6.1f}% {r['mdd']*100:6.1f}% {r['calmar']:7.2f} {r['n']:>7} {r['wr']:5.1f}%  {r['is_pos']}/{r['oos_pos']}{flag}", flush=True)
            if best is None or r["calmar"]>best[1]["calmar"]: best=(tag,r)

# overlap vs insider-buy
rel_syms={c["sym"] for c in cands}
ov=len(rel_syms & buy_syms)/len(rel_syms)*100 if rel_syms else 0
print(f"\n=== BEST by Calmar: {best[0]} -> CAGR {best[1]['cagr']*100:+.1f}% / DD {best[1]['mdd']*100:.1f}% / Calmar {best[1]['calmar']:.2f} ===", flush=True)
print(f"  overlap: {ov:.0f}% of pledge-release symbols also had an insider open-market BUY (distinctness check)", flush=True)
print("  READ: clears bar if Calmar>=0.6 + CAGR>>FD + both halves positive + low insider overlap.", flush=True)
