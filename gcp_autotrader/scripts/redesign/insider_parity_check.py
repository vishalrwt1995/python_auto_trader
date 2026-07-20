"""INSIDER parity re-validation: re-run the LOCKED god-mode config but compute per-leg value =
shares(secAcq) × close-on-disclosure-date instead of the old filer secVal (which live XBRL shows
is junk). Confirms the ≥Rs5L cluster gate + the edge survive the value-recompute the LIVE feed
forces. Locked config: cluster(>=2) & b200>50 & Nifty>100DMA, turn>=10cr, h90, s10, 2.5ATR fixed
stop, 1.5% risk. Survivorship-safe (pead_full_bars_2014). READ-ONLY, single-process, cached only."""
import os
for _v in ("OMP_NUM_THREADS","OPENBLAS_NUM_THREADS","MKL_NUM_THREADS","NUMEXPR_NUM_THREADS","VECLIB_MAXIMUM_THREADS"):
    os.environ[_v]="4"
import sys, json, glob, pickle, statistics
from bisect import bisect_left, bisect_right
from datetime import datetime
from collections import defaultdict
sys.path.insert(0,"/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.backtest.costs import compute_leg_cost, CostConfig

C=os.path.expanduser("~/.autotrader_backtest_cache"); PIT=os.path.join(C,"insider_pit")
UPSTOX=CostConfig.upstox(); CAP0,SLIP,IS_END=200_000.0,0.001,"2020-12-31"
MIN_LEG=500000.0; MIN_BUYERS=2; TURN_MIN=10e7; B200_MIN=50.0
HOLD,SLOTS,RISK_PCT,ATR_MULT=90,10,0.015,2.5
INFORMED=("promoter","director","key managerial","immediate relative","promoter group")

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
    SYM[s]={"d":d,"o":o,"c":c,"atr":atr14(h,l,c),"turn":turn,"idx":{dt:i for i,dt in enumerate(d)}}
rf=json.load(open(f"{C}/regime_faithful_2015.json")); rd=sorted(rf.keys())
b200h=pickle.load(open(f"{C}/swing_b200_history.pkl","rb")); bdd=sorted(b200h.keys())
mkt=json.load(open(f"{C}/market_inputs_2015.json"))
md=sorted(x for x in mkt if mkt[x].get("nifty_close")); nc=[float(mkt[x]["nifty_close"]) for x in md]
ma100=[None]*len(nc); run=0.0
for i in range(len(nc)):
    run+=nc[i]
    if i>=100: run-=nc[i-100]
    if i>=99: ma100[i]=run/100.0
def nifty_ok(d):
    i=bisect_left(md,d)-1
    return i<0 or ma100[i] is None or nc[i]>ma100[i]
def b200_at(d):
    i=bisect_right(bdd,d)-1
    return b200h[bdd[i]] if i>=0 else 0.0

recs=[]
for fn in sorted(glob.glob(os.path.join(PIT,"*.json"))):
    try: recs.extend(json.load(open(fn)))
    except Exception: pass
print(f"  {len(recs):,} raw PIT rows\n", flush=True)

# aggregate clusters with value = shares(secAcq) × close-on-disclosure-day
agg=defaultdict(lambda:{"n":0,"val":0.0,"cats":set()})
for r in recs:
    if "buy" not in str(r.get("tdpTransactionType","")).lower(): continue
    if not any(k in str(r.get("personCategory","")).lower() for k in INFORMED): continue
    mode=str(r.get("acqMode","")).lower()
    if "market" not in mode or "off" in mode: continue
    sym=str(r.get("symbol") or "").strip().upper()
    S=SYM.get(sym); dd=dd_of(r)
    if not S or not dd: continue
    shares=fnum(r.get("secAcq"))
    if not shares or shares<=0: continue
    di=bisect_right(S["d"],dd)-1                    # close on/just before disclosure day
    if di<0: continue
    val=shares*S["c"][di]                            # <-- recomputed value (shares × close)
    if val<MIN_LEG: continue
    a=agg[(sym,dd)]; a["n"]+=1; a["val"]+=val; a["cats"].add(str(r.get("personCategory")))

cands=[]
for (sym,dd),a in agg.items():
    if a["n"]<MIN_BUYERS: continue                   # cluster gate (post value-recompute)
    S=SYM[sym]; ref=bisect_right(S["d"],dd)          # entry = next trading day after disclosure
    if ref>=len(S["c"]) or ref<1 or S["atr"][ref-1] is None or S["atr"][ref-1]<=0: continue
    ed=S["d"][ref]
    if S["o"][ref]<30 or S["turn"][ref] is None: continue
    cands.append({"ed":ed,"sym":sym,"ref":ref,"sl":ATR_MULT*S["atr"][ref-1],"turn":S["turn"][ref],
                  "n":a["n"],"b200":b200_at(ed),"nifty_ok":nifty_ok(ed)})
cands.sort(key=lambda x:x["ed"])
print(f"clusters (value=shares×close, >=2 legs): {len(cands):,}\n", flush=True)

def seg(points,start,years):
    if not points or start<=0: return 0.0,0.0
    peak=start; mdd=0.0; last=start
    for _,eq in points: peak=max(peak,eq); mdd=min(mdd,eq/peak-1.0); last=eq
    return ((last/start)**(1/years)-1 if years>0 else 0.0), mdd

def walk():
    equity=CAP0; free=[""]*SLOTS; openp=[]; closed=[]
    for c in cands:
        if c["turn"]<TURN_MIN or c["b200"]<=B200_MIN or not c["nifty_ok"]: continue
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
            if k<len(S["c"]) and S["c"][k]<=stop: xpx=stop; xi=k; break   # daily-close stop
        xd=S["d"][xi]; ef=epx*(1+SLIP); xf=xpx*(1-SLIP)
        pnl=(xf-ef)*qty-(compute_leg_cost(side="BUY",qty=qty,price=ef,is_swing=True,cfg=UPSTOX)
                         +compute_leg_cost(side="SELL",qty=qty,price=xf,is_swing=True,cfg=UPSTOX))
        free[slot]=xd; openp.append((xd,pnl));
    for xd,pnl in openp: equity+=pnl; closed.append((xd,pnl))
    closed.sort(); curve=[]; eq=CAP0
    for xd,pnl in closed: eq+=pnl; curve.append((xd,eq))
    y0=int(closed[0][0][:4]);y1=int(closed[-1][0][:4])
    isp=[p for p in curve if p[0]<=IS_END]; oosp=[p for p in curve if p[0]>IS_END]; eqis=isp[-1][1] if isp else CAP0
    fc,fdd=seg(curve,CAP0,y1-y0+1); ic,idd=seg(isp,CAP0,max(1,2020-y0+1)); oc,odd=seg(oosp,eqis,max(1,y1-2021+1))
    wr=100*sum(1 for _,p in closed if p>0)/len(closed)
    yr=defaultdict(float)
    for xd,pnl in closed: yr[xd[:4]]+=pnl
    return eq,fc,fdd,ic,idd,oc,odd,len(closed),wr,dict(yr)

eq,fc,fdd,ic,idd,oc,odd,n,wr,yr=walk()
print("=== PARITY WALK (value = shares × close) — locked config h90 s10 ===", flush=True)
print(f"  final=Rs{eq:,.0f}  CAGR={fc*100:+.1f}%  maxDD={fdd*100:.1f}%  Calmar={fc/abs(fdd) if fdd<0 else 0:.2f}", flush=True)
print(f"  IS Calmar {ic/abs(idd) if idd<0 else 0:.2f} ({ic*100:+.1f}%) | OOS Calmar {oc/abs(odd) if odd<0 else 0:.2f} ({oc*100:+.1f}%)", flush=True)
print(f"  trades={n} (~{n//12}/yr)  WR={wr:.0f}%", flush=True)
print(f"\n  ORIGINAL (filer secVal): +23.0% CAGR / -12.5% DD / Calmar 1.84 (IS 2.85 / OOS 1.75)", flush=True)
print(f"  per-year net: " + "  ".join(f"{y}:{v/1000:+.0f}k" for y,v in sorted(yr.items())), flush=True)
print("\n  READ: if CAGR/DD/Calmar stay in the same ballpark + both halves positive -> parity holds,", flush=True)
print("  the shares×close value-recompute is safe, channel is validated for the live XBRL feed.", flush=True)
