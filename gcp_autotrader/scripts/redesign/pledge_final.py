"""Promoter-Pledge FINAL numbers for review — locked config (gated, hold=60, slots=10, 2.5xATR stop,
1.5% risk, compounding from Rs.2L). Emits: total profit + CAGR, trades/year, and the full year-wise
P&L ledger (entries, exits, realized P&L, year-end equity, return %). Same engine as pledge_walk.py,
instrumented per-trade. Survivorship-safe, real Upstox cost, IS/OOS split. READ-ONLY, cached only."""
import os
for _v in ("OMP_NUM_THREADS","OPENBLAS_NUM_THREADS","MKL_NUM_THREADS","NUMEXPR_NUM_THREADS","VECLIB_MAXIMUM_THREADS"):
    os.environ[_v]="4"
import sys, json, glob, pickle
from bisect import bisect_right, bisect_left
from datetime import datetime
from collections import defaultdict
sys.path.insert(0,"/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.backtest.costs import compute_leg_cost, CostConfig

C=os.path.expanduser("~/.autotrader_backtest_cache"); PIT=os.path.join(C,"insider_pit")
UPSTOX=CostConfig.upstox(); CAP0,SLIP,IS_END=200_000.0,0.001,"2020-12-31"
TURN_MIN,PRICE_MIN,ATR_MULT,RISK_PCT,B200_MIN=10e7,30.0,2.5,0.015,50.0
HOLD,SLOTS=60,10

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

print("loading ...", flush=True)
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
    cands.append({"ed":ed,"sym":sym,"ref":ref,"sl":ATR_MULT*S["atr"][ref-1],
                  "relval":shares*S["o"][ref],"b200":b200_at(ed),"nifty_ok":nifty_ok(ed)})
cands.sort(key=lambda x:(x["ed"], -x["relval"]))

equity=CAP0; free=[""]*SLOTS; openp=[]; trades=[]
for c in cands:
    if c["b200"]<=B200_MIN or not c["nifty_ok"]: continue
    ed=c["ed"]; still=[]
    for xd,pnl in openp:
        if xd<=ed: equity+=pnl
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
    pnl=(xf-ef)*qty-(compute_leg_cost(side="BUY",qty=qty,price=ef,is_swing=True,cfg=UPSTOX)
                     +compute_leg_cost(side="SELL",qty=qty,price=xf,is_swing=True,cfg=UPSTOX))
    free[slot]=xd; openp.append((xd,pnl))
    trades.append({"entry":ed,"exit":xd,"pnl":pnl,"notional":qty*epx})
for xd,pnl in openp: equity+=pnl

# year ledger
byexit=sorted(trades,key=lambda t:t["exit"])
years=sorted({int(t["entry"][:4]) for t in trades} | {int(t["exit"][:4]) for t in trades})
ent=defaultdict(int); exi=defaultdict(int); pnly=defaultdict(float)
for t in trades:
    ent[int(t["entry"][:4])]+=1; exi[int(t["exit"][:4])]+=1; pnly[int(t["exit"][:4])]+=t["pnl"]
# equity curve by exit for year-end + maxDD
eq=CAP0; peak=CAP0; mdd=0.0; yend={}
for t in byexit:
    eq+=t["pnl"]; peak=max(peak,eq); mdd=min(mdd,eq/peak-1.0); yend[int(t["exit"][:4])]=eq
span=years[-1]-years[0]+1; cagr=(equity/CAP0)**(1/span)-1
wins=sum(1 for t in trades if t["pnl"]>0); avg_notional=sum(t["notional"] for t in trades)/len(trades)

print(f"\n{'='*74}\n  PROMOTER-PLEDGE channel — FINAL backtest numbers (PAPER/backtest, Rs.2L base)\n{'='*74}", flush=True)
print(f"  Config: gated (b200>50 & Nifty>100DMA) | 60-day hold | 10 slots | 2.5xATR stop | 1.5% risk | compounding", flush=True)
print(f"  Period: {years[0]}-{years[-1]} ({span}y, survivorship-safe incl. delisted) | real Upstox cost + 0.1% slip\n", flush=True)
print(f"  Start capital        Rs.{CAP0:>12,.0f}", flush=True)
print(f"  Final capital        Rs.{equity:>12,.0f}", flush=True)
print(f"  TOTAL PROFIT         Rs.{equity-CAP0:>12,.0f}   ({(equity/CAP0-1)*100:+.0f}% total)", flush=True)
print(f"  CAGR                 {cagr*100:>12.1f}%", flush=True)
print(f"  Max drawdown         {mdd*100:>12.1f}%", flush=True)
print(f"  Calmar               {cagr/abs(mdd):>12.2f}", flush=True)
print(f"  Total trades         {len(trades):>12}   ({len(trades)/span:.1f}/yr avg)", flush=True)
print(f"  Win rate             {100*wins/len(trades):>11.1f}%", flush=True)
print(f"  Avg position size    Rs.{avg_notional:>12,.0f}\n", flush=True)

print(f"  {'Year':6}{'Entries':>9}{'Exits':>7}{'Realized P&L':>16}{'Year-end eq':>15}{'Return':>9}", flush=True)
prev=CAP0
for y in years:
    ye=yend.get(y,prev); ret=(ye/prev-1)*100 if prev>0 else 0
    print(f"  {y:<6}{ent[y]:>9}{exi[y]:>7}   Rs.{pnly[y]:>11,.0f}   Rs.{ye:>11,.0f}   {ret:>+7.1f}%", flush=True)
    prev=ye
isp=sum(t["pnl"] for t in trades if t["exit"]<=IS_END); oosp=sum(t["pnl"] for t in trades if t["exit"]>IS_END)
print(f"\n  IS (<=2020) realized P&L:  Rs.{isp:>11,.0f}      OOS (>=2021) realized P&L:  Rs.{oosp:>11,.0f}", flush=True)
print(f"\n  NOTE: most of the CAGR is macro-gate beta (long liquid mid-caps in good regimes); the pledge", flush=True)
print(f"  signal's OWN cross-sectional excess is ~+1.9%/60d. Real + additive, but a diversifier not a mover.", flush=True)
