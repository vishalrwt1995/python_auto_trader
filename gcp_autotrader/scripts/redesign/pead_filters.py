"""PEAD grind-harder — ENGINE-TEST the selection-alpha filters from pead_features.py in the
full portfolio walk (slots, cost, slip), IS/OOS. READ-ONLY, local, zero cost. A filter is a
WIN only if it beats baseline Calmar AND net-up in BOTH halves (the swing-grind discipline:
CSV buckets must survive the portfolio walk — reallocation/cost/slots often kill them)."""
from __future__ import annotations
import os, sys, json, pickle
from bisect import bisect_left
from datetime import date
from statistics import mean

sys.path.insert(0, "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.domain.pead_signals import earnings_surprise, pre_event_runup, ANTI_PUMP_LOOKBACK, ATR_SL_MULT
from autotrader.domain.swing_exit import simulate_exit
from autotrader.backtest.costs import compute_leg_cost, CostConfig

C = os.path.expanduser("~/.autotrader_backtest_cache")
UPSTOX = CostConfig.upstox(); SLIP = 0.001; CAPITAL = 200_000.0
ev = json.load(open(f"{C}/pead_nse_result_dates_2012_2026.json"))["events"]
bars = pickle.load(open(f"{C}/swing_adj_bars_2015.pkl", "rb"))
mkt = json.load(open(f"{C}/market_inputs_2015.json"))
mdl = sorted(d for d in mkt if mkt[d].get("nifty_close"))
pk=-1e18; ddd={}
for d in mdl:
    v=float(mkt[d]["nifty_close"]); pk=max(pk,v); ddd[d]=v/pk-1.0
def mdd_at(d):
    i=bisect_left(mdl,d)-1; return ddd[mdl[i]] if i>=0 else None
def atr14(hi,lo,cl):
    tr=[hi[0]-lo[0]]
    for i in range(1,len(cl)): tr.append(max(hi[i]-lo[i],abs(hi[i]-cl[i-1]),abs(lo[i]-cl[i-1])))
    out=[None]*len(cl); s=0.0
    for i in range(len(tr)):
        s+=tr[i]
        if i>=14: s-=tr[i-14]
        if i>=13: out[i]=s/14.0
    return out
SYM={}
for s,b in bars.items():
    if not b or len(b)<ANTI_PUMP_LOOKBACK+20: continue
    d=[x[0] for x in b]; o=[float(x[1]) for x in b]; hi=[float(x[2]) for x in b]
    lo=[float(x[3]) for x in b]; cl=[float(x[4]) for x in b]; vol=[float(x[5]) for x in b]
    SYM[s]={"d":d,"o":o,"cl":cl,"vol":vol,"bars":b,"atr":atr14(hi,lo,cl)}
# pool with features (base gates surprise>=5%, anti-pump<75%, mkt-dd>-5%)
pool=[]
for e in ev:
    sy=SYM.get(e["symbol"])
    if sy is None or e["date"]<"2015-01-01": continue
    dl=sy["d"]; ri=bisect_left(dl,e["date"])
    if ri>=len(dl) or ri<ANTI_PUMP_LOOKBACK+1 or ri+1>=len(sy["cl"]): continue
    sp=earnings_surprise(sy["cl"],ri); ru=pre_event_runup(sy["cl"],ri); m=mdd_at(dl[ri]); atr=sy["atr"][ri]
    if sp is None or ru is None or m is None or not atr or atr<=0: continue
    if not(sp>=0.05 and ru<0.75 and m>-0.05): continue
    v20=mean(sy["vol"][ri-20:ri]) if ri>=20 else sy["vol"][ri]
    pool.append({"sym":e["symbol"],"ei":ri+1,"entry_d":dl[ri+1],"sp":sp,"ru":ru,"atr":atr,
                 "gapfrac":((sy["o"][ri]/sy["cl"][ri-1]-1.0)/sp if sp else 0),
                 "vr":(sy["vol"][ri]/v20 if v20>0 else 0)})
pool.sort(key=lambda c:c["entry_d"])

def run(flt, slots=5, risk=3000.0, max_hold=60):
    free=[""]*slots; tr=[]
    for c in pool:
        if not flt(c): continue
        sy=SYM[c["sym"]]; ei=c["ei"]; ed=c["entry_d"]
        k=next((j for j in range(slots) if free[j]<=ed),None)
        if k is None: continue
        epx=sy["o"][ei]; sl=ATR_SL_MULT*c["atr"]; qty=int(risk//sl)
        if qty<1 or epx<=0: continue
        if qty*epx>CAPITAL/slots: qty=int((CAPITAL/slots)//epx)
        if qty<1: continue
        off,xpx,_=simulate_exit(sy["bars"],ei,True,sl,max_hold,trail_R=1.0,activate_R=1.75)
        xi=min(ei+off,len(sy["bars"])-1); free[k]=sy["d"][xi]
        ef=epx*(1+SLIP); xf=xpx*(1-SLIP); g=(xf-ef)*qty
        cost=(compute_leg_cost(side="BUY",qty=qty,price=ef,is_swing=True,cfg=UPSTOX)
              +compute_leg_cost(side="SELL",qty=qty,price=xf,is_swing=True,cfg=UPSTOX))
        tr.append({"entry_d":ed,"exit_d":sy["d"][xi],"net":g-cost})
    return tr
def met(tr,lo=None,hi=None):
    t=[x for x in tr if (lo is None or x["exit_d"]>=lo) and (hi is None or x["entry_d"]<=hi)]
    if not t: return None
    byd={}
    for x in t: byd[x["exit_d"]]=byd.get(x["exit_d"],0.0)+x["net"]
    eq=CAPITAL; cur=[CAPITAL]
    for d in sorted(byd): eq+=byd[d]; cur.append(eq)
    p=-1e18; m=0.0
    for v in cur: p=max(p,v); m=min(m,v/p-1)
    y=(date.fromisoformat(t[-1]["exit_d"])-date.fromisoformat(t[0]["entry_d"])).days/365.25
    cg=((cur[-1]/CAPITAL)**(1/y)-1) if y>0 and cur[-1]>0 else 0.0
    return dict(n=len(t),net=sum(x["net"] for x in t),cagr=cg,mdd=m,calmar=(cg/abs(m)) if m else 0.0)

FILTERS = [
    ("BASELINE (no selection)", lambda c: True),
    ("drop negative run-up",    lambda c: c["ru"] >= 0.0),
    ("run-up >= 15%",           lambda c: c["ru"] >= 0.15),
    ("run-up 15-35% goldilocks",lambda c: 0.15 <= c["ru"] < 0.35),
    ("low-vol <1.5x (under-rx)",lambda c: c["vr"] < 1.5),
    ("gap-heavy (gapfrac>=0.6)",lambda c: c["gapfrac"] >= 0.6),
    ("big reaction >=15%",      lambda c: c["sp"] >= 0.15),
    ("QUALITY: ru>=0 & vr<1.5", lambda c: c["ru"] >= 0.0 and c["vr"] < 1.5),
    ("QUALITY: ru>=0 & gap>=.6",lambda c: c["ru"] >= 0.0 and c["gapfrac"] >= 0.6),
    ("QUALITY: ru15-35 & vr<1.5",lambda c: 0.15 <= c["ru"] < 0.35 and c["vr"] < 1.5),
]
b=run(FILTERS[0][1]); bf,bi,bo=met(b),met(b,hi="2020-12-31"),met(b,lo="2021-01-01")
print(f"pool={len(pool)}  (baseline qualified into portfolio below)\n")
print(f"{'filter':28} {'FULL n/CAGR/DD/Cal':30} {'IS CAGR/Cal':14} {'OOS CAGR/Cal':14}")
print("="*92)
for name,flt in FILTERS:
    tr=run(flt); f,i,o=met(tr),met(tr,hi="2020-12-31"),met(tr,lo="2021-01-01")
    if not f or not i or not o: print(f"{name:28} (insufficient)"); continue
    win="  WIN" if (f["calmar"]>bf["calmar"] and i["net"]>=bi["net"] and o["net"]>=bo["net"]) else ""
    imp="  +Cal" if f["calmar"]>bf["calmar"] else ""
    print(f"{name:28} n={f['n']:4} {f['cagr']*100:4.1f}%/{f['mdd']*100:5.1f}%/{f['calmar']:.2f}   "
          f"{i['cagr']*100:4.1f}%/{i['calmar']:.2f}    {o['cagr']*100:4.1f}%/{o['calmar']:.2f}{win or imp}")

# ── robustness: by-year net for the headline winners (one-year-carried check) ──
print("\n=== BY-YEAR NET (Rs'000) — is the edge broad or one-year-carried? ===")
def byyear(flt):
    tr = run(flt); yr = {}
    for x in tr: yr[x["exit_d"][:4]] = yr.get(x["exit_d"][:4], 0.0) + x["net"]
    return yr
for name, flt in [("baseline", lambda c: True),
                  ("drop-neg-runup", lambda c: c["ru"] >= 0.0),
                  ("run-up>=15%", lambda c: c["ru"] >= 0.15)]:
    yr = byyear(flt)
    pos = sum(1 for v in yr.values() if v > 0)
    print(f"{name:16} +yrs={pos}/{len(yr)}  " + " ".join(f"{y}:{v/1000:+.0f}" for y, v in sorted(yr.items())))
