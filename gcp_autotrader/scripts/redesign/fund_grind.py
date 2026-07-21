"""Fundamentals factor grind (catalog #4, ★ long-shot) — completes the killed/long-shot sweep.
Data: fund_fin.json (annual income/balance/cashflow for 337 CURRENT survivors, captured 2026-06-28).
Two hard limits stated up front: (1) SURVIVORSHIP — only 337 companies that still exist/report today,
so any long-only result is inflated (failed/delisted names absent); the cross-sectional top-minus-bottom
SPREAD partly controls for it (both legs are survivors). (2) POINT-IN-TIME — all captured on one date,
so I assume FY-ending-Mar-YYYY results are AVAILABLE ~YYYY-07-01 (3-mo publication lag) to avoid
look-ahead. Test growth (revenue/profit YoY) + quality (margin, ROA) as cross-sectional factors:
forward 120d NET return, top-quintile vs bottom-quintile SPREAD, IS(<=2020)/OOS(>=2021). Survivor only
if the spread is same-sign + material in BOTH halves. READ-ONLY, single-process, cached+local."""
import os, json, pickle, statistics
from bisect import bisect_right
C=os.path.expanduser("~/.autotrader_backtest_cache")
S="/private/tmp/claude-501/-Users-apple-Projects-Migrated-Auto-Trading-Python-GCP/439e48e8-a413-4a1d-9d0a-530e53a5e277/scratchpad"
COST, IS_END, TURN_MIN, PRICE_MIN = 0.007, "2020-12-31", 10e7, 30.0

fin=json.load(open(f"{S}/fund_fin.json"))
i2s={r["isin"]:r["symbol"] for r in json.load(open(f"{S}/isin_symbol.json"))}
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

def fnum(x):
    try: return float(str(x).replace(",",""))
    except: return None
# build {isin: {year:int -> {cat:value}}}
by={}
for r in fin:
    p=str(r["period"]);
    if not p.startswith("Mar "): continue
    yr=int(p.split()[1]); v=fnum(r["value"])
    if v is None: continue
    by.setdefault(r["isin"],{}).setdefault(yr,{})[r["category"]]=v

# events: for each isin-year with prior year, factor values + available date (Jul 1 of that yr) + fwd ret
def fwd(sym, avail, H):
    Sd=SYM.get(sym)
    if not Sd: return None
    ref=bisect_right(Sd["d"], avail)
    if ref>=len(Sd["c"]) or ref<1: return None
    if Sd["turn"][ref] is None or Sd["turn"][ref]<TURN_MIN or Sd["c"][ref]<PRICE_MIN: return None
    if ref+H>=len(Sd["c"]) or Sd["c"][ref]<=0: return None
    return Sd["c"][ref+H]/Sd["c"][ref]-1.0-COST

FACTORS=["rev_growth","profit_growth","margin","roa"]
H=120
events=[]
for isin,yrs in by.items():
    sym=i2s.get(isin)
    if not sym: continue
    for y in sorted(yrs):
        cur,prev=yrs.get(y),yrs.get(y-1)
        if not cur or not prev: continue
        rev,pr=cur.get("revenue"),cur.get("net_profit")
        rev0,pr0=prev.get("revenue"),prev.get("net_profit")
        op,ta=cur.get("operating_profit"),cur.get("total_asset")
        f={}
        if rev and rev0 and rev0>0: f["rev_growth"]=rev/rev0-1
        if pr is not None and pr0 not in (None,0) and pr0>0: f["profit_growth"]=pr/pr0-1
        if op is not None and rev and rev>0: f["margin"]=op/rev
        if pr is not None and ta and ta>0: f["roa"]=pr/ta
        if not f: continue
        avail=f"{y}-07-01"
        r120=fwd(sym,avail,H)
        if r120 is None: continue
        events.append({"y":y,"avail":avail,**f,"r":r120})
print(f"{len(events)} isin-year events (337 survivors, fwd{H}d net), survivorship-INFLATED\n", flush=True)

def spread(fac, lo, hi):
    # cross-sectional per-year quintile top-minus-bottom, averaged over years in [lo,hi]
    tops, bots = [], []
    for y in range(2014,2027):
        pool=[e for e in events if e["y"]==y and fac in e and lo<=f"{y}-07-01"<=hi]
        pool=[e for e in pool if fac in e]
        if len(pool)<15: continue
        pool.sort(key=lambda e:e[fac]); k=max(1,len(pool)//5)
        bots+=[e["r"] for e in pool[:k]]; tops+=[e["r"] for e in pool[-k:]]
    if len(tops)<20 or len(bots)<20: return None
    return statistics.mean(tops)-statistics.mean(bots), statistics.mean(tops), statistics.mean(bots), len(tops)

print("=== cross-sectional factor: top-quintile MINUS bottom-quintile fwd120 NET, IS vs OOS ===", flush=True)
print(f"  {'factor':14}{'IS spread':>11}{'OOS spread':>12}  {'IS top/bot':>16}{'OOS top/bot':>18}  robust?", flush=True)
for fac in FACTORS:
    si=spread(fac,"0000",IS_END); so=spread(fac,IS_END,"9999")
    if not si or not so:
        print(f"  {fac:14}   thin/na"); continue
    robust = si[0]*so[0]>0 and min(abs(si[0]),abs(so[0]))>0.01
    print(f"  {fac:14}{si[0]*100:>+10.2f}%{so[0]*100:>+11.2f}%  {si[1]*100:>+7.1f}/{si[2]*100:<7.1f}{so[1]*100:>+8.1f}/{so[2]*100:<8.1f}  {'<<SURVIVOR' if robust else '(no)'}", flush=True)
print("\nNOTE: survivorship-inflated (337 current survivors only) — even a robust spread is an UPPER bound;", flush=True)
print("factors are also the most-arbitraged signals. fund_ratios + fund_hold = single snapshot (2026-06-28) => un-backtestable.", flush=True)
