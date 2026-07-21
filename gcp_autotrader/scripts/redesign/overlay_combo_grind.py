"""Grind the two genuinely-unexplored hypotheses, CHEAPLY (one single-process script, cached data):
(1) OVERLAYS — does adding a confirmer (px>200DMA / OI-long-buildup / no-heavy-short / bulk-block-buy
    near the event) to the LIVE insider-buy + pledge-revoke event sets improve forward return, robustly
    IS/OOS, without over-thinning?
(2) COMBINATIONS — do conjunctions of COMMON signals (px>200DMA ∩ OI-long-buildup ∩ macro-gate ∩
    no-heavy-short) on the F&O universe beat baseline forward return, IS/OOS?
Signals from cached/local: insider_pit (buys + pledge-revokes), short_selling.json, fo_futstk_oi.json,
bulk/block deals, pead_full_bars_2014.pkl (survivorship-safe), market_inputs + b200. Entry = next
trading day (no look-ahead). Forward NET return (−0.7% cost). IS(<=2020)/OOS(>=2021). READ-ONLY, cached."""
import os, json, glob, pickle, statistics
from bisect import bisect_right, bisect_left
from datetime import datetime, timedelta
C=os.path.expanduser("~/.autotrader_backtest_cache"); PIT=os.path.join(C,"insider_pit")
S="/private/tmp/claude-501/-Users-apple-Projects-Migrated-Auto-Trading-Python-GCP/439e48e8-a413-4a1d-9d0a-530e53a5e277/scratchpad"
COST, IS_END, TURN_MIN, PRICE_MIN = 0.007, "2020-12-31", 10e7, 30.0

print("loading bars + signals ...", flush=True)
bars=pickle.load(open(f"{C}/pead_full_bars_2014.pkl","rb"))
SYM={}
for s,b in bars.items():
    if len(b)<210: continue
    d=[x[0] for x in b]; c=[x[4] for x in b]; v=[x[5] for x in b]
    turn=[None]*len(c); run=0.0; sma=[None]*len(c); sr=0.0
    for i in range(len(c)):
        if i>=1: run+=c[i-1]*v[i-1]
        if i>=21: run-=c[i-21]*v[i-21]
        if i>=21: turn[i]=run/20.0
        sr+=c[i]
        if i>=200: sr-=c[i-200]
        if i>=199: sma[i]=sr/200.0
    SYM[s]={"d":d,"c":c,"turn":turn,"sma":sma,"idx":{dd:i for i,dd in enumerate(d)}}

def dd_of(r):
    s=str(r.get("date","")).split()[0] if r.get("date") else ""
    try: return datetime.strptime(s,"%d-%b-%Y").strftime("%Y-%m-%d")
    except: return None
recs=[]
for fn in sorted(glob.glob(os.path.join(PIT,"*.json"))):
    try: recs.extend(json.load(open(fn)))
    except: pass
def entry_after(sym, dd):
    Sd=SYM.get(sym)
    if not Sd or not dd: return None
    ref=bisect_right(Sd["d"], dd)
    return ref if 1<=ref<len(Sd["c"]) else None
INF=("promoter","director","key managerial","managerial","immediate relative","relative")
buys={}; revokes={}
for r in recs:
    sym=str(r.get("symbol") or "").strip().upper(); t=str(r.get("tdpTransactionType","")).lower()
    cat=str(r.get("personCategory","")).lower(); m=str(r.get("acqMode","")).lower(); dd=dd_of(r)
    if "buy" in t and any(k in cat for k in INF) and "market" in m and "off" not in m:
        e=entry_after(sym,dd);
        if e is not None: buys.setdefault(sym,set()).add(e)
    if "revoke" in t and "promoter" in cat:
        e=entry_after(sym,dd)
        if e is not None: revokes.setdefault(sym,set()).add(e)

# short-selling: per symbol {barindex: heavy_short bool}
short=json.load(open(f"{S}/short_selling.json"))
sq={}
for r in short:
    sym=r["symbol"]; Sd=SYM.get(sym)
    if not Sd: continue
    i=Sd["idx"].get(r["date"][:10])
    if i is not None: sq.setdefault(sym,{})[i]=float(r["qty"])
heavy={}
for sym,dq in sq.items():
    idxs=sorted(dq); heavy[sym]=set()
    for j,i in enumerate(idxs):
        if j>=5:
            m=statistics.mean([dq[idxs[k]] for k in range(max(0,j-20),j)])
            if m>0 and dq[i]>2*m: heavy[sym].add(i)
def heavy_short_near(sym, ref):
    h=heavy.get(sym)
    return bool(h and any((ref-x) in range(0,6) for x in h if abs(ref-x)<=5)) if h else False

# OI long-buildup: per symbol {barindex: bool}  (doi>0 AND price up 3d)
oi=json.load(open(f"{S}/fo_futstk_oi.json"))
oibuild={}
for r in oi:
    sym=r["symbol"]; Sd=SYM.get(sym)
    if not Sd: continue
    i=Sd["idx"].get(r["date"][:10])
    if i is None or i<3: continue
    try: doi=float(r["doi"])
    except: continue
    if doi>0 and Sd["c"][i]>Sd["c"][i-3]: oibuild.setdefault(sym,set()).add(i)
def oi_buildup_near(sym, ref):
    o=oibuild.get(sym)
    return bool(o and any((ref-x) in range(0,6) for x in o)) if o else False

# bulk+block BUY dates per symbol -> bar indices
dealbuy={}
for fn in ("bulk_deals.json","block_deals.json"):
    for r in json.load(open(f"{S}/{fn}")):
        if str(r.get("buy_sell","")).upper()!="BUY": continue
        sym=r["symbol"]; Sd=SYM.get(sym)
        if not Sd: continue
        i=Sd["idx"].get(r["date"][:10])
        if i is not None: dealbuy.setdefault(sym,set()).add(i)
def deal_buy_near(sym, ref):
    dset=dealbuy.get(sym)
    return bool(dset and any((ref-x) in range(0,11) for x in dset)) if dset else False

# macro gate (b200>50 & Nifty>100DMA)
b200h=pickle.load(open(f"{C}/swing_b200_history.pkl","rb")); bdd=sorted(b200h.keys())
mkt=json.load(open(f"{C}/market_inputs_2015.json")); md=sorted(x for x in mkt if mkt[x].get("nifty_close")); nc=[float(mkt[x]["nifty_close"]) for x in md]
nma=[None]*len(nc); run=0.0
for i in range(len(nc)):
    run+=nc[i]
    if i>=100: run-=nc[i-100]
    if i>=99: nma[i]=run/100.0
def macro_ok(dt):
    bi=bisect_right(bdd,dt)-1; b=b200h[bdd[bi]] if bi>=0 else 0
    ni=bisect_left(md,dt)-1; nok=ni<0 or nma[ni] is None or nc[ni]>nma[ni]
    return b>50 and nok

def fwd(sym, ref, k=60):
    Sd=SYM[sym]
    return (Sd["c"][ref+k]/Sd["c"][ref]-1.0-COST) if ref+k<len(Sd["c"]) and Sd["c"][ref]>0 else None
def fillable(sym, ref):
    Sd=SYM[sym]
    return Sd["turn"][ref] is not None and Sd["turn"][ref]>=TURN_MIN and Sd["c"][ref]>=PRICE_MIN

def stat(vals):
    v=[x for x in vals if x is not None]
    return f"avg={statistics.mean(v)*100:+5.2f}% med={statistics.median(v)*100:+5.2f}% WR={100*sum(1 for x in v if x>0)/len(v):4.0f}% n={len(v)}" if v else "n/a"

print(f"  insider-buy symbols {len(buys)} | pledge-revoke symbols {len(revokes)}\n", flush=True)
print("="*70, flush=True)
print("(1) OVERLAY: does a confirmer improve the insider-buy / pledge-revoke events? (fwd60 NET)", flush=True)
print("="*70, flush=True)
for name,evset in [("INSIDER-BUY",buys),("PLEDGE-REVOKE",revokes)]:
    evs=[]
    for sym,refs in evset.items():
        for ref in refs:
            if not fillable(sym,ref): continue
            dt=SYM[sym]["d"][ref]; f=fwd(sym,ref,60)
            if f is None: continue
            evs.append({"dt":dt,"f":f,
                "above200":SYM[sym]["sma"][ref] is not None and SYM[sym]["c"][ref]>SYM[sym]["sma"][ref],
                "oi":oi_buildup_near(sym,ref),"noheavy":not heavy_short_near(sym,ref),"deal":deal_buy_near(sym,ref)})
    def half(lo,hi): return [e for e in evs if lo<e["dt"]<=hi]
    print(f"\n{name}  (fillable events: {len(evs)})", flush=True)
    for lbl,lo,hi in [("IS ","0000",IS_END),("OOS",IS_END,"9999")]:
        H=half(lo,hi)
        print(f"  {lbl} base(all):            {stat([e['f'] for e in H])}", flush=True)
        for conf in ("above200","oi","noheavy","deal"):
            wi=[e['f'] for e in H if e[conf]]; wo=[e['f'] for e in H if not e[conf]]
            print(f"       +{conf:9} {stat(wi)}   |  without: {stat(wo)}", flush=True)

print("\n"+"="*70, flush=True)
print("(2) COMBINATION: common-signal conjunction on F&O universe (fwd20 NET, base f20 IS-0.10/OOS+0.56)", flush=True)
print("="*70, flush=True)
fno=set(oibuild)
combo=[]
for sym in fno:
    Sd=SYM.get(sym)
    if not Sd: continue
    for i in range(210,len(Sd["c"])-20):
        if not fillable(sym,i): continue
        dt=Sd["d"][i]
        a=Sd["sma"][i] is not None and Sd["c"][i]>Sd["sma"][i]
        o=i in oibuild.get(sym,())
        nh=not heavy_short_near(sym,i)
        mg=macro_ok(dt)
        f=fwd(sym,i,20)
        if f is None: continue
        combo.append((dt,a,o,nh,mg,f))
def cstat(pred,lo,hi): return stat([f for (dt,a,o,nh,mg,f) in combo if lo<dt<=hi and pred(a,o,nh,mg)])
for name,pred in [("OI-buildup alone",lambda a,o,nh,mg:o),
                  ("OI ∩ px>200DMA",lambda a,o,nh,mg:o and a),
                  ("OI ∩ px>200 ∩ macro",lambda a,o,nh,mg:o and a and mg),
                  ("OI ∩ px>200 ∩ macro ∩ no-heavy-short",lambda a,o,nh,mg:o and a and mg and nh)]:
    print(f"  {name:42} IS {cstat(pred,'0000',IS_END)}", flush=True)
    print(f"  {'':42} OOS {cstat(pred,IS_END,'9999')}", flush=True)
print("\nREAD: overlay wins if a confirmer's 'with' subset robustly beats 'without' + the base, both halves,", flush=True)
print("without over-thinning. combination wins if a conjunction beats the f20 baseline both halves.", flush=True)
