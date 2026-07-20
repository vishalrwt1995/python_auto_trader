"""Insider channel GOD-MODE harness -- enriches every insider-buy signal with its features
(category, cluster, value, holding-delta, first-buy, prior-return) + the replicated brain regime
AT ENTRY (regime label, tactical/trend/breadth/vol-stress scores, b200), caches them, and offers
a flexible walk(cfg) that reports FULL + IS(<=2020) + OOS(>=2021) CAGR/DD/Calmar separately so
every sweep is overfit-guarded (require both halves healthy, prefer plateaus). PHASE 1 here:
regime-overlay comparison (the requested DD lever) -- every gate head-to-head at the v2 base.
Survivorship-safe. READ-ONLY, single-process, cached only (zero GCP cost)."""
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
UPSTOX=CostConfig.upstox()
CAP0,SLIP,IS_END=200_000.0,0.001,"2020-12-31"
ENRICH=os.path.join(C,"insider_cands_enriched.pkl")

def atr14(h,l,c):
    tr=[h[0]-l[0]]
    for i in range(1,len(c)): tr.append(max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1])))
    out=[None]*len(c); s=0.0
    for i in range(len(tr)):
        s+=tr[i]
        if i>=14: s-=tr[i-14]
        if i>=13: out[i]=s/14.0
    return out

def build():
    print("building enriched candidates ...", flush=True)
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
        SYM[s]={"d":d,"o":o,"h":h,"l":l,"c":c,"atr":atr14(h,l,c),"turn":turn}
    rf=json.load(open(f"{C}/regime_faithful_2015.json")); rd=sorted(rf.keys())
    b200=pickle.load(open(f"{C}/swing_b200_history.pkl","rb")); bdd=sorted(b200.keys())
    sect_raw=json.load(open(f"{C}/sector_map.json")); SEC={}
    for _v2 in sect_raw.values():
        if isinstance(_v2,dict) and _v2.get("sym") and _v2.get("sector"):
            SEC[str(_v2["sym"]).strip().upper()]=_v2["sector"]
    recs=[]
    for fn in sorted(glob.glob(os.path.join(PIT,"*.json"))):
        try: recs.extend(json.load(open(fn)))
        except Exception: pass
    INFORMED=("promoter","director","key managerial","immediate relative","promoter group")
    def fnum(x):
        try: return float(str(x).replace(",",""))
        except Exception: return None
    def dd_of(r):
        s=str(r.get("date","")).split()[0]
        try: return datetime.strptime(s,"%d-%b-%Y").strftime("%Y-%m-%d")
        except Exception: return None
    agg=defaultdict(lambda:{"val":0.0,"n":0,"cats":set(),"dpct":0.0})
    per_sym_dates=defaultdict(list)
    for r in recs:
        if "buy" not in str(r.get("tdpTransactionType","")).lower(): continue
        if not any(k in str(r.get("personCategory","")).lower() for k in INFORMED): continue
        mode=str(r.get("acqMode","")).lower()
        if "market" not in mode or "off" in mode: continue
        val=fnum(r.get("secVal")) or fnum(r.get("buyValue")) or 0.0
        if val<500000: continue
        dd=dd_of(r)
        if dd is None or r.get("symbol") not in SYM: continue
        a=agg[(r["symbol"],dd)]; a["val"]+=val; a["n"]+=1; a["cats"].add(str(r.get("personCategory")).lower())
        a["dpct"]=max(a["dpct"],(fnum(r.get("afterAcqSharesPer")) or 0)-(fnum(r.get("befAcqSharesPer")) or 0))
        per_sym_dates[r["symbol"]].append(dd)
    for s in per_sym_dates: per_sym_dates[s]=sorted(set(per_sym_dates[s]))
    def rlook(d):
        i=bisect_right(rd,d)-1
        return rf[rd[i]] if i>=0 else {}
    def blook(d):
        i=bisect_right(bdd,d)-1
        return b200[bdd[i]] if i>=0 else 0.0
    cands=[]
    for (sym,dd),a in agg.items():
        S=SYM[sym]; ref=bisect_right(S["d"],dd)
        if ref>=len(S["c"]) or ref<1 or S["atr"][ref-1] is None or S["atr"][ref-1]<=0: continue
        if S["o"][ref]<30 or S["turn"][ref] is None: continue
        ed=S["d"][ref]; rg=rlook(ed)
        # first-buy: no prior informed buy for this symbol in the 90 days before dd
        dts=per_sym_dates[sym]; dcur=datetime.strptime(dd,"%Y-%m-%d")
        prior=[x for x in dts if x<dd and (dcur-datetime.strptime(x,"%Y-%m-%d")).days<=90]
        cat=("promoter" if any("promoter" in c for c in a["cats"]) else
             "director" if any("director" in c for c in a["cats"]) else "kmp/rel")
        pr20=(S["c"][ref-1]/S["c"][ref-21]-1.0) if ref>=22 and S["c"][ref-21]>0 else 0.0
        cands.append({"ed":ed,"sym":sym,"ref":ref,"sl":2.5*S["atr"][ref-1],"turn":S["turn"][ref],
                      "cat":cat,"n":a["n"],"val":a["val"],"dpct":a["dpct"],"first":len(prior)==0,
                      "pr20":pr20,"regime":rg.get("regime","?"),"tact":rg.get("tactical_trend_score",50.0),
                      "trend":rg.get("trend_score",50.0),"vstress":rg.get("volatility_stress_score",50.0),
                      "b200":blook(ed),"sec":SEC.get(sym.upper(),"?")})
    cands.sort(key=lambda x:x["ed"])
    pickle.dump({"SYM":SYM,"cands":cands}, open(ENRICH,"wb"))
    print(f"  cached {len(cands):,} enriched candidates -> {ENRICH}\n", flush=True)
    return SYM, cands

if os.path.exists(ENRICH) and "--rebuild" not in sys.argv:
    obj=pickle.load(open(ENRICH,"rb")); SYM,cands=obj["SYM"],obj["cands"]
    print(f"loaded {len(cands):,} enriched candidates from cache\n", flush=True)
else:
    SYM,cands=build()

print("distinct regime labels:", dict(sorted(((r,sum(1 for c in cands if c['regime']==r)) for r in set(c['regime'] for c in cands)), key=lambda x:-x[1])), flush=True)
print(flush=True)

def seg(points, start_eq, years):
    if not points or start_eq<=0: return 0.0,0.0
    peak=start_eq; mdd=0.0; last=start_eq
    for _,eq in points:
        peak=max(peak,eq); mdd=min(mdd,eq/peak-1.0); last=eq
    cagr=(last/start_eq)**(1/years)-1 if years>0 else 0.0
    return cagr, mdd

def walk(cfg):
    hold=cfg["hold"]; slots=cfg["slots"]; turn_min=cfg.get("turn_min",10e7)
    stop_mult=cfg.get("stop_mult",2.5); risk_pct=cfg.get("risk_pct",0.015)
    sect_cap=cfg.get("sect_cap",0); rfil=cfg.get("regime"); sfil=cfg.get("select")
    dd_halt=cfg.get("dd_halt",0.0)
    equity=CAP0; free=[""]*slots; ssec=[None]*slots; openpos=[]; closed=[]; peak_live=CAP0
    for c in cands:
        if c["turn"]<turn_min: continue
        if rfil and not rfil(c): continue
        if sfil and not sfil(c): continue
        ed=c["ed"]
        still=[]
        for xd,pnl,sl in openpos:
            if xd<=ed: equity+=pnl; peak_live=max(peak_live,equity); closed.append((xd,pnl))
            else: still.append((xd,pnl,sl))
        openpos=still
        for k in range(slots):
            if free[k] and free[k]<=ed: free[k]=""; ssec[k]=None
        if dd_halt>0 and equity < (1-dd_halt)*peak_live: continue     # portfolio DD governor
        if sect_cap and c["sec"]!="?" and sum(1 for k in range(slots) if ssec[k]==c["sec"])>=sect_cap: continue
        slot=next((k for k in range(slots) if not free[k]),None)
        if slot is None: continue
        S=SYM[c["sym"]]; ref=c["ref"]; entry_px=S["o"][ref]
        if entry_px<=0: continue
        sl_dist=c["sl"]
        qty=int((risk_pct*equity)//sl_dist)
        if qty<1: continue
        if qty*entry_px>equity/slots: qty=int((equity/slots)//entry_px)
        if qty<1: continue
        xi=min(ref+hold,len(S["c"])-1); exit_px=S["c"][xi]
        if stop_mult:
            stop=entry_px-(stop_mult/2.5)*sl_dist   # sl already 2.5xATR; scale to stop_mult
            for k in range(ref+1,xi+1):
                if S["l"][k]<=stop: exit_px=stop; xi=k; break
        xd=S["d"][xi]; ef=entry_px*(1+SLIP); xf=exit_px*(1-SLIP)
        pnl=(xf-ef)*qty-(compute_leg_cost(side="BUY",qty=qty,price=ef,is_swing=True,cfg=UPSTOX)
                         +compute_leg_cost(side="SELL",qty=qty,price=xf,is_swing=True,cfg=UPSTOX))
        free[slot]=xd; ssec[slot]=c["sec"]; openpos.append((xd,pnl,slot))
    for xd,pnl,sl in openpos: equity+=pnl; closed.append((xd,pnl))
    if len(closed)<10: return None
    closed.sort()
    curve=[]; eq=CAP0
    for xd,pnl in closed: eq+=pnl; curve.append((xd,eq))
    y0=int(closed[0][0][:4]); y1=int(closed[-1][0][:4])
    is_pts=[p for p in curve if p[0]<=IS_END]; oos_pts=[p for p in curve if p[0]>IS_END]
    eq_is=is_pts[-1][1] if is_pts else CAP0
    fc,fdd=seg(curve,CAP0,y1-y0+1)
    ic,idd=seg(is_pts,CAP0,max(1,2020-y0+1))
    oc,odd=seg(oos_pts,eq_is,max(1,y1-2021+1))
    wr=100*sum(1 for _,p in closed if p>0)/len(closed)
    yr=defaultdict(float)
    for xd,pnl in closed: yr[xd[:4]]+=pnl
    return {"cagr":fc,"dd":fdd,"cal":fc/abs(fdd) if fdd<0 else 0,"n":len(closed),
            "wr":wr,"is_cagr":ic,"is_dd":idd,"is_cal":ic/abs(idd) if idd<0 else 0,
            "oos_cagr":oc,"oos_dd":odd,"oos_cal":oc/abs(odd) if odd<0 else 0,
            "worst_yr":min(yr.values()),"yr":dict(yr)}

def show(tag,r):
    if not r: print(f"  {tag:34} -- <10 trades"); return
    print(f"  {tag:34} CAGR{r['cagr']*100:+6.1f}% DD{r['dd']*100:6.1f}% Cal{r['cal']:5.2f} | "
          f"IS Cal{r['is_cal']:5.2f}({r['is_cagr']*100:+5.1f}%) OOS Cal{r['oos_cal']:5.2f}({r['oos_cagr']*100:+5.1f}%) "
          f"| n{r['n']:>4} WR{r['wr']:4.0f}% wYr{r['worst_yr']/1000:+.0f}k", flush=True)

# ---- Nifty MA gates need nifty series ----
mkt=json.load(open(f"{C}/market_inputs_2015.json"))
md=sorted(x for x in mkt if mkt[x].get("nifty_close")); nc=[float(mkt[x]["nifty_close"]) for x in md]
def _ma(n):
    out=[None]*len(nc); run=0.0
    for i in range(len(nc)):
        run+=nc[i]
        if i>=n: run-=nc[i-n]
        if i>=n-1: out[i]=run/n
    return out
MA={n:_ma(n) for n in (50,100,200)}
def nifty_gate(n):
    def f(c):
        i=bisect_left(md,c["ed"])-1
        if i<0 or MA[n][i] is None: return True
        return nc[i]>MA[n][i]
    return f

BASE=dict(hold=60,slots=8,turn_min=10e7,stop_mult=2.5,risk_pct=0.015)
print("=== PHASE 1: REGIME OVERLAY COMPARISON (base: turn>10cr h60 s8) ===\n", flush=True)
show("none (no regime gate)", walk({**BASE,"regime":None}))
for n in (50,100,200): show(f"Nifty>{n}DMA", walk({**BASE,"regime":nifty_gate(n)}))
show("brain: regime==TREND_UP", walk({**BASE,"regime":lambda c:c["regime"]=="TREND_UP"}))
show("brain: TREND_UP or RANGE", walk({**BASE,"regime":lambda c:c["regime"] in ("TREND_UP","RANGE")}))
show("brain: not CHOP/BEAR/HIGHVOL", walk({**BASE,"regime":lambda c:c["regime"] not in ("CHOP","BEAR","HIGH_VOL","RISK_OFF","DOWN")}))
for t in (40,50,60): show(f"brain: tactical>{t}", walk({**BASE,"regime":lambda c,t=t:c["tact"]>t}))
for t in (50,60,70): show(f"brain: trend_score>{t}", walk({**BASE,"regime":lambda c,t=t:c["trend"]>t}))
for t in (40,50,60): show(f"brain: vol_stress<{t}", walk({**BASE,"regime":lambda c,t=t:c["vstress"]<t}))
for t in (40,50,60): show(f"breadth: b200>{t}", walk({**BASE,"regime":lambda c,t=t:c["b200"]>t}))
# promising combos
show("Nifty>100DMA & tactical>50", walk({**BASE,"regime":lambda c:nifty_gate(100)(c) and c["tact"]>50}))
show("TREND_UP/RANGE & tactical>50", walk({**BASE,"regime":lambda c:c["regime"] in ("TREND_UP","RANGE") and c["tact"]>50}))
print("\nREAD: pick the regime gate with best OOS Calmar AND healthy IS Calmar (both halves), not", flush=True)
print("just top full-sample. Carry it to Phase 2 (selection).", flush=True)
