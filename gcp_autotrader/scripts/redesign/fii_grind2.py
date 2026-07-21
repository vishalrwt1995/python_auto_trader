"""FII/DII composite — combine the OOS-robust survivors into ONE economically-coherent signal (fade
the crowd, follow FII futures) and test whether it (a) holds OOS as a combination, (b) converts to a
net-of-cost tradeable Nifty-timing edge, (c) works as an ADDITIVE regime overlay on Nifty>100DMA.

Composite (each component z20-standardized, signed by the screen's direction; higher=bullish):
  + FII.tnet_z20 + FII idx-fut flow (confirming)  −Pro.fnet_z20 −Client.fratio −DII.optbull −FII idx-opt cum5 (contrarian).
Honest guard vs multiple-testing: component directions come from economic logic + both-half sign
consistency, and the VERDICT rests on the composite's OOS number + net-of-cost timing result, not the
in-sample fit. No-look-ahead (signal EOD d -> act d+1). READ-ONLY, cached + scratchpad."""
import os, sys, json, statistics
C=os.path.expanduser("~/.autotrader_backtest_cache"); SCRATCH=sys.argv[1] if len(sys.argv)>1 else "."
IS_END="2020-12-31"; SWITCH_COST=0.0005   # ~0.05% round-trip on a Nifty ETF/future per regime switch

mkt=json.load(open(f"{C}/market_inputs_2015.json"))
md=sorted(x for x in mkt if mkt[x].get("nifty_close")); nc=[float(mkt[x]["nifty_close"]) for x in md]
nidx={d:i for i,d in enumerate(md)}
ma100=[None]*len(nc); run=0.0
for i in range(len(nc)):
    run+=nc[i]
    if i>=100: run-=nc[i-100]
    if i>=99: ma100[i]=run/100.0

poi=json.load(open(f"{SCRATCH}/poi_full.json"))
def Fnum(x):
    try: return float(x)
    except: return 0.0
byc={}
for r in poi:
    byc.setdefault(r["client_type"],{})[r["date"][:10]]={
        "fnet":Fnum(r["fut_idx_long"])-Fnum(r["fut_idx_short"]),
        "fratio":Fnum(r["fut_idx_long"])/(Fnum(r["fut_idx_long"])+Fnum(r["fut_idx_short"])+1),
        "optbull":(Fnum(r["opt_idx_call_long"])+Fnum(r["opt_idx_put_short"]))-(Fnum(r["opt_idx_call_short"])+Fnum(r["opt_idx_put_long"])),
        "tnet":Fnum(r["total_long"])-Fnum(r["total_short"])}
fd=json.load(open(f"{SCRATCH}/fii_deriv.json"))
idxfut={}; idxopt={}
for r in fd:
    net=Fnum(r["buy_amt_cr"])-Fnum(r["sell_amt_cr"]); d=r["date"][:10]
    if r["instrument"]=="INDEX FUTURES": idxfut[d]=net
    elif r["instrument"]=="INDEX OPTIONS": idxopt[d]=net

def zser(raw):   # {date:val} -> {date: z20 (vs trailing 20d excl today)}
    ds=sorted(raw); out={}
    for i,d in enumerate(ds):
        if i<20: continue
        w=[raw[ds[j]] for j in range(i-20,i)]; sd=statistics.pstdev(w) or 1.0
        out[d]=(raw[d]-statistics.mean(w))/sd
    return out
def cum(raw,n):
    ds=sorted(raw); out={}
    for i,d in enumerate(ds): out[d]=sum(raw[ds[j]] for j in range(max(0,i-n+1),i+1))
    return out

comp_parts={
    "+FII.tnet":    zser({d:byc["FII"][d]["tnet"] for d in byc["FII"]}),
    "+FIIfut_flow": zser(idxfut),
    "-Pro.fnet":    zser({d:-byc["Pro"][d]["fnet"] for d in byc["Pro"]}),
    "-Client.frat": zser({d:-byc["Client"][d]["fratio"] for d in byc["Client"]}),
    "-DII.optbull": zser({d:-byc["DII"][d]["optbull"] for d in byc["DII"]}),
    "-FIIopt_cum5": zser({d:-v for d,v in cum(idxopt,5).items()}),
}
dates=sorted(set.intersection(*[set(p) for p in comp_parts.values()]) & set(nidx))
comp={d:sum(comp_parts[k][d] for k in comp_parts) for d in dates}

def fwd20(d):
    i=nidx.get(d)
    if i is None or i+1+20>=len(nc) or nc[i+1]<=0: return None
    return nc[i+1+20]/nc[i+1]-1.0
def qspread(sub):
    ps=[(comp[d],fwd20(d)) for d in sub if fwd20(d) is not None]
    ps.sort(key=lambda x:x[0]); k=max(1,len(ps)//5)
    return statistics.mean([p[1] for p in ps[-k:]])-statistics.mean([p[1] for p in ps[:k]]), len(ps)
si,ni=qspread([d for d in dates if d<=IS_END]); so,no=qspread([d for d in dates if d>IS_END])
print(f"COMPOSITE quintile spread (fwd Nifty f20):  IS {si*100:+.2f}% (n={ni})   OOS {so*100:+.2f}% (n={no})", flush=True)
print(f"  {'ROBUST' if si*so>0 and min(abs(si),abs(so))>0.004 else 'WEAK/REVERSES'} — combination {'holds' if si*so>0 else 'fails'} OOS\n", flush=True)

# ---- (b) timing backtest: hold Nifty when composite>0 (act next day), else cash; net of switch cost ----
def timing(dset, thresh=0.0):
    ds=[d for d in dates if dset(d)]; ds.sort()
    eq=1.0; peak=1.0; mdd=0.0; prev_in=False; days_in=0; ret_bh=1.0
    for d in ds:
        i=nidx[d]
        if i+1>=len(nc): continue
        r=nc[i+1]/nc[i]-1.0 if nc[i]>0 else 0.0          # next-day Nifty return
        ret_bh*=(1+r)
        in_mkt = comp[d]>thresh
        if in_mkt:
            eq*=(1+r); days_in+=1
            if not prev_in: eq*=(1-SWITCH_COST)
        else:
            if prev_in: eq*=(1-SWITCH_COST)
        prev_in=in_mkt; peak=max(peak,eq); mdd=min(mdd,eq/peak-1)
    yrs=len(ds)/252 or 1
    return dict(cagr=eq**(1/yrs)-1, mdd=mdd, cal=(eq**(1/yrs)-1)/abs(mdd) if mdd<0 else 0,
                bh_cagr=ret_bh**(1/yrs)-1, pct_in=100*days_in/len(ds) if ds else 0, eq=eq)
print("=== (b) TIMING: long Nifty when composite>0 else cash (net of 0.05% switch) vs buy-hold ===", flush=True)
for lbl,dset in [("IS ",lambda d:d<=IS_END),("OOS",lambda d:d>IS_END)]:
    t=timing(dset)
    print(f"  {lbl}: strat CAGR {t['cagr']*100:+5.1f}% / DD {t['mdd']*100:5.1f}% / Cal {t['cal']:4.2f} | buy-hold {t['bh_cagr']*100:+5.1f}% | in-mkt {t['pct_in']:.0f}%", flush=True)

# ---- (c) overlay: within Nifty>100DMA, does composite>=0 beat composite<0 forward return? ----
def ov(sub,cond):
    v=[fwd20(d) for d in sub if cond(d) and fwd20(d) is not None]
    return (statistics.mean(v)*100, len(v)) if v else (0,0)
print("\n=== (c) OVERLAY: within Nifty>100DMA regime, composite bull vs bear (additive-gate test) ===", flush=True)
for lbl,lo,hi in [("IS ","0000",IS_END),("OOS",IS_END,"9999")]:
    up=lambda d: (nidx[d]<len(ma100) and ma100[nidx[d]] is not None and nc[nidx[d]]>ma100[nidx[d]])
    a,na=ov([d for d in dates if lo<d<=hi], lambda d: up(d) and comp[d]>=0)
    b,nb=ov([d for d in dates if lo<d<=hi], lambda d: up(d) and comp[d]<0)
    print(f"  {lbl}: Nifty-UP & comp>=0: f20 {a:+.2f}% (n={na})   |  Nifty-UP & comp<0: f20 {b:+.2f}% (n={nb})", flush=True)
print("\nREAD: verdict rests on (a) composite OOS spread + (b) OOS timing Calmar/CAGR vs buy-hold +", flush=True)
print("(c) OOS overlay separation. If OOS is weak/inconsistent across these, the survivors were multiple-testing noise.", flush=True)
