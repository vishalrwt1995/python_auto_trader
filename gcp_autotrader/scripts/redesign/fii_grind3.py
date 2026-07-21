"""FII/DII BEST-CONFIG search — the composite is a real but modest signal; find the config that best
converts it. (1) cash-threshold sweep: go to cash only when composite < percentile P (avoid only the
worst days, keep more beta); (2) proportional exposure-tilt: scale Nifty exposure by composite z
(0..1.5x, no full sit-out). Metric: OOS CAGR/DD/Calmar vs buy-hold, IS shown for robustness. If NO
config beats buy-hold on BOTH return and Calmar robustly, the signal is real-but-too-weak to trade
standalone. READ-ONLY, cached + scratchpad."""
import os, sys, json, statistics
C=os.path.expanduser("~/.autotrader_backtest_cache"); SCRATCH=sys.argv[1] if len(sys.argv)>1 else "."
IS_END="2020-12-31"; SW=0.0005
mkt=json.load(open(f"{C}/market_inputs_2015.json"))
md=sorted(x for x in mkt if mkt[x].get("nifty_close")); nc=[float(mkt[x]["nifty_close"]) for x in md]; nidx={d:i for i,d in enumerate(md)}
poi=json.load(open(f"{SCRATCH}/poi_full.json"))
def Fn(x):
    try:return float(x)
    except:return 0.0
byc={}
for r in poi:
    byc.setdefault(r["client_type"],{})[r["date"][:10]]={"fnet":Fn(r["fut_idx_long"])-Fn(r["fut_idx_short"]),
        "fratio":Fn(r["fut_idx_long"])/(Fn(r["fut_idx_long"])+Fn(r["fut_idx_short"])+1),
        "optbull":(Fn(r["opt_idx_call_long"])+Fn(r["opt_idx_put_short"]))-(Fn(r["opt_idx_call_short"])+Fn(r["opt_idx_put_long"])),
        "tnet":Fn(r["total_long"])-Fn(r["total_short"])}
fd=json.load(open(f"{SCRATCH}/fii_deriv.json")); idxfut={}; idxopt={}
for r in fd:
    net=Fn(r["buy_amt_cr"])-Fn(r["sell_amt_cr"]); d=r["date"][:10]
    if r["instrument"]=="INDEX FUTURES": idxfut[d]=net
    elif r["instrument"]=="INDEX OPTIONS": idxopt[d]=net
def zser(raw):
    ds=sorted(raw); out={}
    for i,d in enumerate(ds):
        if i<20: continue
        w=[raw[ds[j]] for j in range(i-20,i)]; sd=statistics.pstdev(w) or 1.0; out[d]=(raw[d]-statistics.mean(w))/sd
    return out
def cum(raw,n):
    ds=sorted(raw); return {d:sum(raw[ds[j]] for j in range(max(0,i-n+1),i+1)) for i,d in enumerate(ds)}
parts={"a":zser({d:byc["FII"][d]["tnet"] for d in byc["FII"]}),"b":zser(idxfut),
       "c":zser({d:-byc["Pro"][d]["fnet"] for d in byc["Pro"]}),"d":zser({d:-byc["Client"][d]["fratio"] for d in byc["Client"]}),
       "e":zser({d:-byc["DII"][d]["optbull"] for d in byc["DII"]}),"f":zser({d:-v for d,v in cum(idxopt,5).items()})}
dates=sorted(set.intersection(*[set(p) for p in parts.values()]) & set(nidx))
comp={d:sum(parts[k][d] for k in parts) for d in dates}
# percentile ranks of comp within each half (for threshold sweep)
def pct_ranks(sub):
    vals=sorted(comp[d] for d in sub); import bisect
    return {d: 100*bisect.bisect_left(vals,comp[d])/len(vals) for d in sub}
IS=[d for d in dates if d<=IS_END]; OOS=[d for d in dates if d>IS_END]
prk={**pct_ranks(IS),**pct_ranks(OOS)}
def run(sub, mode, P=0.0, k=0.0):
    ds=sorted(sub); eq=1.0;peak=1.0;mdd=0.0;prev=1.0;din=0;bh=1.0
    for d in ds:
        i=nidx[d]
        if i+1>=len(nc) or nc[i]<=0: continue
        r=nc[i+1]/nc[i]-1.0; bh*=(1+r)
        if mode=="thresh": expo=0.0 if prk[d]<P else 1.0
        else: expo=max(0.0,min(1.5,0.75+k*comp[d]))     # tilt
        eq*=(1+expo*r);
        if expo>0: din+=1
        eq*=(1-SW*abs(expo-prev)); prev=expo; peak=max(peak,eq); mdd=min(mdd,eq/peak-1)
    y=len(ds)/252 or 1
    return dict(cagr=eq**(1/y)-1,mdd=mdd,cal=(eq**(1/y)-1)/abs(mdd) if mdd<0 else 0,bh=bh**(1/y)-1,pin=100*din/len(ds) if ds else 0)
bh_is=run(IS,"thresh",P=-1)["bh"]; bh_oos=run(OOS,"thresh",P=-1)["bh"]
print(f"buy-hold Nifty: IS {bh_is*100:+.1f}% CAGR / OOS {bh_oos*100:+.1f}% CAGR\n", flush=True)
print("=== cash-threshold sweep (cash when composite < Pth percentile; else 100% long) ===", flush=True)
print(f"  {'P':>4}{'IS CAGR':>9}{'IS Cal':>8}{'OOS CAGR':>10}{'OOS DD':>8}{'OOS Cal':>9}{'in-mkt':>8}", flush=True)
for P in (0,10,20,30,40,50):
    i=run(IS,"thresh",P=P); o=run(OOS,"thresh",P=P)
    beat=" <<" if o["cagr"]>bh_oos and o["cal"]>0.45 else ""
    print(f"  {P:>4}{i['cagr']*100:>+8.1f}%{i['cal']:>8.2f}{o['cagr']*100:>+9.1f}%{o['mdd']*100:>7.1f}%{o['cal']:>9.2f}{o['pin']:>7.0f}%{beat}", flush=True)
print("\n=== proportional exposure tilt (0.75 + k*comp_z, clipped 0..1.5x) ===", flush=True)
print(f"  {'k':>5}{'IS CAGR':>9}{'IS Cal':>8}{'OOS CAGR':>10}{'OOS DD':>8}{'OOS Cal':>9}", flush=True)
for k in (0.05,0.1,0.15,0.2):
    i=run(IS,"tilt",k=k); o=run(OOS,"tilt",k=k)
    beat=" <<" if o["cagr"]>bh_oos and o["cal"]>0.45 else ""
    print(f"  {k:>5}{i['cagr']*100:>+8.1f}%{i['cal']:>8.2f}{o['cagr']*100:>+9.1f}%{o['mdd']*100:>7.1f}%{o['cal']:>9.2f}{beat}", flush=True)
print("\nREAD: '<<' = beats buy-hold OOS CAGR AND Cal>0.45. If none, signal is real-but-too-weak to trade standalone.", flush=True)
