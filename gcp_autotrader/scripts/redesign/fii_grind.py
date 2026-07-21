"""FII/DII HARD grind — throw every reasonable signal construction at forward Nifty and find any that
is OOS-robust. Positioning (poi: FII/DII/Pro/Client index-fut net / ratio / z20 / 20d-trend / daily-
delta / net-bullish-options / total-net) + FLOW (fii_deriv: index-fut & index-opt net buy Rs, levels
+ 5/10d cumulatives) + smart-minus-dumb divergence + retail contrarian. For each signal: forward-Nifty
f20 quintile spread (top20% minus bottom20% of the signal) computed SEPARATELY in IS(<=2020) and
OOS(>=2021). A signal is a SURVIVOR only if the spread has the SAME SIGN in both halves AND both
|spread|>=0.4%/20d (a consistent directional predictor, not an IS mirage that reverses). No-look-ahead:
signal EOD d -> forward Nifty from d+1. READ-ONLY, cached + scratchpad pulls (zero further GCP cost)."""
import os, sys, json, statistics
C=os.path.expanduser("~/.autotrader_backtest_cache")
SCRATCH=sys.argv[1] if len(sys.argv)>1 else "."
IS_END="2020-12-31"; MIN_SPREAD=0.004

# ---- load Nifty + forward returns ----
mkt=json.load(open(f"{C}/market_inputs_2015.json"))
md=sorted(x for x in mkt if mkt[x].get("nifty_close")); nc=[float(mkt[x]["nifty_close"]) for x in md]
nidx={d:i for i,d in enumerate(md)}
def fwd20(d):
    i=nidx.get(d)
    if i is None or i+1+20>=len(nc) or nc[i+1]<=0: return None
    return nc[i+1+20]/nc[i+1]-1.0            # act d+1, hold 20 sessions

# ---- load positioning (poi): per (date, client) ----
poi=json.load(open(f"{SCRATCH}/poi_full.json"))
byc={}   # client -> {date: rowdict}
def F(x):
    try: return float(x)
    except: return 0.0
for r in poi:
    byc.setdefault(r["client_type"],{})[r["date"][:10]] = {
        "fnet": F(r["fut_idx_long"])-F(r["fut_idx_short"]),
        "fratio": F(r["fut_idx_long"])/(F(r["fut_idx_long"])+F(r["fut_idx_short"])+1),
        "optbull": (F(r["opt_idx_call_long"])+F(r["opt_idx_put_short"]))-(F(r["opt_idx_call_short"])+F(r["opt_idx_put_long"])),
        "tnet": F(r["total_long"])-F(r["total_short"]),
        "sfnet": F(r["fut_stk_long"])-F(r["fut_stk_short"]),
    }

# ---- load flow (fii_deriv): per date, INDEX FUTURES + INDEX OPTIONS net buy Rs ----
fd=json.load(open(f"{SCRATCH}/fii_deriv.json"))
idxfut_flow={}; idxopt_flow={}
for r in fd:
    net=F(r["buy_amt_cr"])-F(r["sell_amt_cr"]); d=r["date"][:10]
    if r["instrument"]=="INDEX FUTURES": idxfut_flow[d]=net
    elif r["instrument"]=="INDEX OPTIONS": idxopt_flow[d]=net

# ---- build per-date signal features (with trailing transforms) ----
alldates=sorted(set(byc.get("FII",{})) & set(nidx))
def series(getter, dates):
    return {d:getter(d) for d in dates if getter(d) is not None}
def z20(vals, dates, d):     # standardized vs trailing 20d (excl today)
    i=dates.index(d) if d in dates else -1
    if i<20: return None
    w=[vals[dates[j]] for j in range(i-20,i)]
    mu=statistics.mean(w); sd=statistics.pstdev(w) or 1.0
    return (vals[d]-mu)/sd
def trend(vals, dates, d):
    i=dates.index(d) if d in dates else -1
    if i<20: return None
    w=[vals[dates[j]] for j in range(i-20,i)]
    return vals[d]-statistics.mean(w)
def d1(vals, dates, d):
    i=dates.index(d) if d in dates else -1
    return vals[d]-vals[dates[i-1]] if i>=1 else None
def cum(vals, dates, d, n):
    i=dates.index(d) if d in dates else -1
    return sum(vals[dates[j]] for j in range(max(0,i-n+1),i+1)) if i>=0 else None

signals={}   # name -> {date: signal_value}  (higher = candidate-bullish; screen finds true dir)
for c in ("FII","DII","Pro","Client"):
    cd=sorted(byc[c]);
    fnet={d:byc[c][d]["fnet"] for d in cd}
    signals[f"{c}.fnet_z20"]={d:z20(fnet,cd,d) for d in cd}
    signals[f"{c}.fnet_trend"]={d:trend(fnet,cd,d) for d in cd}
    signals[f"{c}.fnet_d1"]={d:d1(fnet,cd,d) for d in cd}
    signals[f"{c}.fratio"]={d:byc[c][d]["fratio"] for d in cd}
    ob={d:byc[c][d]["optbull"] for d in cd}
    signals[f"{c}.optbull_z20"]={d:z20(ob,cd,d) for d in cd}
    tn={d:byc[c][d]["tnet"] for d in cd}
    signals[f"{c}.tnet_z20"]={d:z20(tn,cd,d) for d in cd}
fd_dates=sorted(idxfut_flow)
signals["flow.idxfut_lvl"]=dict(idxfut_flow)
signals["flow.idxfut_cum5"]={d:cum(idxfut_flow,fd_dates,d,5) for d in fd_dates}
signals["flow.idxfut_cum10"]={d:cum(idxfut_flow,fd_dates,d,10) for d in fd_dates}
signals["flow.idxopt_lvl"]=dict(idxopt_flow)
signals["flow.idxopt_cum5"]={d:cum(idxopt_flow,fd_dates,d,5) for d in fd_dates}
# smart-minus-dumb divergence (FII net z - Client net z)
fii_cd=sorted(byc["FII"]); cli_cd=sorted(byc["Client"])
fii_z={d:z20({x:byc["FII"][x]["fnet"] for x in fii_cd},fii_cd,d) for d in fii_cd}
cli_z={d:z20({x:byc["Client"][x]["fnet"] for x in cli_cd},cli_cd,d) for d in cli_cd}
signals["div.FIIminusClient"]={d:(fii_z[d]-cli_z[d]) for d in fii_z if fii_z.get(d) is not None and cli_z.get(d) is not None}

def qspread(pairs):   # pairs=[(sig,fwd)] -> mean(top20% fwd) - mean(bottom20% fwd)
    ps=[p for p in pairs if p[0] is not None and p[1] is not None]
    if len(ps)<40: return None,0
    ps.sort(key=lambda x:x[0]); k=max(1,len(ps)//5)
    bot=[p[1] for p in ps[:k]]; top=[p[1] for p in ps[-k:]]
    return statistics.mean(top)-statistics.mean(bot), len(ps)

print(f"{len(signals)} signals × forward-Nifty f20, IS(<=2020)/OOS(>=2021) quintile spread\n", flush=True)
print(f"  {'signal':24}{'IS spread':>11}{'OOS spread':>12}{'n(IS/OOS)':>12}  robust?", flush=True)
rows=[]
for name,sv in signals.items():
    isp=[(sv.get(d), fwd20(d)) for d in sv if d<=IS_END]
    oosp=[(sv.get(d), fwd20(d)) for d in sv if d>IS_END]
    si,ni=qspread(isp); so,no=qspread(oosp)
    if si is None or so is None: continue
    robust = (si*so>0) and min(abs(si),abs(so))>=MIN_SPREAD
    rows.append((name,si,so,ni,no,robust))
for name,si,so,ni,no,robust in sorted(rows,key=lambda r:-(min(abs(r[1]),abs(r[2])) if r[1]*r[2]>0 else -1)):
    flag=" <<< SURVIVOR" if robust else ("  (reverses)" if si*so<0 else "")
    print(f"  {name:24}{si*100:>+10.2f}%{so*100:>+11.2f}%{ni:>7}/{no:<5}{flag}", flush=True)
surv=[r for r in rows if r[5]]
print(f"\n=== {len(surv)} OOS-robust survivors (same-sign spread both halves, |spread|>=0.4%/20d) ===", flush=True)
for name,si,so,_,_,_ in surv: print(f"  {name}: IS {si*100:+.2f}% / OOS {so*100:+.2f}%", flush=True)
if not surv: print("  NONE — every construction either reverses OOS or is too weak. Directional signal not present.", flush=True)
