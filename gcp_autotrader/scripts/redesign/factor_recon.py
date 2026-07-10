"""FACTOR RECON — head-to-head test of price/volume, long-only equity factors on the
survivorship-safe full-universe daily data (pead_full_bars_2014.pkl, incl. delisted).
Monthly rebalance, point-in-time liquid universe, realistic delivery costs, IS/OOS + by-year
+ vs Nifty. Goal: which factor(s) have a deployable, differentiated edge worth a channel?
READ-ONLY, local, single-process, zero cost. Honest bar: net-of-cost, both halves positive."""
from __future__ import annotations
import os, json, pickle
from bisect import bisect_right
from statistics import mean, pstdev
from collections import defaultdict

C = os.path.expanduser("~/.autotrader_backtest_cache")
CAP = 100_000.0
TOPN = 20                 # long top-N equal-weight
MIN_TURN = 5e7            # >= Rs5cr 60d median turnover (fillable)
PRICE_MIN = 30.0
COST_RT = 0.0058          # delivery round-trip (Upstox, per costs.py)
START = "2015-01-01"
FACTORS = ["MOM12_1", "MOM6_1", "LOWVOL", "STREV", "HI52"]

print("loading survivorship-safe full-universe daily bars ...")
bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
mkt = json.load(open(f"{C}/market_inputs_2015.json"))
md = sorted(d for d in mkt if mkt[d].get("nifty_close"))
nc = {d: float(mkt[d]["nifty_close"]) for d in md}

SYM = {}
for s, b in bars.items():
    if not b or len(b) < 300:
        continue
    SYM[s] = {"d": [x[0] for x in b], "c": [float(x[4]) for x in b],
              "h": [float(x[2]) for x in b], "v": [float(x[5]) for x in b]}
print(f"symbols with >=300 bars: {len(SYM)}")

# monthly rebalance dates = last trading day of each month (nifty calendar)
rebal = [md[i-1] for i in range(1, len(md)) if md[i][:7] != md[i-1][:7]] + [md[-1]]
rebal = [d for d in rebal if d >= START]
print(f"monthly rebalance dates {rebal[0]} .. {rebal[-1]} ({len(rebal)})")

def idx_le(S, dt):
    i = bisect_right(S["d"], dt) - 1
    return i if i >= 0 else None

# ---- precompute factor panel per rebalance date (one pass, shared universe) ----
panel = {}   # date -> {sym: {factor: score}}
for dt in rebal:
    row = {}
    for s, S in SYM.items():
        i = idx_le(S, dt)
        if i is None or i < 252:
            continue
        c, v, h = S["c"], S["v"], S["h"]
        px = c[i]
        if px < PRICE_MIN:
            continue
        turns = sorted(c[k]*v[k] for k in range(i-60, i))
        if turns[len(turns)//2] < MIN_TURN:      # 60d median turnover
            continue
        f = {}
        if c[i-252] > 0: f["MOM12_1"] = c[i-21]/c[i-252]-1.0
        if c[i-126] > 0: f["MOM6_1"] = c[i-21]/c[i-126]-1.0
        rets = [c[k]/c[k-1]-1.0 for k in range(i-126, i) if c[k-1] > 0]
        if len(rets) > 40: f["LOWVOL"] = -pstdev(rets)
        if c[i-21] > 0: f["STREV"] = -(c[i]/c[i-21]-1.0)
        hi = max(h[i-252:i+1])
        if hi > 0: f["HI52"] = px/hi
        if f: row[s] = f
    panel[dt] = row

def fwd(S, d0, d1):
    i0, i1 = idx_le(S, d0), idx_le(S, d1)
    if i0 is None or i1 is None or i1 <= i0 or S["c"][i0] <= 0:
        return None
    return S["c"][i1]/S["c"][i0]-1.0

def run(fac):
    yr = defaultdict(float); rows = []; prev = set()
    for j in range(len(rebal)-1):
        d0, d1 = rebal[j], rebal[j+1]
        fv = {s: r[fac] for s, r in panel[d0].items() if fac in r}
        if len(fv) < TOPN:
            continue
        pick = [s for s, _ in sorted(fv.items(), key=lambda kv: -kv[1])[:TOPN]]
        mret = [r for s in pick if (r := fwd(SYM[s], d0, d1)) is not None]
        if not mret:
            continue
        gross = mean(mret)
        turnover = len(set(pick) - prev)/TOPN
        net = gross - turnover*COST_RT
        yr[d1[:4]] += net; rows.append((d1, net, gross)); prev = set(pick)
    years = sorted(yr)
    if not years:
        return None
    eq = 1.0
    for y in years: eq *= (1+yr[y])
    cg = eq**(1/len(years))-1
    eqc = peak = 1.0; mdd = 0.0
    for _, net, _ in rows:
        eqc *= (1+net); peak = max(peak, eqc); mdd = min(mdd, eqc/peak-1)
    IS = [n for d, n, _ in rows if d <= "2020-12-31"]
    OOS = [n for d, n, _ in rows if d >= "2021-01-01"]
    grossyr = defaultdict(float)
    for d, _, g in rows: grossyr[d[:4]] += g
    geq = 1.0
    for y in years: geq *= (1+grossyr[y])
    return dict(n=len(rows), cagr=100*cg, gross_cagr=100*(geq**(1/len(years))-1),
                mdd=100*mdd, calmar=(cg/abs(mdd) if mdd < 0 else 0),
                posyr=sum(1 for y in years if yr[y] > 0), nyr=len(years),
                isann=100*mean(IS) if IS else 0, oosann=100*mean(OOS) if OOS else 0,
                yr={y: round(100*yr[y], 1) for y in years})

# nifty benchmark CAGR over the same window
n0, n1 = nc[min(nc, key=lambda d: abs(md.index(d)-md.index(rebal[0])) if d in md else 9e9)], nc[rebal[-1]] if rebal[-1] in nc else nc[md[-1]]
ny = (md.index(rebal[-1]) - md.index(rebal[0]))/252 if rebal[0] in md and rebal[-1] in md else len(rebal)/12
nifty_cg = 100*((n1/n0)**(1/ny)-1)

print(f"\n=== FACTOR RECON  (top{TOPN} EW, monthly, >=Rs5cr turnover, delivery cost {COST_RT*100:.2f}% RT) ===")
print(f"Nifty benchmark CAGR over window ~ {nifty_cg:.1f}%\n")
print(f"{'factor':9} {'n':>4} {'NET cagr':>9} {'gross':>7} {'maxDD':>7} {'Calmar':>7} {'+yrs':>6} {'IS/mo':>7} {'OOS/mo':>7}")
res = {}
for fac in FACTORS:
    r = run(fac); res[fac] = r
    if not r:
        print(f"{fac:9} (none)"); continue
    flag = "  <==" if (r["isann"] > 0 and r["oosann"] > 0 and r["cagr"] > nifty_cg) else ""
    print(f"{fac:9} {r['n']:>4} {r['cagr']:>8.1f}% {r['gross_cagr']:>6.1f}% {r['mdd']:>6.1f}% "
          f"{r['calmar']:>7.2f} {r['posyr']:>3}/{r['nyr']:<2} {r['isann']:>+6.2f} {r['oosann']:>+6.2f}{flag}")

# combo: momentum + low-vol blended rank (z-score-ish via rank average)
def run_combo(facs):
    yr = defaultdict(float); rows = []; prev = set()
    for j in range(len(rebal)-1):
        d0, d1 = rebal[j], rebal[j+1]
        universe = [s for s, r in panel[d0].items() if all(f in r for f in facs)]
        if len(universe) < TOPN:
            continue
        ranks = defaultdict(float)
        for f in facs:
            order = sorted(universe, key=lambda s: -panel[d0][s][f])
            for rank, s in enumerate(order):
                ranks[s] += rank
        pick = sorted(universe, key=lambda s: ranks[s])[:TOPN]
        mret = [r for s in pick if (r := fwd(SYM[s], d0, d1)) is not None]
        if not mret:
            continue
        turnover = len(set(pick) - prev)/TOPN
        yr[d1[:4]] += mean(mret) - turnover*COST_RT
        rows.append((d1, mean(mret)-turnover*COST_RT)); prev = set(pick)
    years = sorted(yr); eq = 1.0
    for y in years: eq *= (1+yr[y])
    eqc = peak = 1.0; mdd = 0.0
    for _, net in rows:
        eqc *= (1+net); peak = max(peak, eqc); mdd = min(mdd, eqc/peak-1)
    IS = [n for d, n in rows if d <= "2020-12-31"]; OOS = [n for d, n in rows if d >= "2021-01-01"]
    cg = eq**(1/len(years))-1
    return dict(n=len(rows), cagr=100*cg, mdd=100*mdd, calmar=(cg/abs(mdd) if mdd < 0 else 0),
                posyr=sum(1 for y in years if yr[y] > 0), nyr=len(years),
                isann=100*mean(IS) if IS else 0, oosann=100*mean(OOS) if OOS else 0)

print("\n--- blends (rank-average) ---")
for combo in [["MOM12_1", "LOWVOL"], ["MOM6_1", "LOWVOL"], ["MOM12_1", "HI52"]]:
    r = run_combo(combo)
    flag = "  <==" if (r["isann"] > 0 and r["oosann"] > 0 and r["cagr"] > nifty_cg) else ""
    print(f"{'+'.join(combo):20} n={r['n']:>4} NET cagr {r['cagr']:>5.1f}% maxDD {r['mdd']:>6.1f}% "
          f"Calmar {r['calmar']:>5.2f} +yrs {r['posyr']}/{r['nyr']} IS/mo {r['isann']:+.2f} OOS/mo {r['oosann']:+.2f}{flag}")

print("\nby-year NET % (winners):")
for fac in FACTORS:
    if res.get(fac):
        print(f"  {fac:9} " + " ".join(f"{y}:{res[fac]['yr'][y]:+.0f}" for y in sorted(res[fac]['yr'])))
print("\nDeployable channel = NET CAGR > Nifty, BOTH halves positive, Calmar reasonable, fillable universe.")
