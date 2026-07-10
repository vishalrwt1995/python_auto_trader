"""FACTOR STRESS — take the recon winner (Momentum x Low-Vol blend) and hit it with the full
bar: FILLABILITY (turnover tiers Rs5/25/100cr — does the edge survive in tradeable names, or
is it mid-cap fiction?), TOPN concentration, REBALANCE freq (monthly vs quarterly = the CORE
overlap axis), COST stress, and CORRELATION vs a CORE-like quarterly-large-cap proxy.
READ-ONLY, local, single-process, zero cost. Panel built once, all configs swept from it."""
from __future__ import annotations
import os, json, pickle
from bisect import bisect_right
from statistics import mean, pstdev
from collections import defaultdict

C = os.path.expanduser("~/.autotrader_backtest_cache")
CAP = 100_000.0; PRICE_MIN = 30.0; START = "2015-01-01"

print("loading full-universe survivorship-safe bars ...")
bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
mkt = json.load(open(f"{C}/market_inputs_2015.json"))
md = sorted(d for d in mkt if mkt[d].get("nifty_close"))
nc = {d: float(mkt[d]["nifty_close"]) for d in md}
SYM = {}
for s, b in bars.items():
    if not b or len(b) < 300: continue
    SYM[s] = {"d": [x[0] for x in b], "c": [float(x[4]) for x in b],
              "h": [float(x[2]) for x in b], "v": [float(x[5]) for x in b]}

month_end = [md[i-1] for i in range(1, len(md)) if md[i][:7] != md[i-1][:7]] + [md[-1]]
MONTHLY = [d for d in month_end if d >= START]
QUARTERLY = [d for i, d in enumerate(MONTHLY) if i % 3 == 0]

def idx_le(S, dt):
    i = bisect_right(S["d"], dt) - 1
    return i if i >= 0 else None

# panel WITHOUT liquidity filter (store turnover so we can sweep tiers cheaply)
def build_panel(dates):
    P = {}
    for dt in dates:
        row = {}
        for s, S in SYM.items():
            i = idx_le(S, dt)
            if i is None or i < 252: continue
            c, v, h = S["c"], S["v"], S["h"]
            if c[i] < PRICE_MIN: continue
            turns = sorted(c[k]*v[k] for k in range(i-60, i))
            medturn = turns[len(turns)//2]
            rets = [c[k]/c[k-1]-1.0 for k in range(i-126, i) if c[k-1] > 0]
            if len(rets) <= 40 or c[i-252] <= 0: continue
            row[s] = {"MOM": c[i-21]/c[i-252]-1.0, "LOWVOL": -pstdev(rets), "turn": medturn}
        P[dt] = row
    return P

def fwd(S, d0, d1):
    i0, i1 = idx_le(S, d0), idx_le(S, d1)
    if i0 is None or i1 is None or i1 <= i0 or S["c"][i0] <= 0: return None
    return S["c"][i1]/S["c"][i0]-1.0

def backtest(P, rebal, topn, min_turn, cost, facs=("MOM", "LOWVOL"), large_cap_rank=None):
    yr = defaultdict(float); rows = []; prev = set(); monthly_net = []
    for j in range(len(rebal)-1):
        d0, d1 = rebal[j], rebal[j+1]
        uni = [s for s, r in P[d0].items() if r["turn"] >= min_turn]
        if large_cap_rank:            # restrict to top-K by turnover (CORE-like large-cap proxy)
            uni = sorted(uni, key=lambda s: -P[d0][s]["turn"])[:large_cap_rank]
        if len(uni) < topn: continue
        ranks = defaultdict(float)
        for f in facs:
            for rk, s in enumerate(sorted(uni, key=lambda s: -P[d0][s][f])): ranks[s] += rk
        pick = sorted(uni, key=lambda s: ranks[s])[:topn]
        mret = [r for s in pick if (r := fwd(SYM[s], d0, d1)) is not None]
        if not mret: continue
        net = mean(mret) - (len(set(pick)-prev)/topn)*cost
        yr[d1[:4]] += net; rows.append((d1, net)); monthly_net.append((d1, net)); prev = set(pick)
    years = sorted(yr)
    if not years: return None
    eq = 1.0
    for y in years: eq *= (1+yr[y])
    eqc = peak = 1.0; mdd = 0.0
    for _, net in rows:
        eqc *= (1+net); peak = max(peak, eqc); mdd = min(mdd, eqc/peak-1)
    IS = [n for d, n in rows if d <= "2020-12-31"]; OOS = [n for d, n in rows if d >= "2021-01-01"]
    cg = eq**(1/len(years))-1
    return dict(n=len(rows), cagr=100*cg, mdd=100*mdd, calmar=(cg/abs(mdd) if mdd < 0 else 0),
                posyr=sum(1 for y in years if yr[y] > 0), nyr=len(years),
                isann=100*mean(IS) if IS else 0, oosann=100*mean(OOS) if OOS else 0,
                series=dict(monthly_net))

print("building monthly + quarterly panels ...")
PM = build_panel(MONTHLY); PQ = build_panel(QUARTERLY)
ny = (md.index(MONTHLY[-1]) - md.index(MONTHLY[0]))/252
nifty_cg = 100*((nc[MONTHLY[-1]]/nc[MONTHLY[0]])**(1/ny)-1)
print(f"Nifty benchmark CAGR ~ {nifty_cg:.1f}%\n")

def show(lbl, r):
    if not r: print(f"  {lbl:34} (none)"); return
    ok = "  <==" if (r["isann"] > 0 and r["oosann"] > 0 and r["cagr"] > nifty_cg) else ""
    print(f"  {lbl:34} n={r['n']:>4} NET {r['cagr']:>5.1f}% maxDD {r['mdd']:>6.1f}% Calmar {r['calmar']:>5.2f} "
          f"+yrs {r['posyr']:>2}/{r['nyr']:<2} IS/mo {r['isann']:>+.2f} OOS/mo {r['oosann']:>+.2f}{ok}")

print("=== 1) FILLABILITY: MOM+LOWVOL blend, top20 monthly, turnover tiers (the make-or-break) ===")
for mt, lbl in [(5e7, ">=Rs5cr"), (2.5e8, ">=Rs25cr"), (1e9, ">=Rs100cr"), (5e9, ">=Rs500cr")]:
    show(f"top20 monthly {lbl}", backtest(PM, MONTHLY, 20, mt, 0.0058))

print("\n=== 2) TOPN concentration (>=Rs25cr, monthly) ===")
for tn in [15, 20, 30, 50]:
    show(f"top{tn} monthly >=Rs25cr", backtest(PM, MONTHLY, tn, 2.5e8, 0.0058))

print("\n=== 3) REBALANCE freq (top20 >=Rs25cr) — monthly vs quarterly (CORE overlap axis) ===")
show("top20 MONTHLY  >=Rs25cr", backtest(PM, MONTHLY, 20, 2.5e8, 0.0058))
show("top20 QUARTERLY >=Rs25cr", backtest(PQ, QUARTERLY, 20, 2.5e8, 0.0058))

print("\n=== 4) COST stress (top20 monthly >=Rs25cr) ===")
for cst in [0.0058, 0.008, 0.012]:
    show(f"cost {cst*100:.2f}% RT", backtest(PM, MONTHLY, 20, 2.5e8, cst))

print("\n=== 5) INCREMENTAL vs CORE proxy — corr(monthly-blend, quarterly-large-cap-blend) ===")
active = backtest(PM, MONTHLY, 20, 2.5e8, 0.0058)              # our candidate
core_like = backtest(PQ, QUARTERLY, 30, 1e9, 0.0058, large_cap_rank=100)  # CORE-ish: q'ly, top30 of top100-liquid
if active and core_like:
    a, b = active["series"], core_like["series"]
    common = sorted(set(a) & set(b))
    if len(common) > 8:
        av = [a[d] for d in common]; bv = [b[d] for d in common]
        ma, mb = mean(av), mean(bv)
        cov = mean((x-ma)*(y-mb) for x, y in zip(av, bv))
        sa, sb = pstdev(av), pstdev(bv)
        corr = cov/(sa*sb) if sa*sb else 0
        print(f"  candidate (monthly top20 >=25cr): NET {active['cagr']:.1f}% Calmar {active['calmar']:.2f}")
        print(f"  CORE-proxy (q'ly top30 of top100): NET {core_like['cagr']:.1f}% Calmar {core_like['calmar']:.2f}")
        print(f"  return correlation on {len(common)} overlapping quarters = {corr:.2f}  "
              f"({'HIGH → redundant with CORE' if corr > 0.8 else 'MODERATE → partly incremental' if corr > 0.6 else 'LOW → genuinely new exposure'})")

print("\nDeployable = edge holds at >=Rs25-100cr (fillable), plateau across topN/rebal, survives cost, LOW-ish corr to CORE.")
