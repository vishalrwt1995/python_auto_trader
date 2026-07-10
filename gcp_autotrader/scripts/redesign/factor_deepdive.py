"""FACTOR DEEP-DIVE — honest DAILY-marked risk for the Momentum x Low-Vol candidate.
Kills the quarterly-marking flattery by tracking a true daily equity curve. Tests:
 (a) monthly vs quarterly, DAILY maxDD/Calmar/Sharpe, IS/OOS, by-year;
 (b) Nifty-200DMA regime overlay (hold only in up-regimes → tame momentum crashes);
 (c) POSITION-level overlap (Jaccard) vs a CORE proxy (not just return corr).
READ-ONLY, local, single-process, zero cost. Panel memoized per date."""
from __future__ import annotations
import os, json, pickle, math
from bisect import bisect_right
from statistics import mean, pstdev
from collections import defaultdict

C = os.path.expanduser("~/.autotrader_backtest_cache")
PRICE_MIN = 30.0; START = "2015-01-01"; COST_RT = 0.0058

print("loading full-universe survivorship-safe bars ...")
bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
mkt = json.load(open(f"{C}/market_inputs_2015.json"))
md = sorted(d for d in mkt if mkt[d].get("nifty_close"))
nc = {d: float(mkt[d]["nifty_close"]) for d in md}
SYM = {}
for s, b in bars.items():
    if not b or len(b) < 300: continue
    SYM[s] = {"d": [x[0] for x in b], "c": [float(x[4]) for x in b], "v": [float(x[5]) for x in b]}

ser = [nc[d] for d in md]
ma200 = {md[i]: (mean(ser[i-199:i+1]) if i >= 199 else None) for i in range(len(md))}
month_end = [md[i-1] for i in range(1, len(md)) if md[i][:7] != md[i-1][:7]] + [md[-1]]
MONTHLY = [d for d in month_end if d >= START]
QUARTERLY = [d for i, d in enumerate(MONTHLY) if i % 3 == 0]

def idx_le(S, dt):
    i = bisect_right(S["d"], dt) - 1
    return i if i >= 0 else None

_panel = {}
def panel_at(dt):
    if dt in _panel: return _panel[dt]
    row = {}
    for s, S in SYM.items():
        i = idx_le(S, dt)
        if i is None or i < 252: continue
        c, v = S["c"], S["v"]
        if c[i] < PRICE_MIN or c[i-252] <= 0: continue
        turns = sorted(c[k]*v[k] for k in range(i-60, i)); mt = turns[len(turns)//2]
        rets = [c[k]/c[k-1]-1.0 for k in range(i-126, i) if c[k-1] > 0]
        if len(rets) <= 40: continue
        row[s] = {"MOM": c[i-21]/c[i-252]-1.0, "LOWVOL": -pstdev(rets), "turn": mt}
    _panel[dt] = row
    return row

def picks_at(dt, topn, min_turn, large_cap_rank=None, facs=("MOM", "LOWVOL")):
    p = panel_at(dt)
    uni = [s for s, r in p.items() if r["turn"] >= min_turn]
    if large_cap_rank: uni = sorted(uni, key=lambda s: -p[s]["turn"])[:large_cap_rank]
    if len(uni) < topn: return []
    ranks = defaultdict(float)
    for f in facs:
        for rk, s in enumerate(sorted(uni, key=lambda s: -p[s][f])): ranks[s] += rk
    return sorted(uni, key=lambda s: ranks[s])[:topn]

def daily_curve(rebal, topn, min_turn, regime=False):
    rebset = set(rebal); holds = {}; pv = 1.0; prev = set(); curve = []
    dates = [d for d in md if rebal[0] <= d <= rebal[-1]]
    for k, t in enumerate(dates):
        if holds and k > 0:
            tp = dates[k-1]
            for s in list(holds):
                S = SYM[s]; i = idx_le(S, t); ip = idx_le(S, tp)
                if i is not None and ip is not None and i != ip and S["c"][ip] > 0:
                    holds[s] *= S["c"][i]/S["c"][ip]
            pv = sum(holds.values())
        if t in rebset:
            on = (not regime) or (ma200.get(t) is not None and nc[t] > ma200[t])
            newp = set(picks_at(t, topn, min_turn)) if on else set()
            turnover = len(newp ^ prev)/max(1, topn)
            pv *= (1 - 0.5*turnover*COST_RT)
            holds = {s: pv/len(newp) for s in newp} if newp else {}
            prev = newp
        curve.append((t, pv))
    return curve

def stats(curve):
    ds = [d for d, _ in curve]; pv = [p for _, p in curve]
    dr = [(ds[i], pv[i]/pv[i-1]-1.0) for i in range(1, len(pv)) if pv[i-1] > 0]
    yrs = len(pv)/252
    cagr = (pv[-1]/pv[0])**(1/yrs)-1 if pv[0] > 0 else 0
    peak = -1e9; mdd = 0.0
    for p in pv:
        peak = max(peak, p); mdd = min(mdd, p/peak-1)
    rr = [r for _, r in dr]; sd = pstdev(rr) if len(rr) > 2 else 0
    sharpe = mean(rr)/sd*math.sqrt(252) if sd > 0 else 0
    IS = [r for d, r in dr if d <= "2020-12-31"]; OOS = [r for d, r in dr if d >= "2021-01-01"]
    def ann(x): return 100*(math.prod(1+r for r in x)**(252/len(x))-1) if x else 0
    yv = defaultdict(lambda: 1.0)
    for d, r in dr: yv[d[:4]] *= (1+r)
    return dict(cagr=100*cagr, mdd=100*mdd, calmar=(cagr/abs(mdd) if mdd < 0 else 0), sharpe=sharpe,
                isc=ann(IS), oosc=ann(OOS), yr={y: round(100*(yv[y]-1), 1) for y in sorted(yv)})

def line(lbl, s):
    ok = "  <==" if (s["isc"] > 0 and s["oosc"] > 0) else ""
    print(f"  {lbl:38} CAGR {s['cagr']:>5.1f}%  DAILY-maxDD {s['mdd']:>6.1f}%  Calmar {s['calmar']:>5.2f}  "
          f"Sharpe {s['sharpe']:>4.2f}  IS {s['isc']:>+5.1f}% OOS {s['oosc']:>+5.1f}%{ok}")

ny = (md.index(MONTHLY[-1]) - md.index(MONTHLY[0]))/252
print(f"Nifty CAGR ~{100*((nc[MONTHLY[-1]]/nc[MONTHLY[0]])**(1/ny)-1):.1f}%  (all DD below are DAILY-marked = honest)\n")

print("=== TRUE daily-marked risk: MOM+LOWVOL top20 >=Rs25cr ===")
mo = stats(daily_curve(MONTHLY, 20, 2.5e8)); line("monthly", mo)
qu = stats(daily_curve(QUARTERLY, 20, 2.5e8)); line("quarterly", qu)
print("\n=== + Nifty-200DMA regime overlay (cash when Nifty < 200DMA — tame crashes) ===")
mor = stats(daily_curve(MONTHLY, 20, 2.5e8, regime=True)); line("monthly + regime", mor)
qur = stats(daily_curve(QUARTERLY, 20, 2.5e8, regime=True)); line("quarterly + regime", qur)

print("\n=== fillable large-cap tilt (>=Rs100cr) for comparison ===")
line("quarterly >=Rs100cr", stats(daily_curve(QUARTERLY, 20, 1e9)))
line("quarterly >=Rs100cr + regime", stats(daily_curve(QUARTERLY, 20, 1e9, regime=True)))

print("\n=== by-year NET % (daily-compounded) ===")
for lbl, s in [("quarterly", qu), ("quarterly+regime", qur)]:
    print(f"  {lbl:18} " + " ".join(f"{y}:{s['yr'][y]:+.0f}" for y in sorted(s['yr'])))

print("\n=== POSITION overlap vs CORE proxy (Jaccard of held names, quarterly) ===")
js = []
for t in QUARTERLY:
    a = set(picks_at(t, 20, 2.5e8)); b = set(picks_at(t, 30, 1e9, large_cap_rank=100))
    if a and b: js.append(len(a & b)/len(a | b))
print(f"  mean Jaccard(candidate top20>=25cr, CORE-proxy top30 of top100) = {mean(js):.2f}  "
      f"({'HIGH overlap' if mean(js) > 0.5 else 'MODERATE' if mean(js) > 0.25 else 'LOW → different names held'})")

print("\nHonest shippable = daily-marked Calmar the number to trust; regime overlay if it lifts Calmar without gutting return.")
