"""Subtask 2 -- relative-value REAL grind. Rebalance every `fwd` days: rank eligible universe
by sector-adjusted `lookback`-day spread, buy the top `topn_book` laggards equal-weight
(~Rs20k/name), hold `fwd` days, sell. Full Upstox round-trip cost applied per position
(costs.py, is_swing=True). Base config + 3 neighboring configs for a plateau check.
READ-ONLY, single-process, local cache only, zero GCP cost."""
import os, json, pickle, statistics
from collections import defaultdict
from autotrader.backtest.costs import compute_round_trip_cost, CostConfig

CACHE = os.path.expanduser("~/.autotrader_backtest_cache")
IS_END, OOS_START = "2022-12-31", "2023-01-01"
MIN_PRICE, TOPN_UNIVERSE = 30.0, 800
COST = CostConfig.upstox()

print("loading bars + sector map ...", flush=True)
raw = pickle.load(open(f"{CACHE}/swing_adj_bars_2015.pkl", "rb"))
sect_raw = json.load(open(f"{CACHE}/sector_map.json"))
SYM2SEC = {}
for _v in sect_raw.values():
    if isinstance(_v, dict) and _v.get("sym") and _v.get("sector"):
        SYM2SEC[str(_v["sym"]).strip().upper()] = _v["sector"]

SYM = {}
for s, b in raw.items():
    if not b or len(b) < 260:
        continue
    d = [x[0] for x in b]; c = [x[4] for x in b]; v = [x[5] for x in b]
    SYM[s] = {"d": d, "c": c, "v": v, "idx": {dt_: i for i, dt_ in enumerate(d)}}
del raw
all_dates = sorted({d for S in SYM.values() for d in S["d"]})
print(f"  {len(SYM):,} symbols | {len(all_dates):,} dates\n", flush=True)


def sim(lookback, fwd, topn_book, min_price=MIN_PRICE):
    trades = []
    i = 300
    while i + lookback < len(all_dates) and i + fwd < len(all_dates):
        d = all_dates[i]
        elig = []
        for sym, S in SYM.items():
            j = S["idx"].get(d)
            if j is None or j < lookback or j + fwd >= len(S["c"]):
                continue
            if S["c"][j] < min_price:
                continue
            c0 = S["c"][j - lookback]
            if c0 <= 0:
                continue
            elig.append((sym, S, j, S["c"][j] * S["v"][j], S["c"][j] / c0 - 1.0))
        if elig:
            elig.sort(key=lambda x: -x[3])
            elig = elig[:TOPN_UNIVERSE]
            by_sec = defaultdict(list)
            for sym, S, j, _, r in elig:
                sec = SYM2SEC.get(sym)
                if sec:
                    by_sec[sec].append(r)
            sec_avg = {sec: statistics.mean(rs) for sec, rs in by_sec.items() if len(rs) >= 3}
            tagged = []
            for sym, S, j, _, r in elig:
                sec = SYM2SEC.get(sym)
                if sec and sec in sec_avg:
                    tagged.append((sym, S, j, r - sec_avg[sec]))
            tagged.sort(key=lambda x: x[3])  # most negative spread (laggard) first
            for sym, S, j, spread in tagged[:topn_book]:
                entry = S["c"][j]; exitp = S["c"][j + fwd]
                qty = max(1, int(20000 / entry))
                cost = compute_round_trip_cost(qty=qty, entry_price=entry, exit_price=exitp,
                                                is_swing=True, cfg=COST)
                net = (exitp - entry) * qty - cost
                trades.append({"d": d, "net_ret": net / (entry * qty), "net_rs": net})
        i += fwd
    return trades


def calmar_cagr(rows):
    if not rows:
        return 0.0, 0.0, 0.0
    by_d = defaultdict(float)
    for t in rows:
        by_d[t["d"]] += t["net_rs"]
    eq, curve = 100000.0, [100000.0]
    for dd in sorted(by_d):
        eq += by_d[dd]; curve.append(eq)
    peak, maxdd = curve[0], 0.0
    for v in curve:
        peak = max(peak, v); maxdd = min(maxdd, (v - peak) / peak if peak else 0.0)
    years = max(1, len(set(t["d"][:4] for t in rows)))
    total_ret = (curve[-1] - curve[0]) / curve[0]
    cagr = (1 + total_ret) ** (1 / years) - 1 if total_ret > -1 else -1.0
    calmar = cagr / abs(maxdd) if maxdd else 0.0
    return cagr * 100, maxdd * 100, calmar


def report(trades, label):
    if not trades:
        print(f"{label}: no trades"); return
    isr = [t for t in trades if t["d"] <= IS_END]
    oos = [t for t in trades if t["d"] >= OOS_START]
    by_year = defaultdict(float)
    for t in trades:
        by_year[t["d"][:4]] += t["net_rs"]
    n = len(trades); wr = 100 * sum(1 for t in trades if t["net_rs"] > 0) / n
    net_total = sum(t["net_rs"] for t in trades)
    cagr, maxdd, calmar = calmar_cagr(trades)
    is_cagr, is_dd, is_cal = calmar_cagr(isr)
    oos_cagr, oos_dd, oos_cal = calmar_cagr(oos)
    pos_years = sum(1 for y, v in by_year.items() if v > 0)
    print(f"{label}: n={n} WR={wr:.1f}% net=Rs{net_total:,.0f} CAGR={cagr:.1f}% "
          f"maxDD={maxdd:.1f}% Calmar={calmar:.2f} yrs+={pos_years}/{len(by_year)}")
    print(f"   IS(n={len(isr)}): CAGR={is_cagr:.1f}% Calmar={is_cal:.2f}   "
          f"OOS(n={len(oos)}): CAGR={oos_cagr:.1f}% Calmar={oos_cal:.2f}")
    print("   by_year: " + " ".join(f"{y}:{v:+,.0f}" for y, v in sorted(by_year.items())))


print("=== BASE: lookback=5d hold=5d top10 laggards, Rs20k/name, full Upstox cost ===")
report(sim(lookback=5, fwd=5, topn_book=10), "base")

print("\n=== PLATEAU CHECK (neighboring configs -- looking for a stable region, not a lucky point) ===")
report(sim(lookback=5, fwd=5, topn_book=15), "topn=15")
report(sim(lookback=5, fwd=7, topn_book=10), "hold=7d")
report(sim(lookback=8, fwd=5, topn_book=10), "lookback=8d")

print("\nRead: needs CAGR>0 AND Calmar competitive with live channels (~0.5+) in BOTH IS and OOS,")
print("stable (not collapsing) across the plateau configs, majority of years positive.")
print("If cost erases it or OOS/IS diverge sharply -> same fate as FII/DII (real signal, dead trade).")
