"""Subtask 1 diagnostics -- seasonality + pairs/relative-value. Cheap pass/fail screen only,
mirrors swing_sector_rs_diag.py's style (IS<=2022 / OOS>=2023, avgR/WR-style read, no cost
model yet -- that's for the full grind if either survives). READ-ONLY, single-process,
local cache only, zero GCP cost."""
import os, json, pickle, statistics, datetime as dt
from collections import defaultdict

CACHE = os.path.expanduser("~/.autotrader_backtest_cache")
IS_END, OOS_START = "2022-12-31", "2023-01-01"

# ------------------------------------------------------------------ seasonality (Nifty)
print("=" * 90)
print("SEASONALITY -- day-of-week + month-of-year, Nifty daily closes 2015-2026")
print("=" * 90)
mi = json.load(open(f"{CACHE}/market_inputs_2015.json"))
dates = sorted(mi.keys())
closes = [(d, mi[d]["nifty_close"]) for d in dates if mi[d].get("nifty_close")]
rets = []
for i in range(1, len(closes)):
    d, c = closes[i]
    _, c0 = closes[i - 1]
    if c0:
        rets.append((d, c0, c, c / c0 - 1.0))

def _split(rows):
    isr = [r for r in rows if r[0] <= IS_END]
    oos = [r for r in rows if r[0] >= OOS_START]
    return isr, oos

def _m(rows):
    if not rows:
        return "  n/a  "
    vals = [r[3] for r in rows]
    return f"mean={statistics.mean(vals)*100:+.3f}% n={len(rows)}"

print("\nby weekday:")
by_wd = defaultdict(list)
for r in rets:
    wd = dt.date.fromisoformat(r[0]).weekday()
    by_wd[wd].append(r)
for wd, name in enumerate(["Mon", "Tue", "Wed", "Thu", "Fri"]):
    isr, oos = _split(by_wd.get(wd, []))
    print(f"  {name:<4} IS: {_m(isr):<22} OOS: {_m(oos)}")

print("\nby month:")
by_mo = defaultdict(list)
for r in rets:
    mo = int(r[0][5:7])
    by_mo[mo].append(r)
for mo in range(1, 13):
    isr, oos = _split(by_mo.get(mo, []))
    print(f"  M{mo:02d}  IS: {_m(isr):<22} OOS: {_m(oos)}")

print("\nRead: look for a weekday/month that's robustly positive (or negative) BOTH halves,")
print("materially away from the others. Noise if it flips sign or is IS-only.")

# ------------------------------------------------------------------ pairs / relative-value
print()
print("=" * 90)
print("PAIRS / RELATIVE-VALUE -- stock vs sector 5d spread, does an extreme laggard revert?")
print("=" * 90)
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
print(f"  {len(SYM):,} symbols with enough history | {len(SYM2SEC):,} sector mappings", flush=True)

MIN_PRICE, TOPN = 30.0, 800
LOOKBACK, FWD = 5, 5           # short-horizon: 5d spread measurement, 5d forward hold

def ret_n(c, j, n):
    return (c[j] / c[j - n] - 1.0) if j >= n and c[j - n] > 0 else None

all_dates = sorted({d for S in SYM.values() for d in S["d"]})
sample_dates = all_dates[300::3]  # stride-3, skip warmup -- keeps this a cheap screen not a full grind
print(f"  sampling {len(sample_dates):,} of {len(all_dates):,} trading dates (stride 3)", flush=True)

rows = []
for di, d in enumerate(sample_dates):
    if di % 300 == 0:
        print(f"  {d} ({di}/{len(sample_dates)})", flush=True)
    elig = []
    for sym, S in SYM.items():
        j = S["idx"].get(d)
        if j is None or j < LOOKBACK or j + FWD >= len(S["c"]):
            continue
        if S["c"][j] < MIN_PRICE:
            continue
        r5 = ret_n(S["c"], j, LOOKBACK)
        if r5 is None:
            continue
        elig.append((sym, S, j, S["c"][j] * S["v"][j], r5))
    if not elig:
        continue
    elig.sort(key=lambda x: -x[3])
    elig = elig[:TOPN]
    by_sec = defaultdict(list)
    for sym, S, j, _, r5 in elig:
        sec = SYM2SEC.get(sym)
        if sec:
            by_sec[sec].append(r5)
    sec_avg = {sec: statistics.mean(rs) for sec, rs in by_sec.items() if len(rs) >= 3}
    for sym, S, j, _, r5 in elig:
        sec = SYM2SEC.get(sym)
        if not sec or sec not in sec_avg:
            continue
        fwd = ret_n(S["c"], j + FWD, FWD)
        if fwd is None:
            continue
        rows.append({"d": d, "spread": r5 - sec_avg[sec], "raw_r5": r5, "fwd": fwd})

print(f"\n  {len(rows):,} (date, symbol) observations tagged\n", flush=True)

def bucket(pool, lo, hi, lbl, field="spread"):
    a = [r for r in pool if lo <= r[field] < hi and r["d"] <= IS_END]
    b = [r for r in pool if lo <= r[field] < hi and r["d"] >= OOS_START]
    def m(t):
        if not t:
            return "  n/a  "
        return f"avgFwd={statistics.mean(x['fwd'] for x in t)*100:+.3f}% WR={100*sum(1 for x in t if x['fwd']>0)/len(t):4.1f}%"
    print(f"  {lbl:22} IS(n={len(a):>5}): {m(a):<26} OOS(n={len(b):>5}): {m(b)}", flush=True)

print("=== forward 5d return by 5d spread-vs-sector bucket ===")
for lo, hi, lbl in [(-1, -0.08, "<-8% (extreme lag)"), (-0.08, -0.03, "-8%..-3% (laggard)"),
                     (-0.03, 0.03, "-3%..3% (neutral)"), (0.03, 0.08, "3-8% (leader)"),
                     (0.08, 10, ">=8% (extreme lead)")]:
    bucket(rows, lo, hi, lbl)

print("\nRead: long-only relative-value thesis needs the LAGGARD buckets robustly POSITIVE")
print("both halves (spread reverts) -- if flat/negative, or leaders/laggards both similar,")
print("there's no exploitable mean-reversion here (matches single-stock MR's known failure).")

print()
print("=" * 90)
print("CONFOUND CHECK -- same rows, bucketed by RAW 5d return (no sector adjustment)")
print("If this shows the SAME/stronger pattern, sector-adjustment adds nothing and this")
print("is plain single-stock MR reappearing (already killed). Sector-adjustment only earns")
print("its keep if the ABOVE (sector-relative) result is cleaner/stronger than THIS.")
print("=" * 90)
for lo, hi, lbl in [(-1, -0.08, "<-8% (extreme lag)"), (-0.08, -0.03, "-8%..-3% (laggard)"),
                     (-0.03, 0.03, "-3%..3% (neutral)"), (0.03, 0.08, "3-8% (leader)"),
                     (0.08, 10, ">=8% (extreme lead)")]:
    bucket(rows, lo, hi, lbl, field="raw_r5")

# direct side-by-side: correlation between spread and raw_r5 -- if near 1.0, they're
# measuring almost the same thing and sector-adjustment is cosmetic
import math
xs = [r["spread"] for r in rows]
ys = [r["raw_r5"] for r in rows]
mx, my = statistics.mean(xs), statistics.mean(ys)
cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / len(xs)
sx = math.sqrt(sum((x - mx) ** 2 for x in xs) / len(xs))
sy = math.sqrt(sum((y - my) ** 2 for y in ys) / len(ys))
corr = cov / (sx * sy) if sx > 0 and sy > 0 else float("nan")
print(f"\ncorr(sector-adjusted spread, raw 5d return) = {corr:.3f}")
print("(near 1.0 = sector-adjustment barely moves the ranking; well below 1.0 = it's doing real work)")
