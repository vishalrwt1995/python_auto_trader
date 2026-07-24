"""GRIND #2 — Shareholding Patterns (promoter stake-change accumulation). Pass-1 diagnostic.
Revives the 'ownership accumulation' idea we killed ONLY for lack of history — now real quarterly
history 2016-26 (89k filings). SIGNAL: quarter-over-quarter change in promoter+group holding %
(`pr_and_prgrp`). Promoters INCREASING their stake = informed accumulation (bullish); DECREASING =
distribution (bearish check). Structurally DIFFERENT from insider (quarterly NET stock incl off-market
/ creeping / preferential, not open-market event flow) -> expected low overlap = truer diversifier test.
Entry = NEXT trading day after `broadcastDate` (public disclosure, no look-ahead). Fillable turn>=10cr &
px>=30. fwd20/40/60/90 NET (-0.7% cost), IS(<=2020)/OOS(>=2021) vs all-liquid baseline. Survivorship-safe
(pead_full_bars_2014). READ-ONLY, single-process."""
import os, json, pickle, statistics
from bisect import bisect_right
from datetime import datetime
from collections import defaultdict

C = os.path.expanduser("~/.autotrader_backtest_cache")
S = "/private/tmp/claude-501/-Users-apple-Projects-Migrated-Auto-Trading-Python-GCP/439e48e8-a413-4a1d-9d0a-530e53a5e277/scratchpad"
COST, IS_END, TURN_MIN, PRICE_MIN = 0.007, "2020-12-31", 10e7, 30.0
BASE = {"f20": (-0.10, 0.56), "f40": (0.6, 1.9), "f60": (1.46, 3.30), "f90": (2.4, 5.0)}  # all-liquid ref

rows = json.load(open(f"{S}/shareholding_master.json"))
bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
SYM = {}
for s, b in bars.items():
    if len(b) < 70: continue
    d = [x[0] for x in b]; c = [x[4] for x in b]; v = [x[5] for x in b]
    turn = [None] * len(c); run = 0.0
    for i in range(len(c)):
        if i >= 1: run += c[i-1]*v[i-1]
        if i >= 21: run -= c[i-21]*v[i-21]
        if i >= 21: turn[i] = run/20.0
    SYM[s] = {"d": d, "c": c, "turn": turn}

def dt(x):
    try: return datetime.strptime(str(x).split()[0], "%d-%b-%Y").strftime("%Y-%m-%d")
    except Exception: return None
def fnum(x):
    try: return float(str(x).replace(",", ""))
    except Exception: return None

# per-symbol chronological filings -> Q-o-Q promoter% delta
bysym = defaultdict(list)
for r in rows:
    sym = str(r.get("symbol") or "").strip().upper(); d = dt(r.get("broadcastDate") or r.get("date"))
    pr = fnum(r.get("pr_and_prgrp"))
    if sym and d and pr is not None and 0 <= pr <= 100:
        bysym[sym].append((d, pr))
events = []
for sym, lst in bysym.items():
    lst.sort()
    for i in range(1, len(lst)):
        d, pr = lst[i]; d0, pr0 = lst[i-1]
        events.append({"sym": sym, "date": d, "dpr": pr - pr0, "pr": pr})
print(f"{len(rows)} filings | {len(bysym)} symbols | {len(events)} Q-o-Q change events", flush=True)

def build(pred):
    out = []
    for e in events:
        if not pred(e): continue
        Sd = SYM.get(e["sym"])
        if not Sd: continue
        ref = bisect_right(Sd["d"], e["date"])
        if ref >= len(Sd["c"]) or ref < 1: continue
        if Sd["turn"][ref] is None or Sd["turn"][ref] < TURN_MIN or Sd["c"][ref] < PRICE_MIN: continue
        def fwd(k): return (Sd["c"][ref+k]/Sd["c"][ref]-1.0-COST) if ref+k < len(Sd["c"]) and Sd["c"][ref] > 0 else None
        out.append({"dd": e["date"], "f20": fwd(20), "f40": fwd(40), "f60": fwd(60), "f90": fwd(90)})
    return out
def stat(pool, k):
    v = [r[k] for r in pool if r[k] is not None]
    return f"avg={statistics.mean(v)*100:+5.2f}% med={statistics.median(v)*100:+5.2f}% WR={100*sum(1 for x in v if x>0)/len(v):3.0f}% n={len(v)}" if v else "n/a"
def report(label, pool):
    a = [r for r in pool if r["dd"] <= IS_END]; z = [r for r in pool if r["dd"] > IS_END]
    print(f"\n>>> {label} (fillable n={len(pool)}; IS {len(a)} / OOS {len(z)})", flush=True)
    for k in ("f20", "f40", "f60", "f90"):
        b = BASE[k]; print(f"    {k}  IS: {stat(a,k)}   OOS: {stat(z,k)}   [base {b[0]:+.2f}/{b[1]:+.2f}]", flush=True)

print("=== Shareholding Patterns: promoter Q-o-Q stake change -> forward NET return ===", flush=True)
report("promoter INCREASE >= +0.5%", build(lambda e: e["dpr"] >= 0.5))
report("promoter INCREASE >= +1.0%", build(lambda e: e["dpr"] >= 1.0))
report("promoter INCREASE >= +2.0%", build(lambda e: e["dpr"] >= 2.0))
report("promoter DECREASE <= -1.0% (bearish check)", build(lambda e: e["dpr"] <= -1.0))
report("promoter ~FLAT (|d|<0.25%) — control", build(lambda e: abs(e["dpr"]) < 0.25))
print("\nREAD: real edge = promoter INCREASE beats baseline BOTH halves (monotone in size) AND flat/decrease lag. Then overlap-check vs insider + portfolio walk.", flush=True)
