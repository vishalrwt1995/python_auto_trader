"""Overnight-drift diagnostic (Edge C) -- the close->open drift anomaly. Two questions:
(1) does the anomaly exist here (avg overnight return >> avg intraday return)? and, decisively,
(2) is there ANY liquid subset whose overnight return beats the ~0.6% delivery round-trip cost
(buy at close T, sell at open T+1 = full-STT round trip)? Buckets overnight return by today's
intraday move + close-position-in-range, IS(<=2020)/OOS(>=2021). Naive overnight drift (~0.05%)
is far below the 0.6% hurdle, so this is mostly a fast, decisive rule-OUT unless an extreme
subset surprises. Survivorship-safe (pead_full_bars_2014). READ-ONLY, single-process, cached
only (zero GCP cost). Touches no prod module."""
import os
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "4"
import pickle, statistics
from collections import defaultdict

C = os.path.expanduser("~/.autotrader_backtest_cache")
MIN_PRICE, TURN_MIN, MIN_HIST, HURDLE, IS_END = 30.0, 10e7, 30, 0.006, "2020-12-31"

print("loading survivorship-safe daily bars ...", flush=True)
bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
print(f"  {len(bars):,} symbols\n", flush=True)

tot_on, tot_id = [], []                 # all overnight / all intraday returns
by_move = defaultdict(list)             # intraday-move bin -> overnight returns (date-tagged)
for sym, b in bars.items():
    n = len(b)
    if n < MIN_HIST + 2: continue
    d = [x[0] for x in b]; o = [x[1] for x in b]; h = [x[2] for x in b]; l = [x[3] for x in b]; c = [x[4] for x in b]; v = [x[5] for x in b]
    run = 0.0; turn = [None] * n
    for i in range(n):
        if i >= 1: run += c[i-1] * v[i-1]
        if i >= 21: run -= c[i-21] * v[i-21]
        if i >= 21: turn[i] = run / 20.0
    for j in range(MIN_HIST, n - 1):
        if c[j] < MIN_PRICE or turn[j] is None or turn[j] < TURN_MIN or o[j] <= 0 or c[j] <= 0: continue
        overnight = o[j+1] / c[j] - 1.0
        intraday = c[j] / o[j] - 1.0
        tot_on.append(overnight); tot_id.append(intraday)
        rng = h[j] - l[j]
        cpos = (c[j] - l[j]) / rng if rng > 0 else 0.5
        strong_close = cpos >= 0.8
        if intraday <= -0.03:   mv = "intraday <= -3%"
        elif intraday <= -0.01: mv = "intraday -3..-1%"
        elif intraday < 0.01:   mv = "intraday -1..+1%"
        elif intraday < 0.03:   mv = "intraday +1..+3%"
        elif intraday < 0.06:   mv = "intraday +3..+6%"
        else:                   mv = "intraday >= +6%"
        by_move[mv].append((d[j], overnight))
        if strong_close and intraday >= 0.03:
            by_move["** up>=3% & close-top20% **"].append((d[j], overnight))

print(f"=== anomaly check (gross, all {len(tot_on):,} liquid stock-days) ===", flush=True)
print(f"  avg OVERNIGHT (close->open): {statistics.mean(tot_on)*100:+.3f}% / night", flush=True)
print(f"  avg INTRADAY  (open->close): {statistics.mean(tot_id)*100:+.3f}% / day", flush=True)
print(f"  (overnight round-trip cost hurdle: {HURDLE*100:.2f}%)\n", flush=True)

def stat(recs):
    if not recs: return "   n/a   "
    xs = [r[1] for r in recs]
    net = statistics.mean(xs) - HURDLE
    return f"gross={statistics.mean(xs)*100:+5.2f}% net={net*100:+5.2f}% WR_net={100.0*sum(1 for x in xs if x>HURDLE)/len(xs):4.1f}%"

print("=== overnight return by today's intraday move (does a strong day continue overnight?) ===\n", flush=True)
for mv in ["intraday <= -3%", "intraday -3..-1%", "intraday -1..+1%", "intraday +1..+3%",
           "intraday +3..+6%", "intraday >= +6%", "** up>=3% & close-top20% **"]:
    recs = by_move.get(mv, [])
    a = [r for r in recs if r[0] <= IS_END]; z = [r for r in recs if r[0] > IS_END]
    print(f"  {mv:30}  n={len(recs):>7,}", flush=True)
    print(f"      IS(n={len(a):>6,}): {stat(a)}   OOS(n={len(z):>6,}): {stat(z)}", flush=True)
print("\nREAD: overnight is tradeable ONLY if some subset shows net>0 (gross > 0.6% hurdle)", flush=True)
print("robustly in BOTH halves. If every bin's net is negative -> cost kills it, hard rule-out.", flush=True)
