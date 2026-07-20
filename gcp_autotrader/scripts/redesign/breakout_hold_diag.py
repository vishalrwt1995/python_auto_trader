"""Breakout-hold edge diagnostic (Edge A) -- is entering on the day a LIQUID stock closes at a
new 52-week (or 20-day) high, then held, a genuine stock-only edge, DISTINCT from the monthly
MOMENTUM channel? Cheap-first: fixed-horizon forward net return (fwd10/fwd20, minus ~0.7%
round-trip Upstox-swing cost+slip) bucketed by breakout type, IS(<=2020)/OOS(>=2021), vs an
ALL-liquid-stock baseline (the alpha-vs-beta control). If breakout buckets don't robustly beat
the baseline in BOTH halves -> it's beta, kill. If they do -> promote to a full portfolio walk.
Survivorship-safe (pead_full_bars_2014, incl delisted). Liquidity-gated (turnover_20d>=10cr) to
stay in fillable names (avoids swing's thin-stock capacity trap). READ-ONLY, single-process,
thread-capped, reuses cached bars only (zero GCP cost). Touches no prod module."""
import os
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "4"
import pickle, statistics
from collections import deque, defaultdict

C = os.path.expanduser("~/.autotrader_backtest_cache")
MIN_PRICE = 30.0
TURN_MIN = 10e7           # >= Rs 10 cr 20d-mean turnover (liquid, fillable)
MIN_HIST = 252
COST = 0.007             # ~0.58% Upstox swing round-trip + slippage, conservative
IS_END = "2020-12-31"

print("loading survivorship-safe daily bars (pead_full_bars_2014) ...", flush=True)
bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
print(f"  {len(bars):,} symbols\n", flush=True)

def prior_max(a, n):
    """out[i] = max(a[i-n .. i-1]); None if no prior window."""
    out = [None] * len(a); dq = deque()
    for i in range(len(a)):
        while dq and dq[0] < i - n: dq.popleft()
        out[i] = a[dq[0]] if dq else None
        while dq and a[dq[-1]] <= a[i]: dq.pop()
        dq.append(i)
    return out

# buckets: (label) -> list of (date, fwd10, fwd20)   [fwd already net of COST]
B = defaultdict(list)

for sym, b in bars.items():
    n = len(b)
    if n < MIN_HIST + 25: continue
    d = [x[0] for x in b]; o = [x[1] for x in b]; c = [x[4] for x in b]; v = [x[5] for x in b]
    pm252 = prior_max(c, 252)
    pm20 = prior_max(c, 20)
    # rolling 20d mean turnover (prior 20d, excl today)
    turn = [None] * n; run = 0.0
    for i in range(n):
        if i >= 1: run += c[i-1] * v[i-1]
        if i >= 21: run -= c[i-21] * v[i-21]
        if i >= 21: turn[i] = run / 20.0
    for j in range(MIN_HIST, n - 20):
        if c[j] < MIN_PRICE: continue
        if turn[j] is None or turn[j] < TURN_MIN: continue
        if pm252[j] is None or pm20[j] is None: continue
        f10 = c[j+10] / c[j] - 1.0 - COST
        f20 = c[j+20] / c[j] - 1.0 - COST
        rec = (d[j], f10, f20)
        B["ALL_liquid (baseline)"].append(rec)
        new252 = c[j] >= pm252[j]
        new20 = c[j] >= pm20[j]
        near252 = (not new252) and c[j] >= 0.98 * pm252[j]
        if new252:
            B["NEW 52wk-high"].append(rec)
        elif near252:
            B["within 2% of 52wk-high"].append(rec)
        if new20 and not new252:
            B["NEW 20d-high (not 52wk)"].append(rec)
        if new252 and new20:
            B["NEW 52wk-high (+20d confirm)"].append(rec)

def stat(recs, idx):
    if not recs: return "   n/a   "
    xs = [r[idx] for r in recs]
    avg = statistics.mean(xs) * 100
    wr = 100.0 * sum(1 for x in xs if x > 0) / len(xs)
    return f"avg={avg:+5.2f}% WR={wr:4.1f}%"

def report(label):
    recs = B[label]
    a = [r for r in recs if r[0] <= IS_END]
    z = [r for r in recs if r[0] > IS_END]
    print(f"  {label:32}  n={len(recs):>7,}", flush=True)
    print(f"      fwd10  IS(n={len(a):>6,}): {stat(a,1)}   OOS(n={len(z):>6,}): {stat(z,1)}", flush=True)
    print(f"      fwd20  IS(n={len(a):>6,}): {stat(a,2)}   OOS(n={len(z):>6,}): {stat(z,2)}", flush=True)

print("=== forward NET return (minus 0.7% cost+slip) by breakout type ===\n", flush=True)
for lbl in ["ALL_liquid (baseline)", "NEW 52wk-high", "NEW 52wk-high (+20d confirm)",
            "within 2% of 52wk-high", "NEW 20d-high (not 52wk)"]:
    report(lbl)
    print(flush=True)

print("READ: a breakout bucket is a real (non-beta) edge ONLY if its fwd return robustly", flush=True)
print("BEATS the ALL_liquid baseline in BOTH IS and OOS. If it merely matches baseline ->", flush=True)
print("the 'edge' is just liquid-stock beta and it's a kill. If it beats -> full walk next.", flush=True)
