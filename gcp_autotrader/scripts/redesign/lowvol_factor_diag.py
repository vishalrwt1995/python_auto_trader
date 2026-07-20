"""Low-vol factor diagnostic (Edge B) -- is buying LOW realized-vol liquid stocks a standalone
edge (the low-vol anomaly), distinct from MOMENTUM (which only uses low-vol as a secondary
filter within momentum winners)? Cheap-first: bucket eligible stock-days by trailing 126d daily
realized vol, report forward NET return (fwd20/fwd60, minus 0.7% cost+slip), IS(<=2020)/
OOS(>=2021), vs ALL-liquid baseline. Low-vol's real value is usually risk-adjusted (lower DD /
higher Calmar / diversification), so even a return that merely MATCHES baseline with far lower
vol can be worth a walk -- but a return that LAGS baseline in both halves is a kill. Survivorship
-safe (pead_full_bars_2014). READ-ONLY, single-process, thread-capped, cached only (zero GCP
cost). Touches no prod module."""
import os
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "4"
import pickle, statistics
from collections import defaultdict

C = os.path.expanduser("~/.autotrader_backtest_cache")
MIN_PRICE, TURN_MIN, MIN_HIST, COST, IS_END = 30.0, 10e7, 130, 0.007, "2020-12-31"

print("loading survivorship-safe daily bars ...", flush=True)
bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
print(f"  {len(bars):,} symbols\n", flush=True)

B = defaultdict(list)   # bin -> (date, fwd20, fwd60)  [net]
for sym, b in bars.items():
    n = len(b)
    if n < MIN_HIST + 65: continue
    d = [x[0] for x in b]; c = [x[4] for x in b]; v = [x[5] for x in b]
    ret = [0.0] * n
    for i in range(1, n):
        ret[i] = c[i] / c[i-1] - 1.0 if c[i-1] > 0 else 0.0
    turn = [None] * n; run = 0.0
    for i in range(n):
        if i >= 1: run += c[i-1] * v[i-1]
        if i >= 21: run -= c[i-21] * v[i-21]
        if i >= 21: turn[i] = run / 20.0
    for j in range(MIN_HIST, n - 60):
        if c[j] < MIN_PRICE or turn[j] is None or turn[j] < TURN_MIN: continue
        window = ret[j-126:j]
        vol = statistics.pstdev(window)   # daily realized vol
        f20 = c[j+20] / c[j] - 1.0 - COST
        f60 = c[j+60] / c[j] - 1.0 - COST
        rec = (d[j], f20, f60)
        B["ALL_liquid (baseline)"].append(rec)
        if vol < 0.012:   lbl = "1 very-low  (<1.2%/d)"
        elif vol < 0.018: lbl = "2 low       (1.2-1.8%)"
        elif vol < 0.025: lbl = "3 mid       (1.8-2.5%)"
        elif vol < 0.035: lbl = "4 high      (2.5-3.5%)"
        else:             lbl = "5 very-high (>3.5%/d)"
        B[lbl].append(rec)

def stat(recs, idx):
    if not recs: return "   n/a   "
    xs = [r[idx] for r in recs]
    return f"avg={statistics.mean(xs)*100:+5.2f}% WR={100.0*sum(1 for x in xs if x>0)/len(xs):4.1f}%"

def report(label):
    recs = B[label]
    a = [r for r in recs if r[0] <= IS_END]; z = [r for r in recs if r[0] > IS_END]
    print(f"  {label:24}  n={len(recs):>7,}", flush=True)
    print(f"      fwd20  IS(n={len(a):>6,}): {stat(a,1)}   OOS(n={len(z):>6,}): {stat(z,1)}", flush=True)
    print(f"      fwd60  IS(n={len(a):>6,}): {stat(a,2)}   OOS(n={len(z):>6,}): {stat(z,2)}", flush=True)

print("=== forward NET return by trailing-126d realized-vol bin ===\n", flush=True)
for lbl in ["ALL_liquid (baseline)", "1 very-low  (<1.2%/d)", "2 low       (1.2-1.8%)",
            "3 mid       (1.8-2.5%)", "4 high      (2.5-3.5%)", "5 very-high (>3.5%/d)"]:
    report(lbl); print(flush=True)
print("READ: low-vol edge if bins 1-2 beat/match baseline in BOTH halves (real value is the", flush=True)
print("lower-DD/diversification a walk would show). If low-vol LAGS baseline both halves -> kill.", flush=True)
