"""DELIVERY-ACCUMULATION edge hunt — selection-alpha feature diagnostic (god-mode exhaustive).

Pairs nse_delivery_daily (isolated ~/.autotrader_grind_cache/delivery.pkl) with survivorship-safe
price bars (pead_full_bars_2014.pkl). For every liquid (symbol, date) it builds delivery features
(level / spike z-score / change-vs-trail / 5d trend / qty-spike) + price context, and buckets
FORWARD returns (5/10/20d) IS(<=2022) / OOS(>=2023). Edge = a bucket beats the pool in BOTH halves.

This is the pre-portfolio scan (find WHICH delivery signal drifts). Engine-test survives later.
READ-ONLY, local, single-process, thread-capped. No prod / existing-backtest file touched."""
import os, pickle
from statistics import mean, pstdev
from collections import defaultdict

GC = os.path.expanduser("~/.autotrader_grind_cache")
BC = os.path.expanduser("~/.autotrader_backtest_cache")
PRICE_MIN, TURN_MIN = 30.0, 1e8   # fillable: >=Rs30, >=Rs10cr 20d avg turnover
IS_MAX = "2022"                    # IS <=2022, OOS >=2023

print("loading delivery + survivorship-safe price bars ...", flush=True)
deliv = pickle.load(open(f"{GC}/delivery.pkl", "rb"))
bars = pickle.load(open(f"{BC}/pead_full_bars_2014.pkl", "rb"))

S = []   # samples: (yr, pct, dz, dchg, dtrend, qz, ret5, disthi, fwd5, fwd10, fwd20)
for sym, dl in deliv.items():
    b = bars.get(sym)
    if not b or len(b) < 60:
        continue
    bd = {row[0]: i for i, row in enumerate(b)}
    c = [row[4] for row in b]; v = [row[5] for row in b]
    dd = [(d, pct, qty) for (d, pct, qty, ttl) in dl if d in bd]
    pcts = [x[1] for x in dd]; qtys = [x[2] for x in dd]
    for k in range(21, len(dd) - 21):
        d = dd[k][0]; i = bd[d]
        if i < 20 or i + 20 >= len(c) or c[i] < PRICE_MIN:
            continue
        turn = mean(c[j] * v[j] for j in range(i - 20, i))
        if turn < TURN_MIN:
            continue
        trail = pcts[k - 20:k]
        dm = mean(trail); ds = pstdev(trail) if len(trail) > 1 else 0.0
        pct = pcts[k]
        dz = (pct - dm) / ds if ds > 1e-6 else 0.0
        dchg = pct - dm
        dtrend = pcts[k] - pcts[k - 5]
        qtrail = qtys[k - 20:k]; qm = mean(qtrail) if qtrail else 0
        qz = (qtys[k] / qm) if qm > 0 else 1.0
        ret5 = c[i] / c[i - 5] - 1.0 if c[i - 5] > 0 else 0.0
        hi20 = max(c[i - 19:i + 1]); disthi = (hi20 - c[i]) / hi20 if hi20 > 0 else 0.0
        S.append((d[:4], pct, dz, dchg, dtrend, qz, ret5, disthi,
                  c[i + 5] / c[i] - 1.0, c[i + 10] / c[i] - 1.0, c[i + 20] / c[i] - 1.0))
print(f"  liquid samples: {len(S):,}\n", flush=True)

FW = {"fwd5": 8, "fwd10": 9, "fwd20": 10}
def pool(fw):
    isb = [s[FW[fw]] for s in S if s[0] <= IS_MAX]; oo = [s[FW[fw]] for s in S if s[0] >= "2023"]
    return mean(isb) * 100, mean(oo) * 100, len(isb), len(oo)
pis, poo, nis, noo = pool("fwd10")
print(f"=== POOL fwd10: IS {pis:+.2f}% ({nis:,}) | OOS {poo:+.2f}% ({noo:,}) — edge beats BOTH ===\n", flush=True)


def scan(idx, edges, label, fw="fwd10"):
    print(f"[{label}]  (fwd10 mean%, n)")
    for lo, hi in zip([-1e18] + edges, edges + [1e18]):
        isb = [s[FW[fw]] for s in S if lo <= s[idx] < hi and s[0] <= IS_MAX]
        oo = [s[FW[fw]] for s in S if lo <= s[idx] < hi and s[0] >= "2023"]
        if len(isb) < 50 or len(oo) < 50:
            continue
        im, om = mean(isb) * 100, mean(oo) * 100
        tag = "  <== EDGE" if (im > pis and om > poo) else ("  (bad both)" if (im < pis and om < poo) else "")
        print(f"  {lo:>8.1f}..{hi:<8.1f} IS {im:+.2f}({len(isb):>6}) | OOS {om:+.2f}({len(oo):>6}){tag}", flush=True)

scan(1, [40, 60, 75, 90], "delivery % LEVEL")
scan(2, [0, 1, 2, 3], "delivery SPIKE z-score (vs 20d)")
scan(3, [0, 10, 20], "delivery CHANGE vs 20d-avg (pp)")
scan(4, [-10, 0, 10], "delivery 5d TREND (pp)")
scan(5, [1.0, 2.0, 4.0], "delivery-QTY spike (x20d)")
scan(6, [-0.05, 0.0, 0.05], "price ret_5d (context)")
scan(7, [0.03, 0.10, 0.20], "dist from 20d-high (context)")

print("\n=== INTERACTION: high delivery (pct>=75) x price context ===", flush=True)
HD = [s for s in S if s[1] >= 75]
def inter(sub, idx, edges, label):
    print(f"[hi-deliv x {label}]")
    for lo, hi in zip([-1e18] + edges, edges + [1e18]):
        isb = [s[9] for s in sub if lo <= s[idx] < hi and s[0] <= IS_MAX]
        oo = [s[9] for s in sub if lo <= s[idx] < hi and s[0] >= "2023"]
        if len(isb) < 40 or len(oo) < 40:
            continue
        im, om = mean(isb) * 100, mean(oo) * 100
        tag = "  <== EDGE" if (im > pis and om > poo) else ""
        print(f"  {lo:>7.2f}..{hi:<7.2f} IS {im:+.2f}({len(isb):>5}) | OOS {om:+.2f}({len(oo):>5}){tag}", flush=True)
inter(HD, 6, [-0.05, 0.0, 0.05], "ret5")
inter(HD, 7, [0.03, 0.10], "dist-from-high")
inter(HD, 2, [1, 2], "spike-z")
print("\nRead: a delivery bucket that beats pool fwd10 in BOTH halves = candidate edge -> engine-test next.", flush=True)
