"""BULK/BLOCK-DEAL FOLLOW edge hunt — selection-alpha feature diagnostic (god-mode exhaustive).

Signal: after a BUY deal (institution accumulating), does the stock drift? Pairs deals.pkl
(isolated) with survivorship-safe pead_full_bars_2014. Aggregates BUY deals per (symbol,date),
builds features (deal-value / 20d-turnover = conviction; block-vs-bulk; liquidity tier; price
context) and buckets FORWARD returns (5/10/20d) IS(<=2020)/OOS(>=2021). Edge = beats pool BOTH.
Watching the fillability trap that killed delivery (does the edge survive in LIQUID names?).
READ-ONLY, local, single-process, thread-capped. No prod/existing-backtest file touched."""
import os, pickle
from statistics import mean
from collections import defaultdict

GC = os.path.expanduser("~/.autotrader_grind_cache"); BC = os.path.expanduser("~/.autotrader_backtest_cache")
PRICE_MIN = 30.0
print("loading deals + survivorship-safe bars ...", flush=True)
D = pickle.load(open(f"{GC}/deals.pkl", "rb"))
bars = pickle.load(open(f"{BC}/pead_full_bars_2014.pkl", "rb"))

# aggregate BUY deals per (symbol, date): total buy value, count, is_block
buy = defaultdict(lambda: [0.0, 0, 0])   # (sym,date) -> [buy_value, n_deals, is_block]
for src, isblk in (("block", 1), ("bulk", 0)):
    for (d, sym, bs, qty, px) in D[src]:
        if bs != "BUY" or qty <= 0 or px <= 0:
            continue
        k = (sym, d); buy[k][0] += qty * px; buy[k][1] += 1; buy[k][2] = max(buy[k][2], isblk)

SYM = {}
for sym, b in bars.items():
    if not b or len(b) < 60:
        continue
    SYM[sym] = {"d": [x[0] for x in b], "c": [x[4] for x in b], "v": [x[5] for x in b],
                "bd": {x[0]: i for i, x in enumerate(b)}}

S = []   # (yr, ratio, isblk, turn, ret5, disthi, fwd5, fwd10, fwd20)
for (sym, d), (bval, ndl, isblk) in buy.items():
    Sy = SYM.get(sym)
    if Sy is None or d not in Sy["bd"]:
        continue
    i = Sy["bd"][d]; c = Sy["c"]; v = Sy["v"]
    if i < 20 or i + 20 >= len(c) or c[i] < PRICE_MIN:
        continue
    turn = mean(c[j] * v[j] for j in range(i - 20, i))
    if turn <= 0:
        continue
    ratio = bval / turn
    ret5 = c[i] / c[i - 5] - 1.0 if c[i - 5] > 0 else 0.0
    hi20 = max(c[i - 19:i + 1]); disthi = (hi20 - c[i]) / hi20 if hi20 > 0 else 0.0
    S.append((d[:4], ratio, isblk, turn, ret5, disthi,
              c[i + 5] / c[i] - 1.0, c[i + 10] / c[i] - 1.0, c[i + 20] / c[i] - 1.0))
print(f"  BUY-deal samples (in bars, liquid-price): {len(S):,}\n", flush=True)

F = {"fwd5": 6, "fwd10": 7, "fwd20": 8}
def pool(fw="fwd10"):
    a = [s[F[fw]] for s in S if s[0] <= "2020"]; b = [s[F[fw]] for s in S if s[0] >= "2021"]
    return mean(a) * 100, mean(b) * 100, len(a), len(b)
pis, poo, nis, noo = pool()
print(f"=== POOL fwd10: IS {pis:+.2f}% ({nis:,}) | OOS {poo:+.2f}% ({noo:,}) ===\n", flush=True)

def scan(idx, edges, label, fw="fwd10"):
    print(f"[{label}]  (fwd10 mean%, n)")
    for lo, hi in zip([-1e18] + edges, edges + [1e18]):
        a = [s[F[fw]] for s in S if lo <= s[idx] < hi and s[0] <= "2020"]
        b = [s[F[fw]] for s in S if lo <= s[idx] < hi and s[0] >= "2021"]
        if len(a) < 40 or len(b) < 40:
            continue
        im, om = mean(a) * 100, mean(b) * 100
        tag = "  <== EDGE" if (im > pis and om > poo) else ("  (bad both)" if (im < pis and om < poo) else "")
        print(f"  {lo:>9.3f}..{hi:<9.1f} IS {im:+.2f}({len(a):>5}) | OOS {om:+.2f}({len(b):>5}){tag}", flush=True)

scan(1, [0.1, 0.5, 1.0, 3.0], "deal-value / 20d-turnover (conviction)")
scan(2, [1], "block(1) vs bulk(0)")
scan(3, [2.5e8, 1e9, 5e9], "liquidity: 20d turnover (Rs)")
scan(4, [-0.05, 0.0, 0.05], "price ret_5d (context)")
scan(5, [0.0, 0.05, 0.15], "dist from 20d-high")
print("\n=== high-conviction (ratio>=1x daily turnover) x liquidity ===", flush=True)
HC = [s for s in S if s[1] >= 1.0]
for lo, hi in [(-1e18, 2.5e8), (2.5e8, 1e9), (1e9, 5e9), (5e9, 1e18)]:
    a = [s[7] for s in HC if lo <= s[3] < hi and s[0] <= "2020"]; b = [s[7] for s in HC if lo <= s[3] < hi and s[0] >= "2021"]
    if len(a) < 30 and len(b) < 30:
        continue
    print(f"  turn {lo:.0e}..{hi:.0e}  IS {mean(a)*100 if a else 0:+.2f}({len(a)}) | OOS {mean(b)*100 if b else 0:+.2f}({len(b)})", flush=True)
print("\nRead: a bucket beating pool fwd10 BOTH halves = candidate -> engine-test. Liquid+edge = fillable.", flush=True)
