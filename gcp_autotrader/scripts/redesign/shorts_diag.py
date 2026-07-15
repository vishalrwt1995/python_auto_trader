"""SHORT-SQUEEZE edge hunt — selection-alpha feature diagnostic (god-mode exhaustive).

Thesis (LONG side, stock-only): NSE discloses daily short-sold qty per symbol. When short pressure
spikes but price holds/reverses (or is oversold), trapped shorts cover -> upward drift = long bounce.
Pairs shorts.pkl (isolated) with survivorship-safe pead_full_bars_2014. Builds features
(short-intensity = short_qty / day_volume; short_qty absolute; price reversal; oversold RSI; dist from
20d low/high; liquidity tier) and buckets FORWARD returns (5/10/20d) IS(<=2020)/OOS(>=2021). Edge =
beats pool BOTH halves. Watching the fillability trap that killed delivery/deals (survive in LIQUID?).
Also prints the short-intensity distribution first (is the signal even meaningful?).
READ-ONLY, local, single-process, thread-capped. No prod/existing-backtest file touched."""
import os, pickle
from statistics import mean, median
from collections import defaultdict

GC = os.path.expanduser("~/.autotrader_grind_cache"); BC = os.path.expanduser("~/.autotrader_backtest_cache")
PRICE_MIN = 30.0
print("loading shorts + survivorship-safe bars ...", flush=True)
SH = pickle.load(open(f"{GC}/shorts.pkl", "rb"))
bars = pickle.load(open(f"{BC}/pead_full_bars_2014.pkl", "rb"))

# aggregate short qty per (symbol, date) (usually already unique)
short = defaultdict(int)
for (d, sym, qty) in SH:
    if qty > 0:
        short[(sym, d)] += qty

def rsi14(c):
    o = [None]*len(c)
    if len(c) < 15: return o
    g = l = 0.0
    for i in range(1, 15):
        dd = c[i]-c[i-1]; g += max(dd, 0.0); l += max(-dd, 0.0)
    g /= 14; l /= 14; o[14] = 100-100/(1+g/(l or 1e-9))
    for i in range(15, len(c)):
        dd = c[i]-c[i-1]; g = (g*13+max(dd, 0.0))/14; l = (l*13+max(-dd, 0.0))/14
        o[i] = 100-100/(1+g/(l or 1e-9))
    return o

SYM = {}
for sym, b in bars.items():
    if not b or len(b) < 60: continue
    SYM[sym] = {"d": [x[0] for x in b], "c": [x[4] for x in b], "v": [x[5] for x in b],
                "rsi": rsi14([x[4] for x in b]), "bd": {x[0]: i for i, x in enumerate(b)}}

S = []   # (yr, sint, sqty, turn, ret5, distlo, disthi, rsi, fwd5, fwd10, fwd20)
sint_all = []
for (sym, d), sq in short.items():
    Sy = SYM.get(sym)
    if Sy is None or d not in Sy["bd"]: continue
    i = Sy["bd"][d]; c = Sy["c"]; v = Sy["v"]
    if i < 20 or i + 20 >= len(c) or c[i] < PRICE_MIN or v[i] <= 0: continue
    turn = mean(c[j]*v[j] for j in range(i-20, i))
    if turn <= 0: continue
    sint = sq / v[i]                        # short-intensity = short qty / that-day volume
    sint_all.append(sint)
    ret5 = c[i]/c[i-5]-1.0 if c[i-5] > 0 else 0.0
    lo20 = min(c[i-19:i+1]); hi20 = max(c[i-19:i+1])
    distlo = (c[i]-lo20)/lo20 if lo20 > 0 else 0.0
    disthi = (hi20-c[i])/hi20 if hi20 > 0 else 0.0
    rsi = Sy["rsi"][i] or 50.0
    S.append((d[:4], sint, sq, turn, ret5, distlo, disthi, rsi,
              c[i+5]/c[i]-1.0, c[i+10]/c[i]-1.0, c[i+20]/c[i]-1.0))
print(f"  usable short-events (in bars, liquid-price): {len(S):,}", flush=True)
sa = sorted(sint_all)
print(f"  short-intensity (qty/vol) distn: p50={sa[len(sa)//2]*100:.2f}% p90={sa[int(len(sa)*.9)]*100:.2f}% "
      f"p99={sa[int(len(sa)*.99)]*100:.2f}% max={sa[-1]*100:.1f}%\n", flush=True)

F = {"fwd5": 8, "fwd10": 9, "fwd20": 10}
def pool(fw="fwd10"):
    a = [s[F[fw]] for s in S if s[0] <= "2020"]; b = [s[F[fw]] for s in S if s[0] >= "2021"]
    return mean(a)*100, mean(b)*100, len(a), len(b)
for fw in ("fwd5", "fwd10", "fwd20"):
    pis, poo, nis, noo = pool(fw)
    print(f"=== POOL {fw}: IS {pis:+.2f}% ({nis:,}) | OOS {poo:+.2f}% ({noo:,}) ===", flush=True)
print()
pis, poo, _, _ = pool("fwd10")

def scan(idx, edges, label, fw="fwd10"):
    print(f"[{label}]  (fwd10 mean%, n)")
    for lo, hi in zip([-1e18]+edges, edges+[1e18]):
        a = [s[F[fw]] for s in S if lo <= s[idx] < hi and s[0] <= "2020"]
        b = [s[F[fw]] for s in S if lo <= s[idx] < hi and s[0] >= "2021"]
        if len(a) < 30 or len(b) < 30: continue
        im, om = mean(a)*100, mean(b)*100
        tag = "  <== EDGE" if (im > pis and om > poo) else ("  (bad both)" if (im < pis and om < poo) else "")
        print(f"  {lo:>10.4f}..{hi:<10.2f} IS {im:+.2f}({len(a):>5}) | OOS {om:+.2f}({len(b):>5}){tag}", flush=True)

scan(1, [0.02, 0.05, 0.10, 0.20], "short-intensity qty/vol (squeeze pressure)")
scan(4, [-0.05, 0.0, 0.05], "ret_5d (prior move: <0 = falling into it)")
scan(5, [0.02, 0.05, 0.10], "dist from 20d-LOW (0 = at the low)")
scan(7, [35, 45, 55], "RSI (oversold = squeeze fuel)")
scan(3, [2.5e8, 1e9, 5e9], "liquidity: 20d turnover (Rs)")

print("\n=== high short-intensity (>=5%) x reversal/oversold combos (fwd10) ===", flush=True)
HI = [s for s in S if s[1] >= 0.05]
print(f"  high-sint pool: n={len(HI)}", flush=True)
def combo(lbl, pred):
    a = [s[9] for s in HI if pred(s) and s[0] <= "2020"]; b = [s[9] for s in HI if pred(s) and s[0] >= "2021"]
    if len(a) < 20 or len(b) < 20: print(f"  {lbl:38} (thin)"); return
    im, om = mean(a)*100, mean(b)*100
    tag = "  <== EDGE" if (im > pis and om > poo) else ""
    print(f"  {lbl:38} IS {im:+.2f}({len(a):>4}) | OOS {om:+.2f}({len(b):>4}){tag}", flush=True)
combo("sint>=5% & ret5<=0 (falling)", lambda s: s[4] <= 0)
combo("sint>=5% & ret5>0 (held/up)", lambda s: s[4] > 0)
combo("sint>=5% & rsi<=40 (oversold)", lambda s: s[7] <= 40)
combo("sint>=5% & distlo<=3% (at low)", lambda s: s[5] <= 0.03)
combo("sint>=5% & >=50cr liquid", lambda s: s[3] >= 5e8)
combo("sint>=5% & ret5<=0 & >=50cr", lambda s: s[4] <= 0 and s[3] >= 5e8)
print("\nRead: any bucket beating pool BOTH halves = candidate -> engine-test. Liquid+edge = fillable.", flush=True)
