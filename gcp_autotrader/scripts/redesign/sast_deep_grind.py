"""SAST Reg 29 DEEP grind — Phase-2 grind #1 (the SDD-SAST feed, 50,822 rows 2000-26; SUPERSEDES the
15.5k `type=reg29` set — deeper + carries acqSaleType/promoterType/acquisitionMode/%-stake).

THESIS: a promoter/institutional large-stake OPEN-MARKET ACQUISITION (crossing a SAST 5%/2% threshold)
is the bigger-conviction cousin of the insider open-market-buy edge -> forward outperformance. Two-sided:
market ACQUISITIONS bullish, market SALES bearish.

Data: scratchpad/sast_reg29_sdd_deep.json. Entry = NEXT trading day after `timestamp` (the public
DISSEMINATION datetime; the `acquirerDate` transaction date is earlier, so entering after `timestamp`
is genuinely look-ahead-free — same rule as the live insider channel). Fillable turn>=10cr & px>=30.
fwd10/20/60 NET (-0.7% round-trip), IS(<=2020)/OOS(>=2021) vs the all-liquid baseline. Survivorship-safe
(pead_full_bars_2014, incl delisted). READ-ONLY, single-process, cached. Pass-1 diagnostic only."""
import os, sys, json, pickle, statistics
from bisect import bisect_right
from datetime import datetime
from collections import Counter

C = os.path.expanduser("~/.autotrader_backtest_cache")
S = "/private/tmp/claude-501/-Users-apple-Projects-Migrated-Auto-Trading-Python-GCP/439e48e8-a413-4a1d-9d0a-530e53a5e277/scratchpad"
COST, IS_END, TURN_MIN, PRICE_MIN = 0.007, "2020-12-31", 10e7, 30.0
# all-liquid baseline (avg fwd NET %, IS/OOS) from the established sast_diag harness
BASE = {"f10": (-0.39, 0.03), "f20": (-0.10, 0.56), "f60": (1.46, 3.30)}

rows = json.load(open(f"{S}/sast_reg29_sdd_deep.json"))
bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
SYM = {}
for s, b in bars.items():
    if len(b) < 70:
        continue
    d = [x[0] for x in b]; c = [x[4] for x in b]; v = [x[5] for x in b]
    turn = [None] * len(c); run = 0.0
    for i in range(len(c)):
        if i >= 1: run += c[i - 1] * v[i - 1]
        if i >= 21: run -= c[i - 21] * v[i - 21]
        if i >= 21: turn[i] = run / 20.0
    SYM[s] = {"d": d, "c": c, "turn": turn}


def dt(x):
    try:
        return datetime.strptime(str(x).split()[0], "%d-%b-%Y").strftime("%Y-%m-%d")
    except Exception:
        return None


def fnum(x):
    try:
        return float(str(x).replace(",", ""))
    except Exception:
        return None


def build(pred):
    out = []
    for r in rows:
        if not pred(r):
            continue
        sym = str(r.get("symbol") or "").strip().upper()
        dd = dt(r.get("timestamp"))          # public dissemination date
        Sd = SYM.get(sym)
        if not Sd or not dd:
            continue
        ref = bisect_right(Sd["d"], dd)       # first bar strictly after disclosure = next trading day
        if ref >= len(Sd["c"]) or ref < 1:
            continue
        if Sd["turn"][ref] is None or Sd["turn"][ref] < TURN_MIN or Sd["c"][ref] < PRICE_MIN:
            continue
        def fwd(k):
            return (Sd["c"][ref + k] / Sd["c"][ref] - 1.0 - COST) if ref + k < len(Sd["c"]) and Sd["c"][ref] > 0 else None
        out.append({"dd": dd, "f10": fwd(10), "f20": fwd(20), "f60": fwd(60)})
    return out


def stat(pool, k):
    v = [r[k] for r in pool if r[k] is not None]
    if not v:
        return "n/a"
    return f"avg={statistics.mean(v) * 100:+5.2f}% med={statistics.median(v) * 100:+5.2f}% WR={100 * sum(1 for x in v if x > 0) / len(v):3.0f}% n={len(v)}"


def report(label, pool):
    a = [r for r in pool if r["dd"] <= IS_END]; z = [r for r in pool if r["dd"] > IS_END]
    print(f"\n>>> {label} (fillable n={len(pool)}; IS {len(a)} / OOS {len(z)})", flush=True)
    for k in ("f10", "f20", "f60"):
        b = BASE[k]
        print(f"    {k}  IS: {stat(a, k)}   OOS: {stat(z, k)}   [base {b[0]:+.2f}/{b[1]:+.2f}]", flush=True)


def tl(r, f):
    return str(r.get(f, "")).lower()


om = lambda r: "open market" in tl(r, "acquisitionMode")
acq = lambda r: tl(r, "acqSaleType") == "acquisition"
sale = lambda r: tl(r, "acqSaleType") == "sale"
prom = lambda r: str(r.get("promoterType", "")).upper() == "Y"

print(f"{len(rows)} SAST reg29-deep rows | {len(SYM)} symbols with bars", flush=True)
print("acqSaleType:", dict(Counter(str(r.get('acqSaleType')) for r in rows).most_common(6)), flush=True)
print("acquisitionMode:", dict(Counter(str(r.get('acquisitionMode')) for r in rows).most_common(6)), flush=True)
print("\n=== SAST Reg29 DEEP forward NET return (entry = next day after dissemination) ===", flush=True)
report("OPEN-MARKET ACQUISITION - PROMOTER", build(lambda r: om(r) and acq(r) and prom(r)))
report("OPEN-MARKET ACQUISITION - NON-PROMOTER", build(lambda r: om(r) and acq(r) and not prom(r)))
report("OPEN-MARKET ACQUISITION - ALL", build(lambda r: om(r) and acq(r)))
report("OPEN-MARKET SALE - ALL (bearish check)", build(lambda r: om(r) and sale(r)))
report("ANY-MODE ACQUISITION - ALL", build(acq))
report("OM ACQ w/ post-stake >=25% (conviction cut)", build(lambda r: om(r) and acq(r) and (fnum(r.get("totAftDiluted")) or 0) >= 25))
print("\nREAD: real edge = ACQ beats baseline BOTH halves AND SALE lags baseline. Then overlap-check vs the live insider channel before any build.", flush=True)
