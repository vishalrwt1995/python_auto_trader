"""Insider/promoter-BUY diagnostic -- does informed open-market buying predict forward drift?
Loads cached PIT disclosures, calibrates filters (prints distinct personCategory / transaction /
acqMode), then measures forward NET return (fwd10/20/60, minus 0.7% cost+slip) for open-market
BUYS, bucketed by person category, buy size (Rs), holding-% delta, and buyer-cluster, IS(<=2020)/
OOS(>=2021) vs known ALL-liquid baselines. CRITICAL no-look-ahead: entry = NEXT trading day after
the DISCLOSURE date (public info), never the private transaction date. Dose-response (bigger buys
/ promoter>employee / holding-up => stronger fwd) is the credibility test for a real edge.
Survivorship-safe (pead_full_bars_2014). READ-ONLY, single-process, cached only (zero GCP cost)."""
import os
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "4"
import json, glob, pickle, statistics
from bisect import bisect_right
from datetime import datetime
from collections import defaultdict, Counter

C = os.path.expanduser("~/.autotrader_backtest_cache")
PIT = os.path.join(C, "insider_pit")
COST, IS_END = 0.007, "2020-12-31"
# known ALL-liquid baselines (same universe/cost) from breakout+lowvol diags:
BASE = {"fwd10": (-0.39, 0.03), "fwd20": (-0.10, 0.56), "fwd60": (1.46, 3.30)}

print("loading cached PIT disclosures ...", flush=True)
recs = []
for fn in sorted(glob.glob(os.path.join(PIT, "*.json"))):
    try: recs.extend(json.load(open(fn)))
    except Exception: pass
print(f"  {len(recs):,} raw disclosures\n", flush=True)

print("=== filter calibration (distinct values) ===", flush=True)
print("  personCategory:", dict(Counter(str(r.get('personCategory','?')) for r in recs).most_common(10)), flush=True)
print("  tdpTransactionType:", dict(Counter(str(r.get('tdpTransactionType','?')) for r in recs).most_common(8)), flush=True)
print("  acqMode:", dict(Counter(str(r.get('acqMode','?')) for r in recs).most_common(14)), flush=True)

print("\nloading survivorship-safe bars ...", flush=True)
bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
SYM = {}
for s, b in bars.items():
    if len(b) < 70: continue
    SYM[s] = {"d": [x[0] for x in b], "o": [x[1] for x in b], "c": [x[4] for x in b]}
print(f"  {len(SYM):,} symbols\n", flush=True)

INFORMED = ("promoter", "director", "key managerial", "immediate relative", "promoter group")
def is_informed(cat): return any(k in str(cat).lower() for k in INFORMED)
def fnum(x):
    try: return float(str(x).replace(",", ""))
    except Exception: return None
def disc_date(r):
    s = str(r.get("date", "")).split()[0]
    try: return datetime.strptime(s, "%d-%b-%Y").strftime("%Y-%m-%d")
    except Exception: return None

# build signals: informed OPEN-MARKET buys, dedup to one per (symbol, disclosure-day) = cluster
sig = defaultdict(lambda: {"val": 0.0, "n": 0, "dpct": 0.0, "cats": set()})
kept = 0
for r in recs:
    if "buy" not in str(r.get("tdpTransactionType", "")).lower(): continue
    if not is_informed(r.get("personCategory")): continue
    mode = str(r.get("acqMode", "")).lower()
    if "market" not in mode or "off" in mode: continue          # open-market only
    val = fnum(r.get("secVal")) or fnum(r.get("buyValue")) or 0.0
    if val < 500000: continue                                    # >= Rs 5 lakh
    dd = disc_date(r)
    if dd is None or r.get("symbol") not in SYM: continue
    k = (r["symbol"], dd); s = sig[k]
    s["val"] += val; s["n"] += 1; s["cats"].add(str(r.get("personCategory")))
    db = (fnum(r.get("afterAcqSharesPer")) or 0) - (fnum(r.get("befAcqSharesPer")) or 0)
    s["dpct"] = max(s["dpct"], db); kept += 1
print(f"informed open-market buys: {kept:,} rows -> {len(sig):,} (symbol,day) signals\n", flush=True)

# forward returns
def fwd(symrec, ref, k):
    c = symrec["c"]
    return (c[ref+k]/c[ref]-1.0-COST) if ref+k < len(c) and c[ref] > 0 else None
rows = []
for (sym, dd), s in sig.items():
    S = SYM[sym]; ref = bisect_right(S["d"], dd)        # next trading day after disclosure
    if ref >= len(S["c"]) or ref < 1: continue
    cat = ("promoter" if any("promoter" in c.lower() for c in s["cats"]) else
           "director" if any("director" in c.lower() for c in s["cats"]) else
           "kmp/rel")
    rows.append({"dd": dd, "sym": sym, "val": s["val"], "n": s["n"], "dpct": s["dpct"], "cat": cat,
                 "f10": fwd(S, ref, 10), "f20": fwd(S, ref, 20), "f60": fwd(S, ref, 60)})
print(f"matched {len(rows):,} signals to bars\n", flush=True)

def stat(pool, key):
    v = [r[key] for r in pool if r[key] is not None]
    if not v: return "   n/a   "
    return f"avg={statistics.mean(v)*100:+5.2f}% med={statistics.median(v)*100:+5.2f}% WR={100*sum(1 for x in v if x>0)/len(v):4.1f}% n={len(v)}"
def report(label, pool):
    a = [r for r in pool if r["dd"] <= IS_END]; z = [r for r in pool if r["dd"] > IS_END]
    print(f"  {label:26} (n={len(pool):>6,})", flush=True)
    for k in ("f10", "f20", "f60"):
        b = BASE[k[:1]+"wd"+k[1:]]
        print(f"      {k}  IS: {stat(a,k)}   OOS: {stat(z,k)}   [base {b[0]:+.2f}/{b[1]:+.2f}]", flush=True)

print("=== ALL informed open-market buys ===", flush=True); report("all informed buys", rows)
print("\n=== by person category ===", flush=True)
for cat in ("promoter", "director", "kmp/rel"): report(cat, [r for r in rows if r["cat"] == cat])
print("\n=== by buy size (Rs) ===", flush=True)
for lo, hi, lbl in [(5e5,25e5,"5-25 lakh"),(25e5,1e7,"25L-1cr"),(1e7,5e7,"1-5 cr"),(5e7,1e18,">5 cr")]:
    report(lbl, [r for r in rows if lo <= r["val"] < hi])
print("\n=== by holding-% increase ===", flush=True)
for lo, hi, lbl in [(0,0.1,"<0.1pp"),(0.1,0.5,"0.1-0.5pp"),(0.5,2,"0.5-2pp"),(2,100,">2pp")]:
    report(lbl, [r for r in rows if lo <= r["dpct"] < hi])
print("\n=== buyer cluster (multiple insiders same day) ===", flush=True)
report("single buyer", [r for r in rows if r["n"] == 1]); report("2+ buyers", [r for r in rows if r["n"] >= 2])
print("\nREAD: real edge if fwd BEATS baseline BOTH halves + dose-response (promoter>employee,", flush=True)
print("bigger buys/holding-up => stronger). Flat/no-dose -> noise. Promising -> full account walk.", flush=True)
