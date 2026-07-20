"""F&O-inclusion effect diagnostic -- when NSE adds a stock to derivatives, does attention/
liquidity produce a tradeable drift (an index-inclusion analog)? Detects inclusions (symbol in
F&O month M but not M-1), measures pre-inclusion run-up (20d before month start) and forward NET
return (fwd20/60/120, minus 0.7% cost+slip) at month start, IS(<=2020)/OOS(>=2021). Compare vs
the SAME-universe baseline already computed in lowvol_factor_diag: fwd20 IS -0.10%/OOS +0.56%,
fwd60 IS +1.46%/OOS +3.30%. Survivorship-safe (pead_full_bars_2014). READ-ONLY, single-process,
cached only (zero GCP cost). Touches no prod module."""
import os
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "4"
import pickle, json, statistics
from bisect import bisect_left

C = os.path.expanduser("~/.autotrader_backtest_cache")
COST, IS_END = 0.007, "2020-12-31"
bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
fno = json.load(open(f"{C}/fno_membership_by_month.json"))
months = sorted(fno.keys())

SYM = {}
for s, b in bars.items():
    if len(b) < 40: continue
    SYM[s] = {"d": [x[0] for x in b], "c": [x[4] for x in b]}

incl = []   # (month, sym)
for i in range(1, len(months)):
    prev = set(fno[months[i-1]]); cur = set(fno[months[i]])
    for s in (cur - prev): incl.append((months[i], s))
print(f"detected {len(incl):,} F&O inclusions over {len(months)} months ({months[0]}..{months[-1]})\n", flush=True)

rows = []   # (month, pre20, f20, f60, f120)
for m, s in incl:
    S = SYM.get(s)
    if not S: continue
    ref = bisect_left(S["d"], f"{m}-01")
    if ref < 25 or ref + 20 >= len(S["c"]): continue
    c = S["c"]
    pre20 = c[ref]/c[ref-20]-1.0 if c[ref-20] > 0 else None
    def fwd(k): return (c[ref+k]/c[ref]-1.0-COST) if ref+k < len(c) and c[ref] > 0 else None
    rows.append((m, pre20, fwd(20), fwd(60), fwd(120)))

def stat(vals):
    vals = [v for v in vals if v is not None]
    if not vals: return "   n/a   "
    return f"avg={statistics.mean(vals)*100:+5.2f}% med={statistics.median(vals)*100:+5.2f}% WR={100*sum(1 for v in vals if v>0)/len(vals):4.1f}% n={len(vals)}"

isr = [r for r in rows if r[0] <= f"{IS_END[:7]}"]; oosr = [r for r in rows if r[0] > f"{IS_END[:7]}"]
print(f"matched {len(rows):,} inclusions with price data  (IS={len(isr)} OOS={len(oosr)})\n", flush=True)
print("=== pre-inclusion 20d run-up ===", flush=True)
print(f"  IS : {stat([r[1] for r in isr])}", flush=True)
print(f"  OOS: {stat([r[1] for r in oosr])}\n", flush=True)
for k, idx in [("fwd20", 2), ("fwd60", 3), ("fwd120", 4)]:
    print(f"=== {k} NET return after inclusion ===", flush=True)
    print(f"  IS : {stat([r[idx] for r in isr])}", flush=True)
    print(f"  OOS: {stat([r[idx] for r in oosr])}", flush=True)
print("\n  baseline (same universe): fwd20 IS -0.10%/OOS +0.56%  |  fwd60 IS +1.46%/OOS +3.30%", flush=True)
print("  READ: edge only if post-inclusion fwd beats baseline BOTH halves. Thin event count", flush=True)
print("  (~few/month) caps capacity even if positive -> at best a small corp_action-style channel.", flush=True)
