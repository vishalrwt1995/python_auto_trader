"""GRIND #10 — IPO post-listing drift + OFS overhang. Pass-1. IPO: buy at listing, does it drift?
(classic finding = post-IPO UNDERperformance; test mainboard EQ/BE only, exclude SME/debt). OFS
(offer-for-sale = promoter/govt selling a stake at a floor discount via auction): post-OFS the overhang
clears -> possible re-rate, OR the sale signals distress. Entry: IPO = listing day close; OFS = next day
after offerDate. fwd5/20/60 NET, IS/OOS. Survivorship-safe. OFS symbol has an auction suffix -> map by
longest bars-universe prefix. READ-ONLY."""
import os, json, pickle, statistics
from bisect import bisect_left, bisect_right
from datetime import datetime

C = os.path.expanduser("~/.autotrader_backtest_cache")
S = "/private/tmp/claude-501/-Users-apple-Projects-Migrated-Auto-Trading-Python-GCP/439e48e8-a413-4a1d-9d0a-530e53a5e277/scratchpad"
COST, IS_END, TURN_MIN, PRICE_MIN = 0.007, "2020-12-31", 10e7, 30.0
BASE = {"f5": (-0.2, 0.1), "f20": (-0.10, 0.56), "f60": (1.46, 3.30)}

bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
SYM = {}
for s, b in bars.items():
    if len(b) < 30: continue
    dd = [x[0] for x in b]; c = [x[4] for x in b]; v = [x[5] for x in b]
    turn = [None]*len(c); run = 0.0
    for i in range(len(c)):
        if i >= 1: run += c[i-1]*v[i-1]
        if i >= 21: run -= c[i-21]*v[i-21]
        if i >= 21: turn[i] = run/20.0
    SYM[s] = {"d": dd, "c": c, "turn": turn}
UNIV = set(SYM.keys())

def dt(x):
    for f in ("%d-%b-%Y", "%d-%B-%Y"):
        try: return datetime.strptime(str(x).split()[0], f).strftime("%Y-%m-%d")
        except Exception: pass
    return None
def ofs_base(sym):
    sym = sym.upper()
    if sym in UNIV: return sym
    for cut in range(len(sym)-2, 2, -1):
        if sym[:cut] in UNIV: return sym[:cut]
    return None

def fwd_from(Sd, ref):
    def f(k): return (Sd["c"][ref+k]/Sd["c"][ref]-1.0-COST) if ref+k < len(Sd["c"]) and Sd["c"][ref] > 0 else None
    return {"f5": f(5), "f20": f(20), "f60": f(60)}

def stat(pool, k):
    v = [r[k] for r in pool if r[k] is not None]
    return f"avg={statistics.mean(v)*100:+5.2f}% med={statistics.median(v)*100:+5.2f}% WR={100*sum(1 for x in v if x>0)/len(v):3.0f}% n={len(v)}" if v else "n/a"
def report(label, pool):
    a = [r for r in pool if r["dd"] <= IS_END]; z = [r for r in pool if r["dd"] > IS_END]
    print(f"\n>>> {label} (fillable {len(pool)}; IS {len(a)}/OOS {len(z)})", flush=True)
    for k in ("f5", "f20", "f60"):
        b = BASE[k]; print(f"    {k}  IS: {stat(a,k)}   OOS: {stat(z,k)}   [base {b[0]:+.2f}/{b[1]:+.2f}]", flush=True)

# IPO post-listing drift (mainboard EQ/BE, entry = listing day close)
ipo = json.load(open(f"{S}/ipo_public_past.json"))
ipev = []
for r in ipo:
    if str(r.get("securityType")) not in ("EQ", "BE"): continue
    sym = str(r.get("symbol") or "").strip().upper(); d = dt(r.get("listingDate"))
    Sd = SYM.get(sym)
    if not Sd or not d: continue
    ref = bisect_left(Sd["d"], d)
    if ref >= len(Sd["c"]) or Sd["d"][ref] != d: continue     # need the listing day present
    if Sd["turn"][ref] is None or Sd["turn"][ref] < TURN_MIN or Sd["c"][ref] < PRICE_MIN: continue
    ipev.append({"dd": d, **fwd_from(Sd, ref)})

# OFS overhang (entry = next day after offerDate)
ofs = json.load(open(f"{S}/ofs_past.json"))
ofsev = []
for r in ofs:
    base = ofs_base(str(r.get("symbol") or "")); d = dt(r.get("offerDate"))
    if not base or not d: continue
    Sd = SYM[base]; ref = bisect_right(Sd["d"], d)
    if ref >= len(Sd["c"]) or ref < 1: continue
    if Sd["turn"][ref] is None or Sd["turn"][ref] < TURN_MIN or Sd["c"][ref] < PRICE_MIN: continue
    ofsev.append({"dd": d, **fwd_from(Sd, ref)})

print(f"IPO mainboard fillable: {len(ipev)} | OFS fillable: {len(ofsev)}", flush=True)
print("=== IPO post-listing drift (mainboard, entry = listing-day close) ===", flush=True)
report("IPO listing drift", ipev)
print("\n=== OFS overhang (entry = next day after offerDate) ===", flush=True)
report("OFS drift", ofsev)
print("\nREAD: real edge = beats baseline BOTH halves. IPO usually underperforms; OFS ambiguous.", flush=True)
