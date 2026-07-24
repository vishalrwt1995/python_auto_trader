"""GRIND #5 — Scheme of Arrangements (M&A / demerger / capital-reduction). Pass-1. THESIS: corporate
restructuring events (esp. DEMERGERS = value-unlocking) drive equity re-rating; genuinely orthogonal to
every live channel (event, not accumulation) -> clean diversifier IF it has an edge. Data has company NAME
only (no symbol/ISIN) -> build name->symbol map from datasets that carry BOTH (board_meetings sm_name->
bm_symbol, shareholding name->symbol), normalize legal suffixes. Classify scheme_details by keyword. Entry =
next trading day after `date` (scheme-doc filing). Fillable turn>=10cr & px>=30. fwd20/60/90 NET, IS/OOS by
type. Survivorship-safe. READ-ONLY."""
import os, json, pickle, re, statistics
from bisect import bisect_right
from datetime import datetime
from collections import Counter, defaultdict

C = os.path.expanduser("~/.autotrader_backtest_cache")
S = "/private/tmp/claude-501/-Users-apple-Projects-Migrated-Auto-Trading-Python-GCP/439e48e8-a413-4a1d-9d0a-530e53a5e277/scratchpad"
COST, IS_END, TURN_MIN, PRICE_MIN = 0.007, "2020-12-31", 10e7, 30.0
BASE = {"f20": (-0.10, 0.56), "f60": (1.46, 3.30), "f90": (2.40, 5.00)}

def norm(n):
    n = str(n).lower()
    n = re.sub(r"[^a-z0-9 ]", " ", n)
    for w in (" limited", " ltd", " private", " pvt", " india", " corporation", " company", " co ", " the "):
        n = n.replace(w, " ")
    return re.sub(r"\s+", " ", n).strip()

# name -> symbol map from datasets carrying both
name2sym = {}
bm = json.load(open(f"{S}/board_meetings.json"))
for r in bm:
    sym = str(r.get("bm_symbol") or "").strip().upper(); nm = r.get("sm_name") or r.get("bm_desc")
    if sym and nm: name2sym.setdefault(norm(nm), sym)
shp = json.load(open(f"{S}/shareholding_master.json"))
for r in shp:
    sym = str(r.get("symbol") or "").strip().upper(); nm = r.get("name")
    if sym and nm: name2sym.setdefault(norm(nm), sym)

bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
SYM = {}
for s, b in bars.items():
    if len(b) < 70: continue
    dd = [x[0] for x in b]; c = [x[4] for x in b]; v = [x[5] for x in b]
    turn = [None]*len(c); run = 0.0
    for i in range(len(c)):
        if i >= 1: run += c[i-1]*v[i-1]
        if i >= 21: run -= c[i-21]*v[i-21]
        if i >= 21: turn[i] = run/20.0
    SYM[s] = {"d": dd, "c": c, "turn": turn}

def dt(x):
    try: return datetime.strptime(str(x).split()[0], "%d-%b-%Y").strftime("%Y-%m-%d")
    except Exception: return None
def stype(r):
    s = str(r.get("scheme_details", "")).lower()
    if "demerg" in s or "spin" in s: return "demerger"
    if "merg" in s or "amalgamat" in s: return "merger"
    if "reduction" in s or "capital" in s: return "capital_reduction"
    return "other"

d = json.load(open(f"{S}/scheme_arrangements.json"))
mapped = 0; events = defaultdict(list)
for r in d:
    sym = name2sym.get(norm(r.get("company"))); dd = dt(r.get("date"))
    if sym: mapped += 1
    if sym and dd and sym in SYM: events[stype(r)].append((sym, dd))
print(f"{len(d)} scheme rows | name->symbol mapped: {mapped} ({100*mapped/len(d):.0f}%) | in bars universe by type: "
      + " ".join(f"{k}:{len(v)}" for k, v in events.items()), flush=True)
print("scheme_details types:", dict(Counter(stype(r) for r in d).most_common()), flush=True)

def build(evs):
    out = []
    for sym, dd in evs:
        Sd = SYM.get(sym)
        if not Sd: continue
        ref = bisect_right(Sd["d"], dd)
        if ref >= len(Sd["c"]) or ref < 1: continue
        if Sd["turn"][ref] is None or Sd["turn"][ref] < TURN_MIN or Sd["c"][ref] < PRICE_MIN: continue
        def fwd(k): return (Sd["c"][ref+k]/Sd["c"][ref]-1.0-COST) if ref+k < len(Sd["c"]) and Sd["c"][ref] > 0 else None
        out.append({"dd": dd, "f20": fwd(20), "f60": fwd(60), "f90": fwd(90)})
    return out
def stat(pool, k):
    v = [r[k] for r in pool if r[k] is not None]
    return f"avg={statistics.mean(v)*100:+5.2f}% med={statistics.median(v)*100:+5.2f}% WR={100*sum(1 for x in v if x>0)/len(v):3.0f}% n={len(v)}" if v else "n/a"
def report(label, evs):
    pool = build(evs); a = [r for r in pool if r["dd"] <= IS_END]; z = [r for r in pool if r["dd"] > IS_END]
    print(f"\n>>> {label} (mapped events {len(evs)}; fillable {len(pool)}; IS {len(a)}/OOS {len(z)})", flush=True)
    for k in ("f20", "f60", "f90"):
        b = BASE[k]; print(f"    {k}  IS: {stat(a,k)}   OOS: {stat(z,k)}   [base {b[0]:+.2f}/{b[1]:+.2f}]", flush=True)

print("\n=== Scheme of Arrangements: forward NET return by type (entry = next day after filing) ===", flush=True)
report("DEMERGER (value-unlock thesis)", events["demerger"])
report("MERGER / amalgamation", events["merger"])
report("ALL schemes", [e for v in events.values() for e in v])
print("\nREAD: real edge = a type beats baseline BOTH halves w/ enough events. Thin/heterogeneous data likely.", flush=True)
