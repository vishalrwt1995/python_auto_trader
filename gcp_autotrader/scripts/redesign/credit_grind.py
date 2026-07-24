"""GRIND #3 — SDD Credit Rating (rating-action event). Pass-1 diagnostic. THESIS: a credit-rating
UPGRADE is a fundamental-improvement event -> equity drifts up (classic event edge); DOWNGRADE bearish.
Genuinely orthogonal to insider (credit event, not insider buying) -> expected ~zero overlap = clean
diversifier candidate. Ratings are on DEBT instruments (Symbol=NOTLISTED, ISIN=bond) so map to equity via
ISIN issuer-prefix (chars[:8]; char[8]=instrument type 1=eq/7=debt). DEDUP to one event per (symbol, date)
-- companies file the same action across many debt ISINs same-day. Entry = next trading day after
BroadcastDateTime (no look-ahead). Fillable turn>=10cr & px>=30. fwd20/40/60 NET, IS/OOS vs baseline.
Survivorship-safe. READ-ONLY."""
import os, json, pickle, statistics
from bisect import bisect_right
from datetime import datetime
from collections import defaultdict

C = os.path.expanduser("~/.autotrader_backtest_cache")
S = "/private/tmp/claude-501/-Users-apple-Projects-Migrated-Auto-Trading-Python-GCP/439e48e8-a413-4a1d-9d0a-530e53a5e277/scratchpad"
COST, IS_END, TURN_MIN, PRICE_MIN = 0.007, "2020-12-31", 10e7, 30.0
BASE = {"f20": (-0.10, 0.56), "f40": (0.60, 1.90), "f60": (1.46, 3.30)}

d = json.load(open(f"{S}/sdd_credit_rating.json"))
ism = json.load(open(f"{S}/isin_symbol.json")); ism = ism.get("data", ism) if isinstance(ism, dict) else ism
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
univ = set(SYM.keys())
pref2sym = {}
for e in ism:
    isin = str(e.get("isin", "")); sym = str(e.get("symbol", "")).strip().upper()
    if len(isin) >= 9 and sym in univ: pref2sym[isin[:8]] = sym

def dt(x):
    try: return datetime.strptime(str(x).split()[0], "%d-%b-%Y").strftime("%Y-%m-%d")
    except Exception: return None
def sym_of(r):
    isin = str(r.get("ISIN", "")); return pref2sym.get(isin[:8]) if len(isin) >= 8 else None

# dedup to one event per (symbol, date, action)
def events(action):
    seen = {}
    for r in d:
        if str(r.get("RatingAction")) != action: continue
        sym = sym_of(r); dd = dt(r.get("BroadcastDateTime"))
        if not sym or not dd: continue
        seen.setdefault((sym, dd), r)
    return [{"sym": s, "date": dd} for (s, dd) in seen]

def build(evs):
    out = []
    for e in evs:
        Sd = SYM.get(e["sym"])
        if not Sd: continue
        ref = bisect_right(Sd["d"], e["date"])
        if ref >= len(Sd["c"]) or ref < 1: continue
        if Sd["turn"][ref] is None or Sd["turn"][ref] < TURN_MIN or Sd["c"][ref] < PRICE_MIN: continue
        def fwd(k): return (Sd["c"][ref+k]/Sd["c"][ref]-1.0-COST) if ref+k < len(Sd["c"]) and Sd["c"][ref] > 0 else None
        out.append({"dd": e["date"], "f20": fwd(20), "f40": fwd(40), "f60": fwd(60)})
    return out
def stat(pool, k):
    v = [r[k] for r in pool if r[k] is not None]
    return f"avg={statistics.mean(v)*100:+5.2f}% med={statistics.median(v)*100:+5.2f}% WR={100*sum(1 for x in v if x>0)/len(v):3.0f}% n={len(v)}" if v else "n/a"
def report(label, evs):
    pool = build(evs); a = [r for r in pool if r["dd"] <= IS_END]; z = [r for r in pool if r["dd"] > IS_END]
    print(f"\n>>> {label} (unique events {len(evs)}; fillable {len(pool)}; IS {len(a)}/OOS {len(z)})", flush=True)
    for k in ("f20", "f40", "f60"):
        b = BASE[k]; print(f"    {k}  IS: {stat(a,k)}   OOS: {stat(z,k)}   [base {b[0]:+.2f}/{b[1]:+.2f}]", flush=True)

print(f"{len(d)} rating rows | {len(pref2sym)} equity issuer-prefixes", flush=True)
print("=== SDD Credit Rating: forward NET return by action (entry = next day after broadcast) ===", flush=True)
report("UPGRADE", events("Upgrade"))
report("DOWNGRADE (bearish check)", events("Downgrade"))
report("REAFFIRM (control)", events("Reaffirm"))
print("\nREAD: real edge = UPGRADE beats baseline BOTH halves AND beats REAFFIRM control AND DOWNGRADE lags. Then overlap-check vs insider + portfolio walk.", flush=True)
