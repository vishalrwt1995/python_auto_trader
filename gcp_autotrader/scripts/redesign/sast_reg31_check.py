"""GRIND #4a — SAST Reg 31 (pledge encumbrance) QUICK overlap-kill-check. SAST Reg31 'Release' = promoter
un-pledging = the EXACT corporate event the LIVE pledge channel already trades (via insider PIT pledge-revoke,
Calmar 2.18). Two regulatory feeds of the same event -> strong redundancy prior (cf. SAST29 vs insider).
Also polluted: 11.6k 'Margin Pledge for trading' releases = routine broker-margin noise, not loan-deleveraging.
Quick edge diagnostic: does SAST Reg31 Release predict fwd returns (dedup symbol+date, entry next day after
broadcast, fillable, IS/OOS vs baseline)? If yes -> same as live pledge = redundant; if noisy -> kill. Either
way expected kill. READ-ONLY."""
import os, json, pickle, statistics
from bisect import bisect_right
from datetime import datetime
from collections import Counter

C = os.path.expanduser("~/.autotrader_backtest_cache")
S = "/private/tmp/claude-501/-Users-apple-Projects-Migrated-Auto-Trading-Python-GCP/439e48e8-a413-4a1d-9d0a-530e53a5e277/scratchpad"
COST, IS_END, TURN_MIN, PRICE_MIN = 0.007, "2020-12-31", 10e7, 30.0
BASE = {"f20": (-0.10, 0.56), "f40": (0.60, 1.90), "f60": (1.46, 3.30)}

rows = json.load(open(f"{S}/sast_reg31.json"))
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
def rel(r): return "release" in str(r.get("typeOfEvent", "")).lower()
def margin(r): return "margin pledge" in str(r.get("reasonForEncumbrance", "")).lower()

def events(pred):
    seen = {}
    for r in rows:
        if not pred(r): continue
        sym = str(r.get("symbol") or "").strip().upper(); d = dt(r.get("broadcastDateTime"))
        if sym and d: seen.setdefault((sym, d), True)
    return [(s, d) for (s, d) in seen]

def build(evs):
    out = []
    for sym, d in evs:
        Sd = SYM.get(sym)
        if not Sd: continue
        ref = bisect_right(Sd["d"], d)
        if ref >= len(Sd["c"]) or ref < 1: continue
        if Sd["turn"][ref] is None or Sd["turn"][ref] < TURN_MIN or Sd["c"][ref] < PRICE_MIN: continue
        def fwd(k): return (Sd["c"][ref+k]/Sd["c"][ref]-1.0-COST) if ref+k < len(Sd["c"]) and Sd["c"][ref] > 0 else None
        out.append({"dd": d, "f20": fwd(20), "f40": fwd(40), "f60": fwd(60)})
    return out
def stat(pool, k):
    v = [r[k] for r in pool if r[k] is not None]
    return f"avg={statistics.mean(v)*100:+5.2f}% med={statistics.median(v)*100:+5.2f}% WR={100*sum(1 for x in v if x>0)/len(v):3.0f}% n={len(v)}" if v else "n/a"
def report(label, evs):
    pool = build(evs); a = [r for r in pool if r["dd"] <= IS_END]; z = [r for r in pool if r["dd"] > IS_END]
    print(f"\n>>> {label} (unique events {len(evs)}; fillable {len(pool)}; IS {len(a)}/OOS {len(z)})", flush=True)
    for k in ("f20", "f40", "f60"):
        b = BASE[k]; print(f"    {k}  IS: {stat(a,k)}   OOS: {stat(z,k)}   [base {b[0]:+.2f}/{b[1]:+.2f}]", flush=True)

print(f"{len(rows)} reg31 rows | releases: {sum(rel(r) for r in rows)} | margin-pledge: {sum(margin(r) for r in rows)}", flush=True)
print("=== SAST Reg31 pledge-RELEASE forward NET return (vs live pledge channel Calmar 2.18) ===", flush=True)
report("ALL releases", events(rel))
report("LOAN-collateral releases (excl margin-pledge)", events(lambda r: rel(r) and not margin(r)))
print("\nREAD: SAST Reg31 Release = same corporate event as the LIVE pledge channel (insider PIT revoke). If it shows the pledge edge -> redundant; if noisy -> kill. Either way not a new channel.", flush=True)
