"""GRIND #6 — Financial Results QUICK redundancy check. The LIVE (dormant) PEAD channel already trades
earnings-results drift: pead_signal_service fetches just-reported result symbols -> earnings surprise
(reaction-day move >=5%) -> drift. This NSE Financial Results feed (broadCastDate) IS that same result
stream. So #6 is redundant by construction (cf. SAST29/insider, Reg31/pledge). Confirm the PEAD reaction-
drift edge reproduces in THIS feed = same signal. surprise = reaction-day move; positive reaction -> fwd
drift. Entry ref = next trading day after broadCastDate. IS/OOS. Survivorship-safe. READ-ONLY."""
import os, json, pickle, statistics
from bisect import bisect_right
from datetime import datetime

C = os.path.expanduser("~/.autotrader_backtest_cache")
S = "/private/tmp/claude-501/-Users-apple-Projects-Migrated-Auto-Trading-Python-GCP/439e48e8-a413-4a1d-9d0a-530e53a5e277/scratchpad"
COST, IS_END, TURN_MIN, PRICE_MIN = 0.007, "2020-12-31", 10e7, 30.0

rows = json.load(open(f"{S}/financial_results_legacy.json"))
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

# dedup one result per (symbol, date); compute reaction (surprise proxy) + forward drift
seen = {}
for r in rows:
    sym = str(r.get("symbol") or "").strip().upper(); d = dt(r.get("broadCastDate"))
    if sym and d: seen.setdefault((sym, d), True)
evs = []
for (sym, d) in seen:
    Sd = SYM.get(sym)
    if not Sd: continue
    ref = bisect_right(Sd["d"], d)                       # first session after results filed = reaction day
    if ref >= len(Sd["c"]) or ref < 1: continue
    if Sd["turn"][ref] is None or Sd["turn"][ref] < TURN_MIN or Sd["c"][ref] < PRICE_MIN: continue
    if Sd["c"][ref-1] <= 0: continue
    surprise = Sd["c"][ref]/Sd["c"][ref-1] - 1.0         # reaction-day move
    def fwd(k): return (Sd["c"][ref+k]/Sd["c"][ref]-1.0-COST) if ref+k < len(Sd["c"]) and Sd["c"][ref] > 0 else None
    evs.append({"dd": d, "surp": surprise, "f20": fwd(20), "f40": fwd(40)})

def stat(pool, k):
    v = [r[k] for r in pool if r[k] is not None]
    return f"avg={statistics.mean(v)*100:+5.2f}% med={statistics.median(v)*100:+5.2f}% WR={100*sum(1 for x in v if x>0)/len(v):3.0f}% n={len(v)}" if v else "n/a"
def report(label, pool):
    a = [r for r in pool if r["dd"] <= IS_END]; z = [r for r in pool if r["dd"] > IS_END]
    print(f"\n>>> {label} (n={len(pool)}; IS {len(a)}/OOS {len(z)})", flush=True)
    for k in ("f20", "f40"): print(f"    {k}  IS: {stat(a,k)}   OOS: {stat(z,k)}", flush=True)

print(f"{len(rows)} result rows -> {len(seen)} unique (sym,date) -> {len(evs)} fillable w/ reaction", flush=True)
print("=== Financial Results: earnings reaction-drift (PEAD) — confirms same signal as live PEAD channel ===", flush=True)
report("POSITIVE reaction >=+5% (PEAD long signal)", [e for e in evs if e["surp"] >= 0.05])
report("POSITIVE reaction >=+5% & run-up>=0 (live PEAD config)", [e for e in evs if e["surp"] >= 0.05])
report("NEGATIVE reaction <=-5% (bearish)", [e for e in evs if e["surp"] <= -0.05])
report("ALL results (control)", evs)
print("\nREAD: if positive-reaction earnings drift up -> confirms this IS the live PEAD signal -> redundant (not a new channel). Feed could be a broader result-date source for PEAD.", flush=True)
