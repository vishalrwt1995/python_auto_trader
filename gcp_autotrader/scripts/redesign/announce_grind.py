"""GRIND #11 — final queue. (a) Related-Party: 2022-24 only, no IS -> data-limited kill. (b) Voting:
metadata-nested governance-dissent, parse-heavy + documented low/no edge (same governance-noise class as
killed SHP/board) -> defer/kill. (c) ANNOUNCEMENTS firehose: the one with genuine potential (order-wins/
contracts = real positive surprises). Only the 2025 sample is local (full 2M-row history deferred), so this
is a 1-YEAR peek (NO IS/OOS) — a pulse here would warrant the heavy backfill; flat = kill. Order-win event
study: entry = next day after an_dt, fwd1/5/20 vs all-announcement control. Symbol in feed. READ-ONLY."""
import os, json, pickle, statistics
from bisect import bisect_right
from datetime import datetime

C = os.path.expanduser("~/.autotrader_backtest_cache")
S = "/private/tmp/claude-501/-Users-apple-Projects-Migrated-Auto-Trading-Python-GCP/439e48e8-a413-4a1d-9d0a-530e53a5e277/scratchpad"
COST, TURN_MIN, PRICE_MIN = 0.007, 10e7, 30.0

bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
SYM = {}
for s, b in bars.items():
    if len(b) < 40: continue
    dd = [x[0] for x in b]; c = [x[4] for x in b]; v = [x[5] for x in b]
    turn = [None]*len(c); run = 0.0
    for i in range(len(c)):
        if i >= 1: run += c[i-1]*v[i-1]
        if i >= 21: run -= c[i-21]*v[i-21]
        if i >= 21: turn[i] = run/20.0
    SYM[s] = {"d": dd, "c": c, "turn": turn}

def dt(x):
    for f in ("%d-%b-%Y", "%Y-%m-%d", "%d-%B-%Y"):
        try: return datetime.strptime(str(x).split()[0].split("T")[0], f).strftime("%Y-%m-%d")
        except Exception: pass
    return None

ann = json.load(open(f"{S}/announcements_2025.json"))
KW = ("order", "contract", "bagged", "awarded", "secures", "wins order", "receives order", "letter of award", "loa")
def is_orderwin(r):
    t = str(r.get("desc", "")).lower() + " " + str(r.get("attchmntText", ""))[:200].lower()
    if any(bad in t for bad in ("winding", "reorder", "disorder", "order of ")): pass
    return any(k in t for k in KW)

def study(pred, label):
    seen = {}
    for r in ann:
        if not pred(r): continue
        sym = str(r.get("symbol") or "").strip().upper(); d = dt(r.get("an_dt") or r.get("dt") or r.get("sort_date"))
        if sym and d: seen.setdefault((sym, d), True)
    out = []
    for (sym, d) in seen:
        Sd = SYM.get(sym)
        if not Sd: continue
        ref = bisect_right(Sd["d"], d)
        if ref >= len(Sd["c"]) or ref < 1: continue
        if Sd["turn"][ref] is None or Sd["turn"][ref] < TURN_MIN or Sd["c"][ref] < PRICE_MIN: continue
        def fwd(k): return (Sd["c"][ref+k]/Sd["c"][ref]-1.0-COST) if ref+k < len(Sd["c"]) and Sd["c"][ref] > 0 else None
        out.append({"f1": fwd(1), "f5": fwd(5), "f20": fwd(20)})
    def st(k):
        vv = [r[k] for r in out if r[k] is not None]
        return f"avg={statistics.mean(vv)*100:+5.2f}% med={statistics.median(vv)*100:+5.2f}% WR={100*sum(1 for x in vv if x>0)/len(vv):3.0f}% n={len(vv)}" if vv else "n/a"
    print(f"\n>>> {label} (fillable {len(out)})", flush=True)
    for k in ("f1", "f5", "f20"): print(f"    {k}: {st(k)}", flush=True)

print(f"Announcements 2025 sample: {len(ann)} rows (1-year peek, NO IS/OOS possible)", flush=True)
print("=== Announcements ORDER-WIN event study (2025 only) — entry next day after announcement ===", flush=True)
study(is_orderwin, "ORDER-WIN / CONTRACT announcements")
study(lambda r: True, "ALL announcements (control)")
print("\nREAD: 1-YEAR peek only. A clear order-win drift beating the control would justify the 2M-row backfill; flat/noise = kill. NOT an IS/OOS validation.", flush=True)
