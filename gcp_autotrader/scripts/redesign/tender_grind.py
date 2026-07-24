"""GRIND #8 — Tender Offer (buyback / takeover-open-offer / delisting tenders). Pass-1. Three theses:
BUYBACK (undervaluation + float reduction), TAKEOVER open-offer (acquisition premium -> stock re-rates
toward offer px), DELISTING (reverse-book-building squeeze). Note: offerDate = tender-window OPEN (the
board/acquirer announcement is weeks EARLIER -> may already be priced, like the killed ex-date buyback).
Entry = next trading day after offerDate. fwd5/20/60 NET, IS/OOS by type. Thin (~18/yr split 3 ways).
Survivorship-safe. READ-ONLY."""
import os, json, pickle, statistics
from bisect import bisect_right
from datetime import datetime
from collections import Counter

C = os.path.expanduser("~/.autotrader_backtest_cache")
S = "/private/tmp/claude-501/-Users-apple-Projects-Migrated-Auto-Trading-Python-GCP/439e48e8-a413-4a1d-9d0a-530e53a5e277/scratchpad"
COST, IS_END, TURN_MIN, PRICE_MIN = 0.007, "2020-12-31", 10e7, 30.0

rows = json.load(open(f"{S}/tender_past.json"))
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
def otype(r):
    o = str(r.get("offerType", "")).lower().replace(" ", "")
    if "buyback" in o: return "buyback"
    if "takeover" in o or "takeover" in o: return "takeover"
    if "delist" in o: return "delisting"
    return "other"

def build(pred):
    out = []
    for r in rows:
        if not pred(r): continue
        sym = str(r.get("symbol") or "").strip().upper(); d = dt(r.get("offerDate"))
        Sd = SYM.get(sym)
        if not Sd or not d: continue
        ref = bisect_right(Sd["d"], d)
        if ref >= len(Sd["c"]) or ref < 1: continue
        if Sd["turn"][ref] is None or Sd["turn"][ref] < TURN_MIN or Sd["c"][ref] < PRICE_MIN: continue
        def fwd(k): return (Sd["c"][ref+k]/Sd["c"][ref]-1.0-COST) if ref+k < len(Sd["c"]) and Sd["c"][ref] > 0 else None
        out.append({"dd": d, "f5": fwd(5), "f20": fwd(20), "f60": fwd(60)})
    return out
def stat(pool, k):
    v = [r[k] for r in pool if r[k] is not None]
    return f"avg={statistics.mean(v)*100:+5.2f}% med={statistics.median(v)*100:+5.2f}% WR={100*sum(1 for x in v if x>0)/len(v):3.0f}% n={len(v)}" if v else "n/a"
def report(label, pred):
    pool = build(pred); a = [r for r in pool if r["dd"] <= IS_END]; z = [r for r in pool if r["dd"] > IS_END]
    print(f"\n>>> {label} (fillable {len(pool)}; IS {len(a)}/OOS {len(z)})", flush=True)
    for k in ("f5", "f20", "f60"): print(f"    {k}  IS: {stat(a,k)}   OOS: {stat(z,k)}", flush=True)

print(f"{len(rows)} tender rows | types: {dict(Counter(otype(r) for r in rows).most_common())}", flush=True)
print("=== Tender Offer forward NET by type (entry = next day after offerDate) ===", flush=True)
report("TAKEOVER open-offer (acquisition premium)", lambda r: otype(r) == "takeover")
report("BUYBACK tender", lambda r: otype(r) == "buyback")
report("DELISTING", lambda r: otype(r) == "delisting")
print("\nREAD: thin data. Real edge = a type beats baseline (f20 ~+0.5%) both halves w/ enough events. offerDate is lagged (post-announcement) so likely priced.", flush=True)
