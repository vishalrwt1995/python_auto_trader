"""GRIND #7 — Board Meetings. Pass-1. Most purposes map to already-tested signals (Results->PEAD,
Dividend/Buyback->killed, Bonus->corp channel). The one FRESH angle: FUND-RAISING intimations (~4.3k) —
a board meeting called to consider raising capital, disclosed EARLIER than the QIP/preferential outcome
(so 2015-26 history vs the 2023+ outcome feeds). Ambiguous a priori: growth-capex (bullish) vs dilution/
distress (bearish). Entry = next trading day after bm_timestamp (intimation disclosure, no look-ahead).
fwd5/20/60 NET, IS/OOS, incl px>200DMA split (growth-in-uptrend vs dilution). Survivorship-safe. READ-ONLY."""
import os, json, pickle, statistics
from bisect import bisect_right
from datetime import datetime

C = os.path.expanduser("~/.autotrader_backtest_cache")
S = "/private/tmp/claude-501/-Users-apple-Projects-Migrated-Auto-Trading-Python-GCP/439e48e8-a413-4a1d-9d0a-530e53a5e277/scratchpad"
COST, IS_END, TURN_MIN, PRICE_MIN = 0.007, "2020-12-31", 10e7, 30.0
BASE = {"f5": (-0.2, 0.1), "f20": (-0.10, 0.56), "f60": (1.46, 3.30)}

rows = json.load(open(f"{S}/board_meetings.json"))
bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
SYM = {}
for s, b in bars.items():
    if len(b) < 220: continue
    dd = [x[0] for x in b]; c = [x[4] for x in b]; v = [x[5] for x in b]
    turn = [None]*len(c); run = 0.0; sma = [None]*len(c); rs = 0.0
    for i in range(len(c)):
        if i >= 1: run += c[i-1]*v[i-1]
        if i >= 21: run -= c[i-21]*v[i-21]
        if i >= 21: turn[i] = run/20.0
        rs += c[i]
        if i >= 200: rs -= c[i-200]
        if i >= 199: sma[i] = rs/200.0
    SYM[s] = {"d": dd, "c": c, "turn": turn, "sma": sma}

def dt(x):
    try: return datetime.strptime(str(x).split()[0], "%d-%b-%Y").strftime("%Y-%m-%d")
    except Exception: return None
def is_fundraise(r):
    p = str(r.get("bm_purpose", "")).lower(); return "fund rais" in p or "fund-rais" in p

seen = {}
for r in rows:
    if not is_fundraise(r): continue
    sym = str(r.get("bm_symbol") or "").strip().upper(); d = dt(r.get("bm_timestamp") or r.get("bm_date"))
    if sym and d: seen.setdefault((sym, d), True)
evs = []
for (sym, d) in seen:
    Sd = SYM.get(sym)
    if not Sd: continue
    ref = bisect_right(Sd["d"], d)
    if ref >= len(Sd["c"]) or ref < 1: continue
    if Sd["turn"][ref] is None or Sd["turn"][ref] < TURN_MIN or Sd["c"][ref] < PRICE_MIN or Sd["sma"][ref] is None: continue
    up = Sd["c"][ref] > Sd["sma"][ref]
    def fwd(k): return (Sd["c"][ref+k]/Sd["c"][ref]-1.0-COST) if ref+k < len(Sd["c"]) and Sd["c"][ref] > 0 else None
    evs.append({"dd": d, "up": up, "f5": fwd(5), "f20": fwd(20), "f60": fwd(60)})

def stat(pool, k):
    v = [r[k] for r in pool if r[k] is not None]
    return f"avg={statistics.mean(v)*100:+5.2f}% med={statistics.median(v)*100:+5.2f}% WR={100*sum(1 for x in v if x>0)/len(v):3.0f}% n={len(v)}" if v else "n/a"
def report(label, pool):
    a = [r for r in pool if r["dd"] <= IS_END]; z = [r for r in pool if r["dd"] > IS_END]
    print(f"\n>>> {label} (n={len(pool)}; IS {len(a)}/OOS {len(z)})", flush=True)
    for k in ("f5", "f20", "f60"):
        b = BASE[k]; print(f"    {k}  IS: {stat(a,k)}   OOS: {stat(z,k)}   [base {b[0]:+.2f}/{b[1]:+.2f}]", flush=True)

print(f"{len(rows)} board rows | fund-raising unique events {len(seen)} | fillable {len(evs)}", flush=True)
print("=== Board Meetings FUND-RAISING intimation -> forward NET (entry = next day after intimation) ===", flush=True)
report("ALL fund-raising", evs)
report("fund-raising & px>200DMA (growth thesis)", [e for e in evs if e["up"]])
report("fund-raising & px<200DMA (dilution/distress)", [e for e in evs if not e["up"]])
print("\nREAD: real edge = a subset beats baseline BOTH halves w/ enough events. Ambiguous signal; likely dilution-driven noise.", flush=True)
