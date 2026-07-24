"""GRIND #2 Shareholding Patterns — pass-2 SALVAGE. Pass-1 showed the raw promoter Q-o-Q increase is
noisy (IS f20 negative, doesn't beat the flat control). Salvage attempt: does a px>200DMA trend filter
(which rescued SAST) isolate a clean accumulation subset? CRITICAL: px>200DMA lifts EVERY name's forward
return, so the honest test is INC+trend vs FLAT+trend (matched control) — real promoter alpha only if
INC beats its own trend-matched control BOTH halves. If a subset survives -> portfolio walk -> Calmar.
Entry = next trading day after broadcastDate. fwd40/60/90 NET. IS/OOS. Survivorship-safe. READ-ONLY."""
import os, json, pickle, statistics
from bisect import bisect_right
from datetime import datetime
from collections import defaultdict

C = os.path.expanduser("~/.autotrader_backtest_cache")
S = "/private/tmp/claude-501/-Users-apple-Projects-Migrated-Auto-Trading-Python-GCP/439e48e8-a413-4a1d-9d0a-530e53a5e277/scratchpad"
COST, IS_END, TURN_MIN, PRICE_MIN, CAP = 0.007, "2020-12-31", 10e7, 30.0, 1_000_000.0

rows = json.load(open(f"{S}/shareholding_master.json"))
bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
SYM = {}
for s, b in bars.items():
    if len(b) < 220: continue
    d = [x[0] for x in b]; c = [x[4] for x in b]; v = [x[5] for x in b]
    turn = [None]*len(c); run = 0.0; sma = [None]*len(c); rs = 0.0
    for i in range(len(c)):
        if i >= 1: run += c[i-1]*v[i-1]
        if i >= 21: run -= c[i-21]*v[i-21]
        if i >= 21: turn[i] = run/20.0
        rs += c[i]
        if i >= 200: rs -= c[i-200]
        if i >= 199: sma[i] = rs/200.0
    SYM[s] = {"d": d, "c": c, "turn": turn, "sma": sma, "idx": {dd: i for i, dd in enumerate(d)}}

def dt(x):
    try: return datetime.strptime(str(x).split()[0], "%d-%b-%Y").strftime("%Y-%m-%d")
    except Exception: return None
def fnum(x):
    try: return float(str(x).replace(",", ""))
    except Exception: return None

bysym = defaultdict(list)
for r in rows:
    sym = str(r.get("symbol") or "").strip().upper(); d = dt(r.get("broadcastDate") or r.get("date")); pr = fnum(r.get("pr_and_prgrp"))
    if sym and d and pr is not None and 0 <= pr <= 100: bysym[sym].append((d, pr))
events = []
for sym, lst in bysym.items():
    lst.sort()
    for i in range(1, len(lst)):
        events.append({"sym": sym, "date": lst[i][0], "dpr": lst[i][1]-lst[i-1][1]})

def enrich(e):
    Sd = SYM.get(e["sym"])
    if not Sd: return None
    ref = bisect_right(Sd["d"], e["date"])
    if ref >= len(Sd["c"]) or ref < 1: return None
    if Sd["turn"][ref] is None or Sd["turn"][ref] < TURN_MIN or Sd["c"][ref] < PRICE_MIN: return None
    if Sd["sma"][ref] is None: return None
    e["ref"] = ref; e["up"] = Sd["c"][ref] > Sd["sma"][ref]
    def fwd(k): return (Sd["c"][ref+k]/Sd["c"][ref]-1.0-COST) if ref+k < len(Sd["c"]) and Sd["c"][ref] > 0 else None
    e["f40"], e["f60"], e["f90"] = fwd(40), fwd(60), fwd(90)
    return e
ev = [x for x in (enrich(e) for e in events) if x]

def stat(pool, k):
    v = [r[k] for r in pool if r.get(k) is not None]
    return f"avg={statistics.mean(v)*100:+5.2f}% med={statistics.median(v)*100:+5.2f}% WR={100*sum(1 for x in v if x>0)/len(v):3.0f}% n={len(v)}" if v else "n/a"
def report(label, pool):
    a = [r for r in pool if r["date"] <= IS_END]; z = [r for r in pool if r["date"] > IS_END]
    print(f"\n>>> {label} (n={len(pool)}; IS {len(a)}/OOS {len(z)})", flush=True)
    for k in ("f40", "f60", "f90"): print(f"    {k}  IS: {stat(a,k)}   OOS: {stat(z,k)}", flush=True)

print(f"{len(ev)} enriched events | px>200DMA: {sum(e['up'] for e in ev)}", flush=True)
print("=== SALVAGE: promoter INC + px>200DMA vs trend-MATCHED flat control ===", flush=True)
report("INC>=0.5 & px>200DMA", [e for e in ev if e["dpr"] >= 0.5 and e["up"]])
report("INC>=1.0 & px>200DMA", [e for e in ev if e["dpr"] >= 1.0 and e["up"]])
report("FLAT (|d|<0.25) & px>200DMA  [MATCHED CONTROL]", [e for e in ev if abs(e["dpr"]) < 0.25 and e["up"]])
report("INC>=0.5 & px<200DMA (falling-knife)", [e for e in ev if e["dpr"] >= 0.5 and not e["up"]])

# portfolio walk of the best subset (INC>=0.5 & px>200DMA), hold 60, K=10
cal = sorted({d for s in SYM.values() for d in s["d"]}); cidx = {d: i for i, d in enumerate(cal)}
def close_on(sym, t):
    i = SYM[sym]["idx"].get(t); return SYM[sym]["c"][i] if i is not None else None
def walk(evs, hold, K):
    ebd = defaultdict(list)
    for e in evs: ebd[e["date"]].append(e)
    notion = CAP/K; op = []; held = set(); realized = 0.0; ntr = wins = 0; ypnl = defaultdict(float); eq = []
    for ti, t in enumerate(cal):
        keep = []
        for p in op:
            if p["xc"] <= ti:
                px = close_on(p["sym"], cal[p["xc"]]) or p["ep"]; pnl = notion*(px/p["ep"]-1-COST)
                realized += pnl; ntr += 1; wins += pnl > 0; ypnl[t[:4]] += pnl; held.discard(p["sym"])
            else: keep.append(p)
        op = keep
        for e in ebd.get(t, []):
            if len(op) >= K or e["sym"] in held: continue
            Sd = SYM[e["sym"]]; ref = e["ref"]
            if ref+hold >= len(Sd["c"]): continue
            xc = cidx.get(Sd["d"][ref+hold])
            if xc is None: continue
            op.append({"sym": e["sym"], "xc": xc, "ep": Sd["c"][ref]}); held.add(e["sym"])
        eq.append(CAP + realized + sum(notion*((close_on(p["sym"], t) or p["ep"])/p["ep"]-1) for p in op))
    peak = eq[0]; mdd = 0.0
    for v in eq: peak = max(peak, v); mdd = min(mdd, v/peak-1)
    yrs = (datetime.strptime(cal[-1], "%Y-%m-%d")-datetime.strptime(cal[0], "%Y-%m-%d")).days/365.25
    cagr = (eq[-1]/eq[0])**(1/yrs)-1
    return cagr, mdd, cagr/abs(mdd) if mdd else 0, ntr, ntr/yrs, wins/ntr if ntr else 0, dict(ypnl)

print("\n=== PORTFOLIO WALK: INC>=0.5 & px>200DMA (K=10) ===", flush=True)
for h in (40, 60, 90):
    cagr, mdd, cal_, n, tryr, wr, yp = walk([e for e in ev if e["dpr"] >= 0.5 and e["up"]], h, 10)
    print(f"  hold {h}d: CAGR {cagr*100:+.1f}%  DD {mdd*100:.1f}%  Calmar {cal_:.2f}  {tryr:.0f}tr/yr  WR {wr*100:.0f}%", flush=True)
print("\nREAD: real promoter alpha ONLY if INC+trend beats FLAT+trend (matched control) BOTH halves. If it just matches the control, px>200DMA is doing the work, not the promoter signal -> kill.", flush=True)
