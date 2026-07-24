"""GRIND #10 OFS — pass-3 MAX-EDGE. God-mode proved real orthogonal both-halves alpha (beats turnover
control), portfolio Calmar 0.48-0.55 (just under 0.6 bar), dragged by down years (2015/18/22 = bad markets).
Levers to clear 0.6: MACRO GATE (b200>50 cut bad-breadth years), OVERSUBSCRIBED selection (demand),
px>200DMA (name uptrend), entry+5 (skip dip), hold 60/90. Grid -> walk with IS/OOS Calmar SPLIT (both-halves
bar) + tr/yr (thinness guard). Plateau not peak. Survivorship-safe. READ-ONLY."""
import os, json, pickle, statistics
from bisect import bisect_right
from datetime import datetime
from collections import defaultdict

C = os.path.expanduser("~/.autotrader_backtest_cache")
S = "/private/tmp/claude-501/-Users-apple-Projects-Migrated-Auto-Trading-Python-GCP/439e48e8-a413-4a1d-9d0a-530e53a5e277/scratchpad"
COST, IS_END, TURN_MIN, PRICE_MIN, CAP = 0.007, "2020-12-31", 10e7, 30.0, 1_000_000.0

bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
SYM = {}
for s, b in bars.items():
    if len(b) < 90: continue
    dd = [x[0] for x in b]; c = [x[4] for x in b]; v = [x[5] for x in b]
    turn = [None]*len(c); run = 0.0; sma = [None]*len(c); rs = 0.0
    for i in range(len(c)):
        if i >= 1: run += c[i-1]*v[i-1]
        if i >= 21: run -= c[i-21]*v[i-21]
        if i >= 21: turn[i] = run/20.0
        rs += c[i]
        if i >= 200: rs -= c[i-200]
        if i >= 199: sma[i] = rs/200.0
    SYM[s] = {"d": dd, "c": c, "turn": turn, "sma": sma, "idx": {x: i for i, x in enumerate(dd)}}
UNIV = set(SYM.keys())
b200h = pickle.load(open(f"{C}/swing_b200_history.pkl", "rb")); b200d = sorted(b200h)
def b200_on(d):
    i = bisect_right(b200d, d)-1; return b200h[b200d[i]] if i >= 0 else 0.0

def dt(x):
    for f in ("%d-%b-%Y", "%d-%B-%Y"):
        try: return datetime.strptime(str(x).split()[0], f).strftime("%Y-%m-%d")
        except Exception: pass
    return None
def base_sym(sym):
    sym = sym.upper()
    if sym in UNIV: return sym
    for cut in range(len(sym)-2, 2, -1):
        if sym[:cut] in UNIV: return sym[:cut]
    return None
def fnum(x):
    try: return float(str(x).replace(",", "").strip())
    except Exception: return None

ofs = json.load(open(f"{S}/ofs_past.json"))
seen = {}
for r in ofs:
    b = base_sym(str(r.get("symbol") or "")); d = dt(r.get("offerDate"))
    if b and d: seen.setdefault((b, d), r)
ev = []
for (sym, d), r in seen.items():
    Sd = SYM[sym]; r0 = bisect_right(Sd["d"], d)
    if r0 < 1 or r0 >= len(Sd["c"]): continue
    if Sd["turn"][r0] is None or Sd["turn"][r0] < TURN_MIN or Sd["c"][r0] < PRICE_MIN: continue
    e5 = r0 + 5
    if e5 >= len(Sd["c"]): continue
    ev.append({"sym": sym, "eref": e5, "edate": Sd["d"][e5], "sub": fnum(r.get("noOfTimes")),
               "up": Sd["sma"][e5] is not None and Sd["c"][e5] > Sd["sma"][e5], "b200": b200_on(Sd["d"][e5])})
print(f"OFS events (entry+5): {len(ev)}", flush=True)

cal = sorted({x for s in SYM.values() for x in s["d"]}); cidx = {x: i for i, x in enumerate(cal)}
def close_on(sym, t):
    i = SYM[sym]["idx"].get(t); return SYM[sym]["c"][i] if i is not None else None
def seg_cal(curve, start_eq, y0, y1):
    if not curve or start_eq <= 0 or y1 < y0: return 0.0, 0.0
    peak = start_eq; mdd = 0.0
    for _, e in curve: peak = max(peak, e); mdd = min(mdd, e/peak-1)
    return (curve[-1][1]/start_eq)**(1/max(1, y1-y0+1))-1, mdd
def walk(evs, hold, K):
    ebd = defaultdict(list)
    for e in evs: ebd[e["edate"]].append(e)
    notion = CAP/K; op = []; held = set(); realized = 0.0; closed = []; ypnl = defaultdict(float)
    for ti, t in enumerate(cal):
        keep = []
        for p in op:
            if p["xc"] <= ti:
                px = close_on(p["sym"], cal[p["xc"]]) or p["ep"]; pnl = notion*(px/p["ep"]-1-COST)
                realized += pnl; closed.append((t, pnl)); ypnl[t[:4]] += pnl; held.discard(p["sym"])
            else: keep.append(p)
        op = keep
        for e in ebd.get(t, []):
            if len(op) >= K or e["sym"] in held: continue
            Sd = SYM[e["sym"]]; ref = e["eref"]
            if ref+hold >= len(Sd["c"]): continue
            xc = cidx.get(Sd["d"][ref+hold])
            if xc is None: continue
            op.append({"sym": e["sym"], "xc": xc, "ep": Sd["c"][ref]}); held.add(e["sym"])
    if len(closed) < 10: return None
    closed.sort(); eq = CAP; curve = []
    for t, p in closed: eq += p; curve.append((t, eq))
    y0, y1 = int(closed[0][0][:4]), int(closed[-1][0][:4])
    isp = [x for x in curve if x[0] <= IS_END]; oosp = [x for x in curve if x[0] > IS_END]
    eqis = isp[-1][1] if isp else CAP
    fc, fdd = seg_cal(curve, CAP, y0, y1); ic, idd = seg_cal(isp, CAP, y0, 2020); oc, odd = seg_cal(oosp, eqis, 2021, y1)
    yrs = (datetime.strptime(cal[-1], "%Y-%m-%d")-datetime.strptime(cal[0], "%Y-%m-%d")).days/365.25
    return {"cagr": fc, "dd": fdd, "cal": fc/abs(fdd) if fdd else 0, "iscal": ic/abs(idd) if idd else 0,
            "ooscal": oc/abs(odd) if odd else 0, "n": len(closed), "tryr": len(closed)/yrs,
            "wr": 100*sum(1 for _, p in closed if p > 0)/len(closed), "ypnl": dict(ypnl)}

filters = {
    "ALL": lambda e: True,
    "sub>=1": lambda e: e["sub"] is not None and e["sub"] >= 1,
    "sub>=2": lambda e: e["sub"] is not None and e["sub"] >= 2,
    "px>200DMA": lambda e: e["up"],
    "sub>=1 & px>200DMA": lambda e: e["sub"] is not None and e["sub"] >= 1 and e["up"],
}
print("\n=== GRID: filter x gate x hold (K=5, entry+5) -> Calmar full/IS/OOS, CAGR, tr/yr ===", flush=True)
best = None
for fn, pred in filters.items():
    for gate in ("none", "b200>50"):
        base_evs = [e for e in ev if pred(e) and (gate == "none" or e["b200"] > 50)]
        for h in (60, 90):
            r = walk(base_evs, h, 5)
            if not r: print(f"  {fn:20} {gate:8} h{h}: thin (n<10)"); continue
            tag = f"{fn:20} {gate:8} h{h}"
            print(f"  {tag}: Cal {r['cal']:.2f} (IS {r['iscal']:.2f}/OOS {r['ooscal']:.2f})  CAGR {r['cagr']*100:+.1f}%  DD {r['dd']*100:.0f}%  {r['tryr']:.0f}tr/yr WR{r['wr']:.0f}", flush=True)
            if r["tryr"] >= 4 and r["iscal"] > 0.3 and r["ooscal"] > 0.3 and (best is None or min(r["iscal"], r["ooscal"]) > min(best[0]["iscal"], best[0]["ooscal"])):
                best = (r, tag)
if best:
    r, tag = best
    print(f"\n=== BEST both-halves-robust plateau: {tag} ===", flush=True)
    print(f"    Calmar {r['cal']:.2f} (IS {r['iscal']:.2f}/OOS {r['ooscal']:.2f})  CAGR {r['cagr']*100:+.1f}%  maxDD {r['dd']*100:.1f}%  {r['tryr']:.0f}tr/yr  WR {r['wr']:.0f}%", flush=True)
    print("    year-wise: " + " ".join(f"{y}:{v/1e3:+.0f}k" for y, v in sorted(r["ypnl"].items())), flush=True)
print("\nREAD: clears the bar only if a PLATEAU (not one cell) shows IS & OOS Calmar both >~0.6 w/ >=5 tr/yr. Gating that thins to <4/yr = fragile.", flush=True)
