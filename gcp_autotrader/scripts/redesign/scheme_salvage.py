"""GRIND #5 Scheme — pass-2 SALVAGE. Pass-1: demerger too thin (IS n=11), merger marginal. Salvage:
does MERGER + px>200DMA beat a MOMENTUM-MATCHED control (any px>200DMA fillable liquid name-date)? — the
SHP lesson: px>200DMA lifts everything, so real event alpha only if merger+trend beats the trend baseline
BOTH halves. Also portfolio-walk the best subset. Entry = next day after scheme filing. Survivorship-safe.
READ-ONLY."""
import os, json, pickle, statistics
from bisect import bisect_right
from datetime import datetime
from collections import defaultdict

C = os.path.expanduser("~/.autotrader_backtest_cache")
S = "/private/tmp/claude-501/-Users-apple-Projects-Migrated-Auto-Trading-Python-GCP/439e48e8-a413-4a1d-9d0a-530e53a5e277/scratchpad"
COST, IS_END, TURN_MIN, PRICE_MIN, CAP = 0.007, "2020-12-31", 10e7, 30.0, 1_000_000.0
import re
def norm(n):
    n = str(n).lower(); n = re.sub(r"[^a-z0-9 ]", " ", n)
    for w in (" limited", " ltd", " private", " pvt", " india", " corporation", " company", " co ", " the "):
        n = n.replace(w, " ")
    return re.sub(r"\s+", " ", n).strip()
name2sym = {}
for f, nk, sk in (("board_meetings.json", "sm_name", "bm_symbol"), ("shareholding_master.json", "name", "symbol")):
    for r in json.load(open(f"{S}/{f}")):
        sym = str(r.get(sk) or "").strip().upper(); nm = r.get(nk)
        if sym and nm: name2sym.setdefault(norm(nm), sym)

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
    SYM[s] = {"d": dd, "c": c, "turn": turn, "sma": sma, "idx": {x: i for i, x in enumerate(dd)}}

def dt(x):
    try: return datetime.strptime(str(x).split()[0], "%d-%b-%Y").strftime("%Y-%m-%d")
    except Exception: return None
def fwd(Sd, ref, k): return (Sd["c"][ref+k]/Sd["c"][ref]-1.0-COST) if ref+k < len(Sd["c"]) and Sd["c"][ref] > 0 else None
def fillable(Sd, ref): return Sd["turn"][ref] is not None and Sd["turn"][ref] >= TURN_MIN and Sd["c"][ref] >= PRICE_MIN and Sd["sma"][ref] is not None

# merger events + px>200DMA
d = json.load(open(f"{S}/scheme_arrangements.json"))
def is_merger(r):
    s = str(r.get("scheme_details", "")).lower(); return "merg" in s or "amalgamat" in s
mev = []
for r in d:
    if not is_merger(r): continue
    sym = name2sym.get(norm(r.get("company"))); dd = dt(r.get("date"))
    Sd = SYM.get(sym)
    if not Sd or not dd: continue
    ref = bisect_right(Sd["d"], dd)
    if ref < 1 or ref >= len(Sd["c"]) or not fillable(Sd, ref): continue
    up = Sd["c"][ref] > Sd["sma"][ref]
    mev.append({"sym": sym, "dd": dd, "ref": ref, "up": up,
                "f20": fwd(Sd, ref, 20), "f60": fwd(Sd, ref, 60), "f90": fwd(Sd, ref, 90)})

# MOMENTUM-matched control: sample every 63rd trading day per symbol where px>200DMA & fillable
ctrl = []
for sym, Sd in SYM.items():
    for ref in range(200, len(Sd["c"]) - 90, 63):
        if fillable(Sd, ref) and Sd["c"][ref] > Sd["sma"][ref]:
            ctrl.append({"dd": Sd["d"][ref], "f20": fwd(Sd, ref, 20), "f60": fwd(Sd, ref, 60), "f90": fwd(Sd, ref, 90)})

def stat(pool, k):
    v = [r[k] for r in pool if r.get(k) is not None]
    return f"avg={statistics.mean(v)*100:+5.2f}% med={statistics.median(v)*100:+5.2f}% WR={100*sum(1 for x in v if x>0)/len(v):3.0f}% n={len(v)}" if v else "n/a"
def report(label, pool):
    a = [r for r in pool if r["dd"] <= IS_END]; z = [r for r in pool if r["dd"] > IS_END]
    print(f"\n>>> {label} (n={len(pool)}; IS {len(a)}/OOS {len(z)})", flush=True)
    for k in ("f20", "f60", "f90"): print(f"    {k}  IS: {stat(a,k)}   OOS: {stat(z,k)}", flush=True)

print(f"merger fillable events: {len(mev)} (px>200DMA: {sum(e['up'] for e in mev)}) | momentum-control samples: {len(ctrl)}", flush=True)
print("=== SALVAGE: merger+px>200DMA vs momentum-matched control (any px>200DMA liquid name) ===", flush=True)
report("MERGER & px>200DMA", [e for e in mev if e["up"]])
report("MOMENTUM CONTROL (px>200DMA, all liquid)", ctrl)

# portfolio walk merger+px>200DMA
cal = sorted({x for s in SYM.values() for x in s["d"]}); cidx = {x: i for i, x in enumerate(cal)}
def close_on(sym, t):
    i = SYM[sym]["idx"].get(t); return SYM[sym]["c"][i] if i is not None else None
def walk(evs, hold, K):
    ebd = defaultdict(list)
    for e in evs: ebd[e["dd"]].append(e)
    notion = CAP/K; op = []; held = set(); realized = 0.0; ntr = wins = 0; eq = []
    for ti, t in enumerate(cal):
        keep = []
        for p in op:
            if p["xc"] <= ti:
                px = close_on(p["sym"], cal[p["xc"]]) or p["ep"]; pnl = notion*(px/p["ep"]-1-COST)
                realized += pnl; ntr += 1; wins += pnl > 0; held.discard(p["sym"])
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
    return cagr, mdd, cagr/abs(mdd) if mdd else 0, ntr, ntr/yrs
print("\n=== PORTFOLIO WALK: merger+px>200DMA (K=8) ===", flush=True)
for h in (40, 60, 90):
    cg, dd, cal_, n, tr = walk([e for e in mev if e["up"]], h, 8)
    print(f"  hold {h}d: CAGR {cg*100:+.1f}%  DD {dd*100:.1f}%  Calmar {cal_:.2f}  {tr:.0f}tr/yr", flush=True)
print("\nREAD: real merger alpha only if merger+trend beats the momentum control BOTH halves. If it just matches, it's momentum, not M&A -> kill.", flush=True)
