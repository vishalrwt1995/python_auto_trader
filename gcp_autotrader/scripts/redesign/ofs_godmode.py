"""GRIND #10 OFS — god-mode. Pass-1 found a real both-halves f60 edge (OFS overhang clears -> re-rate:
IS +2.7%/OOS +8.8%, WR 62-66%). Now the channel questions:
 (1) BETA test — is f60 real alpha or just large-cap/PSU beta? vs a TURNOVER-MATCHED control (any liquid
     name in the OFS turnover tier, sampled) BOTH halves.
 (2) ENTRY TIMING — f5 is negative (dip); does entering ~day+5 (after the dip) beat day-0?
 (3) PORTFOLIO WALK — K slots, hold 40/60/90, full 0.7% cost -> CAGR/Calmar/DD/year-wise.
 (4) OVERLAP vs insider (should be ~0 — supply event, orthogonal).
 (5) SELECTION — subscription (noOfTimes) demand.
OFS symbol has auction suffix -> map by longest bars-universe prefix. Entry ref relative to offerDate.
Survivorship-safe. READ-ONLY."""
import os, json, pickle, statistics
from bisect import bisect_right, bisect_left
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
    turn = [None]*len(c); run = 0.0
    for i in range(len(c)):
        if i >= 1: run += c[i-1]*v[i-1]
        if i >= 21: run -= c[i-21]*v[i-21]
        if i >= 21: turn[i] = run/20.0
    SYM[s] = {"d": dd, "c": c, "turn": turn, "idx": {x: i for i, x in enumerate(dd)}}
UNIV = set(SYM.keys())

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

# enrich OFS events
ofs = json.load(open(f"{S}/ofs_past.json"))
seen = {}
for r in ofs:
    b = base_sym(str(r.get("symbol") or "")); d = dt(r.get("offerDate"))
    if b and d: seen.setdefault((b, d), r)
ev = []
for (sym, d), r in seen.items():
    Sd = SYM[sym]; ref0 = bisect_right(Sd["d"], d)
    if ref0 < 1 or ref0 >= len(Sd["c"]): continue
    if Sd["turn"][ref0] is None or Sd["turn"][ref0] < TURN_MIN or Sd["c"][ref0] < PRICE_MIN: continue
    ev.append({"sym": sym, "dd": Sd["d"][ref0], "ref": ref0, "turn": Sd["turn"][ref0], "sub": fnum(r.get("noOfTimes"))})
turns = sorted(e["turn"] for e in ev); tmed = turns[len(turns)//2]
print(f"OFS fillable events: {len(ev)} | median turnover Rs{tmed/1e7:.0f}cr | subscription available: {sum(e['sub'] is not None for e in ev)}", flush=True)

def fwd(sym, ref, entry_off, hold):
    Sd = SYM[sym]; e = ref + entry_off
    if e + hold >= len(Sd["c"]) or e >= len(Sd["c"]) or Sd["c"][e] <= 0: return None
    return Sd["c"][e+hold]/Sd["c"][e] - 1.0 - COST

def stat(vals):
    v = [x for x in vals if x is not None]
    return f"avg={statistics.mean(v)*100:+5.2f}% med={statistics.median(v)*100:+5.2f}% WR={100*sum(1 for x in v if x>0)/len(v):3.0f}% n={len(v)}" if v else "n/a"

# (1) BETA control: turnover-matched universe sample (turn in [0.5x,2x] OFS median), quarterly
ctrl = []
for sym, Sd in SYM.items():
    for ref in range(60, len(Sd["c"])-90, 63):
        t = Sd["turn"][ref]
        if t and 0.5*tmed <= t <= 2*tmed and Sd["c"][ref] >= PRICE_MIN:
            ctrl.append((Sd["d"][ref], fwd(sym, ref, 0, 60)))
print("\n(1) BETA TEST — OFS f60 vs turnover-matched control (day-0 entry):", flush=True)
for lbl, lo, hi in (("IS ", "0000", IS_END), ("OOS", IS_END, "9999")):
    o = [fwd(e["sym"], e["ref"], 0, 60) for e in ev if lo < e["dd"] <= hi]
    c = [f for d, f in ctrl if lo < d <= hi]
    print(f"    {lbl} OFS: {stat(o)}   |  control: {stat(c)}", flush=True)

# (2) ENTRY TIMING
print("\n(2) ENTRY TIMING (hold 60, IS/OOS):", flush=True)
for off in (0, 3, 5, 10):
    for lbl, lo, hi in (("IS ", "0000", IS_END), ("OOS", IS_END, "9999")):
        print(f"    entry +{off:>2}d {lbl}: {stat([fwd(e['sym'], e['ref'], off, 60) for e in ev if lo < e['dd'] <= hi])}", flush=True)

# (3) PORTFOLIO WALK (entry +5, hold 40/60/90)
cal = sorted({x for s in SYM.values() for x in s["d"]}); cidx = {x: i for i, x in enumerate(cal)}
def close_on(sym, t):
    i = SYM[sym]["idx"].get(t); return SYM[sym]["c"][i] if i is not None else None
def walk(entry_off, hold, K):
    ebd = defaultdict(list)
    for e in ev:
        Sd = SYM[e["sym"]]; ei = e["ref"]+entry_off
        if ei < len(Sd["d"]): ebd[Sd["d"][ei]].append((e["sym"], ei))
    notion = CAP/K; op = []; held = set(); realized = 0.0; ntr = wins = 0; ypnl = defaultdict(float); eq = []
    for ti, t in enumerate(cal):
        keep = []
        for p in op:
            if p["xc"] <= ti:
                px = close_on(p["sym"], cal[p["xc"]]) or p["ep"]; pnl = notion*(px/p["ep"]-1-COST)
                realized += pnl; ntr += 1; wins += pnl > 0; ypnl[t[:4]] += pnl; held.discard(p["sym"])
            else: keep.append(p)
        op = keep
        for sym, ei in ebd.get(t, []):
            if len(op) >= K or sym in held: continue
            Sd = SYM[sym]
            if ei+hold >= len(Sd["c"]): continue
            xc = cidx.get(Sd["d"][ei+hold])
            if xc is None: continue
            op.append({"sym": sym, "xc": xc, "ep": Sd["c"][ei]}); held.add(sym)
        eq.append(CAP + realized + sum(notion*((close_on(p["sym"], t) or p["ep"])/p["ep"]-1) for p in op))
    peak = eq[0]; mdd = 0.0
    for v in eq: peak = max(peak, v); mdd = min(mdd, v/peak-1)
    yrs = (datetime.strptime(cal[-1], "%Y-%m-%d")-datetime.strptime(cal[0], "%Y-%m-%d")).days/365.25
    cagr = (eq[-1]/eq[0])**(1/yrs)-1
    return cagr, mdd, cagr/abs(mdd) if mdd else 0, ntr, ntr/yrs, wins/ntr if ntr else 0, dict(ypnl)
print("\n(3) PORTFOLIO WALK (entry +5d, K=6):", flush=True)
for h in (40, 60, 90):
    cg, dd, cl, n, tr, wr, yp = walk(5, h, 6)
    print(f"    hold {h}d: CAGR {cg*100:+.1f}% DD {dd*100:.1f}% Calmar {cl:.2f} {tr:.0f}tr/yr WR {wr*100:.0f}%", flush=True)
cg, dd, cl, n, tr, wr, yp = walk(5, 60, 6)
print("    h60 year-wise: " + " ".join(f"{y}:{v/1e3:+.0f}k" for y, v in sorted(yp.items())), flush=True)

# (4) OVERLAP vs insider
ins = pickle.load(open(f"{C}/insider_cands_enriched.pkl", "rb"))["cands"]
insd = defaultdict(list)
for c in ins:
    if c["sym"] in SYM: insd[c["sym"]].append(SYM[c["sym"]]["idx"].get(str(c["ed"])[:10]))
ov = sum(1 for e in ev if any(x is not None and abs(x-e["ref"]) <= 10 for x in insd.get(e["sym"], [])))
print(f"\n(4) OVERLAP vs insider: {ov}/{len(ev)} = {100*ov/len(ev):.0f}%", flush=True)

# (5) subscription selection
print("\n(5) SELECTION by subscription (noOfTimes>=1 oversubscribed, hold60 entry+5):", flush=True)
for lbl, lo, hi in (("IS ", "0000", IS_END), ("OOS", IS_END, "9999")):
    hi_sub = [fwd(e["sym"], e["ref"], 5, 60) for e in ev if e["sub"] and e["sub"] >= 1 and lo < e["dd"] <= hi]
    print(f"    oversubscribed {lbl}: {stat(hi_sub)}", flush=True)
print("\nREAD: real channel only if OFS beats the turnover-matched control BOTH halves AND Calmar clears ~0.6. Else it's large-cap beta.", flush=True)
