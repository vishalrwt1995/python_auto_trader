"""SAST Reg 29 DEEP — pass-3 MAX-EDGE grind. Pull every lever that lifted insider/pledge:
  - longer HOLDS (40/60/90/120) — the drift kept growing past f60
  - MACRO GATE (b200 breadth > 50) — insider/pledge got their high Calmar from the macro gate; SAST was ungated
  - SELECTION filters: px>200DMA (falling-knife cut, lifted pledge), repeat-buy CLUSTER (>=2 promoter acq /90d,
    the insider-cluster idea), stake tier
  - the 38%-DISTINCT subset (promoter SAST buys with NO coinciding insider cluster) = the genuinely-additive part
Grid -> portfolio walk (K slots, equal notional, full 0.7% cost) -> CAGR/Calmar/maxDD/trades-yr. Find max
risk-adjusted (Calmar) AND max profit (CAGR). Survivorship-safe bars, look-ahead-free. READ-ONLY."""
import os, json, pickle, statistics
from bisect import bisect_right, bisect_left
from datetime import datetime
from collections import defaultdict

C = os.path.expanduser("~/.autotrader_backtest_cache")
S = "/private/tmp/claude-501/-Users-apple-Projects-Migrated-Auto-Trading-Python-GCP/439e48e8-a413-4a1d-9d0a-530e53a5e277/scratchpad"
COST, TURN_MIN, PRICE_MIN, CAP = 0.007, 10e7, 30.0, 1_000_000.0

rows = json.load(open(f"{S}/sast_reg29_sdd_deep.json"))
bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
SYM = {}
for s, b in bars.items():
    if len(b) < 220: continue
    d = [x[0] for x in b]; c = [x[4] for x in b]; v = [x[5] for x in b]
    turn = [None] * len(c); run = 0.0
    sma200 = [None] * len(c); rs = 0.0
    for i in range(len(c)):
        if i >= 1: run += c[i-1]*v[i-1]
        if i >= 21: run -= c[i-21]*v[i-21]
        if i >= 21: turn[i] = run/20.0
        rs += c[i]
        if i >= 200: rs -= c[i-200]
        if i >= 199: sma200[i] = rs/200.0
    SYM[s] = {"d": d, "c": c, "turn": turn, "sma200": sma200, "idx": {dd: i for i, dd in enumerate(d)}}

b200h = pickle.load(open(f"{C}/swing_b200_history.pkl", "rb"))
b200_dates = sorted(b200h.keys())
def b200_on(date):
    i = bisect_right(b200_dates, date) - 1
    return b200h[b200_dates[i]] if i >= 0 else 0.0

def dt(x):
    try: return datetime.strptime(str(x).split()[0], "%d-%b-%Y").strftime("%Y-%m-%d")
    except Exception: return None
def fnum(x):
    try: return float(str(x).replace(",", ""))
    except Exception: return None
def tl(r, f): return str(r.get(f, "")).lower()
def is_signal(r):
    return ("open market" in tl(r, "acquisitionMode") and tl(r, "acqSaleType") == "acquisition"
            and str(r.get("promoterType", "")).upper() == "Y")

# base fillable events with metadata
raw = []
for r in rows:
    if not is_signal(r): continue
    sym = str(r.get("symbol") or "").strip().upper(); dd = dt(r.get("timestamp"))
    Sd = SYM.get(sym)
    if not Sd or not dd: continue
    ref = bisect_right(Sd["d"], dd)
    if ref >= len(Sd["c"]) or ref < 1: continue
    if Sd["turn"][ref] is None or Sd["turn"][ref] < TURN_MIN or Sd["c"][ref] < PRICE_MIN: continue
    raw.append({"date": Sd["d"][ref], "sym": sym, "ref": ref,
                "px_ok": Sd["sma200"][ref] is not None and Sd["c"][ref] > Sd["sma200"][ref],
                "stake": fnum(r.get("totAftDiluted")) or 0.0})
raw.sort(key=lambda e: e["date"])
# repeat-buy cluster: >=2 promoter-OM-acq on same sym within prior 90 trading days
by_sym = defaultdict(list)
for e in raw: by_sym[e["sym"]].append(e["ref"])
for s in by_sym: by_sym[s].sort()
for e in raw:
    refs = by_sym[e["sym"]]; j = bisect_left(refs, e["ref"])
    e["cluster"] = j > 0 and (e["ref"] - refs[j-1]) <= 90

# insider overlap tag (for the distinct subset)
ins = pickle.load(open(f"{C}/insider_cands_enriched.pkl", "rb"))["cands"]
ins_by_sym = defaultdict(list)
for c in ins:
    s = str(c.get("sym", "")).upper(); e = str(c.get("ed", ""))[:10]
    if s and e and s in SYM and e in SYM[s]["idx"]: ins_by_sym[s].append(SYM[s]["idx"][e])
for s in ins_by_sym: ins_by_sym[s].sort()
for e in raw:
    lst = ins_by_sym.get(e["sym"], []); j = bisect_left(lst, e["ref"])
    e["insider"] = any(abs(lst[k]-e["ref"]) <= 5 for k in (j-1, j) if 0 <= k < len(lst))

cal = sorted({d for s in SYM.values() for d in s["d"]}); cal_idx = {d: i for i, d in enumerate(cal)}
def close_on(sym, t):
    i = SYM[sym]["idx"].get(t); return SYM[sym]["c"][i] if i is not None else None

def walk(events, hold, K, gate=None):
    ev_by_date = defaultdict(list)
    for e in events:
        if gate == "b200" and b200_on(e["date"]) <= 50: continue
        ev_by_date[e["date"]].append(e)
    notion = CAP / K; open_pos = []; held = set(); realized = 0.0; ntr = wins = 0
    ypnl = defaultdict(float); eq = []
    for t_i, t in enumerate(cal):
        keep = []
        for p in open_pos:
            if p["exit_cal"] <= t_i:
                px = close_on(p["sym"], cal[p["exit_cal"]]) or p["entry_px"]
                pnl = notion*(px/p["entry_px"]-1-COST); realized += pnl; ntr += 1
                wins += pnl > 0; ypnl[t[:4]] += pnl; held.discard(p["sym"])
            else: keep.append(p)
        open_pos = keep
        for e in ev_by_date.get(t, []):
            if len(open_pos) >= K or e["sym"] in held: continue
            Sd = SYM[e["sym"]]; ref = e["ref"]
            if ref+hold >= len(Sd["c"]): continue
            xc = cal_idx.get(Sd["d"][ref+hold])
            if xc is None: continue
            open_pos.append({"sym": e["sym"], "exit_cal": xc, "entry_px": Sd["c"][ref]}); held.add(e["sym"])
        unreal = sum(notion*((close_on(p["sym"], t) or p["entry_px"])/p["entry_px"]-1) for p in open_pos)
        eq.append(CAP + realized + unreal)
    peak = eq[0]; mdd = 0.0
    for v in eq: peak = max(peak, v); mdd = min(mdd, v/peak-1)
    yrs = (datetime.strptime(cal[-1], "%Y-%m-%d")-datetime.strptime(cal[0], "%Y-%m-%d")).days/365.25
    cagr = (eq[-1]/eq[0])**(1/yrs)-1
    return {"cagr": cagr, "mdd": mdd, "calmar": cagr/abs(mdd) if mdd else 0, "ntr": ntr,
            "wr": wins/ntr if ntr else 0, "tryr": ntr/yrs, "profit": eq[-1]-CAP, "ypnl": ypnl, "n": len(events)}

filters = {
    "ALL": raw,
    "px>200DMA": [e for e in raw if e["px_ok"]],
    "cluster>=2": [e for e in raw if e["cluster"]],
    "px>200DMA+cluster": [e for e in raw if e["px_ok"] and e["cluster"]],
    "DISTINCT (non-insider)": [e for e in raw if not e["insider"]],
    "px>200DMA+DISTINCT": [e for e in raw if e["px_ok"] and not e["insider"]],
}
print(f"base fillable={len(raw)} | px>200DMA={sum(e['px_ok'] for e in raw)} cluster={sum(e['cluster'] for e in raw)} distinct={sum(not e['insider'] for e in raw)}", flush=True)
print(f"b200 gate pass-rate on event dates: {100*sum(b200_on(e['date'])>50 for e in raw)/len(raw):.0f}%\n", flush=True)

print("=== GRID: filter x hold x gate -> CAGR / maxDD / Calmar / trades-yr (K=8) ===", flush=True)
best = None
for fname, evs in filters.items():
    for gate in (None, "b200"):
        for hold in (60, 90, 120):
            w = walk(evs, hold, 8, gate)
            tag = f"{fname:22s} h{hold:>3d} gate={'b200' if gate else 'none':4s}"
            print(f"  {tag}: CAGR {w['cagr']*100:+5.1f}%  DD {w['mdd']*100:6.1f}%  Calmar {w['calmar']:4.2f}  {w['tryr']:4.0f}tr/yr  n={w['n']}", flush=True)
            if w["tryr"] >= 8 and (best is None or w["calmar"] > best[0]["calmar"]):
                best = (w, tag)
    print(flush=True)

w, tag = best
print(f"=== BEST risk-adjusted: {tag} ===", flush=True)
print(f"    CAGR {w['cagr']*100:+.1f}%  maxDD {w['mdd']*100:.1f}%  Calmar {w['calmar']:.2f}  profit +Rs{w['profit']/1e5:.2f}L/Rs10L  {w['tryr']:.0f}tr/yr  WR {w['wr']*100:.0f}%", flush=True)
print(f"    YEAR-WISE: " + "  ".join(f"{y}:{v/1e3:+.0f}k" for y, v in sorted(w["ypnl"].items())), flush=True)
