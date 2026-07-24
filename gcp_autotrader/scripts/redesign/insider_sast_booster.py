"""SAST-cluster BOOSTER validation on the CANONICAL insider_engine.walk (the exact validated god engine).
Question: does an insider open-market buy that is ALSO confirmed by a promoter SAST Reg-29 open-market
acquisition nearby mark a higher-conviction trade -> lift insider Calmar? Attaches a SAST-confirmation flag
to each insider candidate, then runs the LOCKED god config (cluster>=2 & b200>50 & Nifty>100DMA, turn>=10cr,
2.5ATR, 1.5% risk) with NO extra filter (= live channel, must reproduce the validated ~+21%/Cal~2.0 baseline)
vs +SAST-confirmed, at BOTH plateau points (h90 s10 AND h60 s8). Same bar as insider_trend_parity: additive
ONLY if it lifts Calmar in BOTH IS and OOS at BOTH points; else it's a lucky cell. READ-ONLY, cached."""
import sys, os, json
from datetime import datetime
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from insider_engine import walk, show, nifty_gate, SYM, cands

S = "/private/tmp/claude-501/-Users-apple-Projects-Migrated-Auto-Trading-Python-GCP/439e48e8-a413-4a1d-9d0a-530e53a5e277/scratchpad"
sast = json.load(open(f"{S}/sast_reg29_sdd_deep.json"))


def dt(x):
    try: return datetime.strptime(str(x).split()[0], "%d-%b-%Y").strftime("%Y-%m-%d")
    except Exception: return None
def tl(r, f): return str(r.get(f, "")).lower()
def is_sig(r):
    return ("open market" in tl(r, "acquisitionMode") and tl(r, "acqSaleType") == "acquisition"
            and str(r.get("promoterType", "")).upper() == "Y")

# per-symbol sorted SAST promoter-OM-acquisition dissemination dates
sast_by_sym = {}
for r in sast:
    if not is_sig(r): continue
    sym = str(r.get("symbol") or "").strip().upper(); d = dt(r.get("timestamp"))
    if sym and d: sast_by_sym.setdefault(sym, []).append(d)
for s in sast_by_sym: sast_by_sym[s].sort()

# per-symbol trading-day index from the insider engine's own SYM bars (same calendar as cands)
IDX = {}
def idx_of(sym, date):
    if sym not in IDX:
        Sd = SYM.get(sym)
        IDX[sym] = {d: i for i, d in enumerate(Sd["d"])} if Sd else {}
    return IDX[sym].get(date)

WIN = 10  # trading-day window for "confirmation"
n_near = n_cluster = 0
for c in cands:
    sym = c["sym"]; ed = c["ed"]; lst = sast_by_sym.get(sym, [])
    near = cluster = False
    if lst:
        ei = idx_of(sym, ed)
        if ei is not None:
            offs = [idx_of(sym, d) - ei for d in lst if idx_of(sym, d) is not None]
            near = any(abs(o) <= WIN for o in offs)
            cluster = sum(1 for o in offs if -90 <= o <= WIN) >= 2   # repeat-buy cluster ending near ed
    c["sast_near"] = near; c["sast_cluster"] = cluster
    n_near += near; n_cluster += cluster

print(f"insider cands: {len(cands)} | SAST-confirmed(near +-{WIN}d): {n_near} ({100*n_near/len(cands):.0f}%) | "
      f"SAST-cluster-confirmed: {n_cluster} ({100*n_cluster/len(cands):.0f}%)\n", flush=True)

MACRO = lambda c: c["b200"] > 50 and c["n"] >= 2 and nifty_gate(100)(c)   # locked god gate
for hold, slots, tag in ((90, 10, "PLATEAU CENTRE h90 s10"), (60, 8, "CONSERVATIVE ALT h60 s8")):
    base = {"hold": hold, "slots": slots, "turn_min": 10e7, "stop_mult": 2.5, "risk_pct": 0.015, "regime": MACRO}
    print(f"=== {tag} (canonical insider_engine.walk) ===", flush=True)
    show("NONE (= live insider baseline)", walk({**base}))
    show("+ SAST-confirmed (near)", walk({**base, "select": lambda c: c["sast_near"]}))
    show("+ SAST-cluster confirmed", walk({**base, "select": lambda c: c["sast_cluster"]}))
    print("", flush=True)
print("READ: NONE must ~reproduce validated insider Cal~2.0 (faithful). SAST booster is a real additive win", flush=True)
print("ONLY if it lifts Calmar in BOTH IS and OOS at BOTH hold/slot points; a single-cell lift = overfit, shelve.", flush=True)
