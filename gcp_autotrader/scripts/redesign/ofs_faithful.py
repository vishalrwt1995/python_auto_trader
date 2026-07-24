"""GRIND #10 OFS — FAITHFUL validation. Re-runs OFS through the canonical insider_engine.walk (real
open-price entry, 2.5xATR protective stop, full Upstox cost, risk-based sizing) — NOT the close-only walk —
to confirm the px>200DMA/h90 edge survives a realistic harness. Enriches OFS events into the insider cand
schema (entry = offerDate+5 trading days; sl = 2.5*ATR to match insider's stop convention; select = px>200DMA).
PLATEAU sweep over hold + filter to check px>200DMA/h90 is a robust plateau, not a lucky cell. Both-halves
(IS/OOS Calmar). READ-ONLY, cached."""
import sys, os, json, pickle
from bisect import bisect_right
from datetime import datetime
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import insider_engine as ie
from insider_engine import walk, show, SYM

S = "/private/tmp/claude-501/-Users-apple-Projects-Migrated-Auto-Trading-Python-GCP/439e48e8-a413-4a1d-9d0a-530e53a5e277/scratchpad"
C = os.path.expanduser("~/.autotrader_backtest_cache")
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
def sma200_at(Sd, ref):
    if ref < 200: return None
    w = Sd["c"][ref-199:ref+1]; return sum(w)/len(w) if len(w) == 200 else None

ofs = json.load(open(f"{S}/ofs_past.json"))
seen = {}
for r in ofs:
    b = base_sym(str(r.get("symbol") or "")); d = dt(r.get("offerDate"))
    if b and d: seen.setdefault((b, d), True)
cands = []
for (sym, d) in seen:
    Sd = SYM.get(sym)
    if not Sd: continue
    r0 = bisect_right(Sd["d"], d); ref = r0 + 5           # entry = offerDate + 5 trading days
    if r0 < 1 or ref >= len(Sd["c"]) or ref >= len(Sd["atr"]): continue
    atr = Sd["atr"][ref]; turn = Sd["turn"][ref]; px = Sd["c"][ref]
    if not atr or not turn or atr <= 0 or turn < 10e7 or px < 30: continue
    sma = sma200_at(Sd, ref)
    cands.append({"sym": sym, "ed": Sd["d"][ref], "ref": ref, "sl": 2.5*atr, "turn": turn,
                  "n": 1, "b200": b200_on(Sd["d"][ref]), "sec": "?",
                  "up": sma is not None and px > sma})
cands.sort(key=lambda c: c["ed"])
print(f"OFS faithful cands: {len(cands)} (px>200DMA: {sum(c['up'] for c in cands)})", flush=True)

def run(tag, hold, slots, sel):
    ie.cands = [c for c in cands if (sel is None or sel(c))]
    cfg = {"hold": hold, "slots": slots, "turn_min": 10e7, "stop_mult": 2.5, "risk_pct": 0.015}
    show(tag, walk(cfg))

print("\n=== FAITHFUL (insider_engine: open entry, 2.5ATR stop, Upstox cost, risk-sized) ===", flush=True)
print("--- ALL OFS (no name filter) ---", flush=True)
for h in (60, 90, 120): run(f"ALL h{h} s5", h, 5, None)
print("--- px>200DMA (the max-edge pick) ---", flush=True)
for h in (60, 90, 120): run(f"px>200DMA h{h} s5", h, 5, lambda c: c["up"])
print("--- PLATEAU sweep px>200DMA (hold x slots) ---", flush=True)
for h in (75, 90, 105):
    for s in (4, 5, 6): run(f"px>200DMA h{h} s{s}", h, s, lambda c: c["up"])
print("\nREAD: robust only if px>200DMA shows a PLATEAU (neighbors all IS&OOS Calmar >~0.6, graceful), not one lucky cell. Faithful IS the pre-build bar.", flush=True)
