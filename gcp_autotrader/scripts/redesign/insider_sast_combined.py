"""DECISIVE test: is a separate SAST channel additive to the SYSTEM, or does it make it worse?
Enriches SAST promoter-OM-acquisition events into the EXACT insider_engine cand schema (same bars, same
ATR/turn/cost harness) and runs, all through insider_engine.walk (module-level `cands` reassigned):
  A. insider-alone @ locked god config           -> baseline (must reproduce Cal 1.84)
  B. SAST-distinct channel @ insider config       -> its deduped production standalone
  C. SAST-distinct channel @ SAST-best (h60/no-gate/cluster) -> its best-case standalone
  D. COMBINED merged book (insider + SAST-distinct, shared slots+capital) @ insider config -> decisive
Verdict: a separate SAST channel is worth it ONLY if D's Calmar > A's (1.84). Distinct = SAST events with NO
insider event within +-5 trading days on the same symbol (the overlapping ones dedup to insider in prod).
READ-ONLY, single-process, cached."""
import sys, os, json, pickle
from bisect import bisect_right, bisect_left
from datetime import datetime
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import insider_engine as ie
from insider_engine import walk, show, nifty_gate, SYM

S = "/private/tmp/claude-501/-Users-apple-Projects-Migrated-Auto-Trading-Python-GCP/439e48e8-a413-4a1d-9d0a-530e53a5e277/scratchpad"
C = os.path.expanduser("~/.autotrader_backtest_cache")
b200h = pickle.load(open(f"{C}/swing_b200_history.pkl", "rb")); b200d = sorted(b200h)
def b200_on(d):
    i = bisect_right(b200d, d) - 1
    return b200h[b200d[i]] if i >= 0 else 0.0

sast = json.load(open(f"{S}/sast_reg29_sdd_deep.json"))
def dt(x):
    try: return datetime.strptime(str(x).split()[0], "%d-%b-%Y").strftime("%Y-%m-%d")
    except Exception: return None
def tl(r, f): return str(r.get(f, "")).lower()
def is_sig(r):
    return ("open market" in tl(r, "acquisitionMode") and tl(r, "acqSaleType") == "acquisition"
            and str(r.get("promoterType", "")).upper() == "Y")

orig = list(ie.cands)
insider_ev = defaultdict(list)
for c in orig: insider_ev[c["sym"]].append(c["ref"])
for s in insider_ev: insider_ev[s].sort()

# enrich SAST promoter-OM-acq -> cand schema
evs = []
for r in sast:
    if not is_sig(r): continue
    sym = str(r.get("symbol") or "").strip().upper(); d = dt(r.get("timestamp"))
    S = SYM.get(sym)
    if not S or not d: continue
    pos = bisect_right(S["d"], d)                 # next trading day after dissemination
    if pos < 1 or pos >= len(S["d"]): continue
    evs.append({"sym": sym, "ref": pos, "ed": S["d"][pos]})
evs.sort(key=lambda e: e["ed"])
sast_refs = defaultdict(list)
for e in evs: sast_refs[e["sym"]].append(e["ref"])
for s in sast_refs: sast_refs[s].sort()

sast_cands = []
for e in evs:
    sym, ref = e["sym"], e["ref"]; S = SYM[sym]
    if ref >= len(S["atr"]): continue
    atr = S["atr"][ref]; turn = S["turn"][ref]; px = S["c"][ref]
    if not atr or not turn or atr <= 0 or px < 30: continue
    iev = insider_ev.get(sym, []); j = bisect_left(iev, ref)
    dist = not any(abs(iev[k] - ref) <= 5 for k in (j - 1, j) if 0 <= k < len(iev))
    srefs = sast_refs[sym]; jj = bisect_left(srefs, ref)
    clust = jj > 0 and (ref - srefs[jj - 1]) <= 90
    sast_cands.append({"sym": sym, "ed": e["ed"], "ref": ref, "sl": atr, "turn": turn,
                       "n": 2 if clust else 1, "b200": b200_on(e["ed"]), "sec": "?", "dist": dist})
distinct = [c for c in sast_cands if c["dist"]]
print(f"insider cands: {len(orig)} | SAST enriched: {len(sast_cands)} | SAST-distinct (non-insider): {len(distinct)}", flush=True)
print(f"sanity — insider sl vs SYM atr scale: cand.sl={orig[0]['sl']:.1f} atr[ref]={SYM[orig[0]['sym']]['atr'][orig[0]['ref']]:.1f}\n", flush=True)

MACRO = lambda c: c["b200"] > 50 and c["n"] >= 2 and nifty_gate(100)(c)
ICFG = {"hold": 90, "slots": 10, "turn_min": 10e7, "stop_mult": 2.5, "risk_pct": 0.015, "regime": MACRO}
SCFG = {"hold": 60, "slots": 8, "turn_min": 10e7, "stop_mult": 2.5, "risk_pct": 0.015, "select": lambda c: c["n"] >= 2}

def run(tag, cand_list, cfg):
    ie.cands = sorted(cand_list, key=lambda c: c["ed"]); show(tag, walk(cfg))

print("=== A. insider-alone (locked god config) — BASELINE ===", flush=True)
run("A insider only", orig, ICFG)
print("\n=== B. SAST-distinct channel @ insider config (deduped=distinct) ===", flush=True)
run("B SAST-distinct", distinct, ICFG)
print("\n=== C. SAST-distinct channel @ SAST-best (h60/no-gate/cluster) ===", flush=True)
run("C SAST-distinct best", distinct, SCFG)
print("\n=== D. COMBINED merged book (insider + SAST-distinct, shared 10 slots) @ insider config — DECISIVE ===", flush=True)
run("D combined", orig + distinct, ICFG)
print("\n=== D2. COMBINED (+ALL sast incl overlap = double-count) — worst case ===", flush=True)
run("D2 combined+all", orig + sast_cands, ICFG)
ie.cands = orig
print("\nVERDICT: separate SAST channel is additive ONLY if D Calmar > A (1.84). If D <= A, a separate channel makes the system WORSE.", flush=True)
