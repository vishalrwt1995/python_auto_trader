"""Faithful re-confirm of the insider stock-trend overlay through the CANONICAL insider_engine.walk
(the exact validated god-mode engine, shipped-domain-consistent enriched candidate cache). Attaches a
stock px>N-DMA flag to each candidate and runs the LOCKED god config (cluster>=2 & b200>50 & Nifty>100DMA,
turn>=10cr, h90, s10, 2.5ATR, 1.5% risk) with NO trend filter (= live channel, must reproduce the
validated ~+21%/Cal~2.0 baseline) vs +px>50/100/200DMA, at the plateau centre (h90 s10) AND the
conservative alt (h60 s8). READ-ONLY, single-process, cached (zero GCP cost)."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from insider_engine import walk, show, nifty_gate, SYM, cands

def sma_at(closes, ref, n):
    if ref < n - 1 or ref >= len(closes): return None
    w = closes[ref - n + 1:ref + 1]
    return sum(w) / len(w) if len(w) == n else None
attached = 0
for c in cands:
    S = SYM.get(c["sym"]); ref = c["ref"]
    px = S["c"][ref] if (S and ref < len(S["c"])) else None
    for n, key in ((50, "a50"), (100, "a100"), (200, "a200")):
        m = sma_at(S["c"], ref, n) if S else None
        c[key] = bool(px is not None and m is not None and px > m)
    attached += 1
print(f"attached stock-trend flags to {attached} candidates\n", flush=True)

MACRO = lambda c: c["b200"] > 50 and c["n"] >= 2 and nifty_gate(100)(c)   # locked god gate
for hold, slots, tag in ((90, 10, "PLATEAU CENTRE h90 s10"), (60, 8, "CONSERVATIVE ALT h60 s8")):
    base = {"hold": hold, "slots": slots, "turn_min": 10e7, "stop_mult": 2.5, "risk_pct": 0.015, "regime": MACRO}
    print(f"=== {tag} (canonical insider_engine.walk) ===", flush=True)
    show("NONE (= live channel)", walk({**base}))
    show("+ px>50DMA", walk({**base, "select": lambda c: c["a50"]}))
    show("+ px>100DMA", walk({**base, "select": lambda c: c["a100"]}))
    show("+ px>200DMA", walk({**base, "select": lambda c: c["a200"]}))
    print("", flush=True)
print("READ: NONE must ~reproduce the validated insider baseline (harness faithful). The trend filter is")
print("a real additive win only if it lifts Calmar in BOTH IS and OOS across the DMA plateau at BOTH hold/slot points.")
