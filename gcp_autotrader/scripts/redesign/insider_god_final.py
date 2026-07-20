"""Insider GOD-MODE final robustness -- confirm the locked winner isn't liquidity- or sizing-
fragile: winner (b200>50 & Nifty>100DMA & cluster) across turnover floors {10,25,50cr} x
risk_pct {1.0,1.5,2.0%} at the plateau centre (h90 s10) and a conservative alt (h60 s8).
Prints the definitive locked numbers + god-mode lift vs un-engineered v3 (Calmar 0.51).
READ-ONLY, single-process, cached (zero GCP cost)."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from insider_engine import walk, show, nifty_gate, G_B200_50, G_CLUSTER
N100=nifty_gate(100)
def AND(*fs): return lambda c: all(f(c) for f in fs)
WIN=AND(G_B200_50,N100,G_CLUSTER)

print("=== liquidity robustness (winner h90 s10) ===", flush=True)
for tm in (10e7,25e7,50e7):
    show(f"turn>{int(tm/1e7)}cr", walk(dict(hold=90,slots=10,turn_min=tm,regime=WIN)))
print("\n=== sizing robustness (winner h90 s10, turn>10cr) ===", flush=True)
for rp in (0.010,0.015,0.020):
    show(f"risk={rp*100:.1f}%", walk(dict(hold=90,slots=10,risk_pct=rp,regime=WIN)))
print("\n=== the two lock candidates ===", flush=True)
show("PRIMARY  h90 s10", walk(dict(hold=90,slots=10,regime=WIN)))
show("CONSERV  h60 s8 ", walk(dict(hold=60,slots=8,regime=WIN)))
print("\n=== god-mode lift: un-engineered vs engineered ===", flush=True)
show("v3 (turn10+nifty100 only)", walk(dict(hold=60,slots=8,regime=N100)))
show("GOD (b200&nifty100&cluster h90s10)", walk(dict(hold=90,slots=10,regime=WIN)))
