"""Insider GOD-MODE Phase 3+4 -- on the two cluster frontier bases from P2, grind:
(3) hold x slots, regime+selection stacking; (4) stop tightness x sector-cap x DD-governor.
Goal: push the frontier -- max CAGR AND max Calmar (min DD), robust across IS+OOS. Uses shared
insider_engine.walk. READ-ONLY, single-process, cached (zero GCP cost)."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from insider_engine import walk, show, nifty_gate, G_B200_50, G_CLUSTER

N100=nifty_gate(100)
def AND(*fs): return lambda c: all(f(c) for f in fs)

print("=== P3a: hold x slots  [base: b200>50 & cluster(2+)] ===", flush=True)
for slots in (6,8,10,12):
    for hold in (40,60,90,120):
        show(f"h{hold} s{slots}", walk(dict(hold=hold,slots=slots,regime=AND(G_B200_50,G_CLUSTER))))
print("\n=== P3a: hold x slots  [base: Nifty>100DMA & cluster(2+)] ===", flush=True)
for slots in (6,8,10,12):
    for hold in (40,60,90,120):
        show(f"h{hold} s{slots}", walk(dict(hold=hold,slots=slots,regime=AND(N100,G_CLUSTER))))

print("\n=== P3b: regime + selection STACKING (h60 s8) ===", flush=True)
CL=G_CLUSTER; B=G_B200_50
prom=lambda c:c["cat"]=="promoter"; v1cr=lambda c:c["val"]>=1e7; n3=lambda c:c["n"]>=3
tact50=lambda c:c["tact"]>50; vstr50=lambda c:c["vstress"]<50
STACKS={
 "b200>50 & cluster (ref)": AND(B,CL),
 "nifty100 & cluster (ref)": AND(N100,CL),
 "b200>50 & nifty100 & cluster": AND(B,N100,CL),
 "b200>50 & cluster & promoter": AND(B,CL,prom),
 "b200>50 & cluster & val>1cr": AND(B,CL,v1cr),
 "b200>50 & cluster & tactical>50": AND(B,CL,tact50),
 "b200>50 & cluster & vstress<50": AND(B,CL,vstr50),
 "b200>50 & n>=3 buyers": AND(B,n3),
 "nifty100 & cluster & promoter": AND(N100,CL,prom),
 "nifty100 & cluster & val>1cr": AND(N100,CL,v1cr),
 "(b200>50|nifty100) & cluster": lambda c:(B(c) or N100(c)) and CL(c),
}
for name,fn in STACKS.items(): show(name, walk(dict(hold=60,slots=8,regime=fn)))

print("\n=== P4: stop x sector-cap x DD-governor  [best low-DD base: b200>50 & cluster, h60 s8] ===", flush=True)
base=AND(G_B200_50,G_CLUSTER)
for stop in (2.0,2.5,3.0,None):
    show(f"stop={stop} sect=0 dd=0", walk(dict(hold=60,slots=8,regime=base,stop_mult=stop)))
for sc in (0,2,3):
    show(f"stop=2.5 sect={sc} dd=0", walk(dict(hold=60,slots=8,regime=base,sect_cap=sc)))
for dd in (0.0,0.15,0.20,0.25):
    show(f"stop=2.5 sect=0 dd={dd}", walk(dict(hold=60,slots=8,regime=base,dd_halt=dd)))

print("\n=== P4: same on high-CAGR base: nifty100 & cluster, h60 s8 ===", flush=True)
base2=AND(N100,G_CLUSTER)
for stop in (2.0,2.5,3.0,None):
    show(f"stop={stop} sect=0 dd=0", walk(dict(hold=60,slots=8,regime=base2,stop_mult=stop)))
for sc in (0,2,3):
    show(f"stop=2.5 sect={sc} dd=0", walk(dict(hold=60,slots=8,regime=base2,sect_cap=sc)))
for dd in (0.0,0.20,0.25,0.30):
    show(f"stop=2.5 sect=0 dd={dd}", walk(dict(hold=60,slots=8,regime=base2,dd_halt=dd)))
print("\nREAD: pick the frontier config(s) with best CAGR+Calmar robust both halves for P5 lock.", flush=True)
