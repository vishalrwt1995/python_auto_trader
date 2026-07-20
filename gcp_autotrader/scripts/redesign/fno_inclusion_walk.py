"""F&O-inclusion account walk -- quantify the inclusion drift as a real channel (the fwd20
per-signal edge beat baseline both halves; this checks it survives a 5-slot compounding account
with a trail exit + real costs, and how thin/DD-y it is). Entry = open of first trading day of
the inclusion month; exit = prod swing_exit (2.5xATR SL, 1.75R arm, 1.0R trail, 25d max hold --
capped short since edge fades by 60d). Survivorship-safe. READ-ONLY, single-process, cached
only (zero GCP cost). Imports prod modules read-only; edits none."""
import os
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "4"
import sys, pickle, json, statistics
from bisect import bisect_left
sys.path.insert(0, "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.domain.swing_exit import simulate_exit
from autotrader.backtest.costs import compute_leg_cost, CostConfig

C = os.path.expanduser("~/.autotrader_backtest_cache")
UPSTOX = CostConfig.upstox()
CAP0, SLOTS, RISK_PCT, SLIP = 200_000.0, 5, 0.015, 0.001
ATR_MULT, TRAIL_R, ACT_R, MAX_HOLD = 2.5, 1.0, 1.75, 25
MIN_PRICE, IS_END = 30.0, "2020-12-31"

bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
fno = json.load(open(f"{C}/fno_membership_by_month.json"))
months = sorted(fno.keys())

def atr14(h, l, c):
    tr = [h[0]-l[0]]
    for i in range(1, len(c)): tr.append(max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])))
    out = [None]*len(c); s = 0.0
    for i in range(len(tr)):
        s += tr[i]
        if i >= 14: s -= tr[i-14]
        if i >= 13: out[i] = s/14.0
    return out

SYM = {}
for s, b in bars.items():
    if len(b) < 40: continue
    d=[x[0] for x in b]; o=[x[1] for x in b]; h=[x[2] for x in b]; l=[x[3] for x in b]; c=[x[4] for x in b]
    SYM[s] = {"d":d,"o":o,"c":c,"bars":b,"atr":atr14(h,l,c)}

cands = []
for i in range(1, len(months)):
    prev = set(fno[months[i-1]])
    for s in (set(fno[months[i]]) - prev):
        S = SYM.get(s)
        if not S: continue
        ref = bisect_left(S["d"], f"{months[i]}-01")
        if ref < 20 or ref+1 >= len(S["c"]): continue
        a = S["atr"][ref]
        if not a or a <= 0 or S["o"][ref] < MIN_PRICE: continue
        cands.append((S["d"][ref], s, ref, ATR_MULT*a))
cands.sort()

equity = CAP0; free = [""]*SLOTS; openpos = []; trades = []; peak = equity; maxdd = 0.0
for entry_d, s, ei, sl_dist in cands:
    still = []
    for xd, pnl in openpos:
        if xd <= entry_d: equity += pnl; peak = max(peak, equity); maxdd = min(maxdd, equity/peak-1.0)
        else: still.append((xd, pnl))
    openpos = still
    for k in range(SLOTS):
        if free[k] and free[k] <= entry_d: free[k] = ""
    slot = next((k for k in range(SLOTS) if not free[k]), None)
    if slot is None: continue
    S = SYM[s]; entry_px = S["o"][ei]
    if entry_px <= 0: continue
    qty = int((RISK_PCT*equity)//sl_dist)
    if qty < 1: continue
    if qty*entry_px > equity/SLOTS: qty = int((equity/SLOTS)//entry_px)
    if qty < 1: continue
    off, exit_px, _ = simulate_exit(S["bars"], ei, True, sl_dist, MAX_HOLD, trail_R=TRAIL_R, activate_R=ACT_R)
    xi = min(ei+off, len(S["bars"])-1); xd = S["d"][xi]
    ef = entry_px*(1+SLIP); xf = exit_px*(1-SLIP)
    pnl = (xf-ef)*qty - (compute_leg_cost(side="BUY", qty=qty, price=ef, is_swing=True, cfg=UPSTOX)
                         + compute_leg_cost(side="SELL", qty=qty, price=xf, is_swing=True, cfg=UPSTOX))
    free[slot] = xd; openpos.append((xd, pnl))
    trades.append({"ed":entry_d, "xd":xd, "sym":s, "pnl":pnl, "R":pnl/(sl_dist*qty)})
for xd, pnl in openpos:
    equity += pnl; peak = max(peak, equity); maxdd = min(maxdd, equity/peak-1.0)

span = int(max(t["xd"] for t in trades)[:4]) - int(min(t["ed"] for t in trades)[:4]) + 1
cagr = (equity/CAP0)**(1/span) - 1
wr = 100*sum(1 for t in trades if t["pnl"]>0)/len(trades)
isr = [t["R"] for t in trades if t["ed"] <= IS_END]; oosr = [t["R"] for t in trades if t["ed"] > IS_END]
print("=== F&O-INCLUSION ACCOUNT WALK (trail exit, 5-slot compounding, real Upstox costs) ===\n", flush=True)
print(f"  final=Rs{equity:,.0f}  CAGR={cagr*100:+.1f}%  maxDD={maxdd*100:.1f}%  "
      f"Calmar={cagr/abs(maxdd) if maxdd<0 else 0:.2f}", flush=True)
print(f"  trades={len(trades)} (~{len(trades)/span:.0f}/yr)  WR={wr:.1f}%  span={span}yr", flush=True)
print(f"  avgR: IS(n={len(isr)})={statistics.mean(isr):+.3f}  OOS(n={len(oosr)})={statistics.mean(oosr):+.3f}", flush=True)
print(f"  net Rs profit total = Rs{equity-CAP0:,.0f}  (~Rs{(equity-CAP0)/span:,.0f}/yr on Rs2L)", flush=True)
print("\n  READ: robust +avgR both halves + sane DD -> a real (thin) event channel, pead-tier.", flush=True)
print("  Weigh ~Rs/yr vs the full build cost (ingest+services+dashboard+3 deploys) before shipping.", flush=True)
