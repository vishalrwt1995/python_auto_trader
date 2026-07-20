"""F&O-inclusion FIXED-HOLD walk -- the ATR-trail whipsawed a drift edge to zero; a drift wants
a time-based exit. Entry = open of first trading day of inclusion month; exit = close at ei+HOLD.
Two variants: pure fixed-hold, and fixed-hold + a protective 2.5xATR stop (disaster guard).
5-slot compounding account, real Upstox costs + slippage, IS(<=2020)/OOS(>=2021). This is the
decisive account-level test: does the +2-4% fwd20 per-signal edge translate to a real (thin)
channel? Survivorship-safe. READ-ONLY, single-process, cached only (zero GCP cost)."""
import os
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "4"
import sys, pickle, json, statistics
from bisect import bisect_left
sys.path.insert(0, "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.backtest.costs import compute_leg_cost, CostConfig

C = os.path.expanduser("~/.autotrader_backtest_cache")
UPSTOX = CostConfig.upstox()
CAP0, SLOTS, RISK_PCT, SLIP = 200_000.0, 5, 0.015, 0.001
ATR_MULT, HOLD, MIN_PRICE, IS_END = 2.5, 20, 30.0, "2020-12-31"

bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
fno = json.load(open(f"{C}/fno_membership_by_month.json"))
months = sorted(fno.keys())

def atr14(h, l, c):
    tr = [h[0]-l[0]]
    for i in range(1, len(c)): tr.append(max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])))
    out=[None]*len(c); s=0.0
    for i in range(len(tr)):
        s += tr[i]
        if i >= 14: s -= tr[i-14]
        if i >= 13: out[i] = s/14.0
    return out

SYM = {}
for s, b in bars.items():
    if len(b) < 40: continue
    SYM[s] = {"d":[x[0] for x in b],"o":[x[1] for x in b],"h":[x[2] for x in b],
              "l":[x[3] for x in b],"c":[x[4] for x in b],"atr":atr14([x[2] for x in b],[x[3] for x in b],[x[4] for x in b])}

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

def run(use_stop):
    equity=CAP0; free=[""]*SLOTS; openpos=[]; trades=[]; peak=equity; maxdd=0.0
    for entry_d, s, ei, sl_dist in cands:
        still=[]
        for xd,pnl in openpos:
            if xd<=entry_d: equity+=pnl; peak=max(peak,equity); maxdd=min(maxdd,equity/peak-1.0)
            else: still.append((xd,pnl))
        openpos=still
        for k in range(SLOTS):
            if free[k] and free[k]<=entry_d: free[k]=""
        slot=next((k for k in range(SLOTS) if not free[k]), None)
        if slot is None: continue
        S=SYM[s]; entry_px=S["o"][ei]
        if entry_px<=0: continue
        qty=int((RISK_PCT*equity)//sl_dist)
        if qty<1: continue
        if qty*entry_px>equity/SLOTS: qty=int((equity/SLOTS)//entry_px)
        if qty<1: continue
        # exit: fixed hold at ei+HOLD close, optional protective stop
        xi=min(ei+HOLD, len(S["c"])-1); exit_px=S["c"][xi]; stop_px=entry_px-sl_dist
        if use_stop:
            for k in range(ei+1, xi+1):
                if S["l"][k]<=stop_px: exit_px=stop_px; xi=k; break
        xd=S["d"][xi]
        ef=entry_px*(1+SLIP); xf=exit_px*(1-SLIP)
        pnl=(xf-ef)*qty-(compute_leg_cost(side="BUY",qty=qty,price=ef,is_swing=True,cfg=UPSTOX)
                         +compute_leg_cost(side="SELL",qty=qty,price=xf,is_swing=True,cfg=UPSTOX))
        free[slot]=xd; openpos.append((xd,pnl))
        trades.append({"ed":entry_d,"xd":xd,"pnl":pnl,"R":pnl/(sl_dist*qty)})
    for xd,pnl in openpos:
        equity+=pnl; peak=max(peak,equity); maxdd=min(maxdd,equity/peak-1.0)
    span=int(max(t["xd"] for t in trades)[:4])-int(min(t["ed"] for t in trades)[:4])+1
    cagr=(equity/CAP0)**(1/span)-1
    wr=100*sum(1 for t in trades if t["pnl"]>0)/len(trades)
    isr=[t["R"] for t in trades if t["ed"]<=IS_END]; oosr=[t["R"] for t in trades if t["ed"]>IS_END]
    tag = "fixed-hold + 2.5ATR stop" if use_stop else "pure fixed-hold (no stop)"
    print(f"  {tag}", flush=True)
    print(f"    final=Rs{equity:,.0f}  CAGR={cagr*100:+.1f}%  maxDD={maxdd*100:.1f}%  "
          f"Calmar={cagr/abs(maxdd) if maxdd<0 else 0:.2f}  trades={len(trades)} (~{len(trades)/span:.0f}/yr)  WR={wr:.1f}%", flush=True)
    print(f"    avgR: IS(n={len(isr)})={statistics.mean(isr):+.3f}  OOS(n={len(oosr)})={statistics.mean(oosr):+.3f}  "
          f"| net Rs{equity-CAP0:,.0f} (~Rs{(equity-CAP0)/span:,.0f}/yr)", flush=True)

print(f"=== F&O-INCLUSION FIXED-{HOLD}d-HOLD ACCOUNT WALK (5-slot, compounding, real costs) ===\n", flush=True)
run(False); print(flush=True); run(True)
print("\n  READ: if fixed-hold clears ~Calmar 0.6 + CAGR>>FD both halves -> the edge was real and", flush=True)
print("  the TRAIL was the wrong exit. If still flat -> the fwd20 diag was an un-capturable average.", flush=True)
