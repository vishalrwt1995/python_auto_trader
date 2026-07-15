"""DELIVERY parity backtest — imports the SHIPPED domain code so live can't drift from the validated
config (same discipline as swing_final.py / momentum_grind.py / pead_faithful.py).

Reproduces the stocks-only lock (deliv>=75, 25-50cr, hold20, 5 slots, ATR2.5 arm1.75 trail1.0, size-aware
fills, ETFs excluded) using domain/delivery_signals (gates, atr14, turnover_20d_cr, select_for_slots, is_etf)
+ domain/pead_book (sl_distance, position_size) — the EXACT modules the live channel uses. If this doesn't
land ~11.8% @Rs2L / ~12% @Rs5L, 6-of-7 positive years, there is a prod/backtest gap to fix.
Survivorship-safe (pead_full_bars incl delisted). READ-ONLY, single-process, thread-capped."""
import os
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "4"
import sys, pickle
from math import sqrt
from datetime import date
from collections import defaultdict
sys.path.insert(0, "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.domain import delivery_signals as ds     # THE SHIPPED gates/atr/turnover/selection
from autotrader.domain import pead_book                  # THE SHIPPED sizing (generic)
from autotrader.domain.swing_exit import simulate_exit
from autotrader.backtest.costs import compute_leg_cost, CostConfig

GC = os.path.expanduser("~/.autotrader_grind_cache"); BC = os.path.expanduser("~/.autotrader_backtest_cache")
UPSTOX = CostConfig.upstox()
HALF_SPREAD, IMPACT, MAX_PART = 0.0005, 0.01, 0.02

deliv = pickle.load(open(f"{GC}/delivery.pkl", "rb"))
bars = pickle.load(open(f"{BC}/pead_full_bars_2014.pkl", "rb"))
SYM = {}
for sym, b in bars.items():
    if not b or len(b) < 60:
        continue
    SYM[sym] = {"b": b, "d": [x[0] for x in b], "o": [x[1] for x in b], "c": [x[4] for x in b],
                "v": [x[5] for x in b], "atr": ds.atr14(b), "bd": {x[0]: i for i, x in enumerate(b)}}

# candidate pool via the SHIPPED gate (deliv>=75, 25-50cr band, price>=30, NOT is_etf)
POOL = []
for sym, dl in deliv.items():
    S = SYM.get(sym)
    if S is None:
        continue
    c, v, bd = S["c"], S["v"], S["bd"]
    for (d, pct, qty, ttl) in dl:
        if d not in bd or pct < 75:
            continue
        i = bd[d]
        if i < ds.MIN_BARS or i + 1 >= len(c):
            continue
        turn_cr = ds.turnover_20d_cr(c, v, i)
        a = S["atr"][i]
        if not a or a <= 0:
            continue
        if not ds.passes_delivery_gates(pct, turn_cr, c[i], sym):   # SHIPPED gate (ETF-excluded)
            continue
        POOL.append({"d": d, "sym": sym, "i": i, "atr": a, "turn_cr": turn_cr, "deliv_pct": pct})


def walk(capital, risk, slots):
    free = [""] * slots
    tr = []
    notional_cap = 0.20 * capital     # SHIPPED default (== cap/5 at 5 slots)
    by_day = defaultdict(list)
    for r in POOL:
        by_day[r["d"]].append(r)
    for d in sorted(by_day):
        ranked = ds.select_for_slots(by_day[d], 0, len(by_day[d]))     # SHIPPED ranking by deliv_pct
        for r in ranked:
            slot = next((k for k in range(slots) if free[k] <= d), None)
            if slot is None:
                continue
            S = SYM[r["sym"]]; ei = r["i"] + 1; epx = S["o"][ei]
            if epx <= 0:
                continue
            sl = pead_book.sl_distance(r["atr"], epx, ds.ATR_SL_MULT)     # SHIPPED sizing
            qty = pead_book.position_size(epx, sl, risk, notional_cap)
            turn_rs = r["turn_cr"] * 1e7
            if turn_rs > 0:
                qty = min(qty, int(MAX_PART * turn_rs / epx))            # participation cap
            if qty < 1 or sl <= 0:
                continue
            part = (qty * epx) / turn_rs if turn_rs > 0 else 0.0
            sp = HALF_SPREAD + IMPACT * sqrt(part)
            off, xpx, _ = simulate_exit(S["b"], ei, True, sl, ds.MAX_HOLD_DAYS,
                                        trail_R=ds.TRAIL_R, activate_R=ds.ACTIVATE_R)
            xi = min(ei + off, len(S["b"]) - 1); free[slot] = S["d"][xi]
            ef = epx * (1 + sp); xf = xpx * (1 - sp); gross = (xf - ef) * qty
            cost = (compute_leg_cost(side="BUY", qty=qty, price=ef, is_swing=True, cfg=UPSTOX)
                    + compute_leg_cost(side="SELL", qty=qty, price=xf, is_swing=True, cfg=UPSTOX))
            tr.append({"ed": d, "xd": S["d"][xi], "net": gross - cost})
    return tr


def met(tr, capital, lo=None, hi=None):
    t = [x for x in tr if (lo is None or x["xd"] >= lo) and (hi is None or x["ed"] <= hi)]
    if not t:
        return None
    days = sorted({x["xd"] for x in t}); eq = capital; cur = [capital]; byd = defaultdict(float)
    for x in t:
        byd[x["xd"]] += x["net"]
    for dd in days:
        eq += byd[dd]; cur.append(eq)
    pk = -1e18; m = 0.0
    for vv in cur:
        pk = max(pk, vv); m = min(m, vv / pk - 1)
    y = (date.fromisoformat(t[-1]["xd"]) - date.fromisoformat(t[0]["ed"])).days / 365.25
    cg = ((cur[-1] / capital) ** (1 / y) - 1) * 100 if y > 0 and cur[-1] > 0 else 0.0
    return dict(n=len(t), cagr=cg, mdd=m * 100, cal=(cg / 100) / abs(m) if m else 0,
                peryr=sum(x['net'] for x in t) / y if y > 0 else 0)


print(f"pool (SHIPPED gate, stocks-only): {len(POOL):,} signal-days\n", flush=True)
for cap, risk, lbl in [(200_000, 3_000, "@Rs2L"), (500_000, 7_500, "@Rs5L")]:
    tr = walk(cap, risk, 5)
    f = met(tr, cap); I = met(tr, cap, hi="2022-12-31"); O = met(tr, cap, lo="2023-01-01")
    print(f"LOCKED {lbl}: CAGR={f['cagr']:.1f}% Cal={f['cal']:.2f} DD={f['mdd']:.1f}% "
          f"Rs{f['peryr']/1000:.1f}k/y n={f['n']} | IS{I['cagr']:.1f} OOS{O['cagr']:.1f}", flush=True)
    by = defaultdict(float)
    for x in tr:
        by[x["xd"][:4]] += x["net"]
    pos = sum(1 for _, vv in by.items() if vv > 0)
    print("  by-yr: " + "  ".join(f"{y}:{vv/1000:+.0f}k" for y, vv in sorted(by.items())) + f"  ({pos}/{len(by)}+)", flush=True)
print("\nParity target: ~11.8% @Rs2L / ~12% @Rs5L, IS≈OOS, 6-of-7 +yrs. Match => shipped domain code faithful.", flush=True)
