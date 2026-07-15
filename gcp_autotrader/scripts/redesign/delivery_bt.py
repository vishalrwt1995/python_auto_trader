"""DELIVERY-ACCUMULATION engine-test — does the CSV edge survive a real portfolio walk?

Thesis (from delivery_diag): high delivery-% + price weakness/dip = accumulation-into-weakness
-> forward drift. Now test it with slots + Upstox cost + slippage + ATR exit, IS/OOS + per-year
(the swing/PEAD lesson: CSV buckets often die in the walk). Survivorship-safe universe
(pead_full_bars_2014). Reuses ONLY read-only prod primitives (swing_exit.simulate_exit, costs).
Isolated: delivery from ~/.autotrader_grind_cache; no prod/existing-backtest file touched.
READ-ONLY, local, single-process, thread-capped."""
import os
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "4"
import sys, pickle
from statistics import mean
from datetime import date
from collections import defaultdict
sys.path.insert(0, "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.domain.swing_exit import simulate_exit
from autotrader.backtest.costs import compute_leg_cost, CostConfig

GC = os.path.expanduser("~/.autotrader_grind_cache")
BC = os.path.expanduser("~/.autotrader_backtest_cache")
UPSTOX = CostConfig.upstox(); SLIP = 0.001
CAPITAL = 200_000.0; RISK = 3_000.0; SLOTS = 5
PRICE_MIN, TURN_MIN, ATR_MULT = 30.0, 1e8, 2.5
TRAIL_R, ACTIVATE_R = 1.0, 1.75

print("loading delivery + survivorship-safe bars ...", flush=True)
deliv = pickle.load(open(f"{GC}/delivery.pkl", "rb"))
bars = pickle.load(open(f"{BC}/pead_full_bars_2014.pkl", "rb"))

def atr14(b):
    h = [x[2] for x in b]; l = [x[3] for x in b]; c = [x[4] for x in b]
    tr = [h[0] - l[0]]
    for i in range(1, len(c)):
        tr.append(max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])))
    out = [None] * len(c); s = 0.0
    for i in range(len(tr)):
        s += tr[i]
        if i >= 14: s -= tr[i - 14]
        if i >= 13: out[i] = s / 14.0
    return out

# per-symbol aligned structures
SYM = {}
for sym, b in bars.items():
    if not b or len(b) < 60:
        continue
    SYM[sym] = {"b": b, "d": [x[0] for x in b], "o": [x[1] for x in b], "c": [x[4] for x in b],
                "v": [x[5] for x in b], "atr": atr14(b), "bd": {x[0]: i for i, x in enumerate(b)}}

def build(deliv_min, dip_kind, dip_thr):
    """candidates: deliv_pct>=deliv_min AND dip condition; entry next open."""
    cands = []
    for sym, dl in deliv.items():
        S = SYM.get(sym)
        if S is None:
            continue
        c, bd = S["c"], S["bd"]
        for (d, pct, qty, ttl) in dl:
            if pct < deliv_min or d not in bd:
                continue
            i = bd[d]
            if i < 20 or i + 1 >= len(c) or c[i] < PRICE_MIN:
                continue
            if mean(c[j] * S["v"][j] for j in range(i - 20, i)) < TURN_MIN:
                continue
            if dip_kind == "ret5":
                if not (c[i] / c[i - 5] - 1.0) <= dip_thr:
                    continue
            elif dip_kind == "disthi":
                hi = max(c[i - 19:i + 1])
                if not (hi > 0 and (hi - c[i]) / hi >= dip_thr):
                    continue
            atr = S["atr"][i]
            if not atr or atr <= 0:
                continue
            cands.append((d, sym, i, atr))
    cands.sort()
    return cands

def walk(cands, max_hold):
    free = [""] * SLOTS; tr = []
    for (d, sym, i, atr) in cands:
        slot = next((k for k in range(SLOTS) if free[k] <= d), None)
        if slot is None:
            continue
        S = SYM[sym]; ei = i + 1
        epx = S["o"][ei]; sl = ATR_MULT * atr; qty = int(RISK // sl)
        if qty < 1 or epx <= 0:
            continue
        if qty * epx > CAPITAL / SLOTS:
            qty = int((CAPITAL / SLOTS) // epx)
        if qty < 1:
            continue
        off, xpx, _ = simulate_exit(S["b"], ei, True, sl, max_hold, trail_R=TRAIL_R, activate_R=ACTIVATE_R)
        xi = min(ei + off, len(S["b"]) - 1); free[slot] = S["d"][xi]
        ef = epx * (1 + SLIP); xf = xpx * (1 - SLIP); gross = (xf - ef) * qty
        cost = (compute_leg_cost(side="BUY", qty=qty, price=ef, is_swing=True, cfg=UPSTOX)
                + compute_leg_cost(side="SELL", qty=qty, price=xf, is_swing=True, cfg=UPSTOX))
        tr.append({"ed": d, "xd": S["d"][xi], "net": gross - cost, "R": (gross / (sl * qty)) if sl * qty else 0})
    return tr

def metrics(tr, lo=None, hi=None):
    t = [x for x in tr if (lo is None or x["xd"] >= lo) and (hi is None or x["ed"] <= hi)]
    if not t:
        return None
    days = sorted({x["xd"] for x in t}); eq = CAPITAL; curve = [CAPITAL]; byd = defaultdict(float)
    for x in t:
        byd[x["xd"]] += x["net"]
    for dd in days:
        eq += byd[dd]; curve.append(eq)
    pk = -1e18; mdd = 0.0
    for vv in curve:
        pk = max(pk, vv); mdd = min(mdd, vv / pk - 1)
    y = (date.fromisoformat(t[-1]["xd"]) - date.fromisoformat(t[0]["ed"])).days / 365.25
    cg = ((curve[-1] / CAPITAL) ** (1 / y) - 1) * 100 if y > 0 and curve[-1] > 0 else 0.0
    net = sum(x["net"] for x in t)
    return dict(n=len(t), net=net, cagr=cg, mdd=mdd * 100, calmar=(cg / 100) / abs(mdd) if mdd else 0,
                wr=100 * sum(1 for x in t if x["net"] > 0) / len(t), avgR=mean(x["R"] for x in t))

def run(deliv_min, dip_kind, dip_thr, max_hold, label):
    tr = walk(build(deliv_min, dip_kind, dip_thr), max_hold)
    f = metrics(tr); i = metrics(tr, hi="2022-12-31"); o = metrics(tr, lo="2023-01-01")
    if not f:
        print(f"  {label:34} (no trades)"); return
    win = i and o and i["net"] > 0 and o["net"] > 0
    print(f"  {label:34} n={f['n']:>4}({f['n']/6.0:>4.1f}/yr) CAGR={f['cagr']:>5.1f}% Cal={f['calmar']:>4.2f} "
          f"DD={f['mdd']:>6.1f}% WR={f['wr']:>3.0f}% avgR={f['avgR']:+.2f} | IS {i['cagr'] if i else 0:>5.1f}%/{i['net'] if i else 0:>7,.0f} "
          f"OOS {o['cagr'] if o else 0:>5.1f}%/{o['net'] if o else 0:>7,.0f}{'  <== both+' if win else ''}", flush=True)

print("=== DELIVERY-ACCUMULATION engine-test (5 slots, RISK 3k, Upstox cost, ~6yr 2020-26) ===", flush=True)
print("--- high delivery + dip (ret5) x hold ---", flush=True)
run(75, "ret5", 0.0, 10, "deliv>=75 & ret5<=0, hold10")
run(75, "ret5", -0.05, 10, "deliv>=75 & ret5<=-5%, hold10")
run(75, "ret5", -0.10, 10, "deliv>=75 & ret5<=-10%, hold10")
run(75, "ret5", -0.05, 20, "deliv>=75 & ret5<=-5%, hold20")
print("--- high delivery + below-20d-high x hold ---", flush=True)
run(75, "disthi", 0.05, 10, "deliv>=75 & >5% below hi, hold10")
run(75, "disthi", 0.10, 20, "deliv>=75 & >10% below hi, hold20")
print("--- delivery level sweep (with -5% dip, hold10) ---", flush=True)
run(70, "ret5", -0.05, 10, "deliv>=70 & ret5<=-5%, hold10")
run(85, "ret5", -0.05, 10, "deliv>=85 & ret5<=-5%, hold10")
print("\nKEEP = net-up in BOTH halves AND Calmar/DD reasonable. Then survivorship + per-year next.", flush=True)
