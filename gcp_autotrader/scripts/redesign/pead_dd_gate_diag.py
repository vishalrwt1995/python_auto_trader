"""PEAD market_dd_gate calibration check — is -5% the right cutoff, or does the edge persist
further into a drawdown? PEAD is DORMANT RIGHT NOW (NIFTY -8.65%, gate requires >-5%). The
regime-conditionality itself is already validated ("PEAD drifts ~+6% near highs and dies in
corrections" -- domain/pead_signals.py docstring) -- this tests whether the SPECIFIC -5%
threshold is calibrated or overly conservative, by bucketing forward R by market_dd LEVEL
instead of hard-gating it. Reuses the real prod gates (earnings_surprise, pre_event_runup,
run-up floor) + real exit (swing_exit, PEAD geometry: 2.5xATR, 1.75R arm, 1.0R trail, 60d hold)
+ real Upstox costs. Survivorship-safe (pead_full_bars_2014, incl delisted). Does not touch
domain/pead_signals.py or pead_faithful.py. READ-ONLY, single-process, thread-capped."""
import os
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "4"
import sys, json, pickle, statistics
from bisect import bisect_left
from datetime import date
sys.path.insert(0, "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.domain.pead_signals import (
    earnings_surprise, pre_event_runup, ANTI_PUMP_MAX_RUNUP, ANTI_KNIFE_MIN_RUNUP,
    ANTI_PUMP_LOOKBACK, MAX_HOLD_DAYS, ATR_SL_MULT,
)
from autotrader.domain.swing_exit import simulate_exit
from autotrader.backtest.costs import compute_leg_cost, CostConfig

C = os.path.expanduser("~/.autotrader_backtest_cache")
UPSTOX = CostConfig.upstox()
SLIP, CAPITAL, RISK, SLOTS = 0.001, 200_000.0, 3_000.0, 5
TRAIL_R, ACTIVATE_R = 1.0, 1.75

print("loading events + survivorship-safe bars + market index ...", flush=True)
ev = json.load(open(f"{C}/pead_nse_result_dates_2012_2026.json"))["events"]
bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
mkt = json.load(open(f"{C}/market_inputs_2015.json"))

mdates = sorted(d for d in mkt if mkt[d].get("nifty_close"))
nifty = [float(mkt[d]["nifty_close"]) for d in mdates]
peak = -1e18; dd_by_date = {}
for d, v in zip(mdates, nifty):
    peak = max(peak, v); dd_by_date[d] = v / peak - 1.0 if peak > 0 else 0.0
def market_dd(d):
    i = bisect_left(mdates, d) - 1
    return dd_by_date[mdates[i]] if i >= 0 else None

def atr14(hi, lo, cl):
    tr = [hi[0] - lo[0]]
    for i in range(1, len(cl)):
        tr.append(max(hi[i] - lo[i], abs(hi[i] - cl[i-1]), abs(lo[i] - cl[i-1])))
    out = [None] * len(cl); s = 0.0
    for i in range(len(tr)):
        s += tr[i]
        if i >= 14: s -= tr[i-14]
        if i >= 13: out[i] = s / 14.0
    return out

SYM = {}
for s, b in bars.items():
    if not b or len(b) < ANTI_PUMP_LOOKBACK + 20: continue
    d = [x[0] for x in b]; o = [float(x[1]) for x in b]
    hi = [float(x[2]) for x in b]; lo = [float(x[3]) for x in b]; cl = [float(x[4]) for x in b]
    SYM[s] = {"d": d, "o": o, "hi": hi, "lo": lo, "cl": cl, "atr": atr14(hi, lo, cl), "bars": b, "idx": {dt: i for i, dt in enumerate(d)}}
print(f"  {len(SYM)} symbols\n", flush=True)

def reaction_idx(sy, rd):
    dl = sy["d"]; i = bisect_left(dl, rd)
    return i if i < len(dl) else None

# candidates: apply surprise + anti-pump/anti-knife gates, but NOT the market_dd gate --
# record market_dd as a feature to bucket by instead
cands = []
for e in ev:
    sy = SYM.get(e["symbol"])
    if sy is None or e["date"] < "2015-01-01": continue
    ri = reaction_idx(sy, e["date"])
    if ri is None or ri < ANTI_PUMP_LOOKBACK + 1 or ri + 1 >= len(sy["cl"]): continue
    surprise = earnings_surprise(sy["cl"], ri)
    runup = pre_event_runup(sy["cl"], ri)
    mdd = market_dd(sy["d"][ri])
    if surprise is None or runup is None or mdd is None: continue
    if surprise < 0.05 or not (ANTI_KNIFE_MIN_RUNUP <= runup < ANTI_PUMP_MAX_RUNUP): continue
    atr = sy["atr"][ri]
    if not atr or atr <= 0: continue
    cands.append({"sym": e["symbol"], "ri": ri, "ei": ri+1, "entry_d": sy["d"][ri+1],
                  "surprise": surprise, "mdd": mdd, "atr": atr})
cands.sort(key=lambda c: c["entry_d"])
print(f"gate-1+2-passing candidates (surprise+runup, NO market_dd gate): {len(cands):,}\n", flush=True)

def walk(pool):
    free = [""] * SLOTS; tr = []
    for c in pool:
        sy = SYM[c["sym"]]; ei = c["ei"]; entry_d = c["entry_d"]
        slot = next((k for k in range(SLOTS) if free[k] <= entry_d), None)
        if slot is None: continue
        entry_px = sy["o"][ei]; sl_dist = ATR_SL_MULT * c["atr"]
        qty = int(RISK // sl_dist)
        if qty < 1 or entry_px <= 0: continue
        if qty * entry_px > CAPITAL / SLOTS: qty = int((CAPITAL / SLOTS) // entry_px)
        if qty < 1: continue
        off, exit_px, _ = simulate_exit(sy["bars"], ei, True, sl_dist, MAX_HOLD_DAYS, trail_R=TRAIL_R, activate_R=ACTIVATE_R)
        exit_i = min(ei + off, len(sy["bars"]) - 1); free[slot] = sy["d"][exit_i]
        ef = entry_px * (1 + SLIP); xf = exit_px * (1 - SLIP)
        gross = (xf - ef) * qty
        cost = (compute_leg_cost(side="BUY", qty=qty, price=ef, is_swing=True, cfg=UPSTOX)
                + compute_leg_cost(side="SELL", qty=qty, price=xf, is_swing=True, cfg=UPSTOX))
        tr.append({"ed": entry_d, "xd": sy["d"][exit_i], "R": (gross - cost) / (sl_dist * qty), "net": gross - cost})
    return tr

def bucket(lo, hi, lbl):
    pool = [c for c in cands if lo <= c["mdd"] < hi]
    a = [x for x in walk(pool) if x["ed"] <= "2020-12-31"]
    b = [x for x in walk(pool) if x["ed"] >= "2021-01-01"]
    if len(a) < 15 and len(b) < 15:
        print(f"  {lbl:16} n=IS{len(a):>4}/OOS{len(b):>4}  (thin)"); return
    def m(t):
        if not t: return "  n/a  "
        avgR = statistics.mean(x["R"] for x in t); wr = 100*sum(1 for x in t if x["net"]>0)/len(t)
        return f"avgR={avgR:+.3f} WR={wr:4.1f}%"
    print(f"  {lbl:16} n={len(pool):>5}  IS(n={len(a):>4}): {m(a)}   |   OOS(n={len(b):>4}): {m(b)}", flush=True)

print("=== avg R-multiple by market-drawdown bucket (IS<=2020 / OOS>=2021) ===", flush=True)
print(f"  (current live NIFTY dd: -8.65% -- gate requires > -5.0%)\n", flush=True)
bucket(-0.03, 10, ">-3% (near highs)")
bucket(-0.05, -0.03, "-3%..-5%")
bucket(-0.08, -0.05, "-5%..-8% <- CURRENTLY BLOCKED, closest to today")
bucket(-0.12, -0.08, "-8%..-12% <- where we are today (-8.65%)")
bucket(-0.20, -0.12, "-12%..-20%")
bucket(-1.0, -0.20, "<-20% (deep bear)")

print("\nRead: if -5%..-8% and/or -8%..-12% show robust positive avgR both halves -> the gate is", flush=True)
print("overly conservative and PEAD is missing real, currently-available edge right now. If it", flush=True)
print("craters at -5% -> the gate is well-calibrated, matches the documented finding.", flush=True)
