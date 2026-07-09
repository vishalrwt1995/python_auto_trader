"""PEAD grind — OOS-disciplined one-lever-at-a-time sweep. READ-ONLY, single-process,
local, zero cost (see feedback_channel_work_isolation + the pead_faithful.py baseline).

Loads the data + raw event-feature pool ONCE, then runs many configs. For each lever
(surprise / anti-pump / market-dd gate / max-hold / slots / exit-arm) it varies that one
setting with the others at baseline, and prints FULL / IS(2015-20) / OOS(2021-26). A change
is only a WIN if it beats baseline Calmar AND holds (net up) in BOTH halves — plateau, not
peak. Imports the prod gates + exit so it can't drift from live.
"""
from __future__ import annotations
import os, sys, json, pickle
from bisect import bisect_left
from datetime import date

sys.path.insert(0, "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.domain.pead_signals import (
    earnings_surprise, pre_event_runup, ANTI_PUMP_LOOKBACK, ATR_SL_MULT,
)
from autotrader.domain.swing_exit import simulate_exit
from autotrader.backtest.costs import compute_leg_cost, CostConfig

C = os.path.expanduser("~/.autotrader_backtest_cache")
UPSTOX = CostConfig.upstox(); SLIP = 0.001; CAPITAL = 200_000.0

ev = json.load(open(f"{C}/pead_nse_result_dates_2012_2026.json"))["events"]
bars = pickle.load(open(f"{C}/swing_adj_bars_2015.pkl", "rb"))
mkt = json.load(open(f"{C}/market_inputs_2015.json"))
mdates = sorted(d for d in mkt if mkt[d].get("nifty_close"))
peak = -1e18; dd_by_date = {}
for d in mdates:
    v = float(mkt[d]["nifty_close"]); peak = max(peak, v); dd_by_date[d] = v / peak - 1.0
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
    SYM[s] = {"d": d, "o": o, "bars": b, "atr": atr14(hi, lo, cl), "cl": cl}

# raw pool: events with surprise>=0.05 (loosest floor) + valid features, precomputed once
pool = []
for e in ev:
    sy = SYM.get(e["symbol"])
    if sy is None or e["date"] < "2015-01-01": continue
    dl = sy["d"]; ri = bisect_left(dl, e["date"])
    if ri >= len(dl) or ri < ANTI_PUMP_LOOKBACK + 1 or ri + 1 >= len(sy["cl"]): continue
    sp = earnings_surprise(sy["cl"], ri); ru = pre_event_runup(sy["cl"], ri); md = market_dd(dl[ri])
    atr = sy["atr"][ri]
    if sp is None or ru is None or md is None or not atr or atr <= 0 or sp < 0.05: continue
    pool.append({"sym": e["symbol"], "ei": ri + 1, "entry_d": dl[ri + 1],
                 "sp": sp, "ru": ru, "md": md, "atr": atr})
pool.sort(key=lambda c: c["entry_d"])
print(f"raw pool (surprise>=5%, valid): {len(pool)} candidate reactions\n")

def run(surprise_min=0.05, max_runup=0.75, mdd_gate=-0.05, max_hold=60, slots=5,
        risk=3000.0, activate_R=1.75, trail_R=1.0):
    cs = [c for c in pool if c["sp"] >= surprise_min and c["ru"] < max_runup and c["md"] > mdd_gate]
    free = [""] * slots; tr = []
    for c in cs:
        sy = SYM[c["sym"]]; ei = c["ei"]; ed = c["entry_d"]
        slot = next((k for k in range(slots) if free[k] <= ed), None)
        if slot is None: continue
        epx = sy["o"][ei]; sl = ATR_SL_MULT * c["atr"]; qty = int(risk // sl)
        if qty < 1 or epx <= 0: continue
        if qty * epx > CAPITAL / slots: qty = int((CAPITAL / slots) // epx)
        if qty < 1: continue
        off, xpx, _ = simulate_exit(sy["bars"], ei, True, sl, max_hold, trail_R=trail_R, activate_R=activate_R)
        xi = min(ei + off, len(sy["bars"]) - 1); free[slot] = sy["d"][xi]
        ef = epx * (1 + SLIP); xf = xpx * (1 - SLIP); gross = (xf - ef) * qty
        cost = (compute_leg_cost(side="BUY", qty=qty, price=ef, is_swing=True, cfg=UPSTOX)
                + compute_leg_cost(side="SELL", qty=qty, price=xf, is_swing=True, cfg=UPSTOX))
        tr.append({"entry_d": ed, "exit_d": sy["d"][xi], "net": gross - cost})
    return tr

def met(tr, lo=None, hi=None):
    t = [x for x in tr if (lo is None or x["exit_d"] >= lo) and (hi is None or x["entry_d"] <= hi)]
    if not t: return None
    byd = {};
    for x in t: byd[x["exit_d"]] = byd.get(x["exit_d"], 0.0) + x["net"]
    eq = CAPITAL; curve = [CAPITAL]
    for d in sorted(byd): eq += byd[d]; curve.append(eq)
    pk = -1e18; mdd = 0.0
    for v in curve: pk = max(pk, v); mdd = min(mdd, v / pk - 1)
    y = (date.fromisoformat(t[-1]["exit_d"]) - date.fromisoformat(t[0]["entry_d"])).days / 365.25
    cg = ((curve[-1] / CAPITAL) ** (1 / y) - 1) if y > 0 and curve[-1] > 0 else 0.0
    return dict(n=len(t), net=sum(x["net"] for x in t), cagr=cg, mdd=mdd, calmar=(cg/abs(mdd)) if mdd else 0.0)

BASE = dict(surprise_min=0.05, max_runup=0.75, mdd_gate=-0.05, max_hold=60, slots=5, activate_R=1.75)
bt = run(**BASE); bf, bi, bo = met(bt), met(bt, hi="2020-12-31"), met(bt, lo="2021-01-01")

def line(lbl, cfg):
    tr = run(**cfg); f, i, o = met(tr), met(tr, hi="2020-12-31"), met(tr, lo="2021-01-01")
    if not f: print(f"  {lbl:22} (no trades)"); return
    win = ""
    if i and o and bf and f["calmar"] > bf["calmar"] and i["net"] >= bi["net"] and o["net"] >= bo["net"]:
        win = "  <-- WIN (both halves)"
    print(f"  {lbl:22} FULL n={f['n']:4} CAGR={f['cagr']*100:5.1f}% DD={f['mdd']*100:6.1f}% Cal={f['calmar']:4.2f} | "
          f"IS {i['cagr']*100:5.1f}%/{i['calmar']:4.2f}  OOS {o['cagr']*100:5.1f}%/{o['calmar']:4.2f}{win}" if i and o else "")

print(f"BASELINE               FULL n={bf['n']:4} CAGR={bf['cagr']*100:5.1f}% DD={bf['mdd']*100:6.1f}% Cal={bf['calmar']:4.2f} | "
      f"IS {bi['cagr']*100:5.1f}%/{bi['calmar']:4.2f}  OOS {bo['cagr']*100:5.1f}%/{bo['calmar']:4.2f}")
print("=" * 108)
sweeps = [
    ("surprise_min", [0.05, 0.07, 0.10, 0.15]),
    ("max_runup",    [0.40, 0.50, 0.75, 1.00]),
    ("mdd_gate",     [-0.03, -0.05, -0.08, -0.12, -1.00]),
    ("max_hold",     [30, 40, 60, 90, 120]),
    ("slots",        [3, 5, 8, 12]),
    ("activate_R",   [1.0, 1.4, 1.75, 2.25]),
]
for lever, vals in sweeps:
    print(f"[{lever}]")
    for v in vals:
        cfg = dict(BASE); cfg[lever] = v
        line(f"{lever}={v}", cfg)
    print("-" * 108)
