"""Faithful PEAD (Post-Earnings-Announcement Drift) backtest — REBUILT 2026-07-09
after the old scratch harness was lost in the Mac migration. Strictly READ-ONLY,
single-process, NO prod/BQ/deploy (see memory feedback_channel_work_isolation).

Data (all LOCAL, zero cost):
  - NSE result-announcement dates: ~/.autotrader_backtest_cache/pead_nse_result_dates_2012_2026.json
    (95,742 events, fetched from NSE event-calendar; 'Financial Results' purpose only)
  - Daily bars: swing_adj_bars_2015.pkl (2015-2026, liquid universe, [date,o,h,l,c,v])
  - Market index (for the market-dd gate): market_inputs_2015.json (nifty_close per date)

Fidelity: imports the PROD gates from domain/pead_signals.py (earnings_surprise,
pre_event_runup, passes_pead_gates) and the PROD exit from domain/swing_exit.py
(simulate_exit, arm 1.75R / trail 1.0R) + Upstox costs — so it can't drift from live.

Config = GRIND-V2 (the deployed config): surprise>=5%, anti-pump<75% (60d), market-dd>-5%,
max-hold 60d, 2.5xATR stop, entry next-open. Sizing CAPITAL_PEAD=2L / RISK=3k, 5 slots.
Reports IS 2015-2020 / OOS 2021-2026 economics vs the ~7%/26% baseline on record.
"""
from __future__ import annotations
import os, sys, json, pickle
from bisect import bisect_left
from datetime import date

sys.path.insert(0, "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.domain.pead_signals import (
    earnings_surprise, pre_event_runup, passes_pead_gates,
    SURPRISE_MIN, ANTI_PUMP_MAX_RUNUP, ANTI_PUMP_LOOKBACK, MARKET_DD_GATE,
    MAX_HOLD_DAYS, ATR_SL_MULT,
)
from autotrader.domain.swing_exit import simulate_exit
from autotrader.backtest.costs import compute_leg_cost, CostConfig

C = os.path.expanduser("~/.autotrader_backtest_cache")
UPSTOX = CostConfig.upstox()
SLIP = 0.001          # 0.10%/leg paper fill slippage (matches swing_final)
CAPITAL = 200_000.0   # CAPITAL_PEAD
RISK = 3_000.0        # PEAD_RISK_PER_TRADE
SLOTS = 5
TRAIL_R, ACTIVATE_R = 1.0, 1.75   # PEAD/swing exit geometry

# ── load data ────────────────────────────────────────────────────────────────
ev = json.load(open(f"{C}/pead_nse_result_dates_2012_2026.json"))["events"]
bars = pickle.load(open(f"{C}/swing_adj_bars_2015.pkl", "rb"))
mkt = json.load(open(f"{C}/market_inputs_2015.json"))

# nifty trailing-peak drawdown per date (the market-state gate input)
mdates = sorted(d for d in mkt if mkt[d].get("nifty_close"))
nifty = [float(mkt[d]["nifty_close"]) for d in mdates]
peak = -1e18; dd_by_date = {}
for d, v in zip(mdates, nifty):
    peak = max(peak, v)
    dd_by_date[d] = v / peak - 1.0 if peak > 0 else 0.0
def market_dd(d):
    i = bisect_left(mdates, d) - 1
    return dd_by_date[mdates[i]] if i >= 0 else None

# per-symbol series + ATR14
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
    if not b or len(b) < ANTI_PUMP_LOOKBACK + 20:
        continue
    d = [x[0] for x in b]; o = [float(x[1]) for x in b]
    hi = [float(x[2]) for x in b]; lo = [float(x[3]) for x in b]; cl = [float(x[4]) for x in b]
    SYM[s] = {"d": d, "o": o, "hi": hi, "lo": lo, "cl": cl, "atr": atr14(hi, lo, cl),
              "bars": b, "idx": {dt: i for i, dt in enumerate(d)}}

# ── build candidates (look-ahead-free: gates at reaction day ri, enter ri+1 open) ──
def reaction_idx(sy, rd):
    # first trading bar on/after the result date = the reaction session
    dl = sy["d"]; i = bisect_left(dl, rd)
    return i if i < len(dl) else None

cands = []
for e in ev:
    sy = SYM.get(e["symbol"])
    if sy is None or e["date"] < "2015-01-01":
        continue
    ri = reaction_idx(sy, e["date"])
    if ri is None or ri < ANTI_PUMP_LOOKBACK + 1 or ri + 1 >= len(sy["cl"]):
        continue
    surprise = earnings_surprise(sy["cl"], ri)
    runup = pre_event_runup(sy["cl"], ri)
    mdd = market_dd(sy["d"][ri])
    if not passes_pead_gates(surprise, runup, mdd):
        continue
    atr = sy["atr"][ri]
    if not atr or atr <= 0:
        continue
    cands.append({"sym": e["symbol"], "ri": ri, "ei": ri + 1,
                  "entry_d": sy["d"][ri + 1], "surprise": surprise, "atr": atr})
cands.sort(key=lambda c: c["entry_d"])
print(f"universe={len(SYM)} syms | events(>=2015, in-univ)={sum(1 for e in ev if e['date']>='2015-01-01' and e['symbol'] in SYM)} | qualified candidates={len(cands)}")

# ── portfolio sim (5 slots, risk-sized, prod exit + Upstox cost + slip) ──────────
slot_free_on = [""] * SLOTS   # date each slot frees up
trades = []
for c in cands:
    sy = SYM[c["sym"]]; ei = c["ei"]; entry_d = c["entry_d"]
    slot = next((k for k in range(SLOTS) if slot_free_on[k] <= entry_d), None)
    if slot is None:
        continue   # book full
    entry_px = sy["o"][ei]
    sl_dist = ATR_SL_MULT * c["atr"]
    qty = int(RISK // sl_dist)
    if qty < 1 or entry_px <= 0:
        continue
    if qty * entry_px > CAPITAL / SLOTS:           # per-slot notional cap
        qty = int((CAPITAL / SLOTS) // entry_px)
    if qty < 1:
        continue
    off, exit_px, reason = simulate_exit(sy["bars"], ei, True, sl_dist, MAX_HOLD_DAYS,
                                         trail_R=TRAIL_R, activate_R=ACTIVATE_R)
    exit_i = min(ei + off, len(sy["bars"]) - 1)
    slot_free_on[slot] = sy["d"][exit_i]
    ef = entry_px * (1 + SLIP); xf = exit_px * (1 - SLIP)
    gross = (xf - ef) * qty
    cost = (compute_leg_cost(side="BUY", qty=qty, price=ef, is_swing=True, cfg=UPSTOX)
            + compute_leg_cost(side="SELL", qty=qty, price=xf, is_swing=True, cfg=UPSTOX))
    trades.append({"entry_d": entry_d, "exit_d": sy["d"][exit_i], "sym": c["sym"],
                   "qty": qty, "R": (gross / (sl_dist * qty)) if sl_dist * qty else 0.0,
                   "net": gross - cost, "reason": reason})

# ── metrics ──────────────────────────────────────────────────────────────────
def metrics(tr, lo=None, hi=None):
    t = [x for x in tr if (lo is None or x["exit_d"] >= lo) and (hi is None or x["entry_d"] <= hi)]
    if not t:
        return None
    net = sum(x["net"] for x in t); wr = 100 * sum(1 for x in t if x["net"] > 0) / len(t)
    # realized-pnl equity curve for DD/CAGR
    days = sorted({x["exit_d"] for x in t})
    eq = CAPITAL; curve = [CAPITAL]; byd = {}
    for x in t: byd.setdefault(x["exit_d"], 0.0); byd[x["exit_d"]] += x["net"]
    for d in days: eq += byd[d]; curve.append(eq)
    pk = -1e18; mdd = 0.0
    for v in curve: pk = max(pk, v); mdd = min(mdd, v / pk - 1)
    y = (date.fromisoformat(t[-1]["exit_d"]) - date.fromisoformat(t[0]["entry_d"])).days / 365.25
    cg = ((curve[-1] / CAPITAL) ** (1 / y) - 1) if y > 0 and curve[-1] > 0 else 0.0
    return dict(n=len(t), wr=wr, net=net, cagr=cg, mdd=mdd,
                calmar=(cg / abs(mdd)) if mdd else 0.0, avgR=sum(x["R"] for x in t) / len(t))

full = metrics(trades); IS = metrics(trades, hi="2020-12-31"); OOS = metrics(trades, lo="2021-01-01")
print(f"\nconfig: surprise>={SURPRISE_MIN} anti-pump<{ANTI_PUMP_MAX_RUNUP} mkt-dd>{MARKET_DD_GATE} "
      f"max-hold={MAX_HOLD_DAYS} arm={ACTIVATE_R}R | CAP={CAPITAL:.0f} risk={RISK:.0f} slots={SLOTS}")
def show(lbl, m):
    if not m: print(f"  {lbl:14} (no trades)"); return
    print(f"  {lbl:14} n={m['n']:4} WR={m['wr']:4.1f}% net=Rs{m['net']:>10,.0f} CAGR={m['cagr']*100:5.1f}% "
          f"maxDD={m['mdd']*100:6.1f}% Calmar={m['calmar']:4.2f} avgR={m['avgR']:+.2f}")
print("-" * 96)
show("FULL 2015-26", full); show("IS 2015-20", IS); show("OOS 2021-26", OOS)
print("-" * 96)
# per-year
from collections import defaultdict
yr = defaultdict(lambda: [0, 0.0])
for x in trades: yr[x["exit_d"][:4]][0] += 1; yr[x["exit_d"][:4]][1] += x["net"]
print("by exit-year:", "  ".join(f"{y}:{v[1]/1000:+.0f}k(n{v[0]})" for y, v in sorted(yr.items())))
print("\nBaseline-on-record for sanity: ~6.7% raw / ~26% DD at Rs2L (GRIND-V2). Close = harness faithful.")
