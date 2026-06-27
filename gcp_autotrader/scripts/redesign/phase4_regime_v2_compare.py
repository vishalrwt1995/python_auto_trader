"""
Phase 4 — Revalidate swing strategies on regime_v2_core4.json vs regime_core4.json.

Compares:
  v1: regime_core4.json  (no RECOVERY — post-PANIC days labeled RANGE)
  v2: regime_v2_core4.json  (Phase 2 RECOVERY logic: 4-day window after PANIC)

In v2, RECOVERY is a "sit on hands" regime (no swing setups allowed):
  - MOMENTUM/PULLBACK require TREND_UP
  - MEAN_REVERSION requires RANGE/RANGE_ROTATING
  - RECOVERY is therefore 0 entries in swing

This measures: does suppressing swing entries for 4 days after PANIC hurt/help?
"""

from __future__ import annotations

import json
import os
import pickle
import sys

CACHE = os.path.expanduser("~/.autotrader_backtest_cache")
BARS_PKL = os.path.join(CACHE, "swing_adj_bars.pkl")

# Inline from swing_prod_faithful to avoid import path issues
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "../../src"))
sys.path.insert(0, os.path.join(_HERE, "../.."))

from scripts.redesign.swing_prod_faithful import Sym, run, MIN_BARS_SWING, DEFAULT_ACTIVATE_R


def main():
    print("loading bars ...")
    raw = pickle.load(open(BARS_PKL, "rb"))
    symdata = {sym: Sym(bars) for sym, bars in raw.items() if len(bars) >= MIN_BARS_SWING}
    print(f"  {len(symdata)} symbols with >= {MIN_BARS_SWING} bars\n")

    regime_v1 = json.load(open(os.path.join(CACHE, "regime_core4.json")))
    regime_v2 = json.load(open(os.path.join(CACHE, "regime_v2_core4.json")))

    print(f"Regime v1 (no RECOVERY): {len(regime_v1)} days")
    print(f"Regime v2 (w/ RECOVERY): {len(regime_v2)} days, "
          f"RECOVERY={sum(1 for v in regime_v2.values() if v == 'RECOVERY')}\n")

    cap = 100_000
    arm = DEFAULT_ACTIVATE_R

    print(f"=== Phase 4: Regime v1 vs v2 @ ₹{cap/1e5:.0f}L, arm={arm} ===\n")

    print("-- v1: regime_core4 (baseline, no RECOVERY) --")
    r1 = run(symdata, regime_v1, cap, arm)

    print("\n-- v2: regime_v2_core4 (Phase 2 RECOVERY, 4-day post-PANIC window) --")
    r2 = run(symdata, regime_v2, cap, arm)

    print("\n=== DELTA (v2 - v1) ===")
    metrics = [("net ₹", "net"), ("trades", "n"), ("WR%", "wr"), ("maxDD%", "mdd"),
               ("CAGR%", "cagr"), ("Calmar", "calmar")]
    for label, key in metrics:
        v1_ = r1.get(key, 0)
        v2_ = r2.get(key, 0)
        sign = "+" if v2_ >= v1_ else ""
        print(f"  {label:<12} v1={v1_:>8.2f}  v2={v2_:>8.2f}  delta={sign}{v2_-v1_:.2f}")

    print("\n  Per-year NET delta (v2 - v1):")
    years = sorted(set(r1["by_year"]) | set(r2["by_year"]))
    for y in years:
        y1 = r1["by_year"].get(y, 0)
        y2 = r2["by_year"].get(y, 0)
        sign = "+" if y2 >= y1 else ""
        print(f"    {y}: v1=₹{y1:,.0f}  v2=₹{y2:,.0f}  delta={sign}₹{y2-y1:,.0f}")


if __name__ == "__main__":
    main()
