"""P3 — finish the H1 question: is b200>=80 a PLATEAU or a PEAK, and does it survive cost stress?

H1 (b200 floor 70 -> 80) was the only P2 survivor: Calmar 0.45 -> 0.68, maxDD -14.8% -> -9.1%,
net -10%. Before that can be called a finding it owes the full bar this repo applies:

  PHASE A  PLATEAU     b200 floor in {65,70,75,80,85,90}. A real edge is a broad region, not a
                       spike. This matters especially here: the ORIGINAL gate's own validation
                       was described as a "step-up gradient 66-71%", i.e. explicitly NOT a
                       single-threshold cliff — so an isolated 80 spike would contradict the
                       very evidence that established the gate.
  PHASE B  COST STRESS 1x / 2x / 3x per-leg slippage on the two candidates (70 and 80).
                       Thin-edge strategies die here first. 0.10% -> 0.30%/leg.

Also reports YEARS-POSITIVE, a bar item H1 currently fails (2016 and 2020 negative).

Judge on Calmar + years-positive + plateau shape. NOT on net.
Read-only; single-process; no prod module mutated.
"""
from __future__ import annotations

import json
import os
import pickle
import sys

sys.path.insert(0, os.path.dirname(__file__))
import swing_final as H  # noqa: E402

BARS = os.path.expanduser("~/.autotrader_backtest_cache/swing_adj_bars_2015.pkl")
D0, D1 = "2015-01-01", "2026-12-31"

SHIPPED = dict(
    setups=("MOMENTUM", "PULLBACK"), emit_floor=45, compound_pct=2.0, liq_cap_pct=1.0,
    mom_month_block={1}, mom_turnover_exclude=(5.0, 40.0), setup_daily_cap={"MOMENTUM": 2},
    pb_month_block={1, 4, 7}, range_bucket_by_regime=True, range_group_cap=2,
    total_slots=7, tu_slot_cap=5,
)

PLATEAU = [65.0, 70.0, 75.0, 80.0, 85.0, 90.0]
STRESS = [(70.0, 0.0020), (70.0, 0.0030), (80.0, 0.0020), (80.0, 0.0030)]


def summarize(r):
    if not r:
        return None
    by = r.get("by_year") or {}
    pos = sum(1 for v in by.values() if v > 0)
    return dict(n=r.get("n", 0), wr=r.get("wr", 0.0), net=r.get("net", 0.0),
                mdd=r.get("mdd", 0.0), calmar=r.get("calmar", 0.0),
                pos=pos, tot=len(by))


def main() -> None:
    print("loading bars + regime + market_inputs ...")
    raw = pickle.load(open(BARS, "rb"))
    regime = json.load(open(H.REGIME_JSON))
    mi = json.load(open(H.MARKET_INPUTS_JSON))
    print("building indicator series (once) ...")
    symdata = {s: H.Sym(b) for s, b in raw.items() if len(b) >= H.MIN_BARS_SWING}
    print(f"  {len(symdata)} symbols\n")

    base_floor, base_slip = H.MOM_B200_FLOOR, H.SLIP
    rows = []

    print(f"{'='*90}\n=== PHASE A — PLATEAU sweep (slippage 1x = 0.10%/leg)\n{'='*90}")
    for fl in PLATEAU:
        H.MOM_B200_FLOOR, H.SLIP = fl, base_slip
        print(f"\n-- b200 floor {fl:.0f} --")
        s = summarize(H.run(symdata, regime, mi, d0=D0, d1=D1, verbose=True, **SHIPPED))
        rows.append((f"b200>={fl:.0f}  slip 1x", s))

    print(f"\n\n{'='*90}\n=== PHASE B — COST STRESS on the two candidates\n{'='*90}")
    for fl, sl in STRESS:
        H.MOM_B200_FLOOR, H.SLIP = fl, sl
        print(f"\n-- b200 floor {fl:.0f}  slip {sl*100:.2f}%/leg ({sl/base_slip:.0f}x) --")
        s = summarize(H.run(symdata, regime, mi, d0=D0, d1=D1, verbose=True, **SHIPPED))
        rows.append((f"b200>={fl:.0f}  slip {sl/base_slip:.0f}x", s))

    H.MOM_B200_FLOOR, H.SLIP = base_floor, base_slip

    print(f"\n\n{'='*90}\n=== SUMMARY\n{'='*90}")
    print(f"  {'arm':<26} {'n':>4} {'WR%':>6} {'net':>11} {'maxDD':>7} {'Calmar':>7} {'yrs+':>8}")
    for label, s in rows:
        if not s:
            print(f"  {label:<26}  (no result)")
            continue
        print(f"  {label:<26} {s['n']:>4} {s['wr']:>6.1f} {s['net']:>11,.0f} "
              f"{s['mdd']:>7.1f} {s['calmar']:>7.2f} {s['pos']:>4}/{s['tot']:<3}")

    # plateau verdict: is 80 part of a broad region, or a lone spike?
    pl = [(f, s) for (lab, s) in rows if s and lab.endswith("slip 1x")
          for f in [float(lab.split(">=")[1].split()[0])]]
    if len(pl) >= 3:
        best_f, best = max(pl, key=lambda x: x[1]["calmar"])
        neigh = [s["calmar"] for f, s in pl if abs(f - best_f) <= 5.0 and f != best_f]
        print(f"\n  best Calmar {best['calmar']:.2f} at b200>={best_f:.0f}; "
              f"neighbours(+/-5): {[f'{c:.2f}' for c in neigh]}")
        if neigh and min(neigh) >= 0.75 * best["calmar"]:
            print("  -> PLATEAU: neighbours hold >=75% of peak Calmar")
        else:
            print("  -> PEAK/SPIKE: neighbours collapse — treat as overfit, do NOT ship")


if __name__ == "__main__":
    main()
