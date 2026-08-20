"""P5 — the one hypothesis P4's mechanism actually implies: MORE SHOTS, not better picks.

P4 established two things from trade-level data:
  * the scorer does NOT forecast (r = +-0.04 vs net/R; lowest-score bucket beats highest)
  * winners and losers are INDISTINGUISHABLE at entry on every recorded feature
  => selection is ~noise. The edge is the exit asymmetry (+1.76R trails vs -1.06R stops)
     applied inside a permitted regime, giving +0.18R/trade expectancy.

If expectancy is per-trade and selection carries no information, profit scales with the
NUMBER OF SHOTS, not with pickiness. Every prior attempt tightened; this loosens.

And the binding constraint is NOT the score floor -- the emit_floor sweep found 10 == 1
(candidates are not scarce). It should be SLOTS. So scale the slot structure, holding the
5:2 TREND_UP:RANGE ratio and EVERYTHING else (gate, filters, sizing) fixed.

IMPORTANT competing constraint: run() caps total open notional at CAP*equity_scale
(`sum(notional) + new > CAP*_hs -> skip`). So if CAPITAL already binds at 7 slots, extra
slots will add no trades. That is itself the answer: it would mean participation is
capital-limited, not slot-limited, and the fix is allocation, not configuration.

ARM0 is a control and must reproduce 265 / +Rs530,568.
Judge on Calmar + yrs-positive. Read-only, single-process, no prod module mutated.
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
    pb_month_block={1, 4, 7}, range_bucket_by_regime=True,
)

# (label, total_slots, tu_slot_cap, range_group_cap) — holds the shipped 5:2 shape
ARMS = [
    ("ARM0 CONTROL 7 slots (5 TU + 2 RANGE) — shipped", 7, 5, 2),
    ("10 slots (7 TU + 3 RANGE)", 10, 7, 3),
    ("14 slots (10 TU + 4 RANGE)", 14, 10, 4),
    ("21 slots (15 TU + 6 RANGE) — find saturation", 21, 15, 6),
]


def main() -> None:
    print("loading bars + regime + market_inputs ...")
    raw = pickle.load(open(BARS, "rb"))
    regime = json.load(open(H.REGIME_JSON))
    mi = json.load(open(H.MARKET_INPUTS_JSON))
    print("building indicator series (once) ...")
    symdata = {s: H.Sym(b) for s, b in raw.items() if len(b) >= H.MIN_BARS_SWING}
    print(f"  {len(symdata)} symbols\n")

    out = []
    for label, tot, tu, rg in ARMS:
        cfg = dict(SHIPPED, total_slots=tot, tu_slot_cap=tu, range_group_cap=rg)
        print(f"\n{'='*80}\n=== {label}\n{'='*80}")
        r = H.run(symdata, regime, mi, d0=D0, d1=D1, verbose=True, **cfg)
        by = (r or {}).get("by_year") or {}
        out.append((label, r, sum(1 for v in by.values() if v > 0), len(by)))

    print(f"\n\n{'='*100}\n=== SUMMARY — does adding shots add edge, or does CAPITAL bind first?\n{'='*100}")
    print(f"  {'arm':<50} {'n':>4} {'WR%':>6} {'net':>11} {'maxDD':>7} {'Calmar':>7} {'yrs+':>7}")
    for label, r, pos, tot_y in out:
        if not r:
            print(f"  {label:<50}  (no result)")
            continue
        print(f"  {label:<50} {r.get('n',0):>4} {r.get('wr',0):>6.1f} {r.get('net',0):>11,.0f} "
              f"{r.get('mdd',0):>7.1f} {r.get('calmar',0):>7.2f} {pos:>3}/{tot_y:<3}")

    ctrl = out[0][1] if out and out[0][1] else None
    if ctrl:
        ok = ctrl.get("n") == 265 and abs(ctrl.get("net", 0) - 530568) < 1500
        print(f"\n  CONTROL: {'PASS' if ok else '*** FAIL — arms VOID ***'} "
              f"(n={ctrl.get('n')}, net={ctrl.get('net',0):,.0f})")
    ns = [r.get("n", 0) for _l, r, _p, _t in out if r]
    if len(ns) > 1 and max(ns) - ns[0] <= 3:
        print("  ==> trade count did NOT rise with slots => CAPITAL binds, not slots.\n"
              "      Participation is an ALLOCATION question, not a config one.")


if __name__ == "__main__":
    main()
