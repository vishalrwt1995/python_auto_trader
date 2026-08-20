"""P9 — the EXIT grind. The dimension P4's mechanism points at, and that P2-P8 never touched.

Why this exists (and why the earlier phases were aimed wrong): P4 proved swing's edge is NOT
selection -- the scorer's correlation to outcome is +-0.04 and winners/losers are identical on
every entry feature. The edge is the EXIT asymmetry:

    TRAIL     n= 58 @ +1.76R = +Rs886,062   <- the edge
    MAX_HOLD  n=127 @ +0.31R = +Rs385,621   <- 48% of trades, near-zero return, 20 days of slot
    SL        n= 87 @ -1.06R = -Rs855,360
                                expectancy +0.18R/trade

All 28 prior arms varied SELECTION (gates, filters, caps, slots) = the noise axis.
None varied activate_R / trail_R / max_hold = the signal axis. This corrects that.

Coordinate sweep (one dimension at a time; a full grid is unaffordable at ~3-4 min/arm):
  A. max_hold   {8,10,12,15,20*,25}  -- attacks the 127-trade MAX_HOLD pool directly.
                Occupancy is only ~23%, so faster slot recycling may compound.
  B. activate_R {1.0,1.25,1.5,1.75*,2.25} -- where the trail arms. Lower = arm sooner, convert
                MAX_HOLDs into TRAILs but risk arming into noise.
  C. trail_R    {0.5,0.75,1.0*,1.5}  -- how tightly it follows once armed.
(* = shipped)

Per arm we print the EXIT-REASON MIX, because that is the mechanism: a change is only
interesting if it moves trades OUT of the +0.31R bucket INTO the +1.76R bucket without
inflating the -1.06R bucket.

Same bar as everything else: beat control on Calmar, be a PLATEAU not a spike, and any winner
then owes OOS + slippage. Control must reproduce 265 / +Rs530,568 or the run is void.
Read-only; scripts/redesign only; no prod module, state, env or deploy touched.
"""
from __future__ import annotations

import collections
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

ARMS: list[tuple[str, dict | None]] = [("CONTROL shipped (1.75 / 1.0 / 20d)", None)]
for mh in (8, 10, 12, 15, 25):
    ARMS.append((f"A max_hold {mh}d", {"max_hold": mh}))
for ar in (1.0, 1.25, 1.5, 2.25):
    ARMS.append((f"B activate_R {ar}", {"activate_R": ar}))
for tr in (0.5, 0.75, 1.5):
    ARMS.append((f"C trail_R {tr}", {"trail_R": tr}))


def main() -> None:
    print("loading bars + regime + market_inputs ...")
    raw = pickle.load(open(BARS, "rb"))
    regime = json.load(open(H.REGIME_JSON))
    mi = json.load(open(H.MARKET_INPUTS_JSON))
    print("building indicator series (once) ...")
    symdata = {s: H.Sym(b) for s, b in raw.items() if len(b) >= H.MIN_BARS_SWING}
    print(f"  {len(symdata)} symbols\n")

    rows = []
    for label, override in ARMS:
        cfg = dict(SHIPPED)
        if override:
            cfg["mom_exit_override"] = override
        trades: list = []
        print(f"\n{'='*80}\n=== {label}\n{'='*80}")
        r = H.run(symdata, regime, mi, d0=D0, d1=D1, verbose=True, trades_out=trades, **cfg)
        mix = collections.Counter(t["reason"] for t in trades)
        netmix = collections.defaultdict(float)
        for t in trades:
            netmix[t["reason"]] += t["net"]
        by = (r or {}).get("by_year") or {}
        rows.append((label, r, sum(1 for v in by.values() if v > 0), len(by), mix, netmix))
        print(f"    reason mix: " + "  ".join(
            f"{k}={mix[k]}/Rs{netmix[k]:,.0f}" for k in ("TRAIL", "MAX_HOLD", "SL") if mix[k]))

    print(f"\n\n{'='*118}\n=== SUMMARY — a win must move trades from MAX_HOLD(+0.31R) into TRAIL(+1.76R)\n{'='*118}")
    print(f"  {'arm':<30} {'n':>4} {'WR%':>6} {'net':>11} {'maxDD':>7} {'Calmar':>7} {'yrs+':>6} "
          f"{'TRAIL':>6} {'MAXH':>6} {'SL':>5}")
    ctrl_calmar = None
    for label, r, pos, tot, mix, _nm in rows:
        if not r:
            print(f"  {label:<30}  (no result)")
            continue
        if ctrl_calmar is None:
            ctrl_calmar = r.get("calmar", 0.0)
        star = " *" if r.get("calmar", 0) > ctrl_calmar else ""
        print(f"  {label:<30} {r.get('n',0):>4} {r.get('wr',0):>6.1f} {r.get('net',0):>11,.0f} "
              f"{r.get('mdd',0):>7.1f} {r.get('calmar',0):>7.2f} {pos:>3}/{tot:<2} "
              f"{mix['TRAIL']:>6} {mix['MAX_HOLD']:>6} {mix['SL']:>5}{star}")

    c = rows[0][1]
    if c:
        ok = c.get("n") == 265 and abs(c.get("net", 0) - 530568) < 1500
        print(f"\n  CONTROL: {'PASS' if ok else '*** FAIL — arms VOID ***'} "
              f"(n={c.get('n')}, net={c.get('net',0):,.0f})")
    beat = [(r.get("calmar", 0), lab) for lab, r, *_ in rows[1:] if r and r.get("calmar", 0) > (c or {}).get("calmar", 9)]
    if beat:
        beat.sort(reverse=True)
        print(f"  arms beating control on Calmar: {[f'{l} ({v:.2f})' for v, l in beat]}")
        print("  -> next: plateau across the winning dimension, then OOS + 3x slippage.")
    else:
        print("  -> NO arm beats control on Calmar. Exit params confirmed optimal as shipped.")


if __name__ == "__main__":
    main()
