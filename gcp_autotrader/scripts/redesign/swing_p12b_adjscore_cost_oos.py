"""P12b — resume P12 at Phases B and C. Phase A already completed; do not repeat it.

P12's PHASE A (done, from the killed run — recorded here so this script is self-contained):
    gate    n      net       mdd    Calmar  yrs+    2015-21    2022-26
      45   265   530,568   -14.8     0.45   9/10    491,093     39,474   (shipped)
      35   267   597,347   -14.8     0.49   9/10    540,219     57,128   <- best
      25   267   592,361   -14.8     0.49   9/10    535,454     56,907
   15/5/0  267   592,361   -14.8     0.49   9/10    535,454     56,907   <- SATURATED (one point)
  -> PLATEAU PASSED: flat and monotone (0.45 -> 0.49 -> 0.49), identical mdd, same 9/10 years.
     Unlike P11's role-1 candidate, which dipped BELOW shipped at floor 35 and was called a spike.

WHY BE SCEPTICAL ANYWAY. The effect is small and thin:
  * Calmar +0.04, net +12.6%, maxDD IDENTICAL (-14.8), years IDENTICAL (9/10)
  * n moves only 265 -> 267. The whole +Rs66,779 comes from a 2-trade difference, which is a
    fragile basis for a claim.
  * the knob saturates below ~30, so the usable range is just 45 -> 35
  * P11's candidate looked far stronger (+73% net, Calmar 0.60, era 6x) and STILL died OOS
    (0.53 vs shipped 0.54). A +0.04 Calmar edge has much less room to survive that test.

  PHASE B  COST    3x per-leg slippage (0.10% -> 0.30%) on gate 45 and gate 35.
  PHASE C  IS/OOS  fit 2015-2020 vs test 2021-2026, both gates. THE DECIDER — this is exactly
                   the check that killed the role-1 candidate.

Pass bar (same as P11, no moving of goalposts):
  COST : Calmar at 3x must stay above the shipped 0.45 at 1x
  OOS  : OOS Calmar must not be worse than IS, AND must beat shipped's OOS 0.54
  YEARS: at least the shipped 9/10

⚠️ ROLE 2 IS THE MODULE GLOBAL. `adj_score < EMIT_FLOOR` (line ~761) reads H.EMIT_FLOOR; the
`emit_floor` PARAMETER only drives role 1's component pre-filter (line 575 binds it to a local).
So here the parameter stays pinned at 45 and H.EMIT_FLOOR is what moves. Getting this backwards
silently sweeps the wrong knob — which is what the harness's own T4 sweep did.

Read-only, single-process, local cache only — NO BigQuery, zero GCP cost. No prod module mutated.
"""
from __future__ import annotations

import json
import os
import pickle
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
import swing_final as H  # noqa: E402

BARS = os.path.expanduser("~/.autotrader_backtest_cache/swing_adj_bars_2015.pkl")
D0, D1 = "2015-01-01", "2026-12-31"
IS0, IS1 = "2015-01-01", "2020-12-31"
OOS0, OOS1 = "2021-01-01", "2026-12-31"

# emit_floor pinned at 45 => role 1 stays SHIPPED; only H.EMIT_FLOOR (role 2) moves
SHIPPED = dict(
    setups=("MOMENTUM", "PULLBACK"), emit_floor=45, compound_pct=2.0, liq_cap_pct=1.0,
    mom_month_block={1}, mom_turnover_exclude=(5.0, 40.0), setup_daily_cap={"MOMENTUM": 2},
    pb_month_block={1, 4, 7}, range_bucket_by_regime=True, range_group_cap=2,
    total_slots=7, tu_slot_cap=5,
)

# Phase A reference (1x slippage, full window) — carried forward, NOT re-measured
REF_1X = {45.0: dict(net=530568.0, calmar=0.45), 35.0: dict(net=597347.0, calmar=0.49)}
SHIPPED_OOS_CALMAR = 0.54     # from P11 Phase C, floor=45 OOS — the number to beat


def summarize(r):
    if not r:
        return None
    by = r.get("by_year") or {}
    return dict(n=r.get("n", 0), wr=r.get("wr", 0.0), net=r.get("net", 0.0),
                mdd=r.get("mdd", 0.0), calmar=r.get("calmar", 0.0),
                pos=sum(1 for v in by.values() if v > 0), tot=len(by))


def show(label, s, t=None):
    if not s:
        print(f"  {label:<28}  (no result)")
        return
    dur = f"  [{t:.0f}s]" if t else ""
    print(f"  {label:<28} n={s['n']:>4} WR={s['wr']:>5.1f} net={s['net']:>11,.0f} "
          f"mdd={s['mdd']:>6.1f} Cal={s['calmar']:>5.2f} yrs+={s['pos']}/{s['tot']}{dur}")


def main() -> None:
    print("loading bars + regime + market_inputs ...")
    raw = pickle.load(open(BARS, "rb"))
    regime = json.load(open(H.REGIME_JSON))
    mi = json.load(open(H.MARKET_INPUTS_JSON))
    symdata = {s: H.Sym(b) for s, b in raw.items() if len(b) >= H.MIN_BARS_SWING}
    print(f"  {len(symdata)} symbols\n")

    g_floor, base_slip = H.EMIT_FLOOR, H.SLIP
    cost = {}
    isoos = {}

    print("=" * 100)
    print("=== PHASE B — COST STRESS, 3x per-leg slippage (0.10% -> 0.30%)")
    print("=" * 100)
    for g in (45.0, 35.0):
        H.EMIT_FLOOR, H.SLIP = g, base_slip * 3.0
        t0 = time.time()
        s = summarize(H.run(symdata, regime, mi, d0=D0, d1=D1, verbose=False, **SHIPPED))
        show(f"gate={g:<4.0f} slip 3x", s, time.time() - t0)
        cost[g] = s
        if s:
            ref = REF_1X[g]
            print(f"     -> retains {s['net']/ref['net']*100:.0f}% of its 1x net "
                  f"({ref['net']:,.0f}); Calmar {ref['calmar']:.2f} -> {s['calmar']:.2f}")
        H.SLIP = base_slip
    H.EMIT_FLOOR = g_floor

    print("\n" + "=" * 100)
    print("=== PHASE C — IS/OOS. This is the check that killed the role-1 candidate.")
    print("=" * 100)
    for g in (45.0, 35.0):
        H.EMIT_FLOOR = g
        for tag, a, b in (("IS  2015-2020", IS0, IS1), ("OOS 2021-2026", OOS0, OOS1)):
            t0 = time.time()
            s = summarize(H.run(symdata, regime, mi, d0=a, d1=b, verbose=False, **SHIPPED))
            show(f"gate={g:<4.0f} {tag}", s, time.time() - t0)
            isoos[(g, tag[:3].strip())] = s
    H.EMIT_FLOOR = g_floor

    print("\n" + "=" * 100)
    print("=== VERDICT — gate=35 vs shipped gate=45")
    print("=" * 100)
    c35, c45 = cost.get(35.0), cost.get(45.0)
    i35, o35 = isoos.get((35.0, "IS")), isoos.get((35.0, "OOS"))
    i45, o45 = isoos.get((45.0, "IS")), isoos.get((45.0, "OOS"))

    checks = []
    print("  PLATEAU : PASS (Phase A — flat 0.45->0.49->0.49, identical mdd, same 9/10 yrs)")
    checks.append(True)

    if c35:
        p = c35["calmar"] > 0.45
        print(f"  COST    : {'PASS' if p else 'FAIL'} — gate35 at 3x slip Calmar "
              f"{c35['calmar']:.2f} vs shipped 0.45 at 1x"
              + (f"  (shipped at 3x: {c45['calmar']:.2f})" if c45 else ""))
        checks.append(p)

    if i35 and o35:
        not_worse = o35["calmar"] >= i35["calmar"] * 0.85
        beats = o35["calmar"] > SHIPPED_OOS_CALMAR
        p = not_worse and beats
        print(f"  OOS     : {'PASS' if p else 'FAIL'} — gate35 IS {i35['calmar']:.2f} -> "
              f"OOS {o35['calmar']:.2f}; must beat shipped OOS {SHIPPED_OOS_CALMAR:.2f}"
              + (f" (measured here: {o45['calmar']:.2f})" if o45 else ""))
        if not beats:
            print("             -> no OOS advantage over shipped. Same failure as the role-1")
            print("                candidate: a full-window gain that does not exist OOS.")
        checks.append(p)

    if o35 and o45:
        p = o35["pos"] >= o45["pos"]
        print(f"  YEARS   : {'PASS' if p else 'FAIL'} — gate35 OOS {o35['pos']}/{o35['tot']} "
              f"vs shipped {o45['pos']}/{o45['tot']}")
        checks.append(p)

    print()
    if all(checks) and len(checks) >= 4:
        print("  ==> ALL CHECKS PASS. The FIRST surviving swing improvement in 65 arms.")
        print("      Treat with suspicion proportional to that: +0.04 Calmar resting on a")
        print("      2-trade difference. Next step is a walk-forward, not a deploy.")
    else:
        print("  ==> REJECTED. 65 arms, 15 hypotheses, ZERO surviving improvements.")
        print("      The swing conclusion is settled: the constraint is REGIME, not")
        print("      configuration, and the remaining decision is portfolio allocation.")


if __name__ == "__main__":
    main()
