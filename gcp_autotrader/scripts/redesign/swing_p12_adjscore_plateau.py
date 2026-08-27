"""P12 — the LAST untested axis: does dropping the adj_score admission gate (role 2) survive?

P10 found role 2 mildly harmful on the full window:
    EMIT_FLOOR=45 (shipped)   n=265  net 530,568  Cal 0.45  mdd -14.8  9/10
    EMIT_FLOOR=0             n=267  net 592,361  Cal 0.49  mdd -14.8  9/10
+12% net, +0.04 Calmar, identical drawdown. Smaller than A1's apparent +73%, but A1 then died
on the bar (P11: plateau SPIKE, and OOS 0.53 vs shipped 0.54 — a dead heat).

So this gets the SAME bar, no shortcuts. Prior expectation is low: 59 arms across 14 hypotheses
have produced zero surviving improvements. A +12% full-window gain with no plateau or OOS support
is exactly the shape that keeps failing.

  PHASE A  PLATEAU    EMIT_FLOOR in {45,35,25,15,5,0}
  PHASE B  COST       3x per-leg slippage on shipped-45 and the best floor
  PHASE C  IS/OOS     fit 2015-2020 vs test 2021-2026

⚠️ ROLE SEPARATION — this is the whole reason P12 is separate from P11.
Role 2 is the MODULE GLOBAL `EMIT_FLOOR` (line ~761, `adj_score < EMIT_FLOOR`), NOT the
`emit_floor` PARAMETER — line 575 binds the parameter to a local used only by role 1's component
pre-filter. So here the parameter is pinned at 45 (shipped pre-filter, unchanged) while
H.EMIT_FLOOR is swept. P11 did the mirror image. Getting this backwards would silently sweep the
wrong knob, which is exactly what the harness's own T4 sweep did.

Also note from P11: role 1's knob SATURATES below ~20 (floors 15/5/0 gave byte-identical
results). Watch for the same here — identical rows mean the knob stopped binding, and an apparent
"plateau" at the bottom is one point measured several times, not independent evidence.

Read-only, single-process, local cache only — NO BigQuery, zero GCP cost. No prod module mutated.
EMIT_FLOOR=45 doubles as the baseline gate (n=265 / net=530,568); if it fails, abort.
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

# emit_floor pinned at 45 => role 1 (component pre-filter) stays SHIPPED throughout
SHIPPED = dict(
    setups=("MOMENTUM", "PULLBACK"), emit_floor=45, compound_pct=2.0, liq_cap_pct=1.0,
    mom_month_block={1}, mom_turnover_exclude=(5.0, 40.0), setup_daily_cap={"MOMENTUM": 2},
    pb_month_block={1, 4, 7}, range_bucket_by_regime=True, range_group_cap=2,
    total_slots=7, tu_slot_cap=5,
)

GATES = [45.0, 35.0, 25.0, 15.0, 5.0, 0.0]
EXPECT_N, EXPECT_NET = 265, 530568.0


def summarize(r):
    if not r:
        return None
    by = r.get("by_year") or {}
    return dict(n=r.get("n", 0), wr=r.get("wr", 0.0), net=r.get("net", 0.0),
                mdd=r.get("mdd", 0.0), calmar=r.get("calmar", 0.0),
                pos=sum(1 for v in by.values() if v > 0), tot=len(by), by_year=dict(by))


def era(by, lo, hi):
    return sum(v for k, v in by.items() if lo <= str(k)[:4] <= hi)


def show(label, s, t=None):
    if not s:
        print(f"  {label:<30}  (no result)")
        return
    dur = f"  [{t:.0f}s]" if t else ""
    print(f"  {label:<30} n={s['n']:>4} WR={s['wr']:>5.1f} net={s['net']:>11,.0f} "
          f"mdd={s['mdd']:>6.1f} Cal={s['calmar']:>5.2f} yrs+={s['pos']}/{s['tot']}{dur}")


def main() -> None:
    print("loading bars + regime + market_inputs ...")
    raw = pickle.load(open(BARS, "rb"))
    regime = json.load(open(H.REGIME_JSON))
    mi = json.load(open(H.MARKET_INPUTS_JSON))
    symdata = {s: H.Sym(b) for s, b in raw.items() if len(b) >= H.MIN_BARS_SWING}
    print(f"  {len(symdata)} symbols\n")

    g_floor, base_slip = H.EMIT_FLOOR, H.SLIP
    resA = {}

    print("=" * 100)
    print("=== PHASE A — PLATEAU: adj_score admission gate sweep (module global EMIT_FLOOR)")
    print("===            role 1 pre-filter pinned at 45 throughout; only role 2 moves")
    print("=" * 100)
    for g in GATES:
        H.EMIT_FLOOR = g
        t0 = time.time()
        s = summarize(H.run(symdata, regime, mi, d0=D0, d1=D1, verbose=False, **SHIPPED))
        show(f"EMIT_FLOOR={g:<5.0f}", s, time.time() - t0)
        resA[g] = s
        if g == 45.0:
            if not s:
                H.EMIT_FLOOR = g_floor
                raise SystemExit("gate produced no result — abort")
            if abs(s["n"] - EXPECT_N) > 8 or abs(s["net"] - EXPECT_NET) / EXPECT_NET > 0.03:
                H.EMIT_FLOOR = g_floor
                raise SystemExit(f"GATE FAILED at EMIT_FLOOR=45: n={s['n']} net={s['net']:,.0f}. "
                                 f"Abort.")
            print("     -> gate OK (EMIT_FLOOR=45 reproduces the shipped baseline)")
    H.EMIT_FLOOR = g_floor

    print("\n  PLATEAU SHAPE (Calmar by gate, and era split)")
    print(f"  {'gate':>6} {'n':>5} {'net':>11} {'mdd':>7} {'Calmar':>7} {'yrs+':>6}"
          f" {'2015-21':>11} {'2022-26':>11}")
    for g in GATES:
        s = resA[g]
        if not s:
            continue
        print(f"  {g:>6.0f} {s['n']:>5} {s['net']:>11,.0f} {s['mdd']:>7.1f} {s['calmar']:>7.2f} "
              f"{s['pos']:>3}/{s['tot']:<2} {era(s['by_year'],'2015','2021'):>11,.0f} "
              f"{era(s['by_year'],'2022','2026'):>11,.0f}")

    # saturation check — P11's lesson
    nets = [(g, resA[g]["net"]) for g in GATES if resA[g]]
    dupes = {}
    for g, nv in nets:
        dupes.setdefault(round(nv, 2), []).append(g)
    sat = [v for v in dupes.values() if len(v) > 1]
    if sat:
        print(f"\n  ⚠️ SATURATION: identical results at gates {sat} — the knob stops binding "
              f"there, so those are ONE data point, not several.")

    ok = {g: s for g, s in resA.items() if s}
    best_g = max(ok, key=lambda g: ok[g]["calmar"])
    cals = {g: ok[g]["calmar"] for g in ok}
    neigh = [c for g, c in cals.items() if g != best_g and abs(g - best_g) <= 10]
    print(f"\n  best Calmar {cals[best_g]:.2f} at gate={best_g:.0f}; "
          f"neighbours(+/-10): {[f'{c:.2f}' for c in neigh]}")
    plateau_ok = bool(neigh) and min(neigh) >= 0.75 * cals[best_g]
    print("  -> PLATEAU" if plateau_ok else "  -> SPIKE: treat as OVERFIT (same call as b200->80)")

    print("\n" + "=" * 100)
    print("=== PHASE B — COST STRESS (3x per-leg slippage)")
    print("=" * 100)
    for g in sorted({45.0, best_g}):
        H.EMIT_FLOOR, H.SLIP = g, base_slip * 3.0
        t0 = time.time()
        s = summarize(H.run(symdata, regime, mi, d0=D0, d1=D1, verbose=False, **SHIPPED))
        show(f"gate={g:<5.0f} slip 3x", s, time.time() - t0)
        if s and resA.get(g) and resA[g]["net"]:
            print(f"     -> retains {s['net']/resA[g]['net']*100:.0f}% of its own 1x net, "
                  f"Calmar {resA[g]['calmar']:.2f} -> {s['calmar']:.2f}")
    H.EMIT_FLOOR, H.SLIP = g_floor, base_slip

    print("\n" + "=" * 100)
    print("=== PHASE C — IS/OOS (the check that killed A1: 0.53 OOS vs shipped 0.54)")
    print("=" * 100)
    for g in sorted({45.0, best_g}):
        H.EMIT_FLOOR = g
        for tag, a, b in (("IS  2015-2020", IS0, IS1), ("OOS 2021-2026", OOS0, OOS1)):
            t0 = time.time()
            s = summarize(H.run(symdata, regime, mi, d0=a, d1=b, verbose=False, **SHIPPED))
            show(f"gate={g:<5.0f} {tag}", s, time.time() - t0)
    H.EMIT_FLOOR = g_floor

    print("\n" + "=" * 100)
    print("=== VERDICT — same four checks. A1 passed only COST and YEARS and was rejected.")
    print("=" * 100)
    print(f"  PLATEAU : {'PASS' if plateau_ok else 'FAIL'}")
    print("  COST    : see Phase B — needs Calmar above the shipped 0.45 at 3x")
    print("  OOS     : see Phase C — OOS must not be worse than IS, AND must beat shipped's 0.54")
    print("  YEARS   : must at least match the shipped 9/10")
    print("\n  If this fails too, the swing conclusion is settled: 65 arms, 15 hypotheses,")
    print("  zero surviving improvements. The constraint is REGIME, not configuration, and")
    print("  the remaining decision is portfolio allocation rather than engineering.")


if __name__ == "__main__":
    main()
