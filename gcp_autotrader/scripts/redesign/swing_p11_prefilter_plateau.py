"""P11 — is A1 (drop the component pre-filter) a PLATEAU or a CLIFF? Does it survive the bar?

P10 found the component pre-filter (role 1, `comp < _emit_floor`) is actively HARMFUL:
    shipped floor=45   n=265  net 530,568  Cal 0.45  mdd -14.8  9/10 yrs
    floor=0 (A1)       n=291  net 918,673  Cal 0.60  mdd -16.1  9/10 yrs
and — the part that matters for the channel's actual problem — it fixed the era decay:
    2022-26 era net   39,474 -> 239,877   (6x)

That is +73% net and Calmar 0.45 -> 0.60 from DELETING a filter. But two points is not a
finding, it is a hypothesis. `b200 -> 80` looked just as good in P2 (Calmar 0.68) and P3 killed
it as a LONE SPIKE (85 collapsed to 0.03). This script applies the same bar.

  PHASE A  PLATEAU    floor in {45,35,25,15,5,0}. A real effect is a broad monotone-ish region,
                      not a single lucky point. 6 points matches the house standard used for the
                      b200 plateau in P3.
  PHASE B  COST       3x per-leg slippage (0.10% -> 0.30%) on shipped-45 and the best floor.
                      Thin-edge results die here first; more trades means more cost exposure,
                      and A1 trades MORE (n 265 -> 291), so this is the sharpest risk.
  PHASE C  IS/OOS     fit 2015-2020 vs test 2021-2026, both floors. P10's whole appeal was the
                      2022-26 era — if that is real it must show up OOS, not just in-sample.

Judge on Calmar + years-positive + plateau shape + OOS. NOT on net.

⚠️ PROD-FIDELITY CAVEAT — read before getting excited by any result here.
The component pre-filter IS the watchlist builder: the backtest replays it to reconstruct which
~300 names prod's universe_service would have put on the swing watchlist. Lowering the floor
therefore means trading names that would NEVER appear on the live watchlist. So even a perfect
plateau does not make this an env-var change — it needs a real modification to watchlist
construction (or its size cap) in a shared prod module, with all the fidelity risk that carries.
This script measures whether the idea is worth that cost. It does not make it shippable.

Read-only, single-process, local cache only — NO BigQuery, zero GCP cost. No prod module mutated.
Floor 45 doubles as the baseline reproduction gate (n=265 / net=530,568); if it fails, abort.
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

SHIPPED = dict(
    setups=("MOMENTUM", "PULLBACK"), compound_pct=2.0, liq_cap_pct=1.0,
    mom_month_block={1}, mom_turnover_exclude=(5.0, 40.0), setup_daily_cap={"MOMENTUM": 2},
    pb_month_block={1, 4, 7}, range_bucket_by_regime=True, range_group_cap=2,
    total_slots=7, tu_slot_cap=5,
)

FLOORS = [45, 35, 25, 15, 5, 0]
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

    base_slip = H.SLIP
    resA = {}

    # ---------------- PHASE A — plateau ----------------
    print("=" * 100)
    print("=== PHASE A — PLATEAU: component pre-filter floor sweep (floor 45 = shipped = gate)")
    print("=" * 100)
    for fl in FLOORS:
        t0 = time.time()
        s = summarize(H.run(symdata, regime, mi, d0=D0, d1=D1, verbose=False,
                            **dict(SHIPPED, emit_floor=fl)))
        show(f"floor={fl:<3}", s, time.time() - t0)
        resA[fl] = s
        if fl == 45:
            if not s:
                raise SystemExit("gate produced no result — abort")
            if abs(s["n"] - EXPECT_N) > 8 or abs(s["net"] - EXPECT_NET) / EXPECT_NET > 0.03:
                raise SystemExit(f"GATE FAILED at floor=45: got n={s['n']} net={s['net']:,.0f}, "
                                 f"expected {EXPECT_N} / {EXPECT_NET:,.0f}. Abort.")
            print("     -> gate OK (floor=45 reproduces the shipped baseline)")

    print("\n  PLATEAU SHAPE (Calmar by floor, and era split)")
    print(f"  {'floor':>6} {'n':>5} {'net':>11} {'mdd':>7} {'Calmar':>7} {'yrs+':>6}"
          f" {'2015-21':>11} {'2022-26':>11}")
    for fl in FLOORS:
        s = resA[fl]
        if not s:
            continue
        print(f"  {fl:>6} {s['n']:>5} {s['net']:>11,.0f} {s['mdd']:>7.1f} {s['calmar']:>7.2f} "
              f"{s['pos']:>3}/{s['tot']:<2} {era(s['by_year'],'2015','2021'):>11,.0f} "
              f"{era(s['by_year'],'2022','2026'):>11,.0f}")

    ok = {f: s for f, s in resA.items() if s}
    best_f = max(ok, key=lambda f: ok[f]["calmar"])
    cals = {f: ok[f]["calmar"] for f in ok}
    neigh = [c for f, c in cals.items() if f != best_f and abs(f - best_f) <= 10]
    print(f"\n  best Calmar {cals[best_f]:.2f} at floor={best_f}; "
          f"neighbours(+/-10): {[f'{c:.2f}' for c in neigh]}")
    if neigh and min(neigh) >= 0.75 * cals[best_f]:
        print("  -> PLATEAU: neighbours hold >=75% of peak Calmar. The effect looks structural.")
    else:
        print("  -> SPIKE: neighbours fall below 75% of peak. Treat as OVERFIT, like b200->80.")

    # ---------------- PHASE B — cost stress ----------------
    print("\n" + "=" * 100)
    print("=== PHASE B — COST STRESS (3x per-leg slippage). A1 trades MORE, so it pays more cost.")
    print("=" * 100)
    for fl in sorted({45, best_f}):
        H.SLIP = base_slip * 3.0
        t0 = time.time()
        s = summarize(H.run(symdata, regime, mi, d0=D0, d1=D1, verbose=False,
                            **dict(SHIPPED, emit_floor=fl)))
        show(f"floor={fl:<3} slip 3x", s, time.time() - t0)
        H.SLIP = base_slip
        if s and resA.get(fl):
            keep = s["net"] / resA[fl]["net"] if resA[fl]["net"] else 0.0
            print(f"     -> retains {keep*100:.0f}% of its own 1x net, Calmar "
                  f"{resA[fl]['calmar']:.2f} -> {s['calmar']:.2f}")

    # ---------------- PHASE C — IS/OOS ----------------
    print("\n" + "=" * 100)
    print("=== PHASE C — IS/OOS. P10's appeal was the 2022-26 era; it must hold OUT of sample.")
    print("=" * 100)
    for fl in sorted({45, best_f}):
        for tag, a, b in (("IS  2015-2020", IS0, IS1), ("OOS 2021-2026", OOS0, OOS1)):
            t0 = time.time()
            s = summarize(H.run(symdata, regime, mi, d0=a, d1=b, verbose=False,
                                **dict(SHIPPED, emit_floor=fl)))
            show(f"floor={fl:<3} {tag}", s, time.time() - t0)

    print("\n" + "=" * 100)
    print("=== VERDICT CHECKLIST (all four must hold before this is a FINDING, not a hypothesis)")
    print("=" * 100)
    print("  [ ] PLATEAU  — neighbours within 25% of peak Calmar (see Phase A)")
    print("  [ ] COST     — survives 3x slippage with Calmar still above the shipped 0.45")
    print("  [ ] OOS      — OOS Calmar not worse than IS (no fit-period-only effect)")
    print("  [ ] YEARS    — years-positive at least matches the shipped 9/10")
    print("\n  Even if all four pass: this is NOT an env-var change. The pre-filter is the")
    print("  watchlist builder, so shipping it means modifying universe_service's watchlist")
    print("  construction — a shared prod module. Measure first, decide second.")


if __name__ == "__main__":
    main()
