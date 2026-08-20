"""P6 — GATE HYSTERESIS: does requiring the breadth gate to HOLD beat raising it?

Mechanism this tests (from P4/C, not from pattern-matching a bad year):
  2025's entire cohort was 6 trades at b200 71.0-71.1 -- i.e. entered AT the gate -- and
  4 of 6 stopped out. Separately, the 80+ bucket averaged +Rs2,126/trade vs +Rs479 for
  70-80. So marginal-gate entries are systematically the weak ones.

  Raising the threshold is already REJECTED: b200>=80 looked good (Calmar 0.68) but the
  plateau test killed it -- 85 collapses to Calmar 0.03, a lone spike.

  Hypothesis: the defect is entering while breadth sits AT the boundary, not the boundary's
  level. Hysteresis (gate must have HELD for K days) filters 2025's 71.0 cohort while KEEPING
  the 79s and 85s that raising the floor discarded.

Arms: sustain 0 (control) / 3 / 5 / 10 / 20 at the SHIPPED floor of 70.
Plus a plateau read across K, and a 3x-slippage stress on the best K.

A real result must (a) beat control on Calmar, (b) be a PLATEAU across adjacent K, and
(c) survive 3x slippage. One good K surrounded by bad ones = overfit, same verdict as H1.

Read-only. Only scripts/redesign/* touched — no prod module, state, env or deploy.
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

SUSTAINS = [0, 3, 5, 10, 20]


def row(r):
    if not r:
        return None
    by = r.get("by_year") or {}
    return dict(n=r.get("n", 0), wr=r.get("wr", 0.0), net=r.get("net", 0.0),
                mdd=r.get("mdd", 0.0), calmar=r.get("calmar", 0.0),
                pos=sum(1 for v in by.values() if v > 0), tot=len(by))


def main() -> None:
    print("loading bars + regime + market_inputs ...")
    raw = pickle.load(open(BARS, "rb"))
    regime = json.load(open(H.REGIME_JSON))
    mi = json.load(open(H.MARKET_INPUTS_JSON))
    print("building indicator series (once) ...")
    symdata = {s: H.Sym(b) for s, b in raw.items() if len(b) >= H.MIN_BARS_SWING}
    print(f"  {len(symdata)} symbols\n")

    base_sus, base_slip = H.MOM_B200_SUSTAIN, H.SLIP
    out = []
    for k in SUSTAINS:
        H.MOM_B200_SUSTAIN, H.SLIP = k, base_slip
        lab = "sustain 0 (CONTROL = shipped)" if k == 0 else f"sustain {k}d"
        print(f"\n{'='*80}\n=== {lab}\n{'='*80}")
        out.append((lab, row(H.run(symdata, regime, mi, d0=D0, d1=D1, verbose=True, **SHIPPED)), k))

    # stress the best non-control K
    cands = [(s["calmar"], k) for lab, s, k in out if s and k > 0]
    best_k = max(cands)[1] if cands else None
    if best_k:
        for mult in (2, 3):
            H.MOM_B200_SUSTAIN, H.SLIP = best_k, base_slip * mult
            print(f"\n{'='*80}\n=== sustain {best_k}d  slip {mult}x\n{'='*80}")
            out.append((f"sustain {best_k}d  slip {mult}x",
                        row(H.run(symdata, regime, mi, d0=D0, d1=D1, verbose=True, **SHIPPED)), -1))

    # --- P5 follow-up: the 10-slot result (+31.5% net, Calmar 0.46 vs 0.45) is the first
    # positive of this grind, but more concurrent positions => more exposure to thin names.
    # An unstressed positive is not shippable, so stress it here (same data load).
    H.MOM_B200_SUSTAIN = base_sus
    TEN = dict(SHIPPED, total_slots=10, tu_slot_cap=7, range_group_cap=3)
    for mult in (1, 2, 3):
        H.SLIP = base_slip * mult
        print(f"\n{'='*80}\n=== 10 slots  slip {mult}x\n{'='*80}")
        out.append((f"10 slots  slip {mult}x",
                    row(H.run(symdata, regime, mi, d0=D0, d1=D1, verbose=True, **TEN)), -2))

    H.MOM_B200_SUSTAIN, H.SLIP = base_sus, base_slip

    print(f"\n\n{'='*100}\n=== SUMMARY — needs Calmar gain AND plateau across K AND 3x survival\n{'='*100}")
    print(f"  {'arm':<34} {'n':>4} {'WR%':>6} {'net':>11} {'maxDD':>7} {'Calmar':>7} {'yrs+':>7}")
    for lab, s, _k in out:
        if not s:
            print(f"  {lab:<34}  (no result)")
            continue
        print(f"  {lab:<34} {s['n']:>4} {s['wr']:>6.1f} {s['net']:>11,.0f} {s['mdd']:>7.1f} "
              f"{s['calmar']:>7.2f} {s['pos']:>3}/{s['tot']:<3}")

    ctrl = out[0][1]
    if ctrl:
        ok = ctrl["n"] == 265 and abs(ctrl["net"] - 530568) < 1500
        print(f"\n  CONTROL: {'PASS' if ok else '*** FAIL — arms VOID ***'} "
              f"(n={ctrl['n']}, net={ctrl['net']:,.0f})")
    # plateau shape across K
    ks = [(k, s["calmar"]) for lab, s, k in out if s and k > 0]
    if len(ks) >= 3 and ctrl:
        bk, bc = max(ks, key=lambda x: x[1])
        neigh = [c for k, c in ks if k != bk]
        print(f"  best sustain={bk}d Calmar={bc:.2f} vs control {ctrl['calmar']:.2f}; "
              f"others={[f'{c:.2f}' for c in neigh]}")
        if bc <= ctrl["calmar"]:
            print("  -> NO GAIN over shipped. Hysteresis rejected.")
        elif neigh and min(neigh) >= 0.75 * bc:
            print("  -> PLATEAU across K and beats control — worth full validation.")
        else:
            print("  -> SPIKE across K: overfit, do NOT ship (same verdict as H1).")


if __name__ == "__main__":
    main()
