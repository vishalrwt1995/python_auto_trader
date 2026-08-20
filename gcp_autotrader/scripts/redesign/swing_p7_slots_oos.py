"""P7 — OUT-OF-SAMPLE check on the only surviving candidate: 10 slots vs shipped 7.

Why this is required. The slot result (+31.5% net, Calmar 0.46 vs 0.45, 3x-slippage-robust)
was measured on the FULL 2015-2026 sample -- the same span used to pick 10 as the winner over
7/14/21. That is in-sample selection. Today already killed four hypotheses that looked good
in-sample (H1 b200=80 especially: Calmar 0.68 until the plateau test exposed it), so an
unsplit positive does not clear this repo's bar.

Design -- the split is by TIME and deliberately brutal:
    FIT   2015-2021  (7 yrs; where swing's raw edge demonstrably worked, +Rs269,769)
    TEST  2022-2026  (4.4 yrs; where the raw edge died, -Rs66,215, and where the live
                      channel has been mostly gated out)

The question is NOT "what is the best slot count" -- it is:
    does the 7 -> 10 ORDERING hold in a window we did not choose it on?

14 slots is included in both windows as a shape check. If the ordering is stable (10 > 7 in
both), the finding is regime-robust. If 10 > 7 only in FIT, it is an artefact of the
2015-2021 bull years and must NOT ship.

Everything else is the shipped config, untouched. Read-only; scripts/redesign only.
"""
from __future__ import annotations

import json
import os
import pickle
import sys

sys.path.insert(0, os.path.dirname(__file__))
import swing_final as H  # noqa: E402

BARS = os.path.expanduser("~/.autotrader_backtest_cache/swing_adj_bars_2015.pkl")

SHIPPED = dict(
    setups=("MOMENTUM", "PULLBACK"), emit_floor=45, compound_pct=2.0, liq_cap_pct=1.0,
    mom_month_block={1}, mom_turnover_exclude=(5.0, 40.0), setup_daily_cap={"MOMENTUM": 2},
    pb_month_block={1, 4, 7}, range_bucket_by_regime=True,
)

WINDOWS = [("FIT  2015-2021", "2015-01-01", "2021-12-31"),
           ("TEST 2022-2026", "2022-01-01", "2026-12-31")]
SLOTS = [(7, 5, 2), (10, 7, 3), (14, 10, 4)]


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

    res = {}
    for wlab, d0, d1 in WINDOWS:
        for tot, tu, rg in SLOTS:
            cfg = dict(SHIPPED, total_slots=tot, tu_slot_cap=tu, range_group_cap=rg)
            print(f"\n{'='*80}\n=== {wlab}   {tot} slots ({tu} TU + {rg} RANGE)\n{'='*80}")
            res[(wlab, tot)] = row(H.run(symdata, regime, mi, d0=d0, d1=d1, verbose=True, **cfg))

    print(f"\n\n{'='*100}\n=== SUMMARY — does the 7->10 ordering SURVIVE out of sample?\n{'='*100}")
    print(f"  {'window':<16} {'slots':>6} {'n':>4} {'WR%':>6} {'net':>11} {'maxDD':>7} {'Calmar':>7} {'yrs+':>7}")
    for wlab, _d0, _d1 in WINDOWS:
        for tot, _tu, _rg in SLOTS:
            s = res.get((wlab, tot))
            if not s:
                print(f"  {wlab:<16} {tot:>6}   (no result)")
                continue
            print(f"  {wlab:<16} {tot:>6} {s['n']:>4} {s['wr']:>6.1f} {s['net']:>11,.0f} "
                  f"{s['mdd']:>7.1f} {s['calmar']:>7.2f} {s['pos']:>3}/{s['tot']:<3}")
        print()

    print("=== VERDICT ===")
    verdicts = []
    for wlab, _d0, _d1 in WINDOWS:
        a, b = res.get((wlab, 7)), res.get((wlab, 10))
        if not a or not b:
            continue
        dn = b["net"] - a["net"]
        dc = b["calmar"] - a["calmar"]
        better = dn > 0 and dc >= -0.03      # more net, Calmar not materially worse
        verdicts.append(better)
        print(f"  {wlab}: 10 vs 7 -> net {dn:+,.0f} ({100*dn/max(abs(a['net']),1):+.1f}%), "
              f"Calmar {dc:+.2f}  => {'10 BETTER' if better else '10 NOT better'}")
    if len(verdicts) == 2:
        if all(verdicts):
            print("\n  ==> ORDERING HOLDS IN BOTH WINDOWS. Regime-robust; candidate for prod\n"
                  "      (SWING_MAX_POSITIONS=7 env + SWING_RANGE_GROUP_CAP 2->3 code change).")
        elif verdicts[0] and not verdicts[1]:
            print("\n  ==> HOLDS ONLY IN FIT (2015-2021 bull years). IN-SAMPLE ARTEFACT — DO NOT SHIP.")
        elif verdicts[1] and not verdicts[0]:
            print("\n  ==> HOLDS ONLY IN TEST. Suspicious/unstable — do not ship on this evidence.")
        else:
            print("\n  ==> FAILS BOTH. Reject; the full-sample result was a mix artefact.")


if __name__ == "__main__":
    main()
