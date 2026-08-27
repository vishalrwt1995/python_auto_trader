"""P10 — is swing's entry SCORER load-bearing, or is the channel really just an exit strategy?

WHY THIS ARM EXISTS. The 08-18..20 grind (§8 ㉗) established the mechanism: swing's edge is
**exit asymmetry, not selection**. Every scorer it recorded is uncorrelated with outcome —
`adj_score r=-0.006`, `raw_score r=-0.033`, `wl_score r=+0.037` vs net — and the LOWEST
adj_score bucket OUTPERFORMS the highest (+Rs2,105 vs +Rs894/trade). Winners were
indistinguishable from losers at entry on all 11 recorded features.

If that is literally true, the scorer is dead weight and deleting it should not hurt. Nobody has
run that test. ~49 arms swept gates, exits, slots, hysteresis and filters — i.e. every
CONFIGURATION axis — but never asked whether the selection layer contributes anything at all.

This is falsifiable in both directions, which is the point:
  * net holds up without the scorer  => the scorer is noise; swing is an exit strategy wearing a
    selection costume, and the honest rebuild is "screen for liquidity/vol, let the trail sort".
  * net collapses                    => the scorer DOES carry signal that a linear correlation
    missed (non-monotonic, or interacting with the regime multiplier), and r~0 was a misleading
    summary statistic. Also a real finding.

THE SCORER HAS THREE SEPARATE ROLES. Collapsing them would confound the result, so each is
switched independently:

  role 1  fast pre-filter    `comp < _emit_floor`      (component/watchlist score, line ~650)
  role 2  admission gate     `adj_score < EMIT_FLOOR`  (signal score x regime mult, line ~761)
  role 3  slot allocation    `sorted(cands, -wl_score)` (which candidates get the 5+2 slots)

⚠️ Role 2 is NOT reachable via the `emit_floor` PARAMETER. Line 575 binds the parameter to a
local (`_emit_floor`, role 1) while line 761 reads the MODULE GLOBAL `EMIT_FLOOR`. So
`emit_floor=0` silently leaves the adj_score gate at 45. Role 2 requires setting `H.EMIT_FLOOR`.
(Consequence worth recording: the harness's own T4 "EMIT_FLOOR sweep" was sweeping the component
pre-filter, not the signal-score threshold.)

Role 3 needs the additive `CAND_SORT` knob (default None == prod's -wl_score sort, so every
existing swing_p*.py is byte-unaffected).

Read-only, single-process, local cache only -- NO BigQuery, so zero GCP cost. No prod module is
mutated: `domain/`, `services/` and `settings.py` are untouched; the only edit is an additive knob
in this grind harness.

BASELINE REPRODUCTION IS A GATE, NOT A FORMALITY. This harness has a documented history of silent
flag no-ops (role 2 above is one), and an earlier session ran the wrong script entirely and only
caught it on the baseline check. If the baseline does not reproduce n=265 / +Rs530,568, every arm
below is meaningless and the run aborts.
"""
from __future__ import annotations

import json
import os
import pickle
import random
import sys
import time

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

# documented baseline from §8 ㉗ (reproduced 3x identically during that grind)
EXPECT_N, EXPECT_NET = 265, 530568.0
TOL_N, TOL_NET = 8, 0.03          # allow tiny drift, not a different result

SEEDS = (0, 1, 2, 3, 4)           # role 3 is stochastic -> needs >1 seed to beat variance


def summarize(r):
    if not r:
        return None
    by = r.get("by_year") or {}
    return dict(n=r.get("n", 0), wr=r.get("wr", 0.0), net=r.get("net", 0.0),
                mdd=r.get("mdd", 0.0), calmar=r.get("calmar", 0.0),
                pos=sum(1 for v in by.values() if v > 0), tot=len(by),
                by_year=dict(by))


def era(by_year, lo, hi):
    """Era P&L — the grind's central worry was decay (2015-21 Calmar 1.15 -> 2022-26 0.20)."""
    return sum(v for k, v in by_year.items() if lo <= str(k)[:4] <= hi)


def show(label, s, t=None):
    if not s:
        print(f"  {label:<34}  (no result)")
        return
    dur = f"  [{t:.0f}s]" if t else ""
    print(f"  {label:<34} n={s['n']:>4} WR={s['wr']:>5.1f} net={s['net']:>11,.0f} "
          f"mdd={s['mdd']:>6.1f} Cal={s['calmar']:>5.2f} yrs+={s['pos']}/{s['tot']}{dur}")


def main() -> None:
    print("loading bars + regime + market_inputs ...")
    raw = pickle.load(open(BARS, "rb"))
    regime = json.load(open(H.REGIME_JSON))
    mi = json.load(open(H.MARKET_INPUTS_JSON))
    symdata = {s: H.Sym(b) for s, b in raw.items() if len(b) >= H.MIN_BARS_SWING}
    print(f"  {len(symdata)} symbols\n")

    g_floor, g_sort = H.EMIT_FLOOR, H.CAND_SORT
    rows = []

    # ---------------- GATE: baseline must reproduce ----------------
    print("=" * 96)
    print("=== GATE — baseline reproduction (abort if this fails)")
    print("=" * 96)
    t0 = time.time()
    base = summarize(H.run(symdata, regime, mi, d0=D0, d1=D1, verbose=False, **SHIPPED))
    t_base = time.time() - t0
    show("BASELINE (shipped)", base, t_base)
    if not base:
        raise SystemExit("baseline produced no result — abort")
    dn = abs(base["n"] - EXPECT_N)
    dnet = abs(base["net"] - EXPECT_NET) / EXPECT_NET
    print(f"\n  expected n={EXPECT_N} net={EXPECT_NET:,.0f}  ->  got n={base['n']} "
          f"net={base['net']:,.0f}  (dn={dn}, dnet={dnet*100:.2f}%)")
    if dn > TOL_N or dnet > TOL_NET:
        raise SystemExit("BASELINE DOES NOT REPRODUCE — every arm below would be meaningless. "
                         "Abort and fix the harness first.")
    print("  -> baseline reproduces; the additive CAND_SORT knob is inert as intended. Proceeding.")
    rows.append(("baseline (shipped)", base))

    # ---------------- ARM A3 — role 3 randomised ----------------
    print("\n" + "=" * 96)
    print("=== A3 — RANDOM slot ordering (role 3). Both score gates kept at 45.")
    print("     THE SHARPEST TEST: if wl_score ranking carries signal, shuffling must hurt.")
    print("=" * 96)
    H.CAND_SORT = "random"
    a3s = []
    for sd in SEEDS:
        H._CAND_RNG = random.Random(sd)
        t0 = time.time()
        r = summarize(H.run(symdata, regime, mi, d0=D0, d1=D1, verbose=False, **SHIPPED))
        show(f"A3 random order seed={sd}", r, time.time() - t0)
        if r:
            a3s.append(r)
    H.CAND_SORT, H._CAND_RNG = g_sort, None
    if a3s:
        n = len(a3s)
        mean = dict(n=sum(x["n"] for x in a3s) / n, wr=sum(x["wr"] for x in a3s) / n,
                    net=sum(x["net"] for x in a3s) / n, mdd=sum(x["mdd"] for x in a3s) / n,
                    calmar=sum(x["calmar"] for x in a3s) / n,
                    pos=sum(x["pos"] for x in a3s) / n, tot=a3s[0]["tot"],
                    by_year={})
        nets = sorted(x["net"] for x in a3s)
        print(f"\n  A3 across {n} seeds: net min {nets[0]:,.0f} / median {nets[n//2]:,.0f} "
              f"/ max {nets[-1]:,.0f}   (baseline {base['net']:,.0f})")
        rows.append((f"A3 role3 random (mean of {n})", mean))

    # ---------------- INTERIM VERDICT (the answer to the actual question) ----------------
    print("\n" + "=" * 96)
    print("=== INTERIM VERDICT — A3 vs baseline (this is the decisive comparison)")
    print("=" * 96)
    if a3s:
        _bn = base["net"]
        _nets = sorted(x["net"] for x in a3s)
        _med = _nets[len(_nets) // 2]
        _ratio = _med / _bn if _bn else 0.0
        _cals = sorted(x["calmar"] for x in a3s)
        print(f"  baseline        net {_bn:>11,.0f}   Calmar {base['calmar']:.2f}")
        print(f"  A3 random       net {_med:>11,.0f} (median of {len(_nets)})   "
              f"Calmar median {_cals[len(_cals)//2]:.2f}")
        print(f"  A3/baseline net ratio: {_ratio*100:.1f}%   "
              f"seed spread {_nets[0]:,.0f} .. {_nets[-1]:,.0f}")
        if _ratio >= 0.90:
            print("\n  => wl_score ORDERING IS NOISE. Shuffling the pick order costs <10% of net,")
            print("     so the scorer's slot-allocation role carries no signal. Consistent with")
            print("     the grind's r=+0.037. Swing looks like an EXIT strategy.")
        elif _ratio <= 0.70:
            print("\n  => ORDERING CARRIES REAL SIGNAL. Shuffling destroys >30% of net, so r~0 was")
            print("     a misleading summary stat (non-monotonic or regime-interacting). The")
            print("     scorer is load-bearing after all — do NOT delete it.")
        else:
            print("\n  => PARTIAL. Ordering contributes something but not decisively. Needs the")
            print("     era split below before drawing any conclusion.")
        print("\n  ERA SPLIT for A3 seeds (does any effect survive 2022-26?)")
        for _sd, _r in zip(SEEDS, a3s):
            _e1 = era(_r["by_year"], "2015", "2021")
            _e2 = era(_r["by_year"], "2022", "2026")
            print(f"    seed {_sd}   2015-21 {_e1:>11,.0f}   2022-26 {_e2:>11,.0f}")
        _b1 = era(base["by_year"], "2015", "2021")
        _b2 = era(base["by_year"], "2022", "2026")
        print(f"    baseline   2015-21 {_b1:>11,.0f}   2022-26 {_b2:>11,.0f}")
    else:
        print("  A3 produced no results — cannot judge.")
    print("\n  (A2/A1/A4 continue below; they decompose WHICH role mattered, but the")
    print("   headline question is already answered above.)")

    # ---------------- ARM A2 — role 2 off ----------------
    print("\n" + "=" * 96)
    print("=== A2 — drop the ADJ_SCORE admission gate (role 2), via the MODULE GLOBAL.")
    print("     Component pre-filter stays at 45, ordering stays wl_score.")
    print("=" * 96)
    H.EMIT_FLOOR = 0.0
    t0 = time.time()
    a2 = summarize(H.run(symdata, regime, mi, d0=D0, d1=D1, verbose=False, **SHIPPED))
    show("A2 no adj_score gate", a2, time.time() - t0)
    rows.append(("A2 role2 off (adj_score gate)", a2))
    H.EMIT_FLOOR = g_floor

    # ---------------- ARM A1 — role 1 off ----------------
    print("\n" + "=" * 96)
    print("=== A1 — drop the COMPONENT pre-filter (role 1). adj_score gate + wl_score order kept.")
    print("=" * 96)
    cfg = dict(SHIPPED, emit_floor=0)
    t0 = time.time()
    a1 = summarize(H.run(symdata, regime, mi, d0=D0, d1=D1, verbose=False, **cfg))
    show("A1 no component pre-filter", a1, time.time() - t0)
    rows.append(("A1 role1 off (comp pre-filter)", a1))

    # ---------------- ARM A4 — all three off ----------------
    print("\n" + "=" * 96)
    print("=== A4 — NO SCORER AT ALL: both gates off + random ordering.")
    print("=" * 96)
    H.EMIT_FLOOR, H.CAND_SORT = 0.0, "random"
    a4s = []
    for sd in SEEDS[:3]:
        H._CAND_RNG = random.Random(sd)
        t0 = time.time()
        r = summarize(H.run(symdata, regime, mi, d0=D0, d1=D1,
                            verbose=False, **dict(SHIPPED, emit_floor=0)))
        show(f"A4 no scorer seed={sd}", r, time.time() - t0)
        if r:
            a4s.append(r)
    H.EMIT_FLOOR, H.CAND_SORT, H._CAND_RNG = g_floor, g_sort, None
    if a4s:
        n = len(a4s)
        rows.append((f"A4 all roles off (mean of {n})",
                     dict(n=sum(x["n"] for x in a4s) / n, wr=sum(x["wr"] for x in a4s) / n,
                          net=sum(x["net"] for x in a4s) / n,
                          mdd=sum(x["mdd"] for x in a4s) / n,
                          calmar=sum(x["calmar"] for x in a4s) / n,
                          pos=sum(x["pos"] for x in a4s) / n, tot=a4s[0]["tot"], by_year={})))

    # ---------------- SUMMARY ----------------
    print("\n\n" + "=" * 96)
    print("=== SUMMARY — judge on Calmar + years-positive, NOT on net alone")
    print("=" * 96)
    for label, s in rows:
        show(label, s)

    print("\n  ERA SPLIT (the grind's central worry: 2015-21 Calmar 1.15 -> 2022-26 0.20)")
    for label, s in rows:
        if s and s.get("by_year"):
            e1, e2 = era(s["by_year"], "2015", "2021"), era(s["by_year"], "2022", "2026")
            print(f"    {label:<34} 2015-21 {e1:>11,.0f}   2022-26 {e2:>11,.0f}")

    print("\n  READING THIS:")
    print("   * A3 net ~= baseline across seeds  -> wl_score ordering is NOISE (scorer role 3 dead)")
    print("   * A3 net << baseline               -> ordering DOES carry signal; r~0 was misleading")
    print("   * A4 net ~= baseline               -> the whole selection layer is dead weight, and")
    print("                                         swing is an exit strategy; rebuild accordingly")
    print("   * any arm BEATING baseline         -> treat as a hypothesis, NOT a finding. It owes")
    print("                                         IS/OOS, 3x slippage, and a plateau before it")
    print("                                         counts (feedback_validation_bar).")


if __name__ == "__main__":
    main()
