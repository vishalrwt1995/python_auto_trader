"""PR2 fidelity replay — drive the PRODUCTION swing predicates over the cached
multi-emit pool and confirm they reproduce the validated final_config result
(NET +39,310 at ₹1L, 2022-2026).

What is exercised from PRODUCTION code (not re-implementations):
  - regime gate     domain.regime_affinity.swing_setup_allowed_in_regime
  - slot groups     domain.regime_affinity.swing_setup_group + SWING_RANGE_GROUP_CAP
  - exit            domain.swing_exit.simulate_exit (proven == exit_lab.simulate)
  - setup slate     universe_service._MULTI_EMIT_SETUPS membership (the 3 cells)

Two RS variants quantify the one definitional approximation in prod:
  B  backtest-identical: compounded equal-weight index, entry-day stock close —
     exactly final_config's arithmetic. MUST equal +39,310 (±₹1) or the prod
     predicates are NOT equivalent to the validated config.
  A  prod-faithful: arithmetic universe-mean ret60, as-of-day timing (what
     universe_service.rs_vs_mkt actually computes at build time). The delta vs B
     is the honest cost/benefit of the prod RS definition.

Run: PYTHONPATH=src python3 -m autotrader.backtest_v2.prod_replay_validate
"""
from __future__ import annotations

import collections
import os
import pickle

from autotrader.backtest_v2.exit_lab import load_resolved, book_cap, load_calendar
from autotrader.backtest_v2.final_config import build_market
from autotrader.domain.regime_affinity import (
    SWING_RANGE_GROUP_CAP,
    swing_setup_allowed_in_regime,
    swing_setup_group,
)
from autotrader.domain.swing_exit import simulate_exit

POOL = os.path.expanduser("~/.autotrader_backtest_cache/s2_shorts_trades.json")
CANDLES = os.path.expanduser("~/.autotrader_backtest_cache/candles_daily_all.pkl")
YEARS = ["2022", "2023", "2024", "2025", "2026"]
CAPS = [100000, 200000, 300000, 500000]
CAPL = ["₹1L", "₹2L", "₹3L", "₹5L"]
EXPECT = 39310.0  # final_config cross-checked vs oos_validate
# prod selection slate (universe_service._MULTI_EMIT_SETUPS) + per-cell filters
SLATE = {"MOMENTUM", "PULLBACK", "MEAN_REVERSION"}
RS_SETUPS = {"MEAN_REVERSION", "PULLBACK"}   # trading_service: swing_rs_below_market
BREADTH_SETUPS = {"PULLBACK"}                # trading_service: swing_breadth_below_60


def build_arith_mean_ret60():
    """Per-day arithmetic mean of per-symbol 60d returns (prod's rs_vs_mkt market leg)."""
    candles = pickle.load(open(CANDLES, "rb"))
    acc = collections.defaultdict(float)
    cnt = collections.Counter()
    for _sym, bars in candles.items():
        cl = [b[4] for b in bars]
        for i in range(60, len(bars)):
            if cl[i - 60] > 0:
                acc[bars[i][0]] += cl[i] / cl[i - 60] - 1.0
                cnt[bars[i][0]] += 1
    return {d: acc[d] / cnt[d] for d in acc if cnt[d]}


def build_presim(resolved, breadth, pos, lvls, idx_of, mean60, variant):
    out = []
    for t, bars, ei in resolved:
        setup = str(t.get("setup") or "")
        if setup not in SLATE:                       # prod: shorts dead, BREAKOUT not emitted
            continue
        regime = str(t.get("entry_regime") or "")
        if not swing_setup_allowed_in_regime(setup, regime):   # PROD regime gate
            continue
        sld = float(t.get("sl_dist") or 0.0)
        ao = t["as_of"][:10]
        if sld <= 0 or ao not in idx_of:
            continue
        if setup in BREADTH_SETUPS and breadth.get(ao, 0.0) < 60.0:
            continue
        if setup in RS_SETUPS and variant != "D":   # D: no RS filter (control)
            if variant == "B":   # backtest-identical: entry-day close vs compounded index
                sret = (bars[ei][4] / bars[ei - 60][4] - 1.0) if ei >= 60 and bars[ei - 60][4] > 0 else 0.0
                mp = pos.get(ao)
                mret = (lvls[mp] / lvls[mp - 60] - 1.0) if (mp is not None and mp >= 60) else 0.0
            elif variant == "C":  # timing-isolated: as-of-day close vs compounded index
                j = ei - 1
                sret = (bars[j][4] / bars[j - 60][4] - 1.0) if j >= 60 and bars[j - 60][4] > 0 else 0.0
                mp = pos.get(ao)
                mret = (lvls[mp] / lvls[mp - 60] - 1.0) if (mp is not None and mp >= 60) else 0.0
            else:                # A, prod-faithful: as-of-day close vs arithmetic universe mean
                j = ei - 1       # last bar at/before as_of (build-time view)
                sret = (bars[j][4] / bars[j - 60][4] - 1.0) if j >= 60 and bars[j - 60][4] > 0 else 0.0
                mret = mean60.get(ao, 0.0)
            if sret - mret <= 0:
                continue
        is_buy = t.get("direction", "BUY") == "BUY"
        off, px, reason = simulate_exit(bars, ei, is_buy, sld, 20)   # PROD exit
        legs = [(1.0, px, off, reason)]
        out.append({"legs": legs, "entry": float(bars[ei][1]), "is_buy": is_buy,
                    "ao": ao, "wl": float(t.get("wl_score") or 50.0), "holding": max(off, 1),
                    "sld": sld, "cell": f"{setup}×{regime}",
                    "grp": swing_setup_group(setup)})                # PROD grouping
    return out


def walk(presim, cal, idx_of, cap):
    risk, capn = 1500.0 * cap / 1e5, 0.20 * cap
    lh, ph = -0.03 * cap, 0.06 * cap
    n_cal = len(cal)
    grp_cap = {"RANGE": SWING_RANGE_GROUP_CAP, "TREND": 5}           # PROD reserve-2-trend
    by_day = collections.defaultdict(list)
    for s in presim:
        r = book_cap(s["entry"], s["is_buy"], s["sld"], s["legs"], risk, capn)
        if r is None:
            continue
        qty, g, net, gR, reason = r
        by_day[s["ao"]].append({**s, "g": g, "net": net,
                                "exit_idx": min(idx_of[s["ao"]] + s["holding"], n_cal - 1)})
    open_pos = []
    yr = collections.defaultdict(lambda: [0, 0.0, 0.0])
    cellnet = collections.defaultdict(lambda: [0, 0.0])
    for i, day in enumerate(cal):
        dr = 0.0
        still = []
        for p in open_pos:
            if p["exit_idx"] <= i:
                a = yr[p["ao"][:4]]; a[0] += 1; a[1] += p["g"]; a[2] += p["net"]; dr += p["net"]
                c = cellnet[p["cell"]]; c[0] += 1; c[1] += p["net"]
            else:
                still.append(p)
        open_pos = still
        gc = collections.Counter(p["grp"] for p in open_pos)
        for sig in sorted(by_day.get(day, []), key=lambda z: -z["wl"]):  # wl_score rank
            if dr <= lh or dr >= ph or len(open_pos) >= 5:
                continue
            if gc[sig["grp"]] >= grp_cap.get(sig["grp"], 5):
                continue
            open_pos.append(sig); gc[sig["grp"]] += 1
    for p in open_pos:
        a = yr[p["ao"][:4]]; a[0] += 1; a[1] += p["g"]; a[2] += p["net"]
        c = cellnet[p["cell"]]; c[0] += 1; c[1] += p["net"]
    return yr, cellnet


def report(name, res):
    print(f"\n--- {name} ---")
    for label, idx in (("GROSS ₹", 1), ("NET ₹", 2)):
        print(f"  {label:10}" + "".join(f"{c:>12}" for c in CAPL))
        for y in YEARS:
            print(f"  {y:10}" + "".join(f"{res[cap][0].get(y, [0, 0, 0])[idx]:>12,.0f}" for cap in CAPS))
        tot = [sum(res[cap][0].get(y, [0, 0, 0])[idx] for y in YEARS) for cap in CAPS]
        print(f"  {'ALL':10}" + "".join(f"{t:>12,.0f}" for t in tot))
        if idx == 2:
            print(f"  {'%/yr':10}" + "".join(f"{100*tot[i]/CAPS[i]/4.4:>11.1f}%" for i in range(len(CAPS))))
    print("  per-cell NET (₹1L):")
    for cell, c in sorted(res[100000][1].items(), key=lambda kv: -kv[1][1]):
        print(f"    {cell:28} n={c[0]:>4}  NET ₹{c[1]:>10,.0f}")
    return sum(res[100000][0].get(y, [0, 0, 0])[2] for y in YEARS)


def main():
    breadth, pos, lvls = build_market()
    mean60 = build_arith_mean_ret60()
    resolved = load_resolved(POOL)
    cal, idx_of = load_calendar()

    print("=" * 80)
    print("PR2 FIDELITY REPLAY — production predicates over the cached pool")
    print("=" * 80)

    pre_b = build_presim(resolved, breadth, pos, lvls, idx_of, mean60, "B")
    res_b = {cap: walk(pre_b, cal, idx_of, cap) for cap in CAPS}
    net_b = report("VARIANT B — backtest-identical RS (must equal final_config)", res_b)

    pre_c = build_presim(resolved, breadth, pos, lvls, idx_of, mean60, "C")
    res_c = {cap: walk(pre_c, cal, idx_of, cap) for cap in CAPS}
    net_c = report("VARIANT C — as-of timing, compounded index (isolates the look-ahead)", res_c)

    pre_a = build_presim(resolved, breadth, pos, lvls, idx_of, mean60, "A")
    res_a = {cap: walk(pre_a, cal, idx_of, cap) for cap in CAPS}
    net_a = report("VARIANT A — prod-faithful RS (arithmetic mean, as-of timing)", res_a)

    pre_d = build_presim(resolved, breadth, pos, lvls, idx_of, mean60, "D")
    res_d = {cap: walk(pre_d, cal, idx_of, cap) for cap in CAPS}
    net_d = report("VARIANT D — control: NO RS filter (is RS still worth it honestly?)", res_d)

    print("\n" + "=" * 80)
    diff_b = net_b - EXPECT
    ok = abs(diff_b) < 1.0
    print(f"GATE  variant B ₹1L NET = ₹{net_b:,.0f}  vs final_config ₹{EXPECT:,.0f}  "
          f"(diff ₹{diff_b:+,.0f})  ->  {'PASS — prod predicates ≡ validated config' if ok else 'FAIL — predicate divergence, DO NOT TRUST THE EDGE'}")
    print(f"INFO  variant C ₹1L NET = ₹{net_c:,.0f}  (look-ahead removal alone: ₹{net_c - net_b:+,.0f})")
    print(f"INFO  variant A ₹1L NET = ₹{net_a:,.0f}  (market-leg arith-vs-compounded on top: ₹{net_a - net_c:+,.0f})")
    print(f"INFO  variant D ₹1L NET = ₹{net_d:,.0f}  (honest RS filter value = A−D: ₹{net_a - net_d:+,.0f})")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
