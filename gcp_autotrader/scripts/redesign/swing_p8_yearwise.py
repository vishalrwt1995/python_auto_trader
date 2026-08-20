"""P8 — FRESH year-wise P&L for the shipped swing config, computed from trade level.

Runs the shipped config ONCE, continuous 2015-2026 (the authoritative walk: positions span
year boundaries, compounding accumulates), captures every executed trade via run()'s
trades_out collector, and derives the year table independently of the harness's own by_year
summary. The independent total is then checked against the known +Rs530,568 so the table is
verifiable rather than asserted.

Reports BOTH attributions, because they answer different questions and differ materially:
  * by EXIT year  -- when the P&L was realised (what the harness's by_year uses)
  * by ENTRY year -- which signal cohort earned it (better for judging a year's selection)

Read-only; scripts/redesign only; no prod module, state, env or deploy touched.
"""
from __future__ import annotations

import collections
import json
import os
import pickle
import statistics
import sys

sys.path.insert(0, os.path.dirname(__file__))
import swing_final as H  # noqa: E402

BARS = os.path.expanduser("~/.autotrader_backtest_cache/swing_adj_bars_2015.pkl")

SHIPPED = dict(
    setups=("MOMENTUM", "PULLBACK"), emit_floor=45, compound_pct=2.0, liq_cap_pct=1.0,
    mom_month_block={1}, mom_turnover_exclude=(5.0, 40.0), setup_daily_cap={"MOMENTUM": 2},
    pb_month_block={1, 4, 7}, range_bucket_by_regime=True, range_group_cap=2,
    total_slots=7, tu_slot_cap=5,
)


def table(trades, key, title):
    by = collections.defaultdict(list)
    for t in trades:
        by[str(t[key])[:4]].append(t)
    print(f"\n=== {title} ===")
    print(f"  {'year':<6} {'n':>4} {'WR%':>6} {'gross':>12} {'net':>12} {'net/trade':>10} "
          f"{'avgR':>6} {'cum net':>13}")
    cum = 0.0
    tot_n = 0
    for y in sorted(by):
        g = by[y]
        net = sum(x["net"] for x in g)
        gross = sum(x["gross"] for x in g)
        wins = sum(1 for x in g if x["net"] > 0)
        cum += net
        tot_n += len(g)
        print(f"  {y:<6} {len(g):>4} {100*wins/len(g):>6.1f} {gross:>12,.0f} {net:>12,.0f} "
              f"{net/len(g):>10,.0f} {statistics.mean([x['R'] for x in g]):>6.2f} {cum:>13,.0f}")
    print(f"  {'TOTAL':<6} {tot_n:>4} {'':>6} {sum(x['gross'] for x in trades):>12,.0f} "
          f"{sum(x['net'] for x in trades):>12,.0f}")
    return cum


def main() -> None:
    print("loading bars + regime + market_inputs ...")
    raw = pickle.load(open(BARS, "rb"))
    regime = json.load(open(H.REGIME_JSON))
    mi = json.load(open(H.MARKET_INPUTS_JSON))
    print("building indicator series ...")
    symdata = {s: H.Sym(b) for s, b in raw.items() if len(b) >= H.MIN_BARS_SWING}
    print(f"  {len(symdata)} symbols\n")

    trades: list = []
    r = H.run(symdata, regime, mi, d0="2015-01-01", d1="2026-12-31",
              verbose=True, trades_out=trades, **SHIPPED)

    print(f"\ncaptured {len(trades)} trades from the continuous walk")
    net_sum = sum(t["net"] for t in trades)
    print(f"independent net total: Rs{net_sum:,.0f}   harness-reported: Rs{r.get('net',0):,.0f}")
    ok = abs(net_sum - r.get("net", 0)) < 1.0 and abs(net_sum - 530568) < 1500
    print(f"CHECK: {'PASS — table is verifiable' if ok else '*** MISMATCH — do not trust the table ***'}")

    table(trades, "exit_d", "BY EXIT YEAR (when P&L was realised)")
    table(trades, "entry_d", "BY ENTRY YEAR (which signal cohort earned it)")

    # era split on the same trade list
    print("\n=== ERA SPLIT (by exit year) ===")
    for lab, lo, hi in (("2015-2021", "2015", "2021"), ("2022-2026", "2022", "2026")):
        g = [t for t in trades if lo <= str(t["exit_d"])[:4] <= hi]
        if not g:
            continue
        net = sum(x["net"] for x in g)
        wins = sum(1 for x in g if x["net"] > 0)
        print(f"  {lab}: n={len(g):>3}  WR={100*wins/len(g):>5.1f}%  net=Rs{net:>10,.0f}  "
              f"avg=Rs{net/len(g):>8,.0f}  avgR={statistics.mean([x['R'] for x in g]):+.2f}")


if __name__ == "__main__":
    main()
