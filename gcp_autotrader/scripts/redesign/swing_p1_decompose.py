"""P1 — decompose the shipped swing config's trades to locate the 2024 collapse.

Input: the --by-year --trades-out CSV (272 isolated-cohort trades, 2015-2026).
Pure analysis of an existing CSV: no engine run, no BQ, no prod import.

Questions, in order of decision value:
  A. per-year x regime: n / WR / net / avg-R  -> where is the damage
  B. 2024 RANGE deep dive (23 trades, -Rs82,902) -> one bad cell, or 23 bad trades
  C. exit-reason mix per year -> did the EXIT stack fail, or the ENTRY selection
  D. b200 buckets vs outcome -> does the live >=70 gate actually separate winners?
     (this is the gate that has kept swing dark ~11 months; the CSV records b200
      at signal time, so it is directly testable rather than assumed)
  E. 2024 RANGE losers vs historical RANGE winners, feature by feature

Nothing here changes prod. Findings are hypotheses for a separately-validated sweep.
"""
from __future__ import annotations

import collections
import csv
import os
import statistics
import sys

CSV = ("/private/tmp/claude-501/-Users-apple-Projects-Migrated-Auto-Trading-Python-GCP/"
       "439e48e8-a413-4a1d-9d0a-530e53a5e277/scratchpad/swing_trades_shipped.csv")


def f(row, k, d=0.0):
    try:
        return float(row[k])
    except (TypeError, ValueError, KeyError):
        return d


def money(x):
    return f"{x:>10,.0f}"


def main() -> None:
    path = sys.argv[1] if len(sys.argv) > 1 else CSV
    rows = list(csv.DictReader(open(path)))
    print(f"trades: {len(rows)}  ({rows[0]['year']} -> {rows[-1]['year']})\n")

    # ---- A. per-year x regime ------------------------------------------------
    print("=== A. per-year x regime ===")
    print(f"  {'yr':<6} {'regime':<9} {'n':>4} {'WR%':>6} {'net':>11} {'avgR':>7} {'avg_net':>9}")
    agg = collections.defaultdict(list)
    for r in rows:
        agg[(r["year"], r["regime"])].append(r)
    for key in sorted(agg):
        g = agg[key]
        net = sum(f(x, "net") for x in g)
        wins = sum(1 for x in g if f(x, "net") > 0)
        rs = [f(x, "R") for x in g]
        print(f"  {key[0]:<6} {key[1]:<9} {len(g):>4} {100*wins/len(g):>6.1f} {money(net)} "
              f"{statistics.mean(rs):>7.2f} {net/len(g):>9,.0f}")

    # ---- C. exit-reason mix per year ----------------------------------------
    print("\n=== C. exit reason x year (n | net) ===")
    reasons = sorted({r["reason"] for r in rows})
    years = sorted({r["year"] for r in rows})
    print(f"  {'yr':<6} " + " ".join(f"{x[:9]:>18}" for x in reasons))
    for y in years:
        cells = []
        for rs_ in reasons:
            g = [r for r in rows if r["year"] == y and r["reason"] == rs_]
            cells.append(f"{len(g):>3} |{sum(f(x,'net') for x in g):>13,.0f}" if g else f"{'-':>18}")
        print(f"  {y:<6} " + " ".join(cells))

    print("\n=== C2. exit reason overall ===")
    for rs_ in reasons:
        g = [r for r in rows if r["reason"] == rs_]
        net = sum(f(x, "net") for x in g)
        wins = sum(1 for x in g if f(x, "net") > 0)
        print(f"  {rs_:<12} n={len(g):>4}  WR={100*wins/len(g):>5.1f}%  net={money(net)}  "
              f"avg={net/len(g):>9,.0f}  avgR={statistics.mean([f(x,'R') for x in g]):>6.2f}")

    # ---- D. b200 buckets ----------------------------------------------------
    print("\n=== D. b200 at signal vs outcome (0.0 = not populated in early yrs) ===")
    buckets = [(-1, 0.001, "b200=0 (n/a)"), (0.001, 50, "0-50"), (50, 60, "50-60"),
               (60, 70, "60-70"), (70, 80, "70-80"), (80, 101, "80+")]
    for lo, hi, lab in buckets:
        g = [r for r in rows if lo < f(r, "b200") <= hi or (lo == -1 and f(r, "b200") == 0.0)]
        if not g:
            continue
        net = sum(f(x, "net") for x in g)
        wins = sum(1 for x in g if f(x, "net") > 0)
        print(f"  {lab:<14} n={len(g):>4}  WR={100*wins/len(g):>5.1f}%  net={money(net)}  "
              f"avg={net/len(g):>9,.0f}")
    pop = [r for r in rows if f(r, "b200") > 0.0]
    if pop:
        ge70 = [r for r in pop if f(r, "b200") >= 70]
        lt70 = [r for r in pop if f(r, "b200") < 70]
        print(f"\n  --- the LIVE gate, on populated rows only (n={len(pop)}) ---")
        for lab, g in (("b200 >= 70 (gate OPEN)", ge70), ("b200 <  70 (gate SHUT)", lt70)):
            if not g:
                print(f"  {lab:<24} n=0")
                continue
            net = sum(f(x, "net") for x in g)
            wins = sum(1 for x in g if f(x, "net") > 0)
            print(f"  {lab:<24} n={len(g):>4}  WR={100*wins/len(g):>5.1f}%  net={money(net)}  "
                  f"avg={net/len(g):>9,.0f}")

    # ---- B + E. 2024 RANGE deep dive ---------------------------------------
    r24 = [r for r in rows if r["year"] == "2024" and r["regime"] == "RANGE"]
    print(f"\n=== B. 2024 RANGE deep dive (n={len(r24)}) ===")
    if r24:
        r24s = sorted(r24, key=lambda x: f(x, "net"))
        print(f"  {'sym':<13} {'entry_d':<11} {'mo':>3} {'hold':>4} {'net':>10} {'R':>6} "
              f"{'reason':<10} {'b200':>6} {'rsi':>5} {'adx':>5} {'tov_cr':>7}")
        for r in r24s:
            print(f"  {r['sym']:<13} {r['entry_d']:<11} {r['month']:>3} {r['hold']:>4} "
                  f"{f(r,'net'):>10,.0f} {f(r,'R'):>6.2f} {r['reason']:<10} "
                  f"{f(r,'b200'):>6.1f} {f(r,'rsi'):>5.1f} {f(r,'adx_daily'):>5.1f} "
                  f"{f(r,'turnover_cr'):>7.1f}")
        worst = r24s[:5]
        print(f"\n  worst 5 = Rs{sum(f(x,'net') for x in worst):,.0f} of "
              f"Rs{sum(f(x,'net') for x in r24):,.0f} "
              f"({100*sum(f(x,'net') for x in worst)/sum(f(x,'net') for x in r24):.0f}%)")

        hist = [r for r in rows if r["regime"] == "RANGE" and r["year"] not in ("2024",)]
        print("\n=== E. 2024 RANGE vs all other RANGE years (medians) ===")
        print(f"  {'feature':<14} {'2024':>10} {'other yrs':>10}")
        for k in ("b200", "rsi", "adx_daily", "turnover_cr", "atr_pct", "sl_pct",
                  "volume_ratio", "hold", "wl_score"):
            a = statistics.median([f(x, k) for x in r24])
            b = statistics.median([f(x, k) for x in hist])
            print(f"  {k:<14} {a:>10.2f} {b:>10.2f}")


if __name__ == "__main__":
    main()
