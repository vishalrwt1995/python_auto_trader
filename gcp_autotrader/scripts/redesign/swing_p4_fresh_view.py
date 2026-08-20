"""P4 — FRESH VIEW on swing: structural questions never asked, all free from the P1 CSV.

Every prior grind swept THRESHOLDS INSIDE the existing structure (gates, month blocks,
turnover bands, slots, exits-as-configured). These four questions are different in kind —
they ask whether the STRUCTURE is right, not whether a number is:

  A. SCORE -> OUTCOME.  Does adj_score / raw_score / wl_score predict net or R at all?
     Prod sizes every trade the same (2% of equity). If score predicts, flat sizing is
     leaving money on the table and CONVICTION-WEIGHTED sizing is a structural upgrade.
     If score predicts nothing, that is equally valuable: it means the scorer ranks but
     does not forecast, and no selection tightening based on it can work (which would
     retroactively explain why every threshold sweep failed).

  B. THE MAX_HOLD DRAG.  127 of 272 trades exit at MAX_HOLD for avgR +0.31 — 47% of all
     trades tie up a slot for ~20 days to earn ~nothing. Largest untapped pool in the book.
     Are they identifiable AT ENTRY (feature separation vs TRAIL winners)? If yes, skipping
     or short-holding them frees slots for real candidates. If they look identical at entry,
     the drag is irreducible and hold-period tuning is pointless.

  C. 2025 (-Rs87,481 in the continuous run) — never decomposed. 2024 was RANGE; 2025 may
     be something else entirely, and it is the most recent complete year.

  D. SECTOR — the CSV carries `sector` and nothing has ever grouped by it.

Pure CSV analysis. No engine run, no BQ, no prod import, nothing mutated.
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


def corr(xs, ys):
    """Pearson r; 0.0 when undefined."""
    n = len(xs)
    if n < 3:
        return 0.0
    mx, my = statistics.mean(xs), statistics.mean(ys)
    num = sum((a - mx) * (b - my) for a, b in zip(xs, ys))
    dx = sum((a - mx) ** 2 for a in xs) ** 0.5
    dy = sum((b - my) ** 2 for b in ys) ** 0.5
    return num / (dx * dy) if dx and dy else 0.0


def buckets(rows, key, edges):
    out = []
    for lo, hi in zip(edges, edges[1:]):
        g = [r for r in rows if lo <= f(r, key) < hi]
        if len(g) < 5:
            continue
        net = sum(f(x, "net") for x in g)
        out.append((f"{lo:g}-{hi:g}", len(g), 100 * sum(1 for x in g if f(x, "net") > 0) / len(g),
                    net, net / len(g), statistics.mean([f(x, "R") for x in g])))
    return out


def main() -> None:
    rows = list(csv.DictReader(open(sys.argv[1] if len(sys.argv) > 1 else CSV)))
    print(f"trades: {len(rows)}\n")

    # ---- A. does score predict? ----
    print("=== A. SCORE -> OUTCOME (does the scorer FORECAST, or only RANK?) ===")
    for k in ("adj_score", "raw_score", "wl_score"):
        xs = [f(r, k) for r in rows]
        print(f"\n  -- {k}  (r vs net = {corr(xs, [f(r,'net') for r in rows]):+.3f}, "
              f"r vs R = {corr(xs, [f(r,'R') for r in rows]):+.3f}) --")
        lo, hi = min(xs), max(xs)
        edges = [lo + (hi - lo) * i / 4 for i in range(5)]
        print(f"     {'bucket':<14} {'n':>4} {'WR%':>6} {'net':>11} {'avg':>9} {'avgR':>7}")
        for lab, n, wr, net, avg, ar in buckets(rows, k, edges):
            print(f"     {lab:<14} {n:>4} {wr:>6.1f} {net:>11,.0f} {avg:>9,.0f} {ar:>7.2f}")

    # ---- B. MAX_HOLD drag: identifiable at entry? ----
    print("\n\n=== B. MAX_HOLD DRAG — are they separable from TRAIL winners AT ENTRY? ===")
    mh = [r for r in rows if r["reason"] == "MAX_HOLD"]
    tr = [r for r in rows if r["reason"] == "TRAIL"]
    sl = [r for r in rows if r["reason"] == "SL"]
    print(f"  MAX_HOLD n={len(mh)}  net={sum(f(x,'net') for x in mh):>10,.0f}  "
          f"avgR={statistics.mean([f(x,'R') for x in mh]):+.2f}")
    print(f"  TRAIL    n={len(tr)}  net={sum(f(x,'net') for x in tr):>10,.0f}  "
          f"avgR={statistics.mean([f(x,'R') for x in tr]):+.2f}")
    print(f"  SL       n={len(sl)}  net={sum(f(x,'net') for x in sl):>10,.0f}  "
          f"avgR={statistics.mean([f(x,'R') for x in sl]):+.2f}")
    print(f"\n  ENTRY-TIME features (median) — if these are equal, the drag is IRREDUCIBLE:")
    print(f"    {'feature':<14} {'MAX_HOLD':>10} {'TRAIL':>10} {'SL':>10}")
    for k in ("adj_score", "wl_score", "rsi", "adx_daily", "atr_pct", "sl_pct",
              "volume_ratio", "strength", "turnover_cr", "b200", "breadth"):
        a = statistics.median([f(x, k) for x in mh]) if mh else 0
        b = statistics.median([f(x, k) for x in tr]) if tr else 0
        c = statistics.median([f(x, k) for x in sl]) if sl else 0
        print(f"    {k:<14} {a:>10.2f} {b:>10.2f} {c:>10.2f}")

    # ---- C. 2025 ----
    print("\n\n=== C. 2025 decomposition (never done) ===")
    y25 = [r for r in rows if r["year"] == "2025"]
    if not y25:
        print("  no 2025 rows in the isolated-cohort CSV")
    else:
        print(f"  {'sym':<13} {'entry_d':<11} {'reg':<9} {'net':>10} {'R':>6} {'reason':<9} "
              f"{'b200':>6} {'tov_cr':>8}")
        for r in sorted(y25, key=lambda x: f(x, "net")):
            print(f"  {r['sym']:<13} {r['entry_d']:<11} {r['regime']:<9} {f(r,'net'):>10,.0f} "
                  f"{f(r,'R'):>6.2f} {r['reason']:<9} {f(r,'b200'):>6.1f} {f(r,'turnover_cr'):>8.1f}")

    # ---- D. sector ----
    print("\n\n=== D. SECTOR (never grouped before) ===")
    bysec = collections.defaultdict(list)
    for r in rows:
        bysec[r.get("sector") or "UNKNOWN"].append(r)
    out = []
    for sec, g in bysec.items():
        if len(g) < 6:
            continue
        net = sum(f(x, "net") for x in g)
        out.append((net, sec, len(g), 100 * sum(1 for x in g if f(x, "net") > 0) / len(g), net / len(g)))
    print(f"  {'sector':<34} {'n':>4} {'WR%':>6} {'net':>11} {'avg':>9}   (n>=6 only)")
    for net, sec, n, wr, avg in sorted(out, reverse=True):
        print(f"  {sec[:33]:<34} {n:>4} {wr:>6.1f} {net:>11,.0f} {avg:>9,.0f}")
    small = sum(1 for g in bysec.values() if len(g) < 6)
    print(f"  ({small} sectors with n<6 omitted — too thin to read)")


if __name__ == "__main__":
    main()
