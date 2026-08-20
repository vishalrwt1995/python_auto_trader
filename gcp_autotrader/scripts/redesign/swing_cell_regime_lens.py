"""FRESH LENS on the ALREADY-SHIPPED swing config — setup x regime decomposition.

This is DIAGNOSTIC, not exploratory. It runs the live config exactly as shipped
(PR #51, 2026-07-03: ~9.7% CAGR / Calmar 0.60 / -16% DD at Rs5L, arm 1.75) and asks a
single question the prior grind could not answer:

    which (setup x regime) CELLS carry the config, and is any cell a persistent loser?

Why this cannot overfit: no parameter is searched. One arm, the committed one. We are
describing a config already in production, not selecting one. Any cell that looks bad here
is a HYPOTHESIS for a later, separately-validated sweep -- never a change on its own.

The harness already reported by_cell (setup), by_reg (regime), by_dir and by_celldir.
by_cell_regime (setup x regime) is the cross-section it never had.

Small-n warning is printed for every cell: 2022-2026 sliced by setup x regime leaves few
trades per cell, and this repo has already been burned by thin-cell false positives
(pledge magnitude filters, pead "reaction>=25%" collapsing in the portfolio walk). n is
printed everywhere so nothing hides behind a percentage.

Read-only: no prod module is imported for mutation, no state/env is written.
Single-process (CLAUDE.md local-compute rule).
"""
from __future__ import annotations

import collections
import json
import os
import pickle
import sys

sys.path.insert(0, os.path.dirname(__file__))

from swing_prod_faithful import (  # noqa: E402
    BARS_PKL, REGIME_JSON, MIN_BARS_SWING, DEFAULT_ACTIVATE_R, Sym, run,
)

CAP = 500_000  # prod-live CAPITAL_SWING


def main() -> None:
    print("loading bars + regime ...")
    raw = pickle.load(open(BARS_PKL, "rb"))
    regime = json.load(open(REGIME_JSON))
    symdata = {s: Sym(b) for s, b in raw.items() if len(b) >= MIN_BARS_SWING}
    print(f"  {len(symdata)} symbols >= {MIN_BARS_SWING} bars; regime days={len(regime)}\n")

    print("=== SHIPPED CONFIG (Rs5L, arm 1.75) — reproduce, then decompose ===")
    res = run(symdata, regime, CAP, DEFAULT_ACTIVATE_R, verbose=True)

    print(f"\n  headline: trades={res['n']}  WR={res['wr']:.1f}%  net=Rs{res['net']:,.0f}  "
          f"CAGR={res['cagr']:.2f}%  maxDD={res['mdd']:.1f}%  Calmar={res['calmar']:.2f}")
    print("  (sanity: shipped backtest was ~9.7% CAGR / Calmar 0.60 / -16% DD)")

    cr = res.get("by_cell_regime") or {}
    if not cr:
        print("\n  !! by_cell_regime empty — harness instrumentation missing")
        return

    rows = []
    for key, v in cr.items():
        setup, reg = key if isinstance(key, tuple) else (str(key), "?")
        n, wins, net = v["n"], v["wins"], v["net"]
        rows.append((net, setup, reg, n, wins))
    rows.sort()

    print(f"\n=== setup x regime  ({len(rows)} cells, worst net first) ===")
    print(f"  {'setup':<14} {'regime':<12} {'n':>4} {'WR%':>6} {'net':>12} {'net/trade':>10}  flag")
    for net, setup, reg, n, wins in rows:
        wr = 100.0 * wins / n if n else 0.0
        flag = "THIN(n<10)" if n < 10 else ("NEGATIVE" if net < 0 else "")
        print(f"  {setup:<14} {reg:<12} {n:>4} {wr:>6.1f} {net:>12,.0f} {net/max(n,1):>10,.0f}  {flag}")

    neg = [r for r in rows if r[0] < 0]
    thin = [r for r in rows if r[3] < 10]
    print(f"\n  negative cells: {len(neg)}   thin cells (n<10): {len(thin)}")
    if neg:
        drag = sum(r[0] for r in neg)
        print(f"  combined drag from negative cells: Rs{drag:,.0f} "
              f"({100*abs(drag)/max(abs(res['net']),1):.1f}% of net)")
        print("  ^ HYPOTHESES ONLY — each needs its own IS/OOS + portfolio-walk validation")

    # per-year stability for the negative cells: a real loser loses in most years,
    # an artefact is one bad year. This is the cheapest overfit screen available.
    taken = res.get("taken") or []
    if neg and taken:
        by_cy = collections.defaultdict(lambda: [0, 0.0])
        for t in taken:
            by_cy[(t["setup"], t["regime"], t["exit_d"][:4])][0] += 1
            by_cy[(t["setup"], t["regime"], t["exit_d"][:4])][1] += t["net"]
        print("\n=== per-year detail for NEGATIVE cells (is it persistent or one bad year?) ===")
        for net, setup, reg, n, _w in neg:
            yrs = sorted(k[2] for k in by_cy if k[0] == setup and k[1] == reg)
            parts = []
            nloss = 0
            for y in yrs:
                cn, cnet = by_cy[(setup, reg, y)]
                parts.append(f"{y}:n={cn},Rs{cnet:,.0f}")
                if cnet < 0:
                    nloss += 1
            verdict = "PERSISTENT" if yrs and nloss >= max(2, len(yrs) - 1) else "one/two bad yrs"
            print(f"  {setup}/{reg} (net Rs{net:,.0f}) -> {verdict}")
            print(f"      {'  '.join(parts)}")


if __name__ == "__main__":
    main()
