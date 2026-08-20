"""P0 — data integrity for the swing god-mode grind.

Answers two questions that contaminate every windowed conclusion until settled:

  1. 2019 produced ZERO trades in the 11-yr run. Data hole, or correct behaviour?
     A year of entirely non-tradeable regimes (PANIC / TREND_DOWN / CHOP) would
     legitimately emit nothing, because swing_setup_allowed_in_regime() blocks
     MOMENTUM/PULLBACK outside TREND_UP/RANGE.
  2. 2026 also produced zero trades — expected (live channel gated ~11 months),
     but confirm it is the regime series and not missing bars.

Read-only. No engine run: regime JSON + bar-date coverage only.
"""
from __future__ import annotations

import collections
import json
import os
import pickle
import sys

CACHE = os.path.expanduser("~/.autotrader_backtest_cache")
REGIME = os.path.join(CACHE, "regime_faithful_2015.json")
BARS = os.path.join(CACHE, "swing_adj_bars_2015.pkl")

# setups the shipped config runs; both are blocked outside these regimes
TRADEABLE = {"TREND_UP", "RANGE"}


def main() -> None:
    reg = json.load(open(REGIME))
    print(f"regime days: {len(reg)}  ({min(reg)} -> {max(reg)})\n")

    # regime_faithful_2015.json maps date -> FULL SNAPSHOT DICT, not a string. Pull the
    # 'regime' field and fold it through the SAME core-4 fold the harness applies, so the
    # tradeable test matches swing_setup_allowed_in_regime() exactly.
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
    from autotrader.domain.regime_affinity import core4_regime

    def _raw(r):
        return str(r.get("regime", "") if isinstance(r, dict) else r)

    by_year: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    for d, r in reg.items():
        by_year[d[:4]][core4_regime(_raw(r))] += 1

    print("=== regime days per year (TRADEABLE = TREND_UP + RANGE) ===")
    print(f"  {'yr':<6} {'days':>5} {'tradeable':>10} {'%':>6}   breakdown")
    for y in sorted(by_year):
        c = by_year[y]
        tot = sum(c.values())
        tr = sum(v for k, v in c.items() if k in TRADEABLE)
        top = ", ".join(f"{k}:{v}" for k, v in c.most_common(5))
        flag = "  <-- ZERO TRADEABLE" if tr == 0 else ""
        print(f"  {y:<6} {tot:>5} {tr:>10} {100*tr/max(tot,1):>5.1f}%   {top}{flag}")

    print("\n=== bar-date coverage per year (does the pickle even have these years?) ===")
    raw = pickle.load(open(BARS, "rb"))
    yrs: collections.Counter = collections.Counter()
    syms_with_year: dict[str, set] = collections.defaultdict(set)
    for sym, bars in raw.items():
        for b in bars:
            y = str(b[0])[:4]
            yrs[y] += 1
            syms_with_year[y].add(sym)
    print(f"  {'yr':<6} {'bars':>10} {'symbols':>8}")
    for y in sorted(yrs):
        print(f"  {y:<6} {yrs[y]:>10,} {len(syms_with_year[y]):>8}")

    print("\n=== VERDICT ===")
    for y in ("2019", "2026"):
        c = by_year.get(y, collections.Counter())
        tr = sum(v for k, v in c.items() if k in TRADEABLE)
        bars_ok = yrs.get(y, 0) > 0
        if not bars_ok:
            print(f"  {y}: NO BARS -> genuine data hole; exclude {y} from all windows")
        elif tr == 0:
            print(f"  {y}: bars present ({yrs[y]:,}) but ZERO tradeable regime days -> "
                  f"zero trades is CORRECT, not a hole")
        else:
            print(f"  {y}: bars present ({yrs[y]:,}) AND {tr} tradeable days -> "
                  f"zero trades needs another explanation (filters/floor)")


if __name__ == "__main__":
    main()
