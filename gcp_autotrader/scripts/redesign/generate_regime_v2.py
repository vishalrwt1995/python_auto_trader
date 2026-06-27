"""
Phase 3 — Regenerate regime_v2_2015.json with Phase 2 RECOVERY logic applied.

Algorithm (mirrors live market_brain_service.py Phase 2 logic):
  - When PANIC → non-PANIC transition occurs, relabel the post-PANIC days as
    RECOVERY for up to 4 calendar days after the exit date.
  - Exception: if a day falls back to PANIC or TREND_DOWN, stop relabeling
    (PANIC resets, TREND_DOWN is a separate breakdown).

Output: ~/.autotrader_backtest_cache/regime_v2_2015.json
"""

import json
import collections
from datetime import date, timedelta
from pathlib import Path

CACHE = Path.home() / ".autotrader_backtest_cache"
SRC = CACHE / "regime_2015.json"
DST = CACHE / "regime_v2_2015.json"

RECOVERY_WINDOW_DAYS = 4   # calendar days (matches live regime_age_seconds / 86400 < 4.0)
STOP_RELABELING = {"PANIC", "TREND_DOWN"}  # these override RECOVERY


def build_v2(src: dict[str, str]) -> dict[str, str]:
    dates = sorted(src.keys())
    result = dict(src)  # start as a copy

    in_panic = False
    panic_exit_date: date | None = None

    for d_str in dates:
        d = date.fromisoformat(d_str)
        regime = src[d_str]

        if regime == "PANIC":
            in_panic = True
            panic_exit_date = None  # still inside PANIC
            continue

        if in_panic and regime != "PANIC":
            # First day after PANIC exits
            in_panic = False
            panic_exit_date = d

        if panic_exit_date is not None:
            days_since_exit = (d - panic_exit_date).days
            if days_since_exit <= RECOVERY_WINDOW_DAYS:
                if regime in STOP_RELABELING:
                    # New PANIC or TREND_DOWN — clear recovery window, let it be
                    panic_exit_date = None
                    if regime == "PANIC":
                        in_panic = True
                else:
                    result[d_str] = "RECOVERY"
            else:
                panic_exit_date = None  # window expired

    return result


def distribution(data: dict[str, str]) -> dict[str, int]:
    c = collections.Counter(data.values())
    return dict(sorted(c.items(), key=lambda x: -x[1]))


def _print_comparison(src: dict, v2: dict, label: str, dst: Path) -> None:
    v1_dist = distribution(src)
    v2_dist = distribution(v2)
    changed = sum(1 for k in src if src[k] != v2[k])
    print(f"\n{label}: changed {changed} / {len(src)} days")
    all_regimes = sorted(set(v1_dist) | set(v2_dist))
    print(f"  {'Regime':<20} {'v1':>6} {'v2':>6} {'delta':>7}")
    print(f"  {'-'*20} {'-'*6} {'-'*6} {'-'*7}")
    for r in all_regimes:
        v1 = v1_dist.get(r, 0)
        v2_ = v2_dist.get(r, 0)
        print(f"  {r:<20} {v1:>6} {v2_:>6} {v2_-v1:>+7}")
    dst.write_text(json.dumps(v2, indent=2))
    print(f"  Saved: {dst}")


def main():
    # --- regime_v2_2015.json (2015+ full history) ---
    src = json.loads(SRC.read_text())
    print(f"Loaded {len(src)} entries from {SRC.name}")
    v2_full = build_v2(src)
    _print_comparison(src, v2_full, "regime_2015 → regime_v2_2015", DST)

    # --- regime_v2_core4.json (2022+ BQ-derived, core4 folded) ---
    src_c4 = json.loads((CACHE / "regime_core4.json").read_text())
    print(f"\nLoaded {len(src_c4)} entries from regime_core4.json")
    v2_c4 = build_v2(src_c4)
    _print_comparison(src_c4, v2_c4, "regime_core4 → regime_v2_core4", CACHE / "regime_v2_core4.json")

    # Sample RECOVERY windows from core4 transformation
    print("\nSample RECOVERY windows from core4 (first 5):")
    shown = 0
    prev = None
    for d_str in sorted(v2_c4.keys()):
        r_new = v2_c4[d_str]
        r_old = src_c4.get(d_str)
        if r_old != "RECOVERY" and r_new == "RECOVERY" and shown < 5:
            if prev and src_c4.get(prev) == "PANIC":
                print(f"  {prev} (PANIC exit) → {d_str}: {r_old} → RECOVERY")
                shown += 1
        prev = d_str


if __name__ == "__main__":
    main()
