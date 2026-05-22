"""Phase B — Multi-window stability test.

Runs phase_a_comprehensive on 4 windows:
  1mo:  2026-04-22 → 2026-05-21
  3mo:  2026-02-22 → 2026-05-21
  6mo:  2025-11-22 → 2026-05-21
  1yr:  2025-05-22 → 2026-05-21

For each window: per-strategy edge (WR + NetP&L + AvgR). Verify edge consistency.

Notes:
- 1mo + 3mo: ~75% real brain snapshots (most days have snaps)
- 6mo + 1yr: heuristic brain for older dates (NIFTY-derived regime)

Output: ~/.autotrader_backtest_cache/phase_b_summary.json
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from datetime import datetime

CACHE_DIR = Path.home() / ".autotrader_backtest_cache"


def run_window(start: str, end: str, label: str, target: int = 300) -> dict:
    """Run phase_a_comprehensive on the given window and parse results."""
    print(f"\n{'='*80}")
    print(f"Window [{label}]: {start} → {end}")
    print(f"{'='*80}")

    trades_path = CACHE_DIR / f"phase_a_trades_{start}_{end}.json"
    log_path = CACHE_DIR / f"phase_a_log_{start}_{end}.txt"

    repo_root = Path(__file__).resolve().parents[3]
    cmd = [
        sys.executable, "-m", "autotrader.backtest_v2.phase_a_comprehensive",
        start, end, str(target),
    ]
    env = {"PYTHONPATH": str(repo_root / "src")}
    import os
    env = {**os.environ, **env}

    print(f"  Running: {' '.join(cmd[-3:])}")
    result = subprocess.run(cmd, cwd=str(repo_root), env=env, capture_output=True, text=True, timeout=1200)
    log_path.write_text(result.stdout + "\n\n--- STDERR ---\n" + result.stderr)

    if result.returncode != 0:
        print(f"  ❌ Failed: rc={result.returncode}")
        print(result.stderr[:500])
        return {"label": label, "start": start, "end": end, "error": result.stderr[:500]}

    # Parse trades JSON
    if not trades_path.exists():
        return {"label": label, "start": start, "end": end, "error": "no trades file"}

    trades = json.loads(trades_path.read_text())
    n = len(trades)
    if n == 0:
        return {"label": label, "start": start, "end": end, "n": 0}

    # Per-setup summary
    from collections import defaultdict
    per_setup = defaultdict(list)
    for t in trades:
        per_setup[t["setup"]].append(t)

    setups = {}
    for setup, ts in per_setup.items():
        wins = sum(1 for t in ts if t["net_pnl"] > 0)
        net = sum(t["net_pnl"] for t in ts)
        avg_r = sum(t["r_realized"] for t in ts) / len(ts)
        avg_mfe = sum(t["r_mfe"] for t in ts) / len(ts)
        setups[setup] = {
            "n": len(ts),
            "wins": wins,
            "wr_pct": round(wins / len(ts) * 100, 1),
            "net_pnl": round(net, 2),
            "avg_pnl": round(net / len(ts), 2),
            "avg_r": round(avg_r, 3),
            "avg_mfe": round(avg_mfe, 3),
        }

    total_wins = sum(1 for t in trades if t["net_pnl"] > 0)
    total_net = sum(t["net_pnl"] for t in trades)
    return {
        "label": label,
        "start": start,
        "end": end,
        "n": n,
        "wins": total_wins,
        "wr_pct": round(total_wins / n * 100, 1),
        "net_pnl": round(total_net, 2),
        "avg_pnl": round(total_net / n, 2),
        "setups": setups,
    }


def main() -> int:
    windows = [
        ("2026-04-22", "2026-05-21", "1mo"),
        ("2026-02-22", "2026-05-21", "3mo"),
        ("2025-11-22", "2026-05-21", "6mo"),
        ("2025-05-22", "2026-05-21", "1yr"),
    ]
    results = []
    for start, end, label in windows:
        r = run_window(start, end, label)
        results.append(r)

    # Save summary
    summary_path = CACHE_DIR / "phase_b_summary.json"
    summary_path.write_text(json.dumps(results, indent=2))
    print(f"\nSummary saved to {summary_path}")

    # Print cross-window comparison
    print("\n" + "=" * 80)
    print("PHASE B — Strategy edge consistency across windows")
    print("=" * 80)
    setups_seen = set()
    for r in results:
        if "setups" in r:
            setups_seen.update(r["setups"].keys())

    for setup in sorted(setups_seen):
        print(f"\n{setup}")
        print(f"  {'window':10s} {'N':>5s} {'WR%':>6s} {'NetP&L':>12s} {'AvgPnL':>9s} {'AvgR':>7s} {'AvgMFE':>7s}")
        for r in results:
            if "setups" not in r:
                continue
            s = r["setups"].get(setup)
            if not s:
                continue
            print(f"  {r['label']:10s} {s['n']:>5d} {s['wr_pct']:>5.1f}% {s['net_pnl']:>+12.2f} {s['avg_pnl']:>+9.2f} {s['avg_r']:>+7.3f} {s['avg_mfe']:>+7.3f}")

    print(f"\n{'window':10s} {'N':>5s} {'WR%':>6s} {'NetP&L':>12s} {'AvgPnL':>9s}")
    for r in results:
        if "n" not in r or r.get("n", 0) == 0:
            continue
        print(f"  {r['label']:10s} {r['n']:>5d} {r['wr_pct']:>5.1f}% {r['net_pnl']:>+12.2f} {r['avg_pnl']:>+9.2f}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
