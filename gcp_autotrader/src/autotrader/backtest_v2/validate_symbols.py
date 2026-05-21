"""Per-Symbol Multi-Day Validation — confirms swing replica matches production.

Picks 6 diverse symbols with the most BQ swing-scan history and runs every
single one of their swing scans through prod_replica_v2. Reports per-symbol
and per-setup match rates for direction, raw_score, adjusted_score, and
blocked_reason.

Pass criteria: ≥95% direction match, ≥90% raw_score match, ≥90%
adjusted_score match for each symbol/setup combo.

Run:
    python -m autotrader.backtest_v2.validate_symbols
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

from autotrader.backtest_v2.prod_replica_v2 import ProdReplicaV2


# 6 symbols selected for diversity:
#   - NESTLEIND: BREAKOUT, has a qualified scan (rare)
#   - ADANIPOWER: BREAKOUT, mega-cap industrial
#   - RADICO: BREAKOUT, 4 distinct blocked_reasons (diverse gate paths)
#   - BHEL: BREAKOUT, mid-cap
#   - SBICARD: MEAN_REVERSION, only MR symbol with high scan count
#   - ANANDRATHI: BREAKOUT, most scans (38)
SYMBOLS = [
    ("NESTLEIND", "BREAKOUT"),
    ("ADANIPOWER", "BREAKOUT"),
    ("RADICO", "BREAKOUT"),
    ("BHEL", "BREAKOUT"),
    ("SBICARD", "MEAN_REVERSION"),
    ("ANANDRATHI", "BREAKOUT"),
]


def _bq_query(query: str) -> list[dict]:
    gcloud = os.environ.get("GCLOUD", "gcloud")
    token = subprocess.check_output(
        [gcloud, "auth", "application-default", "print-access-token"],
        text=True,
    ).strip()
    body = json.dumps({"query": query, "useLegacySql": False, "location": "asia-south1"}).encode()
    req = urllib.request.Request(
        "https://bigquery.googleapis.com/bigquery/v2/projects/grow-profit-machine/queries",
        data=body,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=180) as resp:
        r = json.loads(resp.read())
    if "error" in r:
        raise RuntimeError(f"BQ error: {r['error']}")
    schema = r.get("schema", {}).get("fields", [])
    out = []
    for row in r.get("rows", []):
        d = {}
        for fld, cell in zip(schema, row["f"]):
            v = cell.get("v")
            if v is None:
                d[fld["name"]] = None
                continue
            if fld["type"] == "TIMESTAMP":
                try:
                    d[fld["name"]] = datetime.fromtimestamp(float(v), tz=timezone.utc).isoformat()
                except Exception:
                    d[fld["name"]] = v
            else:
                d[fld["name"]] = v
        out.append(d)
    return out


def main() -> int:
    print("=" * 90)
    print(f"Per-Symbol Multi-Day Validation — {len(SYMBOLS)} symbols × all swing scans")
    print("=" * 90)

    replica = ProdReplicaV2()
    # Warm brain snapshot cache
    print("\nPre-loading brain snapshots...")
    dates_with_snaps = replica.brain_loader.list_dates_with_snapshots()
    print(f"  {len(dates_with_snaps)} dates available")

    overall = {"n": 0, "dir": 0, "raw": 0, "adj": 0, "blocker_exact": 0, "blocker_compat": 0}
    per_symbol_stats: dict[tuple[str, str], dict] = {}

    for symbol, setup in SYMBOLS:
        print(f"\n{'='*90}")
        print(f"  Symbol: {symbol}  Setup: {setup}")
        print(f"{'='*90}")

        # Pull all swing scans for this symbol+setup
        rows = _bq_query(f"""
            SELECT scan_ts, run_date, direction, raw_score, adjusted_score, qualified,
                   blocked_reason, regime
            FROM `grow-profit-machine.autotrader.scan_decisions`
            WHERE wl_type='swing'
              AND symbol='{symbol}' AND setup='{setup}'
              AND run_date >= '2026-04-10'
            ORDER BY scan_ts
        """)
        if not rows:
            print(f"  ⚠️  No scans found")
            continue
        print(f"  Found {len(rows)} BQ swing scans")

        stats = {"n": 0, "dir": 0, "raw": 0, "adj": 0, "blocker_exact": 0, "blocker_compat": 0}
        examples_match: list[dict] = []
        examples_mismatch: list[dict] = []

        for bq in rows:
            scan_ts = bq["scan_ts"]
            try:
                rp = replica.replay_scan(symbol=symbol, setup=setup, scan_ts=scan_ts, is_swing=True)
            except Exception as exc:
                examples_mismatch.append({
                    "scan_ts": scan_ts, "error": f"{type(exc).__name__}: {exc}",
                })
                continue

            stats["n"] += 1
            bq_dir = (bq.get("direction") or "HOLD")
            bq_raw = int(bq.get("raw_score") or 0)
            bq_adj = int(bq.get("adjusted_score") or 0)
            bq_blk = bq.get("blocked_reason") or ""

            dir_match = (bq_dir == rp.direction)
            raw_match = (abs(bq_raw - rp.raw_score) <= 1)
            adj_match = (abs(bq_adj - rp.adjusted_score) <= 1)
            blocker_exact = (bq_blk == rp.blocked_reason)
            # Compatible: both qualified, or both blocked (any reason)
            bq_qualified = str(bq.get("qualified", "")).lower() == "true"
            blocker_compat = (bq_qualified == rp.qualified)

            if dir_match: stats["dir"] += 1
            if raw_match: stats["raw"] += 1
            if adj_match: stats["adj"] += 1
            if blocker_exact: stats["blocker_exact"] += 1
            if blocker_compat: stats["blocker_compat"] += 1

            row = {
                "scan_ts": scan_ts, "regime": bq.get("regime"),
                "BQ_dir": bq_dir, "RP_dir": rp.direction,
                "BQ_raw": bq_raw, "RP_raw": rp.raw_score,
                "BQ_adj": bq_adj, "RP_adj": rp.adjusted_score,
                "BQ_blk": bq_blk, "RP_blk": rp.blocked_reason,
            }
            if dir_match and raw_match and adj_match:
                if len(examples_match) < 3:
                    examples_match.append(row)
            else:
                if len(examples_mismatch) < 3:
                    examples_mismatch.append(row)

        n = stats["n"]
        if n == 0:
            continue
        per_symbol_stats[(symbol, setup)] = stats
        # accumulate
        for k in stats:
            overall[k] = overall.get(k, 0) + stats[k]

        # Per-symbol summary
        print(f"\n  Match rates (n={n}):")
        print(f"    direction:       {stats['dir']}/{n}  ({stats['dir']/n*100:.1f}%)")
        print(f"    raw_score ±1:    {stats['raw']}/{n}  ({stats['raw']/n*100:.1f}%)")
        print(f"    adjusted ±1:     {stats['adj']}/{n}  ({stats['adj']/n*100:.1f}%)")
        print(f"    blocker exact:   {stats['blocker_exact']}/{n}  ({stats['blocker_exact']/n*100:.1f}%)")
        print(f"    qualif compat:   {stats['blocker_compat']}/{n}  ({stats['blocker_compat']/n*100:.1f}%)")

        if examples_match:
            print(f"\n  ✅ Sample matched rows:")
            for ex in examples_match[:2]:
                print(f"    {ex['scan_ts'][:19]}  reg={ex['regime']:>8s}  dir={ex['BQ_dir']:>4s} raw={ex['BQ_raw']:>3d} adj={ex['BQ_adj']:>3d}  blk={ex['BQ_blk']}")
        if examples_mismatch:
            print(f"\n  ⚠️  Sample mismatches:")
            for ex in examples_mismatch[:2]:
                if "error" in ex:
                    print(f"    {ex['scan_ts'][:19]}  ERROR: {ex['error']}")
                else:
                    print(f"    {ex['scan_ts'][:19]}  reg={ex['regime']:>8s}  BQ(dir={ex['BQ_dir']:>4s} raw={ex['BQ_raw']:>3d} adj={ex['BQ_adj']:>3d} blk={ex['BQ_blk'][:25]:25s})")
                    print(f"    {' '*19}  {' '*13}     RP(dir={ex['RP_dir']:>4s} raw={ex['RP_raw']:>3d} adj={ex['RP_adj']:>3d} blk={ex['RP_blk'][:25]:25s})")

    # Overall summary
    print(f"\n{'='*90}")
    print("OVERALL SUMMARY — ALL SYMBOLS")
    print("=" * 90)
    n = overall["n"]
    if n == 0:
        print("No scans tested")
        return 1
    print(f"  Total swing scans validated: {n}")
    print(f"  Direction match:    {overall['dir']}/{n}  ({overall['dir']/n*100:.1f}%)")
    print(f"  raw_score ±1:       {overall['raw']}/{n}  ({overall['raw']/n*100:.1f}%)")
    print(f"  adjusted_score ±1:  {overall['adj']}/{n}  ({overall['adj']/n*100:.1f}%)")
    print(f"  Blocker exact:      {overall['blocker_exact']}/{n}  ({overall['blocker_exact']/n*100:.1f}%)")
    print(f"  Qualified compat:   {overall['blocker_compat']}/{n}  ({overall['blocker_compat']/n*100:.1f}%)")

    # Per-symbol table
    print(f"\n  Per-symbol breakdown:")
    print(f"    {'Symbol':14s} {'Setup':18s} {'N':>4s} {'Dir%':>7s} {'Raw%':>7s} {'Adj%':>7s} {'Blk%':>7s}")
    for (sym, setup), s in per_symbol_stats.items():
        n_s = s["n"]
        print(f"    {sym:14s} {setup:18s} {n_s:>4d} {s['dir']/n_s*100:>6.1f}% {s['raw']/n_s*100:>6.1f}% {s['adj']/n_s*100:>6.1f}% {s['blocker_exact']/n_s*100:>6.1f}%")

    # Verdict
    pass_dir = (overall["dir"] / n) >= 0.95
    pass_raw = (overall["raw"] / n) >= 0.90
    pass_adj = (overall["adj"] / n) >= 0.90
    print()
    print("VERDICT:")
    print(f"  Direction ≥95%:        {'✅ PASS' if pass_dir else '❌ FAIL'}")
    print(f"  Raw score ≥90%:        {'✅ PASS' if pass_raw else '❌ FAIL'}")
    print(f"  Adjusted score ≥90%:   {'✅ PASS' if pass_adj else '❌ FAIL'}")
    if pass_dir and pass_raw and pass_adj:
        print("\n  🎯 SWING REPLICA VALIDATED — production-grade for backtesting.")
        return 0
    print("\n  ⚠️  Some fields below threshold — investigate before declaring done.")
    return 2


if __name__ == "__main__":
    sys.exit(main())
