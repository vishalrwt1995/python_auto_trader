"""Equivalence Test v2 — validates ProdReplicaV2 against BQ scan_decisions.

For a given historical date:
  1. Pull ALL swing scans from BQ scan_decisions
  2. For each (symbol, setup, scan_ts), run prod_replica_v2 with the brain
     snapshot at that moment
  3. Compare predicted vs BQ-recorded: qualified, raw_score, adjusted_score,
     blocked_reason
  4. Tabulate matches + mismatches by category

## Pass criteria

  - Qualified count: within ±10% of BQ
  - Direction (BUY/SELL/HOLD): ≥95% match
  - raw_score: median diff ≤ 5 points (production has float→int rounding)
  - adjusted_score: median diff ≤ 5 points
  - blocked_reason distribution: ≥80% of mismatches are "documented gate-order"
    or "documented missing gate" (the 5-15% we can't replicate without portfolio state)

## What this proves

If passed: our replica reproduces production behavior closely enough that
multi-day backtests can be trusted within stated tolerances.

Run:
    PYTHONPATH=src python -m autotrader.backtest_v2.equivalence_v2 2026-05-19
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.request
from collections import defaultdict
from typing import Any

from autotrader.backtest_v2.brain_loader import BrainSnapshotLoader
from autotrader.backtest_v2.prod_replica_v2 import ProdReplicaV2, SWING_SETUPS_REPLICATED


# Documented blocked_reasons we KNOW we can't replicate without portfolio state
# or live-data. Mismatches on these are acceptable.
DOCUMENTED_MISSING = {
    "sl_too_wide_for_risk_budget",
    "capital_exhausted",
    "reentry_cooldown",
    "swing_max_positions_reached",
    "policy_max_positions_reached",
    "daily_loss_limit_strategy_restricted",
    "stale_signal_price_moved",
    "live_price_below_vwap",
    "live_price_above_vwap",
    "portfolio_sector_concentrated",
    "portfolio_strategy_concentrated",
}
# These are reasons that start with "earnings_blackout_" — also documented missing
DOCUMENTED_MISSING_PREFIXES = ("earnings_blackout_",)


def _bq_query(query: str) -> list[dict]:
    """Execute BQ via REST API using ADC token."""
    gcloud = os.environ.get("GCLOUD", "gcloud")
    token = subprocess.check_output(
        [gcloud, "auth", "application-default", "print-access-token"],
        text=True,
    ).strip()
    from datetime import datetime, timezone
    body = json.dumps({
        "query": query, "useLegacySql": False, "location": "asia-south1",
    }).encode()
    req = urllib.request.Request(
        "https://bigquery.googleapis.com/bigquery/v2/projects/grow-profit-machine/queries",
        data=body,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
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


def is_documented_missing(reason: str) -> bool:
    if not reason:
        return False
    if reason in DOCUMENTED_MISSING:
        return True
    for prefix in DOCUMENTED_MISSING_PREFIXES:
        if reason.startswith(prefix):
            return True
    return False


def main() -> int:
    test_date = sys.argv[1] if len(sys.argv) > 1 else "2026-05-19"
    print("=" * 80)
    print(f"Equivalence Test v2 — {test_date}")
    print("=" * 80)

    # 1) Pull all swing scans
    setups_csv = ",".join(f"'{s}'" for s in SWING_SETUPS_REPLICATED)
    print(f"\n[1] Pulling BQ swing scans for {test_date}...")
    rows = _bq_query(f"""
        SELECT scan_ts, symbol, setup, direction, raw_score, adjusted_score,
               min_score, qualified, blocked_reason, regime, wl_type
        FROM `grow-profit-machine.autotrader.scan_decisions`
        WHERE run_date = '{test_date}' AND setup IN ({setups_csv})
        ORDER BY scan_ts, symbol, setup
    """)
    if not rows:
        print(f"    ⚠️  No BQ swing scans for {test_date}")
        return 1
    print(f"    Got {len(rows)} BQ rows")

    # Deduplicate by (symbol, setup) — keep last scan (most recent)
    by_key: dict[tuple[str, str], dict] = {}
    for r in rows:
        key = (r["symbol"], r["setup"])
        if key in by_key:
            if r["scan_ts"] > by_key[key]["scan_ts"]:
                by_key[key] = r
        else:
            by_key[key] = r
    print(f"    Deduplicated to {len(by_key)} unique (symbol, setup) pairs")

    # 2) Run replay on each
    print("\n[2] Running ProdReplicaV2 on each scan...")
    replica = ProdReplicaV2()
    # Pre-cache brain snapshots for the day to speed up
    brain_loader = replica.brain_loader
    _ = brain_loader.list_snapshots_for_date(test_date)

    results = []
    for i, ((symbol, setup), bq_row) in enumerate(by_key.items()):
        if i % 100 == 0:
            print(f"    ... {i}/{len(by_key)}")
        # Use scan_ts from BQ to find the matching brain snapshot
        scan_ts = bq_row["scan_ts"]
        wl_type = bq_row.get("wl_type", "swing")
        is_swing = (wl_type or "swing").lower() == "swing"
        replay = replica.replay_scan(
            symbol=symbol, setup=setup, scan_ts=scan_ts, is_swing=is_swing,
        )
        results.append((bq_row, replay))

    # 3) Tabulate
    print("\n[3] Tabulating matches...")

    bq_qualified = sum(1 for r, _ in results if str(r.get("qualified")).lower() == "true")
    rp_qualified = sum(1 for _, r in results if r.qualified)
    direction_match = sum(1 for b, r in results if (b.get("direction") or "HOLD") == r.direction)
    raw_match = sum(1 for b, r in results if abs(int(b.get("raw_score") or 0) - r.raw_score) <= 5)
    adj_match = sum(1 for b, r in results if abs(int(b.get("adjusted_score") or 0) - r.adjusted_score) <= 5)

    print(f"\n    Qualified BQ:       {bq_qualified}")
    print(f"    Qualified Replay:   {rp_qualified}")
    qd = abs(bq_qualified - rp_qualified)
    qd_pct = qd / max(bq_qualified, 1) * 100
    print(f"    Δ qualified:        {rp_qualified - bq_qualified:+d}  ({qd_pct:.1f}%)")
    print()
    n = len(results)
    print(f"    Direction match:    {direction_match}/{n}  ({direction_match/n*100:.1f}%)")
    print(f"    raw_score ±5:       {raw_match}/{n}  ({raw_match/n*100:.1f}%)")
    print(f"    adjusted_score ±5:  {adj_match}/{n}  ({adj_match/n*100:.1f}%)")

    # 4) Blocked-reason analysis
    print("\n[4] Blocked-reason distribution:")
    bq_blockers: dict[str, int] = defaultdict(int)
    rp_blockers: dict[str, int] = defaultdict(int)
    mismatch_documented = 0
    mismatch_undocumented = 0
    mismatch_samples_undoc: list[tuple[dict, Any]] = []

    for bq, rp in results:
        bq_b = bq.get("blocked_reason") or ""
        rp_b = rp.blocked_reason or ""
        if bq_b:
            bq_blockers[bq_b] += 1
        if rp_b:
            rp_blockers[rp_b] += 1
        if bq_b != rp_b:
            if is_documented_missing(bq_b) or is_documented_missing(rp_b):
                mismatch_documented += 1
            else:
                mismatch_undocumented += 1
                if len(mismatch_samples_undoc) < 10:
                    mismatch_samples_undoc.append((bq, rp))

    all_blockers = sorted(set(bq_blockers) | set(rp_blockers),
                          key=lambda b: -(bq_blockers.get(b, 0) + rp_blockers.get(b, 0)))
    print(f"  {'blocked_reason':52s} {'BQ':>6s} {'Replay':>7s} {'Δ':>7s}")
    print("  " + "-" * 80)
    for b in all_blockers[:30]:
        bqn = bq_blockers.get(b, 0)
        rpn = rp_blockers.get(b, 0)
        marker = "📝" if is_documented_missing(b) else ""
        print(f"  {b:52s} {bqn:>6d} {rpn:>7d} {rpn-bqn:>+7d}  {marker}")

    print(f"\n    Documented mismatches:    {mismatch_documented}")
    print(f"    Undocumented mismatches:  {mismatch_undocumented}")

    if mismatch_samples_undoc:
        print("\n    First 10 UNDOCUMENTED mismatches (these need investigation):")
        print(f"    {'symbol':14s} {'setup':16s} {'regime':10s} {'dir':4s} {'BQ qual':>8s} {'RP qual':>8s} {'BQ blk':30s} {'RP blk':30s}")
        for bq, rp in mismatch_samples_undoc:
            bq_q = str(bq.get('qualified', '')).lower() == 'true'
            print(f"    {bq.get('symbol',''):14s} {bq.get('setup',''):16s} {bq.get('regime',''):10s} {bq.get('direction',''):4s} {str(bq_q):>8s} {str(rp.qualified):>8s} {(bq.get('blocked_reason') or '')[:30]:30s} {(rp.blocked_reason or '')[:30]:30s}")

    # 5) Verdict
    print("\n" + "=" * 80)
    print("VERDICT")
    print("=" * 80)
    pass_qualif = qd_pct <= 10
    pass_dir = direction_match / n >= 0.95
    pass_raw = raw_match / n >= 0.80
    pass_adj = adj_match / n >= 0.80
    pass_undoc = mismatch_undocumented <= max(5, int(n * 0.05))   # ≤5% undocumented

    print(f"  ✅ Qualified ±10%:           {pass_qualif} ({qd_pct:.1f}%)")
    print(f"  ✅ Direction match ≥95%:     {pass_dir} ({direction_match/n*100:.1f}%)")
    print(f"  ✅ raw_score ±5 ≥80%:        {pass_raw} ({raw_match/n*100:.1f}%)")
    print(f"  ✅ adjusted_score ±5 ≥80%:   {pass_adj} ({adj_match/n*100:.1f}%)")
    print(f"  ✅ Undocumented mismatches ≤5%: {pass_undoc} ({mismatch_undocumented}/{n} = {mismatch_undocumented/n*100:.1f}%)")

    all_pass = pass_qualif and pass_dir and pass_raw and pass_adj and pass_undoc
    if all_pass:
        print("\n  🎯 ALL CHECKS PASS — replica is calibrated to production.")
        return 0
    print("\n  ❌ Some checks failed — replica needs further investigation.")
    return 2


if __name__ == "__main__":
    sys.exit(main())
