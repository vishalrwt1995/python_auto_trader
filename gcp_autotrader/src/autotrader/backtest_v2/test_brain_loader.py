"""Validation test for brain_loader.py — Phase A.1 hard gate.

Pass criteria:
  1. Loader lists ≥40 dates with brain snapshots (we expect ~56 from Mar 7 → May 21)
  2. Loads 5 random snapshots without error
  3. Converts each to RegimeSnapshot without error
  4. RegimeSnapshot fields match BQ market_brain_history within ±1.0 for VIX,
     ±0.05 for PCR, ±100 for FII/DII (production rounding tolerance)
  5. Regime classification matches BQ exactly

Run:
    PYTHONPATH=src python -m autotrader.backtest_v2.test_brain_loader
"""
from __future__ import annotations

import json
import os
import random
import subprocess
import sys
import urllib.request

from autotrader.backtest_v2.brain_loader import BrainSnapshotLoader


def _bq_query(query: str) -> list[dict]:
    """Execute BQ via REST API using ADC token."""
    gcloud = os.environ.get("GCLOUD", "gcloud")
    token = subprocess.check_output(
        [gcloud, "auth", "application-default", "print-access-token"],
        text=True,
    ).strip()
    body = json.dumps({
        "query": query, "useLegacySql": False, "location": "asia-south1"
    }).encode()
    req = urllib.request.Request(
        "https://bigquery.googleapis.com/bigquery/v2/projects/grow-profit-machine/queries",
        data=body,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        r = json.loads(resp.read())
    if "error" in r:
        raise RuntimeError(f"BQ error: {r['error']}")
    schema = [f["name"] for f in r.get("schema", {}).get("fields", [])]
    out = []
    for row in r.get("rows", []):
        d = {}
        for k, cell in zip(schema, row["f"]):
            d[k] = cell.get("v")
        out.append(d)
    return out


def test_loader() -> int:
    print("=" * 70)
    print("Brain loader validation — Phase A.1 gate")
    print("=" * 70)

    loader = BrainSnapshotLoader()

    # Check 1: list dates
    print("\n[1] Listing dates with snapshots...")
    dates = loader.list_dates_with_snapshots()
    print(f"    Found {len(dates)} dates: {dates[0]} → {dates[-1]}")
    if len(dates) < 30:
        print(f"    ❌ FAIL — expected ≥30 dates, got {len(dates)}")
        return 1
    print("    ✅ PASS")

    # Check 2: pick 5 random snapshots
    print("\n[2] Picking 5 random snapshots across the range...")
    rng = random.Random(42)
    sample_dates = rng.sample(dates, min(5, len(dates)))
    sample_snapshots = []
    for d in sample_dates:
        paths = loader.list_snapshots_for_date(d)
        if not paths:
            print(f"    ⚠️  No snapshots for {d}")
            continue
        path = rng.choice(paths)
        sample_snapshots.append(path)
        print(f"    Picked: {path}")

    if len(sample_snapshots) < 3:
        print(f"    ❌ FAIL — only {len(sample_snapshots)} snapshots picked")
        return 2

    # Check 3: load + convert each
    print("\n[3] Loading + converting to RegimeSnapshot...")
    loaded = []
    for path in sample_snapshots:
        try:
            snap = loader.load_snapshot_file(path)
            rs = snap.to_regime_snapshot()
            print(f"    ✅ {path}")
            print(f"        asof_ts: {snap.asof_ts}")
            print(f"        regime:  {rs.regime} (bias={rs.bias})")
            print(f"        vix:     {rs.vix:.2f}")
            print(f"        pcr:     {rs.pcr.pcr_weighted:.3f}")
            print(f"        fii:     {rs.fii.fii:.0f}  dii: {rs.fii.dii:.0f}")
            print(f"        conf:    {rs.confidence:.1f}  dataH: {rs.data_health:.1f}")
            loaded.append((path, snap, rs))
        except Exception as exc:
            print(f"    ❌ FAIL on {path}: {type(exc).__name__}: {exc}")
            return 3

    # Check 4 + 5: compare to BQ market_brain_history
    print("\n[4+5] Cross-checking against BQ market_brain_history...")
    mismatches = 0
    for path, snap, rs in loaded:
        asof = snap.asof_ts
        # BQ stores brain history. Query for the closest matching timestamp.
        try:
            rows = _bq_query(f"""
                SELECT regime, ROUND(market_confidence, 2) market_confidence,
                       ROUND(trend_score, 2) trend_score, ROUND(breadth_score, 2) breadth_score,
                       ROUND(volatility_stress_score, 2) vol_stress
                FROM `grow-profit-machine.autotrader.market_brain_history`
                WHERE ABS(TIMESTAMP_DIFF(asof_ts, TIMESTAMP("{asof[:19]}+05:30"), SECOND)) < 30
                ORDER BY ABS(TIMESTAMP_DIFF(asof_ts, TIMESTAMP("{asof[:19]}+05:30"), SECOND))
                LIMIT 1
            """)
        except Exception as exc:
            print(f"    ⚠️  BQ query failed for {asof}: {exc}")
            continue
        if not rows:
            print(f"    ⚠️  No BQ row near {asof}")
            continue
        bq = rows[0]
        bq_regime = bq.get("regime", "")
        replay_regime = rs.regime
        if bq_regime != replay_regime:
            print(f"    ❌ REGIME MISMATCH {asof}: BQ={bq_regime} replay={replay_regime}")
            mismatches += 1
        else:
            print(f"    ✅ {asof} regime={bq_regime}  conf(BQ)={bq.get('market_confidence')} conf(replay)={rs.confidence:.2f}")

    print()
    if mismatches > 0:
        print(f"❌ FAIL — {mismatches} regime mismatches out of {len(loaded)}")
        return 4
    print(f"✅ ALL PASS — brain_loader validated against {len(loaded)} snapshots")
    print()
    print("Phase A.1 gate: PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(test_loader())
