#!/usr/bin/env python3
"""M6 — CLI wrapper for the daily_metrics rollup.

The actual logic lives in `autotrader.services.daily_metrics_service.run()`
so the same code path serves both the nightly Cloud Scheduler trigger
(`POST /jobs/compute-daily-metrics`) and ad-hoc manual backfills via this
script.

Usage:
    python scripts/redesign/compute_daily_metrics.py \\
        --project grow-profit-machine \\
        --dataset autotrader \\
        --since 2026-04-01 \\
        --until 2026-04-23
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default="grow-profit-machine")
    ap.add_argument("--dataset", default="autotrader")
    ap.add_argument("--since", required=True, help="YYYY-MM-DD inclusive")
    ap.add_argument("--until", default=None, help="YYYY-MM-DD inclusive; defaults to --since")
    ap.add_argument("--dry-run", action="store_true",
                    help="print rollup rows instead of writing to BQ")
    args = ap.parse_args(argv)

    # Add src/ to path so the import resolves when run from the repo root.
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
    from autotrader.services.daily_metrics_service import run

    out = run(
        project=args.project,
        dataset=args.dataset,
        since=args.since,
        until=args.until,
        dry_run=args.dry_run,
    )
    if args.dry_run:
        for row in out.get("rollup_rows", []):
            print(row)
    return 0 if not out.get("failed") else 1


if __name__ == "__main__":
    sys.exit(main())
