#!/usr/bin/env python3
"""M6 — Roll up the `attribution` BQ table into `daily_metrics`.

Designed to run as a scheduled Cloud Run Job (nightly, post-market). One
invocation reads attribution rows for the target date, folds them into a
single DailyMetrics row per trade_date, and writes to the `daily_metrics`
BQ table. The weekly review dashboard reads `daily_metrics`, not the raw
attribution stream.

Deliberately a single script, not a service (per the "no over-engineering"
mandate). If the scheduler misses a day, re-run with --since covering the
gap; the target table dedupes on (trade_date) via the caller truncating
first when needed.

Usage:
    python scripts/redesign/compute_daily_metrics.py \
        --project grow-profit-machine \
        --dataset autotrader \
        --since 2026-04-01 \
        --until 2026-04-23
"""
from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

logger = logging.getLogger("compute_daily_metrics")


def _query(project: str, dataset: str, since: str, until: str) -> list[dict]:
    """Pull attribution rows for [since, until] inclusive."""
    from google.cloud import bigquery  # type: ignore

    client = bigquery.Client(project=project)
    sql = f"""
        SELECT
          trade_date, position_tag, symbol, edge_name,
          expected_r, realized_r, r_delta,
          expected_hold_minutes, actual_hold_minutes, hold_delta_minutes,
          mfe_r, mae_r, exit_reason, channel, paper
        FROM `{project}.{dataset}.attribution`
        WHERE trade_date BETWEEN '{since}' AND '{until}'
    """
    rows = [dict(r) for r in client.query(sql).result()]
    logger.info("fetched %d attribution rows between %s..%s", len(rows), since, until)
    return rows


def _group_by_date(rows: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        d = str(r.get("trade_date") or "")[:10]
        if not d:
            continue
        grouped[d].append(r)
    return dict(grouped)


def _date_range(since: str, until: str) -> list[str]:
    d0 = datetime.fromisoformat(since).date()
    d1 = datetime.fromisoformat(until).date()
    out: list[str] = []
    cur = d0
    while cur <= d1:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


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

    until = args.until or args.since

    # Deferred so --help doesn't require GCP / src on the path.
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
    from autotrader.adapters.bigquery_client import BigQueryClient
    from autotrader.domain.attribution import rollup

    rows = _query(args.project, args.dataset, args.since, until)
    grouped = _group_by_date(rows)

    wrote = 0
    bq = BigQueryClient(args.project, args.dataset) if not args.dry_run else None
    for d in _date_range(args.since, until):
        day_rows = grouped.get(d, [])
        metrics = rollup(day_rows, trade_date=d)
        bq_row = metrics.to_bq_row()
        if args.dry_run:
            print(bq_row)
            continue
        try:
            assert bq is not None
            bq.insert_daily_metrics(bq_row)
            wrote += 1
        except Exception:
            logger.exception("daily_metrics_insert_failed date=%s", d)

    logger.info("done days=%d wrote=%d", len(_date_range(args.since, until)), wrote)
    return 0


if __name__ == "__main__":
    sys.exit(main())
