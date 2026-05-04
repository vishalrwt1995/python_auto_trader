"""M6 — Roll up the `attribution` BQ table into `daily_metrics`.

The actual rollup math lives in `domain.attribution.rollup`; this service is
the orchestration layer that:
  - queries the attribution table for [since, until],
  - groups by trade_date,
  - calls `rollup()` per day,
  - writes one DailyMetrics row per day to BQ.

Reused by:
  - `POST /jobs/compute-daily-metrics` (Cloud Scheduler nightly trigger).
  - `scripts/redesign/compute_daily_metrics.py` (manual CLI for backfills).
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)


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


def run(
    project: str = "grow-profit-machine",
    dataset: str = "autotrader",
    since: str | None = None,
    until: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Roll up attribution → daily_metrics for [since, until] inclusive.

    Returns a JSON-friendly dict with stats so callers (HTTP endpoint, CLI)
    can surface progress + per-day failures without re-parsing logs.

    `since` defaults to the previous calendar day (UTC); `until` defaults to
    `since`. The combination "no args" is the nightly Scheduler default —
    one day behind so we know the trade table has settled.
    """
    if since is None:
        since = (datetime.utcnow().date() - timedelta(days=1)).isoformat()
    if until is None:
        until = since

    from autotrader.adapters.bigquery_client import BigQueryClient
    from autotrader.domain.attribution import rollup

    rows = _query(project, dataset, since, until)
    grouped = _group_by_date(rows)

    days = _date_range(since, until)
    bq = None if dry_run else BigQueryClient(project, dataset)

    wrote = 0
    failed: list[str] = []
    rollup_rows: list[dict[str, Any]] = []
    for d in days:
        day_rows = grouped.get(d, [])
        metrics = rollup(day_rows, trade_date=d)
        bq_row = metrics.to_bq_row()
        if dry_run:
            rollup_rows.append(bq_row)
            continue
        try:
            assert bq is not None
            bq.insert_daily_metrics(bq_row)
            wrote += 1
        except Exception:
            logger.exception("daily_metrics_insert_failed date=%s", d)
            failed.append(d)

    logger.info(
        "done since=%s until=%s days=%d wrote=%d failed=%d attribution_rows=%d",
        since, until, len(days), wrote, len(failed), len(rows),
    )
    return {
        "since": since,
        "until": until,
        "days": len(days),
        "wrote": wrote,
        "failed": failed,
        "attribution_rows": len(rows),
        "dry_run": dry_run,
        # Only populated on dry runs — the rollup payloads themselves.
        "rollup_rows": rollup_rows if dry_run else [],
    }
