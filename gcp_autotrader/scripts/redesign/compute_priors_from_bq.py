#!/usr/bin/env python3
"""M3 — Rebuild config/priors/priors_v1.json from BQ `trades`.

This is INTENTIONALLY a single script, not an engine (per the user's
"no over-engineering" mandate). It:

  1. Queries BigQuery `trades` table, grouping by (regime, setup, direction).
  2. Computes win_rate, avg_win_r, avg_loss_r per group using the
     existing trade schema (entry_price, sl_price, net_pnl, qty, side).
  3. Writes priors_v1.json atomically (tmp + rename) so a concurrent
     service reading the file never sees a half-written JSON.

Usage:
    python scripts/redesign/compute_priors_from_bq.py \
        --project grow-profit-machine \
        --dataset autotrader \
        --table trades \
        --since 2025-01-01

Run it whenever you want to refresh priors — typically weekly via a
scheduled Cloud Run Job after enough trades accumulate. Writes to the
SAME path the runtime loads from, so the next process restart picks up
the fresh values.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

logger = logging.getLogger("compute_priors")

# The project loads priors from config/priors/priors_v1.json relative to
# the repo root. Resolve that regardless of where the script is invoked
# from.
_REPO_ROOT = Path(__file__).resolve().parents[2]
_PRIORS_PATH = _REPO_ROOT / "config" / "priors" / "priors_v1.json"


def _query(project: str, dataset: str, table: str, since: str) -> list[dict]:
    """Pull minimal columns from BQ. Imported lazily so --help works without BQ."""
    from google.cloud import bigquery  # type: ignore

    client = bigquery.Client(project=project)
    sql = f"""
        SELECT
            UPPER(COALESCE(regime, ''))   AS regime,
            UPPER(COALESCE(strategy, '')) AS setup,
            UPPER(COALESCE(side, ''))     AS side,
            entry_price,
            sl_price,
            qty,
            net_pnl,
            pnl,
            exit_reason
        FROM `{project}.{dataset}.{table}`
        WHERE trade_date >= '{since}'
          AND entry_price > 0
          AND sl_price    > 0
          AND qty         > 0
    """
    rows = client.query(sql).result()
    return [dict(r) for r in rows]


def _realized_r(row: dict) -> float:
    """Signed R for a trade. +1.5 = made 1.5R, -1.0 = took a full stop."""
    entry = float(row.get("entry_price") or 0.0)
    sl = float(row.get("sl_price") or 0.0)
    qty = int(row.get("qty") or 0)
    side = str(row.get("side") or "").upper()
    pnl = float(row.get("net_pnl") or row.get("pnl") or 0.0)

    sl_dist = abs(entry - sl)
    if sl_dist <= 0 or qty <= 0:
        return 0.0
    # Per-share R
    per_share_pnl = pnl / qty
    # LONG: profit when exit > entry → positive R. SHORT: symmetric.
    r = per_share_pnl / sl_dist
    # `pnl` sign already encodes direction via broker P&L convention.
    return r


def _bucket_direction(side: str) -> str:
    s = str(side or "").strip().upper()
    if s in ("BUY", "LONG"):
        return "LONG"
    if s in ("SELL", "SHORT"):
        return "SHORT"
    return s


def _summarize(rows: list[dict]) -> dict[str, dict]:
    """Group rows by (regime, setup, direction); compute priors per group."""
    buckets: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    for r in rows:
        regime = str(r.get("regime") or "").upper()
        setup = str(r.get("setup") or "").upper()
        direction = _bucket_direction(r.get("side") or "")
        if not (regime and setup and direction):
            continue
        r_realized = _realized_r(r)
        buckets[(regime, setup, direction)].append(r_realized)

    out: dict[str, dict] = {}
    for (regime, setup, direction), rs in buckets.items():
        wins = [x for x in rs if x > 0]
        losses = [x for x in rs if x <= 0]
        n = len(rs)
        win_rate = len(wins) / n if n else 0.0
        avg_win_r = sum(wins) / len(wins) if wins else 0.0
        avg_loss_r = sum(losses) / len(losses) if losses else -1.0
        key = f"{regime}:{setup}:{direction}"
        out[key] = {
            "win_rate": round(win_rate, 4),
            "avg_win_r": round(avg_win_r, 4),
            "avg_loss_r": round(avg_loss_r, 4),
            "n": n,
        }
    return out


def _atomic_write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Write to tmp file in the same dir then rename — readers on the
    # other side never see a partial JSON.
    fd, tmp_name = tempfile.mkstemp(prefix="priors_", suffix=".json", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, sort_keys=True)
            fh.write("\n")
        os.replace(tmp_name, path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Rebuild priors_v1.json from BQ trades")
    ap.add_argument("--project", required=False, default=os.getenv("GCP_PROJECT_ID", "grow-profit-machine"))
    ap.add_argument("--dataset", default=os.getenv("BQ_DATASET", "autotrader"))
    ap.add_argument("--table", default=os.getenv("BQ_TRADES_TABLE", "trades"))
    ap.add_argument("--since", default="2025-01-01",
                    help="ISO date — trades earlier than this are ignored")
    ap.add_argument("--dry-run", action="store_true", help="Print resulting JSON; do not write.")
    ap.add_argument("--min-default", type=int, default=30,
                    help="min_sample_size to embed in the output")
    args = ap.parse_args()

    logger.info("querying bq project=%s dataset=%s table=%s since=%s",
                args.project, args.dataset, args.table, args.since)
    rows = _query(args.project, args.dataset, args.table, args.since)
    logger.info("rows=%d", len(rows))

    entries = _summarize(rows)
    payload: dict = {
        "_comment": "Regenerated by scripts/redesign/compute_priors_from_bq.py. Do not edit by hand.",
        "version": 1,
        "min_sample_size": int(args.min_default),
        "_default": {
            "win_rate": 0.40,
            "avg_win_r": 1.50,
            "avg_loss_r": -1.00,
            "n": 0,
        },
    }
    payload.update(entries)

    if args.dry_run:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    _atomic_write(_PRIORS_PATH, payload)
    logger.info("wrote priors path=%s keys=%d", _PRIORS_PATH, len(entries))
    return 0


if __name__ == "__main__":
    sys.exit(main())
