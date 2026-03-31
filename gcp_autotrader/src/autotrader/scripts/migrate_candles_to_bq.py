"""One-time migration: GCS candle JSON files → BigQuery candles_1d / candles_5m.

Usage::

    python -m autotrader.scripts.migrate_candles_to_bq \\
        --project grow-profit-machine \\
        --bucket autotrader-data \\
        --dataset autotrader \\
        --dry-run

Progress is tracked in Firestore ``migration_state/candles`` so the script can be
re-run safely (already-migrated GCS paths are skipped).

GCS layout assumed:
  candles/1d/<symbol>/<date>.json   or   candles/1d/<symbol>.json
  candles/5m/<symbol>/<date>.json   or   candles/5m/<symbol>.json

Each JSON file contains a list of candle arrays::

    [[ts, open, high, low, close, volume], ...]     # 1D
    [[ts, open, high, low, close, volume], ...]     # 5m

where ts is an ISO-8601 string or epoch-ms integer.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

_BATCH_SIZE = 1000


def _parse_ts(v: Any) -> tuple[str, str]:
    """Return (candle_ts_iso, trade_date_iso).  v may be ISO string or epoch-ms."""
    if v is None:
        now = datetime.now(tz=timezone.utc)
        return now.isoformat(timespec="seconds"), now.date().isoformat()
    s = str(v).strip()
    if s.isdigit() and len(s) >= 13:
        dt = datetime.fromtimestamp(int(s) / 1000, tz=timezone.utc)
    else:
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            dt = datetime.now(tz=timezone.utc)
    return dt.isoformat(timespec="seconds"), dt.date().isoformat()


def _candle_to_1d_row(symbol: str, instrument_key: str, exchange: str, segment: str, candle: Any) -> dict | None:
    """Convert a raw candle array/dict to a BigQuery candles_1d row."""
    try:
        if isinstance(candle, (list, tuple)) and len(candle) >= 6:
            ts_iso, trade_date = _parse_ts(candle[0])
            return {
                "trade_date": trade_date,
                "symbol": symbol.upper(),
                "exchange": exchange,
                "segment": segment,
                "open": float(candle[1]),
                "high": float(candle[2]),
                "low": float(candle[3]),
                "close": float(candle[4]),
                "volume": float(candle[5]),
                "instrument_key": instrument_key,
            }
        if isinstance(candle, dict):
            ts_iso, trade_date = _parse_ts(candle.get("timestamp") or candle.get("ts"))
            return {
                "trade_date": trade_date,
                "symbol": symbol.upper(),
                "exchange": exchange,
                "segment": segment,
                "open": float(candle.get("open") or 0),
                "high": float(candle.get("high") or 0),
                "low": float(candle.get("low") or 0),
                "close": float(candle.get("close") or 0),
                "volume": float(candle.get("volume") or 0),
                "instrument_key": instrument_key,
            }
    except Exception:
        pass
    return None


def _candle_to_5m_row(symbol: str, instrument_key: str, candle: Any) -> dict | None:
    """Convert a raw candle array/dict to a BigQuery candles_5m row."""
    try:
        if isinstance(candle, (list, tuple)) and len(candle) >= 6:
            ts_iso, trade_date = _parse_ts(candle[0])
            return {
                "candle_ts": ts_iso,
                "trade_date": trade_date,
                "symbol": symbol.upper(),
                "open": float(candle[1]),
                "high": float(candle[2]),
                "low": float(candle[3]),
                "close": float(candle[4]),
                "volume": float(candle[5]),
                "instrument_key": instrument_key,
            }
        if isinstance(candle, dict):
            ts_iso, trade_date = _parse_ts(candle.get("timestamp") or candle.get("ts"))
            return {
                "candle_ts": ts_iso,
                "trade_date": trade_date,
                "symbol": symbol.upper(),
                "open": float(candle.get("open") or 0),
                "high": float(candle.get("high") or 0),
                "low": float(candle.get("low") or 0),
                "close": float(candle.get("close") or 0),
                "volume": float(candle.get("volume") or 0),
                "instrument_key": instrument_key,
            }
    except Exception:
        pass
    return None


def _symbol_from_path(gcs_path: str) -> str:
    """Extract symbol from a GCS path like candles/1d/RELIANCE/2024-01-01.json."""
    parts = gcs_path.rstrip("/").split("/")
    if len(parts) >= 3:
        return parts[-2].upper() if parts[-1].endswith(".json") else parts[-1].upper()
    return "UNKNOWN"


def migrate_candles(
    project_id: str,
    bucket_name: str,
    dataset: str,
    prefix_1d: str = "candles/1d",
    prefix_5m: str = "candles/5m",
    dry_run: bool = False,
    max_files: int = 0,
) -> None:
    from google.cloud import storage  # type: ignore[import-untyped]
    from autotrader.adapters.bigquery_client import BigQueryClient
    from autotrader.adapters.firestore_state import FirestoreStateStore

    bq = BigQueryClient(project_id, dataset)
    state = FirestoreStateStore(project_id)
    gcs_client = storage.Client(project=project_id)
    bucket = gcs_client.bucket(bucket_name)

    # Load migration progress
    progress_doc = state.get_json("migration_state", "candles") or {}
    done_paths: set[str] = set(progress_doc.get("done_paths") or [])
    total_rows_1d = int(progress_doc.get("total_rows_1d") or 0)
    total_rows_5m = int(progress_doc.get("total_rows_5m") or 0)
    files_processed = 0

    def _flush_1d(rows: list[dict], sym: str) -> None:
        nonlocal total_rows_1d
        if rows:
            logger.info("  1d insert symbol=%s rows=%d", sym, len(rows))
            if not dry_run:
                bq.insert_candles_1d_batch(rows)
            total_rows_1d += len(rows)

    def _flush_5m(rows: list[dict], sym: str) -> None:
        nonlocal total_rows_5m
        if rows:
            logger.info("  5m insert symbol=%s rows=%d", sym, len(rows))
            if not dry_run:
                bq.insert_candles_5m_batch(rows)
            total_rows_5m += len(rows)

    def _save_progress() -> None:
        if not dry_run:
            state.set_json("migration_state", "candles", {
                "done_paths": list(done_paths),
                "total_rows_1d": total_rows_1d,
                "total_rows_5m": total_rows_5m,
            })

    # ── 1D candles ───────────────────────────────────────────────────────
    logger.info("=== Migrating 1D candles prefix=%s ===", prefix_1d)
    batch: list[dict] = []
    last_symbol = ""
    for blob in bucket.list_blobs(prefix=prefix_1d):
        if not blob.name.endswith(".json"):
            continue
        if blob.name in done_paths:
            continue
        if max_files and files_processed >= max_files:
            break
        symbol = _symbol_from_path(blob.name)
        if symbol != last_symbol and batch:
            _flush_1d(batch, last_symbol)
            batch = []
        last_symbol = symbol
        try:
            raw = json.loads(blob.download_as_text())
            candles = raw if isinstance(raw, list) else (raw.get("candles") or raw.get("data") or [])
            rows = [r for c in candles if (r := _candle_to_1d_row(symbol, "", "NSE", "CASH", c)) is not None]
            batch.extend(rows)
            if len(batch) >= _BATCH_SIZE:
                _flush_1d(batch, symbol)
                batch = []
            done_paths.add(blob.name)
            files_processed += 1
            if files_processed % 100 == 0:
                _save_progress()
                logger.info("  progress 1d files=%d rows=%d", files_processed, total_rows_1d)
        except Exception:
            logger.exception("  1d_file_failed path=%s", blob.name)
    if batch:
        _flush_1d(batch, last_symbol)
    _save_progress()

    # ── 5m candles ───────────────────────────────────────────────────────
    logger.info("=== Migrating 5m candles prefix=%s ===", prefix_5m)
    batch = []
    last_symbol = ""
    files_processed_5m = 0
    for blob in bucket.list_blobs(prefix=prefix_5m):
        if not blob.name.endswith(".json"):
            continue
        if blob.name in done_paths:
            continue
        if max_files and files_processed_5m >= max_files:
            break
        symbol = _symbol_from_path(blob.name)
        if symbol != last_symbol and batch:
            _flush_5m(batch, last_symbol)
            batch = []
        last_symbol = symbol
        try:
            raw = json.loads(blob.download_as_text())
            candles = raw if isinstance(raw, list) else (raw.get("candles") or raw.get("data") or [])
            rows = [r for c in candles if (r := _candle_to_5m_row(symbol, "", c)) is not None]
            batch.extend(rows)
            if len(batch) >= _BATCH_SIZE:
                _flush_5m(batch, symbol)
                batch = []
            done_paths.add(blob.name)
            files_processed_5m += 1
            if files_processed_5m % 100 == 0:
                _save_progress()
                logger.info("  progress 5m files=%d rows=%d", files_processed_5m, total_rows_5m)
        except Exception:
            logger.exception("  5m_file_failed path=%s", blob.name)
    if batch:
        _flush_5m(batch, last_symbol)
    _save_progress()

    logger.info(
        "=== Candle migration complete 1d_rows=%d 5m_rows=%d dry_run=%s ===",
        total_rows_1d, total_rows_5m, dry_run,
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    parser = argparse.ArgumentParser(description="Migrate GCS candle JSON files to BigQuery")
    parser.add_argument("--project", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--dataset", default="autotrader")
    parser.add_argument("--prefix-1d", default="candles/1d")
    parser.add_argument("--prefix-5m", default="candles/5m")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-files", type=int, default=0, help="0 = unlimited")
    args = parser.parse_args()
    migrate_candles(
        project_id=args.project,
        bucket_name=args.bucket,
        dataset=args.dataset,
        prefix_1d=args.prefix_1d,
        prefix_5m=args.prefix_5m,
        dry_run=args.dry_run,
        max_files=args.max_files,
    )


if __name__ == "__main__":
    main()
