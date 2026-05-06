#!/usr/bin/env python3
"""Backfill BQ `candles_1d` from the canonical GCS cache.

Live system writes 1d candles to GCS (`cache/score_1d/{exchange}/{segment}/{symbol}.json`)
as the source of truth, with a best-effort dual-write to BigQuery `candles_1d`. Over time
the dual-write fell behind for a chunk of the universe — this script reconciles them.

Idempotent: queries existing `(symbol, trade_date)` pairs in BQ and only inserts the
missing rows. Safe to re-run.

Examples
--------
    # Backfill the full universe
    python scripts/redesign/backfill_1d_to_bq.py

    # Backfill only symbols that have traded in the last 90 days (fastest path
    # to unblock the backtester's daily-bias gate)
    python scripts/redesign/backfill_1d_to_bq.py --traded-only

    # Specific symbols
    python scripts/redesign/backfill_1d_to_bq.py --symbols RELIANCE,SBIN,INFY

    # Dry run — print what would happen, write nothing
    python scripts/redesign/backfill_1d_to_bq.py --traded-only --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path


def _setup_path() -> None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    log = logging.getLogger("backfill_1d")

    ap = argparse.ArgumentParser(prog="backfill-1d-to-bq",
                                 description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--project", default="grow-profit-machine")
    ap.add_argument("--dataset", default="autotrader")
    ap.add_argument("--bucket", default="grow-profit-machine-autotrader-data")
    ap.add_argument("--gcs-prefix", default="cache/score_1d/",
                    help="GCS prefix to scan for daily candle JSON files")
    ap.add_argument("--symbols", default=None,
                    help="comma-separated list of symbols to backfill (overrides --traded-only)")
    ap.add_argument("--traded-only", action="store_true",
                    help="only backfill symbols that traded in the last N days (see --traded-window)")
    ap.add_argument("--traded-window", type=int, default=90,
                    help="days to look back for --traded-only (default 90)")
    ap.add_argument("--batch-size", type=int, default=2000,
                    help="rows per BQ insert batch (default 2000)")
    ap.add_argument("--dry-run", action="store_true",
                    help="print plan + counts but don't write")
    args = ap.parse_args(argv)

    _setup_path()

    from autotrader.adapters.bigquery_client import BigQueryClient
    from autotrader.adapters.gcs_store import GoogleCloudStorageStore

    bq = BigQueryClient(project_id=args.project, dataset=args.dataset)
    gcs = GoogleCloudStorageStore(bucket_name=args.bucket)

    # ── 1. Resolve symbol set ────────────────────────────────────────────
    if args.symbols:
        symbol_filter: set[str] | None = {
            s.strip().upper() for s in args.symbols.split(",") if s.strip()
        }
        log.info("symbol_filter explicit count=%d", len(symbol_filter))
    elif args.traded_only:
        sql = (
            f"SELECT DISTINCT symbol FROM `{args.project}.{args.dataset}.trades` "
            f"WHERE trade_date >= DATE_SUB(CURRENT_DATE('Asia/Kolkata'), "
            f"INTERVAL {int(args.traded_window)} DAY)"
        )
        traded_rows = bq.query(sql)
        symbol_filter = {str(r["symbol"]).upper() for r in traded_rows if r.get("symbol")}
        log.info("symbol_filter traded_only window=%dd count=%d",
                 args.traded_window, len(symbol_filter))
    else:
        symbol_filter = None
        log.info("symbol_filter all (no filter)")

    # ── 2. List GCS paths ────────────────────────────────────────────────
    log.info("gcs_list prefix=%s", args.gcs_prefix)
    all_paths = [p for p in gcs.list_paths(args.gcs_prefix) if p.endswith(".json")]
    log.info("gcs_list found=%d", len(all_paths))

    # Path: cache/score_1d/{exchange}/{segment}/{symbol}.json
    # We need exchange + segment for the BQ row.
    todo: list[tuple[str, str, str, str]] = []  # (path, symbol, exchange, segment)
    for p in all_paths:
        parts = p.split("/")
        if len(parts) < 4:
            continue
        symbol = parts[-1].replace(".json", "").upper()
        segment = parts[-2].upper()
        exchange = parts[-3].upper()
        if symbol_filter is not None and symbol not in symbol_filter:
            continue
        todo.append((p, symbol, exchange, segment))
    log.info("filtered_paths count=%d", len(todo))

    if not todo:
        log.warning("no_paths_to_backfill — exiting")
        return 0

    # ── 3. Pre-load existing (symbol, trade_date) keys for the filter set ──
    existing_keys: set[tuple[str, str]] = set()
    if symbol_filter is None:
        # Full universe — too expensive to list; we'll skip per-symbol.
        # In this mode we rely on per-symbol existence checks instead.
        log.info("existing_keys: deferred (full-universe mode)")
    else:
        sym_list = "', '".join(sorted(symbol_filter))
        sql = (
            f"SELECT symbol, CAST(trade_date AS STRING) AS trade_date "
            f"FROM `{args.project}.{args.dataset}.candles_1d` "
            f"WHERE symbol IN ('{sym_list}')"
        )
        rows = bq.query(sql)
        for r in rows:
            existing_keys.add((str(r["symbol"]).upper(), str(r["trade_date"])))
        log.info("existing_keys preloaded=%d", len(existing_keys))

    # ── 4. Walk paths, build rows, insert in batches ────────────────────
    total_seen = 0
    total_skipped_existing = 0
    total_inserted = 0
    total_paths_done = 0
    total_paths_empty = 0
    total_paths_failed = 0

    pending: list[dict] = []

    def flush() -> int:
        nonlocal pending
        if not pending:
            return 0
        n = len(pending)
        if not args.dry_run:
            bq.insert_candles_1d_batch(pending)
        pending = []
        return n

    for idx, (path, symbol, exchange, segment) in enumerate(todo, 1):
        try:
            candles = gcs.read_candles(path)
        except Exception:
            log.warning("gcs_read_failed path=%s", path, exc_info=True)
            total_paths_failed += 1
            continue
        if not candles:
            total_paths_empty += 1
            continue

        # Per-symbol existence check (full-universe mode)
        if symbol_filter is None:
            sql = (
                f"SELECT CAST(trade_date AS STRING) AS trade_date "
                f"FROM `{args.project}.{args.dataset}.candles_1d` "
                f"WHERE symbol = '{symbol}'"
            )
            rows = bq.query(sql)
            existing_for_sym = {str(r["trade_date"]) for r in rows}
        else:
            existing_for_sym = {td for (s, td) in existing_keys if s == symbol}

        sym_inserted = 0
        sym_skipped = 0
        for c in candles:
            if not isinstance(c, (list, tuple)) or len(c) < 6:
                continue
            total_seen += 1
            ts_str = str(c[0])
            trade_date = ts_str[:10] if len(ts_str) >= 10 else ts_str
            if (symbol, trade_date) in existing_keys or trade_date in existing_for_sym:
                total_skipped_existing += 1
                sym_skipped += 1
                continue
            try:
                pending.append({
                    "trade_date": trade_date,
                    "symbol": symbol,
                    "exchange": exchange,
                    "segment": segment,
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume": float(c[5]),
                    "instrument_key": "",
                })
            except (TypeError, ValueError):
                continue
            sym_inserted += 1
            if len(pending) >= args.batch_size:
                flushed = flush()
                total_inserted += flushed

        total_paths_done += 1
        if idx % 100 == 0 or sym_inserted >= 100:
            log.info("progress symbol=%s done=%d/%d sym_new=%d sym_skipped=%d batch_pending=%d",
                     symbol, idx, len(todo), sym_inserted, sym_skipped, len(pending))

    final = flush()
    total_inserted += final

    log.info("─" * 64)
    log.info("BACKFILL 1d → BQ %s", "(DRY RUN)" if args.dry_run else "DONE")
    log.info("paths_seen=%d done=%d empty=%d failed=%d",
             len(todo), total_paths_done, total_paths_empty, total_paths_failed)
    log.info("rows_seen=%d skipped_existing=%d inserted=%d",
             total_seen, total_skipped_existing, total_inserted)
    log.info("─" * 64)
    return 0


if __name__ == "__main__":
    sys.exit(main())
