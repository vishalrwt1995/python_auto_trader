#!/usr/bin/env python3
"""M5 — Poll Upstox option chain for a set of instruments, compute
OptionMetrics, and write to Firestore `option_metrics/{symbol}` (always)
plus BigQuery `option_metrics_history` (when --bq-archive is set).

Designed to run as a scheduled Cloud Run Job (every 5 min during market
hours). Explicitly NOT a long-running service — one invocation pulls a
snapshot and exits. Two consecutive snapshots in Firestore = trend.

Why archive to BQ
-----------------
Firestore holds only the LATEST snapshot per symbol — fine for live
scoring (`score_signal` Layer-7), useless for backtests that need the
PCR live saw at scan tick T weeks ago. The BQ writer captures every poll
so pure-replay can read back the exact PCR signal live consumed,
closing the ~5%-of-100pt scoring approximation gap.

Usage:
    python scripts/redesign/poll_option_chain.py \
        --symbols "NIFTY:NSE_INDEX|Nifty 50,BANKNIFTY:NSE_INDEX|Nifty Bank" \
        --expiry 2025-11-27 --bq-archive

The `--symbols` arg is a comma-separated list of `display:instrument_key`.
Pass `--bq-archive` (or set `OPTION_METRICS_BQ_ARCHIVE=1`) to also write
to BigQuery — recommended for production runs so history accumulates.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", required=True,
                    help="comma-separated display:instrument_key pairs")
    ap.add_argument("--expiry", required=True, help="Expiry date YYYY-MM-DD")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--bq-archive", action="store_true",
                    default=os.getenv("OPTION_METRICS_BQ_ARCHIVE", "").lower() in {"1", "true", "yes"},
                    help="also append a row to BigQuery `option_metrics_history`")
    ap.add_argument("--bq-project", default=os.getenv("GCP_PROJECT_ID", "grow-profit-machine"))
    ap.add_argument("--bq-dataset", default=os.getenv("BQ_DATASET", "autotrader"))
    args = ap.parse_args()

    # Imports deferred so --help works without GCP deps.
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
    from autotrader.adapters.firestore_state import FirestoreStateStore
    from autotrader.adapters.upstox_client import UpstoxClient
    from autotrader.domain.option_analytics import compute_metrics, _g

    upstox = UpstoxClient()
    state = FirestoreStateStore(os.getenv("GCP_PROJECT_ID", "grow-profit-machine"))

    # Lazy BQ client — only built when archival is on, and shared across
    # all (symbol, expiry) pairs so we make one streaming insert at the end.
    bq_client = None
    bq_table_ref = None
    bq_rows: list[dict] = []
    if args.bq_archive and not args.dry_run:
        try:
            from google.cloud import bigquery  # type: ignore[import-untyped]
            bq_client = bigquery.Client(project=args.bq_project)
            bq_table_ref = f"{args.bq_project}.{args.bq_dataset}.option_metrics_history"
        except Exception as e:
            logging.warning("bq_archive_disabled err=%s — proceeding with Firestore-only", e)
            bq_client = None

    pairs = [p.strip() for p in args.symbols.split(",") if p.strip()]
    wrote = 0
    for pair in pairs:
        try:
            display, ik = pair.split(":", 1)
        except ValueError:
            logging.warning("bad pair=%s expected display:instrument_key", pair)
            continue
        chain = upstox.get_option_chain(ik, args.expiry)
        if not chain:
            logging.warning("empty_chain display=%s ik=%s expiry=%s", display, ik, args.expiry)
            continue

        # Approximate spot from the row closest to itself — many rows
        # include `underlying_spot_price`. If not, fall back to the
        # mid of ATM straddle's LTPs.
        spot = 0.0
        for r in chain:
            for key in ("underlying_spot_price", "spot_price"):
                v = r.get(key) or 0
                try:
                    spot = max(spot, float(v or 0))
                except Exception:
                    pass
            if spot > 0:
                break

        m = compute_metrics(chain, spot=spot)
        now_utc = datetime.now(timezone.utc)
        display_key = display.strip().upper()
        payload = {
            "symbol": display_key,
            "instrument_key": ik.strip(),
            "expiry": args.expiry,
            "spot": round(spot, 2),
            "max_pain_strike": m.max_pain_strike,
            "put_call_ratio": m.put_call_ratio,
            "oi_change_pcr": m.oi_change_pcr,
            "iv_skew": m.iv_skew,
            "n_rows": m.n_rows,
            "ts_epoch": time.time(),
            "ts_iso": now_utc.isoformat(),
        }

        if args.dry_run:
            print(payload)
        else:
            state.set_json("option_metrics", display_key, payload)
            wrote += 1

        if bq_client is not None:
            # Compute aggregate OI sums for the BQ row — these aren't on
            # OptionMetrics but are cheap to derive and let backtests build
            # alternate PCR variants without re-pulling the chain.
            pe_oi = sum(_g(r, "put_options", "market_data", "oi") for r in chain)
            ce_oi = sum(_g(r, "call_options", "market_data", "oi") for r in chain)
            bq_rows.append({
                "asof_ts": now_utc.isoformat(),
                "run_date": now_utc.date().isoformat(),
                "underlying": display_key,
                "instrument_key": ik.strip(),
                "expiry": args.expiry,
                "spot": round(spot, 2) if spot else None,
                "pe_oi_total": pe_oi,
                "ce_oi_total": ce_oi,
                "put_call_ratio": m.put_call_ratio,
                "oi_change_pcr": m.oi_change_pcr,
                "max_pain_strike": m.max_pain_strike,
                "iv_skew": m.iv_skew,
                "n_rows": m.n_rows,
            })

    if bq_client is not None and bq_rows:
        # `insert_rows_json` returns a list of error dicts (empty on success).
        errors = bq_client.insert_rows_json(bq_table_ref, bq_rows)
        if errors:
            logging.warning("bq_archive_partial_failure errors=%s", errors)
        else:
            logging.info("bq_archive_wrote n=%d table=%s", len(bq_rows), bq_table_ref)

    logging.info("done pairs=%d wrote=%d bq_rows=%d", len(pairs), wrote, len(bq_rows))
    return 0


if __name__ == "__main__":
    sys.exit(main())
