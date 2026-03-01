# GCP AutoTrader (Groww + Sheets + GCS)

Production-oriented Python runtime to migrate the existing Apps Script trading bot to:

- Google Cloud Run (compute + scheduler target)
- Google Sheets (operator UI / config / logs)
- Google Cloud Storage (historical + cache data)
- Firestore (runtime state, locks, idempotency, pending orders)
- Secret Manager (Groww credentials/tokens)

This repo keeps your original Apps Script code intact and adds a parallel GCP stack for safe migration.

Start with:

1. `docs/GCP_PRODUCTION_SETUP.md`
2. `python3 -m pip install -r requirements.txt`
3. `PYTHONPATH=src python3 -m autotrader.jobs health`
4. `PYTHONPATH=src python3 -m autotrader.jobs bootstrap-sheets`

## Universe V2 Job

Universe V2 is a single idempotent pipeline for:
- raw Upstox snapshot fetch with last-good fallback
- canonical master upsert (ISIN-first dedupe, NSE primary preference)
- 1D candle refresh/backfill to Jan-2000 horizon (cache keyed by Upstox `instrument_key` for symbol-collision safety)
- tradability stats + Swing/Intraday eligibility from `⚙️ Config`

Run locally:

- `PYTHONPATH=src python3 -m autotrader.jobs universe-v2-refresh --build-limit 0 --replace false --candle-api-cap 600 --run-full-backfill true`

Run on Cloud Run (HTTP):

- `POST /jobs/universe-v2-refresh?build_limit=0&replace=false&candle_api_cap=600&run_full_backfill=true`
- Requires `X-Job-Token` header.

Scheduler helper:

- `deploy/create_scheduler_jobs.sh` now points `autotrader-universe-refresh-append-backfill-0615` to `/jobs/universe-v2-refresh`.

Clean rebuild (no visual duplicates):

- `POST /jobs/universe-v2-refresh?build_limit=0&replace=true&candle_api_cap=600&run_full_backfill=true`
- Then run audit:
  - `POST /jobs/universe-v2-audit`
