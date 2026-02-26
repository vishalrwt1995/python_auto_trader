# Architecture (Low Cost + Robust)

## Core design

- `Cloud Run service` hosts authenticated job endpoints (`/jobs/*`) and health checks.
- `Cloud Scheduler` triggers universe sync, premarket scoring, and live scans.
- `Google Sheets` remains the control panel and audit surface (config, watchlist, scans, signals, orders, positions, logs).
- `Google Cloud Storage` stores candle caches and historical JSON files (replaces Apps Script cache + Drive file storage).
- `Firestore` stores runtime locks/cursors/idempotency/pending orders (replaces Script Properties + LockService).
- `Secret Manager` stores Groww API credentials and refreshed access tokens.

## Why this is low cost

- Cloud Run scales to zero (`min-instances=0`)
- Cloud Scheduler is inexpensive for cron-style orchestration
- GCS is cheap for JSON history/cache objects
- Firestore usage is small for key-value runtime state
- Sheets remains your UI, so no frontend/dashboard cost

## Why this is more robust than Apps Script

- No Apps Script execution time ceilings for every run loop
- Better observability (Cloud Logging/Monitoring)
- Safer secret handling (Secret Manager)
- Real locking/idempotency state outside spreadsheet cells
- Runtime can be versioned/deployed with rollback

## Service boundaries mapped from current Apps Script

- `Universe.gs` -> `services/universe_service.py`
- `Indicators.gs` -> `domain/indicators.py`
- `ScoreEngine.gs` -> `domain/scoring.py`
- `RiskEngine.gs` -> `domain/risk.py`
- `MasterRunner.gs` -> `services/trading_service.py` + Scheduler
- `Config.gs/Auth.gs` -> `adapters/sheets_repository.py` + `adapters/groww_client.py` + Secret Manager
- `DataEngine.gs` -> `adapters/gcs_store.py` + future history backfill jobs
- `OrderEngine.gs` -> `services/order_service.py` (paper + live entry path, pending reconciliation scaffold)

## Recommended production hardening (next phase)

- Add Cloud Tasks for distributed Groww API rate limiting
- Add BigQuery for backtests/feature store (optional)
- Add Pub/Sub for decoupled event-driven order reconciliation
- Add integration tests against Groww sandbox/paper environment before enabling live orders

