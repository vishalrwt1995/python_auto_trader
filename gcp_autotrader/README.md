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
