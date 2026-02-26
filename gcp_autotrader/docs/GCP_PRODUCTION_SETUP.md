# Exact Setup and Go-Live Steps (GCP + Sheets + Storage)

This setup is designed for your current sheet-based workflow and low-cost operations.

## 0. Reality check (important)

GCP removes Apps Script execution/runtime limits, but it does **not** remove:

- Groww API limits/quotas
- Broker-side order/risk restrictions
- GCP service quotas (Cloud Run concurrency, Scheduler frequency, API quotas)

Design for rate limiting and retries. The included code already does local throttling and retry handling.

## 1. Create the Google Sheet from your workbook

1. Upload `/Users/vishalrawat/Auto Trading Python GCP/Groww_AutoTrader_v1_ProdReady.xlsx` to Google Drive.
2. Open it in Google Sheets and convert to a native Google Sheet.
3. Copy the Spreadsheet ID from the URL.
4. Share the sheet with the Cloud Run service account email (Editor access).

## 2. Create GCP project and enable APIs

Replace placeholders:

- `PROJECT_ID`
- `REGION` (recommended: `asia-south1`)

Commands:

```bash
gcloud config set project PROJECT_ID
gcloud services enable \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  secretmanager.googleapis.com \
  firestore.googleapis.com \
  cloudscheduler.googleapis.com \
  artifactregistry.googleapis.com \
  storage.googleapis.com \
  sheets.googleapis.com
```

## 3. Create Artifact Registry + GCS bucket + Firestore

```bash
gcloud artifacts repositories create autotrader \
  --repository-format=docker \
  --location=REGION \
  --description="AutoTrader images"

gsutil mb -l REGION gs://PROJECT_ID-autotrader-data

gcloud firestore databases create --location=nam5 --type=firestore-native
```

Notes:

- Firestore region choice depends on your preference; `nam5` is multi-region. Use a regional location if you want lower cost/latency.

## 4. Create service account and IAM

```bash
gcloud iam service-accounts create autotrader-runner \
  --display-name="AutoTrader Cloud Run"

SA_EMAIL="autotrader-runner@PROJECT_ID.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/secretmanager.secretAccessor"

gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/datastore.user"

gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/logging.logWriter"
```

## 5. Create Secret Manager secrets (Groww credentials + job token)

Create once:

```bash
printf "YOUR_GROWW_API_KEY" | gcloud secrets create groww-api-key --data-file=-
printf "YOUR_GROWW_API_SECRET" | gcloud secrets create groww-api-secret --data-file=-
printf "" | gcloud secrets create groww-access-token --data-file=-
printf "" | gcloud secrets create groww-access-token-expiry --data-file=-
openssl rand -hex 32 | tee /tmp/autotrader_job_token.txt
JOB_TOKEN="$(cat /tmp/autotrader_job_token.txt)"
```

If a secret already exists, add a version instead:

```bash
printf "NEW_VALUE" | gcloud secrets versions add groww-api-key --data-file=-
```

## 6. Build and push the container

From `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader`:

```bash
PROJECT_ID="PROJECT_ID"
REGION="REGION"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/autotrader/autotrader:latest"

gcloud builds submit --config cloudbuild.yaml --substitutions _IMAGE="$IMAGE" .
```

## 7. Deploy Cloud Run service

```bash
cd /Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader
bash deploy/deploy_cloud_run.sh PROJECT_ID REGION autotrader "$IMAGE" "YOUR_SPREADSHEET_ID" "PROJECT_ID-autotrader-data"
```

Then set the service account and secrets/env:

```bash
gcloud run services update autotrader \
  --region REGION \
  --service-account "$SA_EMAIL" \
  --set-env-vars "GROWW_API_KEY_SECRET_NAME=groww-api-key,GROWW_API_SECRET_SECRET_NAME=groww-api-secret,GROWW_ACCESS_TOKEN_SECRET_NAME=groww-access-token,GROWW_ACCESS_TOKEN_EXPIRY_SECRET_NAME=groww-access-token-expiry,JOB_TRIGGER_TOKEN=${JOB_TOKEN},PAPER_TRADE=true,MIN_SIGNAL_SCORE=72,CAPITAL=50000,RISK_PER_TRADE=125,MAX_DAILY_LOSS=300,DAILY_PROFIT_TARGET=200,MAX_TRADES_DAY=5,MAX_POSITIONS=3"
```

Get service URL:

```bash
SERVICE_URL="$(gcloud run services describe autotrader --region REGION --format='value(status.url)')"
echo "$SERVICE_URL"
```

## 8. Bootstrap the sheet schema from the new runtime

```bash
curl -X POST "${SERVICE_URL}/jobs/bootstrap-sheets" \
  -H "X-Job-Token: ${JOB_TOKEN}"
```

This creates/repairs missing tabs like:

- `🧾 Universe Instruments`
- `🗄️ Candle Cache`
- `📘 Score Cache 1D`
- `📗 Score Cache 1D Data`
- `📚 History Backfill`
- `🧩 Project Log`

## 9. Dry-run validation (paper mode only)

Optional local validation before Cloud Run (from `gcp_autotrader`):

```bash
python3 -m pip install -r requirements.txt
PYTHONPATH=src python3 -m autotrader.jobs health
```

1. Universe sync:

```bash
curl -X POST "${SERVICE_URL}/jobs/universe-sync" \
  -H "X-Job-Token: ${JOB_TOKEN}"
```

2. Premarket scoring + watchlist build:

```bash
curl -X POST "${SERVICE_URL}/jobs/premarket-precompute" \
  -H "X-Job-Token: ${JOB_TOKEN}"
```

3. Force one scan outside market hours (for testing):

```bash
curl -X POST "${SERVICE_URL}/jobs/scan-once?force=true" \
  -H "X-Job-Token: ${JOB_TOKEN}"
```

Check your sheet tabs for:

- `📋 Watchlist`
- `📡 Live Scanner`
- `🎯 Signals`
- `📦 Orders` (paper entries)
- `💼 Positions` (paper positions)

## 10. Create Cloud Scheduler jobs (IST schedules)

```bash
bash deploy/create_scheduler_jobs.sh PROJECT_ID REGION "$SERVICE_URL" "$JOB_TOKEN" "Asia/Kolkata"
```

Included schedules:

- Daily universe sync
- Premarket precompute every 5 minutes
- Market scan every minute (09:15 to ~15:20 IST)

## 11. Go live safely (staged rollout)

### Stage A: Paper-only (mandatory)

- Keep `PAPER_TRADE=true`
- Run for at least 5 market sessions
- Compare sheet decisions vs expected behavior
- Confirm no duplicate entries, lock contention, or stale data issues

### Stage B: Live-ready validation

- Confirm Groww order payload mapping with small quantity test
- Validate order fill polling and pending reconciliation
- Test stop-loss/exit flows in controlled conditions

### Stage C: Live mode

Update Cloud Run env:

```bash
gcloud run services update autotrader \
  --region REGION \
  --set-env-vars PAPER_TRADE=false
```

Only enable live order execution endpoint calls after validating on small size.

## 12. Fresh-data strategy (important)

To keep data fresh without Apps Script limits:

- Use Cloud Scheduler every 1 minute during market hours for `scan-once`
- Cache candles in GCS and only fetch incremental windows
- Keep Groww request rate low and controlled (code default local throttle)
- Scale out carefully only after adding distributed rate limiting (Cloud Tasks)

## 13. Monitoring and alerts (recommended before live)

1. Create log-based metrics for:
   - `ERROR`
   - `PENDING_RECON`
   - `Groww auth`
2. Add Cloud Monitoring alerts to email/SMS.
3. Set Cloud Run error alerting and uptime checks on `/healthz`.

## 14. What is already ported vs next work

Ported now:

- Core strategy indicators/scoring/risk sizing
- Universe sync/scoring/watchlist generation
- Sheets/GCS/Firestore/Secret Manager adapters
- Scan loop with paper-safe order path
- Cloud Run + Scheduler deployment skeleton

Next hardening recommended before fully automated live trading:

- Full exit/trailing SL parity with your `OrderEngine.gs`
- History backfill scheduler jobs parity with `DataEngine.gs`
- Distributed rate limiting (Cloud Tasks)
- End-to-end integration tests against broker responses
