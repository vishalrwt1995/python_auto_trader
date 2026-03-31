#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 6 ]]; then
  echo "Usage: $0 <PROJECT_ID> <REGION> <SERVICE_NAME> <IMAGE> <SHEET_ID> <BUCKET>"
  echo "Requires env: JOB_TRIGGER_TOKEN"
  exit 1
fi

PROJECT_ID="$1"
REGION="$2"
SERVICE_NAME="$3"
IMAGE="$4"
SHEET_ID="$5"
BUCKET="$6"

if [[ -z "${JOB_TRIGGER_TOKEN:-}" ]]; then
  echo "ERROR: JOB_TRIGGER_TOKEN env var is required."
  echo "Export it first, then rerun deploy."
  exit 1
fi

if [[ -z "$SHEET_ID" ]]; then
  echo "ERROR: SHEET_ID argument is empty."
  echo "Pass a valid Google Sheet ID as arg #5."
  exit 1
fi

if [[ -z "$BUCKET" ]]; then
  echo "ERROR: BUCKET argument is empty."
  echo "Pass a valid GCS bucket as arg #6."
  exit 1
fi

CLOUD_RUN_CPU="${CLOUD_RUN_CPU:-1}"
CLOUD_RUN_MEMORY="${CLOUD_RUN_MEMORY:-4Gi}"
CLOUD_RUN_MIN_INSTANCES="${CLOUD_RUN_MIN_INSTANCES:-0}"
CLOUD_RUN_MAX_INSTANCES="${CLOUD_RUN_MAX_INSTANCES:-3}"
CLOUD_RUN_TIMEOUT="${CLOUD_RUN_TIMEOUT:-3600}"
CLOUD_RUN_CONCURRENCY="${CLOUD_RUN_CONCURRENCY:-1}"

UPSTOX_CLIENT_ID_SECRET_NAME="${UPSTOX_CLIENT_ID_SECRET_NAME:-upstox-client-id}"
UPSTOX_CLIENT_SECRET_SECRET_NAME="${UPSTOX_CLIENT_SECRET_SECRET_NAME:-upstox-client-secret}"
UPSTOX_ACCESS_TOKEN_SECRET_NAME="${UPSTOX_ACCESS_TOKEN_SECRET_NAME:-upstox-access-token}"
UPSTOX_ACCESS_TOKEN_EXPIRY_SECRET_NAME="${UPSTOX_ACCESS_TOKEN_EXPIRY_SECRET_NAME:-upstox-access-token-expiry}"
UPSTOX_AUTH_CODE_SECRET_NAME="${UPSTOX_AUTH_CODE_SECRET_NAME:-upstox-auth-code}"

ENV_VARS=(
  "GCP_PROJECT_ID=$PROJECT_ID"
  "GCP_REGION=$REGION"
  "GOOGLE_SHEETS_SPREADSHEET_ID=$SHEET_ID"
  "GCS_BUCKET=$BUCKET"
  "FIRESTORE_DATABASE=${FIRESTORE_DATABASE:-(default)}"
  "UPSTOX_API_V2_HOST=${UPSTOX_API_V2_HOST:-https://api.upstox.com/v2}"
  "UPSTOX_API_V3_HOST=${UPSTOX_API_V3_HOST:-https://api.upstox.com/v3}"
  "UPSTOX_REQUESTS_PER_SECOND=${UPSTOX_REQUESTS_PER_SECOND:-50}"
  "UPSTOX_MAX_PER_MINUTE=${UPSTOX_MAX_PER_MINUTE:-500}"
  "UPSTOX_MAX_PER_30MIN=${UPSTOX_MAX_PER_30MIN:-2000}"
  "UPSTOX_MAX_RETRIES=${UPSTOX_MAX_RETRIES:-4}"
  "UPSTOX_CLIENT_ID_SECRET_NAME=$UPSTOX_CLIENT_ID_SECRET_NAME"
  "UPSTOX_CLIENT_SECRET_SECRET_NAME=$UPSTOX_CLIENT_SECRET_SECRET_NAME"
  "UPSTOX_ACCESS_TOKEN_SECRET_NAME=$UPSTOX_ACCESS_TOKEN_SECRET_NAME"
  "UPSTOX_ACCESS_TOKEN_EXPIRY_SECRET_NAME=$UPSTOX_ACCESS_TOKEN_EXPIRY_SECRET_NAME"
  "UPSTOX_AUTH_CODE_SECRET_NAME=$UPSTOX_AUTH_CODE_SECRET_NAME"
  "UPSTOX_NIFTY50_INSTRUMENT_KEY=${UPSTOX_NIFTY50_INSTRUMENT_KEY:-NSE_INDEX|Nifty 50}"
  "UPSTOX_INDIA_VIX_INSTRUMENT_KEY=${UPSTOX_INDIA_VIX_INSTRUMENT_KEY:-NSE_INDEX|India VIX}"
  "UPSTOX_PCR_UNDERLYING_INSTRUMENT_KEY=${UPSTOX_PCR_UNDERLYING_INSTRUMENT_KEY:-NSE_INDEX|Nifty 50}"
  "PAPER_TRADE=${PAPER_TRADE:-true}"
  "JOB_TRIGGER_TOKEN=$JOB_TRIGGER_TOKEN"
  "LOG_LEVEL=${LOG_LEVEL:-INFO}"
  "TZ=${TZ:-Asia/Kolkata}"
)

if [[ -n "${UPSTOX_REDIRECT_URI:-}" ]]; then
  ENV_VARS+=("UPSTOX_REDIRECT_URI=$UPSTOX_REDIRECT_URI")
fi
if [[ -n "${UPSTOX_NOTIFIER_SHARED_SECRET:-}" ]]; then
  ENV_VARS+=("UPSTOX_NOTIFIER_SHARED_SECRET=$UPSTOX_NOTIFIER_SHARED_SECRET")
fi

ENV_VARS_CSV="$(IFS=,; echo "${ENV_VARS[*]}")"

gcloud run deploy "$SERVICE_NAME" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --image "$IMAGE" \
  --platform managed \
  --allow-unauthenticated \
  --port 8080 \
  --cpu "$CLOUD_RUN_CPU" \
  --memory "$CLOUD_RUN_MEMORY" \
  --min-instances "$CLOUD_RUN_MIN_INSTANCES" \
  --max-instances "$CLOUD_RUN_MAX_INSTANCES" \
  --timeout "$CLOUD_RUN_TIMEOUT" \
  --concurrency "$CLOUD_RUN_CONCURRENCY" \
  --update-env-vars "$ENV_VARS_CSV"

echo "Cloud Run service deployed."


# ---------------------------------------------------------------------------
# ws-monitor: deploy as a separate always-on Cloud Run service
# Usage: WS_MONITOR_IMAGE=gcr.io/... bash deploy_cloud_run.sh ... && deploy_ws_monitor
# ---------------------------------------------------------------------------

deploy_ws_monitor() {
  local WS_IMAGE="${WS_MONITOR_IMAGE:?WS_MONITOR_IMAGE env var required}"
  local WS_SERVICE="autotrader-ws-monitor"

  WS_ENV_VARS=(
    "GCP_PROJECT_ID=$PROJECT_ID"
    "GCP_REGION=$REGION"
    "FIRESTORE_DATABASE=${FIRESTORE_DATABASE:-(default)}"
    "UPSTOX_ACCESS_TOKEN_SECRET_NAME=$UPSTOX_ACCESS_TOKEN_SECRET_NAME"
    "LOG_LEVEL=${LOG_LEVEL:-INFO}"
    "TZ=${TZ:-Asia/Kolkata}"
  )
  WS_ENV_CSV="$(IFS=,; echo "${WS_ENV_VARS[*]}")"

  gcloud run deploy "$WS_SERVICE" \
    --project "$PROJECT_ID" \
    --region "$REGION" \
    --image "$WS_IMAGE" \
    --platform managed \
    --no-allow-unauthenticated \
    --cpu 1 \
    --memory 512Mi \
    --min-instances 1 \
    --max-instances 1 \
    --timeout 86400 \
    --concurrency 1 \
    --update-env-vars "$WS_ENV_CSV"

  echo "ws-monitor deployed as $WS_SERVICE (min-instances=1)"
}

if [[ -n "${WS_MONITOR_IMAGE:-}" ]]; then
  deploy_ws_monitor
fi
