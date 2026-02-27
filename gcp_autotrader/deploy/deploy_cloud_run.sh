#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 6 ]]; then
  echo "Usage: $0 <PROJECT_ID> <REGION> <SERVICE_NAME> <IMAGE> <SHEET_ID> <BUCKET>"
  exit 1
fi

PROJECT_ID="$1"
REGION="$2"
SERVICE_NAME="$3"
IMAGE="$4"
SHEET_ID="$5"
BUCKET="$6"

gcloud run deploy "$SERVICE_NAME" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --image "$IMAGE" \
  --platform managed \
  --allow-unauthenticated \
  --port 8080 \
  --cpu 1 \
  --memory 1Gi \
  --min-instances 0 \
  --max-instances 3 \
  --timeout 3600 \
  --concurrency 20 \
  --set-env-vars "GCP_PROJECT_ID=$PROJECT_ID,GCP_REGION=$REGION,GOOGLE_SHEETS_SPREADSHEET_ID=$SHEET_ID,GCS_BUCKET=$BUCKET,FIRESTORE_DATABASE=(default),UPSTOX_API_V2_HOST=https://api.upstox.com/v2,UPSTOX_API_V3_HOST=https://api.upstox.com/v3,UPSTOX_REQUESTS_PER_SECOND=50,UPSTOX_MAX_PER_MINUTE=500,UPSTOX_MAX_PER_30MIN=2000,GROWW_API_HOST=https://api.groww.in,LOG_LEVEL=INFO,TZ=Asia/Kolkata"

echo "Cloud Run service deployed. Set remaining env vars and secrets next."
