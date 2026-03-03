#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${SERVICE_URL:-}" || -z "${JOB_TOKEN:-}" ]]; then
  echo "ERROR: export SERVICE_URL and JOB_TOKEN first."
  exit 1
fi

INTRADAY_MAX_PASSES="${INTRADAY_MAX_PASSES:-30}"
INTRADAY_API_CAP="${INTRADAY_API_CAP:-1200}"
LOOKBACK_TRADING_DAYS="${LOOKBACK_TRADING_DAYS:-60}"
SLEEP_SECONDS="${SLEEP_SECONDS:-45}"

post_job() {
  local path="$1"
  curl --fail-with-body -sS -X POST "${SERVICE_URL}${path}" \
    -H "X-Job-Token: ${JOB_TOKEN}" \
    -H "Content-Type: application/json" \
    -d '{}'
}

echo "== Intraday 5m full baseline backfill loop =="
DONE=0
CONSEC_FAILS=0
for i in $(seq 1 "${INTRADAY_MAX_PASSES}"); do
  echo "[intraday full pass ${i}/${INTRADAY_MAX_PASSES}]"
  RESP="$(post_job "/jobs/intraday-cache-backfill-full?api_cap=${INTRADAY_API_CAP}&lookback_trading_days=${LOOKBACK_TRADING_DAYS}" || true)"
  echo "${RESP}"

  if [[ -z "${RESP}" || "${RESP}" == "Internal Server Error" ]]; then
    CONSEC_FAILS=$((CONSEC_FAILS+1))
  else
    CONSEC_FAILS=0
  fi
  if [[ "${CONSEC_FAILS}" -ge 3 ]]; then
    echo "ERROR: received ${CONSEC_FAILS} consecutive server errors."
    echo "Check /jobs/bootstrap-sheets and Cloud Run env GOOGLE_SHEETS_SPREADSHEET_ID/service-account access before retrying."
    exit 1
  fi

  if [[ "${RESP}" == *'"skipped":"lock_busy"'* ]]; then
    sleep 60
    continue
  fi
  if [[ "${RESP}" == *'"prefillComplete":true'* ]]; then
    DONE=1
    break
  fi
  sleep "${SLEEP_SECONDS}"
done

if [[ "${DONE}" -ne 1 ]]; then
  echo "ERROR: intraday full baseline backfill not complete in ${INTRADAY_MAX_PASSES} passes"
  exit 1
fi

echo "Intraday full baseline backfill complete."
