#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${SERVICE_URL:-}" || -z "${JOB_TOKEN:-}" ]]; then
  echo "ERROR: export SERVICE_URL and JOB_TOKEN first."
  exit 1
fi

CLOSE_MAX_PASSES="${CLOSE_MAX_PASSES:-20}"
SLEEP_SECONDS="${SLEEP_SECONDS:-45}"
CLOSE_API_CAP="${CLOSE_API_CAP:-600}"

post_job() {
  local path="$1"
  curl --fail-with-body -sS -X POST "${SERVICE_URL}${path}" \
    -H "X-Job-Token: ${JOB_TOKEN}" \
    -H "Content-Type: application/json" \
    -d '{}'
}

echo "== Last-candle close update loop =="
DONE=0
for i in $(seq 1 "${CLOSE_MAX_PASSES}"); do
  echo "[close pass ${i}/${CLOSE_MAX_PASSES}]"
  RESP="$(post_job "/jobs/score-cache-update-close?api_cap=${CLOSE_API_CAP}&lookback_days=700&min_bars=320" || true)"
  echo "${RESP}"

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
  echo "ERROR: last-candle update not complete in ${CLOSE_MAX_PASSES} passes"
  exit 1
fi

echo "Last-candle update complete."

