#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${SERVICE_URL:-}" || -z "${JOB_TOKEN:-}" ]]; then
  echo "ERROR: export SERVICE_URL and JOB_TOKEN first."
  exit 1
fi

BACKFILL_MAX_PASSES="${BACKFILL_MAX_PASSES:-20}"
SLEEP_SECONDS="${SLEEP_SECONDS:-45}"
REPLACE_MODE="${REPLACE_MODE:-false}"  # true = rebuild from scratch, false = append mode (production default)
CANDLE_API_CAP="${CANDLE_API_CAP:-600}"
INTRADAY_API_CAP="${INTRADAY_API_CAP:-1200}"
INTRADAY_LOOKBACK_TRADING_DAYS="${INTRADAY_LOOKBACK_TRADING_DAYS:-60}"

post_job() {
  local path="$1"
  curl --fail-with-body -sS -X POST "${SERVICE_URL}${path}" \
    -H "X-Job-Token: ${JOB_TOKEN}" \
    -H "Content-Type: application/json" \
    -d '{}'
}

echo "== Universe V2 refresh (replace=${REPLACE_MODE}) =="
post_job "/jobs/universe-v2-refresh?build_limit=0&replace=${REPLACE_MODE}&candle_api_cap=${CANDLE_API_CAP}&run_full_backfill=true&write_v2_eligibility=false&run_intraday_appended_backfill=true&intraday_api_cap=${INTRADAY_API_CAP}&intraday_lookback_trading_days=${INTRADAY_LOOKBACK_TRADING_DAYS}"
echo

echo "== Backfill loop until prefillComplete=true =="
DONE=0
for i in $(seq 1 "${BACKFILL_MAX_PASSES}"); do
  echo "[backfill pass ${i}/${BACKFILL_MAX_PASSES}]"
  RESP="$(post_job "/jobs/score-cache-backfill-full?api_cap=${CANDLE_API_CAP}&lookback_days=9500&min_bars=320" || true)"
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
  echo "ERROR: universe backfill not complete in ${BACKFILL_MAX_PASSES} passes"
  exit 1
fi

echo "Universe + backfill complete."
