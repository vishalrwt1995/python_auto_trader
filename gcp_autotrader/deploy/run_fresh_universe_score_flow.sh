#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${SERVICE_URL:-}" || -z "${JOB_TOKEN:-}" ]]; then
  echo "ERROR: export SERVICE_URL and JOB_TOKEN first."
  exit 1
fi

BACKFILL_MAX_PASSES="${BACKFILL_MAX_PASSES:-20}"
CLOSE_MAX_PASSES="${CLOSE_MAX_PASSES:-20}"
SLEEP_SECONDS="${SLEEP_SECONDS:-45}"
AUTO_REQUEST_TOKEN="${AUTO_REQUEST_TOKEN:-false}"

post_job() {
  local path="$1"
  curl --fail-with-body -sS -X POST "${SERVICE_URL}${path}" \
    -H "X-Job-Token: ${JOB_TOKEN}" \
    -H "Content-Type: application/json" \
    -d '{}'
}

if [[ "${AUTO_REQUEST_TOKEN}" == "true" ]]; then
  echo "== 0) Request Upstox token =="
  post_job "/jobs/upstox-token-request" || true
  echo
fi

echo "== 1) Universe V2 rebuild (replace=true) =="
post_job "/jobs/universe-v2-refresh?build_limit=0&replace=true&candle_api_cap=600&run_full_backfill=true&write_v2_eligibility=false"
echo

echo "== 1b) Backfill completion loop =="
BACKFILL_DONE=0
for i in $(seq 1 "${BACKFILL_MAX_PASSES}"); do
  echo "[backfill pass ${i}/${BACKFILL_MAX_PASSES}]"
  RESP="$(post_job "/jobs/score-cache-backfill-full?api_cap=600&lookback_days=9500&min_bars=320" || true)"
  echo "${RESP}"

  if [[ "${RESP}" == *'"skipped":"lock_busy"'* ]]; then
    sleep 60
    continue
  fi
  if [[ "${RESP}" == *'"prefillComplete":true'* ]]; then
    BACKFILL_DONE=1
    break
  fi
  sleep "${SLEEP_SECONDS}"
done

if [[ "${BACKFILL_DONE}" -ne 1 ]]; then
  echo "ERROR: backfill not complete in ${BACKFILL_MAX_PASSES} passes"
  exit 1
fi

echo
echo "== 2) Last-candle close update completion loop =="
CLOSE_DONE=0
for i in $(seq 1 "${CLOSE_MAX_PASSES}"); do
  echo "[close pass ${i}/${CLOSE_MAX_PASSES}]"
  RESP="$(post_job "/jobs/score-cache-update-close?api_cap=600&lookback_days=700&min_bars=320" || true)"
  echo "${RESP}"

  if [[ "${RESP}" == *'"skipped":"lock_busy"'* ]]; then
    sleep 60
    continue
  fi
  if [[ "${RESP}" == *'"prefillComplete":true'* ]]; then
    CLOSE_DONE=1
    break
  fi
  sleep "${SLEEP_SECONDS}"
done

if [[ "${CLOSE_DONE}" -ne 1 ]]; then
  echo "ERROR: close-update not complete in ${CLOSE_MAX_PASSES} passes"
  exit 1
fi

echo
echo "== 3) Score refresh (v1 + v2) =="
post_job "/jobs/score-refresh?api_cap=0&cache_only=true&require_fresh_cache=true&fresh_hours=0"
echo

echo "== 4) Universe V2 audit =="
post_job "/jobs/universe-v2-audit"
echo
echo "DONE"
