#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${SERVICE_URL:-}" || -z "${JOB_TOKEN:-}" ]]; then
  echo "ERROR: export SERVICE_URL and JOB_TOKEN first."
  exit 1
fi

CLOSE_MAX_PASSES="${CLOSE_MAX_PASSES:-20}"
SLEEP_SECONDS="${SLEEP_SECONDS:-45}"
CLOSE_API_CAP="${CLOSE_API_CAP:-600}"
INTRADAY_API_CAP="${INTRADAY_API_CAP:-600}"
INTRADAY_LOOKBACK_TRADING_DAYS="${INTRADAY_LOOKBACK_TRADING_DAYS:-60}"
STALL_THRESHOLD_PASSES="${STALL_THRESHOLD_PASSES:-3}"

post_job() {
  local path="$1"
  curl --fail-with-body -sS -X POST "${SERVICE_URL}${path}" \
    -H "X-Job-Token: ${JOB_TOKEN}" \
    -H "Content-Type: application/json" \
    -d '{}'
}

is_close_complete() {
  local resp="$1"
  python3 - "$resp" <<'PY'
import json
import sys
raw = sys.argv[1] if len(sys.argv) > 1 else ""
try:
    data = json.loads(raw)
except Exception:
    print("false")
    raise SystemExit(0)
done = bool(data.get("prefillComplete")) and int(data.get("terminalStaleSkipped", 0) or 0) == 0
print("true" if done else "false")
PY
}

close_metrics() {
  local resp="$1"
  python3 - "$resp" <<'PY'
import json
import sys
raw = sys.argv[1] if len(sys.argv) > 1 else ""
try:
    data = json.loads(raw)
except Exception:
    print("false 0 0 0 0 0")
    raise SystemExit(0)
prefill = bool(data.get("prefillComplete"))
terminal_stale = int(data.get("terminalStaleSkipped", 0) or 0)
pending = int(data.get("staleOrMissing", 0) or 0)
updated = int(data.get("updated", 0) or 0)
fetches = int(data.get("fetches", 0) or 0)
retried = int(data.get("retriedNoChange", 0) or 0)
print(("true" if prefill else "false"), terminal_stale, pending, updated, fetches, retried)
PY
}

echo "== Last-candle close update loop =="
DONE=0
RETRY_STALE="true"
STALL_COUNT=0
LAST_SIG=""
for i in $(seq 1 "${CLOSE_MAX_PASSES}"); do
  echo "[close pass ${i}/${CLOSE_MAX_PASSES}]"
  RESP="$(post_job "/jobs/score-cache-update-close?api_cap=${CLOSE_API_CAP}&lookback_days=700&min_bars=320&retry_stale_terminal_today=${RETRY_STALE}&run_intraday_update=true&intraday_api_cap=${INTRADAY_API_CAP}&intraday_lookback_trading_days=${INTRADAY_LOOKBACK_TRADING_DAYS}" || true)"
  echo "${RESP}"

  if [[ "${RESP}" == *'"skipped":"lock_busy"'* ]]; then
    sleep 60
    continue
  fi

  read -r PREFILL_COMPLETE TERMINAL_STALE PENDING UPDATED FETCHES RETRIED <<<"$(close_metrics "${RESP}")"
  if [[ "$(is_close_complete "${RESP}")" == "true" ]]; then
    DONE=1
    break
  fi

  if [[ "${RETRY_STALE}" == "false" && "${PREFILL_COMPLETE}" == "true" ]]; then
    DONE=1
    break
  fi

  SIG="${PREFILL_COMPLETE}:${TERMINAL_STALE}:${PENDING}:${UPDATED}:${FETCHES}:${RETRIED}"
  if [[ "${SIG}" == "${LAST_SIG}" ]]; then
    STALL_COUNT=$((STALL_COUNT + 1))
  else
    STALL_COUNT=0
  fi
  LAST_SIG="${SIG}"

  if [[ "${RETRY_STALE}" == "true" && "${STALL_COUNT}" -ge "${STALL_THRESHOLD_PASSES}" ]]; then
    echo "No progress detected for ${STALL_THRESHOLD_PASSES} passes; switching retry_stale_terminal_today=false to terminalize stuck rows."
    RETRY_STALE="false"
    STALL_COUNT=0
    LAST_SIG=""
  fi

  sleep "${SLEEP_SECONDS}"
done

if [[ "${DONE}" -ne 1 ]]; then
  echo "ERROR: last-candle update not complete in ${CLOSE_MAX_PASSES} passes"
  exit 1
fi

echo "Last-candle update complete."
