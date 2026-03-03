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

echo "== Last-candle close update loop =="
DONE=0
for i in $(seq 1 "${CLOSE_MAX_PASSES}"); do
  echo "[close pass ${i}/${CLOSE_MAX_PASSES}]"
  RESP="$(post_job "/jobs/score-cache-update-close?api_cap=${CLOSE_API_CAP}&lookback_days=700&min_bars=320&retry_stale_terminal_today=true&run_intraday_update=true&intraday_api_cap=${INTRADAY_API_CAP}&intraday_lookback_trading_days=${INTRADAY_LOOKBACK_TRADING_DAYS}" || true)"
  echo "${RESP}"

  if [[ "${RESP}" == *'"skipped":"lock_busy"'* ]]; then
    sleep 60
    continue
  fi
  if [[ "$(is_close_complete "${RESP}")" == "true" ]]; then
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
