#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${SERVICE_URL:-}" || -z "${JOB_TOKEN:-}" ]]; then
  echo "ERROR: export SERVICE_URL and JOB_TOKEN first."
  exit 1
fi

WATCHLIST_MAX_PASSES="${WATCHLIST_MAX_PASSES:-60}"
SLEEP_SECONDS="${SLEEP_SECONDS:-30}"
TARGET_SIZE="${TARGET_SIZE:-150}"
REQUIRE_FULL_COVERAGE="${REQUIRE_FULL_COVERAGE:-true}"
REQUIRE_TODAY_SCORED="${REQUIRE_TODAY_SCORED:-true}"
MIN_WATCHLIST_SCORE="${MIN_WATCHLIST_SCORE:-1}"
PREMARKET="${PREMARKET:-true}"
INTRADAY_TIMEFRAME="${INTRADAY_TIMEFRAME:-5m}"

TMP_RESP="/tmp/watchlist_generation_resp.json"

check_complete() {
  local body="$1"
  local require_full="$2"
  local require_today="$3"
  python3 - "$body" "$require_full" "$require_today" <<'PY'
import json
import sys

raw = sys.argv[1] if len(sys.argv) > 1 else ""
require_full = str(sys.argv[2] if len(sys.argv) > 2 else "false").strip().lower() == "true"
require_today = str(sys.argv[3] if len(sys.argv) > 3 else "false").strip().lower() == "true"

try:
    data = json.loads(raw)
except Exception:
    print("false")
    raise SystemExit(0)

wl = data.get("watchlist") or {}
ready = bool(wl.get("ready"))
selected = int(wl.get("selected") or 0)
cov = wl.get("coverage") or {}

coverage_ok = True
if require_full:
    coverage_ok = bool(cov.get("todayFull")) if require_today else bool(cov.get("full"))

done = ready and selected > 0 and coverage_ok
print("true" if done else "false")
PY
}

echo "== Watchlist V2 generation loop =="
DONE=0
for i in $(seq 1 "${WATCHLIST_MAX_PASSES}"); do
  echo "[watchlist pass ${i}/${WATCHLIST_MAX_PASSES}]"
  HTTP_CODE="$(curl -sS -o "${TMP_RESP}" -w "%{http_code}" -X POST "${SERVICE_URL}/jobs/watchlist-refresh?target_size=${TARGET_SIZE}&require_full_coverage=${REQUIRE_FULL_COVERAGE}&require_today_scored=${REQUIRE_TODAY_SCORED}&min_watchlist_score=${MIN_WATCHLIST_SCORE}&premarket=${PREMARKET}&intraday_timeframe=${INTRADAY_TIMEFRAME}" -H "X-Job-Token: ${JOB_TOKEN}" -H "Content-Type: application/json" -d '{}' || true)"
  RESP="$(cat "${TMP_RESP}" 2>/dev/null || true)"
  echo "${RESP}"

  if [[ "${HTTP_CODE}" == "503" ]]; then
    echo "Service unavailable, retrying..."
    sleep "${SLEEP_SECONDS}"
    continue
  fi
  if [[ "${RESP}" == *'"skipped":"lock_busy"'* ]]; then
    echo "Lock busy, retrying..."
    sleep 60
    continue
  fi
  if [[ "${HTTP_CODE}" != "200" ]]; then
    echo "HTTP ${HTTP_CODE}, retrying..."
    sleep "${SLEEP_SECONDS}"
    continue
  fi
  if [[ "$(check_complete "${RESP}" "${REQUIRE_FULL_COVERAGE}" "${REQUIRE_TODAY_SCORED}")" == "true" ]]; then
    DONE=1
    break
  fi
  sleep "${SLEEP_SECONDS}"
done

if [[ "${DONE}" -ne 1 ]]; then
  echo "ERROR: watchlist generation not complete in ${WATCHLIST_MAX_PASSES} passes"
  exit 1
fi

echo "Watchlist generation complete."
