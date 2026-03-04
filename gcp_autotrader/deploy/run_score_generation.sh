#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${SERVICE_URL:-}" || -z "${JOB_TOKEN:-}" ]]; then
  echo "ERROR: export SERVICE_URL and JOB_TOKEN first."
  exit 1
fi

post_job() {
  local path="$1"
  curl --fail-with-body -sS -X POST "${SERVICE_URL}${path}" \
    -H "X-Job-Token: ${JOB_TOKEN}" \
    -H "Content-Type: application/json" \
    -d '{}'
}

echo "== Score refresh (universe v2 + market regime) =="
post_job "/jobs/score-refresh?api_cap=0&cache_only=true&require_fresh_cache=true&fresh_hours=0"
echo

echo "== Universe V2 audit =="
post_job "/jobs/universe-v2-audit"
echo

echo "Score generation complete."
