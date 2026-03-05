#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 5 ]]; then
  echo "Usage: $0 <PROJECT_ID> <REGION> <SERVICE_URL> <JOB_TOKEN> <TIMEZONE>"
  exit 1
fi

PROJECT_ID="$1"
REGION="$2"
SERVICE_URL="${3%/}"
JOB_TOKEN="$4"
TIMEZONE="$5"

create_job () {
  local NAME="$1"
  local SCHEDULE="$2"
  local URI="$3"
  local BODY="${4:-{}}"
  local ATTEMPT_DEADLINE="${5:-30m}"
  gcloud scheduler jobs create http "$NAME" \
    --project "$PROJECT_ID" \
    --location "$REGION" \
    --schedule "$SCHEDULE" \
    --time-zone "$TIMEZONE" \
    --uri "$URI" \
    --http-method POST \
    --headers "Content-Type=application/json,X-Job-Token=$JOB_TOKEN" \
    --attempt-deadline "$ATTEMPT_DEADLINE" \
    --message-body "$BODY" \
    || gcloud scheduler jobs update http "$NAME" \
      --project "$PROJECT_ID" \
      --location "$REGION" \
      --schedule "$SCHEDULE" \
      --time-zone "$TIMEZONE" \
      --uri "$URI" \
      --http-method POST \
      --update-headers "Content-Type=application/json,X-Job-Token=$JOB_TOKEN" \
      --attempt-deadline "$ATTEMPT_DEADLINE" \
      --message-body "$BODY"
}

# Bootstrap/repair once per day (optional)
create_job "autotrader-bootstrap-sheets" "0 4 * * 1-5" "$SERVICE_URL/jobs/bootstrap-sheets"

# Upstox access-token request (notifier flow) shortly after daily token expiry (~03:30 IST).
# User approval is still required in the Upstox app/flow; the notifier webhook stores the token automatically.
create_job "autotrader-upstox-token-request" "35 3 * * 1-5" "$SERVICE_URL/jobs/upstox-token-request"

# Universe V2 morning pipeline (raw refresh -> canonical build -> backfill only newly appended instruments).
UNIVERSE_PIPELINE_URI="$SERVICE_URL/jobs/universe-v2-refresh?replace=false&build_limit=0&candle_api_cap=600&run_full_backfill=true&write_v2_eligibility=false&run_intraday_appended_backfill=true&intraday_api_cap=1200&intraday_lookback_trading_days=60"
create_job "autotrader-universe-v2-refresh-0615" "15 6 * * 1-5" "$UNIVERSE_PIPELINE_URI" "{}" "30m"

# Morning latest 1D/5m update:
# - early passes retry stale terminals to catch same-day provider catch-up
# - final pass terminalizes no-progress stale rows so downstream score/watchlist runs don't stall
CLOSE_UPDATE_URI_RETRY="$SERVICE_URL/jobs/score-cache-update-close?api_cap=600&lookback_days=700&min_bars=320&retry_stale_terminal_today=true&run_intraday_update=true&intraday_api_cap=600&intraday_lookback_trading_days=60"
CLOSE_UPDATE_URI_TERMINAL="$SERVICE_URL/jobs/score-cache-update-close?api_cap=600&lookback_days=700&min_bars=320&retry_stale_terminal_today=false&run_intraday_update=true&intraday_api_cap=600&intraday_lookback_trading_days=60"
create_job "autotrader-score-cache-update-close-0705" "5 7 * * 1-5" "$CLOSE_UPDATE_URI_RETRY"
create_job "autotrader-score-cache-update-close-0725" "25 7 * * 1-5" "$CLOSE_UPDATE_URI_RETRY"
create_job "autotrader-score-cache-update-close-0745" "45 7 * * 1-5" "$CLOSE_UPDATE_URI_RETRY"
create_job "autotrader-score-cache-update-close-0805" "5 8 * * 1-5" "$CLOSE_UPDATE_URI_TERMINAL"

# Score refresh after latest daily candle update window:
# - computes v1 scoring
# - recomputes universe v2 eligibility from cache (no extra candle API fetch)
MORNING_SCORE_URI="$SERVICE_URL/jobs/score-refresh?api_cap=0&cache_only=true&require_fresh_cache=true&fresh_hours=0"
create_job "autotrader-score-0830" "30 8 * * 1-5" "$MORNING_SCORE_URI" "{}" "30m"

# Watchlist V2 cadence.
WATCHLIST_V2_URI="$SERVICE_URL/jobs/watchlist-refresh?target_size=150&require_full_coverage=false&require_today_scored=false&min_watchlist_score=1&premarket=false&intraday_timeframe=5m"
WATCHLIST_V2_PREMARKET_URI="$SERVICE_URL/jobs/watchlist-refresh?target_size=150&require_full_coverage=false&require_today_scored=false&min_watchlist_score=1&premarket=true&intraday_timeframe=5m"
create_job "autotrader-watchlist-v2-premarket-0900" "0 9 * * 1-5" "$WATCHLIST_V2_PREMARKET_URI"
create_job "autotrader-watchlist-v2-5m-0930" "30-59/5 9 * * 1-5" "$WATCHLIST_V2_URI"
create_job "autotrader-watchlist-v2-5m-1000" "0-30/5 10 * * 1-5" "$WATCHLIST_V2_URI"
create_job "autotrader-watchlist-v2-15m-1045" "45 10 * * 1-5" "$WATCHLIST_V2_URI"
create_job "autotrader-watchlist-v2-15m-11to12" "0,15,30,45 11-12 * * 1-5" "$WATCHLIST_V2_URI"
create_job "autotrader-watchlist-v2-15m-1300" "0 13 * * 1-5" "$WATCHLIST_V2_URI"
create_job "autotrader-watchlist-v2-final-1445" "45 14 * * 1-5" "$WATCHLIST_V2_URI"

# Live scanner loop (strict market hours only: 09:20..15:30 IST, weekdays).
SCAN_URI="$SERVICE_URL/jobs/scan-once?force=false&allow_live_orders=false"
create_job "autotrader-scan-market-5m" "20-55/5 9-14 * * 1-5" "$SCAN_URI"
create_job "autotrader-scan-market-1530" "0-30/5 15 * * 1-5" "$SCAN_URI"

# Full 1D backfill remains available via the same endpoint for manual/on-demand use.

# Cleanup old schedule from previous versions (best-effort)
gcloud scheduler jobs delete "autotrader-premarket-precompute-9am" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-universe-sync" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-raw-universe-refresh" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-universe-build" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-score-cache-prefetch-close" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-score-cache-update-close" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-score-cache-backfill-full" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-score-cache-backfill-full-0630" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-universe-refresh-append-backfill-0610" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-universe-refresh-append-backfill-0615" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-score-cache-backfill-full-0700" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-score-bod-0710" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-premarket-precompute" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-score-cache-backfill-full-2015" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-score-cache-backfill-full-2115" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-premarket-precompute-0916" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-premarket-precompute-0921" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-score-cache-update-close-1845" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-score-cache-update-close-1930" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-score-eod-1940" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-eod-close-update-score-1810" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-eod-close-update-score-1840" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-eod-close-update-score-1910" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-watchlist-refresh-0915" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-watchlist-refresh-0921" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-watchlist-refresh-0916" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true

gcloud scheduler jobs delete "autotrader-intraday-cache-update-close-0815" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-intraday-cache-update-close-0825" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-intraday-cache-backfill-appended-0630" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true

# Cleanup old scanner schedules from previous versions (best-effort).
gcloud scheduler jobs delete "autotrader-scan-market-1" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-scan-market-2" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true
gcloud scheduler jobs delete "autotrader-scan-market-1535" \
  --project "$PROJECT_ID" \
  --location "$REGION" \
  --quiet || true

echo "Scheduler jobs created/updated."
