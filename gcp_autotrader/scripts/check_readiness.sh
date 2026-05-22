#!/usr/bin/env bash
# Pre-market readiness check for autotrader Cloud Run service.
# Run AFTER: gcloud auth login
#
# Verifies:
#   1. Active gcloud config + project
#   2. Live Cloud Run revision (expect autotrader-00234-7rt or newer)
#   3. Traffic routed to latest revision (100%)
#   4. Env vars: PAPER_TRADE, USE_PLAYBOOK_V1, SWING_MIN_SIGNAL_SCORE, VIX_TREND_MAX
#   5. Cloud Scheduler jobs enabled
#   6. Most recent brain snapshot timestamp (should be ~9 AM IST or later from last trading day)
#   7. Recent error count from logs (last 12h)
#   8. BigQuery scan_decisions row count from last trading day

set -u

GCLOUD=/Users/vishalrawat/google-cloud-sdk/bin/gcloud
PROJECT=grow-profit-machine
REGION=asia-south1
SVC=autotrader

echo "=========================================================================="
echo "AUTOTRADER PRE-MARKET READINESS CHECK — $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "=========================================================================="

# 0. Auth check
echo
echo "## 0. gcloud auth"
ACTIVE_ACCT=$($GCLOUD config get-value account 2>/dev/null)
ACTIVE_PROJ=$($GCLOUD config get-value project 2>/dev/null)
echo "  Active account: $ACTIVE_ACCT"
echo "  Active project: $ACTIVE_PROJ"
if [ "$ACTIVE_PROJ" != "$PROJECT" ]; then
  echo "  ⚠️  Expected project=$PROJECT  Run: gcloud config set project $PROJECT"
fi

# 1. Live revision
echo
echo "## 1. Live Cloud Run revision"
LIVE_REV=$($GCLOUD --project=$PROJECT run services describe $SVC --region=$REGION \
  --format="value(status.traffic[0].revisionName)" 2>&1)
echo "  Current: $LIVE_REV"
echo "  Expected: autotrader-00234-7rt (or newer with Tier 1+2 fixes)"

# 2. Traffic split (must be 100% on one revision)
echo
echo "## 2. Traffic routing"
$GCLOUD --project=$PROJECT run services describe $SVC --region=$REGION \
  --format="table(status.traffic[].revisionName,status.traffic[].percent)" 2>&1 | head -10

# 3. Env vars
echo
echo "## 3. Env vars (must show: PAPER_TRADE=true, USE_PLAYBOOK_V1=true, SWING_MIN_SIGNAL_SCORE=45, VIX_TREND_MAX=18)"
$GCLOUD --project=$PROJECT run services describe $SVC --region=$REGION --format=json 2>/dev/null \
  | python3 -c "
import json, sys
d = json.load(sys.stdin)
spec = d.get('spec', {}).get('template', {}).get('spec', {}).get('containers', [{}])[0]
env = {e['name']: e.get('value','') for e in spec.get('env', [])}
keys = ['PAPER_TRADE','USE_PLAYBOOK_V1','SWING_MIN_SIGNAL_SCORE','VIX_TREND_MAX','MAX_OPEN_SWINGS','SCAN_INTERVAL_SECONDS','RISK_PER_TRADE','SWING_RISK_PER_TRADE']
for k in keys:
    v = env.get(k, '(unset)')
    flag = '✅' if v != '(unset)' else '⚠️ '
    print(f'  {flag} {k}={v}')"

# 4. Scheduler jobs
echo
echo "## 4. Cloud Scheduler jobs (should show ENABLED for swing-scan, intraday-scan, brain-snap)"
$GCLOUD --project=$PROJECT scheduler jobs list --location=$REGION \
  --format="table(name.basename(),state,schedule)" 2>&1 | head -20

# 5. Most recent brain snapshot
echo
echo "## 5. Most recent brain snapshot"
LATEST_BRAIN=$($GCLOUD --project=$PROJECT storage ls "gs://grow-profit-machine-state/state/market_brain/history/" 2>&1 \
  | tail -2 | head -1)
echo "  Latest folder: $LATEST_BRAIN"
$GCLOUD --project=$PROJECT storage ls "${LATEST_BRAIN}" 2>&1 | tail -3 | head -2

# 6. Recent error count (last 12 hours)
echo
echo "## 6. Recent ERROR-severity logs (last 12h)"
ERR_COUNT=$($GCLOUD --project=$PROJECT logging read \
  "resource.type=cloud_run_revision AND resource.labels.service_name=$SVC AND severity>=ERROR" \
  --freshness=12h --format="value(timestamp)" --limit=100 2>/dev/null | wc -l)
echo "  Error log entries (last 12h): $ERR_COUNT"
if [ "$ERR_COUNT" -gt 0 ]; then
  echo "  Last 3 errors:"
  $GCLOUD --project=$PROJECT logging read \
    "resource.type=cloud_run_revision AND resource.labels.service_name=$SVC AND severity>=ERROR" \
    --freshness=12h --format="value(timestamp,textPayload)" --limit=3 2>&1 | head -10
fi

# 7. BigQuery activity from last trading day
echo
echo "## 7. BigQuery activity (last trading day, May 21)"
$GCLOUD --project=$PROJECT alpha bq query --use_legacy_sql=false \
  "SELECT COUNT(*) AS n FROM \`$PROJECT.autotrader.scan_decisions\` WHERE DATE(scan_ts, 'Asia/Kolkata') = '2026-05-21'" \
  2>/dev/null | tail -5
echo

echo "## 8. Watchlist freshness"
$GCLOUD --project=$PROJECT alpha bq query --use_legacy_sql=false \
  "SELECT effective_date, COUNT(*) AS n FROM \`$PROJECT.autotrader.watchlist_history\` WHERE effective_date >= '2026-05-21' GROUP BY effective_date ORDER BY effective_date DESC LIMIT 3" \
  2>/dev/null | tail -10

echo
echo "=========================================================================="
echo "✅ Done. Review output above. Tomorrow's open: 9:15 IST."
echo "=========================================================================="
