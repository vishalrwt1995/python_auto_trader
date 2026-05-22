"""Direct-API readiness check for autotrader. Uses ADC access token (no gcloud).

Checks:
  1. Cloud Run service revision + traffic
  2. Env vars on live revision
  3. Cloud Scheduler jobs (enabled/disabled + last run)
  4. Latest brain snapshot in GCS
  5. Recent error log count
  6. BigQuery scan_decisions for yesterday
  7. Watchlist freshness
"""
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

TOKEN = Path("/tmp/adc_access_token.txt").read_text().strip()
PROJECT = "grow-profit-machine"
REGION = "asia-south1"
SVC = "autotrader"

HDR = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
    "x-goog-user-project": PROJECT,
}


def _get(url, hdr=HDR):
    req = urllib.request.Request(url, headers=hdr)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        return e.code, body
    except Exception as e:
        return 0, str(e)


def _post(url, body, hdr=HDR):
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers=hdr, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")
    except Exception as e:
        return 0, str(e)


print("=" * 75)
print(f"AUTOTRADER READINESS CHECK — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"  Project: {PROJECT}   Region: {REGION}   Service: {SVC}")
print("=" * 75)

# ----- 1. Cloud Run service + revisions -----
print("\n## 1. Cloud Run service (Revision + Traffic)")
url = f"https://run.googleapis.com/v2/projects/{PROJECT}/locations/{REGION}/services/{SVC}"
status, body = _get(url)
if status != 200:
    print(f"  ❌ {status}: {body[:200]}")
    sys.exit(1)
svc = json.loads(body)
print(f"  uri:               {svc.get('uri')}")
print(f"  latestReadyRev:    {svc.get('latestReadyRevision','').split('/')[-1]}")
print(f"  latestCreatedRev:  {svc.get('latestCreatedRevision','').split('/')[-1]}")
print(f"  conditions:")
for c in svc.get("conditions", [])[:3]:
    print(f"    - {c.get('type')}: {c.get('state','')}  ({c.get('message','')[:60]})")
print(f"  traffic:")
for t in svc.get("trafficStatuses", svc.get("traffic", [])):
    rev = t.get("revision", "").split("/")[-1]
    pct = t.get("percent", t.get("Percent", 0))
    typ = t.get("type", "")
    print(f"    {pct}% -> {rev or '(latest)'}  type={typ}")

# ----- 2. Env vars on live revision -----
print("\n## 2. Env vars on live revision")
container = (svc.get("template", {}).get("containers", [{}]) or [{}])[0]
env = {e["name"]: e.get("value", "") for e in container.get("env", [])}
keys = ["PAPER_TRADE", "USE_PLAYBOOK_V1", "SWING_MIN_SIGNAL_SCORE",
        "VIX_TREND_MAX", "MAX_OPEN_SWINGS", "SCAN_INTERVAL_SECONDS",
        "RISK_PER_TRADE", "SWING_RISK_PER_TRADE", "TRADING_MODE"]
for k in keys:
    v = env.get(k, "(unset)")
    flag = "✅" if v != "(unset)" else "⚠️ "
    print(f"  {flag} {k}={v}")
# Also show any keys we didn't expect
extra = [k for k in env if k not in keys]
if extra:
    print(f"  Other env keys ({len(extra)}): {', '.join(sorted(extra)[:10])}{'...' if len(extra)>10 else ''}")

# ----- 3. Cloud Scheduler jobs -----
print("\n## 3. Cloud Scheduler jobs (target=autotrader)")
url = f"https://cloudscheduler.googleapis.com/v1/projects/{PROJECT}/locations/{REGION}/jobs"
status, body = _get(url)
if status != 200:
    print(f"  ❌ {status}: {body[:200]}")
else:
    jobs = json.loads(body).get("jobs", [])
    print(f"  {len(jobs)} jobs total")
    for j in jobs:
        name = j.get("name", "").split("/")[-1]
        state = j.get("state", "")
        sched = j.get("schedule", "")
        tz = j.get("timeZone", "")
        last = j.get("lastAttemptTime", "")[:19].replace("T", " ")
        last_status = "OK" if j.get("status", {}).get("code") == 0 else (f"code={j.get('status', {}).get('code')}" if j.get("status") else "n/a")
        flag = "✅" if state == "ENABLED" else "⚠️ "
        print(f"  {flag} {name:35s} {state:10s} {sched:20s} ({tz})  last={last} [{last_status}]")

# ----- 4. Latest brain snapshot in GCS -----
print("\n## 4. Latest brain snapshot in GCS")
bucket = f"{PROJECT}-state"
# List date prefixes
url = f"https://storage.googleapis.com/storage/v1/b/{bucket}/o?prefix=state/market_brain/history/&delimiter=/&maxResults=200"
status, body = _get(url)
if status != 200:
    # Maybe bucket name is different — try alternates
    for alt_bucket in [f"{PROJECT}", "autotrader-state", "grow-profit-machine-state"]:
        url2 = f"https://storage.googleapis.com/storage/v1/b/{alt_bucket}/o?prefix=state/market_brain/history/&delimiter=/&maxResults=200"
        s2, b2 = _get(url2)
        if s2 == 200:
            bucket = alt_bucket
            status, body = s2, b2
            break
if status == 200:
    data = json.loads(body)
    prefixes = sorted(data.get("prefixes", []))[-5:]
    print(f"  bucket: gs://{bucket}/")
    print(f"  Latest 5 date folders:")
    for p in prefixes:
        print(f"    {p}")
    if prefixes:
        latest_date_prefix = prefixes[-1]
        url = f"https://storage.googleapis.com/storage/v1/b/{bucket}/o?prefix={urllib.parse.quote(latest_date_prefix)}&maxResults=200"
        s, b = _get(url)
        if s == 200:
            items = json.loads(b).get("items", [])
            print(f"  Snapshots in {latest_date_prefix}: {len(items)}")
            for it in items[-3:]:
                name = it["name"].split("/")[-1]
                updated = it.get("updated", "")[:19].replace("T", " ")
                print(f"    {name}  updated={updated}")
else:
    print(f"  ❌ Could not list GCS: {status}: {body[:200]}")

# ----- 5. Recent error log count -----
print("\n## 5. Recent error logs (last 12h)")
url = "https://logging.googleapis.com/v2/entries:list"
twelve_h_ago = (datetime.now(timezone.utc) - timedelta(hours=12)).strftime("%Y-%m-%dT%H:%M:%SZ")
log_filter = (
    f'resource.type="cloud_run_revision" '
    f'AND resource.labels.service_name="{SVC}" '
    f'AND severity>=ERROR '
    f'AND timestamp>="{twelve_h_ago}"'
)
body_req = {
    "resourceNames": [f"projects/{PROJECT}"],
    "filter": log_filter,
    "orderBy": "timestamp desc",
    "pageSize": 10,
}
status, body = _post(url, body_req)
if status != 200:
    print(f"  ❌ {status}: {body[:200]}")
else:
    entries = json.loads(body).get("entries", [])
    print(f"  Error entries (last 12h): {len(entries)}")
    for e in entries[:5]:
        ts = e.get("timestamp", "")[:19].replace("T", " ")
        sev = e.get("severity", "")
        msg = e.get("textPayload") or e.get("jsonPayload", {}).get("message", "")
        if not msg:
            msg = json.dumps(e.get("jsonPayload", {}))[:120]
        print(f"    [{ts}] {sev}: {str(msg)[:120]}")

# ----- 6. BigQuery scan_decisions yesterday -----
print("\n## 6. BigQuery scan_decisions (yesterday 2026-05-21)")
url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/queries"
sql = (
    "SELECT COUNT(*) AS n, MAX(scan_ts) AS last_ts "
    f"FROM `{PROJECT}.autotrader.scan_decisions` "
    "WHERE DATE(scan_ts, 'Asia/Kolkata') = '2026-05-21'"
)
status, body = _post(url, {"query": sql, "useLegacySql": False, "timeoutMs": 30000})
if status != 200:
    print(f"  ❌ {status}: {body[:300]}")
else:
    j = json.loads(body)
    rows = j.get("rows", [])
    if rows:
        cells = rows[0].get("f", [])
        n = cells[0].get("v", "0")
        last = cells[1].get("v", "")
        print(f"  scan_decisions for 2026-05-21: {n} rows (last scan at {last})")
    else:
        print(f"  No rows returned. Raw: {body[:200]}")

# ----- 7. Watchlist freshness -----
print("\n## 7. Watchlist freshness")
sql = (
    "SELECT effective_date, COUNT(*) AS n "
    f"FROM `{PROJECT}.autotrader.watchlist_history` "
    "WHERE effective_date >= DATE_SUB(CURRENT_DATE('Asia/Kolkata'), INTERVAL 5 DAY) "
    "GROUP BY effective_date ORDER BY effective_date DESC LIMIT 5"
)
status, body = _post(url, {"query": sql, "useLegacySql": False, "timeoutMs": 30000})
if status != 200:
    print(f"  ❌ {status}: {body[:300]}")
else:
    j = json.loads(body)
    rows = j.get("rows", [])
    if rows:
        for r in rows:
            cells = r.get("f", [])
            d = cells[0].get("v", "")
            n = cells[1].get("v", "0")
            print(f"  {d}: {n} symbols")
    else:
        print(f"  No recent watchlist rows. Raw: {body[:200]}")

# ----- 8. Active signals (most recent) -----
print("\n## 8. Most recent signals (last 5)")
sql = (
    "SELECT scan_ts, symbol, setup, direction, raw_score, adjusted_score, status "
    f"FROM `{PROJECT}.autotrader.signals` "
    "ORDER BY scan_ts DESC LIMIT 5"
)
status, body = _post(url, {"query": sql, "useLegacySql": False, "timeoutMs": 30000})
if status == 200:
    j = json.loads(body)
    rows = j.get("rows", [])
    for r in rows:
        cells = [c.get("v", "") for c in r.get("f", [])]
        print(f"  {cells}")
else:
    print(f"  Skipped: {status}: {body[:120]}")

print("\n" + "=" * 75)
print("✅ Readiness check complete.")
print("=" * 75)
