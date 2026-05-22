"""Round 2: finish readiness checks with correct bucket + schema."""
import json
import urllib.parse
import urllib.request
from pathlib import Path

TOKEN = Path("/tmp/adc_access_token.txt").read_text().strip()
PROJECT = "grow-profit-machine"
BUCKET = "grow-profit-machine-autotrader-data"
HDR = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json",
       "x-goog-user-project": PROJECT}

def _get(url):
    req = urllib.request.Request(url, headers=HDR)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")

def _post(url, body):
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers=HDR, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")


print("=" * 75)
print("FOLLOW-UP READINESS CHECK")
print("=" * 75)

# ----- 1. Latest brain snapshot -----
print(f"\n## 1. Brain snapshots in gs://{BUCKET}/state/market_brain/history/")
url = f"https://storage.googleapis.com/storage/v1/b/{BUCKET}/o?prefix=state/market_brain/history/&delimiter=/&maxResults=200"
s, b = _get(url)
if s == 200:
    d = json.loads(b)
    prefixes = sorted(d.get("prefixes", []))
    print(f"  {len(prefixes)} date folders")
    print(f"  Latest 5: {[p.split('/')[-2] for p in prefixes[-5:]]}")
    if prefixes:
        latest = prefixes[-1]
        url2 = f"https://storage.googleapis.com/storage/v1/b/{BUCKET}/o?prefix={urllib.parse.quote(latest)}"
        s2, b2 = _get(url2)
        items = json.loads(b2).get("items", [])
        print(f"  Snapshots in {latest}: {len(items)}")
        for it in items[-3:]:
            name = it["name"].split("/")[-1]
            updated = it.get("updated", "")[:19].replace("T", " ")
            sz = it.get("size", "0")
            print(f"    {name}  ({sz} bytes)  updated={updated}")
else:
    print(f"  ❌ {s}: {b[:200]}")

# ----- 2. Watchlist freshness (corrected schema) -----
print("\n## 2. Watchlist freshness (run_date)")
url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/queries"
sql = (
    "SELECT run_date, regime, risk_mode, selected, ARRAY_LENGTH(symbols) AS n_sym "
    f"FROM `{PROJECT}.autotrader.watchlist_history` "
    "WHERE run_date >= DATE_SUB(CURRENT_DATE('Asia/Kolkata'), INTERVAL 5 DAY) "
    "ORDER BY generated_at DESC LIMIT 8"
)
s, b = _post(url, {"query": sql, "useLegacySql": False, "timeoutMs": 30000})
if s == 200:
    rows = json.loads(b).get("rows", [])
    if rows:
        print(f"  {'date':12s} {'regime':10s} {'risk':10s} {'selected':>8s} {'n_sym':>6s}")
        for r in rows:
            c = [x.get("v", "") for x in r.get("f", [])]
            print(f"  {c[0]:12s} {c[1]:10s} {c[2]:10s} {c[3]:>8s} {c[4]:>6s}")
    else:
        print(f"  ⚠️  No rows (last 5 days). Raw: {b[:200]}")
else:
    print(f"  ❌ {s}: {b[:200]}")

# ----- 3. Recent signals (corrected schema) -----
print("\n## 3. Recent signals (last 10)")
sql = (
    "SELECT scan_ts, symbol, direction, score, regime, risk_mode, entry_placed, blocked_reason "
    f"FROM `{PROJECT}.autotrader.signals` "
    "ORDER BY scan_ts DESC LIMIT 10"
)
s, b = _post(url, {"query": sql, "useLegacySql": False, "timeoutMs": 30000})
if s == 200:
    rows = json.loads(b).get("rows", [])
    print(f"  {'ts':22s} {'sym':12s} {'dir':6s} {'score':>5s} {'regime':10s} {'risk':10s} {'placed':>6s} blocked")
    for r in rows:
        c = [x.get("v", "") for x in r.get("f", [])]
        # ts is epoch float, convert
        try:
            from datetime import datetime
            ts = datetime.fromtimestamp(float(c[0])).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            ts = c[0]
        print(f"  {ts:22s} {(c[1] or '')[:12]:12s} {(c[2] or '')[:6]:6s} {c[3]:>5s} {(c[4] or '')[:10]:10s} {(c[5] or '')[:10]:10s} {(c[6] or '')[:6]:>6s} {(c[7] or '')[:40]}")

# ----- 4. Scan_decisions recent (yesterday + today) -----
print("\n## 4. scan_decisions recent activity (May 21)")
sql = (
    "SELECT DATE(scan_ts, 'Asia/Kolkata') AS d, COUNT(*) AS n_total, "
    "COUNTIF(qualified) AS n_qualified, COUNTIF(NOT qualified) AS n_blocked "
    f"FROM `{PROJECT}.autotrader.scan_decisions` "
    "WHERE DATE(scan_ts, 'Asia/Kolkata') >= '2026-05-19' "
    "GROUP BY d ORDER BY d DESC"
)
s, b = _post(url, {"query": sql, "useLegacySql": False, "timeoutMs": 30000})
if s == 200:
    rows = json.loads(b).get("rows", [])
    if rows:
        print(f"  {'date':12s} {'total':>8s} {'qualified':>10s} {'blocked':>8s}")
        for r in rows:
            c = [x.get("v", "") for x in r.get("f", [])]
            print(f"  {c[0]:12s} {c[1]:>8s} {c[2]:>10s} {c[3]:>8s}")
    else:
        print(f"  Raw: {b[:300]}")
else:
    print(f"  ❌ {s}: {b[:300]}")

# ----- 5. Latest market_brain_history regime + risk_mode -----
print("\n## 5. Latest brain regime + risk_mode")
sql = (
    "SELECT asof_ts, regime, risk_mode, market_confidence, participation, "
    "trend_score, breadth_score, leadership_score "
    f"FROM `{PROJECT}.autotrader.market_brain_history` "
    "ORDER BY asof_ts DESC LIMIT 3"
)
s, b = _post(url, {"query": sql, "useLegacySql": False, "timeoutMs": 30000})
if s == 200:
    rows = json.loads(b).get("rows", [])
    for r in rows:
        c = [x.get("v", "") for x in r.get("f", [])]
        try:
            from datetime import datetime
            ts = datetime.fromtimestamp(float(c[0])).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            ts = c[0]
        print(f"  [{ts}] regime={c[1]} risk={c[2]} confidence={c[3]} participation={c[4]} trend={c[5]} breadth={c[6]} leadership={c[7]}")
else:
    print(f"  ⚠️ {s}: {b[:200]}")

# ----- 6. Recent trades (any from yesterday?) -----
print("\n## 6. Recent trades (last 7d)")
sql = (
    "SELECT * FROM `grow-profit-machine.autotrader.trades` "
    "WHERE DATE(entry_ts, 'Asia/Kolkata') >= DATE_SUB(CURRENT_DATE('Asia/Kolkata'), INTERVAL 7 DAY) "
    "ORDER BY entry_ts DESC LIMIT 10"
)
s, b = _post(url, {"query": sql, "useLegacySql": False, "timeoutMs": 30000, "maxResults": 10})
if s == 200:
    j = json.loads(b)
    fields = [f["name"] for f in j.get("schema", {}).get("fields", [])]
    rows = j.get("rows", [])
    print(f"  {len(rows)} trade rows (last 7 days)")
    for r in rows[:5]:
        cells = {fields[i]: x.get("v", "") for i, x in enumerate(r.get("f", []))}
        sym = cells.get("symbol", "?")
        side = cells.get("side", "?")
        status = cells.get("status", "?")
        pnl = cells.get("realized_pnl", cells.get("pnl", "?"))
        et = cells.get("entry_ts", "")
        try:
            from datetime import datetime
            et_str = datetime.fromtimestamp(float(et)).strftime("%Y-%m-%d %H:%M") if et else "?"
        except Exception:
            et_str = et
        print(f"    [{et_str}] {sym} {side} status={status} pnl={pnl}")
else:
    print(f"  ⚠️ {s}: {b[:300]}")

# ----- 7. VIX check -----
print("\n## 7. Recent NIFTY VIX (for VIX_TREND_MAX gate context)")
sql = (
    "SELECT * FROM `grow-profit-machine.autotrader.market_brain_history` "
    "ORDER BY asof_ts DESC LIMIT 1"
)
s, b = _post(url, {"query": sql, "useLegacySql": False, "timeoutMs": 30000})
if s == 200:
    j = json.loads(b)
    fields = [f["name"] for f in j.get("schema", {}).get("fields", [])]
    rows = j.get("rows", [])
    for r in rows:
        cells = {fields[i]: x.get("v", "") for i, x in enumerate(r.get("f", []))}
        # find any field with 'vix' in name
        for k, v in cells.items():
            if "vix" in k.lower() or "volatility" in k.lower():
                print(f"  {k} = {v}")

print("\n" + "=" * 75)
