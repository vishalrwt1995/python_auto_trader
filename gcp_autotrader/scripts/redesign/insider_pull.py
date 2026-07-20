"""Insider (PIT) historical pull -- monthly corporates-pit API 2015-01 -> 2026-07 via the
CONFIRMED NSE handshake (session -> homepage+insider-page warm-up -> browser headers). Caches
each month's raw `data` array to ~/.autotrader_backtest_cache/insider_pit/{YYYY-MM}.json.
Idempotent (skips months already cached with data), polite (0.8s delay, re-warm every 25 req),
single-process, zero GCP cost (free public NSE data). Reports coverage + per-year + promoter
counts, and flags any recent-year empties (a live-build concern, not a backtest one)."""
import os, json, time, calendar, requests

CACHE = os.path.expanduser("~/.autotrader_backtest_cache/insider_pit")
os.makedirs(CACHE, exist_ok=True)
H = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/122.0 Safari/537.36",
     "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9", "Referer": "https://www.nseindia.com/"}
API = "https://www.nseindia.com/api/corporates-pit?index=equities&from_date={f}&to_date={t}"
WARM = ("https://www.nseindia.com/",
        "https://www.nseindia.com/companies-listing/corporate-filings-insider-trading")

def warm(sess):
    for u in WARM:
        try: sess.get(u, headers=H, timeout=12)
        except Exception: pass

sess = requests.Session(); warm(sess)
months = [(y, m) for y in range(2015, 2027) for m in range(1, 13) if (y, m) <= (2026, 7)]
req_count = 0; per_year = {}; total = 0; empties = []
for y, m in months:
    fn = os.path.join(CACHE, f"{y}-{m:02d}.json")
    if os.path.exists(fn) and os.path.getsize(fn) > 50:
        try:
            n = len(json.load(open(fn))); per_year[y] = per_year.get(y, 0) + n; total += n
            continue
        except Exception:
            pass
    last = calendar.monthrange(y, m)[1]
    url = API.format(f=f"01-{m:02d}-{y}", t=f"{last:02d}-{m:02d}-{y}")
    data = None
    for attempt in (1, 2):
        try:
            r = sess.get(url, headers=H, timeout=35)
            if r.status_code == 200:
                data = r.json().get("data", []); break
            else:
                print(f"  {y}-{m:02d}: status={r.status_code} (attempt {attempt})", flush=True)
        except Exception as e:
            print(f"  {y}-{m:02d}: {e} (attempt {attempt})", flush=True)
        warm(sess); time.sleep(1.5)
    if data is None:
        print(f"  {y}-{m:02d}: FAILED (left uncached for retry)", flush=True)
        time.sleep(0.8); continue
    json.dump(data, open(fn, "w"))
    per_year[y] = per_year.get(y, 0) + len(data); total += len(data)
    if len(data) == 0: empties.append(f"{y}-{m:02d}")
    print(f"  {y}-{m:02d}: {len(data):>5} records", flush=True)
    req_count += 1
    if req_count % 25 == 0: warm(sess)
    time.sleep(0.8)

print(f"\n=== PULL DONE: {total:,} total insider records ===", flush=True)
for y in sorted(per_year): print(f"  {y}: {per_year[y]:>7,}")
if empties: print(f"\n  EMPTY months ({len(empties)}): {empties}")
print(f"\n  cached -> {CACHE}", flush=True)
