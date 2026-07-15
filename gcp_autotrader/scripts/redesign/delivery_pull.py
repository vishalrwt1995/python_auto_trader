"""ISOLATED pull of nse_delivery_daily -> local grind cache (ONE 118MB scan, dry-run-confirmed).
New-channel grind (delivery-accumulation). Pure-Python (NO pandas -> no venv change). Writes to
~/.autotrader_grind_cache/ (SEPARATE from ~/.autotrader_backtest_cache). Per-symbol dict:
{symbol: [(date, deliv_pct, deliv_qty, ttl_trd_qty), ...]} sorted by date. After this, all
delivery grinding is LOCAL (no re-scan). READ-ONLY on BQ; no prod/existing-backtest file touched."""
import os, pickle
from collections import defaultdict
from google.cloud import bigquery

OUT = os.path.expanduser("~/.autotrader_grind_cache/delivery.pkl")
client = bigquery.Client(project="grow-profit-machine")
q = ("SELECT symbol, date, deliv_pct, deliv_qty, ttl_trd_qty "
     "FROM `grow-profit-machine.autotrader.nse_delivery_daily`")
print("querying (118MB) + streaming rows (no pandas) ...", flush=True)
job = client.query(q, location="asia-south1")
by_sym = defaultdict(list)
n = 0
for r in job.result(page_size=50000):
    by_sym[r[0]].append((str(r[1]), float(r[2] or 0.0), int(r[3] or 0), int(r[4] or 0)))
    n += 1
for s in by_sym:
    by_sym[s].sort()
pickle.dump(dict(by_sym), open(OUT, "wb"))
mn = min(v[0][0] for v in by_sym.values()); mx = max(v[-1][0] for v in by_sym.values())
print(f"pulled {n:,} rows | {len(by_sym):,} symbols | {mn} -> {mx} -> {OUT}", flush=True)
