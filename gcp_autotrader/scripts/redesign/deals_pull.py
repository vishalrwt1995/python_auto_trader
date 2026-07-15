"""ISOLATED pull of nse_block_deals + nse_bulk_deals -> local grind cache (dry-run-confirmed
0.96MB + 17.5MB). Channel-2: bulk/block-deal follow. Pure-Python (no venv change). Writes to
~/.autotrader_grind_cache/deals.pkl. READ-ONLY BQ; no prod/existing-backtest file touched."""
import os, pickle
from collections import Counter
from google.cloud import bigquery

OUT = os.path.expanduser("~/.autotrader_grind_cache/deals.pkl")
client = bigquery.Client(project="grow-profit-machine")
def pull(tbl):
    q = f"SELECT date, symbol, buy_sell, qty, price FROM `grow-profit-machine.autotrader.{tbl}`"
    out = []
    for r in client.query(q, location="asia-south1").result(page_size=50000):
        out.append((str(r[0]), r[1], str(r[2] or "").strip().upper(), int(r[3] or 0), float(r[4] or 0.0)))
    return out

print("pulling block + bulk deals ...", flush=True)
block = pull("nse_block_deals"); bulk = pull("nse_bulk_deals")
pickle.dump({"block": block, "bulk": bulk}, open(OUT, "wb"))
for name, D in [("block", block), ("bulk", bulk)]:
    ds = [x[0] for x in D]
    print(f"  {name}: {len(D):,} deals | {min(ds)} -> {max(ds)} | syms {len({x[1] for x in D}):,} "
          f"| buy_sell {dict(Counter(x[2] for x in D).most_common(4))}", flush=True)
print(f"-> {OUT}", flush=True)
