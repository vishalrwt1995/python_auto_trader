"""ISOLATED pull of nse_short_selling -> local grind cache (dry-run-confirmed 3.2MB, ~Rs0).
Channel-3: short-squeeze follow. NSE publishes daily short-sold qty per symbol; thesis (LONG side,
stock-only) = spike in short-selling + reversal -> shorts cover -> upward drift. Pure-Python (no venv
change). Writes ~/.autotrader_grind_cache/shorts.pkl. READ-ONLY BQ; no prod/existing-backtest file touched."""
import os, pickle
from collections import Counter
from google.cloud import bigquery

OUT = os.path.expanduser("~/.autotrader_grind_cache/shorts.pkl")
client = bigquery.Client(project="grow-profit-machine")
q = "SELECT date, symbol, qty FROM `grow-profit-machine.autotrader.nse_short_selling`"
out = []
for r in client.query(q, location="asia-south1").result(page_size=50000):
    out.append((str(r[0]), r[1], int(r[2] or 0)))
pickle.dump(out, open(OUT, "wb"))
ds = [x[0] for x in out]
print(f"short_selling: {len(out):,} rows | {min(ds)} -> {max(ds)} | syms {len({x[1] for x in out}):,}", flush=True)
# how many symbol-days have a real short qty
nz = [x for x in out if x[2] > 0]
print(f"  nonzero-qty rows: {len(nz):,} | median qty {sorted(x[2] for x in nz)[len(nz)//2]:,}", flush=True)
print(f"-> {OUT}", flush=True)
