"""ISOLATED one-time 5m pull -> local hash-bucket shards (memory-bounded, no laptop hang).

Full universe candles_5m_full (2,638 syms / 165.6M rows / dry-run 10.83GB ≈ ₹5). ONE scan only —
no ORDER BY (avoids BQ resources-exceeded on a 165M-row global sort) and no symbol-batch loop
(clustering prunes poorly → batching would re-scan ~40%/batch = the ₹61k trap). Rows stream unordered
and route to 50 hash-buckets written incrementally (bounded RAM ~150MB). The grind reads a bucket,
groups by the `symbol` field in-row, processes symbol-by-symbol single-process. Pure-Python (no pandas).
Result-read via REST paging is FREE (only the 10.8GB scan is billed). READ-ONLY BQ; no prod touched.

Bucket file format: repeated pickle.dump(batch) — read back with repeated pickle.load until EOFError.
Each row: (symbol, bar_ts_iso, trade_date, open, high, low, close, volume)."""
import os, pickle, time
from google.cloud import bigquery

OUT = os.path.expanduser("~/.autotrader_grind_cache/intraday_5m")
os.makedirs(OUT, exist_ok=True)
NB = 50
client = bigquery.Client(project="grow-profit-machine")
q = ("SELECT symbol, bar_ts, trade_date, open, high, low, close, volume "
     "FROM `grow-profit-machine.autotrader.candles_5m_full`")
print("launching 5m full-universe scan (one-time, ~10.8GB)...", flush=True)
job = client.query(q, location="asia-south1")
files = [open(f"{OUT}/bucket_{i:02d}.pkl", "wb") for i in range(NB)]
bufs = [[] for _ in range(NB)]
def flush(i):
    if bufs[i]:
        pickle.dump(bufs[i], files[i]); bufs[i] = []
n = 0; t0 = time.time()
for r in job.result(page_size=100000):
    b = (hash(r[0]) & 0x7fffffff) % NB
    bufs[b].append((r[0], str(r[1]), str(r[2]), float(r[3] or 0), float(r[4] or 0),
                    float(r[5] or 0), float(r[6] or 0), float(r[7] or 0)))
    if len(bufs[b]) >= 50000:
        flush(b)
    n += 1
    if n % 5_000_000 == 0:
        print(f"  {n:,} rows, {time.time()-t0:.0f}s", flush=True)
for i in range(NB):
    flush(i); files[i].close()
syms = set()
for i in range(NB):
    with open(f"{OUT}/bucket_{i:02d}.pkl", "rb") as f:
        while True:
            try:
                for row in pickle.load(f): syms.add(row[0])
            except EOFError:
                break
print(f"DONE: {n:,} rows, {len(syms):,} symbols, {NB} buckets, {time.time()-t0:.0f}s -> {OUT}", flush=True)
