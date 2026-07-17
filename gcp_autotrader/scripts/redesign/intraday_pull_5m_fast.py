"""FAST 5m full-universe pull via the BigQuery Storage Read API (gRPC streaming, ~10-50x REST).

Uses the EXACT same query text as the prior run so it's a CACHE HIT => ₹0 scan (ran <24h ago).
Storage-API streams Arrow RecordBatches (bounded RAM), routed to 50 hash-bucket shards written
incrementally. bar_ts is converted UTC->IST here (harness expects '%Y-%m-%dT%H:%M:%S+05:30').
Cost: ₹0 scan (cache) + ~₹2 Storage read; verified after via INFORMATION_SCHEMA. No laptop-hang
(streamed + sharded). READ-ONLY BQ. Bucket format: repeated pickle.dump(batch) -> read w/ pickle.load
until EOFError. Row: (symbol, ts_ist_iso, trade_date, open, high, low, close, volume)."""
import os, pickle, time
from datetime import timedelta, timezone
from google.cloud import bigquery, bigquery_storage

OUT = os.path.expanduser("~/.autotrader_grind_cache/intraday_5m")
os.makedirs(OUT, exist_ok=True)
NB = 50
IST = timezone(timedelta(hours=5, minutes=30))
# EXACT prior text (cache hit). Do not edit or it re-scans (₹5.7).
Q = ("SELECT symbol, bar_ts, trade_date, open, high, low, close, volume "
     "FROM `grow-profit-machine.autotrader.candles_5m_full`")

client = bigquery.Client(project="grow-profit-machine")
bqs = bigquery_storage.BigQueryReadClient()
print("running query (expect cache hit => ₹0 scan) ...", flush=True)
job = client.query(Q, location="asia-south1")
rows = job.result()
print(f"cache_hit={job.cache_hit} total_bytes_billed={ (job.total_bytes_billed or 0)/1e9:.2f}GB; "
      f"streaming via Storage API ...", flush=True)

files = [open(f"{OUT}/bucket_{i:02d}.pkl", "wb") for i in range(NB)]
bufs = [[] for _ in range(NB)]
def flush(i):
    if bufs[i]:
        pickle.dump(bufs[i], files[i]); bufs[i] = []
n = 0; t0 = time.time()
for batch in rows.to_arrow_iterable(bqstorage_client=bqs):
    d = batch.to_pydict()
    syms = d["symbol"]; ts = d["bar_ts"]; td = d["trade_date"]
    op = d["open"]; hi = d["high"]; lo = d["low"]; cl = d["close"]; vo = d["volume"]
    for k in range(len(syms)):
        s = syms[k]
        t = ts[k]  # tz-aware UTC datetime
        tist = t.astimezone(IST).strftime("%Y-%m-%dT%H:%M:%S+05:30") if t is not None else ""
        b = (hash(s) & 0x7fffffff) % NB
        bufs[b].append((s, tist, str(td[k]), float(op[k] or 0), float(hi[k] or 0),
                        float(lo[k] or 0), float(cl[k] or 0), float(vo[k] or 0)))
        if len(bufs[b]) >= 50000:
            flush(b)
    n += len(syms)
    if n % 10_000_000 < len(syms):
        print(f"  {n:,} rows, {time.time()-t0:.0f}s", flush=True)
for i in range(NB):
    flush(i); files[i].close()
print(f"DONE: {n:,} rows, {NB} buckets, {time.time()-t0:.0f}s -> {OUT}", flush=True)
