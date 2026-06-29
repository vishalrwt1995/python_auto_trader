"""Pull survivorship-free, split/bonus-adjusted daily bars for the swing candidate
set (bt_swing_candidates) into a local pickle cache for the prod-faithful harness.

Single-process I/O only (CLAUDE.md local-compute rule). Resumable: skips if cache
exists. Source of truth = BQ bt_bhavcopy_adj (adjusted) + bt_swing_candidates.
"""
from __future__ import annotations

import collections
import os
import pickle
import sys
import time

from google.cloud import bigquery
from google.oauth2.credentials import Credentials

CACHE = os.path.expanduser("~/.autotrader_backtest_cache/swing_adj_bars.pkl")
PROJECT = "grow-profit-machine"


def main() -> None:
    if os.path.exists(CACHE) and "--force" not in sys.argv:
        sz = os.path.getsize(CACHE) / 1e6
        print(f"cache exists ({sz:.0f} MB) -> {CACHE}  (use --force to repull)")
        return
    tok = os.environ.get("CLOUDSDK_AUTH_ACCESS_TOKEN")
    if not tok:
        sys.exit("set CLOUDSDK_AUTH_ACCESS_TOKEN")
    client = bigquery.Client(project=PROJECT, credentials=Credentials(token=tok), location="asia-south1")
    q = f"""
      SELECT a.symbol, CAST(a.date AS STRING) d, a.open, a.high, a.low, a.close, a.volume
      FROM `{PROJECT}.autotrader.bt_bhavcopy_adj` a
      JOIN `{PROJECT}.autotrader.bt_swing_candidates` c USING(symbol)
      WHERE a.date >= '2014-01-01'
      ORDER BY a.symbol, a.date
    """
    print("querying adjusted candidate bars (2014-01 -> 2026) ...")
    t0 = time.time()
    job = client.query(q)
    bysym: dict[str, list] = collections.defaultdict(list)
    n = 0
    for r in job.result(page_size=100000):
        bysym[r["symbol"]].append([r["d"], float(r["open"] or 0.0), float(r["high"] or 0.0),
                                   float(r["low"] or 0.0), float(r["close"] or 0.0), float(r["volume"] or 0.0)])
        n += 1
        if n % 250000 == 0:
            print(f"  {n:,} rows  ({time.time()-t0:.0f}s)")
    os.makedirs(os.path.dirname(CACHE), exist_ok=True)
    pickle.dump(dict(bysym), open(CACHE, "wb"), protocol=pickle.HIGHEST_PROTOCOL)
    print(f"saved {n:,} bars across {len(bysym):,} symbols -> {CACHE}  ({time.time()-t0:.0f}s, "
          f"{os.path.getsize(CACHE)/1e6:.0f} MB)")


if __name__ == "__main__":
    main()
