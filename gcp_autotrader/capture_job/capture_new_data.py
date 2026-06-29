"""① New-data capture — daily snapshot archiver (run once per trading day).

Purpose: start accruing OUR OWN point-in-time history of live-only Upstox signals
(max-pain, PCR, change-OI for index underlyings) so they become BACKTESTABLE in a
few months. Captures the CURRENT/active expiry snapshot — historical backfill is
not possible for these (verified: expired expiries return null).

v1 scope (tested SDK signatures): index options positioning for Nifty 50 + Bank Nifty.
v2 TODO: FII/DII live (MarketApi.get_fii_data / get_dii_data — confirm sig on first run),
         ATM greeks/IV (MarketQuoteV3Api.get_market_quote_option_greek),
         market depth (MarketQuoteApi.get_full_market_quote, watchlist+index),
         news (NewsApi.get_news, watchlist/holdings).

Auth: UPSTOX_ACCESS_TOKEN = analytics token (read-only, 1yr).
Run:  PYTHONPATH=src UPSTOX_ACCESS_TOKEN=... python3 scripts/capture_new_data.py [YYYY-MM-DD]
Lands in BQ: grow-profit-machine.autotrader.capture_index_positioning
"""
from __future__ import annotations

import os
import sys
from datetime import date as _date

import upstox_client
from google.cloud import bigquery

PROJECT = "grow-profit-machine"
TABLE = f"{PROJECT}.autotrader.capture_index_positioning"
UNDERLYINGS = {"NIFTY 50": "NSE_INDEX|Nifty 50", "NIFTY BANK": "NSE_INDEX|Nifty Bank"}

SCHEMA = [
    bigquery.SchemaField("capture_date", "DATE"),
    bigquery.SchemaField("underlying", "STRING"),
    bigquery.SchemaField("instrument_key", "STRING"),
    bigquery.SchemaField("expiry", "STRING"),
    bigquery.SchemaField("max_pain", "FLOAT"),
    bigquery.SchemaField("spot_closing_price", "FLOAT"),
    bigquery.SchemaField("pcr", "FLOAT"),
    bigquery.SchemaField("total_call_change_oi", "FLOAT"),
    bigquery.SchemaField("total_put_change_oi", "FLOAT"),
    bigquery.SchemaField("captured_at", "TIMESTAMP"),
]


def _client() -> upstox_client.ApiClient:
    cfg = upstox_client.Configuration()
    cfg.access_token = os.environ["UPSTOX_ACCESS_TOKEN"]
    return upstox_client.ApiClient(cfg)


def _ensure_table(bq: bigquery.Client) -> None:
    try:
        bq.get_table(TABLE)
    except Exception:
        t = bigquery.Table(TABLE, schema=SCHEMA)
        t.time_partitioning = bigquery.TimePartitioning(field="capture_date")
        bq.create_table(t)
        print(f"created {TABLE}")


def _nearest_expiry(opt: upstox_client.OptionsApi, ik: str) -> str | None:
    try:
        exps = sorted({c.expiry for c in opt.get_option_contracts(instrument_key=ik).data})
        if not exps:
            return None
        e = exps[0]
        return e.strftime("%Y-%m-%d") if hasattr(e, "strftime") else str(e)[:10]
    except Exception as exc:
        print(f"  expiry fetch failed {ik}: {exc}")
        return None


def _d(resp) -> dict:
    try:
        return (resp.to_dict() or {}).get("data") or {}
    except Exception:
        return {}


def capture(run_date: str) -> list[dict]:
    client = _client()
    m = upstox_client.MarketApi(client)
    opt = upstox_client.OptionsApi(client)
    rows: list[dict] = []
    for name, ik in UNDERLYINGS.items():
        expiry = _nearest_expiry(opt, ik)
        if not expiry:
            print(f"  {name}: no expiry, skipping"); continue
        mp = _d(m.get_max_pain_data(ik, expiry, run_date, 30))
        pc = _d(m.get_pcr_data(ik, expiry, run_date, 30))
        co = _d(m.get_change_oi_data(ik, expiry, run_date, 30))
        rows.append({
            "capture_date": run_date,
            "underlying": name,
            "instrument_key": ik,
            "expiry": expiry,
            "max_pain": mp.get("max_pain"),
            "spot_closing_price": mp.get("spot_closing_price") or pc.get("spot_closing_price"),
            "pcr": pc.get("pcr"),
            "total_call_change_oi": co.get("total_call_change_oi"),
            "total_put_change_oi": co.get("total_put_change_oi"),
            "captured_at": None,  # set server-side below
        })
        print(f"  {name} exp={expiry} max_pain={mp.get('max_pain')} pcr={pc.get('pcr')}")
    return rows


def main() -> int:
    run_date = sys.argv[1] if len(sys.argv) > 1 else _date.today().isoformat()
    print(f"capturing index positioning for {run_date} ...")
    rows = capture(run_date)
    if not rows:
        print("no rows captured"); return 1
    bq = bigquery.Client(project=PROJECT)
    _ensure_table(bq)
    # idempotent: drop today's rows first (load job → immediately mutable), then append
    bq.query(f"DELETE FROM `{TABLE}` WHERE capture_date='{run_date}'", location="asia-south1").result()
    for r in rows:
        r["captured_at"] = None
    job = bq.load_table_from_json(
        rows, TABLE,
        job_config=bigquery.LoadJobConfig(schema=SCHEMA, write_disposition="WRITE_APPEND"),
        location="asia-south1",
    )
    job.result()
    # stamp captured_at
    bq.query(f"UPDATE `{TABLE}` SET captured_at=CURRENT_TIMESTAMP() "
             f"WHERE capture_date='{run_date}' AND captured_at IS NULL", location="asia-south1").result()
    print(f"wrote {len(rows)} rows to {TABLE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
