"""② + forward: fundamentals snapshot — pull the deepest history each endpoint
returns (4 annual financials, 4 holding quarters, latest ratios) tagged with a
`captured_date`. Run once now = the historical pull; run monthly (scheduled) =
forward-archive that accrues point-in-time history (look-ahead-free).

Lands in BQ:
  autotrader.fundamentals_ratios     (captured_date, isin, ratio, company_value, sector_value)
  autotrader.fundamentals_financials (captured_date, isin, statement, category, period, frequency, value, change)
  autotrader.fundamentals_holdings   (captured_date, isin, category, period, value)

Auth: UPSTOX_ACCESS_TOKEN = analytics token.
Run:  PYTHONPATH=... python3 capture_fundamentals.py [LIMIT]   (LIMIT = test on N ISINs)
"""
from __future__ import annotations

import os
import sys
import time
from datetime import date as _date

import upstox_client
from upstox_client.rest import ApiException
from google.cloud import bigquery

PROJECT = "grow-profit-machine"
DS = f"{PROJECT}.autotrader"
TODAY = _date.today().isoformat()

T_RATIOS = f"{DS}.fundamentals_ratios"
T_FIN = f"{DS}.fundamentals_financials"
T_HOLD = f"{DS}.fundamentals_holdings"

SCHEMAS = {
    T_RATIOS: [
        bigquery.SchemaField("captured_date", "DATE"),
        bigquery.SchemaField("isin", "STRING"),
        bigquery.SchemaField("ratio", "STRING"),
        bigquery.SchemaField("company_value", "STRING"),
        bigquery.SchemaField("sector_value", "STRING"),
    ],
    T_FIN: [
        bigquery.SchemaField("captured_date", "DATE"),
        bigquery.SchemaField("isin", "STRING"),
        bigquery.SchemaField("statement", "STRING"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("period", "STRING"),
        bigquery.SchemaField("frequency", "STRING"),
        bigquery.SchemaField("value", "FLOAT"),
        bigquery.SchemaField("change", "STRING"),
    ],
    T_HOLD: [
        bigquery.SchemaField("captured_date", "DATE"),
        bigquery.SchemaField("isin", "STRING"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("period", "STRING"),
        bigquery.SchemaField("value", "FLOAT"),
    ],
}


def _ensure(bq):
    for tbl, schema in SCHEMAS.items():
        try:
            bq.get_table(tbl)
        except Exception:
            t = bigquery.Table(tbl, schema=schema)
            t.time_partitioning = bigquery.TimePartitioning(field="captured_date")
            bq.create_table(t)
            print(f"created {tbl}")


def _data(resp):
    try:
        return (resp.to_dict() or {}).get("data")
    except Exception:
        return None


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def pull_isin(f, isin: str):
    ratios, fins, holds = [], [], []
    # ratios (latest)
    try:
        for r in (_data(f.get_key_ratios(isin)) or []):
            ratios.append({"captured_date": TODAY, "isin": isin, "ratio": r.get("name"),
                           "company_value": r.get("company_value"), "sector_value": r.get("sector_value")})
    except ApiException:
        pass
    # balance sheet (4 yearly): history of total_asset/total_liability
    try:
        d = _data(f.get_balance_sheet(isin)) or {}
        for h in (d.get("history") or []):
            for cat in ("total_asset", "total_liability"):
                if cat in h:
                    fins.append({"captured_date": TODAY, "isin": isin, "statement": "balance",
                                 "category": cat, "period": h.get("period"), "frequency": d.get("time_period"),
                                 "value": _num(h.get(cat)), "change": None})
    except ApiException:
        pass
    # income statement + cash flow: nested {category, history:[{value, period, change}]}
    for meth, key, stmt in ((f.get_income_statement, "income_statement", "income"),
                            (f.get_cash_flow, "cash_flow", "cashflow")):
        try:
            d = _data(meth(isin)) or {}
            for blk in (d.get(key) or []):
                cat = blk.get("category")
                for h in (blk.get("history") or []):
                    fins.append({"captured_date": TODAY, "isin": isin, "statement": stmt,
                                 "category": cat, "period": h.get("period"), "frequency": d.get("time_period"),
                                 "value": _num(h.get("value")), "change": h.get("change")})
        except ApiException:
            pass
    # share holdings (4 quarters): [{category, history:[{value, period}]}]
    try:
        for blk in (_data(f.get_share_holdings(isin)) or []):
            cat = blk.get("category")
            for h in (blk.get("history") or []):
                holds.append({"captured_date": TODAY, "isin": isin, "category": cat,
                              "period": h.get("period"), "value": _num(h.get("value"))})
    except ApiException:
        pass
    return ratios, fins, holds


def _flush(bq, buf):
    for tbl, rows in buf.items():
        if rows:
            bq.load_table_from_json(rows, tbl,
                job_config=bigquery.LoadJobConfig(schema=SCHEMAS[tbl], write_disposition="WRITE_APPEND"),
                location="asia-south1").result()
            rows.clear()


def main() -> int:
    limit = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else None
    cfg = upstox_client.Configuration(); cfg.access_token = os.environ["UPSTOX_ACCESS_TOKEN"]
    f = upstox_client.FundamentalsApi(upstox_client.ApiClient(cfg))
    bq = bigquery.Client(project=PROJECT)
    _ensure(bq)

    isins = [r["isin"] for r in bq.query(
        "SELECT DISTINCT isin FROM `grow-profit-machine.autotrader.bt_bhavcopy_adj` "
        "WHERE date >= '2026-01-01' AND isin IS NOT NULL AND series='EQ' ORDER BY isin",
        location="asia-south1").result()]
    # resumable: skip ISINs already captured TODAY
    done = {r["isin"] for r in bq.query(
        f"SELECT DISTINCT isin FROM `{T_RATIOS}` WHERE captured_date='{TODAY}'", location="asia-south1").result()} \
        if not limit else set()
    todo = [i for i in isins if i not in done]
    if limit:
        todo = todo[:limit]
    print(f"universe={len(isins)} done_today={len(done)} todo={len(todo)}", flush=True)

    buf = {T_RATIOS: [], T_FIN: [], T_HOLD: []}
    t0 = time.time()
    for i, isin in enumerate(todo):
        ra, fi, ho = pull_isin(f, isin)
        buf[T_RATIOS].extend(ra); buf[T_FIN].extend(fi); buf[T_HOLD].extend(ho)
        if (i + 1) % 50 == 0:
            _flush(bq, buf)
            print(f"  {i+1}/{len(todo)} ISINs ({time.time()-t0:.0f}s)", flush=True)
        time.sleep(1.0)  # ~5 calls/ISIN; pace under 2000/30min
    _flush(bq, buf)
    print(f"DONE {len(todo)} ISINs in {time.time()-t0:.0f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
