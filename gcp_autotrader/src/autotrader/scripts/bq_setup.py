"""One-time BigQuery dataset + table setup script.

Usage:
    python -m autotrader.scripts.bq_setup --project grow-profit-machine --dataset autotrader

Run this once before deploying the trading system. Safe to re-run (uses
create_if_needed semantics via the schema update flag).
"""
from __future__ import annotations

import argparse
import sys


TABLES: dict[str, list[dict]] = {
    "trades": [
        {"name": "trade_date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "position_tag", "type": "STRING", "mode": "NULLABLE"},
        {"name": "symbol", "type": "STRING", "mode": "NULLABLE"},
        {"name": "side", "type": "STRING", "mode": "NULLABLE"},
        {"name": "qty", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "entry_price", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "exit_price", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "sl_price", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "target", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "pnl", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "pnl_pct", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "exit_reason", "type": "STRING", "mode": "NULLABLE"},
        {"name": "strategy", "type": "STRING", "mode": "NULLABLE"},
        {"name": "entry_ts", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "exit_ts", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "hold_minutes", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "regime", "type": "STRING", "mode": "NULLABLE"},
        {"name": "risk_mode", "type": "STRING", "mode": "NULLABLE"},
        {"name": "market_confidence", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "signal_score", "type": "INTEGER", "mode": "NULLABLE"},
    ],
    "signals": [
        {"name": "scan_ts", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "run_date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "symbol", "type": "STRING", "mode": "NULLABLE"},
        {"name": "direction", "type": "STRING", "mode": "NULLABLE"},
        {"name": "score", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "ltp", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "sl", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "target", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "qty", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "regime", "type": "STRING", "mode": "NULLABLE"},
        {"name": "risk_mode", "type": "STRING", "mode": "NULLABLE"},
        {"name": "entry_placed", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "blocked_reason", "type": "STRING", "mode": "NULLABLE"},
        {"name": "scanner_run_id", "type": "STRING", "mode": "NULLABLE"},
    ],
    "market_brain_history": [
        {"name": "asof_ts", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "run_date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "regime", "type": "STRING", "mode": "NULLABLE"},
        {"name": "risk_mode", "type": "STRING", "mode": "NULLABLE"},
        {"name": "participation", "type": "STRING", "mode": "NULLABLE"},
        {"name": "market_confidence", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "breadth_confidence", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "leadership_confidence", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "trend_score", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "breadth_score", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "volatility_stress_score", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "data_quality_score", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "selected_watchlist_count", "type": "INTEGER", "mode": "NULLABLE"},
    ],
    "candles_1d": [
        {"name": "trade_date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "symbol", "type": "STRING", "mode": "NULLABLE"},
        {"name": "exchange", "type": "STRING", "mode": "NULLABLE"},
        {"name": "segment", "type": "STRING", "mode": "NULLABLE"},
        {"name": "open", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "high", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "low", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "close", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "volume", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "instrument_key", "type": "STRING", "mode": "NULLABLE"},
    ],
    "candles_5m": [
        {"name": "candle_ts", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "trade_date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "symbol", "type": "STRING", "mode": "NULLABLE"},
        {"name": "open", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "high", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "low", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "close", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "volume", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "instrument_key", "type": "STRING", "mode": "NULLABLE"},
    ],
    "audit_log": [
        {"name": "log_ts", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "run_date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "module", "type": "STRING", "mode": "NULLABLE"},
        {"name": "action", "type": "STRING", "mode": "NULLABLE"},
        {"name": "status", "type": "STRING", "mode": "NULLABLE"},
        {"name": "message", "type": "STRING", "mode": "NULLABLE"},
        {"name": "context", "type": "JSON", "mode": "NULLABLE"},
        {"name": "exec_id", "type": "STRING", "mode": "NULLABLE"},
    ],
    "watchlist_history": [
        {"name": "generated_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "run_date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "regime", "type": "STRING", "mode": "NULLABLE"},
        {"name": "risk_mode", "type": "STRING", "mode": "NULLABLE"},
        {"name": "selected", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "symbols", "type": "STRING", "mode": "REPEATED"},
    ],
}

# Tables partitioned by a date column
PARTITION_BY: dict[str, str] = {
    "trades": "trade_date",
    "signals": "run_date",
    "market_brain_history": "run_date",
    "candles_1d": "trade_date",
    "candles_5m": "trade_date",
    "audit_log": "run_date",
    "watchlist_history": "run_date",
}

# Tables with clustering columns
CLUSTER_BY: dict[str, list[str]] = {
    "candles_1d": ["symbol"],
    "candles_5m": ["symbol"],
    "trades": ["symbol"],
    "signals": ["symbol"],
}


def setup(project_id: str, dataset: str, location: str = "asia-south1") -> None:
    from google.cloud import bigquery  # type: ignore[import-untyped]

    client = bigquery.Client(project=project_id)
    dataset_ref = f"{project_id}.{dataset}"

    # Create dataset
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset already exists: {dataset_ref}")
    except Exception:
        ds = bigquery.Dataset(dataset_ref)
        ds.location = location
        client.create_dataset(ds, exists_ok=True)
        print(f"Created dataset: {dataset_ref}")

    # Create tables
    for table_name, schema_dicts in TABLES.items():
        table_id = f"{dataset_ref}.{table_name}"
        schema = [
            bigquery.SchemaField(f["name"], f["type"], mode=f.get("mode", "NULLABLE"))
            for f in schema_dicts
        ]
        table = bigquery.Table(table_id, schema=schema)

        # Partitioning
        if table_name in PARTITION_BY:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=PARTITION_BY[table_name],
            )

        # Clustering
        if table_name in CLUSTER_BY:
            table.clustering_fields = CLUSTER_BY[table_name]

        try:
            client.get_table(table_id)
            print(f"  Table already exists: {table_name}")
        except Exception:
            client.create_table(table, exists_ok=True)
            print(f"  Created table: {table_name}")

    print(f"\nBigQuery setup complete for project={project_id} dataset={dataset}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Create autotrader BigQuery dataset and tables")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--dataset", default="autotrader", help="BigQuery dataset name")
    parser.add_argument("--location", default="asia-south1", help="Dataset location")
    args = parser.parse_args()
    setup(args.project, args.dataset, args.location)


if __name__ == "__main__":
    main()
