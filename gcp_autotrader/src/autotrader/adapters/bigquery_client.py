"""BigQuery adapter — analytics, trade history, candles, signals, audit log."""
from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)

# Lazily imported so the module loads even if google-cloud-bigquery is not installed
# in unit-test environments.
_bq_module: Any = None


def _bq():
    global _bq_module
    if _bq_module is None:
        from google.cloud import bigquery  # type: ignore[import-untyped]

        _bq_module = bigquery
    return _bq_module


class BigQueryClient:
    """Thin wrapper around the BigQuery streaming-insert API.

    All insert methods are best-effort: errors are logged but never raised so
    that a BigQuery outage never blocks the trading engine.
    """

    def __init__(self, project_id: str, dataset: str) -> None:
        self._project = project_id
        self._dataset = dataset
        self._client: Any = None

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _get_client(self) -> Any:
        if self._client is None:
            self._client = _bq().Client(project=self._project)
        return self._client

    def _table_ref(self, table: str) -> str:
        return f"{self._project}.{self._dataset}.{table}"

    def _insert(self, table: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        try:
            client = self._get_client()
            errors = client.insert_rows_json(self._table_ref(table), rows)
            if errors:
                log.warning("bq_insert_errors table=%s errors=%s", table, errors[:3])
        except Exception:
            log.exception("bq_insert_failed table=%s rows=%d", table, len(rows))

    # ------------------------------------------------------------------ #
    # Public insert methods
    # ------------------------------------------------------------------ #

    def insert_trade(self, trade: dict[str, Any]) -> None:
        """Record a completed trade (position closed)."""
        self._insert("trades", [trade])

    def insert_trades_batch(self, trades: list[dict[str, Any]]) -> None:
        """Batch-insert completed trades (used by migration script)."""
        self._insert("trades", trades)

    def insert_signal(self, signal: dict[str, Any]) -> None:
        """Record a scanner signal (entry placed or blocked)."""
        self._insert("signals", [signal])

    def insert_signals_batch(self, signals: list[dict[str, Any]]) -> None:
        """Batch-insert scanner signals (called at end of scan run)."""
        self._insert("signals", signals)

    def insert_market_brain(self, snapshot: dict[str, Any]) -> None:
        """Record a market-brain regime snapshot."""
        self._insert("market_brain_history", [snapshot])

    def insert_market_brain_batch(self, snapshots: list[dict[str, Any]]) -> None:
        """Batch-insert market-brain history (used by migration script)."""
        self._insert("market_brain_history", snapshots)

    def insert_watchlist_snapshot(self, snapshot: dict[str, Any]) -> None:
        """Record a watchlist generation event."""
        self._insert("watchlist_history", [snapshot])

    def insert_candles_1d_batch(self, rows: list[dict[str, Any]]) -> None:
        """Batch-insert daily OHLCV candles."""
        self._insert("candles_1d", rows)

    def insert_candles_5m_batch(self, rows: list[dict[str, Any]]) -> None:
        """Batch-insert 5-minute OHLCV candles."""
        self._insert("candles_5m", rows)

    def insert_audit_log(self, entry: dict[str, Any]) -> None:
        """Record a system audit-log entry."""
        self._insert("audit_log", [entry])

    def insert_audit_log_batch(self, entries: list[dict[str, Any]]) -> None:
        """Batch-insert audit-log entries."""
        self._insert("audit_log", entries)

    def insert_scan_decisions_batch(self, decisions: list[dict[str, Any]]) -> None:
        """Batch-insert per-symbol scan decisions (both qualified and rejected).

        Each row captures the full indicator snapshot, score breakdown, and
        rejection reason for every symbol evaluated in a scan cycle.  This
        gives a complete audit trail for tuning thresholds and weights.
        """
        self._insert("scan_decisions", decisions)

    # ------------------------------------------------------------------ #
    # Query helpers (for dashboard / reconcile reads)
    # ------------------------------------------------------------------ #

    def query(self, sql: str) -> list[dict[str, Any]]:
        """Run a SQL query and return rows as list of dicts.

        Best-effort: returns [] on error.
        """
        try:
            client = self._get_client()
            result = client.query(sql).result()
            return [dict(row) for row in result]
        except Exception:
            log.exception("bq_query_failed sql_prefix=%s", sql[:120])
            return []

    def query_trades(
        self,
        *,
        since_date: str | None = None,
        symbol: str | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """Convenience query for recent trades."""
        filters = []
        if since_date:
            filters.append(f"trade_date >= '{since_date}'")
        if symbol:
            filters.append(f"symbol = '{symbol.upper()}'")
        where = f"WHERE {' AND '.join(filters)}" if filters else ""
        sql = (
            f"SELECT * FROM `{self._table_ref('trades')}` "
            f"{where} ORDER BY entry_ts DESC LIMIT {limit}"
        )
        return self.query(sql)

    def query_pnl_summary(self, since_date: str) -> dict[str, Any]:
        """Return aggregate PnL stats since a given date."""
        sql = f"""
            SELECT
              COUNT(*) AS total_trades,
              COUNTIF(pnl > 0) AS winning_trades,
              COUNTIF(pnl <= 0) AS losing_trades,
              ROUND(SUM(pnl), 2) AS total_pnl,
              ROUND(AVG(pnl), 2) AS avg_pnl,
              ROUND(MAX(pnl), 2) AS best_trade,
              ROUND(MIN(pnl), 2) AS worst_trade
            FROM `{self._table_ref('trades')}`
            WHERE trade_date >= '{since_date}'
        """
        rows = self.query(sql)
        return rows[0] if rows else {}
