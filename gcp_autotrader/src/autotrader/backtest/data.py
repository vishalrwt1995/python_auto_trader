"""Historical data loaders for the backtester.

Three sources, in order of preference:
  1. **BigQuery `candles_5m` / `candles_1d`** — primary (clustered by symbol,
     partitioned by trade_date). Cheap when filtered by date + symbol.
  2. **GCS candle cache** (`history/{tf}/{exchange}/{segment}/{sym}.json`) —
     fallback when BQ doesn't have a window (older than 5m retention) or
     is unavailable in the test environment.
  3. **Live Upstox API** — last-resort when neither cache has the bar.
     Only used when explicitly opted in via `allow_live_fetch=True`.

The backtester never falls through to live fetch by default — historical
runs must be reproducible, and a live fetch hides drift.

Data window today (verified 2026-05-04 in BQ):
    candles_1d           : 2016-04-27 → 2026-04-30 (10 years)
    candles_5m           : 2026-01-30 → 2026-04-30 (~3 months)
    scan_decisions       : 2026-04-10 → 2026-05-04 (90-day TTL)
    market_brain_history : 2026-04-02 → 2026-05-04
    trades               : 2026-04-16 → 2026-05-04
"""
from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Iterable

from autotrader.backtest.types import Bar

log = logging.getLogger(__name__)


# ── BQ client (lazy, optional dependency) ─────────────────────────────────


def _bq_client(project: str) -> Any:
    """Lazily construct a BQ client. Raises if google-cloud-bigquery is missing —
    backtests need real data, this is not an optional dep."""
    from google.cloud import bigquery  # type: ignore[import-untyped]

    return bigquery.Client(project=project)


def _query(project: str, sql: str) -> list[dict[str, Any]]:
    client = _bq_client(project)
    job = client.query(sql)
    return [dict(row) for row in job.result()]


# ── Candle loaders ────────────────────────────────────────────────────────


@dataclass
class CandleQuery:
    """Specifies a candle window. `timeframe` is one of:
        "5m"  — read candles_5m raw
        "15m" — read candles_5m and aggregate (3 × 5m)
        "1d"  — read candles_1d
    """
    symbol: str
    timeframe: str           # "5m" | "15m" | "1d"
    since: str               # YYYY-MM-DD inclusive
    until: str               # YYYY-MM-DD inclusive


def load_candles_bq(
    *,
    project: str,
    dataset: str,
    query: CandleQuery,
) -> list[Bar]:
    """Load candles from BigQuery for one symbol over one window.

    For 15m: pulls 5m bars, aggregates by floor-15min IST. For 1d: reads
    candles_1d directly. For 5m: passes through.
    """
    sym = query.symbol.strip().upper()
    tf = query.timeframe.lower()
    if tf not in ("5m", "15m", "1d"):
        raise ValueError(f"unsupported timeframe: {tf}")

    if tf == "1d":
        sql = f"""
            SELECT
              FORMAT_DATE('%Y-%m-%dT00:00:00+05:30', trade_date) AS ts,
              open, high, low, close, volume
            FROM `{project}.{dataset}.candles_1d`
            WHERE symbol = '{sym}'
              AND trade_date BETWEEN '{query.since}' AND '{query.until}'
            ORDER BY trade_date
        """
        rows = _query(project, sql)
        return [
            Bar(symbol=sym, ts=str(r["ts"]), open=float(r["open"]),
                high=float(r["high"]), low=float(r["low"]),
                close=float(r["close"]), volume=float(r["volume"]),
                timeframe="1d")
            for r in rows
        ]

    if tf == "5m":
        sql = f"""
            SELECT
              FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S+05:30', candle_ts, 'Asia/Kolkata') AS ts,
              open, high, low, close, volume
            FROM `{project}.{dataset}.candles_5m`
            WHERE symbol = '{sym}'
              AND trade_date BETWEEN '{query.since}' AND '{query.until}'
            ORDER BY candle_ts
        """
        rows = _query(project, sql)
        return [
            Bar(symbol=sym, ts=str(r["ts"]), open=float(r["open"]),
                high=float(r["high"]), low=float(r["low"]),
                close=float(r["close"]), volume=float(r["volume"]),
                timeframe="5m")
            for r in rows
        ]

    # 15m: aggregate 3 × 5m → 15m. Group by 15-minute window in IST.
    sql = f"""
        WITH src AS (
          SELECT
            candle_ts,
            TIMESTAMP_TRUNC(candle_ts, MINUTE, 'Asia/Kolkata') AS minute_ist,
            open, high, low, close, volume
          FROM `{project}.{dataset}.candles_5m`
          WHERE symbol = '{sym}'
            AND trade_date BETWEEN '{query.since}' AND '{query.until}'
        ),
        bucketed AS (
          SELECT
            TIMESTAMP_SECONDS(
              CAST(FLOOR(UNIX_SECONDS(candle_ts) / 900) * 900 AS INT64)
            ) AS bucket_ts,
            candle_ts, open, high, low, close, volume
          FROM src
        )
        SELECT
          FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S+05:30', bucket_ts, 'Asia/Kolkata') AS ts,
          ARRAY_AGG(STRUCT(candle_ts, open) ORDER BY candle_ts ASC LIMIT 1)[OFFSET(0)].open AS open,
          MAX(high) AS high,
          MIN(low) AS low,
          ARRAY_AGG(STRUCT(candle_ts, close) ORDER BY candle_ts DESC LIMIT 1)[OFFSET(0)].close AS close,
          SUM(volume) AS volume,
          COUNT(*) AS n_5m
        FROM bucketed
        GROUP BY bucket_ts
        HAVING n_5m >= 1
        ORDER BY bucket_ts
    """
    rows = _query(project, sql)
    return [
        Bar(symbol=sym, ts=str(r["ts"]), open=float(r["open"]),
            high=float(r["high"]), low=float(r["low"]),
            close=float(r["close"]), volume=float(r["volume"]),
            timeframe="15m")
        for r in rows
    ]


def load_candles_bulk_bq(
    *,
    project: str,
    dataset: str,
    symbols: list[str],
    timeframe: str,
    since: str,
    until: str,
) -> dict[str, list[Bar]]:
    """Bulk-load candles for many symbols in a single BQ scan. The clustered
    `symbol` column makes this dramatically cheaper than N independent queries.

    Returns: {symbol: [Bar, ...] sorted by ts}
    """
    tf = timeframe.lower()
    if not symbols:
        return {}
    sym_list = ",".join(f"'{s.strip().upper()}'" for s in symbols)

    if tf == "1d":
        sql = f"""
            SELECT
              symbol,
              FORMAT_DATE('%Y-%m-%dT00:00:00+05:30', trade_date) AS ts,
              open, high, low, close, volume
            FROM `{project}.{dataset}.candles_1d`
            WHERE symbol IN ({sym_list})
              AND trade_date BETWEEN '{since}' AND '{until}'
            ORDER BY symbol, trade_date
        """
    elif tf == "5m":
        sql = f"""
            SELECT
              symbol,
              FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S+05:30', candle_ts, 'Asia/Kolkata') AS ts,
              open, high, low, close, volume
            FROM `{project}.{dataset}.candles_5m`
            WHERE symbol IN ({sym_list})
              AND trade_date BETWEEN '{since}' AND '{until}'
            ORDER BY symbol, candle_ts
        """
    elif tf == "15m":
        sql = f"""
            WITH src AS (
              SELECT
                symbol,
                TIMESTAMP_SECONDS(
                  CAST(FLOOR(UNIX_SECONDS(candle_ts) / 900) * 900 AS INT64)
                ) AS bucket_ts,
                candle_ts, open, high, low, close, volume
              FROM `{project}.{dataset}.candles_5m`
              WHERE symbol IN ({sym_list})
                AND trade_date BETWEEN '{since}' AND '{until}'
            )
            SELECT
              symbol,
              FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S+05:30', bucket_ts, 'Asia/Kolkata') AS ts,
              ARRAY_AGG(STRUCT(candle_ts, open) ORDER BY candle_ts ASC LIMIT 1)[OFFSET(0)].open AS open,
              MAX(high) AS high,
              MIN(low) AS low,
              ARRAY_AGG(STRUCT(candle_ts, close) ORDER BY candle_ts DESC LIMIT 1)[OFFSET(0)].close AS close,
              SUM(volume) AS volume
            FROM src
            GROUP BY symbol, bucket_ts
            ORDER BY symbol, bucket_ts
        """
    else:
        raise ValueError(f"unsupported timeframe: {tf}")

    rows = _query(project, sql)
    out: dict[str, list[Bar]] = defaultdict(list)
    for r in rows:
        sym = str(r["symbol"]).upper()
        out[sym].append(Bar(
            symbol=sym, ts=str(r["ts"]), open=float(r["open"]),
            high=float(r["high"]), low=float(r["low"]),
            close=float(r["close"]), volume=float(r["volume"]),
            timeframe=tf,
        ))
    return dict(out)


# ── Decision / signal loaders ─────────────────────────────────────────────


@dataclass
class ScanDecisionRow:
    """One row from `scan_decisions` — the live system's record of what it
    saw and decided at scan-tick T. Used by `replay_live` to drive sim
    execution from real historical signals."""
    scan_ts: str          # ISO-8601 IST
    run_date: str
    symbol: str
    setup: str
    direction: str
    raw_score: float
    adjusted_score: float
    min_score: float
    qualified: bool
    blocked_reason: str
    ltp: float
    atr: float
    atr_mult: float
    rsi: float
    vwap: float
    regime: str
    risk_mode: str
    wl_type: str
    daily_trend: str


def load_scan_decisions(
    *,
    project: str,
    dataset: str,
    since: str,
    until: str,
    qualified_only: bool = False,
    setups: list[str] | None = None,
    symbols: list[str] | None = None,
) -> list[ScanDecisionRow]:
    """Pull every scanner decision in [since, until]. Filters are SARGable
    (run on BQ, not in Python). Reverse-chrono sort kept by `ORDER BY scan_ts`."""
    where = [
        f"run_date BETWEEN '{since}' AND '{until}'",
    ]
    if qualified_only:
        where.append("qualified = TRUE")
    if setups:
        s = ",".join(f"'{x.strip().upper()}'" for x in setups)
        where.append(f"setup IN ({s})")
    if symbols:
        s = ",".join(f"'{x.strip().upper()}'" for x in symbols)
        where.append(f"symbol IN ({s})")
    sql = f"""
        SELECT
          FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S+05:30', scan_ts, 'Asia/Kolkata') AS scan_ts,
          FORMAT_DATE('%Y-%m-%d', run_date) AS run_date,
          symbol, setup, direction, raw_score, adjusted_score, min_score,
          qualified, IFNULL(blocked_reason, '') AS blocked_reason,
          ltp, atr, atr_mult, rsi, vwap, regime, risk_mode, wl_type,
          IFNULL(daily_trend, 'NEUTRAL') AS daily_trend
        FROM `{project}.{dataset}.scan_decisions`
        WHERE {' AND '.join(where)}
        ORDER BY scan_ts
    """
    rows = _query(project, sql)
    return [
        ScanDecisionRow(
            scan_ts=str(r["scan_ts"]),
            run_date=str(r["run_date"]),
            symbol=str(r["symbol"]).upper(),
            setup=str(r["setup"] or "AUTO").upper(),
            direction=str(r["direction"]).upper(),
            raw_score=float(r.get("raw_score") or 0.0),
            adjusted_score=float(r.get("adjusted_score") or 0.0),
            min_score=float(r.get("min_score") or 0.0),
            qualified=bool(r["qualified"]),
            blocked_reason=str(r["blocked_reason"]),
            ltp=float(r.get("ltp") or 0.0),
            atr=float(r.get("atr") or 0.0),
            atr_mult=float(r.get("atr_mult") or 1.74),
            rsi=float(r.get("rsi") or 50.0),
            vwap=float(r.get("vwap") or 0.0),
            regime=str(r.get("regime") or "RANGE"),
            risk_mode=str(r.get("risk_mode") or "NORMAL"),
            wl_type=str(r.get("wl_type") or "intraday"),
            daily_trend=str(r.get("daily_trend") or "NEUTRAL"),
        )
        for r in rows
    ]


# ── Market brain history loader ───────────────────────────────────────────


@dataclass
class BrainSnapshot:
    asof_ts: str          # ISO-8601 IST
    run_date: str
    regime: str
    risk_mode: str
    market_confidence: float
    breadth_score: float
    trend_score: float
    breadth_confidence: float
    volatility_stress_score: float
    data_quality_score: float


def load_market_brain(
    *, project: str, dataset: str, since: str, until: str,
) -> list[BrainSnapshot]:
    sql = f"""
        SELECT
          FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S+05:30', asof_ts, 'Asia/Kolkata') AS asof_ts,
          FORMAT_DATE('%Y-%m-%d', run_date) AS run_date,
          regime, risk_mode,
          IFNULL(market_confidence, 50) AS market_confidence,
          IFNULL(breadth_score, 50) AS breadth_score,
          IFNULL(trend_score, 50) AS trend_score,
          IFNULL(breadth_confidence, 50) AS breadth_confidence,
          IFNULL(volatility_stress_score, 50) AS volatility_stress_score,
          IFNULL(data_quality_score, 50) AS data_quality_score
        FROM `{project}.{dataset}.market_brain_history`
        WHERE run_date BETWEEN '{since}' AND '{until}'
        ORDER BY asof_ts
    """
    rows = _query(project, sql)
    return [
        BrainSnapshot(
            asof_ts=str(r["asof_ts"]),
            run_date=str(r["run_date"]),
            regime=str(r["regime"]),
            risk_mode=str(r["risk_mode"]),
            market_confidence=float(r["market_confidence"]),
            breadth_score=float(r["breadth_score"]),
            trend_score=float(r["trend_score"]),
            breadth_confidence=float(r["breadth_confidence"]),
            volatility_stress_score=float(r["volatility_stress_score"]),
            data_quality_score=float(r["data_quality_score"]),
        )
        for r in rows
    ]


def brain_lookup(snapshots: list[BrainSnapshot]) -> "BrainTimeline":
    """Build a fast as-of lookup over brain snapshots."""
    return BrainTimeline(snapshots)


class BrainTimeline:
    """As-of lookup: given a timestamp, return the most recent BrainSnapshot
    at or before that ts. Used by replay strategies to feed regime/breadth
    into scoring or gates per bar."""

    def __init__(self, snaps: list[BrainSnapshot]) -> None:
        self._snaps = sorted(snaps, key=lambda s: s.asof_ts)

    def asof(self, ts: str) -> BrainSnapshot | None:
        if not self._snaps:
            return None
        # Binary search would be O(log n) but linear is fine for ≤5k snaps.
        last: BrainSnapshot | None = None
        for s in self._snaps:
            if s.asof_ts > ts:
                break
            last = s
        return last


# ── Trades ground-truth loader (for validation) ───────────────────────────


def load_trades_truth(
    *, project: str, dataset: str, since: str, until: str,
) -> list[dict[str, Any]]:
    sql = f"""
        SELECT
          FORMAT_DATE('%Y-%m-%d', trade_date) AS trade_date,
          position_tag, symbol, side, qty,
          entry_price, exit_price, sl_price, target,
          pnl, IFNULL(net_pnl, pnl) AS net_pnl, exit_reason, strategy,
          FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S+05:30', entry_ts, 'Asia/Kolkata') AS entry_ts,
          FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S+05:30', exit_ts, 'Asia/Kolkata') AS exit_ts,
          hold_minutes, regime, risk_mode, signal_score
        FROM `{project}.{dataset}.trades`
        WHERE trade_date BETWEEN '{since}' AND '{until}'
        ORDER BY entry_ts
    """
    return _query(project, sql)


__all__ = [
    "CandleQuery",
    "load_candles_bq",
    "load_candles_bulk_bq",
    "ScanDecisionRow",
    "load_scan_decisions",
    "BrainSnapshot",
    "BrainTimeline",
    "load_market_brain",
    "brain_lookup",
    "load_trades_truth",
]
