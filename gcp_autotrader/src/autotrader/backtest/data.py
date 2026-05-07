"""Historical data loaders for the backtester.

Two candle sources, with **GCS preferred** because it is the canonical
store the live system reads from at scan time:

  1. **GCS candle cache** — `load_candles_bulk_gcs()`
       1d : `cache/score_1d/{ex}/{seg}/{sym}.json`     (full history)
       5m : `cache/candles/5m/{ex}/{seg}/{sym}.json`   (rolling ~5 months)
       15m: `cache/candles/15m/{ex}/{seg}/{sym}.json`  (rolling)
     Same JSON the live `score_signal()` reads → backtests are faithful by
     construction. No BQ archival lag, no streaming-insert window limits.

  2. **BigQuery `candles_5m` / `candles_1d`** — `load_candles_bulk_bq()`
     Best-effort dual-write of the GCS cache; partitioned + clustered for
     SQL joins (e.g. slippage calibration). Coverage is incomplete for some
     symbols / dates, so prefer GCS for replay.

The backtester never falls through to live Upstox fetch — historical runs
must be reproducible.

Data windows (verified 2026-05-05):
    GCS score_1d         : 2000-02-22 → today (full)
    GCS candles/5m       : ~2025-12-04 → today (last ~5 months, rolling)
    GCS candles/15m      : ~2026-04-02 → today
    BQ candles_1d        : 2016-04-27 → 2026-04-30 (incomplete coverage)
    BQ candles_5m        : 2026-01-30 → 2026-04-30 (incomplete coverage)
    BQ scan_decisions    : 2026-04-10 → 2026-05-04 (90-day TTL)
    BQ market_brain      : 2026-04-02 → 2026-05-04
    BQ trades            : 2026-04-16 → 2026-05-04
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
    """Lazily construct a BQ client and resize its underlying urllib3 pool.

    Why the resize
    --------------
    The default urllib3 connection pool inside `AuthorizedSession` is 10.
    Backtests do dozens of concurrent BQ + GCS calls (candle loaders,
    scan_decisions, market_brain, daily candles), and oauth refreshes
    spike fan-out further. Hitting the 10-conn limit produces noisy
    "Connection pool is full, discarding connection: oauth2.googleapis.com"
    warnings + a fresh TLS handshake per discarded conn = wasted seconds
    and confused logs. 64 covers our peak fan-out with headroom.

    Raises if google-cloud-bigquery is missing — backtests need real data.
    """
    from google.cloud import bigquery  # type: ignore[import-untyped]

    client = bigquery.Client(project=project)
    try:
        from requests.adapters import HTTPAdapter
        session = client._http  # AuthorizedSession (subclass of Session)
        adapter = HTTPAdapter(pool_connections=64, pool_maxsize=64)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
    except Exception:
        # Best-effort: an SDK rev that hides _http or refactors transports
        # will fall back to the old warnings without breaking the query.
        pass
    return client


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


# ── GCS candle loader (canonical source — preferred over BQ) ──────────────


_DEFAULT_GCS_BUCKET = "grow-profit-machine-autotrader-data"


def _gcs_path_for(timeframe: str, symbol: str, exchange: str, segment: str) -> str:
    """Mirror live writers in `universe_service`:
        1d  → cache/score_1d/{ex}/{seg}/{sym}.json
        5m  → cache/candles/5m/{ex}/{seg}/{sym}.json
        15m → cache/candles/15m/{ex}/{seg}/{sym}.json
    """
    sym = symbol.strip().upper()
    ex = exchange.strip().upper()
    seg = segment.strip().upper()
    tf = timeframe.lower()
    if tf == "1d":
        return f"cache/score_1d/{ex}/{seg}/{sym}.json"
    if tf in ("5m", "15m"):
        return f"cache/candles/{tf}/{ex}/{seg}/{sym}.json"
    raise ValueError(f"unsupported timeframe for GCS loader: {tf}")


def _aggregate_5m_to_15m(bars_5m: list[Bar]) -> list[Bar]:
    """Aggregate 5m bars into 15m by IST 15-minute bucket. Used as a
    fallback when the 15m cache file is missing for a symbol."""
    if not bars_5m:
        return []
    by_bucket: dict[str, list[Bar]] = defaultdict(list)
    for b in bars_5m:
        # Bucket key: floor minute to nearest 15m. Bar.ts is "YYYY-MM-DDTHH:MM:SS+05:30"
        try:
            hh, mm = int(b.ts[11:13]), int(b.ts[14:16])
        except (ValueError, IndexError):
            continue
        bucket_min = (mm // 15) * 15
        bucket = f"{b.ts[:11]}{hh:02d}:{bucket_min:02d}:00+05:30"
        by_bucket[bucket].append(b)
    out: list[Bar] = []
    for bucket_ts in sorted(by_bucket.keys()):
        members = sorted(by_bucket[bucket_ts], key=lambda x: x.ts)
        out.append(Bar(
            symbol=members[0].symbol,
            ts=bucket_ts,
            open=members[0].open,
            high=max(m.high for m in members),
            low=min(m.low for m in members),
            close=members[-1].close,
            volume=sum(m.volume for m in members),
            timeframe="15m",
        ))
    return out


def load_candles_bulk_gcs(
    *,
    symbols: list[str],
    timeframe: str,                # "5m" | "15m" | "1d"
    since: str,                    # YYYY-MM-DD inclusive (compared on ts[:10])
    until: str,                    # YYYY-MM-DD inclusive
    bucket: str = _DEFAULT_GCS_BUCKET,
    exchange: str = "NSE",
    segment: str = "CASH",
    concurrency: int = 32,
    aggregate_15m_from_5m: bool = True,
) -> dict[str, list[Bar]]:
    """Bulk-load candles from GCS for many symbols.

    Reads the same JSON files the live system writes (`cache/score_1d/...`
    and `cache/candles/{tf}/...`), filters to the requested date window in
    Python, and returns the same shape as `load_candles_bulk_bq()`.

    For 15m: if the per-symbol 15m cache file is missing, falls back to
    aggregating from 5m (matches the BQ loader's behavior).

    Concurrency: ThreadPoolExecutor with `concurrency` workers — GCS reads
    are network-bound, so high parallelism is fine.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    from autotrader.adapters.gcs_store import GoogleCloudStorageStore

    tf = timeframe.lower()
    if tf not in ("5m", "15m", "1d"):
        raise ValueError(f"unsupported timeframe: {tf}")
    if not symbols:
        return {}

    gcs = GoogleCloudStorageStore(bucket_name=bucket)
    syms_clean = sorted({s.strip().upper() for s in symbols if s and s.strip()})

    def _fetch_one(sym: str) -> tuple[str, list[Bar]]:
        primary = _gcs_path_for(tf, sym, exchange, segment)
        try:
            raw = gcs.read_candles(primary)
        except Exception:
            log.warning("gcs_candles_read_failed sym=%s tf=%s path=%s",
                        sym, tf, primary, exc_info=True)
            raw = []

        if not raw and tf == "15m" and aggregate_15m_from_5m:
            # Fallback: aggregate from 5m
            try:
                five_min_bars = _read_window_as_bars(
                    gcs, sym, "5m", since, until, exchange, segment,
                )
                return sym, _aggregate_5m_to_15m(five_min_bars)
            except Exception:
                log.warning("gcs_15m_aggregate_failed sym=%s",
                            sym, exc_info=True)
                return sym, []

        return sym, _filter_and_box(raw, sym, tf, since, until)

    out: dict[str, list[Bar]] = {}
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futs = [pool.submit(_fetch_one, s) for s in syms_clean]
        for fut in as_completed(futs):
            sym, bars = fut.result()
            if bars:
                out[sym] = bars
    return out


def _read_window_as_bars(
    gcs: Any,
    symbol: str,
    timeframe: str,
    since: str,
    until: str,
    exchange: str,
    segment: str,
) -> list[Bar]:
    path = _gcs_path_for(timeframe, symbol, exchange, segment)
    raw = gcs.read_candles(path)
    return _filter_and_box(raw, symbol, timeframe, since, until)


def _filter_and_box(
    raw: list,
    symbol: str,
    timeframe: str,
    since: str,
    until: str,
) -> list[Bar]:
    """Convert raw `[ts, o, h, l, c, v]` rows from GCS into Bar list,
    filtered to [since, until] inclusive on the date portion of ts."""
    out: list[Bar] = []
    for c in raw:
        if not isinstance(c, (list, tuple)) or len(c) < 6:
            continue
        ts = str(c[0])
        d = ts[:10]
        if d < since or d > until:
            continue
        try:
            out.append(Bar(
                symbol=symbol,
                ts=ts,
                open=float(c[1]),
                high=float(c[2]),
                low=float(c[3]),
                close=float(c[4]),
                volume=float(c[5]),
                timeframe=timeframe,
            ))
        except (TypeError, ValueError):
            continue
    out.sort(key=lambda b: b.ts)
    return out


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


# Setups produced by a SEPARATE scanner from the watchlist-driven main loop.
# They land in `scan_decisions` with the same schema but represent a different
# code path (the phase-1 picks scanner) and shouldn't be treated as watchlist
# strategy assignments. Including them poisons the per-day mapping with
# strategies live never actually trades on those (date, symbol)s.
_NON_WATCHLIST_SETUP_PREFIXES: tuple[str, ...] = ("PHASE1_", "SHORT_")


def build_watchlist_per_day(
    decisions: list[ScanDecisionRow],
) -> dict[tuple[str, str], str]:
    """Distil `scan_decisions` rows into a `(run_date, symbol) → setup` map.

    Why this exists
    ---------------
    Live's watchlist is read from Firestore on every scan tick (every 5min)
    and the strategy assignment can drift mid-day if Firestore is updated.
    The watchlist itself is NOT archived to BQ — but every scan tick that
    reads it persists `setup` on its `scan_decisions` row, so we can
    reconstruct the dominant per-day mapping by majority-voting those rows.

    Strategy
    --------
    1. Filter out PHASE1_* and SHORT_* setups — they're written by a
       separate scanner (phase-1 picks) that doesn't follow the watchlist's
       strategy assignment.
    2. Filter out AUTO / empty placeholders — these mean the Firestore row
       didn't specify a strategy; pure-replay should fall back to best-of-N
       rather than restrict to a fake "AUTO" setup.
    3. For each (date, symbol), pick the MOST-FREQUENT setup across all
       scan ticks that day. This matches live's actual behavior: the
       strategy that gets the most scan-tick airtime is the one the trader
       actually fires when the score qualifies.
    4. Ties broken alphabetically for determinism (so reruns of the same
       backtest produce identical mappings).

    Returns
    -------
    `{(YYYY-MM-DD, SYMBOL): SETUP}`. Empty if `decisions` is empty.

    Pure-replay uses this to restrict candidate setups per stock per day,
    closing the parity gap that arises when pure-replay tries every setup
    against every stock instead of the watchlist-assigned one.
    """
    from collections import Counter

    counts: dict[tuple[str, str], Counter] = {}
    for d in decisions:
        setup = (d.setup or "").strip().upper()
        if not setup or setup == "AUTO":
            continue
        if setup.startswith(_NON_WATCHLIST_SETUP_PREFIXES):
            continue
        key = (d.run_date, d.symbol.upper())
        counts.setdefault(key, Counter())[setup] += 1

    out: dict[tuple[str, str], str] = {}
    for key, counter in counts.items():
        # `most_common` returns items sorted by count desc; we then re-sort
        # the top-tier ties alphabetically for determinism.
        top_count = counter.most_common(1)[0][1]
        tied = sorted(s for s, c in counter.items() if c == top_count)
        out[key] = tied[0]
    return out


__all__ = [
    "CandleQuery",
    "load_candles_bq",
    "load_candles_bulk_bq",
    "load_candles_bulk_gcs",
    "ScanDecisionRow",
    "load_scan_decisions",
    "build_watchlist_per_day",
    "BrainSnapshot",
    "BrainTimeline",
    "load_market_brain",
    "brain_lookup",
    "load_trades_truth",
]
