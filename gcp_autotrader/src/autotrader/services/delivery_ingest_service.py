"""DELIVERY-channel daily ingestion — NSE ``sec_bhavdata_full`` -> BQ ``nse_delivery_daily``.

The ONE genuinely-new infra piece the delivery channel needs (PEAD didn't): Upstox
candles are OHLCV only and do NOT carry delivery-%. The delivery signal's entire input
(``deliv_pct``) lives only in NSE's per-session ``sec_bhavdata_full`` CSV, so delivery
needs a daily post-close job to fetch + parse it into BigQuery.

Flow (~19:00 IST, after NSE publishes EOD):
  1. Fetch ``sec_bhavdata_full_DDMMYYYY.csv`` for the latest available session (warm-up
     cookie from nseindia.com + a browser User-Agent + Referer — the PROVEN handshake
     from ``scripts/redesign/bhavcopy_scope.py``). Back up to 5 recent dates until a
     200-with-content lands (weekend/holiday tolerant).
  2. ``parse_bhavdata`` -> EQ-series rows with {date, symbol, deliv_pct, deliv_qty,
     ttl_trd_qty, close_price, turnover_cr}. Rows with an unparseable DELIV_PER (e.g.
     ``"-"``) are skipped — no silent bad data.
  3. Idempotent BQ write: DELETE that session's rows then INSERT (dedupe by date), so
     re-running the same evening never duplicates.

Fail-closed everywhere (memory rule): any fetch/parse error -> log + ``{"skipped": ...}``,
no partial/garbage write. A missed fetch = no delivery signal next day = no trades = safe.

Validated config the downstream signal uses (2026-07-14, STOCKS-ONLY): ~11.8% CAGR,
deliv_pct >= 75, 25-50cr 20d-mean turnover, hold 20d. See ``domain/delivery_signals``.

Tests cover ``parse_bhavdata`` ONLY (fixture CSV string) — no live NSE/BQ in tests.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

logger = logging.getLogger(__name__)

# NSE archive endpoint for the full security bhavcopy (delivery-% carrying CSV).
_BHAV_URL = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{ddmmyyyy}.csv"
_NSE_WARMUP = "https://www.nseindia.com/"
# Browser-like headers + a warm-up cookie from nseindia.com are required, otherwise
# nsearchives returns 403/empty. Same handshake proven in bhavcopy_scope.py.
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}
_BQ_TABLE = "nse_delivery_daily"
_MAX_BACKUP_DAYS = 5          # try asof then up to 5 recent sessions before giving up
_MIN_CONTENT_BYTES = 300      # a real bhavcopy is ~200KB; a 200-with-tiny-body is a stub


def parse_bhavdata(raw: bytes, date_str: str) -> list[dict[str, Any]]:
    """Parse an NSE ``sec_bhavdata_full`` CSV into EQ-series delivery rows.

    The CSV header carries leading spaces on every field after the first
    (``SYMBOL, SERIES, ...``) — we strip each header cell before indexing. Only
    ``SERIES == "EQ"`` rows are kept. Rows whose ``DELIV_PER`` can't parse to a float
    (e.g. ``"-"`` for non-delivery series that slipped the EQ filter, or blanks) are
    skipped — fail-closed, never emit a bad ``deliv_pct``.

    Returns one dict per kept row::

        {date, symbol, deliv_pct, deliv_qty, ttl_trd_qty, close_price, turnover_cr}

    ``turnover_cr`` = ``TURNOVER_LACS / 100`` (lakh -> crore). ``date`` is supplied by
    the caller (it is not a per-row field in this CSV).
    """
    try:
        rows = raw.decode("utf-8", "replace").splitlines()
    except Exception as exc:                                   # pragma: no cover - defensive
        logger.error("delivery_ingest_parse_decode_failed err=%s", exc)
        return []
    if not rows:
        return []
    hdr = [h.strip() for h in rows[0].split(",")]
    need = ["SYMBOL", "SERIES", "CLOSE_PRICE", "TTL_TRD_QNTY", "TURNOVER_LACS", "DELIV_QTY", "DELIV_PER"]
    try:
        idx = {col: hdr.index(col) for col in need}
    except ValueError as exc:
        logger.error("delivery_ingest_parse_bad_header err=%s hdr=%s", exc, hdr[:12])
        return []
    max_i = max(idx.values())

    out: list[dict[str, Any]] = []
    for ln in rows[1:]:
        c = ln.split(",")
        if len(c) <= max_i:
            continue
        if c[idx["SERIES"]].strip() != "EQ":
            continue
        try:
            deliv_pct = float(c[idx["DELIV_PER"]].strip())          # "-" / "" -> ValueError -> skip
        except ValueError:
            continue
        try:
            close_price = float(c[idx["CLOSE_PRICE"]].strip())
            turnover_lacs = float(c[idx["TURNOVER_LACS"]].strip())
            ttl_trd_qty = int(float(c[idx["TTL_TRD_QNTY"]].strip()))
            deliv_qty = int(float(c[idx["DELIV_QTY"]].strip()))
        except (ValueError, IndexError):
            continue
        out.append({
            "date": date_str,
            "symbol": c[idx["SYMBOL"]].strip().upper(),
            "deliv_pct": deliv_pct,
            "deliv_qty": deliv_qty,
            "ttl_trd_qty": ttl_trd_qty,
            "close_price": round(close_price, 2),
            "turnover_cr": round(turnover_lacs / 100.0, 4),
        })
    return out


class DeliveryIngestService:
    """Daily NSE delivery-% ingestion into BQ ``nse_delivery_daily`` (fail-closed)."""

    def __init__(self, *, bq) -> None:
        self.bq = bq

    def _fetch_session(self) -> tuple[str, bytes] | None:
        """Fetch the latest available session's bhavcopy. Returns ``(iso_date, raw)``
        for the first date (asof, then backing up) that returns 200 with real content;
        None if none of the last ``_MAX_BACKUP_DAYS+1`` days is available.

        Deferred imports (requests, time_utils) so the module + parse tests load without
        network deps.
        """
        import time as _time
        import requests
        from autotrader.time_utils import now_ist

        sess = requests.Session()
        try:
            sess.get(_NSE_WARMUP, headers=_HEADERS, timeout=12)   # warm-up cookie
        except Exception:
            logger.warning("delivery_ingest_warmup_failed — continuing", exc_info=True)

        asof = now_ist().date()
        for back in range(_MAX_BACKUP_DAYS + 1):
            d = asof - timedelta(days=back)
            if d.weekday() >= 5:                                  # skip Sat/Sun quickly
                continue
            url = _BHAV_URL.format(ddmmyyyy=d.strftime("%d%m%Y"))
            try:
                r = sess.get(url, headers=_HEADERS, timeout=25)
            except Exception as exc:
                logger.warning("delivery_ingest_fetch_error date=%s err=%s", d.isoformat(), exc)
                _time.sleep(0.4)
                continue
            if r.status_code == 200 and len(r.content) >= _MIN_CONTENT_BYTES:
                logger.info("delivery_ingest_fetched date=%s bytes=%d", d.isoformat(), len(r.content))
                return d.isoformat(), r.content
            logger.info("delivery_ingest_no_file date=%s status=%s bytes=%d",
                        d.isoformat(), r.status_code, len(r.content or b""))
            _time.sleep(0.4)
        return None

    def _write_deduped(self, date_str: str, rows: list[dict[str, Any]]) -> None:
        """Idempotent per-date write: DELETE that date's rows, then INSERT. Uses the
        existing BQ entry points only (``query`` for DML, ``_insert`` for streaming) —
        no adapter change. Best-effort insert (adapter swallows+logs)."""
        table = f"grow-profit-machine.autotrader.{_BQ_TABLE}"
        self.bq.query(f"DELETE FROM `{table}` WHERE date = '{date_str}'")
        self.bq._insert(_BQ_TABLE, rows)

    def run(self, asof: str | None = None) -> dict[str, Any]:
        """Fetch + parse + write the latest session's delivery-% to BQ.

        ``asof`` is accepted for signature parity / future targeting but the fetch always
        resolves the latest AVAILABLE NSE session (backing up over weekends/holidays);
        it is echoed into the summary. Returns a summary dict, or ``{"skipped": reason}``
        on any failure (fail-closed — never a partial write).
        """
        try:
            fetched = self._fetch_session()
        except Exception as exc:
            logger.error("delivery_ingest_fetch_failed err=%s", exc, exc_info=True)
            return {"skipped": "fetch_failed", "asof": asof}
        if not fetched:
            logger.warning("delivery_ingest_no_session_available asof=%s", asof)
            return {"skipped": "no_session_available", "asof": asof}

        date_str, raw = fetched
        rows = parse_bhavdata(raw, date_str)
        if not rows:
            logger.warning("delivery_ingest_no_rows_parsed date=%s", date_str)
            return {"skipped": "no_rows_parsed", "asof": asof, "date": date_str}

        try:
            self._write_deduped(date_str, rows)
        except Exception as exc:
            logger.error("delivery_ingest_write_failed date=%s err=%s", date_str, exc, exc_info=True)
            return {"skipped": "bq_write_failed", "asof": asof, "date": date_str, "rows": len(rows)}

        summary = {"asof": asof or date_str, "date": date_str, "rows": len(rows),
                   "eq_symbols": len({r["symbol"] for r in rows})}
        logger.info("delivery_ingest_summary %s", summary)
        return summary
