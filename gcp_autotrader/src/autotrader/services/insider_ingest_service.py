"""INSIDER-channel daily ingestion — NSE SEBI-PIT disclosures -> BQ ``nse_insider_daily``.

The genuinely-new infra the insider channel needs (like delivery's bhavcopy): insider
transactions live only in NSE's ``corporates-pit`` disclosure feed, not in Upstox candles.
A daily post-close job fetches the recent window and upserts it into BigQuery; the morning
scan then clusters the latest dissemination day.

Flow (~19:30 IST, after the day's filings are disseminated):
  1. Fetch ``/api/corporates-pit?index=equities&from_date=..&to_date=..`` for a rolling
     ``window_days`` (default 7) — a warm-up cookie from nseindia.com + browser User-Agent +
     Referer (the PROVEN handshake, same as bhavcopy_scope.py / insider_pull.py). A rolling
     window (not just "today") absorbs SEBI's up-to-2-day disclosure lag + late filings, and
     re-writing the window each evening keeps BQ idempotent.
  2. ``parse_pit`` -> disclosure rows {date (dissemination YYYY-MM-DD), symbol, acq_name,
     person_category, transaction_type, acq_mode, sec_val, bef_pct, after_pct, disseminated_ts}.
     Rows with an unparseable dissemination date are skipped (fail-closed, no bad ``date``).
  3. Idempotent BQ write: DELETE rows with date >= window_start, then INSERT — re-running the
     same evening never duplicates, and late filings for recent days get refreshed.

Fail-closed everywhere (memory rule): any fetch/parse error -> log + ``{"skipped": ...}``, no
partial write. A missed fetch = no fresh clusters next day = no insider trades = safe.

Note: historical backfill (2015 -> now, ~341k rows) is a ONE-TIME load from the cached
scripts/redesign pull (``~/.autotrader_backtest_cache/insider_pit/*.json``); this service only
maintains the recent rolling window in prod.

Validated config the downstream signal uses (2026-07-20, GOD-MODE): cluster >=2 informed
open-market buys, double macro-gate (b200>50 & Nifty>100DMA), hold 90d. See ``domain/insider_signals``.

Tests cover ``parse_pit`` ONLY (fixture JSON) — no live NSE/BQ in tests.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

logger = logging.getLogger(__name__)

_PIT_URL = ("https://www.nseindia.com/api/corporates-pit"
            "?index=equities&from_date={frm}&to_date={to}")
_NSE_WARMUP = ("https://www.nseindia.com/",
               "https://www.nseindia.com/companies-listing/corporate-filings-insider-trading")
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}
_BQ_TABLE = "nse_insider_daily"
_WINDOW_DAYS = 7


def _fnum(x: Any) -> float | None:
    try:
        return float(str(x).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _disc_date(rec: dict[str, Any]) -> str | None:
    """Dissemination date -> YYYY-MM-DD from the PIT ``date`` field ('30-Jan-2016 17:08')."""
    from datetime import datetime
    s = str(rec.get("date", "")).split()[0] if rec.get("date") else ""
    if not s:
        return None
    try:
        return datetime.strptime(s, "%d-%b-%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def parse_pit(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Parse raw ``corporates-pit`` JSON records into ``nse_insider_daily`` rows.

    Keeps every disclosure with a parseable dissemination date + a symbol (all
    category/transaction/mode filtering happens later in ``domain.insider_signals`` so the
    raw feed is preserved for audit + re-analysis). ``sec_val`` falls back to ``buyValue``.
    Rows with no dissemination date are skipped (fail-closed).
    """
    out: list[dict[str, Any]] = []
    for r in records or []:
        sym = str(r.get("symbol") or "").strip().upper()
        dd = _disc_date(r)
        if not sym or not dd:
            continue
        out.append({
            "date": dd,
            "symbol": sym,
            "acq_name": str(r.get("acqName") or "")[:256],
            "person_category": str(r.get("personCategory") or ""),
            "transaction_type": str(r.get("tdpTransactionType") or ""),
            "acq_mode": str(r.get("acqMode") or ""),
            "sec_val": _fnum(r.get("secVal")) or _fnum(r.get("buyValue")) or 0.0,
            "bef_pct": _fnum(r.get("befAcqSharesPer")) or 0.0,
            "after_pct": _fnum(r.get("afterAcqSharesPer")) or 0.0,
            "disseminated_ts": str(r.get("date") or ""),
        })
    return out


class InsiderIngestService:
    """Daily NSE SEBI-PIT ingestion into BQ ``nse_insider_daily`` (fail-closed)."""

    def __init__(self, *, bq, window_days: int = _WINDOW_DAYS) -> None:
        self.bq = bq
        self.window_days = window_days

    def _fetch_window(self) -> tuple[str, list[dict[str, Any]]] | None:
        """Fetch the rolling ``window_days`` of PIT disclosures. Returns ``(window_start_iso,
        records)`` or None on failure. Deferred imports so the module + parse tests load
        without network deps."""
        import requests
        from autotrader.time_utils import now_ist

        sess = requests.Session()
        for u in _NSE_WARMUP:
            try:
                sess.get(u, headers=_HEADERS, timeout=12)
            except Exception:
                logger.warning("insider_ingest_warmup_failed url=%s — continuing", u)
        asof = now_ist().date()
        start = asof - timedelta(days=self.window_days)
        url = _PIT_URL.format(frm=start.strftime("%d-%m-%Y"), to=asof.strftime("%d-%m-%Y"))
        try:
            r = sess.get(url, headers=_HEADERS, timeout=35)
        except Exception as exc:
            logger.error("insider_ingest_fetch_error err=%s", exc)
            return None
        if r.status_code != 200:
            logger.warning("insider_ingest_bad_status status=%s", r.status_code)
            return None
        try:
            data = r.json().get("data", [])
        except Exception as exc:
            logger.error("insider_ingest_json_failed err=%s body=%.120r", exc, r.text)
            return None
        return start.isoformat(), data

    def _write_deduped(self, window_start: str, rows: list[dict[str, Any]]) -> None:
        """Idempotent window write: DELETE rows with date >= window_start, then INSERT. Uses
        only existing BQ entry points (``query`` for DML, ``_insert`` for streaming)."""
        table = f"grow-profit-machine.autotrader.{_BQ_TABLE}"
        self.bq.query(f"DELETE FROM `{table}` WHERE date >= '{window_start}'")
        self.bq._insert(_BQ_TABLE, rows)

    def run(self, asof: str | None = None) -> dict[str, Any]:
        """Fetch + parse + upsert the recent PIT window into BQ. Returns a summary dict, or
        ``{"skipped": reason}`` on any failure (fail-closed — never a partial write)."""
        try:
            fetched = self._fetch_window()
        except Exception as exc:
            logger.error("insider_ingest_fetch_failed err=%s", exc, exc_info=True)
            return {"skipped": "fetch_failed", "asof": asof}
        if fetched is None:
            logger.warning("insider_ingest_no_data asof=%s", asof)
            return {"skipped": "no_data", "asof": asof}

        window_start, records = fetched
        rows = parse_pit(records)
        if not rows:
            logger.warning("insider_ingest_no_rows_parsed window_start=%s raw=%d", window_start, len(records))
            return {"skipped": "no_rows_parsed", "asof": asof, "window_start": window_start}

        try:
            self._write_deduped(window_start, rows)
        except Exception as exc:
            logger.error("insider_ingest_write_failed err=%s", exc, exc_info=True)
            return {"skipped": "bq_write_failed", "asof": asof, "rows": len(rows)}

        summary = {"asof": asof or window_start, "window_start": window_start, "rows": len(rows),
                   "symbols": len({r["symbol"] for r in rows}),
                   "latest_date": max(r["date"] for r in rows)}
        logger.info("insider_ingest_summary %s", summary)
        return summary
