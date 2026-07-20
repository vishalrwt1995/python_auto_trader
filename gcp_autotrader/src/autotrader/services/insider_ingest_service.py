"""INSIDER-channel daily ingestion — NSE SEBI-PIT (corporates-pit-gg) -> BQ ``nse_insider_daily``.

NSE restructured the insider feed ~2026-05 (see memory reference_nse_insider_pit_endpoint):
  * OLD ``/api/corporates-pit`` (rich JSON, all fields) -> DEAD after 02-May-2026.
  * NEW ``/api/corporates-pit-gg?index=equities`` -> a FILING INDEX only (appId, symbol,
    broadcastDateTime, regulation, xmlFileName ...). The transaction detail (acquirer, category,
    buy/sell, mode, shares, holding%) moved into the per-filing XBRL doc at ``xmlFileName``
    (BSE ``in-bse-co`` taxonomy).

So the ingest is now two-stage:
  1. Fetch the ``corporates-pit-gg`` index for a rolling ``window_days`` window (absorbs SEBI's
     <=2-day disclosure lag + late filings), filter to filings whose broadcast date is in-window.
  2. For each filing: fetch + ``parse_insider_xbrl`` its XBRL -> one row per transaction leg
     (a filing can carry several — Disclosure1/2/..). Tolerate transient 404 on just-filed docs
     (archived by the next-morning scan) — skip + log, never a partial/garbage write.
  3. Idempotent BQ upsert: DELETE date>=window_start then INSERT.

Row schema (nse_insider_daily): date, symbol, acq_name, person_category, transaction_type,
acq_mode, shares, sec_val (filer value — UNRELIABLE, kept for audit only), bef_pct, after_pct,
app_id, xbrl_url, disseminated_ts. The signal computes value = shares × reaction-close (the filer
``sec_val`` is not trusted — it showed Rs 1 for a 10M-share buy).

Fail-closed everywhere: any fetch/parse error -> log + skip; a missed fetch = no clusters next
day = no insider trades = safe. Historical backfill (2015->May-2026, 341k rows) is a one-time
load from the cached corporates-pit pull; this service only maintains the recent rolling window.

Tests cover ``parse_insider_xbrl`` (real XBRL fixture) + ``_index_filings`` — no live NSE/BQ.
"""
from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

_INDEX_URL = "https://www.nseindia.com/api/corporates-pit-gg?index=equities&from_date={frm}&to_date={to}"
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
_NS = "{http://www.bseindia.com/xbrl/co/2017-09-15/in-bse-co}"
_WINDOW_DAYS = 3


def _fnum(x: Any) -> float | None:
    try:
        return float(str(x).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _bcast_date(s: Any) -> str | None:
    """'20-Jul-2026 16:33:41' -> '2026-07-20' (dissemination date = the tradeable knowledge date)."""
    t = str(s or "").split()[0] if s else ""
    if not t:
        return None
    try:
        return datetime.strptime(t, "%d-%b-%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def parse_insider_xbrl(xbrl_text: str | bytes, index_rec: dict[str, Any]) -> list[dict[str, Any]]:
    """Parse ONE filing's BSE ``in-bse-co`` PIT XBRL into transaction-leg rows.

    A filing = a company-level context (``MainI``) + one context per transaction leg
    (``Disclosure1``, ``Disclosure2``, ...). Emits one row per Disclosure leg with the fields the
    signal needs. ``symbol`` + dissemination ``date`` come from the reliable index record; the
    per-leg facts come from the XBRL. Holding-% is stored as PERCENT (XBRL stores a fraction:
    0.1407 = 14.07% -> ×100). The filer ``sec_val`` is kept but NOT trusted downstream. Returns
    ``[]`` on any parse error (fail-closed).
    """
    try:
        root = ET.fromstring(xbrl_text)
    except Exception as exc:
        logger.warning("insider_xbrl_parse_failed app_id=%s err=%s", index_rec.get("appId"), exc)
        return []
    facts: dict[str, dict[str, str]] = {}
    for el in root.iter():
        ctx = el.get("contextRef")
        if not ctx:
            continue
        val = (el.text or "").strip()
        if val:
            facts.setdefault(ctx, {})[el.tag.split("}")[-1]] = val

    dd = _bcast_date(index_rec.get("broadcastDateTime"))
    sym = str(index_rec.get("symbol") or "").strip().upper()
    if not dd or not sym:
        return []
    rows: list[dict[str, Any]] = []
    for ctx, f in facts.items():
        if not ctx.lower().startswith("disclosure"):
            continue
        shares = _fnum(f.get("SecuritiesAcquiredOrDisposedNumberOfSecurity"))
        if shares is None or shares <= 0:
            continue
        rows.append({
            "date": dd,
            "symbol": sym,
            "acq_name": str(f.get("NameOfThePerson", ""))[:256],
            "person_category": f.get("CategoryOfPerson", ""),
            "transaction_type": f.get("SecuritiesAcquiredOrDisposedTransactionType", ""),
            "acq_mode": f.get("ModeOfAcquisitionOrDisposal", ""),
            "shares": shares,
            "sec_val": _fnum(f.get("SecuritiesAcquiredOrDisposedValueOfSecurity")) or 0.0,
            "bef_pct": (_fnum(f.get("SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding")) or 0.0) * 100.0,
            "after_pct": (_fnum(f.get("SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding")) or 0.0) * 100.0,
            "app_id": str(index_rec.get("appId", "")),
            "xbrl_url": str(index_rec.get("xmlFileName", "")),
            "disseminated_ts": str(index_rec.get("broadcastDateTime", "")),
        })
    return rows


def _index_filings(records: list[dict[str, Any]], window_start: str) -> list[dict[str, Any]]:
    """PURE: from the raw corporates-pit-gg index, keep filings whose broadcast date is on/after
    ``window_start`` and that carry an ``xmlFileName``. Dedupe by appId (keep first)."""
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for r in records or []:
        dd = _bcast_date(r.get("broadcastDateTime"))
        aid = str(r.get("appId") or "")
        if not dd or dd < window_start or not r.get("xmlFileName"):
            continue
        if aid and aid in seen:
            continue
        seen.add(aid)
        out.append(r)
    return out


class InsiderIngestService:
    """Daily NSE SEBI-PIT (corporates-pit-gg + per-filing XBRL) ingestion into BQ (fail-closed)."""

    def __init__(self, *, bq, window_days: int = _WINDOW_DAYS) -> None:
        self.bq = bq
        self.window_days = window_days

    def _session(self):
        import requests
        sess = requests.Session()
        for u in _NSE_WARMUP:
            try:
                sess.get(u, headers=_HEADERS, timeout=12)
            except Exception:
                logger.warning("insider_ingest_warmup_failed url=%s — continuing", u)
        return sess

    def _fetch_index(self, sess, start: date, end: date) -> list[dict[str, Any]] | None:
        url = _INDEX_URL.format(frm=start.strftime("%d-%m-%Y"), to=end.strftime("%d-%m-%Y"))
        try:
            r = sess.get(url, headers=_HEADERS, timeout=35)
        except Exception as exc:
            logger.error("insider_ingest_index_fetch_error err=%s", exc)
            return None
        if r.status_code != 200:
            logger.warning("insider_ingest_index_bad_status status=%s", r.status_code)
            return None
        try:
            return r.json().get("data", [])
        except Exception as exc:
            logger.error("insider_ingest_index_json_failed err=%s body=%.120r", exc, r.text)
            return None

    def _fetch_xbrl(self, sess, rec: dict[str, Any]) -> str | None:
        """Fetch a filing's XBRL text (xmlFileName, ixbrl fallback). None on 404/error (skip)."""
        import time as _time
        for url in (rec.get("xmlFileName"), rec.get("ixbrl")):
            if not url:
                continue
            for attempt in (1, 2):
                try:
                    r = sess.get(url, headers=_HEADERS, timeout=25)
                except Exception as exc:
                    logger.warning("insider_xbrl_fetch_err app_id=%s err=%s", rec.get("appId"), exc)
                    _time.sleep(0.5); continue
                if r.status_code == 200 and r.text.lstrip().startswith("<?xml"):
                    return r.text
                if r.status_code == 404:
                    break  # not archived yet — try fallback url / skip (retried next run)
                _time.sleep(0.5)
        return None

    def _write_deduped(self, window_start: str, rows: list[dict[str, Any]]) -> None:
        table = f"grow-profit-machine.autotrader.{_BQ_TABLE}"
        self.bq.query(f"DELETE FROM `{table}` WHERE date >= '{window_start}'")
        self.bq._insert(_BQ_TABLE, rows)

    def run(self, asof: str | None = None) -> dict[str, Any]:
        """Fetch the rolling index -> per-filing XBRL -> parse -> upsert to BQ. Fail-closed."""
        import time as _time
        try:
            from autotrader.time_utils import now_ist
            end = now_ist().date()
        except Exception:
            end = date.today()
        start = end - timedelta(days=self.window_days)
        window_start = start.isoformat()

        try:
            sess = self._session()
            records = self._fetch_index(sess, start, end)
        except Exception as exc:
            logger.error("insider_ingest_fetch_failed err=%s", exc, exc_info=True)
            return {"skipped": "index_fetch_failed", "asof": asof}
        if records is None:
            return {"skipped": "index_unavailable", "asof": asof}

        filings = _index_filings(records, window_start)
        rows: list[dict[str, Any]] = []
        missing = 0
        for rec in filings:
            xbrl = self._fetch_xbrl(sess, rec)
            if xbrl is None:
                missing += 1
                continue
            rows.extend(parse_insider_xbrl(xbrl, rec))
            _time.sleep(0.35)

        if not rows:
            logger.warning("insider_ingest_no_rows window_start=%s index=%d filings=%d missing_xbrl=%d",
                           window_start, len(records), len(filings), missing)
            return {"skipped": "no_rows_parsed", "asof": asof, "window_start": window_start,
                    "filings": len(filings), "missing_xbrl": missing}
        try:
            self._write_deduped(window_start, rows)
        except Exception as exc:
            logger.error("insider_ingest_write_failed err=%s", exc, exc_info=True)
            return {"skipped": "bq_write_failed", "asof": asof, "rows": len(rows)}

        summary = {"asof": asof or window_start, "window_start": window_start,
                   "index_records": len(records), "filings_in_window": len(filings),
                   "missing_xbrl": missing, "rows": len(rows),
                   "symbols": len({r["symbol"] for r in rows}),
                   "latest_date": max(r["date"] for r in rows)}
        logger.info("insider_ingest_summary %s", summary)
        return summary
