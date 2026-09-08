"""Daily NSE corporate-actions calendar ingestion -> BQ ``nse_corp_actions_live``.

Built for ``domain/corp_action_guard.py`` (see that module for the HEG incident this exists to
prevent a repeat of). Endpoint validated live before building on it (this project's rule against
guessing external APIs): ``nseindia.com/api/corporates-corporateActions?index=equities`` returns a
JSON list with ``symbol, isin, subject, exDate, recDate`` (plus block-deal/no-delivery fields this
module ignores) -- confirmed to carry HEG's demerger (``subject="Demerger"``, ``recDate="07-Sep-
2026"``) when queried with an explicit date range. Same warm-up-cookie handshake as
``delivery_ingest_service.py`` (NSE 403s without it); that handshake itself has been degrading
(``nse_cookie_fetch_failed`` since ~08-23, per PROJECT_KNOWLEDGE.md) but the live spike that
validated this endpoint got a 200 on the real data call even when the warm-up itself 403'd, so this
follows the same fetch-anyway pattern rather than treating a failed warm-up as fatal.

Fetches a ROLLING WINDOW (today - `_LOOKBACK_DAYS` to today + `_LOOKAHEAD_DAYS`), not a single day
-- NSE publishes some record dates weeks ahead, and the lookback gives margin if a day's ingest is
ever missed. Idempotent per-window write: ``DELETE WHERE ex_date >= window_start`` then INSERT,
same idiom as ``insider_ingest_service.py``'s multi-day catch-up window -- rows already ingested
from an earlier day's window and now outside today's lookback are left untouched, so coverage only
grows over time even though each run's fetch range is a rolling slice.

A brand-new table (``nse_corp_actions_live``), deliberately separate from the pre-existing
``nse_corp_actions`` (a one-time historical load from a killed buyback/dividend strategy, stale
since 2026-08-27 -- kept untouched as a source of realistic historical fixtures for tests, not as
anything this service writes to).

BQ is the durable/historical record; the guard's actual hot-path read (inside
``order_service.place_exit_order``, on every SL-type exit) must not depend on a live BQ query on
that path -- so this service ALSO mirrors the parsed rows into one small Firestore document
(``corp_action_calendar/current``, via the same ``state.get_json``/``set_json`` helper the
pledge/insider scan-state ledgers already use), which is what the guard actually reads. The Firestore
write is best-effort and does not fail the run if it errors — BQ having the data is what matters for
history; the live cache missing one refresh just means the guard's fail-open path (see
``domain/corp_action_guard.py``) applies until the next successful ingest.

Fail-closed everywhere: any fetch/parse error -> log + ``{"skipped": ...}``, never a partial write.
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import Any

logger = logging.getLogger(__name__)

_URL = "https://www.nseindia.com/api/corporates-corporateActions"
_WARMUP = "https://www.nseindia.com/"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}
_BQ_TABLE = "nse_corp_actions_live"
_LOOKBACK_DAYS = 10
_LOOKAHEAD_DAYS = 21
CORP_ACTION_CALENDAR_COLLECTION = "corp_action_calendar"
CORP_ACTION_CALENDAR_KEY = "current"


def _parse_nse_date(raw: Any) -> str | None:
    """PURE: NSE's ``DD-Mon-YYYY`` (e.g. ``07-Sep-2026``) -> ISO, or None for ``"-"``/unparseable."""
    s = str(raw or "").strip()
    if not s or s == "-":
        return None
    try:
        return dt.datetime.strptime(s, "%d-%b-%Y").date().isoformat()
    except ValueError:
        return None


def parse_corp_actions(raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """PURE: NSE's raw JSON rows -> normalized ``{symbol, isin, subject, ex_date, rec_date}``.

    Rows missing a ``symbol``, or with neither date parseable, are dropped -- a row this guard
    could not key on or window against is worse than useless, since a bad row that silently
    doesn't match anything can never accidentally suppress a real exit (fail-closed by omission).
    """
    out: list[dict[str, Any]] = []
    for row in raw_rows or ():
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        ex_date = _parse_nse_date(row.get("exDate"))
        rec_date = _parse_nse_date(row.get("recDate"))
        if not ex_date and not rec_date:
            continue
        out.append({
            "symbol": symbol,
            "isin": str(row.get("isin") or "").strip(),
            "subject": str(row.get("subject") or "").strip(),
            "ex_date": ex_date,
            "rec_date": rec_date,
        })
    return out


class CorpCalendarIngestService:
    """Daily rolling-window ingestion of NSE's corporate-actions calendar (fail-closed)."""

    def __init__(self, *, bq, state=None) -> None:
        self.bq = bq
        self.state = state

    def _fetch(self, window_start: dt.date, window_end: dt.date) -> list[dict[str, Any]] | None:
        """Deferred import of ``requests`` so this module loads without network deps in tests."""
        import requests

        sess = requests.Session()
        try:
            sess.get(_WARMUP, headers=_HEADERS, timeout=12)
        except Exception:
            logger.warning("corp_calendar_ingest_warmup_failed — continuing", exc_info=True)

        params = {
            "index": "equities",
            "from_date": window_start.strftime("%d-%m-%Y"),
            "to_date": window_end.strftime("%d-%m-%Y"),
        }
        try:
            r = sess.get(_URL, headers=_HEADERS, params=params, timeout=20)
        except Exception as exc:
            logger.warning("corp_calendar_ingest_fetch_error err=%s", exc)
            return None
        if r.status_code != 200:
            logger.warning("corp_calendar_ingest_bad_status status=%s bytes=%d", r.status_code, len(r.content or b""))
            return None
        try:
            data = r.json()
        except Exception as exc:
            logger.warning("corp_calendar_ingest_bad_json err=%s", exc)
            return None
        if not isinstance(data, list):
            logger.warning("corp_calendar_ingest_unexpected_shape type=%s", type(data).__name__)
            return None
        return data

    def _write_deduped(self, window_start_iso: str, rows: list[dict[str, Any]]) -> None:
        table = f"grow-profit-machine.autotrader.{_BQ_TABLE}"
        self.bq.query(f"DELETE FROM `{table}` WHERE ex_date >= '{window_start_iso}'")
        if rows:
            self.bq._insert(_BQ_TABLE, rows)

    def _write_calendar_cache(self, rows: list[dict[str, Any]], updated_at: str) -> None:
        """Best-effort Firestore mirror for the guard's hot-path read. Never raises — a failed
        cache write must not fail the whole ingest run (BQ already has the durable copy)."""
        if self.state is None:
            return
        try:
            self.state.set_json(
                CORP_ACTION_CALENDAR_COLLECTION, CORP_ACTION_CALENDAR_KEY,
                {"rows": rows, "updated_at": updated_at}, merge=False,
            )
        except Exception:
            logger.warning("corp_calendar_cache_write_failed", exc_info=True)

    def run(self, asof: str | None = None) -> dict[str, Any]:
        """Fetch + parse + write the rolling window. Returns a summary, or ``{"skipped": reason}``
        on any failure (fail-closed — never a partial write)."""
        from autotrader.time_utils import now_ist

        today = now_ist().date()
        window_start = today - dt.timedelta(days=_LOOKBACK_DAYS)
        window_end = today + dt.timedelta(days=_LOOKAHEAD_DAYS)

        try:
            raw = self._fetch(window_start, window_end)
        except Exception as exc:
            logger.error("corp_calendar_ingest_fetch_failed err=%s", exc, exc_info=True)
            return {"skipped": "fetch_failed", "asof": asof}
        if raw is None:
            return {"skipped": "fetch_failed", "asof": asof}

        rows = parse_corp_actions(raw)

        try:
            self._write_deduped(window_start.isoformat(), rows)
        except Exception as exc:
            logger.error("corp_calendar_ingest_write_failed err=%s", exc, exc_info=True)
            return {"skipped": "bq_write_failed", "asof": asof, "rows_parsed": len(rows)}

        self._write_calendar_cache(rows, dt.datetime.now(dt.timezone.utc).isoformat())

        summary = {
            "asof": asof or today.isoformat(),
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "raw_rows": len(raw),
            "rows": len(rows),
            "symbols": len({r["symbol"] for r in rows}),
        }
        logger.info("corp_calendar_ingest_summary %s", summary)
        return summary
