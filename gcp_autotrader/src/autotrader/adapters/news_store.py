"""M5 — News adapter: Firestore-backed reader for news items.

We don't have a single authoritative news feed from Upstox; instead we
let an external poller (scheduled Cloud Run Job, manual seed, or a
future RSS adapter) write into `news_items/{doc_id}` and this module
reads them. Keeps the trading path free of blocking external HTTP calls.

News item schema (convention, enforced by tests + consumers):
    {
        "symbol": "INFY",          # uppercase NSE symbol, or "" for market-wide
        "ts_epoch": 1730000000.0,  # event time (seconds since epoch)
        "headline": "Q2 miss...",
        "source": "reuters" | "moneycontrol" | ...,
        "sentiment": "BULLISH" | "BEARISH" | "NEUTRAL",
        "score": 0.8,              # sentiment magnitude 0..1 (optional)
        "url": "https://...",      # optional
        "ingested_ts": 1730000010.0
    }

Design:
  * No background thread — callers poll via `recent_for_symbol` when
    they need it. Avoids the "new service for every new source" trap.
  * TTL enforced at read-time (window_seconds arg) rather than via
    Firestore TTL policy so tests can scroll time without touching GCP.
"""
from __future__ import annotations

import time
from typing import Any

from autotrader.adapters.firestore_state import FirestoreStateStore


class NewsStore:
    """Thin wrapper over FirestoreStateStore for the news_items collection."""

    _COLLECTION = "news_items"

    def __init__(self, state: FirestoreStateStore) -> None:
        self.state = state

    def put(self, doc_id: str, item: dict[str, Any]) -> None:
        """Insert/upsert a news item. Writer stamps ingested_ts if missing."""
        payload = dict(item)
        payload.setdefault("ingested_ts", time.time())
        payload["symbol"] = str(payload.get("symbol", "") or "").upper()
        payload["sentiment"] = str(payload.get("sentiment", "NEUTRAL") or "NEUTRAL").upper()
        self.state.set_json(self._COLLECTION, str(doc_id), payload)

    def recent_for_symbol(
        self, symbol: str, *, window_seconds: float = 3 * 3600, limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Return news items for `symbol` newer than `window_seconds`.

        Symbol-match is case-insensitive; passes the empty string to get
        market-wide news. We also include symbol="" entries when looking
        for a specific symbol (broader-market headlines matter too).

        Best-effort: Firestore errors produce an empty list, not an
        exception — news is a nice-to-have enrichment, not a gate.
        """
        try:
            # list_by_prefix with an empty prefix returns all docs up to limit.
            raw = self.state.list_by_prefix(self._COLLECTION, "", limit=limit * 4)
        except Exception:
            return []
        cutoff = time.time() - float(window_seconds or 0)
        sym = str(symbol or "").upper()
        out: list[dict[str, Any]] = []
        for row in raw or []:
            if not isinstance(row, dict):
                continue
            ts = float(row.get("ts_epoch", 0) or 0)
            if ts < cutoff:
                continue
            row_sym = str(row.get("symbol", "") or "").upper()
            if sym and row_sym and row_sym != sym:
                continue
            out.append(row)
        # Newest first
        out.sort(key=lambda r: float(r.get("ts_epoch", 0) or 0), reverse=True)
        return out[:limit]


def aggregate_sentiment(items: list[dict[str, Any]]) -> tuple[str, float]:
    """Roll up a news-item list into (label, confidence 0..1).

    Simple majority-with-magnitude rule: signed score = sum(
        +score if BULLISH else -score if BEARISH else 0
    ). Label is BULLISH / BEARISH / NEUTRAL based on sign and magnitude.

    Returns (NEUTRAL, 0.0) when the list is empty — "no news" != signal.
    """
    if not items:
        return "NEUTRAL", 0.0
    signed = 0.0
    mag = 0.0
    for r in items:
        s = float(r.get("score", 0.5) or 0.5)
        sent = str(r.get("sentiment", "NEUTRAL")).upper()
        if sent == "BULLISH":
            signed += s
            mag += s
        elif sent == "BEARISH":
            signed -= s
            mag += s
    if mag <= 0:
        return "NEUTRAL", 0.0
    conf = min(1.0, abs(signed) / mag)
    if abs(signed) < 0.1 * mag:
        return "NEUTRAL", conf
    return ("BULLISH" if signed > 0 else "BEARISH"), conf


__all__ = ["NewsStore", "aggregate_sentiment"]
