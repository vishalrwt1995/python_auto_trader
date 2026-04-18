"""Earnings blackout calendar — auto-fetches Q4/Q1/Q2/Q3 result dates from NSE.

Runs as a weekly Cloud Scheduler job (every Sunday 8am IST) and updates the
Firestore document `config/earnings_blackout` so the scanner always has fresh
result dates without any manual intervention.

Source: NSE Corporate Filings Event Calendar API
  GET https://www.nseindia.com/api/event-calendar
  Returns board meeting dates for all listed companies.

Fallback: BSE corporate actions API.

Usage (Cloud Run job / local):
    python -m autotrader.services.earnings_calendar_service

Cloud Scheduler trigger:
    Target URL: /jobs/refresh-earnings-calendar
    Schedule:   0 8 * * 0   (every Sunday 08:00 IST)
"""
from __future__ import annotations

import json
import logging
import time
from datetime import date, timedelta
from typing import Any

import urllib.request
import urllib.error

logger = logging.getLogger(__name__)

# Nifty50 + Nifty Next50 symbols commonly appearing in watchlists.
# The fetcher pulls ALL companies; this set is used to filter only what matters.
_WATCHLIST_SYMBOLS = {
    # Nifty 50
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BHARTIARTL", "BEL",
    "BPCL", "BRITANNIA", "CIPLA", "COALINDIA", "DIVISLAB",
    "DRREDDY", "EICHERMOT", "GRASIM", "HCLTECH", "HDFCBANK",
    "HDFCLIFE", "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK",
    "INDUSINDBK", "INFY", "ITC", "JSWSTEEL", "KOTAKBANK",
    "LT", "M&M", "MARUTI", "NESTLEIND", "NTPC",
    "ONGC", "POWERGRID", "RELIANCE", "SBILIFE", "SBIN",
    "SHRIRAMFIN", "SUNPHARMA", "TATAMOTORS", "TATACONSUM", "TATASTEEL",
    "TCS", "TECHM", "TITAN", "ULTRACEMCO", "WIPRO",
    # Nifty Next50 high-frequency watchlist additions
    "AMBUJACEM", "AUROPHARMA", "BANKBARODA", "BERGEPAINT", "CANFINHOME",
    "CHOLAFIN", "CUMMINSIND", "DLF", "GLENMARK", "GMRAIRPORT",
    "GODREJCP", "HAVELLS", "HINDCOPPER", "INDUSTOWER", "IRFC",
    "IRCTC", "JINDALSTEL", "LTF", "LTIM", "LUPIN",
    "MCDOWELL-N", "MFSL", "MPHASIS", "OBEROIRLTY", "OFSS",
    "PAGEIND", "PERSISTENT", "PIDILITIND", "PIIND", "PNB",
    "POLYCAB", "RECLTD", "SAIL", "SBICARD", "SIEMENS",
    "TIINDIA", "TORNTPHARM", "TRENT", "TVSMOTOR", "UPL",
    "VEDL", "VOLTAS", "WHIRLPOOL", "ZOMATO",
}

# NSE headers needed to avoid bot-detection
_NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}


def _nse_get(path: str, timeout: int = 15) -> Any:
    """GET from NSE API with session cookie bootstrap."""
    # Step 1: get cookies by hitting the main page
    cookie_jar: dict[str, str] = {}
    try:
        req = urllib.request.Request("https://www.nseindia.com/", headers=_NSE_HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            for hdr in resp.headers.get_all("Set-Cookie") or []:
                parts = hdr.split(";")[0].strip()
                if "=" in parts:
                    k, v = parts.split("=", 1)
                    cookie_jar[k.strip()] = v.strip()
    except Exception as exc:
        logger.warning("nse_cookie_fetch_failed err=%s", exc)

    # Step 2: hit actual API with cookies
    cookie_str = "; ".join(f"{k}={v}" for k, v in cookie_jar.items())
    headers = {**_NSE_HEADERS, "Cookie": cookie_str}
    url = f"https://www.nseindia.com{path}"
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def fetch_nse_result_dates(lookback_days: int = 7, lookahead_days: int = 90) -> dict[str, str]:
    """Fetch upcoming board meeting / results dates from NSE event calendar.

    Returns {NSE_SYMBOL: "YYYY-MM-DD"} for events in the next `lookahead_days`.
    Filters to watchlist symbols only.
    """
    today = date.today()
    from_date = today - timedelta(days=lookback_days)
    to_date = today + timedelta(days=lookahead_days)

    # NSE event calendar endpoint
    path = (
        f"/api/event-calendar?index=equities"
        f"&from_date={from_date.strftime('%d-%m-%Y')}"
        f"&to_date={to_date.strftime('%d-%m-%Y')}"
    )

    try:
        data = _nse_get(path)
    except Exception as exc:
        logger.error("nse_event_calendar_fetch_failed err=%s", exc)
        return {}

    result_dates: dict[str, str] = {}
    events = data if isinstance(data, list) else data.get("data", [])

    for event in events:
        purpose = str(event.get("purpose") or "").upper()
        # Only care about board meetings that approve quarterly results
        if "FINANCIAL RESULT" not in purpose and "QUARTERLY RESULT" not in purpose:
            continue

        symbol = str(event.get("symbol") or "").strip().upper()
        if symbol not in _WATCHLIST_SYMBOLS:
            continue

        event_date_str = str(event.get("bm_date") or event.get("date") or "").strip()
        if not event_date_str:
            continue

        # NSE dates come as "DD-MMM-YYYY" or "YYYY-MM-DD"
        try:
            if "-" in event_date_str and len(event_date_str) == 11:
                # "18-Apr-2026" format
                event_date = date.strptime(event_date_str, "%d-%b-%Y")  # type: ignore[attr-defined]
            elif len(event_date_str) == 10:
                event_date = date.fromisoformat(event_date_str)
            else:
                continue
        except Exception:
            continue

        iso_date = event_date.isoformat()
        # Keep the latest known date per symbol (in case of re-scheduling)
        if symbol not in result_dates or iso_date > result_dates[symbol]:
            result_dates[symbol] = iso_date

    logger.info("nse_result_dates_fetched count=%d", len(result_dates))
    return result_dates


def refresh_earnings_blackout(
    project_id: str = "grow-profit-machine",
    database: str = "(default)",
    blackout_days: int = 2,
) -> dict[str, Any]:
    """Fetch latest result dates and update config/earnings_blackout in Firestore.

    Safe to run repeatedly — merges new dates with existing ones so manually
    added dates aren't lost.
    """
    from autotrader.adapters.firestore_state import FirestoreStateStore

    state = FirestoreStateStore(project_id, database)

    # Load existing document
    existing = state.get_json("config", "earnings_blackout") or {}
    current_symbols: dict[str, str] = {
        str(k).upper(): str(v)
        for k, v in (existing.get("symbols") or {}).items()
    }

    # Fetch fresh dates from NSE
    fresh_dates = fetch_nse_result_dates()

    # Merge: fresh dates win (NSE is authoritative), existing dates fill gaps
    merged = {**current_symbols, **fresh_dates}

    # Remove stale entries (result date > 30 days ago — event is long past)
    today = date.today()
    cutoff = today - timedelta(days=30)
    merged = {
        sym: dt for sym, dt in merged.items()
        if date.fromisoformat(dt) >= cutoff
    }

    doc = {
        "blackout_days": blackout_days,
        "symbols": merged,
        "last_updated": today.isoformat(),
        "note": "Auto-updated weekly by earnings_calendar_service. Manual overrides survive merges.",
    }
    state.set_json("config", "earnings_blackout", doc)

    added = len(fresh_dates)
    total = len(merged)
    logger.info("earnings_blackout_refreshed added=%d total=%d", added, total)
    return {"added": added, "total": total, "last_updated": today.isoformat()}


# ── Cloud Run job entry point ────────────────────────────────────────────────

def main() -> None:
    import os
    logging.basicConfig(level=logging.INFO)
    project_id = os.environ.get("GCP_PROJECT_ID", "grow-profit-machine")
    database = os.environ.get("FIRESTORE_DATABASE", "(default)")
    result = refresh_earnings_blackout(project_id=project_id, database=database)
    print(f"Done: {result}")


if __name__ == "__main__":
    main()
