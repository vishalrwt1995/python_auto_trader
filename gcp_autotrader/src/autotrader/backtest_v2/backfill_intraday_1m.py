"""Backfill 1-minute NIFTY + VIX historical candles from Upstox v3 API.

Production uses live Upstox quote responses at scan time. The change_pct
and vix values are NOT archived in our cache. By pulling 1m candles for
both NIFTY and India VIX over the brain-snapshot window, we can compute
exact LTP at any historical scan timestamp to within 1-minute precision.

Output:
  GCS: cache/backfill/1m/NSE_INDEX_NIFTY_50.json
  GCS: cache/backfill/1m/NSE_INDEX_India_VIX.json
  Local cache: ~/.autotrader_backtest_cache/cache__backfill__1m__*.json

Usage:
    python -m autotrader.backtest_v2.backfill_intraday_1m 2026-03-07 2026-05-21
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from autotrader.adapters.secrets_manager import SecretManagerStore
from autotrader.adapters.upstox_client import UpstoxClient
from autotrader.settings import UpstoxSettings


NIFTY_KEY = "NSE_INDEX|Nifty 50"
VIX_KEY = "NSE_INDEX|India VIX"


def _build_client() -> UpstoxClient:
    """Build Upstox client with production secret names."""
    upstox = UpstoxSettings(
        api_v2_host="https://api.upstox.com/v2",
        api_v3_host="https://api.upstox.com/v3",
        client_id_secret_name="upstox-client-id",
        client_secret_secret_name="upstox-client-secret",
        access_token_secret_name="upstox-access-token",
        access_token_expiry_secret_name="upstox-access-token-expiry",
        analytics_token_secret_name="upstox-analytics-token",
    )
    secrets = SecretManagerStore("grow-profit-machine")
    return UpstoxClient(upstox, secrets)


def fetch_1m_for_range(
    client: UpstoxClient,
    instrument_key: str,
    from_date: str,
    to_date: str,
) -> list[list[Any]]:
    """Fetch 1m candles. Upstox v3 may return all at once or page by date.

    Returns merged list sorted by timestamp ascending.
    """
    candles = client.get_historical_candles_v3_intraday_range(
        instrument_key=instrument_key,
        from_date=from_date,
        to_date=to_date,
        unit="minutes",
        interval=1,
    )
    # Sort by ts ascending (API returns newest-first usually)
    candles = sorted(candles, key=lambda c: str(c[0]))
    return candles


def backfill(
    client: UpstoxClient,
    instrument_key: str,
    label: str,
    start_date: date,
    end_date: date,
    chunk_days: int = 30,
) -> list[list[Any]]:
    """Backfill in chunks (some APIs have date-range limits).

    Returns deduplicated chronological list of candles.
    """
    out: list[list[Any]] = []
    cur = start_date
    while cur <= end_date:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), end_date)
        print(f"  [{label}] fetching {cur.isoformat()} to {chunk_end.isoformat()}...", flush=True)
        try:
            candles = fetch_1m_for_range(
                client, instrument_key,
                from_date=cur.isoformat(),
                to_date=chunk_end.isoformat(),
            )
            print(f"    → got {len(candles)} bars", flush=True)
            out.extend(candles)
        except Exception as exc:
            print(f"    ⚠️  failed: {type(exc).__name__}: {exc}", flush=True)
        cur = chunk_end + timedelta(days=1)

    # Dedup by timestamp (1m unique key)
    by_ts: dict[str, list[Any]] = {}
    for c in out:
        ts = str(c[0])
        if ts not in by_ts:
            by_ts[ts] = c
    deduped = sorted(by_ts.values(), key=lambda c: str(c[0]))
    return deduped


def save_local_and_print(candles: list[list[Any]], label: str) -> str:
    """Save to local cache, return path."""
    cache_dir = Path.home() / ".autotrader_backtest_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    safe_label = label.replace("|", "_").replace(" ", "_")
    out_path = cache_dir / f"cache__backfill__1m__{safe_label}.json"
    with open(out_path, "w") as fh:
        json.dump(candles, fh)
    print(f"  💾 Saved {len(candles)} bars to {out_path}", flush=True)
    print(f"     Earliest: {candles[0][0] if candles else 'none'}")
    print(f"     Latest:   {candles[-1][0] if candles else 'none'}")
    return str(out_path)


def main() -> int:
    start_str = sys.argv[1] if len(sys.argv) > 1 else "2026-03-07"
    end_str = sys.argv[2] if len(sys.argv) > 2 else "2026-05-21"
    start_date = date.fromisoformat(start_str)
    end_date = date.fromisoformat(end_str)
    print(f"Backfilling 1m candles: {start_str} to {end_str}")
    print(f"  Window: {(end_date - start_date).days + 1} calendar days")

    client = _build_client()
    try:
        token = client.ensure_read_token()
        print(f"  ✅ Read token OK (len={len(token)})")
    except Exception as exc:
        print(f"  ❌ Auth failed: {exc}")
        return 1

    # NIFTY 1m
    print("\n[1] NIFTY 50 — 1m candles")
    nifty_candles = backfill(client, NIFTY_KEY, "NIFTY", start_date, end_date, chunk_days=30)
    save_local_and_print(nifty_candles, NIFTY_KEY)

    # India VIX 1m
    print("\n[2] India VIX — 1m candles")
    vix_candles = backfill(client, VIX_KEY, "VIX", start_date, end_date, chunk_days=30)
    save_local_and_print(vix_candles, VIX_KEY)

    # Summary
    print("\n[3] Summary")
    print(f"  NIFTY 1m bars: {len(nifty_candles)}")
    print(f"  VIX   1m bars: {len(vix_candles)}")
    print(f"  Expected per trading day: ~375 bars (09:15 to 15:29)")
    n_trading_days = max(1, len([1 for d in range((end_date - start_date).days + 1)
                                  if (start_date + timedelta(days=d)).weekday() < 5]))
    print(f"  Trading days in window: ~{n_trading_days}")
    print(f"  NIFTY avg bars/day: {len(nifty_candles) / n_trading_days:.0f}")

    print("\n✅ Backfill complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
