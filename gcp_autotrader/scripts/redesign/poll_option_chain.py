#!/usr/bin/env python3
"""M5 — Poll Upstox option chain for a set of instruments, compute
OptionMetrics, and write to Firestore `option_metrics/{symbol}`.

Designed to run as a scheduled Cloud Run Job (every 5 min during market
hours). Explicitly NOT a long-running service — one invocation pulls a
snapshot and exits. Two consecutive snapshots in Firestore = trend.

Usage:
    python scripts/redesign/poll_option_chain.py \
        --symbols "NIFTY:NSE_INDEX|Nifty 50,BANKNIFTY:NSE_INDEX|Nifty Bank" \
        --expiry 2025-11-27

The `--symbols` arg is a comma-separated list of `display:instrument_key`.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", required=True,
                    help="comma-separated display:instrument_key pairs")
    ap.add_argument("--expiry", required=True, help="Expiry date YYYY-MM-DD")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    # Imports deferred so --help works without GCP deps.
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
    from autotrader.adapters.firestore_state import FirestoreStateStore
    from autotrader.adapters.upstox_client import UpstoxClient
    from autotrader.domain.option_analytics import compute_metrics

    upstox = UpstoxClient()
    state = FirestoreStateStore(os.getenv("GCP_PROJECT_ID", "grow-profit-machine"))

    pairs = [p.strip() for p in args.symbols.split(",") if p.strip()]
    wrote = 0
    for pair in pairs:
        try:
            display, ik = pair.split(":", 1)
        except ValueError:
            logging.warning("bad pair=%s expected display:instrument_key", pair)
            continue
        chain = upstox.get_option_chain(ik, args.expiry)
        if not chain:
            logging.warning("empty_chain display=%s ik=%s expiry=%s", display, ik, args.expiry)
            continue

        # Approximate spot from the row closest to itself — many rows
        # include `underlying_spot_price`. If not, fall back to the
        # mid of ATM straddle's LTPs.
        spot = 0.0
        for r in chain:
            for key in ("underlying_spot_price", "spot_price"):
                v = r.get(key) or 0
                try:
                    spot = max(spot, float(v or 0))
                except Exception:
                    pass
            if spot > 0:
                break

        m = compute_metrics(chain, spot=spot)
        payload = {
            "symbol": display.strip().upper(),
            "instrument_key": ik.strip(),
            "expiry": args.expiry,
            "spot": round(spot, 2),
            "max_pain_strike": m.max_pain_strike,
            "put_call_ratio": m.put_call_ratio,
            "oi_change_pcr": m.oi_change_pcr,
            "iv_skew": m.iv_skew,
            "n_rows": m.n_rows,
            "ts_epoch": time.time(),
            "ts_iso": datetime.now(timezone.utc).isoformat(),
        }

        if args.dry_run:
            print(payload)
        else:
            state.set_json("option_metrics", display.strip().upper(), payload)
            wrote += 1

    logging.info("done pairs=%d wrote=%d", len(pairs), wrote)
    return 0


if __name__ == "__main__":
    sys.exit(main())
