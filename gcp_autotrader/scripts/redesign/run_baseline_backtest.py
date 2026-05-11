#!/usr/bin/env python3
"""Baseline backtest suite — runs pure-replay with all gates ON, then
counterfactual runs with individual gates toggled OFF to isolate impact.

Usage:
    cd gcp_autotrader
    PYTHONPATH=src .venv/bin/python scripts/redesign/run_baseline_backtest.py

Requires `gcloud auth` configured for vishalrwt1995@gmail.com.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("baseline_backtest")


def _patch_credentials():
    """Inject gcloud access token as ADC so BQ/GCS clients authenticate
    with the correct account (vishalrwt1995@gmail.com)."""
    import google.auth.credentials

    result = subprocess.run(
        ["/Users/vishalrawat/google-cloud-sdk/bin/gcloud",
         "auth", "print-access-token",
         "--account=vishalrwt1995@gmail.com"],
        capture_output=True, text=True,
    )
    token = result.stdout.strip()
    if not token:
        raise RuntimeError(f"gcloud token fetch failed: {result.stderr}")

    from google.oauth2.credentials import Credentials
    _creds = Credentials(token=token)

    _orig_bq_client = None

    def _patched_bq_client(project):
        from autotrader.backtest.data import _bq_client as _orig
        from google.cloud import bigquery
        client = bigquery.Client(project=project, credentials=_creds)
        try:
            from requests.adapters import HTTPAdapter
            session = client._http
            adapter = HTTPAdapter(pool_connections=64, pool_maxsize=64)
            session.mount("https://", adapter)
            session.mount("http://", adapter)
        except Exception:
            pass
        return client

    import autotrader.backtest.data as data_mod
    data_mod._bq_client = _patched_bq_client

    from autotrader.adapters import gcs_store
    _orig_init = gcs_store.GoogleCloudStorageStore.__init__

    def _patched_gcs_init(self, bucket_name=None, **kwargs):
        from google.cloud import storage
        self._client = storage.Client(
            project="grow-profit-machine", credentials=_creds,
        )
        self._bucket_name = bucket_name or "grow-profit-machine-autotrader-data"
        self._bucket = self._client.bucket(self._bucket_name)

    gcs_store.GoogleCloudStorageStore.__init__ = _patched_gcs_init

    log.info("credentials_patched account=vishalrwt1995@gmail.com")


def run_backtest(label: str, out_dir: str, **overrides) -> dict:
    """Run a single pure-replay backtest and return its metrics."""
    from autotrader.backtest.runner import RunSpec, run_pure_replay
    from autotrader.backtest.reports import print_summary, write_all

    defaults = dict(
        project="grow-profit-machine",
        dataset="autotrader",
        since="2026-04-16",
        until="2026-05-09",
        timeframe="5m",
        warmup_days=7,
        starting_cash=1_000_000.0,
        per_trade_risk_inr=5_000.0,
        max_concurrent=5,
        label=label,
        out_dir=out_dir,
        candle_source="gcs",
        apply_watchlist_per_day=True,
        apply_brain_haircut=True,
        apply_dynamic_min_score=True,
        apply_daily_bias=True,
        apply_strategy_entry_gate=True,
        is_swing=False,
    )
    defaults.update(overrides)

    spec = RunSpec(**defaults)
    log.info("=" * 64)
    log.info("RUNNING: %s", label)
    log.info("=" * 64)

    result = run_pure_replay(spec)
    print_summary(result)

    return {
        "label": label,
        "metrics": result.metrics,
        "per_setup": result.per_setup,
        "per_regime": result.per_regime,
        "meta": result.meta,
    }


def main():
    _patch_credentials()

    out_base = str(Path(__file__).resolve().parents[2] / "backtests" / "baseline_suite")
    os.makedirs(out_base, exist_ok=True)

    results = []

    # ── Run 1: BASELINE (all gates ON = current live behaviour) ──────
    results.append(run_backtest("baseline_all_gates", out_base))

    # ── Run 2: No brain haircut (isolate its impact) ─────────────────
    results.append(run_backtest(
        "no_brain_haircut", out_base,
        apply_brain_haircut=False,
    ))

    # ── Run 3: No affinity multiplier (isolate RANGE dead zone) ──────
    # Note: affinity is inside PureReplayStrategy, not a RunSpec toggle.
    # We test by lowering threshold instead — effectively same as removing
    # the affinity multiplier's effect on marginal scores.
    results.append(run_backtest(
        "low_threshold_60", out_base,
        min_signal_score=60,
    ))

    # ── Run 4: No strategy entry gates ───────────────────────────────
    results.append(run_backtest(
        "no_strategy_gates", out_base,
        apply_strategy_entry_gate=False,
    ))

    # ── Run 5: No daily bias (Layer 5 off) ───────────────────────────
    results.append(run_backtest(
        "no_daily_bias", out_base,
        apply_daily_bias=False,
    ))

    # ── Summary comparison ───────────────────────────────────────────
    print("\n" + "=" * 80)
    print("  COMPARISON SUMMARY")
    print("=" * 80)
    header = f"  {'Label':30s} {'Trades':>7s} {'WinRate':>8s} {'NetPnL':>12s} {'Sharpe':>8s} {'MaxDD':>8s} {'PF':>6s}"
    print(header)
    print("  " + "-" * 78)
    for r in results:
        m = r["metrics"]
        print(f"  {r['label']:30s} "
              f"{m.get('n_trades', 0):>7d} "
              f"{m.get('win_rate', 0):>7.1f}% "
              f"{m.get('net_pnl', 0):>11,.0f} "
              f"{m.get('sharpe', 0):>8.2f} "
              f"{m.get('max_drawdown_pct', 0):>7.1f}% "
              f"{m.get('profit_factor', 0):>6.2f}")
    print("=" * 80)

    summary_path = os.path.join(out_base, "comparison_summary.json")
    with open(summary_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    log.info("comparison_written path=%s", summary_path)


if __name__ == "__main__":
    main()
