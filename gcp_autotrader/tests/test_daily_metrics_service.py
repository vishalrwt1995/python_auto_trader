"""Tests for `daily_metrics_service.run()` — the rollup orchestrator wired
to the new `POST /jobs/compute-daily-metrics` endpoint.

The pure rollup math is covered by `test_m6_attribution.py`. Here we assert
that the orchestrator:
  - groups attribution rows by trade_date,
  - calls `BigQueryClient.insert_daily_metrics` once per day in [since, until],
  - emits an empty rollup row for days with no attribution rows,
  - swallows per-day insert failures (counted in `failed`, not raised),
  - honours `dry_run=True` (no BQ writes),
  - defaults `since` to "yesterday UTC" and `until` to `since` when omitted.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from autotrader.services import daily_metrics_service as dms


def _attr_row(date: str, symbol: str = "INFY", **kw) -> dict:
    base = {
        "trade_date": date,
        "position_tag": f"AT-{symbol}-1",
        "symbol": symbol,
        "edge_name": "BREAKOUT",
        "expected_r": 1.0,
        "realized_r": 1.5,
        "r_delta": 0.5,
        "expected_hold_minutes": 60,
        "actual_hold_minutes": 55,
        "hold_delta_minutes": -5,
        "mfe_r": 2.0,
        "mae_r": -0.3,
        "exit_reason": "TARGET_HIT",
        "channel": "intraday",
        "paper": True,
    }
    base.update(kw)
    return base


def test_run_groups_by_date_and_writes_one_row_per_day():
    """Each calendar day in [since, until] must produce exactly one BQ insert."""
    fake_rows = [
        _attr_row("2026-04-15", "AAA"),
        _attr_row("2026-04-15", "BBB"),
        _attr_row("2026-04-16", "CCC"),
    ]
    bq_mock = MagicMock()
    with patch.object(dms, "_query", return_value=fake_rows), \
         patch("autotrader.adapters.bigquery_client.BigQueryClient", return_value=bq_mock):
        out = dms.run(
            project="p", dataset="d",
            since="2026-04-15", until="2026-04-16",
            dry_run=False,
        )

    assert out["wrote"] == 2
    assert out["failed"] == []
    assert out["days"] == 2
    assert out["attribution_rows"] == 3
    assert bq_mock.insert_daily_metrics.call_count == 2


def test_run_writes_empty_rollup_for_days_with_no_rows():
    """A day in [since, until] with zero attribution rows still gets a row
    (so the daily_metrics table has a continuous date axis)."""
    bq_mock = MagicMock()
    with patch.object(dms, "_query", return_value=[]), \
         patch("autotrader.adapters.bigquery_client.BigQueryClient", return_value=bq_mock):
        out = dms.run(
            project="p", dataset="d",
            since="2026-04-15", until="2026-04-17",
            dry_run=False,
        )

    assert out["wrote"] == 3   # one per day, even though no input rows
    assert out["days"] == 3
    assert bq_mock.insert_daily_metrics.call_count == 3


def test_run_dry_run_does_not_write_to_bq():
    fake_rows = [_attr_row("2026-04-15", "AAA")]
    bq_mock = MagicMock()
    with patch.object(dms, "_query", return_value=fake_rows), \
         patch("autotrader.adapters.bigquery_client.BigQueryClient", return_value=bq_mock) as bq_ctor:
        out = dms.run(
            project="p", dataset="d",
            since="2026-04-15", until="2026-04-15",
            dry_run=True,
        )

    # In dry_run mode the BQ client should not even be instantiated.
    assert bq_ctor.call_count == 0
    assert bq_mock.insert_daily_metrics.call_count == 0
    assert out["dry_run"] is True
    assert len(out["rollup_rows"]) == 1
    assert out["wrote"] == 0


def test_run_swallows_per_day_insert_failures():
    """One bad day should not abort the whole run — the failure is recorded
    in the `failed` list and the next days continue."""
    fake_rows = [
        _attr_row("2026-04-15", "AAA"),
        _attr_row("2026-04-16", "BBB"),
    ]
    bq_mock = MagicMock()
    # Day 1 raises; day 2 succeeds.
    bq_mock.insert_daily_metrics.side_effect = [RuntimeError("boom"), None]
    with patch.object(dms, "_query", return_value=fake_rows), \
         patch("autotrader.adapters.bigquery_client.BigQueryClient", return_value=bq_mock):
        out = dms.run(
            project="p", dataset="d",
            since="2026-04-15", until="2026-04-16",
        )

    assert out["wrote"] == 1
    assert out["failed"] == ["2026-04-15"]
    assert bq_mock.insert_daily_metrics.call_count == 2


def test_run_defaults_since_to_yesterday_utc():
    """No-arg call (the nightly Scheduler default) targets yesterday UTC."""
    bq_mock = MagicMock()
    expected_yesterday = (datetime.utcnow().date() - timedelta(days=1)).isoformat()
    with patch.object(dms, "_query", return_value=[]) as qm, \
         patch("autotrader.adapters.bigquery_client.BigQueryClient", return_value=bq_mock):
        out = dms.run(project="p", dataset="d")

    assert out["since"] == expected_yesterday
    assert out["until"] == expected_yesterday
    assert out["days"] == 1
    # _query was called with that same day on both sides.
    qm.assert_called_once_with("p", "d", expected_yesterday, expected_yesterday)
