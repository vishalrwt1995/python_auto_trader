"""Tests for M6 — AttributionLog + daily-metrics rollup + alerts.

Pure-function tests: the BQ write path is a thin pass-through (covered
by structural assertions that the wire-up exists in order_service) but
the row-building + rollup + alert logic is fully unit-tested.
"""
from __future__ import annotations

import inspect

from autotrader.domain.attribution import (
    AlertThresholds,
    AttributionRow,
    DailyMetrics,
    _realized_r,
    build_row_from_position,
    detect_alerts,
    rollup,
)


# ──────────────────────────────────────────────────────────────────────────
# Realized-R math
# ──────────────────────────────────────────────────────────────────────────


def test_realized_r_long_winner():
    # Entry 100, SL 98 → sl_dist=2. Exit 104 → move=+4 → +2R.
    assert _realized_r(100.0, 104.0, 2.0, "BUY") == 2.0


def test_realized_r_long_loser():
    # Entry 100, SL 98 → sl_dist=2. Exit 98 → -1R.
    assert _realized_r(100.0, 98.0, 2.0, "BUY") == -1.0


def test_realized_r_short_winner():
    # SHORT: entry 100, exit 96, sl_dist 2 → move-flipped = +4 → +2R.
    assert _realized_r(100.0, 96.0, 2.0, "SELL") == 2.0


def test_realized_r_zero_sl_dist_safe():
    # Guard against div-by-zero when sl_price == entry_price.
    assert _realized_r(100.0, 101.0, 0.0, "BUY") == 0.0


# ──────────────────────────────────────────────────────────────────────────
# build_row_from_position
# ──────────────────────────────────────────────────────────────────────────


def test_build_row_populates_expected_and_realized_r():
    pos = {
        "position_tag": "AT-X-1",
        "symbol": "INFY",
        "side": "BUY",
        "strategy": "BREAKOUT",
        "entry_price": 1000.0,
        "exit_price": 1020.0,
        "sl_price": 990.0,       # sl_dist = 10
        "entry_ts": "2026-04-23 09:30:00",
        "exit_ts": "2026-04-23 10:15:00",
        "hold_minutes": 45,
        "signal_score": 78,
        "regime": "TREND_UP",
        "risk_mode": "NORMAL",
        "exit_reason": "TARGET_HIT",
        "channel": "intraday",
        "paper": True,
        "thesis": {
            "edge_name": "BREAKOUT_TREND_UP",
            "edge_version": "v1",
            "expected_r": 1.25,
            "expected_hold_minutes": 60,
            "invalidation_price": 990.0,
            "regime_at_entry": "TREND_UP",
            "risk_mode_at_entry": "NORMAL",
        },
    }
    row = build_row_from_position(pos)
    assert isinstance(row, AttributionRow)
    assert row.symbol == "INFY"
    assert row.realized_r == 2.0               # +20 / 10
    assert row.expected_r == 1.25
    assert row.r_delta == round(2.0 - 1.25, 4)
    assert row.actual_hold_minutes == 45
    assert row.hold_delta_minutes == 45 - 60   # finished early
    assert row.edge_name == "BREAKOUT_TREND_UP"
    assert row.trade_date == "2026-04-23"
    assert row.regime_at_entry == "TREND_UP"
    assert row.channel == "intraday"
    assert row.paper is True


def test_build_row_degrades_gracefully_when_no_thesis():
    pos = {
        "symbol": "RELIANCE",
        "side": "BUY",
        "entry_price": 2000.0,
        "exit_price": 1990.0,
        "sl_price": 1980.0,
        "hold_minutes": 12,
        "exit_ts": "2026-04-23 10:00:00",
    }
    row = build_row_from_position(pos)
    # No thesis → expected_r=0, edge_name="", but realized_r still computed.
    assert row.edge_name == ""
    assert row.expected_r == 0.0
    assert row.realized_r == round((1990 - 2000) / (2000 - 1980), 4)  # -0.5R
    assert row.r_delta == row.realized_r


def test_build_row_to_bq_row_is_plain_dict():
    pos = {"symbol": "X", "side": "BUY", "entry_price": 100, "exit_price": 101, "sl_price": 99}
    d = build_row_from_position(pos).to_bq_row()
    assert isinstance(d, dict)
    assert d["symbol"] == "X"
    # All fields serialisable (no dataclass instances hiding)
    for v in d.values():
        assert isinstance(v, (str, int, float, bool)) or v is None


# ──────────────────────────────────────────────────────────────────────────
# rollup
# ──────────────────────────────────────────────────────────────────────────


def test_rollup_empty_yields_zero_row():
    m = rollup([], trade_date="2026-04-23")
    assert isinstance(m, DailyMetrics)
    assert m.n_trades == 0
    assert m.win_rate == 0.0
    assert m.trade_date == "2026-04-23"


def _row_dict(realized_r: float, expected_r: float = 1.0, mae_r: float = 0.2, hold: int = 30) -> dict:
    return {
        "trade_date": "2026-04-23",
        "realized_r": realized_r,
        "expected_r": expected_r,
        "actual_hold_minutes": hold,
        "mae_r": mae_r,
    }


def test_rollup_mixed_win_loss():
    rows = [
        _row_dict(+2.0),
        _row_dict(+1.0),
        _row_dict(-1.0),
        _row_dict(-0.5),
        _row_dict(+1.5),
    ]
    m = rollup(rows)
    assert m.n_trades == 5
    assert m.n_wins == 3
    assert m.win_rate == 0.6
    # mean_realized_r = (2+1-1-0.5+1.5)/5 = 0.6
    assert abs(m.mean_realized_r - 0.6) < 1e-9
    assert m.mean_expected_r == 1.0
    assert abs(m.mean_r_delta - (-0.4)) < 1e-9


def test_rollup_accepts_attributionrow_instances_too():
    r = AttributionRow(
        trade_date="2026-04-23", position_tag="t", symbol="X", side="BUY",
        strategy="BREAKOUT", edge_name="", edge_version="", regime_at_entry="",
        regime_at_exit="", risk_mode_at_entry="", signal_score=0,
        expected_r=1.0, realized_r=2.0, r_delta=1.0,
        expected_hold_minutes=30, actual_hold_minutes=20, hold_delta_minutes=-10,
        mfe_r=0.0, mae_r=0.3, invalidation_price=0.0, exit_reason="TARGET_HIT",
        channel="intraday", paper=True, entry_ts="", exit_ts="",
    )
    m = rollup([r])
    assert m.n_trades == 1
    assert m.n_wins == 1


# ──────────────────────────────────────────────────────────────────────────
# detect_alerts
# ──────────────────────────────────────────────────────────────────────────


def test_alerts_suppressed_for_small_sample():
    # <5 trades → alerts suppressed regardless of how bad the metrics are.
    metrics = DailyMetrics(
        trade_date="2026-04-23", n_trades=3, n_wins=0, win_rate=0.0,
        gross_pnl=0.0, mean_realized_r=-2.0, mean_expected_r=1.0,
        mean_r_delta=-3.0, mean_hold_minutes=10.0, worst_drawdown_r=2.5,
    )
    assert detect_alerts(metrics) == []


def test_alerts_fire_when_thresholds_breached():
    metrics = DailyMetrics(
        trade_date="2026-04-23", n_trades=8, n_wins=1, win_rate=0.125,
        gross_pnl=0.0, mean_realized_r=-1.5, mean_expected_r=0.8,
        mean_r_delta=-2.3, mean_hold_minutes=20.0, worst_drawdown_r=1.8,
    )
    alerts = detect_alerts(metrics)
    joined = "|".join(alerts)
    assert "win_rate_below" in joined
    assert "realized_r_lags_expected_by" in joined
    assert "mae_over" in joined


def test_alerts_honour_custom_thresholds():
    # Raise the win-rate floor enough that the high-winrate day still alerts.
    metrics = DailyMetrics(
        trade_date="2026-04-23", n_trades=10, n_wins=5, win_rate=0.5,
        gross_pnl=0.0, mean_realized_r=0.1, mean_expected_r=0.1,
        mean_r_delta=0.0, mean_hold_minutes=20.0, worst_drawdown_r=0.5,
    )
    custom = AlertThresholds(low_win_rate=0.6)
    alerts = detect_alerts(metrics, custom)
    assert any("win_rate_below" in a for a in alerts)


def test_rollup_attaches_alerts_tuple_when_breach():
    rows = [_row_dict(realized_r=-1.2, expected_r=0.8, mae_r=0.3) for _ in range(6)]
    m = rollup(rows, trade_date="2026-04-23")
    # Mean r_delta = -2.0, below floor of -1.0 → alert fires.
    assert m.alerts
    assert any("realized_r_lags_expected" in a for a in m.alerts)


# ──────────────────────────────────────────────────────────────────────────
# Wire-up: settings flag + order_service hook + BigQuery helper
# ──────────────────────────────────────────────────────────────────────────


def test_settings_exposes_attribution_flag():
    from autotrader.settings import RuntimeSettings
    r = RuntimeSettings(log_level="INFO", paper_trade=True, job_trigger_token="t")
    assert r.use_attribution_log_v1 is False


def test_settings_from_env_reads_attribution_flag():
    from autotrader import settings as settings_mod
    src = inspect.getsource(settings_mod)
    assert 'use_attribution_log_v1=_env_bool("USE_ATTRIBUTION_LOG_V1"' in src


def test_order_service_wires_attribution_under_flag():
    from autotrader.services import order_service
    src = inspect.getsource(order_service)
    # Gated on the flag
    assert "use_attribution_log_v1" in src
    # Uses the domain builder
    assert "build_row_from_position" in src
    # Best-effort writer (never blocks close)
    assert "_bq_insert_attribution_best_effort" in src


def test_bigquery_client_has_attribution_helpers():
    from autotrader.adapters import bigquery_client
    src = inspect.getsource(bigquery_client)
    assert "def insert_attribution(" in src
    assert "def insert_daily_metrics(" in src


def test_compute_daily_metrics_script_exists_and_imports():
    import importlib.util
    from pathlib import Path
    p = Path(__file__).resolve().parents[1] / "scripts" / "redesign" / "compute_daily_metrics.py"
    assert p.exists()
    spec = importlib.util.spec_from_file_location("compute_daily_metrics", p)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    assert hasattr(mod, "main")
