"""M6 — AttributionLog: per-trade attribution row + daily-metrics rollup.

The existing `trades` BQ table records the minimal "what happened" row
(entry, exit, pnl). AttributionLog is the "WHY did it happen" row: the
Edge that fired, the Thesis at entry, realized R vs expected R, regime
drift, MFE/MAE, hold-time delta. This is what the weekly review reads
to decide which edges to keep, kill, or tune.

This module is pure: it only builds the row. Persistence is via the
existing BigQueryClient through order_service.

Row schema (matches the `attribution` BQ table created in M6 deploy):
    trade_date, position_tag, symbol, side, strategy (setup),
    edge_name, edge_version,
    regime_at_entry, regime_at_exit, risk_mode_at_entry,
    signal_score, expected_r, realized_r, r_delta,
    expected_hold_minutes, actual_hold_minutes, hold_delta_minutes,
    mfe_r, mae_r, invalidation_price,
    exit_reason, channel, paper, entry_ts, exit_ts
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class AttributionRow:
    trade_date: str
    position_tag: str
    symbol: str
    side: str
    strategy: str
    edge_name: str
    edge_version: str
    regime_at_entry: str
    regime_at_exit: str
    risk_mode_at_entry: str
    signal_score: int
    expected_r: float
    realized_r: float
    r_delta: float            # realized − expected
    expected_hold_minutes: int
    actual_hold_minutes: int
    hold_delta_minutes: int
    mfe_r: float
    mae_r: float
    invalidation_price: float
    exit_reason: str
    channel: str
    paper: bool
    entry_ts: str
    exit_ts: str

    def to_bq_row(self) -> dict[str, Any]:
        """Return a dict safe for BigQuery streaming insert."""
        return asdict(self)


def _realized_r(entry_price: float, exit_price: float, sl_dist: float, side: str) -> float:
    if sl_dist <= 0:
        return 0.0
    move = exit_price - entry_price
    if str(side).upper() == "SELL":
        move = -move
    return round(move / sl_dist, 4)


def build_row_from_position(pos: dict[str, Any]) -> AttributionRow:
    """Assemble an AttributionRow from a CLOSED position document.

    Expects the keys written by order_service._save_position_firestore
    + the Thesis dict (when USE_PLAYBOOK_V1 was on at entry). Missing
    keys degrade gracefully to empty-string / zero.
    """
    thesis = pos.get("thesis") or {}
    entry_price = float(pos.get("entry_price") or 0.0)
    exit_price = float(pos.get("exit_price") or 0.0)
    sl_price = float(pos.get("sl_price") or 0.0)
    side = str(pos.get("side") or "BUY").upper()

    sl_dist = abs(entry_price - sl_price)
    realized_r = _realized_r(entry_price, exit_price, sl_dist, side)

    expected_r = float(thesis.get("expected_r") or 0.0)
    expected_hold = int(thesis.get("expected_hold_minutes") or 0)
    actual_hold = int(pos.get("hold_minutes") or 0)

    return AttributionRow(
        trade_date=str(pos.get("exit_ts") or "")[:10],
        position_tag=str(pos.get("position_tag") or ""),
        symbol=str(pos.get("symbol") or ""),
        side=side,
        strategy=str(pos.get("strategy") or "").upper(),
        edge_name=str(thesis.get("edge_name") or ""),
        edge_version=str(thesis.get("edge_version") or ""),
        regime_at_entry=str(thesis.get("regime_at_entry") or "") or str(pos.get("regime") or ""),
        regime_at_exit=str(pos.get("regime_at_exit") or pos.get("regime") or ""),
        risk_mode_at_entry=str(thesis.get("risk_mode_at_entry") or "") or str(pos.get("risk_mode") or ""),
        signal_score=int(pos.get("signal_score") or 0),
        expected_r=expected_r,
        realized_r=realized_r,
        r_delta=round(realized_r - expected_r, 4),
        expected_hold_minutes=expected_hold,
        actual_hold_minutes=actual_hold,
        hold_delta_minutes=actual_hold - expected_hold,
        mfe_r=float(pos.get("max_favorable_excursion_r") or 0.0),
        mae_r=float(pos.get("max_adverse_excursion_r") or 0.0),
        invalidation_price=float(thesis.get("invalidation_price") or sl_price),
        exit_reason=str(pos.get("exit_reason") or ""),
        channel=str(pos.get("channel") or ("swing" if pos.get("is_swing") else "intraday")),
        paper=bool(pos.get("paper", True)),
        entry_ts=str(pos.get("entry_ts") or ""),
        exit_ts=str(pos.get("exit_ts") or ""),
    )


# ──────────────────────────────────────────────────────────────────────────
# Daily metrics — aggregate a list of AttributionRow (or their dicts) into
# the rollup row we track over time.
# ──────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class DailyMetrics:
    trade_date: str
    n_trades: int
    n_wins: int
    win_rate: float
    gross_pnl: float
    mean_realized_r: float
    mean_expected_r: float
    mean_r_delta: float     # realized − expected, mean across trades
    mean_hold_minutes: float
    worst_drawdown_r: float
    alerts: tuple[str, ...] = field(default_factory=tuple)

    def to_bq_row(self) -> dict[str, Any]:
        d = asdict(self)
        d["alerts"] = list(self.alerts)
        return d


# Alert thresholds — tunable. These are the "something is wrong, page
# the operator" lines, not the "everything is fine" thresholds.
@dataclass(frozen=True)
class AlertThresholds:
    min_trades_for_alert: int = 5
    # Win rate more than 15 points BELOW 0.40 (i.e. <0.25) is anomalous
    # regardless of edge expectations.
    low_win_rate: float = 0.25
    # Realized R lagging expected R by >1R mean is a regime-mismatch alert.
    r_delta_floor: float = -1.0
    # Any single trade MAE > 1.5R means SL placement was overrun —
    # probably a gap or broker latency event.
    max_mae_r: float = 1.5


def detect_alerts(metrics: DailyMetrics, t: AlertThresholds | None = None) -> list[str]:
    t = t or AlertThresholds()
    alerts: list[str] = []
    if metrics.n_trades < t.min_trades_for_alert:
        return alerts      # too small a sample — noise not signal
    if metrics.win_rate < t.low_win_rate:
        alerts.append(f"win_rate_below_{t.low_win_rate:.2f}")
    if metrics.mean_r_delta < t.r_delta_floor:
        alerts.append(f"realized_r_lags_expected_by_{-t.r_delta_floor:.1f}R")
    if metrics.worst_drawdown_r > t.max_mae_r:
        alerts.append(f"mae_over_{t.max_mae_r:.1f}R_stop_overrun")
    return alerts


def rollup(rows: list[AttributionRow | dict], trade_date: str = "") -> DailyMetrics:
    """Fold a list of AttributionRow (or their dict forms) into DailyMetrics."""
    # Normalize all to dicts for one code path.
    dicts: list[dict] = []
    for r in rows:
        if isinstance(r, AttributionRow):
            dicts.append(r.to_bq_row())
        elif isinstance(r, dict):
            dicts.append(r)
    if not dicts:
        return DailyMetrics(
            trade_date=trade_date, n_trades=0, n_wins=0, win_rate=0.0,
            gross_pnl=0.0, mean_realized_r=0.0, mean_expected_r=0.0,
            mean_r_delta=0.0, mean_hold_minutes=0.0, worst_drawdown_r=0.0,
        )
    n = len(dicts)
    n_wins = sum(1 for r in dicts if float(r.get("realized_r") or 0.0) > 0)
    realized_rs = [float(r.get("realized_r") or 0.0) for r in dicts]
    expected_rs = [float(r.get("expected_r") or 0.0) for r in dicts]
    hold_mins = [int(r.get("actual_hold_minutes") or 0) for r in dicts]
    mae_rs = [abs(float(r.get("mae_r") or 0.0)) for r in dicts]
    mean_real = sum(realized_rs) / n
    mean_exp = sum(expected_rs) / n
    metrics = DailyMetrics(
        trade_date=trade_date or (dicts[0].get("trade_date") or ""),
        n_trades=n,
        n_wins=n_wins,
        win_rate=round(n_wins / n, 4),
        gross_pnl=0.0,          # callers join from trades table if needed
        mean_realized_r=round(mean_real, 4),
        mean_expected_r=round(mean_exp, 4),
        mean_r_delta=round(mean_real - mean_exp, 4),
        mean_hold_minutes=round(sum(hold_mins) / n, 2),
        worst_drawdown_r=round(max(mae_rs) if mae_rs else 0.0, 4),
    )
    return DailyMetrics(
        **{**metrics.__dict__, "alerts": tuple(detect_alerts(metrics))},
    )


__all__ = [
    "AttributionRow",
    "DailyMetrics",
    "AlertThresholds",
    "build_row_from_position",
    "rollup",
    "detect_alerts",
]
