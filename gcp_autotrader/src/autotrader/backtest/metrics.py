"""Performance metrics for backtest results.

Produces the industry-standard summary stats from a list of `SimTrade` and
the equity curve. Pure functions — easy to unit-test, easy to run in walk-
forward and Monte Carlo bootstraps.

Formulas
--------
* **Win rate** = wins / total_closed
* **Profit factor** = sum(positive net_pnl) / abs(sum(negative net_pnl))
* **Expectancy (₹)** = mean(net_pnl)
* **Expectancy (R)** = mean(realized_r)
* **Sharpe (annualized)** = mean(daily_ret) / stdev(daily_ret) × √252
* **Sortino (annualized)** = mean(daily_ret) / stdev(downside_ret) × √252
* **Max drawdown %** = max((peak_equity - trough_equity) / peak_equity) over the curve
* **MAR ratio** = annualized_return / max_drawdown_pct
* **Avg hold (bars)** = mean(bars_held)

The Sharpe/Sortino computation buckets equity to **daily granularity** (one
last-of-day point per IST date) — using per-bar equity for Sharpe over-
reports the ratio because intraday bars have lower variance than daily.
"""
from __future__ import annotations

import math
import statistics
from collections import defaultdict
from typing import Iterable

from autotrader.backtest.types import EquityPoint, SimTrade


# ── Top-level summary ────────────────────────────────────────────────────


def summarize(
    trades: list[SimTrade],
    equity_curve: list[EquityPoint],
    *,
    starting_cash: float = 1_000_000.0,
    risk_free_rate_annual: float = 0.06,
) -> dict[str, float]:
    """Return the canonical summary dict.

    Keys:
      n_trades, wins, losses, breakeven, win_rate, gross_pnl, net_pnl,
      total_costs, profit_factor, expectancy_inr, expectancy_r,
      avg_win_inr, avg_loss_inr, max_win_inr, max_loss_inr,
      avg_hold_bars, avg_mfe_r, avg_mae_r,
      sharpe, sortino, max_drawdown_pct, mar,
      starting_cash, ending_equity, total_return_pct
    """
    n = len(trades)
    if n == 0:
        return _empty_summary(starting_cash, equity_curve)

    wins = [t for t in trades if t.net_pnl > 0]
    losses = [t for t in trades if t.net_pnl < 0]
    bes = [t for t in trades if t.net_pnl == 0]

    gross_pnl = sum(t.gross_pnl for t in trades)
    total_costs = sum(t.costs for t in trades)
    net_pnl = sum(t.net_pnl for t in trades)

    pos_sum = sum(t.net_pnl for t in wins)
    neg_sum = sum(t.net_pnl for t in losses)
    profit_factor = (pos_sum / abs(neg_sum)) if neg_sum < 0 else float("inf")

    daily_returns = _daily_returns_from_equity(equity_curve, starting_cash)
    sharpe = _sharpe(daily_returns, risk_free_rate_annual)
    sortino = _sortino(daily_returns, risk_free_rate_annual)
    max_dd_pct = _max_drawdown_pct(equity_curve)

    ending = equity_curve[-1].equity if equity_curve else starting_cash
    total_ret_pct = ((ending - starting_cash) / starting_cash * 100) if starting_cash > 0 else 0.0

    annualized_ret_pct = _annualize_return(equity_curve, total_ret_pct)
    mar = (annualized_ret_pct / max_dd_pct) if max_dd_pct > 0 else float("inf")

    return {
        "n_trades": n,
        "wins": len(wins),
        "losses": len(losses),
        "breakeven": len(bes),
        "win_rate": round(len(wins) / n * 100, 2),
        "gross_pnl": round(gross_pnl, 2),
        "net_pnl": round(net_pnl, 2),
        "total_costs": round(total_costs, 2),
        "profit_factor": round(profit_factor, 3) if math.isfinite(profit_factor) else profit_factor,
        "expectancy_inr": round(net_pnl / n, 2),
        "expectancy_r": round(statistics.mean(t.realized_r for t in trades), 4),
        "avg_win_inr": round(pos_sum / len(wins), 2) if wins else 0.0,
        "avg_loss_inr": round(neg_sum / len(losses), 2) if losses else 0.0,
        "max_win_inr": round(max((t.net_pnl for t in wins), default=0.0), 2),
        "max_loss_inr": round(min((t.net_pnl for t in losses), default=0.0), 2),
        "avg_hold_bars": round(statistics.mean(t.bars_held for t in trades), 2),
        "avg_mfe_r": round(statistics.mean(t.mfe_r for t in trades), 4),
        "avg_mae_r": round(statistics.mean(t.mae_r for t in trades), 4),
        "sharpe": round(sharpe, 3),
        "sortino": round(sortino, 3),
        "max_drawdown_pct": round(max_dd_pct, 3),
        "mar": round(mar, 3) if math.isfinite(mar) else mar,
        "starting_cash": round(starting_cash, 2),
        "ending_equity": round(ending, 2),
        "total_return_pct": round(total_ret_pct, 3),
        "annualized_return_pct": round(annualized_ret_pct, 3),
    }


def _empty_summary(starting_cash: float, equity_curve: list[EquityPoint]) -> dict[str, float]:
    ending = equity_curve[-1].equity if equity_curve else starting_cash
    return {
        "n_trades": 0, "wins": 0, "losses": 0, "breakeven": 0,
        "win_rate": 0.0, "gross_pnl": 0.0, "net_pnl": 0.0, "total_costs": 0.0,
        "profit_factor": 0.0, "expectancy_inr": 0.0, "expectancy_r": 0.0,
        "avg_win_inr": 0.0, "avg_loss_inr": 0.0, "max_win_inr": 0.0, "max_loss_inr": 0.0,
        "avg_hold_bars": 0.0, "avg_mfe_r": 0.0, "avg_mae_r": 0.0,
        "sharpe": 0.0, "sortino": 0.0, "max_drawdown_pct": 0.0, "mar": 0.0,
        "starting_cash": round(starting_cash, 2),
        "ending_equity": round(ending, 2),
        "total_return_pct": 0.0, "annualized_return_pct": 0.0,
    }


# ── Helpers ──────────────────────────────────────────────────────────────


def _daily_returns_from_equity(
    equity_curve: list[EquityPoint], starting_cash: float,
) -> list[float]:
    """Bucket equity points to last-of-day, then compute simple returns."""
    if not equity_curve:
        return []
    by_day: dict[str, float] = {}
    for ep in equity_curve:
        day = ep.ts[:10]   # YYYY-MM-DD
        by_day[day] = ep.equity
    days = sorted(by_day.keys())
    eq = [by_day[d] for d in days]
    rets: list[float] = []
    prev = starting_cash
    for v in eq:
        if prev > 0:
            rets.append((v - prev) / prev)
        prev = v
    return rets


def _sharpe(daily_returns: list[float], rf_annual: float) -> float:
    if not daily_returns or len(daily_returns) < 2:
        return 0.0
    daily_rf = rf_annual / 252.0
    excess = [r - daily_rf for r in daily_returns]
    mu = statistics.mean(excess)
    sd = statistics.stdev(excess)
    if sd == 0:
        return 0.0
    return (mu / sd) * math.sqrt(252)


def _sortino(daily_returns: list[float], rf_annual: float) -> float:
    if not daily_returns or len(daily_returns) < 2:
        return 0.0
    daily_rf = rf_annual / 252.0
    excess = [r - daily_rf for r in daily_returns]
    downside = [min(0, e) for e in excess]
    if not any(d < 0 for d in downside):
        return float("inf")
    mu = statistics.mean(excess)
    # Downside deviation: sqrt(mean(min(0,r)²))
    dd_var = sum(d * d for d in downside) / max(1, len(downside) - 1)
    dd = math.sqrt(dd_var)
    if dd == 0:
        return 0.0
    return (mu / dd) * math.sqrt(252)


def _max_drawdown_pct(equity_curve: list[EquityPoint]) -> float:
    if not equity_curve:
        return 0.0
    peak = equity_curve[0].equity
    max_dd = 0.0
    for ep in equity_curve:
        if ep.equity > peak:
            peak = ep.equity
        if peak > 0:
            dd = (peak - ep.equity) / peak * 100
            if dd > max_dd:
                max_dd = dd
    return max_dd


def _annualize_return(equity_curve: list[EquityPoint], total_ret_pct: float) -> float:
    if not equity_curve:
        return 0.0
    days_set = {ep.ts[:10] for ep in equity_curve}
    n_days = max(1, len(days_set))
    if n_days <= 0:
        return 0.0
    # Annualize via 252-day calendar.
    annualized = ((1 + total_ret_pct / 100) ** (252 / n_days) - 1) * 100
    return annualized


# ── Per-setup / per-regime breakdowns ────────────────────────────────────


def per_setup_stats(trades: list[SimTrade]) -> dict[str, dict[str, float]]:
    return _group_stats(trades, key=lambda t: t.setup or "UNKNOWN")


def per_regime_stats(trades: list[SimTrade]) -> dict[str, dict[str, float]]:
    return _group_stats(trades, key=lambda t: t.regime_at_entry or "UNKNOWN")


def per_setup_regime_stats(trades: list[SimTrade]) -> dict[str, dict[str, float]]:
    return _group_stats(
        trades,
        key=lambda t: f"{t.setup or 'UNKNOWN'}|{t.regime_at_entry or 'UNKNOWN'}",
    )


def _group_stats(trades: Iterable[SimTrade], *, key) -> dict[str, dict[str, float]]:
    groups: dict[str, list[SimTrade]] = defaultdict(list)
    for t in trades:
        groups[key(t)].append(t)
    out: dict[str, dict[str, float]] = {}
    for k, group in groups.items():
        if not group:
            continue
        wins = [t for t in group if t.net_pnl > 0]
        net = sum(t.net_pnl for t in group)
        out[k] = {
            "n": len(group),
            "wins": len(wins),
            "win_rate": round(len(wins) / len(group) * 100, 2),
            "net_pnl": round(net, 2),
            "expectancy_r": round(statistics.mean(t.realized_r for t in group), 4),
            "expectancy_inr": round(net / len(group), 2),
            "avg_mfe_r": round(statistics.mean(t.mfe_r for t in group), 4),
            "avg_mae_r": round(statistics.mean(t.mae_r for t in group), 4),
        }
    return out


__all__ = [
    "summarize",
    "per_setup_stats",
    "per_regime_stats",
    "per_setup_regime_stats",
]
