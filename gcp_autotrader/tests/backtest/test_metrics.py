"""Metrics tests — Sharpe, drawdown, expectancy, per-bucket grouping."""
from __future__ import annotations

import math

from autotrader.backtest.metrics import (
    _annualize_return,
    per_regime_stats,
    per_setup_stats,
    summarize,
)
from autotrader.backtest.types import EquityPoint, SimTrade


def _trade(net_pnl=100.0, gross_pnl=120.0, costs=20.0, realized_r=1.0,
           setup="BREAKOUT", regime="RANGE", bars_held=5,
           mfe_r=1.5, mae_r=-0.3):
    return SimTrade(
        trade_id="t1", symbol="X", side="BUY", qty=10, setup=setup, is_swing=False,
        entry_ts="2026-04-16T09:30:00+05:30", entry_price=100.0,
        exit_ts="2026-04-16T10:30:00+05:30", exit_price=110.0,
        initial_sl=99.0, target=110.0, sl_dist=1.0,
        gross_pnl=gross_pnl, costs=costs, net_pnl=net_pnl, realized_r=realized_r,
        mfe_r=mfe_r, mae_r=mae_r, bars_held=bars_held,
        exit_reason="TARGET_HIT", regime_at_entry=regime,
    )


def _eq(ts, equity, cash=None):
    return EquityPoint(ts=ts, equity=equity, cash=cash or equity,
                       positions_value=0.0, drawdown_pct=0.0,
                       open_positions=0)


def test_summarize_empty_trades_returns_baseline():
    out = summarize(trades=[], equity_curve=[], starting_cash=1_000_000.0)
    assert out["n_trades"] == 0
    assert out["net_pnl"] == 0.0
    assert out["sharpe"] == 0.0
    assert out["starting_cash"] == 1_000_000.0


def test_win_rate_and_expectancy():
    trades = [_trade(net_pnl=100), _trade(net_pnl=-50), _trade(net_pnl=200)]
    eq = [_eq("2026-04-16T09:30:00+05:30", 1_000_000.0)]
    out = summarize(trades=trades, equity_curve=eq, starting_cash=1_000_000.0)
    assert out["n_trades"] == 3
    assert out["wins"] == 2
    assert out["losses"] == 1
    assert out["win_rate"] == round(2 / 3 * 100, 2)
    assert out["expectancy_inr"] == round((100 - 50 + 200) / 3, 2)


def test_profit_factor():
    """PF = sum_wins / |sum_losses|. 300 wins / 100 losses = 3.0"""
    trades = [_trade(net_pnl=300), _trade(net_pnl=-100)]
    eq = [_eq("2026-04-16T09:30:00+05:30", 1_000_000.0)]
    out = summarize(trades=trades, equity_curve=eq, starting_cash=1_000_000.0)
    assert out["profit_factor"] == 3.0


def test_max_drawdown_pct_simple():
    """Equity curve: 1M → 1.1M → 0.9M. Peak=1.1M, trough=0.9M → DD=18.18%."""
    eq = [
        _eq("2026-04-16T09:30:00+05:30", 1_000_000),
        _eq("2026-04-16T10:00:00+05:30", 1_100_000),
        _eq("2026-04-16T10:30:00+05:30", 900_000),
    ]
    out = summarize(trades=[_trade()], equity_curve=eq, starting_cash=1_000_000.0)
    # (1.1M - 0.9M) / 1.1M = 18.1818%
    assert 18.0 < out["max_drawdown_pct"] < 18.3


def test_per_setup_groups_correctly():
    trades = [
        _trade(net_pnl=100, setup="BREAKOUT"),
        _trade(net_pnl=-50, setup="BREAKOUT"),
        _trade(net_pnl=200, setup="MEAN_REVERSION"),
    ]
    out = per_setup_stats(trades)
    assert "BREAKOUT" in out
    assert "MEAN_REVERSION" in out
    assert out["BREAKOUT"]["n"] == 2
    assert out["MEAN_REVERSION"]["n"] == 1
    assert out["BREAKOUT"]["net_pnl"] == 50.0


def test_sharpe_zero_when_no_variance():
    """Constant equity → no daily returns → Sharpe = 0."""
    eq = [
        _eq("2026-04-16T09:30:00+05:30", 1_000_000),
        _eq("2026-04-17T09:30:00+05:30", 1_000_000),
    ]
    out = summarize(trades=[_trade()], equity_curve=eq, starting_cash=1_000_000.0)
    assert out["sharpe"] == 0.0


def test_sharpe_positive_for_uptrend():
    """Steady up-trend equity → Sharpe should be finite and positive."""
    eq = [
        _eq("2026-04-16T09:30:00+05:30", 1_000_000),
        _eq("2026-04-17T09:30:00+05:30", 1_010_000),
        _eq("2026-04-18T09:30:00+05:30", 1_020_500),
        _eq("2026-04-19T09:30:00+05:30", 1_031_500),
    ]
    out = summarize(trades=[_trade()], equity_curve=eq, starting_cash=1_000_000.0)
    assert out["sharpe"] > 0
    assert math.isfinite(out["sharpe"])


def test_per_regime_aggregates_by_regime_at_entry():
    trades = [
        _trade(regime="TREND", net_pnl=200),
        _trade(regime="RANGE", net_pnl=-50),
        _trade(regime="TREND", net_pnl=100),
    ]
    out = per_regime_stats(trades)
    assert out["TREND"]["n"] == 2
    assert out["RANGE"]["n"] == 1
    assert out["TREND"]["net_pnl"] == 300.0


# ── Annualization guards ────────────────────────────────────────────────


def _curve_over_n_days(n_days: int, *, ending_equity: float = 1_050_000.0,
                       starting_equity: float = 1_000_000.0) -> list[EquityPoint]:
    """Build an equity curve spanning `n_days` distinct calendar days,
    moving linearly from starting_equity to ending_equity."""
    pts: list[EquityPoint] = []
    for i in range(n_days):
        # Distinct YYYY-MM-DD per index (ts[:10] is what _annualize_return buckets on)
        day = f"2026-04-{(i % 28) + 1:02d}"
        # If we span >28 days, push month forward — keeps timestamps unique-day.
        month = 4 + (i // 28)
        day = f"2026-{month:02d}-{((i % 28) + 1):02d}"
        eq = starting_equity + (ending_equity - starting_equity) * (i + 1) / n_days
        pts.append(_eq(f"{day}T15:30:00+05:30", eq))
    return pts


def test_annualize_short_window_returns_total_unchanged():
    """Guard 1: < 21 trading days is too short to extrapolate — return total_ret_pct.

    Why this matters: a 14-day run with -25% return geometric-extrapolates to
    a number that's mathematically valid but operationally meaningless.
    """
    curve = _curve_over_n_days(14)
    # With n_days=14 the function should bypass extrapolation and pass through
    # the input unchanged regardless of magnitude.
    assert _annualize_return(curve, total_ret_pct=-25.0) == -25.0
    assert _annualize_return(curve, total_ret_pct=300.0) == 300.0
    assert _annualize_return(curve, total_ret_pct=0.0) == 0.0


def test_annualize_account_blowup_clamps_to_minus_100():
    """Guard 2: total_ret_pct ≤ -100% (account went bust) — return -100.

    Geometric extrapolation of base ≤ 0 is undefined (NaN/complex in Python).
    """
    curve = _curve_over_n_days(60)
    # -100% exactly: base = 0, undefined → -100.
    assert _annualize_return(curve, total_ret_pct=-100.0) == -100.0
    # Below -100% (impossible in equity terms but defensively handled).
    assert _annualize_return(curve, total_ret_pct=-150.0) == -100.0


def test_annualize_caps_absurd_magnitudes():
    """Guard 3: extreme one-day spikes that compound to > 9999% are capped.

    Pre-fix the smoke run produced 3.6e+25 for a -27% return over 14 IST days
    after the n_days<21 guard kicked in (so this test uses 25 days and a
    return that, when extrapolated, lands above the cap).
    """
    curve = _curve_over_n_days(25)
    # 100% over 25 days extrapolates to (2.0 ** (252/25) - 1)*100 ≈ 109,000% — must cap.
    out = _annualize_return(curve, total_ret_pct=100.0)
    assert out == 9999.0


def test_annualize_normal_case_geometric_compound():
    """Sanity: 10% over 252 trading days should round-trip to ~10% annualized."""
    curve = _curve_over_n_days(252)
    out = _annualize_return(curve, total_ret_pct=10.0)
    # Allow tiny float wiggle.
    assert 9.99 < out < 10.01


def test_annualize_empty_curve_returns_zero():
    assert _annualize_return([], total_ret_pct=50.0) == 0.0


def test_annualize_mid_range_loss_compounds_correctly():
    """A -20% drawdown over ~63 trading days (one quarter) should annualize
    to roughly -59% [(0.8 ** 4 - 1)*100 = -59.04%], NOT 1e+20."""
    curve = _curve_over_n_days(63)
    out = _annualize_return(curve, total_ret_pct=-20.0)
    # 0.8 ** (252/63) = 0.8 ** 4 = 0.4096 → -59.04%
    assert -60.0 < out < -58.0
    # Crucially: must be finite, must not blow up.
    assert math.isfinite(out)
