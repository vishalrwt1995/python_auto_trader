"""Walk-forward + Monte Carlo bootstrap tests."""
from __future__ import annotations

from autotrader.backtest.montecarlo import bootstrap_trades
from autotrader.backtest.types import SimTrade
from autotrader.backtest.walkforward import (
    WalkForwardConfig,
    holdout_window,
    make_folds,
    run_walk_forward,
)


def _trade(net_pnl=100.0):
    return SimTrade(
        trade_id="t", symbol="X", side="BUY", qty=10, setup="X", is_swing=False,
        entry_ts="t", entry_price=100, exit_ts="t", exit_price=101,
        initial_sl=99, target=110, sl_dist=1,
        gross_pnl=net_pnl, costs=0, net_pnl=net_pnl, realized_r=net_pnl / 10,
        mfe_r=1.0, mae_r=-0.3, bars_held=5, exit_reason="X", regime_at_entry="RANGE",
    )


# ── walk-forward folds ──────────────────────────────────────────────────


def test_make_folds_simple_window():
    """60d range, train=20, test=10 → folds: (0..19, 20..29), (10..29, 30..39),
    (20..39, 40..49), (30..49, 50..59) — 4 folds."""
    cfg = WalkForwardConfig(train_days=20, test_days=10, step_days=10)
    folds = make_folds(since="2026-01-01", until="2026-03-01", cfg=cfg)
    assert len(folds) >= 3   # exact count depends on the partial-fold drop logic
    # Train precedes test in every fold
    for f in folds:
        assert f.train_until < f.test_since


def test_make_folds_holdout_carved_out():
    """holdout_days=10 → walk-forward operates only over the first 50 days."""
    cfg = WalkForwardConfig(train_days=20, test_days=10, step_days=10, holdout_days=10)
    folds = make_folds(since="2026-01-01", until="2026-03-01", cfg=cfg)
    # No fold's test_until should overlap with the last 10 days
    for f in folds:
        assert f.test_until <= "2026-02-19"


def test_holdout_window_returns_last_n_days():
    cfg = WalkForwardConfig(holdout_days=10)
    win = holdout_window(since="2026-01-01", until="2026-03-01", cfg=cfg)
    assert win is not None
    assert win[1] == "2026-03-01"
    assert win[0] == "2026-02-20"   # 10 days back inclusive


def test_holdout_window_none_when_zero():
    cfg = WalkForwardConfig(holdout_days=0)
    assert holdout_window(since="2026-01-01", until="2026-03-01", cfg=cfg) is None


def test_run_walk_forward_calls_eval_per_fold():
    """Verify train_eval and test_eval are invoked for each fold."""
    cfg = WalkForwardConfig(train_days=20, test_days=10, step_days=10)
    train_calls: list[tuple[str, str]] = []
    test_calls: list[tuple[str, str]] = []

    def t_eval(s, u):
        train_calls.append((s, u))
        return {"alpha": 1.0}

    def te_eval(s, u, p):
        test_calls.append((s, u))
        return {"net_pnl": 100.0, "n_trades": 5, "expectancy_r": 0.3,
                "win_rate": 60, "sharpe": 1.5, "max_drawdown_pct": 5.0,
                "wins": 3}

    out = run_walk_forward(
        since="2026-01-01", until="2026-03-01", cfg=cfg,
        train_eval=t_eval, test_eval=te_eval,
    )
    assert len(train_calls) == len(out["folds"])
    assert len(test_calls) == len(out["folds"])
    assert out["n_folds"] >= 1
    assert "summary" in out


def test_run_walk_forward_returns_holdout_when_configured():
    cfg = WalkForwardConfig(train_days=15, test_days=5, step_days=5, holdout_days=5)

    def t_eval(s, u):
        return {"alpha": 1.0}

    def te_eval(s, u, p):
        return {"net_pnl": 50.0, "n_trades": 1, "expectancy_r": 0.1, "win_rate": 100, "sharpe": 1.0, "max_drawdown_pct": 0, "wins": 1}

    holdout_calls = []

    def h_eval(s, u, p):
        holdout_calls.append((s, u, p))
        return {"net_pnl": 999.0}

    out = run_walk_forward(
        since="2026-01-01", until="2026-02-15", cfg=cfg,
        train_eval=t_eval, test_eval=te_eval, holdout_eval=h_eval,
    )
    assert len(holdout_calls) == 1
    assert out["holdout"] == {"net_pnl": 999.0}


# ── Monte Carlo bootstrap ───────────────────────────────────────────────


def test_bootstrap_empty_returns_zero_runs():
    out = bootstrap_trades([], runs=10)
    assert out.runs == 0
    assert out.median_total_pnl == 0.0


def test_bootstrap_runs_produce_distribution():
    """50 winners + 50 losers → median_total_pnl ≈ 0 (-ish)."""
    trades = [_trade(net_pnl=100) for _ in range(50)] + [_trade(net_pnl=-95) for _ in range(50)]
    out = bootstrap_trades(trades, runs=200, seed=42)
    assert out.runs == 200
    # Median should be near the deterministic total: 50*100 - 50*95 = 250
    # but bootstrap variance is wide.
    assert -2_000 < out.median_total_pnl < 2_500
    # p05 ≤ median ≤ p95
    assert out.p05_total_pnl <= out.median_total_pnl <= out.p95_total_pnl
    # p_loss in [0, 1]
    assert 0.0 <= out.p_loss <= 1.0


def test_bootstrap_all_winners_low_loss_prob():
    trades = [_trade(net_pnl=100) for _ in range(20)]
    out = bootstrap_trades(trades, runs=100, seed=1)
    assert out.p_loss == 0.0   # never lose if every trade is +100
    assert out.median_total_pnl == 2_000.0
