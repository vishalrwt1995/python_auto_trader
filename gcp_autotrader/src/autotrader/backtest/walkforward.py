"""Walk-forward harness — rolling train/test windows over historical data.

Walk-forward splits a long backtest window into successive (train, test)
folds:
    fold 1: train [d0, d0+train), test [d0+train, d0+train+test)
    fold 2: train [d0+test, d0+test+train), test [d0+test+train, ...)
    ...

For each fold, the user supplies a `train_eval` callable that takes the
training window and returns "tuned parameters" — typically a `dict[str, Any]`
representing the strategy config. Then the same harness runs the strategy
with those params over the test window and aggregates results.

The harness does NOT do parameter optimization itself — that's the user's
callable. It only provides the windowing + aggregation infrastructure.
This keeps the harness honest about what's "tuning" vs. "evaluation."

Out-of-sample protection
------------------------
The first `holdout_days` of the input range are RESERVED — the harness
refuses to expose them in any train fold. They're available only via the
final `holdout_eval` step, which runs after walk-forward folds finish.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Callable

log = logging.getLogger(__name__)


@dataclass
class WalkForwardConfig:
    train_days: int = 60
    test_days: int = 20
    holdout_days: int = 0     # 0 = no holdout
    step_days: int | None = None   # default = test_days (non-overlapping test)


@dataclass
class Fold:
    fold_idx: int
    train_since: str
    train_until: str
    test_since: str
    test_until: str


def make_folds(
    *, since: str, until: str, cfg: WalkForwardConfig,
) -> list[Fold]:
    """Compute the list of train/test folds. `since` and `until` are
    YYYY-MM-DD inclusive; the function clamps test windows that would run
    past `until` and drops the final partial fold if it's < 50% of test_days.
    """
    d_since = date.fromisoformat(since)
    d_until = date.fromisoformat(until)
    if d_since >= d_until:
        return []

    holdout_end = d_until
    holdout_start = (
        d_until - timedelta(days=cfg.holdout_days - 1)
        if cfg.holdout_days > 0 else d_until + timedelta(days=1)
    )
    # Walk-forward operates over [d_since, holdout_start)
    wf_until = holdout_start - timedelta(days=1)

    step = timedelta(days=cfg.step_days or cfg.test_days)
    folds: list[Fold] = []
    cursor = d_since
    fold_idx = 0
    while True:
        train_end = cursor + timedelta(days=cfg.train_days - 1)
        test_start = train_end + timedelta(days=1)
        test_end = test_start + timedelta(days=cfg.test_days - 1)
        if test_start > wf_until:
            break
        if test_end > wf_until:
            test_end = wf_until
        # Drop fold if test window < 50% of intended.
        actual_test_days = (test_end - test_start).days + 1
        if actual_test_days < (cfg.test_days // 2):
            break
        folds.append(Fold(
            fold_idx=fold_idx,
            train_since=cursor.isoformat(),
            train_until=train_end.isoformat(),
            test_since=test_start.isoformat(),
            test_until=test_end.isoformat(),
        ))
        cursor += step
        fold_idx += 1
    return folds


def holdout_window(*, since: str, until: str, cfg: WalkForwardConfig) -> tuple[str, str] | None:
    """Return (since, until) for the held-out test window, or None if
    holdout_days==0."""
    if cfg.holdout_days <= 0:
        return None
    d_until = date.fromisoformat(until)
    holdout_start = d_until - timedelta(days=cfg.holdout_days - 1)
    return (holdout_start.isoformat(), until)


def run_walk_forward(
    *,
    since: str, until: str,
    cfg: WalkForwardConfig,
    train_eval: Callable[[str, str], dict],
    test_eval: Callable[[str, str, dict], dict],
    holdout_eval: Callable[[str, str, dict], dict] | None = None,
) -> dict:
    """Run the harness end-to-end.

    `train_eval(train_since, train_until)` -> tuned params dict
    `test_eval(test_since, test_until, params)` -> per-fold metrics dict
    `holdout_eval(holdout_since, holdout_until, final_params)` -> dict
    """
    folds = make_folds(since=since, until=until, cfg=cfg)
    if not folds:
        return {"folds": [], "n_folds": 0, "holdout": None,
                "summary": {}, "notes": "no folds — window too short"}

    fold_results: list[dict] = []
    last_params: dict = {}
    for f in folds:
        params = train_eval(f.train_since, f.train_until)
        last_params = params
        metrics = test_eval(f.test_since, f.test_until, params)
        fold_results.append({
            "fold_idx": f.fold_idx,
            "train": [f.train_since, f.train_until],
            "test": [f.test_since, f.test_until],
            "params": params,
            "metrics": metrics,
        })

    holdout = None
    if holdout_eval is not None:
        win = holdout_window(since=since, until=until, cfg=cfg)
        if win:
            holdout = holdout_eval(win[0], win[1], last_params)

    # Aggregate test-window metrics: mean expectancy_r, sum net_pnl, etc.
    summary = _aggregate_folds([r["metrics"] for r in fold_results])
    return {
        "folds": fold_results,
        "n_folds": len(fold_results),
        "holdout": holdout,
        "summary": summary,
    }


def _aggregate_folds(per_fold: list[dict]) -> dict:
    if not per_fold:
        return {}
    keys = ("n_trades", "wins", "net_pnl", "expectancy_r", "win_rate", "sharpe", "max_drawdown_pct")
    agg: dict[str, float] = {}
    for k in keys:
        vals = [m.get(k, 0.0) for m in per_fold if isinstance(m.get(k), (int, float))]
        if not vals:
            continue
        if k in ("n_trades", "wins", "net_pnl"):
            agg[k] = round(sum(vals), 4)
        else:
            agg[k] = round(sum(vals) / len(vals), 4)
    return agg


__all__ = [
    "WalkForwardConfig", "Fold",
    "make_folds", "holdout_window", "run_walk_forward",
]
