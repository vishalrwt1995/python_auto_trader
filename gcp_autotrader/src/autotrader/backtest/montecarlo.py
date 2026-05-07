"""Monte Carlo bootstrap for backtest equity curves.

Given a list of closed trades, this resamples them (with replacement) N times
to construct a distribution of possible equity outcomes. Useful for:
  * "Is my Sharpe statistically significant?"  (CI on Sharpe)
  * "What's the 5%-tail drawdown I should plan capital around?"  (VaR)
  * "Could this strategy plausibly produce a flat year by chance?"

Caveats
-------
* Independent-trades assumption: bootstrap presumes trades are i.i.d. They're
  not — there are regime / portfolio correlations. The result is directional,
  not a literal probability.
* Doesn't reorder by time — equity curve uses synthetic order. Use this for
  drawdown/return distributions, NOT for time-aware metrics.
"""
from __future__ import annotations

import random
import statistics
from dataclasses import dataclass

from autotrader.backtest.types import SimTrade


@dataclass
class MonteCarloResult:
    runs: int
    median_total_pnl: float
    p05_total_pnl: float        # 5th percentile (bad outcome)
    p95_total_pnl: float        # 95th percentile (good outcome)
    median_max_dd_pct: float
    p95_max_dd_pct: float       # the worst-DD case in 95% of runs
    p_loss: float               # probability of negative total P&L
    p_doubles: float            # probability of 2× starting capital


def bootstrap_trades(
    trades: list[SimTrade],
    *,
    starting_cash: float = 1_000_000.0,
    runs: int = 5_000,
    seed: int = 1729,
) -> MonteCarloResult:
    if not trades:
        return MonteCarloResult(
            runs=0, median_total_pnl=0.0, p05_total_pnl=0.0, p95_total_pnl=0.0,
            median_max_dd_pct=0.0, p95_max_dd_pct=0.0,
            p_loss=0.0, p_doubles=0.0,
        )
    rng = random.Random(seed)
    n = len(trades)
    pnl_values = [t.net_pnl for t in trades]

    total_pnls: list[float] = []
    max_dds: list[float] = []
    n_loss = 0
    n_double = 0

    for _ in range(runs):
        sample = [pnl_values[rng.randrange(n)] for _ in range(n)]
        # Walk equity through this synthetic order to compute DD.
        equity = starting_cash
        peak = starting_cash
        max_dd = 0.0
        for p in sample:
            equity += p
            if equity > peak:
                peak = equity
            if peak > 0:
                dd = (peak - equity) / peak * 100
                if dd > max_dd:
                    max_dd = dd
        total = equity - starting_cash
        total_pnls.append(total)
        max_dds.append(max_dd)
        if total < 0:
            n_loss += 1
        if equity >= starting_cash * 2:
            n_double += 1

    total_pnls.sort()
    max_dds.sort()

    def _percentile(seq: list[float], p: float) -> float:
        if not seq:
            return 0.0
        idx = int(p / 100 * (len(seq) - 1))
        return seq[idx]

    return MonteCarloResult(
        runs=runs,
        median_total_pnl=round(statistics.median(total_pnls), 2),
        p05_total_pnl=round(_percentile(total_pnls, 5), 2),
        p95_total_pnl=round(_percentile(total_pnls, 95), 2),
        median_max_dd_pct=round(statistics.median(max_dds), 3),
        p95_max_dd_pct=round(_percentile(max_dds, 95), 3),
        p_loss=round(n_loss / runs, 4),
        p_doubles=round(n_double / runs, 4),
    )


__all__ = ["MonteCarloResult", "bootstrap_trades"]
