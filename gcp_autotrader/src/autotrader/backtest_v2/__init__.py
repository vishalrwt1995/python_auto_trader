"""backtest_v2 — clean rebuild per docs/BACKTEST_PLAN.md.

Principles:
- Import production code as-is. Do NOT reimplement.
- Mock the I/O adapters only.
- Patch time-of-day calls through a `BacktestClock`.
- Validate against BQ ground truth at every phase.

This package is kept separate from `autotrader.backtest` (the old, partially
working replay_pure) so we never accidentally import the old code.
"""
