"""Production-grade event-driven backtester for the autotrader algo.

This package replays trading decisions against historical market data so we can
validate strategies, A/B-test gates, and quantify edge before risking capital.

Architecture
------------
* `types`      — Bar, Order, Fill, SimTrade, Position dataclasses (the wire format).
* `costs`      — Indian-market round-trip cost calculator (STT/exchange/SEBI/GST/brokerage).
* `slippage`   — pluggable slippage models (% of bar range / fixed bps / nil).
* `account`    — SimAccount: orders, fills, positions, equity, drawdown.
* `data`       — historical candle + decision + market-brain loaders (BQ + GCS + Upstox).
* `engine`     — event-driven bar-by-bar replay loop with deterministic ordering.
* `replay_live`— "Given the signals the live system actually emitted, what would the
                 sim execution + exit FSM produce?" — pulls from `scan_decisions`.
* `replay_pure`— "Re-run scoring + gates + FSM from scratch on historical candles
                 using current code." — decoupling-driven.
* `metrics`    — Sharpe, Sortino, max DD, MAR, profit factor, expectancy, win rate.
* `reports`    — per-trade CSV + summary stats + per-setup×per-regime tables.
* `walkforward`— rolling train/test windows for parameter validation.
* `montecarlo` — trade-shuffle bootstrap CIs on equity curve.

Honesty disclosures
-------------------
* Options-flow scores are not backfillable — pure-replay runs with score_options=0
  unless the user supplies a snapshot. The cost is logged separately so you can
  see how much it would have moved live results.
* Slippage is modeled, not measured. The default model is a starting point;
  recalibrate as live fills accumulate (`slippage.calibrate_from_fills(...)`).
* Bar-internal SL/TP precedence assumes worst-case (SL hit before TP within
  the same bar). This biases reported P&L downward — the right way to be wrong.
* No look-ahead is enforced by the engine: signals computed at bar T can only
  read bars ≤ T. Violations are asserted at the data-layer boundary.
"""
from __future__ import annotations

__all__ = [
    "types",
    "costs",
    "slippage",
    "account",
    "data",
    "engine",
    "metrics",
    "reports",
]
