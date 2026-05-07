"""Live-decision replay tests — verifies scan_decisions → orders flow.

Synthetic ScanDecisionRow + synthetic bars; no BQ involvement.
"""
from __future__ import annotations

from autotrader.backtest.account import SimAccount, SimAccountConfig
from autotrader.backtest.data import ScanDecisionRow
from autotrader.backtest.engine import BacktestEngine
from autotrader.backtest.replay_live import LiveDecisionStrategy, LiveReplayConfig
from autotrader.backtest.slippage import NoSlippage
from autotrader.backtest.types import Bar


def _decision(symbol="ACME", scan_ts="2026-04-16T09:30:00+05:30",
              direction="BUY", qualified=True, blocked_reason="",
              setup="BREAKOUT", ltp=100.0, atr=1.0, atr_mult=1.74):
    return ScanDecisionRow(
        scan_ts=scan_ts, run_date="2026-04-16",
        symbol=symbol, setup=setup, direction=direction,
        raw_score=80.0, adjusted_score=80.0, min_score=70.0,
        qualified=qualified, blocked_reason=blocked_reason,
        ltp=ltp, atr=atr, atr_mult=atr_mult, rsi=55, vwap=100.0,
        regime="RANGE", risk_mode="NORMAL", wl_type="intraday",
        daily_trend="NEUTRAL",
    )


def _bar(sym, ts, o, h, low_, c):
    return Bar(symbol=sym, ts=ts, open=o, high=h, low=low_, close=c,
               volume=1000.0, timeframe="5m")


def test_qualified_decision_fires_order_and_opens_position():
    """A qualified BUY decision at 09:30 opens at the next bar's open."""
    decisions = [_decision()]
    cfg = LiveReplayConfig(per_trade_risk_inr=5_000.0)
    strat = LiveDecisionStrategy(decisions, cfg)
    acc = SimAccount(slippage=NoSlippage())
    eng = BacktestEngine(account=acc, strategy=strat)

    bars = [
        _bar("ACME", "2026-04-16T09:30:00+05:30", 100, 100.5, 99.5, 100),
        _bar("ACME", "2026-04-16T09:35:00+05:30", 100.0, 102, 99.5, 101),
        _bar("ACME", "2026-04-16T09:40:00+05:30", 101, 105, 101, 104),  # gain
        _bar("ACME", "2026-04-16T09:45:00+05:30", 104, 105, 95.5, 96),  # SL @ 100-1.74 = 98.26
    ]
    result = eng.run(bars)
    assert len(result.trades) == 1
    t = result.trades[0]
    assert t.symbol == "ACME"
    assert t.side == "BUY"


def test_blocked_decision_skipped_by_default():
    """blocked_reason set → no order unless `unblock_reasons` includes it."""
    decisions = [_decision(qualified=False, blocked_reason="some_gate")]
    strat = LiveDecisionStrategy(decisions, LiveReplayConfig())
    acc = SimAccount(slippage=NoSlippage())
    eng = BacktestEngine(account=acc, strategy=strat)
    bars = [
        _bar("ACME", "2026-04-16T09:30:00+05:30", 100, 100, 100, 100),
        _bar("ACME", "2026-04-16T09:35:00+05:30", 100, 102, 99, 101),
    ]
    eng.run(bars)
    assert acc.positions == {}
    assert acc.closed_trades == []


def test_unblock_reasons_re_enables_blocked_decision():
    """unblock_reasons=('some_gate',) → that blocked decision becomes eligible."""
    decisions = [_decision(qualified=False, blocked_reason="some_gate")]
    cfg = LiveReplayConfig(unblock_reasons=("some_gate",))
    strat = LiveDecisionStrategy(decisions, cfg)
    acc = SimAccount(slippage=NoSlippage())
    eng = BacktestEngine(account=acc, strategy=strat)
    bars = [
        _bar("ACME", "2026-04-16T09:30:00+05:30", 100, 100, 100, 100),
        _bar("ACME", "2026-04-16T09:35:00+05:30", 100, 100.5, 99.6, 100.2),
        _bar("ACME", "2026-04-16T09:40:00+05:30", 100, 100, 99, 99.5),
    ]
    eng.run(bars)
    # Position opened (and may close at EOD)
    assert len(acc.fills) >= 1
    # First fill is the entry
    assert acc.fills[0].side == "BUY"


def test_setups_filter_excludes_other_setups():
    decisions = [
        _decision(setup="BREAKOUT"),
        _decision(symbol="OTHER", setup="MEAN_REVERSION"),
    ]
    cfg = LiveReplayConfig(setups_filter=("BREAKOUT",))
    strat = LiveDecisionStrategy(decisions, cfg)
    # Only BREAKOUT decision survives `_row_eligible`
    keys = list(strat._by_key.keys())
    syms = [k[0] for k in keys]
    assert "ACME" in syms
    assert "OTHER" not in syms


def test_pyramid_guard_blocks_second_open_per_symbol_side():
    """Two BUY decisions on the same symbol — second should be skipped."""
    d1 = _decision(symbol="ACME", scan_ts="2026-04-16T09:30:00+05:30")
    d2 = _decision(symbol="ACME", scan_ts="2026-04-16T09:45:00+05:30")
    strat = LiveDecisionStrategy([d1, d2], LiveReplayConfig())
    acc = SimAccount(slippage=NoSlippage())
    eng = BacktestEngine(account=acc, strategy=strat)
    bars = [
        _bar("ACME", "2026-04-16T09:30:00+05:30", 100, 100, 100, 100),
        _bar("ACME", "2026-04-16T09:35:00+05:30", 100, 100.5, 99.6, 100),  # fills entry 1
        _bar("ACME", "2026-04-16T09:45:00+05:30", 100, 100, 99.7, 100),    # 2nd decision tick
        _bar("ACME", "2026-04-16T09:50:00+05:30", 100, 100, 99.7, 100),    # would be 2nd fill
    ]
    eng.run(bars)
    # Only ONE entry fill (purpose=ENTRY)
    entry_fills = [f for f in acc.fills if f.parent_tag and acc.cash != 1_000_000.0]
    # Easier: at most one open position at any time AND at most one BUY entry
    buy_entries = [f for f in acc.fills if f.side == "BUY"]
    assert len(buy_entries) == 1


def test_max_concurrent_caps_open_positions():
    """max_concurrent=1 → only one position open at a time across symbols."""
    decisions = [
        _decision(symbol="A", scan_ts="2026-04-16T09:30:00+05:30"),
        _decision(symbol="B", scan_ts="2026-04-16T09:30:00+05:30"),
    ]
    cfg = LiveReplayConfig(max_concurrent=1)
    strat = LiveDecisionStrategy(decisions, cfg)
    acc = SimAccount(slippage=NoSlippage())
    eng = BacktestEngine(account=acc, strategy=strat)
    bars = [
        _bar("A", "2026-04-16T09:30:00+05:30", 100, 100, 100, 100),
        _bar("B", "2026-04-16T09:30:00+05:30", 100, 100, 100, 100),
        _bar("A", "2026-04-16T09:35:00+05:30", 100, 100, 99.7, 100),
        _bar("B", "2026-04-16T09:35:00+05:30", 100, 100, 99.7, 100),
    ]
    eng.run(bars)
    # Only one of A/B should have a BUY entry
    buy_entries = [f for f in acc.fills if f.side == "BUY"]
    assert len(buy_entries) <= 1


def test_pending_meta_is_per_instance_not_class_level():
    """Regression: two strategy instances must NOT share `_pending_meta`."""
    s1 = LiveDecisionStrategy([_decision(symbol="A")], LiveReplayConfig())
    s2 = LiveDecisionStrategy([_decision(symbol="B")], LiveReplayConfig())
    s1._pending_meta["fake"] = None  # type: ignore[assignment]
    assert "fake" not in s2._pending_meta
