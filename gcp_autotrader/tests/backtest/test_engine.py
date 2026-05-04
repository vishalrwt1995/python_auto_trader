"""Engine integration tests — full bar-to-trade flow with synthetic data.

These tests verify the engine wiring without touching BigQuery. We feed
hand-crafted bars and a tiny Strategy that opens one position, then assert
the position closes at SL or target as expected.
"""
from __future__ import annotations

from dataclasses import dataclass

from autotrader.backtest.account import SimAccount, SimAccountConfig
from autotrader.backtest.engine import BacktestEngine, EngineConfig, StrategyContext
from autotrader.backtest.slippage import NoSlippage
from autotrader.backtest.types import Bar, Fill, Position
from autotrader.domain.exit_fsm import ExitState


def _bar(sym, ts, o, h, low_, c, vol=1000.0):
    return Bar(symbol=sym, ts=ts, open=o, high=h, low=low_, close=c,
               volume=vol, timeframe="5m")


@dataclass
class _OneShotStrategy:
    """Places a single MARKET BUY on the first bar; then registers the
    position when the fill lands. SL=99, target=110."""
    fired: bool = False
    pending_tag: str = ""

    def on_bar(self, ctx: StrategyContext) -> None:
        if self.fired:
            return
        order = ctx.account.place_order(
            symbol=ctx.bar.symbol, side="BUY", qty=10,
            order_type="MARKET", ts=ctx.bar.ts, parent_tag="t1", purpose="ENTRY",
        )
        self.pending_tag = order.parent_tag
        self.fired = True

    def on_fill(self, ctx: StrategyContext, fill: Fill) -> None:
        if fill.parent_tag != self.pending_tag:
            return
        if fill.parent_tag in ctx.account.positions:
            return
        pos = Position(
            tag=fill.parent_tag, symbol=fill.symbol, side="BUY", qty=fill.qty,
            setup="TEST", is_swing=False,
            entry_price=fill.price, entry_ts=fill.ts,
            entry_atr=1.0, entry_regime="RANGE",
            initial_sl=99.0, target=110.0,
            sl_dist=fill.price - 99.0,
            current_sl=99.0, best_price=fill.price,
            fsm_state=ExitState.INITIAL.value,
        )
        ctx.account.open_position(fill=fill, position=pos)

    def finalize(self, account: SimAccount) -> None:
        return


def test_engine_opens_and_hits_target():
    """Bar 1: order placed. Bar 2: fill at open=100. Bar 3: high=110 → target hit."""
    bars = [
        _bar("X", "2026-04-16T09:30:00+05:30", 100, 100, 100, 100),
        _bar("X", "2026-04-16T09:35:00+05:30", 100, 102, 99.5, 101),
        _bar("X", "2026-04-16T09:40:00+05:30", 101, 110, 101, 109.5),
    ]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    eng = BacktestEngine(account=acc, strategy=_OneShotStrategy())
    result = eng.run(bars)

    assert len(result.trades) == 1
    t = result.trades[0]
    assert t.exit_reason == "TARGET_HIT"
    assert t.exit_price == 110.0
    assert t.gross_pnl > 0


def test_engine_opens_and_hits_sl():
    """Bar 3 has low=98 → SL=99 hits before any target. Net P&L negative."""
    bars = [
        _bar("X", "2026-04-16T09:30:00+05:30", 100, 100, 100, 100),
        _bar("X", "2026-04-16T09:35:00+05:30", 100, 102, 99.5, 101),
        _bar("X", "2026-04-16T09:40:00+05:30", 101, 102, 98.0, 99.5),
    ]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    eng = BacktestEngine(account=acc, strategy=_OneShotStrategy())
    result = eng.run(bars)

    assert len(result.trades) == 1
    t = result.trades[0]
    assert t.exit_reason == "SL_HIT"
    assert t.exit_price == 99.0
    assert t.net_pnl < 0


def test_engine_pessimistic_sl_before_target_when_both_hit():
    """If a single bar would hit BOTH SL and target, SL fires first."""
    # Bar 3: low=98 (≤SL 99) AND high=111 (≥target 110). SL should win.
    bars = [
        _bar("X", "2026-04-16T09:30:00+05:30", 100, 100, 100, 100),
        _bar("X", "2026-04-16T09:35:00+05:30", 100, 102, 99.5, 101),
        _bar("X", "2026-04-16T09:40:00+05:30", 101, 111, 98.0, 105),
    ]
    acc = SimAccount(slippage=NoSlippage())
    eng = BacktestEngine(account=acc, strategy=_OneShotStrategy())
    result = eng.run(bars)
    assert len(result.trades) == 1
    assert result.trades[0].exit_reason == "SL_HIT"


def test_engine_finalizes_open_positions_at_eod():
    """Position still open after last bar → close as EOD_FORCE."""
    bars = [
        _bar("X", "2026-04-16T09:30:00+05:30", 100, 100, 100, 100),
        _bar("X", "2026-04-16T09:35:00+05:30", 100, 100.5, 99.6, 100.2),
    ]
    acc = SimAccount(slippage=NoSlippage())
    eng = BacktestEngine(account=acc, strategy=_OneShotStrategy())
    result = eng.run(bars)
    assert len(result.trades) == 1
    assert result.trades[0].exit_reason == "EOD_FORCE"


def test_engine_emits_one_equity_point_per_bar():
    bars = [
        _bar("X", "2026-04-16T09:30:00+05:30", 100, 100, 100, 100),
        _bar("X", "2026-04-16T09:35:00+05:30", 100, 102, 99.5, 101),
        _bar("X", "2026-04-16T09:40:00+05:30", 101, 110, 101, 109.5),
    ]
    acc = SimAccount(slippage=NoSlippage())
    eng = BacktestEngine(account=acc, strategy=_OneShotStrategy())
    result = eng.run(bars)
    assert len(result.equity_curve) == len(bars)
