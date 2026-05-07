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


def test_engine_clock_is_real_epoch_seconds():
    """Regression: FSM expects `tick.ts` and `entry_epoch` in seconds.

    Before this test was introduced, the engine drove the FSM with a
    monotonic bar-counter (incremented by 1.0 per bar) and hardcoded
    `entry_epoch=0.0`. That silently broke `flat_timeout_s` (7200s) and
    `confirm_debounce_s` (15s) — sims never fired FLAT_TIMEOUT and only
    rarely reached CONFIRMED. The fix parses `bar.ts` to epoch.
    """
    from autotrader.backtest.engine import _iso_to_epoch

    bars = [
        _bar("X", "2026-04-16T09:30:00+05:30", 100, 100, 100, 100),
        _bar("X", "2026-04-16T09:35:00+05:30", 100, 100.1, 99.6, 100),
    ]
    acc = SimAccount(slippage=NoSlippage())
    eng = BacktestEngine(account=acc, strategy=_OneShotStrategy())
    eng.run(bars)
    # After the run, sim_epoch should equal the last bar's parsed timestamp,
    # not the bar count (which would be 2.0).
    assert eng._sim_epoch == _iso_to_epoch("2026-04-16T09:35:00+05:30")
    assert eng._sim_epoch > 1_700_000_000.0   # 2023-Nov-ish — definitely not bar count


def test_engine_fsm_debounce_uses_real_seconds():
    """FSM debounce (confirm_debounce_s=15) must compare seconds, not bar counts.

    Before the units fix, the engine used a bar-counter for `tick.ts` and
    `confirm_started_epoch`. That meant the 15-second debounce became
    15-bar (~75 minutes at 5m), and positions almost never reached CONFIRMED.
    With real epoch seconds, two consecutive 5m bars are 300s apart — well
    past 15s — so a position with sustained MFE ≥ 0.8R should reach
    CONFIRMED on the second qualifying bar.
    """
    from autotrader.backtest.types import Position
    from autotrader.domain.exit_fsm import ExitState

    sym = "X"
    # Strategy that opens a position and keeps a handle for inspection.
    class _Strat:
        def __init__(self) -> None:
            self.fired = False
            self.tag = "pos1"

        def on_bar(self, ctx):
            if self.fired:
                return
            ctx.account.place_order(
                symbol=ctx.bar.symbol, side="BUY", qty=10,
                order_type="MARKET", ts=ctx.bar.ts,
                parent_tag=self.tag, purpose="ENTRY",
            )
            self.fired = True

        def on_fill(self, ctx, fill):
            if fill.parent_tag != self.tag or self.tag in ctx.account.positions:
                return
            pos = Position(
                tag=self.tag, symbol=fill.symbol, side="BUY", qty=fill.qty,
                setup="TEST", is_swing=False,
                entry_price=fill.price, entry_ts=fill.ts,
                entry_atr=1.0, entry_regime="RANGE",
                initial_sl=99.0, target=200.0,    # target far away — won't fire
                sl_dist=fill.price - 99.0,
                current_sl=99.0, best_price=fill.price,
                fsm_state=ExitState.INITIAL.value,
            )
            ctx.account.open_position(fill=fill, position=pos)

        def finalize(self, account):
            return

    bars = [
        _bar(sym, "2026-04-16T09:30:00+05:30", 100, 100, 100, 100),
        _bar(sym, "2026-04-16T09:35:00+05:30", 100, 100, 100, 100),    # entry fills @ open=100
        _bar(sym, "2026-04-16T09:40:00+05:30", 100.5, 101, 100.4, 101),  # MFE=1R → confirm_arming
        _bar(sym, "2026-04-16T09:45:00+05:30", 101, 101, 100.9, 101),   # +5min → CONFIRMED
    ]
    # Hook the engine to capture FSM state after each bar so we can assert
    # the CONFIRMED transition happened pre-finalize.
    seen_states: list[str] = []

    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    strat = _Strat()
    eng = BacktestEngine(account=acc, strategy=strat)

    orig_tick = eng._tick_fsm_for_symbol

    def _spy(bar):
        orig_tick(bar)
        p = acc.positions.get("pos1")
        if p is not None:
            seen_states.append(p.fsm_state)

    eng._tick_fsm_for_symbol = _spy   # type: ignore[assignment]
    eng.run(bars)

    # By the 4th bar (09:45), debounce should have elapsed (300s >> 15s) and
    # the FSM should have transitioned INITIAL → CONFIRMED.
    assert ExitState.CONFIRMED.value in seen_states, (
        f"expected CONFIRMED at some point; saw states={seen_states}. "
        "If only INITIAL appears, the debounce is being measured in bar-counts not seconds."
    )
