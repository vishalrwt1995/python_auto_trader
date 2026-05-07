"""SimAccount tests — order matching, fill mechanics, position lifecycle."""
from __future__ import annotations

from autotrader.backtest.account import SimAccount, SimAccountConfig
from autotrader.backtest.slippage import NoSlippage
from autotrader.backtest.types import Bar, OrderStatus


def _bar(sym="TEST", ts="2026-04-16T09:35:00+05:30",
         o=100.0, h=101.0, low_=99.0, c=100.5):
    return Bar(symbol=sym, ts=ts, open=o, high=h, low=low_, close=c,
               volume=1000.0, timeframe="5m")


def test_market_order_fills_at_next_bar_open():
    acc = SimAccount(slippage=NoSlippage())
    o = acc.place_order(symbol="TEST", side="BUY", qty=10,
                        order_type="MARKET", ts="2026-04-16T09:30:00+05:30")
    fills = acc.resolve_bar(_bar())
    assert len(fills) == 1
    assert fills[0].price == 100.0
    assert o.status == OrderStatus.FILLED


def test_market_order_does_not_fill_same_bar():
    """If placed_ts >= bar.ts, no fill (no look-ahead)."""
    acc = SimAccount(slippage=NoSlippage())
    bar = _bar(ts="2026-04-16T09:30:00+05:30")
    acc.place_order(symbol="TEST", side="BUY", qty=10,
                    order_type="MARKET", ts="2026-04-16T09:30:00+05:30")
    fills = acc.resolve_bar(bar)
    assert fills == []


def test_limit_buy_fills_only_if_low_crosses():
    acc = SimAccount(slippage=NoSlippage())
    acc.place_order(symbol="TEST", side="BUY", qty=10,
                    order_type="LIMIT", limit_price=99.0,
                    ts="2026-04-16T09:30:00+05:30")
    # Bar low=99.5 → no fill
    fills = acc.resolve_bar(_bar(o=100, h=101, low_=99.5, c=100))
    assert fills == []
    # Bar low=98 → fills at limit_price 99
    acc2 = SimAccount(slippage=NoSlippage())
    acc2.place_order(symbol="TEST", side="BUY", qty=10,
                     order_type="LIMIT", limit_price=99.0,
                     ts="2026-04-16T09:30:00+05:30")
    fills = acc2.resolve_bar(_bar(o=99.5, h=100, low_=98, c=99.5))
    assert len(fills) == 1
    assert fills[0].price == 99.0   # filled at limit, not at low


def test_stop_sell_fills_at_stop_price():
    acc = SimAccount(slippage=NoSlippage())
    acc.place_order(symbol="TEST", side="SELL", qty=10,
                    order_type="STOP", stop_price=99.0,
                    ts="2026-04-16T09:30:00+05:30")
    fills = acc.resolve_bar(_bar(o=100, h=100.5, low_=98, c=99.5))
    assert len(fills) == 1
    assert fills[0].price == 99.0


def test_buy_reduces_cash_by_notional_plus_costs():
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    initial_cash = acc.cash
    acc.place_order(symbol="TEST", side="BUY", qty=100,
                    order_type="MARKET", ts="2026-04-16T09:30:00+05:30")
    acc.resolve_bar(_bar(o=1000.0))
    spent = initial_cash - acc.cash
    # Notional 100,000 + ~30₹ cost
    assert 100_000 < spent < 100_100


def test_mark_to_market_emits_one_equity_point():
    acc = SimAccount(slippage=NoSlippage())
    acc.mark_to_market({"TEST": 100.0}, ts="2026-04-16T09:30:00+05:30")
    assert len(acc.equity_curve) == 1
    assert acc.equity_curve[0].equity == acc.cash


def test_drawdown_pct_tracks_high_water_mark():
    acc = SimAccount(slippage=NoSlippage())
    initial = acc.cash
    # First mark — equity = initial
    acc.mark_to_market({}, "t1")
    assert acc.equity_curve[-1].drawdown_pct == 0.0
    # Manually shrink cash to simulate a loss
    acc.cash = initial * 0.95
    acc.mark_to_market({}, "t2")
    # 5% drawdown
    assert 4.9 < acc.equity_curve[-1].drawdown_pct < 5.1
