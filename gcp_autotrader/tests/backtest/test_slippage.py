"""Slippage model tests."""
from __future__ import annotations

from autotrader.backtest.slippage import BarRangePct, FixedBps, NoSlippage
from autotrader.backtest.types import Bar


def _bar(o=100.0, h=101.0, low_=99.0, c=100.5):
    return Bar(symbol="TEST", ts="2026-04-16T09:30:00+05:30",
               open=o, high=h, low=low_, close=c, volume=1000.0, timeframe="5m")


def test_no_slippage_returns_theoretical():
    m = NoSlippage()
    assert m.adjust(theoretical=100.0, side="BUY", bar=_bar()) == 100.0
    assert m.adjust(theoretical=100.0, side="SELL", bar=_bar()) == 100.0


def test_fixed_bps_adverse_for_both_sides():
    """FixedBps 5: BUY pays +0.05%, SELL receives -0.05%."""
    m = FixedBps(bps=5.0)
    buy = m.adjust(theoretical=100.0, side="BUY", bar=_bar())
    sell = m.adjust(theoretical=100.0, side="SELL", bar=_bar())
    assert buy == 100.05
    assert sell == 99.95


def test_bar_range_pct_caps_at_max_bps():
    """Wide-range bar: slippage caps at cap_bps. 100₹ × 25bp = ₹0.25."""
    m = BarRangePct(pct_of_range=0.10, cap_bps=25.0, floor_bps=1.0)
    wide_bar = _bar(o=100, h=110, low_=90, c=100)   # range=20, 10% = ₹2 → over cap
    buy = m.adjust(theoretical=100.0, side="BUY", bar=wide_bar)
    assert buy == 100.25   # capped


def test_bar_range_pct_floors_at_min_bps():
    """Tight bar: slippage at least floor_bps."""
    m = BarRangePct(pct_of_range=0.10, cap_bps=25.0, floor_bps=1.0)
    tight_bar = _bar(o=100, h=100.0, low_=100.0, c=100.0)  # range=0
    buy = m.adjust(theoretical=100.0, side="BUY", bar=tight_bar)
    assert buy == 100.01   # floor 1bp = 0.01


def test_bar_range_pct_proportional_within_band():
    """Range=2, 10% of range = 0.20 → between floor (0.01) and cap (0.25)."""
    m = BarRangePct(pct_of_range=0.10, cap_bps=25.0, floor_bps=1.0)
    bar = _bar(o=100, h=101, low_=99, c=100)  # range=2
    buy = m.adjust(theoretical=100.0, side="BUY", bar=bar)
    assert buy == 100.20
    sell = m.adjust(theoretical=100.0, side="SELL", bar=bar)
    assert sell == 99.80
