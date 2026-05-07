"""Tests for M4 — PortfolioBook + DD governors + trading_service wire-up.

Covers:
  * Channel budget math (capital × pct).
  * Channel-exhaustion hard-block.
  * Daily/weekly/monthly DD halts (each in isolation + combined).
  * Daily-throttle as a SOFT decision (allowed, multiplier=0.5).
  * Unknown channel is fail-closed (blocked).
  * DD state math (dd_pct derivation, only losses count, gains don't
    negatively contribute).
  * Structural: trading_service calls check_can_open gated behind
    USE_PORTFOLIO_BOOK_V1 and records channel on position schema.
"""
from __future__ import annotations

import inspect

from autotrader.domain.portfolio_book import (
    DEFAULT_CHANNEL_PCT,
    DdThresholds,
    build_book,
    check_can_open,
)


# ──────────────────────────────────────────────────────────────────────────
# Channel budget math
# ──────────────────────────────────────────────────────────────────────────


def test_default_channel_allocation_sums_to_1():
    total = sum(DEFAULT_CHANNEL_PCT.values())
    assert abs(total - 1.0) < 1e-6


def test_channel_budget_r_is_capital_times_pct():
    book = build_book(capital=50_000.0, open_risk_by_channel={})
    assert book.channel_budget_r("intraday") == round(50_000.0 * 0.40, 2)
    assert book.channel_budget_r("swing") == round(50_000.0 * 0.40, 2)
    assert book.channel_budget_r("positional") == round(50_000.0 * 0.15, 2)
    assert book.channel_budget_r("hedge") == round(50_000.0 * 0.05, 2)


def test_channel_budget_r_zero_for_unknown_channel():
    book = build_book(capital=50_000.0, open_risk_by_channel={})
    assert book.channel_budget_r("ghost") == 0.0


# ──────────────────────────────────────────────────────────────────────────
# check_can_open — hard blocks
# ──────────────────────────────────────────────────────────────────────────


def test_allow_when_headroom_and_no_dd():
    book = build_book(capital=50_000.0, open_risk_by_channel={"intraday": 5_000.0})
    d = check_can_open(book, "intraday", risk_amount=1_000.0)
    assert d.allowed
    assert d.size_multiplier == 1.0


def test_block_when_channel_budget_would_be_exceeded():
    # intraday budget = 40% × 50k = 20k. Already 18k open + 3k new = 21k > 20k → block.
    book = build_book(capital=50_000.0, open_risk_by_channel={"intraday": 18_000.0})
    d = check_can_open(book, "intraday", risk_amount=3_000.0)
    assert not d.allowed
    assert d.reason == "portfolio_channel_budget_exceeded"


def test_block_daily_halt_at_3pct():
    book = build_book(
        capital=50_000.0, open_risk_by_channel={},
        daily_pnl=-(0.031 * 50_000.0),    # 3.1% daily DD
    )
    d = check_can_open(book, "intraday", risk_amount=100.0)
    assert not d.allowed
    assert d.reason == "portfolio_daily_dd_halt"


def test_block_weekly_halt_at_5pct():
    book = build_book(
        capital=50_000.0, open_risk_by_channel={},
        weekly_pnl=-(0.051 * 50_000.0),
    )
    d = check_can_open(book, "intraday", risk_amount=100.0)
    assert not d.allowed
    assert d.reason == "portfolio_weekly_dd_halt"


def test_block_monthly_halt_at_8pct():
    book = build_book(
        capital=50_000.0, open_risk_by_channel={},
        monthly_pnl=-(0.081 * 50_000.0),
    )
    d = check_can_open(book, "intraday", risk_amount=100.0)
    assert not d.allowed
    assert d.reason == "portfolio_monthly_dd_halt"


def test_block_unknown_channel_fail_closed():
    book = build_book(capital=50_000.0, open_risk_by_channel={})
    d = check_can_open(book, "ghost", risk_amount=100.0)
    assert not d.allowed
    assert d.reason == "portfolio_unknown_channel"


# ──────────────────────────────────────────────────────────────────────────
# Soft throttle
# ──────────────────────────────────────────────────────────────────────────


def test_daily_throttle_allows_with_half_size():
    # Daily DD 2% (between 1.5% throttle and 3% halt) → allowed at 50%.
    book = build_book(
        capital=50_000.0, open_risk_by_channel={"intraday": 1_000.0},
        daily_pnl=-(0.02 * 50_000.0),
    )
    d = check_can_open(book, "intraday", risk_amount=500.0)
    assert d.allowed
    assert d.size_multiplier == 0.5


def test_positive_daily_pnl_does_not_trigger_throttle():
    """DD only counts LOSSES. A winning day shouldn't trip the throttle."""
    book = build_book(
        capital=50_000.0, open_risk_by_channel={"intraday": 1_000.0},
        daily_pnl=+(0.05 * 50_000.0),      # 5% UP day
    )
    d = check_can_open(book, "intraday", risk_amount=500.0)
    assert d.allowed
    assert d.size_multiplier == 1.0


def test_monthly_halt_priority_over_daily_throttle():
    """Monthly halt should fire even if the daily pnl is tiny — ordering matters."""
    book = build_book(
        capital=50_000.0, open_risk_by_channel={},
        daily_pnl=0.0,
        monthly_pnl=-(0.09 * 50_000.0),
    )
    d = check_can_open(book, "intraday", risk_amount=100.0)
    assert not d.allowed
    assert d.reason == "portfolio_monthly_dd_halt"


# ──────────────────────────────────────────────────────────────────────────
# DrawdownState math
# ──────────────────────────────────────────────────────────────────────────


def test_drawdown_pct_ignores_positive_pnl():
    book = build_book(capital=100_000.0, open_risk_by_channel={},
                      daily_pnl=+2_000.0, weekly_pnl=+10_000.0)
    assert book.dd.daily_dd_pct == 0.0
    assert book.dd.weekly_dd_pct == 0.0


def test_drawdown_pct_on_zero_capital_is_zero():
    book = build_book(capital=0.0, open_risk_by_channel={}, daily_pnl=-500.0)
    assert book.dd.daily_dd_pct == 0.0


def test_custom_thresholds_override():
    book = build_book(
        capital=50_000.0, open_risk_by_channel={},
        daily_pnl=-(0.01 * 50_000.0),
        thresholds=DdThresholds(daily_throttle_pct=0.005, daily_halt_pct=0.02,
                                 weekly_halt_pct=0.05, monthly_halt_pct=0.08),
    )
    # With stricter thresholds, 1% DD triggers throttle.
    d = check_can_open(book, "intraday", risk_amount=100.0)
    assert d.allowed
    assert d.size_multiplier == 0.5


# ──────────────────────────────────────────────────────────────────────────
# Wire-up: trading_service + order_service structure
# ──────────────────────────────────────────────────────────────────────────


def test_trading_service_wires_portfolio_book_behind_flag():
    from autotrader.services import trading_service
    src = inspect.getsource(trading_service)
    assert "portfolio_check_can_open(" in src
    assert "use_portfolio_book_v1" in src
    assert "build_portfolio_book(" in src


def test_order_service_records_channel_on_position():
    from autotrader.services import order_service
    src = inspect.getsource(order_service)
    # The position doc must include a 'channel' field so the book can
    # aggregate open risk without re-deriving it.
    assert '"channel":' in src


def test_firestore_state_exposes_rolling_pnl_and_open_risk_helpers():
    from autotrader.adapters import firestore_state
    src = inspect.getsource(firestore_state)
    assert "get_realized_pnl_since(" in src
    assert "get_open_risk_by_channel(" in src
