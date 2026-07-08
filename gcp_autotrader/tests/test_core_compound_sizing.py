"""Tests for the CORE compounding sizing fix (2026-07-08).

CORE previously sized new buys off the FIXED channel_capital and never reinvested gains
(~30% idle cash). The fix: `nav_sizing=True` values stayers at their CURRENT price and the
caller passes the current NAV, so the freed cash + reinvested gains get deployed. Gated by
`core_compound_sizing` (default True, env CORE_COMPOUND_SIZING). CORE-only.
"""
from __future__ import annotations

import inspect

from autotrader.services.core_trading_service import plan_core_rebalance


def _target(n=30, price=100.0):
    return [{"symbol": f"S{i}", "ref_price": price, "instrument_key": f"k{i}"} for i in range(n)]


def test_nav_sizing_default_is_false_preserves_prod():
    """Default (no nav_sizing arg) must equal explicit nav_sizing=False = prod pre-2026-07."""
    target = _target()
    holdings = [{"symbol": "S0", "qty": 10, "entry_price": 50.0, "instrument_key": "k0"}]
    default = plan_core_rebalance(target, holdings, 300000.0, None)
    explicit_fixed = plan_core_rebalance(target, holdings, 300000.0, None, nav_sizing=False)
    assert default == explicit_fixed


def test_compound_deploys_more_off_grown_nav():
    """With the book grown, compounding (nav_sizing + NAV as capital) deploys materially more
    than fixed sizing off the original ₹3L — the idle-cash fix."""
    target = _target()
    # 10 stayers bought at 50, now worth 100 (2x) — the appreciated book
    holdings = [{"symbol": f"S{i}", "qty": 10, "entry_price": 50.0, "instrument_key": f"k{i}"} for i in range(10)]
    fixed = plan_core_rebalance(target, holdings, 300000.0, None, nav_sizing=False)
    nav = plan_core_rebalance(target, holdings, 400000.0, None, nav_sizing=True)   # NAV grown to 4L
    fixed_buy = sum(b["qty"] * b["entry_price"] for b in fixed["buys"])
    nav_buy = sum(b["qty"] * b["entry_price"] for b in nav["buys"])
    assert nav_buy > fixed_buy, (nav_buy, fixed_buy)


def test_nav_sizing_values_stayers_at_current_not_entry():
    """Same capital, appreciated stayers: nav_sizing values them at CURRENT price (higher) ->
    smaller new-buy budget than fixed (entry basis). Proves the basis actually switched."""
    target = _target()
    holdings = [{"symbol": f"S{i}", "qty": 10, "entry_price": 50.0, "instrument_key": f"k{i}"} for i in range(10)]
    fixed = plan_core_rebalance(target, holdings, 300000.0, None, nav_sizing=False)   # held_cost = 10*10*50
    nav = plan_core_rebalance(target, holdings, 300000.0, None, nav_sizing=True)      # held_cost = 10*10*100
    fixed_buy = sum(b["qty"] * b["entry_price"] for b in fixed["buys"])
    nav_buy = sum(b["qty"] * b["entry_price"] for b in nav["buys"])
    assert nav_buy < fixed_buy   # higher stayer valuation -> less budget at the same capital


def test_settings_has_core_compound_flag_default_true():
    from autotrader import settings as st
    src = inspect.getsource(st)
    assert "core_compound_sizing: bool = True" in src
    assert 'core_compound_sizing=_env_bool("CORE_COMPOUND_SIZING", True)' in src


def test_run_core_rebalance_wires_compound_off_nav():
    from autotrader.services import core_trading_service as m
    src = inspect.getsource(m)
    assert "core_compound_sizing" in src          # reads the flag
    assert "nav_sizing=nav_sizing" in src          # passes it to plan
    assert "sum(int(p.get(\"qty\") or 0) * _cur_px(p)" in src  # computes NAV from held x current price
