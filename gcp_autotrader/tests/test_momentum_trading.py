"""Unit tests for the Momentum x Low-Vol rebalance plan (services/momentum_trading_service).
Pure plan logic only (the live run_* wrapper is PAPER-validated, not unit-tested)."""
from __future__ import annotations

from autotrader.services.momentum_trading_service import plan_momentum_rebalance


class _Cfg:
    pass


def _basket(syms, price=100.0):
    return [{"symbol": s, "ref_price": price, "instrument_key": f"K{s}"} for s in syms]


def _held(syms, qty=10, price=100.0):
    return [{"symbol": s, "qty": qty, "entry_price": price, "instrument_key": f"K{s}",
             "position_tag": f"T{s}"} for s in syms]


def test_fresh_buys_equal_weight():
    tb = _basket([f"S{i}" for i in range(20)], price=100.0)
    plan = plan_momentum_rebalance(tb, [], 200000.0, _Cfg())
    assert len(plan["sells"]) == 0
    assert len(plan["buys"]) == 20                     # Rs2L / 20 / Rs100 = 100 shares each
    assert all(b["qty"] >= 1 for b in plan["buys"])
    assert {b["reason"] for b in plan["buys"]} == {"MOM_ADD"}


def test_regime_cash_sells_all_no_buys():
    plan = plan_momentum_rebalance([], _held([f"S{i}" for i in range(20)]), 200000.0, _Cfg())
    assert len(plan["sells"]) == 20 and len(plan["buys"]) == 0
    assert {s["reason"] for s in plan["sells"]} == {"MOM_DROP"}


def test_stayers_kept_drops_sold():
    tb = _basket(["A", "B", "C"] + [f"N{i}" for i in range(17)])
    plan = plan_momentum_rebalance(tb, _held(["A", "B", "X", "Y"]), 200000.0, _Cfg())
    sold = {s["symbol"] for s in plan["sells"]}
    bought = {b["symbol"] for b in plan["buys"]}
    assert sold == {"X", "Y"}                           # dropped names exited
    assert "A" not in bought and "B" not in bought      # stayers not re-bought (idempotency)
    assert "C" in bought                                # new name bought


def test_capital_zero_no_buys_but_still_sells_drops():
    plan = plan_momentum_rebalance(_basket(["A", "B"]), _held(["X"]), 0.0, _Cfg())
    assert plan["buys"] == []
    assert {s["symbol"] for s in plan["sells"]} == {"X"}


def test_per_name_cap_excludes_unaffordable():
    # one name priced above the per-name cap (1.5x slice) can't be equal-weighted -> skipped
    tb = _basket(["CHEAP"], price=100.0) + [{"symbol": "PRICEY", "ref_price": 40000.0, "instrument_key": "KP"}]
    plan = plan_momentum_rebalance(tb, [], 200000.0, _Cfg())   # slice=10k, cap=15k < 40k
    bought = {b["symbol"] for b in plan["buys"]}
    assert "CHEAP" in bought and "PRICEY" not in bought
