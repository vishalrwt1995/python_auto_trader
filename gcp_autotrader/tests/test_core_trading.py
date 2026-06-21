"""CORE-2/3 tests: build_target_basket (signal) + plan_core_rebalance (the rebalancing planner)."""
from __future__ import annotations

import types
import pytest

from autotrader.services import core_signal_service as sig
from autotrader.services import core_trading_service as svc
from autotrader.domain import core_signals as cs


def _bars(n=260, start=100.0, drift=0.001, vol_step=0.0, base_date="2024"):
    """Synthetic ascending daily bars [date,o,h,l,c,v]; drift sets momentum, vol_step sets noise."""
    bars = []
    px = start
    for i in range(n):
        px *= (1 + drift + (vol_step if i % 2 == 0 else -vol_step))
        d = f"20{20 + i // 250:02d}-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}"
        bars.append([f"d{i:04d}", px, px * 1.01, px * 0.99, px, 5_000_000.0])  # turnover ~5cr*px high
    return bars


def test_build_target_basket_picks_30():
    # 35 symbols, varying drift (momentum) and vol_step (volatility) -> blend top-30
    hist = {}
    for k in range(35):
        hist[f"S{k}"] = _bars(drift=0.0005 + k * 0.0002, vol_step=0.001 + (k % 5) * 0.003)
    basket = sig.build_target_basket(hist)
    assert len(basket) == cs.TOPN                       # exactly 30
    assert all("symbol" in c and c["ref_price"] > 0 for c in basket)


def test_build_target_basket_excludes_short_and_illiquid():
    hist = {f"S{k}": _bars(drift=0.001 + k * 0.0003) for k in range(31)}
    hist["SHORT"] = _bars(n=100)                          # too little history -> excluded
    basket = sig.build_target_basket(hist)
    assert "SHORT" not in {c["symbol"] for c in basket}


def _cfg():
    return types.SimpleNamespace()


def _basket(syms):
    return [{"symbol": s, "ref_price": 100.0, "instrument_key": f"ik_{s}"} for s in syms]


def test_plan_rebalance_buys_new_sells_dropped_keeps_stayers():
    target = _basket([f"T{i}" for i in range(30)])
    held = [{"symbol": "T0", "qty": 100, "instrument_key": "ik_T0"},   # stayer (in target)
            {"symbol": "OLD1", "qty": 50, "instrument_key": "ik_OLD1"},  # dropped -> sell
            {"symbol": "OLD2", "qty": 40, "instrument_key": "ik_OLD2"}]  # dropped -> sell
    plan = svc.plan_core_rebalance(target, held, 300000.0, _cfg())
    sells = {s["symbol"] for s in plan["sells"]}
    buys = {b["symbol"] for b in plan["buys"]}
    assert sells == {"OLD1", "OLD2"}                      # dropped names exit
    assert "T0" not in buys                               # stayer not re-bought
    assert len(buys) == 29                                # the 29 new target names
    # equal-weight sizing: 1/30 of 3L / 100 = 100 sh
    assert plan["buys"][0]["qty"] == int((1 / 30) * 300000 // 100)


def test_plan_rebalance_full_fresh():
    target = _basket([f"T{i}" for i in range(30)])
    plan = svc.plan_core_rebalance(target, [], 300000.0, _cfg())
    assert len(plan["buys"]) == 30 and plan["sells"] == []


def test_plan_rebalance_capital_zero_and_empty():
    assert svc.plan_core_rebalance(_basket(["A"]), [], 0.0, _cfg()) == {"sells": [], "buys": []}
    assert svc.plan_core_rebalance([], [{"symbol": "X", "qty": 10}], 300000.0, _cfg()) == {"sells": [], "buys": []}


def test_plan_rebalance_sell_only_when_qty_positive():
    plan = svc.plan_core_rebalance(_basket([f"T{i}" for i in range(30)]),
                                   [{"symbol": "OLD", "qty": 0, "instrument_key": "ik"}], 300000.0, _cfg())
    assert plan["sells"] == []                            # zero-qty holding -> nothing to sell
