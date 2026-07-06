"""Regression tests for the realized-cost model (2026-07-03).

Prod's booked net_pnl formerly used risk.calc_round_trip_brokerage, which
under-charged ~3x (STT 0.025%/leg for all trades vs delivery's real 0.1%/leg,
plus no DP charge). That understated cost drag and — critically — compounding
amplified the error year over year. Realized net_pnl now uses the full Upstox
model in backtest/costs.py via order_service._realized_round_trip_cost.

These tests PIN the cost so it cannot silently regress and so backtest
(costs.py) and prod (this helper) stay identical — the precondition for the
compounding CAGR to be prod-replicable.
"""
from autotrader.services.order_service import _realized_round_trip_cost
from autotrader.backtest.costs import compute_round_trip_cost, CostConfig


def test_delivery_20k_matches_documented_upstox_rate():
    # ₹20K delivery round-trip = ₹115.24 (documented Upstox rate, CLAUDE.md Rule 7).
    cost = _realized_round_trip_cost("CNC", 100, 200.0, 200.0)
    assert abs(cost - 115.24) < 0.5, f"expected ~115.24, got {cost}"


def test_intraday_20k_matches_documented_upstox_rate():
    # ₹20K intraday round-trip ≈ ₹54.25 (documented Upstox rate).
    cost = _realized_round_trip_cost("MIS", 100, 200.0, 200.0)
    assert abs(cost - 54.25) < 0.5, f"expected ~54.25, got {cost}"


def test_delivery_costs_more_than_intraday():
    # Delivery pays STT on BOTH legs (0.1% each) + DP charge; intraday pays STT
    # sell-side only. Delivery must be materially more expensive for same notional.
    d = _realized_round_trip_cost("CNC", 100, 200.0, 200.0)
    i = _realized_round_trip_cost("MIS", 100, 200.0, 200.0)
    assert d > i * 1.8, f"delivery {d} should be ~2x intraday {i}"


def test_product_aliases_route_to_delivery():
    # CNC / D / DELIVERY must all be treated as delivery (case-insensitive).
    base = _realized_round_trip_cost("CNC", 100, 200.0, 200.0)
    for alias in ("D", "DELIVERY", "cnc", "delivery"):
        assert abs(_realized_round_trip_cost(alias, 100, 200.0, 200.0) - base) < 0.01


def test_unknown_product_defaults_to_intraday_schedule():
    # Anything not delivery routes to intraday (cheaper) — matches the is_swing
    # derivation used at entry (order_service line ~698).
    unknown = _realized_round_trip_cost("", 100, 200.0, 200.0)
    intraday = _realized_round_trip_cost("MIS", 100, 200.0, 200.0)
    assert abs(unknown - intraday) < 0.01


def test_zero_qty_is_free():
    assert _realized_round_trip_cost("CNC", 0, 200.0, 200.0) == 0.0


def test_matches_costs_module_exactly():
    # The helper must equal costs.py directly — backtest and prod share ONE model.
    u = CostConfig.upstox()
    for product, is_swing in (("CNC", True), ("MIS", False)):
        for qty, entry, ex in ((100, 200.0, 210.0), (37, 512.5, 498.0), (800, 500.0, 500.0)):
            got = _realized_round_trip_cost(product, qty, entry, ex)
            ref = compute_round_trip_cost(qty=qty, entry_price=entry, exit_price=ex,
                                          is_swing=is_swing, cfg=u)
            assert got == ref, f"{product} {qty}: helper {got} != costs.py {ref}"


def test_cost_is_material_vs_old_undercharge():
    # Guard the whole point: the new delivery cost must be substantially higher
    # than the old risk.py under-charge (~₹35.8 on ₹20K). If someone reverts the
    # source, this fails.
    cost = _realized_round_trip_cost("CNC", 100, 200.0, 200.0)
    assert cost > 90.0, f"delivery ₹20K cost {cost} looks like the old under-charge, not full Upstox"
