"""Cost calculator tests — verifies Indian-market round-trip math.

Spot-checks against Zerodha's published charge calculator. Numbers will
drift if rates change — the goal here is to catch silly bugs (sign errors,
unit confusion, missing GST), not to be a regulatory-compliant rate ledger.
"""
from __future__ import annotations

from autotrader.backtest.costs import (
    CostConfig,
    compute_leg_cost,
    compute_round_trip_cost,
)


def test_zero_qty_or_zero_price_returns_zero():
    assert compute_leg_cost(side="BUY", qty=0, price=100, is_swing=False) == 0.0
    assert compute_leg_cost(side="BUY", qty=10, price=0, is_swing=False) == 0.0


def test_intraday_buy_no_stt_no_dp():
    """Intraday BUY: no STT (sell-only), no DP. Brokerage capped at ₹20."""
    # Notional ₹100k, brokerage = min(20, 30) = 20
    cost = compute_leg_cost(side="BUY", qty=100, price=1000.0, is_swing=False)
    # Brokerage 20 + exchange 2.97 + sebi 0.1 + stamp 3.0 + GST on (b+e+s) = 4.15
    # → ~30.22 ₹
    assert 25 < cost < 35


def test_intraday_sell_includes_stt():
    """Intraday SELL: STT is 0.025% × notional. No stamp duty.
    Diff (sell - buy) ≈ STT_intraday (25₹) − stamp_intraday (3₹) = 22₹."""
    cost_buy = compute_leg_cost(side="BUY", qty=100, price=1000.0, is_swing=False)
    cost_sell = compute_leg_cost(side="SELL", qty=100, price=1000.0, is_swing=False)
    diff = cost_sell - cost_buy
    assert cost_sell > cost_buy
    assert 18 <= diff <= 26   # STT 25 minus stamp 3 = 22


def test_delivery_charges_brokerage_and_dp_on_sell():
    """Delivery (swing) on Upstox default: ₹20 brokerage/leg, full STT both
    sides, DP ₹20 on sell. (Zerodha had free delivery — see zerodha() test.)"""
    cfg = CostConfig()  # Upstox default
    buy = compute_leg_cost(side="BUY", qty=100, price=1000.0, is_swing=True, cfg=cfg)
    sell = compute_leg_cost(side="SELL", qty=100, price=1000.0, is_swing=True, cfg=cfg)
    # Buy: brokerage 20 + STT 100 + exchange 2.97 + sebi 0.1 + GST + stamp 15
    # Sell: brokerage 20 + STT 100 + exchange 2.97 + sebi 0.1 + GST + DP 23.6
    assert buy > 100  # STT alone is 100
    assert sell > buy  # DP (₹23.6) > stamp (₹15) — sell higher
    assert (sell - buy) > 0


def test_zerodha_delivery_is_free_brokerage():
    """Backward compat: CostConfig.zerodha() keeps delivery brokerage at ₹0."""
    cfg = CostConfig.zerodha()
    buy = compute_leg_cost(side="BUY", qty=100, price=1000.0, is_swing=True, cfg=cfg)
    upstox_buy = compute_leg_cost(side="BUY", qty=100, price=1000.0, is_swing=True)
    # Zerodha has no delivery brokerage; Upstox adds ₹20 brokerage + 18% GST on it
    assert abs((upstox_buy - buy) - 20.0 * (1 + 0.18)) < 0.01


def test_default_config_is_upstox():
    """The module default must be Upstox, not Zerodha."""
    assert CostConfig() == CostConfig.upstox()
    assert CostConfig() != CostConfig.zerodha()


def test_verified_roundtrip_values_20k():
    """Lock the externally-verified round-trip costs on a ₹20k position.

    Verified 2026-05-27 against Upstox's published rates + an independent
    worked example. These are regression guards — if they drift, a rate
    changed and the intraday/swing viability math must be re-checked.
    """
    qty, px = 40, 500.0  # ₹20,000 notional
    # Upstox (default)
    intraday_up = compute_round_trip_cost(qty=qty, entry_price=px, exit_price=px, is_swing=False)
    swing_up = compute_round_trip_cost(qty=qty, entry_price=px, exit_price=px, is_swing=True)
    assert abs(intraday_up - 54.25) < 0.5, f"Upstox intraday RT drifted: {intraday_up}"
    assert abs(swing_up - 115.25) < 0.5, f"Upstox swing RT drifted: {swing_up}"
    # Zerodha (legacy)
    z = CostConfig.zerodha()
    intraday_z = compute_round_trip_cost(qty=qty, entry_price=px, exit_price=px, is_swing=False, cfg=z)
    swing_z = compute_round_trip_cost(qty=qty, entry_price=px, exit_price=px, is_swing=True, cfg=z)
    assert abs(intraday_z - 21.20) < 0.5, f"Zerodha intraday RT drifted: {intraday_z}"
    assert abs(swing_z - 60.37) < 0.5, f"Zerodha swing RT drifted: {swing_z}"


def test_round_trip_consistency():
    """Round-trip should equal entry leg + exit leg."""
    rt = compute_round_trip_cost(qty=100, entry_price=1000.0, exit_price=1010.0,
                                  is_swing=False)
    leg_buy = compute_leg_cost(side="BUY", qty=100, price=1000.0, is_swing=False)
    leg_sell = compute_leg_cost(side="SELL", qty=100, price=1010.0, is_swing=False)
    assert rt == round(leg_buy + leg_sell, 2)


def test_brokerage_cap_kicks_in_for_large_notional():
    """For ₹50L notional intraday, brokerage caps at ₹20 not 0.03%×50L=₹1500.
    Total cost should be way less than that (~355₹), driven by exchange + stamp."""
    cost = compute_leg_cost(side="BUY", qty=5_000, price=1000.0, is_swing=False)
    # Brokerage component ≤ ₹20 (capped). Total dominated by exchange (~148.5)
    # + stamp (150) + GST overhead — total under ₹500.
    assert cost < 500
    # Confirm brokerage isn't proportional: 0.03%×50L = ₹1500 worth of brokerage
    # alone would explode the total. We're well under that.
    assert cost < 1_500


def test_swing_buy_has_higher_stamp_than_intraday():
    """Delivery stamp duty (0.015%) is 5× intraday stamp (0.003%)."""
    intraday = compute_leg_cost(side="BUY", qty=100, price=1000.0, is_swing=False)
    swing = compute_leg_cost(side="BUY", qty=100, price=1000.0, is_swing=True)
    assert swing > intraday   # delivery has STT and 5× stamp
