"""Regression tests for swing compounding + liquidity-cap sizing (2026-07-03).

Pins the calc_swing_position_size overrides that implement the 9.7% config:
- risk_override / capital_override → size off ROLLING equity (compounding)
- max_qty → liquidity cap (≤ X% of daily turnover)
All default to None → identical to the pre-compounding flat behavior (guarded).
"""
from autotrader.domain.risk import calc_swing_position_size
from autotrader.settings import StrategySettings


def _cfg():
    return StrategySettings(
        capital_swing=500_000, swing_risk_per_trade=7500,
        swing_atr_sl_mult=2.5, swing_rr=2.0,
    )


def test_flat_behavior_unchanged_when_no_overrides():
    # No overrides → uses swing_risk_per_trade + channel_capital. Must be stable.
    p = calc_swing_position_size(500.0, 10.0, "BUY", _cfg())
    # sl_dist = 2.5*10 = 25; raw = 7500//25 = 300; cap = 20%*5L/500 = 200 → 200
    assert p.qty == 200


def test_compounding_scales_risk_and_cap_up():
    cfg = _cfg()
    flat = calc_swing_position_size(500.0, 10.0, "BUY", cfg)
    # Equity grown to ₹10L, 2% risk = ₹20k, cap = 20% of ₹10L = ₹2L → 400 shares
    comp = calc_swing_position_size(500.0, 10.0, "BUY", cfg,
                                    risk_override=20_000, capital_override=1_000_000)
    assert comp.qty > flat.qty
    assert comp.qty == 400  # cap binds: 20% of ₹10L / ₹500


def test_compounding_shrinks_after_drawdown():
    cfg = _cfg()
    # Equity fell to ₹3L, 2% = ₹6k risk → raw 6000//25 = 240; cap 20%*3L/500=120 → 120
    comp = calc_swing_position_size(500.0, 10.0, "BUY", cfg,
                                    risk_override=6_000, capital_override=300_000)
    assert comp.qty == 120


def test_liquidity_cap_binds():
    cfg = _cfg()
    # Without cap: qty 200. With max_qty=50 (thin stock) → capped to 50.
    capped = calc_swing_position_size(500.0, 10.0, "BUY", cfg, max_qty=50)
    assert capped.qty == 50


def test_liquidity_cap_zero_blocks_trade():
    # Fail-closed path: missing turnover → max_qty=0 → qty=0 → skipped downstream.
    cfg = _cfg()
    p = calc_swing_position_size(500.0, 10.0, "BUY", cfg, max_qty=0)
    assert p.qty == 0


def test_liquidity_cap_does_not_inflate():
    # max_qty larger than the risk/capital qty must NOT raise qty (it's a ceiling).
    cfg = _cfg()
    p = calc_swing_position_size(500.0, 10.0, "BUY", cfg, max_qty=10_000)
    assert p.qty == 200  # still bound by the 20% capital cap, not inflated


def test_compounding_plus_liquidity_cap_together():
    cfg = _cfg()
    # Compounded would be 400, but liquidity caps to 150 → 150 wins.
    p = calc_swing_position_size(500.0, 10.0, "BUY", cfg,
                                 risk_override=20_000, capital_override=1_000_000, max_qty=150)
    assert p.qty == 150


def test_max_loss_consistent_after_cap():
    # max_loss must reflect the FINAL (capped) qty, not the pre-cap qty.
    cfg = _cfg()
    p = calc_swing_position_size(500.0, 10.0, "BUY", cfg, max_qty=50)
    assert p.qty == 50
    # sl_dist = 25; max_loss ≈ 50*25 + brokerage
    assert 1200 < p.max_loss < 1350
