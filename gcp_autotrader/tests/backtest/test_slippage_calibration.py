"""Slippage calibration tests — verifies the fit math without touching BQ.

`load_calibration_data` is a thin BQ wrapper; tested manually against
real data. The maths in `calibrate_from_fills` is the part that needs
to be locked down with unit tests because a silent error there would
flow into every backtest's P&L numbers.
"""
from __future__ import annotations

import math

import pytest

from autotrader.backtest.slippage import BarRangePct
from autotrader.backtest.slippage_calibration import (
    CalibrationFill,
    MIN_RELIABLE_N,
    MIN_USABLE_N,
    _percentile,
    calibrate_from_fills,
)


# ── Per-fill arithmetic ──────────────────────────────────────────────────


def test_buy_adverse_when_actual_above_theoretical():
    """BUY: actual_price > theoretical → adverse > 0."""
    f = CalibrationFill(
        symbol="ACME", side="BUY", entry_ts="2026-04-16T09:30:00+05:30",
        theoretical_price=100.0, actual_price=100.10,
        bar_high=100.5, bar_low=99.5,
    )
    assert f.adverse_per_share() == pytest.approx(0.10)
    # 0.10 / 100 × 10_000 = 10 bps
    assert f.adverse_bps() == pytest.approx(10.0)
    # 0.10 / (100.5 - 99.5) = 0.10
    assert f.pct_of_range() == pytest.approx(0.10)


def test_sell_adverse_when_actual_below_theoretical():
    """SELL: actual_price < theoretical → adverse > 0."""
    f = CalibrationFill(
        symbol="ACME", side="SELL", entry_ts="2026-04-16T09:30:00+05:30",
        theoretical_price=200.0, actual_price=199.50,
        bar_high=200.5, bar_low=199.0,
    )
    assert f.adverse_per_share() == pytest.approx(0.50)
    # 0.50 / 200 × 10_000 = 25 bps
    assert f.adverse_bps() == pytest.approx(25.0)


def test_favorable_fill_clamps_to_zero_adverse():
    """If we got a BETTER fill than theoretical (rare), the calibrator
    treats it as 0 adverse — the slippage model only models the bad direction."""
    f = CalibrationFill(
        symbol="ACME", side="BUY", entry_ts="2026-04-16T09:30:00+05:30",
        theoretical_price=100.0, actual_price=99.95,    # paid LESS = favorable
        bar_high=100.5, bar_low=99.5,
    )
    assert f.adverse_per_share() == 0.0
    assert f.adverse_bps() == 0.0
    assert f.pct_of_range() == 0.0


def test_zero_bar_range_yields_zero_pct():
    """Halted-day bars (high==low) must not divide by zero."""
    f = CalibrationFill(
        symbol="ACME", side="BUY", entry_ts="2026-04-16T09:30:00+05:30",
        theoretical_price=100.0, actual_price=100.10,
        bar_high=100.0, bar_low=100.0,
    )
    assert f.bar_range() == 0.0
    assert f.pct_of_range() == 0.0


# ── Percentile helper ────────────────────────────────────────────────────


def test_percentile_endpoints():
    xs = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert _percentile(xs, 0.0) == 1.0
    assert _percentile(xs, 1.0) == 5.0


def test_percentile_interpolation():
    xs = [10.0, 20.0, 30.0, 40.0, 50.0]
    # p=0.5 → idx = 0.5 * 4 = 2.0 → exactly xs[2] = 30
    assert _percentile(xs, 0.5) == pytest.approx(30.0)
    # p=0.25 → idx = 1.0 → xs[1] = 20
    assert _percentile(xs, 0.25) == pytest.approx(20.0)
    # p=0.10 → idx = 0.4 → 0.6*xs[0] + 0.4*xs[1] = 6 + 8 = 14
    assert _percentile(xs, 0.10) == pytest.approx(14.0)


def test_percentile_empty_list_is_zero():
    assert _percentile([], 0.5) == 0.0


# ── Fit logic ────────────────────────────────────────────────────────────


def _make_fills(n: int, *, slip_bps: float = 10.0, base_price: float = 100.0,
                bar_range: float = 1.0) -> list[CalibrationFill]:
    """Generate `n` synthetic BUY fills with a fixed adverse bps."""
    adverse = base_price * slip_bps / 10_000.0
    return [
        CalibrationFill(
            symbol="X", side="BUY",
            entry_ts=f"2026-04-16T09:{30+i:02d}:00+05:30",
            theoretical_price=base_price,
            actual_price=base_price + adverse,
            bar_high=base_price + bar_range / 2,
            bar_low=base_price - bar_range / 2,
        )
        for i in range(n)
    ]


def test_calibrate_returns_default_for_empty_input():
    result = calibrate_from_fills([])
    assert result.n_fills == 0
    assert result.confidence == "low"
    assert isinstance(result.model, BarRangePct)
    assert "INSUFFICIENT" in result.summary


def test_calibrate_low_confidence_below_reliable_n():
    """5 fills is enough to fit (≥ MIN_USABLE_N) but flagged low confidence."""
    fills = _make_fills(MIN_USABLE_N, slip_bps=10.0)
    result = calibrate_from_fills(fills)
    assert result.n_fills == MIN_USABLE_N
    assert result.confidence == "low"
    assert "WARNING" in result.summary


def test_calibrate_moderate_confidence_at_reliable_n():
    fills = _make_fills(MIN_RELIABLE_N, slip_bps=10.0)
    result = calibrate_from_fills(fills)
    assert result.n_fills == MIN_RELIABLE_N
    assert result.confidence == "moderate"


def test_calibrate_high_confidence_above_2x_reliable_n():
    fills = _make_fills(MIN_RELIABLE_N * 2, slip_bps=10.0)
    result = calibrate_from_fills(fills)
    assert result.confidence == "high"


def test_calibrate_recovers_uniform_pct_of_range():
    """All fills have adverse = 0.10 of bar_range → median pct_of_range
    should round to 0.10 (within rounding precision)."""
    # adverse_per_share = base_price * slip_bps/10_000 = 100 * 10/10_000 = 0.10
    # bar_range = 1.0 → pct = 0.10
    fills = _make_fills(50, slip_bps=10.0, base_price=100.0, bar_range=1.0)
    result = calibrate_from_fills(fills)

    assert result.median_pct_of_range == pytest.approx(0.10, abs=0.001)
    # The fitted model should round-trip into a BarRangePct with that pct.
    assert isinstance(result.model, BarRangePct)
    assert math.isclose(result.model.pct_of_range, 0.10, abs_tol=0.001)


def test_calibrate_clamps_pathological_pct_of_range():
    """An adverse 5x larger than bar_range should be clamped to 0.5
    rather than flowing into BarRangePct with pct_of_range=5.0."""
    fills = _make_fills(50, slip_bps=500.0, base_price=100.0, bar_range=1.0)
    result = calibrate_from_fills(fills)
    # adverse = 100 * 500/10000 = 5.0 per share, bar_range = 1.0 → pct=5.0
    # which the calibrator must clip down to 0.5 (sanity ceiling).
    assert result.model.pct_of_range == 0.5


def test_calibrate_cap_bps_tracks_p95():
    """A fill set with a tail of 50bps fills should produce cap_bps ≈ 50."""
    # 50 fills at 10bps + 5 fills at 50bps. p95 should land near 50bps.
    fills = _make_fills(50, slip_bps=10.0) + _make_fills(5, slip_bps=50.0)
    result = calibrate_from_fills(fills)
    # p95 of [10×50, 50×5] is in the upper region; the calibrator clamps
    # cap_bps to [5, 100] and we expect well above 25 (the default cap).
    assert result.model.cap_bps >= 25.0
    assert result.p95_adverse_bps >= 25.0


def test_calibrate_drops_zero_range_bars_from_fit():
    """Zero-range bars (halts) must be dropped before fitting, not crash."""
    valid = _make_fills(20, slip_bps=10.0)
    halted = [
        CalibrationFill(
            symbol="HALT", side="BUY", entry_ts="2026-04-16T10:00:00+05:30",
            theoretical_price=100.0, actual_price=100.10,
            bar_high=100.0, bar_low=100.0,    # zero range
        )
        for _ in range(5)
    ]
    result = calibrate_from_fills(valid + halted)
    assert result.n_fills == 20    # halted dropped
    assert result.raw_metrics["n_dropped_zero_range"] == 5


def test_calibrate_drops_zero_theoretical_price_rows():
    """Bad data — theoretical_price=0 — must be dropped silently."""
    valid = _make_fills(15, slip_bps=10.0)
    bad = [
        CalibrationFill(
            symbol="X", side="BUY", entry_ts="ts",
            theoretical_price=0.0, actual_price=10.0,
            bar_high=11.0, bar_low=9.0,
        )
        for _ in range(3)
    ]
    result = calibrate_from_fills(valid + bad)
    assert result.n_fills == 15
    assert result.raw_metrics["n_dropped_bad_price"] == 3


def test_calibrate_floor_bps_respects_minimum():
    """Even when p10 < 0.5 (unrealistically tight fills), floor_bps must
    not drop below 0.5 — we never want the sim to model 'free fills'."""
    # All fills at 0.1 bps → p10 ≈ 0.1, but floor must clamp to 0.5
    fills = _make_fills(50, slip_bps=0.1)
    result = calibrate_from_fills(fills)
    assert result.model.floor_bps >= 0.5
