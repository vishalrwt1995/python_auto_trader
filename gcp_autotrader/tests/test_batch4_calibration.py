"""Tests for Batch 4 — Calibration (2026-04-22).

Post-mortem review of recent trades highlighted two mis-calibrations:

4.1 Target R:R too high for trend setups. A 2R target meant intraday
    winners (which typically peaked at 1.2-1.5R MFE and faded) tripped
    the trailing-stop post-target logic instead of booking a clean
    TARGET_HIT. At ~35% hit rate × ~1.3R actual capture vs 65% × 1R
    loss, 2R target was NEGATIVE expectancy despite headline R:R.
    Default lowered to 1.25R. MEAN_REVERSION / VWAP_REVERSAL keep 2.0R
    via a new rr_intraday_reversion knob — fade setups need meaningful
    excursion to be worth the counter-trend risk.

4.2 SL multiplier-vs-volatility tier shape was inverted. The old tiers
    widened SL progressively with volatility (high-vol stocks got 1.25×
    on top of base), but high-vol stocks have wider actual intraday
    range so they were stopped anyway at higher loss. Meanwhile mid-vol
    stocks (1.5-3% ATR) sat right inside normal candle noise at the
    1.5× base and got noise-stopped. New tier puts the widener on the
    broad middle; extremes stay at or below base.
"""
from __future__ import annotations

import inspect

from autotrader.domain.risk import calc_position_size
from autotrader.services import trading_service as ts_mod
from autotrader.settings import StrategySettings


# ─── 4.1: target R:R lowered for trend setups ──────────────────────────


def test_rr_intraday_default_is_trend_appropriate():
    """Default rr_intraday must be in the 1.2-1.3R trend-setup band."""
    s = StrategySettings()
    assert 1.1 <= s.rr_intraday <= 1.35, (
        f"rr_intraday out of Batch 4.1 band: {s.rr_intraday}. Expected "
        "1.2-1.3R for trend setups based on MFE distribution analysis."
    )


def test_rr_intraday_reversion_keeps_wider_target():
    """MEAN_REVERSION / VWAP_REVERSAL target must stay wider (>= 1.8R)
    because fades need meaningful snap-back to be worth the counter-trend
    risk."""
    s = StrategySettings()
    assert hasattr(s, "rr_intraday_reversion"), (
        "StrategySettings missing rr_intraday_reversion — Batch 4.1 expected a "
        "separate knob for MEAN_REVERSION / VWAP_REVERSAL target R:R."
    )
    assert s.rr_intraday_reversion >= 1.8, (
        f"rr_intraday_reversion too tight ({s.rr_intraday_reversion}). Fade "
        "strategies need >= 1.8R; 1.25R cuts them off mid-snap."
    )


def test_rr_intraday_reversion_is_strictly_wider_than_trend_default():
    """The reversion target must be strictly wider than the trend default —
    otherwise there's no point having a separate knob."""
    s = StrategySettings()
    assert s.rr_intraday_reversion > s.rr_intraday, (
        f"rr_intraday_reversion={s.rr_intraday_reversion} not wider than "
        f"rr_intraday={s.rr_intraday} — Batch 4.1 expected reversion > trend."
    )


def test_calc_position_size_respects_rr_override():
    """calc_position_size must honour rr_override for reversal strategies."""
    cfg = StrategySettings()
    # Trend: target = entry + sl_dist × rr_intraday (1.25)
    trend = calc_position_size(100.0, 2.0, "BUY", cfg)
    rev = calc_position_size(100.0, 2.0, "BUY", cfg, rr_override=cfg.rr_intraday_reversion)
    assert rev.target > trend.target, (
        f"rr_override didn't widen target: trend={trend.target} rev={rev.target}. "
        "Batch 4.1 expected the reversion override to produce a farther target."
    )
    # Sanity: targets should actually differ by the R:R ratio
    trend_r = (trend.target - trend.entry_price) / trend.sl_dist
    rev_r = (rev.target - rev.entry_price) / rev.sl_dist
    assert abs(trend_r - cfg.rr_intraday) < 0.01, f"trend R = {trend_r}, expected {cfg.rr_intraday}"
    assert abs(rev_r - cfg.rr_intraday_reversion) < 0.01, f"rev R = {rev_r}, expected {cfg.rr_intraday_reversion}"


def test_trading_service_passes_rr_override_for_reversals():
    """trading_service must pass rr_override=cfg.rr_intraday_reversion when
    the setup is MEAN_REVERSION or VWAP_REVERSAL."""
    src = inspect.getsource(ts_mod)
    assert "rr_intraday_reversion" in src, (
        "trading_service doesn't reference rr_intraday_reversion — Batch 4.1 "
        "expected the scanner to route MEAN_REVERSION / VWAP_REVERSAL to the "
        "wider target."
    )
    assert "rr_override" in src, (
        "trading_service doesn't pass rr_override to calc_position_size — "
        "Batch 4.1 expected strategy-aware target routing."
    )
    # The MEAN_REVERSION and VWAP_REVERSAL strings must appear in the override
    # selection block (they do elsewhere too, but this is structural proof)
    assert 'MEAN_REVERSION", "VWAP_REVERSAL' in src or "MEAN_REVERSION\", \"VWAP_REVERSAL" in src, (
        "trading_service rr_override selection must match the reversal strategies."
    )


# ─── 4.2: SL multiplier tier re-shaped ─────────────────────────────────


def test_atr_pct_tier_widens_mid_vol():
    """The ATR-% tier in trading_service must apply a widener multiplier to
    the mid-vol band (1.5-3.0% ATR), not to the high-vol extreme."""
    src = inspect.getsource(ts_mod)
    # New tier structure references 0.015 and 0.030 boundaries
    assert "0.015" in src, (
        "trading_service ATR-% tier missing the 1.5% boundary — Batch 4.2 "
        "expected low/mid split at atr_pct=0.015."
    )
    assert "0.030" in src, (
        "trading_service ATR-% tier missing the 3.0% boundary — Batch 4.2 "
        "expected mid/high split at atr_pct=0.030."
    )


def test_atr_pct_tier_no_longer_escalates_on_high_vol():
    """Old tiers applied a 1.25× multiplier on very-high-vol stocks (atr_pct
    > 2.5%). Batch 4.2 removes this — high-vol stocks stay at base."""
    src = inspect.getsource(ts_mod)
    # The old 0.025 boundary and 1.25 multiplier combination shouldn't both
    # appear in the widener tier anymore.
    import re
    # Find the atr_pct tier block
    m = re.search(
        r"_atr_pct = ind\.atr / ltp.*?_atr_mult = max\(0\.8",
        src, re.DOTALL,
    )
    assert m is not None, "ATR-% tier block not found in trading_service"
    block = m.group(0)
    # The new block must NOT contain both "0.025" and "1.25" paired (the old
    # "very high vol → 1.25×" rule).
    assert "_atr_pct > 0.025" not in block, (
        "Old 'very high vol (>2.5%) → 1.25×' rule still present — Batch 4.2 "
        "removed this escalation because wider SL on already-spiky names just "
        "means more loss per noise-stop, not fewer stops."
    )


def test_atr_pct_mid_vol_receives_widener():
    """The mid-vol widener must multiply _atr_mult by ≥1.15× so the effective
    SL on mid-vol stocks widens meaningfully from the old ~1.0× baseline."""
    src = inspect.getsource(ts_mod)
    import re
    m = re.search(
        r"elif _atr_pct <= 0\.030:\s*\n\s*_atr_mult = round\(_atr_mult \* ([0-9.]+),",
        src,
    )
    assert m is not None, (
        "mid-vol widener line not found — Batch 4.2 expected a clause like "
        "'elif _atr_pct <= 0.030: _atr_mult = round(_atr_mult * 1.20, 3)'."
    )
    mult = float(m.group(1))
    assert mult >= 1.15, (
        f"mid-vol widener multiplier {mult} is not a meaningful widener — "
        "Batch 4.2 expected >= 1.15 so base 1.5 × widener ≈ 1.75+ effective SL."
    )


def test_atr_pct_low_vol_stays_tighter():
    """Low-vol stocks (<1.5% ATR) must still get a tighter SL — less room
    wasted on names that rarely move."""
    src = inspect.getsource(ts_mod)
    import re
    m = re.search(
        r"if _atr_pct < 0\.015:\s*\n\s*_atr_mult = round\(_atr_mult \* ([0-9.]+),",
        src,
    )
    assert m is not None, (
        "low-vol tighter line not found — Batch 4.2 expected a clause like "
        "'if _atr_pct < 0.015: _atr_mult = round(_atr_mult * 0.87, 3)'."
    )
    mult = float(m.group(1))
    assert mult < 1.0, (
        f"low-vol multiplier {mult} is not tighter — Batch 4.2 expected < 1.0."
    )
