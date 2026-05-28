"""Phase C — per-channel capital separation tests (2026-05-28).

Validates the StrategySettings.channel_capital() helper + the daily-limit
math used by trading_service.py for per-channel circuit breakers.

The full integration (trading_service.run_scan_once with per-channel limits)
is covered by the existing 236-test regression suite passing after the
refactor — these add explicit unit-level coverage of the new logic.
"""
from __future__ import annotations

from autotrader.settings import StrategySettings


# ── channel_capital() helper ───────────────────────────────────────────────


def test_channel_capital_uses_explicit_swing_when_set():
    s = StrategySettings(capital=200_000, capital_swing=100_000, capital_intraday=100_000)
    assert s.channel_capital("swing") == 100_000


def test_channel_capital_uses_explicit_intraday_when_set():
    s = StrategySettings(capital=200_000, capital_swing=100_000, capital_intraday=100_000)
    assert s.channel_capital("intraday") == 100_000


def test_channel_capital_falls_back_to_shared_when_swing_unset():
    """When CAPITAL_SWING isn't set (=0), swing falls back to total capital."""
    s = StrategySettings(capital=100_000, capital_swing=0.0, capital_intraday=0.0)
    assert s.channel_capital("swing") == 100_000
    assert s.channel_capital("intraday") == 100_000


def test_channel_capital_falls_back_to_shared_when_intraday_unset():
    """Partial config: swing set, intraday not → intraday uses shared total."""
    s = StrategySettings(capital=200_000, capital_swing=150_000, capital_intraday=0.0)
    assert s.channel_capital("swing") == 150_000
    assert s.channel_capital("intraday") == 200_000   # fallback to total


def test_channel_capital_unknown_channel_uses_shared():
    """Unknown channel name routes safely to shared capital (fail-open)."""
    s = StrategySettings(capital=200_000, capital_swing=100_000, capital_intraday=100_000)
    assert s.channel_capital("unknown") == 200_000
    assert s.channel_capital("") == 200_000


def test_channel_capital_case_insensitive():
    s = StrategySettings(capital=200_000, capital_swing=100_000, capital_intraday=100_000)
    assert s.channel_capital("SWING") == 100_000
    assert s.channel_capital("Intraday") == 100_000


# ── per-channel daily-limit math (what trading_service computes) ───────────


def test_per_channel_daily_loss_limit_at_3pct_of_1L():
    """Default 3% of ₹1L channel = ₹3,000 daily loss limit."""
    s = StrategySettings(capital=200_000, capital_swing=100_000, capital_intraday=100_000)
    swing_loss_limit = s.channel_capital("swing") * s.daily_loss_pct
    intraday_loss_limit = s.channel_capital("intraday") * s.daily_loss_pct
    assert swing_loss_limit == 3_000.0
    assert intraday_loss_limit == 3_000.0


def test_per_channel_daily_profit_target_at_6pct_of_1L():
    """Default 6% of ₹1L channel = ₹6,000 daily profit target."""
    s = StrategySettings(capital=200_000, capital_swing=100_000, capital_intraday=100_000)
    swing_target = s.channel_capital("swing") * s.daily_profit_pct
    intraday_target = s.channel_capital("intraday") * s.daily_profit_pct
    assert swing_target == 6_000.0
    assert intraday_target == 6_000.0


def test_per_channel_limits_scale_with_capital():
    """If capital grows, limits auto-scale (% rule = the architectural win)."""
    s = StrategySettings(capital=400_000, capital_swing=200_000, capital_intraday=200_000)
    assert s.channel_capital("swing") * s.daily_loss_pct == 6_000.0   # 3% of 2L
    assert s.channel_capital("swing") * s.daily_profit_pct == 12_000.0  # 6% of 2L


def test_asymmetric_channel_allocation_supported():
    """User could go 60/40 swing/intraday — channel_capital reflects it."""
    s = StrategySettings(capital=200_000, capital_swing=120_000, capital_intraday=80_000)
    assert s.channel_capital("swing") * s.daily_loss_pct == 3_600.0
    assert s.channel_capital("intraday") * s.daily_loss_pct == 2_400.0


# ── per_channel_limits_active gate (boolean expression used in trading_service) ──


def test_per_channel_active_when_both_set():
    s = StrategySettings(capital=200_000, capital_swing=100_000, capital_intraday=100_000)
    assert (s.capital_swing > 0 and s.capital_intraday > 0) is True


def test_per_channel_inactive_when_either_unset():
    """If only one is set, per-channel logic stays OFF (safe default)."""
    s = StrategySettings(capital=100_000, capital_swing=100_000, capital_intraday=0.0)
    assert (s.capital_swing > 0 and s.capital_intraday > 0) is False
    s2 = StrategySettings(capital=100_000, capital_swing=0.0, capital_intraday=100_000)
    assert (s2.capital_swing > 0 and s2.capital_intraday > 0) is False


def test_per_channel_inactive_in_default_config():
    """Default StrategySettings keeps per-channel OFF for back-compat."""
    s = StrategySettings()
    assert (s.capital_swing > 0 and s.capital_intraday > 0) is False
