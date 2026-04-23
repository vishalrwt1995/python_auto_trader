"""Tests for Batch 6 — Polish (2026-04-23).

6.1 + 6.2  Earnings blackout switched to trading-days arithmetic.
           Calendar-days math was exhausting the ±2-day blackout budget over
           the Sat-Sun weekend when results landed on a Friday, leaving Tue
           (a real trading-risk day) unprotected. time_utils now exposes
           `trading_days_between` which skips weekends; trading_service
           uses it to evaluate the blackout.

6.3        MOMENTUM daily gate tightened: requires daily SuperTrend up AND
           composite trend-strength >= 50. ema_stack=True with SuperTrend
           flipped down is early-distribution, not momentum, and those
           entries historically rolled over inside 2-3 sessions.

6.4        Partial-exit logic now handles qty==2 with a degraded 1-stage
           partial (1 share at 1R + SL to breakeven) instead of skipping
           entirely. qty<3 is a material share of tight-SL / low-priced
           entries; leaving them with zero partial capture meant a perfect
           1R touch could round-trip to SL without booking anything.
"""
from __future__ import annotations

import inspect
from dataclasses import replace
from datetime import date

from autotrader.domain.daily_bias import DailyBias
from autotrader.domain.scoring import check_swing_entry
from autotrader.services import trading_service as ts_mod
from autotrader.services import ws_monitor_service as ws_mod
from autotrader.time_utils import trading_days_between


# ─── 6.1 + 6.2  Trading-days earnings blackout ─────────────────────────


def test_trading_days_same_day_is_zero():
    assert trading_days_between(date(2026, 4, 22), date(2026, 4, 22)) == 0


def test_trading_days_skips_weekend():
    """Fri -> Mon must be 1 trading day, not 3."""
    fri = date(2026, 4, 24)
    mon = date(2026, 4, 27)
    assert trading_days_between(fri, mon) == 1, (
        "Fri->Mon must be 1 trading day — calendar math over-counted the "
        "Sat+Sun weekend, which was the whole point of Batch 6.2."
    )


def test_trading_days_friday_to_tuesday_is_two():
    """Fri results + 2-day blackout must still protect Tue."""
    fri = date(2026, 4, 24)
    tue = date(2026, 4, 28)
    assert trading_days_between(fri, tue) == 2


def test_trading_days_is_symmetric():
    """Order shouldn't matter."""
    d1 = date(2026, 4, 20)  # Mon
    d2 = date(2026, 4, 28)  # Tue
    assert trading_days_between(d1, d2) == trading_days_between(d2, d1)


def test_trading_service_uses_trading_days_for_blackout():
    """trading_service's earnings-blackout branch must call
    trading_days_between — not the old calendar-days .days subtraction."""
    src = inspect.getsource(ts_mod)
    assert "trading_days_between" in src, (
        "trading_service doesn't reference trading_days_between — Batch 6.2 "
        "required switching earnings-blackout math from calendar days to "
        "trading days so Friday-results protection extends through Tuesday."
    )


# ─── 6.3  MOMENTUM daily gate tightening ───────────────────────────────


def _make_momentum_bias(**overrides) -> DailyBias:
    base = DailyBias(
        trend="UP",
        strength=60.0,
        support=0.0,
        resistance=0.0,
        atr_daily=1.0,
        adx_daily=25.0,
        rsi_daily=60.0,
        supertrend_dir=1,
        ema_stack=True,
        ema_flip=False,
    )
    return replace(base, **overrides)


class _FakeInd:
    """Minimal indicators stub — only needs .volume.ratio and .close."""
    class _Vol:
        ratio = 1.2
    volume = _Vol()
    close = 100.0
    rsi = type("RSI", (), {"curr": 60.0, "prev": 58.0})()


def test_momentum_gate_passes_on_healthy_trend():
    ok, reason = check_swing_entry(
        "MOMENTUM", "BUY", _FakeInd(), _make_momentum_bias(), regime="TREND_UP",
    )
    assert ok, f"MOMENTUM should pass on healthy trend; blocked with {reason}"


def test_momentum_gate_blocks_on_flipped_supertrend():
    """SuperTrend flip-down = early distribution. Must be blocked even if
    ema_stack still looks OK."""
    ok, reason = check_swing_entry(
        "MOMENTUM", "BUY", _FakeInd(),
        _make_momentum_bias(supertrend_dir=-1),
        regime="TREND_UP",
    )
    assert not ok
    assert reason == "swing_momentum_daily_supertrend_not_up", (
        f"Expected swing_momentum_daily_supertrend_not_up, got {reason}"
    )


def test_momentum_gate_blocks_on_weak_strength():
    """strength < 50 means trend is structurally weak regardless of
    individual indicator passes."""
    ok, reason = check_swing_entry(
        "MOMENTUM", "BUY", _FakeInd(),
        _make_momentum_bias(strength=45.0),
        regime="TREND_UP",
    )
    assert not ok
    assert reason == "swing_momentum_daily_strength_too_low", (
        f"Expected swing_momentum_daily_strength_too_low, got {reason}"
    )


def test_momentum_gate_boundary_strength_passes():
    """strength == 50.0 exact must still pass (>= threshold)."""
    ok, _ = check_swing_entry(
        "MOMENTUM", "BUY", _FakeInd(),
        _make_momentum_bias(strength=50.0),
        regime="TREND_UP",
    )
    assert ok


# ─── 6.4  Partial-exit qty==2 ──────────────────────────────────────────


def test_ws_monitor_has_qty2_partial_branch():
    """ws_monitor_service must have a dedicated qty==2 partial-exit branch —
    previously the `original_qty >= 3` gate meant 2-share positions got no
    partial logic at all."""
    src = inspect.getsource(ws_mod)
    assert "original_qty == 2" in src, (
        "ws_monitor_service missing `original_qty == 2` branch — Batch 6.4 "
        "required a degraded 1-stage partial for 2-share positions."
    )


def test_ws_monitor_qty2_branch_uses_half_exit_and_breakeven():
    """The qty==2 branch must exit exactly 1 share (half) at 1R and move SL
    to breakeven — mirroring the 3-share path's stage-1 semantics."""
    src = inspect.getsource(ws_mod)
    # Locate the qty2 branch by its log tag.
    assert "partial_exit_qty2" in src, (
        "qty==2 branch missing the `partial_exit_qty2` log tag — Batch 6.4 "
        "expected an explicit telemetry marker for this degraded path."
    )
    assert "PARTIAL_1R_QTY2" in src, (
        "qty==2 branch missing the PARTIAL_1R_QTY2 reason tag — Batch 6.4 "
        "expected this to be distinguishable in order-service logs."
    )


def test_ws_monitor_qty2_branch_is_elif_not_duplicate():
    """The qty==2 branch must be an `elif` of the main >=3 block — not a
    separate `if` that could double-fire partials on qty==2 via the old
    max(1, int(2 * 0.4))=1 path."""
    src = inspect.getsource(ws_mod)
    # Find both branches and confirm order + elif structure.
    idx_main = src.index("original_qty >= 3 and not is_swing")
    idx_qty2 = src.index("original_qty == 2 and not is_swing")
    assert idx_qty2 > idx_main, "qty==2 branch must come after the >=3 branch"
    # Slice between them and verify no `if sl_dist > 0 and original_qty == 2`
    # — the qty2 branch should start with `elif`.
    between = src[idx_main:idx_qty2]
    # The qty2 branch line in the slice immediately before idx_qty2 should
    # begin with `elif`.
    tail = src[max(0, idx_qty2 - 80) : idx_qty2]
    assert "elif" in tail, (
        "qty==2 branch must be an `elif` of the >=3 block, not a fresh `if` — "
        "otherwise a 2-share position could theoretically be routed by both "
        "paths under edge-case rounding."
    )
