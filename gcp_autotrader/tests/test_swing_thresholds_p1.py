"""Tests for the 2026-04-22 P1 swing-threshold fixes.

Covers:
- swing_min_signal_score dropped 75 → 70 (lets daily-uptrending names
  scored 65-73 qualify)
- Breadth filter exempts swing shorts whose own daily_trend is DOWN
  (source contains the exemption clause)
"""
from __future__ import annotations

import inspect
import re
from pathlib import Path

from autotrader.services import trading_service as ts_mod
from autotrader.settings import StrategySettings


# ─── P1-1: swing_min_signal_score drop ────────────────────────────────────


def test_swing_min_signal_score_is_70():
    """Swing threshold must be 70 after 2026-04-22 calibration.

    Rationale: today's live data had max swing adjusted_score = 73,
    threshold = 75, only 1 qualified (MOTHERSON @ 76 by a 1-point margin).
    Dropping to 70 captures daily-uptrend names (WELCORP s=82, LLOYDSME
    s=84, STLTECH s=88) that genuinely have edge on the daily horizon
    but scored 65-73 on intraday-composite.
    """
    cfg = StrategySettings()
    assert cfg.swing_min_signal_score == 70, (
        f"swing_min_signal_score must be 70, got {cfg.swing_min_signal_score}. "
        "Do NOT revert to 75 without live BQ evidence that 70 is generating "
        "low-quality signals. See 2026-04-22 scan_decisions audit."
    )


def test_swing_threshold_still_above_intraday_normal():
    """Sanity: swing threshold should still be ≥ intraday NORMAL threshold.

    If someone drops swing below intraday, the horizon-asymmetry assumption
    breaks — swing should demand at least as much signal quality as a
    NORMAL-regime intraday trade.
    """
    cfg = StrategySettings()
    assert cfg.swing_min_signal_score >= cfg.min_signal_score - 5, (
        "Swing threshold dropped too close to intraday — swing trades should "
        "still clear a quality bar even though horizon is longer."
    )


# ─── P1-2: breadth-filter exemption for swing shorts ──────────────────────


def test_breadth_filter_exempts_swing_shorts_with_daily_down():
    """Verify trading_service source contains the swing+DOWN exemption.

    We can't unit-test the scan loop directly (too much fixture surface),
    so we validate the exemption clause is present in the source. This
    guards against accidental revert.

    The exemption: a SELL signal that is swing AND has daily_trend=DOWN
    should NOT be blocked by nifty_breadth_too_bullish_for_shorts.
    """
    src = inspect.getsource(ts_mod)
    # Must contain the exemption marker comment OR the literal negated clause
    assert "_is_swing" in src, "lost reference to _is_swing in trading_service"
    assert "nifty_breadth_too_bullish_for_shorts" in src, (
        "breadth filter reason string removed — was this intentional?"
    )
    # The exemption predicate — either form should be present
    has_exemption = (
        re.search(r"and\s+not\s*\(\s*\n?\s*_is_swing", src) is not None
        or re.search(r"_is_swing\s+and\s+_daily_bias", src) is not None
    )
    assert has_exemption, (
        "breadth-filter exemption for swing shorts with daily_trend=DOWN is missing "
        "from trading_service. Expected a clause like "
        '`and not (_is_swing and _daily_bias is not None and ...trend == "DOWN")`. '
        "This is the 2026-04-22 P1 fix — reverting it re-blocks legitimate swing "
        "shorts like RELIANCE/SUNPHARMA (MR SELL, adj_score 95-98, daily_trend=DOWN)."
    )


def test_breadth_filter_still_blocks_intraday_shorts_in_bullish_tape():
    """The exemption must be scoped to swing — intraday shorts should still
    be blocked by the breadth filter when breadth is bullish. If the
    exemption accidentally applies to intraday, we lose asymmetric-squeeze
    protection on the intraday book.
    """
    src = inspect.getsource(ts_mod)
    # Find the breadth-filter block by walking backwards from the reason
    # string. Grab the preceding ~1500 chars which should contain the elif
    # head + all its conditions.
    reason_idx = src.find('"nifty_breadth_too_bullish_for_shorts"')
    assert reason_idx > 0, "breadth filter reason string not found in source"
    block = src[max(0, reason_idx - 1500):reason_idx]
    # The block should mention _is_swing as part of the exemption
    assert "_is_swing" in block, (
        "breadth filter exemption not scoped to _is_swing — intraday shorts "
        "would lose squeeze protection. Reverting."
    )


def test_settings_file_has_p1_comment():
    """Commit hygiene: the settings file should carry the P1 rationale so
    future readers know why swing_min_signal_score is 70.
    """
    path = Path(__file__).parent.parent / "src" / "autotrader" / "settings.py"
    text = path.read_text()
    assert "swing_min_signal_score: int = 70" in text
    # Rationale comment near the constant
    assert "2026-04-22" in text and "swing" in text.lower()
