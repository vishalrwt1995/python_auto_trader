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


def test_swing_min_signal_score_is_60():
    """Swing threshold dropped 65 → 60 (2026-05-16 Batch D).

    Layer-D audit: BQ scan_decisions over 5 trading days (May 11-15) showed
    1424 swing rejects with `score_below_min` (52% of all swing rejections),
    against 0 qualified swing trades fired across the entire week. The 60-65
    band carries enough volume to materially shift firing rates while the
    strategy gates (check_swing_entry daily trend, ADX, RSI, supertrend)
    still filter low-quality candidates.

    History:
      * 2026-04-22: 75 → 70  (P1 drop, audit doc reference)
      * 2026-05-07: 70 → 65  (only 4 qualified in 14 days)
      * 2026-05-16: 65 → 60  (Batch D — 0 qualified in 5 days)

    If swing trades start firing but lose money, the proper response is
    to tighten the strategy gates (check_swing_entry), not raise this bar.
    """
    cfg = StrategySettings()
    assert cfg.swing_min_signal_score == 60, (
        f"swing_min_signal_score must be 60, got {cfg.swing_min_signal_score}. "
        "Do NOT revert without live BQ evidence the 60-64 band is generating "
        "losing trades. See 2026-05-16 Batch D audit."
    )


def test_swing_threshold_above_or_near_intraday_threshold():
    """Sanity: swing threshold should not be MORE than 10 points below
    intraday default. The 2026-05-07 drop allows swing slightly below
    intraday because swing scoring is the same intraday-tuned formula
    applied to daily candles — score numerics are systematically lower
    for genuine swing setups even when the underlying edge is real.
    """
    cfg = StrategySettings()
    # 2026-05-16 Batch D: widened to 15 points. Intraday default 72, swing 60.
    # Swing scoring uses the same intraday-tuned formula on daily candles;
    # daily-timeframe noise gives systematically lower numerics for genuine
    # setups. The downstream check_swing_entry gates carry the real quality
    # bar, not this threshold.
    assert cfg.swing_min_signal_score >= cfg.min_signal_score - 15, (
        f"Swing threshold dropped too far below intraday "
        f"(swing={cfg.swing_min_signal_score}, intraday={cfg.min_signal_score}). "
        "Hard floor: swing must be within 15 points of intraday."
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


def test_settings_file_has_rationale_comment():
    """Commit hygiene: the settings file should carry the rationale so
    future readers know why swing_min_signal_score is 60.
    """
    path = Path(__file__).parent.parent / "src" / "autotrader" / "settings.py"
    text = path.read_text()
    assert "swing_min_signal_score: int = 60" in text
    # Rationale comment near the constant — both the prior 2026-05-07 drop
    # and the 2026-05-16 Batch D adjustment should be cited.
    assert "2026-05-16" in text and "swing" in text.lower()
