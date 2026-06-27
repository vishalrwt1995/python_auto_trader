"""Tests for the 2026-05-08 strategy audit fixes.

Background: a deep audit over 29 days of live data + 5-month backtest
identified concrete calibration issues per strategy. This file verifies
the four fixes shipped:

  1. MORNING_FADE: hard-blocked in EVERY regime (was previously TREND_UP
     only). 5-month backtest in RANGE — its supposed sweet spot at 1.4×
     affinity — produced 30 trades with 17% WR / -₹64k. Thesis dead.

  2. SHORT_PULLBACK: hard-blocked in TREND_UP and RANGE (previously CHOP
     only). 4/4 backtest losses + structurally wrong (shorting strength
     in non-bearish regimes). Allowed in TREND_DOWN, PANIC, RECOVERY.

  3. SHORT_BREAKDOWN: hard-blocked in TREND_UP (was implicit-only via
     0.24× effective affinity). Explicit block prevents wasted scan
     cycles. Allowed in TREND_DOWN (1.3× sweet spot) and PANIC (0.8×).

  4. MEAN_REVERSION + PULLBACK strategy entry gates: relaxed to match
     the direction-vote logic and capture more legitimate setups. See
     scoring.py for full justification.

These tests are structural-grep guards (matching the pattern from
test_batch1_life_support.py): they assert the code change is present
without trying to simulate the full pipeline.
"""
from __future__ import annotations

import inspect

from autotrader.domain import regime_affinity, scoring
from autotrader.domain.regime_affinity import (
    _HARD_BLOCKS,
    regime_hard_blocks_strategy,
)


# ─── 1. MORNING_FADE kill ────────────────────────────────────────────────


def test_morning_fade_hard_blocked_only_in_trend_up_panic_recovery():
    """2026-05-20 (Batch H): MORNING_FADE re-enabled in CHOP, RANGE, TREND_DOWN.

    The 2026-05-08 blanket hard-block was based on a 30-trade backtest with
    17% WR — small sample, possibly affected by buggy backtest infra. Live
    data 2026-05-20 produced 10 score-100 MORNING_FADE signals that ALL
    got blocked, meaning we never got real evidence either way.

    Re-enabled in the mean-reverting / fade-friendly regimes:
      - CHOP:       pure mean-revert regime, fade thesis correct
      - RANGE:      mean-revert regime, fade thesis correct
      - TREND_DOWN: a morning pop in a down market is a valid fade

    Still blocked where fading is structurally wrong:
      - TREND_UP:   fading rallies in a bull market = catching knives
      - PANIC:      PANIC opens often gap-down, inverts the "stock popped" thesis
      - RECOVERY:   continuation regime, fading goes against the trend
    """
    blocked_regimes = ("TREND_UP", "PANIC", "RECOVERY")
    allowed_regimes = ("RANGE", "TREND_DOWN", "RANGE_ROTATING")
    for regime in blocked_regimes:
        assert regime_hard_blocks_strategy(regime, "MORNING_FADE"), (
            f"MORNING_FADE must stay hard-blocked in {regime} (fading is "
            f"structurally wrong in this regime)"
        )
    for regime in allowed_regimes:
        assert not regime_hard_blocks_strategy(regime, "MORNING_FADE"), (
            f"MORNING_FADE must be ALLOWED in {regime} regime"
        )


def test_morning_fade_in_blocked_regime_sets_individually():
    """Belt-and-suspenders: assert MORNING_FADE membership in the regime sets
    where it stays blocked, and ABSENCE from the regimes where it's allowed."""
    for regime in ("TREND_UP", "PANIC", "RECOVERY"):
        block_set = _HARD_BLOCKS.get(regime, set())
        assert "MORNING_FADE" in block_set, (
            f"MORNING_FADE missing from _HARD_BLOCKS[{regime!r}]"
        )
    for regime in ("RANGE", "TREND_DOWN", "RANGE_ROTATING"):
        block_set = _HARD_BLOCKS.get(regime, set())
        assert "MORNING_FADE" not in block_set, (
            f"MORNING_FADE incorrectly present in _HARD_BLOCKS[{regime!r}]"
        )


# ─── 2. SHORT_PULLBACK hard-blocks ──────────────────────────────────────


def test_short_pullback_hard_blocked_in_trend_up_and_range():
    """Shorting strength in non-bearish regimes is structurally wrong.
    4/4 backtest losses + 0 live qualifications confirm. Block in
    TREND_UP, RANGE, CHOP. Allow in TREND_DOWN, PANIC, RECOVERY where
    bearish structure makes shorts viable."""
    assert regime_hard_blocks_strategy("TREND_UP", "SHORT_PULLBACK")
    assert regime_hard_blocks_strategy("RANGE", "SHORT_PULLBACK")
    # Allow in regimes where shorts have edge (validated by affinity matrix):
    #   TREND_DOWN: 1.2× — the actual sweet spot
    #   PANIC: 0.6× — moderate but possible
    #   RECOVERY: 0.5× — penalised but not blocked (early reversion shorts)
    assert not regime_hard_blocks_strategy("TREND_DOWN", "SHORT_PULLBACK")
    assert not regime_hard_blocks_strategy("PANIC", "SHORT_PULLBACK")
    assert not regime_hard_blocks_strategy("RECOVERY", "SHORT_PULLBACK")


# ─── 3. SHORT_BREAKDOWN hard-block in TREND_UP ──────────────────────────


def test_short_breakdown_hard_blocked_in_trend_up():
    """SHORT_BREAKDOWN was implicit-blocked in TREND_UP via 0.24× effective
    affinity (0.4× regime × 0.6× SELL-dampening). Explicit hard-block
    saves wasted scan cycles. Strategy still allowed in its sweet-spot
    regimes TREND_DOWN (1.3×) and PANIC (0.8×)."""
    assert regime_hard_blocks_strategy("TREND_UP", "SHORT_BREAKDOWN")
    # RANGE block preserved:
    assert regime_hard_blocks_strategy("RANGE", "SHORT_BREAKDOWN")
    # Allow in bearish regimes where the strategy has edge:
    assert not regime_hard_blocks_strategy("TREND_DOWN", "SHORT_BREAKDOWN")
    assert not regime_hard_blocks_strategy("PANIC", "SHORT_BREAKDOWN")
    assert not regime_hard_blocks_strategy("RECOVERY", "SHORT_BREAKDOWN")


# ─── 4. MEAN_REVERSION strategy entry gate relaxation ───────────────────


def test_mean_reversion_rsi_gate_relaxed_to_match_direction_logic():
    """MEAN_REVERSION had 0 live trades / 1953 scans despite p90 score=80
    in RANGE (well above threshold 71). Block-cascade analysis showed the
    strategy gate (RSI≤35 in non-RANGE) contradicted the direction logic
    (bull vote at RSI<40), creating a dead band where direction said BUY
    but strategy gate rejected. Aligning thresholds removes the
    contradiction and unlocks ~5-20 more trades/month.

    New thresholds:
      - Non-RANGE BUY: ≤40 (was ≤35) — matches direction logic
      - RANGE BUY:     ≤45 (was ≤40) — slightly looser, still oversold
      - Non-RANGE SELL: ≥60 (was ≥60) — already aligned
      - RANGE SELL:    ≥58 (was ≥65) — matches direction logic at 60
    """
    src = inspect.getsource(scoring.check_strategy_entry)
    # Look for the new RSI thresholds in the MEAN_REVERSION/VWAP_REVERSAL block.
    assert "rsi_limit = 45 if _is_range_like else 40" in src, (
        "MEAN_REVERSION BUY RSI gate must be 45 (RANGE) / 40 (other) per "
        "2026-05-08 audit. The previous (40 / 35) values contradicted the "
        "RSI<40 direction-vote threshold and produced 0 trades / 1953 scans."
    )
    assert "rsi_floor = 58 if _is_range_like else 60" in src, (
        "MEAN_REVERSION SELL RSI gate must be 58 (RANGE) / 60 (other) per "
        "2026-05-08 audit. RANGE 65 was too strict; matching direction "
        "logic at RSI>60 unlocks legitimate reversal entries."
    )


def test_mean_reversion_vwap_extension_loosened_to_06_pct():
    """1.0% VWAP deviation is rare on 5m bars — p50 deviation is ~0.4%
    in our universe. 0.6% threshold captures genuine reversal setups
    without firing on bid/ask noise."""
    src = inspect.getsource(scoring.check_strategy_entry)
    assert "if vwap_dev < 0.6:" in src, (
        "VWAP extension gate must be 0.6% per 2026-05-08 audit. The "
        "previous 1.0% rejected ~90% of intraday reversal signals on 5m."
    )


# ─── 5. PULLBACK strategy entry gate widening ───────────────────────────


def test_pullback_rsi_band_widened():
    """PULLBACK had 0 live trades / 96 scans. RSI 38-65 BUY band missed
    legitimate pullback entries on RS leaders (which often have RSI 60-75
    during pullbacks). Widened to 35-70 for BUY, 38-65 for SELL.

    The structural anchor (ema_stack required for BUY, !ema_stack for
    SELL) stays strict — only the band widens.
    """
    src = inspect.getsource(scoring.check_strategy_entry)
    # New BUY band: 35 ≤ RSI ≤ 70
    assert "if is_buy and not (35 <= rsi <= 70):" in src, (
        "PULLBACK BUY RSI band must be 35-70 (was 38-65) per 2026-05-08 "
        "audit. Stocks in strong uptrends often pull back to RSI 60-70 "
        "range, not 38-50."
    )
    # New SELL band: 38 ≤ RSI ≤ 65
    assert "if not is_buy and not (38 <= rsi <= 65):" in src, (
        "PULLBACK SELL RSI band must be 38-65 (was 40-62) per 2026-05-08 audit."
    )


def test_pullback_ema_distance_widened_to_5pct():
    """EMA distance ±3% was too tight on 5m bars where liquid Indian
    stocks routinely move 3-4% intraday during uptrends. Widened to ±5%
    captures more legitimate pullback entries while still rejecting
    'price already extended' or 'trend broken' signals."""
    src = inspect.getsource(scoring.check_strategy_entry)
    # Both extended and broken checks should now use 5.0
    assert "if _ema_dist_pct > 5.0:" in src, (
        "PULLBACK EMA-distance 'price extended' gate must be 5% (was 3%)"
    )
    assert "if _ema_dist_pct < -5.0:" in src, (
        "PULLBACK EMA-distance 'price broke' gate must be -5% (was -3%)"
    )


# ─── 6. Sanity guards ───────────────────────────────────────────────────


def test_vwap_trend_unaffected_by_audit():
    """User explicitly requested VWAP_TREND be left alone. Verify its
    strategy entry gate hasn't been touched."""
    src = inspect.getsource(scoring.check_strategy_entry)
    # VWAP_TREND gate should still be the unchanged (close-vs-VWAP, ADX≥18) form.
    assert "strategy_vwap_trend_price_below_vwap" in src
    assert "strategy_vwap_trend_price_above_vwap" in src
    assert "if ind.adx < 18:" in src, (
        "VWAP_TREND ADX gate must remain at 18 — audit found this strategy "
        "near breakeven (no urgent fix needed)."
    )


def test_breakout_still_hard_blocked_in_trend_regimes():
    """BREAKOUT had 0/9 live WR, hard-blocked since 2026-05-06. The audit
    didn't re-enable it; future re-enable requires VCP/cup-handle pattern
    detection. This test guards against accidental re-enable."""
    assert regime_hard_blocks_strategy("TREND_UP", "BREAKOUT")
    assert regime_hard_blocks_strategy("TREND_DOWN", "BREAKOUT")
    assert regime_hard_blocks_strategy("RANGE", "BREAKOUT")
    assert regime_hard_blocks_strategy("PANIC", "BREAKOUT")
    # RECOVERY: not in hard-blocks (allow re-emergence in recovery regime
    # where new highs are legitimate continuation entries — affinity is 1.1×)
    assert not regime_hard_blocks_strategy("RECOVERY", "BREAKOUT")


def test_recovery_regime_now_in_hard_blocks_dict():
    """RECOVERY regime previously had no entry in _HARD_BLOCKS, silently
    allowing all strategies. 2026-05-08 audit added MORNING_FADE block;
    this test guards against the entry being removed."""
    assert "RECOVERY" in _HARD_BLOCKS, (
        "_HARD_BLOCKS must include RECOVERY regime — required for the "
        "MORNING_FADE all-regime block. Previously this regime was "
        "implicit-allowed (no entry in dict)."
    )


def test_unknown_strategy_or_regime_returns_false():
    """Defensive: an unknown strategy or regime must not crash, must
    return False (allow). Important for strategies not yet in the matrix."""
    assert not regime_hard_blocks_strategy("TREND_UP", "FOOBAR_STRATEGY")
    assert not regime_hard_blocks_strategy("UNKNOWN_REGIME", "MORNING_FADE")
    assert not regime_hard_blocks_strategy("", "MORNING_FADE")
    assert not regime_hard_blocks_strategy("TREND_UP", "")


def test_auto_default_strategies_never_blocked():
    """AUTO and DEFAULT are catchall categories used when watchlist
    setup label is absent. They must NEVER be hard-blocked or trades
    can't fire on legitimate fallback signals."""
    for regime in ("TREND_UP", "TREND_DOWN", "RANGE", "PANIC", "RECOVERY", "RANGE_ROTATING"):
        assert not regime_hard_blocks_strategy(regime, "AUTO")
        assert not regime_hard_blocks_strategy(regime, "DEFAULT")


# ─── 7. PHASE1_MOMENTUM allowlist emergency fix (2026-05-08 09:02 IST) ──


def test_phase1_momentum_in_allowed_strategies_for_trend_up():
    """PHASE1_MOMENTUM must be in the market-policy allowlist or the
    intraday watchlist (often 100% PHASE1_MOMENTUM in TREND_UP days)
    gets blanket-blocked by `policy_strategy_blocked`. Discovered live
    on 2026-05-08 morning when today's watchlist was 150 PHASE1_MOMENTUM
    rows and would have fired zero trades."""
    import inspect
    from autotrader.services import market_brain_service as mbs_mod
    src = inspect.getsource(mbs_mod)
    # Grep for the strategy in the allowed_strategies list-literal.
    assert '"PHASE1_MOMENTUM",' in src, (
        "PHASE1_MOMENTUM must be in market_brain_service.allowed_strategies "
        "list. Without it, _strategy_allowed() in trading_service rejects "
        "every PHASE1_MOMENTUM watchlist row with policy_strategy_blocked."
    )
    assert '"PHASE1_REVERSAL",' in src, (
        "PHASE1_REVERSAL must be in allowed_strategies for the same reason "
        "(bearish-regime oversold-bounce setup, used in PANIC/TREND_DOWN)."
    )


def test_phase1_momentum_removed_from_panic_trenddown():
    """PHASE1_MOMENTUM (like MOMENTUM) must be removed from the allowlist
    in PANIC/TREND_DOWN — chasing strength in those regimes fails.
    PHASE1_REVERSAL stays since oversold bounces are the edge there."""
    import inspect
    from autotrader.services import market_brain_service as mbs_mod
    src = inspect.getsource(mbs_mod)
    # Look for the regime filter blocks that remove PHASE1_MOMENTUM.
    # Each block should mention "PHASE1_MOMENTUM" alongside MOMENTUM.
    # Two filter blocks: PANIC, TREND_DOWN.
    assert src.count('"PHASE1_MOMENTUM"') >= 3, (
        "Expected PHASE1_MOMENTUM in: 1× allowed_strategies list + 2× "
        "regime-removal sets (PANIC, TREND_DOWN). "
        f"Found {src.count('PHASE1_MOMENTUM')} occurrences."
    )


# ─── 8. PHASE1_MOMENTUM hard-block in TREND_UP (added mid-session 2026-05-08) ─


def test_phase1_momentum_hard_blocked_in_trend_up():
    """PHASE1_MOMENTUM fires on stale Phase 1 (premarket-selected) data even
    when Phase 2 (today's intraday momentum) is available. OLECTRA SL_HIT
    in 8 minutes today (-₹47) on a Phase 1 selection while Phase 2 had
    just produced fresh candidates 6 min earlier.

    Hard-blocked regimes (4 of 6):
      * TREND_UP — added 2026-05-08 mid-session (this fix)
      * RANGE/PANIC — earlier audit (momentum-chasing fails)

    Allowed-through-_HARD_BLOCKS regimes (2 of 6):
      * TREND_DOWN — filtered at market_policy.allowed_strategies level
      * RECOVERY — the only regime where PHASE1_MOMENTUM actually adds value
        (early-bull where Phase 2 may struggle; affinity 1.1×)
    """
    assert regime_hard_blocks_strategy("TREND_UP", "PHASE1_MOMENTUM"), (
        "PHASE1_MOMENTUM must be hard-blocked in TREND_UP — Phase 2 is the "
        "proper signal here. Live evidence: OLECTRA SL_HIT in 8 min."
    )
    # Already-blocked regimes from earlier audit work
    assert regime_hard_blocks_strategy("RANGE", "PHASE1_MOMENTUM")
    assert regime_hard_blocks_strategy("PANIC", "PHASE1_MOMENTUM")
    # Not in _HARD_BLOCKS (other gating layer applies)
    assert not regime_hard_blocks_strategy("TREND_DOWN", "PHASE1_MOMENTUM")
    assert not regime_hard_blocks_strategy("RECOVERY", "PHASE1_MOMENTUM")
