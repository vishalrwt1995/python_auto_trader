"""Tests for the 2026-05-07 swing multi-emission fix.

Background (the actual root-cause):

  Pre-fix watchlist generator picked ONE setup label per swing-eligible stock
  using winner-takes-all on the four component scores
  ({BREAKOUT, PULLBACK, MEAN_REVERSION, MOMENTUM}). In a TREND_UP regime, the
  BREAKOUT component dominates universally because most stocks sit near
  20-day highs. Result on 2026-05-07: 24 swing rows, ALL labelled BREAKOUT
  BUY. Downstream, BREAKOUT in TREND_UP is hard-blocked (`regime_affinity`
  parked it 2026-05-06 due to 0/9 live WR). Net swing trades over 14 days: 0.

  And separately: trading_service.py:813 vetoed MEAN_REVERSION as a swing
  strategy and silently retagged it to intraday — wasting watchlist scoring
  effort for what is one of the highest-WR swing setups historically.

The fix:
  - universe_service.build_watchlist emits ONE swing watchlist row PER setup
    whose component score clears the swing min threshold. A stock that fits
    both PULLBACK and MEAN_REVERSION patterns appears as two rows; trading
    service then evaluates each independently against its own gates.
  - trading_service drops MEAN_REVERSION from `_intraday_only_strategies`,
    letting daily-frame mean-reversion run as swing.

These tests are a regression guard. They do not attempt a full simulation —
producing synthetic OHLCV that triggers all four component thresholds is
fixture-heavy and brittle. The structural-grep approach matches the pattern
already established by `tests/test_batch1_life_support.py` for guarding
trading-path invariants.
"""
from __future__ import annotations

import inspect

from autotrader.services import trading_service as ts_mod
from autotrader.services import universe_service as us_mod


# ─── 1. MEAN_REVERSION veto-lift ────────────────────────────────────────


def test_mean_reversion_no_longer_in_swing_intraday_only_set():
    """trading_service must not silently retag setupLabel=MEAN_REVERSION,
    wlType=swing rows to intraday — that was burning watchlist scoring
    effort on a legitimate swing edge.

    Historical veto set:
        {MOMENTUM, OPEN_DRIVE, VWAP_REVERSAL, VWAP_TREND, MEAN_REVERSION}

    Current veto set (after MEAN_REVERSION + MOMENTUM removal):
        {OPEN_DRIVE, VWAP_REVERSAL, VWAP_TREND}

    Removal history:
        - 2026-05-07: MEAN_REVERSION removed (daily-frame edge: RSI<35 +
          range_pos<0.30 + atr_pct sane)
        - 2026-05-26 (E2E audit): MOMENTUM removed. The original "bar-count
          timing" justification predated the daily-calibrated
          `check_swing_entry()` MOMENTUM branch at scoring.py:804-838 which
          uses ALL daily indicators (trend, ema_stack, supertrend_dir, ADX,
          strength, RSI). Universe also computes MOMENTUM score on daily
          candles (universe_service.py:4769-4786). Real-data impact of the
          veto: 0 MOMENTUM swing scans across 13 trading days (2026-05-12 →
          2026-05-25) — the score was computed and silently discarded.
    """
    src = inspect.getsource(ts_mod)
    # The set-literal must still exist (we kept the veto for genuine
    # intraday-clock-only strategies — OPEN_DRIVE etc).
    assert "_intraday_only_strategies" in src, (
        "expected the veto set to still be defined — only its membership "
        "should change, not its existence"
    )
    # Locate the set definition.
    import re
    m = re.search(
        r"_intraday_only_strategies\s*=\s*\{([^}]*)\}",
        src,
        re.DOTALL,
    )
    assert m is not None, "could not locate _intraday_only_strategies set literal"
    members_blob = m.group(1)
    members = {
        s.strip().strip("'").strip('"')
        for s in members_blob.replace("\n", " ").split(",")
        if s.strip()
    }
    # Strategies that have been verified as daily-frame-safe MUST NOT be in the set.
    # Re-adding them re-introduces the bug where swing rows get silently retagged
    # to intraday, wasting watchlist scoring effort on legitimate swing edges.
    for must_not_be_present in ("MEAN_REVERSION", "MOMENTUM"):
        assert must_not_be_present not in members, (
            f"{must_not_be_present} must NOT be in _intraday_only_strategies "
            f"(got {members}). Re-adding it re-introduces the bug where "
            f"wl_type=swing + strategy={must_not_be_present} rows are silently "
            "retagged to intraday."
        )
    # Strategies that DO have an intraday-clock dependency must stay vetoed
    # (defensive — guards against accidental over-removal). These setups use
    # `ind.vwap` (intraday VWAP), bar-count timing, or fall through
    # `check_swing_entry()` with no swing-specific gate.
    for must_stay in ("OPEN_DRIVE", "VWAP_REVERSAL", "VWAP_TREND"):
        assert must_stay in members, (
            f"{must_stay} must remain in _intraday_only_strategies — "
            "its strategy code uses intraday-clock structure (open auction, "
            "VWAP cross, bar-count timing) or falls through check_swing_entry "
            "with no daily-frame gate. Lifting the veto here without a "
            "strategy-side refactor would produce broken swing trades."
        )


# ─── 2. Multi-emission ──────────────────────────────────────────────────


def test_multi_emission_loop_present_in_build_watchlist():
    """universe_service.build_watchlist must emit one swing row per qualifying
    setup component, not pick one winner. The marker is the explicit
    `_long_candidates` list (the new structure) plus a per-candidate emit
    loop. Reverting to winner-takes-all reintroduces the 2026-05-07 failure
    mode where 100% of TREND_UP swing rows are BREAKOUT (all hard-blocked).
    """
    src = inspect.getsource(us_mod)
    assert "_long_candidates" in src, (
        "expected `_long_candidates` list in build_watchlist — this is the "
        "marker for the multi-emission refactor. Without it, swing watchlist "
        "regresses to winner-takes-all and TREND_UP regimes produce 100% "
        "BREAKOUT rows that all hard-block downstream. See 2026-05-07."
    )
    # The candidate list must include the three swing-compatible long setups.
    # MOMENTUM is intentionally NOT here — trading_service still vetoes it
    # because its strategy code uses bar-count timing.
    for label in ("BREAKOUT", "PULLBACK", "MEAN_REVERSION"):
        assert f'"{label}"' in src, (
            f"expected {label} in the swing _long_candidates list — "
            "all three are swing-compatible and must be eligible to emit"
        )
    # Short-side multi-emission must also be present so SHORT_BREAKDOWN /
    # SHORT_PULLBACK can fire alongside long setups when the regime allows.
    assert "_short_candidates" in src
    assert '"SHORT_BREAKDOWN"' in src
    assert '"SHORT_PULLBACK"' in src


def test_multi_emission_uses_min_score_eff_as_floor():
    """The per-candidate emit must use the same `min_score_eff` threshold
    that the existing post-emission filter uses. Otherwise we'd flood
    swing_scored with marginal rows that the next line drops anyway.
    """
    src = inspect.getsource(us_mod)
    # Look for the emit loop shape: iterating candidates, comparing to
    # min_score_eff, appending to swing_scored.
    assert "_emit_floor" in src and "min_score_eff" in src, (
        "expected `_emit_floor = float(min_score_eff)` used as the cutoff "
        "for per-candidate emission. Drift in this constant would either "
        "flood the universe with sub-threshold rows (waste) or under-emit "
        "and re-create the 0-swing-trade regression."
    )


def test_multi_emission_fallback_when_no_candidate_clears_floor():
    """If a stock has all four component scores below the swing min, we must
    still emit ONE row (highest-component fallback) so the universe view
    stays diagnostic-complete. Otherwise weak-but-present stocks vanish from
    swing_scored entirely and we lose visibility into "what the universe
    could trade if thresholds were lower".
    """
    src = inspect.getsource(us_mod)
    assert "_any_emitted" in src, (
        "expected `_any_emitted` flag so the fallback row is appended only "
        "when no candidate cleared the floor"
    )
    assert "if not _any_emitted:" in src, (
        "fallback branch missing — without it, stocks with all components "
        "below threshold drop out of swing_scored entirely, breaking the "
        "diagnostic 'universe of swing-eligible stocks' view"
    )


# ─── 3. Sanity: long + short candidates are mutually-shaped ─────────────


def test_short_candidates_include_correct_labels_and_directions():
    """A regression-only test: the short candidates list must use SELL
    direction and the canonical SHORT_BREAKDOWN / SHORT_PULLBACK labels.
    A typo here would silently make short emissions invalid downstream.
    """
    src = inspect.getsource(us_mod)
    import re
    # Find the SHORT_BREAKDOWN tuple within the _short_candidates assignment
    # and verify it pairs with SELL.
    pat = re.compile(
        r'_short_candidates\s*=\s*\[(.*?)\]',
        re.DOTALL,
    )
    m = pat.search(src)
    assert m is not None, "could not locate _short_candidates list literal"
    blob = m.group(1)
    assert '("SHORT_BREAKDOWN", "SELL"' in blob, (
        "SHORT_BREAKDOWN must be paired with SELL direction"
    )
    assert '("SHORT_PULLBACK", "SELL"' in blob, (
        "SHORT_PULLBACK must be paired with SELL direction"
    )


def test_earnings_penalty_applied_per_candidate_not_just_winner():
    """The 20% recent-event penalty must apply to ALL emitted rows for a
    stock with `recentEventFlag=True`, not just the historical winner-takes-
    all final_score. Otherwise multi-emission would give un-penalised
    PULLBACK / MEAN_REVERSION rows for earnings-event stocks while the old
    BREAKOUT row got penalised — inconsistent risk treatment.
    """
    src = inspect.getsource(us_mod)
    # Look for the per-candidate penalty application. The shape we expect:
    # `_candidates = [(lbl, dir_, sc * 0.80) for ...]` inside the recentEventFlag branch.
    import re
    # Match the multi-emission earnings penalty (per-candidate scoring),
    # not the fallback path. We require: comprehension over _candidates,
    # multiplying score by 0.80, gated on recentEventFlag.
    earnings_block = re.search(
        r"if bool\(r\.get\(['\"]recentEventFlag['\"]\)\):\s*\n\s*_candidates\s*=\s*\[",
        src,
    )
    assert earnings_block is not None, (
        "expected per-candidate earnings penalty applied via list "
        "comprehension on _candidates — without it, multi-emission rows "
        "for earnings-event stocks would dodge the 20% penalty that the "
        "winner-takes-all final_score used to enforce"
    )
    # And the multiplier must still be 0.80 (drift would silently change
    # earnings risk treatment).
    assert "* 0.80" in src
