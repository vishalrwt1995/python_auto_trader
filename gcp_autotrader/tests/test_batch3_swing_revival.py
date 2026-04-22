"""Tests for Batch 3 — Swing Revival (2026-04-22).

Three fixes aimed at getting swing-side setups functional after the
04-16/04-20/04-21 post-mortems showed near-zero swing activity:

3.1 Persist `best_price` across ws_monitor restarts. Without persistence,
    a service restart silently resets the trailing high-watermark to
    entry_price, regressing the trail-SL reference and giving up every
    gain that wasn't already locked in via sl_moved/target_passed.

3.2 Scorer actually consumes `daily_bias.strength`. Previously strength
    was a ±3 cosmetic bonus; now the alignment magnitude scales directly
    with strength so a weak UP trend (strength=25) scores fewer alignment
    points than a strong UP trend (strength=90).

3.3 Swing short-side scoring unblocked for individually bearish stocks
    outside PANIC/TREND_DOWN. A stock in a clean multi-week daily
    downtrend could never surface as SHORT_PULLBACK in RANGE/CHOP because
    the universe scorer short-circuited shorts to bearish regimes only.
    The downstream breadth filter already protects against shorting into
    a bid tape — this gate was redundant and silently killed valid setups.
"""
from __future__ import annotations

import inspect
import re

from autotrader.services import ws_monitor_service as ws_mod
from autotrader.services import universe_service as uni_mod
from autotrader.domain import scoring as scoring_mod
from autotrader.domain.daily_bias import DailyBias
from autotrader.domain.indicators import compute_indicators
from autotrader.domain.models import RegimeSnapshot
from autotrader.domain.scoring import determine_direction, score_signal
from autotrader.settings import StrategySettings


# ─── 3.1: best_price persistence ───────────────────────────────────────


def test_ws_monitor_tracks_best_price_persist_throttle():
    """ws_monitor must maintain a _best_last_persist throttle dict so that
    best_price updates aren't written every tick."""
    src = inspect.getsource(ws_mod)
    assert "_best_last_persist" in src, (
        "ws_monitor_service missing _best_last_persist throttle dict — Batch "
        "3.1 expected a per-tag timestamp tracker to rate-limit Firestore "
        "writes when best_price advances on every tick."
    )


def test_ws_monitor_persists_best_price_on_advance():
    """When best_price advances in the tick handler, ws_monitor must write
    it to Firestore so a restart doesn't reset the trailing high-watermark
    to entry_price."""
    src = inspect.getsource(ws_mod)
    # There must be at least one Firestore write that carries best_price
    assert '"best_price": round(best, 2)' in src or '"best_price":' in src, (
        "no best_price field is ever persisted to Firestore — Batch 3.1 "
        "expected update_position(..., {'best_price': ...}) on advance."
    )


def test_ws_monitor_refresh_prefers_firestore_best_price_on_restart():
    """On position refresh, the carry-forward logic must consult the
    Firestore-persisted best_price as a fallback (via `pos`), not just the
    in-memory `old` dict which is empty after a restart."""
    src = inspect.getsource(ws_mod)
    assert "_carry" in src or 'pos.get("best_price"' in src, (
        "refresh doesn't fall back to Firestore's best_price — Batch 3.1 "
        "expected a _carry helper or equivalent that reads pos.get('best_price')"
        " as a fallback when in-memory is empty (post-restart)."
    )


def test_ws_monitor_breakeven_persists_best_price_too():
    """The breakeven transition already persists sl_price + sl_moved; it
    must now also persist best_price so restart sees a consistent snapshot
    (sl_moved=True without a best_price = trailing from entry_price)."""
    src = inspect.getsource(ws_mod)
    # Locate the first breakeven persist block
    m = re.search(
        r"breakeven_sl tag=.*?update_position\(tag, \{([^}]*)\}",
        src, re.DOTALL,
    )
    assert m is not None, "breakeven_sl persist block not found"
    payload = m.group(1)
    assert "best_price" in payload, (
        "breakeven persist doesn't include best_price — Batch 3.1 expected "
        "the breakeven write to snapshot best_price for restart safety."
    )


# ─── 3.2: daily_bias.strength actually weights the score ───────────────


def _daily_bias(trend: str, strength: float) -> DailyBias:
    return DailyBias(
        trend=trend,
        strength=strength,
        support=95.0,
        resistance=105.0,
        atr_daily=1.0,
        adx_daily=25.0,
        rsi_daily=55.0,
        supertrend_dir=1 if trend == "UP" else -1,
        ema_stack=(trend == "UP"),
        ema_flip=(trend == "DOWN"),
    )


def _trending_candles(n: int = 130, start: float = 200.0, step: float = 0.5):
    rows = []
    px = start
    for i in range(n):
        px += step
        rows.append((
            f"2025-01-{(i % 28) + 1:02d}T10:{i % 60:02d}:00+05:30",
            px - 0.2, px + 1.0, px - 0.8, px, 5000 + i * 20,
        ))
    return rows


def test_scorer_alignment_scales_with_strength():
    """Weak-trend alignment must score fewer points than strong-trend
    alignment. Previously both collapsed to +15 for UP; now scaled."""
    cfg = StrategySettings()
    ind = compute_indicators(_trending_candles(), cfg)
    assert ind is not None, "synthetic candles didn't produce indicators"
    regime = RegimeSnapshot(regime="TREND", bias="BULLISH", vix=12.0)
    d = determine_direction(ind, regime)
    # Force BUY so the alignment block is hit predictably
    if d != "BUY":
        d = "BUY"

    weak = score_signal("TEST", d, ind, regime, cfg,
                       daily_bias=_daily_bias("UP", 20.0), setup="PULLBACK")
    strong = score_signal("TEST", d, ind, regime, cfg,
                         daily_bias=_daily_bias("UP", 90.0), setup="PULLBACK")
    assert strong.breakdown.alignment > weak.breakdown.alignment, (
        f"alignment didn't scale with strength: weak={weak.breakdown.alignment} "
        f"strong={strong.breakdown.alignment}. Batch 3.2 expected strength=90 "
        "to produce a higher alignment score than strength=20."
    )


def test_scorer_counter_trend_penalty_scales_with_strength():
    """A BUY against a strong DOWN daily trend must be penalised more than
    a BUY against a weak DOWN daily trend."""
    cfg = StrategySettings()
    ind = compute_indicators(_trending_candles(), cfg)
    assert ind is not None
    regime = RegimeSnapshot(regime="TREND", bias="BULLISH", vix=12.0)

    weak_counter = score_signal("TEST", "BUY", ind, regime, cfg,
                                daily_bias=_daily_bias("DOWN", 20.0), setup="PULLBACK")
    strong_counter = score_signal("TEST", "BUY", ind, regime, cfg,
                                  daily_bias=_daily_bias("DOWN", 90.0), setup="PULLBACK")
    assert strong_counter.breakdown.alignment < weak_counter.breakdown.alignment, (
        f"counter-trend penalty didn't scale: weak={weak_counter.breakdown.alignment} "
        f"strong={strong_counter.breakdown.alignment}. Strong opposing trend "
        "should produce more-negative alignment."
    )


def test_scorer_alignment_source_references_strength():
    """Structural: scoring.py source must reference daily_bias.strength in
    the alignment computation so the knob is actually consumed, not just a
    cosmetic ±3 post-adjustment."""
    src = inspect.getsource(scoring_mod)
    assert "daily_bias.strength" in src, (
        "scoring.py doesn't reference daily_bias.strength — Batch 3.2 expected "
        "strength to influence the alignment magnitude."
    )
    assert "_strength_norm" in src or "strength or 0) / 100" in src, (
        "scoring.py doesn't scale alignment by strength — Batch 3.2 expected "
        "a proportional multiplier, not the old ±3 cosmetic bonus."
    )


# ─── 3.3: swing short-side scoring in non-bearish regimes ──────────────


def test_universe_short_scoring_allows_individually_bearish_stocks():
    """universe_service must allow SHORT_* scoring for individual stocks in
    clean daily downtrends even outside PANIC/TREND_DOWN regimes."""
    src = inspect.getsource(uni_mod)
    assert "_allow_short_scoring" in src or "_stock_bearish_structure" in src, (
        "universe_service doesn't compute per-stock bearish flag — Batch 3.3 "
        "expected short-side scoring to be gated on individual daily structure "
        "(ema50 < ema200 AND close < ema50), not just overall regime."
    )
    # Still exclude TREND_UP so we don't short strength in a clean bull tape
    assert 'canonical_regime != "TREND_UP"' in src, (
        "short-side unblock must still exclude TREND_UP — Batch 3.3 expected "
        "a guard against shorting into a strongly-rising broad tape."
    )


def test_universe_short_scoring_gate_structurally_widened():
    """The gate that previously read `if is_bearish_regime:` must now admit
    non-bearish regimes when stock-level structure is bearish."""
    src = inspect.getsource(uni_mod)
    assert re.search(
        r"if _allow_short_scoring:|if\s+is_bearish_regime\s+or\s+",
        src,
    ), (
        "short-side gate not widened — Batch 3.3 expected the `if "
        "is_bearish_regime:` predicate to be replaced/extended so non-bearish "
        "regimes with bearish stock structure also qualify."
    )
