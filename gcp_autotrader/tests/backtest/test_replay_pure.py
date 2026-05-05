"""Pure-replay strategy tests — verifies recompute-from-candles wiring.

These tests do not exercise the real `compute_indicators` / `score_signal`
codepaths (those have their own unit tests in tests/test_indicators.py and
tests/test_scoring.py). Instead they monkey-patch the indicator + scoring
hooks at the `replay_pure` import level so we can assert the *strategy
control flow*: warm-up gating, signal de-duplication, pyramid guard,
direction filter, score-threshold, fill → Position hydration, and the
brain-snapshot mapper.

Synthetic bars are tiny (a handful per test); BQ is never touched.
"""
from __future__ import annotations

from dataclasses import dataclass

import autotrader.backtest.replay_pure as rp
from autotrader.backtest.account import SimAccount, SimAccountConfig
from autotrader.backtest.data import BrainSnapshot, BrainTimeline
from autotrader.backtest.engine import BacktestEngine
from autotrader.backtest.slippage import NoSlippage
from autotrader.backtest.types import Bar
from autotrader.domain.models import RegimeSnapshot, ScoreBreakdown, SignalScore


# ── Helpers ──────────────────────────────────────────────────────────────


def _bar(sym: str, ts: str, price: float = 100.0) -> Bar:
    return Bar(
        symbol=sym, ts=ts,
        open=price, high=price + 0.5, low=price - 0.5, close=price,
        volume=1000.0, timeframe="5m",
    )


@dataclass
class _FakeInd:
    """Minimum viable IndicatorSnapshot stand-in for the fields the
    strategy actually reads after the score check (close, high, low)."""
    close: float = 100.0
    high: float = 100.5
    low: float = 99.5


def _qualified_score(score: float = 85.0) -> SignalScore:
    return SignalScore(score=score, direction="BUY", breakdown=ScoreBreakdown())


def _patch_score_pipeline(
    monkeypatch,
    *,
    direction: str = "BUY",
    score: float = 85.0,
    ind=None,
    atr: float = 1.0,
):
    """Replace the three scoring entry-points with deterministic stubs.

    The real `compute_indicators` requires ≥80 well-formed candles and the
    real `score_signal` reads dozens of indicator fields. Stubbing isolates
    the strategy's control-flow logic from indicator math.
    """
    monkeypatch.setattr(rp, "compute_indicators", lambda candles, cfg: ind or _FakeInd())
    monkeypatch.setattr(rp, "determine_direction", lambda i, r, setup="": direction)
    monkeypatch.setattr(rp, "score_signal",
                        lambda symbol, direction, ind, regime, cfg, setup="":
                        _qualified_score(score))

    # calc_atr is imported lazily inside `_maybe_signal`; patch the
    # canonical module so the lazy `from … import calc_atr, normalize_candles`
    # also picks up the stub.
    import autotrader.domain.indicators as ind_mod
    monkeypatch.setattr(ind_mod, "calc_atr", lambda candles, n=14: atr)
    monkeypatch.setattr(ind_mod, "normalize_candles", lambda c: c)


def _make_warmup_bars(sym: str, count: int = 90, base_ts: str = "2026-04-15") -> list[Bar]:
    """Generate `count` 5-minute bars on `base_ts` so the strategy's
    rolling history has ≥80 entries before the in-window bar arrives."""
    out: list[Bar] = []
    for i in range(count):
        # 09:00 + 5*i minutes; we don't care about the wall clock, only
        # ordering, so simple HH:MM math is fine for test purposes.
        h, m = divmod(9 * 60 + i * 5, 60)
        ts = f"{base_ts}T{h:02d}:{m:02d}:00+05:30"
        out.append(_bar(sym, ts, price=100.0))
    return out


# ── Brain snapshot mapping ───────────────────────────────────────────────


def test_make_regime_snapshot_bullish_when_trend_high():
    brain = BrainTimeline([
        BrainSnapshot(
            asof_ts="2026-04-16T09:30:00+05:30", run_date="2026-04-16",
            regime="TREND_UP", risk_mode="NORMAL",
            market_confidence=80, breadth_score=70, trend_score=75,
            breadth_confidence=60, volatility_stress_score=20, data_quality_score=90,
        )
    ])
    snap = rp._make_regime_snapshot(brain, "2026-04-16T09:30:00+05:30")
    assert snap.regime == "TREND_UP"
    assert snap.bias == "BULLISH"
    assert 0.0 <= snap.confidence <= 1.0


def test_make_regime_snapshot_bearish_when_trend_low():
    brain = BrainTimeline([
        BrainSnapshot(
            asof_ts="2026-04-16T09:30:00+05:30", run_date="2026-04-16",
            regime="TREND_DOWN", risk_mode="DEFENSIVE",
            market_confidence=40, breadth_score=30, trend_score=25,
            breadth_confidence=50, volatility_stress_score=70, data_quality_score=85,
        )
    ])
    snap = rp._make_regime_snapshot(brain, "2026-04-16T09:30:00+05:30")
    assert snap.regime == "TREND_DOWN"
    assert snap.bias == "BEARISH"


def test_make_regime_snapshot_default_when_brain_empty():
    snap = rp._make_regime_snapshot(BrainTimeline([]), "2026-04-16T09:30:00+05:30")
    assert snap.regime == "RANGE"
    assert snap.bias == "NEUTRAL"


# ── Strategy control flow ────────────────────────────────────────────────


def test_pure_replay_does_not_fire_with_insufficient_warmup(monkeypatch):
    """Strategy must require ≥80 bars of history before calling indicators."""
    _patch_score_pipeline(monkeypatch)
    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT",),
        apply_affinity_multiplier=False, apply_hard_blocks=False,
    )
    strat = rp.PureReplayStrategy(cfg=cfg)

    # Only 5 bars total — well under the 80-bar warmup floor.
    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(5)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    eng = BacktestEngine(account=acc, strategy=strat)
    result = eng.run(bars)

    assert len(result.trades) == 0
    assert len(acc.positions) == 0


def test_pure_replay_fires_signal_after_warmup(monkeypatch):
    """With ≥80 warmup bars hydrated and a qualified score, the strategy
    must emit one MARKET order; the engine fills at next bar's open."""
    _patch_score_pipeline(monkeypatch, direction="BUY", score=85.0, atr=1.0)

    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT",),
        per_trade_risk_inr=5_000.0,
        apply_affinity_multiplier=False, apply_hard_blocks=False,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(cfg=cfg, warmup_bars=warmup)

    bars = [
        # First in-window bar — strategy fires order here.
        _bar("ACME", "2026-04-16T09:30:00+05:30", price=100.0),
        # Second bar — fill lands at this bar's open.
        _bar("ACME", "2026-04-16T09:35:00+05:30", price=100.0),
        # Third bar — let the position breathe a tick.
        _bar("ACME", "2026-04-16T09:40:00+05:30", price=101.0),
    ]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    eng = BacktestEngine(account=acc, strategy=strat)
    result = eng.run(bars)

    # Exactly one trade — the engine's EOD finalize closes the open position
    # at the last bar, so we read the closed-trade record (not acc.positions,
    # which has been drained by finalize).
    assert len(result.trades) == 1
    t = result.trades[0]
    assert t.symbol == "ACME"
    assert t.side == "BUY"
    assert t.entry_price == 100.0
    # Hydrated SL = entry - atr*1.74 = 100 - 1.74 = 98.26
    # Hydrated target = entry + atr*1.74*2 = 100 + 3.48 = 103.48
    # We don't see SL/target on Trade directly; instead verify exit reason
    # is EOD_FORCE (no SL or target was breached during the 3 bars).
    assert t.exit_reason == "EOD_FORCE"


def test_pure_replay_skips_when_score_below_threshold(monkeypatch):
    """Score < min_signal_score → no order placed."""
    _patch_score_pipeline(monkeypatch, direction="BUY", score=50.0)  # below default 72

    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT",), min_signal_score=72,
        apply_affinity_multiplier=False, apply_hard_blocks=False,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(cfg=cfg, warmup_bars=warmup)

    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    eng = BacktestEngine(account=acc, strategy=strat)
    result = eng.run(bars)

    assert len(result.trades) == 0
    assert len(acc.positions) == 0


def test_pure_replay_skips_when_direction_is_hold(monkeypatch):
    """determine_direction = HOLD → no scoring, no order."""
    _patch_score_pipeline(monkeypatch, direction="HOLD", score=99.0)

    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT",),
        apply_affinity_multiplier=False, apply_hard_blocks=False,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(cfg=cfg, warmup_bars=warmup)

    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    eng = BacktestEngine(account=acc, strategy=strat)
    result = eng.run(bars)

    assert len(result.trades) == 0


def test_pure_replay_direction_filter_blocks_opposite(monkeypatch):
    """direction_filter='SELL' must block a BUY signal even when qualified."""
    _patch_score_pipeline(monkeypatch, direction="BUY", score=85.0)

    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT",), direction_filter="SELL",
        apply_affinity_multiplier=False, apply_hard_blocks=False,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(cfg=cfg, warmup_bars=warmup)

    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    eng = BacktestEngine(account=acc, strategy=strat)
    eng.run(bars)

    assert len(acc.positions) == 0


def test_pure_replay_no_double_fire_same_day(monkeypatch):
    """Once a (date, symbol, setup, direction) has fired, no second order
    on a later bar within the same day."""
    _patch_score_pipeline(monkeypatch, direction="BUY", score=85.0)

    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT",),
        apply_affinity_multiplier=False, apply_hard_blocks=False,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(cfg=cfg, warmup_bars=warmup)

    # Several in-window bars on the same date — strategy keeps producing
    # qualified scores but the (date, sym, setup, dir) key dedupes.
    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(6)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    eng = BacktestEngine(account=acc, strategy=strat)
    result = eng.run(bars)

    # Exactly one trade despite 6 qualifying bars — per-day dedupe + pyramid
    # guard ensures only one position per (date, symbol, setup, direction).
    assert len(result.trades) == 1


def test_pure_replay_max_concurrent_caps_open_positions(monkeypatch):
    """max_concurrent=1 must reject the second symbol's signal while
    the first symbol's position is open."""
    _patch_score_pipeline(monkeypatch, direction="BUY", score=85.0)

    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT",), max_concurrent=1,
        apply_affinity_multiplier=False, apply_hard_blocks=False,
    )
    warmup = {
        "ACME": _make_warmup_bars("ACME", 90),
        "OTHER": _make_warmup_bars("OTHER", 90),
    }
    strat = rp.PureReplayStrategy(cfg=cfg, warmup_bars=warmup)

    bars = [
        _bar("ACME", "2026-04-16T09:30:00+05:30"),
        _bar("OTHER", "2026-04-16T09:30:00+05:30"),
        _bar("ACME", "2026-04-16T09:35:00+05:30"),    # ACME order fills here
        _bar("OTHER", "2026-04-16T09:35:00+05:30"),   # OTHER's order would fill here
        _bar("ACME", "2026-04-16T09:40:00+05:30"),
        _bar("OTHER", "2026-04-16T09:40:00+05:30"),
    ]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    eng = BacktestEngine(account=acc, strategy=strat)
    result = eng.run(bars)

    # Only one trade — the second symbol's signal must be capped because
    # the first symbol's order is in-flight (or already filled) when the
    # second qualifies. Without the cap we'd see 2 trades.
    assert len(result.trades) == 1


def test_pure_replay_resets_fired_today_at_date_boundary(monkeypatch):
    """The fired-today set must clear when the calendar date changes,
    so a fresh signal can fire on the next trading day."""
    _patch_score_pipeline(monkeypatch, direction="BUY", score=85.0)

    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT",),
        apply_affinity_multiplier=False, apply_hard_blocks=False,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(cfg=cfg, warmup_bars=warmup)

    # Day 1: fires on bar 1, fills on bar 2; we then SL out via low
    # crashing through the SL on bar 3 so the position closes.
    # Day 2: a fresh in-window bar should produce a NEW order.
    bars = [
        _bar("ACME", "2026-04-16T09:30:00+05:30", price=100.0),
        _bar("ACME", "2026-04-16T09:35:00+05:30", price=100.0),
        # Force SL hit: set low far below SL=98.26
        Bar(symbol="ACME", ts="2026-04-16T09:40:00+05:30",
            open=100.0, high=100.0, low=90.0, close=90.0,
            volume=1000.0, timeframe="5m"),
        # Day 2 — fresh date, fired set cleared.
        _bar("ACME", "2026-04-17T09:30:00+05:30", price=100.0),
        _bar("ACME", "2026-04-17T09:35:00+05:30", price=100.0),
    ]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    eng = BacktestEngine(account=acc, strategy=strat)
    result = eng.run(bars)

    # Two trades expected: day-1 closed via SL or EOD, day-2 opened and
    # then closed via EOD finalize. The day-2 entry proves the
    # fired-today set was reset across the date boundary.
    assert len(result.trades) == 2, (
        f"expected 2 trades (day-1 + day-2); got {len(result.trades)}. "
        "fired-today set may not be resetting at midnight."
    )


# ── Regime affinity multiplier ───────────────────────────────────────────


def _brain_with_regime(regime: str, ts: str = "2026-04-15T15:30:00+05:30") -> BrainTimeline:
    """Build a one-snapshot BrainTimeline pinned to a specific regime."""
    return BrainTimeline([
        BrainSnapshot(
            asof_ts=ts, run_date=ts[:10],
            regime=regime, risk_mode="NORMAL",
            market_confidence=70, breadth_score=55, trend_score=50,
            breadth_confidence=55, volatility_stress_score=30, data_quality_score=85,
        )
    ])


def test_affinity_multiplier_boosts_aligned_setup(monkeypatch):
    """RANGE × VWAP_REVERSAL × BUY = 1.3x. A raw score of 60 (below the
    72 threshold) should clear after the multiplier (60×1.3=78)."""
    _patch_score_pipeline(monkeypatch, direction="BUY", score=60.0, atr=1.0)

    cfg = rp.PureReplayConfig(
        setups=("VWAP_REVERSAL",),
        min_signal_score=72,
        apply_affinity_multiplier=True,
        apply_hard_blocks=False,    # not testing hard-blocks here
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(
        cfg=cfg, brain=_brain_with_regime("RANGE"), warmup_bars=warmup,
    )

    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    eng = BacktestEngine(account=acc, strategy=strat)
    result = eng.run(bars)

    # Without affinity boost the raw score 60 < 72 → no trade. With the
    # 1.3× boost adjusted = 78 ≥ 72 → trade fires.
    assert len(result.trades) == 1, (
        "affinity multiplier should have lifted score 60×1.3=78 over the "
        "72 threshold; check the matrix or the multiplier wiring."
    )


def test_affinity_multiplier_dampens_mismatched_setup(monkeypatch):
    """TREND_DOWN × MOMENTUM × BUY = 0.3x. Raw score 90 should drop to 27
    and fail the 72 threshold."""
    _patch_score_pipeline(monkeypatch, direction="BUY", score=90.0, atr=1.0)

    cfg = rp.PureReplayConfig(
        setups=("MOMENTUM",),
        min_signal_score=72,
        apply_affinity_multiplier=True,
        apply_hard_blocks=False,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(
        cfg=cfg, brain=_brain_with_regime("TREND_DOWN"), warmup_bars=warmup,
    )

    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    eng = BacktestEngine(account=acc, strategy=strat)
    result = eng.run(bars)

    # Buying MOMENTUM in TREND_DOWN gets damped to 0.3× → 90×0.3=27 < 72.
    # Without the multiplier this would have fired on a 90-score signal.
    assert len(result.trades) == 0


def test_affinity_disabled_uses_raw_score(monkeypatch):
    """With apply_affinity_multiplier=False the raw score is compared
    directly to the threshold — even mismatched (regime, setup) pairs
    fire if the raw score qualifies."""
    _patch_score_pipeline(monkeypatch, direction="BUY", score=85.0, atr=1.0)

    cfg = rp.PureReplayConfig(
        setups=("MOMENTUM",),
        min_signal_score=72,
        apply_affinity_multiplier=False,    # disabled
        apply_hard_blocks=False,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(
        cfg=cfg, brain=_brain_with_regime("TREND_DOWN"), warmup_bars=warmup,
    )

    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    eng = BacktestEngine(account=acc, strategy=strat)
    result = eng.run(bars)

    # Without affinity the raw 85 ≥ 72 fires regardless of mismatched regime.
    assert len(result.trades) == 1


# ── Regime hard-blocks ───────────────────────────────────────────────────


def test_hard_block_drops_breakout_in_chop(monkeypatch):
    """CHOP regime hard-blocks BREAKOUT — even a 100-score signal must not fire."""
    _patch_score_pipeline(monkeypatch, direction="BUY", score=100.0, atr=1.0)

    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT",),
        apply_affinity_multiplier=False,    # isolate hard-block effect
        apply_hard_blocks=True,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(
        cfg=cfg, brain=_brain_with_regime("CHOP"), warmup_bars=warmup,
    )

    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    eng = BacktestEngine(account=acc, strategy=strat)
    result = eng.run(bars)

    assert len(result.trades) == 0, (
        "BREAKOUT in CHOP must be hard-blocked by pure-replay (matches live "
        "scan_service policy gate)."
    )


def test_hard_block_disabled_lets_blocked_setup_through(monkeypatch):
    """With apply_hard_blocks=False, BREAKOUT in CHOP fires anyway —
    useful for counterfactual 'what if we removed this gate' studies."""
    _patch_score_pipeline(monkeypatch, direction="BUY", score=85.0, atr=1.0)

    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT",),
        apply_affinity_multiplier=False,    # raw score path
        apply_hard_blocks=False,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(
        cfg=cfg, brain=_brain_with_regime("CHOP"), warmup_bars=warmup,
    )

    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    eng = BacktestEngine(account=acc, strategy=strat)
    result = eng.run(bars)

    assert len(result.trades) == 1


def test_hard_block_does_not_affect_unrelated_setup_in_same_regime(monkeypatch):
    """CHOP hard-blocks BREAKOUT but allows VWAP_REVERSAL — verify only
    the blocked setup is dropped, not all signals in that regime."""
    _patch_score_pipeline(monkeypatch, direction="BUY", score=85.0, atr=1.0)

    # Both setups would qualify on raw score; only BREAKOUT should be blocked.
    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT", "VWAP_REVERSAL"),
        apply_affinity_multiplier=False,
        apply_hard_blocks=True,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(
        cfg=cfg, brain=_brain_with_regime("CHOP"), warmup_bars=warmup,
    )

    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    eng = BacktestEngine(account=acc, strategy=strat)
    result = eng.run(bars)

    # Exactly one trade — VWAP_REVERSAL fires, BREAKOUT is hard-blocked.
    assert len(result.trades) == 1
    assert result.trades[0].setup == "VWAP_REVERSAL"
