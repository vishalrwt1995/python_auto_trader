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
                        lambda symbol, direction, ind, regime, cfg,
                               daily_bias=None, setup="":
                        _qualified_score(score))
    # `_maybe_signal_best` calls `check_strategy_entry` to mirror live's
    # per-strategy hard gate. The real function reads `ind.adx`, `ind.vwap`,
    # `ind.rsi.curr` — fields the test's `_FakeInd` doesn't carry. Stub it
    # to pass-through in the same way the other scoring entry-points are
    # stubbed; the swing-entry helper is patched separately on tests that
    # exercise it.
    monkeypatch.setattr(rp, "check_strategy_entry",
                        lambda strategy, direction, ind, regime="": (True, ""))

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


def _brain_with_regime(
    regime: str, ts: str = "2026-04-15T15:30:00+05:30",
    risk_mode: str = "NORMAL",
) -> BrainTimeline:
    """Build a one-snapshot BrainTimeline pinned to a specific regime."""
    return BrainTimeline([
        BrainSnapshot(
            asof_ts=ts, run_date=ts[:10],
            regime=regime, risk_mode=risk_mode,
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


# ── Brain-state helper + adjust_signal haircut ───────────────────────────


def test_make_brain_state_with_none_returns_neutral_defaults():
    """Without a snapshot, fall back to NORMAL/RANGE so adjust_signal becomes
    a no-op multiplier (1.0) — never accidentally penalise an empty timeline."""
    state = rp._make_brain_state(None, "2026-04-16T09:30:00+05:30")
    assert state.risk_mode == "NORMAL"
    assert state.regime == "RANGE"


def test_make_brain_state_threads_archived_fields():
    snap = BrainSnapshot(
        asof_ts="2026-04-16T09:30:00+05:30", run_date="2026-04-16",
        regime="CHOP", risk_mode="DEFENSIVE",
        market_confidence=44, breadth_score=33, trend_score=22,
        breadth_confidence=55, volatility_stress_score=80, data_quality_score=90,
    )
    state = rp._make_brain_state(snap, "ignored")
    assert state.regime == "CHOP"
    assert state.risk_mode == "DEFENSIVE"
    assert state.market_confidence == 44
    assert state.volatility_stress_score == 80


def test_brain_haircut_dampens_defensive_risk_mode(monkeypatch):
    """DEFENSIVE risk_mode applies ×0.82 to score. Raw 80 → adjusted 66 < 72,
    so a signal that would qualify under NORMAL must be filtered here.

    This isolates the haircut from affinity (disabled) and from the dynamic
    threshold lookup (we pin a static 72 via cfg.min_signal_score).
    """
    _patch_score_pipeline(monkeypatch, direction="BUY", score=80.0, atr=1.0)
    cfg = rp.PureReplayConfig(
        setups=("VWAP_REVERSAL",),
        min_signal_score=72,
        apply_affinity_multiplier=False,
        apply_hard_blocks=False,
        apply_brain_haircut=True,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(
        cfg=cfg, brain=_brain_with_regime("RANGE", risk_mode="DEFENSIVE"),
        warmup_bars=warmup,
    )
    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    result = BacktestEngine(account=acc, strategy=strat).run(bars)
    assert len(result.trades) == 0


def test_brain_haircut_disabled_passes_marginal_score(monkeypatch):
    """Same setup as above but with apply_brain_haircut=False — raw 80 ≥ 72
    fires regardless of DEFENSIVE risk_mode. Confirms the flag is the only
    thing keeping the trade out."""
    _patch_score_pipeline(monkeypatch, direction="BUY", score=80.0, atr=1.0)
    cfg = rp.PureReplayConfig(
        setups=("VWAP_REVERSAL",),
        min_signal_score=72,
        apply_affinity_multiplier=False,
        apply_hard_blocks=False,
        apply_brain_haircut=False,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(
        cfg=cfg, brain=_brain_with_regime("RANGE", risk_mode="DEFENSIVE"),
        warmup_bars=warmup,
    )
    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    result = BacktestEngine(account=acc, strategy=strat).run(bars)
    assert len(result.trades) == 1


def test_brain_haircut_chop_panic_extra_penalty(monkeypatch):
    """CHOP/PANIC regimes get an extra ×0.88 on top of the risk_mode multiplier.
    NORMAL × CHOP: ×1.0 × 0.88 = ×0.88. Raw 80 → 70 < 72, blocked.
    NORMAL × RANGE: ×1.0 × 1.0 = ×1.0. Raw 80 → 80 ≥ 72, fires."""
    _patch_score_pipeline(monkeypatch, direction="BUY", score=80.0, atr=1.0)
    cfg = rp.PureReplayConfig(
        setups=("VWAP_REVERSAL",),
        min_signal_score=72,
        apply_affinity_multiplier=False,
        apply_hard_blocks=False,    # don't double-block via hard-block
        apply_brain_haircut=True,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    # CHOP path → blocked by haircut.
    strat_chop = rp.PureReplayStrategy(
        cfg=cfg, brain=_brain_with_regime("CHOP"), warmup_bars=warmup,
    )
    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc_chop = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    res_chop = BacktestEngine(account=acc_chop, strategy=strat_chop).run(bars)
    assert len(res_chop.trades) == 0


# ── Dynamic min_signal_score ─────────────────────────────────────────────


def test_resolve_min_score_swing_uses_swing_default():
    """Swing path bypasses risk_mode lookup — fixed at swing_default."""
    state = rp._make_brain_state(
        BrainSnapshot(
            asof_ts="x", run_date="x", regime="RANGE", risk_mode="DEFENSIVE",
            market_confidence=50, breadth_score=50, trend_score=50,
            breadth_confidence=50, volatility_stress_score=50, data_quality_score=50,
        ), "x",
    )
    out = rp._resolve_min_score(
        is_swing=True, brain_state=state, static_default=72, swing_default=75,
    )
    assert out == 75   # swing_default, not the DEFENSIVE-65 lookup


def test_resolve_min_score_intraday_uses_risk_mode_table():
    """Intraday: AGGRESSIVE → 75, NORMAL → 72, DEFENSIVE → 65, LOCKDOWN → 58."""
    for risk_mode, expected in [
        ("AGGRESSIVE", 75), ("NORMAL", 72),
        ("DEFENSIVE", 65), ("LOCKDOWN", 58),
    ]:
        state = rp._make_brain_state(
            BrainSnapshot(
                asof_ts="x", run_date="x", regime="RANGE", risk_mode=risk_mode,
                market_confidence=50, breadth_score=50, trend_score=50,
                breadth_confidence=50, volatility_stress_score=50, data_quality_score=50,
            ), "x",
        )
        out = rp._resolve_min_score(
            is_swing=False, brain_state=state,
            static_default=72, swing_default=75,
        )
        assert out == expected, f"{risk_mode}: expected {expected}, got {out}"


def test_dynamic_min_score_lets_defensive_marginal_through(monkeypatch):
    """DEFENSIVE risk_mode lowers the bar from 72 → 65. A raw-66 score
    fails the static 72 but clears the dynamic 65 threshold.

    Affinity + brain haircut disabled to isolate the threshold logic.
    """
    _patch_score_pipeline(monkeypatch, direction="BUY", score=66.0, atr=1.0)
    cfg = rp.PureReplayConfig(
        setups=("VWAP_REVERSAL",),
        min_signal_score=None,         # use dynamic
        apply_affinity_multiplier=False,
        apply_hard_blocks=False,
        apply_brain_haircut=False,     # don't haircut score below threshold
        apply_dynamic_min_score=True,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(
        cfg=cfg, brain=_brain_with_regime("RANGE", risk_mode="DEFENSIVE"),
        warmup_bars=warmup,
    )
    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    result = BacktestEngine(account=acc, strategy=strat).run(bars)
    assert len(result.trades) == 1


def test_dynamic_min_score_aggressive_raises_bar(monkeypatch):
    """AGGRESSIVE risk_mode raises threshold to 75. Raw 73 clears the static
    72 default but fails the AGGRESSIVE-75 dynamic threshold."""
    _patch_score_pipeline(monkeypatch, direction="BUY", score=73.0, atr=1.0)
    cfg = rp.PureReplayConfig(
        setups=("VWAP_REVERSAL",),
        min_signal_score=None,
        apply_affinity_multiplier=False,
        apply_hard_blocks=False,
        apply_brain_haircut=False,
        apply_dynamic_min_score=True,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(
        cfg=cfg, brain=_brain_with_regime("RANGE", risk_mode="AGGRESSIVE"),
        warmup_bars=warmup,
    )
    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    result = BacktestEngine(account=acc, strategy=strat).run(bars)
    assert len(result.trades) == 0


def test_dynamic_min_score_static_override_wins(monkeypatch):
    """If cfg.min_signal_score is set, it wins over dynamic — AGGRESSIVE's
    raised bar should NOT apply when the caller explicitly pins 60."""
    _patch_score_pipeline(monkeypatch, direction="BUY", score=65.0, atr=1.0)
    cfg = rp.PureReplayConfig(
        setups=("VWAP_REVERSAL",),
        min_signal_score=60,           # explicit override
        apply_affinity_multiplier=False,
        apply_hard_blocks=False,
        apply_brain_haircut=False,
        apply_dynamic_min_score=True,  # would compute 75 if consulted
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(
        cfg=cfg, brain=_brain_with_regime("RANGE", risk_mode="AGGRESSIVE"),
        warmup_bars=warmup,
    )
    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    result = BacktestEngine(account=acc, strategy=strat).run(bars)
    assert len(result.trades) == 1


# ── Daily-bias + swing-entry gate ────────────────────────────────────────


def test_swing_entry_gate_blocks_without_daily_bias(monkeypatch):
    """Swing requires daily_bias to clear `check_swing_entry`. Without
    daily_bars loaded, the gate auto-rejects ('swing_no_daily_data') and
    no swing trade fires regardless of how high the score is."""
    _patch_score_pipeline(monkeypatch, direction="BUY", score=95.0, atr=1.0)
    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT",),
        min_signal_score=72,
        is_swing=True,
        apply_affinity_multiplier=False,
        apply_hard_blocks=False,
        apply_brain_haircut=False,
        apply_swing_entry_gate=True,
        apply_daily_bias=True,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    # No daily_bars passed — gate should reject.
    strat = rp.PureReplayStrategy(
        cfg=cfg, brain=_brain_with_regime("TREND_UP"), warmup_bars=warmup,
    )
    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    result = BacktestEngine(account=acc, strategy=strat).run(bars)
    assert len(result.trades) == 0


def test_swing_entry_gate_disabled_lets_signal_through(monkeypatch):
    """With apply_swing_entry_gate=False, swing fires even without daily
    bias — diagnostic mode for measuring what the gate filters out."""
    _patch_score_pipeline(monkeypatch, direction="BUY", score=85.0, atr=1.0)
    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT",),
        min_signal_score=72,
        is_swing=True,
        apply_affinity_multiplier=False,
        apply_hard_blocks=False,
        apply_brain_haircut=False,
        apply_swing_entry_gate=False,
        apply_daily_bias=False,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(
        cfg=cfg, brain=_brain_with_regime("TREND_UP"), warmup_bars=warmup,
    )
    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    result = BacktestEngine(account=acc, strategy=strat).run(bars)
    assert len(result.trades) == 1


def test_swing_entry_gate_fires_when_daily_bias_passes(monkeypatch):
    """When daily-bias check passes (mock returns OK), swing trade fires.
    Patches `check_swing_entry` directly so we don't need to synthesize
    50 daily candles satisfying the BREAKOUT-swing daily-ADX≥25 gate."""
    _patch_score_pipeline(monkeypatch, direction="BUY", score=85.0, atr=1.0)
    monkeypatch.setattr(
        rp, "check_swing_entry",
        lambda strategy, direction, ind, daily_bias, regime="": (True, ""),
    )
    # Stub the bias lookup so the strategy's internal cache thinks daily
    # data is present (truthy non-None). check_swing_entry is what matters.
    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT",),
        min_signal_score=72,
        is_swing=True,
        apply_affinity_multiplier=False,
        apply_hard_blocks=False,
        apply_brain_haircut=False,
        apply_swing_entry_gate=True,
        apply_daily_bias=False,    # don't try to compute real bias
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(
        cfg=cfg, brain=_brain_with_regime("TREND_UP"), warmup_bars=warmup,
    )
    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    result = BacktestEngine(account=acc, strategy=strat).run(bars)
    assert len(result.trades) == 1


def test_daily_bias_cache_returns_none_with_empty_candles():
    """With no daily bars loaded for the symbol, the helper short-circuits
    to None instead of calling compute_daily_bias on an empty list."""
    strat = rp.PureReplayStrategy(cfg=rp.PureReplayConfig(), warmup_bars={"ACME": []})
    out = strat._daily_bias_for("ACME", "2026-04-16T09:30:00+05:30")
    assert out is None


def test_daily_bias_cache_returns_none_with_too_few_candles():
    """≤50 daily bars → compute_daily_bias undefined; helper short-circuits.
    Builds 30 dummy daily bars dated strictly before the query date."""
    daily = [
        Bar(symbol="ACME", ts=f"2026-03-{d:02d}T15:30:00+05:30",
            open=100.0, high=101.0, low=99.0, close=100.5,
            volume=1000.0, timeframe="1d")
        for d in range(1, 31)
    ]
    strat = rp.PureReplayStrategy(
        cfg=rp.PureReplayConfig(), daily_bars={"ACME": daily},
    )
    out = strat._daily_bias_for("ACME", "2026-04-16T09:30:00+05:30")
    assert out is None     # only 30 candles, need ≥50


# ── Strategy-entry gate (mirrors live trading_service.py:1159) ──────────


def test_strategy_entry_gate_blocks_when_check_returns_false(monkeypatch):
    """`check_strategy_entry` returning (False, reason) must drop the
    candidate even if score, direction, regime affinity, and threshold
    all qualify. This is the per-strategy hard gate that distinguishes
    BREAKOUT (volume surge + 52w-high) from VWAP_REVERSAL (RSI extreme +
    VWAP-extension) — without it, the simulator over-fires by 3-12×.
    """
    _patch_score_pipeline(monkeypatch, direction="BUY", score=85.0, atr=1.0)
    # Override the pass-through stub from _patch_score_pipeline with a
    # gate that always blocks.
    monkeypatch.setattr(
        rp, "check_strategy_entry",
        lambda strategy, direction, ind, regime="": (False, "no_volume_surge"),
    )

    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT",),
        min_signal_score=72,
        apply_affinity_multiplier=False,
        apply_hard_blocks=False,
        apply_brain_haircut=False,
        apply_strategy_entry_gate=True,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(cfg=cfg, warmup_bars=warmup)
    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    result = BacktestEngine(account=acc, strategy=strat).run(bars)
    assert len(result.trades) == 0, (
        "check_strategy_entry returning False must block the signal "
        "(matches live's gate at trading_service.py:1159)"
    )


def test_strategy_entry_gate_disabled_lets_signal_through(monkeypatch):
    """With apply_strategy_entry_gate=False the gate is skipped — useful
    for diagnostic 'what did the per-strategy gate filter out?' studies."""
    _patch_score_pipeline(monkeypatch, direction="BUY", score=85.0, atr=1.0)
    # This stub would block, but the flag should bypass it.
    monkeypatch.setattr(
        rp, "check_strategy_entry",
        lambda strategy, direction, ind, regime="": (False, "would_block"),
    )

    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT",),
        min_signal_score=72,
        apply_affinity_multiplier=False,
        apply_hard_blocks=False,
        apply_brain_haircut=False,
        apply_strategy_entry_gate=False,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(cfg=cfg, warmup_bars=warmup)
    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    result = BacktestEngine(account=acc, strategy=strat).run(bars)
    assert len(result.trades) == 1


def test_strategy_entry_gate_filters_some_setups_not_others(monkeypatch):
    """Gate blocks BREAKOUT but allows VWAP_REVERSAL — verify the surviving
    setup fires and the blocked one is dropped. This is the realistic
    multi-setup scenario where each strategy has its own per-bar gate."""
    _patch_score_pipeline(monkeypatch, direction="BUY", score=85.0, atr=1.0)

    def selective_gate(strategy, direction, ind, regime=""):
        if strategy == "BREAKOUT":
            return False, "no_breakout_conditions"
        return True, ""
    monkeypatch.setattr(rp, "check_strategy_entry", selective_gate)

    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT", "VWAP_REVERSAL"),
        min_signal_score=72,
        apply_affinity_multiplier=False,
        apply_hard_blocks=False,
        apply_brain_haircut=False,
        apply_strategy_entry_gate=True,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(cfg=cfg, warmup_bars=warmup)
    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    result = BacktestEngine(account=acc, strategy=strat).run(bars)

    assert len(result.trades) == 1
    assert result.trades[0].setup == "VWAP_REVERSAL", (
        "BREAKOUT must be blocked by its gate; VWAP_REVERSAL must survive."
    )


# ── Best-of-N candidate selection ─────────────────────────────────────────


def test_best_setup_wins_when_multiple_candidates_qualify(monkeypatch):
    """When several setups all clear every gate on the same bar, the
    highest-scoring one wins — matches live's "single signal per name"
    semantics where the watchlist pre-assigns ONE strategy per stock.

    With raw scores BREAKOUT=70, VWAP_REVERSAL=85 (raw post-affinity),
    the strategy must pick VWAP_REVERSAL. We synthesize different scores
    by patching score_signal to return different values per setup.
    """
    # We can't use _patch_score_pipeline directly because it stubs
    # score_signal to return a constant — set up the indicator/direction
    # stubs by hand and a setup-aware score_signal.
    from autotrader.domain.models import ScoreBreakdown, SignalScore

    monkeypatch.setattr(rp, "compute_indicators", lambda candles, cfg: _FakeInd())
    monkeypatch.setattr(rp, "determine_direction", lambda i, r, setup="": "BUY")
    monkeypatch.setattr(
        rp, "check_strategy_entry",
        lambda strategy, direction, ind, regime="": (True, ""),
    )
    import autotrader.domain.indicators as ind_mod
    monkeypatch.setattr(ind_mod, "calc_atr", lambda candles, n=14: 1.0)
    monkeypatch.setattr(ind_mod, "normalize_candles", lambda c: c)

    score_per_setup = {"BREAKOUT": 75.0, "VWAP_REVERSAL": 90.0, "MOMENTUM": 80.0}
    monkeypatch.setattr(
        rp, "score_signal",
        lambda symbol, direction, ind, regime, cfg, daily_bias=None, setup="":
            SignalScore(
                score=score_per_setup.get(setup, 70.0),
                direction=direction,
                breakdown=ScoreBreakdown(),
            ),
    )

    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT", "VWAP_REVERSAL", "MOMENTUM"),
        min_signal_score=72,
        apply_affinity_multiplier=False,
        apply_hard_blocks=False,
        apply_brain_haircut=False,
        apply_strategy_entry_gate=True,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(cfg=cfg, warmup_bars=warmup)
    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    result = BacktestEngine(account=acc, strategy=strat).run(bars)

    assert len(result.trades) == 1
    assert result.trades[0].setup == "VWAP_REVERSAL", (
        f"highest-scoring setup should win; got {result.trades[0].setup}"
    )


# ── Watchlist per-day setup restriction ─────────────────────────────────


def test_watchlist_restricts_candidates_to_assigned_setup(monkeypatch):
    """When watchlist_per_day has an entry for (date, symbol), pure-replay
    must only evaluate the live-assigned setup — not the full cfg.setups
    list. This is the parity-critical wiring: live picks ONE strategy per
    stock at watchlist build, and pure-replay needs to mirror that.

    With BREAKOUT scoring 90 and VWAP_REVERSAL scoring 75, best-of-N would
    pick BREAKOUT. The watchlist says VWAP_REVERSAL — so VWAP_REVERSAL
    must fire even though it's the lower-scoring candidate.
    """
    from autotrader.domain.models import ScoreBreakdown, SignalScore

    monkeypatch.setattr(rp, "compute_indicators", lambda candles, cfg: _FakeInd())
    monkeypatch.setattr(rp, "determine_direction", lambda i, r, setup="": "BUY")
    monkeypatch.setattr(
        rp, "check_strategy_entry",
        lambda strategy, direction, ind, regime="": (True, ""),
    )
    import autotrader.domain.indicators as ind_mod
    monkeypatch.setattr(ind_mod, "calc_atr", lambda candles, n=14: 1.0)
    monkeypatch.setattr(ind_mod, "normalize_candles", lambda c: c)

    # Setup-aware score: BREAKOUT would beat VWAP_REVERSAL on best-of-N.
    score_per_setup = {"BREAKOUT": 90.0, "VWAP_REVERSAL": 75.0}
    monkeypatch.setattr(
        rp, "score_signal",
        lambda symbol, direction, ind, regime, cfg, daily_bias=None, setup="":
            SignalScore(
                score=score_per_setup.get(setup, 0.0),
                direction=direction, breakdown=ScoreBreakdown(),
            ),
    )

    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT", "VWAP_REVERSAL"),
        min_signal_score=72,
        apply_affinity_multiplier=False,
        apply_hard_blocks=False,
        apply_brain_haircut=False,
        apply_watchlist_per_day=True,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    # Watchlist says VWAP_REVERSAL on Apr 16.
    watchlist = {("2026-04-16", "ACME"): "VWAP_REVERSAL"}
    strat = rp.PureReplayStrategy(
        cfg=cfg, warmup_bars=warmup, watchlist_per_day=watchlist,
    )

    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    result = BacktestEngine(account=acc, strategy=strat).run(bars)

    assert len(result.trades) == 1
    assert result.trades[0].setup == "VWAP_REVERSAL", (
        "Watchlist per-day must restrict candidates to the live-assigned "
        "setup, not let best-of-N override it."
    )


def test_watchlist_drops_signal_when_no_entry_for_date_symbol(monkeypatch):
    """When watchlist_per_day is populated but has no entry for the bar's
    (date, symbol), the signal is DROPPED — not fired via best-of-N
    fallback. The watchlist's silence on a stock means live did not scan
    that stock that day; pure-replay mirrors that by not trading.

    Pre-fix this fell back to best-of-N and manufactured trades for
    (date, symbol)s live deliberately skipped, blowing the trade count
    to 1.7× live's actual count.
    """
    _patch_score_pipeline(monkeypatch, direction="BUY", score=85.0, atr=1.0)

    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT", "VWAP_REVERSAL"),
        min_signal_score=72,
        apply_affinity_multiplier=False,
        apply_hard_blocks=False,
        apply_brain_haircut=False,
        apply_watchlist_per_day=True,
        morning_fade_overlay=False,    # disable overlay so test isolates the drop semantics
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    # Watchlist has an entry for a DIFFERENT symbol — no entry for ACME.
    watchlist = {("2026-04-16", "OTHER_SYMBOL"): "MOMENTUM"}
    strat = rp.PureReplayStrategy(
        cfg=cfg, warmup_bars=warmup, watchlist_per_day=watchlist,
    )

    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    result = BacktestEngine(account=acc, strategy=strat).run(bars)

    # No trade — ACME wasn't on the watchlist that day. Disable
    # apply_watchlist_per_day to enable best-of-N fallback for diagnostics.
    assert len(result.trades) == 0


def test_watchlist_empty_map_skips_gate_entirely(monkeypatch):
    """When watchlist_per_day is empty (e.g. no scan_decisions in the
    window — cold backfill), the gate must be skipped and best-of-N
    re-asserts itself — otherwise EVERY signal in a fresh-data backtest
    would be silently dropped."""
    _patch_score_pipeline(monkeypatch, direction="BUY", score=85.0, atr=1.0)

    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT",),
        min_signal_score=72,
        apply_affinity_multiplier=False,
        apply_hard_blocks=False,
        apply_brain_haircut=False,
        apply_watchlist_per_day=True,    # gate enabled but map is empty
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(
        cfg=cfg, warmup_bars=warmup, watchlist_per_day={},   # explicitly empty
    )

    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    result = BacktestEngine(account=acc, strategy=strat).run(bars)
    assert len(result.trades) == 1


def test_watchlist_disabled_via_flag(monkeypatch):
    """With apply_watchlist_per_day=False, the watchlist is ignored even
    if populated — useful for counterfactual 'what if every setup ran on
    every stock?' studies."""
    from autotrader.domain.models import ScoreBreakdown, SignalScore

    monkeypatch.setattr(rp, "compute_indicators", lambda candles, cfg: _FakeInd())
    monkeypatch.setattr(rp, "determine_direction", lambda i, r, setup="": "BUY")
    monkeypatch.setattr(
        rp, "check_strategy_entry",
        lambda strategy, direction, ind, regime="": (True, ""),
    )
    import autotrader.domain.indicators as ind_mod
    monkeypatch.setattr(ind_mod, "calc_atr", lambda candles, n=14: 1.0)
    monkeypatch.setattr(ind_mod, "normalize_candles", lambda c: c)

    score_per_setup = {"BREAKOUT": 90.0, "VWAP_REVERSAL": 75.0}
    monkeypatch.setattr(
        rp, "score_signal",
        lambda symbol, direction, ind, regime, cfg, daily_bias=None, setup="":
            SignalScore(
                score=score_per_setup.get(setup, 0.0),
                direction=direction, breakdown=ScoreBreakdown(),
            ),
    )

    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT", "VWAP_REVERSAL"),
        min_signal_score=72,
        apply_affinity_multiplier=False,
        apply_hard_blocks=False,
        apply_brain_haircut=False,
        apply_watchlist_per_day=False,    # disabled
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    watchlist = {("2026-04-16", "ACME"): "VWAP_REVERSAL"}
    strat = rp.PureReplayStrategy(
        cfg=cfg, warmup_bars=warmup, watchlist_per_day=watchlist,
    )

    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    result = BacktestEngine(account=acc, strategy=strat).run(bars)

    # Best-of-N reasserts itself: BREAKOUT (90) beats VWAP_REVERSAL (75)
    # because the watchlist-restriction path was bypassed.
    assert len(result.trades) == 1
    assert result.trades[0].setup == "BREAKOUT"


def test_watchlist_uses_setup_not_in_cfg_setups(monkeypatch):
    """If the watchlist names a setup outside cfg.setups (config drift
    between live and replay), pure-replay should still evaluate it —
    the watchlist is the source of truth, not cfg.setups. Otherwise
    we'd silently drop signals for any stock whose live-assigned setup
    we forgot to list."""
    _patch_score_pipeline(monkeypatch, direction="BUY", score=85.0, atr=1.0)

    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT",),    # narrow list
        min_signal_score=72,
        apply_affinity_multiplier=False,
        apply_hard_blocks=False,
        apply_brain_haircut=False,
        apply_watchlist_per_day=True,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    # Watchlist names MOMENTUM — NOT in cfg.setups.
    watchlist = {("2026-04-16", "ACME"): "MOMENTUM"}
    strat = rp.PureReplayStrategy(
        cfg=cfg, warmup_bars=warmup, watchlist_per_day=watchlist,
    )

    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    result = BacktestEngine(account=acc, strategy=strat).run(bars)

    assert len(result.trades) == 1
    assert result.trades[0].setup == "MOMENTUM", (
        "Watchlist must override cfg.setups when the live-assigned setup "
        "isn't in the config — otherwise we silently drop live's choice."
    )


def test_build_watchlist_per_day_collapses_duplicates():
    """Dups with the same setup collapse silently."""
    from autotrader.backtest.data import (
        ScanDecisionRow, build_watchlist_per_day,
    )

    def _row(date, sym, setup):
        return ScanDecisionRow(
            scan_ts=f"{date}T09:30:00+05:30", run_date=date, symbol=sym,
            setup=setup, direction="BUY", raw_score=80, adjusted_score=85,
            min_score=72, qualified=True, blocked_reason="", ltp=100.0,
            atr=1.0, atr_mult=1.74, rsi=55.0, vwap=100.0,
            regime="RANGE", risk_mode="NORMAL",
            wl_type="intraday", daily_trend="NEUTRAL",
        )

    rows = [
        _row("2026-04-16", "RELIANCE", "BREAKOUT"),
        _row("2026-04-16", "RELIANCE", "BREAKOUT"),    # dup, same setup
        _row("2026-04-16", "TCS", "VWAP_REVERSAL"),
        _row("2026-04-17", "RELIANCE", "MOMENTUM"),    # diff date, OK
    ]
    out = build_watchlist_per_day(rows)
    assert out[("2026-04-16", "RELIANCE")] == "BREAKOUT"
    assert out[("2026-04-16", "TCS")] == "VWAP_REVERSAL"
    assert out[("2026-04-17", "RELIANCE")] == "MOMENTUM"
    assert len(out) == 3


def test_build_watchlist_per_day_majority_vote_on_conflict():
    """When multiple setups are seen for the same (date, symbol), the most
    frequent wins. Live's watchlist can refresh mid-day from Firestore;
    the dominant setup is the one that gets the most scan-tick airtime."""
    from autotrader.backtest.data import (
        ScanDecisionRow, build_watchlist_per_day,
    )

    def _row(setup):
        return ScanDecisionRow(
            scan_ts="2026-04-16T09:30:00+05:30", run_date="2026-04-16",
            symbol="ACME", setup=setup, direction="BUY", raw_score=80,
            adjusted_score=85, min_score=72, qualified=True,
            blocked_reason="", ltp=100.0, atr=1.0, atr_mult=1.74,
            rsi=55.0, vwap=100.0, regime="RANGE", risk_mode="NORMAL",
            wl_type="intraday", daily_trend="NEUTRAL",
        )
    # 5 ticks: 3× VWAP_TREND, 1× BREAKOUT, 1× VWAP_REVERSAL → VWAP_TREND wins
    rows = [_row("VWAP_TREND")] * 3 + [_row("BREAKOUT")] + [_row("VWAP_REVERSAL")]
    out = build_watchlist_per_day(rows)
    assert out[("2026-04-16", "ACME")] == "VWAP_TREND"


def test_build_watchlist_per_day_excludes_phase1_and_short():
    """PHASE1_* and SHORT_* come from a separate scanner — their rows must
    not contribute to the watchlist mapping. If PHASE1_MOMENTUM has 5
    ticks and BREAKOUT has only 2, BREAKOUT still wins because PHASE1_*
    is filtered out before voting."""
    from autotrader.backtest.data import (
        ScanDecisionRow, build_watchlist_per_day,
    )

    def _row(setup):
        return ScanDecisionRow(
            scan_ts="2026-04-16T09:30:00+05:30", run_date="2026-04-16",
            symbol="ACME", setup=setup, direction="BUY", raw_score=80,
            adjusted_score=85, min_score=72, qualified=True,
            blocked_reason="", ltp=100.0, atr=1.0, atr_mult=1.74,
            rsi=55.0, vwap=100.0, regime="RANGE", risk_mode="NORMAL",
            wl_type="intraday", daily_trend="NEUTRAL",
        )
    rows = (
        [_row("PHASE1_MOMENTUM")] * 5
        + [_row("BREAKOUT")] * 2
        + [_row("SHORT_BREAKDOWN")]    # also excluded
    )
    out = build_watchlist_per_day(rows)
    # BREAKOUT wins despite having fewer rows — PHASE1_* + SHORT_* skipped.
    assert out[("2026-04-16", "ACME")] == "BREAKOUT"


def test_build_watchlist_per_day_excludes_only_phase1_means_empty():
    """If a (date, symbol) has ONLY phase-1 / short rows (no main-scanner
    setup), the watchlist map has no entry — pure-replay falls back to
    best-of-N for that stock."""
    from autotrader.backtest.data import (
        ScanDecisionRow, build_watchlist_per_day,
    )
    rows = [
        ScanDecisionRow(
            scan_ts="2026-04-16T09:30:00+05:30", run_date="2026-04-16",
            symbol="ACME", setup="PHASE1_MOMENTUM", direction="BUY",
            raw_score=80, adjusted_score=85, min_score=72, qualified=True,
            blocked_reason="", ltp=100.0, atr=1.0, atr_mult=1.74,
            rsi=55.0, vwap=100.0, regime="RANGE", risk_mode="NORMAL",
            wl_type="intraday", daily_trend="NEUTRAL",
        ),
    ]
    out = build_watchlist_per_day(rows)
    assert ("2026-04-16", "ACME") not in out
    assert len(out) == 0


def test_build_watchlist_per_day_skips_auto_placeholder():
    """`setup='AUTO'` rows must NOT populate the watchlist — AUTO is a
    placeholder that means 'live didn't have a setup field on the
    Firestore watchlist row'. Pure-replay should fall back to best-of-N
    for those (date, symbol)s rather than restrict to a fake 'AUTO'
    setup that no real strategy implements."""
    from autotrader.backtest.data import (
        ScanDecisionRow, build_watchlist_per_day,
    )
    rows = [
        ScanDecisionRow(
            scan_ts="2026-04-16T09:30:00+05:30", run_date="2026-04-16",
            symbol="ACME", setup="AUTO", direction="BUY", raw_score=80,
            adjusted_score=85, min_score=72, qualified=True,
            blocked_reason="", ltp=100.0, atr=1.0, atr_mult=1.74,
            rsi=55.0, vwap=100.0, regime="RANGE", risk_mode="NORMAL",
            wl_type="intraday", daily_trend="NEUTRAL",
        ),
    ]
    out = build_watchlist_per_day(rows)
    assert ("2026-04-16", "ACME") not in out
    assert len(out) == 0


def test_best_setup_picks_only_one_even_when_all_qualify(monkeypatch):
    """No matter how many setups qualify on the same bar, only ONE order
    fires per (date, symbol, direction). This is the structural guarantee
    that pure-replay matches live's pyramid guard ('one position per
    name+side') without relying on the per-symbol watchlist."""
    _patch_score_pipeline(monkeypatch, direction="BUY", score=85.0, atr=1.0)

    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT", "VWAP_TREND", "VWAP_REVERSAL", "MOMENTUM", "OPEN_DRIVE"),
        min_signal_score=72,
        apply_affinity_multiplier=False,
        apply_hard_blocks=False,
        apply_brain_haircut=False,
        apply_strategy_entry_gate=True,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(cfg=cfg, warmup_bars=warmup)
    bars = [_bar("ACME", f"2026-04-16T09:{30+i*5:02d}:00+05:30") for i in range(3)]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    result = BacktestEngine(account=acc, strategy=strat).run(bars)

    # All 5 setups would qualify (same stub score 85 ≥ 72), but only one
    # trade must fire because best-of-N selection picks a single winner.
    assert len(result.trades) == 1
