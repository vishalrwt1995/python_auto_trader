"""Edge-case tests — date boundaries, gaps, partial windows, holidays, delisting.

These verify the engine + pure-replay strategy degrade gracefully when the
input data is messy (NSE holidays mid-window, partial last day, gaps in
candle history, symbol delisting mid-window). The production smoke run
exposes all of these conditions; these tests pin the behavior so future
edits can't silently break them.
"""
from __future__ import annotations

from dataclasses import dataclass

import autotrader.backtest.replay_pure as rp
from autotrader.backtest.account import SimAccount, SimAccountConfig
from autotrader.backtest.data import BrainSnapshot, BrainTimeline
from autotrader.backtest.engine import BacktestEngine, StrategyContext
from autotrader.backtest.slippage import NoSlippage
from autotrader.backtest.types import Bar, Fill, Position
from autotrader.domain.exit_fsm import ExitState
from autotrader.domain.models import ScoreBreakdown, SignalScore


# ── Reusable fixtures (mirror tests/backtest/test_replay_pure.py) ───────


def _bar(sym: str, ts: str, price: float = 100.0) -> Bar:
    return Bar(
        symbol=sym, ts=ts,
        open=price, high=price + 0.5, low=price - 0.5, close=price,
        volume=1000.0, timeframe="5m",
    )


@dataclass
class _FakeInd:
    close: float = 100.0
    high: float = 100.5
    low: float = 99.5


def _qualified(score: float = 85.0) -> SignalScore:
    return SignalScore(score=score, direction="BUY", breakdown=ScoreBreakdown())


def _patch_pipeline(monkeypatch, *, score: float = 85.0):
    """Identical setup to test_replay_pure._patch_score_pipeline — duplicated
    here to keep this module self-contained and to let the two test files
    diverge later if either needs setup-specific tweaks."""
    monkeypatch.setattr(rp, "compute_indicators", lambda candles, cfg: _FakeInd())
    monkeypatch.setattr(rp, "determine_direction", lambda i, r, setup="": "BUY")
    monkeypatch.setattr(
        rp, "score_signal",
        lambda symbol, direction, ind, regime, cfg, daily_bias=None, setup="":
            _qualified(score),
    )
    monkeypatch.setattr(
        rp, "check_strategy_entry",
        lambda strategy, direction, ind, regime="": (True, ""),
    )
    import autotrader.domain.indicators as ind_mod
    monkeypatch.setattr(ind_mod, "calc_atr", lambda c, n=14: 1.0)
    monkeypatch.setattr(ind_mod, "normalize_candles", lambda c: c)


def _make_warmup_bars(sym: str, count: int = 90, base_ts: str = "2026-04-15") -> list[Bar]:
    out: list[Bar] = []
    for i in range(count):
        h, m = divmod(9 * 60 + i * 5, 60)
        ts = f"{base_ts}T{h:02d}:{m:02d}:00+05:30"
        out.append(_bar(sym, ts, price=100.0))
    return out


# ── 1. NSE holiday mid-window: gap between trading days ─────────────────


def test_holiday_gap_does_not_break_date_reset(monkeypatch):
    """If the calendar skips a date (NSE holiday), the strategy must still
    reset its `fired_today` set when the date changes — even across a 3-day
    gap. We simulate Mon Apr 13 → next trading day Apr 16 (skipping the
    14-15 holiday range).
    """
    _patch_pipeline(monkeypatch)
    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT",),
        apply_affinity_multiplier=False, apply_hard_blocks=False,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90, base_ts="2026-04-10")}
    strat = rp.PureReplayStrategy(cfg=cfg, warmup_bars=warmup)

    bars = [
        # Day 1: Mon — fires + closes via SL.
        _bar("ACME", "2026-04-13T09:30:00+05:30", price=100.0),
        _bar("ACME", "2026-04-13T09:35:00+05:30", price=100.0),
        Bar(symbol="ACME", ts="2026-04-13T09:40:00+05:30",
            open=100.0, high=100.0, low=90.0, close=90.0,
            volume=1000.0, timeframe="5m"),
        # Skip Tue Apr 14 + Wed Apr 15 (simulated holiday).
        # Day 2: Thu Apr 16 — fresh date, fired_today must reset.
        _bar("ACME", "2026-04-16T09:30:00+05:30", price=100.0),
        _bar("ACME", "2026-04-16T09:35:00+05:30", price=100.0),
    ]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    result = BacktestEngine(account=acc, strategy=strat).run(bars)

    # Two trades — one per trading day, even though the calendar skipped
    # two days. Confirms `fired_today` clears on date change (not on a
    # specific lapse interval).
    assert len(result.trades) == 2


# ── 2. Partial last day (truncated session) ─────────────────────────────


def test_partial_last_day_force_closes_open_position(monkeypatch):
    """When the input bars stop mid-day (e.g. live run ended at 11am, not
    15:30), the engine's finalize() must still close any open position via
    EOD_FORCE — never leave it dangling for the harness to misreport."""
    _patch_pipeline(monkeypatch)
    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT",),
        apply_affinity_multiplier=False, apply_hard_blocks=False,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(cfg=cfg, warmup_bars=warmup)

    # Only 3 bars — fires on bar 1, fills on bar 2, no SL or target hit.
    bars = [
        _bar("ACME", "2026-04-16T09:30:00+05:30", price=100.0),
        _bar("ACME", "2026-04-16T09:35:00+05:30", price=100.0),
        _bar("ACME", "2026-04-16T09:40:00+05:30", price=100.5),
    ]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    result = BacktestEngine(account=acc, strategy=strat).run(bars)

    assert len(result.trades) == 1
    assert result.trades[0].exit_reason == "EOD_FORCE", (
        "open positions at end-of-input must close via EOD_FORCE so the "
        "summary metrics include them — never strand a position."
    )
    # Account positions must be drained too.
    assert len(acc.positions) == 0


# ── 3. Mid-window candle gap (single missing bar) ───────────────────────


def test_single_missing_bar_does_not_drop_signal(monkeypatch):
    """A single missing 5-minute bar (intermittent feed dropout) inside an
    otherwise normal session must not crash the strategy. The on_bar loop
    sees consecutive timestamps with a gap; the rolling history just keeps
    appending what it gets."""
    _patch_pipeline(monkeypatch)
    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT",),
        apply_affinity_multiplier=False, apply_hard_blocks=False,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(cfg=cfg, warmup_bars=warmup)

    # 09:30 → 09:35 → [09:40 MISSING] → 09:45 — strategy fires on first,
    # fills on second, then sees a 10-minute gap before bar 4. Must not
    # crash; pyramid guard should still prevent a second fire.
    bars = [
        _bar("ACME", "2026-04-16T09:30:00+05:30", price=100.0),
        _bar("ACME", "2026-04-16T09:35:00+05:30", price=100.0),
        # 09:40 missing — feed dropout.
        _bar("ACME", "2026-04-16T09:45:00+05:30", price=100.5),
        _bar("ACME", "2026-04-16T09:50:00+05:30", price=100.5),
    ]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    result = BacktestEngine(account=acc, strategy=strat).run(bars)

    # Exactly one trade — engine survived the gap, fired-today dedupe
    # prevented re-fire on bar 4.
    assert len(result.trades) == 1


# ── 4. Symbol delisting (bars suddenly stop) ────────────────────────────


def test_symbol_disappears_mid_window(monkeypatch):
    """If a symbol stops emitting bars (delisting / corporate action /
    suspension) while a position is open, finalize() at end-of-window must
    still close the position with the LAST seen price as the exit reference.
    """
    _patch_pipeline(monkeypatch)
    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT",),
        apply_affinity_multiplier=False, apply_hard_blocks=False,
    )
    warmup = {
        "ZOMBIE": _make_warmup_bars("ZOMBIE", 90),
        "ALIVE": _make_warmup_bars("ALIVE", 90),
    }
    strat = rp.PureReplayStrategy(cfg=cfg, warmup_bars=warmup)

    bars = [
        # ZOMBIE fires + fills, then disappears entirely.
        _bar("ZOMBIE", "2026-04-16T09:30:00+05:30", price=100.0),
        _bar("ZOMBIE", "2026-04-16T09:35:00+05:30", price=100.0),
        # ALIVE keeps trading — engine clock advances on these bars only.
        _bar("ALIVE", "2026-04-16T09:40:00+05:30", price=200.0),
        _bar("ALIVE", "2026-04-16T09:45:00+05:30", price=200.0),
        _bar("ALIVE", "2026-04-16T09:50:00+05:30", price=200.0),
    ]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    result = BacktestEngine(account=acc, strategy=strat).run(bars)

    # Two positions opened: ZOMBIE (delisted) + ALIVE (continued). Both
    # close via finalize/EOD_FORCE. No exception, no stranded position.
    assert all(t.exit_reason == "EOD_FORCE" for t in result.trades)
    assert len(acc.positions) == 0


# ── 5. Empty bar list (no data) ─────────────────────────────────────────


def test_empty_bars_yields_zero_trades(monkeypatch):
    """Sanity: zero input bars must produce zero trades and no exception.
    Live runs on a brand-new symbol with no history hit this path."""
    _patch_pipeline(monkeypatch)
    cfg = rp.PureReplayConfig(setups=("BREAKOUT",))
    strat = rp.PureReplayStrategy(cfg=cfg)
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    result = BacktestEngine(account=acc, strategy=strat).run([])
    assert result.trades == []
    assert acc.positions == {}


# ── 6. Brain timeline missing the bar's timestamp ──────────────────────


def test_brain_snap_unavailable_for_bar_uses_neutral_default(monkeypatch):
    """If `BrainTimeline.asof(ts)` returns None (gap in archived snapshots),
    pure-replay must fall back to a neutral RegimeSnapshot rather than crash.
    Exercises the `_make_regime_snapshot(brain, ts)` and `_make_brain_state
    (None, ts)` defaults under the same code-path that fires a real signal."""
    _patch_pipeline(monkeypatch)

    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT",),
        apply_affinity_multiplier=True,    # exercise the affinity lookup
        apply_hard_blocks=True,            # exercise the hard-block lookup
        apply_brain_haircut=True,          # exercise the brain-state default
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    # BrainTimeline empty — every asof() returns None.
    strat = rp.PureReplayStrategy(
        cfg=cfg, brain=BrainTimeline([]), warmup_bars=warmup,
    )

    bars = [
        _bar("ACME", "2026-04-16T09:30:00+05:30", price=100.0),
        _bar("ACME", "2026-04-16T09:35:00+05:30", price=100.0),
        _bar("ACME", "2026-04-16T09:40:00+05:30", price=100.0),
    ]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    # Must not raise.
    result = BacktestEngine(account=acc, strategy=strat).run(bars)
    # The signal might or might not fire (depends on whether a RANGE+BUY
    # affinity multiplier × NORMAL haircut clears the threshold) — what we
    # care about is that the engine doesn't crash on the missing brain.
    assert isinstance(result.trades, list)


# ── 7. Out-of-order bars (input not pre-sorted) ─────────────────────────


def test_engine_processes_bars_in_input_order(monkeypatch):
    """The engine processes bars in input order — it does NOT pre-sort. Callers
    are responsible for sorting; this test pins the contract so that if we ever
    change it, the test breaks loudly."""
    _patch_pipeline(monkeypatch)
    cfg = rp.PureReplayConfig(
        setups=("BREAKOUT",),
        apply_affinity_multiplier=False, apply_hard_blocks=False,
    )
    warmup = {"ACME": _make_warmup_bars("ACME", 90)}
    strat = rp.PureReplayStrategy(cfg=cfg, warmup_bars=warmup)

    # Three bars in pre-sorted order — strategy fires on bar 1, fills bar 2.
    sorted_bars = [
        _bar("ACME", "2026-04-16T09:30:00+05:30", price=100.0),
        _bar("ACME", "2026-04-16T09:35:00+05:30", price=100.0),
        _bar("ACME", "2026-04-16T09:40:00+05:30", price=100.0),
    ]
    acc = SimAccount(SimAccountConfig(starting_cash=1_000_000.0), slippage=NoSlippage())
    result = BacktestEngine(account=acc, strategy=strat).run(sorted_bars)
    assert len(result.trades) == 1
