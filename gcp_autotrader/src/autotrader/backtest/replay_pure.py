"""Pure-replay strategy — re-runs scoring + direction from raw candles.

Where `LiveDecisionStrategy` (replay_live.py) replays the live system's
already-computed `scan_decisions` rows, this strategy throws those away
and recomputes everything from candle history. That's the only way to
answer: "is the scoring calibrated correctly?" — because if the inputs
to the score are wrong (or the score-vs-min threshold is wrong), the
live `scan_decisions` rows already reflect that bias.

What this DOES re-compute
--------------------------
* Per-bar `IndicatorSnapshot` via `domain.indicators.compute_indicators`
  — RSI, EMA stack, MACD, Supertrend, VWAP, ATR, ADX, Bollinger, Stoch.
* Per-bar `direction` via `domain.scoring.determine_direction`.
* Per-bar `score` via `domain.scoring.score_signal`.
* **Regime affinity multiplier** via `domain.regime_affinity.regime_strategy_multiplier`
  — same call live makes, same matrix, same direction-dampening logic.
* **Regime hard-blocks** via `domain.regime_affinity.regime_hard_blocks_strategy`
  — pure-replay also drops a (regime, strategy) pair if live would have
  policy-blocked it. Without this we'd over-fire on CHOP/RANGE/PANIC days.
* `qualified = affinity_score >= min_signal_score` (affinity_score = score
  × multiplier, clipped to [0, 100]).

What this does NOT re-compute (deferred / honesty disclosure)
-------------------------------------------------------------
* **Daily-bias swing adjustments** — wired (see `_load_daily_bias`
  + `cfg.apply_daily_bias` below). Daily candles are loaded once at
  warm-up and `compute_daily_bias` runs as-of each session date so
  scoring's Layer-5 alignment fires the same way live does.
* **Per-strategy entry gates** (`check_strategy_entry`, `check_swing_entry`)
  — these gates ALSO live in `domain.scoring`. They are pure functions
  but operate on `IndicatorSnapshot`, so we wire them in cheaply.
  Pure-replay calls `check_swing_entry` when `cfg.is_swing=True` (hard
  gate matching live); intraday entry gates are scoring-internal already.
* **Brain confidence adjustment** (`market_policy_service.adjust_signal`)
  — wired (see `cfg.apply_brain_haircut`). The live function reads only
  `risk_mode` + `regime`, both archived in `market_brain_history`, so
  pure-replay reproduces the haircut exactly via the same call.
* **Dynamic `min_signal_score`** — wired (see `cfg.apply_dynamic_min_score`).
  Replicates the `_SCORE_THRESHOLDS` lookup from `trading_service.py:970-1008`
  keyed by `BrainSnapshot.risk_mode`.
* **News gate** — already faithful: live has `use_news_signals_v1=False`
  hardcoded (`scan_service`), so news never gates a live entry. Sim
  matches live by also not gating.
* **Options PCR fine-tuning** — APPROXIMATE. Live's score gets ±5 points
  from real-time PCR (`score_signal` Layer-7); historical option chain
  isn't archived, so pure-replay scores without it. The PCR signal is
  ≤5% of the 100-point scale, so the residual approximation is bounded
  but real — calibrate against live fills (see `slippage_calibration`)
  to absorb any systematic bias.
* **Universe selection** — sim runs over the symbols you give it.

Wiring
------
* Construct with: `bars_warmup` (per-symbol pre-window history for
  indicator warm-up — at least 80 bars needed) + `BrainTimeline`.
* On each engine bar T for symbol S, the strategy:
    1. Appends bar to the symbol's rolling history.
    2. If history ≥ 80 bars, calls `compute_indicators`.
    3. Looks up regime/risk via `BrainTimeline.asof(T)`.
    4. Calls `determine_direction`, `score_signal`, optionally
       `check_strategy_entry`.
    5. If `score >= min_signal_score` and direction != HOLD, emits a
       MARKET order. The fill lands at T+1's open — same no-look-ahead
       contract as live.
"""
from __future__ import annotations

import logging
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque

from autotrader.backtest.account import SimAccount
from autotrader.backtest.data import BrainTimeline
from autotrader.backtest.engine import StrategyContext
from autotrader.backtest.types import Bar, Fill, Position
from autotrader.domain.exit_fsm import ExitState
from autotrader.domain.indicators import compute_indicators
from autotrader.domain.models import (
    Bias,
    Candle,
    FreshnessSnapshot,
    IndicatorSnapshot,
    MarketBrainState,
    PcrSnapshot,
    NiftySnapshot,
    NiftyStructureSnapshot,
    FiiDiiSnapshot,
    RegimeSnapshot,
    ScoreBreakdown,
    SignalScore,
)
from autotrader.domain.regime_affinity import (
    regime_hard_blocks_strategy,
    regime_strategy_multiplier,
)
from autotrader.domain.scoring import (
    check_strategy_entry,
    check_swing_entry,
    determine_direction,
    score_signal,
)
from autotrader.services.market_policy_service import MarketPolicyService
from autotrader.settings import StrategySettings

log = logging.getLogger(__name__)


# Default warmup window. compute_indicators needs ≥ 80 bars internally;
# we keep a slightly larger buffer to ensure trailing indicators (52w high
# at line 266 of indicators.py uses 251 bars) at least degrade gracefully.
_DEFAULT_HISTORY_LEN = 260


@dataclass
class PureReplayConfig:
    """Tunables for pure-replay. Defaults aim to match live behavior."""

    # Per-trade risk (₹). Same as LiveReplayConfig.
    per_trade_risk_inr: float = 5_000.0
    # Override the static min_signal_score from StrategySettings. None = use
    # `StrategySettings.min_signal_score`.
    min_signal_score: int | None = None
    # Setups to consider on each bar. Live builds a watchlist with ONE
    # strategy per stock, then scans only that strategy. Pure-replay can't
    # replicate the watchlist (we don't archive it bar-by-bar), so instead
    # we evaluate every listed setup, run each through its strategy-entry
    # gate, score the survivors, and fire the highest-scoring one (matching
    # live's "best signal wins per name+direction" semantics imposed by the
    # pyramid guard). The full list mirrors the universe of setups the live
    # scanner can assign at watchlist build time, minus PHASE1_* (which
    # depend on offline phase-1 picks not archived per bar).
    setups: tuple[str, ...] = (
        "BREAKOUT", "VWAP_TREND", "VWAP_REVERSAL",
        "MOMENTUM", "OPEN_DRIVE",
    )
    # Restrict directions: None = both, "BUY" or "SELL" to limit.
    direction_filter: str | None = None
    # Default ATR multiple for SL distance. Same default as live.
    default_atr_mult: float = 1.74
    # Default reward:risk for synthetic target.
    default_rr: float = 2.0
    # Max concurrent positions — 0 = unlimited.
    max_concurrent: int = 0
    # When True, swing decisions route to delivery cost model.
    honor_wl_type: bool = True
    # When True, log every order placed. Verbose.
    debug_orders: bool = False
    # How many bars of warm-up history to keep per symbol.
    history_len: int = _DEFAULT_HISTORY_LEN
    # Treat the position as swing if true. Pure replay runs intraday by default.
    is_swing: bool = False
    # When True, apply regime_strategy_multiplier to score before threshold
    # comparison (matches live). Disable for cleaner A/B of raw scoring.
    apply_affinity_multiplier: bool = True
    # When True, drop signals where regime_hard_blocks_strategy returns True
    # (matches live's policy-block in scan_service). Disable to study what
    # those filtered setups would have done if allowed.
    apply_hard_blocks: bool = True
    # When True, apply `MarketPolicyService.adjust_signal` after the affinity
    # multiplier — this is the second stage live applies in `trading_service.
    # py:850-857`. The haircut depends on `risk_mode` (×0.60–1.08) and
    # `regime` (×0.88 in CHOP/PANIC), both archived in market_brain_history,
    # so pure-replay reproduces it exactly. Disable for A/B comparisons.
    apply_brain_haircut: bool = True
    # When True, replicate live's per-tick `min_signal_score` lookup keyed by
    # `risk_mode` (`trading_service.py:970-1008`). Defaults: AGGRESSIVE=75,
    # NORMAL=72, DEFENSIVE=65, LOCKDOWN=58. When False, the static
    # `cfg.min_signal_score` (or `StrategySettings.min_signal_score`) is used
    # for every bar regardless of brain state.
    apply_dynamic_min_score: bool = True
    # When True, swing trades (`is_swing=True`) are also gated by
    # `domain.scoring.check_swing_entry` against the daily-bias snapshot.
    # Live applies this gate unconditionally for swing in `trading_service`.
    # Disable to measure what swing trades that fail the daily-bias gate
    # would have done if allowed (diagnostic only — live never disables it).
    apply_swing_entry_gate: bool = True
    # When True, score_signal receives the daily-bias snapshot for Layer-5
    # (daily-trend alignment, ±15 pts). Required for swing because
    # check_swing_entry rejects without daily_bias. Set False to measure
    # the residual scoring contribution from the daily timeframe.
    apply_daily_bias: bool = True
    # When True, gate every candidate signal through `check_strategy_entry`
    # — the strategy-specific hard gate live runs at `trading_service.py:1159`
    # right before sizing. Without it, BREAKOUT fires on bars without a
    # volume surge, VWAP_REVERSAL fires near VWAP, etc., and the simulator
    # over-trades by ~3-12× depending on setup. Disable only for diagnostic
    # A/B (e.g. "what would the system have done with no per-strategy gate?").
    apply_strategy_entry_gate: bool = True
    # When True and `watchlist_per_day` is populated, restrict candidate
    # setups on each bar to the ONE strategy live actually assigned to the
    # stock that day (read back from `scan_decisions.setup`). This closes
    # the "100% MOMENTUM" parity gap that arises when pure-replay tries
    # every setup against every stock instead of the watchlist-assigned one.
    # When the map has no entry for a (date, symbol), pure-replay falls back
    # to evaluating all setups in `cfg.setups` and firing the highest-scoring
    # candidate (the pre-watchlist behavior) — useful for symbols added to
    # the watchlist after `since` or for counterfactual runs.
    apply_watchlist_per_day: bool = True
    # When True, evaluate MORNING_FADE as an overlay on every (date, symbol)
    # regardless of watchlist assignment. Live's universe_service doesn't yet
    # tag stocks as MORNING_FADE candidates (it's a new setup, 2026-05-06),
    # so to backtest the strategy at all we need pure-replay to try it on
    # any stock that meets its time + price + volume gates. Disable to study
    # MORNING_FADE only on stocks the watchlist would have surfaced.
    morning_fade_overlay: bool = True


@dataclass
class _PendingPositionMeta:
    symbol: str
    direction: str
    qty: int
    atr: float
    atr_mult: float
    setup: str
    is_swing: bool
    regime: str
    score: float
    raw_score: float = 0.0          # pre-affinity, for diagnostics
    affinity_mult: float = 1.0      # what was applied (1.0 = none)
    affinity_score: float = 0.0     # post-affinity, pre-brain-haircut
    threshold: int = 0              # min_score this fill cleared (dynamic-aware)
    risk_mode: str = "NORMAL"       # brain risk_mode at signal time


def _bar_to_candle(b: Bar) -> Candle:
    """Convert a backtest Bar to the indicator module's Candle tuple."""
    return (b.ts, b.open, b.high, b.low, b.close, b.volume)


# Mirrors `_SCORE_THRESHOLDS` in `services/trading_service.py:970-1008`.
# Keep in sync — a divergence here means pure-replay quietly fires more or
# fewer signals than live in the same regime, which is exactly what we
# claim the sim doesn't do.
_RISK_MODE_MIN_SCORE: dict[str, int] = {
    "AGGRESSIVE": 75,
    "NORMAL":     72,
    "DEFENSIVE":  65,
    "LOCKDOWN":   58,
}
# Empty in live as of 2026-04-22 — kept here as a documented hook so that
# if `_REGIME_MIN_SCORE` is ever re-populated upstream, the corresponding
# pure-replay change is a one-line edit, not a re-architecture.
_REGIME_MIN_SCORE: dict[str, int] = {}


def _make_brain_state(snap: Any, ts: str) -> MarketBrainState:
    """Build a `MarketBrainState` from a `BrainSnapshot` (or None).

    `MarketPolicyService.adjust_signal` reads only `risk_mode` + `regime`,
    both archived in `market_brain_history`, so the rest of the fields fall
    back to their dataclass defaults. This is intentionally a thin shim:
    if live's `adjust_signal` ever grows to depend on more state, this
    function is the single place to thread the new field through (or to
    surface the gap as `UNVERIFIED`).
    """
    if snap is None:
        return MarketBrainState(asof_ts=ts)
    return MarketBrainState(
        asof_ts=snap.asof_ts,
        regime=snap.regime,
        risk_mode=snap.risk_mode,
        market_confidence=snap.market_confidence,
        breadth_score=snap.breadth_score,
        trend_score=snap.trend_score,
        breadth_confidence=snap.breadth_confidence,
        volatility_stress_score=snap.volatility_stress_score,
        data_quality_score=snap.data_quality_score,
    )


def _resolve_min_score(
    *,
    is_swing: bool,
    brain_state: MarketBrainState | None,
    static_default: int,
    swing_default: int,
) -> int:
    """Replicate the dynamic min-score lookup live runs every scan tick.

    See `services/trading_service.py:984-1008`:
      * Swing: fixed `swing_min_signal_score` (no risk_mode haircut — swing
        already has a high static bar and we compare the *affinity*-adjusted
        score, not the brain-haircut score, in live).
      * Intraday: lookup `risk_mode` in `_SCORE_THRESHOLDS`, then prefer
        `_REGIME_MIN_SCORE[regime]` if it's lower (regime-discount lane).
    """
    if is_swing:
        return swing_default
    risk_mode = brain_state.risk_mode if brain_state else "NORMAL"
    base = _RISK_MODE_MIN_SCORE.get(risk_mode, static_default)
    if brain_state is not None:
        regime_floor = _REGIME_MIN_SCORE.get(brain_state.regime)
        if regime_floor is not None and regime_floor < base:
            base = regime_floor
    return base


def _make_regime_snapshot(brain: BrainTimeline, ts: str) -> RegimeSnapshot:
    """Construct a minimal RegimeSnapshot from BrainTimeline.

    Only `regime` and `bias` materially affect `score_signal`; the other
    fields exist for live's full pipeline but the score function reads
    only `regime.regime`, `regime.bias`, `regime.confidence`. We default
    everything else to neutral values.
    """
    snap = brain.asof(ts)
    if snap is None:
        return RegimeSnapshot(regime="RANGE", bias="NEUTRAL")

    # Map brain regime to RegimeSnapshot.bias. Live derives bias from the
    # full brain pipeline; for replay we use a simple mapping based on
    # trend_score so bullish/bearish biases flow through scoring.
    bias: Bias = "NEUTRAL"
    if snap.trend_score >= 60:
        bias = "BULLISH"
    elif snap.trend_score <= 40:
        bias = "BEARISH"

    return RegimeSnapshot(
        regime=snap.regime,
        bias=bias,
        confidence=snap.market_confidence / 100.0,
        data_health=snap.data_quality_score / 100.0,
        # Provide neutral defaults for sub-snapshots so scoring doesn't crash
        # when accessing ind.regime.pcr.value, etc.
        pcr=PcrSnapshot(),
        fii=FiiDiiSnapshot(),
        nifty=NiftySnapshot(),
        nifty_structure=NiftyStructureSnapshot(),
        freshness=FreshnessSnapshot(),
    )


class PureReplayStrategy:
    """Recomputes scoring/direction per bar — drives sim execution.

    Produces qualified signals from raw candles + brain history with no
    dependency on the live system's `scan_decisions` table. Use this to
    answer: "would a different scoring weight / threshold / setup mix
    have made money on this period?"
    """

    def __init__(
        self,
        *,
        cfg: PureReplayConfig | None = None,
        strategy_settings: StrategySettings | None = None,
        brain: BrainTimeline | None = None,
        warmup_bars: dict[str, list[Bar]] | None = None,
        daily_bars: dict[str, list[Bar]] | None = None,
        watchlist_per_day: dict[tuple[str, str], str] | None = None,
    ) -> None:
        self.cfg = cfg or PureReplayConfig()
        self.s_cfg = strategy_settings or StrategySettings()
        self.brain = brain or BrainTimeline([])
        # `(run_date, symbol) → setup` map distilled from live scan_decisions.
        # When non-empty + cfg.apply_watchlist_per_day, pure-replay restricts
        # candidate setups to the live-assigned one per stock per day so the
        # simulator matches live's "one strategy per stock" semantics. Empty
        # = fall back to evaluating every setup in cfg.setups (best-of-N).
        self._watchlist_per_day: dict[tuple[str, str], str] = (
            watchlist_per_day or {}
        )
        self._history: dict[str, Deque[Candle]] = {}
        self._pending_meta: dict[str, _PendingPositionMeta] = {}
        # Stateless service — methods read only their args; instantiating once
        # avoids re-allocation per bar.
        self._policy = MarketPolicyService()
        # Per-symbol daily-candle store for `compute_daily_bias`. We keep the
        # raw lists (not pre-computed DailyBias) and re-run the function as-of
        # each session date — `compute_daily_bias` slices on the trailing
        # window internally and is cheap enough to call per-symbol per-day.
        self._daily_candles: dict[str, list[list]] = {}
        if daily_bars:
            for sym, bars in daily_bars.items():
                self._daily_candles[sym.upper()] = [
                    [b.ts, b.open, b.high, b.low, b.close, b.volume]
                    for b in bars
                ]
        # Cached `DailyBias` per (symbol, date) — invalidated when the date
        # rolls. Live recomputes every scan; we do once per day per symbol.
        self._daily_bias_cache: dict[tuple[str, str], Any] = {}
        # Track signals already fired today per (symbol, setup, direction) to
        # avoid double-firing every bar after we cross the threshold. Cleared
        # at midnight (date change in bar.ts).
        self._fired_today: set[tuple[str, str, str, str]] = set()
        self._last_seen_date: str = ""

        # Hydrate per-symbol warm-up so the first in-window bar already has
        # enough history for compute_indicators (which needs ≥ 80 bars).
        if warmup_bars:
            for sym, bars in warmup_bars.items():
                key = sym.upper()
                dq: Deque[Candle] = deque(maxlen=self.cfg.history_len)
                for b in bars[-self.cfg.history_len :]:
                    dq.append(_bar_to_candle(b))
                self._history[key] = dq

    # ── Engine callbacks ─────────────────────────────────────────────

    def on_bar(self, ctx: StrategyContext) -> None:
        bar = ctx.bar
        sym = bar.symbol.upper()
        # Roll the symbol history forward.
        dq = self._history.get(sym)
        if dq is None:
            dq = deque(maxlen=self.cfg.history_len)
            self._history[sym] = dq
        dq.append(_bar_to_candle(bar))

        # Reset per-day fired set when the calendar date changes.
        bar_date = bar.ts[:10]
        if bar_date != self._last_seen_date:
            self._fired_today.clear()
            self._last_seen_date = bar_date

        if len(dq) < 80:
            return  # not enough warm-up

        # Live builds a watchlist with ONE strategy per stock and scans
        # only that strategy. We don't archive the watchlist per bar, so
        # we evaluate every configured setup and fire only the best-scoring
        # one — matching live's "single signal per name per bar" semantics
        # (the pyramid guard inside `_emit_signal` enforces it as a backstop
        # but candidate-selection here keeps cost down and avoids ordering
        # artifacts where setup A always wins setup B by virtue of loop
        # position rather than score).
        self._maybe_signal_best(ctx, sym, list(dq))

    def on_fill(self, ctx: StrategyContext, fill: Fill) -> None:
        tag = fill.parent_tag
        if not tag:
            return
        if tag in ctx.account.positions:
            return  # already opened
        meta = self._pending_meta.pop(tag, None)
        if meta is None:
            return  # not one of ours

        sl_dist = meta.atr * meta.atr_mult
        if meta.direction == "BUY":
            initial_sl = round(fill.price - sl_dist, 2)
            target = round(fill.price + sl_dist * self.cfg.default_rr, 2)
        else:
            initial_sl = round(fill.price + sl_dist, 2)
            target = round(fill.price - sl_dist * self.cfg.default_rr, 2)

        pos = Position(
            tag=tag,
            symbol=meta.symbol,
            side=meta.direction,
            qty=meta.qty,
            setup=meta.setup,
            is_swing=meta.is_swing,
            entry_price=fill.price,
            entry_ts=fill.ts,
            entry_atr=meta.atr,
            entry_regime=meta.regime,
            initial_sl=initial_sl,
            target=target,
            sl_dist=abs(fill.price - initial_sl),
            current_sl=initial_sl,
            best_price=fill.price,
            fsm_state=ExitState.INITIAL.value,
        )
        ctx.account.open_position(fill=fill, position=pos)
        if self.cfg.debug_orders:
            log.info(
                "pure_replay_position_opened tag=%s sym=%s side=%s qty=%d entry=%.2f "
                "sl=%.2f tgt=%.2f score=%.1f",
                pos.tag, pos.symbol, pos.side, pos.qty, pos.entry_price,
                pos.initial_sl, pos.target, meta.score,
            )

    def finalize(self, account: SimAccount) -> None:
        return

    # ── Helpers ──────────────────────────────────────────────────────

    def _daily_bias_for(self, sym: str, bar_ts: str):
        """Return cached `DailyBias` for (sym, bar_date), recomputing on
        first miss. Returns None if there are no daily candles loaded for
        the symbol or `compute_daily_bias` rejects (≤50 bars / NaNs).
        Importing inside the method keeps the dependency lazy — backtests
        that don't pass `daily_bars=` never need to import daily_bias."""
        bar_date = bar_ts[:10]
        cache_key = (sym, bar_date)
        if cache_key in self._daily_bias_cache:
            return self._daily_bias_cache[cache_key]

        candles = self._daily_candles.get(sym)
        if not candles:
            self._daily_bias_cache[cache_key] = None
            return None

        # Cut off candles strictly before today's session — daily bar for
        # the current trading date is incomplete during the day, and
        # leaking it would be look-ahead.
        usable = [c for c in candles if str(c[0])[:10] < bar_date]
        if len(usable) < 50:
            self._daily_bias_cache[cache_key] = None
            return None

        from autotrader.domain.daily_bias import compute_daily_bias
        bias = compute_daily_bias(usable)
        self._daily_bias_cache[cache_key] = bias
        return bias

    def _maybe_signal_best(
        self,
        ctx: StrategyContext,
        sym: str,
        candles: list[Candle],
    ) -> None:
        """Evaluate every configured setup, pick the highest-scoring one
        that survives every gate (hard-block, direction, strategy-entry,
        swing-entry, threshold), and emit it. Matches live's per-name
        semantics: live builds a watchlist with one strategy per stock, so
        per-name there's never more than one candidate. We approximate that
        by scoring all candidates and selecting the winner.
        """
        ind = compute_indicators(candles, self.s_cfg)
        if ind is None:
            return

        regime = _make_regime_snapshot(self.brain, ctx.bar.ts)
        brain_snap = self.brain.asof(ctx.bar.ts)
        brain_state = _make_brain_state(brain_snap, ctx.bar.ts)

        # Daily bias is per-(symbol, date) — compute once for the bar.
        daily_bias = self._daily_bias_for(sym, ctx.bar.ts) if self.cfg.apply_daily_bias else None

        # Resolve threshold once (independent of setup — depends only on
        # is_swing + risk_mode). Pre-fix this was duplicated per-setup.
        if self.cfg.min_signal_score is not None:
            threshold = self.cfg.min_signal_score
        elif self.cfg.apply_dynamic_min_score:
            threshold = _resolve_min_score(
                is_swing=self.cfg.is_swing,
                brain_state=brain_state,
                static_default=self.s_cfg.min_signal_score,
                swing_default=self.s_cfg.swing_min_signal_score,
            )
        else:
            threshold = self.s_cfg.min_signal_score

        # ── Resolve candidate setup list ───────────────────────────────
        # If watchlist gating is on AND we have a non-empty mapping, the
        # presence/absence of an entry for (date, symbol) is the source of
        # truth:
        #   * entry exists → restrict candidates to that one setup (live
        #     scanned this stock today with that strategy assignment)
        #   * no entry     → drop the signal entirely (live did NOT scan
        #     this stock today; pure-replay must not invent a trade)
        # Falling back to best-of-N here is the trap: it manufactures
        # trades for (date, symbol)s live deliberately skipped. Pre-fix
        # the fallback let MOMENTUM fire on every unscanned stock and
        # blew the trade count to 1.7× live.
        # Set apply_watchlist_per_day=False to disable the gate entirely
        # for "what would have happened if every stock was on watchlist"
        # counterfactual studies.
        bar_date = ctx.bar.ts[:10]
        if self.cfg.apply_watchlist_per_day and self._watchlist_per_day:
            wl_setup = self._watchlist_per_day.get((bar_date, sym))
            if wl_setup is None and not self.cfg.morning_fade_overlay:
                return  # live didn't scan this stock today
            setups_list: list[str] = []
            if wl_setup is not None:
                setups_list.append(wl_setup)
            # MORNING_FADE overlay: always tried regardless of watchlist
            # mapping, because the strategy fires on a price+time pattern,
            # not a watchlist label. Its time-gate (09:45-10:15 IST) and
            # price-gate (>1.5% pop with volume) make it self-selecting —
            # 99% of bars will fail the gate, so the overlay cost is tiny.
            if self.cfg.morning_fade_overlay and "MORNING_FADE" not in setups_list:
                setups_list.append("MORNING_FADE")
            if not setups_list:
                return
            setups_to_try: tuple[str, ...] = tuple(setups_list)
        else:
            setups_to_try = self.cfg.setups

        # ── Gather candidates ──────────────────────────────────────────
        candidates: list[tuple[int, str, str, float, float, int, int]] = []
        # tuple: (score_for_threshold, setup, direction, raw_score,
        #         affinity_mult, affinity_score, adjusted_score)
        for setup in setups_to_try:
            # 1. Hard-block (regime × strategy)
            if self.cfg.apply_hard_blocks and regime_hard_blocks_strategy(
                regime.regime, setup
            ):
                continue

            # 2. Direction vote
            direction = determine_direction(ind, regime, setup=setup)
            if direction == "HOLD":
                continue
            if self.cfg.direction_filter and direction != self.cfg.direction_filter:
                continue

            # 3. Strategy-specific hard gate (mirrors live trading_service.py:1159).
            # This is the gate that distinguishes BREAKOUT (needs ADX≥20 + near
            # 52w-high + vol_ratio≥1.2) from VWAP_REVERSAL (needs RSI extreme +
            # vwap_dev≥1%) etc. Without it, every setup that survives the
            # direction vote fires, which over-trades by 3-12× depending on
            # the setup.
            if self.cfg.apply_strategy_entry_gate:
                ok, reason = check_strategy_entry(setup, direction, ind, regime=regime.regime)
                if not ok:
                    if self.cfg.debug_orders:
                        log.debug(
                            "pure_replay_strategy_gate_blocked sym=%s setup=%s dir=%s reason=%s",
                            sym, setup, direction, reason,
                        )
                    continue

            # 4. Swing-specific gate (intraday is a no-op in this branch).
            if self.cfg.is_swing and self.cfg.apply_swing_entry_gate:
                ok, _reason = check_swing_entry(setup, direction, ind, daily_bias, regime.regime)
                if not ok:
                    continue

            # 5. Score (score_signal natively handles MORNING_FADE bypass)
            sig: SignalScore = score_signal(
                symbol=sym, direction=direction, ind=ind, regime=regime,
                cfg=self.s_cfg, daily_bias=daily_bias, setup=setup,
            )
            raw_score = float(sig.score)

            # 6. Affinity multiplier
            if self.cfg.apply_affinity_multiplier:
                affinity_mult = regime_strategy_multiplier(regime.regime, setup, direction)
                affinity_score = max(0, min(100, int(round(raw_score * affinity_mult))))
            else:
                affinity_mult = 1.0
                affinity_score = int(round(raw_score))

            # 7. Brain-state haircut (intraday only consults this for threshold)
            if self.cfg.apply_brain_haircut:
                adjusted_score = self._policy.adjust_signal(affinity_score, brain_state)
            else:
                adjusted_score = affinity_score

            score_for_threshold = affinity_score if self.cfg.is_swing else adjusted_score
            if score_for_threshold < threshold:
                continue

            candidates.append((
                score_for_threshold, setup, direction, raw_score,
                affinity_mult, affinity_score, adjusted_score,
            ))

        if not candidates:
            return

        # ── Pick best-scoring candidate ────────────────────────────────
        # Sort by score_for_threshold desc; ties broken by raw_score desc.
        candidates.sort(key=lambda c: (c[0], c[3]), reverse=True)
        score_for_threshold, setup, direction, raw_score, \
            affinity_mult, affinity_score, adjusted_score = candidates[0]

        # ── Per-bar gates that don't change with setup ────────────────
        # One-shot per (date, symbol, setup, direction).
        fkey = (ctx.bar.ts[:10], sym, setup, direction)
        if fkey in self._fired_today:
            return

        # Concurrency cap.
        if self.cfg.max_concurrent > 0:
            in_flight = len(ctx.account.positions) + len(self._pending_meta)
            if in_flight >= self.cfg.max_concurrent:
                return

        # Pyramid guard — never two open trades on same (symbol, side).
        for p in ctx.account.positions.values():
            if p.symbol == sym and p.side == direction:
                return

        # Position sizing using ATR.
        from autotrader.domain.indicators import calc_atr, normalize_candles
        atr = calc_atr(normalize_candles(candles), 14)
        if atr <= 0:
            return
        sl_dist = atr * self.cfg.default_atr_mult
        if sl_dist <= 0:
            return
        qty = max(1, int(self.cfg.per_trade_risk_inr / sl_dist))
        notional = qty * ind.close
        if direction == "BUY" and notional > ctx.account.cash * 1.05:
            qty = max(1, int(ctx.account.cash * 1.0 / ind.close))
            if qty <= 0:
                return

        tag = f"PR-{sym}-{ctx.sim_epoch:.0f}-{uuid.uuid4().hex[:6]}"
        ctx.account.place_order(
            symbol=sym, side=direction, qty=qty,
            order_type="MARKET", ts=ctx.bar.ts,
            parent_tag=tag, purpose="ENTRY",
        )
        self._pending_meta[tag] = _PendingPositionMeta(
            symbol=sym, direction=direction, qty=qty,
            atr=atr, atr_mult=self.cfg.default_atr_mult,
            setup=setup, is_swing=self.cfg.is_swing,
            regime=regime.regime, score=adjusted_score,
            raw_score=raw_score, affinity_mult=affinity_mult,
            affinity_score=affinity_score, threshold=threshold,
            risk_mode=brain_state.risk_mode,
        )
        self._fired_today.add(fkey)
        if self.cfg.debug_orders:
            log.info(
                "pure_replay_signal sym=%s setup=%s dir=%s raw=%.1f mult=%.2f "
                "aff=%d adj=%d thr=%d risk=%s qty=%d ltp=%.2f atr=%.2f n_cands=%d",
                sym, setup, direction, raw_score, affinity_mult,
                affinity_score, adjusted_score, threshold, brain_state.risk_mode,
                qty, ind.close, atr, len(candidates),
            )


__all__ = ["PureReplayConfig", "PureReplayStrategy"]
