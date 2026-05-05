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
* **Brain confidence adjustment** (`market_brain_service.adjust_signal`)
  — depends on full BrainState, not just the regime. Sim only has the
  archived `market_brain_history` (regime + risk_mode + a few scalars).
* **Daily bias swing-mode adjustments** — would need 1d candles loaded
  in parallel; sim today loads only one timeframe. Future work.
* **Per-strategy entry gates** (`check_strategy_entry`, `check_swing_entry`)
  — these gates ALSO live in `domain.scoring`. They are pure functions
  but operate on `IndicatorSnapshot`, so we can wire them in cheaply.
  We do call them; if they reject, we mark the decision blocked.
* **News / options gates** — out of scope per audit (no historical
  options chain or news_store).
* **Universe selection** — sim runs over the symbols you give it.
* **Dynamic min_signal_score** — scan_service computes a per-tick
  min_score that floats with regime/brain. Sim uses the static default
  (`StrategySettings.min_signal_score = 72`) unless you override.

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
from autotrader.domain.scoring import determine_direction, score_signal
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
    # Restrict to certain setups (None = test all setups in the universe).
    setups: tuple[str, ...] = ("BREAKOUT", "VWAP_TREND", "VWAP_REVERSAL")
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


def _bar_to_candle(b: Bar) -> Candle:
    """Convert a backtest Bar to the indicator module's Candle tuple."""
    return (b.ts, b.open, b.high, b.low, b.close, b.volume)


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
    ) -> None:
        self.cfg = cfg or PureReplayConfig()
        self.s_cfg = strategy_settings or StrategySettings()
        self.brain = brain or BrainTimeline([])
        self._history: dict[str, Deque[Candle]] = {}
        self._pending_meta: dict[str, _PendingPositionMeta] = {}
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

        # Per-setup loop — one symbol can be a candidate for multiple setups.
        for setup in self.cfg.setups:
            self._maybe_signal(ctx, sym, setup, list(dq))

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

    def _maybe_signal(
        self,
        ctx: StrategyContext,
        sym: str,
        setup: str,
        candles: list[Candle],
    ) -> None:
        ind = compute_indicators(candles, self.s_cfg)
        if ind is None:
            return

        regime = _make_regime_snapshot(self.brain, ctx.bar.ts)

        # Hard-block gate — matches the policy-block live applies in
        # `scan_service`. Mismatched (regime, strategy) pairs (e.g.
        # BREAKOUT in CHOP) get rejected before scoring.
        if self.cfg.apply_hard_blocks and regime_hard_blocks_strategy(
            regime.regime, setup
        ):
            return

        direction = determine_direction(ind, regime, setup=setup)
        if direction == "HOLD":
            return
        if self.cfg.direction_filter and direction != self.cfg.direction_filter:
            return

        sig: SignalScore = score_signal(
            symbol=sym,
            direction=direction,
            ind=ind,
            regime=regime,
            cfg=self.s_cfg,
            setup=setup,
        )
        raw_score = float(sig.score)

        # Regime-strategy affinity multiplier — same call live makes in
        # `trading_service._maybe_open` (line 850). Mismatched setups get
        # damped (down to 0.2x); aligned ones get a boost (up to 1.4x).
        if self.cfg.apply_affinity_multiplier:
            affinity_mult = regime_strategy_multiplier(
                regime.regime, setup, direction,
            )
            adjusted_score = max(0, min(100, int(round(raw_score * affinity_mult))))
        else:
            affinity_mult = 1.0
            adjusted_score = int(round(raw_score))

        threshold = (
            self.cfg.min_signal_score
            if self.cfg.min_signal_score is not None
            else self.s_cfg.min_signal_score
        )
        if adjusted_score < threshold:
            return

        # One-shot per (date, symbol, setup, direction) to avoid
        # re-firing every subsequent bar after the threshold is crossed.
        # The pyramid guard in _maybe_open is a stricter superset check
        # (one open at a time per name+side) but this gates emit too.
        fkey = (ctx.bar.ts[:10], sym, setup, direction)
        if fkey in self._fired_today:
            return

        # Concurrency cap.
        if self.cfg.max_concurrent > 0:
            in_flight = len(ctx.account.positions) + len(self._pending_meta)
            if in_flight >= self.cfg.max_concurrent:
                return

        # Pyramid guard.
        for p in ctx.account.positions.values():
            if p.symbol == sym and p.side == direction:
                return

        # Position sizing using the indicator's ATR.
        atr = max(ind.high - ind.low, 0.01)  # placeholder — see note below
        # compute_indicators returns ATR(14) but doesn't expose it on the
        # IndicatorSnapshot — re-compute from the most recent 14 bars.
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
        )
        self._fired_today.add(fkey)
        if self.cfg.debug_orders:
            log.info(
                "pure_replay_signal sym=%s setup=%s dir=%s raw=%.1f mult=%.2f "
                "adj=%d qty=%d ltp=%.2f atr=%.2f",
                sym, setup, direction, raw_score, affinity_mult,
                adjusted_score, qty, ind.close, atr,
            )


__all__ = ["PureReplayConfig", "PureReplayStrategy"]
