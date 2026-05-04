"""Event-driven bar-by-bar backtest engine.

The engine is **strategy-agnostic** — it walks bars in chronological order,
calls `Strategy.on_bar(bar, ctx)` to let the strategy place orders, then
resolves order matching, applies the exit FSM to every open position, and
marks-to-market.

Strategies plug in by implementing the `Strategy` Protocol. Two ship today:
  * `LiveDecisionStrategy`  (in `replay_live.py`)  — drives sim execution
                            from `scan_decisions` rows.
  * `PureReplayStrategy`    (in `replay_pure.py`)  — re-runs scoring + gates
                            from scratch on historical candles.

Determinism contract
--------------------
* Bars are processed in (ts, symbol) order. Ties broken by symbol asc.
* Strategy.on_bar is called BEFORE order resolution for that bar — so a
  signal generated on bar T can place a market order that fills at bar T+1
  (no look-ahead).
* FSM transitions run AFTER order resolution — same-bar SL/target hits are
  resolved as exit fills before the FSM ticks. This matches live: ticks
  flow through the order matcher first, FSM reads post-fill state.
* mark-to-market runs LAST in each bar — so the equity curve reflects all
  fills produced by that bar.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Protocol

from autotrader.backtest.account import SimAccount
from autotrader.backtest.types import (
    Bar,
    BacktestResult,
    Fill,
    Order,
    Position,
    SimTrade,
)
from autotrader.domain.exit_fsm import (
    ExitState,
    FsmConfig,
    PositionView,
    TickEvent,
    transition,
)

log = logging.getLogger(__name__)


# ── Strategy protocol ─────────────────────────────────────────────────────


@dataclass
class StrategyContext:
    """What `Strategy.on_bar` receives. Lightweight, mutable-by-engine."""
    bar: Bar
    account: SimAccount
    last_prices: dict[str, float]      # latest close per symbol — engine maintains
    bar_index_for_symbol: int          # 0-based count of bars seen for THIS symbol

    # Engine clock — wall-clock-based mfe debounce in the FSM expects "epoch
    # seconds." We use the bar's monotonically-increasing index as a proxy
    # epoch (ts_index × 60 → "minute-equivalent epoch"). The FSM's debounce
    # threshold (default 15s) means: same-bar confirmation. For backtest, we
    # set the debounce via `FsmConfig` → backtest config override.
    sim_epoch: float


class Strategy(Protocol):
    """Plug-in interface for backtest strategies.

    The engine calls `on_bar` for every bar of every symbol in the universe.
    The strategy decides whether to place orders, open positions, or do nothing.
    Exit-FSM ticking is engine-level; strategies that want custom exit logic
    can override by setting `pos.fsm_state = ExitState.TERMINAL` and placing
    their own EXIT order.
    """

    def on_bar(self, ctx: StrategyContext) -> None: ...

    def on_fill(self, ctx: StrategyContext, fill: Fill) -> None:
        """Optional. Called after each fill the engine produces. Default no-op."""

    def finalize(self, account: SimAccount) -> None:
        """Optional. Called after the last bar; close-out hook."""


# ── Engine ────────────────────────────────────────────────────────────────


@dataclass
class EngineConfig:
    fsm_cfg: FsmConfig | None = None
    # Force-close all intraday positions at this bar-of-day index. Set to None
    # to let positions ride to EOD candle by symbol-natural exit.
    eod_intraday_close: bool = True
    # If True, the engine logs FSM events to debug — useful when validating.
    log_fsm_events: bool = False
    # Max positions at any time (engine-enforced safety). 0 = unlimited.
    max_open_positions: int = 0


class BacktestEngine:
    """Walks bars in chronological order, drives strategy + FSM + account."""

    def __init__(
        self,
        *,
        account: SimAccount,
        strategy: Strategy,
        cfg: EngineConfig | None = None,
    ) -> None:
        self.account = account
        self.strategy = strategy
        self.cfg = cfg or EngineConfig()
        self.fsm_cfg = self.cfg.fsm_cfg or FsmConfig()
        self._sym_bar_count: dict[str, int] = {}
        self._last_prices: dict[str, float] = {}
        self._sim_epoch: float = 0.0

    # ── Main loop ─────────────────────────────────────────────────────

    def run(self, bars: list[Bar]) -> BacktestResult:
        """Run the backtest end-to-end. `bars` should be the FULL multi-symbol
        bar stream — engine sorts by (ts, symbol) for determinism."""
        bars_sorted = sorted(bars, key=lambda b: (b.ts, b.symbol))
        t0 = time.time()
        n = 0

        for bar in bars_sorted:
            self._sim_epoch += 1.0
            self._sym_bar_count[bar.symbol] = self._sym_bar_count.get(bar.symbol, 0) + 1
            self._last_prices[bar.symbol] = bar.close

            ctx = StrategyContext(
                bar=bar,
                account=self.account,
                last_prices=self._last_prices,
                bar_index_for_symbol=self._sym_bar_count[bar.symbol],
                sim_epoch=self._sim_epoch,
            )

            # 1. Strategy decides — places orders for fills on bar T+1.
            try:
                self.strategy.on_bar(ctx)
            except Exception:
                log.exception("strategy_on_bar_failed ts=%s sym=%s", bar.ts, bar.symbol)

            # 2. Resolve all open orders against this bar.
            fills = self.account.resolve_bar(bar)
            for f in fills:
                try:
                    self.strategy.on_fill(ctx, f)  # type: ignore[attr-defined]
                except AttributeError:
                    pass
                except Exception:
                    log.exception("strategy_on_fill_failed order=%s", f.order_id)

            # 3. Tick the FSM for every open position on THIS symbol.
            self._tick_fsm_for_symbol(bar)

            # 4. Mark-to-market. One equity point per bar (per symbol — coalesced
            #    by ts in the post-process step if the user wants single-stream).
            self.account.mark_to_market(self._last_prices, bar.ts)
            n += 1

        # Finalize: close any still-open positions at the last seen close.
        self._finalize()
        try:
            self.strategy.finalize(self.account)  # type: ignore[attr-defined]
        except AttributeError:
            pass
        except Exception:
            log.exception("strategy_finalize_failed")

        elapsed = time.time() - t0
        log.info("backtest_engine_done bars=%d trades=%d elapsed=%.2fs",
                 n, len(self.account.closed_trades), elapsed)

        return BacktestResult(
            trades=list(self.account.closed_trades),
            equity_curve=list(self.account.equity_curve),
            meta={
                "bars_processed": str(n),
                "elapsed_s": f"{elapsed:.2f}",
                "starting_cash": str(self.account.cfg.starting_cash),
            },
        )

    # ── FSM ticking ───────────────────────────────────────────────────

    def _tick_fsm_for_symbol(self, bar: Bar) -> None:
        """Apply exit FSM to every open position whose symbol matches this bar.

        For each open position we synthesize a TickEvent at bar.close and let
        `domain.exit_fsm.transition()` decide. If the FSM emits an exit reason
        OR moves the SL, we either close the position (TERMINAL) or update
        its current_sl + best_price + state.

        Within-bar SL/target hits: we use a 3-step sub-tick — at bar.high (for
        BUY) or bar.low (for SELL) we check target/SL first, then settle at
        bar.close for state evolution. This matches the conservative bar-internal
        precedence: SL hit precedes target hit.
        """
        # Snapshot tags first; mutating during iteration confuses dict.
        for tag in list(self.account.positions.keys()):
            pos = self.account.positions.get(tag)
            if pos is None or pos.symbol != bar.symbol:
                continue
            if pos.fsm_state == ExitState.TERMINAL.value:
                continue

            pos.bars_held += 1

            # ── SUB-TICK 1: Check intra-bar SL hit (pessimistic worst-case price) ──
            adverse_price = bar.low if pos.side == "BUY" else bar.high
            if self._would_hit_sl(pos, adverse_price):
                self._close_at_sl(pos, bar)
                continue

            # ── SUB-TICK 2: Check intra-bar target hit (only if SL didn't hit) ──
            favorable_price = bar.high if pos.side == "BUY" else bar.low
            if self._would_hit_target(pos, favorable_price):
                self._close_at_target(pos, bar)
                continue

            # ── SUB-TICK 3: settle at close, evolve FSM state ──
            view = self._make_position_view(pos)
            tick = TickEvent(ltp=bar.close, ts=self._sim_epoch,
                             regime=pos.entry_regime, entry_regime=pos.entry_regime)
            out = transition(view, tick, self.fsm_cfg)

            # Update mutable FSM state on the position.
            pos.fsm_state = out.next_state.value
            if out.sl_changed and out.new_sl > 0:
                pos.current_sl = out.new_sl
            # Update best_price + peak_mfe + confirm_started_epoch from the view.
            if pos.side == "BUY":
                pos.best_price = max(pos.best_price or pos.entry_price, bar.close)
            else:
                pos.best_price = min(pos.best_price or pos.entry_price, bar.close) \
                    if pos.best_price else bar.close
            pos.peak_mfe_r = max(pos.peak_mfe_r, out.mfe_r_now)
            pos.mfe_r = max(pos.mfe_r, out.mfe_r_now)
            # MAE: r-from-entry at bar's adverse extreme.
            adverse_r = self._r_from(pos, adverse_price)
            pos.mae_r = min(pos.mae_r, adverse_r)
            # Sync confirm_started_epoch from FSM events.
            if "confirm_arming" in out.events and pos.confirm_started_epoch == 0.0:
                pos.confirm_started_epoch = self._sim_epoch
            elif "confirm_aborted" in out.events:
                pos.confirm_started_epoch = 0.0

            if out.exit_reason:
                # FSM-driven exit (FLAT_TIMEOUT, etc.) — exit at bar.close.
                self._close_at_fsm_exit(pos, bar, out.exit_reason)

    def _make_position_view(self, pos: Position) -> PositionView:
        return PositionView(
            tag=pos.tag,
            side=pos.side,
            entry_price=pos.entry_price,
            atr=pos.entry_atr,
            sl_dist=pos.sl_dist,
            is_swing=pos.is_swing,
            entry_epoch=0.0,
            target=pos.target,
            state=ExitState(pos.fsm_state),
            best_price=pos.best_price or pos.entry_price,
            peak_mfe_r=pos.peak_mfe_r,
            current_sl=pos.current_sl,
            confirm_started_epoch=pos.confirm_started_epoch,
        )

    # ── Exit helpers ──────────────────────────────────────────────────

    def _would_hit_sl(self, pos: Position, adverse_price: float) -> bool:
        sl = pos.current_sl
        if sl <= 0:
            return False
        if pos.side == "BUY":
            return adverse_price <= sl
        return adverse_price >= sl

    def _would_hit_target(self, pos: Position, favorable_price: float) -> bool:
        t = pos.target
        if t <= 0:
            return False
        # Don't fire target if we've already promoted to RUNNER (let it ride).
        if pos.fsm_state == ExitState.RUNNER.value:
            return False
        if pos.side == "BUY":
            return favorable_price >= t
        return favorable_price <= t

    def _r_from(self, pos: Position, price: float) -> float:
        if pos.sl_dist <= 0:
            return 0.0
        d = 1 if pos.side == "BUY" else -1
        return round((price - pos.entry_price) * d / pos.sl_dist, 4)

    def _close_at_sl(self, pos: Position, bar: Bar) -> None:
        # Adverse fill: exact stop price, but apply slippage 1× tick worse.
        # We model this as: fill at stop_price (no further adjustment — stop is
        # already an adverse-by-design fill). Slippage is built into account
        # only for ENTRY purpose orders; stop fills are at-stop by convention.
        exit_side = "SELL" if pos.side == "BUY" else "BUY"
        order = self.account.place_order(
            symbol=pos.symbol, side=exit_side, qty=pos.qty,
            order_type="MARKET", ts=bar.ts, parent_tag=pos.tag,
            purpose="EXIT_SL",
        )
        # Manual fill at the stop price — bypassing slippage adjust for stops.
        from autotrader.backtest.costs import compute_leg_cost
        cost = compute_leg_cost(
            side=exit_side, qty=pos.qty, price=pos.current_sl,
            is_swing=pos.is_swing, cfg=self.account.cfg.cost_cfg,
        )
        order.status = order.status.__class__("FILLED")  # type: ignore[arg-type]
        from autotrader.backtest.types import OrderStatus
        order.status = OrderStatus.FILLED
        order.filled_price = pos.current_sl
        order.filled_qty = pos.qty
        order.filled_ts = bar.ts
        self.account.open_orders.pop(order.order_id, None)
        # Cash impact
        notional = pos.qty * pos.current_sl
        if exit_side == "BUY":
            self.account.cash -= notional + cost
        else:
            self.account.cash += notional - cost
        fill = Fill(
            order_id=order.order_id, symbol=pos.symbol, side=exit_side,
            qty=pos.qty, price=pos.current_sl, ts=bar.ts,
            parent_tag=pos.tag, costs=cost,
        )
        self.account.fills.append(fill)
        self.account.close_position(tag=pos.tag, exit_fill=fill, exit_reason="SL_HIT")

    def _close_at_target(self, pos: Position, bar: Bar) -> None:
        exit_side = "SELL" if pos.side == "BUY" else "BUY"
        order = self.account.place_order(
            symbol=pos.symbol, side=exit_side, qty=pos.qty,
            order_type="MARKET", ts=bar.ts, parent_tag=pos.tag,
            purpose="EXIT_TARGET",
        )
        from autotrader.backtest.types import OrderStatus
        from autotrader.backtest.costs import compute_leg_cost
        cost = compute_leg_cost(
            side=exit_side, qty=pos.qty, price=pos.target,
            is_swing=pos.is_swing, cfg=self.account.cfg.cost_cfg,
        )
        order.status = OrderStatus.FILLED
        order.filled_price = pos.target
        order.filled_qty = pos.qty
        order.filled_ts = bar.ts
        self.account.open_orders.pop(order.order_id, None)
        notional = pos.qty * pos.target
        if exit_side == "BUY":
            self.account.cash -= notional + cost
        else:
            self.account.cash += notional - cost
        fill = Fill(
            order_id=order.order_id, symbol=pos.symbol, side=exit_side,
            qty=pos.qty, price=pos.target, ts=bar.ts,
            parent_tag=pos.tag, costs=cost,
        )
        self.account.fills.append(fill)
        self.account.close_position(tag=pos.tag, exit_fill=fill, exit_reason="TARGET_HIT")

    def _close_at_fsm_exit(self, pos: Position, bar: Bar, reason: str) -> None:
        """FSM-driven exit (FLAT_TIMEOUT) — fills at bar.close."""
        exit_side = "SELL" if pos.side == "BUY" else "BUY"
        from autotrader.backtest.types import OrderStatus
        from autotrader.backtest.costs import compute_leg_cost
        cost = compute_leg_cost(
            side=exit_side, qty=pos.qty, price=bar.close,
            is_swing=pos.is_swing, cfg=self.account.cfg.cost_cfg,
        )
        order = self.account.place_order(
            symbol=pos.symbol, side=exit_side, qty=pos.qty,
            order_type="MARKET", ts=bar.ts, parent_tag=pos.tag,
            purpose="EXIT_FSM",
        )
        order.status = OrderStatus.FILLED
        order.filled_price = bar.close
        order.filled_qty = pos.qty
        order.filled_ts = bar.ts
        self.account.open_orders.pop(order.order_id, None)
        notional = pos.qty * bar.close
        if exit_side == "BUY":
            self.account.cash -= notional + cost
        else:
            self.account.cash += notional - cost
        fill = Fill(
            order_id=order.order_id, symbol=pos.symbol, side=exit_side,
            qty=pos.qty, price=bar.close, ts=bar.ts,
            parent_tag=pos.tag, costs=cost,
        )
        self.account.fills.append(fill)
        self.account.close_position(tag=pos.tag, exit_fill=fill, exit_reason=reason)

    # ── Finalize ──────────────────────────────────────────────────────

    def _finalize(self) -> None:
        """Close any positions still open at the end of the bar stream."""
        if not self.account.positions:
            return
        for tag in list(self.account.positions.keys()):
            pos = self.account.positions.get(tag)
            if pos is None:
                continue
            last_px = self._last_prices.get(pos.symbol, pos.entry_price)
            # Synthesize a closing bar at last_px.
            bar = Bar(
                symbol=pos.symbol, ts=pos.entry_ts, open=last_px,
                high=last_px, low=last_px, close=last_px, volume=0.0,
                timeframe="EOD",
            )
            self._close_at_fsm_exit(pos, bar, "EOD_FORCE")


__all__ = ["Strategy", "StrategyContext", "EngineConfig", "BacktestEngine"]
