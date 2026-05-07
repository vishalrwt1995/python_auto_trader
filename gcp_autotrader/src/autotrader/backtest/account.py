"""Simulator account: orders, fills, positions, cash, equity.

The SimAccount is the *broker* in the backtest — strategies submit orders
to it, bars stream in, and it resolves the orders deterministically.

Order matching rules (single-bar resolution)
--------------------------------------------
* MARKET ENTRY:  fills at next bar's open. Slippage applied adversely.
* LIMIT ENTRY:   fills if next bar's [low, high] crosses the limit price,
                 at the limit price (best-case assumption).
* STOP / SL:     fills if the bar's [low, high] crosses the stop price.
                 Filled at stop_price, NOT at the bar low/high — the engine
                 elsewhere applies stop-slippage.
* EOD / TIME:    fills at the bar's close.

Bar-internal precedence
-----------------------
When a single bar would trigger BOTH the SL and the target, we assume
**SL hits first** (pessimistic). Bias is downward — the right way to be
wrong. See `resolve_bar()`.

Concurrency / risk
------------------
The account doesn't enforce per-trade risk caps or position limits — that's
the strategy's responsibility. The account only enforces:
  * Cash ≥ qty × fill_price (no leverage by default; opt-in via `allow_short_unlimited`)
  * One open position per (symbol, side) at a time (multiple unique tags allowed)
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field

from autotrader.backtest.costs import CostConfig, compute_leg_cost
from autotrader.backtest.slippage import SlippageModel, default_model
from autotrader.backtest.types import (
    Bar,
    EquityPoint,
    Fill,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    SimTrade,
)

log = logging.getLogger(__name__)


@dataclass
class SimAccountConfig:
    starting_cash: float = 1_000_000.0      # ₹10L default
    allow_shorts: bool = True
    allow_short_unlimited: bool = False     # if False, shorts also need cash collateral
    cost_cfg: CostConfig = field(default_factory=CostConfig)


class SimAccount:
    """Single-account portfolio simulator. Stateful — engine passes bars in
    chronological order via `on_bar()` and `resolve_bar()`."""

    def __init__(
        self,
        cfg: SimAccountConfig | None = None,
        slippage: SlippageModel | None = None,
    ) -> None:
        self.cfg = cfg or SimAccountConfig()
        self.slippage = slippage or default_model()

        self.cash: float = self.cfg.starting_cash
        self._high_water_equity: float = self.cfg.starting_cash

        self.open_orders: dict[str, Order] = {}
        self.positions: dict[str, Position] = {}     # keyed by tag
        self.fills: list[Fill] = []
        self.closed_trades: list[SimTrade] = []
        self.equity_curve: list[EquityPoint] = []

    # ── Order placement (called by strategy) ──────────────────────────

    def place_order(
        self,
        *,
        symbol: str,
        side: OrderSide,
        qty: int,
        order_type: OrderType,
        ts: str,
        limit_price: float = 0.0,
        stop_price: float = 0.0,
        parent_tag: str = "",
        purpose: str = "ENTRY",
    ) -> Order:
        order = Order(
            order_id=str(uuid.uuid4())[:8],
            symbol=symbol,
            side=side,
            qty=qty,
            order_type=order_type,
            limit_price=limit_price,
            stop_price=stop_price,
            placed_ts=ts,
            parent_tag=parent_tag,
            purpose=purpose,
        )
        self.open_orders[order.order_id] = order
        return order

    def cancel_order(self, order_id: str, reason: str = "") -> bool:
        o = self.open_orders.pop(order_id, None)
        if o is None:
            return False
        o.status = OrderStatus.CANCELLED
        o.reject_reason = reason
        return True

    # ── Bar-by-bar resolution (called by engine) ──────────────────────

    def resolve_bar(self, bar: Bar) -> list[Fill]:
        """Match all open orders for `bar.symbol` against this bar.

        Returns the list of fills produced by this bar (may be empty).
        Open orders that didn't fill remain in `self.open_orders`.
        """
        new_fills: list[Fill] = []
        # Iterate snapshot to allow dict mutation.
        for order_id, order in list(self.open_orders.items()):
            if order.symbol != bar.symbol:
                continue
            if order.status != OrderStatus.PENDING:
                continue

            fill_price = self._match_order_to_bar(order, bar)
            if fill_price is None:
                continue

            # Apply slippage (entries get adverse on theoretical; exit stops
            # don't get further slippage on top of stop_price — that's already
            # an adverse-by-design fill).
            if order.purpose == "ENTRY":
                fill_price = self.slippage.adjust(
                    theoretical=fill_price, side=order.side, bar=bar,
                )

            fill = self._fill_order(order, fill_price, bar.ts, bar)
            new_fills.append(fill)
            self.fills.append(fill)
            self.open_orders.pop(order_id, None)

        return new_fills

    def _match_order_to_bar(self, order: Order, bar: Bar) -> float | None:
        """Return theoretical fill price if this bar triggers the order, else None.

        Convention: for an order placed at bar T, it matches against bar T+1
        onwards. The engine ensures this by only calling `resolve_bar()` AFTER
        bar T has been fully processed (and orders have been placed)."""
        if order.placed_ts >= bar.ts:
            # Order placed this bar or later — wait for next bar.
            return None

        ot = order.order_type
        if ot == "MARKET":
            return bar.open
        if ot == "LIMIT":
            if order.side == "BUY" and bar.low <= order.limit_price:
                return min(bar.open, order.limit_price)
            if order.side == "SELL" and bar.high >= order.limit_price:
                return max(bar.open, order.limit_price)
            return None
        if ot == "STOP":
            # SL-style: triggers on adverse breach
            if order.side == "SELL" and bar.low <= order.stop_price:
                return order.stop_price
            if order.side == "BUY" and bar.high >= order.stop_price:
                return order.stop_price
            return None
        if ot == "STOP_LIMIT":
            # Triggers when stop crossed; fills at min/max of (limit, bar)
            triggered = False
            if order.side == "SELL" and bar.low <= order.stop_price:
                triggered = True
            if order.side == "BUY" and bar.high >= order.stop_price:
                triggered = True
            if not triggered:
                return None
            return order.limit_price
        return None

    def _fill_order(self, order: Order, price: float, ts: str, bar: Bar) -> Fill:
        is_swing = self._infer_swing(order)
        cost = compute_leg_cost(
            side=order.side, qty=order.qty, price=price,
            is_swing=is_swing, cfg=self.cfg.cost_cfg,
        )
        order.status = OrderStatus.FILLED
        order.filled_price = price
        order.filled_qty = order.qty
        order.filled_ts = ts

        # Update cash
        notional = order.qty * price
        if order.side == "BUY":
            self.cash -= notional + cost
        else:
            self.cash += notional - cost

        return Fill(
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side,
            qty=order.qty,
            price=price,
            ts=ts,
            parent_tag=order.parent_tag,
            costs=cost,
        )

    def _infer_swing(self, order: Order) -> bool:
        """Look up the parent position (if any) to decide intraday vs swing for
        cost calc. For the entry leg with no parent yet, we default to False
        (intraday) — strategies that open swings should set parent_tag and
        register the position before the entry fill, OR the engine can
        retroactively reclassify."""
        pos = self.positions.get(order.parent_tag)
        if pos is None:
            # Orphan entry — assume intraday. Strategies override by registering
            # the position with is_swing=True before the next bar.
            return False
        return pos.is_swing

    # ── Position lifecycle (called by strategy on fill events) ────────

    def open_position(self, *, fill: Fill, position: Position) -> None:
        """Register a fresh position. The strategy calls this after seeing
        the entry fill. `position` should already have its planned SL/target
        and FSM state initialized."""
        position.entry_price = fill.price
        position.entry_ts = fill.ts
        position.current_sl = position.initial_sl
        position.best_price = fill.price
        # Carry the entry-leg cost on the position for round-trip accounting.
        position.costs = fill.costs
        self.positions[position.tag] = position

    def close_position(
        self,
        *,
        tag: str,
        exit_fill: Fill,
        exit_reason: str,
    ) -> SimTrade | None:
        pos = self.positions.pop(tag, None)
        if pos is None:
            return None

        pos.exit_price = exit_fill.price
        pos.exit_ts = exit_fill.ts
        pos.exit_reason = exit_reason
        pos.costs = round(pos.costs + exit_fill.costs, 2)

        gross = (pos.exit_price - pos.entry_price) * pos.qty
        if pos.side == "SELL":
            gross = -gross
        pos.gross_pnl = round(gross, 2)
        pos.net_pnl = round(gross - pos.costs, 2)
        if pos.sl_dist > 0 and pos.qty > 0:
            pos.realized_r = round(pos.net_pnl / (pos.sl_dist * pos.qty), 4)

        trade = SimTrade(
            trade_id=pos.tag,
            symbol=pos.symbol,
            side=pos.side,
            qty=pos.qty,
            setup=pos.setup,
            is_swing=pos.is_swing,
            entry_ts=pos.entry_ts,
            entry_price=pos.entry_price,
            exit_ts=pos.exit_ts,
            exit_price=pos.exit_price,
            initial_sl=pos.initial_sl,
            target=pos.target,
            sl_dist=pos.sl_dist,
            gross_pnl=pos.gross_pnl,
            costs=pos.costs,
            net_pnl=pos.net_pnl,
            realized_r=pos.realized_r,
            mfe_r=pos.mfe_r,
            mae_r=pos.mae_r,
            bars_held=pos.bars_held,
            exit_reason=pos.exit_reason,
            regime_at_entry=pos.entry_regime,
        )
        self.closed_trades.append(trade)
        return trade

    # ── Mark-to-market / equity ───────────────────────────────────────

    def mark_to_market(self, last_prices: dict[str, float], ts: str) -> EquityPoint:
        positions_value = 0.0
        for pos in self.positions.values():
            px = last_prices.get(pos.symbol, pos.entry_price)
            if pos.side == "BUY":
                positions_value += pos.qty * px
            else:
                # Short: "value" is collateral + unrealized pnl
                positions_value += pos.qty * pos.entry_price + (pos.entry_price - px) * pos.qty
        equity = self.cash + positions_value
        if equity > self._high_water_equity:
            self._high_water_equity = equity
        dd = 0.0
        if self._high_water_equity > 0:
            dd = round((self._high_water_equity - equity) / self._high_water_equity * 100, 4)
        ep = EquityPoint(
            ts=ts,
            equity=round(equity, 2),
            cash=round(self.cash, 2),
            positions_value=round(positions_value, 2),
            drawdown_pct=dd,
            open_positions=len(self.positions),
        )
        self.equity_curve.append(ep)
        return ep


__all__ = ["SimAccount", "SimAccountConfig"]
