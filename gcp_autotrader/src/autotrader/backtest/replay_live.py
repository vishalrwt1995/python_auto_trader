"""Live-decision replay strategy.

"Given the signals the live system actually emitted, what would the sim
execution + exit FSM produce?"

This strategy is the FASTEST path to a useful backtest answer because it
sidesteps the entire decoupling problem — we don't re-run scoring/gates,
we just replay them. What we DO simulate fresh:
  1. Order placement at the next bar's open (with slippage)
  2. Bar-by-bar SL / target / FSM ticks
  3. Costs (STT/exchange/GST/etc.)

What this measures
------------------
* Whether the exit FSM is leaving R on the table or stopping out too early.
* Whether the cost model meaningfully erodes the strategy's edge.
* Counterfactual: if we set `unblock_reasons=[...]`, we can include signals
  that were *blocked* in the live system and see what they would have
  earned. This is how you A/B individual gates.

What this does NOT measure
--------------------------
* Whether the SCORING is calibrated correctly — that's pure-replay's job.
* Whether the universe selection / watchlist is right.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass

from autotrader.backtest.account import SimAccount
from autotrader.backtest.data import ScanDecisionRow
from autotrader.backtest.engine import StrategyContext
from autotrader.backtest.types import Fill, Position
from autotrader.domain.exit_fsm import ExitState

log = logging.getLogger(__name__)


@dataclass
class LiveReplayConfig:
    # Per-trade risk (₹) — used to derive qty when we don't have it on the row.
    per_trade_risk_inr: float = 5_000.0
    # Default ATR multiple for SL distance when row.atr_mult is missing/zero.
    default_atr_mult: float = 1.74
    # Default reward:risk for synthetic target when row doesn't carry it.
    default_rr: float = 2.0
    # Reasons to UNBLOCK — i.e. include signals that were blocked for these reasons
    # as if they had qualified. Use this to A/B individual gates.
    unblock_reasons: tuple[str, ...] = ()
    # Force only certain setups (None = include all qualified).
    setups_filter: tuple[str, ...] | None = None
    # Restrict directions: None = both, "BUY" or "SELL" to limit.
    direction_filter: str | None = None
    # Max concurrent positions — engine-side cap. 0 = unlimited.
    max_concurrent: int = 0
    # When True, swing decisions (wl_type='swing') route to delivery cost model.
    honor_wl_type: bool = True
    # When True, emit a debug log for every order placed. Verbose.
    debug_orders: bool = False


@dataclass
class _PendingPositionMeta:
    """Metadata captured at order placement, hydrated into a Position on fill."""
    symbol: str
    direction: str
    qty: int
    atr: float
    atr_mult: float
    setup: str
    wl_type: str
    regime: str


class LiveDecisionStrategy:
    """Replays `scan_decisions` rows as live orders into the SimAccount.

    Wiring
    ------
    * Construct with the full list of decisions for the run window.
    * The strategy indexes them by (symbol, scan_ts) and consumes them as
      bars stream by; on each bar T for symbol S, if a decision exists at
      scan_ts <= T, we emit a market entry order which fills at T+1.
    * SL = ltp - (atr × atr_mult) for BUY, ltp + (atr × atr_mult) for SELL.
    * Target = ltp + rr × sl_dist for BUY (mirrored for SELL).
    * Position size = floor(per_trade_risk / sl_dist), min 1.
    """

    def __init__(
        self,
        decisions: list[ScanDecisionRow],
        cfg: LiveReplayConfig | None = None,
    ) -> None:
        self.cfg = cfg or LiveReplayConfig()
        self._by_key: dict[tuple[str, str], ScanDecisionRow] = {}
        self._consumed: set[tuple[str, str]] = set()
        # Per-instance pending-meta (NOT class-level — must be unique per run).
        self._pending_meta: dict[str, _PendingPositionMeta] = {}

        for d in decisions:
            if not self._row_eligible(d):
                continue
            key = (d.symbol.upper(), d.scan_ts)
            # If multiple rows hit the same (sym, ts), keep the latest by
            # adjusted_score (best-quality decision wins). scan_ts is unique
            # in practice but defend.
            existing = self._by_key.get(key)
            if existing is None or d.adjusted_score >= existing.adjusted_score:
                self._by_key[key] = d

    def _row_eligible(self, d: ScanDecisionRow) -> bool:
        """Decide whether this row should produce an order."""
        if d.qualified:
            include = True
        else:
            include = d.blocked_reason in self.cfg.unblock_reasons
        if not include:
            return False
        if d.direction not in ("BUY", "SELL"):
            return False
        if self.cfg.direction_filter and d.direction != self.cfg.direction_filter:
            return False
        if self.cfg.setups_filter and d.setup not in self.cfg.setups_filter:
            return False
        if d.atr <= 0 or d.ltp <= 0:
            return False
        return True

    # ── Engine callbacks ─────────────────────────────────────────────

    def on_bar(self, ctx: StrategyContext) -> None:
        bar = ctx.bar
        # Look for a decision at scan_ts <= bar.ts that hasn't been consumed.
        # We only fire ONCE per (symbol, scan_ts).
        for key, decision in list(self._by_key.items()):
            sym, ts = key
            if sym != bar.symbol:
                continue
            if key in self._consumed:
                continue
            if ts > bar.ts:
                continue
            self._maybe_open(ctx, decision)
            self._consumed.add(key)

    def on_fill(self, ctx: StrategyContext, fill: Fill) -> None:
        """When an entry fill lands, register the position with the account."""
        tag = fill.parent_tag
        if not tag:
            return
        if tag in ctx.account.positions:
            return  # already opened
        meta = self._pending_meta.pop(tag, None)
        if meta is None:
            return  # not one of ours (probably an exit fill)

        is_swing = (meta.wl_type == "swing") if self.cfg.honor_wl_type else False
        side = meta.direction
        atr = meta.atr
        atr_mult = meta.atr_mult or self.cfg.default_atr_mult
        sl_dist = atr * atr_mult
        if side == "BUY":
            initial_sl = round(fill.price - sl_dist, 2)
            target = round(fill.price + sl_dist * self.cfg.default_rr, 2)
        else:
            initial_sl = round(fill.price + sl_dist, 2)
            target = round(fill.price - sl_dist * self.cfg.default_rr, 2)

        pos = Position(
            tag=tag,
            symbol=meta.symbol,
            side=side,
            qty=meta.qty,
            setup=meta.setup,
            is_swing=is_swing,
            entry_price=fill.price,
            entry_ts=fill.ts,
            entry_atr=atr,
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
            log.info("position_opened tag=%s sym=%s side=%s qty=%d entry=%.2f sl=%.2f tgt=%.2f",
                     pos.tag, pos.symbol, pos.side, pos.qty, pos.entry_price,
                     pos.initial_sl, pos.target)

    def finalize(self, account: SimAccount) -> None:
        # Engine handles open-position close-out in `_finalize`; nothing to do.
        return

    # ── Helpers ──────────────────────────────────────────────────────

    def _maybe_open(self, ctx: StrategyContext, d: ScanDecisionRow) -> None:
        # Concurrency cap. Count BOTH open positions and our own pending
        # entries — orders placed this bar haven't filled yet but will, so
        # they reserve a slot toward the cap.
        if self.cfg.max_concurrent > 0:
            in_flight = len(ctx.account.positions) + len(self._pending_meta)
            if in_flight >= self.cfg.max_concurrent:
                return
        # Don't pyramid on the same (symbol, side) — one open at a time per name.
        for p in ctx.account.positions.values():
            if p.symbol == d.symbol and p.side == d.direction:
                return

        sl_dist = d.atr * (d.atr_mult or self.cfg.default_atr_mult)
        if sl_dist <= 0:
            return
        qty = max(1, int(self.cfg.per_trade_risk_inr / sl_dist))
        # Cash sanity: for longs, ensure cash covers notional. Allow leverage
        # for shorts — we book collateral but don't enforce strictly here.
        notional = qty * d.ltp
        if d.direction == "BUY" and notional > ctx.account.cash * 1.05:
            qty = max(1, int(ctx.account.cash * 1.0 / d.ltp))
            if qty <= 0:
                return

        tag = f"BT-{d.symbol}-{ctx.sim_epoch:.0f}-{uuid.uuid4().hex[:6]}"
        ctx.account.place_order(
            symbol=d.symbol, side=d.direction, qty=qty,
            order_type="MARKET", ts=ctx.bar.ts,
            parent_tag=tag, purpose="ENTRY",
        )
        self._pending_meta[tag] = _PendingPositionMeta(
            symbol=d.symbol, direction=d.direction, qty=qty,
            atr=d.atr, atr_mult=d.atr_mult or self.cfg.default_atr_mult,
            setup=d.setup, wl_type=d.wl_type, regime=d.regime,
        )
        if self.cfg.debug_orders:
            log.info("entry_placed sym=%s side=%s qty=%d ltp=%.2f atr=%.2f setup=%s",
                     d.symbol, d.direction, qty, d.ltp, d.atr, d.setup)


__all__ = ["LiveReplayConfig", "LiveDecisionStrategy"]
