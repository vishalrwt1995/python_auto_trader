"""Order service — entry/exit via Upstox, positions stored in Firestore, trades in BigQuery."""
from __future__ import annotations

import logging
import random
import string
import time
from dataclasses import dataclass
from typing import Any

from autotrader.adapters.bigquery_client import BigQueryClient
from autotrader.adapters.firestore_state import FirestoreStateStore
from autotrader.adapters.pubsub_client import PubSubClient
from autotrader.adapters.upstox_client import UpstoxClient
from autotrader.domain.risk import calc_round_trip_brokerage
from autotrader.settings import AppSettings
from autotrader.time_utils import now_ist_str, now_utc_iso, parse_any_ts, today_ist

logger = logging.getLogger(__name__)


def _bq_insert_with_retry(bq: BigQueryClient, trade_row: dict[str, Any], tag: str, max_attempts: int = 3) -> None:
    """Insert a trade row to BigQuery with exponential backoff retries."""
    for attempt in range(1, max_attempts + 1):
        try:
            bq.insert_trade(trade_row)
            return
        except Exception:
            if attempt < max_attempts:
                wait = 2 ** attempt
                logger.warning("bq_trade_insert_retry tag=%s attempt=%d wait=%ds", tag, attempt, wait)
                time.sleep(wait)
            else:
                logger.error("bq_trade_insert_failed_permanent tag=%s after %d attempts", tag, max_attempts)


def make_ref_id() -> str:
    ts = format(int(time.time() * 1000), "x")[-6:].upper()
    rand = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(3))
    return f"AT-{ts}-{rand}"


def _order_status(raw: str | None) -> str:
    s = str(raw or "").strip().upper()
    if not s:
        return "UNKNOWN"
    if s in {"COMPLETE", "COMPLETED", "FILLED", "TRADED", "EXECUTED"}:
        return "FILLED"
    return s


def _is_final_non_fill(status: str) -> bool:
    return status.upper() in {"REJECTED", "CANCELLED", "CANCELED", "FAILED", "EXPIRED"}


@dataclass
class OrderService:
    settings: AppSettings
    state: FirestoreStateStore
    upstox: UpstoxClient
    bq: BigQueryClient
    pubsub: PubSubClient | None = None

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _append_order_log_sheets(self, row: list[Any]) -> None:
        pass  # Sheets removed; orders persisted in Firestore

    def _append_position_sheets(self, row: list[Any]) -> None:
        pass  # Sheets removed; positions persisted in Firestore

    def _extract_order_snapshot(self, order_id: str, ref_id: str) -> dict[str, Any] | None:
        """Find an order by order_id or ref_id from today's Upstox order list."""
        try:
            orders = self.upstox.list_orders()
        except Exception:
            logger.exception("list_orders failed during reconciliation")
            return None
        for obj in orders:
            oid = str(
                obj.get("order_id") or obj.get("upstox_order_id") or obj.get("id") or ""
            ).strip()
            rid = str(
                obj.get("order_reference_id") or obj.get("reference_id") or obj.get("tag") or ""
            ).strip()
            if (order_id and oid == order_id) or (ref_id and rid == ref_id):
                return {
                    "status": _order_status(
                        obj.get("status") or obj.get("order_status") or obj.get("state")
                    ),
                    "filled_qty": float(
                        obj.get("filled_quantity") or obj.get("quantity") or 0
                    ),
                    "avg_fill_price": float(
                        obj.get("average_price") or obj.get("avg_price") or 0
                    ),
                    "message": str(obj.get("message") or obj.get("remark") or ""),
                    "raw": obj,
                }
        return None

    def _await_fill(
        self,
        order_id: str,
        ref_id: str,
        qty: int,
        timeout_ms: int = 25_000,
        poll_ms: int = 1_200,
    ) -> dict[str, Any]:
        started = time.time()
        while (time.time() - started) * 1000 < timeout_ms:
            snap = self._extract_order_snapshot(order_id, ref_id)
            if snap:
                status = _order_status(str(snap.get("status", "")))
                filled_qty = float(snap.get("filled_qty", 0))
                if status == "FILLED" or (qty > 0 and filled_qty >= qty):
                    return {"filled": True, "terminal": False, "snapshot": snap}
                if _is_final_non_fill(status):
                    return {"filled": False, "terminal": True, "snapshot": snap}
            time.sleep(poll_ms / 1000.0)
        return {
            "filled": False,
            "terminal": False,
            "snapshot": self._extract_order_snapshot(order_id, ref_id),
        }

    def _save_position_firestore(
        self,
        *,
        position_tag: str,
        symbol: str,
        exchange: str,
        segment: str,
        side: str,
        qty: int,
        entry_price: float,
        sl_price: float,
        target: float,
        atr: float,
        strategy: str = "",
        order_id: str = "",
        regime: str = "",
        risk_mode: str = "",
        signal_score: int = 0,
        product: str = "MIS",
        wl_type: str = "intraday",
        instrument_key: str = "",
    ) -> None:
        _sl_dist = round(abs(entry_price - sl_price), 4)
        doc = {
            "position_tag": position_tag,
            "symbol": symbol,
            "exchange": exchange,
            "segment": segment,
            "side": side,
            "qty": qty,
            "original_qty": qty,        # never mutated — used for partial exit sizing
            "sl_dist": _sl_dist,        # distance from entry to SL — used for R-multiple targets
            "entry_price": round(entry_price, 2),
            "sl_price": round(sl_price, 2),
            "target": round(target, 2),
            "atr": round(atr, 4),
            "strategy": strategy,
            "order_id": order_id,
            "status": "OPEN",
            "exit_price": 0.0,
            "exit_reason": "",
            "entry_ts": now_ist_str(),
            "exit_ts": "",
            "pnl": 0.0,
            "partial_pnl": 0.0,         # accumulated P&L from partial exits (not in final pnl yet)
            "partial_exit_1_done": False,
            "partial_exit_2_done": False,
            "regime": regime,
            "risk_mode": risk_mode,
            "signal_score": signal_score,
            "product": product,
            "wl_type": wl_type,
            "instrument_key": instrument_key,
            # Mode stickiness — record whether this position was opened in
            # paper or live. Once written, exit/GTT paths must honour the
            # position's recorded mode rather than the current runtime flag.
            # This prevents a paper→live flip mid-day from routing a fake
            # paper entry's exit through the real broker (and vice versa).
            "paper": bool(self.settings.runtime.paper_trade),
            # M0.6 MFE/MAE — updated live by ws_monitor on each tick. Stored
            # as R-multiples (favorable excursion = how far past entry we got
            # before reversing; adverse = worst drawdown before exit). Used by
            # M6 AttributionLog + backtest harness to score edge quality. Also
            # breakeven-fire diagnostics: breakeven_sl_fired=True + trigger_mfe
            # lets us confirm the FSM actually moved the stop where expected.
            "max_favorable_excursion_r": 0.0,
            "max_adverse_excursion_r": 0.0,
            "max_favorable_excursion_price": round(entry_price, 2),
            "max_adverse_excursion_price": round(entry_price, 2),
            "breakeven_sl_fired": False,
            "breakeven_sl_trigger_mfe_r": 0.0,
            "breakeven_sl_trigger_ts": "",
        }
        self.state.save_position(position_tag, doc)
        if self.pubsub:
            self.pubsub.publish_position_opened(doc)

    def _close_position_firestore(
        self,
        *,
        position_tag: str,
        exit_price: float,
        exit_reason: str,
    ) -> None:
        pos = self.state.get_position(position_tag)
        if not pos:
            logger.warning("close_position: tag not found in Firestore tag=%s", position_tag)
            return
        entry_price = float(pos.get("entry_price") or 0)
        qty = int(pos.get("qty") or 0)
        side = str(pos.get("side") or "BUY").upper()
        multiplier = 1 if side == "BUY" else -1
        pnl = round((exit_price - entry_price) * qty * multiplier, 2)
        # P0-1 (2026-04-22): subtract real brokerage + taxes to report NET P&L.
        # Prior behavior wrote gross pnl to BQ, masking ~0.10–0.20% cost drag
        # per trade. That cost eats the edge on small positions — trades must
        # clear brokerage + STT + GST + exchange before they're profitable.
        brokerage = calc_round_trip_brokerage(qty, entry_price, exit_price) if qty > 0 else 0.0
        # Add per-leg partial-exit brokerage if any partials happened.
        partial_brk = float(pos.get("partial_brokerage", 0.0) or 0.0)
        brokerage = round(brokerage + partial_brk, 2)
        partial_pnl = float(pos.get("partial_pnl", 0.0) or 0.0)
        gross_pnl = round(pnl + partial_pnl, 2)
        net_pnl = round(gross_pnl - brokerage, 2)
        exit_ts = now_ist_str()
        # Compute real hold duration from entry_ts → exit_ts (both IST strings).
        entry_ts_str = str(pos.get("entry_ts") or "")
        hold_minutes = 0
        try:
            entry_dt = parse_any_ts(entry_ts_str)
            exit_dt = parse_any_ts(exit_ts)
            if entry_dt and exit_dt:
                hold_minutes = max(0, int((exit_dt - entry_dt).total_seconds() // 60))
        except Exception:
            hold_minutes = 0
        self.state.update_position(
            position_tag,
            {
                "status": "CLOSED",
                "exit_price": round(exit_price, 2),
                "exit_reason": exit_reason,
                "exit_ts": exit_ts,
                "pnl": gross_pnl,
                "brokerage": brokerage,
                "net_pnl": net_pnl,
                "hold_minutes": hold_minutes,
            },
        )
        # M0.5 paper GTT cleanup — remove the paper_gtts row on close so the
        # ws_monitor reconciler doesn't keep polling a closed position.
        try:
            self.state.delete_paper_gtt(position_tag)
        except Exception:
            logger.debug("paper_gtt_delete_failed_on_close tag=%s", position_tag, exc_info=True)
        # Publish position_closed event
        if self.pubsub:
            closed_doc = {**(self.state.get_position(position_tag) or {}), "exit_price": round(exit_price, 2), "exit_reason": exit_reason}
            self.pubsub.publish_position_closed(closed_doc)
        # Write completed trade to BigQuery — retry up to 3 times to reduce data gaps.
        # `pnl` kept for backward compatibility with existing dashboards/queries; it
        # now equals gross P&L including partial exits. Use `net_pnl` for the real
        # post-cost number going forward.
        trade_row = {
            "trade_date": today_ist(),
            "position_tag": position_tag,
            "symbol": str(pos.get("symbol") or ""),
            "side": side,
            "qty": qty,
            "entry_price": entry_price,
            "exit_price": round(exit_price, 2),
            "sl_price": float(pos.get("sl_price") or 0),
            "target": float(pos.get("target") or 0),
            "pnl": gross_pnl,
            "brokerage": brokerage,
            "net_pnl": net_pnl,
            "pnl_pct": round(gross_pnl / (entry_price * qty) * 100, 4) if entry_price and qty else 0.0,
            "net_pnl_pct": round(net_pnl / (entry_price * qty) * 100, 4) if entry_price and qty else 0.0,
            "exit_reason": exit_reason,
            "strategy": str(pos.get("strategy") or ""),
            "entry_ts": str(pos.get("entry_ts") or ""),
            "exit_ts": exit_ts,
            "hold_minutes": hold_minutes,
            "regime": str(pos.get("regime") or ""),
            "risk_mode": str(pos.get("risk_mode") or ""),
            "market_confidence": 0.0,
            "signal_score": int(pos.get("signal_score") or 0),
        }
        _bq_insert_with_retry(self.bq, trade_row, position_tag)

    # ------------------------------------------------------------------ #
    # GTT SL management — CNC/delivery positions only
    # ------------------------------------------------------------------ #

    def _place_gtt_sl(
        self,
        *,
        position_tag: str,
        instrument_key: str,
        side: str,
        qty: int,
        sl_price: float,
    ) -> str | None:
        """Place a GTT SL order after a swing/CNC entry fills. Returns gtt_id or None.

        M0.4 (paper path): paper swings now write a Firestore paper_gtts row
        backed by ws_monitor polling, so paper has the same SL-order semantics
        as live (was: no SL at all; paper relied on ws_monitor per-tick price
        compare with no fail-over if monitor crashed).
        """
        if not instrument_key:
            return None
        # Mode stickiness: look up the position's recorded mode first so a
        # runtime flip doesn't switch the SL routing mid-flight. New entries
        # don't have a row yet — fall back to runtime for those.
        pos = self.state.get_position(position_tag) or {}
        _pos_paper = bool(pos.get("paper", self.settings.runtime.paper_trade))
        if _pos_paper:
            # M0.5 paper GTT — write a Firestore doc polled by ws_monitor.
            try:
                self.state.save_paper_gtt(position_tag, {
                    "instrument_key": instrument_key,
                    "symbol": str(pos.get("symbol") or ""),
                    "side": side.upper(),
                    "qty": int(qty),
                    "trigger_price": float(sl_price),
                    "exit_side": "SELL" if side.upper() == "BUY" else "BUY",
                    "created_at": now_ist_str(),
                    "source": "entry",
                })
                logger.info("paper_gtt_saved tag=%s sl=%.2f", position_tag, sl_price)
                return f"paper:{position_tag}"
            except Exception:
                logger.exception("paper_gtt_save_failed tag=%s", position_tag)
                return None
        exit_side = "SELL" if side.upper() == "BUY" else "BUY"
        return self._place_live_gtt_with_retries(
            position_tag=position_tag,
            instrument_key=instrument_key,
            exit_side=exit_side,
            qty=qty,
            sl_price=sl_price,
        )

    def _place_live_gtt_with_retries(
        self,
        *,
        position_tag: str,
        instrument_key: str,
        exit_side: str,
        qty: int,
        sl_price: float,
        max_attempts: int = 3,
    ) -> str | None:
        """Place live GTT SL with bounded retries. Returns gtt_id or None on all failures."""
        import time as _time
        last_exc: Exception | None = None
        for _attempt in range(max_attempts):
            try:
                resp = self.upstox.place_gtt_order(
                    instrument_token=instrument_key,
                    transaction_type=exit_side,
                    quantity=qty,
                    trigger_price=sl_price,
                    tag=f"sl_{position_tag[-8:]}",
                )
                gtt_id = str(
                    resp.get("id") or (resp.get("data") or {}).get("id") or ""
                ).strip()
                if gtt_id:
                    self.state.update_position(position_tag, {"gtt_sl_id": gtt_id})
                    logger.info(
                        "gtt_sl_placed tag=%s gtt_id=%s sl=%.2f attempt=%d",
                        position_tag, gtt_id, sl_price, _attempt + 1,
                    )
                    return gtt_id
                logger.warning(
                    "gtt_sl_empty_response tag=%s attempt=%d resp=%s",
                    position_tag, _attempt + 1, resp,
                )
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "gtt_sl_place_attempt_failed tag=%s sl=%.2f attempt=%d",
                    position_tag, sl_price, _attempt + 1, exc_info=True,
                )
            if _attempt < max_attempts - 1:
                _time.sleep(0.5 * (2 ** _attempt))  # 0.5, 1.0s backoff
        logger.error(
            "gtt_sl_place_failed_all_attempts tag=%s sl=%.2f last_exc=%s",
            position_tag, sl_price, last_exc,
        )
        return None

    def _cancel_gtt_sl(self, position_tag: str) -> None:
        """Cancel the GTT SL order stored on a position (if any). Idempotent."""
        pos = self.state.get_position(position_tag)
        if not pos:
            return
        # Mode stickiness: honour the position's recorded mode.
        _pos_paper = bool(pos.get("paper", self.settings.runtime.paper_trade))
        if _pos_paper:
            # Paper: clean up the paper_gtts Firestore row.
            try:
                self.state.delete_paper_gtt(position_tag)
            except Exception:
                logger.debug(
                    "paper_gtt_cancel_failed tag=%s", position_tag, exc_info=True,
                )
            return
        gtt_id = str(pos.get("gtt_sl_id") or "").strip()
        if not gtt_id:
            return
        try:
            self.upstox.delete_gtt_order(gtt_id)
            self.state.update_position(position_tag, {"gtt_sl_id": ""})
            logger.info("gtt_sl_cancelled tag=%s gtt_id=%s", position_tag, gtt_id)
        except Exception:
            logger.warning("gtt_sl_cancel_failed tag=%s gtt_id=%s", position_tag, gtt_id, exc_info=True)

    def refresh_swing_gtt_sl(
        self,
        *,
        position_tag: str,
        instrument_key: str,
        new_sl_price: float,
    ) -> str | None:
        """Cancel existing GTT SL and place a new one at the updated SL price.

        Called by SwingReconciliationService when trailing SL ratchets up.
        Returns new gtt_id or None on error. In paper mode this rewrites the
        paper_gtts Firestore row so ws_monitor's reconciler picks up the new
        trigger on its next 60s poll.
        """
        self._cancel_gtt_sl(position_tag)
        pos = self.state.get_position(position_tag)
        if not pos:
            return None
        return self._place_gtt_sl(
            position_tag=position_tag,
            instrument_key=instrument_key,
            side=str(pos.get("side") or "BUY"),
            qty=int(pos.get("qty") or 0),
            sl_price=new_sl_price,
        )

    # ------------------------------------------------------------------ #
    # Entry order
    # ------------------------------------------------------------------ #

    def place_entry_order(
        self,
        *,
        symbol: str,
        exchange: str,
        segment: str,
        side: str,
        qty: int,
        entry_price: float,
        sl_price: float,
        target: float,
        atr: float,
        product: str,
        score: int,
        reason: str,
        instrument_key: str = "",
        strategy: str = "",
        regime: str = "",
        risk_mode: str = "",
        allow_live_orders: bool = False,
        wl_type: str = "intraday",
    ) -> dict[str, Any] | None:
        if self.state.already_fired_today(symbol, side):
            return {"skipped": "duplicate_idempotency"}

        # Critical safety guard: never place an order without a valid stop-loss.
        # sl_price=0 means position sizing failed — passing this to the broker creates
        # a position with no stop which can cause unlimited loss.
        if sl_price <= 0:
            logger.error(
                "place_entry_order blocked: sl_price=%.2f symbol=%s side=%s qty=%d — refusing to place order without valid SL",
                sl_price, symbol, side, qty,
            )
            return {"error": "sl_price_zero_or_negative", "symbol": symbol, "sl_price": sl_price}
        if qty <= 0:
            logger.error("place_entry_order blocked: qty=%d symbol=%s — refusing to place zero-qty order", qty, symbol)
            return {"error": "qty_zero_or_negative", "symbol": symbol, "qty": qty}

        # 2026-04-21 post-mortem: SL/target side validation.
        # Bug from 04-20/04-21: 6 BUY trades had SL placed ABOVE entry (e.g.,
        # FINCABLES entry 940.4 SL 941.18; MAHABANK entry 79.6 SL 79.7). The
        # order_service.place_bracket_order downstream uses abs() on SL/target
        # distances, silently masking this bug — the broker then snapped the
        # bracket to the other side at the wrong magnitude. Fail loud here so
        # the sign error surfaces in audit logs instead of as a mystery loss.
        _side_u = side.upper()
        if _side_u == "BUY":
            if sl_price >= entry_price:
                logger.error(
                    "place_entry_order blocked: BUY with SL >= entry (%.4f >= %.4f) symbol=%s — inverted stop",
                    sl_price, entry_price, symbol,
                )
                return {"error": "sl_on_wrong_side", "symbol": symbol, "side": side, "entry": entry_price, "sl": sl_price}
            if target <= entry_price:
                logger.error(
                    "place_entry_order blocked: BUY with target <= entry (%.4f <= %.4f) symbol=%s — inverted target",
                    target, entry_price, symbol,
                )
                return {"error": "target_on_wrong_side", "symbol": symbol, "side": side, "entry": entry_price, "target": target}
        elif _side_u == "SELL":
            if sl_price <= entry_price:
                logger.error(
                    "place_entry_order blocked: SELL with SL <= entry (%.4f <= %.4f) symbol=%s — inverted stop",
                    sl_price, entry_price, symbol,
                )
                return {"error": "sl_on_wrong_side", "symbol": symbol, "side": side, "entry": entry_price, "sl": sl_price}
            if target >= entry_price:
                logger.error(
                    "place_entry_order blocked: SELL with target >= entry (%.4f >= %.4f) symbol=%s — inverted target",
                    target, entry_price, symbol,
                )
                return {"error": "target_on_wrong_side", "symbol": symbol, "side": side, "entry": entry_price, "target": target}

        # Minimum SL distance floor: 0.8% of entry. Stops tighter than this on
        # Indian mid/small-caps sit inside single-candle noise. 04-16 had 11/15
        # SELL trades with sub-1% SLs — all lost. 0.8% is a soft floor; if ATR
        # math produces tighter, widen to the floor and continue (don't block).
        _min_sl_dist_pct = 0.008
        _sl_dist_pct = abs(entry_price - sl_price) / entry_price if entry_price > 0 else 0.0
        if _sl_dist_pct < _min_sl_dist_pct and entry_price > 0:
            _old_sl = sl_price
            _buffer = entry_price * _min_sl_dist_pct
            sl_price = round(entry_price - _buffer, 2) if _side_u == "BUY" else round(entry_price + _buffer, 2)
            # Proportionally widen target to preserve planned R:R
            _old_tgt_dist = abs(target - entry_price)
            _old_sl_dist = abs(entry_price - _old_sl)
            if _old_sl_dist > 0:
                _rr = _old_tgt_dist / _old_sl_dist
                _new_tgt_dist = _buffer * _rr
                target = round(entry_price + _new_tgt_dist, 2) if _side_u == "BUY" else round(entry_price - _new_tgt_dist, 2)
            logger.warning(
                "place_entry_order sl_floor_applied symbol=%s side=%s entry=%.4f old_sl=%.4f new_sl=%.4f new_target=%.4f dist_pct=%.4f",
                symbol, side, entry_price, _old_sl, sl_price, target, _sl_dist_pct,
            )

        if not instrument_key:
            # Log prominently — ws-monitor needs a valid Upstox instrument_key
            # to subscribe on WebSocket; without it no intraday SL/target exits fire.
            logger.warning(
                "place_entry_order: instrument_key missing for symbol=%s — "
                "ws-monitor will attempt universe fallback; check universe coverage",
                symbol,
            )

        ref_id = make_ref_id()
        paper = self.settings.runtime.paper_trade or not allow_live_orders

        # Log to Sheets (non-blocking)
        self._append_order_log_sheets([
            now_ist_str(),
            ref_id if paper else "",
            symbol, exchange, segment,
            side, qty, "BRACKET" if not paper else "PAPER",
            round(entry_price, 2), round(sl_price, 2), round(target, 2),
            "PAPER" if paper else "SENT",
            "", "",
        ])

        # ---- Paper trade ----
        if paper:
            pos_tag = f"BOTP:{ref_id}"
            # Batch 7 (2026-04-23): apply entry-side paper slippage so paper
            # P&L tracks live P&L. MARKET entries fill through the spread in
            # the adverse direction (BUY fills above LTP, SELL fills below).
            # SL and target are left at their planned prices — SL slippage is
            # applied at exit time, target is a LIMIT order so no slippage.
            _entry_slip_pct = float(self.settings.strategy.paper_entry_slippage_pct or 0.0)
            if _entry_slip_pct > 0:
                if side.upper() == "BUY":
                    entry_price = round(entry_price * (1.0 + _entry_slip_pct), 2)
                else:
                    entry_price = round(entry_price * (1.0 - _entry_slip_pct), 2)
            self._save_position_firestore(
                position_tag=pos_tag,
                symbol=symbol, exchange=exchange, segment=segment,
                side=side, qty=qty, entry_price=entry_price,
                sl_price=sl_price, target=target, atr=atr,
                strategy=strategy, order_id=ref_id,
                regime=regime, risk_mode=risk_mode, signal_score=score,
                product=product, wl_type=wl_type,
                instrument_key=instrument_key,
            )
            # Keep Sheets copy for human visibility
            self._append_position_sheets([
                now_ist_str(), symbol, exchange, segment, side,
                round(entry_price, 2), qty,
                round(abs(entry_price - sl_price), 2),
                round(target, 2), round(sl_price, 2),
                round(entry_price, 2), 0.0, round(atr, 4),
                "OPEN", pos_tag, "",
            ])
            self.state.mark_fired_today(symbol, side)
            # M0.5 paper GTT — write a paper_gtts row for every paper entry
            # (intraday + swing) so ws_monitor's 60s reconciler can fire the
            # stop if the tick stream stalls. Swing paper previously had no
            # SL backstop at all — a ws_monitor crash meant unbounded loss
            # on open paper swings until manual intervention.
            self._place_gtt_sl(
                position_tag=pos_tag,
                instrument_key=instrument_key,
                side=side,
                qty=qty,
                sl_price=sl_price,
            )
            logger.info(
                "paper_order symbol=%s side=%s qty=%d entry=%.2f sl=%.2f target=%.2f tag=%s wl_type=%s",
                symbol, side, qty, entry_price, sl_price, target, pos_tag, wl_type,
            )
            return {"paper": True, "order_id": ref_id, "position_tag": pos_tag}

        # ---- Live order ----
        token = instrument_key or symbol
        is_swing = str(product).upper() in {"CNC", "D", "DELIVERY"}
        try:
            if is_swing:
                # Swing/CNC: regular MARKET order (bracket not supported for delivery)
                resp = self.upstox.place_order({
                    "quantity": qty,
                    "product": "D",
                    "validity": "DAY",
                    "price": 0,
                    "tag": ref_id,
                    "instrument_token": token,
                    "order_type": "MARKET",
                    "transaction_type": side.upper(),
                    "disclosed_quantity": 0,
                    "trigger_price": 0,
                    "is_amo": False,
                })
            else:
                resp = self.upstox.place_bracket_order(
                    instrument_token=token,
                    transaction_type=side.upper(),
                    quantity=qty,
                    stop_loss=abs(entry_price - sl_price),
                    square_off=abs(target - entry_price),
                    order_reference_id=ref_id,
                )
        except Exception as exc:
            logger.exception("live_order_failed symbol=%s product=%s", symbol, product)
            return {"error": str(exc), "status": "API_FAIL"}

        order_id = str(
            resp.get("order_id") or resp.get("upstox_order_id") or resp.get("data", {}).get("order_id") or ref_id
        )

        probe = self._await_fill(order_id, ref_id, qty)
        if probe.get("filled"):
            snap = probe.get("snapshot") or {}
            fill_price = float(snap.get("avg_fill_price") or entry_price)
            pos_tag = f"BOT:{order_id}:{ref_id}"
            self._save_position_firestore(
                position_tag=pos_tag,
                symbol=symbol, exchange=exchange, segment=segment,
                side=side, qty=qty, entry_price=fill_price,
                sl_price=sl_price, target=target, atr=atr,
                strategy=strategy, order_id=order_id,
                regime=regime, risk_mode=risk_mode, signal_score=score,
                product=product, wl_type=wl_type,
                instrument_key=token,
            )
            self._append_position_sheets([
                now_ist_str(), symbol, exchange, segment, side,
                round(fill_price, 2), qty,
                round(abs(fill_price - sl_price), 2),
                round(target, 2), round(sl_price, 2),
                round(fill_price, 2), 0.0, round(atr, 4),
                "OPEN", pos_tag, "",
            ])
            # Place GTT SL for CNC/delivery positions — broker-level SL protection.
            # Live-only path: paper swings already got a paper_gtts row above.
            if is_swing:
                _gtt_id = self._place_gtt_sl(
                    position_tag=pos_tag,
                    instrument_key=token,
                    side=side,
                    qty=qty,
                    sl_price=sl_price,
                )
                # M0.4 assertion: a missing GTT after all retries means the
                # position has no broker-level SL. Flag needs_manual_gtt=True
                # so the premarket reconcile + dashboard surface it, and
                # trigger an immediate market exit since holding overnight
                # without an SL is strictly worse than paying the round-trip.
                if not _gtt_id:
                    logger.critical(
                        "live_swing_no_gtt_sl_emergency_exit tag=%s sl=%.2f",
                        pos_tag, sl_price,
                    )
                    self.state.update_position(pos_tag, {
                        "needs_manual_gtt": True,
                        "gtt_place_failed_at": now_ist_str(),
                    })
                    try:
                        self.place_exit_order(
                            position_tag=pos_tag,
                            instrument_key=token,
                            exit_reason="EMERGENCY_NO_GTT",
                        )
                    except Exception:
                        logger.exception(
                            "emergency_exit_after_gtt_fail_failed tag=%s", pos_tag,
                        )
            self.state.mark_fired_today(symbol, side)
            # Save order record to Firestore
            self.state.save_order(ref_id, {
                "ref_id": ref_id,
                "symbol": symbol, "side": side, "qty": qty,
                "order_type": "BRACKET", "entry_price": fill_price,
                "sl_price": sl_price, "target": target,
                "status": "FILLED", "order_id": order_id,
                "paper": False, "sent_at": now_ist_str(),
            })
            return {"order_id": order_id, "order_status": "FILLED", "fill_price": fill_price, "position_tag": pos_tag}

        if probe.get("terminal"):
            self.state.clear_fired_today(symbol, side)
            return {"order_id": order_id, "order_status": "TERMINAL_NONFILL"}

        # Still pending — save for reconcile
        self.state.save_pending_order(
            ref_id,
            {
                "kind": "entry",
                "order_id": order_id,
                "ref_id": ref_id,
                "symbol": symbol,
                "exchange": exchange,
                "segment": segment,
                "side": side,
                "qty": qty,
                "entry_price": entry_price,
                "sl_price": sl_price,
                "target": target,
                "atr": atr,
                "product": product,
                "score": score,
                "reason": reason,
                "strategy": strategy,
                "regime": regime,
                "risk_mode": risk_mode,
                "instrument_key": token,
                "day": today_ist(),
            },
            kind="entry",
        )
        self.state.mark_fired_today(symbol, side)
        return {"order_id": order_id, "order_status": "PENDING_RECON"}

    # ------------------------------------------------------------------ #
    # Exit order (called by WebSocket monitor on SL/target hit)
    # ------------------------------------------------------------------ #

    def place_exit_order(
        self,
        *,
        position_tag: str,
        instrument_key: str,
        exit_reason: str = "MANUAL",
        is_amo: bool = False,
    ) -> dict[str, Any]:
        pos = self.state.get_position(position_tag)
        if not pos:
            return {"error": "position_not_found", "tag": position_tag}
        if str(pos.get("status", "")) != "OPEN":
            return {"skipped": "already_closed", "tag": position_tag}

        symbol = str(pos.get("symbol") or "")
        side = str(pos.get("side") or "BUY").upper()
        qty = int(pos.get("qty") or 0)
        exit_side = "SELL" if side == "BUY" else "BUY"
        # Mode stickiness: use the position's recorded mode, falling back to
        # the runtime flag for legacy positions that predate the field.
        paper = bool(pos.get("paper", self.settings.runtime.paper_trade))
        if paper != bool(self.settings.runtime.paper_trade):
            logger.warning(
                "exit_mode_override tag=%s pos_paper=%s runtime_paper=%s — "
                "honouring position's recorded mode",
                position_tag, paper, self.settings.runtime.paper_trade,
            )

        if paper:
            # Paper exit: use current LTP as proxy
            try:
                quote = self.upstox.get_quote(instrument_key)
                exit_price = quote.ltp or float(pos.get("entry_price") or 0)
            except Exception:
                exit_price = float(pos.get("entry_price") or 0)
            # Batch 7 (2026-04-23): apply SL-side paper slippage. SL_HIT /
            # EOD_CLOSE / FLAT_TIMEOUT / regime-tighten exits all use market
            # orders, which fill through the L2 book when multiple traders
            # hit the same level. TARGET_HIT is a LIMIT order — no slippage.
            # PARTIAL_* exits are also market-tagged, so the same slippage
            # applies; we conservatively shift them too.
            _reason_upper = str(exit_reason or "").upper()
            _is_limit_exit = "TARGET" in _reason_upper
            _sl_slip_pct = float(self.settings.strategy.paper_sl_slippage_pct or 0.0)
            if _sl_slip_pct > 0 and not _is_limit_exit and exit_price > 0:
                # Exit side is opposite of entry side: BUY entry exits via SELL
                # (adverse fill = lower), SELL entry exits via BUY (adverse = higher).
                if side == "BUY":
                    exit_price = round(exit_price * (1.0 - _sl_slip_pct), 2)
                else:
                    exit_price = round(exit_price * (1.0 + _sl_slip_pct), 2)
            self._close_position_firestore(
                position_tag=position_tag,
                exit_price=exit_price,
                exit_reason=exit_reason,
            )
            logger.info("paper_exit tag=%s reason=%s exit_price=%.2f", position_tag, exit_reason, exit_price)
            return {"paper": True, "exit_price": exit_price, "exit_reason": exit_reason}

        # Live exit: MARKET order
        ref_id = make_ref_id()
        # Use delivery product for swing/CNC positions, intraday for MIS
        _pos_wl_type = str(pos.get("wl_type") or "intraday").strip().lower()
        _exit_product = "D" if _pos_wl_type == "swing" else "I"
        # Cancel GTT SL before placing market exit to avoid double-fill
        if _pos_wl_type == "swing":
            self._cancel_gtt_sl(position_tag)
        try:
            resp = self.upstox.place_order({
                "quantity": qty,
                "product": _exit_product,
                "validity": "DAY",
                "price": 0,
                "tag": ref_id,
                "instrument_token": instrument_key,
                "order_type": "MARKET",
                "transaction_type": exit_side,
                "disclosed_quantity": 0,
                "trigger_price": 0,
                "is_amo": is_amo,
            })
        except Exception as exc:
            logger.exception("exit_order_failed tag=%s", position_tag)
            return {"error": str(exc), "tag": position_tag}

        order_id = str(resp.get("order_id") or resp.get("data", {}).get("order_id") or ref_id)

        if is_amo:
            # AMO queued — will execute at market open; mark position PENDING_AMO_EXIT so
            # the EOD reconcile loop or premarket flow won't double-exit it.
            self.state.update_position(position_tag, {
                "exit_reason": exit_reason,
                "amo_exit_order_id": order_id,
                "status": "PENDING_AMO_EXIT",
            })
            logger.info("amo_exit_queued tag=%s order_id=%s reason=%s", position_tag, order_id, exit_reason)
            return {"order_id": order_id, "exit_reason": exit_reason, "amo": True}

        probe = self._await_fill(order_id, ref_id, qty, timeout_ms=10_000)
        fill_price = 0.0
        if probe.get("filled"):
            snap = probe.get("snapshot") or {}
            fill_price = float(snap.get("avg_fill_price") or 0)
        else:
            # M0.3 exit-price fallback chain. The broker fill probe timed out;
            # we need a best-effort exit price to book P&L. Old code used one
            # get_quote + entry_price fallback, which silently booked ₹0 P&L
            # trades whenever the quote endpoint hiccupped at exit time.
            # New chain:
            #   1) Retry get_quote up to 3x with 500ms backoff (covers
            #      transient 429s and sub-second network blips).
            #   2) Fall back to the ws_monitor's last-known tick stored in
            #      Firestore collection `ws_last_tick/{instrument_key}`.
            #   3) Give up: mark position EXIT_FAILED with reason
            #      `<reason>_EXIT_PRICE_UNKNOWN` and leave status=OPEN so the
            #      next scan cycle retries. Never fall back to entry_price —
            #      writing a ₹0 P&L row corrupts daily-PnL aggregation and
            #      silently hides the loss.
            import time as _time
            for _attempt in range(3):
                try:
                    fill_price = float(self.upstox.get_quote(instrument_key).ltp or 0)
                    if fill_price > 0:
                        break
                except Exception:
                    logger.warning(
                        "live_exit_quote_retry tag=%s ik=%s attempt=%d",
                        position_tag, instrument_key, _attempt + 1,
                    )
                if _attempt < 2:
                    _time.sleep(0.5)
            if fill_price <= 0:
                # Fallback 2: ws_monitor last-tick cache
                try:
                    _tick = self.state.get_json("ws_last_tick", instrument_key) or {}
                    _ws_ltp = float(_tick.get("ltp") or 0)
                    if _ws_ltp > 0:
                        fill_price = _ws_ltp
                        logger.info(
                            "live_exit_price_from_ws_tick tag=%s ltp=%.2f age_s=%s",
                            position_tag, fill_price, _tick.get("age_s", "?"),
                        )
                except Exception:
                    logger.debug("ws_last_tick_fetch_failed", exc_info=True)

        if fill_price <= 0:
            # M0.3: abort-and-retry rather than write a corrupt ₹0 P&L row.
            # The position stays OPEN; exit_reason is stamped with
            # EXIT_PRICE_UNKNOWN so the next scan can retry. An operator alert
            # is raised via LogSink so ws_monitor or a human can intervene.
            logger.error(
                "live_exit_price_unknown tag=%s order_id=%s — aborting close, position stays OPEN",
                position_tag, order_id,
            )
            self.state.update_position(position_tag, {
                "last_exit_attempt_order_id": order_id,
                "last_exit_attempt_reason": exit_reason,
                "last_exit_attempt_ts": now_ist_str(),
                "exit_price_unknown_count": int(pos.get("exit_price_unknown_count", 0)) + 1,
            })
            return {
                "error": "exit_price_unknown",
                "order_id": order_id,
                "tag": position_tag,
                "exit_reason": f"{exit_reason}_EXIT_PRICE_UNKNOWN",
            }

        self._close_position_firestore(
            position_tag=position_tag,
            exit_price=fill_price,
            exit_reason=exit_reason,
        )
        return {
            "order_id": order_id,
            "exit_price": fill_price,
            "exit_reason": exit_reason,
            "filled": probe.get("filled", False),
        }

    def place_partial_exit_order(
        self,
        *,
        position_tag: str,
        instrument_key: str,
        exit_qty: int,
        exit_reason: str = "PARTIAL_TARGET",
    ) -> dict[str, Any]:
        """Exit a fraction of the position, reducing qty in Firestore but keeping status OPEN.

        Called by ws_monitor at Stage 1 (1:1 R:R, 40%) and Stage 2 (1.5:1 R:R, 30%).
        The final remaining qty is closed by the normal `place_exit_order` path.

        For paper trades: uses live LTP as exit price, records partial P&L.
        For live trades: places a MARKET order for `exit_qty` shares. The bracket SL/target
        still covers the full original qty at broker level — on the next SL/target hit,
        only `qty_remaining` will be booked (the rest already exited). This works correctly
        because the live position's net qty at broker matches Firestore after partial fills.
        """
        pos = self.state.get_position(position_tag)
        if not pos:
            return {"error": "position_not_found", "tag": position_tag}
        if str(pos.get("status", "")) != "OPEN":
            return {"skipped": "already_closed", "tag": position_tag}

        symbol = str(pos.get("symbol") or "")
        side = str(pos.get("side") or "BUY").upper()
        current_qty = int(pos.get("qty") or 0)
        exit_qty = max(1, min(exit_qty, current_qty - 1))  # always leave at least 1 share
        remaining_qty = current_qty - exit_qty
        exit_side = "SELL" if side == "BUY" else "BUY"
        entry_price = float(pos.get("entry_price") or 0)
        # Mode stickiness: honour the position's recorded mode.
        paper = bool(pos.get("paper", self.settings.runtime.paper_trade))

        if paper:
            try:
                quote = self.upstox.get_quote(instrument_key)
                exit_price = quote.ltp or entry_price
            except Exception:
                exit_price = entry_price
            # Batch 7 (2026-04-23): partial exits are market-tagged and fill
            # through the book just like full SL exits — apply SL-slippage. The
            # PARTIAL_1R / PARTIAL_1_5R / PARTIAL_1R_QTY2 reasons don't contain
            # "TARGET" so they're not accidentally treated as limit fills.
            _sl_slip_pct = float(self.settings.strategy.paper_sl_slippage_pct or 0.0)
            if _sl_slip_pct > 0 and exit_price > 0:
                if side == "BUY":
                    exit_price = round(exit_price * (1.0 - _sl_slip_pct), 2)
                else:
                    exit_price = round(exit_price * (1.0 + _sl_slip_pct), 2)
            multiplier = 1 if side == "BUY" else -1
            partial_pnl = round((exit_price - entry_price) * exit_qty * multiplier, 2)
            new_partial_pnl = round(float(pos.get("partial_pnl", 0)) + partial_pnl, 2)
            # Track partial-exit brokerage so final close can include it in net_pnl.
            # Each partial exit has its own entry+exit legs at the partial qty.
            partial_brk = calc_round_trip_brokerage(exit_qty, entry_price, exit_price)
            new_partial_brk = round(float(pos.get("partial_brokerage", 0)) + partial_brk, 2)
            self.state.update_position(position_tag, {
                "qty": remaining_qty,
                "partial_pnl": new_partial_pnl,
                "partial_brokerage": new_partial_brk,
                f"{exit_reason.lower()}_exit_price": round(exit_price, 2),
                f"{exit_reason.lower()}_exit_qty": exit_qty,
            })
            logger.info(
                "paper_partial_exit tag=%s reason=%s exit_qty=%d remaining=%d price=%.2f pnl=%.2f brk=%.2f",
                position_tag, exit_reason, exit_qty, remaining_qty, exit_price, partial_pnl, partial_brk,
            )
            return {
                "paper": True, "partial": True,
                "exit_price": exit_price, "exit_qty": exit_qty,
                "remaining_qty": remaining_qty, "partial_pnl": partial_pnl,
            }

        # Live: place MARKET order for exit_qty, then update Firestore qty
        ref_id = make_ref_id()
        _pos_wl_type = str(pos.get("wl_type") or "intraday").strip().lower()
        _exit_product = "D" if _pos_wl_type == "swing" else "I"
        try:
            resp = self.upstox.place_order({
                "quantity": exit_qty,
                "product": _exit_product,
                "validity": "DAY",
                "price": 0,
                "tag": ref_id,
                "instrument_token": instrument_key,
                "order_type": "MARKET",
                "transaction_type": exit_side,
                "disclosed_quantity": 0,
                "trigger_price": 0,
                "is_amo": False,
            })
        except Exception as exc:
            logger.exception("partial_exit_order_failed tag=%s", position_tag)
            return {"error": str(exc), "tag": position_tag}

        order_id = str(resp.get("order_id") or resp.get("data", {}).get("order_id") or ref_id)
        probe = self._await_fill(order_id, ref_id, exit_qty, timeout_ms=8_000)
        fill_price = float((probe.get("snapshot") or {}).get("avg_fill_price") or 0)
        if fill_price <= 0:
            try:
                fill_price = float(self.upstox.get_quote(instrument_key).ltp or entry_price)
            except Exception:
                fill_price = entry_price

        multiplier = 1 if side == "BUY" else -1
        partial_pnl = round((fill_price - entry_price) * exit_qty * multiplier, 2)
        new_partial_pnl = round(float(pos.get("partial_pnl", 0)) + partial_pnl, 2)
        partial_brk = calc_round_trip_brokerage(exit_qty, entry_price, fill_price)
        new_partial_brk = round(float(pos.get("partial_brokerage", 0)) + partial_brk, 2)
        self.state.update_position(position_tag, {
            "qty": remaining_qty,
            "partial_pnl": new_partial_pnl,
            "partial_brokerage": new_partial_brk,
            f"{exit_reason.lower()}_exit_price": round(fill_price, 2),
            f"{exit_reason.lower()}_exit_qty": exit_qty,
        })
        logger.info(
            "live_partial_exit tag=%s reason=%s exit_qty=%d remaining=%d fill=%.2f pnl=%.2f brk=%.2f",
            position_tag, exit_reason, exit_qty, remaining_qty, fill_price, partial_pnl, partial_brk,
        )
        return {
            "order_id": order_id, "partial": True,
            "exit_price": fill_price, "exit_qty": exit_qty,
            "remaining_qty": remaining_qty, "partial_pnl": partial_pnl,
            "filled": probe.get("filled", False),
        }

    # ------------------------------------------------------------------ #
    # Reconcile pending entry orders
    # ------------------------------------------------------------------ #

    def reconcile_pending_entries(self, max_items: int = 15) -> dict[str, int | bool]:
        if self.settings.runtime.paper_trade:
            return {"processed": 0, "pending": 0, "filled": 0, "failed": 0, "skippedPaper": True}
        items = self.state.list_pending_orders("entry", limit=max_items)
        processed = pending = filled = failed = 0
        for item in items:
            processed += 1
            ref_id = str(item.get("ref_id") or "")
            order_id = str(item.get("order_id") or "")
            symbol = str(item.get("symbol") or "")
            side = str(item.get("side") or "BUY")
            snap = self._extract_order_snapshot(order_id, ref_id)
            if not snap:
                pending += 1
                continue
            status = _order_status(str(snap.get("status") or ""))
            if status == "FILLED":
                fill_price = float(snap.get("avg_fill_price") or item.get("entry_price") or 0)
                qty = int(float(item.get("qty") or 0))
                pos_tag = f"BOT:{order_id}:{ref_id}"
                self._save_position_firestore(
                    position_tag=pos_tag,
                    symbol=symbol,
                    exchange=str(item.get("exchange") or "NSE"),
                    segment=str(item.get("segment") or "CASH"),
                    side=side,
                    qty=qty,
                    entry_price=fill_price,
                    sl_price=float(item.get("sl_price") or 0),
                    target=float(item.get("target") or 0),
                    atr=float(item.get("atr") or 0),
                    strategy=str(item.get("strategy") or ""),
                    order_id=order_id,
                    regime=str(item.get("regime") or ""),
                    risk_mode=str(item.get("risk_mode") or ""),
                    signal_score=int(item.get("score") or 0),
                    instrument_key=str(item.get("instrument_key") or ""),
                    wl_type=str(item.get("wl_type") or "intraday"),
                )
                self._append_position_sheets([
                    now_ist_str(), symbol,
                    str(item.get("exchange") or "NSE"),
                    str(item.get("segment") or "CASH"),
                    side, round(fill_price, 2),
                    qty,
                    round(abs(fill_price - float(item.get("sl_price") or fill_price)), 2),
                    round(float(item.get("target") or 0), 2),
                    round(float(item.get("sl_price") or 0), 2),
                    round(fill_price, 2), 0.0,
                    round(float(item.get("atr") or 0), 4),
                    "OPEN", pos_tag, "",
                ])
                self.state.delete_pending_order(ref_id, kind="entry")
                filled += 1
            elif _is_final_non_fill(status):
                self.state.clear_fired_today(symbol, side)
                self.state.delete_pending_order(ref_id, kind="entry")
                failed += 1
            else:
                pending += 1
            if processed % 3 == 0:
                time.sleep(0.12)
        return {"processed": processed, "pending": pending, "filled": filled, "failed": failed}

    # ------------------------------------------------------------------ #
    # Reconcile open positions at EOD
    # ------------------------------------------------------------------ #

    def reconcile_open_positions(self, force_close: bool = False) -> dict[str, Any]:
        """Check all OPEN positions against Upstox order status and close settled ones.

        Called by the eod-position-reconcile scheduler job. Paper positions
        close at live LTP. If LTP is unavailable and force_close=False, skip
        the position and let the next pass try again. The final pass passes
        force_close=True so we don't leave positions open past the close.
        """
        open_positions = self.state.list_open_positions()
        checked = closed = remaining = 0
        errors = []

        for pos in open_positions:
            checked += 1
            tag = str(pos.get("position_tag") or pos.get("_id") or "")
            if not tag:
                continue
            # Skip swing/CNC positions — they persist overnight
            _pos_wl_type = str(pos.get("wl_type") or "intraday").strip().lower()
            if _pos_wl_type == "swing":
                remaining += 1
                logger.info("eod_skip_swing tag=%s symbol=%s", tag, pos.get("symbol", ""))
                continue
            # Skip positions with a queued AMO exit — will settle at market open
            if str(pos.get("status") or "") == "PENDING_AMO_EXIT":
                remaining += 1
                logger.info("eod_skip_amo_exit tag=%s symbol=%s", tag, pos.get("symbol", ""))
                continue
            order_id = str(pos.get("order_id") or "")
            symbol = str(pos.get("symbol") or "")
            paper = not order_id or str(order_id).startswith("AT-")

            if paper or self.settings.runtime.paper_trade:
                # Paper close at LTP — never silently fall back to entry_price
                # because that would always book ₹0 and corrupt P&L stats.
                # instrument_key is not stored on paper positions — look it up
                # from the universe collection so Upstox quote succeeds.
                instrument_key = str(pos.get("instrument_key") or "")
                if not instrument_key and symbol:
                    try:
                        uni_row = self.state.get_json("universe", symbol)
                        instrument_key = str(uni_row.get("instrument_key") or "") if uni_row else ""
                    except Exception:
                        pass
                instrument_key = instrument_key or symbol
                ltp = 0.0
                try:
                    if instrument_key:
                        ltp = float(self.upstox.get_quote(instrument_key).ltp or 0.0)
                except Exception:
                    logger.warning("eod_paper_quote_failed tag=%s symbol=%s ik=%s", tag, symbol, instrument_key, exc_info=True)
                    ltp = 0.0
                if ltp <= 0:
                    if not force_close:
                        # Skip — next recon pass (or final pass) will retry.
                        logger.warning("eod_paper_skip_no_ltp tag=%s symbol=%s — will retry on next pass", tag, symbol)
                        remaining += 1
                        continue
                    # Final pass: must close. Mark with distinct reason so it's
                    # excluded from real P&L stats downstream.
                    exit_price = float(pos.get("entry_price") or 0)
                    self._close_position_firestore(
                        position_tag=tag, exit_price=exit_price, exit_reason="EOD_CLOSE_NO_QUOTE"
                    )
                    closed += 1
                    continue
                self._close_position_firestore(
                    position_tag=tag, exit_price=ltp, exit_reason="EOD_CLOSE"
                )
                closed += 1
                continue

            # Live: check Upstox order status
            snap = self._extract_order_snapshot(order_id, "")
            if snap and _order_status(str(snap.get("status") or "")) == "FILLED":
                # Bracket order already closed (SL or target hit)
                fill_price = float(snap.get("avg_fill_price") or 0)
                sl_price = float(pos.get("sl_price") or 0)
                tgt = float(pos.get("target") or 0)
                entry = float(pos.get("entry_price") or 0)
                # Determine reason by proximity
                if sl_price and abs(fill_price - sl_price) < abs(fill_price - tgt):
                    reason = "SL_HIT"
                elif tgt and abs(fill_price - tgt) < abs(fill_price - sl_price):
                    reason = "TARGET_HIT"
                elif fill_price and entry and abs((fill_price - entry) / entry) < 0.001:
                    reason = "EOD_CLOSE"
                else:
                    reason = "CLOSED"
                self._close_position_firestore(
                    position_tag=tag, exit_price=fill_price, exit_reason=reason
                )
                closed += 1
            else:
                # Still open at EOD — force close with market exit
                instrument_key = str(pos.get("instrument_key") or symbol)
                try:
                    result = self.place_exit_order(
                        position_tag=tag,
                        instrument_key=instrument_key,
                        exit_reason="EOD_CLOSE",
                    )
                    if result.get("error"):
                        errors.append({"tag": tag, "error": result["error"]})
                        remaining += 1
                    else:
                        closed += 1
                except Exception as exc:
                    logger.exception("eod_exit_failed tag=%s", tag)
                    errors.append({"tag": tag, "error": str(exc)})
                    remaining += 1
            time.sleep(0.1)

        return {
            "checked": checked,
            "closed": closed,
            "remaining": remaining,
            "errors": errors[:5],
        }
