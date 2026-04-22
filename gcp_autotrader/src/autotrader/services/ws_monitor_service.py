"""WebSocket real-time exit monitor.

Deployed as a separate Cloud Run service (autotrader-ws-monitor) with
min-instances=1 so the container stays alive across days. Each trading
day runs a fresh ``WsMonitorService`` instance; between EOD and the next
market open the container idles inside ``_main`` (no exit, no reschedule
loop).

Lifecycle (per trading day):
  1. Load all OPEN positions from Firestore on startup + refresh every 15s.
  2. Build instrument-key → position-tag map.
  3. Subscribe to Upstox WebSocket for all symbols.
  4. On each tick:
     - ltp ≤ sl_price  → SL_HIT exit
     - ltp ≥ target    → TARGET_HIT exit
     - time ≥ 15:25    → EOD_CLOSE exit (force-close remaining)
  5. On disconnect: reconnect with exponential back-off.
  6. At 15:30: close WebSocket, end the day. Outer loop sleeps until the
     next weekday 09:00 IST and starts a fresh service instance.

Run with::

    python -m autotrader.services.ws_monitor_service
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# IST offset seconds
_IST_OFFSET = 5 * 3600 + 30 * 60

_EOD_CLOSE_MINUTE = 15 * 60 + 25   # 15:25 IST — let positions run closer to close
_HARD_STOP_MINUTE = 15 * 60 + 30   # 15:30 IST


def _ist_minutes_now() -> int:
    utc_sec = time.time()
    ist_sec = int(utc_sec) + _IST_OFFSET
    return ((ist_sec % 86400) // 3600) * 60 + ((ist_sec % 3600) // 60)


class WsMonitorService:
    """Real-time position monitor via Upstox WebSocket."""

    POSITION_REFRESH_INTERVAL = 15  # seconds — reduced from 60 to catch new positions faster

    def __init__(
        self,
        *,
        project_id: str,
        access_token: str,
        firestore_database: str = "(default)",
        access_token_secret_name: str = "",
    ) -> None:
        from autotrader.adapters.firestore_state import FirestoreStateStore
        from autotrader.adapters.upstox_ws_client import UpstoxWsClient

        self.state = FirestoreStateStore(project_id, firestore_database)
        self.ws = UpstoxWsClient(access_token)
        self.ws.on_quote = self._on_quote  # type: ignore[assignment]
        self.ws.on_disconnect = self._on_disconnect  # type: ignore[assignment]

        # Keep Secret Manager details so we can re-fetch the token on 401/disconnect
        self._project_id = project_id
        self._access_token_secret_name = access_token_secret_name

        # key → {"position_tag", "sl_price", "target", "side", "instrument_key"}
        self._positions: dict[str, dict] = {}
        self._exiting: set[str] = set()   # tags being exited — prevent double-exit
        self._last_refresh = 0.0
        self._stop_event = asyncio.Event()
        # Throttle Firestore SL persistence: track last-persist time per tag.
        # Breakeven / regime-tighten events persist immediately; trailing updates
        # are throttled to at most once per 30 seconds to avoid excessive writes.
        self._sl_last_persist: dict[str, float] = {}
        # Batch 3.1 (2026-04-22): throttle best_price persistence independently of
        # sl_price. Without persisted best_price, a ws_monitor restart would reset
        # the trailing high-watermark to entry_price, regressing the trailing-stop
        # reference point and silently losing every gain accrued before the crash.
        self._best_last_persist: dict[str, float] = {}
        # Current brain regime — refreshed alongside positions. Used to tighten
        # stops when the market turns while we hold a trend position.
        self._current_regime: str = ""

    # ------------------------------------------------------------------ #
    # Entry point
    # ------------------------------------------------------------------ #

    def stop(self) -> None:
        """Signal the monitor to stop (called from signal handler)."""
        self._stop_event.set()

    async def run(self) -> None:
        logger.info("ws_monitor starting")
        await self._refresh_positions()
        monitor_task = asyncio.create_task(self.ws.run_forever())
        refresh_task = asyncio.create_task(self._refresh_loop())
        eod_task = asyncio.create_task(self._eod_watchdog())
        await self._stop_event.wait()
        logger.info("ws_monitor stopping")
        self.ws.stop()
        monitor_task.cancel()
        refresh_task.cancel()
        eod_task.cancel()
        for t in (monitor_task, refresh_task, eod_task):
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        logger.info("ws_monitor stopped")

    # ------------------------------------------------------------------ #
    # Position management
    # ------------------------------------------------------------------ #

    def _resolve_instrument_key(self, pos: dict) -> str:
        """Return a valid Upstox instrument_key for this position.

        Tries pos['instrument_key'] first (saved since fix d89c008).
        Falls back to a Firestore universe lookup for older positions
        that were created before instrument_key was persisted.
        Never falls back to raw symbol — that format is not accepted
        by the Upstox WebSocket subscription API.
        """
        ikey = str(pos.get("instrument_key") or "").strip()
        if ikey:
            return ikey
        symbol = str(pos.get("symbol") or "").strip().upper()
        if not symbol:
            return ""
        try:
            uni_row = self.state.get_json("universe", symbol)
            ikey = str(uni_row.get("instrument_key") or "") if uni_row else ""
            if ikey:
                logger.info(
                    "instrument_key_resolved_from_universe symbol=%s ikey=%s",
                    symbol, ikey,
                )
        except Exception:
            logger.debug("universe_instrument_key_lookup_failed symbol=%s", symbol, exc_info=True)
        return ikey

    async def _refresh_positions(self) -> None:
        try:
            open_positions = self.state.list_open_positions()
            new_map: dict[str, dict] = {}
            for pos in open_positions:
                tag = str(pos.get("position_tag") or pos.get("_id") or "")
                ikey = self._resolve_instrument_key(pos)
                if not tag or not ikey:
                    logger.warning(
                        "skip_ws_subscribe: missing instrument_key tag=%s symbol=%s — forcing immediate exit",
                        tag, pos.get("symbol", ""),
                    )
                    # Force-close positions we can't monitor on WS to prevent unlimited loss.
                    # For paper: exit_price = entry_price (best we can do without a live quote).
                    # For live: same — last resort, better than leaving position unmonitored.
                    if tag and tag not in self._exiting:
                        self._exiting.add(tag)
                        # Schedule as a task so it doesn't block the refresh loop
                        asyncio.create_task(self._do_exit(tag, ikey or "", "NO_INSTRUMENT_KEY"))
                    continue
                entry_price = float(pos.get("entry_price") or 0)
                atr = float(pos.get("atr") or 0)
                # Parse entry timestamp to epoch for time-based exit
                entry_ts_str = str(pos.get("entry_ts") or "")
                try:
                    from autotrader.time_utils import parse_any_ts
                    _dt = parse_any_ts(entry_ts_str)
                    entry_epoch = _dt.timestamp() if _dt else time.time()
                except Exception:
                    entry_epoch = time.time()
                # Preserve best_price tracking across refreshes.
                # Batch 3.1 (2026-04-22): merge Firestore-persisted state with
                # in-memory. In-memory wins when present (freshest — updated every
                # tick), else fall back to the persisted Firestore value so a
                # service restart doesn't wipe best_price / sl_moved / flags.
                old = self._positions.get(ikey, {})
                def _carry(key: str, default):
                    mem_val = old.get(key)
                    if mem_val is not None:
                        return mem_val
                    fs_val = pos.get(key)
                    return fs_val if fs_val is not None else default
                wl_type = str(pos.get("wl_type") or "intraday").strip().lower()
                _orig_qty = int(pos.get("original_qty") or pos.get("qty") or 0)
                _sl_dist = float(pos.get("sl_dist") or 0)
                if _sl_dist <= 0 and entry_price > 0:
                    _sl_dist = abs(entry_price - float(pos.get("sl_price") or entry_price))
                new_map[ikey] = {
                    "position_tag": tag,
                    "sl_price": float(pos.get("sl_price") or 0),
                    "target": float(pos.get("target") or 0),
                    "side": str(pos.get("side") or "BUY").upper(),
                    "instrument_key": ikey,
                    "entry_price": entry_price,
                    "atr": atr,
                    "entry_epoch": entry_epoch,
                    "wl_type": wl_type,
                    # Entry regime (for regime-change exit logic)
                    "entry_regime": str(pos.get("regime") or "").strip().upper(),
                    # Carry forward best_price from previous tick tracking —
                    # Firestore fallback survives a ws_monitor restart (Batch 3.1).
                    "best_price": float(_carry("best_price", entry_price) or entry_price),
                    "sl_moved": bool(_carry("sl_moved", False)),
                    # Target-trailing flag: once we blow through target, we don't exit —
                    # we switch to a tighter trail to let the winner run.
                    "target_passed": bool(_carry("target_passed", False)),
                    "regime_tightened": bool(_carry("regime_tightened", False)),
                    # Partial exit tracking — read from Firestore (persisted) or carry forward
                    "original_qty": _orig_qty,
                    "sl_dist": _sl_dist,
                    "partial_exit_1_done": pos.get("partial_exit_1_done", old.get("partial_exit_1_done", False)),
                    "partial_exit_2_done": pos.get("partial_exit_2_done", old.get("partial_exit_2_done", False)),
                }
            self._positions = new_map
            self._last_refresh = time.time()
            # Refresh current regime from Firestore market_brain state — cheap read,
            # used by the tick handler to decide whether to tighten stops when the
            # market turns against a live trend position.
            try:
                brain = self.state.get_market_brain()
                self._current_regime = str(brain.get("regime") or "").strip().upper() if brain else ""
            except Exception:
                logger.debug("brain_state_refresh_failed", exc_info=True)
            # Re-subscribe if instrument set changed
            self.ws.set_instruments(list(new_map.keys()))
            logger.info("positions_refreshed count=%d regime=%s", len(new_map), self._current_regime or "?")
        except Exception:
            logger.exception("position_refresh_failed")

    async def _refresh_loop(self) -> None:
        while True:
            await asyncio.sleep(self.POSITION_REFRESH_INTERVAL)
            await self._refresh_positions()

    # ------------------------------------------------------------------ #
    # Tick handler
    # ------------------------------------------------------------------ #

    # Time-based exit: close position if it hasn't moved meaningfully after N minutes.
    # "Meaningful" = gained at least 0.3× ATR from entry.  Prevents dead capital.
    # 2026-04-21 post-mortem: Reverted 45 min → 120 min. The 45-min cap was killing
    # VWAP_TREND BUYs in TREND_UP regime before the thesis had time to play out
    # (10/14 trades on 04-21 exited FLAT_TIMEOUT, 0 TARGET_HIT). Indian intraday
    # trend legs typically need 60–120 min to develop; 45 min timeout was
    # systematically exiting at breakeven right before continuation.
    _FLAT_TIMEOUT_SEC = 120 * 60  # 120 minutes

    async def _on_quote(self, instrument_key: str, ltp: float, ts: float) -> None:
        pos = self._positions.get(instrument_key)
        if not pos:
            return
        tag = pos["position_tag"]
        if tag in self._exiting:
            return

        sl = pos["sl_price"]
        target = pos["target"]
        side = pos["side"]
        entry_price = pos.get("entry_price", 0.0)
        atr = pos.get("atr", 0.0)
        best = pos.get("best_price", entry_price)
        sl_dist = pos.get("sl_dist", 0.0)
        original_qty = pos.get("original_qty", 0)

        # Emergency SL: if sl_price is 0 (missing), compute from ATR to prevent unlimited loss
        if sl == 0.0 and entry_price > 0 and atr > 0:
            _emergency_dist = atr * 2.0
            sl = round(entry_price - _emergency_dist, 2) if side == "BUY" else round(entry_price + _emergency_dist, 2)
            pos["sl_price"] = sl
            logger.warning(
                "emergency_sl_assigned tag=%s sl=%.2f entry=%.2f atr=%.2f side=%s",
                tag, sl, entry_price, atr, side,
            )
            try:
                self.state.update_position(tag, {"sl_price": sl})
                self._sl_last_persist[tag] = time.time()
            except Exception as _e:
                logger.warning("emergency_sl_persist_failed tag=%s err=%s", tag, _e)

        # ── Track best price seen since entry ────────────────────────
        _best_advanced = False
        if side == "BUY" and ltp > best:
            pos["best_price"] = ltp
            best = ltp
            _best_advanced = True
        elif side == "SELL" and ltp < best:
            pos["best_price"] = ltp
            best = ltp
            _best_advanced = True

        # Batch 3.1 (2026-04-22): persist best_price to Firestore throttled at
        # 60s. Without this, a ws_monitor restart loses the high-watermark and
        # the trailing-stop reference regresses to entry_price — silently wiping
        # any gains that weren't already locked in via sl_moved/target_passed.
        if _best_advanced and time.time() - self._best_last_persist.get(tag, 0) >= 60:
            try:
                self.state.update_position(tag, {"best_price": round(best, 2)})
                self._best_last_persist[tag] = time.time()
            except Exception as _e:
                logger.warning("best_price_persist_failed tag=%s err=%s", tag, _e)

        # ── Swing vs intraday parameters ────────────────────────────
        is_swing = pos.get("wl_type") == "swing"
        _breakeven_atr_mult = 1.5 if is_swing else 1.0   # swing needs more room
        _trail_atr_mult = 2.5 if is_swing else 1.5       # wider trail for swing
        _breakeven_buffer = 0.15 if is_swing else 0.1    # buffer above entry

        # ── Breakeven SL: once price reaches N× ATR profit, move SL to entry ──
        if not pos.get("sl_moved") and entry_price > 0 and atr > 0:
            if side == "BUY" and best >= entry_price + atr * _breakeven_atr_mult:
                pos["sl_price"] = entry_price + (atr * _breakeven_buffer)
                pos["sl_moved"] = True
                sl = pos["sl_price"]
                logger.info("breakeven_sl tag=%s new_sl=%.2f best=%.2f swing=%s", tag, sl, best, is_swing)
                # Persist immediately — critical: restart must not regress to original SL
                # Batch 3.1: persist best_price alongside so the trailing reference survives.
                try:
                    self.state.update_position(tag, {"sl_price": sl, "sl_moved": True, "best_price": round(best, 2)})
                    self._sl_last_persist[tag] = time.time()
                    self._best_last_persist[tag] = time.time()
                except Exception as _e:
                    logger.warning("sl_persist_failed tag=%s err=%s", tag, _e)
            elif side == "SELL" and best <= entry_price - atr * _breakeven_atr_mult:
                pos["sl_price"] = entry_price - (atr * _breakeven_buffer)
                pos["sl_moved"] = True
                sl = pos["sl_price"]
                logger.info("breakeven_sl tag=%s new_sl=%.2f best=%.2f swing=%s", tag, sl, best, is_swing)
                try:
                    self.state.update_position(tag, {"sl_price": sl, "sl_moved": True, "best_price": round(best, 2)})
                    self._sl_last_persist[tag] = time.time()
                    self._best_last_persist[tag] = time.time()
                except Exception as _e:
                    logger.warning("sl_persist_failed tag=%s err=%s", tag, _e)

        # ── Target-passed trailing: when ltp crosses target, don't exit — switch
        # to a tighter trail (1.2× ATR) from best so a strong winner keeps running.
        # Only the initial target is abandoned; SL still protects downside.
        if not pos.get("target_passed") and target > 0:
            if (side == "BUY" and ltp >= target) or (side == "SELL" and ltp <= target):
                pos["target_passed"] = True
                pos["sl_moved"] = True   # activate trailing immediately
                logger.info("target_passed_trailing tag=%s ltp=%.2f target=%.2f", tag, ltp, target)
                # Batch 3.1 (2026-04-22): persist target_passed + best_price so a
                # ws_monitor restart doesn't re-trigger this one-shot transition.
                try:
                    self.state.update_position(tag, {
                        "target_passed": True, "sl_moved": True, "best_price": round(best, 2),
                    })
                    self._best_last_persist[tag] = time.time()
                except Exception as _e:
                    logger.warning("target_passed_persist_failed tag=%s err=%s", tag, _e)

        _active_trail_mult = 1.2 if pos.get("target_passed") else _trail_atr_mult

        # ── Regime-change tighten: if we entered in TREND_UP/RECOVERY but the
        # market has turned to CHOP/PANIC, tighten SL to 0.8× ATR from current
        # LTP immediately. One-shot: only applied once per position.
        #
        # Batch 2.3 (2026-04-22): gated to intraday only. Swing positions hold
        # 3-10 days and their SL is sized on daily ATR (2.5×). Applying an
        # intraday regime tighten (0.8× ATR from current LTP) to swing is
        # wrong on two axes: (a) daily regime has NOT flipped — the flip we
        # detect is on the intraday timeframe, which is noise for a multi-day
        # hold; (b) the 0.8× multiplier is 3.1× tighter than swing's 2.5×
        # daily-ATR SL, so the very first intraday squeeze guarantees stop-out.
        # Prior behaviour silently undermined the swing thesis every time
        # intraday regime flipped mid-day (~daily occurrence in RANGE tape).
        cur_regime = getattr(self, "_current_regime", "")
        entry_regime = pos.get("entry_regime", "")
        _pos_is_swing = str(pos.get("wl_type") or "intraday").strip().lower() == "swing"
        if (
            not pos.get("regime_tightened")
            and atr > 0
            and not _pos_is_swing
            and entry_regime in ("TREND_UP", "RECOVERY")
            and cur_regime in ("CHOP", "PANIC", "TREND_DOWN")
        ):
            tighten_dist = atr * 0.8
            if side == "BUY":
                new_sl = round(ltp - tighten_dist, 2)
                if new_sl > sl:
                    pos["sl_price"] = new_sl
                    sl = new_sl
            else:
                new_sl = round(ltp + tighten_dist, 2)
                if new_sl < sl or sl == 0:
                    pos["sl_price"] = new_sl
                    sl = new_sl
            pos["regime_tightened"] = True
            logger.info(
                "regime_change_tighten tag=%s entry=%s now=%s new_sl=%.2f ltp=%.2f",
                tag, entry_regime, cur_regime, sl, ltp,
            )
            # Persist immediately — regime tighten is a one-shot risk-reduction event
            try:
                self.state.update_position(tag, {"sl_price": sl, "regime_tightened": True})
                self._sl_last_persist[tag] = time.time()
            except Exception as _e:
                logger.warning("sl_persist_failed tag=%s err=%s", tag, _e)

        # ── Partial exits: 3-stage profit capture ─────────────────────────────
        # Stage 1 at 1:1 R:R (40% of position): locks in partial profit, SL moves to breakeven.
        # Stage 2 at 1.5:1 R:R (30% of position): books more while trade runs.
        # Remaining 30%: trails via the standard trailing-stop logic below.
        # Only active on intraday (bracket handles the full qty at broker level for live).
        # Skipped if sl_dist is unknown or original_qty is too small to split.
        if sl_dist > 0 and original_qty >= 3 and not is_swing:
            _stage1_price = (entry_price + sl_dist) if side == "BUY" else (entry_price - sl_dist)
            _stage2_price = (entry_price + sl_dist * 1.5) if side == "BUY" else (entry_price - sl_dist * 1.5)

            _stage1_hit = (side == "BUY" and ltp >= _stage1_price) or (side == "SELL" and ltp <= _stage1_price)
            _stage2_hit = (side == "BUY" and ltp >= _stage2_price) or (side == "SELL" and ltp <= _stage2_price)

            if _stage1_hit and not pos.get("partial_exit_1_done"):
                _exit_qty_1 = max(1, int(original_qty * 0.4))
                pos["partial_exit_1_done"] = True
                # Move SL to breakeven immediately on stage-1 trigger
                _be_buffer = atr * 0.1 if atr > 0 else 0
                _new_be_sl = round(entry_price + _be_buffer, 2) if side == "BUY" else round(entry_price - _be_buffer, 2)
                if (side == "BUY" and _new_be_sl > sl) or (side == "SELL" and (_new_be_sl < sl or sl == 0)):
                    pos["sl_price"] = _new_be_sl
                    pos["sl_moved"] = True
                    sl = _new_be_sl
                try:
                    self.state.update_position(tag, {
                        "partial_exit_1_done": True,
                        "sl_price": sl,
                        "sl_moved": True,
                    })
                    self._sl_last_persist[tag] = time.time()
                except Exception as _e:
                    logger.warning("partial_persist_failed tag=%s err=%s", tag, _e)
                logger.info("partial_exit_stage1 tag=%s qty=%d ltp=%.2f sl_moved_to=%.2f", tag, _exit_qty_1, ltp, sl)
                asyncio.create_task(self._do_partial_exit(tag, instrument_key, _exit_qty_1, "PARTIAL_1R"))

            elif _stage2_hit and pos.get("partial_exit_1_done") and not pos.get("partial_exit_2_done"):
                _exit_qty_2 = max(1, int(original_qty * 0.3))
                pos["partial_exit_2_done"] = True
                try:
                    self.state.update_position(tag, {"partial_exit_2_done": True})
                except Exception as _e:
                    logger.warning("partial_persist_failed tag=%s err=%s", tag, _e)
                logger.info("partial_exit_stage2 tag=%s qty=%d ltp=%.2f", tag, _exit_qty_2, ltp)
                asyncio.create_task(self._do_partial_exit(tag, instrument_key, _exit_qty_2, "PARTIAL_1_5R"))

        # ── Trailing stop: once past breakeven (or target), trail SL at N× ATR from best ──
        _sl_changed = False
        if pos.get("sl_moved") and atr > 0:
            trail_dist = atr * _active_trail_mult
            if side == "BUY":
                new_sl = round(best - trail_dist, 2)
                if new_sl > sl:
                    pos["sl_price"] = new_sl
                    sl = new_sl
                    _sl_changed = True
            elif side == "SELL":
                new_sl = round(best + trail_dist, 2)
                if new_sl < sl:
                    pos["sl_price"] = new_sl
                    sl = new_sl
                    _sl_changed = True
        # Persist trailing SL updates throttled to at most once per 30s to avoid
        # excessive Firestore writes while still surviving a restart.
        if _sl_changed and time.time() - self._sl_last_persist.get(tag, 0) >= 30:
            try:
                self.state.update_position(tag, {"sl_price": sl})
                self._sl_last_persist[tag] = time.time()
            except Exception as _e:
                logger.warning("sl_persist_failed tag=%s err=%s", tag, _e)

        # ── SL check only — target is absorbed into trailing once passed ──
        exit_reason: str | None = None
        if side == "BUY":
            if sl > 0 and ltp <= sl:
                exit_reason = "SL_HIT"
            elif not pos.get("target_passed") and target > 0 and ltp >= target:
                # Legacy path — should be rare now since target-passed flag fires first.
                exit_reason = "TARGET_HIT"
        else:  # SELL
            if sl > 0 and ltp >= sl:
                exit_reason = "SL_HIT"
            elif not pos.get("target_passed") and target > 0 and ltp <= target:
                exit_reason = "TARGET_HIT"

        # ── Time-based exit: close if flat after 2 hours (intraday only) ──
        if not exit_reason and not is_swing and entry_price > 0 and atr > 0:
            elapsed = time.time() - pos.get("entry_epoch", time.time())
            if elapsed >= self._FLAT_TIMEOUT_SEC:
                # "Flat" = hasn't moved 0.3× ATR from entry in either direction
                move = abs(ltp - entry_price)
                if move < atr * 0.3:
                    exit_reason = "FLAT_TIMEOUT"

        if exit_reason:
            self._exiting.add(tag)
            logger.info(
                "exit_triggered tag=%s reason=%s ltp=%.2f sl=%.2f target=%.2f best=%.2f",
                tag, exit_reason, ltp, sl, target, best,
            )
            asyncio.create_task(self._do_exit(tag, instrument_key, exit_reason))

    async def _do_exit(self, tag: str, instrument_key: str, exit_reason: str) -> None:
        try:
            from autotrader.services.order_service import OrderService
            # Lazy-build a minimal OrderService for exit only (no Sheets/BQ wired here)
            os_svc: OrderService = self._get_order_service()
            result = os_svc.place_exit_order(
                position_tag=tag,
                instrument_key=instrument_key,
                exit_reason=exit_reason,
            )
            logger.info("exit_completed tag=%s result=%s", tag, result)
            # Remove from position map so it doesn't trigger again
            self._positions = {k: v for k, v in self._positions.items() if v["position_tag"] != tag}
            self._exiting.discard(tag)   # clean up so set doesn't grow unbounded
        except Exception:
            logger.exception("exit_failed tag=%s", tag)
            self._exiting.discard(tag)   # allow retry on next tick

    async def _do_partial_exit(self, tag: str, instrument_key: str, exit_qty: int, reason: str) -> None:
        """Place a partial exit order without closing the position.

        The position stays OPEN with reduced qty.  Final exit happens via the
        normal _do_exit path when SL or trailing stop is hit on the remainder.
        """
        try:
            from autotrader.services.order_service import OrderService
            os_svc: OrderService = self._get_order_service()
            result = os_svc.place_partial_exit_order(
                position_tag=tag,
                instrument_key=instrument_key,
                exit_qty=exit_qty,
                exit_reason=reason,
            )
            logger.info("partial_exit_done tag=%s reason=%s result=%s", tag, reason, result)
            # Update in-memory qty so the next tick uses the correct remaining qty
            for ikey, pos in self._positions.items():
                if pos.get("position_tag") == tag:
                    _rem = result.get("remaining_qty", 0)
                    if _rem > 0:
                        pos["qty"] = _rem
                    break
        except Exception:
            logger.exception("partial_exit_failed tag=%s reason=%s", tag, reason)

    # ------------------------------------------------------------------ #
    # EOD watchdog
    # ------------------------------------------------------------------ #

    async def _eod_watchdog(self) -> None:
        """Force-close remaining positions at 15:10 and stop at 15:30."""
        eod_close_done = False
        while True:
            await asyncio.sleep(15)
            mins = _ist_minutes_now()
            if not eod_close_done and mins >= _EOD_CLOSE_MINUTE:
                logger.info("eod_close_triggered remaining_positions=%d", len(self._positions))
                for ikey, pos in list(self._positions.items()):
                    tag = pos["position_tag"]
                    # Skip swing positions — they persist overnight
                    if pos.get("wl_type") == "swing":
                        logger.info("eod_skip_swing tag=%s", tag)
                        continue
                    if tag not in self._exiting:
                        self._exiting.add(tag)
                        asyncio.create_task(self._do_exit(tag, ikey, "EOD_CLOSE"))
                eod_close_done = True
            if mins >= _HARD_STOP_MINUTE:
                logger.info("eod_hard_stop reached — shutting down ws_monitor")
                self._stop_event.set()
                break

    # ------------------------------------------------------------------ #
    # Disconnect handler
    # ------------------------------------------------------------------ #

    async def _on_disconnect(self) -> None:
        logger.warning("ws_disconnected — will reconnect")
        # Refresh positions on reconnect: picks up new positions, drops closed ones
        await self._refresh_positions()
        # Re-fetch the access token from Secret Manager on every disconnect so
        # that a daily token rotation (Upstox tokens expire at 03:30 IST) or a
        # manual token refresh is automatically picked up without a service restart.
        if self._project_id and self._access_token_secret_name:
            try:
                from google.cloud import secretmanager  # type: ignore[import-untyped]
                sm = secretmanager.SecretManagerServiceClient()
                secret_path = (
                    f"projects/{self._project_id}/secrets/"
                    f"{self._access_token_secret_name}/versions/latest"
                )
                new_token = sm.access_secret_version(
                    request={"name": secret_path}
                ).payload.data.decode("utf-8").strip()
                if new_token and new_token != self.ws._token:
                    self.ws._token = new_token
                    logger.info("ws_token_refreshed_from_secret_manager")
            except Exception:
                logger.warning("ws_token_refresh_failed — will retry with existing token", exc_info=True)

    # ------------------------------------------------------------------ #
    # Lazy OrderService builder
    # ------------------------------------------------------------------ #

    def _get_order_service(self) -> "OrderService":  # type: ignore[name-defined]  # noqa: F821
        from autotrader.container import get_container
        return get_container().order_service()


# ---------------------------------------------------------------------------
# CLI entry point — invoked by the ws-monitor Cloud Run container
# ---------------------------------------------------------------------------

async def _health_server(port: int) -> None:
    """Minimal HTTP server so Cloud Run health checks pass."""
    async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.read(1024)
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK")
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(_handle, "0.0.0.0", port)
    async with server:
        await server.serve_forever()


_IST_TZ = timezone(timedelta(hours=5, minutes=30))


def _seconds_until_next_market_open() -> float:
    """Seconds from now until the next IST 09:00 on a weekday (Mon–Fri).

    Skips Sat/Sun but not market holidays — a holiday wake-up just spins
    up an idle WS session, which is harmless. Avoiding 15:30→next-09:00
    rescheduling thrash is the goal, not perfect calendar awareness.
    """
    now = datetime.now(_IST_TZ)
    target = now.replace(hour=9, minute=0, second=0, microsecond=0)
    if target <= now:
        target = target + timedelta(days=1)
    while target.weekday() >= 5:  # 5=Sat, 6=Sun
        target = target + timedelta(days=1)
    return max(0.0, (target - now).total_seconds())


async def _main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )

    # Start health server immediately so Cloud Run health checks pass
    # even during slow WS initialisation or Secret Manager lookups.
    port = int(os.environ.get("PORT", "8080"))
    asyncio.create_task(_health_server(port))
    await asyncio.sleep(0.5)  # yield to event loop so health server binds before any blocking calls

    project_id = os.environ.get("GCP_PROJECT_ID", "")
    firestore_db = os.environ.get("FIRESTORE_DATABASE", "(default)")
    access_token_secret = os.environ.get("UPSTOX_ACCESS_TOKEN_SECRET_NAME", "")

    if not project_id:
        raise RuntimeError("GCP_PROJECT_ID env var required")

    from google.cloud import secretmanager  # type: ignore[import-untyped]
    sm_client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{project_id}/secrets/{access_token_secret}/versions/latest"

    # Process-level shutdown event: tripped by SIGTERM/SIGINT to exit the
    # multi-day loop. Distinct from svc.stop() which only ends the current
    # trading day so the loop can sleep through to the next market open.
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    while not shutdown_event.is_set():
        # Re-fetch token each morning — Upstox tokens rotate at 03:30 IST.
        access_token = sm_client.access_secret_version(
            request={"name": secret_name}
        ).payload.data.decode("utf-8").strip()

        svc = WsMonitorService(
            project_id=project_id,
            access_token=access_token,
            firestore_database=firestore_db,
            access_token_secret_name=access_token_secret,
        )

        def _on_signal(_svc: WsMonitorService = svc) -> None:
            shutdown_event.set()
            _svc.stop()

        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, _on_signal)

        await svc.run()

        if shutdown_event.is_set():
            break

        sleep_secs = _seconds_until_next_market_open()
        logger.info(
            "ws_monitor idle until next market open sleep_seconds=%.0f",
            sleep_secs,
        )
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=sleep_secs)
            break  # shutdown signalled during sleep
        except asyncio.TimeoutError:
            pass  # normal — wake for next trading day
        logger.info("ws_monitor waking for next trading day")


if __name__ == "__main__":
    asyncio.run(_main())
