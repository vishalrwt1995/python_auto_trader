"""WebSocket real-time exit monitor.

Deployed as a separate Cloud Run service (autotrader-ws-monitor) with
min-instances=1 so it stays alive during market hours.

Lifecycle:
  1. Load all OPEN positions from Firestore on startup + refresh every 60s.
  2. Build instrument-key → position-tag map.
  3. Subscribe to Upstox WebSocket for all symbols.
  4. On each tick:
     - ltp ≤ sl_price  → SL_HIT exit
     - ltp ≥ target    → TARGET_HIT exit
     - time ≥ 15:25    → EOD_CLOSE exit (force-close remaining)
  5. On disconnect: reconnect with exponential back-off.
  6. At 15:30: close WebSocket, stop service.

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
from datetime import datetime

logger = logging.getLogger(__name__)

# IST offset seconds
_IST_OFFSET = 5 * 3600 + 30 * 60

_EOD_CLOSE_MINUTE = 15 * 60 + 25   # 15:25 IST — let positions run closer to close
_HARD_STOP_MINUTE = 15 * 60 + 30   # 15:30 IST


def _ist_minutes_now() -> int:
    utc_sec = time.time()
    ist_sec = utc_sec + _IST_OFFSET
    dt = datetime.utcfromtimestamp(ist_sec)
    return dt.hour * 60 + dt.minute


class WsMonitorService:
    """Real-time position monitor via Upstox WebSocket."""

    POSITION_REFRESH_INTERVAL = 15  # seconds — reduced from 60 to catch new positions faster

    def __init__(
        self,
        *,
        project_id: str,
        access_token: str,
        firestore_database: str = "(default)",
    ) -> None:
        from autotrader.adapters.firestore_state import FirestoreStateStore
        from autotrader.adapters.upstox_ws_client import UpstoxWsClient

        self.state = FirestoreStateStore(project_id, firestore_database)
        self.ws = UpstoxWsClient(access_token)
        self.ws.on_quote = self._on_quote  # type: ignore[assignment]
        self.ws.on_disconnect = self._on_disconnect  # type: ignore[assignment]

        # key → {"position_tag", "sl_price", "target", "side", "instrument_key"}
        self._positions: dict[str, dict] = {}
        self._exiting: set[str] = set()   # tags being exited — prevent double-exit
        self._last_refresh = 0.0
        self._stop_event = asyncio.Event()
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
                        "skip_ws_subscribe: missing instrument_key tag=%s symbol=%s",
                        tag, pos.get("symbol", ""),
                    )
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
                # Preserve best_price tracking across refreshes
                old = self._positions.get(ikey, {})
                wl_type = str(pos.get("wl_type") or "intraday").strip().lower()
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
                    # Carry forward best_price from previous tick tracking
                    "best_price": old.get("best_price", entry_price),
                    "sl_moved": old.get("sl_moved", False),
                    # Target-trailing flag: once we blow through target, we don't exit —
                    # we switch to a tighter trail to let the winner run.
                    "target_passed": old.get("target_passed", False),
                    "regime_tightened": old.get("regime_tightened", False),
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
    # Reduced from 120 min → 45 min: stale positions often end up as small losers
    # at EOD; freeing the slot earlier lets a better setup take over.
    _FLAT_TIMEOUT_SEC = 45 * 60  # 45 minutes

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

        # ── Track best price seen since entry ────────────────────────
        if side == "BUY" and ltp > best:
            pos["best_price"] = ltp
            best = ltp
        elif side == "SELL" and ltp < best:
            pos["best_price"] = ltp
            best = ltp

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
            elif side == "SELL" and best <= entry_price - atr * _breakeven_atr_mult:
                pos["sl_price"] = entry_price - (atr * _breakeven_buffer)
                pos["sl_moved"] = True
                sl = pos["sl_price"]
                logger.info("breakeven_sl tag=%s new_sl=%.2f best=%.2f swing=%s", tag, sl, best, is_swing)

        # ── Target-passed trailing: when ltp crosses target, don't exit — switch
        # to a tighter trail (1.2× ATR) from best so a strong winner keeps running.
        # Only the initial target is abandoned; SL still protects downside.
        if not pos.get("target_passed") and target > 0:
            if (side == "BUY" and ltp >= target) or (side == "SELL" and ltp <= target):
                pos["target_passed"] = True
                pos["sl_moved"] = True   # activate trailing immediately
                logger.info("target_passed_trailing tag=%s ltp=%.2f target=%.2f", tag, ltp, target)

        _active_trail_mult = 1.2 if pos.get("target_passed") else _trail_atr_mult

        # ── Regime-change tighten: if we entered in TREND_UP/RECOVERY but the
        # market has turned to CHOP/PANIC, tighten SL to 0.8× ATR from current
        # LTP immediately. One-shot: only applied once per position.
        cur_regime = getattr(self, "_current_regime", "")
        entry_regime = pos.get("entry_regime", "")
        if (
            not pos.get("regime_tightened")
            and atr > 0
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

        # ── Trailing stop: once past breakeven (or target), trail SL at N× ATR from best ──
        if pos.get("sl_moved") and atr > 0:
            trail_dist = atr * _active_trail_mult
            if side == "BUY":
                new_sl = round(best - trail_dist, 2)
                if new_sl > sl:
                    pos["sl_price"] = new_sl
                    sl = new_sl
            elif side == "SELL":
                new_sl = round(best + trail_dist, 2)
                if new_sl < sl:
                    pos["sl_price"] = new_sl
                    sl = new_sl

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

    # Fetch access token from Secret Manager
    from google.cloud import secretmanager  # type: ignore[import-untyped]
    sm_client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{project_id}/secrets/{access_token_secret}/versions/latest"
    access_token = sm_client.access_secret_version(request={"name": secret_name}).payload.data.decode("utf-8").strip()

    svc = WsMonitorService(
        project_id=project_id,
        access_token=access_token,
        firestore_database=firestore_db,
    )

    # Graceful shutdown on SIGTERM/SIGINT
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, svc.stop)

    await svc.run()


if __name__ == "__main__":
    asyncio.run(_main())
