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
     - time ≥ 15:10    → EOD_CLOSE exit (force-close remaining)
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

_EOD_CLOSE_MINUTE = 15 * 60 + 10   # 15:10 IST in minutes-since-midnight
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

    async def _refresh_positions(self) -> None:
        try:
            open_positions = self.state.list_open_positions()
            new_map: dict[str, dict] = {}
            for pos in open_positions:
                tag = str(pos.get("position_tag") or pos.get("_id") or "")
                ikey = str(pos.get("instrument_key") or pos.get("symbol") or "")
                if not tag or not ikey:
                    continue
                new_map[ikey] = {
                    "position_tag": tag,
                    "sl_price": float(pos.get("sl_price") or 0),
                    "target": float(pos.get("target") or 0),
                    "side": str(pos.get("side") or "BUY").upper(),
                    "instrument_key": ikey,
                }
            self._positions = new_map
            self._last_refresh = time.time()
            # Re-subscribe if instrument set changed
            self.ws.set_instruments(list(new_map.keys()))
            logger.info("positions_refreshed count=%d", len(new_map))
        except Exception:
            logger.exception("position_refresh_failed")

    async def _refresh_loop(self) -> None:
        while True:
            await asyncio.sleep(self.POSITION_REFRESH_INTERVAL)
            await self._refresh_positions()

    # ------------------------------------------------------------------ #
    # Tick handler
    # ------------------------------------------------------------------ #

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

        exit_reason: str | None = None
        if side == "BUY":
            if sl > 0 and ltp <= sl:
                exit_reason = "SL_HIT"
            elif target > 0 and ltp >= target:
                exit_reason = "TARGET_HIT"
        else:  # SELL
            if sl > 0 and ltp >= sl:
                exit_reason = "SL_HIT"
            elif target > 0 and ltp <= target:
                exit_reason = "TARGET_HIT"

        if exit_reason:
            self._exiting.add(tag)
            logger.info(
                "exit_triggered tag=%s reason=%s ltp=%.2f sl=%.2f target=%.2f",
                tag, exit_reason, ltp, sl, target,
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
        except Exception:
            logger.exception("exit_failed tag=%s", tag)
            self._exiting.discard(tag)

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
