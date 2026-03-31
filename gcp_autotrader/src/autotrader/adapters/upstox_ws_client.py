"""Upstox WebSocket v3 market-data client.

Connects to the Upstox streamer endpoint, subscribes to Full-mode quotes for a
list of instrument keys, and invokes callbacks on each price tick.

Usage::

    client = UpstoxWsClient(access_token="<token>")
    await client.subscribe(["NSE_EQ|INE002A01018", "NSE_EQ|INE009A01021"])
    client.on_quote = my_callback   # async def my_callback(key, ltp, ts)
    await client.run_forever()
"""
from __future__ import annotations

import asyncio
import json
import logging
import struct
import time
from typing import Any, Callable, Coroutine

log = logging.getLogger(__name__)

# Upstox streamer v3 URL (no trailing slash needed; auth is via query param or header).
_STREAMER_URL = "wss://api.upstox.com/v3/feed/market-data-feed"

# Proto-buf decode is optional; fall back to JSON if protobuf not installed.
try:
    from google.protobuf import descriptor_pool as _dp  # noqa: F401
    _PROTOBUF_AVAILABLE = True
except ImportError:
    _PROTOBUF_AVAILABLE = False


def _extract_ltp_from_payload(raw: bytes) -> list[tuple[str, float]]:
    """Decode a Upstox WS binary frame and return [(instrument_key, ltp), ...]."""
    # Upstox v3 frames are JSON-encoded bytes for the first segment.
    # Binary proto fallback is not implemented here; rely on JSON mode.
    try:
        payload = json.loads(raw.decode("utf-8", errors="replace"))
    except Exception:
        return []
    feeds = payload.get("feeds") or {}
    results: list[tuple[str, float]] = []
    for key, feed_data in feeds.items():
        if not isinstance(feed_data, dict):
            continue
        # Full mode: feed_data -> {"fullFeed": {"marketFF": {"ltpc": {"ltp": ...}}}}
        ltpc = (
            (feed_data.get("fullFeed") or {})
            .get("marketFF", {})
            .get("ltpc", {})
        )
        ltp = float(ltpc.get("ltp") or 0)
        if ltp > 0:
            results.append((str(key), ltp))
    return results


class UpstoxWsClient:
    """Async Upstox WebSocket client for live price ticks.

    Attributes
    ----------
    on_quote : async callable (instrument_key: str, ltp: float, ts: float) -> None
        Called on every price update.  Default: no-op.
    on_disconnect : async callable () -> None
        Called when the connection drops (before reconnect attempt).
    """

    def __init__(self, access_token: str, *, reconnect_delay: float = 5.0) -> None:
        self._token = access_token
        self._reconnect_delay = reconnect_delay
        self._instrument_keys: list[str] = []
        self._running = False
        self._ws: Any = None

        # Callbacks — replace with your own coroutines
        self.on_quote: Callable[[str, float, float], Coroutine] = self._noop_quote
        self.on_disconnect: Callable[[], Coroutine] = self._noop_disconnect

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def set_instruments(self, instrument_keys: list[str]) -> None:
        """Set the list of instrument keys to subscribe to."""
        self._instrument_keys = list(instrument_keys)

    async def run_forever(self) -> None:
        """Connect, subscribe, and loop until stopped or fatal error."""
        self._running = True
        while self._running:
            try:
                await self._connect_and_stream()
            except Exception:
                log.exception("ws_stream_error — reconnecting in %.1fs", self._reconnect_delay)
            if not self._running:
                break
            await self.on_disconnect()
            await asyncio.sleep(self._reconnect_delay)

    def stop(self) -> None:
        """Signal the run loop to stop after the current connection closes."""
        self._running = False

    # ------------------------------------------------------------------ #
    # Internal
    # ------------------------------------------------------------------ #

    async def _connect_and_stream(self) -> None:
        import websockets  # type: ignore[import-untyped]

        url = f"{_STREAMER_URL}?token={self._token}"
        log.info("ws_connecting url=%s instruments=%d", _STREAMER_URL, len(self._instrument_keys))

        async with websockets.connect(
            url,
            extra_headers={"Authorization": f"Bearer {self._token}"},
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            self._ws = ws
            log.info("ws_connected")
            await self._subscribe(ws)
            async for message in ws:
                if isinstance(message, bytes):
                    for key, ltp in _extract_ltp_from_payload(message):
                        await self.on_quote(key, ltp, time.time())
                # str messages are control/status frames — ignore

    async def _subscribe(self, ws: Any) -> None:
        if not self._instrument_keys:
            return
        payload = json.dumps({
            "guid": "autotrader-monitor",
            "method": "sub",
            "data": {
                "mode": "full",
                "instrumentKeys": self._instrument_keys,
            },
        })
        await ws.send(payload)
        log.info("ws_subscribed keys=%d", len(self._instrument_keys))

    @staticmethod
    async def _noop_quote(key: str, ltp: float, ts: float) -> None:
        pass

    @staticmethod
    async def _noop_disconnect() -> None:
        pass
