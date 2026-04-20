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
import time
from typing import Any, Callable, Coroutine

log = logging.getLogger(__name__)

# Upstox streamer v3 authorize endpoint. Returns a one-shot `authorizedRedirectUri`
# (a wss://wsfeeder-api.upstox.com/... URL with `requestId` + `code` query params)
# that the client must connect to within ~60s. Connecting directly to the bare
# `wss://api.upstox.com/v3/feed/market-data-feed` URL handshakes but never
# streams ticks — the feeder backend only honours authorized redirect URIs.
_AUTHORIZE_URL = "https://api.upstox.com/v3/feed/market-data-feed/authorize"

# Upstox v3 streams protobuf-encoded binary frames. The schema is
# MarketDataFeed.proto (committed alongside this file). Decoded classes are
# generated into MarketDataFeed_pb2.
from . import MarketDataFeed_pb2 as _pb  # type: ignore[attr-defined]


def _extract_ltp_from_payload(raw: bytes) -> list[tuple[str, float]]:
    """Decode a Upstox v3 WS binary protobuf frame → [(instrument_key, ltp), ...].

    Handles all three Feed variants:
      - Feed.ltpc             (RequestMode=ltpc)
      - Feed.fullFeed         (RequestMode=full_d5 / full_d30; marketFF or indexFF)
      - Feed.firstLevelWithGreeks  (RequestMode=option_greeks)

    The first frame after subscribe is Type=market_info with no price feeds —
    returns empty list, caller ignores. All subsequent frames carry live ticks.
    """
    if not raw:
        return []
    try:
        resp = _pb.FeedResponse()
        resp.ParseFromString(raw)
    except Exception:
        log.exception("ws_protobuf_decode_failed len=%d first8=%r", len(raw), raw[:8])
        return []
    results: list[tuple[str, float]] = []
    # map<string, Feed>
    for key, feed in resp.feeds.items():
        ltp = 0.0
        which = feed.WhichOneof("FeedUnion")
        if which == "ltpc":
            ltp = float(feed.ltpc.ltp or 0.0)
        elif which == "fullFeed":
            inner = feed.fullFeed.WhichOneof("FullFeedUnion")
            if inner == "marketFF":
                ltp = float(feed.fullFeed.marketFF.ltpc.ltp or 0.0)
            elif inner == "indexFF":
                ltp = float(feed.fullFeed.indexFF.ltpc.ltp or 0.0)
        elif which == "firstLevelWithGreeks":
            ltp = float(feed.firstLevelWithGreeks.ltpc.ltp or 0.0)
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

    # Watchdog: if no ticks arrive for this long during market hours, log a
    # warning. If it persists to TICK_STALE_RECONNECT_SEC, force reconnect.
    # These thresholds assume liquid intraday NSE names where a subscribed
    # symbol should produce ticks at least every few seconds during market.
    TICK_STALE_WARN_SEC = 30
    TICK_STALE_RECONNECT_SEC = 90

    def __init__(self, access_token: str, *, reconnect_delay: float = 5.0) -> None:
        self._token = access_token
        self._reconnect_delay = reconnect_delay
        self._instrument_keys: list[str] = []
        self._subscribed_keys: set[str] = set()  # what the broker actually knows about
        self._running = False
        self._ws: Any = None
        self._last_tick_ts: float = 0.0
        self._stale_warned: bool = False

        # Callbacks — replace with your own coroutines
        self.on_quote: Callable[[str, float, float], Coroutine] = self._noop_quote
        self.on_disconnect: Callable[[], Coroutine] = self._noop_disconnect

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def set_instruments(self, instrument_keys: list[str]) -> None:
        """Update the desired instrument list.

        If a WS connection is already live, diff against the currently-subscribed
        set and send `sub` / `unsub` frames for the delta. This makes newly
        opened positions start receiving ticks within the next refresh cycle
        instead of waiting for a reconnect.
        """
        new_keys = list(dict.fromkeys(instrument_keys))  # preserve order, dedupe
        self._instrument_keys = new_keys
        ws = self._ws
        if ws is None:
            return  # initial subscribe will happen on connect
        # Diff against what the broker is already streaming
        desired = set(new_keys)
        current = set(self._subscribed_keys)
        to_add = sorted(desired - current)
        to_drop = sorted(current - desired)
        if to_add:
            asyncio.create_task(self._send_sub(ws, "sub", to_add))
        if to_drop:
            asyncio.create_task(self._send_sub(ws, "unsub", to_drop))

    async def run_forever(self) -> None:
        """Connect, subscribe, and loop until stopped or fatal error."""
        self._running = True
        watchdog = asyncio.create_task(self._watchdog_loop())
        try:
            while self._running:
                try:
                    await self._connect_and_stream()
                except Exception:
                    log.exception("ws_stream_error — reconnecting in %.1fs", self._reconnect_delay)
                if not self._running:
                    break
                await self.on_disconnect()
                await asyncio.sleep(self._reconnect_delay)
        finally:
            watchdog.cancel()
            try:
                await watchdog
            except (asyncio.CancelledError, Exception):
                pass

    def stop(self) -> None:
        """Signal the run loop to stop after the current connection closes."""
        self._running = False

    # ------------------------------------------------------------------ #
    # Internal
    # ------------------------------------------------------------------ #

    async def _fetch_authorized_uri(self) -> str:
        """GET /v3/feed/market-data-feed/authorize → authorizedRedirectUri.

        Upstox v3 requires a one-shot redirect URI (contains `requestId` + `code`
        query params) that must be connected to within ~60s. The bare streamer
        URL accepts the TCP handshake but never emits feed frames.
        """
        import httpx  # type: ignore[import-untyped]

        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(
                _AUTHORIZE_URL,
                headers={
                    "Authorization": f"Bearer {self._token}",
                    "Accept": "application/json",
                },
            )
            r.raise_for_status()
            data = r.json().get("data") or {}
            uri = data.get("authorizedRedirectUri") or data.get("authorized_redirect_uri")
            if not uri:
                raise RuntimeError(f"ws_authorize_missing_uri resp={r.text[:200]}")
            return uri

    async def _connect_and_stream(self) -> None:
        import websockets  # type: ignore[import-untyped]

        url = await self._fetch_authorized_uri()
        log.info("ws_connecting instruments=%d", len(self._instrument_keys))

        async with websockets.connect(
            url,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            self._ws = ws
            self._subscribed_keys = set()  # fresh connection = nothing subscribed yet
            log.info("ws_connected")
            await self._send_sub(ws, "sub", self._instrument_keys)
            self._last_tick_ts = time.time()  # reset staleness clock
            self._stale_warned = False
            try:
                async for message in ws:
                    if isinstance(message, bytes):
                        ticks = _extract_ltp_from_payload(message)
                        if ticks:
                            self._last_tick_ts = time.time()
                            self._stale_warned = False
                            for key, ltp in ticks:
                                await self.on_quote(key, ltp, self._last_tick_ts)
                    # str messages are control/status frames — ignore
            finally:
                self._ws = None

    async def _send_sub(self, ws: Any, method: str, keys: list[str]) -> None:
        """Send a sub/unsub frame for the given keys and update tracking."""
        if not keys:
            return
        # Upstox v3 modes: "ltpc" | "full" (LTP+5-level depth+OHLC) |
        # "full_d30" (30-level, Plus tier) | "option_greeks".
        # CRITICAL: v3 requires the sub frame as a BINARY WebSocket frame,
        # UTF-8 encoded. Sending as text is silently accepted but never
        # activates the feed (server holds the socket open and streams
        # nothing). v2 accepted text; v3 does not.
        payload = json.dumps({
            "guid": f"autotrader-monitor-{method}",
            "method": method,  # "sub" | "unsub"
            "data": {
                "mode": "full",
                "instrumentKeys": list(keys),
            },
        }).encode("utf-8")
        try:
            await ws.send(payload)
        except Exception:
            log.exception("ws_%s_send_failed keys=%d", method, len(keys))
            return
        if method == "sub":
            self._subscribed_keys.update(keys)
            log.info("ws_subscribed keys=%d total=%d", len(keys), len(self._subscribed_keys))
        else:
            self._subscribed_keys.difference_update(keys)
            log.info("ws_unsubscribed keys=%d total=%d", len(keys), len(self._subscribed_keys))

    async def _watchdog_loop(self) -> None:
        """Log a WARN at TICK_STALE_WARN_SEC, force a reconnect at TICK_STALE_RECONNECT_SEC.

        Guards against silent socket stalls (seen in practice when the token
        goes stale or the remote drops ticks without closing the TCP stream).
        """
        try:
            while self._running:
                await asyncio.sleep(15)
                if not self._subscribed_keys or self._last_tick_ts == 0:
                    continue  # nothing subscribed yet, or just connected
                silence = time.time() - self._last_tick_ts
                if silence >= self.TICK_STALE_RECONNECT_SEC:
                    log.warning(
                        "ws_tick_silence_reconnect silence=%.0fs subscribed=%d — forcing reconnect",
                        silence, len(self._subscribed_keys),
                    )
                    ws = self._ws
                    if ws is not None:
                        try:
                            await ws.close(code=4000, reason="tick_silence_watchdog")
                        except Exception:
                            pass
                    self._last_tick_ts = time.time()  # avoid tight reconnect loop
                    self._stale_warned = False
                elif silence >= self.TICK_STALE_WARN_SEC and not self._stale_warned:
                    log.warning(
                        "ws_tick_silence silence=%.0fs subscribed=%d — monitoring",
                        silence, len(self._subscribed_keys),
                    )
                    self._stale_warned = True
        except asyncio.CancelledError:
            raise

    @staticmethod
    async def _noop_quote(key: str, ltp: float, ts: float) -> None:
        pass

    @staticmethod
    async def _noop_disconnect() -> None:
        pass
