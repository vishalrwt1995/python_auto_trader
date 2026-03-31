"""Google Cloud Pub/Sub publisher adapter.

All publishes are best-effort (errors are logged, never raised) so that a
Pub/Sub outage never blocks the trading engine.

Topics used:
  position-events  — position opened / closed
  trade-signals    — each scanner signal (placed or blocked)
  regime-events    — market-brain regime change snapshots
"""
from __future__ import annotations

import json
import logging
from typing import Any

log = logging.getLogger(__name__)


class PubSubClient:
    """Thin wrapper around the Pub/Sub publisher API with lazy initialisation."""

    def __init__(self, project_id: str) -> None:
        self._project = project_id
        self._publisher: Any = None

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _pub(self) -> Any:
        if self._publisher is None:
            from google.cloud import pubsub_v1  # type: ignore[import-untyped]
            self._publisher = pubsub_v1.PublisherClient()
        return self._publisher

    def _topic_path(self, topic: str) -> str:
        return self._pub().topic_path(self._project, topic)

    def _publish(self, topic: str, payload: dict[str, Any]) -> None:
        try:
            data = json.dumps(payload, default=str).encode("utf-8")
            future = self._pub().publish(self._topic_path(topic), data=data)
            future.result(timeout=5)
        except Exception:
            log.warning("pubsub_publish_failed topic=%s — non-critical", topic, exc_info=True)

    # ------------------------------------------------------------------ #
    # Domain-specific publish helpers
    # ------------------------------------------------------------------ #

    def publish_position_opened(self, position: dict[str, Any]) -> None:
        """Emit when a new position is opened (entry filled)."""
        self._publish("position-events", {"event": "position_opened", **position})

    def publish_position_closed(self, position: dict[str, Any]) -> None:
        """Emit when a position is closed (SL/target/EOD)."""
        self._publish("position-events", {"event": "position_closed", **position})

    def publish_trade_signal(self, signal: dict[str, Any]) -> None:
        """Emit for every scanner signal (entry placed or blocked)."""
        self._publish("trade-signals", signal)

    def publish_trade_signals_batch(self, signals: list[dict[str, Any]]) -> None:
        """Emit multiple signals — one message per signal."""
        for sig in signals:
            self._publish("trade-signals", sig)

    def publish_regime_changed(self, snapshot: dict[str, Any]) -> None:
        """Emit when the market-brain regime snapshot is updated."""
        self._publish("regime-events", snapshot)
