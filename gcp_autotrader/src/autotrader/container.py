from __future__ import annotations

import logging
import os
import sys
from functools import lru_cache
from logging.handlers import RotatingFileHandler
from pathlib import Path

from autotrader.adapters.bigquery_client import BigQueryClient
from autotrader.adapters.firestore_state import FirestoreStateStore
from autotrader.adapters.gcs_store import GoogleCloudStorageStore
from autotrader.adapters.pubsub_client import PubSubClient
from autotrader.adapters.secrets_manager import SecretManagerStore
from autotrader.adapters.upstox_client import UpstoxClient
from autotrader.services.log_sink import LogSink
from autotrader.services.market_brain_service import MarketBrainService
from autotrader.services.order_service import OrderService
from autotrader.services.regime_service import MarketRegimeService
from autotrader.services.trading_service import TradingService
from autotrader.services.universe_service import UniverseService
from autotrader.settings import AppSettings


def configure_logging(level: str) -> None:
    root = logging.getLogger()
    lvl = getattr(logging, str(level or "INFO").upper(), logging.INFO)
    root.setLevel(lvl)
    for h in list(root.handlers):
        root.removeHandler(h)

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(lvl)
    sh.setFormatter(fmt)
    root.addHandler(sh)

    log_file = (os.getenv("AUTOTRADER_LOG_FILE") or "").strip()
    if not log_file:
        # Local dev writes into repo; Cloud Run falls back to /tmp if cwd is not writable.
        preferred = Path.cwd() / "logs" / "autotrader.log"
        fallback = Path("/tmp/autotrader.log")
        try:
            preferred.parent.mkdir(parents=True, exist_ok=True)
            log_file = str(preferred)
        except Exception:
            log_file = str(fallback)
    try:
        fp = Path(log_file)
        fp.parent.mkdir(parents=True, exist_ok=True)
        fh = RotatingFileHandler(
            fp,
            maxBytes=int(os.getenv("AUTOTRADER_LOG_MAX_BYTES", "5242880")),
            backupCount=max(1, int(os.getenv("AUTOTRADER_LOG_BACKUP_COUNT", "5"))),
            encoding="utf-8",
        )
        fh.setLevel(lvl)
        fh.setFormatter(fmt)
        root.addHandler(fh)
        logging.getLogger(__name__).info("file_logging_enabled path=%s", fp)
    except Exception:
        logging.getLogger(__name__).exception("file_logging_enable_failed path=%s", log_file)

    for name in ["uvicorn", "uvicorn.error", "uvicorn.access", "fastapi"]:
        lg = logging.getLogger(name)
        lg.setLevel(lvl)
        lg.propagate = True
        lg.handlers = []


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    settings = AppSettings.from_env()
    configure_logging(settings.runtime.log_level)
    return settings


class AppContainer:
    def __init__(self, settings: AppSettings):
        self.settings = settings
        self.secrets = SecretManagerStore(settings.gcp.project_id)
        self.gcs = GoogleCloudStorageStore(settings.gcp.bucket_name)
        self.state = FirestoreStateStore(settings.gcp.project_id, settings.gcp.firestore_database)
        self.bq = BigQueryClient(settings.gcp.project_id, settings.gcp.bq_dataset)
        self.pubsub = PubSubClient(settings.gcp.project_id)
        self.upstox = UpstoxClient(settings.upstox, self.secrets)
        self._regime_service: MarketRegimeService | None = None
        self._universe_service: UniverseService | None = None
        self._market_brain_service: MarketBrainService | None = None
        self._order_service: OrderService | None = None
        self._trading_service: TradingService | None = None

    def log_sink(self) -> LogSink:
        return LogSink()

    def regime_service(self) -> MarketRegimeService:
        if self._regime_service is None:
            self._regime_service = MarketRegimeService(self.upstox, self.settings.strategy)
        return self._regime_service

    def universe_service(self) -> UniverseService:
        if self._universe_service is None:
            self._universe_service = UniverseService(self.gcs, self.upstox, self.settings.strategy)
            self._universe_service.bq = self.bq
            self._universe_service.state = self.state
        return self._universe_service

    def market_brain_service(self) -> MarketBrainService:
        if self._market_brain_service is None:
            self._market_brain_service = MarketBrainService(
                regime_service=self.regime_service(),
                universe_service=self.universe_service(),
                gcs=self.gcs,
                state=self.state,
                bq=self.bq,
                pubsub=self.pubsub,
            )
            self.universe_service().set_market_brain_service(self._market_brain_service)
        return self._market_brain_service

    def order_service(self) -> OrderService:
        if self._order_service is None:
            self._order_service = OrderService(self.settings, self.state, self.upstox, self.bq, self.pubsub)
        return self._order_service

    def trading_service(self) -> TradingService:
        if self._trading_service is None:
            self._trading_service = TradingService(
                settings=self.settings,
                state=self.state,
                gcs=self.gcs,
                upstox=self.upstox,
                regime_service=self.regime_service(),
                market_brain_service=self.market_brain_service(),
                order_service=self.order_service(),
                log_sink=self.log_sink(),
                pubsub=self.pubsub,
            )
        return self._trading_service


@lru_cache(maxsize=1)
def get_container() -> AppContainer:
    return AppContainer(get_settings())
