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
from autotrader.services.log_sink import LogSink, set_default_bq as _log_sink_set_default_bq
from autotrader.services.market_brain_service import MarketBrainService
from autotrader.services.order_service import OrderService
from autotrader.services.regime_service import MarketRegimeService
from autotrader.services.pead_reconciliation_service import PeadReconciliationService
from autotrader.services.delivery_reconciliation_service import DeliveryReconciliationService
from autotrader.services.insider_reconciliation_service import InsiderReconciliationService
from autotrader.services.pledge_reconciliation_service import PledgeReconciliationService
from autotrader.services.corp_action_reconciliation_service import CorpActionReconciliationService
from autotrader.services.swing_reconciliation_service import SwingReconciliationService
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
        # Wire LogSink's default BQ adapter so every `LogSink()` constructed
        # without explicit DI (jobs.py / web/api.py — 32 sites) persists its
        # action buffer to the `audit_log` table on flush.
        # Discovered 2026-05-07: audit_log table was empty since launch
        # because flush_actions() never wrote anywhere.
        _log_sink_set_default_bq(self.bq)
        self.pubsub = PubSubClient(settings.gcp.project_id)
        self.upstox = UpstoxClient(settings.upstox, self.secrets)
        self._regime_service: MarketRegimeService | None = None
        self._universe_service: UniverseService | None = None
        self._market_brain_service: MarketBrainService | None = None
        self._order_service: OrderService | None = None
        self._trading_service: TradingService | None = None
        self._swing_reconciliation_service: SwingReconciliationService | None = None
        self._pead_reconciliation_service: PeadReconciliationService | None = None
        self._delivery_reconciliation_service: DeliveryReconciliationService | None = None
        self._insider_reconciliation_service: InsiderReconciliationService | None = None
        self._pledge_reconciliation_service: PledgeReconciliationService | None = None
        self._corp_action_reconciliation_service: CorpActionReconciliationService | None = None

    def log_sink(self) -> LogSink:
        # Pass `bq` explicitly here; `LogSink()` constructed elsewhere
        # without DI falls back to the module-level default set above.
        return LogSink(bq=self.bq)

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
                thresholds=self.settings.regime_thresholds,
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


    def swing_reconciliation_service(self) -> SwingReconciliationService:
        if self._swing_reconciliation_service is None:
            self._swing_reconciliation_service = SwingReconciliationService(
                settings=self.settings,
                state=self.state,
                gcs=self.gcs,
                upstox=self.upstox,
                order_service=self.order_service(),
            )
        return self._swing_reconciliation_service

    def pead_reconciliation_service(self) -> PeadReconciliationService:
        if self._pead_reconciliation_service is None:
            self._pead_reconciliation_service = PeadReconciliationService(
                settings=self.settings,
                state=self.state,
                upstox=self.upstox,
                order_service=self.order_service(),
            )
        return self._pead_reconciliation_service

    def run_pead_scan(self, reaction_date: str | None = None) -> dict:
        """Run the daily PEAD entry scan (entry pipeline). Thin wrapper around the
        pure-core service function wired with the container's adapters (PAPER)."""
        from autotrader.services import pead_trading_service
        return pead_trading_service.run_pead_scan_once(
            settings=self.settings,
            upstox=self.upstox,
            state=self.state,
            order_service=self.order_service(),
            bq=self.bq,
            reaction_date=reaction_date,
        )

    def delivery_reconciliation_service(self) -> DeliveryReconciliationService:
        if self._delivery_reconciliation_service is None:
            self._delivery_reconciliation_service = DeliveryReconciliationService(
                settings=self.settings,
                state=self.state,
                upstox=self.upstox,
                order_service=self.order_service(),
            )
        return self._delivery_reconciliation_service

    def run_delivery_scan(self, reaction_date: str | None = None) -> dict:
        """Run the daily delivery-accumulation entry scan (PAPER). Reads deliv-% from BQ
        nse_delivery_daily, fetches dailies, applies the stock-only 25-50cr gate, places
        CNC entries. No-op unless CAPITAL_DELIVERY>0."""
        from autotrader.services import delivery_trading_service
        return delivery_trading_service.run_delivery_scan_once(
            settings=self.settings,
            upstox=self.upstox,
            state=self.state,
            order_service=self.order_service(),
            bq=self.bq,
            reaction_date=reaction_date,
        )

    def run_delivery_ingest(self, asof: str | None = None) -> dict:
        """Fetch NSE sec_bhavdata_full for the latest session → load delivery-% to BQ
        nse_delivery_daily (the delivery channel's daily data feed). Fail-closed."""
        from autotrader.services.delivery_ingest_service import DeliveryIngestService
        return DeliveryIngestService(bq=self.bq).run(asof=asof)

    def insider_reconciliation_service(self) -> InsiderReconciliationService:
        if self._insider_reconciliation_service is None:
            self._insider_reconciliation_service = InsiderReconciliationService(
                settings=self.settings,
                state=self.state,
                upstox=self.upstox,
                order_service=self.order_service(),
            )
        return self._insider_reconciliation_service

    def run_insider_scan(self, reaction_date: str | None = None) -> dict:
        """Run the daily insider cluster-buy entry scan (PAPER). Applies the double macro gate
        (b200>50 AND Nifty>100DMA), clusters BQ nse_insider_daily disclosures, fetches dailies,
        places CNC entries. No-op unless CAPITAL_INSIDER>0."""
        from autotrader.services import insider_trading_service
        kw = dict(settings=self.settings, upstox=self.upstox, state=self.state,
                  order_service=self.order_service(), bq=self.bq)
        # Explicit date = manual/backfill run, settle exactly that day (unchanged behaviour).
        # Scheduled run (no date) = catch-up, so weekend-dated filings and late-landing rows are
        # not skipped forever by a single MAX(date) read.
        if reaction_date:
            return insider_trading_service.run_insider_scan_once(reaction_date=reaction_date, **kw)
        return insider_trading_service.run_insider_scan_catchup(**kw)

    def run_insider_ingest(self, asof: str | None = None) -> dict:
        """Fetch the recent NSE SEBI-PIT disclosure window → upsert to BQ nse_insider_daily
        (the insider channel's daily data feed). Fail-closed."""
        from autotrader.services.insider_ingest_service import InsiderIngestService
        return InsiderIngestService(bq=self.bq).run(asof=asof)

    def pledge_reconciliation_service(self) -> PledgeReconciliationService:
        if self._pledge_reconciliation_service is None:
            self._pledge_reconciliation_service = PledgeReconciliationService(
                settings=self.settings,
                state=self.state,
                upstox=self.upstox,
                order_service=self.order_service(),
            )
        return self._pledge_reconciliation_service

    def run_pledge_scan(self, reaction_date: str | None = None) -> dict:
        """Run the daily promoter pledge-release entry scan (PAPER). Applies the double macro gate
        (b200>50 AND Nifty>100DMA), reads pledge-revoke rows from BQ nse_insider_daily (SAME feed as
        insider — no separate ingest), fetches dailies, applies px>200DMA + turnover>=25cr, places
        CNC entries. No-op unless CAPITAL_PLEDGE>0."""
        from autotrader.services import pledge_trading_service
        kw = dict(settings=self.settings, upstox=self.upstox, state=self.state,
                  order_service=self.order_service(), bq=self.bq)
        # Explicit date = manual/backfill run, settle exactly that day (unchanged behaviour).
        # Scheduled run (no date) = catch-up, so weekend-dated filings and late-landing rows are
        # not skipped forever by a single MAX(date) read.
        if reaction_date:
            return pledge_trading_service.run_pledge_scan_once(reaction_date=reaction_date, **kw)
        return pledge_trading_service.run_pledge_scan_catchup(**kw)

    def corp_action_reconciliation_service(self) -> CorpActionReconciliationService:
        if self._corp_action_reconciliation_service is None:
            self._corp_action_reconciliation_service = CorpActionReconciliationService(
                settings=self.settings,
                state=self.state,
                upstox=self.upstox,
                order_service=self.order_service(),
            )
        return self._corp_action_reconciliation_service

    def run_corp_action_scan(self, entry_date: str | None = None) -> dict:
        """Run the daily corp-action (bonus/split) entry scan — second sub-strategy of the
        EVENT/PEAD channel, sharing its pool (PAPER). No-op unless CORP_MAX_POSITIONS>0."""
        from autotrader.services import corp_action_trading_service
        return corp_action_trading_service.run_corp_action_scan_once(
            settings=self.settings,
            upstox=self.upstox,
            state=self.state,
            order_service=self.order_service(),
            bq=self.bq,
            entry_date=entry_date,
        )

    def run_gap_fade_scan(self, entry_date: str | None = None) -> dict:
        """Run the at-the-open gap-fade SHORT scan — own GAP_FADE channel, intraday MIS
        (PAPER). No-op unless GAPFADE_MAX_POSITIONS>0. Exit is the side-aware FSM (short SL +
        EOD cover) in ws_monitor — no separate reconciliation service needed."""
        from autotrader.services import gap_fade_trading_service
        return gap_fade_trading_service.run_gap_fade_scan_once(
            settings=self.settings,
            upstox=self.upstox,
            state=self.state,
            order_service=self.order_service(),
            bq=self.bq,
            entry_date=entry_date,
        )

    def run_core_rebalance(self, asof: str | None = None) -> dict:
        """Run the quarterly CORE rebalance — own CORE channel, long-only buy-and-HOLD CNC
        (PAPER). The system's BETA engine (large-cap top-30 momentum+low-vol blend). No-op
        unless CAPITAL_CORE>0 AND CORE_ENABLED. Buy-and-hold — no stops; ws_monitor treats
        wl_type="core" as an overnight hold (Rule 8)."""
        from autotrader.services import core_trading_service
        return core_trading_service.run_core_rebalance_once(
            settings=self.settings,
            upstox=self.upstox,
            state=self.state,
            order_service=self.order_service(),
            bq=self.bq,
            asof=asof,
        )

    def run_momentum_rebalance(self, asof: str | None = None) -> dict:
        """Run the monthly Momentum x Low-Vol rebalance — own channel, long-only buy-and-HOLD
        CNC (PAPER). Top-20 momentum+low-vol blend (>=Rs10cr), buffer x1.5, Nifty-100DMA regime
        overlay. No-op unless CAPITAL_MOMENTUM>0 AND MOMENTUM_ENABLED. Buy-and-hold — no stops;
        ws_monitor treats wl_type="momentum" as an overnight hold (Rule 8)."""
        from autotrader.services import momentum_trading_service
        return momentum_trading_service.run_momentum_rebalance_once(
            settings=self.settings,
            upstox=self.upstox,
            state=self.state,
            order_service=self.order_service(),
            bq=self.bq,
            asof=asof,
        )


@lru_cache(maxsize=1)
def get_container() -> AppContainer:
    return AppContainer(get_settings())
