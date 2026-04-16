from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, default)).strip())
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(str(os.getenv(name, default)).strip())
    except Exception:
        return default


@dataclass(frozen=True)
class StrategySettings:
    capital: float = 50_000.0
    risk_per_trade: float = 125.0
    max_daily_loss: float = 300.0
    daily_profit_target: float = 375.0
    max_trades_day: int = 5
    max_positions: int = 3
    min_signal_score: int = 72
    ema_fast: int = 9
    ema_med: int = 21
    ema_slow: int = 50
    rsi_period: int = 14
    rsi_buy_min: float = 45.0
    rsi_buy_max: float = 65.0
    rsi_sell_min: float = 35.0
    rsi_sell_max: float = 55.0
    vol_mult: float = 1.5
    atr_sl_mult: float = 1.5
    rr_intraday: float = 2.0   # raised from 1.5 → 2.0: mean-reversion hit rate ~35%; need 2:1 R:R to be profitable
    vix_safe_max: float = 20.0
    vix_trend_max: float = 15.0
    pcr_bull_min: float = 0.8
    pcr_bear_max: float = 1.2
    nifty_trend_pct: float = 0.3
    # Swing-specific settings
    swing_atr_sl_mult: float = 2.5
    swing_rr: float = 2.0
    swing_risk_per_trade: float = 200.0
    swing_max_positions: int = 5
    swing_max_hold_days: int = 10
    swing_min_signal_score: int = 75


@dataclass(frozen=True)
class UpstoxSettings:
    api_v2_host: str
    api_v3_host: str
    client_id_secret_name: str
    client_secret_secret_name: str
    access_token_secret_name: str
    access_token_expiry_secret_name: str
    redirect_uri: str = ""
    auth_code_secret_name: str = ""
    notifier_shared_secret: str = ""
    instruments_complete_url: str = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
    requests_per_second: int = 50
    max_per_minute: int = 500
    max_per_30min: int = 2000
    max_retries: int = 4
    nifty50_instrument_key: str = "NSE_INDEX|Nifty 50"
    india_vix_instrument_key: str = "NSE_INDEX|India VIX"
    pcr_underlying_instrument_key: str = "NSE_INDEX|Nifty 50"
    pcr_expiry_date: str = ""


@dataclass(frozen=True)
class GcpSettings:
    project_id: str
    region: str
    bucket_name: str
    spreadsheet_id: str = ""
    firestore_database: str = "(default)"
    bq_dataset: str = "autotrader"
    pubsub_topic_positions: str = "position-events"
    pubsub_topic_signals: str = "trade-signals"
    pubsub_topic_regime: str = "regime-events"


@dataclass(frozen=True)
class RuntimeSettings:
    paper_trade: bool
    job_trigger_token: str
    log_level: str
    timezone_name: str = "Asia/Kolkata"


@dataclass(frozen=True)
class AppSettings:
    gcp: GcpSettings
    upstox: UpstoxSettings
    runtime: RuntimeSettings
    strategy: StrategySettings

    @staticmethod
    def from_env() -> "AppSettings":
        strategy = StrategySettings(
            capital=_env_float("CAPITAL", 50000),
            risk_per_trade=_env_float("RISK_PER_TRADE", 125),
            max_daily_loss=_env_float("MAX_DAILY_LOSS", 300),
            daily_profit_target=_env_float("DAILY_PROFIT_TARGET", 375),
            max_trades_day=_env_int("MAX_TRADES_DAY", 5),
            max_positions=_env_int("MAX_POSITIONS", 3),
            min_signal_score=_env_int("MIN_SIGNAL_SCORE", 72),
            ema_fast=_env_int("EMA_FAST", 9),
            ema_med=_env_int("EMA_MED", 21),
            ema_slow=_env_int("EMA_SLOW", 50),
            rsi_period=_env_int("RSI_PERIOD", 14),
            rsi_buy_min=_env_float("RSI_BUY_MIN", 45),
            rsi_buy_max=_env_float("RSI_BUY_MAX", 65),
            rsi_sell_min=_env_float("RSI_SELL_MIN", 35),
            rsi_sell_max=_env_float("RSI_SELL_MAX", 55),
            vol_mult=_env_float("VOL_MULT", 1.5),
            atr_sl_mult=_env_float("ATR_SL_MULT", 1.5),
            rr_intraday=_env_float("RR_INTRADAY", 2.0),
            vix_safe_max=_env_float("VIX_SAFE_MAX", 20),
            vix_trend_max=_env_float("VIX_TREND_MAX", 15),
            pcr_bull_min=_env_float("PCR_BULL_MIN", 0.8),
            pcr_bear_max=_env_float("PCR_BEAR_MAX", 1.2),
            nifty_trend_pct=_env_float("NIFTY_TREND_PCT", 0.3),
            swing_atr_sl_mult=_env_float("SWING_ATR_SL_MULT", 2.5),
            swing_rr=_env_float("SWING_RR", 2.0),
            swing_risk_per_trade=_env_float("SWING_RISK_PER_TRADE", 200),
            swing_max_positions=_env_int("SWING_MAX_POSITIONS", 5),
            swing_max_hold_days=_env_int("SWING_MAX_HOLD_DAYS", 10),
            swing_min_signal_score=_env_int("SWING_MIN_SIGNAL_SCORE", 75),
        )
        return AppSettings(
            gcp=GcpSettings(
                project_id=_env("GCP_PROJECT_ID"),
                region=_env("GCP_REGION", "asia-south1"),
                bucket_name=_env("GCS_BUCKET"),
                spreadsheet_id=os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", ""),
                firestore_database=_env("FIRESTORE_DATABASE", "(default)"),
                bq_dataset=_env("BQ_DATASET", "autotrader"),
                pubsub_topic_positions=_env("PUBSUB_TOPIC_POSITIONS", "position-events"),
                pubsub_topic_signals=_env("PUBSUB_TOPIC_SIGNALS", "trade-signals"),
                pubsub_topic_regime=_env("PUBSUB_TOPIC_REGIME", "regime-events"),
            ),
            upstox=UpstoxSettings(
                api_v2_host=_env("UPSTOX_API_V2_HOST", "https://api.upstox.com/v2").rstrip("/"),
                api_v3_host=_env("UPSTOX_API_V3_HOST", "https://api.upstox.com/v3").rstrip("/"),
                client_id_secret_name=_env("UPSTOX_CLIENT_ID_SECRET_NAME"),
                client_secret_secret_name=_env("UPSTOX_CLIENT_SECRET_SECRET_NAME"),
                access_token_secret_name=_env("UPSTOX_ACCESS_TOKEN_SECRET_NAME"),
                access_token_expiry_secret_name=_env("UPSTOX_ACCESS_TOKEN_EXPIRY_SECRET_NAME"),
                redirect_uri=_env("UPSTOX_REDIRECT_URI", ""),
                auth_code_secret_name=_env("UPSTOX_AUTH_CODE_SECRET_NAME", ""),
                notifier_shared_secret=_env("UPSTOX_NOTIFIER_SHARED_SECRET", ""),
                instruments_complete_url=_env(
                    "UPSTOX_INSTRUMENTS_COMPLETE_URL",
                    "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz",
                ),
                requests_per_second=max(1, _env_int("UPSTOX_REQUESTS_PER_SECOND", 50)),
                max_per_minute=max(1, _env_int("UPSTOX_MAX_PER_MINUTE", 500)),
                max_per_30min=max(1, _env_int("UPSTOX_MAX_PER_30MIN", 2000)),
                max_retries=max(1, _env_int("UPSTOX_MAX_RETRIES", 4)),
                nifty50_instrument_key=_env("UPSTOX_NIFTY50_INSTRUMENT_KEY", "NSE_INDEX|Nifty 50"),
                india_vix_instrument_key=_env("UPSTOX_INDIA_VIX_INSTRUMENT_KEY", "NSE_INDEX|India VIX"),
                pcr_underlying_instrument_key=_env("UPSTOX_PCR_UNDERLYING_INSTRUMENT_KEY", "NSE_INDEX|Nifty 50"),
                pcr_expiry_date=_env("UPSTOX_PCR_EXPIRY_DATE", ""),
            ),
            runtime=RuntimeSettings(
                paper_trade=_env_bool("PAPER_TRADE", True),
                job_trigger_token=_env("JOB_TRIGGER_TOKEN"),
                log_level=_env("LOG_LEVEL", "INFO"),
                timezone_name=_env("TZ", "Asia/Kolkata"),
            ),
            strategy=strategy,
        )
