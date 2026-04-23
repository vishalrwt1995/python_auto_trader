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
    # Batch 4.1 (2026-04-22): dropped 2.0 → 1.25. Post-mortem review of recent
    # trades showed winners routinely peaked at 1.2-1.5R MFE then faded; the
    # 2R target meant those winners tripped the trailing-stop post-target
    # logic instead of booking a clean TARGET_HIT, realizing less than the
    # plan. At 35% hit rate × 1.3R actual capture vs 65% × 1R loss, 2R was
    # NEGATIVE expectancy despite the headline R:R. 1.25R target should hit
    # more often (more trades resolve cleanly) and realized R closer to plan.
    # MEAN_REVERSION keeps a higher target (see rr_intraday_reversion) — fade
    # setups need meaningful excursion to be worth the counter-trend risk.
    rr_intraday: float = 1.25
    # Per-strategy R:R override: MEAN_REVERSION / VWAP_REVERSAL fades need
    # wider targets because the "snap back" on oversold names routinely does
    # 2-3R; a 1.25R target cuts them off right where the move is accelerating.
    rr_intraday_reversion: float = 2.0
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
    # P1 (2026-04-22): dropped 75 → 70 after live observation that scorer-eligible
    # daily-uptrending names (WELCORP daily_strength=82, LLOYDSME=84, STLTECH=88)
    # cluster at adjusted_score 62–73 in RANGE/NORMAL regimes. Intraday uses a
    # risk-mode-tiered threshold (58–75) and adjusted_score post brain-haircut;
    # swing uses _affinity_score (pre-haircut) against this single threshold.
    # A 3–10 day swing trade's edge is the daily trend — over-filtering at 75
    # on intraday-composite scoring kills the sample size (see 2026-04-22:
    # 35 evaluations → 1 qualified at 76/75, 1-point margin).
    swing_min_signal_score: int = 70
    # Batch 2.1 (2026-04-22): re-entry cooldown. When a position closes
    # (SL hit, target hit, or timeout), the scanner should NOT immediately
    # re-stage the same symbol on the next 3-min cycle. The watchlist will
    # naturally re-score that name as a strong setup (price just moved
    # through SL/target), and without a cooldown the bot would enter the
    # same trade again — compounding a losing thesis. Empirically 04-16
    # showed multiple symbols churned 2-3 times in under 30 min. 30-min
    # default chosen to be > 1 intraday-candle (15m) so the next signal
    # comes from a fresh candle cycle, not the SL breakout bar.
    reentry_cooldown_minutes: int = 30
    # P0-2 (2026-04-22): strategy kill-switch. Strategies listed here are
    # stripped from `allowed_strategies` regardless of regime. Used to disable
    # known-bad strategies surfaced by live P&L analysis.
    # Current blocklist:
    #   VWAP_REVERSAL — 13 trades over 30d, 23% win-rate, -0.61% avg P&L,
    #     12/13 closed at EOD never reaching target or SL. Negative expectancy.
    # Re-enable only after backtest or replay proves the strategy has edge.
    disabled_strategies: tuple[str, ...] = ("VWAP_REVERSAL",)
    # Batch 7 (2026-04-23): paper-trade slippage modeling. Paper fills used to
    # assume LTP=fill-price with zero cost, but live execution pays bid-ask +
    # impact cost. Un-modelled slippage flattered paper P&L vs live by roughly
    # 0.15-0.25% per round-trip (measured on 2026-02 to 2026-03 trade ledger
    # comparing paper-tagged vs live-tagged same-setup trades). These two
    # percentages shift paper fills adversely so paper P&L tracks live.
    # Entry slippage: MARKET order fills through the spread + momentum kick.
    # SL slippage: market-order exit triggered mid-bar, fills further through
    # the L2 book when multiple traders hit the same level.
    # Target slippage is zero — target orders are LIMIT, so fills happen AT
    # the price or not at all.
    paper_entry_slippage_pct: float = 0.0010   # 0.10%
    paper_sl_slippage_pct: float = 0.0020      # 0.20%


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
    # ── Redesign feature flags (default OFF — flip to opt in) ──
    # M1: 5-state exit FSM (INITIAL/CONFIRMED/RUNNER/LOSING/TERMINAL).
    # When False, the legacy exit precedence in ws_monitor runs.
    use_exit_fsm_v1: bool = False
    # M2: Playbook hard-block layer + Edge registry. When False, the legacy
    # scorer decides entries directly.
    use_playbook_v1: bool = False
    # M3: expected_edge_R scoring (backtest-derived R priors). When False,
    # the legacy signal_score drives entry ranking.
    use_expected_edge_r_v1: bool = False
    # M4: PortfolioBook channel budgets + DD governors. When False, the
    # legacy max_positions / risk_per_trade gates apply.
    use_portfolio_book_v1: bool = False


@dataclass(frozen=True)
class RegimeThresholds:
    """Market-Brain regime classification thresholds.

    Defaults match the magic numbers previously hard-coded in
    `MarketBrainService._map_regime` / `_map_risk_mode` (PR-1, 2026-04-20).
    Externalising them lets us tune without redeploying code, and the
    table-driven regime tests lock behaviour to these defaults so any
    env override is an explicit, reviewable change.
    """
    # PANIC entry
    panic_stress_min: float = 82.0
    panic_breadth_max: float = 12.0
    panic_dq_max: float = 30.0
    # TREND_UP entry (standard)
    trend_up_trend_min: float = 70.0
    trend_up_breadth_min: float = 62.0
    trend_up_leadership_min: float = 56.0
    trend_up_stress_max: float = 48.0
    # TREND_UP entry (high-breadth alternative)
    trend_up_hi_breadth_min: float = 80.0
    trend_up_hi_leadership_min: float = 60.0
    trend_up_hi_stress_max: float = 48.0
    # TREND_DOWN entry
    trend_down_trend_max: float = 36.0
    trend_down_breadth_max: float = 40.0
    trend_down_leadership_max: float = 45.0
    # CHOP entry
    chop_stress_min: float = 62.0
    chop_leadership_max: float = 46.0
    chop_appetite_max: float = 46.0
    # RECOVERY entry
    recovery_trend_min: float = 40.0
    recovery_breadth_min: float = 35.0
    recovery_leadership_min: float = 40.0
    # PANIC exit guard (stay-in-PANIC conditions)
    panic_exit_stress_above: float = 65.0
    panic_exit_breadth_below: float = 22.0
    # TREND_UP hysteresis (stay-in)
    trend_up_hold_trend_min: float = 60.0
    trend_up_hold_breadth_min: float = 55.0
    trend_up_hold_leadership_min: float = 50.0
    # TREND_UP hysteresis (entry after absence)
    trend_up_reenter_trend_min: float = 74.0
    trend_up_reenter_breadth_min: float = 66.0
    trend_up_reenter_leadership_min: float = 58.0
    trend_up_reenter_hi_breadth_min: float = 82.0
    trend_up_reenter_hi_leadership_min: float = 62.0
    trend_up_reenter_hi_stress_max: float = 45.0
    # General transition damper (sub-threshold age in seconds)
    transition_min_age_sec: float = 240.0
    # Risk mode thresholds
    lockdown_stress_min: float = 85.0
    lockdown_dq_max: float = 35.0
    defensive_stress_min: float = 65.0
    defensive_dq_max: float = 55.0
    aggressive_appetite_min: float = 66.0
    aggressive_stress_max: float = 50.0
    aggressive_dq_min: float = 65.0
    # Signal-staleness decay (PR-1)
    signal_fresh_max_sec: float = 120.0     # < this → 0 penalty
    signal_stale_full_sec: float = 900.0    # > this → full penalty (40 pts)
    signal_max_penalty: float = 40.0
    # Pubsub emission cadence (PR-1)
    pubsub_heartbeat_sec: float = 300.0     # emit even without transition after this long


@dataclass(frozen=True)
class AppSettings:
    gcp: GcpSettings
    upstox: UpstoxSettings
    runtime: RuntimeSettings
    strategy: StrategySettings
    regime_thresholds: RegimeThresholds = RegimeThresholds()

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
            # Batch 4.1 (2026-04-22): default aligned to dataclass (1.25)
            rr_intraday=_env_float("RR_INTRADAY", 1.25),
            rr_intraday_reversion=_env_float("RR_INTRADAY_REVERSION", 2.0),
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
            # Batch 1.3 (2026-04-22): default aligned to dataclass (70). Prior
            # divergence (dataclass=70, env default=75) meant production — which
            # constructs StrategySettings via from_env() — silently used the OLD
            # pre-P1 threshold while unit tests constructing StrategySettings()
            # directly saw the P1 value. The P1 swing-threshold calibration only
            # takes effect because no SWING_MIN_SIGNAL_SCORE env var is set in
            # Cloud Run today, so from_env's default must be authoritative.
            swing_min_signal_score=_env_int("SWING_MIN_SIGNAL_SCORE", 70),
            reentry_cooldown_minutes=_env_int("REENTRY_COOLDOWN_MINUTES", 30),
            paper_entry_slippage_pct=_env_float("PAPER_ENTRY_SLIPPAGE_PCT", 0.0010),
            paper_sl_slippage_pct=_env_float("PAPER_SL_SLIPPAGE_PCT", 0.0020),
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
                use_exit_fsm_v1=_env_bool("USE_EXIT_FSM_V1", False),
                use_playbook_v1=_env_bool("USE_PLAYBOOK_V1", False),
                use_expected_edge_r_v1=_env_bool("USE_EXPECTED_EDGE_R_V1", False),
                use_portfolio_book_v1=_env_bool("USE_PORTFOLIO_BOOK_V1", False),
            ),
            strategy=strategy,
            regime_thresholds=RegimeThresholds(
                panic_stress_min=_env_float("REGIME_PANIC_STRESS_MIN", 82.0),
                panic_breadth_max=_env_float("REGIME_PANIC_BREADTH_MAX", 12.0),
                panic_dq_max=_env_float("REGIME_PANIC_DQ_MAX", 30.0),
                trend_up_trend_min=_env_float("REGIME_TREND_UP_TREND_MIN", 70.0),
                trend_up_breadth_min=_env_float("REGIME_TREND_UP_BREADTH_MIN", 62.0),
                trend_up_leadership_min=_env_float("REGIME_TREND_UP_LEADERSHIP_MIN", 56.0),
                trend_up_stress_max=_env_float("REGIME_TREND_UP_STRESS_MAX", 48.0),
                trend_up_hi_breadth_min=_env_float("REGIME_TREND_UP_HI_BREADTH_MIN", 80.0),
                trend_up_hi_leadership_min=_env_float("REGIME_TREND_UP_HI_LEADERSHIP_MIN", 60.0),
                trend_up_hi_stress_max=_env_float("REGIME_TREND_UP_HI_STRESS_MAX", 48.0),
                trend_down_trend_max=_env_float("REGIME_TREND_DOWN_TREND_MAX", 36.0),
                trend_down_breadth_max=_env_float("REGIME_TREND_DOWN_BREADTH_MAX", 40.0),
                trend_down_leadership_max=_env_float("REGIME_TREND_DOWN_LEADERSHIP_MAX", 45.0),
                chop_stress_min=_env_float("REGIME_CHOP_STRESS_MIN", 62.0),
                chop_leadership_max=_env_float("REGIME_CHOP_LEADERSHIP_MAX", 46.0),
                chop_appetite_max=_env_float("REGIME_CHOP_APPETITE_MAX", 46.0),
                recovery_trend_min=_env_float("REGIME_RECOVERY_TREND_MIN", 40.0),
                recovery_breadth_min=_env_float("REGIME_RECOVERY_BREADTH_MIN", 35.0),
                recovery_leadership_min=_env_float("REGIME_RECOVERY_LEADERSHIP_MIN", 40.0),
                panic_exit_stress_above=_env_float("REGIME_PANIC_EXIT_STRESS_ABOVE", 65.0),
                panic_exit_breadth_below=_env_float("REGIME_PANIC_EXIT_BREADTH_BELOW", 22.0),
                trend_up_hold_trend_min=_env_float("REGIME_TREND_UP_HOLD_TREND_MIN", 60.0),
                trend_up_hold_breadth_min=_env_float("REGIME_TREND_UP_HOLD_BREADTH_MIN", 55.0),
                trend_up_hold_leadership_min=_env_float("REGIME_TREND_UP_HOLD_LEADERSHIP_MIN", 50.0),
                trend_up_reenter_trend_min=_env_float("REGIME_TREND_UP_REENTER_TREND_MIN", 74.0),
                trend_up_reenter_breadth_min=_env_float("REGIME_TREND_UP_REENTER_BREADTH_MIN", 66.0),
                trend_up_reenter_leadership_min=_env_float("REGIME_TREND_UP_REENTER_LEADERSHIP_MIN", 58.0),
                trend_up_reenter_hi_breadth_min=_env_float("REGIME_TREND_UP_REENTER_HI_BREADTH_MIN", 82.0),
                trend_up_reenter_hi_leadership_min=_env_float("REGIME_TREND_UP_REENTER_HI_LEADERSHIP_MIN", 62.0),
                trend_up_reenter_hi_stress_max=_env_float("REGIME_TREND_UP_REENTER_HI_STRESS_MAX", 45.0),
                transition_min_age_sec=_env_float("REGIME_TRANSITION_MIN_AGE_SEC", 240.0),
                lockdown_stress_min=_env_float("REGIME_LOCKDOWN_STRESS_MIN", 85.0),
                lockdown_dq_max=_env_float("REGIME_LOCKDOWN_DQ_MAX", 35.0),
                defensive_stress_min=_env_float("REGIME_DEFENSIVE_STRESS_MIN", 65.0),
                defensive_dq_max=_env_float("REGIME_DEFENSIVE_DQ_MAX", 55.0),
                aggressive_appetite_min=_env_float("REGIME_AGGRESSIVE_APPETITE_MIN", 66.0),
                aggressive_stress_max=_env_float("REGIME_AGGRESSIVE_STRESS_MAX", 50.0),
                aggressive_dq_min=_env_float("REGIME_AGGRESSIVE_DQ_MIN", 65.0),
                signal_fresh_max_sec=_env_float("REGIME_SIGNAL_FRESH_MAX_SEC", 120.0),
                signal_stale_full_sec=_env_float("REGIME_SIGNAL_STALE_FULL_SEC", 900.0),
                signal_max_penalty=_env_float("REGIME_SIGNAL_MAX_PENALTY", 40.0),
                pubsub_heartbeat_sec=_env_float("REGIME_PUBSUB_HEARTBEAT_SEC", 300.0),
            ),
        )
