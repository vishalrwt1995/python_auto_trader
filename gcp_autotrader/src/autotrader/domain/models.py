from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


Candle = tuple[str, float, float, float, float, float]
Direction = Literal["BUY", "SELL", "HOLD"]
RegimeKind = Literal["TREND", "RANGE", "AVOID"]
Bias = Literal["BULLISH", "BEARISH", "NEUTRAL"]
MarketPhase = Literal["PREMARKET", "POST_OPEN", "LIVE", "EOD"]
MarketRegimeV2 = Literal["TREND_UP", "TREND_DOWN", "RANGE", "CHOP", "PANIC", "RECOVERY"]
ParticipationKind = Literal["STRONG", "MODERATE", "WEAK"]
RiskModeKind = Literal["AGGRESSIVE", "NORMAL", "DEFENSIVE", "LOCKDOWN"]
IntradayStateKind = Literal["PREOPEN", "OPEN_DRIVE", "OPEN_FADE", "TREND_DAY", "CHOP_DAY", "EVENT_RISK"]
PermissionKind = Literal["ENABLED", "REDUCED", "DISABLED"]


@dataclass
class Quote:
    ltp: float
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: float = 0.0
    change_pct: float = 0.0
    change: float = 0.0
    bid: float = 0.0
    ask: float = 0.0
    ts: str = ""


@dataclass
class PcrSnapshot:
    pcr: float = 1.0
    max_pain: float = 0.0
    call_oi: float = 0.0
    put_oi: float = 0.0
    pcr_near: float = 1.0
    pcr_next: float = 1.0
    pcr_monthly: float = 1.0
    pcr_weighted: float = 1.0
    pcr_term_slope: float = 0.0
    oi_change_pcr: float = 1.0
    oi_change_call: float = 0.0
    oi_change_put: float = 0.0
    oi_concentration: float = 0.0
    call_wall: float = 0.0
    put_wall: float = 0.0
    call_wall_dist_pct: float = 0.0
    put_wall_dist_pct: float = 0.0
    max_pain_dist_pct: float = 0.0
    expiry_near: str = ""
    expiry_next: str = ""
    expiry_monthly: str = ""
    expiries_used: int = 0
    confidence: float = 0.0
    fetched_at: str = ""


@dataclass
class FiiDiiSnapshot:
    fii: float = 0.0
    dii: float = 0.0
    as_of_date: str = ""
    fetched_at: str = ""
    freshness_score: float = 0.0


@dataclass
class NiftySnapshot:
    ltp: float = 0.0
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    change_pct: float = 0.0
    quote_ts: str = ""
    age_sec: float = 0.0


@dataclass
class NiftyStructureSnapshot:
    timeframe: str = "15m"
    bars: int = 0
    last_candle_ts: str = ""
    ema_stack: str = "MIXED"
    supertrend_dir: int = 0
    rsi: float = 50.0
    macd_hist: float = 0.0
    adx: float = 0.0
    atr_pct: float = 0.0
    ema_spread_pct: float = 0.0
    vwap_gap_pct: float = 0.0
    gap_pct: float = 0.0
    opening_range_break: str = "NONE"
    trend_strength: float = 0.0
    chop_risk: float = 0.0
    structure_regime: str = "UNKNOWN"


@dataclass
class FreshnessSnapshot:
    generated_at: str = ""
    session_phase: str = "UNKNOWN"
    nifty_age_sec: float = 0.0
    vix_age_sec: float = 0.0
    pcr_age_sec: float = 0.0
    fii_age_hours: float = 0.0
    score: float = 0.0


@dataclass
class RegimeSnapshot:
    regime: RegimeKind = "RANGE"
    bias: Bias = "NEUTRAL"
    vix: float = 0.0
    pcr: PcrSnapshot = field(default_factory=PcrSnapshot)
    fii: FiiDiiSnapshot = field(default_factory=FiiDiiSnapshot)
    nifty: NiftySnapshot = field(default_factory=NiftySnapshot)
    nifty_structure: NiftyStructureSnapshot = field(default_factory=NiftyStructureSnapshot)
    freshness: FreshnessSnapshot = field(default_factory=FreshnessSnapshot)
    confidence: float = 0.0
    data_health: float = 0.0
    source_quality: float = 0.0
    sub_regime: str = "UNKNOWN"
    rationale: str = ""
    source: str = "computed"


@dataclass
class IndicatorLine:
    curr: float
    prev: float


@dataclass
class MacdState:
    macd: float
    signal: float
    hist: float
    prev_hist: float
    crossed: str | None


@dataclass
class SuperTrendState:
    value: float | None
    dir: int
    prev_dir: int
    fresh: bool


@dataclass
class VolumeState:
    curr: float
    avg: float
    ratio: float


@dataclass
class Patterns:
    doji: bool
    bull_engulf: bool
    bear_engulf: bool
    bear_candle: bool


@dataclass
class BollingerState:
    upper: float
    mid: float
    lower: float


@dataclass
class StochasticState:
    k: float
    d: float


@dataclass
class IndicatorSnapshot:
    close: float
    prev_close: float
    open: float
    high: float
    low: float
    ema_fast: IndicatorLine
    ema_med: IndicatorLine
    ema_slow: IndicatorLine
    ema20: IndicatorLine
    ema50: IndicatorLine
    rsi: IndicatorLine
    macd: MacdState
    supertrend: SuperTrendState
    vwap: float
    obv_curr: float
    obv_prev: float
    atr: float
    adx: float
    bb: BollingerState | None
    stoch: StochasticState
    volume: VolumeState
    ema_stack: bool
    ema_flip: bool
    ema20_above_ema50: bool
    above_ema20: bool
    above_ema50: bool
    near_breakout: bool
    breakout: bool
    dist_from_52w_high: float
    obv_rising: bool
    patterns: Patterns
    candles: list[Candle]


@dataclass
class ScoreBreakdown:
    regime: float = 0.0
    options: float = 0.0
    technical: float = 0.0
    volume: float = 0.0
    alignment: float = 0.0  # multi-timeframe alignment (daily vs 15m)
    penalty: float = 0.0


@dataclass
class SignalScore:
    score: int
    direction: Direction
    breakdown: ScoreBreakdown = field(default_factory=ScoreBreakdown)


@dataclass
class PositionSizing:
    qty: int
    sl_price: float
    target: float
    sl_dist: float
    entry_price: float
    max_loss: float
    max_gain: float
    brokerage: float


@dataclass
class UniverseRow:
    row_number: int
    symbol: str
    exchange: str = "NSE"
    segment: str = "CASH"
    allowed_product: str = "BOTH"
    strategy_pref: str = "AUTO"
    sector: str = "UNKNOWN"
    beta: float = 1.0
    enabled: str = "Y"
    priority: float = 0.0
    notes: str = ""
    provider: str = ""
    instrument_key: str = ""
    source_segment: str = ""
    security_type: str = ""


@dataclass
class WatchlistRow:
    symbol: str
    exchange: str = "NSE"
    segment: str = "CASH"
    product: str = "CNC"
    strategy: str = "AUTO"
    sector: str = "UNKNOWN"
    beta: float = 1.0
    enabled: bool = True
    note: str = ""
    wl_type: str = "intraday"  # "intraday" or "swing"


@dataclass
class MarketBrainState:
    asof_ts: str
    phase: MarketPhase = "PREMARKET"
    regime: MarketRegimeV2 = "RANGE"
    sub_regime_v2: str = "BASELINE"
    structure_state: str = "ORDERLY"
    recovery_state: str = "NONE"
    event_state: str = "NONE"
    participation: ParticipationKind = "MODERATE"
    risk_mode: RiskModeKind = "NORMAL"
    intraday_state: IntradayStateKind = "PREOPEN"
    run_degraded_flag: bool = False
    long_bias: float = 0.5
    short_bias: float = 0.5
    size_multiplier: float = 1.0
    max_positions_multiplier: float = 1.0
    swing_permission: PermissionKind = "ENABLED"
    allowed_strategies: list[str] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)
    trend_score: float = 50.0
    breadth_score: float = 50.0
    leadership_score: float = 50.0
    volatility_stress_score: float = 50.0
    liquidity_health_score: float = 50.0
    data_quality_score: float = 50.0
    market_confidence: float = 50.0
    breadth_confidence: float = 50.0
    leadership_confidence: float = 50.0
    phase2_confidence: float = 50.0
    policy_confidence: float = 50.0
    run_integrity_confidence: float = 50.0


@dataclass
class MarketPolicy:
    regime: MarketRegimeV2 = "RANGE"
    risk_mode: RiskModeKind = "NORMAL"
    allowed_strategies: list[str] = field(default_factory=list)
    swing_permission: PermissionKind = "ENABLED"
    size_multiplier: float = 1.0
    max_positions_multiplier: float = 1.0
    watchlist_target_multiplier: float = 1.0
    watchlist_min_score_boost: int = 0
    intraday_phase2_enabled: bool = True
    breakout_enabled: bool = True
    open_drive_enabled: bool = True
    long_enabled: bool = True
    short_enabled: bool = True
    liquidity_bucket_floor: str = "D"
    dynamic_sector_cap_share: float = 0.20
    correlation_threshold: float = 0.85
    policy_confidence: float = 50.0
    reasons: list[str] = field(default_factory=list)
