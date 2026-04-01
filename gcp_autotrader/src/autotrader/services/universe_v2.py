from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from typing import Any

from autotrader.domain.indicators import calc_atr, normalize_candles


UNIVERSE_V2_HEADERS: list[str] = [
    "Canonical ID",
    "Primary Exchange",
    "Secondary Exchange",
    "Secondary Instrument Key",
    "Bars 1D",
    "Last 1D Date",
    "Price Last",
    "Turnover Med 60D",
    "ATR 14",
    "ATR Pct 14D",
    "Gap Risk 60D",
    "Beta",
    "Turnover Rank 60D",
    "Liquidity Bucket",
    "Data Quality Flag",
    "Stale Days",
    "Eligible Swing",
    "Eligible Intraday",
    "Disable Reason",
    "Universe Mode",
    "Universe V2 Updated At",
]


@dataclass(frozen=True)
class CanonicalListing:
    canonical_id: str
    symbol: str
    primary_exchange: str
    primary_instrument_key: str
    primary_source_segment: str
    security_type: str
    isin: str
    name: str
    secondary_exchange: str = ""
    secondary_instrument_key: str = ""


@dataclass
class TradabilityStats:
    bars_1d: int = 0
    price_last: float = 0.0
    turnover_med_60d: float = 0.0
    atr_14: float = 0.0
    atr_pct_14d: float = 0.0
    gap_risk_60d: float = 0.0
    beta: float = 0.0
    turnover_rank_60d: int | None = None
    liquidity_bucket: str = ""


@dataclass(frozen=True)
class ModeThresholds:
    swing_topn_turnover_60d: int
    intraday_topn_turnover_60d: int
    min_bars_swing: int
    min_bars_intraday: int
    min_price_mode: float
    max_atr_pct_swing: float
    max_atr_pct_intraday: float
    max_gap_risk_mode: float


@dataclass(frozen=True)
class UniverseControls:
    mode: str
    min_bars_hard: int
    min_price_hard: float
    max_gap_risk_hard: float
    max_atr_pct_hard: float
    stale_days_max: int
    mode_thresholds: dict[str, ModeThresholds]

    def active_thresholds(self) -> ModeThresholds:
        m = str(self.mode or "BALANCED").strip().upper()
        return self.mode_thresholds.get(m) or self.mode_thresholds["BALANCED"]


@dataclass(frozen=True)
class EligibilityResult:
    eligible_swing: bool
    eligible_intraday: bool
    disable_reason: str


def canonical_id_from_fields(isin: str, exchange: str, symbol: str) -> str:
    i = str(isin or "").strip().upper()
    if i:
        return i
    return f"{str(exchange or '').strip().upper()}:{str(symbol or '').strip().upper()}"


def choose_primary_listing(rows: list[dict[str, Any]]) -> CanonicalListing | None:
    if not rows:
        return None
    sorted_rows = sorted(
        rows,
        key=lambda r: (0 if str(r.get("exchange", "")).upper() == "NSE" else 1, str(r.get("symbol", ""))),
    )
    primary = sorted_rows[0]
    secondary = next((r for r in sorted_rows[1:] if str(r.get("exchange", "")).upper() != str(primary.get("exchange", "")).upper()), None)
    return CanonicalListing(
        canonical_id=str(primary.get("canonical_id", "")),
        symbol=str(primary.get("symbol", "")),
        primary_exchange=str(primary.get("exchange", "")),
        primary_instrument_key=str(primary.get("instrument_key", "")),
        primary_source_segment=str(primary.get("source_segment", "")),
        security_type=str(primary.get("security_type", "")),
        isin=str(primary.get("isin", "")),
        name=str(primary.get("name", "")),
        secondary_exchange=str(secondary.get("exchange", "")) if secondary else "",
        secondary_instrument_key=str(secondary.get("instrument_key", "")) if secondary else "",
    )


def compute_tradability_stats(candles: list[list[Any]]) -> TradabilityStats:
    norm = normalize_candles(candles)
    if not norm:
        return TradabilityStats()

    bars = len(norm)
    price_last = float(norm[-1][4] or 0.0)
    turnovers = [float(c[4]) * float(c[5]) for c in norm]
    last_60_turnovers = turnovers[-60:] if len(turnovers) >= 60 else turnovers
    turnover_med_60d = float(statistics.median(last_60_turnovers)) if last_60_turnovers else 0.0

    atr_window = norm[-252:] if len(norm) > 252 else norm
    atr_14 = float(calc_atr(atr_window, period=14) or 0.0) if len(atr_window) >= 2 else 0.0
    atr_pct = float((atr_14 / price_last) if price_last > 0 else 0.0)

    gap_values: list[float] = []
    start_idx = max(1, len(norm) - 60)
    for i in range(start_idx, len(norm)):
        prev_close = float(norm[i - 1][4] or 0.0)
        o = float(norm[i][1] or 0.0)
        if prev_close > 0:
            gap_values.append(abs((o / prev_close) - 1.0))
    gap_risk_60d = float(sum(gap_values) / len(gap_values)) if gap_values else 0.0

    return TradabilityStats(
        bars_1d=bars,
        price_last=price_last,
        turnover_med_60d=turnover_med_60d,
        atr_14=atr_14,
        atr_pct_14d=atr_pct,
        gap_risk_60d=gap_risk_60d,
    )


def compute_beta(stock_candles: list[list[Any]], nifty_candles: list[list[Any]], lookback_days: int = 90) -> float:
    """Compute beta of stock vs Nifty50 using daily close returns over lookback_days."""
    stock_norm = normalize_candles(stock_candles)
    nifty_norm = normalize_candles(nifty_candles)
    if not stock_norm or not nifty_norm:
        return 0.0

    stock_norm = stock_norm[-lookback_days:]
    nifty_norm = nifty_norm[-lookback_days:]

    # Build date→close maps (timestamp prefix YYYY-MM-DD)
    stock_by_date = {c[0][:10]: float(c[4]) for c in stock_norm if c[4]}
    nifty_by_date = {c[0][:10]: float(c[4]) for c in nifty_norm if c[4]}

    common_dates = sorted(set(stock_by_date) & set(nifty_by_date))
    if len(common_dates) < 20:
        return 0.0

    stock_closes = [stock_by_date[d] for d in common_dates]
    nifty_closes = [nifty_by_date[d] for d in common_dates]

    stock_rets = [(stock_closes[i] / stock_closes[i - 1]) - 1.0 for i in range(1, len(stock_closes))]
    nifty_rets = [(nifty_closes[i] / nifty_closes[i - 1]) - 1.0 for i in range(1, len(nifty_closes))]

    n = len(stock_rets)
    if n < 10:
        return 0.0

    mean_s = sum(stock_rets) / n
    mean_n = sum(nifty_rets) / n
    cov = sum((stock_rets[i] - mean_s) * (nifty_rets[i] - mean_n) for i in range(n)) / n
    var_n = sum((r - mean_n) ** 2 for r in nifty_rets) / n

    if var_n == 0.0:
        return 0.0
    return round(cov / var_n, 4)


def assign_turnover_rank_and_bucket(stats_by_symbol: dict[str, TradabilityStats]) -> None:
    ranked = [
        (sym, float(stats.turnover_med_60d))
        for sym, stats in stats_by_symbol.items()
        if math.isfinite(float(stats.turnover_med_60d)) and float(stats.turnover_med_60d) > 0
    ]
    ranked.sort(key=lambda x: (-x[1], x[0]))
    n = len(ranked)
    if n <= 0:
        return
    # Use cross-sectional quartiles for a stable A/B/C/D liquidity bucketing.
    q = max(1, math.ceil(n / 4))

    for idx, (sym, _) in enumerate(ranked, start=1):
        if idx <= q:
            bucket = "A"
        elif idx <= q * 2:
            bucket = "B"
        elif idx <= q * 3:
            bucket = "C"
        else:
            bucket = "D"
        stats = stats_by_symbol[sym]
        stats.turnover_rank_60d = idx
        stats.liquidity_bucket = bucket


def classify_eligibility(
    *,
    stats: TradabilityStats,
    data_quality_flag: str,
    stale_days: int,
    controls: UniverseControls,
    suspended_or_delisted: bool = False,
    enabled: bool = True,
) -> EligibilityResult:
    q = str(data_quality_flag or "").strip().upper()
    t = controls.active_thresholds()

    checks_hard: list[tuple[bool, str]] = [
        (not bool(enabled), "ROW_DISABLED"),
        (bool(suspended_or_delisted), "DELISTED_OR_SUSPENDED"),
        (q == "MISSING", "DATA_MISSING"),
        (q == "INVALID_KEY", "INVALID_INSTRUMENT_KEY"),
        (q == "STALE", "STALE_1D_CANDLE"),
        (int(stale_days) > int(controls.stale_days_max), "STALE_DAYS_EXCEEDED"),
        (int(stats.bars_1d) < int(controls.min_bars_hard), "BARS_LT_MIN_HARD"),
        (float(stats.price_last) < float(controls.min_price_hard), "PRICE_LT_MIN_HARD"),
        (float(stats.atr_pct_14d) > float(controls.max_atr_pct_hard), "ATR_PCT_GT_MAX_HARD"),
        (float(stats.gap_risk_60d) > float(controls.max_gap_risk_hard), "GAP_RISK_GT_MAX_HARD"),
    ]
    for failed, reason in checks_hard:
        if failed:
            return EligibilityResult(False, False, reason)

    rank = stats.turnover_rank_60d
    checks_swing: list[tuple[bool, str]] = [
        (rank is None, "TURNOVER_RANK_MISSING"),
        (rank is not None and rank > int(t.swing_topn_turnover_60d), "SWING_TOPN_FAIL"),
        (int(stats.bars_1d) < int(t.min_bars_swing), "SWING_MIN_BARS_FAIL"),
        (float(stats.price_last) < float(t.min_price_mode), "SWING_MIN_PRICE_FAIL"),
        (float(stats.atr_pct_14d) > float(t.max_atr_pct_swing), "SWING_MAX_ATR_FAIL"),
        (float(stats.gap_risk_60d) > float(t.max_gap_risk_mode), "SWING_MAX_GAP_FAIL"),
    ]
    for failed, reason in checks_swing:
        if failed:
            return EligibilityResult(False, False, reason)

    checks_intraday: list[tuple[bool, str]] = [
        (rank is None, "TURNOVER_RANK_MISSING"),
        (rank is not None and rank > int(t.intraday_topn_turnover_60d), "INTRADAY_TOPN_FAIL"),
        (int(stats.bars_1d) < int(t.min_bars_intraday), "INTRADAY_MIN_BARS_FAIL"),
        (float(stats.atr_pct_14d) > float(t.max_atr_pct_intraday), "INTRADAY_MAX_ATR_FAIL"),
    ]
    for failed, reason in checks_intraday:
        if failed:
            return EligibilityResult(True, False, reason)

    return EligibilityResult(True, True, "")
