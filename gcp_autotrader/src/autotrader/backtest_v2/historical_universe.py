"""Historical Universe — reconstruct per-date eligibility using production
pure functions from `services/universe_v2.py`.

For any historical date:
  1. Truncate each symbol's daily candles to that date
  2. Call `compute_tradability_stats(candles)` (production pure function)
  3. Call `assign_turnover_rank_and_bucket(stats_by_symbol)` (production)
  4. Call `classify_eligibility(stats, controls)` per symbol (production)
  5. Return list of "universe rows" matching live's Firestore format

These rows then feed `build_watchlist` to get the actual watchlist live
would have produced — same code, same data, same outputs.

Designed for caching: the same date's universe should be computed once
and reused across symbols/setups.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from autotrader.backtest_v2.data import HistoricalDataset
from autotrader.services.universe_v2 import (
    ModeThresholds,
    TradabilityStats,
    UniverseControls,
    assign_turnover_rank_and_bucket,
    classify_eligibility,
    compute_tradability_stats,
)

logger = logging.getLogger(__name__)


# Match the production UNIVERSE_V2_CONFIG_DEFAULTS for BALANCED mode.
# These come from services/universe_service.py:49-80 in production.
_BALANCED_MODE = ModeThresholds(
    swing_topn_turnover_60d=1000,      # top 1000 by turnover qualify for swing
    intraday_topn_turnover_60d=500,    # top 500 by turnover qualify for intraday
    min_bars_swing=200,                 # need 200+ days of history for swing
    min_bars_intraday=200,
    min_price_mode=50.0,                # ≥₹50 to filter penny stocks
    max_atr_pct_swing=0.06,             # ≤6% daily ATR for swing
    max_atr_pct_intraday=0.06,
    max_gap_risk_mode=0.05,             # ≤5% avg overnight gap
)

_DEFAULT_CONTROLS = UniverseControls(
    mode="BALANCED",
    min_bars_hard=90,                   # hard floor
    min_price_hard=20.0,
    max_gap_risk_hard=0.10,
    max_atr_pct_hard=0.12,
    stale_days_max=14,
    mode_thresholds={"BALANCED": _BALANCED_MODE},
)


@dataclass
class UniverseRow:
    """Approximates the production universe row format."""
    symbol: str
    enabled: bool
    fresh: bool
    eligible_swing: bool
    eligible_intraday: bool
    turnover_rank_60d: int | None
    liquidity_bucket: str
    sector: str
    disable_reason: str
    bars_1d: int
    price_last: float
    turnover_med_60d: float
    atr_pct_14d: float
    gap_risk_60d: float

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict matching live universe row schema for build_watchlist."""
        return {
            "symbol": self.symbol,
            "enabled": self.enabled,
            "fresh": self.fresh,
            "eligibleSwing": self.eligible_swing,
            "eligibleIntraday": self.eligible_intraday,
            "turnoverRank60D": self.turnover_rank_60d,
            "liquidityBucket": self.liquidity_bucket,
            "sector": self.sector,
            "disableReason": self.disable_reason,
            "bars1d": self.bars_1d,
            "priceLast": self.price_last,
            "turnoverMed60D": self.turnover_med_60d,
            "atrPct14D": self.atr_pct_14d,
            "gapRisk60D": self.gap_risk_60d,
        }


class HistoricalUniverse:
    """Compute eligibility for any historical date using production pure functions.

    Aggressive caching: results cached by date so the same date is computed
    once across all subsequent queries.
    """

    def __init__(
        self,
        dataset: HistoricalDataset,
        *,
        symbols: list[str] | None = None,
        controls: UniverseControls | None = None,
    ) -> None:
        self.ds = dataset
        self.controls = controls or _DEFAULT_CONTROLS
        # If no symbols given, discover from GCS daily cache
        if symbols is None:
            symbols = dataset.list_daily_symbols(limit=10000)
        self.symbols = symbols
        self._candle_cache: dict[str, list[list[Any]]] = {}
        self._universe_cache: dict[str, list[UniverseRow]] = {}

    def _candles(self, symbol: str) -> list[list[Any]]:
        if symbol not in self._candle_cache:
            self._candle_cache[symbol] = self.ds.daily_candles(symbol)
        return self._candle_cache[symbol]

    def universe_for_date(self, as_of: str) -> list[UniverseRow]:
        """Return the universe (with eligibility flags) for `as_of` date.

        This is the EXACT live mechanism, recomputed from candles.
        """
        if as_of in self._universe_cache:
            return self._universe_cache[as_of]

        # Step 1: per-symbol tradability stats from candles truncated to as_of
        stats_by_symbol: dict[str, TradabilityStats] = {}
        candles_by_symbol: dict[str, list[list[Any]]] = {}
        for sym in self.symbols:
            all_candles = self._candles(sym)
            truncated = [c for c in all_candles if str(c[0])[:10] <= as_of]
            if not truncated:
                continue
            stats = compute_tradability_stats(truncated)
            stats_by_symbol[sym] = stats
            candles_by_symbol[sym] = truncated

        # Step 2: turnover ranking + buckets (cross-sectional)
        assign_turnover_rank_and_bucket(stats_by_symbol)

        # Step 3: eligibility per symbol
        rows: list[UniverseRow] = []
        for sym, stats in stats_by_symbol.items():
            # Compute staleness: how many calendar days since last candle?
            truncated = candles_by_symbol[sym]
            last_ts = str(truncated[-1][0])[:10]
            try:
                from datetime import date as _date_cls
                last_d = _date_cls.fromisoformat(last_ts)
                as_d = _date_cls.fromisoformat(as_of)
                stale_days = (as_d - last_d).days
            except Exception:
                stale_days = 0

            # Data quality: FRESH if recent, STALE otherwise
            quality = "FRESH" if stale_days <= self.controls.stale_days_max else "STALE"

            result = classify_eligibility(
                stats=stats,
                data_quality_flag=quality,
                stale_days=stale_days,
                controls=self.controls,
                suspended_or_delisted=False,
                enabled=True,
            )

            rows.append(UniverseRow(
                symbol=sym,
                enabled=True,
                fresh=(quality == "FRESH"),
                eligible_swing=result.eligible_swing,
                eligible_intraday=result.eligible_intraday,
                turnover_rank_60d=stats.turnover_rank_60d,
                liquidity_bucket=stats.liquidity_bucket,
                sector="",  # TODO: load sector mapping; for now empty (live mapping is current-only)
                disable_reason=result.disable_reason,
                bars_1d=stats.bars_1d,
                price_last=stats.price_last,
                turnover_med_60d=stats.turnover_med_60d,
                atr_pct_14d=stats.atr_pct_14d,
                gap_risk_60d=stats.gap_risk_60d,
            ))

        self._universe_cache[as_of] = rows
        return rows

    def watchlist_swing_for_date(self, as_of: str, target_size: int = 300) -> list[str]:
        """Build the SWING watchlist for `as_of`.

        Mimics live's build_watchlist for swing: take eligibleSwing symbols
        ranked by turnover, return top N. Does NOT apply brain regime filter
        here — caller decides regime separately (different setups respond
        differently to regime).
        """
        rows = self.universe_for_date(as_of)
        eligible = [r for r in rows if r.eligible_swing and r.fresh and r.enabled]
        eligible.sort(key=lambda r: (r.turnover_rank_60d or 999999))
        return [r.symbol for r in eligible[:target_size]]

    def watchlist_intraday_for_date(self, as_of: str, target_size: int = 150) -> list[str]:
        rows = self.universe_for_date(as_of)
        eligible = [r for r in rows if r.eligible_intraday and r.fresh and r.enabled]
        eligible.sort(key=lambda r: (r.turnover_rank_60d or 999999))
        return [r.symbol for r in eligible[:target_size]]
