from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from typing import Any, Callable

from autotrader.time_utils import IST, parse_any_ts


@dataclass
class MarketBreadthService:
    liquidity_turnover_rank_max: int = 1000
    min_bars: int = 60

    @staticmethod
    def _clip01(v: float) -> float:
        if not math.isfinite(float(v)):
            return 0.0
        return max(0.0, min(1.0, float(v)))

    @staticmethod
    def _norm(v: float, lo: float, hi: float) -> float:
        if hi <= lo:
            return 0.0
        return MarketBreadthService._clip01((float(v) - float(lo)) / (float(hi) - float(lo)))

    @staticmethod
    def _ema_last(closes: list[float], period: int) -> float:
        if not closes:
            return 0.0
        p = max(1, int(period))
        if len(closes) <= p:
            return float(closes[-1])
        k = 2.0 / (p + 1.0)
        ema = float(closes[0])
        for c in closes[1:]:
            ema = (float(c) * k) + (ema * (1.0 - k))
        return float(ema)

    @staticmethod
    def _daily_close(candles: list[list[object]]) -> float:
        if not candles:
            return 0.0
        try:
            return float(candles[-1][4] or 0.0)
        except Exception:
            return 0.0

    @staticmethod
    def _is_eligible_liquidity(row: dict[str, Any], *, turnover_rank_max: int) -> bool:
        if not bool(row.get("enabled")):
            return False
        if not bool(row.get("fresh")):
            return False
        if not (bool(row.get("eligibleSwing")) or bool(row.get("eligibleIntraday"))):
            return False
        liq = str(row.get("liquidityBucket") or "").strip().upper()
        if liq in {"A", "B"}:
            return True
        try:
            rank = int(row.get("turnoverRank60D") or 0)
        except Exception:
            rank = 0
        return rank > 0 and rank <= int(turnover_rank_max)

    def compute_breadth_snapshot(
        self,
        *,
        universe_rows: list[dict[str, Any]],
        expected_lcd: str,
        daily_candle_fetcher: Callable[[dict[str, Any], str], list[list[object]]],
    ) -> dict[str, Any]:
        qualified = [
            r
            for r in universe_rows
            if self._is_eligible_liquidity(r, turnover_rank_max=self.liquidity_turnover_rank_max)
        ]
        total = len(qualified)
        if total == 0:
            return {
                "score": 35.0,
                "qualifiedCount": 0,
                "aboveEma20Pct": 0.0,
                "aboveEma50Pct": 0.0,
                "positive20dPct": 0.0,
                "near20dHighPct": 0.0,
                "near20dLowPct": 0.0,
                "medianRet20Pct": 0.0,
                "advanceDeclineRatio": 0.0,
                "volWeightedAdvanceDeclineRatio": 0.0,
                "sectorBreadthPct": 0.0,
                "sectorCoveragePct": 0.0,
                "dataQuality": "LOW",
                "reason": "NO_LIQUIDITY_QUALIFIED_ROWS",
            }

        above20 = 0
        above50 = 0
        positive20 = 0
        near_high = 0
        near_low = 0
        advances = 0
        declines = 0
        vol_adv = 0.0
        vol_dec = 0.0
        ret20_vals: list[float] = []
        sector_signal: dict[str, list[float]] = {}
        processed = 0

        for row in qualified:
            candles = daily_candle_fetcher(row, expected_lcd)
            if len(candles) < self.min_bars:
                continue
            closes = [float(c[4] or 0.0) for c in candles if len(c) >= 6]
            vols = [float(c[5] or 0.0) for c in candles if len(c) >= 6]
            if len(closes) < 21 or len(vols) < 21:
                continue
            processed += 1
            close = float(closes[-1])
            ema20 = self._ema_last(closes, 20)
            ema50 = self._ema_last(closes, 50)
            if close > ema20:
                above20 += 1
            if close > ema50:
                above50 += 1

            prev20 = float(closes[-21]) if float(closes[-21]) > 0 else 0.0
            ret20 = ((close / prev20) - 1.0) if prev20 > 0 else 0.0
            ret20_vals.append(ret20)
            if ret20 >= 0:
                positive20 += 1
                advances += 1
                vol_adv += max(0.0, float(vols[-1]))
            else:
                declines += 1
                vol_dec += max(0.0, float(vols[-1]))

            high20 = max(float(c[2] or 0.0) for c in candles[-20:])
            low20 = min(float(c[3] or 0.0) for c in candles[-20:])
            if high20 > 0 and ((high20 - close) / high20) <= 0.02:
                near_high += 1
            if low20 > 0 and ((close - low20) / low20) <= 0.02:
                near_low += 1

            sector = str(row.get("sector") or "UNKNOWN").strip().upper()
            if sector and sector != "UNKNOWN":
                sector_signal.setdefault(sector, []).append(ret20)

        denom = max(1, processed)
        above20_pct = (above20 * 100.0) / denom
        above50_pct = (above50 * 100.0) / denom
        positive20_pct = (positive20 * 100.0) / denom
        near_high_pct = (near_high * 100.0) / denom
        near_low_pct = (near_low * 100.0) / denom
        median_ret20 = float(statistics.median(ret20_vals)) if ret20_vals else 0.0
        ad_ratio = (advances / max(1, declines)) if processed else 0.0
        vw_ad_ratio = (vol_adv / max(1e-9, vol_dec)) if processed else 0.0

        sector_scores: list[float] = []
        for _, vals in sector_signal.items():
            if not vals:
                continue
            pos = sum(1 for x in vals if x >= 0.0)
            sector_scores.append((pos * 100.0) / len(vals))
        sector_cov_pct = (len(sector_signal) * 100.0 / max(1, processed)) if processed else 0.0
        sector_breadth_pct = float(statistics.mean(sector_scores)) if sector_scores else 50.0

        score01 = (
            0.18 * self._norm(above20_pct, 30.0, 80.0)
            + 0.18 * self._norm(above50_pct, 25.0, 75.0)
            + 0.17 * self._norm(positive20_pct, 30.0, 80.0)
            + 0.10 * self._norm(near_high_pct, 10.0, 45.0)
            + 0.10 * (1.0 - self._norm(near_low_pct, 10.0, 45.0))
            + 0.12 * self._norm(median_ret20, -0.05, 0.08)
            + 0.10 * self._norm(ad_ratio, 0.6, 1.8)
            + 0.05 * self._norm(vw_ad_ratio, 0.6, 1.8)
        )
        if sector_scores:
            score01 = (score01 * 0.95) + (0.05 * self._norm(sector_breadth_pct, 35.0, 80.0))
        score = max(0.0, min(100.0, score01 * 100.0))

        quality = "HIGH" if processed >= 120 else ("MEDIUM" if processed >= 60 else "LOW")
        return {
            "score": float(round(score, 2)),
            "qualifiedCount": int(total),
            "processedCount": int(processed),
            "aboveEma20Pct": float(round(above20_pct, 2)),
            "aboveEma50Pct": float(round(above50_pct, 2)),
            "positive20dPct": float(round(positive20_pct, 2)),
            "near20dHighPct": float(round(near_high_pct, 2)),
            "near20dLowPct": float(round(near_low_pct, 2)),
            "medianRet20Pct": float(round(median_ret20 * 100.0, 3)),
            "advanceDeclineRatio": float(round(ad_ratio, 4)),
            "volWeightedAdvanceDeclineRatio": float(round(vw_ad_ratio, 4)),
            "sectorBreadthPct": float(round(sector_breadth_pct, 2)),
            "sectorCoveragePct": float(round(sector_cov_pct, 2)),
            "dataQuality": quality,
            "reason": "",
        }
