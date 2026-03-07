from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

from autotrader.time_utils import IST, parse_any_ts


@dataclass
class MarketLeadershipService:
    leader_sample_size: int = 120
    min_daily_bars: int = 40

    @staticmethod
    def _clip01(v: float) -> float:
        if not math.isfinite(float(v)):
            return 0.0
        return max(0.0, min(1.0, float(v)))

    @staticmethod
    def _norm(v: float, lo: float, hi: float) -> float:
        if hi <= lo:
            return 0.0
        return MarketLeadershipService._clip01((float(v) - float(lo)) / (float(hi) - float(lo)))

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
    def _today_intraday_bars(bars: list[list[object]], now_i: datetime) -> list[list[object]]:
        out: list[list[object]] = []
        td = now_i.astimezone(IST).date()
        for c in bars:
            ts = parse_any_ts(c[0]) if c else None
            if ts is None:
                continue
            if ts.astimezone(IST).date() == td:
                out.append(c)
        out.sort(key=lambda x: str(x[0]))
        return out

    def compute_leadership_snapshot(
        self,
        *,
        universe_rows: list[dict[str, Any]],
        expected_lcd: str,
        now_i: datetime,
        daily_candle_fetcher: Callable[[dict[str, Any], str], list[list[object]]],
        intraday_candle_fetcher: Callable[[dict[str, Any], str, datetime], list[list[object]]],
        intraday_timeframe: str = "5m",
    ) -> dict[str, Any]:
        leaders = [
            r
            for r in universe_rows
            if bool(r.get("enabled"))
            and bool(r.get("fresh"))
            and (bool(r.get("eligibleSwing")) or bool(r.get("eligibleIntraday")))
        ]
        leaders.sort(
            key=lambda r: (
                int(r.get("turnoverRank60D") or 999999),
                -float(r.get("turnoverMed60D") or 0.0),
                str(r.get("symbol") or ""),
            )
        )
        leaders = leaders[: max(20, int(self.leader_sample_size))]
        if not leaders:
            return {
                "score": 40.0,
                "breakoutHoldRate": 0.0,
                "failedBreakoutRate": 0.0,
                "gapFollowThroughRate": 0.0,
                "openDriveContinuationRate": 0.0,
                "closeStrengthLeaders": 0.0,
                "leaderPersistenceRate": 0.0,
                "leadersProcessed": 0,
                "reason": "NO_LEADERS",
            }

        breakout_total = 0
        breakout_hold = 0
        breakout_fail = 0
        gap_total = 0
        gap_follow = 0
        open_drive_total = 0
        open_drive_follow = 0
        close_strength_vals: list[float] = []
        persistence_total = 0
        persistence_pass = 0
        processed = 0

        for row in leaders:
            daily = daily_candle_fetcher(row, expected_lcd)
            if len(daily) < self.min_daily_bars:
                continue
            processed += 1
            closes = [float(c[4] or 0.0) for c in daily]
            highs = [float(c[2] or 0.0) for c in daily]
            lows = [float(c[3] or 0.0) for c in daily]
            opens = [float(c[1] or 0.0) for c in daily]
            close_now = float(closes[-1] if closes else 0.0)
            if close_now <= 0:
                continue
            high20_prev = max(highs[-21:-1]) if len(highs) >= 21 else max(highs[:-1]) if len(highs) > 1 else 0.0
            if high20_prev > 0 and close_now > high20_prev:
                breakout_total += 1
                rng = max(1e-9, highs[-1] - lows[-1])
                close_strength = (close_now - lows[-1]) / rng
                if close_strength >= 0.6:
                    breakout_hold += 1
                else:
                    breakout_fail += 1

            prev_close = float(closes[-2] if len(closes) >= 2 else close_now)
            gap_pct = ((opens[-1] / prev_close) - 1.0) if prev_close > 0 else 0.0
            if abs(gap_pct) >= 0.008:
                gap_total += 1
                body_follow = (close_now - opens[-1]) if gap_pct > 0 else (opens[-1] - close_now)
                if body_follow > 0:
                    gap_follow += 1

            rng = max(1e-9, highs[-1] - lows[-1])
            close_strength_vals.append((close_now - lows[-1]) / rng)

            ema20 = self._ema_last(closes, 20)
            ema50 = self._ema_last(closes, 50)
            persistence_total += 1
            if close_now > ema20 and close_now > ema50:
                persistence_pass += 1

            try:
                bars = intraday_candle_fetcher(row, intraday_timeframe, now_i)
            except Exception:
                bars = []
            today = self._today_intraday_bars(bars, now_i)
            if len(today) >= 6:
                open_drive_total += 1
                first3 = today[:3]
                last3 = today[-3:]
                od_high = max(float(c[2] or 0.0) for c in first3)
                od_low = min(float(c[3] or 0.0) for c in first3)
                close_last = float(last3[-1][4] or 0.0)
                if close_last > od_high or close_last < od_low:
                    open_drive_follow += 1

        hold_rate = (breakout_hold / max(1, breakout_total)) if breakout_total else 0.0
        fail_rate = (breakout_fail / max(1, breakout_total)) if breakout_total else 0.0
        gap_follow_rate = (gap_follow / max(1, gap_total)) if gap_total else 0.0
        open_drive_rate = (open_drive_follow / max(1, open_drive_total)) if open_drive_total else 0.0
        close_strength = float(statistics.mean(close_strength_vals)) if close_strength_vals else 0.0
        persistence_rate = (persistence_pass / max(1, persistence_total)) if persistence_total else 0.0

        score01 = (
            0.26 * self._clip01(hold_rate)
            + 0.18 * (1.0 - self._clip01(fail_rate))
            + 0.16 * self._clip01(gap_follow_rate)
            + 0.16 * self._clip01(open_drive_rate)
            + 0.12 * self._clip01(close_strength)
            + 0.12 * self._clip01(persistence_rate)
        )
        score = max(0.0, min(100.0, score01 * 100.0))
        return {
            "score": float(round(score, 2)),
            "breakoutHoldRate": float(round(hold_rate * 100.0, 2)),
            "failedBreakoutRate": float(round(fail_rate * 100.0, 2)),
            "gapFollowThroughRate": float(round(gap_follow_rate * 100.0, 2)),
            "openDriveContinuationRate": float(round(open_drive_rate * 100.0, 2)),
            "closeStrengthLeaders": float(round(close_strength * 100.0, 2)),
            "leaderPersistenceRate": float(round(persistence_rate * 100.0, 2)),
            "leadersProcessed": int(processed),
            "reason": "",
        }
