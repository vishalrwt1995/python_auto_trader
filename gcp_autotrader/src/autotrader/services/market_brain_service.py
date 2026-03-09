from __future__ import annotations

import logging
import math
import statistics
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

from autotrader.adapters.firestore_state import FirestoreStateStore
from autotrader.adapters.gcs_store import GoogleCloudStorageStore
from autotrader.domain.models import MarketBrainState, MarketPolicy, RegimeSnapshot
from autotrader.services.market_breadth_service import MarketBreadthService
from autotrader.services.market_leadership_service import MarketLeadershipService
from autotrader.services.market_policy_service import MarketPolicyService
from autotrader.services.regime_service import MarketRegimeService
from autotrader.time_utils import IST, now_ist, parse_any_ts

logger = logging.getLogger(__name__)


@dataclass
class MarketBrainService:
    regime_service: MarketRegimeService
    universe_service: Any
    gcs: GoogleCloudStorageStore
    state: FirestoreStateStore
    breadth_service: MarketBreadthService = field(default_factory=MarketBreadthService)
    leadership_service: MarketLeadershipService = field(default_factory=MarketLeadershipService)
    policy_service: MarketPolicyService = field(default_factory=MarketPolicyService)
    latest_state_path: str = "state/market_brain/latest.json"
    history_prefix: str = "state/market_brain/history"
    _last_context: dict[str, Any] = field(default_factory=dict, init=False, repr=False)

    @staticmethod
    def _clip(v: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, float(v)))

    @staticmethod
    def _norm(v: float, lo: float, hi: float) -> float:
        if hi <= lo:
            return 0.0
        return MarketBrainService._clip((float(v) - float(lo)) / (float(hi) - float(lo)), 0.0, 1.0)

    @staticmethod
    def _phase_from_clock(now_i: datetime) -> str:
        if now_i.weekday() >= 5:
            return "EOD"
        mins = now_i.hour * 60 + now_i.minute
        if mins < 555:
            return "PREMARKET"
        if mins < 615:
            return "POST_OPEN"
        if mins <= 930:
            return "LIVE"
        return "EOD"

    @staticmethod
    def _state_from_dict(payload: dict[str, Any] | None) -> MarketBrainState | None:
        if not isinstance(payload, dict):
            return None
        asof = str(payload.get("asof_ts") or "").strip()
        if not asof:
            return None
        return MarketBrainState(
            asof_ts=asof,
            phase=str(payload.get("phase") or "PREMARKET"),  # type: ignore[arg-type]
            regime=str(payload.get("regime") or "RANGE"),  # type: ignore[arg-type]
            participation=str(payload.get("participation") or "MODERATE"),  # type: ignore[arg-type]
            risk_mode=str(payload.get("risk_mode") or "NORMAL"),  # type: ignore[arg-type]
            intraday_state=str(payload.get("intraday_state") or "PREOPEN"),  # type: ignore[arg-type]
            long_bias=float(payload.get("long_bias") or 0.5),
            short_bias=float(payload.get("short_bias") or 0.5),
            size_multiplier=float(payload.get("size_multiplier") or 1.0),
            max_positions_multiplier=float(payload.get("max_positions_multiplier") or 1.0),
            swing_permission=str(payload.get("swing_permission") or "ENABLED"),  # type: ignore[arg-type]
            allowed_strategies=[str(x) for x in (payload.get("allowed_strategies") or []) if str(x).strip()],
            reasons=[str(x) for x in (payload.get("reasons") or []) if str(x).strip()],
            trend_score=float(payload.get("trend_score") or 50.0),
            breadth_score=float(payload.get("breadth_score") or 50.0),
            leadership_score=float(payload.get("leadership_score") or 50.0),
            volatility_stress_score=float(payload.get("volatility_stress_score") or 50.0),
            liquidity_health_score=float(payload.get("liquidity_health_score") or 50.0),
            data_quality_score=float(payload.get("data_quality_score") or 50.0),
        )

    def _read_latest_context(self) -> dict[str, Any]:
        payload = self.state.get_json("market_brain", "latest")
        if isinstance(payload, dict):
            return payload
        payload = self.gcs.read_json(self.latest_state_path, default={})
        return payload if isinstance(payload, dict) else {}

    def read_latest_market_brain_state(self) -> MarketBrainState | None:
        return self._state_from_dict(self._read_latest_context().get("state"))

    def persist_market_brain_state(
        self,
        state: MarketBrainState,
        *,
        context: dict[str, Any] | None = None,
        policy: MarketPolicy | None = None,
    ) -> None:
        payload = {
            "state": asdict(state),
            "context": context or {},
            "policy": asdict(policy) if policy is not None else {},
        }
        self.state.set_json("market_brain", "latest", payload, merge=False)
        self.gcs.write_json(self.latest_state_path, payload)
        asof = parse_any_ts(state.asof_ts) or now_ist()
        d = asof.astimezone(IST).strftime("%Y-%m-%d")
        t = asof.astimezone(IST).strftime("%H%M%S")
        self.gcs.write_json(f"{self.history_prefix}/{d}/{t}.json", payload)

    def _build_rows(self, expected_lcd: str) -> list[dict[str, Any]]:
        rows = self.universe_service._watchlist_v2_candidates(expected_lcd)
        return [r for r in rows if bool(r.get("enabled"))]

    def _daily_fetch(self, row: dict[str, Any], expected_lcd: str) -> list[list[object]]:
        return self.universe_service._watchlist_daily_candles(row, expected_lcd)

    def _intraday_fetch(self, row: dict[str, Any], timeframe: str, now_i: datetime) -> list[list[object]]:
        return self.universe_service._watchlist_intraday_candles(row, timeframe=timeframe, now_i=now_i)

    def compute_breadth_snapshot(self, *, expected_lcd: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
        return self.breadth_service.compute_breadth_snapshot(
            universe_rows=rows,
            expected_lcd=expected_lcd,
            daily_candle_fetcher=lambda row, lcd: self._daily_fetch(row, lcd),
        )

    def compute_leadership_snapshot(self, *, expected_lcd: str, rows: list[dict[str, Any]], now_i: datetime) -> dict[str, Any]:
        return self.leadership_service.compute_leadership_snapshot(
            universe_rows=rows,
            expected_lcd=expected_lcd,
            now_i=now_i,
            daily_candle_fetcher=lambda row, lcd: self._daily_fetch(row, lcd),
            intraday_candle_fetcher=lambda row, timeframe, ts: self._intraday_fetch(row, timeframe, ts),
            intraday_timeframe="5m",
        )

    def _compute_liquidity_health(self, rows: list[dict[str, Any]]) -> tuple[float, dict[str, Any]]:
        if not rows:
            return 35.0, {"eligible": 0, "topLiqPassPct": 0.0, "weakLiqConcentrationPct": 100.0}
        eligible = [r for r in rows if bool(r.get("fresh")) and (bool(r.get("eligibleIntraday")) or bool(r.get("eligibleSwing")))]
        if not eligible:
            return 35.0, {"eligible": 0, "topLiqPassPct": 0.0, "weakLiqConcentrationPct": 100.0}
        top_ranked = [r for r in eligible if int(r.get("turnoverRank60D") or 999999) <= 500]
        top_liq = [r for r in top_ranked if str(r.get("liquidityBucket") or "").upper() in {"A", "B"}]
        weak = [r for r in eligible if str(r.get("liquidityBucket") or "").upper() in {"C", "D", ""}]
        top_pass = (len(top_liq) * 100.0 / max(1, len(top_ranked))) if top_ranked else 0.0
        weak_conc = (len(weak) * 100.0 / max(1, len(eligible)))

        turnover_vals = sorted([float(r.get("turnoverMed60D") or 0.0) for r in eligible if float(r.get("turnoverMed60D") or 0.0) > 0])
        if turnover_vals:
            p20 = turnover_vals[min(len(turnover_vals) - 1, max(0, int(math.floor(0.20 * (len(turnover_vals) - 1)))))]
            p50 = statistics.median(turnover_vals)
            p80 = turnover_vals[min(len(turnover_vals) - 1, max(0, int(math.floor(0.80 * (len(turnover_vals) - 1)))))]
        else:
            p20 = p50 = p80 = 0.0

        ranked_by_turnover = sorted(
            eligible,
            key=lambda x: float(x.get("turnoverMed60D") or 0.0),
            reverse=True,
        )
        top_n = ranked_by_turnover[: max(10, min(30, len(ranked_by_turnover)))]
        top5 = ranked_by_turnover[:5]
        top_n_sum = sum(float(r.get("turnoverMed60D") or 0.0) for r in top_n)
        top5_sum = sum(float(r.get("turnoverMed60D") or 0.0) for r in top5)
        top5_conc = (top5_sum / max(1e-9, top_n_sum)) if top_n else 1.0

        bucket_counts = {
            "A": sum(1 for r in eligible if str(r.get("liquidityBucket") or "").upper() == "A"),
            "B": sum(1 for r in eligible if str(r.get("liquidityBucket") or "").upper() == "B"),
            "C": sum(1 for r in eligible if str(r.get("liquidityBucket") or "").upper() == "C"),
            "D": sum(1 for r in eligible if str(r.get("liquidityBucket") or "").upper() in {"D", ""}),
        }
        entropy = 0.0
        total = float(len(eligible))
        if total > 0:
            for c in bucket_counts.values():
                if c <= 0:
                    continue
                p = c / total
                entropy += -p * math.log(p)
            entropy /= math.log(4.0)

        fallback_only = [
            r for r in eligible if bool(r.get("eligibleSwing")) and (not bool(r.get("eligibleIntraday")))
        ]
        fallback_only_pct = (len(fallback_only) * 100.0 / max(1, len(eligible)))

        top_liq_component = self._norm(top_pass, 40.0, 92.0)
        weak_component = 1.0 - self._norm(weak_conc, 18.0, 68.0)
        turnover_component = (0.55 * self._norm(p50, 2.0e7, 7.0e8)) + (0.45 * self._norm(p20, 5.0e6, 1.5e8))
        concentration_component = 1.0 - self._norm(top5_conc, 0.28, 0.70)
        entropy_component = self._norm(entropy, 0.35, 0.95)
        fallback_penalty = self._norm(fallback_only_pct, 12.0, 55.0)
        stress_regime_execution_penalty = self._norm(weak_conc, 28.0, 75.0) * (1.0 - top_liq_component)

        score01 = (
            (0.27 * top_liq_component)
            + (0.21 * weak_component)
            + (0.20 * turnover_component)
            + (0.16 * concentration_component)
            + (0.16 * entropy_component)
            - (0.11 * fallback_penalty)
            - (0.09 * stress_regime_execution_penalty)
        )
        score = float(round(self._clip(score01 * 100.0, 0.0, 100.0), 2))
        return score, {
            "eligible": len(eligible),
            "topLiqPassPct": round(top_pass, 2),
            "weakLiqConcentrationPct": round(weak_conc, 2),
            "candidateTurnoverPercentiles": {
                "p20": round(float(p20), 2),
                "p50": round(float(p50), 2),
                "p80": round(float(p80), 2),
            },
            "top5LiquidityConcentrationPct": round(float(top5_conc * 100.0), 2),
            "liquidityDistributionEntropy": round(float(entropy), 4),
            "fallbackOnlyCandidatePct": round(float(fallback_only_pct), 2),
            "stressRegimeExecutionPenalty": round(float(stress_regime_execution_penalty), 4),
        }

    def _compute_data_quality(
        self,
        *,
        rows: list[dict[str, Any]],
        breadth: dict[str, Any],
        leadership: dict[str, Any],
        regime_ctx: dict[str, Any],
    ) -> tuple[float, dict[str, Any]]:
        total = len(rows)
        fresh = sum(1 for r in rows if bool(r.get("fresh")))
        decision = sum(1 for r in rows if bool(r.get("decisionPresent")))
        fresh_pct = (fresh * 100.0 / max(1, total)) if total else 0.0
        decision_pct = (decision * 100.0 / max(1, total)) if total else 0.0
        breadth_processed = float(breadth.get("processedCount") or 0.0)
        leaders_processed = float(leadership.get("leadersProcessed") or 0.0)
        intraday_bars = float(((regime_ctx.get("intraday") or {}).get("bars") or 0.0))
        score01 = (
            0.35 * self._norm(fresh_pct, 70.0, 100.0)
            + 0.25 * self._norm(decision_pct, 70.0, 100.0)
            + 0.15 * self._norm(breadth_processed, 50.0, 400.0)
            + 0.15 * self._norm(leaders_processed, 30.0, 120.0)
            + 0.10 * self._norm(intraday_bars, 0.0, 75.0)
        )
        base_quality_score = float(round(score01 * 100.0, 2))

        now_i = now_ist()
        phase_hint = str(regime_ctx.get("_phaseHint") or "").strip().upper()
        if phase_hint in {"PREMARKET", "POST_OPEN", "LIVE", "EOD"}:
            phase = phase_hint
        else:
            phase = self._phase_from_clock(now_i.astimezone(IST))
        is_live_window = phase in {"POST_OPEN", "LIVE"}
        watchlist_ts = parse_any_ts(self.state.get_runtime_prop("runtime:watchlist_last_run_ts", ""))
        scanner_ts = parse_any_ts(self.state.get_runtime_prop("runtime:scanner_last_run_ts", ""))
        signals_ts = parse_any_ts(self.state.get_runtime_prop("runtime:signals_last_write_ts", ""))
        phase2_eligible_count = int(self.state.get_runtime_prop("runtime:watchlist_last_phase2_eligible_count", "0") or "0")
        phase2_branch_entered = str(self.state.get_runtime_prop("runtime:watchlist_last_phase2_branch_entered", "")).strip().upper() in {"Y", "YES", "TRUE", "1"}

        intraday_phase2_penalty = 0.0
        pipeline_alignment_penalty = 0.0
        stale_writer_penalty = 0.0
        writer_age_min: dict[str, float | None] = {"watchlist": None, "scanner": None, "signals": None}

        if is_live_window:
            if intraday_bars < 6.0:
                intraday_phase2_penalty += 7.0
            if phase2_eligible_count <= 0:
                intraday_phase2_penalty += 14.0 if (phase2_branch_entered or intraday_bars >= 6.0) else 8.0

            writer_ts = [("watchlist", watchlist_ts), ("scanner", scanner_ts), ("signals", signals_ts)]
            observed: list[datetime] = []
            for name, ts in writer_ts:
                if ts is None:
                    continue
                ti = ts.astimezone(IST)
                observed.append(ti)
                age_min = max(0.0, (now_i.astimezone(IST) - ti).total_seconds() / 60.0)
                writer_age_min[name] = round(age_min, 2)
                if name == "scanner":
                    stale_writer_penalty += 16.0 * self._norm(age_min, 12.0, 90.0)
                elif name == "watchlist":
                    stale_writer_penalty += 10.0 * self._norm(age_min, 15.0, 120.0)
                else:
                    stale_writer_penalty += 8.0 * self._norm(age_min, 20.0, 120.0)
            if scanner_ts is None:
                stale_writer_penalty += 12.0

            if len(observed) >= 2:
                spread_min = max(0.0, (max(observed) - min(observed)).total_seconds() / 60.0)
                pipeline_alignment_penalty += 12.0 * self._norm(spread_min, 5.0, 60.0)
            elif len(observed) == 1:
                pipeline_alignment_penalty += 4.0

        score = self._clip(
            base_quality_score - intraday_phase2_penalty - pipeline_alignment_penalty - stale_writer_penalty,
            0.0,
            100.0,
        )
        score = float(round(score, 2))
        return score, {
            "totalRows": total,
            "freshPct": round(fresh_pct, 2),
            "decisionCoveragePct": round(decision_pct, 2),
            "breadthProcessed": int(breadth_processed),
            "leadersProcessed": int(leaders_processed),
            "intradayBars": int(intraday_bars),
            "baseQualityScore": round(base_quality_score, 2),
            "intradayPhase2Penalty": round(float(intraday_phase2_penalty), 2),
            "pipelineAlignmentPenalty": round(float(pipeline_alignment_penalty), 2),
            "staleWriterPenalty": round(float(stale_writer_penalty), 2),
            "phase2EligibleCount": int(phase2_eligible_count),
            "phase2BranchEntered": bool(phase2_branch_entered),
            "writerAgeMin": writer_age_min,
        }

    def _compute_trend_score(self, regime_ctx: dict[str, Any]) -> float:
        daily = regime_ctx.get("daily", {}) if isinstance(regime_ctx.get("daily"), dict) else {}
        close = float(daily.get("close") or 0.0)
        ema50 = float(daily.get("ema50") or 0.0)
        ema200 = float(daily.get("ema200") or 0.0)
        atr_pct = float(daily.get("atrPct") or 0.0)
        atr_med = float(daily.get("atrMedian252") or 0.0)
        trend_up = bool(daily.get("trendUp"))
        trend_down = bool(daily.get("trendDown"))
        spread = ((ema50 - ema200) / close) if close > 0 else 0.0
        close_component = self._norm(close, ema200 * 0.92, ema200 * 1.08) if ema200 > 0 else 0.5
        vol_component = (1.0 - self._norm(atr_pct, atr_med * 0.6, atr_med * 1.8)) if atr_med > 0 else 0.5
        score01 = (
            0.35 * self._norm(spread, -0.06, 0.06)
            + 0.25 * (1.0 if trend_up else (0.0 if trend_down else 0.5))
            + 0.20 * close_component
            + 0.20 * vol_component
        )
        return float(round(self._clip(score01 * 100.0, 0.0, 100.0), 2))

    def _compute_volatility_stress(
        self,
        *,
        regime_ctx: dict[str, Any],
        live_regime: RegimeSnapshot | None,
    ) -> tuple[float, dict[str, Any]]:
        daily = regime_ctx.get("daily", {}) if isinstance(regime_ctx.get("daily"), dict) else {}
        intraday = regime_ctx.get("intraday", {}) if isinstance(regime_ctx.get("intraday"), dict) else {}
        atr_pct = float(daily.get("atrPct") or 0.0)
        atr_med = float(daily.get("atrMedian252") or 0.0)
        range_exp = float(intraday.get("rangeExpansion30m") or 0.0)
        slope = abs(float(intraday.get("vwapSlope") or 0.0))
        vix = float(live_regime.vix) if live_regime is not None else 0.0
        chop = float(getattr(getattr(live_regime, "nifty_structure", None), "chop_risk", 0.0) or 0.0) if live_regime else 0.0
        gap = abs(float(getattr(getattr(live_regime, "nifty_structure", None), "gap_pct", 0.0) or 0.0)) if live_regime else 0.0
        atr_p = (atr_pct / atr_med) if atr_med > 0 else 1.0
        score01 = (
            0.28 * self._norm(vix, 11.0, 28.0)
            + 0.24 * self._norm(atr_p, 0.8, 1.8)
            + 0.14 * self._norm(gap, 0.2, 2.0)
            + 0.18 * self._norm(range_exp, 0.8, 2.0)
            + 0.10 * self._norm(chop, 25.0, 75.0)
            + 0.06 * self._norm(slope, 0.0001, 0.004)
        )
        return float(round(self._clip(score01 * 100.0, 0.0, 100.0), 2)), {
            "vix": round(vix, 2),
            "atrPercentileProxy": round(atr_p, 4),
            "gapStress": round(gap, 3),
            "intradayRangeExpansion": round(range_exp, 4),
            "chopRisk": round(chop, 2),
        }

    def _classify_intraday_state(self, *, phase: str, regime_ctx: dict[str, Any]) -> str:
        if phase == "PREMARKET":
            return "PREOPEN"
        intraday = regime_ctx.get("intraday", {}) if isinstance(regime_ctx.get("intraday"), dict) else {}
        bars = int(intraday.get("bars") or 0)
        slope = float(intraday.get("vwapSlope") or 0.0)
        expansion = float(intraday.get("rangeExpansion30m") or 0.0)
        if bars < 4:
            return "EVENT_RISK"
        if abs(slope) >= 0.0015 and expansion >= 1.3:
            return "OPEN_DRIVE" if phase == "POST_OPEN" else "TREND_DAY"
        if abs(slope) <= 0.0005 and expansion >= 1.3:
            return "OPEN_FADE"
        if abs(slope) >= 0.0009 and expansion >= 1.05:
            return "TREND_DAY"
        return "CHOP_DAY"

    def _map_regime(
        self,
        *,
        trend_score: float,
        breadth_score: float,
        leadership_score: float,
        volatility_stress_score: float,
        data_quality_score: float,
        risk_appetite: float,
        prev: MarketBrainState | None,
    ) -> str:
        regime = "RANGE"
        if volatility_stress_score >= 82.0 or breadth_score <= 18.0 or data_quality_score <= 30.0:
            regime = "PANIC"
        elif trend_score >= 70.0 and breadth_score >= 62.0 and leadership_score >= 56.0 and volatility_stress_score <= 48.0:
            regime = "TREND_UP"
        elif trend_score <= 36.0 and breadth_score <= 40.0 and leadership_score <= 45.0:
            regime = "TREND_DOWN"
        elif volatility_stress_score >= 62.0 and leadership_score <= 46.0 and risk_appetite <= 46.0:
            regime = "CHOP"
        elif prev is not None and prev.regime in {"PANIC", "TREND_DOWN", "CHOP"} and trend_score >= 55.0 and breadth_score >= 50.0 and leadership_score >= 50.0:
            regime = "RECOVERY"

        if prev is None:
            return regime

        if prev.regime == "PANIC":
            if regime != "PANIC":
                if volatility_stress_score > 65.0 or breadth_score < 35.0 or data_quality_score < 45.0:
                    return "PANIC"
        if prev.regime == "TREND_UP" and regime != "TREND_UP":
            if trend_score >= 60.0 and breadth_score >= 55.0 and leadership_score >= 50.0:
                return "TREND_UP"
        if prev.regime not in {"TREND_UP"} and regime == "TREND_UP":
            if not (trend_score >= 74.0 and breadth_score >= 66.0 and leadership_score >= 58.0):
                return prev.regime
        if prev.regime != regime and regime != "PANIC":
            prev_ts = parse_any_ts(prev.asof_ts)
            if prev_ts is not None:
                age_sec = (now_ist() - prev_ts.astimezone(IST)).total_seconds()
                if age_sec <= 240.0:
                    return prev.regime
        return regime

    def _map_risk_mode(
        self,
        *,
        regime: str,
        risk_appetite: float,
        volatility_stress_score: float,
        data_quality_score: float,
    ) -> str:
        if regime == "PANIC" or volatility_stress_score >= 85.0 or data_quality_score < 35.0:
            return "LOCKDOWN"
        if regime in {"CHOP", "TREND_DOWN"} or volatility_stress_score >= 65.0 or data_quality_score < 55.0:
            return "DEFENSIVE"
        if regime in {"TREND_UP", "RECOVERY"} and risk_appetite >= 66.0 and volatility_stress_score <= 50.0 and data_quality_score >= 65.0:
            return "AGGRESSIVE"
        return "NORMAL"

    def _map_bias(self, regime: str, participation: str) -> tuple[float, float]:
        long_bias = 0.55
        short_bias = 0.45
        if regime == "TREND_UP":
            long_bias, short_bias = 0.78, 0.22
        elif regime == "TREND_DOWN":
            long_bias, short_bias = 0.22, 0.78
        elif regime == "CHOP":
            long_bias, short_bias = 0.48, 0.52
        elif regime == "PANIC":
            long_bias, short_bias = 0.15, 0.85
        elif regime == "RECOVERY":
            long_bias, short_bias = 0.68, 0.32
        if participation == "WEAK":
            long_bias = min(long_bias, 0.60)
            short_bias = max(short_bias, 0.40)
        return round(long_bias, 3), round(short_bias, 3)

    def _state_to_watchlist_regime(self, state: MarketBrainState) -> dict[str, Any]:
        context = self._last_context if self._last_context.get("asofTs") == state.asof_ts else {}
        regime_daily = "RANGE"
        if state.regime in {"TREND_UP", "RECOVERY"}:
            regime_daily = "TREND"
        elif state.regime in {"TREND_DOWN", "PANIC"}:
            regime_daily = "RISK_OFF"
        regime_intraday = "CHOPPY"
        if state.intraday_state in {"OPEN_DRIVE", "TREND_DAY"}:
            regime_intraday = "TRENDY"
        regime_ctx = context.get("regimeContext", {}) if isinstance(context.get("regimeContext"), dict) else {}
        return {
            "regimeDaily": regime_daily,
            "regimeIntraday": regime_intraday,
            "daily": regime_ctx.get("daily", {}),
            "intraday": regime_ctx.get("intraday", {}),
            "source": regime_ctx.get("source", {}),
        }

    def _validate_phase_no_lookahead(self, *, state: MarketBrainState, context: dict[str, Any]) -> bool:
        if state.phase != "PREMARKET":
            return True
        regime_ctx = context.get("regimeContext", {}) if isinstance(context.get("regimeContext"), dict) else {}
        source = regime_ctx.get("source", {}) if isinstance(regime_ctx.get("source"), dict) else {}
        daily_source = str(source.get("dailySource") or "").lower()
        if "upstox_api" in daily_source and "expectedlcd" not in daily_source:
            return False
        intraday_source = str(source.get("intradaySource") or "").lower()
        if intraday_source and intraday_source != "premarket_skip":
            return False
        return True

    def validate_no_lookahead_market_brain(self, state: MarketBrainState) -> bool:
        return self._validate_phase_no_lookahead(state=state, context=self._last_context)

    def _build_state(self, *, asof_ts: str, force_phase: str | None = None) -> MarketBrainState:
        asof = parse_any_ts(asof_ts) or now_ist()
        asof_i = asof.astimezone(IST)
        phase = force_phase or self._phase_from_clock(asof_i)
        expected_lcd = self.universe_service._expected_latest_daily_candle_date(asof_i).strftime("%Y-%m-%d")
        rows = self._build_rows(expected_lcd)
        premarket = phase == "PREMARKET"

        prior = self.read_latest_market_brain_state()
        if prior is not None:
            prior_ts = parse_any_ts(prior.asof_ts)
            if prior_ts is not None and prior.phase == phase:
                age = (asof_i - prior_ts.astimezone(IST)).total_seconds()
                if age <= (180.0 if phase in {"LIVE", "POST_OPEN"} else 600.0):
                    self._last_context = self._read_latest_context().get("context", {}) or {}
                    return prior

        regime_ctx = self.universe_service._build_watchlist_v2_regime(
            timeframe="5m",
            expected_lcd=expected_lcd,
            now_i=asof_i,
            premarket=premarket,
        )
        trend_score = self._compute_trend_score(regime_ctx)
        breadth = self.compute_breadth_snapshot(expected_lcd=expected_lcd, rows=rows)
        leadership = self.compute_leadership_snapshot(expected_lcd=expected_lcd, rows=rows, now_i=asof_i)
        liquidity_health_score, liquidity_ctx = self._compute_liquidity_health(rows)

        live_regime: RegimeSnapshot | None = None
        if phase in {"POST_OPEN", "LIVE", "EOD"}:
            try:
                live_regime = self.regime_service.get_market_regime()
            except Exception:
                logger.warning("market_brain_v2 live regime fetch failed", exc_info=True)

        volatility_stress_score, stress_ctx = self._compute_volatility_stress(regime_ctx=regime_ctx, live_regime=live_regime)
        quality_regime_ctx = dict(regime_ctx)
        quality_regime_ctx["_phaseHint"] = phase
        data_quality_score, quality_ctx = self._compute_data_quality(
            rows=rows,
            breadth=breadth,
            leadership=leadership,
            regime_ctx=quality_regime_ctx,
        )

        risk_appetite = (
            (0.26 * trend_score)
            + (0.24 * float(breadth.get("score") or 0.0))
            + (0.20 * float(leadership.get("score") or 0.0))
            + (0.15 * liquidity_health_score)
            + (0.10 * data_quality_score)
            - (0.15 * volatility_stress_score)
        )
        risk_appetite = self._clip(risk_appetite, 0.0, 100.0)
        participation = "STRONG" if (breadth.get("score", 0.0) >= 65.0 and liquidity_health_score >= 65.0) else ("WEAK" if (breadth.get("score", 0.0) < 45.0 or liquidity_health_score < 45.0) else "MODERATE")
        regime = self._map_regime(
            trend_score=trend_score,
            breadth_score=float(breadth.get("score") or 0.0),
            leadership_score=float(leadership.get("score") or 0.0),
            volatility_stress_score=volatility_stress_score,
            data_quality_score=data_quality_score,
            risk_appetite=risk_appetite,
            prev=prior,
        )
        risk_mode = self._map_risk_mode(
            regime=regime,
            risk_appetite=risk_appetite,
            volatility_stress_score=volatility_stress_score,
            data_quality_score=data_quality_score,
        )
        intraday_state = self._classify_intraday_state(phase=phase, regime_ctx=regime_ctx)
        long_bias, short_bias = self._map_bias(regime, participation)
        swing_permission = "ENABLED"
        if regime in {"CHOP", "RECOVERY"}:
            swing_permission = "REDUCED"
        if regime in {"TREND_DOWN", "PANIC"}:
            swing_permission = "DISABLED"

        if risk_mode == "AGGRESSIVE":
            size_multiplier = 1.15
            max_positions_multiplier = 1.25
        elif risk_mode == "NORMAL":
            size_multiplier = 1.0
            max_positions_multiplier = 1.0
        elif risk_mode == "DEFENSIVE":
            size_multiplier = 0.65
            max_positions_multiplier = 0.70
        else:
            size_multiplier = 0.30
            max_positions_multiplier = 0.35

        allowed_strategies = [
            "BREAKOUT",
            "PULLBACK",
            "MEAN_REVERSION",
            "VWAP_TREND",
            "VWAP_REVERSAL",
            "OPEN_DRIVE",
        ]
        if regime in {"CHOP", "PANIC"}:
            allowed_strategies = [s for s in allowed_strategies if s not in {"BREAKOUT", "OPEN_DRIVE", "VWAP_TREND"}]
        if regime in {"TREND_DOWN", "PANIC"}:
            allowed_strategies = [s for s in allowed_strategies if s not in {"BREAKOUT", "PULLBACK"}]
        if not allowed_strategies:
            allowed_strategies = ["MEAN_REVERSION", "VWAP_REVERSAL"]

        reasons = [
            f"phase={phase}",
            f"trend={round(trend_score, 2)}",
            f"breadth={round(float(breadth.get('score') or 0.0), 2)}",
            f"leadership={round(float(leadership.get('score') or 0.0), 2)}",
            f"stress={round(volatility_stress_score, 2)}",
            f"liq={round(liquidity_health_score, 2)}",
            f"dataQ={round(data_quality_score, 2)}",
            f"appetite={round(risk_appetite, 2)}",
        ]

        state = MarketBrainState(
            asof_ts=asof_i.isoformat(),
            phase=phase,  # type: ignore[arg-type]
            regime=regime,  # type: ignore[arg-type]
            participation=participation,  # type: ignore[arg-type]
            risk_mode=risk_mode,  # type: ignore[arg-type]
            intraday_state=intraday_state,  # type: ignore[arg-type]
            long_bias=long_bias,
            short_bias=short_bias,
            size_multiplier=size_multiplier,
            max_positions_multiplier=max_positions_multiplier,
            swing_permission=swing_permission,  # type: ignore[arg-type]
            allowed_strategies=allowed_strategies,
            reasons=reasons,
            trend_score=round(trend_score, 2),
            breadth_score=round(float(breadth.get("score") or 0.0), 2),
            leadership_score=round(float(leadership.get("score") or 0.0), 2),
            volatility_stress_score=round(volatility_stress_score, 2),
            liquidity_health_score=round(liquidity_health_score, 2),
            data_quality_score=round(data_quality_score, 2),
        )

        context = {
            "asofTs": state.asof_ts,
            "expectedLCD": expected_lcd,
            "regimeContext": regime_ctx,
            "riskAppetite": round(risk_appetite, 2),
            "breadthSnapshot": breadth,
            "leadershipSnapshot": leadership,
            "liquiditySnapshot": liquidity_ctx,
            "stressSnapshot": stress_ctx,
            "dataQualitySnapshot": quality_ctx,
            "noLookaheadValid": self._validate_phase_no_lookahead(state=state, context={"regimeContext": regime_ctx}),
        }
        policy = self.policy_service.derive_market_policy(state)
        self.persist_market_brain_state(state, context=context, policy=policy)
        self._last_context = context
        return state

    def build_premarket_market_brain(self, asof_ts: str) -> MarketBrainState:
        return self._build_state(asof_ts=asof_ts, force_phase="PREMARKET")

    def build_post_open_market_brain(self, asof_ts: str) -> MarketBrainState:
        state = self._build_state(asof_ts=asof_ts, force_phase=None)
        if state.phase == "PREMARKET":
            # Explicit POST_OPEN request should not return PREMARKET.
            state.phase = "POST_OPEN"  # type: ignore[assignment]
            policy = self.policy_service.derive_market_policy(state)
            self.persist_market_brain_state(state, context=self._last_context, policy=policy)
        return state

    def review_eod_market_brain(self, trade_date: str) -> dict[str, Any]:
        base = self.gcs.read_json(f"{self.history_prefix}/{trade_date}", default={})
        latest = self._read_latest_context()
        state = self.read_latest_market_brain_state()
        return {
            "tradeDate": trade_date,
            "latestState": asdict(state) if state is not None else {},
            "latestContext": latest.get("context", {}) if isinstance(latest, dict) else {},
            "historyPrefixExists": bool(base),
        }

    def derive_market_policy(self, state: MarketBrainState) -> MarketPolicy:
        return self.policy_service.derive_market_policy(state)

    def adjust_watchlist_rows(self, rows: list[dict[str, Any]], policy: MarketPolicy, *, section: str) -> list[dict[str, Any]]:
        return self.policy_service.adjust_watchlist_rows(rows, policy, section=section)

    def adjust_signal(self, signal_score: int, state: MarketBrainState) -> int:
        return self.policy_service.adjust_signal(signal_score, state)

    def size_position_with_market_brain(
        self,
        position_sizing,
        state: MarketBrainState,
        cfg,
        *,
        setup_confidence_multiplier: float = 1.0,
        liquidity_multiplier: float = 1.0,
        data_quality_multiplier: float = 1.0,
    ):
        return self.policy_service.size_position_with_market_brain(
            position_sizing,
            state,
            cfg,
            setup_confidence_multiplier=setup_confidence_multiplier,
            liquidity_multiplier=liquidity_multiplier,
            data_quality_multiplier=data_quality_multiplier,
        )

    def watchlist_regime_payload(self, state: MarketBrainState) -> dict[str, Any]:
        return self._state_to_watchlist_regime(state)
