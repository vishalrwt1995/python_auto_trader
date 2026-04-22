from __future__ import annotations

import logging
import math
import statistics
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, ClassVar

from autotrader.adapters.bigquery_client import BigQueryClient
from autotrader.adapters.firestore_state import FirestoreStateStore
from autotrader.adapters.gcs_store import GoogleCloudStorageStore
from autotrader.adapters.pubsub_client import PubSubClient
from autotrader.domain.models import MarketBrainState, MarketPolicy, RegimeSnapshot
from autotrader.services.market_breadth_service import MarketBreadthService
from autotrader.services.market_leadership_service import MarketLeadershipService
from autotrader.services.market_policy_service import MarketPolicyService
from autotrader.services.regime_service import MarketRegimeService
from autotrader.settings import RegimeThresholds, StrategySettings
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
    bq: BigQueryClient | None = None
    pubsub: PubSubClient | None = None
    thresholds: RegimeThresholds = field(default_factory=RegimeThresholds)
    latest_state_path: str = "state/market_brain/latest.json"
    history_prefix: str = "state/market_brain/history"
    _last_context: dict[str, Any] = field(default_factory=dict, init=False, repr=False)
    # Tracks last-emitted pubsub snapshot to gate duplicate publishes (PR-1 Item 0.3)
    _last_pubsub_state: dict[str, Any] = field(default_factory=dict, init=False, repr=False)

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
        run_degraded_raw = str(payload.get("run_degraded_flag", "")).strip().upper()
        run_degraded = run_degraded_raw in {"Y", "YES", "TRUE", "1"} if run_degraded_raw else bool(payload.get("run_degraded_flag", False))
        return MarketBrainState(
            asof_ts=asof,
            phase=str(payload.get("phase") or "PREMARKET"),  # type: ignore[arg-type]
            regime=str(payload.get("regime") or "RANGE"),  # type: ignore[arg-type]
            sub_regime_v2=str(payload.get("sub_regime_v2") or "BASELINE"),
            structure_state=str(payload.get("structure_state") or "ORDERLY"),
            recovery_state=str(payload.get("recovery_state") or "NONE"),
            event_state=str(payload.get("event_state") or "NONE"),
            participation=str(payload.get("participation") or "MODERATE"),  # type: ignore[arg-type]
            risk_mode=str(payload.get("risk_mode") or "NORMAL"),  # type: ignore[arg-type]
            intraday_state=str(payload.get("intraday_state") or "PREOPEN"),  # type: ignore[arg-type]
            run_degraded_flag=bool(run_degraded),
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
            market_confidence=float(payload.get("market_confidence") or 50.0),
            breadth_confidence=float(payload.get("breadth_confidence") or 50.0),
            leadership_confidence=float(payload.get("leadership_confidence") or 50.0),
            phase2_confidence=float(payload.get("phase2_confidence") or 50.0),
            policy_confidence=float(payload.get("policy_confidence") or 50.0),
            run_integrity_confidence=float(payload.get("run_integrity_confidence") or 50.0),
            # PR-1 fields — default-safe for pre-migration snapshots
            options_positioning_score=float(payload.get("options_positioning_score") or 50.0),
            flow_score=float(payload.get("flow_score") or 50.0),
            breadth_roc_score=float(payload.get("breadth_roc_score") or 50.0),
            prev_regime=str(payload.get("prev_regime") or ""),
            regime_age_seconds=float(payload.get("regime_age_seconds") or 0.0),
            regime_transitions_today=int(payload.get("regime_transitions_today") or 0),
            signal_age_penalty=float(payload.get("signal_age_penalty") or 0.0),
        )

    def _should_emit_pubsub(self, state: MarketBrainState) -> bool:
        """Gate Pub/Sub publish — emit only on regime/risk_mode transition
        or after `thresholds.pubsub_heartbeat_sec` silence (PR-1 Item 0.3).

        Pre-PR-1 behaviour: every persist emitted, flooding downstream
        consumers (dashboard websocket, alert pipelines) with identical
        snapshots. Now a snapshot is emitted only when interesting state
        changed, or a heartbeat is due so monitors can detect a silent
        brain service.
        """
        last = self._last_pubsub_state
        if not last:
            return True  # first snapshot of process lifetime
        if last.get("regime") != state.regime or last.get("risk_mode") != state.risk_mode:
            return True
        last_ts = last.get("ts")
        try:
            if last_ts is None:
                return True
            age = (now_ist() - last_ts).total_seconds()
        except Exception:
            return True
        return age >= float(self.thresholds.pubsub_heartbeat_sec)

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
        # Sanity validation before persisting — prevents corrupted state from becoming
        # "last-known-good" and contaminating future fallback reads.
        _valid_regimes = {"TREND_UP", "TREND_DOWN", "RANGE", "CHOP", "PANIC", "RECOVERY"}
        _valid_risk_modes = {"NORMAL", "DEFENSIVE", "AGGRESSIVE", "LOCKDOWN"}
        if str(state.regime or "").upper() not in _valid_regimes:
            logger.error(
                "persist_market_brain_state BLOCKED: invalid regime=%r — state not saved",
                state.regime,
            )
            return
        if str(state.risk_mode or "").upper() not in _valid_risk_modes:
            logger.error(
                "persist_market_brain_state BLOCKED: invalid risk_mode=%r — state not saved",
                state.risk_mode,
            )
            return
        # Score plausibility: if ALL scores are exactly 50 or 0, something likely went wrong
        _scores = [state.trend_score, state.breadth_score, state.leadership_score, state.volatility_stress_score]
        if all(s == 0.0 for s in _scores):
            logger.error("persist_market_brain_state BLOCKED: all scores are 0 — state not saved (data pipeline failure)")
            return
        # PR-2: compose narrative once here so it lives alongside state in
        # Firestore — FE picks it up via the existing real-time listener
        # with zero extra round-trips. Failures are swallowed (best-effort).
        try:
            narrative = self.build_narrative(state, context or {})
        except Exception:
            logger.exception("market_brain narrative build failed")
            narrative = {}
        payload = {
            "state": asdict(state),
            "context": context or {},
            "policy": asdict(policy) if policy is not None else {},
            "narrative": narrative,
        }
        self.state.set_json("market_brain", "latest", payload, merge=False)
        self.gcs.write_json(self.latest_state_path, payload)
        asof = parse_any_ts(state.asof_ts) or now_ist()
        d = asof.astimezone(IST).strftime("%Y-%m-%d")
        t = asof.astimezone(IST).strftime("%H%M%S")
        self.gcs.write_json(f"{self.history_prefix}/{d}/{t}.json", payload)
        # Publish regime snapshot to Pub/Sub + BigQuery (best-effort)
        bq_row = {
            "asof_ts": state.asof_ts,
            "run_date": d,
            "regime": state.regime,
            "risk_mode": state.risk_mode,
            "participation": state.participation,
            "market_confidence": state.market_confidence,
            "breadth_confidence": state.breadth_confidence,
            "leadership_confidence": state.leadership_confidence,
            "trend_score": state.trend_score,
            "breadth_score": state.breadth_score,
            "volatility_stress_score": state.volatility_stress_score,
            "data_quality_score": state.data_quality_score,
            "selected_watchlist_count": 0,
            # PR-1 additions — nullable columns; BQ ALTER TABLE required before
            # they populate (rows inserted against an older schema will surface
            # these in insert_errors). The `self.bq.insert` wrapper logs but
            # does not raise, so a pre-migration deploy stays safe.
            "options_positioning_score": state.options_positioning_score,
            "flow_score": state.flow_score,
            "breadth_roc_score": state.breadth_roc_score,
            "prev_regime": state.prev_regime,
            "regime_age_seconds": state.regime_age_seconds,
            "regime_transitions_today": state.regime_transitions_today,
            "signal_age_penalty": state.signal_age_penalty,
        }
        if self.bq:
            self.bq.insert_market_brain(bq_row)
        if self.pubsub and self._should_emit_pubsub(state):
            self.pubsub.publish_regime_changed(bq_row)
            self._last_pubsub_state = {
                "regime": state.regime,
                "risk_mode": state.risk_mode,
                "ts": asof.astimezone(IST),
            }
        # Append snapshot to market_brain/history doc (rolling last-30 array)
        # Lives inside the existing market_brain collection so Firestore rules cover it
        new_snap = {
            "asof_ts": state.asof_ts,
            "regime": state.regime,
            "sub_regime_v2": state.sub_regime_v2 or "",
            "risk_mode": state.risk_mode,
            "participation": state.participation,
            "market_confidence": round(float(state.market_confidence or 0), 1),
            "trend_score": round(float(state.trend_score or 0), 1),
            "breadth_score": round(float(state.breadth_score or 0), 1),
            "volatility_stress_score": round(float(state.volatility_stress_score or 0), 1),
        }
        existing = self.state.get_json("market_brain", "history") or {}
        snaps = existing.get("snapshots", [])
        snaps.append(new_snap)
        snaps = snaps[-30:]  # keep last 30
        self.state.set_json("market_brain", "history", {"snapshots": snaps}, merge=False)

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

    def _compute_liquidity_health(
        self,
        rows: list[dict[str, Any]],
        *,
        volatility_stress_score: float = 50.0,
    ) -> tuple[float, dict[str, Any]]:
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
        # Stress-aware soft ceiling: do not let liquidity score stay unrealistically perfect in stressed regimes.
        soft_ceiling = 98.0
        if float(volatility_stress_score) >= 82.0:
            soft_ceiling = 84.0
        elif float(volatility_stress_score) >= 70.0:
            soft_ceiling = 88.0
        elif float(volatility_stress_score) >= 58.0:
            soft_ceiling = 92.0
        elif float(volatility_stress_score) >= 48.0:
            soft_ceiling = 95.0
        soft_ceiling -= (6.0 * self._norm(fallback_only_pct, 18.0, 60.0))
        soft_ceiling = self._clip(soft_ceiling, 78.0, 98.0)
        if score > soft_ceiling:
            score = float(round(soft_ceiling + ((score - soft_ceiling) * 0.12), 2))
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
            "stressAwareSoftCeiling": round(float(soft_ceiling), 2),
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
        def _safe_int(val: str, default: int = 0) -> int:
            try:
                return int(float(val))
            except (ValueError, TypeError):
                return default
        phase2_eligible_count = _safe_int(self.state.get_runtime_prop("runtime:watchlist_last_phase2_eligible_count", "0") or "0")
        phase2_used_count = _safe_int(self.state.get_runtime_prop("runtime:watchlist_last_phase2_used_count", "0") or "0")
        phase2_branch_entered = str(self.state.get_runtime_prop("runtime:watchlist_last_phase2_branch_entered", "")).strip().upper() in {"Y", "YES", "TRUE", "1"}
        phase2_window_open = str(self.state.get_runtime_prop("runtime:watchlist_last_phase2_window_open", "")).strip().upper() in {"Y", "YES", "TRUE", "1"}
        phase2_policy_enabled = str(self.state.get_runtime_prop("runtime:watchlist_last_phase2_policy_enabled", "")).strip().upper() in {"Y", "YES", "TRUE", "1"}
        phase2_global_skip_reason = str(self.state.get_runtime_prop("runtime:watchlist_last_phase2_global_skip_reason", "")).strip().upper()

        intraday_phase2_penalty = 0.0
        pipeline_alignment_penalty = 0.0
        stale_writer_penalty = 0.0
        writer_age_min: dict[str, float | None] = {"watchlist": None, "scanner": None, "signals": None}

        if is_live_window:
            # Phase 2 eligibility reflects MARKET conditions, not pipeline health.
            # Only penalize DQ when the pipeline itself failed to run the Phase 2 branch
            # (branch_entered=False despite window being open and policy enabled).
            expected_zero_phase2 = (phase2_global_skip_reason in {"PHASE2_WINDOW_CLOSED", "MARKET_POLICY_BLOCKED"}) or (not phase2_window_open) or (not phase2_policy_enabled)
            if not expected_zero_phase2 and not phase2_branch_entered and intraday_bars >= 6.0:
                # Pipeline bug: Phase 2 should have run but didn't — genuine DQ issue.
                intraday_phase2_penalty += 8.0

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
            "phase2UsedCount": int(phase2_used_count),
            "phase2BranchEntered": bool(phase2_branch_entered),
            "phase2WindowOpen": bool(phase2_window_open),
            "phase2PolicyEnabled": bool(phase2_policy_enabled),
            "phase2GlobalSkipReason": str(phase2_global_skip_reason),
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
        vix = float(live_regime.vix) if (live_regime is not None and live_regime.vix > 0) else 15.0  # 15.0 = neutral Indian VIX; 0 means fetch failed
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

    def _derive_secondary_states(
        self,
        *,
        phase: str,
        regime: str,
        trend_score: float,
        breadth_score: float,
        leadership_score: float,
        volatility_stress_score: float,
        liquidity_health_score: float,
        data_quality_score: float,
        risk_appetite: float,
        deltas: dict[str, float],
        regime_ctx: dict[str, Any],
        now_i: datetime,
    ) -> tuple[str, str, str, str]:
        trend_delta = float(deltas.get("trend", 0.0))
        breadth_delta = float(deltas.get("breadth", 0.0))
        leadership_delta = float(deltas.get("leadership", 0.0))
        stress_delta = float(deltas.get("stress", 0.0))

        structure_state = "ORDERLY"
        if regime == "PANIC":
            structure_state = "PANIC_TREND" if abs(float((regime_ctx.get("intraday", {}) or {}).get("vwapSlope") or 0.0)) >= 0.001 else "EVENT_DISTORTION"
        elif regime == "TREND_UP" and breadth_score < 52.0:
            structure_state = "NARROW_TREND"
        elif regime == "TREND_UP" and trend_score >= 75.0 and leadership_score >= 60.0:
            structure_state = "MATURE_TREND"
        elif regime == "TREND_DOWN" and breadth_delta < -2.0:
            structure_state = "DISTRIBUTION"
        elif regime == "CHOP":
            structure_state = "CHOPPY_NOISE"
        elif regime == "RANGE" and volatility_stress_score < 45.0:
            structure_state = "ORDERLY_RANGE"
        elif regime == "RANGE":
            structure_state = "VOLATILE_RANGE"

        recovery_state = "NONE"
        if regime == "RECOVERY":
            if breadth_delta >= 3.0 and leadership_delta >= 2.0 and stress_delta <= 0.0:
                recovery_state = "STRENGTHENING"
            elif breadth_delta < 0.0 or leadership_delta < 0.0:
                recovery_state = "FRAGILE"
            else:
                recovery_state = "EARLY"

        sub_regime = "BASELINE"
        if regime == "PANIC":
            sub_regime = "PANIC_TREND"
        elif regime == "RECOVERY":
            if trend_score >= 58.0 and breadth_score < 48.0:
                sub_regime = "FALSE_RECOVERY"
            elif leadership_score >= 55.0 and breadth_score >= 50.0:
                sub_regime = "RECOVERY_BUILD"
            else:
                sub_regime = "RECOVERY_TENTATIVE"
        elif regime == "TREND_UP" and structure_state == "NARROW_TREND":
            sub_regime = "NARROW_TREND"
        elif regime == "TREND_DOWN" and volatility_stress_score < 55.0 and breadth_delta >= 2.0:
            sub_regime = "SHORT_COVERING_BOUNCE"
        elif regime == "CHOP":
            sub_regime = "HIGH_NOISE_CHOP" if volatility_stress_score >= 62.0 else "LOW_CONVICTION_CHOP"
        elif regime == "RANGE":
            sub_regime = "HEALTHY_RANGE" if volatility_stress_score < 45.0 and data_quality_score >= 70.0 else "LOW_CONVICTION_RANGE"

        event_state = "NONE"
        stress_extreme = volatility_stress_score >= 80.0
        gap_stress = float((regime_ctx.get("intraday", {}) or {}).get("rangeExpansion30m") or 0.0) >= 1.6
        if phase == "POST_OPEN" and gap_stress:
            event_state = "ABNORMAL_GAP"
        elif stress_extreme:
            event_state = "STRESS_EVENT"
        elif now_i.astimezone(IST).weekday() == 3 and phase in {"POST_OPEN", "LIVE"}:
            event_state = "EXPIRY_SESSION"
        elif regime in {"CHOP", "PANIC"} and liquidity_health_score < 55.0 and risk_appetite < 45.0:
            event_state = "EXECUTION_FRAGILITY"

        return sub_regime, structure_state, recovery_state, event_state

    def _confidence_family(
        self,
        *,
        phase: str,
        trend_score: float,
        breadth_score: float,
        leadership_score: float,
        volatility_stress_score: float,
        liquidity_health_score: float,
        data_quality_score: float,
        risk_appetite: float,
        quality_ctx: dict[str, Any],
        no_lookahead_valid: bool,
    ) -> dict[str, float]:
        market_conf = self._clip((0.35 * risk_appetite) + (0.15 * trend_score) + (0.15 * breadth_score) + (0.15 * leadership_score) + (0.10 * liquidity_health_score) + (0.10 * data_quality_score) - (0.15 * volatility_stress_score), 0.0, 100.0)
        breadth_conf = self._clip((0.65 * breadth_score) + (0.20 * self._norm(float(quality_ctx.get("breadthProcessed") or 0.0), 40.0, 350.0) * 100.0) + (0.15 * data_quality_score), 0.0, 100.0)
        leadership_conf = self._clip((0.68 * leadership_score) + (0.17 * self._norm(float(quality_ctx.get("leadersProcessed") or 0.0), 20.0, 130.0) * 100.0) + (0.15 * data_quality_score), 0.0, 100.0)

        phase2_eligible = int(quality_ctx.get("phase2EligibleCount") or 0)
        phase2_used = int(quality_ctx.get("phase2UsedCount") or 0)
        phase2_window_open = bool(quality_ctx.get("phase2WindowOpen"))
        phase2_policy_enabled = bool(quality_ctx.get("phase2PolicyEnabled"))
        expected_zero = (str(quality_ctx.get("phase2GlobalSkipReason") or "").upper() in {"PHASE2_WINDOW_CLOSED", "MARKET_POLICY_BLOCKED"}) or (not phase2_window_open) or (not phase2_policy_enabled)
        if phase == "PREMARKET":
            phase2_conf = 72.0
        elif expected_zero:
            phase2_conf = 68.0
        elif phase2_used > 0:
            phase2_conf = self._clip(58.0 + (0.20 * min(100.0, phase2_used * 2.0)) + (0.22 * min(100.0, phase2_eligible * 1.5)), 35.0, 96.0)
        elif phase2_eligible > 0:
            phase2_conf = self._clip(46.0 + (0.15 * min(100.0, phase2_eligible * 1.8)) - 8.0, 20.0, 82.0)
        else:
            phase2_conf = 26.0

        writer_age = quality_ctx.get("writerAgeMin") if isinstance(quality_ctx.get("writerAgeMin"), dict) else {}
        max_writer_age = max([float(v) for v in writer_age.values() if isinstance(v, (int, float))], default=0.0)
        integrity_penalty = (
            float(quality_ctx.get("pipelineAlignmentPenalty") or 0.0)
            + float(quality_ctx.get("staleWriterPenalty") or 0.0)
            + (8.0 if not no_lookahead_valid else 0.0)
            + (6.0 * self._norm(max_writer_age, 18.0, 90.0))
        )
        run_integrity_conf = self._clip(100.0 - integrity_penalty, 0.0, 100.0)
        policy_conf = self._clip(
            (0.40 * market_conf)
            + (0.20 * data_quality_score)
            + (0.15 * run_integrity_conf)
            + (0.15 * breadth_conf)
            + (0.10 * leadership_conf),
            0.0,
            100.0,
        )
        return {
            "market_confidence": round(float(market_conf), 2),
            "breadth_confidence": round(float(breadth_conf), 2),
            "leadership_confidence": round(float(leadership_conf), 2),
            "phase2_confidence": round(float(phase2_conf), 2),
            "policy_confidence": round(float(policy_conf), 2),
            "run_integrity_confidence": round(float(run_integrity_conf), 2),
        }

    def _classify_intraday_state(self, *, phase: str, regime_ctx: dict[str, Any]) -> str:
        if phase == "PREMARKET":
            return "PREOPEN"
        intraday = regime_ctx.get("intraday", {}) if isinstance(regime_ctx.get("intraday"), dict) else {}
        bars = int(intraday.get("bars") or 0)
        slope = float(intraday.get("vwapSlope") or 0.0)
        expansion = float(intraday.get("rangeExpansion30m") or 0.0)
        if bars < 4:
            # Distinguish market holidays (zero bars expected) from genuine event risk.
            try:
                lcd_ctx = self.universe_service._expected_lcd_context()
                if lcd_ctx.get("marketClosedToday"):
                    return "HOLIDAY"
            except Exception:
                pass
            return "EVENT_RISK"
        if abs(slope) >= 0.0015 and expansion >= 1.3:
            return "OPEN_DRIVE" if phase == "POST_OPEN" else "TREND_DAY"
        if abs(slope) <= 0.0005 and expansion >= 1.3:
            return "OPEN_FADE"
        if abs(slope) >= 0.0009 and expansion >= 1.05:
            return "TREND_DAY"
        return "CHOP_DAY"

    # ---------------------------------------------------------------- #
    # PR-1 helpers: PCR / FII-DII / breadth-RoC / signal-age decay
    # ---------------------------------------------------------------- #

    @staticmethod
    def _pcr_to_positioning_score(pcr: float, *, confidence: float = 100.0) -> float:
        """Map weighted PCR → 0..100 options-positioning score.

        Contrarian read (standard market interpretation):
          • PCR < 0.7  → too many calls vs puts → crowded longs → BEARISH (score → 0)
          • PCR ≈ 1.0  → balanced                               → NEUTRAL (score → 50)
          • PCR > 1.3  → too many puts vs calls → oversold      → BULLISH (score → 100)
        Below 0.3 or above 2.5 we saturate.

        If confidence is low (fewer expiries aggregated, or fallback), we pull
        the score toward 50 (neutral) so a half-known signal doesn't look
        strong. Confidence 100 → no dilution; confidence 0 → full dilution.
        """
        if pcr is None or pcr <= 0:
            return 50.0
        # Piecewise linear: 0.3→0, 0.7→30, 1.0→50, 1.3→70, 2.5→100
        p = float(pcr)
        if p <= 0.3:
            base = 0.0
        elif p <= 0.7:
            base = 30.0 * (p - 0.3) / 0.4
        elif p <= 1.0:
            base = 30.0 + 20.0 * (p - 0.7) / 0.3
        elif p <= 1.3:
            base = 50.0 + 20.0 * (p - 1.0) / 0.3
        elif p <= 2.5:
            base = 70.0 + 30.0 * (p - 1.3) / 1.2
        else:
            base = 100.0
        c = max(0.0, min(100.0, float(confidence))) / 100.0
        return round(50.0 + (base - 50.0) * c, 2)

    @staticmethod
    def _fii_dii_to_flow_score(fii_net: float, dii_net: float, *, freshness: float = 100.0) -> float:
        """Map FII + DII net values (₹ crore) → 0..100 institutional flow score.

        Both flows matter: DII buying can cushion FII selling.
        Combined net = FII + DII (₹ crore, positive = net inflow).

          • < -5000 → 0   (heavy outflow, ~₹5000cr+ net sell)
          • -5000→0 → 0→50 linear
          • 0→+5000 → 50→100 linear
          • > +5000 → 100 (heavy inflow)

        If data is stale (freshness_score below 100), pull toward 50 neutral.
        """
        combined = float(fii_net or 0.0) + float(dii_net or 0.0)
        if combined <= -5000.0:
            base = 0.0
        elif combined >= 5000.0:
            base = 100.0
        else:
            base = 50.0 + (combined / 5000.0) * 50.0
        f = max(0.0, min(100.0, float(freshness))) / 100.0
        return round(50.0 + (base - 50.0) * f, 2)

    def _breadth_roc_score(self, breadth_score: float, prev: MarketBrainState | None) -> float:
        """Rate-of-change of breadth → 0..100 (0=collapsing, 100=expanding).

        Compares current breadth_score to prev.breadth_score. Missing prior
        returns 50 (neutral — no information). One snapshot delta ≈ 1–3 min in
        LIVE phase, so small deltas are expected; we saturate at ±10 points.
        """
        if prev is None:
            return 50.0
        try:
            delta = float(breadth_score) - float(prev.breadth_score)
        except Exception:
            return 50.0
        # Saturate at ±10 breadth-points delta → 0 / 100
        if delta <= -10.0:
            return 0.0
        if delta >= 10.0:
            return 100.0
        return round(50.0 + (delta / 10.0) * 50.0, 2)

    def _signal_age_penalty(self, live_regime: "RegimeSnapshot | None") -> float:
        """Compute points-to-shave from market_confidence based on signal staleness.

        Uses RegimeSnapshot.freshness — specifically nifty_age_sec, vix_age_sec,
        pcr_age_sec. Returns a value in [0, thresholds.signal_max_penalty].

        Threshold-linear: <fresh_max → 0, >stale_full → max_penalty.
        """
        if live_regime is None:
            return 0.0
        try:
            fresh = live_regime.freshness
            ages = [
                float(fresh.nifty_age_sec or 0.0),
                float(fresh.vix_age_sec or 0.0),
                float(fresh.pcr_age_sec or 0.0),
            ]
        except Exception:
            return 0.0
        if not ages:
            return 0.0
        worst = max(ages)
        t = self.thresholds
        if worst <= t.signal_fresh_max_sec:
            return 0.0
        if worst >= t.signal_stale_full_sec:
            return float(t.signal_max_penalty)
        # Linear interpolation between fresh_max and stale_full
        span = max(1e-6, t.signal_stale_full_sec - t.signal_fresh_max_sec)
        frac = (worst - t.signal_fresh_max_sec) / span
        return round(float(t.signal_max_penalty) * frac, 2)

    @staticmethod
    def _count_transitions_today(prev: MarketBrainState | None, is_transition: bool, now_i: datetime) -> int:
        """Rolls transition count forward; resets at IST midnight."""
        if prev is None:
            return 1 if is_transition else 0
        try:
            prev_ts = parse_any_ts(prev.asof_ts)
        except Exception:
            prev_ts = None
        prev_day = prev_ts.astimezone(IST).date() if prev_ts is not None else None
        today = now_i.astimezone(IST).date()
        base = int(getattr(prev, "regime_transitions_today", 0) or 0)
        if prev_day != today:
            base = 0  # rolled into a new trading day
        return base + (1 if is_transition else 0)

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
        t = self.thresholds
        regime = "RANGE"
        # PANIC entry — extreme stress, capitulation breadth, or broken data pipeline
        if volatility_stress_score >= t.panic_stress_min or breadth_score <= t.panic_breadth_max or data_quality_score <= t.panic_dq_max:
            regime = "PANIC"
        elif (
            trend_score >= t.trend_up_trend_min
            and breadth_score >= t.trend_up_breadth_min
            and leadership_score >= t.trend_up_leadership_min
            and volatility_stress_score <= t.trend_up_stress_max
        ):
            regime = "TREND_UP"
        # High-breadth alternative: even when Nifty daily structure (trend_score) is
        # weak (e.g. Nifty below EMA50), broad-market participation can still produce
        # a legitimate trending day.
        elif (
            breadth_score >= t.trend_up_hi_breadth_min
            and leadership_score >= t.trend_up_hi_leadership_min
            and volatility_stress_score <= t.trend_up_hi_stress_max
        ):
            regime = "TREND_UP"
        elif (
            trend_score <= t.trend_down_trend_max
            and breadth_score <= t.trend_down_breadth_max
            and leadership_score <= t.trend_down_leadership_max
        ):
            regime = "TREND_DOWN"
        elif (
            volatility_stress_score >= t.chop_stress_min
            and leadership_score <= t.chop_leadership_max
            and risk_appetite <= t.chop_appetite_max
        ):
            regime = "CHOP"
        elif (
            prev is not None
            and prev.regime in {"PANIC", "TREND_DOWN", "CHOP"}
            and trend_score >= t.recovery_trend_min
            and breadth_score >= t.recovery_breadth_min
            and leadership_score >= t.recovery_leadership_min
        ):
            regime = "RECOVERY"

        if prev is None:
            return regime

        if prev.regime == "PANIC":
            if regime != "PANIC":
                # PANIC exit guard — only market-structure signals gate exit.
                # data_quality_score excluded: in PANIC/LOCKDOWN no scanner runs,
                # which collapses data_quality via stale-writer penalties.
                if volatility_stress_score > t.panic_exit_stress_above or breadth_score < t.panic_exit_breadth_below:
                    return "PANIC"
        if prev.regime == "TREND_UP" and regime != "TREND_UP":
            if (
                trend_score >= t.trend_up_hold_trend_min
                and breadth_score >= t.trend_up_hold_breadth_min
                and leadership_score >= t.trend_up_hold_leadership_min
            ):
                return "TREND_UP"
        if prev.regime not in {"TREND_UP"} and regime == "TREND_UP":
            # Two qualifying paths into TREND_UP:
            # 1) Standard: Nifty trend structure is confirmed (trend_score high)
            # 2) High-breadth: broad participation overrides weak index structure
            _standard_entry = (
                trend_score >= t.trend_up_reenter_trend_min
                and breadth_score >= t.trend_up_reenter_breadth_min
                and leadership_score >= t.trend_up_reenter_leadership_min
            )
            _highbreadth_entry = (
                breadth_score >= t.trend_up_reenter_hi_breadth_min
                and leadership_score >= t.trend_up_reenter_hi_leadership_min
                and volatility_stress_score <= t.trend_up_reenter_hi_stress_max
            )
            if not (_standard_entry or _highbreadth_entry):
                return prev.regime
        if prev.regime != regime and regime != "PANIC":
            prev_ts = parse_any_ts(prev.asof_ts)
            if prev_ts is not None:
                age_sec = (now_ist() - prev_ts.astimezone(IST)).total_seconds()
                if age_sec <= t.transition_min_age_sec:
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
        t = self.thresholds
        # LOCKDOWN: reserved for extreme stress — not every PANIC.
        # PANIC alone → DEFENSIVE (system still trades with caution).
        # LOCKDOWN only when volatility is extreme OR data is broken.
        if volatility_stress_score >= t.lockdown_stress_min or data_quality_score < t.lockdown_dq_max:
            return "LOCKDOWN"
        if (
            regime == "PANIC"
            or regime in {"CHOP", "TREND_DOWN"}
            or volatility_stress_score >= t.defensive_stress_min
            or data_quality_score < t.defensive_dq_max
        ):
            return "DEFENSIVE"
        if (
            regime in {"TREND_UP", "RECOVERY"}
            and risk_appetite >= t.aggressive_appetite_min
            and volatility_stress_score <= t.aggressive_stress_max
            and data_quality_score >= t.aggressive_dq_min
        ):
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
            "canonicalRegime": str(state.regime or ""),
            "riskMode": str(state.risk_mode or ""),
            "structureState": str(state.structure_state or ""),
            "participation": str(state.participation or ""),
            "subRegimeV2": str(state.sub_regime_v2 or ""),
            "runDegradedFlag": bool(state.run_degraded_flag),
            "marketConfidence": float(state.market_confidence or 0.0),
            "breadthConfidence": float(state.breadth_confidence or 0.0),
            "leadershipConfidence": float(state.leadership_confidence or 0.0),
            "phase2Confidence": float(state.phase2_confidence or 0.0),
            "policyConfidence": float(state.policy_confidence or 0.0),
            "runIntegrityConfidence": float(state.run_integrity_confidence or 0.0),
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
        # Guard against false PANIC from breadth data fetch failure.
        # If processedCount < 10 and score < 5, it's likely a data miss — use neutral 50.0.
        # A genuine market crash will have data (stocks trade every day), so processedCount
        # will be high even on bad days. Zero processedCount = service failure, not PANIC.
        _breadth_processed = float(breadth.get("processedCount") or 0.0)
        _breadth_score_raw = float(breadth.get("score") or 0.0)
        if _breadth_score_raw < 5.0 and _breadth_processed < 10:
            logger.warning(
                "breadth_data_sparse processed=%.0f score=%.1f — fallback to neutral 50.0",
                _breadth_processed, _breadth_score_raw,
            )
            breadth = dict(breadth)
            breadth["score"] = 50.0
            breadth["_fallback"] = True
            breadth["_original_score"] = _breadth_score_raw
        leadership = self.compute_leadership_snapshot(expected_lcd=expected_lcd, rows=rows, now_i=asof_i)

        live_regime: RegimeSnapshot | None = None
        if phase in {"POST_OPEN", "LIVE", "EOD"}:
            try:
                live_regime = self.regime_service.get_market_regime()
            except Exception:
                logger.warning("market_brain_v2 live regime fetch failed", exc_info=True)

        volatility_stress_score, stress_ctx = self._compute_volatility_stress(regime_ctx=regime_ctx, live_regime=live_regime)
        try:
            liquidity_health_score, liquidity_ctx = self._compute_liquidity_health(
                rows,
                volatility_stress_score=volatility_stress_score,
            )
        except TypeError:
            # Compatibility for monkeypatched tests/helpers using legacy signature.
            liquidity_health_score, liquidity_ctx = self._compute_liquidity_health(rows)
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
        deltas = {
            "trend": (trend_score - float(prior.trend_score)) if prior is not None else 0.0,
            "breadth": (float(breadth.get("score") or 0.0) - float(prior.breadth_score)) if prior is not None else 0.0,
            "leadership": (float(leadership.get("score") or 0.0) - float(prior.leadership_score)) if prior is not None else 0.0,
            "stress": (volatility_stress_score - float(prior.volatility_stress_score)) if prior is not None else 0.0,
            "liquidity": (liquidity_health_score - float(prior.liquidity_health_score)) if prior is not None else 0.0,
            "quality": (data_quality_score - float(prior.data_quality_score)) if prior is not None else 0.0,
        }
        regime = self._map_regime(
            trend_score=trend_score,
            breadth_score=float(breadth.get("score") or 0.0),
            leadership_score=float(leadership.get("score") or 0.0),
            volatility_stress_score=volatility_stress_score,
            data_quality_score=data_quality_score,
            risk_appetite=risk_appetite,
            prev=prior,
        )
        sub_regime_v2, structure_state, recovery_state, event_state = self._derive_secondary_states(
            phase=phase,
            regime=regime,
            trend_score=trend_score,
            breadth_score=float(breadth.get("score") or 0.0),
            leadership_score=float(leadership.get("score") or 0.0),
            volatility_stress_score=volatility_stress_score,
            liquidity_health_score=liquidity_health_score,
            data_quality_score=data_quality_score,
            risk_appetite=risk_appetite,
            deltas=deltas,
            regime_ctx=regime_ctx,
            now_i=asof_i,
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
            # REDUCED instead of DISABLED: mean-reversion swings are
            # among the best setups in bear markets. Disabling them entirely
            # removes the system's edge in exactly the conditions it should thrive.
            swing_permission = "REDUCED"

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
            # LOCKDOWN: still reduced, but enough to take high-conviction trades.
            size_multiplier = 0.40
            max_positions_multiplier = 0.50

        # ── Regime-level override on top of risk_mode (Item 5) ───────────────
        # PANIC regime → cap at 0.50× regardless of risk_mode so we never over-
        # commit capital when the market is in free-fall.  Strong TREND_UP +
        # AGGRESSIVE is the best environment — allow a modest size boost.
        if regime == "PANIC":
            size_multiplier = min(size_multiplier, 0.50)
            max_positions_multiplier = min(max_positions_multiplier, 0.50)
        elif regime == "TREND_UP" and risk_mode == "AGGRESSIVE":
            size_multiplier = min(1.50, round(size_multiplier * 1.30, 2))
            max_positions_multiplier = min(1.50, round(max_positions_multiplier * 1.20, 2))

        allowed_strategies = [
            "BREAKOUT",
            "SHORT_BREAKDOWN",   # explicit — was only reachable via fragile substring alias
            "PULLBACK",
            "SHORT_PULLBACK",    # explicit — was only reachable via fragile substring alias
            "MEAN_REVERSION",
            "VWAP_TREND",
            "VWAP_REVERSAL",
            "OPEN_DRIVE",
            "MOMENTUM",          # swing relative-strength leader chasing
        ]
        if regime in {"CHOP", "PANIC"}:
            # Remove momentum-chasing strategies; keep reversal/mean-reversion.
            # PULLBACK stays: short-side pullbacks are valid in bear markets.
            allowed_strategies = [s for s in allowed_strategies if s not in {"BREAKOUT", "OPEN_DRIVE", "MOMENTUM"}]
        if regime in {"TREND_DOWN"}:
            # Down-trend: remove BREAKOUT (upside breakouts fail) and MOMENTUM
            # (buying leaders into a downtrend = catching a knife). Keep PULLBACK
            # (short pullbacks are scored in setup scoring), keep VWAP strategies.
            allowed_strategies = [s for s in allowed_strategies if s not in {"BREAKOUT", "OPEN_DRIVE", "MOMENTUM"}]
        if regime == "PANIC":
            # PANIC: minimal strategies, but VWAP_REVERSAL is the best edge.
            # Keep PULLBACK for short-pullback setups. Add VWAP_TREND only if
            # data quality is sufficient (quality gate checked elsewhere).
            allowed_strategies = [s for s in allowed_strategies if s not in {"BREAKOUT", "OPEN_DRIVE", "MOMENTUM"}]
        if not allowed_strategies:
            allowed_strategies = ["MEAN_REVERSION", "VWAP_REVERSAL"]
        # P0-2 (2026-04-22): apply the disabled_strategies blocklist AFTER regime
        # logic. This is the strategy kill-switch — strategies with proven
        # negative expectancy (see StrategySettings.disabled_strategies) are
        # removed regardless of regime. If the blocklist would empty the list,
        # fall back to MEAN_REVERSION so scans still produce some signal.
        _disabled = {str(s).upper() for s in (StrategySettings().disabled_strategies or ())}
        if _disabled:
            _kept = [s for s in allowed_strategies if s.upper() not in _disabled]
            if not _kept:
                _kept = ["MEAN_REVERSION"]
            allowed_strategies = _kept

        no_lookahead_valid = self._validate_phase_no_lookahead(state=MarketBrainState(asof_ts=asof_i.isoformat(), phase=phase), context={"regimeContext": regime_ctx})
        confidence_family = self._confidence_family(
            phase=phase,
            trend_score=trend_score,
            breadth_score=float(breadth.get("score") or 0.0),
            leadership_score=float(leadership.get("score") or 0.0),
            volatility_stress_score=volatility_stress_score,
            liquidity_health_score=liquidity_health_score,
            data_quality_score=data_quality_score,
            risk_appetite=risk_appetite,
            quality_ctx=quality_ctx,
            no_lookahead_valid=no_lookahead_valid,
        )
        run_degraded_flag = bool(
            data_quality_score < 55.0
            or confidence_family.get("run_integrity_confidence", 100.0) < 55.0
            or confidence_family.get("phase2_confidence", 100.0) < 35.0
        )
        # ── PR-1 Tier-0: PCR / FII-DII / breadth-RoC / signal-age decay ──────
        options_positioning_score = 50.0
        flow_score = 50.0
        if live_regime is not None:
            try:
                options_positioning_score = self._pcr_to_positioning_score(
                    float(live_regime.pcr.pcr_weighted or live_regime.pcr.pcr or 1.0),
                    confidence=float(live_regime.pcr.confidence or 0.0),
                )
                flow_score = self._fii_dii_to_flow_score(
                    float(live_regime.fii.fii or 0.0),
                    float(live_regime.fii.dii or 0.0),
                    freshness=float(live_regime.fii.freshness_score or 0.0),
                )
            except Exception:
                logger.debug("market_brain_v2 PCR/FII score derivation failed", exc_info=True)
        breadth_roc_score = self._breadth_roc_score(
            float(breadth.get("score") or 0.0), prior,
        )
        signal_age_penalty = self._signal_age_penalty(live_regime)
        # Decay market_confidence for stale signals — informational telemetry is kept
        # on state.signal_age_penalty so the dashboard can show WHY confidence dipped.
        market_confidence_raw = float(confidence_family.get("market_confidence", 50.0))
        market_confidence_decayed = self._clip(market_confidence_raw - signal_age_penalty, 0.0, 100.0)

        # Regime-transition tracking — enables FE timeline + PANIC-exit-age debugging
        is_transition = bool(prior is not None and prior.regime != regime)
        if is_transition:
            prev_regime_label = prior.regime if prior is not None else ""
            regime_age_seconds = 0.0
        elif prior is not None:
            prev_regime_label = getattr(prior, "prev_regime", "") or ""
            prior_ts = parse_any_ts(prior.asof_ts)
            if prior_ts is not None:
                age_now = (asof_i - prior_ts.astimezone(IST)).total_seconds()
                regime_age_seconds = float(getattr(prior, "regime_age_seconds", 0.0) or 0.0) + max(0.0, age_now)
            else:
                regime_age_seconds = float(getattr(prior, "regime_age_seconds", 0.0) or 0.0)
        else:
            prev_regime_label = ""
            regime_age_seconds = 0.0
        regime_transitions_today = self._count_transitions_today(prior, is_transition, asof_i)

        reasons = [
            f"phase={phase}",
            f"trend={round(trend_score, 2)}",
            f"breadth={round(float(breadth.get('score') or 0.0), 2)}",
            f"leadership={round(float(leadership.get('score') or 0.0), 2)}",
            f"stress={round(volatility_stress_score, 2)}",
            f"liq={round(liquidity_health_score, 2)}",
            f"dataQ={round(data_quality_score, 2)}",
            f"appetite={round(risk_appetite, 2)}",
            f"subRegime={sub_regime_v2}",
            f"struct={structure_state}",
            f"event={event_state}",
            f"optionsPos={round(options_positioning_score, 2)}",
            f"flow={round(flow_score, 2)}",
            f"breadthRoC={round(breadth_roc_score, 2)}",
            f"sigAgePenalty={round(signal_age_penalty, 2)}",
            f"transitionsToday={regime_transitions_today}",
            f"degraded={'Y' if run_degraded_flag else 'N'}",
        ]
        if is_transition:
            reasons.append(f"transitionFrom={prev_regime_label}")

        state = MarketBrainState(
            asof_ts=asof_i.isoformat(),
            phase=phase,  # type: ignore[arg-type]
            regime=regime,  # type: ignore[arg-type]
            sub_regime_v2=sub_regime_v2,
            structure_state=structure_state,
            recovery_state=recovery_state,
            event_state=event_state,
            participation=participation,  # type: ignore[arg-type]
            risk_mode=risk_mode,  # type: ignore[arg-type]
            intraday_state=intraday_state,  # type: ignore[arg-type]
            run_degraded_flag=run_degraded_flag,
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
            market_confidence=float(market_confidence_decayed),
            breadth_confidence=float(confidence_family.get("breadth_confidence", 50.0)),
            leadership_confidence=float(confidence_family.get("leadership_confidence", 50.0)),
            phase2_confidence=float(confidence_family.get("phase2_confidence", 50.0)),
            policy_confidence=float(confidence_family.get("policy_confidence", 50.0)),
            run_integrity_confidence=float(confidence_family.get("run_integrity_confidence", 50.0)),
            options_positioning_score=float(options_positioning_score),
            flow_score=float(flow_score),
            breadth_roc_score=float(breadth_roc_score),
            prev_regime=prev_regime_label,
            regime_age_seconds=float(round(regime_age_seconds, 2)),
            regime_transitions_today=int(regime_transitions_today),
            signal_age_penalty=float(signal_age_penalty),
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
            "deltas": {k: round(float(v), 4) for k, v in deltas.items()},
            "confidenceFamily": confidence_family,
            "noLookaheadValid": no_lookahead_valid,
            "runDegradedFlag": bool(run_degraded_flag),
            # PR-1 Tier-0: new inputs surfaced so the FE can visualise them
            "optionsPositioning": {
                "score": round(float(options_positioning_score), 2),
                "pcrWeighted": round(float(live_regime.pcr.pcr_weighted), 3) if live_regime is not None else None,
                "pcrNear": round(float(live_regime.pcr.pcr_near), 3) if live_regime is not None else None,
                "confidence": round(float(live_regime.pcr.confidence), 2) if live_regime is not None else None,
                "expiriesUsed": int(live_regime.pcr.expiries_used) if live_regime is not None else 0,
            },
            "flowSnapshot": {
                "score": round(float(flow_score), 2),
                "fiiNet": round(float(live_regime.fii.fii), 2) if live_regime is not None else None,
                "diiNet": round(float(live_regime.fii.dii), 2) if live_regime is not None else None,
                "asOfDate": live_regime.fii.as_of_date if live_regime is not None else "",
                "freshness": round(float(live_regime.fii.freshness_score), 2) if live_regime is not None else 0.0,
            },
            "breadthRoC": {
                "score": round(float(breadth_roc_score), 2),
                "currentBreadth": round(float(breadth.get("score") or 0.0), 2),
                "priorBreadth": round(float(prior.breadth_score), 2) if prior is not None else None,
            },
            "signalAgePenalty": round(float(signal_age_penalty), 2),
            "marketConfidenceRaw": round(float(market_confidence_raw), 2),
            "regimeTransition": {
                "isTransition": bool(is_transition),
                "fromRegime": prev_regime_label,
                "toRegime": regime,
                "ageSeconds": round(float(regime_age_seconds), 2),
                "transitionsToday": int(regime_transitions_today),
            },
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

    # ================================================================== #
    # PR-2 Tier-1: narrative + explain composition
    # ==================================================================
    #
    # Both builders are PURE over (state, context, policy) — no network,
    # no Firestore. `build_narrative` is called from persist_market_brain_state
    # so the narrative is stored alongside state/context/policy in the
    # Firestore `market_brain/latest` doc. The dashboard reads it via the
    # existing real-time listener with zero extra round-trips.
    # `build_explain_payload` is called by the HTTP /market-brain/explain
    # route — it decorates the stored state with per-component weights and
    # contributions so the UI can render a transparent breakdown.

    # Component weights mirror the `risk_appetite` formula in _build_state
    # (trend 0.26 + breadth 0.24 + leadership 0.20 + liquidity 0.15 +
    # dataQ 0.10 − stress 0.15). Stress is a *penalty*; we surface its
    # weight as a negative number so the UI can show it as "drag".
    _COMPONENT_WEIGHTS: ClassVar[dict[str, float]] = {
        "trend": 0.26,
        "breadth": 0.24,
        "leadership": 0.20,
        "liquidity_health": 0.15,
        "data_quality": 0.10,
        "volatility_stress": -0.15,
    }

    _COMPONENT_LABELS: ClassVar[dict[str, str]] = {
        "trend": "Trend",
        "breadth": "Breadth",
        "leadership": "Leadership",
        "liquidity_health": "Liquidity",
        "data_quality": "Data Quality",
        "volatility_stress": "Volatility Stress",
    }

    @staticmethod
    def _score_band(score: float, *, inverted: bool = False) -> str:
        """Map a 0..100 score to a qualitative band. `inverted=True` for
        stress-like metrics where high = bad."""
        s = float(score or 0.0)
        if inverted:
            if s >= 75: return "severe"
            if s >= 60: return "elevated"
            if s >= 40: return "moderate"
            if s >= 25: return "calm"
            return "quiet"
        if s >= 75: return "strong"
        if s >= 60: return "firm"
        if s >= 45: return "mixed"
        if s >= 30: return "weak"
        return "broken"

    @staticmethod
    def _score_rationale(key: str, score: float) -> str:
        """Short human-readable explanation of a single component's current level."""
        s = float(score or 0.0)
        if key == "trend":
            if s >= 70: return "Nifty daily structure is clearly up-trending."
            if s <= 30: return "Nifty daily structure has broken down."
            return "Nifty daily structure is mixed / range-bound."
        if key == "breadth":
            if s >= 65: return "Broad participation — a majority of stocks are advancing."
            if s <= 35: return "Narrow participation — few stocks advancing."
            return "Breadth is average."
        if key == "leadership":
            if s >= 60: return "Leading sectors and names are confirming the move."
            if s <= 35: return "No clear leadership — risk-off character."
            return "Leadership is mixed."
        if key == "liquidity_health":
            if s >= 65: return "Liquidity is healthy for entries."
            if s <= 40: return "Liquidity is thin; entries may slip."
            return "Liquidity is acceptable."
        if key == "data_quality":
            if s >= 80: return "Pipeline is running clean."
            if s <= 55: return "Pipeline is degraded; some signals may be stale."
            return "Pipeline is running with minor gaps."
        if key == "volatility_stress":
            if s >= 75: return "Volatility is severe — stress regime behaviour."
            if s >= 60: return "Volatility is elevated."
            if s <= 25: return "Volatility is quiet."
            return "Volatility is within normal range."
        return ""

    def _narrative_headline(self, state: MarketBrainState) -> str:
        """One-line headline ≤ ~80 chars suitable for a dashboard hero."""
        regime = (state.regime or "RANGE").replace("_", " ").title()
        risk = (state.risk_mode or "NORMAL").title()
        participation = (state.participation or "MODERATE").title()
        conf = int(round(float(state.market_confidence or 0.0)))
        return f"{regime} · {risk} mode · {participation} participation · confidence {conf}"

    def build_narrative(
        self,
        state: MarketBrainState,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Compose a rule-based narrative from the state + context.

        Deterministic and testable: same inputs → same output. No LLM,
        no randomness. Intended for display on the dashboard's Market
        Brain page.
        """
        ctx = context or {}
        transition = ctx.get("regimeTransition", {}) if isinstance(ctx.get("regimeTransition"), dict) else {}
        options = ctx.get("optionsPositioning", {}) if isinstance(ctx.get("optionsPositioning"), dict) else {}
        flow = ctx.get("flowSnapshot", {}) if isinstance(ctx.get("flowSnapshot"), dict) else {}
        breadth_roc = ctx.get("breadthRoC", {}) if isinstance(ctx.get("breadthRoC"), dict) else {}

        regime = str(state.regime or "RANGE")
        risk_mode = str(state.risk_mode or "NORMAL")
        sentences: list[str] = []
        key_drivers: list[str] = []
        risks: list[str] = []
        opportunities: list[str] = []

        # Sentence 1 — regime summary
        regime_phrase = {
            "TREND_UP":   "Markets are in a confirmed up-trend",
            "TREND_DOWN": "Markets are in a confirmed down-trend",
            "RANGE":      "Markets are range-bound",
            "CHOP":       "Markets are choppy with no clean direction",
            "PANIC":      "Markets are in PANIC — extreme stress detected",
            "RECOVERY":   "Markets are attempting a recovery from a stressed regime",
        }.get(regime, f"Regime is {regime}")
        sentences.append(f"{regime_phrase}. Risk mode: {risk_mode}.")

        # Sentence 2 — strongest supportive and drag components
        scores = {
            "trend": float(state.trend_score or 0.0),
            "breadth": float(state.breadth_score or 0.0),
            "leadership": float(state.leadership_score or 0.0),
            "liquidity_health": float(state.liquidity_health_score or 0.0),
            "data_quality": float(state.data_quality_score or 0.0),
        }
        strongest = max(scores.items(), key=lambda x: x[1])
        weakest = min(scores.items(), key=lambda x: x[1])
        stress = float(state.volatility_stress_score or 0.0)
        sentences.append(
            f"{self._COMPONENT_LABELS[strongest[0]]} is the strongest component at "
            f"{round(strongest[1])}; {self._COMPONENT_LABELS[weakest[0]]} is the weakest "
            f"at {round(weakest[1])}. Volatility stress is {self._score_band(stress, inverted=True)} "
            f"({round(stress)})."
        )
        key_drivers.append(f"{self._COMPONENT_LABELS[strongest[0]]} {round(strongest[1])}")
        if weakest[1] < 45:
            risks.append(f"Low {self._COMPONENT_LABELS[weakest[0]]} ({round(weakest[1])})")

        # Sentence 3 — regime transition context
        is_transition = bool(transition.get("isTransition"))
        transitions_today = int(transition.get("transitionsToday") or 0)
        prev_regime = str(transition.get("fromRegime") or state.prev_regime or "")
        age_sec = float(transition.get("ageSeconds") or state.regime_age_seconds or 0.0)
        if is_transition and prev_regime:
            sentences.append(
                f"Just transitioned from {prev_regime.replace('_', ' ').title()} "
                f"→ {regime.replace('_', ' ').title()} "
                f"({transitions_today} transition{'s' if transitions_today != 1 else ''} today)."
            )
            if transitions_today >= 3:
                risks.append(f"Flippy regime — {transitions_today} transitions today")
        elif age_sec > 0:
            age_min = int(age_sec // 60)
            if age_min >= 1:
                sentences.append(
                    f"Held in {regime.replace('_', ' ').title()} for {age_min} minute"
                    f"{'s' if age_min != 1 else ''} so far."
                )

        # Sentence 4 — signal-age / degraded
        sig_penalty = float(ctx.get("signalAgePenalty") or state.signal_age_penalty or 0.0)
        if state.run_degraded_flag:
            sentences.append("Run is DEGRADED — some data pipelines are stale.")
            risks.append("Degraded run — trust outputs less than usual")
        elif sig_penalty >= 5.0:
            raw_conf = float(ctx.get("marketConfidenceRaw") or state.market_confidence or 0.0)
            sentences.append(
                f"Confidence adjusted down by {round(sig_penalty)} points for stale signals "
                f"(raw {round(raw_conf)} → effective {round(float(state.market_confidence or 0))})."
            )
            risks.append(f"Stale signals ({round(sig_penalty)}-pt penalty)")

        # Sentence 5 — options/flow/breadth-RoC flavour
        flavour_bits: list[str] = []
        opt_score = float(options.get("score") or state.options_positioning_score or 50.0)
        if opt_score >= 70:
            flavour_bits.append("PCR-implied positioning is bullish")
            opportunities.append("Contrarian long setup (oversold PCR)")
        elif opt_score <= 30:
            flavour_bits.append("PCR-implied positioning is bearish / crowded longs")
            risks.append("Crowded longs (low PCR)")
        flow_score = float(flow.get("score") or state.flow_score or 50.0)
        if flow_score >= 70:
            flavour_bits.append("institutional flows are supportive")
            key_drivers.append(f"Institutional inflows (flow {round(flow_score)})")
        elif flow_score <= 30:
            flavour_bits.append("institutional flows are negative")
            risks.append(f"Institutional outflows (flow {round(flow_score)})")
        roc_score = float(breadth_roc.get("score") or state.breadth_roc_score or 50.0)
        if roc_score >= 70:
            flavour_bits.append("breadth is expanding")
            opportunities.append("Expanding breadth — trend-following edge")
        elif roc_score <= 30:
            flavour_bits.append("breadth is contracting")
            risks.append("Breadth is contracting")
        if flavour_bits:
            sentences.append("Secondary signals: " + ", ".join(flavour_bits) + ".")

        # Opportunities by regime/risk-mode
        if regime in {"TREND_UP", "RECOVERY"} and risk_mode == "AGGRESSIVE":
            opportunities.append("Aggressive sizing allowed for high-conviction setups")
        if state.swing_permission == "ENABLED" and regime == "TREND_UP":
            opportunities.append("Swing positions are fully enabled")
        elif state.swing_permission == "REDUCED":
            risks.append("Swing sizing is reduced")
        elif state.swing_permission == "DISABLED":
            risks.append("Swing positions are disabled")

        # De-duplicate while preserving order
        def _dedup(items: list[str]) -> list[str]:
            seen: set[str] = set()
            out: list[str] = []
            for x in items:
                if x and x not in seen:
                    seen.add(x)
                    out.append(x)
            return out

        return {
            "headline": self._narrative_headline(state),
            "sentences": sentences,
            "key_drivers": _dedup(key_drivers)[:4],
            "risks": _dedup(risks)[:4],
            "opportunities": _dedup(opportunities)[:4],
            "as_of": state.asof_ts,
        }

    def build_explain_payload(
        self,
        state: MarketBrainState,
        context: dict[str, Any] | None = None,
        policy: MarketPolicy | None = None,
        narrative: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Compose the /market-brain/explain response — per-component
        weight + contribution + rationale, plus confidence breakdown and
        secondary signals. Pure; no recomputation.
        """
        ctx = context or {}
        deltas_raw = ctx.get("deltas") if isinstance(ctx.get("deltas"), dict) else {}
        score_values = {
            "trend": float(state.trend_score or 0.0),
            "breadth": float(state.breadth_score or 0.0),
            "leadership": float(state.leadership_score or 0.0),
            "liquidity_health": float(state.liquidity_health_score or 0.0),
            "data_quality": float(state.data_quality_score or 0.0),
            "volatility_stress": float(state.volatility_stress_score or 0.0),
        }
        scores_out: list[dict[str, Any]] = []
        for key, weight in self._COMPONENT_WEIGHTS.items():
            score = score_values[key]
            contribution = round(weight * score, 2)
            delta_key = "stress" if key == "volatility_stress" else ("quality" if key == "data_quality" else ("liquidity" if key == "liquidity_health" else key))
            scores_out.append({
                "key": key,
                "label": self._COMPONENT_LABELS[key],
                "score": round(score, 2),
                "weight": round(weight, 4),
                "contribution": contribution,
                "delta": round(float(deltas_raw.get(delta_key) or 0.0), 2),
                "band": self._score_band(score, inverted=(key == "volatility_stress")),
                "rationale": self._score_rationale(key, score),
                "inverted": key == "volatility_stress",
            })

        total_contribution = round(sum(s["contribution"] for s in scores_out), 2)

        confidence_breakdown = {
            "market": round(float(state.market_confidence or 0.0), 2),
            "market_raw": round(float(ctx.get("marketConfidenceRaw") or state.market_confidence or 0.0), 2),
            "signal_age_penalty": round(float(ctx.get("signalAgePenalty") or state.signal_age_penalty or 0.0), 2),
            "breadth": round(float(state.breadth_confidence or 0.0), 2),
            "leadership": round(float(state.leadership_confidence or 0.0), 2),
            "phase2": round(float(state.phase2_confidence or 0.0), 2),
            "policy": round(float(state.policy_confidence or 0.0), 2),
            "run_integrity": round(float(state.run_integrity_confidence or 0.0), 2),
        }

        transition = ctx.get("regimeTransition", {}) if isinstance(ctx.get("regimeTransition"), dict) else {}
        signals = {
            "options_positioning": ctx.get("optionsPositioning", {"score": state.options_positioning_score}),
            "flow": ctx.get("flowSnapshot", {"score": state.flow_score}),
            "breadth_roc": ctx.get("breadthRoC", {"score": state.breadth_roc_score}),
        }

        return {
            "asof_ts": state.asof_ts,
            "phase": state.phase,
            "regime": state.regime,
            "sub_regime_v2": state.sub_regime_v2,
            "risk_mode": state.risk_mode,
            "participation": state.participation,
            "run_degraded_flag": bool(state.run_degraded_flag),
            "narrative": narrative or self.build_narrative(state, ctx),
            "scores": scores_out,
            "total_contribution": total_contribution,
            "risk_appetite": round(float(ctx.get("riskAppetite") or 0.0), 2),
            "confidence": confidence_breakdown,
            "signals": signals,
            "regime_transition": {
                "is_transition": bool(transition.get("isTransition")),
                "from_regime": transition.get("fromRegime", state.prev_regime),
                "to_regime": transition.get("toRegime", state.regime),
                "age_seconds": float(transition.get("ageSeconds") or state.regime_age_seconds or 0.0),
                "transitions_today": int(transition.get("transitionsToday") or state.regime_transitions_today or 0),
            },
            "policy": asdict(policy) if policy is not None else {},
            "reasons": list(state.reasons or []),
        }
