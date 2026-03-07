from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from autotrader.domain.models import MarketBrainState, MarketPolicy, PositionSizing
from autotrader.settings import StrategySettings


@dataclass
class MarketPolicyService:
    def derive_market_policy(self, state: MarketBrainState) -> MarketPolicy:
        regime = state.regime
        risk_mode = state.risk_mode

        watchlist_target_multiplier = 1.0
        watchlist_min_score_boost = 0
        liquidity_bucket_floor = "D"
        intraday_phase2_enabled = True
        breakout_enabled = True
        open_drive_enabled = True
        long_enabled = True
        short_enabled = True

        if risk_mode == "AGGRESSIVE":
            watchlist_target_multiplier = 1.10
            watchlist_min_score_boost = 0
            liquidity_bucket_floor = "B"
        elif risk_mode == "NORMAL":
            watchlist_target_multiplier = 1.0
            watchlist_min_score_boost = 2
            liquidity_bucket_floor = "B"
        elif risk_mode == "DEFENSIVE":
            watchlist_target_multiplier = 0.75
            watchlist_min_score_boost = 8
            liquidity_bucket_floor = "A"
            open_drive_enabled = False
        else:
            watchlist_target_multiplier = 0.40
            watchlist_min_score_boost = 18
            intraday_phase2_enabled = False
            breakout_enabled = False
            open_drive_enabled = False
            liquidity_bucket_floor = "A"

        if regime in {"CHOP", "PANIC"}:
            breakout_enabled = False
            open_drive_enabled = False
            watchlist_min_score_boost = max(watchlist_min_score_boost, 10)
        if regime in {"TREND_DOWN", "PANIC"}:
            long_enabled = False
        if regime in {"TREND_UP", "RECOVERY"}:
            short_enabled = False if state.long_bias >= 0.65 else short_enabled
        if regime == "PANIC":
            short_enabled = True
        if regime in {"TREND_DOWN", "PANIC"}:
            intraday_phase2_enabled = intraday_phase2_enabled and (state.data_quality_score >= 55.0)

        reasons = [
            f"regime={state.regime}",
            f"riskMode={state.risk_mode}",
            f"sizeMult={round(float(state.size_multiplier), 3)}",
            f"maxPosMult={round(float(state.max_positions_multiplier), 3)}",
            f"swing={state.swing_permission}",
        ]
        if not breakout_enabled:
            reasons.append("breakout_disabled")
        if not open_drive_enabled:
            reasons.append("open_drive_disabled")
        if not long_enabled:
            reasons.append("long_disabled")
        if not short_enabled:
            reasons.append("short_disabled")

        return MarketPolicy(
            regime=state.regime,
            risk_mode=state.risk_mode,
            allowed_strategies=list(state.allowed_strategies),
            swing_permission=state.swing_permission,
            size_multiplier=float(state.size_multiplier),
            max_positions_multiplier=float(state.max_positions_multiplier),
            watchlist_target_multiplier=float(max(0.25, min(1.25, watchlist_target_multiplier))),
            watchlist_min_score_boost=int(max(0, watchlist_min_score_boost)),
            intraday_phase2_enabled=bool(intraday_phase2_enabled),
            breakout_enabled=bool(breakout_enabled),
            open_drive_enabled=bool(open_drive_enabled),
            long_enabled=bool(long_enabled),
            short_enabled=bool(short_enabled),
            liquidity_bucket_floor=str(liquidity_bucket_floor),
            reasons=reasons,
        )

    def adjust_watchlist_rows(
        self,
        rows: list[dict[str, Any]],
        policy: MarketPolicy,
        *,
        section: str,
    ) -> list[dict[str, Any]]:
        if not rows:
            return []
        liq_rank = {"A": 4, "B": 3, "C": 2, "D": 1}
        floor = liq_rank.get(str(policy.liquidity_bucket_floor).upper(), 1)
        out: list[dict[str, Any]] = []
        for row in rows:
            setup = str(row.get("setupLabel") or "").upper()
            source = str(row.get("source") or "").upper()
            liq = str(row.get("liquidityBucket") or "D").upper()
            if liq_rank.get(liq, 0) < floor:
                continue
            if section == "swing":
                if policy.swing_permission == "DISABLED":
                    continue
                if policy.swing_permission == "REDUCED" and setup == "BREAKOUT":
                    continue
            if not policy.intraday_phase2_enabled and source == "PHASE2_INPLAY":
                continue
            if not policy.breakout_enabled and ("BREAKOUT" in setup or setup == "VWAP_TREND"):
                continue
            if not policy.open_drive_enabled and "OPEN" in setup:
                continue
            out.append(row)
        return out

    def adjust_signal(self, signal_score: int, state: MarketBrainState) -> int:
        base = float(max(0, min(100, int(signal_score))))
        mult = 1.0
        if state.risk_mode == "AGGRESSIVE":
            mult = 1.08
        elif state.risk_mode == "NORMAL":
            mult = 1.0
        elif state.risk_mode == "DEFENSIVE":
            mult = 0.82
        else:
            mult = 0.60
        if state.regime in {"CHOP", "PANIC"}:
            mult *= 0.88
        return int(max(0, min(100, round(base * mult))))

    def size_position_with_market_brain(
        self,
        position_sizing: PositionSizing,
        state: MarketBrainState,
        cfg: StrategySettings,
        *,
        setup_confidence_multiplier: float = 1.0,
        liquidity_multiplier: float = 1.0,
        data_quality_multiplier: float = 1.0,
    ) -> PositionSizing:
        del cfg
        risk_mult = (
            max(0.10, float(state.size_multiplier))
            * max(0.40, min(1.40, float(setup_confidence_multiplier)))
            * max(0.40, min(1.25, float(liquidity_multiplier)))
            * max(0.40, min(1.20, float(data_quality_multiplier)))
        )
        qty = int(max(1, math.floor(float(position_sizing.qty) * risk_mult)))
        if state.risk_mode == "LOCKDOWN":
            qty = max(1, min(qty, max(1, int(position_sizing.qty // 2 or 1))))
        return PositionSizing(
            qty=qty,
            sl_price=position_sizing.sl_price,
            target=position_sizing.target,
            sl_dist=position_sizing.sl_dist,
            entry_price=position_sizing.entry_price,
            max_loss=round(float(position_sizing.max_loss) * (qty / max(1, int(position_sizing.qty))), 2),
            max_gain=round(float(position_sizing.max_gain) * (qty / max(1, int(position_sizing.qty))), 2),
            brokerage=position_sizing.brokerage,
        )

    @staticmethod
    def max_positions_limit(base_max_positions: int, state: MarketBrainState) -> int:
        return max(1, int(math.floor(max(1, int(base_max_positions)) * max(0.25, float(state.max_positions_multiplier)))))
