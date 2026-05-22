"""Production Replica v2 — full-pipeline swing-setup replay.

Replicates production's scan logic EXACTLY for swing setups by:
  - Loading the brain snapshot from the moment of each scan (via brain_loader)
  - Computing indicators from production GCS daily candles (point-in-time)
  - Running production scoring code in production order
  - Applying every gate we can reproduce, in the exact production sequence

## Gate order (mirrors trading_service.py:968-1471)

  1. determine_direction()                  → "direction_hold" if HOLD
  2. score_signal()                          → raw_score
  3. regime_strategy_multiplier(raw_score)   → _affinity_score
  4. brain.adjust_signal(_affinity_score)    → adjusted_score
  5. score_below_min check (uses adjusted_score for blocked_reason recording,
     but qualified check for swing uses _affinity_score)
  6. policy gates (in trading_service.py:1227-1370 order):
     - policy_long_disabled / policy_short_disabled
     - nifty_breadth_too_bullish_for_shorts
     - policy_strategy_blocked          (allowed_strategies check)
     - regime_strategy_hard_block       (_HARD_BLOCKS dict)
     - daily_loss_limit_strategy_restricted   [not replicated; needs portfolio]
     - check_swing_entry → strategy-specific reasons
     - playbook (if enabled)            [defaults OFF]
     - portfolio_sector_concentrated    [not replicated; needs portfolio state]
     - portfolio_strategy_concentrated  [not replicated]
     - earnings_blackout                [not replicated; needs earnings cal]

## Documented approximations (cannot replicate without state)

  - `sl_too_wide_for_risk_budget`     — needs current capital state
  - `capital_exhausted`               — needs realized P&L tracking
  - `reentry_cooldown`                — needs recent_exits set
  - `swing_max_positions_reached`     — needs open positions
  - `stale_signal_price_moved`        — needs live LTP (we use candle close)
  - `live_price_below/above_vwap`     — needs live LTP
  - `portfolio_*`                     — needs portfolio state
  - `earnings_blackout_*`             — needs earnings calendar

These gates affect ~5-15% of production scans. The equivalence test will
quantify the gap and document which scans diverge.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from autotrader.backtest_v2.brain_loader import BrainSnapshot, BrainSnapshotLoader
from autotrader.backtest_v2.data import HistoricalDataset
from autotrader.domain.daily_bias import compute_daily_bias
from autotrader.domain.indicators import compute_indicators
from autotrader.domain.regime_affinity import (
    regime_hard_blocks_strategy,
    regime_strategy_multiplier,
)
from autotrader.domain.scoring import check_swing_entry, determine_direction, score_signal
from autotrader.services.market_policy_service import MarketPolicyService
from autotrader.settings import StrategySettings


SWING_SETUPS_REPLICATED = {"BREAKOUT", "MOMENTUM", "PULLBACK", "MEAN_REVERSION"}


@dataclass
class ScanReplayResult:
    """Mirror of one row in BQ scan_decisions table."""
    symbol: str
    setup: str
    scan_ts: str
    direction: str
    raw_score: int
    affinity_score: int    # raw × affinity_mult
    adjusted_score: int    # post brain adjust_signal
    qualified: bool
    blocked_reason: str
    notes: str = ""        # internal notes about which gate fired


class ProdReplicaV2:
    """Drives a single (symbol, setup, scan_ts) through the full pipeline."""

    def __init__(
        self,
        cfg: StrategySettings | None = None,
        dataset: HistoricalDataset | None = None,
        brain_loader: BrainSnapshotLoader | None = None,
    ) -> None:
        from dataclasses import replace
        # VIX_TREND_MAX: 18.0 matches historical production behavior used
        # to generate the scan_decisions table this replica is validated
        # against. The 2026-05-22 live check showed env var UNSET (so live
        # is at the settings.py default of 15.0), but that is a config
        # drift — historical data (May 21 and earlier) was generated with
        # the env override at 18.0. Use 18.0 for equivalence/historical
        # backtests; use 15.0 only when explicitly modeling current-config.
        # The 4-point regime-layer swing happens at VIX in [15.0, 18.0).
        # See: scoring.py:194 `if regime.vix < cfg.vix_trend_max:`
        base_cfg = cfg or StrategySettings()
        try:
            base_cfg = replace(base_cfg, vix_trend_max=18.0)
        except Exception:
            pass
        self.cfg = base_cfg
        self.ds = dataset or HistoricalDataset()
        self.brain_loader = brain_loader or BrainSnapshotLoader()
        self.policy = MarketPolicyService()  # adjust_signal lives here

    # ---------- helpers ----------

    def _swing_score_threshold(self) -> int:
        return int(self.cfg.swing_min_signal_score)

    def _intraday_score_threshold(self, risk_mode: str) -> int:
        _map = {"AGGRESSIVE": 75, "NORMAL": 72, "DEFENSIVE": 65, "LOCKDOWN": 58}
        return _map.get(risk_mode.upper(), 72)

    def _strategy_allowed(self, strategy: str, allowed_list: list[str]) -> bool:
        """Mirror of TradingService._strategy_allowed."""
        s = (strategy or "").strip().upper()
        if not s or s in ("AUTO", "DEFAULT"):
            return True
        if not allowed_list:
            return True
        upper_allowed = [str(x).strip().upper() for x in allowed_list]
        return s in upper_allowed

    # ---------- core replay ----------

    def replay_scan(
        self,
        symbol: str,
        setup: str,
        scan_ts: str,
        is_swing: bool = True,
        brain_snapshot: BrainSnapshot | None = None,
    ) -> ScanReplayResult:
        """Run the full production pipeline for one (symbol, setup, scan_ts) scan.

        Returns ScanReplayResult that should match the BQ scan_decisions row
        for the same (symbol, setup, scan_ts).
        """
        # Default fields for error cases
        empty = ScanReplayResult(
            symbol=symbol, setup=setup, scan_ts=scan_ts,
            direction="HOLD", raw_score=0, affinity_score=0, adjusted_score=0,
            qualified=False, blocked_reason="",
        )

        # 1. Load brain snapshot for this scan_ts
        snap = brain_snapshot or self.brain_loader.find_snapshot_before(scan_ts)
        if snap is None:
            return ScanReplayResult(**{**empty.__dict__, "blocked_reason": "no_brain_snapshot"})
        regime_state = snap.state
        regime_str = regime_state.regime
        risk_mode = regime_state.risk_mode
        # PROD-MATCH (2026-05-22): Production uses `from_market_brain_state()`
        # which builds RegimeSnapshot from MarketBrainState ONLY — it does NOT
        # populate vix / fii / nifty.change_pct (those default to 0.0). The
        # scoring rule in scoring.py:188-203 evaluates against those defaults,
        # giving a fixed regime layer of +4(nifty) +7(vix<15) +2(fii=0) = 13
        # for every BUY scan. (For SELL: similar but fii.fii<-500 is False so
        # fii=+2 also.) Audit on 187 sample scans showed score_regime is
        # ALWAYS exactly 13 in production.
        #
        # Previously we used `snap.to_regime_snapshot()` which loaded real
        # vix/fii values from the brain snapshot — making the replica score
        # regime layer DIFFERENTLY than production. Switching to
        # from_market_brain_state() mirrors production exactly.
        from autotrader.services.regime_service import MarketRegimeService
        regime_obj = MarketRegimeService.from_market_brain_state(snap.state)
        # Use brain snapshot's VIX (production scoring uses same Upstox
        # fetch as brain, so snapshot VIX = scoring VIX).
        # Leave nifty.change_pct = 0.0 — production's Upstox get_quote()
        # returns ohlc.close = current LTP during market hours, which
        # makes the fallback computation evaluate to 0. Empirically
        # verified: setting change_pct=0 matches production BQ data.
        # 1m backfill of NIFTY/VIX is loaded but not used here — kept
        # for potential future use (different scoring versions).

        # 2. Build the candle series production saw at this scan moment.
        #
        #    Production trading_service.py:833-841 sources candles based on
        #    wl_type:
        #      - wl_type=swing    → 1d candles, lookback_days=120
        #      - wl_type=intraday → 15m candles, lookback_days=8
        #
        #    The "swing" naming refers to position holding period; both
        #    "swing" and "intraday" rows can scan the SAME setups (BREAKOUT,
        #    MR etc.) — what differs is which timeframe `ind` is computed
        #    from. Critically, intraday rows on a BREAKOUT setup use 15m
        #    candles, NOT 1d. That's why RSI changes through the day for
        #    these scans.
        #
        #    For SWING swing rows we also synthesize a partial daily bar
        #    from 5m intraday because Upstox's live daily candle API
        #    includes today's still-forming bar with live LTP as close.
        scan_date = scan_ts[:10]

        if is_swing:
            # Swing path: daily candles only, 120-day lookback.
            #
            # CRITICAL (debugged 2026-05-21 via BHEL May 8 vol_ratio bug):
            # production fetches daily candles from Upstox's historical-days
            # API during market hours. That API returns ONLY completed daily
            # bars (today's partial daily isn't included). So at 09:25 May 8,
            # production sees daily series ending at May 7.
            #
            # We must NOT synthesize a partial today bar — doing so introduces
            # a fake current candle that production never sees, breaking
            # vol_ratio, RSI, and other "current bar" indicators.
            daily_all = self.ds.daily_candles(symbol)
            daily_before = [c for c in daily_all if str(c[0])[:10] < scan_date]
            primary_candles = daily_before[-120:] if len(daily_before) > 120 else daily_before
            if len(primary_candles) < 60:
                return ScanReplayResult(**{**empty.__dict__, "blocked_reason": "insufficient_candles"})

            daily_for_bias = primary_candles
        else:
            # Intraday path: 15m candles up to scan_ts, 8-day lookback.
            #
            # Look-ahead fix: the 15m bar whose START is < scan_ts but whose
            # END (start+15min) is > scan_ts is STILL FORMING at scan_ts.
            # Our cache stores its EOD close (15 minutes in the future from
            # scan_ts perspective). Including it leaks future data.
            #
            # Fix: keep only 15m bars whose END (start+15min) ≤ scan_ts,
            # then synthesize a partial 15m bar from 5m candles whose ts
            # is in [last_closed_15m_end, scan_ts).
            scan_ts_short = scan_ts[:19]  # strip timezone for comparison
            intra_15m_all = self.ds.intraday_candles(symbol, timeframe="15m")

            def _add_15min(ts: str) -> str:
                """Return ts + 15min in same ISO format (strip tz for compare)."""
                from datetime import datetime, timedelta
                try:
                    base = datetime.fromisoformat(ts.replace("+05:30", ""))
                    return (base + timedelta(minutes=15)).isoformat()
                except Exception:
                    return ts

            closed_15m = [
                c for c in intra_15m_all
                if _add_15min(str(c[0])[:19]) <= scan_ts_short
            ]
            # Note: we deliberately do NOT synthesize a partial 15m bar from 5m
            # data here. Testing showed the synth bar overshoots reality (5m
            # last close ≠ live LTP at scan_ts), introducing more error than
            # leaving the partial bar out. Production may or may not include
            # the partial — empirically, using closed-only 15m gives RSI 39.0
            # vs BQ's 41.2 (within tolerance) while synth gives 37.1.

            # Production uses 8 days; with ~25 bars/day that's ~200 bars
            primary_candles = closed_15m[-200:] if len(closed_15m) > 200 else closed_15m
            if len(primary_candles) < 30:
                return ScanReplayResult(**{**empty.__dict__, "blocked_reason": "insufficient_candles"})

            # Daily bias still uses daily candles (intraday_daily_map in prod)
            daily_all = self.ds.daily_candles(symbol)
            daily_before = [c for c in daily_all if str(c[0])[:10] < scan_date]
            daily_for_bias = daily_before[-120:] if len(daily_before) > 120 else daily_before

        # 3. Compute indicators (on primary) + daily_bias (on daily)
        try:
            ind = compute_indicators(primary_candles, self.cfg)
            db = compute_daily_bias(daily_for_bias) if daily_for_bias else None
        except Exception as exc:
            return ScanReplayResult(**{**empty.__dict__, "blocked_reason": f"indicator_error:{type(exc).__name__}"})
        if ind is None:
            return ScanReplayResult(**{**empty.__dict__, "blocked_reason": "indicator_none"})

        # 4. determine_direction
        try:
            direction = determine_direction(
                ind, regime_obj, setup=setup,
                wl_type=("swing" if is_swing else "intraday"),
                daily_bias=db,
            )
        except Exception as exc:
            return ScanReplayResult(**{**empty.__dict__, "blocked_reason": f"direction_error:{type(exc).__name__}"})

        if direction == "HOLD":
            return ScanReplayResult(
                symbol=symbol, setup=setup, scan_ts=scan_ts,
                direction="HOLD", raw_score=0, affinity_score=0, adjusted_score=0,
                qualified=False, blocked_reason="direction_hold",
            )

        # 5. score_signal → raw_score
        try:
            sig = score_signal(symbol, direction, ind, regime_obj, self.cfg, daily_bias=db, setup=setup)
        except Exception as exc:
            return ScanReplayResult(**{**empty.__dict__, "direction": direction, "blocked_reason": f"score_error:{type(exc).__name__}"})
        raw_score = int(sig.score)

        # 6. apply affinity multiplier → _affinity_score
        affinity_mult = regime_strategy_multiplier(regime_str, setup, direction)
        affinity_score = max(0, min(100, int(round(raw_score * affinity_mult))))

        # 7. apply brain adjust_signal haircut → adjusted_score
        try:
            adjusted_score = int(self.policy.adjust_signal(affinity_score, regime_state))
        except Exception:
            adjusted_score = affinity_score  # fail-open if brain policy fails
        adjusted_score = max(0, min(100, adjusted_score))

        # 8. dynamic_min_score
        dynamic_min_score = self._swing_score_threshold() if is_swing else self._intraday_score_threshold(risk_mode)

        # 9. Score check — for swing, _score_for_threshold = _affinity_score; for intraday = adjusted_score
        _score_for_threshold = affinity_score if is_swing else adjusted_score

        # 10. Policy gate chain (mirror trading_service.py:1227-1370)
        policy_block_reason = ""
        raw_policy = snap.raw_policy or {}

        # 10a. policy_long_disabled / policy_short_disabled
        if direction == "BUY" and not bool(raw_policy.get("long_enabled", True)):
            policy_block_reason = "policy_long_disabled"
        elif direction == "SELL" and not bool(raw_policy.get("short_enabled", True)):
            policy_block_reason = "policy_short_disabled"

        # 10b. nifty_breadth_too_bullish_for_shorts
        elif (
            direction == "SELL"
            and regime_str in ("RANGE", "CHOP", "RECOVERY")
            and regime_state.breadth_score >= 75
            # P1 exemption: swing shorts whose own daily trend is DOWN
            and not (
                is_swing
                and db is not None
                and str(getattr(db, "trend", "") or "").upper() == "DOWN"
            )
        ):
            policy_block_reason = "nifty_breadth_too_bullish_for_shorts"

        # 10c. policy_strategy_blocked
        elif not self._strategy_allowed(setup, raw_policy.get("allowed_strategies", [])):
            policy_block_reason = "policy_strategy_blocked"

        # 10d. regime_strategy_hard_block
        elif regime_hard_blocks_strategy(regime_str, setup):
            policy_block_reason = "regime_strategy_hard_block"

        # 10e. check_swing_entry (strategy-specific)
        else:
            if is_swing:
                try:
                    gate_ok, gate_reason = check_swing_entry(setup, direction, ind, db, regime=regime_str)
                except Exception as exc:
                    gate_ok = False
                    gate_reason = f"check_swing_entry_error:{type(exc).__name__}"
                if not gate_ok:
                    policy_block_reason = gate_reason

        # 11. Final qualification check
        is_entry_window_open = True  # daily scans assume window open; intraday would need scan_ts time check
        qualified = (
            direction != "HOLD"
            and _score_for_threshold >= dynamic_min_score
            and is_entry_window_open
            and not policy_block_reason
        )

        # 12. Blocked reason determination (BQ semantics: lines 1463-1470)
        if qualified:
            blocked_reason = ""
        elif direction == "HOLD":
            blocked_reason = "direction_hold"
        elif adjusted_score < dynamic_min_score:
            # Note: BQ uses adjusted_score here, NOT _score_for_threshold
            blocked_reason = "score_below_min"
        elif policy_block_reason:
            blocked_reason = policy_block_reason
        else:
            blocked_reason = "entry_window_closed_or_blocked"

        return ScanReplayResult(
            symbol=symbol, setup=setup, scan_ts=scan_ts,
            direction=direction,
            raw_score=raw_score,
            affinity_score=affinity_score,
            adjusted_score=adjusted_score,
            qualified=qualified,
            blocked_reason=blocked_reason,
        )
