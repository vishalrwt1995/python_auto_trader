from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from autotrader.adapters.firestore_state import FirestoreStateStore
from autotrader.adapters.gcs_store import GoogleCloudStorageStore
from autotrader.adapters.groww_client import GrowwClient
from autotrader.adapters.upstox_client import UpstoxClient
from autotrader.adapters.sheets_repository import GoogleSheetsRepository
from autotrader.domain.indicators import compute_indicators
from autotrader.domain.risk import calc_position_size
from autotrader.domain.scoring import determine_direction, score_signal
from autotrader.services.log_sink import LogSink
from autotrader.services.order_service import OrderService
from autotrader.services.market_brain_service import MarketBrainService
from autotrader.services.regime_service import MarketRegimeService
from autotrader.settings import AppSettings
from autotrader.time_utils import is_entry_window_open_ist, is_market_open_ist, now_ist, now_ist_str

logger = logging.getLogger(__name__)


DEFAULT_WATCHLIST_SCAN_BATCH = 25
DEFAULT_WATCHLIST_SCAN_CORE = 10


@dataclass
class TradingService:
    settings: AppSettings
    sheets: GoogleSheetsRepository
    state: FirestoreStateStore
    gcs: GoogleCloudStorageStore
    groww: GrowwClient
    upstox: UpstoxClient
    regime_service: MarketRegimeService
    market_brain_service: MarketBrainService
    order_service: OrderService
    log_sink: LogSink

    @staticmethod
    def _strategy_allowed(strategy: str, allowed: list[str]) -> bool:
        s = str(strategy or "").strip().upper()
        if not s or s in {"AUTO", "DEFAULT"}:
            return True
        allow = {str(x or "").strip().upper() for x in allowed}
        if not allow:
            return True
        if s in allow:
            return True
        # Preserve backward compatibility with existing strategy labels.
        if "BREAKOUT" in s and "BREAKOUT" in allow:
            return True
        if "OPEN" in s and ("OPEN_DRIVE" in allow or "VWAP_TREND" in allow):
            return True
        if "MEAN" in s and ("MEAN_REVERSION" in allow or "VWAP_REVERSAL" in allow):
            return True
        if "PULLBACK" in s and "PULLBACK" in allow:
            return True
        return False

    def _slice_watchlist_for_scan(self, watchlist: list[Any]) -> tuple[list[Any], dict[str, int | bool]]:
        total = len(watchlist)
        if total == 0:
            return [], {"total": 0, "scanned": 0, "core": 0, "rotated": 0, "nextCursor": 0, "wrapped": True}
        batch = DEFAULT_WATCHLIST_SCAN_BATCH
        core = min(total, DEFAULT_WATCHLIST_SCAN_CORE)
        if total <= core + batch:
            self.state.set_runtime_prop("runtime:watchlist_scan_cursor", "0")
            return watchlist[:], {"total": total, "scanned": total, "core": core, "rotated": max(0, total - core), "nextCursor": 0, "wrapped": True}

        rest = watchlist[core:]
        cursor = int(self.state.get_runtime_prop("runtime:watchlist_scan_cursor", "0") or "0")
        if cursor < 0 or cursor >= len(rest):
            cursor = 0
        end = min(len(rest), cursor + batch)
        rotated = rest[cursor:end]
        wrapped = end >= len(rest)
        next_cursor = 0 if wrapped else end
        self.state.set_runtime_prop("runtime:watchlist_scan_cursor", str(next_cursor))
        return watchlist[:core] + rotated, {
            "total": total,
            "scanned": len(watchlist[:core]) + len(rotated),
            "core": len(watchlist[:core]),
            "rotated": len(rotated),
            "nextCursor": next_cursor,
            "wrapped": wrapped,
        }

    def _fetch_candles(
        self,
        symbol: str,
        exchange: str,
        segment: str,
        *,
        instrument_key: str = "",
        timeframe: str = "15m",
        lookback_days: int = 8,
    ) -> list[list[Any]]:
        path = self.gcs.candle_cache_path(symbol, exchange, segment, timeframe)
        cached = self.gcs.read_candles(path)
        need = 80

        # Upstox-first path for scanner runtime candles (current intraday session).
        # We always attempt this first to keep scanner aligned with the active data provider.
        tf = str(timeframe or "").strip().lower()
        if instrument_key and tf in {"15m", "15min", "15minute"}:
            try:
                api = self.upstox.get_intraday_candles_v3(instrument_key, unit="minutes", interval=15)
                if api:
                    cached = self.gcs.merge_candles(path, api)
            except Exception:
                logger.warning(
                    "scanner_upstox_intraday_fetch_failed symbol=%s exchange=%s segment=%s instrument_key=%s",
                    symbol,
                    exchange,
                    segment,
                    instrument_key,
                    exc_info=True,
                )

        if len(cached) >= need:
            return cached[-need:]

        end = now_ist()
        start = end - timedelta(days=lookback_days)
        from_str = start.strftime("%Y-%m-%d %H:%M:%S")
        to_str = end.strftime("%Y-%m-%d %H:%M:%S")

        # Groww fallback preserves backward compatibility and gives deeper historical intraday bars when available.
        api: list[list[Any]] = []
        try:
            api = self.groww.get_candles_range(symbol, exchange, segment, timeframe, from_str, to_str)
        except Exception:
            logger.warning(
                "scanner_groww_candle_fallback_failed symbol=%s exchange=%s segment=%s",
                symbol,
                exchange,
                segment,
                exc_info=True,
            )
        if api:
            merged = self.gcs.merge_candles(path, api)
            return merged[-max(need, 120):]
        return cached[-max(need, 120):]

    def run_scan_once(self, allow_live_orders: bool = False, force: bool = False) -> dict[str, Any]:
        self.log_sink.action("TradingService", "run_scan_once", "START")
        lease = self.state.try_acquire_lock("run_scan_once", ttl_seconds=90)
        if lease is None:
            self.log_sink.action("TradingService", "run_scan_once", "SKIP", "lock busy")
            self.log_sink.flush_all()
            return {"skipped": "lock_busy"}
        try:
            if not force and not is_market_open_ist():
                self.log_sink.action("TradingService", "run_scan_once", "SKIP", "market closed")
                return {"skipped": "market_closed"}

            recon = self.order_service.reconcile_pending_entries(15)
            if recon.get("filled", 0) or recon.get("failed", 0):
                self.log_sink.log("INFO", "OrderRecon", f"Pending entries reconciled {recon}")

            try:
                brain_state = self.market_brain_service.build_post_open_market_brain(now_ist().isoformat())
                market_policy = self.market_brain_service.derive_market_policy(brain_state)
            except Exception:
                logger.exception("scan_once market_brain_v2_unavailable")
                self.log_sink.action("TradingService", "run_scan_once", "SKIP", "market_brain_v2_unavailable")
                return {"skipped": "market_brain_v2_unavailable"}

            regime = MarketRegimeService.from_market_brain_state(brain_state)
            try:
                if hasattr(self.sheets, "write_market_brain_v2"):
                    self.sheets.write_market_brain_v2(brain_state, market_policy)
            except Exception:
                logger.exception("market_brain_write_failed")
            self.log_sink.decision(
                "REGIME",
                "NIFTY",
                brain_state.regime,
                f"risk={brain_state.risk_mode}",
                {
                    "phase": brain_state.phase,
                    "intradayState": brain_state.intraday_state,
                    "trendScore": brain_state.trend_score,
                    "breadthScore": brain_state.breadth_score,
                    "leadershipScore": brain_state.leadership_score,
                    "volStressScore": brain_state.volatility_stress_score,
                    "liqHealthScore": brain_state.liquidity_health_score,
                    "dataQualityScore": brain_state.data_quality_score,
                },
            )
            self.log_sink.decision(
                "MARKET_BRAIN_V2",
                "NIFTY",
                brain_state.regime,
                f"risk={brain_state.risk_mode}",
                {
                    "phase": brain_state.phase,
                    "intradayState": brain_state.intraday_state,
                    "trendScore": brain_state.trend_score,
                    "breadthScore": brain_state.breadth_score,
                    "leadershipScore": brain_state.leadership_score,
                    "volStressScore": brain_state.volatility_stress_score,
                    "liqHealthScore": brain_state.liquidity_health_score,
                    "dataQualityScore": brain_state.data_quality_score,
                },
            )
            if brain_state.risk_mode == "LOCKDOWN" and not force:
                self.log_sink.action(
                    "TradingService",
                    "run_scan_once",
                    "SKIP",
                    "market brain lockdown",
                    {"regime": brain_state.regime, "riskMode": brain_state.risk_mode},
                )
                return {"skipped": "market_brain_lockdown", "regime": brain_state.regime, "riskMode": brain_state.risk_mode}

            max_signals_allowed = self.market_brain_service.policy_service.max_positions_limit(
                self.settings.strategy.max_positions,
                brain_state,
            )

            watchlist = self.sheets.read_watchlist()
            subset, scan_meta = self._slice_watchlist_for_scan(watchlist)
            if not subset:
                self.log_sink.action("TradingService", "run_scan_once", "SKIP", "watchlist empty")
                return {"skipped": "watchlist_empty"}

            symbol_set = {str(w.symbol).strip().upper() for w in subset if str(w.symbol).strip()}
            key_by_symbol: dict[str, str] = {}
            if symbol_set:
                try:
                    for u in self.sheets.read_universe_rows():
                        sym = str(u.symbol or "").strip().upper()
                        if not sym or sym not in symbol_set:
                            continue
                        ik = str(u.instrument_key or "").strip()
                        if ik and sym not in key_by_symbol:
                            key_by_symbol[sym] = ik
                except Exception:
                    logger.warning("scanner_instrument_key_map_build_failed", exc_info=True)

            scan_rows: list[list[Any]] = []
            signal_rows: list[list[Any]] = []
            qualified = 0

            for w in subset:
                instrument_key = key_by_symbol.get(str(w.symbol).strip().upper(), "")
                candles = self._fetch_candles(
                    w.symbol,
                    w.exchange,
                    w.segment,
                    instrument_key=instrument_key,
                    timeframe="15m",
                    lookback_days=8,
                )
                ind = compute_indicators(candles, self.settings.strategy)
                if ind is None:
                    self.log_sink.decision("SCAN", w.symbol, "SKIP", "insufficient_candles", {"candles": len(candles)})
                    scan_rows.append([
                        w.symbol,
                        0,
                        0,
                        0,
                        0,
                        "HOLD",
                        0,
                        "SKIP|INSUFFICIENT_CANDLES",
                        0,
                        0,
                        0,
                        "MIXED",
                        0,
                        "NA",
                        "NA",
                    ])
                    continue
                direction = determine_direction(ind, regime)
                meta = score_signal(w.symbol, direction, ind, regime, self.settings.strategy)
                adjusted_score = int(self.market_brain_service.adjust_signal(meta.score, brain_state))
                ltp = ind.close
                pos = calc_position_size(ltp, ind.atr, direction if direction != "HOLD" else "BUY", self.settings.strategy)
                setup_conf = max(0.45, min(1.30, (adjusted_score / 100.0) + 0.20))
                liq_mult = 1.0 if ind.volume.ratio >= 1.0 else 0.85
                dq_mult = max(0.6, min(1.1, brain_state.data_quality_score / 100.0))
                pos = self.market_brain_service.size_position_with_market_brain(
                    pos,
                    brain_state,
                    self.settings.strategy,
                    setup_confidence_multiplier=setup_conf,
                    liquidity_multiplier=liq_mult,
                    data_quality_multiplier=dq_mult,
                )

                change_pct = ((ltp - ind.prev_close) / ind.prev_close * 100) if ind.prev_close else 0
                ema_state = "BULL_STACK" if ind.ema_stack else ("BEAR_STACK" if ind.ema_flip else "MIXED")
                macd_view = ind.macd.crossed or ("POS" if ind.macd.hist >= 0 else "NEG")
                policy_tag = f"{regime.regime}|{regime.bias}"
                policy_tag = f"{policy_tag}|{brain_state.regime}|{brain_state.risk_mode}"
                scan_rows.append([
                    w.symbol,
                    round(ltp, 2),
                    round(change_pct, 2),
                    int(ind.volume.curr),
                    round(ind.volume.ratio, 2),
                    direction,
                    int(adjusted_score),
                    policy_tag,
                    round(meta.breakdown.options, 0),
                    round(meta.breakdown.technical, 0),
                    round(meta.breakdown.volume, 0),
                    ema_state,
                    round(ind.rsi.curr, 1),
                    macd_view,
                    "UP" if ind.supertrend.dir == 1 else "DOWN",
                ])

                policy_block_reason = ""
                if direction == "BUY" and market_policy is not None and not bool(market_policy.long_enabled):
                    policy_block_reason = "policy_long_disabled"
                elif direction == "SELL" and market_policy is not None and not bool(market_policy.short_enabled):
                    policy_block_reason = "policy_short_disabled"
                elif market_policy is not None and not self._strategy_allowed(w.strategy, market_policy.allowed_strategies):
                    policy_block_reason = "policy_strategy_blocked"
                elif qualified >= max_signals_allowed:
                    policy_block_reason = "policy_max_positions_reached"

                # Force mode is for scanner diagnostics/backfill only; live/paper entries still respect entry window.
                if direction != "HOLD" and adjusted_score >= self.settings.strategy.min_signal_score and is_entry_window_open_ist() and not policy_block_reason:
                    qualified += 1
                    reason = (
                        f"Score={adjusted_score} RSI={ind.rsi.curr:.1f} VolR={ind.volume.ratio:.2f} "
                        f"Reg={regime.bias}"
                    )
                    reason += f" MB={brain_state.regime}/{brain_state.risk_mode}"
                    self.log_sink.decision("SIGNAL", w.symbol, direction, "entry_qualified", {"score": adjusted_score, "reason": reason})
                    signal_rows.append([
                        now_ist_str(), w.symbol, direction, adjusted_score,
                        round(ltp, 2), round(pos.sl_price, 2), round(pos.target, 2),
                        pos.qty, round(pos.max_loss, 2), round(pos.max_gain, 2),
                        w.strategy, regime.regime, regime.bias, "QUALIFIED",
                    ])
                    self.order_service.place_entry_order(
                        symbol=w.symbol,
                        exchange=w.exchange,
                        segment=w.segment,
                        side="BUY" if direction == "BUY" else "SELL",
                        qty=pos.qty,
                        entry_price=pos.entry_price,
                        sl_price=pos.sl_price,
                        target=pos.target,
                        atr=ind.atr,
                        product=w.product,
                        score=adjusted_score,
                        reason=reason,
                        allow_live_orders=allow_live_orders,
                    )
                else:
                    if direction == "HOLD":
                        why = "direction_hold"
                    elif adjusted_score < self.settings.strategy.min_signal_score:
                        why = "score_below_min"
                    elif policy_block_reason:
                        why = policy_block_reason
                    else:
                        why = "entry_window_closed_or_blocked"
                    self.log_sink.decision("SIGNAL", w.symbol, direction, why, {"score": adjusted_score, "min": self.settings.strategy.min_signal_score})

                time.sleep(0.08)

            self.sheets.replace_scan_rows(scan_rows)
            if signal_rows:
                self.sheets.append_signals(signal_rows)
            self.log_sink.action(
                "TradingService",
                "run_scan_once",
                "DONE",
                "scan complete",
                {
                    "rows": len(scan_rows),
                    "qualified": qualified,
                    "maxSignalsAllowed": max_signals_allowed,
                    **scan_meta,
                    **(
                        {
                            "marketBrainRegime": brain_state.regime,
                            "marketBrainRiskMode": brain_state.risk_mode,
                        }
                        if brain_state is not None
                        else {}
                    ),
                },
            )
            return {
                "rows": len(scan_rows),
                "qualified": qualified,
                "maxSignalsAllowed": max_signals_allowed,
                **scan_meta,
                **(
                    {"marketBrainRegime": brain_state.regime, "marketBrainRiskMode": brain_state.risk_mode}
                    if brain_state is not None
                    else {}
                ),
            }
        finally:
            self.state.release_lock(lease)
            self.log_sink.flush_all()
