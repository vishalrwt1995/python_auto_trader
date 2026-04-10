from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from autotrader.adapters.firestore_state import FirestoreStateStore
from autotrader.adapters.gcs_store import GoogleCloudStorageStore
from autotrader.adapters.pubsub_client import PubSubClient
from autotrader.adapters.upstox_client import UpstoxClient
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
    state: FirestoreStateStore
    gcs: GoogleCloudStorageStore
    upstox: UpstoxClient
    regime_service: MarketRegimeService
    market_brain_service: MarketBrainService
    order_service: OrderService
    log_sink: LogSink
    pubsub: PubSubClient | None = None

    def _read_watchlist_with_fallback(self) -> list[Any]:
        """Return watchlist rows — Firestore primary, Sheets fallback."""
        from autotrader.domain.models import WatchlistRow
        rows: list[Any] = []
        try:
            doc = self.state.get_watchlist()
            if doc and isinstance(doc.get("rows"), list) and doc["rows"]:
                rows = []
                for r in doc["rows"]:
                    sym = str(r.get("symbol", "")).strip().upper()
                    if not sym:
                        continue
                    enabled_raw = str(r.get("enabled", "Y") or "Y").strip().upper()
                    if enabled_raw not in {"Y", "YES", "TRUE", "1", "ENABLED"}:
                        continue
                    rows.append(WatchlistRow(
                        symbol=sym,
                        exchange=str(r.get("exchange", "NSE") or "NSE").upper(),
                        segment="CASH",
                        product="MIS",
                        strategy=str(r.get("setup", r.get("strategy", "AUTO")) or "AUTO"),
                        sector=str(r.get("macrosector", r.get("sector", "UNKNOWN")) or "UNKNOWN"),
                        beta=float(r.get("beta", 1.0) or 1.0),
                        enabled=True,
                        note=str(r.get("reason", r.get("notes", "")) or ""),
                    ))
                if rows:
                    logger.debug("watchlist_read_source=firestore count=%d", len(rows))
                    return rows
        except Exception:
            logger.warning("firestore_watchlist_read_failed", exc_info=True)
        return rows

    def _read_universe_instrument_keys(self, symbol_set: set[str]) -> dict[str, str]:
        """Return {symbol: instrument_key} from Firestore."""
        result: dict[str, str] = {}
        try:
            universe_rows = self.state.list_universe(limit=3000)
            for row in universe_rows:
                sym = str(row.get("symbol", "")).strip().upper()
                ik = str(row.get("instrument_key", "") or "").strip()
                if sym and ik and sym in symbol_set:
                    result[sym] = ik
            logger.debug("universe_key_map_source=firestore count=%d", len(result))
        except Exception:
            logger.warning("firestore_universe_read_failed", exc_info=True)
        return result

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
        # Short-side setups: allowed when mean-reversion/reversal strategies are
        # active. These setups are only scored in bearish regimes (PANIC/TREND_DOWN)
        # where MEAN_REVERSION and VWAP_REVERSAL are always allowed.
        if s.startswith("SHORT_") and ("MEAN_REVERSION" in allow or "VWAP_REVERSAL" in allow):
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

        # Upstox intraday candle fallback for symbols not in GCS cache.
        # Only attempt if we have a valid instrument key — raw symbol strings (e.g. "WIPRO")
        # are not accepted by the Upstox v3 API and will return HTTP 400.
        api: list[list[Any]] = []
        if instrument_key:
            try:
                api = self.upstox.get_historical_candles_v3_intraday_range(
                    instrument_key,
                    from_date=start.strftime("%Y-%m-%d"),
                    to_date=end.strftime("%Y-%m-%d"),
                    unit="minutes",
                    interval=int(timeframe.replace("m", "").replace("min", "")) if timeframe else 15,
                )
            except Exception:
                logger.warning(
                    "scanner_upstox_candle_fallback_failed symbol=%s exchange=%s segment=%s",
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
            # LOCKDOWN no longer hard-blocks the scanner. Dynamic min_signal_score
            # (threshold=45 in LOCKDOWN) + size_multiplier=0.40 already enforce
            # capital-preservation. A hard skip means zero signals ever fire in a
            # crash — exactly the opposite of what we want (mean-reversion bounces
            # are most profitable at capitulation lows).
            # We keep the log so dashboards can surface the regime warning.
            if brain_state.risk_mode == "LOCKDOWN":
                self.log_sink.action(
                    "TradingService",
                    "run_scan_once",
                    "WARN",
                    "market brain lockdown — scanning with reduced size + threshold",
                    {"regime": brain_state.regime, "riskMode": brain_state.risk_mode},
                )

            max_signals_allowed = self.market_brain_service.policy_service.max_positions_limit(
                self.settings.strategy.max_positions,
                brain_state,
            )

            # Read watchlist: Firestore is primary, Sheets is fallback
            watchlist = self._read_watchlist_with_fallback()
            subset, scan_meta = self._slice_watchlist_for_scan(watchlist)
            if not subset:
                self.log_sink.action("TradingService", "run_scan_once", "SKIP", "watchlist empty")
                return {"skipped": "watchlist_empty"}

            symbol_set = {str(w.symbol).strip().upper() for w in subset if str(w.symbol).strip()}
            key_by_symbol: dict[str, str] = {}
            if symbol_set:
                try:
                    # Read instrument keys: Firestore universe first, then Sheets fallback
                    universe_rows = self._read_universe_instrument_keys(symbol_set)
                    for sym, ik in universe_rows.items():
                        if ik and sym not in key_by_symbol:
                            key_by_symbol[sym] = ik
                except Exception:
                    logger.warning("scanner_instrument_key_map_build_failed", exc_info=True)

            scan_rows: list[list[Any]] = []
            signal_rows: list[list[Any]] = []
            bq_signals: list[dict[str, Any]] = []
            # Rich per-symbol scan results saved to Firestore for dashboard visibility
            scan_result_rows: list[dict[str, Any]] = []
            import uuid as _uuid
            scanner_run_id = _uuid.uuid4().hex[:12]
            qualified = 0

            # Batch-fetch real-time LTP for all watchlist symbols before the scan loop.
            # This replaces candle close (up to 15-min stale) with live price at entry time.
            # Indicators (EMA/RSI/MACD/SuperTrend) still use candle history — correct.
            live_ltp_map: dict[str, float] = {}
            _all_iks = [ik for ik in key_by_symbol.values() if ik]
            if _all_iks:
                try:
                    _chunk_size = 500
                    for _ci in range(0, len(_all_iks), _chunk_size):
                        _chunk = _all_iks[_ci : _ci + _chunk_size]
                        _quotes = self.upstox.get_ltp_v3(_chunk)
                        for _ik, _q in _quotes.items():
                            if _q.ltp > 0:
                                live_ltp_map[_ik] = _q.ltp
                    logger.info("live_ltp_prefetch fetched=%d of %d", len(live_ltp_map), len(_all_iks))
                except Exception:
                    logger.warning("live_ltp_prefetch_failed — falling back to candle close", exc_info=True)

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
                    scan_result_rows.append({
                        "symbol": w.symbol,
                        "ltp": 0.0,
                        "changePct": 0.0,
                        "volRatio": 0.0,
                        "direction": "SKIP",
                        "score": 0,
                        "emaState": "MIXED",
                        "rsi": 0.0,
                        "macdView": "NA",
                        "supertrend": "NA",
                        "setup": w.strategy or "",
                        "status": "skip",
                        "reason": "insufficient_candles",
                    })
                    continue
                direction = determine_direction(ind, regime)
                meta = score_signal(w.symbol, direction, ind, regime, self.settings.strategy)
                adjusted_score = max(0, min(100, int(self.market_brain_service.adjust_signal(meta.score, brain_state))))
                # Use live LTP (real-time) for entry price; candle close as fallback if API unavailable
                _live = live_ltp_map.get(instrument_key, 0.0)
                ltp = _live if _live > 0 else ind.close

                # ── Volatility-scaled ATR multiplier (Item 4) ────────────────
                # In PANIC/LOCKDOWN the ATR is already 3-4x its normal value.
                # Applying the base 1.5× multiplier would produce gigantic SLs
                # that (a) allow huge drawdowns and (b) force qty → 1 always.
                # We shrink the multiplier so the *effective* SL distance stays
                # sensible for mean-reversion entries.  In strong TREND_UP we
                # give momentum trades more room so they aren't stopped on noise.
                _brain_regime = brain_state.regime if brain_state else "RANGE"
                _brain_risk   = brain_state.risk_mode if brain_state else "NORMAL"
                if _brain_risk == "LOCKDOWN" or _brain_regime == "PANIC":
                    _atr_mult = round(self.settings.strategy.atr_sl_mult * 0.75, 3)
                elif _brain_risk == "DEFENSIVE" or _brain_regime in ("TREND_DOWN", "CHOP"):
                    _atr_mult = round(self.settings.strategy.atr_sl_mult * 0.87, 3)
                elif _brain_risk == "AGGRESSIVE" and _brain_regime == "TREND_UP":
                    _atr_mult = round(self.settings.strategy.atr_sl_mult * 1.20, 3)
                else:
                    _atr_mult = self.settings.strategy.atr_sl_mult

                pos = calc_position_size(ltp, ind.atr, direction if direction != "HOLD" else "BUY", self.settings.strategy, atr_mult_override=_atr_mult)

                # ── Dynamic min_signal_score (Item 3) ────────────────────────
                # adjust_signal() already penalises adjusted_score by 0.60–0.82×
                # in DEFENSIVE/LOCKDOWN + an extra 0.88× in PANIC/CHOP regime.
                # Keeping a static threshold of 72 means even a raw-90 signal
                # gets filtered in PANIC (90 × 0.72 = 64.8 < 72).  We lower the
                # threshold proportionally so the *top decile* of raw signals
                # can still qualify regardless of regime.
                _SCORE_THRESHOLDS = {
                    "AGGRESSIVE": 75,   # bar raised: only the best in bull runs
                    "NORMAL":     72,   # unchanged
                    "DEFENSIVE":  58,   # ≈ raw 72 after ×0.82×0.88 adjustment
                    "LOCKDOWN":   45,   # ≈ raw 85 after ×0.60×0.88 adjustment
                }
                dynamic_min_score = _SCORE_THRESHOLDS.get(
                    brain_state.risk_mode if brain_state else "NORMAL",
                    self.settings.strategy.min_signal_score,
                )

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
                # Rich scan result for Firestore (reason filled in after qualification check)
                _scan_row_rich: dict[str, Any] = {
                    "symbol": w.symbol,
                    "ltp": round(ltp, 2),
                    "changePct": round(change_pct, 2),
                    "volRatio": round(ind.volume.ratio, 2),
                    "direction": direction,
                    "score": int(adjusted_score),
                    "emaState": ema_state,
                    "rsi": round(ind.rsi.curr, 1),
                    "macdView": macd_view,
                    "supertrend": "UP" if ind.supertrend.dir == 1 else "DOWN",
                    "setup": w.strategy or "",
                    "vwap": round(ind.vwap, 2) if ind.vwap else 0.0,
                    "sl": 0.0,
                    "target": 0.0,
                    "qty": 0,
                    "status": "scanned",
                    "reason": "",
                    # Item 2: expose threshold used so dashboard can show gap-to-qualify
                    # and we can tune thresholds from paper-trade data later.
                    "minScore": int(dynamic_min_score),
                    "atrMult": round(_atr_mult, 3),
                }

                policy_block_reason = ""
                if direction == "BUY" and market_policy is not None and not bool(market_policy.long_enabled):
                    policy_block_reason = "policy_long_disabled"
                elif direction == "SELL" and market_policy is not None and not bool(market_policy.short_enabled):
                    policy_block_reason = "policy_short_disabled"
                elif market_policy is not None and not self._strategy_allowed(w.strategy, market_policy.allowed_strategies):
                    policy_block_reason = "policy_strategy_blocked"
                elif qualified >= max_signals_allowed:
                    policy_block_reason = "policy_max_positions_reached"
                # Live VWAP guard: if price drifted to wrong side of VWAP since candle close, reject entry.
                # Only fires when we have a fresh live LTP (_live > 0) — not when falling back to candle close.
                elif direction == "BUY" and _live > 0 and ltp < ind.vwap and w.strategy not in ("MEAN_REVERSION", "VWAP_REVERSAL"):
                    policy_block_reason = "live_price_below_vwap"
                elif direction == "SELL" and _live > 0 and ltp > ind.vwap and w.strategy not in ("MEAN_REVERSION", "VWAP_REVERSAL"):
                    policy_block_reason = "live_price_above_vwap"

                # Force mode is for scanner diagnostics/backfill only; live/paper entries still respect entry window.
                if direction != "HOLD" and adjusted_score >= dynamic_min_score and is_entry_window_open_ist() and not policy_block_reason:
                    qualified += 1
                    reason = (
                        f"Score={adjusted_score} RSI={ind.rsi.curr:.1f} VolR={ind.volume.ratio:.2f} "
                        f"Reg={regime.bias}"
                    )
                    reason += f" MB={brain_state.regime}/{brain_state.risk_mode}"
                    self.log_sink.decision("SIGNAL", w.symbol, direction, "entry_qualified", {"score": adjusted_score, "reason": reason})
                    _scan_ts = now_ist_str()
                    signal_rows.append([
                        _scan_ts, w.symbol, direction, adjusted_score,
                        round(ltp, 2), round(pos.sl_price, 2), round(pos.target, 2),
                        pos.qty, round(pos.max_loss, 2), round(pos.max_gain, 2),
                        w.strategy, regime.regime, regime.bias, "QUALIFIED",
                    ])
                    bq_signals.append({
                        "scan_ts": _scan_ts,
                        "run_date": _scan_ts[:10],
                        "symbol": w.symbol,
                        "direction": direction,
                        "score": adjusted_score,
                        "ltp": round(ltp, 2),
                        "sl": round(pos.sl_price, 2),
                        "target": round(pos.target, 2),
                        "qty": pos.qty,
                        "regime": brain_state.regime if brain_state else regime.regime,
                        "risk_mode": brain_state.risk_mode if brain_state else "",
                        "entry_placed": True,
                        "blocked_reason": "",
                        "scanner_run_id": scanner_run_id,
                    })
                    _scan_row_rich.update({
                        "sl": round(pos.sl_price, 2),
                        "target": round(pos.target, 2),
                        "qty": pos.qty,
                        "status": "qualified",
                        "reason": "entry_qualified",
                    })
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
                    elif adjusted_score < dynamic_min_score:
                        why = "score_below_min"
                    elif policy_block_reason:
                        why = policy_block_reason
                    else:
                        why = "entry_window_closed_or_blocked"
                    self.log_sink.decision("SIGNAL", w.symbol, direction, why, {"score": adjusted_score, "min": dynamic_min_score})
                    _scan_row_rich.update({"status": "filtered", "reason": why})
                scan_result_rows.append(_scan_row_rich)

                time.sleep(0.08)

            run_ts = now_ist_str()
            self.state.set_runtime_prop("runtime:scanner_last_run_ts", run_ts)
            if signal_rows:
                self.state.set_runtime_prop("runtime:signals_last_write_ts", run_ts)
            # Persist scan audit trail to Firestore for dashboard real-time visibility.
            # This replaces the old Google Sheets scan log — every symbol's direction,
            # score, and reason is now visible on the Signals page.
            try:
                self.state.set_json(
                    "scan_results",
                    "latest",
                    {
                        "scan_ts": run_ts,
                        "run_date": run_ts[:10],
                        "scanner_run_id": scanner_run_id,
                        "regime": brain_state.regime if brain_state else "",
                        "risk_mode": brain_state.risk_mode if brain_state else "",
                        "total_watchlist": scan_meta.get("total", 0),
                        "scanned": len(scan_result_rows),
                        "qualified": qualified,
                        "rows": scan_result_rows,
                    },
                    merge=False,
                )
            except Exception:
                logger.warning("scan_results_firestore_save_failed — non-critical", exc_info=True)
            # Publish signals to Pub/Sub + BigQuery (best-effort)
            if bq_signals:
                try:
                    from autotrader.adapters.bigquery_client import BigQueryClient as _BQC
                    bq: _BQC | None = getattr(self.order_service, "bq", None)  # type: ignore[assignment]
                    if bq:
                        bq.insert_signals_batch(bq_signals)
                except Exception:
                    logger.warning("bq_signals_insert_failed — non-critical", exc_info=True)
                if self.pubsub:
                    self.pubsub.publish_trade_signals_batch(bq_signals)
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
