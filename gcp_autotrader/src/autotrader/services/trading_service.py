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
from autotrader.domain.daily_bias import compute_daily_bias
from autotrader.domain.regime_affinity import regime_hard_blocks_strategy, regime_strategy_multiplier
from autotrader.domain.risk import calc_position_size, calc_swing_position_size
from autotrader.domain.scoring import check_strategy_entry, check_swing_entry, determine_direction, score_signal
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
                    wl_type_raw = str(r.get("wlType", r.get("wl_type", "intraday")) or "intraday").strip().lower()
                    wl_type = "swing" if wl_type_raw == "swing" else "intraday"
                    product = "CNC" if wl_type == "swing" else "MIS"
                    rows.append(WatchlistRow(
                        symbol=sym,
                        exchange=str(r.get("exchange", "NSE") or "NSE").upper(),
                        segment="CASH",
                        product=product,
                        strategy=str(r.get("setuplabel", r.get("setupLabel", r.get("setup", r.get("strategy", "AUTO")))) or "AUTO"),
                        sector=str(r.get("macrosector", r.get("sector", "UNKNOWN")) or "UNKNOWN"),
                        beta=float(r.get("beta", 1.0) or 1.0),
                        enabled=True,
                        note=str(r.get("reason", r.get("notes", "")) or ""),
                        wl_type=wl_type,
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

    def _build_portfolio_sector_map(self) -> dict[str, list[str]]:
        """Return {sector: [symbol, ...]} for currently OPEN positions.

        Used to block over-concentration: if 2+ positions are already in the
        same sector we refuse a 3rd.  Sector is read from the universe row;
        falls back to "UNKNOWN" so the check is never silently bypassed.
        """
        sector_map: dict[str, list[str]] = {}
        for pos in self.state.list_open_positions():
            sym = str(pos.get("symbol", "")).strip().upper()
            if not sym:
                continue
            # Lookup sector from universe
            try:
                urow = self.state.get_universe_row(sym)
                sector = str((urow or {}).get("macrosector") or (urow or {}).get("sector") or "UNKNOWN").upper()
            except Exception:
                sector = "UNKNOWN"
            sector_map.setdefault(sector, []).append(sym)
        return sector_map

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

        # Daily candle path for swing candidates.
        # Use a generous calendar-day lookback so we clear the 80-trading-day
        # floor even after weekends/holidays. Then merge API with cache and
        # fall back to cache if the API leg returns short.
        tf = str(timeframe or "").strip().lower()
        if tf in {"1d", "day", "daily"}:
            # 180 calendar days ≈ 125 trading days — safely above need=80 even
            # with a long holiday stretch.
            lookback = max(lookback_days, 180)
            merged: list[list[Any]] = list(cached)
            if instrument_key:
                try:
                    end = now_ist()
                    start = end - timedelta(days=lookback)
                    api = self.upstox.get_historical_candles_v3_days(
                        instrument_key,
                        to_date=end.strftime("%Y-%m-%d"),
                        from_date=start.strftime("%Y-%m-%d"),
                    )
                    if api:
                        merged = self.gcs.merge_candles(path, api)
                except Exception:
                    logger.warning("swing_daily_candle_fetch_failed symbol=%s", symbol, exc_info=True)
            if len(merged) < need:
                logger.warning(
                    "swing_daily_candles_insufficient symbol=%s cached=%d merged=%d ik=%s",
                    symbol, len(cached), len(merged), bool(instrument_key),
                )
            return merged[-max(need, 120):]

        # Upstox-first path for scanner runtime candles (current intraday session).
        # We always attempt this first to keep scanner aligned with the active data provider.
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

    def run_scan_once(
        self,
        allow_live_orders: bool = False,
        force: bool = False,
        wl_type_filter: str = "all",
    ) -> dict[str, Any]:
        # wl_type_filter: "intraday" | "swing" | "all". When set to "intraday" or
        # "swing" the scanner only evaluates watchlist rows of that type. This
        # mirrors the production split — the 3-min cron runs with
        # wl_type=intraday, while the daily 09:20 cron runs with wl_type=swing.
        # "all" preserves legacy behaviour for manual invocations.
        wl_filter = str(wl_type_filter or "all").strip().lower()
        if wl_filter not in {"intraday", "swing", "all"}:
            wl_filter = "all"
        self.log_sink.action("TradingService", "run_scan_once", "START", "", {"wlFilter": wl_filter})
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
            # Regime-adaptive cap: on strong trend days 3 positions is too few —
            # we turn away qualified signals after the 3rd hit. Raise cap to 5
            # only in clearly-favourable regimes so we don't over-expose in chop.
            if brain_state and brain_state.regime in ("TREND_UP", "RECOVERY"):
                max_signals_allowed = max(max_signals_allowed, 5)

            # ── Daily PnL circuit breakers ────────────────────────────────
            # Enforce max_daily_loss and daily_profit_target before scanning.
            # Both settings exist in StrategySettings but were previously never
            # checked.  We evaluate once per scan cycle (not per symbol) to keep
            # the hot path fast.
            _today_pnl = 0.0
            _pnl_block_reason = ""
            try:
                _today_pnl = self.state.get_today_realized_pnl(now_ist_str()[:10])
            except Exception:
                logger.warning("daily_pnl_check_failed — proceeding without limit", exc_info=True)
            # ── Runtime settings overrides (Firestore config/{key}) ───────
            # Allows live tuning of thresholds without redeploy.
            # Supported: min_signal_score, max_positions, risk_per_trade,
            #            max_daily_loss, daily_profit_target,
            #            swing_min_signal_score, swing_max_positions
            cfg = self.settings.strategy
            try:
                _rt_overrides = self.state.get_runtime_settings_overrides()
                if _rt_overrides:
                    from dataclasses import replace as _dc_replace
                    cfg = _dc_replace(cfg, **_rt_overrides)
                    logger.info("runtime_settings_overrides applied keys=%s", list(_rt_overrides))
            except Exception:
                logger.warning("runtime_settings_overrides_failed — using defaults", exc_info=True)

            if _today_pnl <= -abs(cfg.max_daily_loss):
                _pnl_block_reason = "daily_loss_limit_hit"
            elif _today_pnl >= cfg.daily_profit_target:
                _pnl_block_reason = "daily_profit_target_hit"
            if _pnl_block_reason:
                self.log_sink.action(
                    "TradingService", "run_scan_once", "SKIP", _pnl_block_reason,
                    {"todayPnl": _today_pnl, "maxLoss": cfg.max_daily_loss, "profitTarget": cfg.daily_profit_target},
                )
                return {"skipped": _pnl_block_reason, "today_pnl": _today_pnl}

            # Read watchlist: Firestore is primary, Sheets is fallback
            watchlist = self._read_watchlist_with_fallback()
            if wl_filter in {"intraday", "swing"}:
                before = len(watchlist)
                watchlist = [w for w in watchlist if getattr(w, "wl_type", "intraday") == wl_filter]
                logger.info("watchlist_filter wl_type=%s before=%d after=%d", wl_filter, before, len(watchlist))
            subset, scan_meta = self._slice_watchlist_for_scan(watchlist)
            if not subset:
                self.log_sink.action("TradingService", "run_scan_once", "SKIP", "watchlist empty")
                return {"skipped": "watchlist_empty"}

            # Portfolio risk: pre-build sector map of open positions for concentration check
            _portfolio_sectors = self._build_portfolio_sector_map()
            _MAX_SAME_SECTOR = 2   # hard cap: max positions from one sector

            # Count existing swing positions for separate cap
            _open_swing_count = sum(
                1 for p in self.state.list_open_positions()
                if str(p.get("wl_type") or "").strip().lower() == "swing"
            )

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
            bq_decisions: list[dict[str, Any]] = []   # ALL symbols — qualified + rejected
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
                _is_swing = getattr(w, "wl_type", "intraday") == "swing"
                instrument_key = key_by_symbol.get(str(w.symbol).strip().upper(), "")
                candles = self._fetch_candles(
                    w.symbol,
                    w.exchange,
                    w.segment,
                    instrument_key=instrument_key,
                    timeframe="1d" if _is_swing else "15m",
                    lookback_days=120 if _is_swing else 8,
                )
                # ── Daily bias (multi-timeframe confirmation) ──────────────
                # For swing: candles are already daily → compute bias directly.
                # For intraday: fetch daily candles separately for alignment overlay.
                _daily_bias = None
                try:
                    if _is_swing:
                        _daily_bias = compute_daily_bias(candles)
                    elif instrument_key:
                        _daily_candles = self._fetch_candles(
                            w.symbol, w.exchange, w.segment,
                            instrument_key=instrument_key,
                            timeframe="1d", lookback_days=120,
                        )
                        _daily_bias = compute_daily_bias(_daily_candles)
                except Exception:
                    logger.debug("daily_bias_compute_failed symbol=%s", w.symbol)

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
                    _skip_ts = now_ist_str()
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
                    bq_decisions.append({
                        "scan_ts": _skip_ts,
                        "run_date": _skip_ts[:10],
                        "scanner_run_id": scanner_run_id,
                        "symbol": w.symbol,
                        "setup": w.strategy or "",
                        "wl_type": getattr(w, "wl_type", "intraday"),
                        "direction": "SKIP",
                        "raw_score": 0,
                        "adjusted_score": 0,
                        "min_score": 0,
                        "qualified": False,
                        "blocked_reason": "insufficient_candles",
                        "ltp": 0.0,
                        "change_pct": 0.0,
                        "vol_ratio": 0.0,
                        "rsi": 0.0,
                        "macd_view": "NA",
                        "ema_state": "MIXED",
                        "supertrend": "NA",
                        "vwap": 0.0,
                        "atr": 0.0,
                        "atr_mult": 0.0,
                        "score_regime": 0,
                        "score_options": 0,
                        "score_technical": 0,
                        "score_volume": 0,
                        "score_penalty": 0,
                        "regime": brain_state.regime if brain_state else "",
                        "risk_mode": brain_state.risk_mode if brain_state else "",
                    })
                    continue
                direction = determine_direction(ind, regime)
                meta = score_signal(w.symbol, direction, ind, regime, self.settings.strategy, daily_bias=_daily_bias, setup=w.strategy)
                # Apply regime-strategy affinity multiplier
                _affinity_mult = regime_strategy_multiplier(
                    brain_state.regime if brain_state else "RANGE",
                    w.strategy,
                    direction,
                )
                _affinity_score = max(0, min(100, int(round(meta.score * _affinity_mult))))
                adjusted_score = max(0, min(100, int(self.market_brain_service.adjust_signal(_affinity_score, brain_state))))
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

                if _is_swing:
                    pos = calc_swing_position_size(ltp, ind.atr, direction if direction != "HOLD" else "BUY", self.settings.strategy, atr_mult_override=_atr_mult if _atr_mult != self.settings.strategy.atr_sl_mult else None)
                else:
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
                # Per-regime override: RANGE sessions naturally produce lower scores
                # (we already penalise trend strategies there). A flat 72 threshold
                # locks out MEAN_REVERSION that should qualify at 65-70.
                _REGIME_MIN_SCORE = {"RANGE": 65}
                if _is_swing:
                    # Swing: higher bar, not risk-mode-adjusted (swing should only fire on strong signals)
                    dynamic_min_score = self.settings.strategy.swing_min_signal_score
                else:
                    dynamic_min_score = _SCORE_THRESHOLDS.get(
                        brain_state.risk_mode if brain_state else "NORMAL",
                        self.settings.strategy.min_signal_score,
                    )
                    # Regime override (takes precedence when lower than risk-mode
                    # threshold) — e.g. RANGE=65 beats NORMAL=72 so mean-reversion
                    # can still qualify.
                    _regime_min = _REGIME_MIN_SCORE.get(_brain_regime)
                    if _regime_min is not None and _regime_min < dynamic_min_score:
                        dynamic_min_score = _regime_min

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
                    "wlType": getattr(w, "wl_type", "intraday"),
                    "wl_type": getattr(w, "wl_type", "intraday"),  # dual-write; dashboard reads snake_case
                    "vwap": round(ind.vwap, 2) if ind.vwap else 0.0,
                    # sl/target/qty are computed in calc_position_size above — populate
                    # them for every scanned row, not just qualified ones, so the dashboard
                    # signals table can show "would have been" SL/target for blocked rows too.
                    "sl": round(pos.sl_price, 2) if pos.sl_price else 0.0,
                    "target": round(pos.target, 2) if pos.target else 0.0,
                    "qty": int(pos.qty or 0),
                    "status": "scanned",
                    "reason": "",
                    # Item 2: expose threshold used so dashboard can show gap-to-qualify
                    # and we can tune thresholds from paper-trade data later.
                    "minScore": int(dynamic_min_score),
                    "atrMult": round(_atr_mult, 3),
                    "affinityMult": round(_affinity_mult, 2),
                    # Dashboard reads these as snake_case (daily_trend, daily_strength).
                    # Keep both keys so older dashboard builds still see something.
                    "dailyTrend": _daily_bias.trend if _daily_bias else "",
                    "dailyStrength": round(_daily_bias.strength, 1) if _daily_bias else 0.0,
                    "daily_trend": _daily_bias.trend if _daily_bias else "",
                    "daily_strength": round(_daily_bias.strength, 1) if _daily_bias else 0.0,
                }

                policy_block_reason = ""
                if pos.qty == 0:
                    policy_block_reason = "sl_too_wide_for_risk_budget"
                elif direction == "BUY" and market_policy is not None and not bool(market_policy.long_enabled):
                    policy_block_reason = "policy_long_disabled"
                elif direction == "SELL" and market_policy is not None and not bool(market_policy.short_enabled):
                    policy_block_reason = "policy_short_disabled"
                elif market_policy is not None and not self._strategy_allowed(w.strategy, market_policy.allowed_strategies):
                    policy_block_reason = "policy_strategy_blocked"
                elif regime_hard_blocks_strategy(_brain_regime, w.strategy):
                    # Regime playbook: hard-block mismatched strategies (e.g. BREAKOUT in RANGE,
                    # any entry in CHOP). Stronger than the affinity multiplier — prevents
                    # wasting a position slot on a setup that can't work in this regime.
                    policy_block_reason = "regime_strategy_hard_block"
                elif _is_swing and _open_swing_count >= self.settings.strategy.swing_max_positions:
                    policy_block_reason = "swing_max_positions_reached"
                elif not _is_swing and qualified >= max_signals_allowed:
                    policy_block_reason = "policy_max_positions_reached"
                # Staleness gate: if live LTP has moved > 2% away from the candle
                # close (which is what indicators were computed from), the setup is
                # stale — we'd be chasing. Compare against ind.close, NOT pos.entry_price
                # (which is set FROM _live and would always read 0% diff).
                elif _live > 0 and ind.close > 0 and abs(_live - ind.close) / ind.close > 0.02:
                    policy_block_reason = "stale_signal_price_moved"
                # Live VWAP guard: if price drifted to wrong side of VWAP since candle close, reject entry.
                # Only fires when we have a fresh live LTP (_live > 0) — not when falling back to candle close.
                elif direction == "BUY" and _live > 0 and ltp < ind.vwap and w.strategy not in ("MEAN_REVERSION", "VWAP_REVERSAL"):
                    policy_block_reason = "live_price_below_vwap"
                elif direction == "SELL" and _live > 0 and ltp > ind.vwap and w.strategy not in ("MEAN_REVERSION", "VWAP_REVERSAL"):
                    policy_block_reason = "live_price_above_vwap"
                else:
                    # Strategy-specific hard gates: validate that the current market
                    # structure actually matches the setup assigned at watchlist build time.
                    _brain_regime = brain_state.regime if brain_state else ""
                    if _is_swing:
                        _strategy_ok, _strategy_fail = check_swing_entry(w.strategy, direction, ind, _daily_bias, regime=_brain_regime)
                    else:
                        _strategy_ok, _strategy_fail = check_strategy_entry(w.strategy, direction, ind, regime=_brain_regime)
                    if not _strategy_ok:
                        policy_block_reason = _strategy_fail
                    # Portfolio sector concentration: don't pile into the same sector
                    elif w.sector and w.sector.upper() != "UNKNOWN":
                        _sym_sector = w.sector.upper()
                        if len(_portfolio_sectors.get(_sym_sector, [])) >= _MAX_SAME_SECTOR:
                            policy_block_reason = "portfolio_sector_concentrated"

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
                    # Update in-memory counters so later symbols in this cycle see the new position
                    if _is_swing:
                        _open_swing_count += 1
                    if w.sector and w.sector.upper() != "UNKNOWN":
                        _portfolio_sectors.setdefault(w.sector.upper(), []).append(w.symbol)
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
                        strategy=str(w.strategy or ""),
                        regime=brain_state.regime,
                        risk_mode=brain_state.risk_mode,
                        allow_live_orders=allow_live_orders,
                        wl_type=getattr(w, "wl_type", "intraday"),
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

                # ── Decision log (every symbol, qualified + rejected) ────────
                _dec_ts = now_ist_str()
                _is_qualified = _scan_row_rich.get("status") == "qualified"
                bq_decisions.append({
                    "scan_ts": _dec_ts,
                    "run_date": _dec_ts[:10],
                    "scanner_run_id": scanner_run_id,
                    "symbol": w.symbol,
                    "setup": w.strategy or "",
                    "wl_type": getattr(w, "wl_type", "intraday"),
                    "direction": direction,
                    "raw_score": int(meta.score),
                    "adjusted_score": int(adjusted_score),
                    "min_score": int(dynamic_min_score),
                    "qualified": _is_qualified,
                    "blocked_reason": "" if _is_qualified else _scan_row_rich.get("reason", ""),
                    "ltp": round(ltp, 2),
                    "change_pct": round(change_pct, 2),
                    "vol_ratio": round(ind.volume.ratio, 2),
                    "rsi": round(ind.rsi.curr, 1),
                    "macd_view": macd_view,
                    "ema_state": ema_state,
                    "supertrend": "UP" if ind.supertrend.dir == 1 else "DOWN",
                    "vwap": round(ind.vwap, 2) if ind.vwap else 0.0,
                    "atr": round(ind.atr, 4),
                    "adx": round(ind.adx, 1),
                    "atr_mult": round(_atr_mult, 3),
                    "score_regime": int(meta.breakdown.regime),
                    "score_options": int(meta.breakdown.options),
                    "score_technical": int(meta.breakdown.technical),
                    "score_volume": int(meta.breakdown.volume),
                    "score_alignment": int(meta.breakdown.alignment),
                    "score_penalty": int(meta.breakdown.penalty),
                    "affinity_mult": round(_affinity_mult, 2),
                    "daily_trend": _daily_bias.trend if _daily_bias else "",
                    "daily_strength": round(_daily_bias.strength, 1) if _daily_bias else 0.0,
                    "regime": brain_state.regime if brain_state else regime.regime,
                    "risk_mode": brain_state.risk_mode if brain_state else "",
                })

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
            try:
                from autotrader.adapters.bigquery_client import BigQueryClient as _BQC
                bq: _BQC | None = getattr(self.order_service, "bq", None)  # type: ignore[assignment]
                if bq:
                    if bq_signals:
                        bq.insert_signals_batch(bq_signals)
                    if bq_decisions:
                        bq.insert_scan_decisions_batch(bq_decisions)
            except Exception:
                logger.warning("bq_signals_or_decisions_insert_failed — non-critical", exc_info=True)
            if bq_signals and self.pubsub:
                self.pubsub.publish_trade_signals_batch(bq_signals)

            # ── Firestore daily decision summary (running totals per day) ──
            # Splits intraday vs swing stats for dashboard visibility.
            try:
                _today = run_ts[:10]
                _existing = self.state.get_json("decisions", _today) or {}
                _rejection_counts: dict[str, int] = dict(_existing.get("rejection_breakdown", {}))
                _top_scores: list[dict[str, Any]] = list(_existing.get("top_scores", []))

                # Per-type counters (intraday vs swing)
                _intraday_scanned = 0
                _intraday_qualified = 0
                _swing_scanned = 0
                _swing_qualified = 0

                for _d in bq_decisions:
                    _d_type = _d.get("wl_type", "intraday")
                    if _d_type == "swing":
                        _swing_scanned += 1
                        if _d["qualified"]:
                            _swing_qualified += 1
                    else:
                        _intraday_scanned += 1
                        if _d["qualified"]:
                            _intraday_qualified += 1

                    if not _d["qualified"]:
                        _reason = _d["blocked_reason"] or "unknown"
                        _rejection_counts[_reason] = _rejection_counts.get(_reason, 0) + 1
                    # Track top-10 highest scores that didn't qualify (tuning targets)
                    if not _d["qualified"] and _d["direction"] != "HOLD":
                        _top_scores.append({
                            "symbol": _d["symbol"],
                            "score": _d["adjusted_score"],
                            "raw": _d["raw_score"],
                            "direction": _d["direction"],
                            "reason": _d["blocked_reason"],
                            "rsi": _d["rsi"],
                            "vol_ratio": _d["vol_ratio"],
                            "setup": _d["setup"],
                            "wl_type": _d.get("wl_type", "intraday"),
                            "daily_trend": _d.get("daily_trend", ""),
                            "affinity_mult": _d.get("affinity_mult", 1.0),
                            "scan_ts": _d["scan_ts"],
                        })
                # Keep only top-10 by score across all cycles today
                _top_scores = sorted(_top_scores, key=lambda x: x["score"], reverse=True)[:10]
                _scans_today = list(_existing.get("scans", []))
                _scans_today.append({
                    "scan_ts": run_ts,
                    "scanner_run_id": scanner_run_id,
                    "scanned": len(bq_decisions),
                    "qualified": qualified,
                    "intraday_scanned": _intraday_scanned,
                    "intraday_qualified": _intraday_qualified,
                    "swing_scanned": _swing_scanned,
                    "swing_qualified": _swing_qualified,
                    "regime": brain_state.regime if brain_state else "",
                    "risk_mode": brain_state.risk_mode if brain_state else "",
                })
                _all_scores = [_d["adjusted_score"] for _d in bq_decisions if _d["direction"] != "HOLD"]
                _avg_score = round(sum(_all_scores) / len(_all_scores), 1) if _all_scores else 0.0
                self.state.set_json(
                    "decisions",
                    _today,
                    {
                        "date": _today,
                        "last_updated": run_ts,
                        "total_scanned": _existing.get("total_scanned", 0) + len(bq_decisions),
                        "total_qualified": _existing.get("total_qualified", 0) + qualified,
                        "intraday_scanned": _existing.get("intraday_scanned", 0) + _intraday_scanned,
                        "intraday_qualified": _existing.get("intraday_qualified", 0) + _intraday_qualified,
                        "swing_scanned": _existing.get("swing_scanned", 0) + _swing_scanned,
                        "swing_qualified": _existing.get("swing_qualified", 0) + _swing_qualified,
                        "avg_score": _avg_score,
                        "rejection_breakdown": _rejection_counts,
                        "top_scores": _top_scores,
                        "scans": _scans_today[-50:],  # cap at last 50 scan summaries
                    },
                    merge=False,
                )
            except Exception:
                logger.warning("decisions_daily_summary_save_failed — non-critical", exc_info=True)
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
