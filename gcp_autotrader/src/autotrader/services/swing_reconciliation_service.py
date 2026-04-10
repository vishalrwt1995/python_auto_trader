"""Swing position reconciliation — runs premarket each day.

Re-evaluates open swing/CNC positions against fresh daily candles.
Actions taken:
  1. Exit if daily SuperTrend flipped against position (trend broken)
  2. Exit if close price breached the SL (daily candle closed below SL)
  3. Update trailing SL if price made a new best (ratchet overnight)
  4. Exit if target hit (close >= target for BUY, close <= target for SELL)
  5. Exit if max hold days exceeded

Called from the /jobs/swing-reconcile endpoint (premarket, ~09:00 IST).
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

from autotrader.adapters.firestore_state import FirestoreStateStore
from autotrader.adapters.gcs_store import GoogleCloudStorageStore
from autotrader.adapters.upstox_client import UpstoxClient
from autotrader.domain.indicators import calc_atr, calc_supertrend, normalize_candles
from autotrader.services.order_service import OrderService
from autotrader.settings import AppSettings
from autotrader.time_utils import now_ist, now_ist_str, today_ist

logger = logging.getLogger(__name__)

_MAX_HOLD_DAYS_DEFAULT = 10
_TRAIL_ATR_MULT = 2.5     # same as ws_monitor for swing
_BREAKEVEN_ATR_MULT = 1.5


@dataclass
class SwingReconcileResult:
    checked: int = 0
    updated_sl: int = 0
    closed_sl_breach: int = 0
    closed_target: int = 0
    closed_trend_break: int = 0
    closed_max_hold: int = 0
    errors: int = 0
    skipped_no_candles: int = 0
    details: list[dict[str, Any]] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.details is None:
            self.details = []

    def to_dict(self) -> dict[str, Any]:
        return {
            "checked": self.checked,
            "updated_sl": self.updated_sl,
            "closed_sl_breach": self.closed_sl_breach,
            "closed_target": self.closed_target,
            "closed_trend_break": self.closed_trend_break,
            "closed_max_hold": self.closed_max_hold,
            "errors": self.errors,
            "skipped_no_candles": self.skipped_no_candles,
            "details": self.details[:20],  # cap for Firestore
        }


class SwingReconciliationService:
    def __init__(
        self,
        settings: AppSettings,
        state: FirestoreStateStore,
        gcs: GoogleCloudStorageStore,
        upstox: UpstoxClient,
        order_service: OrderService,
    ) -> None:
        self.settings = settings
        self.state = state
        self.gcs = gcs
        self.upstox = upstox
        self.order_service = order_service

    def _fetch_daily_candles(self, symbol: str, exchange: str, instrument_key: str) -> list:
        """Fetch last 120 daily candles for swing re-evaluation."""
        path = self.gcs.candle_cache_path(symbol, exchange, "CASH", "1d")
        cached = self.gcs.read_candles(path)
        if instrument_key:
            try:
                from datetime import timedelta
                end = now_ist()
                api = self.upstox.get_historical_candles_v3_days(
                    instrument_key,
                    to_date=end.strftime("%Y-%m-%d"),
                    from_date=(end - timedelta(days=120)).strftime("%Y-%m-%d"),
                )
                if api:
                    cached = self.gcs.merge_candles(path, api)
            except Exception:
                logger.warning("swing_recon_candle_fetch_failed symbol=%s", symbol, exc_info=True)
        return cached[-120:] if len(cached) >= 120 else cached

    def _days_held(self, entry_ts: str) -> int:
        """Return number of calendar days since entry."""
        try:
            from autotrader.time_utils import parse_any_ts
            entry_dt = parse_any_ts(entry_ts)
            if entry_dt is None:
                return 0
            return max(0, (now_ist() - entry_dt).days)
        except Exception:
            return 0

    def run(self, key_by_symbol: dict[str, str] | None = None) -> SwingReconcileResult:
        """Re-evaluate all open swing positions. Called premarket."""
        result = SwingReconcileResult()
        open_positions = [
            p for p in self.state.list_open_positions()
            if str(p.get("wl_type") or "").strip().lower() == "swing"
        ]
        if not open_positions:
            logger.info("swing_recon no open swing positions")
            return result

        max_hold_days = self.settings.strategy.swing_max_hold_days
        cfg = self.settings.strategy

        for pos in open_positions:
            result.checked += 1
            tag = str(pos.get("position_tag") or pos.get("_id") or "")
            symbol = str(pos.get("symbol") or "")
            exchange = str(pos.get("exchange") or "NSE")
            side = str(pos.get("side") or "BUY").upper()
            entry_price = float(pos.get("entry_price") or 0)
            sl_price = float(pos.get("sl_price") or 0)
            target = float(pos.get("target") or 0)
            atr = float(pos.get("atr") or 0)
            entry_ts = str(pos.get("entry_ts") or "")
            instrument_key = str(pos.get("instrument_key") or symbol)

            # Lookup instrument key from map if provided
            if key_by_symbol and symbol.upper() in key_by_symbol:
                instrument_key = key_by_symbol[symbol.upper()]

            try:
                candles = self._fetch_daily_candles(symbol, exchange, instrument_key)
                candles = normalize_candles(candles)
                if len(candles) < 20:
                    logger.warning("swing_recon_insufficient_candles symbol=%s bars=%d", symbol, len(candles))
                    result.skipped_no_candles += 1
                    continue

                last_close = candles[-1][4]
                last_high = candles[-1][2]
                last_low = candles[-1][3]

                # Update daily ATR for SL calibration
                new_atr = calc_atr(candles, 14)
                if new_atr > 0:
                    atr = new_atr

                # Recalculate SuperTrend on daily
                _, st_dirs = calc_supertrend(candles, 10, 3.0)
                st_dir = st_dirs[-1]

                exit_reason: str | None = None

                # ── 1. Max hold days ────────────────────────────────────────
                days_held = self._days_held(entry_ts)
                if days_held >= max_hold_days:
                    exit_reason = f"MAX_HOLD_{days_held}D"

                # ── 2. Daily SL breach (close below/above SL) ───────────────
                if not exit_reason and sl_price > 0:
                    if side == "BUY" and last_close < sl_price:
                        exit_reason = "SL_BREACH_DAILY"
                    elif side == "SELL" and last_close > sl_price:
                        exit_reason = "SL_BREACH_DAILY"

                # ── 3. Target hit (daily close) ─────────────────────────────
                if not exit_reason and target > 0:
                    if side == "BUY" and last_close >= target:
                        exit_reason = "TARGET_HIT_DAILY"
                    elif side == "SELL" and last_close <= target:
                        exit_reason = "TARGET_HIT_DAILY"

                # ── 4. SuperTrend flip (trend broken) ───────────────────────
                if not exit_reason:
                    if side == "BUY" and st_dir == -1:
                        exit_reason = "DAILY_SUPERTREND_FLIP"
                    elif side == "SELL" and st_dir == 1:
                        exit_reason = "DAILY_SUPERTREND_FLIP"

                if exit_reason:
                    # Place an AMO (After Market Order) to exit at market open.
                    # order_service.place_exit_order handles: GTT cancellation, AMO placement,
                    # and sets position status to PENDING_AMO_EXIT until the AMO fills.
                    try:
                        exit_result = self.order_service.place_exit_order(
                            position_tag=tag,
                            instrument_key=instrument_key,
                            exit_reason=exit_reason,
                            is_amo=True,
                        )
                        amo_queued = exit_result.get("amo", False)
                    except Exception:
                        logger.exception("swing_recon_exit_order_failed tag=%s", tag)
                        amo_queued = False

                    if not amo_queued:
                        # Fallback: mark closed directly with daily close as proxy price
                        exit_price = last_close
                        multiplier = 1 if side == "BUY" else -1
                        pnl = round((exit_price - entry_price) * int(pos.get("qty") or 1) * multiplier, 2)
                        self.state.update_position(tag, {
                            "status": "CLOSED",
                            "exit_price": round(exit_price, 2),
                            "exit_reason": exit_reason,
                            "exit_ts": now_ist_str(),
                            "pnl": pnl,
                        })

                    if exit_reason == "SL_BREACH_DAILY":
                        result.closed_sl_breach += 1
                    elif exit_reason.startswith("TARGET_HIT"):
                        result.closed_target += 1
                    elif exit_reason == "DAILY_SUPERTREND_FLIP":
                        result.closed_trend_break += 1
                    else:
                        result.closed_max_hold += 1
                    result.details.append({
                        "tag": tag, "symbol": symbol, "action": "closed",
                        "reason": exit_reason, "amo_queued": amo_queued,
                    })
                    logger.info(
                        "swing_recon_exit tag=%s symbol=%s reason=%s amo=%s",
                        tag, symbol, exit_reason, amo_queued,
                    )
                    time.sleep(0.05)
                    continue

                # ── 5. Update trailing SL (ratchet up/down) ─────────────────
                best_price = max(last_high, entry_price) if side == "BUY" else min(last_low, entry_price)
                sl_moved = bool(pos.get("sl_moved"))

                # Breakeven move
                if not sl_moved and atr > 0:
                    if side == "BUY" and best_price >= entry_price + atr * _BREAKEVEN_ATR_MULT:
                        new_sl = entry_price + atr * 0.15
                        if new_sl > sl_price:
                            sl_price = round(new_sl, 2)
                            sl_moved = True
                    elif side == "SELL" and best_price <= entry_price - atr * _BREAKEVEN_ATR_MULT:
                        new_sl = entry_price - atr * 0.15
                        if new_sl < sl_price:
                            sl_price = round(new_sl, 2)
                            sl_moved = True

                # Trailing stop ratchet
                if sl_moved and atr > 0:
                    trail_dist = atr * _TRAIL_ATR_MULT
                    if side == "BUY":
                        new_sl = round(best_price - trail_dist, 2)
                        if new_sl > sl_price:
                            sl_price = new_sl
                    elif side == "SELL":
                        new_sl = round(best_price + trail_dist, 2)
                        if new_sl < sl_price:
                            sl_price = new_sl

                # Persist updated SL if changed
                current_sl = float(pos.get("sl_price") or 0)
                if abs(sl_price - current_sl) > 0.01 or sl_moved != bool(pos.get("sl_moved")):
                    self.state.update_position(tag, {
                        "sl_price": round(sl_price, 2),
                        "sl_moved": sl_moved,
                        "atr": round(atr, 4),
                        "recon_ts": now_ist_str(),
                    })
                    # Cancel old GTT SL and place a new one at the updated price
                    try:
                        self.order_service.refresh_swing_gtt_sl(
                            position_tag=tag,
                            instrument_key=instrument_key,
                            new_sl_price=round(sl_price, 2),
                        )
                    except Exception:
                        logger.warning("swing_recon_gtt_refresh_failed tag=%s", tag, exc_info=True)
                    result.updated_sl += 1
                    result.details.append({"tag": tag, "symbol": symbol, "action": "sl_updated", "new_sl": round(sl_price, 2)})
                    logger.info("swing_recon_sl_updated tag=%s symbol=%s new_sl=%.2f", tag, symbol, sl_price)

            except Exception:
                result.errors += 1
                logger.exception("swing_recon_error tag=%s symbol=%s", tag, symbol)
            time.sleep(0.05)

        logger.info(
            "swing_recon_done checked=%d updated_sl=%d closed=%d errors=%d",
            result.checked,
            result.updated_sl,
            result.closed_sl_breach + result.closed_target + result.closed_trend_break + result.closed_max_hold,
            result.errors,
        )
        return result
