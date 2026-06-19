"""PEAD / EVENT channel reconciliation — daily premarket trail + max-hold exit.

Channel-isolated mirror of swing_reconciliation_service for ``wl_type="pead"``:
  1. exit if trading-days-held >= ``pead_max_hold_days`` (40)
  2. ratchet the daily 1R trailing stop (armed at +1.75R) via the SHARED
     ``swing_exit.trailed_stop`` — the single source of truth for the trail math,
     so PEAD's trail can't drift from the validated geometry
  3. conservative daily-close SL-breach backstop (can only exit later/worse than the
     backtest, never earlier — does not inflate results)

The intraday resting stop is enforced by the same paper-GTT / ws_monitor mechanism as
swing (written at entry by order_service); this premarket job ratchets that stop up and
forces the max-hold exit. Reuses ``order_service.place_exit_order`` +
``refresh_swing_gtt_sl`` (both channel-agnostic, keyed by position_tag, paper-aware).

It selects ONLY ``wl_type="pead"`` positions, which swing_reconciliation excludes
(it matches ``wl_type=="swing"`` exactly) — so the swing/intraday exit paths are
untouched. The decision core (``evaluate_pead_position``) is pure + unit-tested; the
service wrapper does the Firestore/Upstox/order I/O (validated in PAPER). Called from
``/jobs/pead-reconcile`` premarket (~09:00 IST), before the entry scan.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from autotrader.domain.indicators import calc_atr, normalize_candles
from autotrader.domain.swing_exit import trailed_stop
from autotrader.time_utils import now_ist_str

logger = logging.getLogger(__name__)


def _date_str(ts: object) -> str | None:
    """Best-effort YYYY-MM-DD from a bar timestamp or an entry_ts string."""
    s = str(ts or "")
    return s[:10] if len(s) >= 10 else None


def _trading_days_held(candles: list, entry_ts: str) -> int | None:
    """Count of daily bars on/after the entry date — holiday/weekend-proof, matches the
    backtest's bar-offset max_hold. None if the entry date is unparseable."""
    ed = _date_str(entry_ts)
    if not ed:
        return None
    return sum(1 for c in candles if (_date_str(c[0]) or "") >= ed)


def evaluate_pead_position(
    *,
    entry_price: float,
    side: str,
    sl_price: float,
    sl_dist: float,
    entry_ts: str,
    candles: list,
    max_hold_days: int,
    trail_r: float,
    activate_r: float,
    sl_moved: bool = False,
) -> dict[str, Any]:
    """PURE: decide the exit/trail action for one PEAD position from fresh daily bars.

    Returns ``{exit_reason, new_sl, peak, sl_moved, days_held}``. ``exit_reason`` is
    None when the position stays open (then ``new_sl`` is the ratcheted trailing stop).
    Mirrors swing_reconciliation's logic exactly but parameterised for PEAD
    (40-day hold). Single source of truth for the trail: ``swing_exit.trailed_stop``.
    """
    is_buy = str(side or "BUY").upper() == "BUY"
    last_close = candles[-1][4]
    days_held = _trading_days_held(candles, entry_ts) or 0

    # 1. max hold
    if days_held >= max_hold_days:
        return {"exit_reason": f"MAX_HOLD_{days_held}D", "new_sl": sl_price,
                "peak": entry_price, "sl_moved": sl_moved, "days_held": days_held}

    # 2. conservative daily-close SL breach backstop
    if sl_price > 0 and ((is_buy and last_close < sl_price) or (not is_buy and last_close > sl_price)):
        return {"exit_reason": "SL_BREACH_DAILY", "new_sl": sl_price,
                "peak": entry_price, "sl_moved": sl_moved, "days_held": days_held}

    # 3. ratchet the daily 1R trailing stop (running peak since entry, floored at entry)
    entry_date = _date_str(entry_ts)
    bars_since = [c for c in candles if entry_date and (_date_str(c[0]) or "") >= entry_date] or candles[-1:]
    if is_buy:
        peak = max([c[2] for c in bars_since] + [entry_price])
    else:
        peak = min([c[3] for c in bars_since] + [entry_price])

    new_sl = sl_price
    if sl_dist > 0:
        base_sl = entry_price - sl_dist if is_buy else entry_price + sl_dist
        stop, armed = trailed_stop(entry_price, is_buy, sl_dist, peak, base_sl,
                                   trail_R=trail_r, activate_R=activate_r)
        new_sl = round(max(sl_price, stop), 2) if is_buy else round(min(sl_price, stop) if sl_price > 0 else stop, 2)
        sl_moved = sl_moved or armed
    return {"exit_reason": None, "new_sl": new_sl, "peak": round(peak, 2),
            "sl_moved": sl_moved, "days_held": days_held}


@dataclass
class PeadReconcileResult:
    checked: int = 0
    updated_sl: int = 0
    closed_sl_breach: int = 0
    closed_max_hold: int = 0
    errors: int = 0
    skipped_no_candles: int = 0
    details: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {"checked": self.checked, "updated_sl": self.updated_sl,
                "closed_sl_breach": self.closed_sl_breach, "closed_max_hold": self.closed_max_hold,
                "errors": self.errors, "skipped_no_candles": self.skipped_no_candles,
                "closed": self.closed_sl_breach + self.closed_max_hold, "details": self.details}


class PeadReconciliationService:
    """Daily premarket trail/exit manager for the PEAD channel (PAPER-safe)."""

    def __init__(self, *, settings, state, upstox, order_service):
        self.settings = settings
        self.state = state
        self.upstox = upstox
        self.order_service = order_service

    def _fetch_daily_candles(self, instrument_key: str, asof: str) -> list:
        from datetime import date, timedelta
        frm = (date.fromisoformat(asof) - timedelta(days=130)).isoformat()
        rows = self.upstox.get_historical_candles_v3_days(instrument_key, to_date=asof, from_date=frm, interval_days=1)
        return list(reversed(rows)) if rows and str(rows[0][0]) > str(rows[-1][0]) else rows

    def run(self, asof: str | None = None) -> PeadReconcileResult:
        from autotrader.time_utils import now_ist
        result = PeadReconcileResult()
        asof = asof or now_ist().strftime("%Y-%m-%d")
        cfg = self.settings.strategy
        open_positions = [p for p in self.state.list_open_positions()
                          if str(p.get("wl_type") or "").strip().lower() == "pead"]
        if not open_positions:
            logger.info("pead_recon no open pead positions")
            return result

        for pos in open_positions:
            result.checked += 1
            tag = str(pos.get("position_tag") or pos.get("_id") or "")
            symbol = str(pos.get("symbol") or "")
            instrument_key = str(pos.get("instrument_key") or symbol)
            try:
                candles = normalize_candles(self._fetch_daily_candles(instrument_key, asof))
                if len(candles) < 20:
                    result.skipped_no_candles += 1
                    continue
                ev = evaluate_pead_position(
                    entry_price=float(pos.get("entry_price") or 0),
                    side=str(pos.get("side") or "BUY"),
                    sl_price=float(pos.get("sl_price") or 0),
                    sl_dist=float(pos.get("sl_dist") or 0.0),
                    entry_ts=str(pos.get("entry_ts") or ""),
                    candles=candles,
                    max_hold_days=cfg.pead_max_hold_days,
                    trail_r=cfg.pead_trail_r,
                    activate_r=cfg.pead_activate_r,
                    sl_moved=bool(pos.get("sl_moved")),
                )

                if ev["exit_reason"]:
                    self._close(pos, tag, instrument_key, ev["exit_reason"], candles[-1][4], result)
                    time.sleep(0.05)
                    continue

                # ratchet SL if it moved up
                cur_sl = float(pos.get("sl_price") or 0)
                if abs(ev["new_sl"] - cur_sl) > 0.01 or ev["sl_moved"] != bool(pos.get("sl_moved")):
                    new_atr = calc_atr(candles, 14)
                    self.state.update_position(tag, {
                        "sl_price": ev["new_sl"], "sl_moved": ev["sl_moved"],
                        "best_price": ev["peak"], "atr": round(new_atr, 4) if new_atr > 0 else pos.get("atr"),
                        "recon_ts": now_ist_str(),
                    })
                    try:
                        self.order_service.refresh_swing_gtt_sl(
                            position_tag=tag, instrument_key=instrument_key, new_sl_price=ev["new_sl"])
                    except Exception:
                        logger.warning("pead_recon_gtt_refresh_failed tag=%s", tag, exc_info=True)
                    result.updated_sl += 1
                    result.details.append({"tag": tag, "symbol": symbol, "action": "trail",
                                           "sl": ev["new_sl"], "days_held": ev["days_held"]})
            except Exception:
                logger.exception("pead_recon_error tag=%s symbol=%s", tag, symbol)
                result.errors += 1
        logger.info("pead_recon_summary %s", result.to_dict())
        return result

    def _close(self, pos, tag, instrument_key, exit_reason, last_close, result):
        try:
            res = self.order_service.place_exit_order(
                position_tag=tag, instrument_key=instrument_key, exit_reason=exit_reason, is_amo=True)
            amo = bool(res.get("amo"))
        except Exception:
            logger.exception("pead_recon_exit_order_failed tag=%s", tag)
            amo = False
        if not amo:
            qty = int(pos.get("qty") or 1)
            side_mult = 1 if str(pos.get("side") or "BUY").upper() == "BUY" else -1
            pnl = round((last_close - float(pos.get("entry_price") or 0)) * qty * side_mult, 2)
            self.state.update_position(tag, {"status": "CLOSED", "exit_price": round(last_close, 2),
                                             "exit_reason": exit_reason, "exit_ts": now_ist_str(), "pnl": pnl})
        if exit_reason == "SL_BREACH_DAILY":
            result.closed_sl_breach += 1
        else:
            result.closed_max_hold += 1
        result.details.append({"tag": tag, "symbol": pos.get("symbol"), "action": "closed",
                               "reason": exit_reason, "amo_queued": amo})
        logger.info("pead_recon_exit tag=%s reason=%s amo=%s", tag, exit_reason, amo)
