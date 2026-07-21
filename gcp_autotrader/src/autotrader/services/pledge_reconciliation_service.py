"""PLEDGE channel reconciliation — daily premarket MAX-HOLD exit + SL-breach backstop.

Channel-isolated mirror of insider/delivery reconciliation for ``wl_type="pledge"``, DELIBERATELY
SIMPLE: the validated pledge edge is a FIXED 60-session hold (a trailing stop whipsaws this slow
drift — proven in the grind), so there is NO trail ratchet. This job only:
  1. exits if trading-days-held >= ``pledge_max_hold_days`` (60)
  2. conservative daily-close SL-breach backstop (fixed 2.0xATR protective stop set at entry; can
     only exit later/worse than the backtest, never earlier — does not inflate results)

The intraday resting stop (paper-GTT / ws_monitor) is written at entry by order_service and is NOT
ratcheted here (fixed hold). Selects ONLY ``wl_type="pledge"`` positions, which swing/insider/
delivery/pead reconciliation exclude — so no other channel's exit path is touched. The decision
core (``evaluate_pledge_position``) is pure + unit-tested; the wrapper does Firestore/Upstox/order
I/O (validated in PAPER). Called from ``/jobs/pledge-reconcile`` premarket (~09:00 IST).

Validated config (2026-07-21): hold 60 sessions, ATR14x2.0 protective stop, NO trail.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from autotrader.domain.indicators import normalize_candles
from autotrader.time_utils import now_ist_str

logger = logging.getLogger(__name__)


def _date_str(ts: object) -> str | None:
    s = str(ts or "")
    return s[:10] if len(s) >= 10 else None


def _trading_days_held(candles: list, entry_ts: str) -> int | None:
    ed = _date_str(entry_ts)
    if not ed:
        return None
    return sum(1 for c in candles if (_date_str(c[0]) or "") >= ed)


def evaluate_pledge_position(
    *,
    entry_price: float,
    side: str,
    sl_price: float,
    entry_ts: str,
    candles: list,
    max_hold_days: int,
) -> dict[str, Any]:
    """PURE: decide the exit action for one PLEDGE position from fresh daily bars.

    Returns ``{exit_reason, days_held}``. ``exit_reason`` is None while the position stays open
    (fixed hold, no trail — the SL is unchanged from entry). Fixed 60-session hold + daily-close
    SL-breach backstop only.
    """
    is_buy = str(side or "BUY").upper() == "BUY"
    last_close = candles[-1][4]
    days_held = _trading_days_held(candles, entry_ts) or 0

    if days_held >= max_hold_days:
        return {"exit_reason": f"MAX_HOLD_{days_held}D", "days_held": days_held}
    if sl_price > 0 and ((is_buy and last_close < sl_price) or (not is_buy and last_close > sl_price)):
        return {"exit_reason": "SL_BREACH_DAILY", "days_held": days_held}
    return {"exit_reason": None, "days_held": days_held}


@dataclass
class PledgeReconcileResult:
    checked: int = 0
    closed_sl_breach: int = 0
    closed_max_hold: int = 0
    errors: int = 0
    skipped_no_candles: int = 0
    details: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {"checked": self.checked, "closed_sl_breach": self.closed_sl_breach,
                "closed_max_hold": self.closed_max_hold, "errors": self.errors,
                "skipped_no_candles": self.skipped_no_candles,
                "closed": self.closed_sl_breach + self.closed_max_hold, "details": self.details}


class PledgeReconciliationService:
    """Daily premarket max-hold/SL-breach exit manager for the PLEDGE channel (PAPER-safe)."""

    def __init__(self, *, settings, state, upstox, order_service):
        self.settings = settings
        self.state = state
        self.upstox = upstox
        self.order_service = order_service

    def _fetch_daily_candles(self, instrument_key: str, asof: str) -> list:
        from datetime import date, timedelta
        frm = (date.fromisoformat(asof) - timedelta(days=200)).isoformat()
        rows = self.upstox.get_historical_candles_v3_days(instrument_key, to_date=asof, from_date=frm, interval_days=1)
        return list(reversed(rows)) if rows and str(rows[0][0]) > str(rows[-1][0]) else rows

    def run(self, asof: str | None = None) -> PledgeReconcileResult:
        from autotrader.time_utils import now_ist
        result = PledgeReconcileResult()
        asof = asof or now_ist().strftime("%Y-%m-%d")
        cfg = self.settings.strategy
        open_positions = [p for p in self.state.list_open_positions()
                          if str(p.get("wl_type") or "").strip().lower() == "pledge"]
        if not open_positions:
            logger.info("pledge_recon no open pledge positions")
            return result

        for pos in open_positions:
            result.checked += 1
            tag = str(pos.get("position_tag") or pos.get("_id") or "")
            symbol = str(pos.get("symbol") or "")
            instrument_key = str(pos.get("instrument_key") or symbol)
            try:
                candles = normalize_candles(self._fetch_daily_candles(instrument_key, asof))
                if len(candles) < 2:
                    result.skipped_no_candles += 1
                    continue
                ev = evaluate_pledge_position(
                    entry_price=float(pos.get("entry_price") or 0),
                    side=str(pos.get("side") or "BUY"),
                    sl_price=float(pos.get("sl_price") or 0),
                    entry_ts=str(pos.get("entry_ts") or ""),
                    candles=candles,
                    max_hold_days=cfg.pledge_max_hold_days,
                )
                if ev["exit_reason"]:
                    self._close(pos, tag, instrument_key, ev["exit_reason"], candles[-1][4], result)
                    time.sleep(0.05)
            except Exception:
                logger.exception("pledge_recon_error tag=%s symbol=%s", tag, symbol)
                result.errors += 1
        logger.info("pledge_recon_summary %s", result.to_dict())
        return result

    def _close(self, pos, tag, instrument_key, exit_reason, last_close, result):
        try:
            res = self.order_service.place_exit_order(
                position_tag=tag, instrument_key=instrument_key, exit_reason=exit_reason, is_amo=True)
            amo = bool(res.get("amo"))
        except Exception:
            logger.exception("pledge_recon_exit_order_failed tag=%s", tag)
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
        result.details.append({"tag": tag, "symbol": pos.get("symbol"), "action": "closed", "reason": exit_reason})
        logger.info("pledge_recon_exit tag=%s reason=%s amo=%s", tag, exit_reason, amo)
