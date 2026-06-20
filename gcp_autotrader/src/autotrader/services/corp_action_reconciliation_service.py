"""Corp-action reconciliation — daily premarket HARD MEETING-DAY EXIT for the corp-action
sub-strategy of the EVENT/PEAD channel.

Selects ONLY ``wl_type="corp_action"`` positions — invisible to pead_reconciliation
(``wl_type=="pead"``) and swing_reconciliation (``wl_type=="swing"``), so PEAD/swing/
intraday exit paths are byte-untouched (Rule 8 safe). The exit is NOT a trail (the
pre-meeting run-up peaks at the meeting and reverses after): it exits at the meeting-day
close. Running premarket, the first session whose last completed bar >= the stored
meeting date IS the meeting session, so booking at that last close == the backtest's
meeting-close exit. A wide protective-SL daily-close breach is the only other exit
(disaster backstop; the backtest holds to the meeting with no stop).

``evaluate_corp_position`` is pure + unit-tested; the service wrapper does the
Firestore/Upstox/order I/O (validated in PAPER). Reuses order_service.place_exit_order
(channel-agnostic). Called from ``/jobs/corp-reconcile`` premarket, before the corp scan.
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


def evaluate_corp_position(
    *,
    entry_price: float,
    side: str,
    sl_price: float,
    meeting_date: str,
    candles: list,
) -> dict[str, Any]:
    """PURE: decide the exit action for one corp-action position from fresh daily bars.

    Returns ``{exit_reason, last_close}``. ``exit_reason`` is None to hold. Exits:
      1. MEETING_EXIT — the last completed session >= the meeting date (book at that close;
         premarket this is the meeting-day close, matching the backtest).
      2. PROTECTIVE_SL — wide daily-close stop breached (disaster backstop only).
    No trailing, no SL ratchet (the strategy holds to the meeting).
    """
    is_buy = str(side or "BUY").upper() == "BUY"
    last_close = candles[-1][4]
    last_date = _date_str(candles[-1][0]) or ""
    md = _date_str(meeting_date) or ""

    if md and last_date >= md:
        return {"exit_reason": "MEETING_EXIT", "last_close": last_close}
    if sl_price > 0 and ((is_buy and last_close < sl_price) or (not is_buy and last_close > sl_price)):
        return {"exit_reason": "PROTECTIVE_SL", "last_close": last_close}
    return {"exit_reason": None, "last_close": last_close}


@dataclass
class CorpReconcileResult:
    checked: int = 0
    closed_meeting: int = 0
    closed_sl: int = 0
    errors: int = 0
    skipped_no_candles: int = 0
    details: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {"checked": self.checked, "closed_meeting": self.closed_meeting,
                "closed_sl": self.closed_sl, "errors": self.errors,
                "skipped_no_candles": self.skipped_no_candles,
                "closed": self.closed_meeting + self.closed_sl, "details": self.details}


class CorpActionReconciliationService:
    """Daily premarket hard meeting-exit manager for the corp-action sub-strategy (PAPER)."""

    def __init__(self, *, settings, state, upstox, order_service):
        self.settings = settings
        self.state = state
        self.upstox = upstox
        self.order_service = order_service

    def _fetch_daily_candles(self, instrument_key: str, asof: str) -> list:
        from datetime import date, timedelta
        frm = (date.fromisoformat(asof) - timedelta(days=60)).isoformat()
        rows = self.upstox.get_historical_candles_v3_days(instrument_key, to_date=asof, from_date=frm, interval_days=1)
        return list(reversed(rows)) if rows and str(rows[0][0]) > str(rows[-1][0]) else rows

    def run(self, asof: str | None = None) -> CorpReconcileResult:
        from autotrader.time_utils import now_ist
        result = CorpReconcileResult()
        asof = asof or now_ist().strftime("%Y-%m-%d")
        open_positions = [p for p in self.state.list_open_positions()
                          if str(p.get("wl_type") or "").strip().lower() == "corp_action"]
        if not open_positions:
            logger.info("corp_recon no open corp_action positions")
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
                ev = evaluate_corp_position(
                    entry_price=float(pos.get("entry_price") or 0),
                    side=str(pos.get("side") or "BUY"),
                    sl_price=float(pos.get("sl_price") or 0),
                    meeting_date=str(pos.get("meeting_date") or ""),
                    candles=candles,
                )
                if ev["exit_reason"]:
                    self._close(pos, tag, instrument_key, ev["exit_reason"], ev["last_close"], result)
                    time.sleep(0.05)
            except Exception:
                logger.exception("corp_recon_error tag=%s symbol=%s", tag, symbol)
                result.errors += 1
        logger.info("corp_recon_summary %s", result.to_dict())
        return result

    def _close(self, pos, tag, instrument_key, exit_reason, last_close, result):
        try:
            res = self.order_service.place_exit_order(
                position_tag=tag, instrument_key=instrument_key, exit_reason=exit_reason, is_amo=True)
            amo = bool(res.get("amo"))
        except Exception:
            logger.exception("corp_recon_exit_order_failed tag=%s", tag)
            amo = False
        if not amo:
            qty = int(pos.get("qty") or 1)
            side_mult = 1 if str(pos.get("side") or "BUY").upper() == "BUY" else -1
            pnl = round((last_close - float(pos.get("entry_price") or 0)) * qty * side_mult, 2)
            self.state.update_position(tag, {"status": "CLOSED", "exit_price": round(last_close, 2),
                                             "exit_reason": exit_reason, "exit_ts": now_ist_str(), "pnl": pnl})
        if exit_reason == "MEETING_EXIT":
            result.closed_meeting += 1
        else:
            result.closed_sl += 1
        result.details.append({"tag": tag, "symbol": pos.get("symbol"), "action": "closed",
                               "reason": exit_reason, "amo_queued": amo})
        logger.info("corp_recon_exit tag=%s reason=%s amo=%s", tag, exit_reason, amo)
