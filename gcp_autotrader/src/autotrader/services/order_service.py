from __future__ import annotations

import logging
import random
import string
import time
from dataclasses import dataclass
from typing import Any

from autotrader.adapters.firestore_state import FirestoreStateStore
from autotrader.adapters.groww_client import GrowwClient
from autotrader.adapters.sheets_repository import GoogleSheetsRepository
from autotrader.settings import AppSettings
from autotrader.time_utils import now_ist_str, today_ist

logger = logging.getLogger(__name__)


def make_ref_id() -> str:
    ts = format(int(time.time() * 1000), "x")[-6:].upper()
    rand = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(3))
    return f"GR-{ts}-{rand}"


def _order_status(raw: str | None) -> str:
    s = str(raw or "").strip().upper()
    if not s:
        return "UNKNOWN"
    if s in {"COMPLETE", "COMPLETED", "FILLED", "TRADED", "EXECUTED"}:
        return "FILLED"
    return s


def _is_final_non_fill(status: str) -> bool:
    return status.upper() in {"REJECTED", "CANCELLED", "CANCELED", "FAILED", "EXPIRED"}


@dataclass
class OrderService:
    settings: AppSettings
    sheets: GoogleSheetsRepository
    state: FirestoreStateStore
    groww: GrowwClient

    def _append_order_log(self, row: list[Any]) -> None:
        self.sheets.append_orders([row])

    def _append_position_row(self, row: list[Any]) -> None:
        self.sheets.append_positions([row])

    def _extract_order_snapshot(self, order_id: str, ref_id: str) -> dict[str, Any] | None:
        try:
            orders = self.groww.list_orders()
        except Exception:
            logger.exception("Failed to list orders for reconciliation")
            return None
        for obj in orders:
            oid = str(obj.get("groww_order_id") or obj.get("order_id") or obj.get("id") or "").strip()
            rid = str(obj.get("order_reference_id") or obj.get("reference_id") or obj.get("ref_id") or "").strip()
            if (order_id and oid == order_id) or (ref_id and rid == ref_id):
                return {
                    "status": _order_status(obj.get("order_status") or obj.get("status") or obj.get("state")),
                    "filled_qty": float(obj.get("filled_quantity") or obj.get("filledQty") or obj.get("executed_quantity") or 0),
                    "avg_fill_price": float(obj.get("average_fill_price") or obj.get("avg_price") or obj.get("averagePrice") or 0),
                    "message": str(obj.get("message") or obj.get("remark") or obj.get("reason") or ""),
                    "raw": obj,
                }
        return None

    def _await_fill(self, order_id: str, ref_id: str, qty: int, timeout_ms: int = 25000, poll_ms: int = 1200) -> dict[str, Any]:
        started = time.time()
        while (time.time() - started) * 1000 < timeout_ms:
            snap = self._extract_order_snapshot(order_id, ref_id)
            if snap:
                status = _order_status(str(snap.get("status", "")))
                filled_qty = float(snap.get("filled_qty", 0))
                if status == "FILLED" or (qty > 0 and filled_qty >= qty):
                    return {"filled": True, "terminal": False, "snapshot": snap}
                if _is_final_non_fill(status):
                    return {"filled": False, "terminal": True, "snapshot": snap}
            time.sleep(poll_ms / 1000.0)
        return {"filled": False, "terminal": False, "snapshot": self._extract_order_snapshot(order_id, ref_id)}

    def place_entry_order(
        self,
        *,
        symbol: str,
        exchange: str,
        segment: str,
        side: str,
        qty: int,
        entry_price: float,
        sl_price: float,
        target: float,
        atr: float,
        product: str,
        score: int,
        reason: str,
        allow_live_orders: bool = False,
    ) -> dict[str, Any] | None:
        if self.state.already_fired_today(symbol, side):
            return {"skipped": "duplicate_idempotency"}

        ref_id = make_ref_id()
        paper = self.settings.runtime.paper_trade or not allow_live_orders

        self._append_order_log([
            now_ist_str(),
            ref_id if paper else "",
            symbol, exchange, segment,
            side, qty, "MARKET", round(entry_price, 2),
            round(sl_price, 2), round(target, 2),
            "PAPER" if paper else "SENT",
            "", "",
        ])

        if paper:
            pos_tag = f"BOTP:{ref_id}"
            self._append_position_row([
                now_ist_str(), symbol, exchange, segment, side,
                round(entry_price, 2), qty, round(abs(entry_price - sl_price), 2), round(target, 2),
                round(sl_price, 2), round(entry_price, 2), 0.0, round(atr, 4), "OPEN", pos_tag, "",
            ])
            self.state.mark_fired_today(symbol, side)
            return {"paper": True, "groww_order_id": ref_id}

        payload = {
            "exchange": exchange,
            "segment": segment,
            "trading_symbol": symbol,
            "quantity": qty,
            "price": 0,
            "trigger_price": 0,
            "order_type": "MARKET",
            "transaction_type": side,
            "product": product,
            "validity": "DAY",
            "order_reference_id": ref_id,
        }
        try:
            resp = self.groww.create_order(payload)
        except Exception as exc:
            logger.exception("Live order create failed")
            return {"error": str(exc), "status": "API_FAIL"}

        order_id = str(resp.get("groww_order_id") or resp.get("order_id") or ref_id)
        probe = self._await_fill(order_id, ref_id, qty)
        if probe.get("filled"):
            snap = probe.get("snapshot") or {}
            fill_price = float(snap.get("avg_fill_price") or entry_price or 0)
            pos_tag = f"BOT:{order_id}:{ref_id}"
            self._append_position_row([
                now_ist_str(), symbol, exchange, segment, side,
                round(fill_price, 2), qty, round(abs(fill_price - sl_price), 2), round(target, 2),
                round(sl_price, 2), round(fill_price, 2), 0.0, round(atr, 4), "OPEN", pos_tag, "",
            ])
            self.state.mark_fired_today(symbol, side)
            return {"groww_order_id": order_id, "order_status": "FILLED", "fill_price": fill_price}

        if probe.get("terminal"):
            self.state.clear_fired_today(symbol, side)
            return {"groww_order_id": order_id, "order_status": "TERMINAL_NONFILL"}

        self.state.save_pending_order(
            ref_id,
            {
                "kind": "entry",
                "order_id": order_id,
                "ref_id": ref_id,
                "symbol": symbol,
                "exchange": exchange,
                "segment": segment,
                "side": side,
                "qty": qty,
                "entry_price": entry_price,
                "sl_price": sl_price,
                "target": target,
                "atr": atr,
                "product": product,
                "score": score,
                "reason": reason,
                "day": today_ist(),
            },
            kind="entry",
        )
        self.state.mark_fired_today(symbol, side)
        return {"groww_order_id": order_id, "order_status": "PENDING_RECON"}

    def reconcile_pending_entries(self, max_items: int = 15) -> dict[str, int | bool]:
        if self.settings.runtime.paper_trade:
            return {"processed": 0, "pending": 0, "filled": 0, "failed": 0, "skippedPaper": True}
        items = self.state.list_pending_orders("entry", limit=max_items)
        processed = pending = filled = failed = 0
        for item in items:
            processed += 1
            ref_id = str(item.get("ref_id") or "")
            order_id = str(item.get("order_id") or "")
            symbol = str(item.get("symbol") or "")
            side = str(item.get("side") or "BUY")
            snap = self._extract_order_snapshot(order_id, ref_id)
            if not snap:
                pending += 1
                continue
            status = _order_status(str(snap.get("status") or ""))
            if status == "FILLED":
                fill_price = float(snap.get("avg_fill_price") or item.get("entry_price") or 0)
                qty = int(float(item.get("qty") or 0))
                pos_tag = f"BOT:{order_id}:{ref_id}"
                self._append_position_row([
                    now_ist_str(),
                    symbol,
                    str(item.get("exchange") or "NSE"),
                    str(item.get("segment") or "CASH"),
                    side,
                    round(fill_price, 2),
                    qty,
                    round(abs(fill_price - float(item.get("sl_price") or fill_price)), 2),
                    round(float(item.get("target") or 0), 2),
                    round(float(item.get("sl_price") or 0), 2),
                    round(fill_price, 2),
                    0.0,
                    round(float(item.get("atr") or 0), 4),
                    "OPEN",
                    pos_tag,
                    "",
                ])
                self.state.delete_pending_order(ref_id, kind="entry")
                filled += 1
            elif _is_final_non_fill(status):
                self.state.clear_fired_today(symbol, side)
                self.state.delete_pending_order(ref_id, kind="entry")
                failed += 1
            else:
                pending += 1
            if processed % 3 == 0:
                time.sleep(0.12)
        return {"processed": processed, "pending": pending, "filled": filled, "failed": failed}

