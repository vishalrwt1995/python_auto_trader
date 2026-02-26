/************************************
* OrderEngine.gs — Place + exit orders (paper/live) with idempotency
* Sheet schemas used (row 3 headers):
* - 📦 Orders: Timestamp | Groww Order ID | Symbol | Exchange | Segment | Direction | Qty | Order Type | Price | SL Price | Target | Status | P&L | Exit Reason
* - 💼 Positions: Timestamp | Symbol | Exchange | Segment | Direction | Entry | Qty | SL Dist | Target | Curr SL | LTP | Unrealised P&L | ATR | Status | Order ID | Exit Reason
************************************/

const PENDING_ENTRY_PREFIX = "pending_entry:";
const PENDING_EXIT_PREFIX = "pending_exit:";
const DEFAULT_ORDER_FILL_WAIT_MS = 25000;
const DEFAULT_ORDER_POLL_MS = 1200;
const BOT_LIVE_POS_PREFIX = "BOT:";
const BOT_PAPER_POS_PREFIX = "BOTP:";


function _sheetHeaderCols_(sheetName) {
 const sh = SS.getSheetByName(sheetName);
 const hdr = sh.getRange(3, 1, 1, sh.getLastColumn()).getValues()[0];
 let last = hdr.length;
 while (last > 0 && (hdr[last - 1] === "" || hdr[last - 1] == null)) last--;
 return Math.max(1, last);
}


function _appendRowSafe_(sheetName, row) {
 const sh = SS.getSheetByName(sheetName);
 const cols = _sheetHeaderCols_(sheetName);
 const out = row.slice(0, cols);
 while (out.length < cols) out.push("");
 const rowNum = sh.getLastRow() + 1;
 sh.getRange(rowNum, 1, 1, cols).setValues([out]);
 return rowNum;
}


function _dedupeKey_(symbol, side) { return `${todayIST()}|${symbol}|${side}`; }
function _alreadyFiredToday_(symbol, side) {
 return PropertiesService.getScriptProperties().getProperty("fired:" + _dedupeKey_(symbol, side)) === "1";
}
function _markFiredToday_(symbol, side) {
 PropertiesService.getScriptProperties().setProperty("fired:" + _dedupeKey_(symbol, side), "1");
}
function _clearFiredToday_(symbol, side) {
 PropertiesService.getScriptProperties().deleteProperty("fired:" + _dedupeKey_(symbol, side));
}

function _pendingEntryKey_(refId) {
 return PENDING_ENTRY_PREFIX + String(refId || "");
}

function _pendingExitKey_(refId) {
 return PENDING_EXIT_PREFIX + String(refId || "");
}

function _savePendingEntry_(refId, payload) {
 if (!refId || !payload) return;
 try {
   PropertiesService.getScriptProperties().setProperty(_pendingEntryKey_(refId), JSON.stringify(payload));
 } catch (e) {}
}

function _clearPendingEntry_(refId) {
 if (!refId) return;
 try {
   PropertiesService.getScriptProperties().deleteProperty(_pendingEntryKey_(refId));
 } catch (e) {}
}

function _savePendingExit_(refId, payload) {
 if (!refId || !payload) return;
 try {
   PropertiesService.getScriptProperties().setProperty(_pendingExitKey_(refId), JSON.stringify(payload));
 } catch (e) {}
}

function _clearPendingExit_(refId) {
 if (!refId) return;
 try {
   PropertiesService.getScriptProperties().deleteProperty(_pendingExitKey_(refId));
 } catch (e) {}
}

function _botPositionTag_(brokerOrderId, refId, isPaper) {
 const ref = String(refId || "").trim();
 if (isPaper) return `${BOT_PAPER_POS_PREFIX}${ref}`;
 const bo = String(brokerOrderId || "").trim();
 return `${BOT_LIVE_POS_PREFIX}${bo}|${ref}`;
}

function _parseBotPositionTag_(v) {
 const s = String(v || "").trim();
 if (!s) return { managed: false, paper: false, brokerOrderId: "", refId: "" };
 if (s.indexOf(BOT_PAPER_POS_PREFIX) === 0) {
   return { managed: true, paper: true, brokerOrderId: "", refId: s.substring(BOT_PAPER_POS_PREFIX.length) };
 }
 if (s.indexOf(BOT_LIVE_POS_PREFIX) === 0) {
   const core = s.substring(BOT_LIVE_POS_PREFIX.length);
   const parts = core.split("|");
   return { managed: true, paper: false, brokerOrderId: String(parts[0] || "").trim(), refId: String(parts[1] || "").trim() };
 }
 return { managed: false, paper: false, brokerOrderId: "", refId: "" };
}

function isManagedPositionRow_(row) {
 try {
   const tag = String((row && row[14]) || "").trim(); // col O
   return _parseBotPositionTag_(tag).managed;
 } catch (e) {
   return false;
 }
}

function _listPendingEntries_() {
 const props = PropertiesService.getScriptProperties();
 return props.getKeys()
   .filter(k => k.indexOf(PENDING_ENTRY_PREFIX) === 0)
   .map(k => {
     try {
       return { key: k, refId: k.substring(PENDING_ENTRY_PREFIX.length), data: JSON.parse(props.getProperty(k) || "{}") };
     } catch (e) {
       return { key: k, refId: k.substring(PENDING_ENTRY_PREFIX.length), data: {} };
     }
   });
}

function _listPendingExits_() {
 const props = PropertiesService.getScriptProperties();
 return props.getKeys()
   .filter(k => k.indexOf(PENDING_EXIT_PREFIX) === 0)
   .map(k => {
     try {
       return { key: k, refId: k.substring(PENDING_EXIT_PREFIX.length), data: JSON.parse(props.getProperty(k) || "{}") };
     } catch (e) {
       return { key: k, refId: k.substring(PENDING_EXIT_PREFIX.length), data: {} };
     }
   });
}

function _hasPendingExitForPosition_(posTag, symbol) {
 const tag = String(posTag || "").trim();
 const sym = String(symbol || "").toUpperCase();
 const items = _listPendingExits_();
 for (let i = 0; i < items.length; i++) {
   const p = items[i].data || {};
   const pTag = String(p.posTag || "").trim();
   if (tag && pTag && pTag === tag) return true;
   if (!tag && sym && String(p.symbol || "").toUpperCase() === sym) return true;
 }
 return false;
}

function _productForSymbol_(symbol, fallback = "CNC") {
 try {
   const wl = getWatchlist();
   const hit = wl.find(w => String(w.symbol || "").toUpperCase() === String(symbol || "").toUpperCase());
   const p = String(hit?.product || fallback).toUpperCase();
   return (p === "MIS" || p === "CNC") ? p : fallback;
 } catch (e) {
   return fallback;
  }
}

function _smartPropKey_(symbol) {
 return "smart:" + String(symbol || "").toUpperCase();
}

function _saveSmartOrderMeta_(symbol, meta) {
 try {
   PropertiesService.getScriptProperties().setProperty(_smartPropKey_(symbol), JSON.stringify(meta || {}));
 } catch (e) {}
}

function _getSmartOrderMeta_(symbol) {
 try {
   const raw = PropertiesService.getScriptProperties().getProperty(_smartPropKey_(symbol));
   return raw ? JSON.parse(raw) : null;
 } catch (e) {
   return null;
 }
}

function _clearSmartOrderMeta_(symbol) {
 try {
   PropertiesService.getScriptProperties().deleteProperty(_smartPropKey_(symbol));
 } catch (e) {}
}

function _smartLimitPrice_(positionSide, slPrice) {
 const sl = Number(slPrice) || 0;
 if (sl <= 0) return 0;
 if (positionSide === "BUY") return Number((sl * 0.995).toFixed(2));  // exit SELL slightly lower
 return Number((sl * 1.005).toFixed(2)); // exit BUY slightly higher
}

function _pxStr_(v) {
 const n = Number(v);
 return isFinite(n) ? n.toFixed(2) : "";
}

function _createSmartProtectiveOrder_(signal, positionSide, qty, slPrice, targetPrice) {
 const cfg = CFG();
 if (cfg.paperTrade) return null;

 const side = positionSide === "BUY" ? "SELL" : "BUY";
 const productType = _productForSymbol_(signal.symbol, signal.product || "CNC");
 let payload = null;
 let smartType = "GTT";

 // For CASH, create GTT stop-loss safety order.
 if (String(signal.segment || "").toUpperCase() === "CASH") {
   payload = {
     reference_id: makeRefId(),
     smart_order_type: "GTT",
     exchange: signal.exchange,
     segment: signal.segment,
     trading_symbol: signal.symbol,
     quantity: Number(qty),
     trigger_price: _pxStr_(slPrice),
     trigger_direction: positionSide === "BUY" ? "DOWN" : "UP",
     product_type: productType,
     duration: "DAY",
     order: {
       transaction_type: side,
       order_type: "LIMIT",
       price: _pxStr_(_smartLimitPrice_(positionSide, slPrice)),
     },
   };
 } else {
   // For non-CASH (e.g., FNO), OCO supports target + stop-loss.
   smartType = "OCO";
   payload = {
     reference_id: makeRefId(),
     smart_order_type: "OCO",
     exchange: signal.exchange,
     segment: signal.segment,
     trading_symbol: signal.symbol,
     quantity: Number(qty),
     net_position_quantity: Number(qty),
     transaction_type: side,
     product_type: productType,
     duration: "DAY",
     target: {
       trigger_price: _pxStr_(targetPrice),
       order_type: "LIMIT",
       price: _pxStr_(targetPrice),
     },
     stop_loss: {
       trigger_price: _pxStr_(slPrice),
       order_type: "SL_M",
       price: null,
     },
   };
 }

 const resp = growwPOST("order-advance/create", payload);
 if (!resp) {
   DECISION("SMART_ORDER", signal.symbol, "FAIL", "create_failed", { smartType, slPrice, targetPrice });
   return null;
 }
 const smartOrderId = resp.smart_order_id || resp.id || resp.order_id || "";
 if (!smartOrderId) {
   DECISION("SMART_ORDER", signal.symbol, "WARN", "created_without_id", { smartType });
   return null;
 }

 _saveSmartOrderMeta_(signal.symbol, {
   smartOrderId,
   smartType,
   symbol: signal.symbol,
   segment: signal.segment,
   exchange: signal.exchange,
   positionSide,
   exitSide: side,
   qty: Number(qty),
   slPrice: Number(slPrice),
   targetPrice: Number(targetPrice || 0),
   productType,
   updatedAt: nowIST(),
 });
 DECISION("SMART_ORDER", signal.symbol, "CREATED", "protective_created", { smartType, smartOrderId, slPrice, targetPrice });
 return { smartOrderId, smartType };
}

function _cancelSmartProtectiveOrder_(symbol, segmentHint) {
 const meta = _getSmartOrderMeta_(symbol);
 if (!meta || !meta.smartOrderId || !meta.smartType) return false;

 const segment = String(segmentHint || meta.segment || "CASH").toUpperCase();
 const endpoint = `order-advance/cancel/${encodeURIComponent(segment)}/${encodeURIComponent(meta.smartType)}/${encodeURIComponent(meta.smartOrderId)}`;
 const resp = growwPOST(endpoint, {});
 _clearSmartOrderMeta_(symbol);
 DECISION("SMART_ORDER", symbol, "CANCEL", "protective_cancelled", { smartOrderId: meta.smartOrderId, ok: !!resp });
 return !!resp;
}

function refreshSmartStopLoss(symbol, exchange, segment, positionSide, qty, slPrice, targetPrice) {
 const cfg = CFG();
 if (cfg.paperTrade) return null;
 const sig = { symbol, exchange, segment, product: _productForSymbol_(symbol, "CNC") };
 _cancelSmartProtectiveOrder_(symbol, segment);
 return _createSmartProtectiveOrder_(sig, positionSide, qty, slPrice, targetPrice);
}

function _numOr0_(v) {
 const n = Number(v);
 return isFinite(n) ? n : 0;
}

function _orderStatus_(raw) {
 const s = String(raw || "").trim().toUpperCase();
 if (!s) return "UNKNOWN";
 if (s === "COMPLETE" || s === "COMPLETED" || s === "FILLED" || s === "TRADED" || s === "EXECUTED") return "FILLED";
 if (s === "REJECTED" || s === "CANCELLED" || s === "CANCELED" || s === "FAILED" || s === "EXPIRED") return s;
 if (s === "OPEN" || s === "PENDING" || s === "PARTIAL" || s === "PARTIALLY_FILLED" || s === "TRIGGER_PENDING") return s;
 return s;
}

function _isFinalNonFill_(status) {
 return ["REJECTED", "CANCELLED", "CANCELED", "FAILED", "EXPIRED"].indexOf(String(status || "").toUpperCase()) >= 0;
}

function _isFilled_(snapshot, qty) {
 const filledQty = _numOr0_(snapshot?.filledQty);
 const status = _orderStatus_(snapshot?.status);
 if (status === "FILLED") return true;
 if (filledQty > 0 && _numOr0_(qty) > 0 && filledQty >= _numOr0_(qty)) return true;
 return false;
}

function _normOrderSnapshot_(obj) {
 if (!obj) return null;
 return {
   status: _orderStatus_(obj.order_status || obj.status || obj.state || obj.orderState),
   filledQty: _numOr0_(obj.filled_quantity || obj.filledQty || obj.executed_quantity || obj.executedQty),
   avgFillPrice: _numOr0_(obj.average_fill_price || obj.averageFillPrice || obj.avg_price || obj.averagePrice || obj.execution_price),
   message: String(obj.message || obj.remark || obj.reason || ""),
   raw: obj,
 };
}

function _orderObjId_(obj) {
 return String(
   obj?.groww_order_id ||
   obj?.order_id ||
   obj?.id ||
   obj?.orderId ||
   ""
 ).trim();
}

function _orderObjRef_(obj) {
 return String(
   obj?.order_reference_id ||
   obj?.reference_id ||
   obj?.ref_id ||
   obj?.client_order_id ||
   obj?.clientOrderId ||
   ""
 ).trim();
}

function _extractOrderList_(payload) {
 if (!payload) return [];
 if (Array.isArray(payload)) return payload;
 const keys = ["orders", "order_list", "items", "results"];
 for (let i = 0; i < keys.length; i++) {
   const arr = payload[keys[i]];
   if (Array.isArray(arr)) return arr;
 }
 if (Array.isArray(payload.data)) return payload.data;
 if (payload.data && Array.isArray(payload.data.orders)) return payload.data.orders;
 return [];
}

function _snapshotFromPayload_(payload, orderId, refId, allowFirst = true) {
 if (!payload) return null;
 const id = String(orderId || "").trim();
 const ref = String(refId || "").trim();

 const candidates = [];
 if (Array.isArray(payload)) {
   for (let i = 0; i < payload.length; i++) candidates.push(payload[i]);
 } else {
   candidates.push(payload);
   const arr = _extractOrderList_(payload);
   for (let i = 0; i < arr.length; i++) candidates.push(arr[i]);
 }
 if (!candidates.length) return null;

 let first = null;
 for (let i = 0; i < candidates.length; i++) {
   const c = candidates[i];
   const snap = _normOrderSnapshot_(c);
   if (!snap) continue;
   if (!first) first = snap;
   const cid = _orderObjId_(c);
   const cref = _orderObjRef_(c);
   if (id && cid && cid === id) return snap;
   if (ref && cref && cref === ref) return snap;
 }
 return allowFirst ? first : null;
}

function _cachedOrderListRoute_(endpoint, ttlSec = 5) {
 const key = "ordlist:" + endpoint;
 const cache = CacheService.getScriptCache();
 try {
   const hit = cache.get(key);
   if (hit) return JSON.parse(hit);
 } catch (e) {}
 const p = growwGET(endpoint);
 if (!p) return null;
 try { cache.put(key, JSON.stringify(p), Math.max(3, Number(ttlSec) || 5)); } catch (e) {}
 return p;
}

function _fetchOrderSnapshot_(orderId, segment, refId) {
 const seg = String(segment || "CASH").toUpperCase();
 const id = String(orderId || "").trim();
 if (!id && !refId) return null;

 const tries = [];
 if (id) tries.push(`order/status/${encodeURIComponent(id)}?segment=${encodeURIComponent(seg)}`);
 if (refId) tries.push(`order/status/reference/${encodeURIComponent(refId)}?segment=${encodeURIComponent(seg)}`);
 if (id) tries.push(`order/detail/${encodeURIComponent(id)}?segment=${encodeURIComponent(seg)}`);

 for (let i = 0; i < tries.length; i++) {
   const p = growwGET(tries[i]);
   if (!p) continue;
   const snap = _snapshotFromPayload_(p, id, refId, true);
   if (snap) return snap;
 }

 // Fallback routes when status/detail route behavior drifts.
 const listRoutes = [
   `order/list?segment=${encodeURIComponent(seg)}&page=0&page_size=100`,
   `order/list?segment=${encodeURIComponent(seg)}&page=1&page_size=100`,
   `order/list?page=0&page_size=100`,
 ];
 for (let i = 0; i < listRoutes.length; i++) {
   const p = _cachedOrderListRoute_(listRoutes[i], 5);
   if (!p) continue;
   const snap = _snapshotFromPayload_(p, id, refId, false);
   if (snap) return snap;
 }
 return null;
}

function _awaitOrderFill_(orderId, segment, refId, qty, timeoutMs = DEFAULT_ORDER_FILL_WAIT_MS, pollMs = DEFAULT_ORDER_POLL_MS) {
 const start = Date.now();
 let last = null;
 while (Date.now() - start < Math.max(3000, Number(timeoutMs) || DEFAULT_ORDER_FILL_WAIT_MS)) {
   const snap = _fetchOrderSnapshot_(orderId, segment, refId);
   if (snap) {
     last = snap;
     if (_isFilled_(snap, qty)) return { filled: true, snapshot: snap };
     if (_isFinalNonFill_(snap.status)) return { filled: false, terminal: true, snapshot: snap };
   }
   Utilities.sleep(Math.max(500, Number(pollMs) || DEFAULT_ORDER_POLL_MS));
 }
 return { filled: false, timeout: true, snapshot: last };
}

function _setOrderLog_(rowNum, patch) {
 if (!rowNum || rowNum < 4) return;
 const sh = SS.getSheetByName(SH.ORDERS);
 if (!sh) return;
 const p = patch || {};
 try {
   if (p.orderId != null && p.orderId !== "") sh.getRange(rowNum, 2).setValue(String(p.orderId));
   if (p.price != null && p.price !== "") sh.getRange(rowNum, 9).setValue(Number(p.price));
   if (p.slPrice != null && p.slPrice !== "") sh.getRange(rowNum, 10).setValue(Number(p.slPrice));
   if (p.target != null && p.target !== "") sh.getRange(rowNum, 11).setValue(Number(p.target));
   if (p.status != null && p.status !== "") sh.getRange(rowNum, 12).setValue(String(p.status));
   if (p.reason != null && p.reason !== "") sh.getRange(rowNum, 14).setValue(String(p.reason));
 } catch (e) {}
}

function _repriceFromFill_(position, side, fillPrice) {
 const entry = _numOr0_(fillPrice) > 0 ? _numOr0_(fillPrice) : _numOr0_(position.entryPrice);
 const slDist = _numOr0_(position.slDist) > 0
   ? _numOr0_(position.slDist)
   : Math.max(0.05, Math.abs(_numOr0_(position.entryPrice) - _numOr0_(position.slPrice)));
 const rr = _numOr0_(position.slDist) > 0
   ? Math.max(1, Math.abs(_numOr0_(position.target) - _numOr0_(position.entryPrice)) / _numOr0_(position.slDist))
   : CFG().rrIntraday;
 const slPrice = side === "BUY" ? entry - slDist : entry + slDist;
 const target = side === "BUY" ? entry + slDist * rr : entry - slDist * rr;
 return {
   entryPrice: Number(entry.toFixed(2)),
   slDist: Number(slDist.toFixed(2)),
   slPrice: Number(slPrice.toFixed(2)),
   target: Number(target.toFixed(2)),
 };
}

function _openLivePositionAfterFill_(signal, side, qty, priced, atr, orderId, refId) {
 const posTag = _botPositionTag_(orderId, refId, false);
 _appendRowSafe_(SH.POSITIONS, [
   nowIST(), signal.symbol, signal.exchange, signal.segment, side,
   priced.entryPrice, qty, priced.slDist, priced.target,
   priced.slPrice, priced.entryPrice, 0, atr, "OPEN",
   posTag, ""
 ]);
}

function reconcilePendingEntryOrders(maxItems = 12) {
 const items = _listPendingEntries_();
 if (!items.length) return { processed: 0, pending: 0, filled: 0, failed: 0 };
 const cfg = CFG();
 if (cfg.paperTrade) {
   // Never reconcile/advance live pending entries while in paper mode.
   return { processed: 0, pending: items.length, filled: 0, failed: 0, skippedPaper: true };
 }
 let processed = 0, pending = 0, filled = 0, failed = 0;

 for (let i = 0; i < items.length; i++) {
   if (processed >= maxItems) break;
   const it = items[i];
   const p = it.data || {};
   const side = String(p.side || "");
   const orderId = String(p.orderId || "");
   const refId = String(p.refId || it.refId || "");
   const symbol = String(p.symbol || "");
   const exchange = String(p.exchange || "NSE");
   const segment = String(p.segment || "CASH");
   const qty = _numOr0_(p.qty);
   const orderRow = Number(p.orderRow || 0);
   const atr = _numOr0_(p.atr) || 1;

   if (!symbol || !orderId || !side) {
     _clearPendingEntry_(refId);
     continue;
   }

   const snap = _fetchOrderSnapshot_(orderId, segment, refId);
   if (!snap) {
     pending++;
     processed++;
     continue;
   }

   if (_isFilled_(snap, qty)) {
     if (!isAlreadyInStock(symbol)) {
       const priced = _repriceFromFill_({
         entryPrice: _numOr0_(p.entryPrice),
         slDist: _numOr0_(p.slDist),
         slPrice: _numOr0_(p.slPrice),
         target: _numOr0_(p.target),
       }, side, _numOr0_(snap.avgFillPrice) || _numOr0_(p.entryPrice));

       _openLivePositionAfterFill_({ symbol, exchange, segment }, side, qty, priced, atr, orderId, refId);
       _createSmartProtectiveOrder_(
         { symbol, exchange, segment, product: String(p.product || "CNC") },
         side, qty, priced.slPrice, priced.target
       );
       updateDailyStats(1, 0);
     }
     _setOrderLog_(orderRow, { orderId, price: _numOr0_(snap.avgFillPrice) || _numOr0_(p.entryPrice), status: "FILLED" });
     DECISION("ORDER", symbol, "FILLED_LATE", "reconciled_pending_entry", { orderId, refId });
     _clearPendingEntry_(refId);
     filled++;
   } else if (_isFinalNonFill_(snap.status)) {
     _setOrderLog_(orderRow, { orderId, status: snap.status || "REJECTED", reason: snap.message || "broker_terminal_status" });
     _clearFiredToday_(symbol, side);
     DECISION("ORDER", symbol, "FAIL", "pending_entry_terminal", { orderId, refId, status: snap.status, msg: snap.message });
     _clearPendingEntry_(refId);
     failed++;
   } else {
     _setOrderLog_(orderRow, { orderId, status: snap.status || "OPEN" });
     pending++;
   }
   processed++;
   if (processed % 3 === 0) Utilities.sleep(120);
 }
 return { processed, pending, filled, failed };
}


function placeOrder(signal, position, meta, regime) {
 const cfg = CFG();
 const side = signal.direction === "BUY" ? "BUY" : "SELL";


 if (_alreadyFiredToday_(signal.symbol, side)) {
   LOG("WARN", "Order", `Skipped duplicate: ${signal.symbol} ${side}`);
   DECISION("ORDER", signal.symbol, "SKIP", "duplicate_idempotency", { side });
   return null;
 }


 const refId = makeRefId();
 const orderPayload = {
   exchange: signal.exchange,
   segment: signal.segment,
   trading_symbol: signal.symbol,
   quantity: position.qty,
   price: 0,
   trigger_price: 0,
   order_type: "MARKET",
   transaction_type: side,
   product: _productForSymbol_(signal.symbol, signal.product || "CNC"),
   validity: "DAY",
   order_reference_id: refId, // supported per docs citeturn0search13
 };


 // Orders log (entry)
 const orderRow = _appendRowSafe_(SH.ORDERS, [
   nowIST(),
   cfg.paperTrade ? refId : "",          // Groww Order ID (filled later on live)
   signal.symbol, signal.exchange, signal.segment,
   side, position.qty, "MARKET", position.entryPrice,
   position.slPrice, position.target,
   cfg.paperTrade ? "PAPER" : "SENT",
   "", ""                                // P&L, Exit Reason
 ]);


 LOG("INFO", "Order", `${cfg.paperTrade ? "PAPER" : "LIVE"} ${side} ${signal.symbol} x${position.qty} ref=${refId}`);
 DECISION("ORDER", signal.symbol, cfg.paperTrade ? "PAPER_ENTRY" : "LIVE_ENTRY", "order_dispatched", {
   side, qty: position.qty, refId, product: orderPayload.product
 });


 if (cfg.paperTrade) {
  // Positions log (open)
  const posTag = _botPositionTag_("", refId, true);
  _appendRowSafe_(SH.POSITIONS, [
     nowIST(), signal.symbol, signal.exchange, signal.segment, side,
     position.entryPrice, position.qty, position.slDist, position.target,
     position.slPrice, position.entryPrice, 0, position.atr, "OPEN", posTag, ""
   ]);
   updateDailyStats(1, 0);
   _markFiredToday_(signal.symbol, side);
  return { paper: true, groww_order_id: refId };
 }


 const resp = growwPOST("order/create", orderPayload);
 if (!resp) {
   LOG("ERR", "Order", `Order failed: ${signal.symbol}`);
   DECISION("ORDER", signal.symbol, "FAIL", "order_api_failed", { side, refId });
   _setOrderLog_(orderRow, { status: "API_FAIL", reason: "order_create_failed" });
   return null;
 }

 const orderId = String(resp.groww_order_id || resp.order_id || refId);
 const initialStatus = _orderStatus_(resp.order_status || resp.status || "SENT");
 _setOrderLog_(orderRow, { orderId, status: initialStatus });

 const fillProbe = _awaitOrderFill_(orderId, signal.segment, refId, position.qty);
 if (fillProbe.filled) {
   const priced = _repriceFromFill_(position, side, _numOr0_(fillProbe.snapshot?.avgFillPrice) || _numOr0_(position.entryPrice));
   _setOrderLog_(orderRow, {
     orderId,
     status: "FILLED",
     price: priced.entryPrice,
     slPrice: priced.slPrice,
     target: priced.target,
   });
   _openLivePositionAfterFill_(signal, side, position.qty, priced, position.atr, orderId, refId);
   updateDailyStats(1, 0);
   _markFiredToday_(signal.symbol, side);
   _createSmartProtectiveOrder_(signal, side, position.qty, priced.slPrice, priced.target);
   return { ...resp, groww_order_id: orderId, order_status: "FILLED", fill_price: priced.entryPrice };
 }

 if (fillProbe.terminal) {
   const st = _orderStatus_(fillProbe.snapshot?.status || "REJECTED");
   _setOrderLog_(orderRow, { orderId, status: st, reason: fillProbe.snapshot?.message || "terminal_nonfill" });
   DECISION("ORDER", signal.symbol, "FAIL", "entry_not_filled_terminal", { side, orderId, status: st });
   return null;
 }

 // Unknown/pending broker state: do not assume fill. Queue reconciliation and block duplicate entries for same side.
 const pendingStatus = _orderStatus_(fillProbe.snapshot?.status || "PENDING");
 _setOrderLog_(orderRow, { orderId, status: "PENDING_RECON", reason: pendingStatus });
 _savePendingEntry_(refId, {
   orderId, refId,
   symbol: signal.symbol, exchange: signal.exchange, segment: signal.segment,
   side, qty: position.qty,
   entryPrice: position.entryPrice, slDist: position.slDist, slPrice: position.slPrice, target: position.target,
   atr: position.atr, product: orderPayload.product,
   orderRow,
   createdAt: nowIST(),
 });
 _markFiredToday_(signal.symbol, side);
 DECISION("ORDER", signal.symbol, "PENDING_RECON", "entry_not_confirmed_immediately", { side, orderId, refId, pendingStatus });
 return { ...resp, groww_order_id: orderId, order_status: "PENDING_RECON" };
}


function exitPosition(symbol, exchange, segment, exitSide, posRow, reason) {
 const cfg = CFG();
 if (!isManagedPositionRow_(posRow)) {
   LOG("WARN", "Exit", `Skipped unmanaged position ${symbol}`);
   DECISION("ORDER", symbol, "SKIP", "unmanaged_position", { reason });
   return null;
 }
 const qty = parseInt(posRow[6], 10) || 0;
 if (qty <= 0) {
   DECISION("ORDER", symbol, "SKIP", "invalid_exit_qty", { qty, reason });
   return null;
 }
 const entry = parseFloat(posRow[5]) || 0;
 const ltp = parseFloat(posRow[10]) || entry;
 const positionSide = String(posRow[4] || "");
 const posTag = String((posRow && posRow[14]) || "").trim();

 // Avoid duplicate exits while broker state is unresolved for this managed row.
 if (!cfg.paperTrade && _hasPendingExitForPosition_(posTag, symbol)) {
   DECISION("ORDER", symbol, "SKIP", "pending_exit_exists", { reason, posTag });
   return null;
 }
 const refId = makeRefId();
 const approxPnl = (positionSide === "BUY") ? (ltp - entry) * qty : (entry - ltp) * qty;

 // Orders log (exit)
 const orderRow = _appendRowSafe_(SH.ORDERS, [
   nowIST(),
   cfg.paperTrade ? refId : "",
   symbol, exchange, segment,
   exitSide, qty, "MARKET", ltp,
   "", "", cfg.paperTrade ? "PAPER_EXIT" : "EXIT_SENT",
   approxPnl.toFixed(2), reason
 ]);

 if (cfg.paperTrade) {
   markPositionClosed(symbol, reason, ltp, approxPnl, posRow);
   return { paper: true, groww_order_id: refId, order_status: "FILLED", fill_price: ltp };
 }

 const payload = {
   exchange, segment, trading_symbol: symbol,
   quantity: qty, price: 0, trigger_price: 0,
   order_type: "MARKET",
   transaction_type: exitSide,
   product: _productForSymbol_(symbol, "CNC"),
   validity: "DAY",
   order_reference_id: refId,
 };
 const resp = growwPOST("order/create", payload);
 if (!resp) {
   _setOrderLog_(orderRow, { status: "API_FAIL", reason: "exit_order_create_failed" });
   DECISION("ORDER", symbol, "FAIL", "exit_order_api_failed", { side: exitSide, qty, refId });
   return null;
 }
 const orderId = String(resp.groww_order_id || resp.order_id || refId);
 const initialStatus = _orderStatus_(resp.order_status || resp.status || "SENT");
 _setOrderLog_(orderRow, { orderId, status: initialStatus });

 const fillProbe = _awaitOrderFill_(orderId, segment, refId, qty);
 if (fillProbe.filled) {
   const exitPrice = _numOr0_(fillProbe.snapshot?.avgFillPrice) || _numOr0_(ltp) || _numOr0_(entry);
   const realizedPnl = (positionSide === "BUY") ? (exitPrice - entry) * qty : (entry - exitPrice) * qty;
   _setOrderLog_(orderRow, { orderId, status: "FILLED", price: Number(exitPrice.toFixed(2)), reason });
   const closed = markPositionClosed(symbol, reason, exitPrice, realizedPnl, posRow);
   if (closed) _cancelSmartProtectiveOrder_(symbol, segment);
   DECISION("ORDER", symbol, "EXIT_FILLED", reason, {
     side: exitSide, qty, orderId, refId, price: Number(exitPrice.toFixed(2)), pnl: Number(realizedPnl.toFixed(2)),
   });
   return { ...resp, groww_order_id: orderId, order_status: "FILLED", fill_price: Number(exitPrice.toFixed(2)) };
 }

 if (fillProbe.terminal) {
   const st = _orderStatus_(fillProbe.snapshot?.status || "REJECTED");
   _setOrderLog_(orderRow, { orderId, status: st, reason: fillProbe.snapshot?.message || "exit_terminal_nonfill" });
   DECISION("ORDER", symbol, "FAIL", "exit_not_filled_terminal", { side: exitSide, qty, orderId, refId, status: st });
   return null;
 }

 const pendingStatus = _orderStatus_(fillProbe.snapshot?.status || "PENDING");
 _setOrderLog_(orderRow, { orderId, status: "PENDING_RECON", reason: pendingStatus });
 _savePendingExit_(refId, {
   orderId, refId,
   symbol, exchange, segment,
   exitSide, qty,
   entry, ltp, positionSide,
   reason: String(reason || "EXIT"),
   posTag,
   orderRow,
   createdAt: nowIST(),
 });
 DECISION("ORDER", symbol, "PENDING_RECON", "exit_not_confirmed_immediately", { side: exitSide, qty, orderId, refId, pendingStatus, posTag });
 return { ...resp, groww_order_id: orderId, order_status: "PENDING_RECON" };
}

function reconcilePendingExitOrders(maxItems = 12) {
 const items = _listPendingExits_();
 if (!items.length) return { processed: 0, pending: 0, filled: 0, failed: 0 };
 const cfg = CFG();
 if (cfg.paperTrade) {
   // Never reconcile/advance live pending exits while in paper mode.
   return { processed: 0, pending: items.length, filled: 0, failed: 0, skippedPaper: true };
 }
 let processed = 0, pending = 0, filled = 0, failed = 0;

 for (let i = 0; i < items.length; i++) {
   if (processed >= maxItems) break;
   const it = items[i];
   const p = it.data || {};
   const orderId = String(p.orderId || "");
   const refId = String(p.refId || it.refId || "");
   const symbol = String(p.symbol || "");
   const segment = String(p.segment || "CASH");
   const qty = _numOr0_(p.qty);
   const entry = _numOr0_(p.entry);
   const ltp = _numOr0_(p.ltp);
   const positionSide = String(p.positionSide || "");
   const reason = String(p.reason || "EXIT");
   const orderRow = Number(p.orderRow || 0);
   const posTag = String(p.posTag || "").trim();

   if (!symbol || !orderId || !positionSide) {
     _clearPendingExit_(refId);
     continue;
   }

   const snap = _fetchOrderSnapshot_(orderId, segment, refId);
   if (!snap) {
     pending++;
     processed++;
     continue;
   }

   if (_isFilled_(snap, qty)) {
     const exitPrice = _numOr0_(snap.avgFillPrice) || ltp || entry;
     const realizedPnl = (positionSide === "BUY") ? (exitPrice - entry) * qty : (entry - exitPrice) * qty;
     const posStub = [];
     posStub[14] = posTag;
     const closed = markPositionClosed(symbol, reason, exitPrice, realizedPnl, posStub);
     _setOrderLog_(orderRow, { orderId, status: "FILLED", price: Number(exitPrice.toFixed(2)), reason });
     if (closed) _cancelSmartProtectiveOrder_(symbol, segment);
     DECISION("ORDER", symbol, "FILLED_LATE_EXIT", "reconciled_pending_exit", { orderId, refId, closed });
     _clearPendingExit_(refId);
     filled++;
   } else if (_isFinalNonFill_(snap.status)) {
     _setOrderLog_(orderRow, { orderId, status: snap.status || "REJECTED", reason: snap.message || "exit_broker_terminal_status" });
     DECISION("ORDER", symbol, "FAIL", "pending_exit_terminal", { orderId, refId, status: snap.status, msg: snap.message });
     _clearPendingExit_(refId);
     failed++;
   } else {
     _setOrderLog_(orderRow, { orderId, status: snap.status || "OPEN" });
     pending++;
   }
   processed++;
   if (processed % 3 === 0) Utilities.sleep(120);
 }
 return { processed, pending, filled, failed };
}

function markPositionClosed(symbol, reason, exitPrice, pnl, posRow) {
 const posSheet = SS.getSheetByName(SH.POSITIONS);
 const data = posSheet.getDataRange().getValues();
 const targetTag = String((posRow && posRow[14]) || "").trim();
 let closed = false;

 for (let i = 3; i < data.length; i++) {
   if (data[i][1] === symbol && data[i][13] === "OPEN") {
     if (targetTag && String(data[i][14] || "").trim() !== targetTag) continue;
     if (!targetTag && !isManagedPositionRow_(data[i])) continue;
     posSheet.getRange(i + 1, 11).setValue(Number((_numOr0_(exitPrice)).toFixed(2)));
     posSheet.getRange(i + 1, 12).setValue(Number((_numOr0_(pnl)).toFixed(2)));
     posSheet.getRange(i + 1, 14).setValue("CLOSED");
     posSheet.getRange(i + 1, 16).setValue(reason);
     closed = true;
     break;
   }
 }

 if (!closed) {
   LOG("WARN", "Exit", `${symbol} close skipped (not found/open)`);
   return false;
 }

 updateDailyStats(0, _numOr0_(pnl));
 LOG("INFO", "Exit", `${symbol} CLOSED PnL=INR${_numOr0_(pnl).toFixed(2)} (${reason})`);
 return true;
}

function squareOffIntraday() {
 const startedAt = Date.now();
 ACTION("OrderEngine", "squareOffIntraday", "START", "", {});
 const cfg = CFG();
 if (!cfg.autoSquareOff) {
   ACTION("OrderEngine", "squareOffIntraday", "SKIP", "auto square-off disabled", { ms: Date.now() - startedAt });
   _flushActionLogs_();
   return;
 }


 // Only square off MIS symbols (based on Watchlist product)
 const wl = getWatchlist();
 const misSet = new Set(wl.filter(w => (w.product || "").toUpperCase() === "MIS").map(w => w.symbol));


 const open = getOpenPositions();
 let exited = 0;
 open.forEach(p => {
   const sym = p[1];
   if (!misSet.has(sym)) return;
   exitPosition(sym, p[2], p[3], p[4] === "BUY" ? "SELL" : "BUY", p, "AUTO_SQUAREOFF");
   exited++;
 });
 ACTION("OrderEngine", "squareOffIntraday", "DONE", "square-off sweep complete", {
   ms: Date.now() - startedAt,
   openPositions: open.length,
   misSymbols: misSet.size,
   exitAttempts: exited,
 });
 _flushActionLogs_();
}
