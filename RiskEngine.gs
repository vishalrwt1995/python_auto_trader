/************************************
* RiskEngine.gs — Non-negotiable risk rules + trailing management
************************************/


function getDailyStats() {
 const sheet = SS.getSheetByName(SH.RISK);
 const data = sheet.getDataRange().getValues();
 const today = todayIST();


 for (let i = 3; i < data.length; i++) {
   if (!data[i][0]) continue;
   try {
     const d = Utilities.formatDate(new Date(data[i][0]), "Asia/Kolkata", "yyyy-MM-dd");
     if (d === today) return { row: i + 1, trades: data[i][1] || 0, pnl: data[i][2] || 0, status: data[i][3] || "ACTIVE" };
   } catch (e) {}
 }
 sheet.appendRow([new Date(), 0, 0, "ACTIVE", "", "", "", "", "", ""]);
 return { row: sheet.getLastRow(), trades: 0, pnl: 0, status: "ACTIVE" };
}


function updateDailyStats(tradeDelta = 1, pnlDelta = 0) {
 const s = getDailyStats();
 const sh = SS.getSheetByName(SH.RISK);
 sh.getRange(s.row, 2).setValue(s.trades + tradeDelta);
 sh.getRange(s.row, 3).setValue(parseFloat(((s.pnl || 0) + pnlDelta).toFixed(2)));
}


function isSystemActive() {
 const cfg = CFG();
 if (!isMarketOpen()) return false;


 const s = getDailyStats();
 if (s.status === "HALTED") { LOG("WARN", "Risk", "HALTED"); return false; }
 if (s.pnl >= cfg.dailyProfitTarget) { LOG("INFO", "Risk", `Daily target hit (₹${cfg.dailyProfitTarget})`); return false; }
 if (s.pnl <= -cfg.maxDailyLoss) { haltSystem(`Daily loss cap hit (₹${Math.abs(s.pnl).toFixed(0)})`); return false; }
 if (s.trades >= cfg.maxTradesDay) { LOG("INFO", "Risk", "Max trades/day"); return false; }
 if (getOpenPositions().length >= cfg.maxPositions) { LOG("INFO", "Risk", "Max positions"); return false; }
 return true;
}


function haltSystem(reason) {
 try {
   const s = getDailyStats();
   const sh = SS.getSheetByName(SH.RISK);
   sh.getRange(s.row, 4).setValue("HALTED");
   sh.getRange(s.row, 5).setValue(reason);
   ALERT("HALTED", reason);
   LOG("ERROR", "Risk", "HALTED: " + reason);
 } catch (e) {}
}


function calcPositionSize(entryPrice, atr, direction) {
 const cfg = CFG();
 const slDist = Math.max(atr * cfg.atrSLMult, entryPrice * 0.005);
 const slPrice = direction === "BUY" ? entryPrice - slDist : entryPrice + slDist;
 const target = direction === "BUY" ? entryPrice + slDist * cfg.rrIntraday : entryPrice - slDist * cfg.rrIntraday;


 let qty = Math.floor(cfg.riskPerTrade / slDist);
 qty = Math.min(qty, Math.floor(cfg.capital * 0.15 / entryPrice)); // max 15% capital per trade
 qty = Math.max(1, qty);


 const brok = calcBrokerage(qty, entryPrice);
 return {
   qty,
   slPrice,
   target,
   slDist,
   entryPrice,
   maxLoss: parseFloat((qty * slDist + brok).toFixed(2)),
   maxGain: parseFloat((qty * slDist * cfg.rrIntraday - brok).toFixed(2)),
   brokerage: brok,
 };
}


function getOpenPositions() {
 return SS.getSheetByName(SH.POSITIONS).getDataRange().getValues().slice(3).filter(r => {
   if (!r[1] || r[13] !== "OPEN") return false;
   if (typeof isManagedPositionRow_ === "function") return isManagedPositionRow_(r);
   return false; // strict: never manage unknown positions
 });
}
function isAlreadyInStock(symbol) {
 return getOpenPositions().some(p => p[1] === symbol);
}

function riskGateAllowsNewTrade_() {
 return isSystemActive();
}


function manageTrailingSL() {
 const cfg = CFG();
 const posSheet = SS.getSheetByName(SH.POSITIONS);
 const data = posSheet.getDataRange().getValues();
 if (data.length < 4) return;


 for (let i = 3; i < data.length; i++) {
   if (!data[i][1] || data[i][13] !== "OPEN") continue;
   if (typeof isManagedPositionRow_ === "function" && !isManagedPositionRow_(data[i])) continue;


   const symbol = data[i][1], exchange = data[i][2], segment = data[i][3];
   const direction = data[i][4];
   const entry = parseFloat(data[i][5]) || 0;
   const qty = parseInt(data[i][6], 10) || 0;
   const target = parseFloat(data[i][8]) || 0;
   const currSL = parseFloat(data[i][9]) || 0;
   const atr = parseFloat(data[i][12]) || 1;


   const live = getLiveQuote(symbol, exchange, segment);
   if (!live || !live.ltp) continue;
   const ltp = live.ltp;


   // update LTP + uPnL
   posSheet.getRange(i + 1, 11).setValue(ltp.toFixed(2));
   const upnl = (direction === "BUY" ? (ltp - entry) : (entry - ltp)) * qty;
   posSheet.getRange(i + 1, 12).setValue(upnl.toFixed(2));


   // Trailing rule: 1x ATR behind current price
   const trail = atr * 1.0;
   let newSL = currSL;


   if (direction === "BUY") {
     if (ltp - trail > currSL) {
       newSL = ltp - trail;
       posSheet.getRange(i + 1, 10).setValue(newSL.toFixed(2));
       LOG("INFO", "Trail", `${symbol} SL→₹${newSL.toFixed(2)}`);
       if (!cfg.paperTrade) refreshSmartStopLoss(symbol, exchange, segment, direction, qty, newSL, target);
     }
     if (ltp <= newSL) { exitPosition(symbol, exchange, segment, "SELL", data[i], "SL_HIT"); continue; }
     if (ltp >= target) { exitPosition(symbol, exchange, segment, "SELL", data[i], "TARGET_HIT"); continue; }
   } else {
     if (ltp + trail < currSL) {
       newSL = ltp + trail;
       posSheet.getRange(i + 1, 10).setValue(newSL.toFixed(2));
       LOG("INFO", "Trail", `${symbol} SL→₹${newSL.toFixed(2)}`);
       if (!cfg.paperTrade) refreshSmartStopLoss(symbol, exchange, segment, direction, qty, newSL, target);
     }
     if (ltp >= newSL) { exitPosition(symbol, exchange, segment, "BUY", data[i], "SL_HIT"); continue; }
     if (ltp <= target) { exitPosition(symbol, exchange, segment, "BUY", data[i], "TARGET_HIT"); continue; }
   }


   Utilities.sleep(150);
 }
}
