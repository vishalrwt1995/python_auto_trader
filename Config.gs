const SS = SpreadsheetApp.getActiveSpreadsheet();
const API_BASE = "https://api.groww.in/v1/";


// --- Sheet names (single source of truth)
const SH = {
 CONFIG: "⚙️ Config",
 WATCHLIST: "📋 Watchlist",
 UNIVERSE: "🧾 Universe Instruments",
 CANDLE_CACHE: "🗄️ Candle Cache",
 SCORE_CACHE_1D: "📘 Score Cache 1D",
 SCORE_CACHE_1D_DATA: "📗 Score Cache 1D Data",
 BACKFILL: "📚 History Backfill",
 DECISIONS: "🧠 Decision Log",
 ACTIONS: "🧩 Project Log",
 MARKET: "🧠 Market Brain",
 SCAN: "📡 Live Scanner",
 SIGNALS: "🎯 Signals",
 ORDERS: "📦 Orders",
 POSITIONS: "💼 Positions",
 PNL: "💰 P&L Tracker",
 RISK: "🛡️ Risk Monitor",
 LOGS: "📝 Logs",
};


// --- Labels used in Config sheet col A
const CFG_LABELS = {
  API_KEY: "API Key",
  ACCESS_TOKEN: "Access Token",
  REFRESH_SECRET: "Refresh Token",
  TOKEN_EXPIRY: "Token Expiry",
  LAST_REFRESHED: "Last Refreshed",
  USER_ID: "User ID",

  CAPITAL: "Total Capital (₹)",
  RISK_PER_TRADE: "Risk Per Trade (₹)",
  MAX_DAILY_LOSS: "Max Daily Loss (₹)",
  DAILY_PROFIT_TARGET: "Daily Profit Target (₹)",
  MAX_TRADES_DAY: "Max Trades Per Day",
  MAX_POSITIONS: "Max Open Positions",
  MIN_SCORE: "Min Signal Score",

  EMA_FAST: "EMA Fast Period",
  EMA_MED: "EMA Medium Period",
  EMA_SLOW: "EMA Slow Period",
  RSI_PERIOD: "RSI Period",
  RSI_BUY_MIN: "RSI Buy Min",
  RSI_BUY_MAX: "RSI Buy Max",
  RSI_SELL_MIN: "RSI Sell Min",
  RSI_SELL_MAX: "RSI Sell Max",

  VOL_MULT: "Volume Multiplier",
  ATR_SL_MULT: "ATR SL Multiplier",
  RR_INTRADAY: "RR Intraday",
  RR_SWING: "RR Swing",

  VIX_SAFE_MAX: "VIX Safe Max",
  VIX_TREND_MAX: "VIX Trend Max",
  PCR_BULL_MIN: "PCR Bull Min",
  PCR_BEAR_MAX: "PCR Bear Max",
  NIFTY_TREND_PCT: "Nifty Trend %",

  PAPER_TRADE: "Paper Trade Mode",
  EMAIL_ALERTS: "Email Alerts",
  AUTO_SQUARE_OFF: "Auto Square-Off",
  TOKEN_AUTO_REFRESH: "Token Auto-Refresh",
  SWING_TRADING: "Swing Trading",
};


// --- Defaults (used when label missing / blank)
const CFG_DEFAULTS = {
 capital: 50000,
 riskPerTrade: 125,     // conservative phase for ₹50k capital
 maxDailyLoss: 300,     // daily hard-stop while stabilizing
 dailyProfitTarget: 200,
 maxTradesDay: 5,
 maxPositions: 3,
 minScore: 72,


 emaFast: 9,
 emaMed: 21,
 emaSlow: 50,
 rsiPeriod: 14,
 rsiBuyMin: 45,
 rsiBuyMax: 65,
 rsiSellMin: 35,
 rsiSellMax: 55,
 volMult: 1.5,
 atrSLMult: 1.5,
 rrIntraday: 1.5,


 vixSafeMax: 20,
 vixTrendMax: 15,
 pcrBullMin: 0.8,
 pcrBearMax: 1.2,
 niftyTrendPct: 0.3,


 paperTrade: true,
 emailAlerts: false,
 autoSquareOff: true,
};


// --- Config reader (label based)
function _cfgSheet_() {
 const s = SS.getSheetByName(SH.CONFIG);
 if (!s) throw new Error(`Missing sheet: ${SH.CONFIG}`);
 return s;
}


function _configLabelMap_() {
 const cache = CacheService.getScriptCache();
 const hit = cache.get("cfg_label_map");
 if (hit) return JSON.parse(hit);


 const s = _cfgSheet_();
 const last = Math.min(300, s.getLastRow());
 const vals = s.getRange(1, 1, last, 2).getValues();
 const map = {};
 for (let i = 0; i < vals.length; i++) {
   const label = (vals[i][0] || "").toString().trim();
   if (!label) continue;
   map[label] = { row: i + 1, colB: vals[i][1] };
 }
 cache.put("cfg_label_map", JSON.stringify(map), 300); // 5 min
 return map;
}


function _getCfgByLabel_(label, fallback) {
 const map = _configLabelMap_();
 if (map[label] && map[label].colB !== "" && map[label].colB != null) return map[label].colB;
 return fallback;
}


/**
 * cfgGet(label)
 * Reads ⚙️ Config!A:B where A is label and B is value.
 * This is used by TestHarness and other modules.
 */
function cfgGet(label, defaultValue = "") {
  return _getCfgByLabel_(String(label), defaultValue);
}

/**
 * cfgSet(label, value)
 * Writes value into ⚙️ Config for the given label (col A match), in col B.
 */
function cfgSet(label, value) {
  const sh = SS.getSheetByName(SH.CONFIG);
  if (!sh) throw new Error("Config sheet not found: " + SH.CONFIG);
  const last = sh.getLastRow();
  const rng = sh.getRange(1, 1, last, 2).getValues();
  const key = String(label).trim();
  for (let i = 0; i < rng.length; i++) {
    if (String(rng[i][0] || "").trim() === key) {
      sh.getRange(i + 1, 2).setValue(value);
      CacheService.getScriptCache().remove("cfg_label_map");
      return true;
    }
  }
  // If label does not exist in template, append it so runtime setters still work.
  const row = sh.getLastRow() + 1;
  sh.getRange(row, 1, 1, 2).setValues([[key, value]]);
  CacheService.getScriptCache().remove("cfg_label_map");
  return true;
}


function _bool_(v) {
 return v === true || String(v).toUpperCase() === "TRUE" || String(v).toUpperCase() === "Y";
}


function CFG() {
 const props = PropertiesService.getScriptProperties();
 const apiKey = String(props.getProperty("GROWW_API_KEY") || _getCfgByLabel_(CFG_LABELS.API_KEY, "") || "").trim();
 const accessToken = String(props.getProperty("GROWW_ACCESS_TOKEN") || _getCfgByLabel_(CFG_LABELS.ACCESS_TOKEN, "") || "").trim();
 const apiSecret = String(props.getProperty("GROWW_API_SECRET") || _getCfgByLabel_(CFG_LABELS.REFRESH_SECRET, "") || "").trim();
 const apiHost = String(props.getProperty("GROWW_API_HOST") || "https://api.groww.in").trim();


 return {
   apiKey,
   accessToken,
   apiSecret,
   apiHost,


   capital: Number(_getCfgByLabel_(CFG_LABELS.CAPITAL, CFG_DEFAULTS.capital)) || CFG_DEFAULTS.capital,
   riskPerTrade: Number(_getCfgByLabel_(CFG_LABELS.RISK_PER_TRADE, CFG_DEFAULTS.riskPerTrade)) || CFG_DEFAULTS.riskPerTrade,
   maxDailyLoss: Number(_getCfgByLabel_(CFG_LABELS.MAX_DAILY_LOSS, CFG_DEFAULTS.maxDailyLoss)) || CFG_DEFAULTS.maxDailyLoss,
   dailyProfitTarget: Number(_getCfgByLabel_(CFG_LABELS.DAILY_PROFIT_TARGET, CFG_DEFAULTS.dailyProfitTarget)) || CFG_DEFAULTS.dailyProfitTarget,
   maxTradesDay: Number(_getCfgByLabel_(CFG_LABELS.MAX_TRADES_DAY, CFG_DEFAULTS.maxTradesDay)) || CFG_DEFAULTS.maxTradesDay,
   maxPositions: Number(_getCfgByLabel_(CFG_LABELS.MAX_POSITIONS, CFG_DEFAULTS.maxPositions)) || CFG_DEFAULTS.maxPositions,
   minScore: Number(_getCfgByLabel_(CFG_LABELS.MIN_SCORE, CFG_DEFAULTS.minScore)) || CFG_DEFAULTS.minScore,


   emaFast: Number(_getCfgByLabel_(CFG_LABELS.EMA_FAST, CFG_DEFAULTS.emaFast)) || CFG_DEFAULTS.emaFast,
   emaMed: Number(_getCfgByLabel_(CFG_LABELS.EMA_MED, CFG_DEFAULTS.emaMed)) || CFG_DEFAULTS.emaMed,
   emaSlow: Number(_getCfgByLabel_(CFG_LABELS.EMA_SLOW, CFG_DEFAULTS.emaSlow)) || CFG_DEFAULTS.emaSlow,
   rsiPeriod: Number(_getCfgByLabel_(CFG_LABELS.RSI_PERIOD, CFG_DEFAULTS.rsiPeriod)) || CFG_DEFAULTS.rsiPeriod,
   rsiBuyMin: Number(_getCfgByLabel_(CFG_LABELS.RSI_BUY_MIN, CFG_DEFAULTS.rsiBuyMin)) || CFG_DEFAULTS.rsiBuyMin,
   rsiBuyMax: Number(_getCfgByLabel_(CFG_LABELS.RSI_BUY_MAX, CFG_DEFAULTS.rsiBuyMax)) || CFG_DEFAULTS.rsiBuyMax,
   rsiSellMin: Number(_getCfgByLabel_(CFG_LABELS.RSI_SELL_MIN, CFG_DEFAULTS.rsiSellMin)) || CFG_DEFAULTS.rsiSellMin,
   rsiSellMax: Number(_getCfgByLabel_(CFG_LABELS.RSI_SELL_MAX, CFG_DEFAULTS.rsiSellMax)) || CFG_DEFAULTS.rsiSellMax,
   volMult: Number(_getCfgByLabel_(CFG_LABELS.VOL_MULT, CFG_DEFAULTS.volMult)) || CFG_DEFAULTS.volMult,
   atrSLMult: Number(_getCfgByLabel_(CFG_LABELS.ATR_SL_MULT, CFG_DEFAULTS.atrSLMult)) || CFG_DEFAULTS.atrSLMult,
   rrIntraday: Number(_getCfgByLabel_(CFG_LABELS.RR_INTRADAY, CFG_DEFAULTS.rrIntraday)) || CFG_DEFAULTS.rrIntraday,


   vixSafeMax: Number(_getCfgByLabel_(CFG_LABELS.VIX_SAFE_MAX, CFG_DEFAULTS.vixSafeMax)) || CFG_DEFAULTS.vixSafeMax,
   vixTrendMax: Number(_getCfgByLabel_(CFG_LABELS.VIX_TREND_MAX, CFG_DEFAULTS.vixTrendMax)) || CFG_DEFAULTS.vixTrendMax,
   pcrBullMin: Number(_getCfgByLabel_(CFG_LABELS.PCR_BULL_MIN, CFG_DEFAULTS.pcrBullMin)) || CFG_DEFAULTS.pcrBullMin,
   pcrBearMax: Number(_getCfgByLabel_(CFG_LABELS.PCR_BEAR_MAX, CFG_DEFAULTS.pcrBearMax)) || CFG_DEFAULTS.pcrBearMax,
   niftyTrendPct: Number(_getCfgByLabel_(CFG_LABELS.NIFTY_TREND_PCT, CFG_DEFAULTS.niftyTrendPct)) || CFG_DEFAULTS.niftyTrendPct,


   paperTrade: _bool_(_getCfgByLabel_(CFG_LABELS.PAPER_TRADE, CFG_DEFAULTS.paperTrade)),
   emailAlerts: _bool_(_getCfgByLabel_(CFG_LABELS.EMAIL_ALERTS, CFG_DEFAULTS.emailAlerts)),
   autoSquareOff: _bool_(_getCfgByLabel_(CFG_LABELS.AUTO_SQUARE_OFF, CFG_DEFAULTS.autoSquareOff)),
 };
}


function getHeaders() {
 const token = _accessToken_();
 return {
   "Authorization": `Bearer ${token}`,
   "X-API-VERSION": "1.0",
   "Content-Type": "application/json",
   "Accept": "application/json",
 };
}

function _accessToken_() {
 const props = PropertiesService.getScriptProperties();
 let token = String(props.getProperty("GROWW_ACCESS_TOKEN") || "").trim();
 if (token) return token;
 try {
   if (typeof ensureAccessToken === "function") token = String(ensureAccessToken() || "").trim();
 } catch (e) {
   LOG("WARN", "Auth", "ensureAccessToken failed: " + e.toString());
 }
 if (!token) token = String(_getCfgByLabel_(CFG_LABELS.ACCESS_TOKEN, "") || "").trim();
 return token;
}

function _apiBase_() {
 const host = String(CFG().apiHost || "").trim().replace(/\/+$/, "");
 if (host) return host + "/v1/";
 return API_BASE;
}


// --- Digest helpers
function generateChecksum(secret, timestamp) {
 const bytes = Utilities.computeDigest(
   Utilities.DigestAlgorithm.SHA_256,
   secret + timestamp,
   Utilities.Charset.UTF_8
 );
 return bytes.map(b => ("0" + (b & 0xFF).toString(16)).slice(-2)).join("");
}


// --- Token refresh (invoked automatically on 401/403)
function autoRefreshToken() {
 const cfg = CFG();
 if (!cfg.apiKey || !cfg.apiSecret) return false;


 try {
   const timestamp = Math.floor(Date.now() / 1000).toString();
   const checksum = generateChecksum(cfg.apiSecret, timestamp);


   const r = UrlFetchApp.fetch(_apiBase_() + "token/api/access", {
     method: "POST",
     headers: {
       "Authorization": `Bearer ${cfg.apiKey}`,
       "Content-Type": "application/json",
       "Accept": "application/json",
       "X-API-VERSION": "1.0",
     },
     payload: JSON.stringify({ key_type: "approval", checksum, timestamp }),
     muteHttpExceptions: true,
   });


   const code = r.getResponseCode();
   const raw = r.getContentText();
   if (code !== 200) {
     LOG("ERR", "Auth", `Refresh failed HTTP ${code}: ${raw.substring(0, 120)}`);
     return false;
   }
   const d = JSON.parse(raw);
   const token = d.token || d.access_token || d.payload?.token;
   const expiry = d.expiry || d.payload?.expiry || "";
   if (!token) {
     LOG("ERR", "Auth", "Refresh success but no token in response");
     return false;
   }


   // Script properties are source of truth.
   const props = PropertiesService.getScriptProperties();
   props.setProperty("GROWW_ACCESS_TOKEN", String(token));
   if (expiry) props.setProperty("GROWW_ACCESS_TOKEN_EXPIRY_ISO", String(expiry));

   // Optional sync to config sheet for visibility.
   const s = _cfgSheet_();
   const map = _configLabelMap_();
   if (map[CFG_LABELS.ACCESS_TOKEN]) s.getRange(map[CFG_LABELS.ACCESS_TOKEN].row, 2).setValue(token);
   if (map[CFG_LABELS.TOKEN_EXPIRY] && expiry) s.getRange(map[CFG_LABELS.TOKEN_EXPIRY].row, 2).setValue(expiry);
   if (map[CFG_LABELS.LAST_REFRESHED]) s.getRange(map[CFG_LABELS.LAST_REFRESHED].row, 2).setValue(nowIST());


   CacheService.getScriptCache().remove("cfg_label_map");
   LOG("INFO", "Auth", "✅ Token auto-refreshed");
   return true;
 } catch (e) {
   LOG("ERR", "Auth", "autoRefreshToken: " + e.toString());
   return false;
 }
}


// --- Robust HTTP (retry + refresh token on auth)
function _fetch_(method, endpoint, bodyObj) {
 const url = _apiBase_() + endpoint;
 const payload = bodyObj ? JSON.stringify(bodyObj) : null;


 const maxAttempts = 3;
 for (let attempt = 1; attempt <= maxAttempts; attempt++) {
   try {
     const res = UrlFetchApp.fetch(url, {
       method,
       headers: getHeaders(),
       payload,
       muteHttpExceptions: true,
     });


     const code = res.getResponseCode();
     const raw = res.getContentText() || "";


     if (code === 401 || code === 403) {
       LOG("WARN", "HTTP", `Auth ${code} on ${endpoint} — refreshing token (attempt ${attempt})`);
       if (autoRefreshToken()) continue; // retry with new token
       return null;
     }


     // Retry on transient errors
     if (code === 429 || (code >= 500 && code <= 599)) {
       LOG("WARN", "HTTP", `Transient HTTP ${code} on ${endpoint} (attempt ${attempt})`);
       Utilities.sleep(300 * attempt);
       continue;
     }


     if (code < 200 || code >= 300) {
       LOG("ERR", "HTTP", `HTTP ${code} [${endpoint}]: ${raw.substring(0, 160)}`);
       return null;
     }


     return parseGrowwResponse(raw, endpoint);
   } catch (e) {
     LOG("ERR", "HTTP", `${method} ${endpoint}: ${e.toString()}`);
     Utilities.sleep(200 * attempt);
   }
 }
 return null;
}


function growwGET(endpoint) { return _fetch_("GET", endpoint); }

function _isTradeWriteEndpoint_(endpoint) {
 const e = String(endpoint || "").toLowerCase();
 return (
   e.indexOf("order/create") === 0 ||
   e.indexOf("order/cancel") === 0 ||
   e.indexOf("order/modify") === 0 ||
   e.indexOf("order-advance/create") === 0 ||
   e.indexOf("order-advance/cancel") === 0
 );
}

function growwPOST(endpoint, body) {
 const ep = String(endpoint || "");
 if (CFG().paperTrade && _isTradeWriteEndpoint_(ep)) {
   LOG("WARN", "HTTP", `Paper mode blocked live POST ${ep}`);
   return { paper_blocked: true, endpoint: ep };
 }
 return _fetch_("POST", ep, body);
}


function parseGrowwResponse(raw, endpoint) {
 const trimmed = (raw || "").trim();
 if (!trimmed || (trimmed[0] !== "{" && trimmed[0] !== "[")) {
   LOG("ERR", "Parse", `Non-JSON [${endpoint}]: ${trimmed.substring(0, 120)}`);
   return null;
 }
 try {
   const d = JSON.parse(trimmed);
   if (String(d.status || "").toUpperCase() === "SUCCESS") return d.payload != null ? d.payload : {};
   if (d.groww_order_id) return d;
   if (d.token || d.access_token) return d;
   // Some APIs return object directly
   if (d.last_price !== undefined || d.candles || d.data) return d;


   const msg = d.message || d.error?.message || d.error?.errorMessage || JSON.stringify(d).substring(0, 160);
   LOG("ERR", "Parse", `[${endpoint}]: ${msg}`);
   return null;
 } catch (e) {
   LOG("ERR", "Parse", `${endpoint}: ${e.toString()}`);
   return null;
 }
}


// --- Time helpers (IST)
function getISTMins() {
 const ist = new Date(new Date().getTime() + 5.5 * 3600000);
 return ist.getUTCHours() * 60 + ist.getUTCMinutes();
}
function todayIST() {
 return Utilities.formatDate(new Date(), "Asia/Kolkata", "yyyy-MM-dd");
}
function nowIST() {
 return Utilities.formatDate(new Date(), "Asia/Kolkata", "dd-MM-yyyy HH:mm:ss");
}
function isMarketOpen() {
 const m = getISTMins();
 const day = new Date(new Date().getTime() + 5.5 * 3600000).getUTCDay(); // 1..5 weekdays
 return day >= 1 && day <= 5 && m >= 555 && m <= 930; // 09:15..15:30
}
function isEntryWindowOpen() {
 // No new entries within last 15 mins
 const m = getISTMins();
 return isMarketOpen() && m <= 915;
}


// --- Logging (batch buffer + trim)
function LOG(level, fn, msg) {
 try {
   const cache = CacheService.getScriptCache();
   const k = "log_buf";
   const existing = cache.get(k);
   const arr = existing ? JSON.parse(existing) : [];
   arr.push([nowIST(), level, fn, String(msg)]);
   cache.put(k, JSON.stringify(arr), 300);


   // Flush if big
   if (arr.length >= 20) _flushLogs_();
 } catch (e) {
   // fallback (do nothing)
 }
}


function _flushLogs_() {
 const cache = CacheService.getScriptCache();
 const k = "log_buf";
 const existing = cache.get(k);
 if (!existing) return;
 cache.remove(k);


 const rows = JSON.parse(existing) || [];
 if (!rows.length) return;


 const sheet = SS.getSheetByName(SH.LOGS);
 if (!sheet) return;


 sheet.getRange(sheet.getLastRow() + 1, 1, rows.length, 4).setValues(rows);


 // Trim keeping header area (first 4 rows)
 const maxRows = 1200;
 const lr = sheet.getLastRow();
 if (lr > maxRows) {
   const deleteCount = lr - maxRows;
   sheet.deleteRows(5, deleteCount);
 }
}

function _actionExecId_() {
 try {
   const cache = CacheService.getScriptCache();
   const key = "action_exec_id";
   let id = cache.get(key);
   if (!id) {
     id = Utilities.getUuid().split("-")[0].toUpperCase();
     cache.put(key, id, 300);
   }
   return id;
 } catch (e) {
   return "NA";
 }
}

function ACTION(module, action, status, message = "", ctx = null) {
 try {
   const cache = CacheService.getScriptCache();
   const key = "action_buf";
   const existing = cache.get(key);
   const arr = existing ? JSON.parse(existing) : [];
   arr.push([
     nowIST(),
     String(module || ""),
     String(action || ""),
     String(status || ""),
     String(message || "").substring(0, 300),
     JSON.stringify(ctx || {}).substring(0, 900),
     todayIST(),
     _actionExecId_(),
   ]);
   cache.put(key, JSON.stringify(arr), 300);
   if (arr.length >= 12) _flushActionLogs_();
 } catch (e) {}
}

function _flushActionLogs_() {
 const cache = CacheService.getScriptCache();
 const key = "action_buf";
 const existing = cache.get(key);
 if (!existing) return;

 const rows = JSON.parse(existing) || [];
 if (!rows.length) return;

 let sheet = SS.getSheetByName(SH.ACTIONS);
 if (!sheet) {
   try { ensureCoreSheets_(); } catch (e) {}
   sheet = SS.getSheetByName(SH.ACTIONS);
 }
 if (!sheet) return;

 sheet.getRange(sheet.getLastRow() + 1, 1, rows.length, rows[0].length).setValues(rows);
 cache.remove(key);

 const maxRows = 3000;
 const lr = sheet.getLastRow();
 if (lr > maxRows) {
   const deleteCount = lr - maxRows;
   sheet.deleteRows(5, deleteCount);
 }
}

function PROJECT_LOG_STATUS() {
 const sh = SS.getSheetByName(SH.ACTIONS);
 const rows = sh ? Math.max(0, sh.getLastRow() - 3) : 0;
 let buffered = 0;
 try {
   const raw = CacheService.getScriptCache().get("action_buf");
   buffered = raw ? (JSON.parse(raw) || []).length : 0;
 } catch (e) {}
 const out = { sheetRows: rows, buffered };
 LOG("INFO", "ActionLog", `status rows=${rows} buffered=${buffered}`);
 _flushLogs_();
 return out;
}


function ALERT(subject, body) {
 try {
   if (!CFG().emailAlerts) return;
   MailApp.sendEmail({
     to: Session.getActiveUser().getEmail(),
     subject: `TradingBot: ${String(subject).substring(0, 120)}`,
     body: `${body}\n\nTime: ${nowIST()}`,
   });
 } catch (e) {}
}


// --- Watchlist
function getWatchlist() {
 const data = SS.getSheetByName(SH.WATCHLIST).getDataRange().getValues();
 return data.slice(3)
   .filter(r => r[1] && String(r[8]).toUpperCase() === "Y")
   .map(r => ({
     symbol: String(r[1]).trim(),
     exchange: String(r[2] || "NSE").trim(),
     segment: String(r[3] || "CASH").trim(),
     product: String(r[4] || "CNC").trim(),
     strategy: String(r[5] || "SWING").trim(),
     sector: String(r[6] || "").trim(),
     beta: parseFloat(r[7]) || 1.0,
   }));
}

function ensureCoreSheets_() {
 const required = [SH.CONFIG, SH.WATCHLIST, SH.MARKET, SH.SCAN, SH.SIGNALS, SH.ORDERS, SH.POSITIONS, SH.PNL, SH.RISK, SH.LOGS];
 required.forEach(n => {
   if (!SS.getSheetByName(n)) throw new Error("Missing sheet: " + n);
 });
 if (!SS.getSheetByName(SH.UNIVERSE)) {
   const s = SS.insertSheet(SH.UNIVERSE);
   s.getRange(1, 1, 1, 21).setValues([["🧾 Universe Instruments — Master list for smart watchlist generation", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]]);
   s.getRange(3, 1, 1, 21).setValues([["#", "Symbol", "Exchange", "Segment", "Allowed Product", "Strategy", "Sector", "Beta", "Enabled", "Priority", "Notes", "Score", "RSI", "Vol Ratio", "Last Scanned", "Last Product", "Last Strategy", "Last Note", "Raw CSV (JSON)", "Sector Source", "Sector Updated At"]]);
 }
 if (!SS.getSheetByName(SH.CANDLE_CACHE)) {
   const s = SS.insertSheet(SH.CANDLE_CACHE);
   s.getRange(1, 1, 1, 13).setValues([["🗄️ Candle Cache — persistent historical OHLCV", "", "", "", "", "", "", "", "", "", "", "", ""]]);
   s.getRange(3, 1, 1, 13).setValues([["Symbol", "Exchange", "Segment", "Timeframe", "Timestamp", "Open", "High", "Low", "Close", "Volume", "Source", "Fetched At", "Raw Candle (JSON)"]]);
 }
 if (!SS.getSheetByName(SH.SCORE_CACHE_1D)) {
   const s = SS.insertSheet(SH.SCORE_CACHE_1D);
   s.getRange(1, 1, 1, 14).setValues([["📘 Score Cache 1D — universe daily candle cache index", "", "", "", "", "", "", "", "", "", "", "", "", ""]]);
   s.getRange(3, 1, 1, 14).setValues([["Symbol", "Exchange", "Segment", "Enabled", "Bars", "Last Candle Time", "Updated At", "Status", "Attempts", "Last Error", "File Name", "ISIN", "Notes", "File Id"]]);
 }
 if (!SS.getSheetByName(SH.SCORE_CACHE_1D_DATA)) {
   const s = SS.insertSheet(SH.SCORE_CACHE_1D_DATA);
   s.getRange(1, 1, 1, 9).setValues([["📗 Score Cache 1D Data — sheet-backed JSON blobs for Stage-A scoring", "", "", "", "", "", "", "", ""]]);
   s.getRange(3, 1, 1, 9).setValues([["Key", "Symbol", "Exchange", "Segment", "Bars", "Last Candle Time", "Updated At", "Blob JSON", "Blob Chars"]]);
   try { s.hideSheet(); } catch (e) {}
 }
 if (!SS.getSheetByName(SH.BACKFILL)) {
   const s = SS.insertSheet(SH.BACKFILL);
   s.getRange(1, 1, 1, 14).setValues([["📚 History Backfill Queue — status of universe history creation", "", "", "", "", "", "", "", "", "", "", "", "", ""]]);
   s.getRange(3, 1, 1, 14).setValues([["Symbol", "Exchange", "Segment", "Timeframe", "Enabled", "Last Candle Time", "Bars Saved", "Status", "Attempts", "Last Error", "Updated At", "File Name", "ISIN", "Sector"]]);
 }
 if (!SS.getSheetByName(SH.DECISIONS)) {
   const s = SS.insertSheet(SH.DECISIONS);
   s.getRange(1, 1, 1, 7).setValues([["🧠 Decision Log — every trading decision", "", "", "", "", "", ""]]);
   s.getRange(3, 1, 1, 7).setValues([["Timestamp", "Stage", "Symbol", "Decision", "Reason", "Context", "Run Date"]]);
 }
 if (!SS.getSheetByName(SH.ACTIONS)) {
   const s = SS.insertSheet(SH.ACTIONS);
   s.getRange(1, 1, 1, 8).setValues([["🧩 Project Log — technical action trace", "", "", "", "", "", "", ""]]);
   s.getRange(3, 1, 1, 8).setValues([["Timestamp", "Module", "Action", "Status", "Message", "Context", "Run Date", "Exec ID"]]);
 }
}


// --- Cost model (approx, cash segment)
function calcBrokerage(qty, price) {
 const to = qty * price;
 const brk = Math.min(20, to * 0.0005);
 const stt = to * 0.00025;
 const nse = to * 0.0000322;
 const gst = (brk + nse) * 0.18;
 const seb = to * 0.000001;
 return parseFloat(((brk + stt + nse + gst + seb) * 2).toFixed(2));
}


function makeRefId() {
 const ts = Date.now().toString(36).toUpperCase().slice(-6);
 const rand = Math.random().toString(36).substring(2, 5).toUpperCase();
 return `GR-${ts}-${rand}`;
}
