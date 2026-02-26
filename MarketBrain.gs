/************************************
* MarketBrain.gs — Market data + regime classification (REFINED)
************************************/


const IDX_NIFTY = "NIFTY"; // Groww uses "NIFTY" for index quotes in CASH segment. citeturn0search5


// --- Parse OHLC sometimes returned as string
function parseOHLC(raw) {
 if (!raw) return { open: 0, high: 0, low: 0, close: 0 };
 if (typeof raw === "object") return raw;
 try {
   const get = key => {
     const m = String(raw).match(new RegExp(key + "\\s*:\\s*([\\d.]+)"));
     return m ? parseFloat(m[1]) : 0;
   };
   return { open: get("open"), high: get("high"), low: get("low"), close: get("close") };
 } catch (e) {
   return { open: 0, high: 0, low: 0, close: 0 };
 }
}


// --- Live Quote
function getLiveQuote(symbol, exchange = "NSE", segment = "CASH") {
 const p = growwGET(
   `live-data/quote?exchange=${encodeURIComponent(exchange)}&segment=${encodeURIComponent(segment)}&trading_symbol=${encodeURIComponent(symbol)}`
 );
 if (!p) return null;
 const ohlc = parseOHLC(p.ohlc);
 const ltp = p.last_price || p.ltp || 0;
 return {
   ltp,
   open: ohlc.open || 0,
   high: ohlc.high || 0,
   low: ohlc.low || 0,
   close: ohlc.close || ltp,
   volume: p.volume || 0,
   changePct: p.day_change_perc || 0,
   change: p.day_change || 0,
   bid: p.depth?.buy?.[0]?.price || p.bid_price || 0,
   ask: p.depth?.sell?.[0]?.price || p.offer_price || 0,
 };
}


// --- Historical candles (lookbackDays ensures enough candles for indicators)
function _candleIntervalStr_(tfStr) {
  // tfStr like "1m","5m","15m","1h","1d"
  const s = String(tfStr || "15m").toLowerCase().trim();
  if (s === "1m" || s === "1min") return "1minute";
  if (s === "2m") return "2minute";
  if (s === "3m") return "3minute";
  if (s === "5m") return "5minute";
  if (s === "10m") return "10minute";
  if (s === "15m") return "15minute";
  if (s === "30m") return "30minute";
  if (s === "60m" || s === "1h") return "60minute";
  if (s === "240m" || s === "4h") return "240minute";
  if (s === "1d" || s === "day") return "1day";
  if (s === "1w" || s === "week") return "1week";
  if (s === "1mo" || s === "1mth" || s === "month") return "1month";
  return "15minute";
}

function _growwSymbol_(exchange, tradingSymbol) {
  const ex = String(exchange || "NSE").toUpperCase();
  const sym = String(tradingSymbol || "").toUpperCase();
  try {
    const exact = _growwSymbolFromUniverseRaw_(ex, sym);
    if (exact) return exact;
  } catch (e) {}
  return ex + "-" + sym;
}

let _growwSymbolUniverseMemo_ = null;

function _growwSymbolFromUniverseRaw_(exchange, tradingSymbol) {
  if (!exchange || !tradingSymbol || typeof SS === "undefined" || typeof SH === "undefined" || !SH.UNIVERSE) return "";
  if (!_growwSymbolUniverseMemo_) {
    _growwSymbolUniverseMemo_ = {};
    try {
      const sh = SS.getSheetByName(SH.UNIVERSE);
      if (!sh) return "";
      const data = sh.getDataRange().getValues().slice(3);
      const rawCol = (typeof UNIVERSE_RAW_COL !== "undefined" && UNIVERSE_RAW_COL > 0) ? (UNIVERSE_RAW_COL - 1) : 18;
      for (let i = 0; i < data.length; i++) {
        const r = data[i];
        const sym = String(r[1] || "").trim().toUpperCase();
        if (!sym) continue;
        const ex = String(r[2] || "NSE").trim().toUpperCase();
        const rawTxt = String(r[rawCol] || "").trim();
        if (!rawTxt || rawTxt[0] !== "{") continue;
        try {
          const raw = JSON.parse(rawTxt);
          let g = String(
            raw.groww_symbol ||
            raw.growwsymbol ||
            raw.groww_trading_symbol ||
            raw.groww_instrument_symbol ||
            ""
          ).trim();
          if (!g) {
            const keys = Object.keys(raw || {});
            for (let k = 0; k < keys.length; k++) {
              const key = String(keys[k] || "").toLowerCase();
              const val = String(raw[keys[k]] || "").trim();
              if (!val) continue;
              // Prefer explicit groww-symbol style fields.
              if (key.indexOf("groww") >= 0 && key.indexOf("symbol") >= 0 && /^[A-Z]+-.+$/i.test(val)) {
                g = val;
                break;
              }
            }
          }
          if (!g) {
            const keys = Object.keys(raw || {});
            for (let k = 0; k < keys.length; k++) {
              const val = String(raw[keys[k]] || "").trim();
              if (!val) continue;
              // Last-resort pattern match for values like NSE-RELIANCE / BSE-500325.
              if (/^(NSE|BSE)-[A-Z0-9._-]+$/i.test(val)) {
                g = val;
                break;
              }
            }
          }
          if (g) _growwSymbolUniverseMemo_[`${ex}|${sym}`] = g.toUpperCase();
        } catch (e2) {}
      }
    } catch (e) {}
  }
  return String(_growwSymbolUniverseMemo_[`${String(exchange).toUpperCase()}|${String(tradingSymbol).toUpperCase()}`] || "");
}

function _fallbackIntervalMinutes_(intervalMinsOrTf) {
  if (typeof intervalMinsOrTf === "number") {
    const n = Math.floor(Number(intervalMinsOrTf));
    return isFinite(n) && n > 0 ? n : 15;
  }
  const s = String(intervalMinsOrTf || "15").toLowerCase().trim();
  if (s === "1m" || s === "1min" || s === "1minute") return 1;
  if (s === "2m" || s === "2minute") return 2;
  if (s === "3m" || s === "3minute") return 3;
  if (s === "5m" || s === "5minute") return 5;
  if (s === "10m" || s === "10minute") return 10;
  if (s === "15m" || s === "15minute") return 15;
  if (s === "30m" || s === "30minute") return 30;
  if (s === "60m" || s === "1h" || s === "60minute") return 60;
  if (s === "240m" || s === "4h" || s === "240minute") return 240;
  if (s === "1d" || s === "day" || s === "1day") return 1440;
  const n = parseInt(s.replace(/[^\d]/g, ""), 10);
  return isFinite(n) && n > 0 ? n : 15;
}

function fetchCandlesRangeDirect_(symbol, exchange, segment, intervalMinsOrTf, start, end, opts) {
  const o = (opts && typeof opts === "object") ? opts : {};
  const allowDeprecatedFallback = o.allowDeprecatedFallback !== false;
  const meta = (o.meta && typeof o.meta === "object") ? o.meta : null;
  const from = Utilities.formatDate(new Date(start), "Asia/Kolkata", "yyyy-MM-dd HH:mm:ss");
  const to = Utilities.formatDate(new Date(end), "Asia/Kolkata", "yyyy-MM-dd HH:mm:ss");
  const tfStr = (typeof intervalMinsOrTf === "string") ? intervalMinsOrTf : (String(intervalMinsOrTf) + "m");
  const candleInterval = _candleIntervalStr_(tfStr);
  const growwSymbol = _growwSymbol_(exchange, symbol);
  const fallbackIntervalMins = _fallbackIntervalMinutes_(intervalMinsOrTf);

  let p = growwGET(
    `historical/candles?exchange=${encodeURIComponent(exchange)}` +
    `&segment=${encodeURIComponent(segment)}` +
    `&groww_symbol=${encodeURIComponent(growwSymbol)}` +
    `&start_time=${encodeURIComponent(from)}` +
    `&end_time=${encodeURIComponent(to)}` +
    `&candle_interval=${encodeURIComponent(candleInterval)}`
  );
  if (meta) {
    meta.primaryTried = true;
    meta.primaryGrowwSymbol = growwSymbol;
    meta.source = p ? "historical/candles" : "";
  }

  // Backward-compat fallback (deprecated historical range endpoint).
  if (!p && allowDeprecatedFallback) {
    p = growwGET(
      `historical/candle/range?exchange=${encodeURIComponent(exchange)}` +
      `&segment=${encodeURIComponent(segment)}` +
      `&trading_symbol=${encodeURIComponent(symbol)}` +
      `&start_time=${encodeURIComponent(from)}` +
      `&end_time=${encodeURIComponent(to)}` +
      `&interval_in_minutes=${encodeURIComponent(String(fallbackIntervalMins))}`
    );
    if (meta && p) meta.source = "historical/candle/range";
  }

  const candles = (p && (p.payload && p.payload.candles)) || p.candles || [];
  if (meta) {
    meta.count = Array.isArray(candles) ? candles.length : 0;
    meta.usedDeprecatedFallback = meta.source === "historical/candle/range";
  }
  // Expected candle format: [timestamp, open, high, low, close, volume]
  return Array.isArray(candles) ? candles.filter(r => r && r.length >= 6) : [];
}

// Direct API fetch only (used by cache sync/backfill internals).
function fetchCandlesDirect_(symbol, exchange, segment, intervalMinsOrTf = 15, lookbackDays = 8) {
  const end = new Date();
  const start = new Date(end.getTime() - Number(lookbackDays) * 24 * 3600 * 1000);
  return fetchCandlesRangeDirect_(symbol, exchange, segment, intervalMinsOrTf, start, end);
}

// Cache-first candles for scanner/model use.
function getCandles(symbol, exchange, segment, intervalMinsOrTf = 15, lookbackDays = 8) {
  const tfStr = (typeof intervalMinsOrTf === "string") ? intervalMinsOrTf : (String(intervalMinsOrTf) + "m");
  const needed = (typeof _barsNeeded_ === "function") ? _barsNeeded_(tfStr, lookbackDays) : 80;
  let cached = [];
  if (typeof getCachedCandles === "function") {
    cached = getCachedCandles(symbol, exchange, segment, tfStr, needed);
  }
  if (cached.length >= needed) return cached;

  const apiCandles = fetchCandlesDirect_(symbol, exchange, segment, tfStr, lookbackDays);
  if (apiCandles.length && typeof upsertCandlesToCache === "function") {
    upsertCandlesToCache(symbol, exchange, segment, tfStr, apiCandles, "groww_api");
    if (typeof getCachedCandles === "function") {
      const merged = getCachedCandles(symbol, exchange, segment, tfStr, needed);
      if (merged.length) return merged;
    }
  }
  return apiCandles;
}

// ---- Compatibility wrappers for TestHarness ----
function fetchQuote_(inst) {
  const symbol = inst.tradingSymbol || inst.symbol || inst.growwSymbol || "NIFTY";
  const exchange = inst.exchange || "NSE";
  const segment = inst.segment || "CASH";
  return getLiveQuote(symbol, exchange, segment);
}

function getIndexQuote_(indexSymbol) {
  return fetchQuote_({ tradingSymbol: String(indexSymbol || "NIFTY").toUpperCase(), exchange: "NSE", segment: "CASH" });
}

function fetchCandles_(symbol, tfStr, lookbackDays) {
  return getCandles(symbol, "NSE", "CASH", tfStr || "15m", Number(lookbackDays || 8));
}



// --- VIX (best-effort; avoid blocking on NSE)
function fetchVIX() {
 try {
   const r = UrlFetchApp.fetch(
     "https://query1.finance.yahoo.com/v8/finance/chart/%5EINDIAVIX?interval=1d&range=1d",
     { muteHttpExceptions: true }
   );
   if (r.getResponseCode() === 200) {
     const d = JSON.parse(r.getContentText());
     const px = d.chart?.result?.[0]?.meta?.regularMarketPrice;
     if (px) return px;
   }
 } catch (e) {}


 // NSE fallback
 try {
   const r2 = UrlFetchApp.fetch("https://www.nseindia.com/api/allIndices", {
     muteHttpExceptions: true,
     headers: { "User-Agent": "Mozilla/5.0", Accept: "application/json", Referer: "https://www.nseindia.com/" },
   });
   if (r2.getResponseCode() !== 200) throw new Error("non-200");
   const d2 = JSON.parse(r2.getContentText());
   const vix = d2.data?.find(i => i.index === "INDIA VIX");
   return vix?.last || 15;
 } catch (e2) {
   return 15;
 }
}


// --- PCR (best-effort)
function fetchPCR() {
 try {
   const r = UrlFetchApp.fetch("https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY", {
     muteHttpExceptions: true,
     headers: { "User-Agent": "Mozilla/5.0", Accept: "application/json", Referer: "https://www.nseindia.com/" },
   });
   if (r.getResponseCode() !== 200) throw new Error("non-200");
   const d = JSON.parse(r.getContentText());
   let callOI = 0, putOI = 0;
   const oiByStrike = {};
   (d.records?.data || []).forEach(row => {
     if (row.CE) {
       callOI += row.CE.openInterest || 0;
       oiByStrike[row.strikePrice] = (oiByStrike[row.strikePrice] || 0) + (row.CE.openInterest || 0);
     }
     if (row.PE) {
       putOI += row.PE.openInterest || 0;
       oiByStrike[row.strikePrice] = (oiByStrike[row.strikePrice] || 0) + (row.PE.openInterest || 0);
     }
   });
   const maxPain = Object.entries(oiByStrike).sort((a, b) => b[1] - a[1])[0]?.[0] || 0;
   return { pcr: parseFloat((callOI > 0 ? putOI / callOI : 1).toFixed(2)), maxPain: parseFloat(maxPain), callOI, putOI };
 } catch (e) {
   return { pcr: 1.0, maxPain: 0, callOI: 0, putOI: 0 };
 }
}


// --- FII/DII (best-effort)
function fetchFIIDII() {
 try {
   const r = UrlFetchApp.fetch("https://www.nseindia.com/api/fiidiiTradeReact", {
     muteHttpExceptions: true,
     headers: { "User-Agent": "Mozilla/5.0", Accept: "application/json", Referer: "https://www.nseindia.com/" },
   });
   if (r.getResponseCode() !== 200) throw new Error("non-200");
   const d = JSON.parse(r.getContentText());
   const t = d[0] || {};
   return { fii: parseFloat(t.netVal || t.NET_BUY_SELL || 0), dii: parseFloat(t.diiNetVal || 0) };
 } catch (e) {
   return { fii: 0, dii: 0 };
 }
}


// --- Market regime classifier (simple but deterministic)
function getMarketRegime() {
 const cfg = CFG();
 const vix = fetchVIX();
 const nifty = getLiveQuote(IDX_NIFTY, "NSE", "CASH") || { ltp: 0, changePct: 0, open: 0, high: 0, low: 0 };
 const pcr = fetchPCR();
 const fii = fetchFIIDII();
 const niftyC = getCandles(IDX_NIFTY, "NSE", "CASH", 15, 8);


 let regime = "TREND", bias = "NEUTRAL";


 if (vix > cfg.vixSafeMax || Math.abs(nifty.changePct) > 2.5) {
   regime = "AVOID";
 } else if (niftyC.length >= 40) {
   const closes = niftyC.map(c => c[4]);
   const e9 = calcEMA(closes, 9);
   const e21 = calcEMA(closes, 21);
   const n = closes.length - 1;
   regime = Math.abs(e9[n] - e21[n]) / (closes[n] || 1) * 100 > 0.2 ? "TREND" : "RANGE";
 }


 const niftyBull = nifty.changePct > cfg.niftyTrendPct;
 const niftyBear = nifty.changePct < -cfg.niftyTrendPct;
 const pcrBull = pcr.pcr >= cfg.pcrBullMin;
 const pcrBear = pcr.pcr <= cfg.pcrBearMax;
 const fiiBull = fii.fii > 0;


 if (niftyBull && pcrBull) bias = "BULLISH";
 else if (niftyBear && pcrBear) bias = "BEARISH";
 else if (niftyBull || (pcrBull && fiiBull)) bias = "BULLISH";
 else if (niftyBear || (pcrBear && !fiiBull)) bias = "BEARISH";


 // Update sheet (best-effort)
 try {
   const sh = SS.getSheetByName(SH.MARKET);
   [
     ["B4", vix.toFixed(1)],
     ["B5", vix < cfg.vixTrendMax ? "✅ SAFE" : vix < cfg.vixSafeMax ? "⚠️ CAUTION" : "🛑 DANGER"],
     ["B6", nifty.ltp], ["B7", (nifty.changePct || 0).toFixed(2) + "%"],
     ["B8", niftyBull ? "BULLISH" : niftyBear ? "BEARISH" : "NEUTRAL"],
     ["B9", nifty.open], ["B10", nifty.high], ["B11", nifty.low],
     ["B12", pcr.pcr], ["B13", pcrBull ? "BULLISH" : pcrBear ? "BEARISH" : "NEUTRAL"],
     ["B14", pcr.maxPain], ["B15", pcr.callOI], ["B16", pcr.putOI],
     ["B17", fii.fii], ["B18", fii.dii], ["B19", (fii.fii + fii.dii).toFixed(0)],
     ["B20", regime], ["B21", bias], ["B23", nowIST()],
   ].forEach(([cell, val]) => sh.getRange(cell).setValue(val));
 } catch (e) {
   LOG("WARN", "MarketBrain", "Sheet write: " + e.toString());
 }


 LOG("INFO", "MarketBrain", `VIX=${vix} Nifty=${(nifty.changePct || 0).toFixed(2)}% Regime=${regime} Bias=${bias}`);
 return { vix, nifty, pcr, fii, regime, bias };
}

function computeMarketRegime_() {
 return getMarketRegime();
}
