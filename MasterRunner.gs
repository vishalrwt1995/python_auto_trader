/************************************
* MasterRunner.gs — Main loop + triggers + health checks
************************************/

const WATCHLIST_SCAN_BATCH_PROP = "WATCHLIST_SCAN_BATCH";
const WATCHLIST_SCAN_CORE_PROP = "WATCHLIST_SCAN_CORE";
const WATCHLIST_SCAN_CURSOR_PROP = "runtime:watchlist_scan_cursor";
const DEFAULT_WATCHLIST_SCAN_BATCH = 25;
const DEFAULT_WATCHLIST_SCAN_CORE = 10;


function systemHealthCheck() {
 // Minimal checks (no secrets in logs)
 ensureCoreSheets_();
 const cfg = CFG();
 const missing = [];
 Object.values(SH).forEach(n => { if (!SS.getSheetByName(n)) missing.push(n); });
 if (missing.length) throw new Error("Missing sheets: " + missing.join(", "));


 if (!cfg.apiKey) LOG("WARN", "Health", "API Key missing in Config");
 if (!cfg.accessToken) LOG("WARN", "Health", "Access Token missing in Config");
 if (!cfg.apiSecret) LOG("WARN", "Health", "Refresh Token/Secret missing in Config");


 _flushLogs_();
 return true;
}

function _sliceWatchlistForScan_(wlAll) {
 const total = Array.isArray(wlAll) ? wlAll.length : 0;
 if (!total) return { list: [], total: 0, scanned: 0, core: 0, rotated: 0, nextCursor: 0, wrapped: true };

 const props = PropertiesService.getScriptProperties();
 const batchRaw = Number(props.getProperty(WATCHLIST_SCAN_BATCH_PROP) || 0);
 const coreRaw = Number(props.getProperty(WATCHLIST_SCAN_CORE_PROP) || 0);
 const batchSize = isFinite(batchRaw) && batchRaw >= 5 ? Math.floor(batchRaw) : DEFAULT_WATCHLIST_SCAN_BATCH;
 const coreSize = Math.min(total, isFinite(coreRaw) && coreRaw >= 0 ? Math.floor(coreRaw) : DEFAULT_WATCHLIST_SCAN_CORE);

 if (total <= coreSize + batchSize) {
   props.setProperty(WATCHLIST_SCAN_CURSOR_PROP, "0");
   return {
     list: wlAll.slice(),
     total,
     scanned: total,
     core: coreSize,
     rotated: Math.max(0, total - coreSize),
     nextCursor: 0,
     wrapped: true,
   };
 }

 const core = wlAll.slice(0, coreSize);
 const rest = wlAll.slice(coreSize);
 const rTotal = rest.length;
 let cursor = Number(props.getProperty(WATCHLIST_SCAN_CURSOR_PROP) || 0);
 if (!isFinite(cursor) || cursor < 0 || cursor >= rTotal) cursor = 0;
 const end = Math.min(rTotal, cursor + batchSize);
 const wrapped = end >= rTotal;
 const rotated = rest.slice(cursor, end);
 const nextCursor = wrapped ? 0 : end;
 props.setProperty(WATCHLIST_SCAN_CURSOR_PROP, String(nextCursor));

 return {
   list: core.concat(rotated),
   total,
   scanned: core.length + rotated.length,
   core: core.length,
   rotated: rotated.length,
   nextCursor,
   wrapped,
 };
}


function runScanOnce() {
 const startedAt = Date.now();
 ACTION("MasterRunner", "runScanOnce", "START", "scan cycle start", {});
 try {
   // Fast pre-lock gate so overnight/premarket compute jobs are not blocked by scan trigger.
   if (!isMarketOpen()) {
     LOG("INFO", "Bot", "Market closed");
     ACTION("MasterRunner", "runScanOnce", "SKIP", "market closed (prelock)", { ms: Date.now() - startedAt });
     _flushLogs_();
     _flushActionLogs_();
     return;
   }
 } catch (e) {
   // If the gate check fails, continue to the locked path and let normal error handling report it.
 }
 const lock = LockService.getScriptLock();
 if (!lock.tryLock(25000)) {
   ACTION("MasterRunner", "runScanOnce", "SKIP", "lock busy", { ms: Date.now() - startedAt });
   return;
 }


 try {
   systemHealthCheck();
   if (typeof reconcilePendingEntryOrders === "function") {
     const recon = reconcilePendingEntryOrders(15);
     if ((recon.filled || 0) > 0 || (recon.failed || 0) > 0) {
       LOG("INFO", "OrderRecon", `Pending entries reconciled: filled=${recon.filled || 0}, failed=${recon.failed || 0}, pending=${recon.pending || 0}`);
     }
   }
   if (typeof reconcilePendingExitOrders === "function") {
     const reconExit = reconcilePendingExitOrders(15);
     if ((reconExit.filled || 0) > 0 || (reconExit.failed || 0) > 0) {
       LOG("INFO", "OrderRecon", `Pending exits reconciled: filled=${reconExit.filled || 0}, failed=${reconExit.failed || 0}, pending=${reconExit.pending || 0}`);
     }
   }


   if (!isMarketOpen()) {
     LOG("INFO", "Bot", "Market closed");
     ACTION("MasterRunner", "runScanOnce", "SKIP", "market closed", { ms: Date.now() - startedAt });
     return;
   }


   const regime = getMarketRegime();
   DECISION("REGIME", "NIFTY", regime.regime, `bias=${regime.bias}`, { vix: regime.vix, pcr: regime.pcr?.pcr });
   if (regime.regime === "AVOID") { LOG("WARN", "Bot", "Regime AVOID — no entries"); }


   // trailing always runs (risk management)
   manageTrailingSL();


   if (!isEntryWindowOpen()) {
     LOG("INFO", "Bot", "Entry window closed");
     ACTION("MasterRunner", "runScanOnce", "SKIP", "entry window closed", { ms: Date.now() - startedAt });
     return;
   }


   if (!isSystemActive()) {
     LOG("INFO", "Bot", "Risk gate blocked");
     ACTION("MasterRunner", "runScanOnce", "SKIP", "risk gate blocked", { ms: Date.now() - startedAt });
     return;
   }

   if (typeof isWatchlistReadyForTrading === "function" && !isWatchlistReadyForTrading()) {
     let info = {};
     try {
       if (typeof getWatchlistReadinessStatus === "function") info = getWatchlistReadinessStatus(true);
     } catch (e) {}
     const covStr = (info && isFinite(info.scored) && isFinite(info.total))
       ? `${info.scored}/${info.total}`
       : "pending";
     LOG("WARN", "Bot", `Trading blocked until full-universe precompute completes (coverage=${covStr})`);
     ACTION("MasterRunner", "runScanOnce", "SKIP", "watchlist precompute gate", {
       ms: Date.now() - startedAt,
       ready: !!(info && info.ready),
       coveragePct: Number(info && info.coverageLive || 0),
       scored: Number(info && info.scored || 0),
       total: Number(info && info.total || 0),
     });
     return;
   }


   const wlAll = getWatchlist();
   const scanWin = _sliceWatchlistForScan_(wlAll);
   const wl = scanWin.list;
   if (!wl.length) {
     LOG("WARN", "Bot", "Watchlist empty");
     ACTION("MasterRunner", "runScanOnce", "SKIP", "watchlist empty", { ms: Date.now() - startedAt });
     return;
   }
   const scanSheet = SS.getSheetByName(SH.SCAN);


   // Clear scan area (rows 4+)
   const lr = scanSheet.getLastRow();
   if (lr > 4) scanSheet.getRange(4, 1, lr - 3, scanSheet.getLastColumn()).clearContent();


   const outRows = [];
   for (const w of wl) {
     if (!isSystemActive()) break; // re-check risk inside loop


     // Candles + indicators (15m default)
     const candles = getCandles(w.symbol, w.exchange, w.segment, 15, 8);
     const ind = computeIndicators(candles);
     if (!ind) {
       LOG("WARN", "Scan", `${w.symbol}: insufficient candles`);
       DECISION("SCAN", w.symbol, "SKIP", "insufficient_candles", { candles: candles.length });
       continue;
     }


     const dir = determineDirection(ind, regime);
     const meta = scoreSignal(w.symbol, dir, ind, regime);


     const ltp = ind.close;
     const atr = ind.atr;
     const pos = calcPositionSize(ltp, atr, dir);


	   // Write scan output
	     const changePct = ind.prevClose ? (((ltp - ind.prevClose) / ind.prevClose) * 100) : 0;
	     const emaState = ind.emaStack ? "BULL_STACK" : (ind.emaFlip ? "BEAR_STACK" : "MIXED");
	     const macdView = ind.macd.crossed || (ind.macd.hist >= 0 ? "POS" : "NEG");
	     outRows.push([
	       w.symbol,
	       Number(ltp.toFixed(2)),
	       Number(changePct.toFixed(2)),
	       Number(ind.volume.curr || 0),
	       Number((ind.volume.ratio || 0).toFixed(2)),
	       dir,
	       Number(meta.score || 0),
	       `${regime.regime}|${regime.bias}`,
	       Number((meta.breakdown && meta.breakdown.options) || 0),
	       Number((meta.breakdown && meta.breakdown.technical) || 0),
	       Number((meta.breakdown && meta.breakdown.volume) || 0),
	       emaState,
	       Number((ind.rsi && ind.rsi.curr || 0).toFixed(1)),
	       macdView,
	       (ind.supertrend.dir === 1 ? "UP" : "DOWN"),
	     ]);


     // Fire signal
     if (dir !== "HOLD" && meta.score >= CFG().minScore && !isAlreadyInStock(w.symbol) && regime.regime !== "AVOID") {
       const reason = `Score=${meta.score} RSI=${ind.rsi.curr.toFixed(1)} VolR=${ind.volume.ratio.toFixed(2)} Reg=${regime.bias}`;
       DECISION("SIGNAL", w.symbol, dir, "entry_qualified", { score: meta.score, reason });
	       _appendRowSafe_(SH.SIGNALS, [
	         nowIST(), w.symbol, dir, meta.score,
	         ltp.toFixed(2), pos.slPrice.toFixed(2), pos.target.toFixed(2),
	         pos.qty, pos.maxLoss.toFixed(2), pos.maxGain.toFixed(2),
	         w.strategy, regime.regime, regime.bias, "QUALIFIED"
	       ]);


       placeOrder({ symbol: w.symbol, exchange: w.exchange, segment: w.segment, direction: dir, product: w.product }, { ...pos, atr }, { score: meta.score, reason }, regime);
     } else {
       const why = dir === "HOLD"
         ? "direction_hold"
         : (meta.score < CFG().minScore ? "score_below_min"
         : (isAlreadyInStock(w.symbol) ? "already_in_position"
         : (regime.regime === "AVOID" ? "regime_avoid" : "blocked")));
       DECISION("SIGNAL", w.symbol, dir, why, { score: meta.score, min: CFG().minScore });
     }


     Utilities.sleep(120);
   }


   if (outRows.length) {
     // Ensure columns exist; write from row 4 col1
     scanSheet.getRange(4, 1, outRows.length, outRows[0].length).setValues(outRows);
   }


   LOG("INFO", "Bot", `Scan complete. rows=${outRows.length}, scanned=${scanWin.scanned}/${scanWin.total}, core=${scanWin.core}, rotated=${scanWin.rotated}, nextCursor=${scanWin.nextCursor}`);
   ACTION("MasterRunner", "runScanOnce", "DONE", "scan complete", {
     ms: Date.now() - startedAt,
     rows: outRows.length,
     scanned: scanWin.scanned,
     total: scanWin.total,
   });
 } catch (e) {
   LOG("ERR", "Bot", e.toString());
   ALERT("Runtime error", e.toString());
   ACTION("MasterRunner", "runScanOnce", "ERROR", String(e), { ms: Date.now() - startedAt });
 } finally {
   _flushLogs_();
   _flushDecisionLogs_();
   _flushActionLogs_();
   lock.releaseLock();
 }
}


// --- Daily reset (keep paper/demo safe)
function dailyReset() {
 const startedAt = Date.now();
 ACTION("MasterRunner", "dailyReset", "START", "", {});
 const sh = SS.getSheetByName(SH.RISK);
 sh.appendRow([new Date(), 0, 0, "ACTIVE", "", "", "", "", "", ""]);
 _clearRuntimeProps_();
 LOG("INFO", "Reset", "Daily reset done");
 _flushLogs_();
 ACTION("MasterRunner", "dailyReset", "DONE", "daily reset complete", { ms: Date.now() - startedAt });
 _flushActionLogs_();
}

function _clearRuntimeProps_() {
 const props = PropertiesService.getScriptProperties();
 const keys = props.getKeys();
 keys
   .filter(k => /^fired:/.test(k) || /^runtime:/.test(k) || /^pending_entry:/.test(k) || /^pending_exit:/.test(k))
   .forEach(k => props.deleteProperty(k));
}


// --- Triggers
function TRIGGER_SYNC_UNIVERSE_DAILY() {
  const out = (typeof syncUniverseFromGrowwInstrumentsDaily === "function")
    ? syncUniverseFromGrowwInstrumentsDaily(0)
    : syncUniverseFromGrowwInstruments(0, true);
  try {
    if (typeof initUniverseScoreCache1D === "function") initUniverseScoreCache1D(false);
  } catch (e) {
    LOG("WARN", "Trigger", "TRIGGER_SYNC_UNIVERSE_DAILY score-cache-index sync failed: " + String(e));
  }
  return out;
}
function TRIGGER_INIT_SCORE_CACHE_1D_DAILY() {
  if (typeof initUniverseScoreCache1D === "function") return initUniverseScoreCache1D(true);
  return null;
}
function TRIGGER_INIT_HISTORY_BACKFILL_DAILY() { return initUniverseHistoryBackfill("1d", false); }

function TRIGGER_PREMARKET_PRECOMPUTE() {
  try {
    const ready = (typeof isWatchlistReadyForTrading === "function") ? !!isWatchlistReadyForTrading() : false;
    // If watchlist is not ready by market open, continue catch-up scoring during market hours.
    if (!ready && typeof _isWeekdayIst_ === "function" && _isWeekdayIst_() && typeof isMarketOpen === "function" && isMarketOpen()) {
      return precomputeUniverseScoringAndFinalizeWatchlist(0, 0, true);
    }
  } catch (e) {
    LOG("WARN", "Trigger", "TRIGGER_PREMARKET_PRECOMPUTE fallback: " + String(e));
  }
  return precomputeUniverseScoringAndFinalizeWatchlist();
}

function TRIGGER_PROCESS_HISTORY_BACKFILL_BATCH() {
  try {
    const ready = (typeof isWatchlistReadyForTrading === "function") ? !!isWatchlistReadyForTrading() : false;
    const m = (typeof getISTMins === "function") ? getISTMins() : 0;
    const universePriorityWindow = (m >= 990 || m < 555); // 16:30..09:15 IST
    if (!ready && universePriorityWindow) {
      ACTION("MasterRunner", "TRIGGER_PROCESS_HISTORY_BACKFILL_BATCH", "SKIP", "prioritize universe pipeline", {
        istMins: m,
        watchlistReady: ready,
      });
      _flushActionLogs_();
      return { skipped: "prioritize_universe_pipeline", istMins: m };
    }
  } catch (e) {
    LOG("WARN", "Trigger", "TRIGGER_PROCESS_HISTORY_BACKFILL_BATCH priority-check failed: " + String(e));
  }
  try {
    if (typeof historyBackfillProgress === "function") {
      const p = historyBackfillProgress("1d");
      if (!(Number(p && p.total || 0) > 0) && typeof initUniverseHistoryBackfill === "function") {
        initUniverseHistoryBackfill("1d", false);
      }
    }
  } catch (e) {
    LOG("WARN", "Trigger", "TRIGGER_PROCESS_HISTORY_BACKFILL_BATCH init-check failed: " + String(e));
  }
  return processUniverseHistoryBackfillBatch();
}

function TRIGGER_PROCESS_SCORE_CACHE_1D_BATCH() {
  try {
    // Run only in post-market + overnight + premarket window (16:30 .. 05:15 IST).
    const m = (typeof getISTMins === "function") ? getISTMins() : 0;
    const inWindow = (m >= 990 || m < 315);
    if (!inWindow) {
      return { skipped: "outside_score_cache_window", istMins: m };
    }
    if (typeof universeScoreCache1DProgress === "function") {
      const p = universeScoreCache1DProgress();
      if (!(Number(p && p.total || 0) > 0) && typeof initUniverseScoreCache1D === "function") {
        initUniverseScoreCache1D(false);
      }
    }
  } catch (e) {
    LOG("WARN", "Trigger", "TRIGGER_PROCESS_SCORE_CACHE_1D_BATCH precheck failed: " + String(e));
  }
  return (typeof processUniverseScoreCache1DBatch === "function")
    ? processUniverseScoreCache1DBatch()
    : { skipped: "missing_process_fn" };
}

function TRIGGER_REFRESH_HISTORY_INCREMENTAL_DAILY() { return refreshUniverseHistoryIncremental(); }

function _triggerSpec_(handler, cfg) {
 return Object.assign({ handler: String(handler || "") }, cfg || {});
}

function _triggerSpecsByProfile_() {
 const LOG_FLUSH = [
   _triggerSpec_("_flushLogs_", { everyMinutes: 10 }),
   _triggerSpec_("_flushDecisionLogs_", { everyMinutes: 10 }),
   _triggerSpec_("_flushActionLogs_", { everyMinutes: 10 }),
 ];
 const UNIVERSE_SYNC = [
   _triggerSpec_("TRIGGER_SYNC_UNIVERSE_DAILY", { atHour: 5, nearMinute: 0, everyDays: 1 }),
 ];
 const SCORE_CACHE = [
   _triggerSpec_("TRIGGER_INIT_SCORE_CACHE_1D_DAILY", { atHour: 16, nearMinute: 30, everyDays: 1 }),
   _triggerSpec_("TRIGGER_PROCESS_SCORE_CACHE_1D_BATCH", { everyMinutes: 5 }),
 ];
 const SCORING = [
   _triggerSpec_("TRIGGER_PREMARKET_PRECOMPUTE", { everyMinutes: 5 }),
 ];
 const BACKFILL = [
   _triggerSpec_("TRIGGER_INIT_HISTORY_BACKFILL_DAILY", { atHour: 8, nearMinute: 25, everyDays: 1 }),
   _triggerSpec_("TRIGGER_PROCESS_HISTORY_BACKFILL_BATCH", { everyMinutes: 15 }),
   _triggerSpec_("TRIGGER_REFRESH_HISTORY_INCREMENTAL_DAILY", { atHour: 16, nearMinute: 25, everyDays: 1 }),
 ];
 const TRADING = [
   _triggerSpec_("runScanOnce", { everyMinutes: 5 }),
   _triggerSpec_("syncWatchlistCandleCacheIncremental", { everyMinutes: 15 }),
   _triggerSpec_("syncWatchlistCandleCacheFull", { atHour: 16, nearMinute: 10, everyDays: 1 }),
   _triggerSpec_("dailyReset", { atHour: 9, nearMinute: 5, everyDays: 1 }),
   _triggerSpec_("squareOffIntraday", { atHour: 15, nearMinute: 20, everyDays: 1 }),
 ];
 return {
   ALL: []
     .concat(TRADING)
     .concat(UNIVERSE_SYNC)
     .concat(SCORE_CACHE)
     .concat(SCORING)
     .concat(BACKFILL)
     .concat(LOG_FLUSH),
   UNIVERSE_ONLY: [].concat(UNIVERSE_SYNC).concat(LOG_FLUSH),
   CANDLE_DATA_ONLY: [].concat(SCORE_CACHE).concat(LOG_FLUSH), // Stage-A score-cache 1D
   SCORE_CACHE_ONLY: [].concat(SCORE_CACHE).concat(LOG_FLUSH), // alias
   SCORING_ONLY: [].concat(SCORING).concat(LOG_FLUSH),
   UNIVERSE_PIPELINE_ONLY: []
     .concat(UNIVERSE_SYNC)
     .concat(SCORE_CACHE)
     .concat(SCORING)
     .concat(LOG_FLUSH),
   TRADING_ONLY: [].concat(TRADING).concat(LOG_FLUSH),
   BACKFILL_ONLY: [].concat(BACKFILL).concat(LOG_FLUSH),
 };
}

function LIST_TRIGGER_PROFILES() {
 const map = _triggerSpecsByProfile_();
 const out = Object.keys(map).sort().map(k => ({
   profile: k,
   handlers: map[k].map(s => s.handler),
   count: map[k].length,
 }));
 out.forEach(r => LOG("INFO", "TriggerProfile", `${r.profile}: ${r.count} => ${r.handlers.join(", ")}`));
 _flushLogs_();
 return out;
}

function _installTimeTriggerSpec_(spec) {
 if (!spec || !spec.handler) throw new Error("Invalid trigger spec");
 let b = ScriptApp.newTrigger(spec.handler).timeBased();
 if (spec.everyMinutes) {
   b = b.everyMinutes(Number(spec.everyMinutes));
 } else {
   if (spec.atHour !== undefined && spec.atHour !== null) b = b.atHour(Number(spec.atHour));
   if (spec.nearMinute !== undefined && spec.nearMinute !== null) b = b.nearMinute(Number(spec.nearMinute));
   b = b.everyDays(Number(spec.everyDays) || 1);
 }
 b.create();
}

function _normalizeTriggerProfile_(profile) {
 const key = String(profile || "ALL").trim().toUpperCase();
 const map = _triggerSpecsByProfile_();
 if (!map[key]) throw new Error("Unknown trigger profile: " + key);
 return key;
}

function setupTriggers(profile = "ALL") {
 const p = _normalizeTriggerProfile_(profile);
 ACTION("MasterRunner", "setupTriggers", "START", "installing triggers", { profile: p });
 // Remove existing triggers to avoid duplicates
 ScriptApp.getProjectTriggers().forEach(t => ScriptApp.deleteTrigger(t));
 const specs = _triggerSpecsByProfile_()[p] || [];
 specs.forEach(_installTimeTriggerSpec_);
 LOG("INFO", "Setup", `✅ Triggers installed (profile=${p})`);
 _flushLogs_();
 ACTION("MasterRunner", "setupTriggers", "DONE", "triggers installed", { count: ScriptApp.getProjectTriggers().length, profile: p });
 _flushActionLogs_();
 return { profile: p, count: ScriptApp.getProjectTriggers().length };
}

function LIST_PROJECT_TRIGGERS() {
 const triggers = ScriptApp.getProjectTriggers();
 const rows = triggers.map((t, i) => ({
   index: i + 1,
   handler: String(t.getHandlerFunction() || ""),
   eventType: String(t.getEventType() || ""),
   source: String(t.getTriggerSource() || ""),
   id: String(t.getUniqueId() || ""),
 }));
 rows.forEach(r => LOG("INFO", "Trigger", `${r.index}. ${r.handler} | ${r.eventType} | ${r.source} | ${r.id}`));
 LOG("INFO", "Trigger", `Total active triggers=${rows.length}`);
 ACTION("MasterRunner", "LIST_PROJECT_TRIGGERS", "DONE", "trigger list projected", { count: rows.length });
 _flushLogs_();
 _flushActionLogs_();
 return rows;
}

function SET_ALL_PROJECT_TRIGGERS() {
 ACTION("MasterRunner", "SET_ALL_PROJECT_TRIGGERS", "START", "", { profile: "ALL" });
 setupTriggers("ALL");
 const out = LIST_PROJECT_TRIGGERS();
 ACTION("MasterRunner", "SET_ALL_PROJECT_TRIGGERS", "DONE", "trigger set complete", { count: out.length });
 _flushActionLogs_();
 return out;
}

function SET_TRIGGER_PROFILE(profile = "ALL") {
 const p = _normalizeTriggerProfile_(profile);
 ACTION("MasterRunner", "SET_TRIGGER_PROFILE", "START", "", { profile: p });
 setupTriggers(p);
 const out = LIST_PROJECT_TRIGGERS();
 ACTION("MasterRunner", "SET_TRIGGER_PROFILE", "DONE", "trigger profile set", { profile: p, count: out.length });
 _flushActionLogs_();
 return out;
}

function SET_UNIVERSE_ONLY_TRIGGERS() { return SET_TRIGGER_PROFILE("UNIVERSE_ONLY"); }
function SET_CANDLE_DATA_ONLY_TRIGGERS() { return SET_TRIGGER_PROFILE("CANDLE_DATA_ONLY"); }
function SET_SCORING_ONLY_TRIGGERS() { return SET_TRIGGER_PROFILE("SCORING_ONLY"); }
function SET_UNIVERSE_PIPELINE_ONLY_TRIGGERS() { return SET_TRIGGER_PROFILE("UNIVERSE_PIPELINE_ONLY"); }
function SET_TRADING_ONLY_TRIGGERS() { return SET_TRIGGER_PROFILE("TRADING_ONLY"); }
function SET_BACKFILL_ONLY_TRIGGERS() { return SET_TRIGGER_PROFILE("BACKFILL_ONLY"); }

function RUN_UNIVERSE_PIPELINE_STEP_NOW() {
 const startedAt = Date.now();
 ACTION("MasterRunner", "RUN_UNIVERSE_PIPELINE_STEP_NOW", "START", "", {});
 const out = {
   universeSync: null,
   scoreCacheProgressBefore: null,
   scoreCacheBatch: null,
   scoreCacheProgressAfter: null,
   scoring: null,
   coverage: null,
 };
 try {
   if (typeof syncUniverseFromGrowwInstrumentsDaily === "function") {
     out.universeSync = syncUniverseFromGrowwInstrumentsDaily(0);
   }
   if (typeof universeScoreCache1DProgress === "function") {
     out.scoreCacheProgressBefore = universeScoreCache1DProgress();
   }
   if (typeof processUniverseScoreCache1DBatch === "function") {
     out.scoreCacheBatch = processUniverseScoreCache1DBatch(0);
   }
   if (typeof universeScoreCache1DProgress === "function") {
     out.scoreCacheProgressAfter = universeScoreCache1DProgress();
   }
   if (typeof PREMARKET_PRECOMPUTE_NOW === "function") {
     out.scoring = PREMARKET_PRECOMPUTE_NOW();
   }
   if (typeof UNIVERSE_COVERAGE_STATUS === "function") {
     out.coverage = UNIVERSE_COVERAGE_STATUS();
   }
   ACTION("MasterRunner", "RUN_UNIVERSE_PIPELINE_STEP_NOW", "DONE", "universe pipeline step complete", {
     ms: Date.now() - startedAt,
     scoreCacheDone: Number(out.scoreCacheProgressAfter && out.scoreCacheProgressAfter.done || 0),
     scoreCacheTotal: Number(out.scoreCacheProgressAfter && out.scoreCacheProgressAfter.total || 0),
     todayCoveragePct: Number(out.coverage && out.coverage.todayCoveragePct || 0),
     ready: !!(out.coverage && out.coverage.ready),
   });
   _flushActionLogs_();
   return out;
 } catch (e) {
   ACTION("MasterRunner", "RUN_UNIVERSE_PIPELINE_STEP_NOW", "ERROR", String(e), { ms: Date.now() - startedAt });
   _flushActionLogs_();
   throw e;
 }
}


// Convenience manual run
function RUN_NOW() { runScanOnce(); }
