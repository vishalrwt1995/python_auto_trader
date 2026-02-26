/***********************
* Maintenance.gs
* Safe ops: clean, reset, restart
***********************/


// ====== CONFIG ======
const SYS = {
 REQUIRED_SHEETS: [
   '⚙️ Config', '📋 Watchlist', '🧾 Universe Instruments', '🗄️ Candle Cache', '📚 History Backfill',
   '🧠 Decision Log', '🧠 Market Brain', '📡 Live Scanner', '🎯 Signals',
   '📦 Orders', '💼 Positions', '🛡️ Risk Monitor', '📝 Logs'
 ],
 OPTIONAL_SHEETS: ['💰 P&L Tracker', '📜 Apps Script Code', '📘 README'],
 LOG_SHEET: '📝 Logs',
 MAX_LOG_ROWS: 2500,          // keep last N rows
 MAX_API_LOG_ROWS: 2500,      // if you have raw_api_log in future
 SAFE_CLEAR_SHEETS: ['🧠 Market Brain', '📡 Live Scanner', '🎯 Signals', '🛡️ Risk Monitor', '🧠 Decision Log'],
 NEVER_CLEAR_SHEETS: ['⚙️ Config', '📋 Watchlist', '🧾 Universe Instruments', '🗄️ Candle Cache', '📚 History Backfill', '📦 Orders', '💼 Positions', '💰 P&L Tracker', '📜 Apps Script Code'],
};


// ====== PUBLIC ENTRYPOINT ======


function HARD_RESTART_SYSTEM() {
 // Single “do-and-die” restart function:
 // 1) stop everything
 // 2) clean sheet + state
 // 3) set safe defaults
 // 4) rebuild triggers
 // 5) run health checks
 const lock = LockService.getScriptLock();
 lock.waitLock(30000);
 try {
   _logOps_('HARD_RESTART_SYSTEM: start');


   STOP_ALL_TRIGGERS();
   CLEAN_SHEET_MINIMAL();
   RESET_RUNTIME_STATE();
   ENSURE_SAFE_CONFIG_DEFAULTS();
   setupTriggers();     // from your refactor
   _logOps_('HARD_RESTART_SYSTEM: triggers created');


   const health = SYSTEM_HEALTHCHECK();
   _logOps_('HARD_RESTART_SYSTEM: health=' + JSON.stringify(health));
   if (!health.ok) throw new Error('Healthcheck failed: ' + health.reason);


   _logOps_('HARD_RESTART_SYSTEM: done ✅');
 } finally {
   lock.releaseLock();
 }
}


// ====== CLEANUP + RESET ======


function STOP_ALL_TRIGGERS() {
 const triggers = ScriptApp.getProjectTriggers();
 triggers.forEach(t => ScriptApp.deleteTrigger(t));
 _logOps_(`STOP_ALL_TRIGGERS: deleted=${triggers.length}`);
}


function CLEAN_SHEET_MINIMAL() {
 const ss = SpreadsheetApp.getActive();
 // 1) validate required sheets exist
 SYS.REQUIRED_SHEETS.forEach(name => {
   if (!ss.getSheetByName(name)) throw new Error(`Missing required sheet: ${name}`);
 });


 // 2) clear runtime sheets (keep headers + styling)
 SYS.SAFE_CLEAR_SHEETS.forEach(name => _clearDataKeepHeader_(name));


 // 3) trim logs
 TRIM_SHEET_KEEP_LAST_ROWS(SYS.LOG_SHEET, SYS.MAX_LOG_ROWS, 3);


 _logOps_('CLEAN_SHEET_MINIMAL: done');
}


function RESET_RUNTIME_STATE() {
 const props = PropertiesService.getScriptProperties();
 const keys = props.getKeys();


 // DO NOT delete secrets/config keys
 const KEEP_EXACT = new Set([
   'GROWW_API_KEY',
   'GROWW_API_SECRET',
   'GROWW_API_HOST',
   'GROWW_ACCESS_TOKEN',
   'GROWW_ACCESS_TOKEN_EXPIRY_ISO'
 ]);


 // Keep anything that looks like a credential/host/config as well
 function isSecretOrConfigKey_(k) {
   const u = String(k).toUpperCase();
   return (
     KEEP_EXACT.has(k) ||
     u.indexOf('SECRET') >= 0 ||
     u.indexOf('TOKEN') >= 0 ||     // keeps tokens too
     u.indexOf('API_KEY') >= 0 ||
     u.indexOf('API_HOST') >= 0 ||
     u.indexOf('HOST') === 0
   );
 }


 // Delete everything else that was created by the bot
 // (idempotency, daily counters, last run stamps, caches, etc.)
 const toDelete = keys.filter(k => !isSecretOrConfigKey_(k));


 toDelete.forEach(k => props.deleteProperty(k));
 _logOps_('RESET_RUNTIME_STATE: cleared=' + toDelete.length + ' keys');
}


function ENSURE_SAFE_CONFIG_DEFAULTS() {
 // Uses your Config label-based getter from Config.gs (assumes you have cfgGet / cfgSet or similar).
 // If you don’t, replace with direct sheet writes.
 try {
   // Force safety defaults
   cfgSet('Paper Trade Mode', true);
   cfgSet('Intraday Capital %', 0);
   cfgSet('Max Trades Per Day', 5);
   cfgSet('Max Open Positions', 3);
   cfgSet('Risk Per Trade (₹)', 125);
   cfgSet('Max Daily Loss (₹)', 300);
   cfgSet('Daily Profit Target (₹)', 200);
   PropertiesService.getScriptProperties().setProperty('WATCHLIST_TARGET_SIZE', '200');
   _logOps_('ENSURE_SAFE_CONFIG_DEFAULTS: applied');
 } catch (e) {
   // If cfgSet not present, do nothing but log
   _logOps_('ENSURE_SAFE_CONFIG_DEFAULTS: cfgSet not found, skipping: ' + e.message);
 }
}


// ====== HEALTHCHECK ======


function SYSTEM_HEALTHCHECK() {
 // Checks: sheet structure, config sanity, required script properties, time sanity
 const ss = SpreadsheetApp.getActive();
 for (const name of SYS.REQUIRED_SHEETS) {
   if (!ss.getSheetByName(name)) return { ok: false, reason: 'Missing sheet ' + name };
 }


 // Config checks
 const paper = _safeCfgGetBool_('Paper Trade Mode', true);
 const maxTrades = _safeCfgGetNum_('Max Trades Per Day', 5);
 const maxOpen = _safeCfgGetNum_('Max Open Positions', 3);
 const risk = _safeCfgGetNum_('Risk Per Trade (₹)', 125);
 const dailyLoss = _safeCfgGetNum_('Max Daily Loss (₹)', 300);
 const profitTarget = _safeCfgGetNum_('Daily Profit Target (₹)', 200);


 if (maxTrades > 5) return { ok: false, reason: 'Max Trades Per Day > 5' };
 if (maxOpen > 3) return { ok: false, reason: 'Max Open Positions > 3' };
 if (risk > 125) return { ok: false, reason: 'Risk Per Trade too high' };
 if (dailyLoss > 300) return { ok: false, reason: 'Daily Loss cap too high' };
 if (profitTarget > 500) return { ok: false, reason: 'Daily profit target too high for stabilization phase' };


 // Required secrets in Script Properties
 const sp = PropertiesService.getScriptProperties();
 const req = ['GROWW_API_KEY', 'GROWW_API_SECRET', 'GROWW_API_HOST'];
 const missing = req.filter(k => !sp.getProperty(k));
 if (missing.length) return { ok: false, reason: 'Missing Script Properties: ' + missing.join(', ') };


 return { ok: true, paperTrade: paper, checkedAt: new Date().toISOString() };
}


// ====== UTILITIES ======


function TRIM_SHEET_KEEP_LAST_ROWS(sheetName, keepLastRows, headerRows) {
 const sh = SpreadsheetApp.getActive().getSheetByName(sheetName);
 if (!sh) return;
 const last = sh.getLastRow();
 if (last <= headerRows + keepLastRows) return;


 const deleteFrom = headerRows + 1;
 const deleteCount = last - headerRows - keepLastRows;
 sh.deleteRows(deleteFrom, deleteCount);
 _logOps_(`TRIM_SHEET_KEEP_LAST_ROWS: ${sheetName} deletedRows=${deleteCount}`);
}


function _clearDataKeepHeader_(sheetName) {
 const sh = SpreadsheetApp.getActive().getSheetByName(sheetName);
 if (!sh) return;


 const lastRow = sh.getLastRow();
 const lastCol = sh.getLastColumn();
 if (lastRow < 2 || lastCol < 1) return;


 // Find header row by scanning first 10 rows for common header keywords
 // (Timestamp/Symbol/Exchange/Segment etc.)
 const scanRows = Math.min(10, lastRow);
 const scan = sh.getRange(1, 1, scanRows, lastCol).getValues();


 let headerRow = null;
 const keywords = ['Timestamp', 'Symbol', 'Exchange', 'Segment', 'Direction', 'LTP', 'Score'];


 for (let r = 0; r < scan.length; r++) {
   const row = scan[r].map(v => (v === null || v === undefined) ? '' : String(v).trim());
   const hit = keywords.some(k => row.indexOf(k) !== -1);
   if (hit) { headerRow = r + 1; break; }
 }


 // fallback: if not found, assume header is row 3 (your template default)
 if (!headerRow) headerRow = 3;


 // Clear everything below header row
 if (lastRow <= headerRow) {
   _logOps_('_clearDataKeepHeader_: ' + sheetName + ' nothing to clear (no data rows)');
   return;
 }


 const numRows = lastRow - headerRow;
 const range = sh.getRange(headerRow + 1, 1, numRows, lastCol);


 // Clear values only (keeps formatting)
 range.clearContent();


 _logOps_('_clearDataKeepHeader_: ' + sheetName + ' cleared rows ' + (headerRow + 1) + ' to ' + lastRow);
}


function _logOps_(msg) {
 try {
   if (typeof ACTION === 'function') ACTION('Maintenance', 'OPS', 'INFO', String(msg || ''), {});
 } catch (_) {}
 // Minimal ops logger that won’t break if your logger changes.
 try {
   if (typeof logInfo === 'function') return logInfo('[OPS] ' + msg);
 } catch (_) {}
 // fallback to Logs sheet
 const sh = SpreadsheetApp.getActive().getSheetByName(SYS.LOG_SHEET);
 if (!sh) return;
 const row = Math.max(sh.getLastRow() + 1, 4);
 sh.getRange(row, 1, 1, 3).setValues([[new Date(), 'OPS', msg]]);
}


function _safeCfgGetBool_(label, fallback) {
 try { return !!cfgGet(label); } catch (e) { return fallback; }
}
function _safeCfgGetNum_(label, fallback) {
 try {
   const v = Number(cfgGet(label));
   return isFinite(v) ? v : fallback;
 } catch (e) { return fallback; }
}


/***********************
* FULL RESET MODES
***********************/


/***********************
* SINGLE TOTAL RESET
***********************/


/**
* ONE BUTTON TOTAL WIPE + RESTART
*
* - Stops all triggers
* - Clears ALL runtime sheets (values only)
* - Keeps Config + Watchlist + Code + README
* - Resets runtime state
* - Reinstalls triggers
* - Runs healthcheck
*/
function CLEAR_ALL_AND_RESTART() {
 const lock = LockService.getScriptLock();
 lock.waitLock(30000);


 try {
   _logOps_('CLEAR_ALL_AND_RESTART: start');


   // 1️⃣ Stop triggers to avoid race conditions
   STOP_ALL_TRIGGERS();


   const ss = SpreadsheetApp.getActive();


   // Sheets we NEVER touch
   const KEEP = new Set([
     '⚙️ Config',
     '📋 Watchlist',
     '🧾 Universe Instruments',
     '🗄️ Candle Cache',
     '📚 History Backfill',
     '📜 Apps Script Code',
     '📘 README'
   ]);


   // 2️⃣ Clear all other sheets (values only)
   ss.getSheets().forEach(sh => {
     const name = sh.getName();
     if (KEEP.has(name)) {
       _logOps_('CLEAR_ALL_AND_RESTART: kept ' + name);
       return;
     }


     const lastRow = sh.getLastRow();
     const lastCol = sh.getLastColumn();


     if (lastRow < 2 || lastCol < 1) {
       _logOps_('CLEAR_ALL_AND_RESTART: ' + name + ' nothing to clear');
       return;
     }


     // Most template sheets have data starting from row 4
     const startRow = 4;


     if (lastRow >= startRow) {
       sh.getRange(startRow, 1, lastRow - startRow + 1, lastCol).clearContent();
       _logOps_('CLEAR_ALL_AND_RESTART: cleared ' + name + ' rows ' + startRow + ' to ' + lastRow);
     }
   });


   // 3️⃣ Reset runtime state (idempotency, daily counters etc.)
   RESET_RUNTIME_STATE();


   // 4️⃣ Enforce safe defaults (if cfgSet exists)
   try {
     if (typeof cfgSet === 'function') {
       cfgSet('Paper Trade Mode', true);
       cfgSet('Intraday Capital %', 0);
      cfgSet('Max Trades Per Day', 5);
      cfgSet('Max Open Positions', 3);
      cfgSet('Risk Per Trade (₹)', 125);
      cfgSet('Max Daily Loss (₹)', 300);
      cfgSet('Daily Profit Target (₹)', 200);
      PropertiesService.getScriptProperties().setProperty('WATCHLIST_TARGET_SIZE', '200');
       _logOps_('CLEAR_ALL_AND_RESTART: safety defaults applied');
     }
   } catch (e) {
     _logOps_('CLEAR_ALL_AND_RESTART: safety defaults error ' + e.message);
   }


   // 5️⃣ Reinstall triggers
   if (typeof setupTriggers !== 'function') {
     throw new Error('setupTriggers() not found');
   }


   setupTriggers();
   _logOps_('CLEAR_ALL_AND_RESTART: triggers installed');


   // 6️⃣ Final healthcheck
   const health = SYSTEM_HEALTHCHECK();
   _logOps_('CLEAR_ALL_AND_RESTART: health=' + JSON.stringify(health));


   if (!health.ok) {
     throw new Error('Healthcheck failed: ' + health.reason);
   }


   _logOps_('CLEAR_ALL_AND_RESTART: DONE ✅');


   return true;


 } finally {
   lock.releaseLock();
 }
}

/**
* FIRST-TIME STYLE RESET (non-destructive to credentials/config)
*
* - Stops all triggers
* - Clears all runtime/data sheets to header-only (row 3 kept)
* - Keeps Config + Apps Script Code + README
* - Clears runtime/script state (keeps API credentials/secrets)
* - Applies safe config defaults
* - Does NOT auto-create triggers (run SET_ALL_PROJECT_TRIGGERS manually after setup)
*/
function RESET_TO_FIRST_SETUP_STATE() {
 const lock = LockService.getScriptLock();
 lock.waitLock(30000);
 try {
   if (typeof ACTION === 'function') ACTION('Maintenance', 'RESET_TO_FIRST_SETUP_STATE', 'START', '', {});
   _logOps_('RESET_TO_FIRST_SETUP_STATE: start');

   // 1) Stop all triggers first
   STOP_ALL_TRIGGERS();

   // 2) Ensure all core sheets exist so reset is deterministic
   try { ensureCoreSheets_(); } catch (e) {}

   const ss = SpreadsheetApp.getActive();
   const KEEP = new Set([
     '⚙️ Config',
     '📜 Apps Script Code',
     '📘 README'
   ]);

   ss.getSheets().forEach(sh => {
     const name = sh.getName();
     if (KEEP.has(name)) {
       _logOps_('RESET_TO_FIRST_SETUP_STATE: kept ' + name);
       return;
     }
     _clearDataKeepHeader_(name);
   });

   // 3) Clear runtime properties but keep secrets/config keys
   RESET_RUNTIME_STATE();

   // 4) Clear in-memory buffers/caches used by logs + lookup maps
   try {
     const cache = CacheService.getScriptCache();
     cache.removeAll(['log_buf', 'decision_buf', 'action_buf', 'action_exec_id', 'cfg_label_map']);
   } catch (e) {}

   // 5) Set safe defaults for first-time controlled run
   try { ENSURE_SAFE_CONFIG_DEFAULTS(); } catch (e) {}

   _logOps_('RESET_TO_FIRST_SETUP_STATE: done ✅ (triggers are OFF)');
   if (typeof ACTION === 'function') {
     ACTION('Maintenance', 'RESET_TO_FIRST_SETUP_STATE', 'DONE', 'first setup state ready', {});
     try { _flushActionLogs_(); } catch (_) {}
   }
   return {
     ok: true,
     next: [
       'syncUniverseFromGrowwInstruments(0)',
       'repairUniverseBlankMetrics()',
       'PREMARKET_PRECOMPUTE_NOW()',
       'UNIVERSE_COVERAGE_STATUS()',
       'initUniverseHistoryBackfill("1d", false)',
       'processUniverseHistoryBackfillBatch(2, "1d")',
       'syncWatchlistCandleCacheIncremental()',
       'SET_ALL_PROJECT_TRIGGERS()',
       'RUN_NOW()',
     ],
   };
 } finally {
   lock.releaseLock();
 }
}
