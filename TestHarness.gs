/***********************
* TestHarness.gs
* Deterministic tests + reporting
***********************/


function RUN_TEST_SUITE() {
 const results = [];
 results.push(_runTest_('CONFIG: read labels', TEST_configRead));
 results.push(_runTest_('AUTH: token refresh', TEST_authRefresh));
 results.push(_runTest_('DATA: quote fetch (NIFTY)', TEST_quoteNifty));
 results.push(_runTest_('DATA: candles fetch (sample)', TEST_candlesSample));
 results.push(_runTest_('IND: compute indicators', TEST_indicators));
 results.push(_runTest_('BRAIN: regime classifier', TEST_regime));
 results.push(_runTest_('RISK: gates sanity', TEST_riskGates));
 results.push(_runTest_('UNIVERSE: watchlist mini batch', TEST_watchlistMiniBatch));
 results.push(_runTest_('E2E: dry run scan->signal->paper order', TEST_e2eDryRun));
 results.push(_runTest_('CACHE: watchlist incremental sync', TEST_cacheSync));
 results.push(_runTest_('HISTORY: init backfill queue', TEST_historyInit));
 results.push(_runTest_('HISTORY: progress snapshot', TEST_historyProgress));


 _printTestReport_(results);


 const failed = results.filter(r => !r.ok);
 if (failed.length) throw new Error('TEST SUITE FAILED: ' + failed.map(f => f.name).join(', '));
 return results;
}


function _runTest_(name, fn) {
 const start = Date.now();
 try {
   const out = fn();
   return { name, ok: true, ms: Date.now() - start, out };
 } catch (e) {
   return { name, ok: false, ms: Date.now() - start, error: String(e && e.message ? e.message : e) };
 }
}


function _printTestReport_(results) {
 const ok = results.filter(r => r.ok).length;
 const fail = results.length - ok;
 _tlog_(`TEST REPORT: ok=${ok} fail=${fail}`);
 results.forEach(r => {
   if (r.ok) _tlog_(`✅ ${r.name} (${r.ms}ms)`);
   else _tlog_(`❌ ${r.name} (${r.ms}ms) err=${r.error}`);
 });
}


// ====== INDIVIDUAL TESTS ======


function TEST_configRead() {
 // expects cfgGet to exist (from Config.gs)
 if (typeof cfgGet !== 'function') throw new Error('cfgGet not found');
 const capital = cfgGet('Total Capital (₹)');
 const risk = cfgGet('Risk Per Trade (₹)');
 if (!capital || !risk) throw new Error('Missing capital/risk in config');
 return { capital, risk };
}


function TEST_authRefresh() {
 // expects refreshAccessToken_ (or equivalent) to exist in your refactor.
 // If your refactor uses different names, map it here.
 if (typeof refreshAccessToken_ !== 'function') {
   // If your auth is inside Groww client, then we just do a cheap authenticated call.
   return { skipped: true, reason: 'refreshAccessToken_ not found; will validate with quote test' };
 }
 const token = refreshAccessToken_();
 if (!token) throw new Error('No token returned from refresh');
 return { tokenPrefix: String(token).slice(0, 12) + '...' };
}


function TEST_quoteNifty() {
 // expects fetchIndexQuote_ or growwQuote_ function - adapt to your refactor
 if (typeof getIndexQuote_ === 'function') {
   const q = getIndexQuote_('NIFTY'); // must be NIFTY
   if (!q || !q.ltp) throw new Error('Invalid NIFTY quote');
   return { ltp: q.ltp };
 }
 if (typeof fetchQuote_ === 'function') {
   const q = fetchQuote_({ tradingSymbol: 'NIFTY', exchange: 'NSE', segment: 'CASH' });
   if (!q) throw new Error('Quote returned empty');
   return q;
 }
 throw new Error('No quote function found (getIndexQuote_ / fetchQuote_)');
}


function TEST_candlesSample() {
 // pick first symbol from watchlist
 const sym = _getFirstWatchlistSymbol_();
 if (!sym) throw new Error('Watchlist empty');
 if (typeof fetchCandles_ !== 'function') throw new Error('fetchCandles_ not found');
 const candles = fetchCandles_(sym, '15m', 8 /*days*/);
 if (!Array.isArray(candles) || candles.length < 50) throw new Error('Not enough candles for indicators');
 return { symbol: sym, candles: candles.length };
}


function TEST_indicators() {
 const sym = _getFirstWatchlistSymbol_();
 if (!sym) throw new Error('Watchlist empty');
 if (typeof fetchCandles_ !== 'function') throw new Error('fetchCandles_ not found');
 if (typeof computeIndicators_ !== 'function') throw new Error('computeIndicators_ not found');


 const candles = fetchCandles_(sym, '15m', 8);
 const ind = computeIndicators_(candles);
 if (!ind || !ind.ema50 || !isFinite(ind.ema50.curr)) throw new Error('Indicators invalid');
 return { symbol: sym, ema50: ind.ema50.curr, rsi: ind.rsi.curr };
}


function TEST_regime() {
 if (typeof computeMarketRegime_ !== 'function') {
   return { skipped: true, reason: 'computeMarketRegime_ not found' };
 }
 const reg = computeMarketRegime_();
 if (!reg || !reg.regime) throw new Error('Regime output invalid');
 return reg;
}


function TEST_riskGates() {
 if (typeof riskGateAllowsNewTrade_ !== 'function') {
   return { skipped: true, reason: 'riskGateAllowsNewTrade_ not found' };
 }
 // should pass in paper mode with no positions
 const ok = riskGateAllowsNewTrade_();
 return { allowsNewTrade: ok };
}

function TEST_watchlistMiniBatch() {
 if (typeof buildSmartWatchlistBatch !== 'function') throw new Error('buildSmartWatchlistBatch not found');
 const out = buildSmartWatchlistBatch(8, 20);
 if (typeof _flushLogs_ === 'function') _flushLogs_();
 if (typeof _flushDecisionLogs_ === 'function') _flushDecisionLogs_();
 return out;
}


function TEST_e2eDryRun() {
 // Runs one scan cycle but forces paper mode
 if (typeof RUN_NOW !== 'function') throw new Error('RUN_NOW not found');
 // ensure paper mode TRUE
 if (typeof cfgSet === 'function') cfgSet('Paper Trade Mode', true);


 const before = new Date().toISOString();
 RUN_NOW(); // should run the main loop and write into scanner/signals
 const after = new Date().toISOString();
 return { ranFrom: before, ranTo: after, note: 'Check Live Scanner + Signals populated' };
}

function TEST_cacheSync() {
 if (typeof syncWatchlistCandleCacheIncremental !== 'function') throw new Error('syncWatchlistCandleCacheIncremental not found');
 const out = syncWatchlistCandleCacheIncremental();
 return out;
}

function TEST_historyInit() {
 if (typeof initUniverseHistoryBackfill !== 'function') throw new Error('initUniverseHistoryBackfill not found');
 const count = initUniverseHistoryBackfill('1d', false);
 return { queueRows: count };
}

function TEST_historyProgress() {
 if (typeof historyBackfillProgress !== 'function') throw new Error('historyBackfillProgress not found');
 return historyBackfillProgress('1d');
}


// ====== HELPERS ======


function _getFirstWatchlistSymbol_() {
  const sh = SS.getSheetByName(SH.WATCHLIST);
  if (!sh) return null;
  const last = sh.getLastRow();
  if (last < 4) return null;
  // Data starts at row 4; col B is Symbol in this template
  const vals = sh.getRange(4, 2, last - 3, 1).getValues().flat().map(v => String(v || "").trim()).filter(Boolean);
  return vals[0] || null;
}


function _tlog_(msg) {
 try {
   if (typeof logInfo === 'function') return logInfo('[TEST] ' + msg);
 } catch (_) {}
 const sh = SpreadsheetApp.getActive().getSheetByName('📝 Logs');
 if (!sh) return;
 const row = Math.max(sh.getLastRow() + 1, 4);
 sh.getRange(row, 1, 1, 3).setValues([[new Date(), 'TEST', msg]]);
}
