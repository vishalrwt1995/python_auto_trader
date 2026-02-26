// DataEngine.gs — cache, history backfill, and decision logs

const HISTORY_FOLDER_PROP = "HISTORY_ARCHIVE_FOLDER_ID";
const CANDLE_CACHE_MAX_ROWS_PROP = "CANDLE_CACHE_MAX_ROWS";
const DEFAULT_CANDLE_CACHE_MAX_ROWS = 850000;
const CANDLE_LOCAL_CACHE_BARS = 500;
const DEFAULT_CACHE_SCAN_MAX_ROWS = 90000;
const CACHE_SYNC_BATCH_PROP = "CACHE_SYNC_BATCH";
const CACHE_SYNC_CURSOR_PROP = "runtime:cache_sync_cursor";
const DEFAULT_CACHE_SYNC_BATCH = 25;
const SCORE_CACHE_1D_BATCH_PROP = "SCORE_CACHE_1D_BATCH";
const SCORE_CACHE_1D_CURSOR_PROP = "runtime:score_cache_1d_cursor";
const SCORE_CACHE_1D_LOOKBACK_DAYS_PROP = "SCORE_CACHE_1D_LOOKBACK_DAYS";
const SCORE_CACHE_1D_TARGET_BARS_PROP = "SCORE_CACHE_1D_TARGET_BARS";
const SCORE_CACHE_1D_MAX_MS_PROP = "SCORE_CACHE_1D_MAX_MS";
const DEFAULT_SCORE_CACHE_1D_BATCH = 24;
const DEFAULT_SCORE_CACHE_1D_LOOKBACK_DAYS = 700;
const DEFAULT_SCORE_CACHE_1D_TARGET_BARS = 320;
const DEFAULT_SCORE_CACHE_1D_MAX_MS = 240000;
const SCORE_CACHE_1D_WINDOW_DAYS = 180;
const SCORE_CACHE_1D_MAX_STALE_DAYS_PROP = "SCORE_CACHE_1D_MAX_STALE_DAYS";
const DEFAULT_SCORE_CACHE_1D_MAX_STALE_DAYS = 45;
const CANDLE_CACHE_TITLE = "🗄️ Candle Cache — persistent historical OHLCV";
const CANDLE_CACHE_HEADERS = ["Symbol", "Exchange", "Segment", "Timeframe", "Timestamp", "Open", "High", "Low", "Close", "Volume", "Source", "Fetched At", "Raw Candle (JSON)"];
const SCORE_CACHE_1D_TITLE = "📘 Score Cache 1D — metadata index for Stage-A universe scoring";
const SCORE_CACHE_1D_HEADERS = ["Symbol", "Exchange", "Segment", "Enabled", "Bars", "Last Candle Time", "Updated At", "Status", "Attempts", "Last Error", "File Name", "ISIN", "Notes", "File Id"];
const SCORE_CACHE_1D_DATA_TITLE = "📗 Score Cache 1D Data — sheet-backed JSON blobs for Stage-A scoring";
const SCORE_CACHE_1D_DATA_HEADERS = ["Key", "Symbol", "Exchange", "Segment", "Bars", "Last Candle Time", "Updated At", "Blob JSON", "Blob Chars"];

let _candleCacheSchemaEnsured_ = false;
let _scoreCache1dSchemaEnsured_ = false;
let _scoreCache1dDataSchemaEnsured_ = false;
let _scoreCache1dDataIndexMemo_ = null;

function _isPlainObjectArg_(v) {
  return !!v && Object.prototype.toString.call(v) === "[object Object]";
}

function _normHistoryTfArg_(tf, fallback = "1d") {
  if (_isPlainObjectArg_(tf)) return fallback;
  const raw = String(tf || fallback).trim().toLowerCase();
  const map = { day: "1d", "1month": "1mo" };
  const s = map[raw] || raw;
  const ok = {
    "1m": 1, "2m": 1, "3m": 1, "5m": 1, "10m": 1, "15m": 1, "30m": 1,
    "60m": 1, "1h": 1, "240m": 1, "4h": 1, "1d": 1, "1w": 1, "1mo": 1
  };
  return ok[s] ? s : fallback;
}

function _numArg_(v, fallback = 0, min = 0, max = 999999) {
  if (_isPlainObjectArg_(v)) return fallback;
  const n = Number(v);
  if (!isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(n)));
}

function _cacheSheet_() {
  ensureCoreSheets_();
  const sh = SS.getSheetByName(SH.CANDLE_CACHE);
  _ensureCandleCacheSchema_(sh);
  return sh;
}

function _ensureCandleCacheSchema_(sh) {
  if (!sh || _candleCacheSchemaEnsured_) return;
  const needCols = CANDLE_CACHE_HEADERS.length;
  if (sh.getMaxColumns() < needCols) {
    sh.insertColumnsAfter(sh.getMaxColumns(), needCols - sh.getMaxColumns());
  }

  const header = sh.getRange(3, 1, 1, needCols).getValues()[0];
  let headerMismatch = false;
  for (let i = 0; i < needCols; i++) {
    if (String(header[i] || "").trim() !== CANDLE_CACHE_HEADERS[i]) {
      headerMismatch = true;
      break;
    }
  }
  if (headerMismatch) sh.getRange(3, 1, 1, needCols).setValues([CANDLE_CACHE_HEADERS]);

  const title = String(sh.getRange(1, 1).getValue() || "").trim();
  if (!title) {
    const row = [CANDLE_CACHE_TITLE];
    while (row.length < needCols) row.push("");
    sh.getRange(1, 1, 1, needCols).setValues([row]);
  }
  _candleCacheSchemaEnsured_ = true;
}

function _scoreCache1dSheet_() {
  ensureCoreSheets_();
  const sh = SS.getSheetByName(SH.SCORE_CACHE_1D);
  _ensureScoreCache1dSchema_(sh);
  return sh;
}

function _scoreCache1dDataSheet_() {
  ensureCoreSheets_();
  const sh = SS.getSheetByName(SH.SCORE_CACHE_1D_DATA);
  _ensureScoreCache1dDataSchema_(sh);
  return sh;
}

function _ensureScoreCache1dSchema_(sh) {
  if (!sh || _scoreCache1dSchemaEnsured_) return;
  const needCols = SCORE_CACHE_1D_HEADERS.length;
  if (sh.getMaxColumns() < needCols) {
    sh.insertColumnsAfter(sh.getMaxColumns(), needCols - sh.getMaxColumns());
  }
  const header = sh.getRange(3, 1, 1, needCols).getValues()[0];
  let mismatch = false;
  for (let i = 0; i < needCols; i++) {
    if (String(header[i] || "").trim() !== SCORE_CACHE_1D_HEADERS[i]) { mismatch = true; break; }
  }
  if (mismatch) sh.getRange(3, 1, 1, needCols).setValues([SCORE_CACHE_1D_HEADERS]);
  const title = String(sh.getRange(1, 1).getValue() || "").trim();
  if (!title) {
    const row = [SCORE_CACHE_1D_TITLE];
    while (row.length < needCols) row.push("");
    sh.getRange(1, 1, 1, needCols).setValues([row]);
  }
  _scoreCache1dSchemaEnsured_ = true;
}

function _ensureScoreCache1dDataSchema_(sh) {
  if (!sh || _scoreCache1dDataSchemaEnsured_) return;
  const needCols = SCORE_CACHE_1D_DATA_HEADERS.length;
  if (sh.getMaxColumns() < needCols) {
    sh.insertColumnsAfter(sh.getMaxColumns(), needCols - sh.getMaxColumns());
  }
  const header = sh.getRange(3, 1, 1, needCols).getValues()[0];
  let mismatch = false;
  for (let i = 0; i < needCols; i++) {
    if (String(header[i] || "").trim() !== SCORE_CACHE_1D_DATA_HEADERS[i]) { mismatch = true; break; }
  }
  if (mismatch) sh.getRange(3, 1, 1, needCols).setValues([SCORE_CACHE_1D_DATA_HEADERS]);
  const title = String(sh.getRange(1, 1).getValue() || "").trim();
  if (!title) {
    const row = [SCORE_CACHE_1D_DATA_TITLE];
    while (row.length < needCols) row.push("");
    sh.getRange(1, 1, 1, needCols).setValues([row]);
  }
  try { if (!sh.isSheetHidden()) sh.hideSheet(); } catch (e) {}
  _scoreCache1dDataSchemaEnsured_ = true;
}

function _rawCandleJson_(rawCandle, normalizedCandle) {
  function toJson_(v) {
    try {
      const txt = JSON.stringify(v);
      if (!txt) return "";
      return txt.length > 49000 ? txt.substring(0, 49000) : txt;
    } catch (e) {
      return "";
    }
  }
  return toJson_(rawCandle) || toJson_(normalizedCandle);
}

function _decisionSheet_() {
  ensureCoreSheets_();
  return SS.getSheetByName(SH.DECISIONS);
}

function _backfillSheet_() {
  ensureCoreSheets_();
  return SS.getSheetByName(SH.BACKFILL);
}

function _historyFolder_() {
  const props = PropertiesService.getScriptProperties();
  const existingId = props.getProperty(HISTORY_FOLDER_PROP);
  if (existingId) {
    try {
      return DriveApp.getFolderById(existingId);
    } catch (e) {}
  }
  const folder = DriveApp.createFolder("Groww_History_Archive");
  props.setProperty(HISTORY_FOLDER_PROP, folder.getId());
  return folder;
}

function _scoreCache1dTargetBars_() {
  const raw = Number(PropertiesService.getScriptProperties().getProperty(SCORE_CACHE_1D_TARGET_BARS_PROP) || 0);
  if (!isFinite(raw) || raw < 120) return DEFAULT_SCORE_CACHE_1D_TARGET_BARS;
  return Math.max(120, Math.min(500, Math.floor(raw)));
}

function _scoreCache1dLookbackDays_() {
  const raw = Number(PropertiesService.getScriptProperties().getProperty(SCORE_CACHE_1D_LOOKBACK_DAYS_PROP) || 0);
  if (!isFinite(raw) || raw < 365) return DEFAULT_SCORE_CACHE_1D_LOOKBACK_DAYS;
  return Math.max(365, Math.min(1080, Math.floor(raw)));
}

function _scoreCache1dBatchSize_() {
  const raw = Number(PropertiesService.getScriptProperties().getProperty(SCORE_CACHE_1D_BATCH_PROP) || 0);
  if (!isFinite(raw) || raw < 1) return DEFAULT_SCORE_CACHE_1D_BATCH;
  return Math.max(1, Math.min(60, Math.floor(raw)));
}

function _scoreCache1dMaxMs_() {
  const raw = Number(PropertiesService.getScriptProperties().getProperty(SCORE_CACHE_1D_MAX_MS_PROP) || 0);
  if (!isFinite(raw) || raw < 60000) return DEFAULT_SCORE_CACHE_1D_MAX_MS;
  return Math.max(60000, Math.min(280000, Math.floor(raw)));
}

function _scoreCache1dMaxStaleDays_() {
  const raw = Number(PropertiesService.getScriptProperties().getProperty(SCORE_CACHE_1D_MAX_STALE_DAYS_PROP) || 0);
  if (!isFinite(raw) || raw < 5) return DEFAULT_SCORE_CACHE_1D_MAX_STALE_DAYS;
  return Math.max(5, Math.min(180, Math.floor(raw)));
}

function _isTransientScoreCache1dError_(err) {
  const s = String(err || "").replace(/[’`]/g, "'").toLowerCase();
  if (!s) return false;
  return (
    s.indexOf("we're sorry") >= 0 ||
    s.indexOf("sorry, a server error") >= 0 ||
    s.indexOf("server error occurred") >= 0 ||
    s.indexOf("please wait a bit and try again") >= 0 ||
    s.indexOf("service unavailable") >= 0 ||
    s.indexOf("temporarily unavailable") >= 0 ||
    s.indexOf("internal error") >= 0 ||
    s.indexOf("bad gateway") >= 0 ||
    s.indexOf("gateway timeout") >= 0 ||
    s.indexOf("returned code 500") >= 0 ||
    s.indexOf("returned code 502") >= 0 ||
    s.indexOf("returned code 503") >= 0 ||
    s.indexOf("returned code 504") >= 0 ||
    s.indexOf("returned code 429") >= 0 ||
    s.indexOf("too many requests") >= 0 ||
    s.indexOf("timed out") >= 0 ||
    s.indexOf("timeout") >= 0 ||
    s.indexOf("resource exhausted") >= 0 ||
    s.indexOf("service invoked too many times") >= 0
  );
}

function _isTransientDriveStorageError_(err) {
  const s = String(err || "").replace(/[’`]/g, "'").toLowerCase();
  if (!s) return false;
  return (
    s.indexOf("we're sorry") >= 0 ||
    s.indexOf("server error occurred") >= 0 ||
    s.indexOf("please wait a bit and try again") >= 0 ||
    s.indexOf("service unavailable") >= 0 ||
    s.indexOf("temporarily unavailable") >= 0 ||
    s.indexOf("internal error") >= 0 ||
    s.indexOf("backend error") >= 0 ||
    s.indexOf("bad gateway") >= 0 ||
    s.indexOf("gateway timeout") >= 0 ||
    s.indexOf("timed out") >= 0 ||
    s.indexOf("timeout") >= 0 ||
    s.indexOf("resource exhausted") >= 0 ||
    s.indexOf("service invoked too many times") >= 0 ||
    s.indexOf("rate limit exceeded") >= 0 ||
    s.indexOf("drive") >= 0 && s.indexOf("service error") >= 0
  );
}

function _retryDriveStorage_(fn, label, maxAttempts) {
  const attempts = Math.max(1, Math.min(6, Number(maxAttempts) || 4));
  let lastErr = null;
  for (let i = 1; i <= attempts; i++) {
    try {
      return fn();
    } catch (e) {
      lastErr = e;
      const errMsg = String(e && e.message ? e.message : e);
      const transient = _isTransientDriveStorageError_(errMsg);
      if (!transient || i >= attempts) throw e;
      LOG("WARN", "Drive", `${label || "op"} transient retry ${i}/${attempts}: ${errMsg.substring(0, 140)}`);
      Utilities.sleep(180 * i + Math.floor(Math.random() * 120));
    }
  }
  if (lastErr) throw lastErr;
  throw new Error("drive_retry_failed");
}

function _scoreCache1dFileName_(symbol, exchange, segment) {
  return `${String(exchange || "NSE").toUpperCase()}_${String(segment || "CASH").toUpperCase()}_${String(symbol || "").toUpperCase()}_score1d.json`;
}

function _scoreCache1dLocalKey_(symbol, exchange, segment) {
  return `scorecache1d:${String(exchange || "NSE").toUpperCase()}:${String(segment || "CASH").toUpperCase()}:${String(symbol || "").toUpperCase()}`;
}

function _scoreCache1dDataKey_(symbol, exchange, segment) {
  return `${String(symbol || "").toUpperCase()}|${String(exchange || "NSE").toUpperCase()}|${String(segment || "CASH").toUpperCase()}`;
}

function _scoreCache1dDataIndexMap_(force = false) {
  if (!force && _scoreCache1dDataIndexMemo_) return _scoreCache1dDataIndexMemo_;
  const sh = _scoreCache1dDataSheet_();
  const rows = sh.getDataRange().getValues().slice(3);
  const map = {};
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const key = String(r[0] || "").trim();
    if (!key) continue;
    map[key] = { row: i + 4, data: r };
  }
  _scoreCache1dDataIndexMemo_ = map;
  return map;
}

function _scoreCache1dDataMemoSet_(key, rowNum, rowVals) {
  if (!_scoreCache1dDataIndexMemo_) _scoreCache1dDataIndexMemo_ = {};
  _scoreCache1dDataIndexMemo_[String(key || "")] = { row: rowNum, data: rowVals };
}

function _readScoreCache1dLocal_(symbol, exchange, segment) {
  try {
    const raw = CacheService.getScriptCache().get(_scoreCache1dLocalKey_(symbol, exchange, segment));
    if (!raw) return [];
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr.map(_normCandle_).filter(Boolean) : [];
  } catch (e) {
    return [];
  }
}

function _writeScoreCache1dLocal_(symbol, exchange, segment, candles) {
  try {
    const keep = (candles || []).slice(-Math.max(200, _scoreCache1dTargetBars_() + 20));
    CacheService.getScriptCache().put(_scoreCache1dLocalKey_(symbol, exchange, segment), JSON.stringify(keep), 1800);
  } catch (e) {}
}

function _readScoreCache1dCandles_(symbol, exchange = "NSE", segment = "CASH", fileId = "") {
  const local = _readScoreCache1dLocal_(symbol, exchange, segment);
  if (local.length) return local;
  const key = _scoreCache1dDataKey_(symbol, exchange, segment);
  const hit = _scoreCache1dDataIndexMap_()[key];
  if (!hit || !hit.data) return [];
  const raw = String(hit.data[7] || "");
  if (!raw) return [];
  let candles = [];
  try {
    const arr = JSON.parse(raw);
    candles = Array.isArray(arr) ? arr.map(_normCandle_).filter(Boolean) : [];
  } catch (e) {
    candles = [];
  }
  if (candles.length) _writeScoreCache1dLocal_(symbol, exchange, segment, candles);
  return candles;
}

function _writeScoreCache1dCandles_(symbol, exchange = "NSE", segment = "CASH", candles = [], fileId = "") {
  const normalized = (candles || []).map(_normCandle_).filter(Boolean);
  if (!normalized.length) return { bars: 0, lastTs: "", fileName: _scoreCache1dFileName_(symbol, exchange, segment), fileId: String(fileId || "") };
  const keepBars = Math.max(200, _scoreCache1dTargetBars_() + 20);
  const trimmed = normalized.slice(-keepBars);
  const fileName = _scoreCache1dFileName_(symbol, exchange, segment);
  const payload = JSON.stringify(trimmed);
  if (payload.length > 49000) throw new Error("score_cache_1d_blob_too_large");
  const bars = trimmed.length;
  const lastTs = String(trimmed[bars - 1][0] || "");
  const key = _scoreCache1dDataKey_(symbol, exchange, segment);
  const sh = _scoreCache1dDataSheet_();
  const idx = _scoreCache1dDataIndexMap_();
  const hit = idx[key];
  const rowVals = [key, String(symbol || "").toUpperCase(), String(exchange || "NSE").toUpperCase(), String(segment || "CASH").toUpperCase(), bars, lastTs, nowIST(), payload, payload.length];
  let rowNum;
  if (hit && hit.row) {
    rowNum = hit.row;
    sh.getRange(rowNum, 1, 1, rowVals.length).setValues([rowVals]);
  } else {
    rowNum = sh.getLastRow() + 1;
    sh.getRange(rowNum, 1, 1, rowVals.length).setValues([rowVals]);
  }
  _scoreCache1dDataMemoSet_(key, rowNum, rowVals);
  _writeScoreCache1dLocal_(symbol, exchange, segment, trimmed);
  return { bars, lastTs, fileName, fileId: `SHEETROW:${rowNum}` };
}

function getUniverseScoreCache1dCandles(symbol, exchange = "NSE", segment = "CASH", minBars = 80, targetBars = 320, fileId = "") {
  const need = Math.max(80, Number(minBars) || 80);
  const target = Math.max(need, Number(targetBars) || _scoreCache1dTargetBars_());
  const arr = _readScoreCache1dCandles_(symbol, exchange, segment, fileId);
  if (!arr.length) return [];
  return arr.slice(-Math.max(target, need));
}

function _fetchScoreCache1dWindowed_(symbol, exchange = "NSE", segment = "CASH", lookbackDays = 0) {
  const days = Math.max(365, Number(lookbackDays) || _scoreCache1dLookbackDays_());
  const end = new Date();
  const start = new Date(end.getTime() - days * 86400000);
  const all = [];
  const seen = {};
  const sources = {};
  let cursor = new Date(start.getTime());
  while (cursor < end) {
    const winEnd = new Date(Math.min(end.getTime(), cursor.getTime() + SCORE_CACHE_1D_WINDOW_DAYS * 86400000));
    const meta = {};
    const part = fetchCandlesRangeDirect_(symbol, exchange, segment, "1d", cursor, winEnd, { allowDeprecatedFallback: false, meta }) || [];
    if (meta.source) sources[meta.source] = true;
    part.forEach(c => {
      const n = _normCandle_(c);
      if (!n) return;
      const k = String(n[0]);
      if (seen[k]) return;
      seen[k] = true;
      all.push(n);
    });
    cursor = new Date(winEnd.getTime() + 1000);
    Utilities.sleep(80);
  }
  all.sort((a, b) => _tsCmp_(a[0], b[0]));
  if (all.length) {
    const lastTs = String(all[all.length - 1][0] || "");
    const lastDt = _tsToDate_(lastTs);
    const staleDays = Math.floor((Date.now() - (lastDt ? lastDt.getTime() : 0)) / 86400000);
    const maxStaleDays = _scoreCache1dMaxStaleDays_();
    if (!lastDt || staleDays > maxStaleDays) {
      const src = Object.keys(sources).sort().join(",") || "none";
      throw new Error(`stale_1d_data_last_ts=${lastTs || "NA"} staleDays=${isFinite(staleDays) ? staleDays : "NA"} src=${src}`);
    }
  }
  return all;
}

function _scoreCache1dIndexMap_() {
  const sh = _scoreCache1dSheet_();
  const rows = sh.getDataRange().getValues().slice(3);
  const map = {};
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const sym = String(r[0] || "").toUpperCase();
    if (!sym) continue;
    const ex = String(r[1] || "NSE").toUpperCase();
    const seg = String(r[2] || "CASH").toUpperCase();
    map[`${sym}|${ex}|${seg}`] = { row: i + 4, data: r };
  }
  return map;
}

function initUniverseScoreCache1D(reset = false) {
  const resetFlag = _isPlainObjectArg_(reset) ? false : !!reset;
  const startedAt = Date.now();
  ACTION("DataEngine", "initUniverseScoreCache1D", "START", "", { reset: resetFlag });
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(25000)) {
    ACTION("DataEngine", "initUniverseScoreCache1D", "SKIP", "lock busy", { ms: Date.now() - startedAt });
    _flushActionLogs_();
    return { skipped: "lock_busy" };
  }
  try {
    ensureCoreSheets_();
    _scoreCache1dDataIndexMemo_ = null;
    const uni = _readUniverseRows_();
    if (!uni.length) throw new Error("Universe sheet is empty or all rows disabled");
    const sh = _scoreCache1dSheet_();
    const existing = _scoreCache1dIndexMap_();
    const targetBars = _scoreCache1dTargetBars_();
    const lookbackDays = _scoreCache1dLookbackDays_();
    const noteTemplate = `targetBars=${targetBars}|lookbackDays=${lookbackDays}`;
    let rowsWritten = 0;
    let appended = 0;
    let updated = 0;
    if (resetFlag) {
      const dataSh = _scoreCache1dDataSheet_();
      const dataLast = dataSh.getLastRow();
      if (dataLast > 3) dataSh.getRange(4, 1, dataLast - 3, SCORE_CACHE_1D_DATA_HEADERS.length).clearContent();
      _scoreCache1dDataIndexMemo_ = {};
      const out = [];
      for (let i = 0; i < uni.length; i++) {
        const u = uni[i];
        const fileName = _scoreCache1dFileName_(u.symbol, u.exchange, u.segment);
        const isin = (String(u.notes || "").match(/isin=([A-Z0-9]+)/i) || [])[1] || "";
        out.push([u.symbol, u.exchange, u.segment, "Y", 0, "", nowIST(), "PENDING", 0, "", fileName, isin, noteTemplate, ""]);
      }
      const last = sh.getLastRow();
      if (last > 3) sh.getRange(4, 1, last - 3, SCORE_CACHE_1D_HEADERS.length).clearContent();
      if (out.length) sh.getRange(4, 1, out.length, out[0].length).setValues(out);
      rowsWritten = out.length;
      appended = out.length;
    } else {
      const appendRows = [];
      const touchRows = [];
      const activeKeys = {};
      for (let i = 0; i < uni.length; i++) {
        const u = uni[i];
        const key = `${u.symbol}|${u.exchange}|${u.segment}`;
        activeKeys[key] = true;
        const hit = existing[key];
        const fileName = _scoreCache1dFileName_(u.symbol, u.exchange, u.segment);
        const isin = (String(u.notes || "").match(/isin=([A-Z0-9]+)/i) || [])[1] || "";
        if (!hit) {
          appendRows.push([u.symbol, u.exchange, u.segment, "Y", 0, "", nowIST(), "PENDING", 0, "", fileName, isin, noteTemplate, ""]);
          continue;
        }
        const r = hit.data || [];
        const enabled = String(r[3] || "Y").toUpperCase();
        const bars = Number(r[4] || 0);
        const lastTs = String(r[5] || "");
        const status = String(r[7] || "PENDING").toUpperCase();
        const attempts = Number(r[8] || 0);
        const lastErr = String(r[9] || "");
        const note = String(r[12] || "");
        const fileId = String(r[13] || "");
        const expectedEnabled = "Y";
        if (enabled !== expectedEnabled || String(r[10] || "") !== fileName || String(r[11] || "") !== isin || note !== noteTemplate) {
          touchRows.push({
            rowNum: hit.row,
            vals: [u.symbol, u.exchange, u.segment, expectedEnabled, bars, lastTs, nowIST(), status, attempts, lastErr, fileName, isin, noteTemplate, fileId]
          });
        }
      }
      if (touchRows.length) {
        touchRows.sort((a, b) => a.rowNum - b.rowNum);
        let blockStart = touchRows[0].rowNum;
        let blockVals = [touchRows[0].vals];
        let prev = touchRows[0].rowNum;
        for (let i = 1; i < touchRows.length; i++) {
          const t = touchRows[i];
          if (t.rowNum === prev + 1) {
            blockVals.push(t.vals);
            prev = t.rowNum;
            continue;
          }
          sh.getRange(blockStart, 1, blockVals.length, SCORE_CACHE_1D_HEADERS.length).setValues(blockVals);
          rowsWritten += blockVals.length;
          updated += blockVals.length;
          blockStart = t.rowNum;
          blockVals = [t.vals];
          prev = t.rowNum;
        }
        sh.getRange(blockStart, 1, blockVals.length, SCORE_CACHE_1D_HEADERS.length).setValues(blockVals);
        rowsWritten += blockVals.length;
        updated += blockVals.length;
      }
      if (appendRows.length) {
        sh.getRange(sh.getLastRow() + 1, 1, appendRows.length, appendRows[0].length).setValues(appendRows);
        rowsWritten += appendRows.length;
        appended = appendRows.length;
      }
    }
    const props = PropertiesService.getScriptProperties();
    props.setProperty(SCORE_CACHE_1D_CURSOR_PROP, "0");
    props.setProperty("score_cache_1d_queue_day", todayIST());
    ACTION("DataEngine", "initUniverseScoreCache1D", "DONE", "queue initialized", {
      ms: Date.now() - startedAt,
      rows: uni.length,
      reset: resetFlag,
      appended,
      updated,
      writes: rowsWritten,
    });
    _flushActionLogs_();
    return { rows: uni.length, reset: resetFlag, appended, updated, writes: rowsWritten };
  } finally {
    lock.releaseLock();
  }
}

function _scoreCache1dBatchWindow_(total, batchSize) {
  if (!(total > 0)) return { start: 0, end: 0, next: 0, wrapped: true };
  const props = PropertiesService.getScriptProperties();
  let cursor = Number(props.getProperty(SCORE_CACHE_1D_CURSOR_PROP) || 0);
  if (!isFinite(cursor) || cursor < 0 || cursor >= total) cursor = 0;
  const size = Math.max(1, Math.min(60, Number(batchSize) || _scoreCache1dBatchSize_()));
  const end = Math.min(total, cursor + size);
  const next = end >= total ? 0 : end;
  props.setProperty(SCORE_CACHE_1D_CURSOR_PROP, String(next));
  return { start: cursor, end, next, wrapped: end >= total };
}

function _isScoreCache1dRowRetryable_(r) {
  const st = String(r && r[7] || "PENDING").toUpperCase();
  const attempts = Number(r && r[8] || 0);
  if (st === "DONE") return false;
  if (st === "PENDING" || st === "IN_PROGRESS") return true;
  return attempts < 5;
}

function processUniverseScoreCache1DBatch(batchSize = 0) {
  const startedAt = Date.now();
  ACTION("DataEngine", "processUniverseScoreCache1DBatch", "START", "", { batchSize: _numArg_(batchSize, 0, 0, 60) });
  ensureCoreSheets_();
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(25000)) {
    ACTION("DataEngine", "processUniverseScoreCache1DBatch", "SKIP", "lock busy", { ms: Date.now() - startedAt });
    _flushActionLogs_();
    return { processed: 0, pending: -1 };
  }
  try {
    const sh = _scoreCache1dSheet_();
    let rows = sh.getDataRange().getValues().slice(3);
    if (!rows.length) {
      try { initUniverseScoreCache1D(false); } catch (e) {}
      rows = sh.getDataRange().getValues().slice(3);
    }
    if (!rows.length) {
      ACTION("DataEngine", "processUniverseScoreCache1DBatch", "DONE", "no rows", { ms: Date.now() - startedAt, processed: 0, pending: 0 });
      _flushActionLogs_();
      return { processed: 0, pending: 0 };
    }

    const enabledRows = rows
      .map((r, i) => ({ r, rowNum: i + 4 }))
      .filter(x => String(x.r[0] || "").trim() && String(x.r[3] || "Y").toUpperCase() === "Y");
    const targetBatch = Number(batchSize) > 0 ? Math.floor(Number(batchSize)) : _scoreCache1dBatchSize_();
    const pendingRows = enabledRows.filter(x => _isScoreCache1dRowRetryable_(x.r));
    const win = _scoreCache1dBatchWindow_(pendingRows.length, targetBatch);
    const picks = pendingRows.slice(win.start, win.end);
    let processed = 0;
    let errors = 0;
    let transientCount = 0;
    let timedOut = false;
    const lookbackDays = _scoreCache1dLookbackDays_();
    const maxMs = _scoreCache1dMaxMs_();
    for (let i = 0; i < picks.length; i++) {
      if (Date.now() - startedAt >= maxMs) {
        timedOut = true;
        break;
      }
      const p = picks[i];
      const r = p.r;
      const symbol = String(r[0] || "").toUpperCase();
      const exchange = String(r[1] || "NSE").toUpperCase();
      const segment = String(r[2] || "CASH").toUpperCase();
      const attempts = Number(r[8] || 0);
      const existingFileId = String(r[13] || "");
      sh.getRange(p.rowNum, 7, 1, 4).setValues([[nowIST(), "IN_PROGRESS", attempts + 1, ""]]);
      try {
        let candles = [];
        let saved = null;
        let done = false;
        let lastErr = "";
        for (let retry = 0; retry < 3; retry++) {
          try {
            try {
              candles = _fetchScoreCache1dWindowed_(symbol, exchange, segment, lookbackDays);
            } catch (ef) {
              throw new Error("FETCH: " + String(ef && ef.message ? ef.message : ef));
            }
            if (!candles.length) throw new Error("FETCH: no_1d_candles");
            try {
              saved = _writeScoreCache1dCandles_(symbol, exchange, segment, candles, existingFileId);
            } catch (ew) {
              throw new Error("WRITE: " + String(ew && ew.message ? ew.message : ew));
            }
            done = true;
            break;
          } catch (e1) {
            lastErr = String(e1 && e1.message ? e1.message : e1);
            if (retry < 2 && _isTransientScoreCache1dError_(lastErr)) {
              Utilities.sleep(350 * (retry + 1));
              continue;
            }
            throw new Error(lastErr);
          }
        }
        if (!done || !saved) throw new Error("score_cache_1d_save_failed");
        sh.getRange(p.rowNum, 5, 1, 10).setValues([[saved.bars, saved.lastTs, nowIST(), "DONE", attempts + 1, "", saved.fileName, String(r[11] || ""), String(r[12] || ""), String(saved.fileId || existingFileId || "")]]);
        processed++;
      } catch (e) {
        const err = String(e && e.message ? e.message : e).substring(0, 220);
        const transient = _isTransientScoreCache1dError_(err);
        sh.getRange(p.rowNum, 7, 1, 4).setValues([[nowIST(), transient ? "PENDING" : "ERROR", attempts + 1, err]]);
        if (transient) transientCount++;
        if (!transient) errors++;
        LOG(transient ? "WARN" : "ERR", "ScoreCache1D", `${symbol} ${exchange}/${segment}: ${err}`);
      }
      Utilities.sleep(120);
    }
    const data = sh.getDataRange().getValues().slice(3)
      .filter(r => String(r[0] || "").trim() && String(r[3] || "Y").toUpperCase() === "Y");
    const pending = data.filter(r => String(r[7] || "").toUpperCase() !== "DONE").length;
    const retryablePending = data.filter(r => _isScoreCache1dRowRetryable_(r)).length;
    const terminalError = data.filter(r => String(r[7] || "").toUpperCase() === "ERROR" && !_isScoreCache1dRowRetryable_(r)).length;
    ACTION("DataEngine", "processUniverseScoreCache1DBatch", "DONE", "batch processed", {
      ms: Date.now() - startedAt,
      processed,
      pending,
      retryablePending,
      terminalError,
      errors,
      transient: transientCount,
      total: data.length,
      timedOut,
      maxMs,
      nextCursor: win.next,
      wrapped: pending === 0 ? true : !!win.wrapped,
    });
    _flushActionLogs_();
    return { processed, pending, retryablePending, terminalError, errors, transient: transientCount, total: data.length, timedOut, maxMs, nextCursor: win.next, wrapped: pending === 0 ? true : !!win.wrapped };
  } finally {
    lock.releaseLock();
  }
}

function universeScoreCache1DProgress() {
  ensureCoreSheets_();
  const rows = _scoreCache1dSheet_().getDataRange().getValues().slice(3)
    .filter(r => String(r[0] || "").trim() && String(r[3] || "Y").toUpperCase() === "Y");
  const total = rows.length;
  const done = rows.filter(r => String(r[7] || "").toUpperCase() === "DONE").length;
  const error = rows.filter(r => String(r[7] || "").toUpperCase() === "ERROR").length;
  const retryablePending = rows.filter(r => _isScoreCache1dRowRetryable_(r)).length;
  const terminalError = rows.filter(r => String(r[7] || "").toUpperCase() === "ERROR" && !_isScoreCache1dRowRetryable_(r)).length;
  const minBars = _scoreCache1dTargetBars_();
  const withTargetBars = rows.filter(r => Number(r[4] || 0) >= minBars).length;
  let dataRows = 0;
  try {
    dataRows = Math.max(0, _scoreCache1dDataSheet_().getLastRow() - 3);
  } catch (e) {}
  return {
    total,
    done,
    pending: Math.max(0, total - done),
    retryablePending,
    terminalError,
    error,
    withTargetBars,
    targetBars: minBars,
    dataRows
  };
}

function _tfToMinutes_(tf) {
  const s = String(tf || "15m").toLowerCase();
  if (s === "1m") return 1;
  if (s === "2m") return 2;
  if (s === "3m") return 3;
  if (s === "5m") return 5;
  if (s === "10m") return 10;
  if (s === "15m") return 15;
  if (s === "30m") return 30;
  if (s === "60m" || s === "1h") return 60;
  if (s === "240m" || s === "4h") return 240;
  if (s === "1d" || s === "day") return 1440;
  if (s === "1w") return 10080;
  if (s === "1mo" || s === "1month") return 43200;
  return 15;
}

function _barsNeeded_(tf, lookbackDays) {
  const mins = _tfToMinutes_(tf);
  if (mins >= 1440) return Math.max(80, Math.ceil(lookbackDays));
  const perDay = Math.max(1, Math.floor((6.25 * 60) / mins));
  return Math.max(80, Math.ceil(perDay * Math.max(1, Number(lookbackDays) || 1)));
}

function _historySpec_(tf) {
  const s = _normHistoryTfArg_(tf, "1d");
  if (s === "1m" || s === "2m" || s === "3m" || s === "5m") return { availableDays: 30, windowDays: 30 };
  if (s === "10m" || s === "15m" || s === "30m") return { availableDays: 90, windowDays: 90 };
  if (s === "60m" || s === "1h" || s === "240m" || s === "4h") return { availableDays: 180, windowDays: 180 };
  if (s === "1d" || s === "1w" || s === "1mo" || s === "1month") {
    const start = new Date("2020-01-01T00:00:00Z");
    const now = new Date();
    const days = Math.ceil((now.getTime() - start.getTime()) / 86400000);
    return { availableDays: Math.max(1080, days), windowDays: 180 };
  }
  return { availableDays: 180, windowDays: 180 };
}

function _tsToDate_(ts) {
  const raw = String(ts || "").trim();
  if (!raw) return null;
  if (/^\d{13}$/.test(raw)) return new Date(Number(raw));
  if (/^\d{10}$/.test(raw)) return new Date(Number(raw) * 1000);
  const ms = Date.parse(raw);
  if (!isFinite(ms)) return null;
  return new Date(ms);
}

function _tsMs_(ts) {
  const d = _tsToDate_(ts);
  const ms = d ? d.getTime() : NaN;
  return isFinite(ms) ? ms : NaN;
}

function _tsCmp_(a, b) {
  const am = _tsMs_(a);
  const bm = _tsMs_(b);
  if (isFinite(am) && isFinite(bm)) return am - bm;
  return String(a || "").localeCompare(String(b || ""));
}

function _normCandle_(row) {
  if (!row || row.length < 6) return null;
  const ts = String(row[0] || "").trim();
  const o = parseFloat(row[1]);
  const h = parseFloat(row[2]);
  const l = parseFloat(row[3]);
  const c = parseFloat(row[4]);
  const v = parseFloat(row[5]);
  if (!ts || !isFinite(o) || !isFinite(h) || !isFinite(l) || !isFinite(c) || !isFinite(v)) return null;
  return [ts, o, h, l, c, v];
}

function _cacheKey_(symbol, exchange, segment, tf) {
  return `cache_last:${String(tf || "").toLowerCase()}:${String(exchange || "").toUpperCase()}:${String(segment || "").toUpperCase()}:${String(symbol || "").toUpperCase()}`;
}

function _lastCacheTsFromProps_(symbol, exchange, segment, tf) {
  const props = PropertiesService.getScriptProperties();
  return String(props.getProperty(_cacheKey_(symbol, exchange, segment, tf)) || "");
}

function _setLastCacheTs_(symbol, exchange, segment, tf, ts) {
  if (!ts) return;
  const props = PropertiesService.getScriptProperties();
  props.setProperty(_cacheKey_(symbol, exchange, segment, tf), String(ts));
}

function _cacheMaxRows_() {
  const props = PropertiesService.getScriptProperties();
  const raw = Number(props.getProperty(CANDLE_CACHE_MAX_ROWS_PROP));
  return isFinite(raw) && raw > 2000 ? Math.floor(raw) : DEFAULT_CANDLE_CACHE_MAX_ROWS;
}

function _localCacheKey_(symbol, exchange, segment, tf) {
  return `candles:${String(tf || "").toLowerCase()}:${String(exchange || "").toUpperCase()}:${String(segment || "").toUpperCase()}:${String(symbol || "").toUpperCase()}`;
}

function _mergeCandles_(base, incoming) {
  const byTs = {};
  (base || []).forEach(c => {
    const n = _normCandle_(c);
    if (!n) return;
    byTs[String(n[0])] = n;
  });
  (incoming || []).forEach(c => {
    const n = _normCandle_(c);
    if (!n) return;
    byTs[String(n[0])] = n;
  });
  return Object.keys(byTs).map(k => byTs[k]).sort((a, b) => _tsCmp_(a[0], b[0]));
}

function _readLocalCandles_(symbol, exchange, segment, tf) {
  try {
    const raw = CacheService.getScriptCache().get(_localCacheKey_(symbol, exchange, segment, tf));
    if (!raw) return [];
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr.map(_normCandle_).filter(Boolean) : [];
  } catch (e) {
    return [];
  }
}

function _writeLocalCandles_(symbol, exchange, segment, tf, candles) {
  try {
    const keep = (candles || []).slice(-CANDLE_LOCAL_CACHE_BARS);
    CacheService.getScriptCache().put(_localCacheKey_(symbol, exchange, segment, tf), JSON.stringify(keep), 1800);
  } catch (e) {}
}

function _scanCacheTail_(symbol, exchange, segment, tf, minBars) {
  const sh = _cacheSheet_();
  const last = sh.getLastRow();
  if (last < 4) return [];
  const props = PropertiesService.getScriptProperties();
  const maxScanRaw = Number(props.getProperty("CANDLE_CACHE_SCAN_MAX_ROWS") || 0);
  const maxScanRows = isFinite(maxScanRaw) && maxScanRaw > 1000 ? Math.floor(maxScanRaw) : DEFAULT_CACHE_SCAN_MAX_ROWS;
  const chunk = 5000;
  const need = Math.max(80, Number(minBars) || 80);
  const sym = String(symbol || "").toUpperCase();
  const ex = String(exchange || "NSE").toUpperCase();
  const seg = String(segment || "CASH").toUpperCase();
  const t = String(tf || "15m").toLowerCase();
  const out = [];
  let scanned = 0;
  let endRow = last;

  while (endRow >= 4 && scanned < maxScanRows && out.length < need * 2) {
    const take = Math.min(chunk, endRow - 3, maxScanRows - scanned);
    if (take <= 0) break;
    const start = endRow - take + 1;
    const vals = sh.getRange(start, 1, take, 10).getValues();
    for (let i = vals.length - 1; i >= 0; i--) {
      const r = vals[i];
      if (
        String(r[0] || "").toUpperCase() !== sym ||
        String(r[1] || "").toUpperCase() !== ex ||
        String(r[2] || "").toUpperCase() !== seg ||
        String(r[3] || "").toLowerCase() !== t
      ) continue;
      const c = _normCandle_([r[4], r[5], r[6], r[7], r[8], r[9]]);
      if (c) out.push(c);
      if (out.length >= need * 2) break;
    }
    scanned += take;
    endRow = start - 1;
  }
  if (!out.length) return [];
  const merged = _mergeCandles_([], out);
  return merged.slice(-Math.max(need, CANDLE_LOCAL_CACHE_BARS));
}

function getCachedCandles(symbol, exchange, segment, tf, minBars = 80) {
  const need = Math.max(80, Number(minBars) || 80);
  const local = _readLocalCandles_(symbol, exchange, segment, tf);
  if (local.length >= need) return local.slice(-need);
  const tail = _scanCacheTail_(symbol, exchange, segment, tf, need);
  if (tail.length) {
    _writeLocalCandles_(symbol, exchange, segment, tf, tail);
    return tail.slice(-need);
  }
  return [];
}

function upsertCandlesToCache(symbol, exchange, segment, tf, candles, source = "groww", opts = {}) {
  const sh = _cacheSheet_();
  const tfNorm = String(tf || "15m").toLowerCase();
  const exNorm = String(exchange || "NSE").toUpperCase();
  const segNorm = String(segment || "CASH").toUpperCase();
  const symNorm = String(symbol || "").toUpperCase();

  const lastTs = String(opts.lastTsHint || _lastCacheTsFromProps_(symNorm, exNorm, segNorm, tfNorm) || "");
  const lastTsMs = _tsMs_(lastTs);
  const rows = [];
  let newLastTs = lastTs;
  const fetchedAt = nowIST();

  candles.forEach(c => {
    const n = _normCandle_(c);
    if (!n) return;
    if (lastTs) {
      const curMs = _tsMs_(n[0]);
      if (isFinite(lastTsMs) && isFinite(curMs)) {
        if (curMs <= lastTsMs) return;
      } else if (String(n[0]) <= lastTs) {
        return;
      }
    }
    rows.push([symNorm, exNorm, segNorm, tfNorm, n[0], n[1], n[2], n[3], n[4], n[5], source, fetchedAt, _rawCandleJson_(c, n)]);
    if (!newLastTs || _tsCmp_(n[0], newLastTs) > 0) newLastTs = String(n[0]);
  });

  if (!rows.length) return 0;

  const maxRows = _cacheMaxRows_();
  const projected = sh.getLastRow() + rows.length;
  if (projected > maxRows) {
    const overflow = projected - maxRows;
    const startDelete = 4; // keep header rows
    const canDelete = Math.max(0, sh.getLastRow() - 3);
    const del = Math.min(overflow, canDelete);
    if (del > 0) sh.deleteRows(startDelete, del);
  }

  sh.getRange(sh.getLastRow() + 1, 1, rows.length, rows[0].length).setValues(rows);
  _setLastCacheTs_(symNorm, exNorm, segNorm, tfNorm, newLastTs);
  const mergedLocal = _mergeCandles_(_readLocalCandles_(symNorm, exNorm, segNorm, tfNorm), candles);
  if (mergedLocal.length) _writeLocalCandles_(symNorm, exNorm, segNorm, tfNorm, mergedLocal);
  return rows.length;
}

function syncCandlesToCache(symbol, exchange, segment, tf, lookbackDays, lastTsHint = "") {
  const candles = fetchCandlesDirect_(symbol, exchange, segment, tf, lookbackDays);
  if (!candles || !candles.length) return { fetched: 0, added: 0 };
  const added = upsertCandlesToCache(symbol, exchange, segment, tf, candles, "groww_api", { lastTsHint });
  return { fetched: candles.length, added };
}

function _cacheSyncWindow_(wl) {
  const total = Array.isArray(wl) ? wl.length : 0;
  if (!total) return { list: [], total: 0, processed: 0, nextCursor: 0, wrapped: true };
  const props = PropertiesService.getScriptProperties();
  const batchRaw = Number(props.getProperty(CACHE_SYNC_BATCH_PROP) || 0);
  const batch = isFinite(batchRaw) && batchRaw >= 5 ? Math.floor(batchRaw) : DEFAULT_CACHE_SYNC_BATCH;
  if (total <= batch) {
    props.setProperty(CACHE_SYNC_CURSOR_PROP, "0");
    return { list: wl.slice(), total, processed: total, nextCursor: 0, wrapped: true };
  }
  let cursor = Number(props.getProperty(CACHE_SYNC_CURSOR_PROP) || 0);
  if (!isFinite(cursor) || cursor < 0 || cursor >= total) cursor = 0;
  const end = Math.min(total, cursor + batch);
  const wrapped = end >= total;
  const nextCursor = wrapped ? 0 : end;
  props.setProperty(CACHE_SYNC_CURSOR_PROP, String(nextCursor));
  return { list: wl.slice(cursor, end), total, processed: end - cursor, nextCursor, wrapped };
}

function syncWatchlistCandleCacheIncremental() {
  const startedAt = Date.now();
  ACTION("DataEngine", "syncWatchlistCandleCacheIncremental", "START", "", {});
  const wl = getWatchlist();
  if (!wl.length) {
    ACTION("DataEngine", "syncWatchlistCandleCacheIncremental", "DONE", "watchlist empty", { ms: Date.now() - startedAt });
    _flushActionLogs_();
    return { symbols: 0, added: 0, total: 0, nextCursor: 0 };
  }
  const win = _cacheSyncWindow_(wl);
  let added = 0;
  win.list.forEach((w, i) => {
    added += syncCandlesToCache(w.symbol, w.exchange, w.segment, "15m", 8).added;
    if (i % 3 === 0) Utilities.sleep(120);
  });
  LOG("INFO", "Cache", `Incremental candle sync added=${added} processed=${win.processed}/${win.total} nextCursor=${win.nextCursor}`);
  ACTION("DataEngine", "syncWatchlistCandleCacheIncremental", "DONE", "incremental cache sync complete", {
    ms: Date.now() - startedAt,
    symbols: win.processed,
    total: win.total,
    added,
    nextCursor: win.nextCursor,
  });
  _flushActionLogs_();
  return { symbols: win.processed, total: win.total, added, nextCursor: win.nextCursor, wrapped: win.wrapped };
}

function syncWatchlistCandleCacheFull() {
  const wl = getWatchlist();
  if (!wl.length) return { symbols: 0, added: 0 };
  let added = 0;
  wl.forEach((w, i) => {
    added += syncCandlesToCache(w.symbol, w.exchange, w.segment, "15m", 90).added;
    added += syncCandlesToCache(w.symbol, w.exchange, w.segment, "1h", 180).added;
    added += syncCandlesToCache(w.symbol, w.exchange, w.segment, "1d", 1080).added;
    if (i % 2 === 0) Utilities.sleep(180);
  });
  LOG("INFO", "Cache", `Full candle sync added=${added} symbols=${wl.length}`);
  return { symbols: wl.length, added };
}

function DECISION(stage, symbol, decision, reason, ctx) {
  try {
    const cache = CacheService.getScriptCache();
    const key = "decision_buf";
    const existing = cache.get(key);
    const arr = existing ? JSON.parse(existing) : [];
    arr.push([
      nowIST(),
      String(stage || ""),
      String(symbol || ""),
      String(decision || ""),
      String(reason || ""),
      JSON.stringify(ctx || {}).substring(0, 900),
      todayIST(),
    ]);
    cache.put(key, JSON.stringify(arr), 300);
    if (arr.length >= 20) _flushDecisionLogs_();
  } catch (e) {}
}

function _flushDecisionLogs_() {
  const cache = CacheService.getScriptCache();
  const key = "decision_buf";
  const existing = cache.get(key);
  if (!existing) return;
  cache.remove(key);
  const rows = JSON.parse(existing) || [];
  if (!rows.length) return;
  const sh = _decisionSheet_();
  sh.getRange(sh.getLastRow() + 1, 1, rows.length, rows[0].length).setValues(rows);
}

function _historyFileName_(symbol, exchange, segment, tf) {
  const tfNorm = _normHistoryTfArg_(tf, "1d");
  return `${String(exchange || "NSE").toUpperCase()}_${String(segment || "CASH").toUpperCase()}_${String(symbol || "").toUpperCase()}_${tfNorm}.json`;
}

function _historyFileByName_(folder, fileName) {
  return _retryDriveStorage_(function() {
    const files = folder.getFilesByName(fileName);
    return files.hasNext() ? files.next() : null;
  }, `getFilesByName(${fileName})`, 4);
}

function _readHistoryCandles_(file) {
  if (!file) return [];
  try {
    const txt = _retryDriveStorage_(function() {
      return file.getBlob().getDataAsString("UTF-8");
    }, `read(${file.getName ? file.getName() : "history"})`, 4);
    const arr = JSON.parse(txt);
    return Array.isArray(arr) ? arr : [];
  } catch (e) {
    return [];
  }
}

function _writeHistoryCandles_(folder, fileName, candles) {
  const payload = JSON.stringify(candles);
  return _retryDriveStorage_(function() {
    const file = _historyFileByName_(folder, fileName);
    if (file) {
      file.setContent(payload);
      return file;
    }
    return folder.createFile(fileName, payload, MimeType.PLAIN_TEXT);
  }, `write(${fileName})`, 5);
}

function _appendHistoryCandles_(symbol, exchange, segment, tf, candles) {
  const folder = _historyFolder_();
  const fileName = _historyFileName_(symbol, exchange, segment, tf);
  const file = _historyFileByName_(folder, fileName);
  const existing = _readHistoryCandles_(file);
  const byTs = {};
  existing.forEach(c => {
    const n = _normCandle_(c);
    if (!n) return;
    byTs[String(n[0])] = n;
  });
  candles.forEach(c => {
    const n = _normCandle_(c);
    if (!n) return;
    byTs[String(n[0])] = n;
  });
  const merged = Object.keys(byTs).map(k => byTs[k]).sort((a, b) => _tsCmp_(a[0], b[0]));
  _writeHistoryCandles_(folder, fileName, merged);
  const lastTs = merged.length ? String(merged[merged.length - 1][0]) : "";
  return { fileName, total: merged.length, lastTs, added: Math.max(0, merged.length - existing.length) };
}

function _fetchHistoryWindowed_(symbol, exchange, segment, tf, lastTs = "") {
  const spec = _historySpec_(tf);
  const now = new Date();
  const lastDt = _tsToDate_(lastTs);
  let start = lastDt ? new Date(lastDt.getTime() + 1000) : new Date(now.getTime() - spec.availableDays * 86400000);
  if (!isFinite(start.getTime()) || start >= now) return [];

  const all = [];
  const seen = {};
  while (start < now) {
    const end = new Date(Math.min(now.getTime(), start.getTime() + spec.windowDays * 86400000));
    const part = fetchCandlesRangeDirect_(symbol, exchange, segment, tf, start, end) || [];
    part.forEach(c => {
      const n = _normCandle_(c);
      if (!n) return;
      const key = String(n[0]);
      if (seen[key]) return;
      seen[key] = true;
      all.push(n);
    });
    start = new Date(end.getTime() + 1000);
    Utilities.sleep(100);
  }
  all.sort((a, b) => _tsCmp_(a[0], b[0]));
  return all;
}

function initUniverseHistoryBackfill(tf = "1d", reset = false) {
  const targetTf = _normHistoryTfArg_(tf, "1d");
  const resetFlag = _isPlainObjectArg_(reset) ? false : !!reset;
  const startedAt = Date.now();
  ACTION("DataEngine", "initUniverseHistoryBackfill", "START", "", { tf: targetTf, reset: resetFlag });
  ensureCoreSheets_();
  const sh = _backfillSheet_();
  const existing = sh.getDataRange().getValues().slice(3);
  const map = {};
  for (let i = 0; i < existing.length; i++) {
    const r = existing[i];
    const k = `${String(r[0] || "").toUpperCase()}|${String(r[1] || "").toUpperCase()}|${String(r[2] || "").toUpperCase()}|${String(r[3] || "").toLowerCase()}`;
    map[k] = { row: i + 4, data: r };
  }

  const uni = _readUniverseRows_();
  if (!uni.length) throw new Error("Universe sheet is empty or all rows disabled");
  const out = [];
  PropertiesService.getScriptProperties().deleteProperty(`runtime:history_ready:${targetTf}`);
  uni.forEach(u => {
    const key = `${u.symbol}|${u.exchange}|${u.segment}|${targetTf}`;
    const hit = map[key];
    const status = (hit && !resetFlag) ? String(hit.data[7] || "PENDING") : "PENDING";
    const attempts = (hit && !resetFlag) ? Number(hit.data[8] || 0) : 0;
    const lastTs = (hit && !resetFlag) ? String(hit.data[5] || "") : "";
    const bars = (hit && !resetFlag) ? Number(hit.data[6] || 0) : 0;
    const fileName = _historyFileName_(u.symbol, u.exchange, u.segment, targetTf);
    const isin = (String(u.notes || "").match(/isin=([A-Z0-9]+)/i) || [])[1] || "";
    out.push([u.symbol, u.exchange, u.segment, targetTf, "Y", lastTs, bars, status, attempts, "", nowIST(), fileName, isin, u.sector || "UNKNOWN"]);
  });

  const last = sh.getLastRow();
  if (last > 3) sh.getRange(4, 1, last - 3, 14).clearContent();
  if (out.length) sh.getRange(4, 1, out.length, out[0].length).setValues(out);
  LOG("INFO", "Backfill", `Queue initialized rows=${out.length} tf=${targetTf}`);
  ACTION("DataEngine", "initUniverseHistoryBackfill", "DONE", "queue initialized", {
    ms: Date.now() - startedAt,
    rows: out.length,
    tf: targetTf,
    reset: resetFlag,
  });
  _flushActionLogs_();
  return out.length;
}

function processUniverseHistoryBackfillBatch(batchSize = 3, tf = "1d") {
  const targetTf = _normHistoryTfArg_(tf, "1d");
  const startedAt = Date.now();
  ACTION("DataEngine", "processUniverseHistoryBackfillBatch", "START", "", { batchSize: _numArg_(batchSize, 0, 0, 500), tf: targetTf });
  ensureCoreSheets_();
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(25000)) {
    ACTION("DataEngine", "processUniverseHistoryBackfillBatch", "SKIP", "lock busy", { ms: Date.now() - startedAt });
    _flushActionLogs_();
    return { processed: 0, pending: -1 };
  }
  try {
    const sh = _backfillSheet_();
    const rows = sh.getDataRange().getValues();
    if (rows.length < 4) {
      ACTION("DataEngine", "processUniverseHistoryBackfillBatch", "DONE", "no rows", { ms: Date.now() - startedAt, processed: 0, pending: 0 });
      _flushActionLogs_();
      return { processed: 0, pending: 0 };
    }

    const propBatch = Number(PropertiesService.getScriptProperties().getProperty("HISTORY_BACKFILL_BATCH") || 0);
    const reqBatch = _numArg_(batchSize, 0, 0, 500);
    const targetBatch = reqBatch > 0 ? reqBatch : (isFinite(propBatch) && propBatch > 0 ? Math.floor(propBatch) : 3);
    const picks = [];
    for (let i = 3; i < rows.length; i++) {
      const r = rows[i];
      if (String(r[4] || "Y").toUpperCase() !== "Y") continue;
      if (String(r[3] || "").toLowerCase() !== targetTf) continue;
      const status = String(r[7] || "PENDING").toUpperCase();
      const attempts = Number(r[8] || 0);
      if (status === "DONE" || attempts >= 5) continue;
      picks.push({ idx: i + 1, row: r });
      if (picks.length >= targetBatch) break;
    }

    let processed = 0;
    for (const p of picks) {
      const r = p.row;
      const symbol = String(r[0] || "").toUpperCase();
      const exchange = String(r[1] || "NSE").toUpperCase();
      const segment = String(r[2] || "CASH").toUpperCase();
      const lastTs = String(r[5] || "");
      const attempts = Number(r[8] || 0);

      sh.getRange(p.idx, 8, 1, 4).setValues([["IN_PROGRESS", attempts + 1, "", nowIST()]]);
      try {
        const candles = _fetchHistoryWindowed_(symbol, exchange, segment, targetTf, lastTs);
        const merged = _appendHistoryCandles_(symbol, exchange, segment, targetTf, candles);
        sh.getRange(p.idx, 6, 1, 7).setValues([[merged.lastTs, merged.total, "DONE", attempts + 1, "", nowIST(), merged.fileName]]);
        DECISION("BACKFILL", symbol, "DONE", `${targetTf}_history_saved`, { bars: merged.total, file: merged.fileName });
        processed++;
      } catch (e) {
        const err = String(e && e.message ? e.message : e).substring(0, 220);
        sh.getRange(p.idx, 8, 1, 4).setValues([["ERROR", attempts + 1, err, nowIST()]]);
        LOG("WARN", "Backfill", `${symbol} ${targetTf}: ${err}`);
      }
      Utilities.sleep(120);
    }

    const data = sh.getDataRange().getValues().slice(3);
    const tfRows = data.filter(r => String(r[3] || "").toLowerCase() === targetTf && String(r[4] || "Y").toUpperCase() === "Y");
    const pending = tfRows.filter(r => String(r[7] || "").toUpperCase() !== "DONE").length;
    const malformedTfRows = data.filter(r => String(r[3] || "").toLowerCase().indexOf("[object object]") >= 0).length;
    if (!tfRows.length) {
      LOG("WARN", "Backfill", `No queue rows found for tf=${targetTf}. Queue may be malformed. malformedTfRows=${malformedTfRows}`);
      PropertiesService.getScriptProperties().deleteProperty(`runtime:history_ready:${targetTf}`);
    } else if (pending === 0) {
      const props = PropertiesService.getScriptProperties();
      const readyKey = `runtime:history_ready:${targetTf}`;
      const alreadyReady = props.getProperty(readyKey) === "1";
      props.setProperty(readyKey, "1");
      if (!alreadyReady) DECISION("BACKFILL", "UNIVERSE", "COMPLETE", `${targetTf}_history_ready`, {});
    }
    ACTION("DataEngine", "processUniverseHistoryBackfillBatch", "DONE", "batch processed", {
      ms: Date.now() - startedAt,
      processed,
      pending,
      tf: targetTf,
      queueRows: tfRows.length,
      malformedTfRows,
    });
    _flushActionLogs_();
    return { processed, pending };
  } finally {
    lock.releaseLock();
  }
}

function refreshUniverseHistoryIncremental(batchSize = 80, tf = "1d") {
  const targetTf = _normHistoryTfArg_(tf, "1d");
  const startedAt = Date.now();
  ACTION("DataEngine", "refreshUniverseHistoryIncremental", "START", "", { batchSize: _numArg_(batchSize, 0, 0, 5000), tf: targetTf });
  ensureCoreSheets_();
  const sh = _backfillSheet_();
  const data = sh.getDataRange().getValues();
  if (data.length < 4) {
    ACTION("DataEngine", "refreshUniverseHistoryIncremental", "DONE", "no rows", { ms: Date.now() - startedAt, processed: 0 });
    _flushActionLogs_();
    return { processed: 0 };
  }
  const propBatch = Number(PropertiesService.getScriptProperties().getProperty("HISTORY_REFRESH_BATCH") || 0);
  const reqBatch = _numArg_(batchSize, 0, 0, 5000);
  const targetBatch = reqBatch > 0 ? reqBatch : (isFinite(propBatch) && propBatch > 0 ? Math.floor(propBatch) : 80);
  const matchingRows = data.slice(3).filter(r => String(r[3] || "").toLowerCase() === targetTf && String(r[4] || "Y").toUpperCase() === "Y").length;
  if (!matchingRows) {
    LOG("WARN", "Backfill", `Incremental refresh skipped: no queue rows for tf=${targetTf}`);
    ACTION("DataEngine", "refreshUniverseHistoryIncremental", "DONE", "no matching queue rows", {
      ms: Date.now() - startedAt,
      processed: 0,
      tf: targetTf,
      queueRows: 0,
    });
    _flushActionLogs_();
    return { processed: 0, queueRows: 0 };
  }
  let processed = 0;
  for (let i = 3; i < data.length; i++) {
    if (processed >= targetBatch) break;
    const r = data[i];
    if (String(r[4] || "Y").toUpperCase() !== "Y") continue;
    if (String(r[3] || "").toLowerCase() !== targetTf) continue;
    if (String(r[7] || "").toUpperCase() !== "DONE") continue;

    const symbol = String(r[0] || "").toUpperCase();
    const exchange = String(r[1] || "NSE").toUpperCase();
    const segment = String(r[2] || "CASH").toUpperCase();
    const lastTs = String(r[5] || "");
    try {
      const candles = _fetchHistoryWindowed_(symbol, exchange, segment, targetTf, lastTs);
      if (candles.length) {
        const merged = _appendHistoryCandles_(symbol, exchange, segment, targetTf, candles);
        sh.getRange(i + 1, 6, 1, 7).setValues([[merged.lastTs, merged.total, "DONE", Number(r[8] || 0), "", nowIST(), merged.fileName]]);
      } else {
        sh.getRange(i + 1, 11).setValue(nowIST());
      }
    } catch (e) {
      const err = String(e && e.message ? e.message : e).substring(0, 220);
      sh.getRange(i + 1, 10).setValue(err);
      sh.getRange(i + 1, 11).setValue(nowIST());
    }
    processed++;
    Utilities.sleep(60);
  }
  LOG("INFO", "Backfill", `Incremental refresh tf=${targetTf} processed=${processed}`);
  ACTION("DataEngine", "refreshUniverseHistoryIncremental", "DONE", "incremental refresh complete", {
    ms: Date.now() - startedAt,
    processed,
    tf: targetTf,
  });
  _flushActionLogs_();
  return { processed };
}

function historyBackfillProgress(tf = "1d") {
  ensureCoreSheets_();
  const targetTf = _normHistoryTfArg_(tf, "1d");
  const rows = _backfillSheet_().getDataRange().getValues().slice(3);
  const data = rows.filter(r => String(r[3] || "").toLowerCase() === targetTf && String(r[4] || "Y").toUpperCase() === "Y");
  const total = data.length;
  const done = data.filter(r => String(r[7] || "").toUpperCase() === "DONE").length;
  const error = data.filter(r => String(r[7] || "").toUpperCase() === "ERROR").length;
  const malformedTfRows = rows.filter(r => String(r[3] || "").toLowerCase().indexOf("[object object]") >= 0).length;
  return { tf: targetTf, total, done, pending: Math.max(0, total - done), error, malformedTfRows };
}
