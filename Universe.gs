// Universe.gs — instruments sync + diversified smart watchlist

const GROWW_INSTRUMENT_CSV_URLS = [
  "https://growwapi-assets.groww.in/instruments/instrument.csv", // current docs URL
  "https://assets.groww.in/instruments/instrument.csv",          // legacy fallback
];

const UNIVERSE = [
  "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","KOTAKBANK",
  "BHARTIARTL","ITC","LT","SBIN","AXISBANK","ASIANPAINT","MARUTI","TITAN",
  "BAJFINANCE","WIPRO","HCLTECH","NESTLEIND","ULTRACEMCO","POWERGRID","NTPC",
  "JSWSTEEL","TATAMOTORS","SUNPHARMA","CIPLA","DRREDDY","DIVISLAB","ADANIPORTS",
  "COALINDIA","BPCL","ONGC","TATASTEEL","HINDALCO","GRASIM","TECHM","INDUSINDBK",
  "EICHERMOT","BAJAJFINSV","BRITANNIA","APOLLOHOSP","TATACONSUM","ADANIENT",
  "HEROMOTOCO","SBILIFE","HDFCLIFE","M&M","UPL","DMART","LTIM",
  "PIDILITIND","DABUR","MARICO","GODREJCP","COLPAL","BERGEPAINT","HAVELLS",
  "VOLTAS","LUPIN","TORNTPHARM","GLAXO","BIOCON","MCDOWELL-N","UNITDSPR",
  "AUROPHARMA","SIEMENS","ABB","BOSCHLTD","CUMMINSIND","MOTHERSON","BALKRISIND",
  "MFSL","ICICIPRULI","GICRE","NIACL","BANKBARODA","PNB","CANBK","FEDERALBNK"
];

const UNIVERSE_SCAN_CURSOR_PROP = "runtime:universe_scan_cursor";
const WATCHLIST_SCREEN_BATCH_PROP = "WATCHLIST_SCREEN_BATCH";
const WATCHLIST_TARGET_SIZE_PROP = "WATCHLIST_TARGET_SIZE";
const WATCHLIST_BUILD_MAX_MS_PROP = "WATCHLIST_BUILD_MAX_MS";
const WATCHLIST_REGIME_MODE_PROP = "WATCHLIST_REGIME_MODE"; // FAST | LIVE
const WATCHLIST_SCORE_FRESH_HOURS_PROP = "WATCHLIST_SCORE_FRESH_HOURS";
const WATCHLIST_SCORE_API_CAP_PROP = "WATCHLIST_SCORE_API_CAP";
const WATCHLIST_SCORE_LOOKBACK_DAYS_PROP = "WATCHLIST_SCORE_LOOKBACK_DAYS";
const WATCHLIST_SCORE_MIN_BARS_PROP = "WATCHLIST_SCORE_MIN_BARS";
const WATCHLIST_READY_DAY_PROP = "watchlist_ready_day";
const WATCHLIST_READY_TS_PROP = "watchlist_ready_ts";
const WATCHLIST_READY_COVERAGE_PROP = "watchlist_ready_coverage";
const PREMARKET_SCORE_BATCH_PROP = "PREMARKET_SCORE_BATCH";
const PREMARKET_SCORE_API_CAP_PROP = "PREMARKET_SCORE_API_CAP";
const PREMARKET_SCORE_MAX_MS_PROP = "PREMARKET_SCORE_MAX_MS";
const PREMARKET_SCORE_START_MINS_PROP = "PREMARKET_SCORE_START_MINS";
const PREMARKET_SCORE_END_MINS_PROP = "PREMARKET_SCORE_END_MINS";
const DEFAULT_WATCHLIST_SCREEN_BATCH = 12;
const DEFAULT_WATCHLIST_TARGET_SIZE = 200;
const DEFAULT_WATCHLIST_BUILD_MAX_MS = 90000;
const DEFAULT_WATCHLIST_REGIME_MODE = "FAST";
const DEFAULT_WATCHLIST_SCORE_FRESH_HOURS = 18;
const DEFAULT_WATCHLIST_SCORE_API_CAP = 4;
const DEFAULT_WATCHLIST_SCORE_LOOKBACK_DAYS = 700;
const DEFAULT_WATCHLIST_SCORE_MIN_BARS = 320;
const DEFAULT_PREMARKET_SCORE_BATCH = 200;
const DEFAULT_PREMARKET_SCORE_API_CAP = 40;
const DEFAULT_PREMARKET_SCORE_MAX_MS = 240000;
const DEFAULT_PREMARKET_SCORE_START_MINS = 315; // 05:15 IST
const DEFAULT_PREMARKET_SCORE_END_MINS = 555; // 09:15 IST
const WATCHLIST_BATCH_MIN_SIZE = 4;
const WATCHLIST_BATCH_MAX_SIZE = 80;
const UNIVERSE_SCORE_HEADERS = ["Score", "RSI", "Vol Ratio", "Last Scanned", "Last Product", "Last Strategy", "Last Note"];
const UNIVERSE_RAW_COL = 19; // trailing column after score block (L:R)
const UNIVERSE_RAW_HEADER = "Raw CSV (JSON)";
const UNIVERSE_SECTOR_SOURCE_COL = 20; // col T
const UNIVERSE_SECTOR_UPDATED_COL = 21; // col U
const UNIVERSE_SECTOR_SOURCE_HEADER = "Sector Source";
const UNIVERSE_SECTOR_UPDATED_HEADER = "Sector Updated At";

function _parseCsvLine_(line) {
  const out = [];
  let curr = "";
  let q = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (q && line[i + 1] === '"') {
        curr += '"';
        i++;
      } else q = !q;
      continue;
    }
    if (ch === "," && !q) {
      out.push(curr);
      curr = "";
      continue;
    }
    curr += ch;
  }
  out.push(curr);
  return out;
}

function _normalizeHdr_(s) {
  return String(s || "").trim().toLowerCase().replace(/[^a-z0-9]+/g, "_");
}

function _col_(idx, name, row) {
  const i = idx[name];
  return i == null ? "" : String(row[i] || "").trim();
}

function _csvRawPayload_(headers, row) {
  const payload = {};
  for (let i = 0; i < headers.length; i++) {
    const key = String(headers[i] || "").trim() || `col_${i + 1}`;
    payload[key] = row[i] == null ? "" : String(row[i]);
  }
  return JSON.stringify(payload);
}

function _tradableFlag_(v, fallback = true) {
  if (v === "") return fallback;
  const s = String(v).trim().toLowerCase();
  return s === "1" || s === "true" || s === "y" || s === "yes";
}

function _loadUniverseSettings_() {
  ensureCoreSheets_();
  const sh = SS.getSheetByName(SH.UNIVERSE);
  const data = sh.getDataRange().getValues().slice(3);
  const bySymbol = {};
  const byIsin = {};
  data.forEach(r => {
    const symbol = String(r[1] || "").toUpperCase().trim();
    if (!symbol) return;
    const notes = String(r[10] || "");
    const isin = (notes.match(/isin=([A-Z0-9]+)/i) || [])[1] || "";
    const item = {
      allowedProduct: String(r[4] || "BOTH").toUpperCase(),
      strategy: String(r[5] || "AUTO").toUpperCase(),
      sector: String(r[6] || "").trim(),
      beta: parseFloat(r[7]) || 1.0,
      enabled: String(r[8] || "Y").toUpperCase(),
      priority: parseFloat(r[9]) || 0,
      notes: notes,
      score: parseFloat(r[11]) || 0,
      lastRSI: parseFloat(r[12]) || 0,
      lastVolRatio: parseFloat(r[13]) || 0,
      lastScanned: String(r[14] || ""),
      lastProduct: String(r[15] || "").toUpperCase(),
      lastStrategy: String(r[16] || "").toUpperCase(),
      lastNote: String(r[17] || ""),
      sectorSource: String(r[UNIVERSE_SECTOR_SOURCE_COL - 1] || "").trim(),
      sectorUpdatedAt: String(r[UNIVERSE_SECTOR_UPDATED_COL - 1] || "").trim(),
    };
    bySymbol[symbol] = item;
    if (isin) byIsin[isin.toUpperCase()] = item;
  });
  return { bySymbol, byIsin };
}

function _ensureUniverseScoreColumns_() {
  const sh = SS.getSheetByName(SH.UNIVERSE);
  const startCol = 12; // col L onwards
  const curr = sh.getRange(3, startCol, 1, UNIVERSE_SCORE_HEADERS.length).getValues()[0];
  let need = false;
  for (let i = 0; i < UNIVERSE_SCORE_HEADERS.length; i++) {
    if (String(curr[i] || "").trim() !== UNIVERSE_SCORE_HEADERS[i]) {
      need = true;
      break;
    }
  }
  if (need) sh.getRange(3, startCol, 1, UNIVERSE_SCORE_HEADERS.length).setValues([UNIVERSE_SCORE_HEADERS]);
  const rawHdr = String(sh.getRange(3, UNIVERSE_RAW_COL).getValue() || "").trim();
  if (rawHdr !== UNIVERSE_RAW_HEADER) sh.getRange(3, UNIVERSE_RAW_COL).setValue(UNIVERSE_RAW_HEADER);
  const srcHdr = String(sh.getRange(3, UNIVERSE_SECTOR_SOURCE_COL).getValue() || "").trim();
  if (srcHdr !== UNIVERSE_SECTOR_SOURCE_HEADER) sh.getRange(3, UNIVERSE_SECTOR_SOURCE_COL).setValue(UNIVERSE_SECTOR_SOURCE_HEADER);
  const updHdr = String(sh.getRange(3, UNIVERSE_SECTOR_UPDATED_COL).getValue() || "").trim();
  if (updHdr !== UNIVERSE_SECTOR_UPDATED_HEADER) sh.getRange(3, UNIVERSE_SECTOR_UPDATED_COL).setValue(UNIVERSE_SECTOR_UPDATED_HEADER);
}

function _preferExchange_(curr, next) {
  if (!curr) return next;
  if (curr.exchange === "NSE") return curr;
  if (next.exchange === "NSE") return next;
  return curr;
}

function _downloadInstrumentsCsv_() {
  for (let i = 0; i < GROWW_INSTRUMENT_CSV_URLS.length; i++) {
    const url = GROWW_INSTRUMENT_CSV_URLS[i];
    try {
      const resp = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
      if (resp.getResponseCode() !== 200) continue;
      const body = String(resp.getContentText() || "").trim();
      if (body) return body;
    } catch (e) {}
  }
  throw new Error("Instrument CSV fetch failed from all configured URLs");
}

function syncUniverseFromGrowwInstruments(limit = 0, appendOnly = false) {
  const startedAt = Date.now();
  const appendMode = !!appendOnly;
  ACTION("Universe", "syncUniverseFromGrowwInstruments", "START", "sync started", { limit: Number(limit) || 0, appendOnly: appendMode });
  ensureCoreSheets_();
  const body = _downloadInstrumentsCsv_();
  const props = PropertiesService.getScriptProperties();

  const lines = body.split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) throw new Error("Instrument CSV has no data rows");

  const headers = _parseCsvLine_(lines[0]).map(_normalizeHdr_);
  const idx = {};
  headers.forEach((h, i) => idx[h] = i);
  const old = _loadUniverseSettings_();
  const srcMaxRowsProp = Number(props.getProperty("UNIVERSE_MAX_SOURCE_ROWS") || 0);
  const srcMaxRows = isFinite(srcMaxRowsProp) && srcMaxRowsProp > 0 ? Math.floor(srcMaxRowsProp) : 0;

  const dedup = {};
  for (let i = 1; i < lines.length; i++) {
    if (srcMaxRows > 0 && i > srcMaxRows) break;
    const row = _parseCsvLine_(lines[i]);
    const exchangeSeg = _col_(idx, "exchange_segment", row).toUpperCase();
    const exchange = (_col_(idx, "exchange", row) || exchangeSeg.split("_")[0]).toUpperCase();
    const segment = (_col_(idx, "segment", row) || exchangeSeg.split("_")[1]).toUpperCase();
    const symbol = (_col_(idx, "trading_symbol", row) || _col_(idx, "symbol", row)).toUpperCase();
    const instrumentType = (_col_(idx, "instrument_type", row) || _col_(idx, "instrument", row)).toUpperCase();
    const series = _col_(idx, "series", row).toUpperCase();
    const isin = _col_(idx, "isin", row).toUpperCase();
    const name = _col_(idx, "name", row) || _col_(idx, "company_name", row);
    const buyAllowed = _tradableFlag_(_col_(idx, "buy_allowed", row));
    const sellAllowed = _tradableFlag_(_col_(idx, "sell_allowed", row));
    const enabled = _tradableFlag_(_col_(idx, "is_enabled", row));
    const delisted = _tradableFlag_(_col_(idx, "is_delisted", row), false);
    const reserved = _tradableFlag_(_col_(idx, "is_reserved", row), false);

    if (!symbol) continue;
    if (exchange !== "NSE" && exchange !== "BSE") continue;
    if (segment !== "CASH") continue;
    if (instrumentType && instrumentType !== "EQ") continue;
    if (series && series !== "EQ") continue;
    if (!buyAllowed || !sellAllowed || !enabled || delisted || reserved) continue;

    const key = isin || symbol;
    const cand = {
      symbol,
      exchange,
      segment: "CASH",
      isin,
      name,
      sector: "",
      rawCsv: _csvRawPayload_(headers, row),
    };
    dedup[key] = _preferExchange_(dedup[key], cand);
  }

  const rows = Object.keys(dedup).map(key => {
    const d = dedup[key];
    const settings = old.byIsin[d.isin] || old.bySymbol[d.symbol] || {};
    const isNew = !(old.byIsin[d.isin] || old.bySymbol[d.symbol]);
    const sector = "UNKNOWN";
    const sectorSource = "";
    const sectorUpdatedAt = "";
    const notesBase = `isin=${d.isin || ""}|name=${d.name || d.symbol}|source=groww_csv`;
    const userNote = (String(settings.notes || "").match(/user=([^|]+)/) || [])[1] || "";
    const notes = userNote ? `${notesBase}|user=${userNote}` : notesBase;
    return {
      symbol: d.symbol,
      exchange: d.exchange,
      segment: "CASH",
      allowedProduct: settings.allowedProduct || "BOTH",
      strategy: settings.strategy || "AUTO",
      sector,
      beta: isFinite(settings.beta) ? settings.beta : 1.0,
      enabled: settings.enabled || "Y",
      priority: isFinite(settings.priority) ? settings.priority : 0,
      notes,
      isin: d.isin || "",
      score: isFinite(settings.score) ? Number(settings.score) : 0,
      lastRSI: isFinite(settings.lastRSI) ? Number(settings.lastRSI) : 0,
      lastVolRatio: isFinite(settings.lastVolRatio) ? Number(settings.lastVolRatio) : 0,
      lastScanned: String(settings.lastScanned || ""),
      lastProduct: String(settings.lastProduct || "").toUpperCase(),
      lastStrategy: String(settings.lastStrategy || "").toUpperCase(),
      lastNote: String(settings.lastNote || ""),
      rawCsv: d.rawCsv || "",
      sectorSource,
      sectorUpdatedAt,
      isNew,
    };
  });

  rows.sort((a, b) => {
    const e = (a.exchange === b.exchange) ? 0 : (a.exchange === "NSE" ? -1 : 1);
    if (e !== 0) return e;
    return a.symbol.localeCompare(b.symbol);
  });

  const propLimit = Number(props.getProperty("UNIVERSE_SYNC_MAX_ROWS") || 0);
  const finalLimit = Number(limit) > 0 ? Number(limit) : (isFinite(propLimit) && propLimit > 0 ? propLimit : 0);
  const capped = finalLimit > 0 ? rows.slice(0, finalLimit) : rows;
  const sh = SS.getSheetByName(SH.UNIVERSE);
  const width = Math.max(UNIVERSE_SECTOR_UPDATED_COL, sh.getLastColumn());
  const toRowVals = (r, rowNum) => ([
    rowNum,
    r.symbol,
    r.exchange,
    r.segment,
    r.allowedProduct,
    r.strategy,
    r.sector,
    r.beta,
    r.enabled,
    r.priority,
    r.notes,
    r.score,
    r.lastRSI,
    r.lastVolRatio,
    r.lastScanned,
    r.lastProduct,
    r.lastStrategy,
    r.lastNote,
    r.rawCsv,
    r.sectorSource,
    r.sectorUpdatedAt,
  ]);
  let out = [];
  let appended = 0;
  let totalRowsAfter = 0;
  if (appendMode) {
    const existingData = sh.getDataRange().getValues().slice(3);
    const existingCount = existingData.filter(r => String(r[1] || "").trim()).length;
    const newRows = capped.filter(r => !!r.isNew);
    out = newRows.map((r, i) => toRowVals(r, existingCount + i + 1));
    if (out.length) {
      sh.getRange(sh.getLastRow() + 1, 1, out.length, out[0].length).setValues(out);
      appended = out.length;
    }
    totalRowsAfter = existingCount + appended;
  } else {
    const last = sh.getLastRow();
    if (last > 3) sh.getRange(4, 1, last - 3, width).clearContent();
    out = capped.map((r, i) => toRowVals(r, i + 1));
    if (out.length) sh.getRange(4, 1, out.length, out[0].length).setValues(out);
    appended = capped.filter(r => !!r.isNew).length;
    totalRowsAfter = out.length;
  }
  _ensureUniverseScoreColumns_();
  props.setProperty(UNIVERSE_SCAN_CURSOR_PROP, "0");
  if (!appendMode || appended > 0) _markWatchlistNotReady_("universe_sync");
  const prioritySymbols = capped.filter(r => r.isNew).map(r => r.symbol);
  LOG(
    "INFO",
    "Universe",
    appendMode
      ? `Eligible universe incremental sync total=${totalRowsAfter}, appended=${appended} (NSE preferred, BSE fallback, CASH EQ only), sector=disabled`
      : `Eligible universe synced rows=${out.length} (NSE preferred, BSE fallback, CASH EQ only), sector=disabled`
  );
  ACTION("Universe", "syncUniverseFromGrowwInstruments", "DONE", "sync complete", {
    ms: Date.now() - startedAt,
    rows: appendMode ? totalRowsAfter : out.length,
    uniqueCompanies: rows.length,
    newSymbols: prioritySymbols.length,
    appended: appendMode ? appended : undefined,
    appendOnly: appendMode,
    sectorDisabled: true,
  });
  _flushActionLogs_();
  return appendMode
    ? { rows: totalRowsAfter, appended, uniqueCompanies: rows.length, sectorDisabled: true }
    : { rows: out.length, uniqueCompanies: rows.length, sectorDisabled: true };
}

function syncUniverseFromGrowwInstrumentsDaily(limit = 0) {
  return syncUniverseFromGrowwInstruments(limit, true);
}

function enrichUniverseSectorsFromNseBatch(batchSize = 0, prioritySymbols = []) {
  const startedAt = Date.now();
  ACTION("Universe", "enrichUniverseSectorsFromNseBatch", "SKIP", "sector enrichment disabled", {
    batchSize: Number(batchSize) || 0,
    priority: Array.isArray(prioritySymbols) ? prioritySymbols.length : 0,
  });
  LOG("INFO", "UniverseSector", "Sector enrichment disabled");
  _flushActionLogs_();
  return {
    total: 0,
    candidates: 0,
    enriched: 0,
    missed: 0,
    errors: 0,
    writes: 0,
    timedOut: false,
    nextCursor: 0,
    disabled: true,
    ms: Date.now() - startedAt,
  };
}

function enrichUniverseSectorsFromNseAll(maxBatches = 3, batchSize = 0) {
  const out = {
    runs: 0,
    candidates: 0,
    enriched: 0,
    missed: 0,
    errors: 0,
    writes: 0,
    timedOut: false,
    disabled: true,
    requestedBatches: Math.max(1, Math.min(30, Number(maxBatches) || 3)),
    requestedBatchSize: Number(batchSize) || 0,
  };
  LOG("INFO", "UniverseSector", "Sector enrichment disabled");
  return out;
}

function repairUniverseBlankMetrics(batchSize = 800) {
  const startedAt = Date.now();
  ACTION("Universe", "repairUniverseBlankMetrics", "START", "", { batchSize: Number(batchSize) || 0 });
  ensureCoreSheets_();
  _ensureUniverseScoreColumns_();
  const sh = SS.getSheetByName(SH.UNIVERSE);
  const last = sh.getLastRow();
  if (last < 4) {
    ACTION("Universe", "repairUniverseBlankMetrics", "DONE", "no rows", { ms: Date.now() - startedAt, updated: 0, total: 0 });
    _flushActionLogs_();
    return { updated: 0, total: 0 };
  }

  const width = Math.max(UNIVERSE_SECTOR_UPDATED_COL, sh.getLastColumn(), 18);
  const total = last - 3;
  const bs = Math.max(100, Number(batchSize) || 800);
  let updated = 0;

  for (let start = 0; start < total; start += bs) {
    const len = Math.min(bs, total - start);
    const rows = sh.getRange(4 + start, 1, len, width).getValues();
    let changedAny = false;
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      const symbol = String(r[1] || "").trim();
      if (!symbol) continue;

      if (!String(r[6] || "").trim()) {
        r[6] = "UNKNOWN";
        changedAny = true;
        updated++;
      }
      if (r[11] === "" || r[11] == null || !isFinite(Number(r[11]))) {
        r[11] = 0;
        changedAny = true;
      }
      if (r[12] === "" || r[12] == null || !isFinite(Number(r[12]))) {
        r[12] = 0;
        changedAny = true;
      }
      if (r[13] === "" || r[13] == null || !isFinite(Number(r[13]))) {
        r[13] = 0;
        changedAny = true;
      }
    }
    if (changedAny) sh.getRange(4 + start, 1, len, width).setValues(rows);
    if (start && start % (bs * 3) === 0) Utilities.sleep(50);
  }

  LOG("INFO", "Universe", `Universe blank repair complete updated=${updated} total=${total}`);
  ACTION("Universe", "repairUniverseBlankMetrics", "DONE", "repair complete", {
    ms: Date.now() - startedAt,
    updated,
    total,
  });
  _flushActionLogs_();
  return { updated, total };
}

function seedUniverseSheetFromStaticList(force = false) {
  ensureCoreSheets_();
  const sh = SS.getSheetByName(SH.UNIVERSE);
  const existing = sh.getDataRange().getValues().slice(3).filter(r => String(r[1] || "").trim());
  if (existing.length && !force) return existing.length;

  const last = sh.getLastRow();
  if (last > 3) sh.getRange(4, 1, last - 3, Math.max(sh.getLastColumn(), UNIVERSE_SECTOR_UPDATED_COL, 11)).clearContent();
  const rows = UNIVERSE.map((sym, i) => [i + 1, sym, "NSE", "CASH", "BOTH", "AUTO", "UNKNOWN", 1.0, "Y", 0, "source=seed"]);
  if (rows.length) sh.getRange(4, 1, rows.length, rows[0].length).setValues(rows);
  _ensureUniverseScoreColumns_();
  PropertiesService.getScriptProperties().setProperty(UNIVERSE_SCAN_CURSOR_PROP, "0");
  LOG("INFO", "Universe", `Seeded ${rows.length} fallback symbols`);
  return rows.length;
}

function _readUniverseRows_() {
  ensureCoreSheets_();
  _ensureUniverseScoreColumns_();
  const sh = SS.getSheetByName(SH.UNIVERSE);
  const data = sh.getDataRange().getValues().slice(3);
  const out = [];
  for (let i = 0; i < data.length; i++) {
    const r = data[i];
    if (!String(r[1] || "").trim()) continue;
    if (String(r[8] || "").toUpperCase() !== "Y") continue;
    out.push({
      _row: i + 4,
      symbol: String(r[1]).trim().toUpperCase(),
      exchange: String(r[2] || "NSE").trim().toUpperCase(),
      segment: String(r[3] || "CASH").trim().toUpperCase(),
      allowedProduct: String(r[4] || "BOTH").trim().toUpperCase(),
      strategyPref: String(r[5] || "AUTO").trim().toUpperCase(),
      sector: "UNKNOWN",
      beta: parseFloat(r[7]) || 1.0,
      priority: parseFloat(r[9]) || 0,
      notes: String(r[10] || ""),
      score: parseFloat(r[11]) || 0,
      lastRSI: parseFloat(r[12]) || 0,
      lastVolRatio: parseFloat(r[13]) || 0,
      lastScanned: String(r[14] || ""),
      lastProduct: String(r[15] || "").toUpperCase(),
      lastStrategy: String(r[16] || "").toUpperCase(),
      lastNote: String(r[17] || ""),
    });
  }
  return out;
}

function _autoStrategy_(ind) {
  if (ind.macd.crossed === "BUY" || ind.macd.crossed === "SELL") return "EMA_CROSS";
  if (ind.rsi.curr >= 45 && ind.rsi.curr <= 65) return "RSI_EMA";
  return "ALL";
}

function _resolveProduct_(candidate, score, regime) {
  if (candidate.allowedProduct === "MIS" || candidate.allowedProduct === "CNC") return candidate.allowedProduct;
  if (regime.regime === "RANGE") return "MIS";
  if (regime.regime === "AVOID") return "CNC";
  if (candidate.beta >= 1.25 && score >= 60) return "MIS";
  return "CNC";
}

function computeUniverseScore(symbol, ind) {
  let score = 0;
  if (ind.emaStack) score += 20;
  else if (ind.ema20AboveEMA50) score += 10;
  if (ind.aboveEMA20) score += 5;
  if (ind.aboveEMA50) score += 5;

  const rsi = ind.rsi.curr;
  if (rsi >= 50 && rsi <= 65) score += 15;
  else if (rsi >= 40 && rsi < 50) score += 8;
  else if (rsi > 65 && rsi <= 75) score += 5;
  if (ind.macd.hist > 0) score += 5;
  if (ind.macd.crossed === "BUY") score += 5;

  if (ind.nearBreakout) score += 10;
  if (ind.breakout) score += 15;
  else if (ind.distFrom52wHigh < 10) score += 8;

  if (ind.vol.ratio >= 1.5) score += 15;
  else if (ind.vol.ratio >= 1.2) score += 10;
  else if (ind.vol.ratio >= 1.0) score += 5;
  if (ind.obvRising) score += 5;

  if (rsi > 80) score -= 15;
  if (rsi < 35) score -= 15;
  if (ind.patterns.doji) score -= 5;
  if (ind.patterns.bearCandle) score -= 5;
  if (ind.distFrom52wHigh > 30) score -= 10;

  return Math.max(0, Math.min(100, Math.round(score)));
}

function _selectDiversified_(scored, limit, misTarget, cncTarget) {
  const picked = [];
  const seen = {};
  const productCount = { MIS: 0, CNC: 0 };

  const sorted = scored
    .slice()
    .sort((a, b) => (b.score - a.score) || (Number(b.beta || 0) - Number(a.beta || 0)) || a.symbol.localeCompare(b.symbol));
  if (!sorted.length) return [];

  function canPick(s, relaxedProduct) {
    if (seen[s.symbol]) return false;
    const p = s.product === "MIS" ? "MIS" : "CNC";
    if (!relaxedProduct) {
      if (p === "MIS" && productCount.MIS >= misTarget) return false;
      if (p === "CNC" && productCount.CNC >= cncTarget) return false;
    }
    return true;
  }

  // Pass 1/2: fill by quality while first honoring product targets.
  for (let pass = 0; pass < 2 && picked.length < limit; pass++) {
    const relaxedProduct = pass > 0;
    for (const s of sorted) {
      if (!canPick(s, relaxedProduct)) continue;
      seen[s.symbol] = true;
      picked.push(s);
      if (s.product === "MIS") productCount.MIS++;
      else productCount.CNC++;
      if (picked.length >= limit) break;
    }
  }

  for (const s of sorted) {
    if (picked.length >= limit) break;
    if (seen[s.symbol]) continue;
    seen[s.symbol] = true;
    picked.push(s);
  }
  return picked.slice(0, limit);
}

function _universeBatchWindow_(total, batchSize) {
  const props = PropertiesService.getScriptProperties();
  let cursor = Number(props.getProperty(UNIVERSE_SCAN_CURSOR_PROP) || 0);
  if (!isFinite(cursor) || cursor < 0 || cursor >= total) cursor = 0;
  const requested = Number(batchSize) > 0 ? Number(batchSize) : DEFAULT_WATCHLIST_SCREEN_BATCH;
  const size = Math.max(WATCHLIST_BATCH_MIN_SIZE, Math.min(WATCHLIST_BATCH_MAX_SIZE, Math.floor(requested)));
  const end = Math.min(total, cursor + size);
  const wrapped = end >= total;
  const next = wrapped ? 0 : end;
  props.setProperty(UNIVERSE_SCAN_CURSOR_PROP, String(next));
  return { start: cursor, end, next, wrapped, size };
}

function _watchlistBuildMaxMs_() {
  const props = PropertiesService.getScriptProperties();
  const raw = Number(props.getProperty(WATCHLIST_BUILD_MAX_MS_PROP) || 0);
  if (!isFinite(raw) || raw < 30000) return DEFAULT_WATCHLIST_BUILD_MAX_MS;
  return Math.max(30000, Math.min(120000, Math.floor(raw)));
}

function _watchlistTargetSize_(limitHint) {
  if (Number(limitHint) > 0) return Math.max(8, Math.floor(Number(limitHint)));
  const props = PropertiesService.getScriptProperties();
  const raw = Number(props.getProperty(WATCHLIST_TARGET_SIZE_PROP) || 0);
  return isFinite(raw) && raw >= 8 ? Math.floor(raw) : DEFAULT_WATCHLIST_TARGET_SIZE;
}

function SET_WATCHLIST_TARGET_SIZE(size = DEFAULT_WATCHLIST_TARGET_SIZE) {
  const n = Math.max(8, Math.min(500, Math.floor(Number(size) || DEFAULT_WATCHLIST_TARGET_SIZE)));
  const startedAt = Date.now();
  ACTION("Universe", "SET_WATCHLIST_TARGET_SIZE", "START", "", { requested: Number(size) || 0, applied: n });
  const props = PropertiesService.getScriptProperties();
  props.setProperty(WATCHLIST_TARGET_SIZE_PROP, String(n));
  _markWatchlistNotReady_("watchlist_target_changed");
  LOG("INFO", "Universe", `Watchlist target size set to ${n}`);
  ACTION("Universe", "SET_WATCHLIST_TARGET_SIZE", "DONE", "target size updated", { target: n, ms: Date.now() - startedAt });
  _flushActionLogs_();
  return { target: n };
}

function GET_WATCHLIST_TARGET_SIZE() {
  return { target: _watchlistTargetSize_(0) };
}

function _watchlistRegimeMode_() {
  const props = PropertiesService.getScriptProperties();
  const raw = String(props.getProperty(WATCHLIST_REGIME_MODE_PROP) || DEFAULT_WATCHLIST_REGIME_MODE).trim().toUpperCase();
  return raw === "LIVE" ? "LIVE" : "FAST";
}

function _watchlistScoreFreshHours_() {
  const props = PropertiesService.getScriptProperties();
  const raw = Number(props.getProperty(WATCHLIST_SCORE_FRESH_HOURS_PROP) || 0);
  if (!isFinite(raw) || raw < 1) return DEFAULT_WATCHLIST_SCORE_FRESH_HOURS;
  return Math.max(1, Math.min(168, Math.floor(raw)));
}

function _watchlistScoreApiCap_() {
  const props = PropertiesService.getScriptProperties();
  const raw = Number(props.getProperty(WATCHLIST_SCORE_API_CAP_PROP) || 0);
  if (!isFinite(raw) || raw < 1) return DEFAULT_WATCHLIST_SCORE_API_CAP;
  return Math.max(1, Math.min(12, Math.floor(raw)));
}

function _watchlistScoreLookbackDays_() {
  const props = PropertiesService.getScriptProperties();
  const raw = Number(props.getProperty(WATCHLIST_SCORE_LOOKBACK_DAYS_PROP) || 0);
  if (!isFinite(raw) || raw < 180) return DEFAULT_WATCHLIST_SCORE_LOOKBACK_DAYS;
  return Math.max(180, Math.min(1080, Math.floor(raw)));
}

function _watchlistScoreMinBars_() {
  const props = PropertiesService.getScriptProperties();
  const raw = Number(props.getProperty(WATCHLIST_SCORE_MIN_BARS_PROP) || 0);
  if (!isFinite(raw) || raw < 60) return DEFAULT_WATCHLIST_SCORE_MIN_BARS;
  return Math.max(60, Math.min(400, Math.floor(raw)));
}

function _premarketScoreBatch_() {
  const props = PropertiesService.getScriptProperties();
  const raw = Number(props.getProperty(PREMARKET_SCORE_BATCH_PROP) || 0);
  if (!isFinite(raw) || raw < 10) return DEFAULT_PREMARKET_SCORE_BATCH;
  return Math.max(10, Math.min(200, Math.floor(raw)));
}

function _premarketScoreApiCap_() {
  const props = PropertiesService.getScriptProperties();
  const raw = Number(props.getProperty(PREMARKET_SCORE_API_CAP_PROP) || 0);
  if (!isFinite(raw) || raw < 2) return DEFAULT_PREMARKET_SCORE_API_CAP;
  return Math.max(2, Math.min(80, Math.floor(raw)));
}

function _premarketScoreMaxMs_() {
  const props = PropertiesService.getScriptProperties();
  const raw = Number(props.getProperty(PREMARKET_SCORE_MAX_MS_PROP) || 0);
  if (!isFinite(raw) || raw < 60000) return DEFAULT_PREMARKET_SCORE_MAX_MS;
  return Math.max(60000, Math.min(320000, Math.floor(raw)));
}

function _premarketStartMins_() {
  const props = PropertiesService.getScriptProperties();
  const raw = Number(props.getProperty(PREMARKET_SCORE_START_MINS_PROP) || 0);
  if (!isFinite(raw)) return DEFAULT_PREMARKET_SCORE_START_MINS;
  return Math.max(0, Math.min(1439, Math.floor(raw)));
}

function _premarketEndMins_() {
  const props = PropertiesService.getScriptProperties();
  const raw = Number(props.getProperty(PREMARKET_SCORE_END_MINS_PROP) || 0);
  if (!isFinite(raw)) return DEFAULT_PREMARKET_SCORE_END_MINS;
  return Math.max(0, Math.min(1439, Math.floor(raw)));
}

function _isWeekdayIst_() {
  const day = new Date(new Date().getTime() + 5.5 * 3600000).getUTCDay();
  return day >= 1 && day <= 5;
}

function _isPremarketScoringWindow_() {
  if (!_isWeekdayIst_()) return false;
  const m = getISTMins();
  const start = _premarketStartMins_();
  const end = _premarketEndMins_();
  if (start <= end) return m >= start && m <= end;
  return m >= start || m <= end;
}

function _parseTsMs_(ts) {
  const s = String(ts || "").trim();
  if (!s) return NaN;
  const m = s.match(/^(\d{2})-(\d{2})-(\d{4}) (\d{2}):(\d{2}):(\d{2})$/);
  if (m) {
    const iso = `${m[3]}-${m[2]}-${m[1]}T${m[4]}:${m[5]}:${m[6]}+05:30`;
    const ms = Date.parse(iso);
    return isFinite(ms) ? ms : NaN;
  }
  const parsed = Date.parse(s.includes("T") ? s : s.replace(" ", "T"));
  return isFinite(parsed) ? parsed : NaN;
}

function _isTsFresh_(ts, maxAgeHours) {
  if (!isFinite(maxAgeHours) || maxAgeHours <= 0) return false;
  const parsed = _parseTsMs_(ts);
  if (!isFinite(parsed)) return false;
  return (Date.now() - parsed) <= maxAgeHours * 3600000;
}

function _candidateNeedsRescore_(c, freshHours) {
  const score = Number(c && c.score || 0);
  if (!isFinite(score) || score <= 0) return true;
  return !_isTsFresh_(c && c.lastScanned, freshHours);
}

function _fetchUniverseScoreCandlesWindowed_(symbol, exchange, segment, lookbackDays) {
  const days = Math.max(180, Number(lookbackDays) || DEFAULT_WATCHLIST_SCORE_LOOKBACK_DAYS);
  const end = new Date();
  const start = new Date(end.getTime() - days * 86400000);
  const all = [];
  const seen = {};
  let cursor = new Date(start.getTime());
  while (cursor < end) {
    const winEnd = new Date(Math.min(end.getTime(), cursor.getTime() + 180 * 86400000));
    const part = fetchCandlesRangeDirect_(symbol, exchange, segment, "1d", cursor, winEnd, { allowDeprecatedFallback: false }) || [];
    part.forEach(c => {
      const n = (typeof _normCandle_ === "function") ? _normCandle_(c) : c;
      if (!n) return;
      const k = String(n[0]);
      if (seen[k]) return;
      seen[k] = true;
      all.push(n);
    });
    cursor = new Date(winEnd.getTime() + 1000);
    Utilities.sleep(70);
  }
  all.sort((a, b) => (typeof _tsCmp_ === "function") ? _tsCmp_(a[0], b[0]) : String(a[0]).localeCompare(String(b[0])));
  return all;
}

function _candlesForUniverseScore_(symbol, exchange, segment, lookbackDays, minBars, allowApi = true) {
  const tf = "1d";
  const need = Math.max(60, Number(minBars) || DEFAULT_WATCHLIST_SCORE_MIN_BARS);
  const targetBars = (typeof _scoreCache1dTargetBars_ === "function") ? _scoreCache1dTargetBars_() : 320;
  const outMeta = { candles: [], source: "none", apiCalls: 0 };

  try {
    if (typeof getUniverseScoreCache1dCandles === "function") {
      const cached1d = getUniverseScoreCache1dCandles(symbol, exchange, segment, need, targetBars) || [];
      if (cached1d.length >= need) {
        outMeta.candles = cached1d.slice(-Math.max(need, targetBars));
        outMeta.source = "score_cache_1d";
        return outMeta;
      }
    }
  } catch (e) {}

  let local = [];
  try {
    if (typeof _readLocalCandles_ === "function") {
      local = _readLocalCandles_(symbol, exchange, segment, tf) || [];
      if (local.length >= need) {
        outMeta.candles = local.slice(-Math.max(need, targetBars));
        outMeta.source = "local_cache";
        return outMeta;
      }
    }
  } catch (e) {}

  let api = [];
  if (allowApi) {
    try {
      api = _fetchUniverseScoreCandlesWindowed_(symbol, exchange, segment, lookbackDays) || [];
      if (api.length) outMeta.apiCalls = 1;
    } catch (e) {}
  } else {
    outMeta.source = "api_cap_blocked";
    outMeta.apiNeeded = true;
  }

  if (api.length && typeof upsertCandlesToCache === "function") {
    try { upsertCandlesToCache(symbol, exchange, segment, tf, api, "groww_api"); } catch (e) {}
  }
  if (api.length && typeof _writeScoreCache1dCandles_ === "function") {
    try { _writeScoreCache1dCandles_(symbol, exchange, segment, api); } catch (e) {}
  }
  if (api.length >= need) {
    outMeta.candles = api.slice(-Math.max(need, targetBars));
    outMeta.source = "groww_api";
    return outMeta;
  }
  if (local.length >= need) {
    outMeta.candles = local.slice(-Math.max(need, targetBars));
    outMeta.source = "local_cache_fallback";
    return outMeta;
  }
  outMeta.candles = api.length ? api : local;
  outMeta.source = api.length ? "groww_api_partial" : (local.length ? "local_partial" : (outMeta.source || "none"));
  return outMeta;
}

function _logUniverseCheckpoint_(msg, force = false) {
  LOG("INFO", "UniverseBuild", msg);
  if (force) {
    try { _flushLogs_(); } catch (e) {}
  }
}

function _regimeForWatchlist_() {
  if (_watchlistRegimeMode_() === "LIVE") return getMarketRegime();
  try {
    const sh = SS.getSheetByName(SH.MARKET);
    if (!sh) throw new Error("market_sheet_missing");
    const regime = String(sh.getRange("B20").getValue() || "").toUpperCase();
    const bias = String(sh.getRange("B21").getValue() || "").toUpperCase();
    const vix = Number(sh.getRange("B4").getValue() || 0);
    const pcr = Number(sh.getRange("B12").getValue() || 0);
    if (regime) {
      return {
        regime,
        bias: bias || "NEUTRAL",
        vix: isFinite(vix) ? vix : 15,
        pcr: { pcr: isFinite(pcr) ? pcr : 1.0 },
        source: "FAST_SHEET",
      };
    }
  } catch (e) {}
  return { regime: "TREND", bias: "NEUTRAL", vix: 15, pcr: { pcr: 1.0 }, source: "FAST_DEFAULT" };
}

function _flushUniverseScoreUpdates_(updates) {
  if (!updates.length) return 0;
  const sh = SS.getSheetByName(SH.UNIVERSE);
  const sorted = updates.slice().sort((a, b) => a.row - b.row);
  let writes = 0;
  let blockStart = sorted[0].row;
  let blockVals = [sorted[0].vals];
  let prev = sorted[0].row;
  for (let i = 1; i < sorted.length; i++) {
    const u = sorted[i];
    if (u.row === prev + 1) {
      blockVals.push(u.vals);
      prev = u.row;
      continue;
    }
    sh.getRange(blockStart, 12, blockVals.length, UNIVERSE_SCORE_HEADERS.length).setValues(blockVals);
    writes += blockVals.length;
    blockStart = u.row;
    blockVals = [u.vals];
    prev = u.row;
  }
  sh.getRange(blockStart, 12, blockVals.length, UNIVERSE_SCORE_HEADERS.length).setValues(blockVals);
  writes += blockVals.length;
  return writes;
}

function _scoreUniverseBatch_(candidates, regime, batchSize, opts = {}) {
  if (!candidates.length) return { scanned: 0, scored: 0, next: 0, wrapped: true, timedOut: false, hitCap: false, fetches: 0 };
  const win = _universeBatchWindow_(candidates.length, batchSize);
  const batch = candidates.slice(win.start, win.end);
  const updates = [];
  let scored = 0;
  let scanned = 0;
  let written = 0;
  let fetches = 0;
  let skippedFresh = 0;
  const startedAt = Date.now();
  const maxMsRaw = Number(opts.maxMs);
  const freshHoursRaw = Number(opts.freshHours);
  const apiCapRaw = Number(opts.apiCap);
  const lookbackRaw = Number(opts.lookbackDays);
  const minBarsRaw = Number(opts.minBars);
  const maxMs = isFinite(maxMsRaw) && maxMsRaw >= 15000 ? Math.max(15000, Math.min(320000, Math.floor(maxMsRaw))) : _watchlistBuildMaxMs_();
  const freshHours = isFinite(freshHoursRaw) && freshHoursRaw >= 0 ? Math.max(0, Math.floor(freshHoursRaw)) : _watchlistScoreFreshHours_();
  const apiCap = isFinite(apiCapRaw) && apiCapRaw > 0 ? Math.max(1, Math.min(100, Math.floor(apiCapRaw))) : _watchlistScoreApiCap_();
  const lookbackDays = isFinite(lookbackRaw) && lookbackRaw >= 30 ? Math.max(30, Math.min(1080, Math.floor(lookbackRaw))) : _watchlistScoreLookbackDays_();
  const minBars = isFinite(minBarsRaw) && minBarsRaw >= 40 ? Math.max(40, Math.min(400, Math.floor(minBarsRaw))) : _watchlistScoreMinBars_();
  const coverageOnly = !!opts.coverageOnly;
  let timedOut = false;
  let hitCap = false;
  const progressEvery = 10;

  _logUniverseCheckpoint_(`start window=${win.start}-${win.end} size=${batch.length} cap=${apiCap} mode=${_watchlistRegimeMode_()}${coverageOnly ? "|coverageOnly=Y" : ""}`, true);

  for (let i = 0; i < batch.length; i++) {
    if (Date.now() - startedAt >= maxMs) {
      timedOut = true;
      if (updates.length) {
        written += _flushUniverseScoreUpdates_(updates);
        updates.length = 0;
      }
      _logUniverseCheckpoint_(`timeout scanned=${scanned} scored=${scored} written=${written} fetches=${fetches}`, true);
      break;
    }
    const c = batch[i];
    scanned++;
    if (coverageOnly) {
      const hasScan = !!_parseTsMs_(c && c.lastScanned);
      if (hasScan) {
        skippedFresh++;
        continue;
      }
    } else if (!_candidateNeedsRescore_(c, freshHours)) {
      skippedFresh++;
      continue;
    }
    try {
      const candleRes = _candlesForUniverseScore_(c.symbol, c.exchange, c.segment, lookbackDays, minBars, fetches < apiCap) || {};
      if (candleRes.apiNeeded && fetches >= apiCap) {
        hitCap = true;
        break;
      }
      const candles = candleRes.candles || [];
      const apiCalls = Number(candleRes.apiCalls || 0);
      if (apiCalls > 0 && fetches + apiCalls > apiCap) {
        hitCap = true;
        break;
      }
      fetches += apiCalls;
      if (!candles || candles.length < minBars) continue;
      const ind = computeIndicators(candles);
      if (!ind) continue;
      const baseScore = computeUniverseScore(c.symbol, ind) + Math.min(5, c.priority);
      const product = _resolveProduct_(c, baseScore, regime);
      const strategy = c.strategyPref === "AUTO" ? _autoStrategy_(ind) : c.strategyPref;
      const score = Math.min(100, Math.round(baseScore));
      const note = `Score=${score}|Reg=${regime.regime}|Bias=${regime.bias}|RSI=${ind.rsi.curr.toFixed(1)}|VR=${ind.vol.ratio.toFixed(2)}|Src=${String(candleRes.source || "")}`;
      updates.push({
        row: c._row,
        vals: [score, Number(ind.rsi.curr.toFixed(2)), Number(ind.vol.ratio.toFixed(3)), nowIST(), product, strategy, note],
      });
      scored++;
      if (updates.length >= 15) {
        written += _flushUniverseScoreUpdates_(updates);
        updates.length = 0;
      }
      if (scanned % progressEvery === 0) {
        _logUniverseCheckpoint_(`progress scanned=${scanned}/${batch.length} scored=${scored} freshSkip=${skippedFresh} fetches=${fetches}`);
      }
      if (fetches > 0 && fetches % 3 === 0) Utilities.sleep(60);
    } catch (e) {
      LOG("WARN", "Universe", `${c.symbol}: ${e.toString()}`);
    }
  }
  if (updates.length) written += _flushUniverseScoreUpdates_(updates);
  _logUniverseCheckpoint_(`done scanned=${scanned} scored=${scored} freshSkip=${skippedFresh} fetches=${fetches}/${apiCap} written=${written} next=${win.next}${hitCap ? " cap=Y" : ""}`, true);
  return { scanned, scored, written, next: win.next, wrapped: win.wrapped, timedOut, hitCap, fetches, skippedFresh };
}

function _scoredUniversePool_(candidates, regime) {
  return candidates
    .filter(c => isFinite(c.score) && Number(c.score) > 0)
    .map(c => {
      const score = Math.max(0, Math.min(100, Math.round(Number(c.score) || 0)));
      return {
        symbol: c.symbol,
        exchange: c.exchange,
        segment: c.segment,
        product: _resolveProduct_(c, score, regime),
        strategy: c.lastStrategy || c.strategyPref || "AUTO",
        sector: "UNKNOWN",
        beta: c.beta,
        score,
        note: c.lastNote || `Score=${score}|Reg=${regime.regime}|Bias=${regime.bias}`,
      };
    })
    .sort((a, b) => b.score - a.score);
}

function _seedUniversePool_(candidates, regime) {
  return candidates
    .map(c => {
      const seedScore = isFinite(c.priority) ? Number(c.priority) : 0;
      const score = Math.max(1, Math.min(100, Math.round(seedScore)));
      return {
        symbol: c.symbol,
        exchange: c.exchange,
        segment: c.segment,
        product: _resolveProduct_(c, score, regime),
        strategy: c.strategyPref || "AUTO",
        sector: "UNKNOWN",
        beta: c.beta,
        score,
        note: `SEED|Reg=${regime.regime}|Bias=${regime.bias}|Priority=${seedScore}`,
      };
    })
    .sort((a, b) => (b.score - a.score) || (Number(b.beta || 0) - Number(a.beta || 0)) || a.symbol.localeCompare(b.symbol));
}

function _writeWatchlistRows_(selected) {
  const wl = SS.getSheetByName(SH.WATCHLIST);
  const last = wl.getLastRow();
  if (last > 3) wl.getRange(4, 1, last - 3, Math.min(wl.getLastColumn(), 10)).clearContent();
  const out = selected.map((s, i) => [i + 1, s.symbol, s.exchange, s.segment, s.product, s.strategy, s.sector, s.beta, "Y", s.note]);
  if (out.length) wl.getRange(4, 1, out.length, out[0].length).setValues(out);
  return out.length;
}

function universeScoreCoverage() {
  const candidates = _readUniverseRows_();
  const total = candidates.length;
  let scored = 0;
  for (let i = 0; i < candidates.length; i++) {
    if (_parseTsMs_(candidates[i].lastScanned)) scored++;
  }
  const missing = Math.max(0, total - scored);
  const coveragePct = total > 0 ? Number(((scored * 100) / total).toFixed(2)) : 0;
  return { total, scored, missing, coveragePct, full: total > 0 && scored >= total };
}

function _scanDayIst_(ts) {
  const ms = _parseTsMs_(ts);
  if (!isFinite(ms)) return "";
  return Utilities.formatDate(new Date(ms), "Asia/Kolkata", "yyyy-MM-dd");
}

function universeScoreCoverageForDay(dayStr = todayIST()) {
  const day = String(dayStr || todayIST());
  const candidates = _readUniverseRows_();
  const total = candidates.length;
  let scored = 0;
  for (let i = 0; i < candidates.length; i++) {
    const c = candidates[i];
    if (_scanDayIst_(c.lastScanned) === day) scored++;
  }
  const missing = Math.max(0, total - scored);
  const coveragePct = total > 0 ? Number(((scored * 100) / total).toFixed(2)) : 0;
  return { day, total, scored, missing, coveragePct, full: total > 0 && scored >= total };
}

function _markWatchlistNotReady_(reason = "") {
  const props = PropertiesService.getScriptProperties();
  props.deleteProperty(WATCHLIST_READY_DAY_PROP);
  props.deleteProperty(WATCHLIST_READY_TS_PROP);
  props.setProperty(WATCHLIST_READY_COVERAGE_PROP, "0");
  if (reason) LOG("WARN", "Universe", `Watchlist not ready: ${reason}`);
}

function _markWatchlistReady_(coveragePct, selectedCount) {
  const props = PropertiesService.getScriptProperties();
  props.setProperty(WATCHLIST_READY_DAY_PROP, todayIST());
  props.setProperty(WATCHLIST_READY_TS_PROP, nowIST());
  props.setProperty(WATCHLIST_READY_COVERAGE_PROP, String(Number(coveragePct || 0).toFixed(2)));
  LOG("INFO", "Universe", `Watchlist ready for trading day=${todayIST()} coverage=${Number(coveragePct || 0).toFixed(2)} selected=${Number(selectedCount || 0)}`);
}

function getWatchlistReadinessStatus(includeCoverage = false) {
  const props = PropertiesService.getScriptProperties();
  const day = String(props.getProperty(WATCHLIST_READY_DAY_PROP) || "");
  const ts = String(props.getProperty(WATCHLIST_READY_TS_PROP) || "");
  const coverage = Number(props.getProperty(WATCHLIST_READY_COVERAGE_PROP) || 0);
  const hasWatchlist = getWatchlist().length > 0;
  const ready = day === todayIST() && hasWatchlist && coverage >= 100;
  const out = {
    ready,
    readyDay: day,
    readyTs: ts,
    coverage,
    hasWatchlist,
    today: todayIST(),
  };
  if (includeCoverage) {
    const live = universeScoreCoverage();
    out.coverageLive = live.coveragePct;
    out.total = live.total;
    out.scored = live.scored;
    out.missing = live.missing;
  }
  return out;
}

function isWatchlistReadyForTrading() {
  return getWatchlistReadinessStatus(false).ready;
}

function buildFinalWatchlistAfterFullUniverseScored(limit = 0) {
  const startedAt = Date.now();
  ACTION("Universe", "buildFinalWatchlistAfterFullUniverseScored", "START", "", { limit: Number(limit) || 0 });
  const coverage = universeScoreCoverage();
  if (!coverage.full) {
    _markWatchlistNotReady_("coverage_incomplete");
    ACTION("Universe", "buildFinalWatchlistAfterFullUniverseScored", "SKIP", "coverage incomplete", {
      ms: Date.now() - startedAt,
      coveragePct: coverage.coveragePct,
      scored: coverage.scored,
      total: coverage.total,
    });
    _flushActionLogs_();
    return { ready: false, selected: 0, coveragePct: coverage.coveragePct, scored: coverage.scored, total: coverage.total };
  }

  const regime = _regimeForWatchlist_();
  const candidates = _readUniverseRows_();
  const scored = _scoredUniversePool_(candidates, regime);
  const n = _watchlistTargetSize_(limit);
  const misTarget = Math.floor(n * 0.5);
  const cncTarget = n - misTarget;
  const selected = _selectDiversified_(scored, n, misTarget, cncTarget);
  _writeWatchlistRows_(selected);
  _markWatchlistReady_(coverage.coveragePct, selected.length);
  DECISION("WATCHLIST", "UNIVERSE", "READY", "full_universe_scored", {
    selected: selected.length,
    coveragePct: coverage.coveragePct,
    scored: coverage.scored,
    total: coverage.total,
    regime: regime.regime,
    bias: regime.bias,
  });
  ACTION("Universe", "buildFinalWatchlistAfterFullUniverseScored", "DONE", "final watchlist ready", {
    ms: Date.now() - startedAt,
    selected: selected.length,
    coveragePct: coverage.coveragePct,
    scored: coverage.scored,
    total: coverage.total,
  });
  _flushActionLogs_();
  return { ready: true, selected: selected.length, coveragePct: coverage.coveragePct, scored: coverage.scored, total: coverage.total };
}

function precomputeUniverseScoringAndFinalizeWatchlist(batchSize = 0, limit = 0, force = false) {
  const startedAt = Date.now();
  ACTION("Universe", "precomputeUniverseScoringAndFinalizeWatchlist", "START", "", { batchSize: Number(batchSize) || 0, limit: Number(limit) || 0, force: !!force });
  if (!force && !_isPremarketScoringWindow_()) {
    ACTION("Universe", "precomputeUniverseScoringAndFinalizeWatchlist", "SKIP", "outside premarket window", {
      ms: Date.now() - startedAt,
      istMins: getISTMins(),
    });
    _flushActionLogs_();
    return { ready: false, skipped: "outside_window" };
  }
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) {
    ACTION("Universe", "precomputeUniverseScoringAndFinalizeWatchlist", "SKIP", "lock busy", { ms: Date.now() - startedAt });
    _flushActionLogs_();
    return { ready: false, skipped: "lock_busy" };
  }
  try {
    let candidates = _readUniverseRows_();
    if (!candidates.length) {
      syncUniverseFromGrowwInstruments(0);
      candidates = _readUniverseRows_();
    }
    if (!candidates.length) throw new Error("No enabled symbols in universe");

    const before = universeScoreCoverage();
    const beforeToday = universeScoreCoverageForDay(todayIST());
    const needDailyRefresh = !beforeToday.full;
    if (needDailyRefresh) _markWatchlistNotReady_("daily_score_refresh_pending");

    let batchOut = { scanned: 0, scored: 0, fetches: 0, timedOut: false, hitCap: false };
    if (needDailyRefresh) {
      const regime = _regimeForWatchlist_();
      const batch = Number(batchSize) > 0 ? Number(batchSize) : _premarketScoreBatch_();
      batchOut = _scoreUniverseBatch_(candidates, regime, batch, {
        coverageOnly: false,
        apiCap: _premarketScoreApiCap_(),
        maxMs: _premarketScoreMaxMs_(),
        lookbackDays: _watchlistScoreLookbackDays_(),
        minBars: _watchlistScoreMinBars_(),
        freshHours: 0,
      });
    }

    const after = universeScoreCoverage();
    const afterToday = universeScoreCoverageForDay(todayIST());
    if (!afterToday.full) {
      ACTION("Universe", "precomputeUniverseScoringAndFinalizeWatchlist", "PARTIAL", "daily coverage pending", {
        ms: Date.now() - startedAt,
        coveragePct: after.coveragePct,
        todayCoveragePct: afterToday.coveragePct,
        scored: after.scored,
        todayScored: afterToday.scored,
        total: after.total,
        batchScanned: Number(batchOut.scanned || 0),
        batchScored: Number(batchOut.scored || 0),
        fetches: Number(batchOut.fetches || 0),
        hitCap: !!batchOut.hitCap,
        timedOut: !!batchOut.timedOut,
      });
      _flushActionLogs_();
      return {
        ready: false,
        coveragePct: after.coveragePct,
        todayCoveragePct: afterToday.coveragePct,
        scored: after.scored,
        todayScored: afterToday.scored,
        total: after.total,
        batchScanned: Number(batchOut.scanned || 0),
        batchScored: Number(batchOut.scored || 0),
        fetches: Number(batchOut.fetches || 0),
        hitCap: !!batchOut.hitCap,
        timedOut: !!batchOut.timedOut,
      };
    }

    const finalOut = buildFinalWatchlistAfterFullUniverseScored(limit || 0);
    ACTION("Universe", "precomputeUniverseScoringAndFinalizeWatchlist", "DONE", "coverage complete and watchlist ready", {
      ms: Date.now() - startedAt,
      selected: Number(finalOut.selected || 0),
      coveragePct: Number(finalOut.coveragePct || 0),
      todayCoveragePct: Number(afterToday.coveragePct || 0),
      scored: Number(finalOut.scored || 0),
      todayScored: Number(afterToday.scored || 0),
      total: Number(finalOut.total || 0),
      batchScanned: Number(batchOut.scanned || 0),
      batchScored: Number(batchOut.scored || 0),
      fetches: Number(batchOut.fetches || 0),
    });
    _flushActionLogs_();
    return {
      ready: true,
      selected: Number(finalOut.selected || 0),
      coveragePct: Number(finalOut.coveragePct || 0),
      todayCoveragePct: Number(afterToday.coveragePct || 0),
      scored: Number(finalOut.scored || 0),
      todayScored: Number(afterToday.scored || 0),
      total: Number(finalOut.total || 0),
      batchScanned: Number(batchOut.scanned || 0),
      batchScored: Number(batchOut.scored || 0),
      fetches: Number(batchOut.fetches || 0),
    };
  } finally {
    lock.releaseLock();
  }
}

function screenAndUpdateWatchlist(limit = 30, batchSize = 0) {
  const startedAt = Date.now();
  _logUniverseCheckpoint_(`invoke limit=${limit} batchHint=${batchSize}`);
  _markWatchlistNotReady_("watchlist_refresh_in_progress");
  const rows = _readUniverseRows_();
  if (!rows.length) {
    syncUniverseFromGrowwInstruments(0);
  }
  const candidates = _readUniverseRows_();
  if (!candidates.length) throw new Error("No enabled symbols in universe");

  const regime = _regimeForWatchlist_();
  _logUniverseCheckpoint_(`regime source=${regime.source || "LIVE"} regime=${regime.regime} bias=${regime.bias}`);
  const propBatch = Number(PropertiesService.getScriptProperties().getProperty(WATCHLIST_SCREEN_BATCH_PROP) || 0);
  const targetBatch = Number(batchSize) > 0 ? Number(batchSize) : (isFinite(propBatch) && propBatch > 0 ? propBatch : DEFAULT_WATCHLIST_SCREEN_BATCH);
  const batchOut = _scoreUniverseBatch_(candidates, regime, targetBatch);
  const refreshed = _readUniverseRows_();
  const scored = _scoredUniversePool_(refreshed, regime);
  const n = _watchlistTargetSize_(limit);
  const misTarget = Math.floor(n * 0.5);
  const cncTarget = n - misTarget;

  if (!scored.length) {
    const existing = getWatchlist();
    if (existing.length) {
      LOG("WARN", "Universe", "No fresh scored pool yet; keeping existing watchlist");
      _logUniverseCheckpoint_(`fallback existing_watchlist selected=${existing.length} elapsedMs=${Date.now() - startedAt}`);
      return { selected: existing.length, sectors: 0, regime: regime.regime, bias: regime.bias, batchScanned: batchOut.scanned, batchScored: batchOut.scored, universeScoredPool: 0, fallback: "existing_watchlist" };
    }
    const seededPool = _seedUniversePool_(candidates, regime);
    const seeded = _selectDiversified_(seededPool, n, misTarget, cncTarget);
    _writeWatchlistRows_(seeded);
    LOG("WARN", "Universe", `Seed watchlist used=${seeded.length} (scored pool pending, sector disabled)`);
    DECISION("WATCHLIST", "UNIVERSE", "SEED", "seeded_without_scores", {
      count: seeded.length,
      sectors: 0,
      batchScanned: batchOut.scanned,
      batchScored: batchOut.scored,
      timedOut: !!batchOut.timedOut,
    });
    return {
      selected: seeded.length,
      sectors: 0,
      regime: regime.regime,
      bias: regime.bias,
      batchScanned: batchOut.scanned,
      batchScored: batchOut.scored,
      universeScoredPool: 0,
      nextCursor: batchOut.next,
      fallback: "seeded_watchlist",
      timedOut: !!batchOut.timedOut,
    };
  }
  const selected = _selectDiversified_(scored, n, misTarget, cncTarget);
  const seen = {};
  selected.forEach(s => seen[s.symbol] = true);
  const topups = _seedUniversePool_(candidates, regime).filter(s => !seen[s.symbol]).slice(0, Math.max(0, n - selected.length));
  const finalSelected = selected.concat(topups);
  _writeWatchlistRows_(finalSelected);

  LOG("INFO", "Universe", `Watchlist refreshed=${finalSelected.length} (scored=${selected.length}, topup=${topups.length}), sector=disabled, batchScanned=${batchOut.scanned}, batchScored=${batchOut.scored}, fetches=${Number(batchOut.fetches || 0)}, cursorNext=${batchOut.next}`);
  DECISION("WATCHLIST", "UNIVERSE", "REFRESH", "diversified_selection", {
    count: finalSelected.length,
    sectors: 0,
    mis: finalSelected.filter(s => s.product === "MIS").length,
    cnc: finalSelected.filter(s => s.product === "CNC").length,
    topup: topups.length,
    batchScanned: batchOut.scanned,
    batchScored: batchOut.scored,
    fetches: Number(batchOut.fetches || 0),
    universeScoredPool: scored.length,
  });
  ALERT("Universe Refresh", `Watchlist ${finalSelected.length} symbols (${regime.regime}/${regime.bias})`);
  _logUniverseCheckpoint_(`refresh selected=${finalSelected.length} scoredPool=${scored.length} fetches=${Number(batchOut.fetches || 0)} elapsedMs=${Date.now() - startedAt}`, true);
  return {
    selected: finalSelected.length,
    sectors: 0,
    regime: regime.regime,
    bias: regime.bias,
    batchScanned: batchOut.scanned,
    batchScored: batchOut.scored,
    fetches: Number(batchOut.fetches || 0),
    universeScoredPool: scored.length,
    nextCursor: batchOut.next,
    timedOut: !!batchOut.timedOut,
    hitCap: !!batchOut.hitCap,
  };
}

function buildSmartWatchlist() {
  return screenAndUpdateWatchlist(0, 0);
}

function buildSmartWatchlistBatch(batchSize = DEFAULT_WATCHLIST_SCREEN_BATCH, limit = 0) {
  const startedAt = Date.now();
  ACTION("Universe", "buildSmartWatchlistBatch", "START", "", { batchSize: Number(batchSize) || 0, limit: Number(limit) || 0 });
  try {
    const out = screenAndUpdateWatchlist(limit || 0, batchSize);
    ACTION("Universe", "buildSmartWatchlistBatch", "DONE", "watchlist refresh complete", {
      ms: Date.now() - startedAt,
      selected: Number(out && out.selected || 0),
      sectors: Number(out && out.sectors || 0),
      batchScanned: Number(out && out.batchScanned || 0),
      batchScored: Number(out && out.batchScored || 0),
      fetches: Number(out && out.fetches || 0),
      timedOut: !!(out && out.timedOut),
      hitCap: !!(out && out.hitCap),
    });
    _flushActionLogs_();
    return out;
  } catch (e) {
    ACTION("Universe", "buildSmartWatchlistBatch", "ERROR", String(e), { ms: Date.now() - startedAt });
    _flushActionLogs_();
    throw e;
  }
}

function PREMARKET_PRECOMPUTE_NOW(batchSize = 0, limit = 0) {
  return precomputeUniverseScoringAndFinalizeWatchlist(batchSize, limit, true);
}

function UNIVERSE_COVERAGE_STATUS() {
  const coverage = universeScoreCoverage();
  const todayCov = universeScoreCoverageForDay(todayIST());
  const readiness = getWatchlistReadinessStatus(false);
  return {
    coveragePct: coverage.coveragePct,
    scored: coverage.scored,
    total: coverage.total,
    missing: coverage.missing,
    todayCoveragePct: todayCov.coveragePct,
    todayScored: todayCov.scored,
    todayMissing: todayCov.missing,
    ready: readiness.ready,
    readyDay: readiness.readyDay,
    readyTs: readiness.readyTs,
    readyCoverage: readiness.coverage,
    hasWatchlist: readiness.hasWatchlist,
  };
}

function UNIVERSE_PIPELINE_STATUS() {
  const out = {
    ts: (typeof nowIST === "function") ? nowIST() : "",
    universeRows: 0,
    scoreCache1d: null,
    coverage: null,
    triggerProfileHint: "Use SET_UNIVERSE_PIPELINE_ONLY_TRIGGERS() / SET_UNIVERSE_ONLY_TRIGGERS() / SET_CANDLE_DATA_ONLY_TRIGGERS() / SET_SCORING_ONLY_TRIGGERS()",
  };
  try {
    const rows = _readUniverseRows_();
    out.universeRows = rows.length;
  } catch (e) {
    out.universeRowsError = String(e);
  }
  try {
    if (typeof universeScoreCache1DProgress === "function") out.scoreCache1d = universeScoreCache1DProgress();
  } catch (e) {
    out.scoreCache1d = { error: String(e) };
  }
  try {
    out.coverage = UNIVERSE_COVERAGE_STATUS();
  } catch (e) {
    out.coverage = { error: String(e) };
  }
  return out;
}

function APPLY_UNIVERSE_PREOPEN_FAST_PROFILE() {
  const props = PropertiesService.getScriptProperties();
  const updates = {
    PREMARKET_SCORE_BATCH: "200",
    PREMARKET_SCORE_API_CAP: "40",
    PREMARKET_SCORE_MAX_MS: "240000",
    SCORE_CACHE_1D_BATCH: "24",
    SCORE_CACHE_1D_MAX_MS: "240000",
    SCORE_CACHE_1D_TARGET_BARS: "320",
    SCORE_CACHE_1D_LOOKBACK_DAYS: "700",
    PREMARKET_SCORE_START_MINS: "315", // 05:15
    PREMARKET_SCORE_END_MINS: "555",   // 09:15
  };
  Object.keys(updates).forEach(k => props.setProperty(k, updates[k]));
  LOG("INFO", "Universe", "Applied universe preopen fast profile");
  return updates;
}
