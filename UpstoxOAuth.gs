/***********************
* UpstoxOAuth.gs
* Minimal Apps Script Web App callback for Upstox OAuth authorization code flow
***********************/

const UPSTOX_CB_PROP_PREFIX = "UPSTOX_OAUTH_CB_";

function _upstoxCbProps_() {
  return {
    ts: UPSTOX_CB_PROP_PREFIX + "TS",
    code: UPSTOX_CB_PROP_PREFIX + "CODE",
    state: UPSTOX_CB_PROP_PREFIX + "STATE",
    error: UPSTOX_CB_PROP_PREFIX + "ERROR",
    errorDesc: UPSTOX_CB_PROP_PREFIX + "ERROR_DESC",
    rawQuery: UPSTOX_CB_PROP_PREFIX + "RAW_QUERY",
    pathInfo: UPSTOX_CB_PROP_PREFIX + "PATH_INFO",
  };
}

function _maskTokenLike_(v) {
  const s = String(v || "");
  if (!s) return "";
  if (s.length <= 8) return s;
  return s.slice(0, 4) + "..." + s.slice(-4);
}

function _upstoxCbStore_(e) {
  const props = PropertiesService.getScriptProperties();
  const p = _upstoxCbProps_();
  const q = (e && e.parameter) ? e.parameter : {};
  const qstr = (e && e.queryString) ? String(e.queryString) : "";
  const pathInfo = (e && e.pathInfo) ? String(e.pathInfo) : "";

  props.setProperty(p.ts, new Date().toISOString());
  props.setProperty(p.rawQuery, qstr);
  props.setProperty(p.pathInfo, pathInfo);

  if (q.code) props.setProperty(p.code, String(q.code));
  else props.deleteProperty(p.code);

  if (q.state) props.setProperty(p.state, String(q.state));
  else props.deleteProperty(p.state);

  if (q.error) props.setProperty(p.error, String(q.error));
  else props.deleteProperty(p.error);

  if (q.error_description) props.setProperty(p.errorDesc, String(q.error_description));
  else props.deleteProperty(p.errorDesc);

  return {
    ts: props.getProperty(p.ts) || "",
    code: props.getProperty(p.code) || "",
    state: props.getProperty(p.state) || "",
    error: props.getProperty(p.error) || "",
    error_description: props.getProperty(p.errorDesc) || "",
    pathInfo,
    rawQuery: qstr,
  };
}

function _upstoxCbHtml_(result) {
  const ok = !!(result && result.code) && !(result && result.error);
  const title = ok ? "Upstox Authorization Received" : "Upstox Authorization Callback";
  const codeLine = result && result.code ? `<p><b>Code:</b> <code>${_maskTokenLike_(result.code)}</code></p>` : "";
  const stateLine = result && result.state ? `<p><b>State:</b> <code>${result.state}</code></p>` : "";
  const errorLine = result && result.error ? `<p style="color:#b00020;"><b>Error:</b> <code>${result.error}</code></p>` : "";
  const errDescLine = result && result.error_description ? `<p style="color:#b00020;"><b>Error Description:</b> ${result.error_description}</p>` : "";
  const tsLine = result && result.ts ? `<p><b>Received At:</b> ${result.ts}</p>` : "";

  const html = `
    <html>
      <head>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>${title}</title>
        <style>
          body { font-family: Arial, sans-serif; padding: 20px; color: #111; }
          .card { max-width: 680px; margin: 0 auto; border: 1px solid #ddd; border-radius: 10px; padding: 18px; }
          code { background: #f5f5f5; padding: 2px 6px; border-radius: 4px; }
          .ok { color: #0a7a2f; }
          .muted { color: #666; font-size: 12px; }
        </style>
      </head>
      <body>
        <div class="card">
          <h2 class="${ok ? "ok" : ""}">${title}</h2>
          <p>${ok ? "The authorization code was captured. Return to Apps Script and exchange it for tokens." : "Callback received."}</p>
          ${codeLine}
          ${stateLine}
          ${errorLine}
          ${errDescLine}
          ${tsLine}
          <p class="muted">Use <code>UPSTOX_OAUTH_CALLBACK_STATUS()</code> in Apps Script to inspect the stored callback payload.</p>
        </div>
      </body>
    </html>
  `;
  return HtmlService.createHtmlOutput(html);
}

// Web App endpoint for OAuth callback (redirect_uri)
function doGet(e) {
  let result = null;
  try {
    result = _upstoxCbStore_(e);
    try {
      ACTION("UpstoxOAuth", "doGet", result.error ? "ERR" : "DONE", result.error ? "callback error" : "callback captured", {
        hasCode: !!result.code,
        state: result.state || "",
        error: result.error || "",
        pathInfo: result.pathInfo || "",
      });
      _flushActionLogs_();
    } catch (logErr) {}
  } catch (err) {
    const msg = String(err && err.message ? err.message : err);
    try {
      ACTION("UpstoxOAuth", "doGet", "ERR", "callback store failed", { err: msg.substring(0, 250) });
      _flushActionLogs_();
    } catch (logErr2) {}
    return HtmlService.createHtmlOutput(`<p>Callback failed: <code>${msg}</code></p>`);
  }
  return _upstoxCbHtml_(result || {});
}

function UPSTOX_OAUTH_CALLBACK_STATUS() {
  const props = PropertiesService.getScriptProperties();
  const p = _upstoxCbProps_();
  const code = String(props.getProperty(p.code) || "");
  const out = {
    ts: String(props.getProperty(p.ts) || ""),
    hasCode: !!code,
    codeMasked: _maskTokenLike_(code),
    codeRaw: code, // explicit for manual token exchange
    state: String(props.getProperty(p.state) || ""),
    error: String(props.getProperty(p.error) || ""),
    error_description: String(props.getProperty(p.errorDesc) || ""),
    pathInfo: String(props.getProperty(p.pathInfo) || ""),
    rawQuery: String(props.getProperty(p.rawQuery) || ""),
  };
  LOG("INFO", "UpstoxOAuth", `callback status hasCode=${out.hasCode} error=${out.error || "none"}`);
  _flushLogs_();
  return out;
}

function UPSTOX_CLEAR_OAUTH_CALLBACK() {
  const props = PropertiesService.getScriptProperties();
  const p = _upstoxCbProps_();
  Object.keys(p).forEach(k => props.deleteProperty(p[k]));
  ACTION("UpstoxOAuth", "UPSTOX_CLEAR_OAUTH_CALLBACK", "DONE", "callback state cleared", {});
  _flushActionLogs_();
  return { ok: true };
}

function UPSTOX_WEBAPP_CALLBACK_URL_HINT() {
  let url = "";
  try {
    url = ScriptApp.getService().getUrl() || "";
  } catch (e) {}
  return {
    webAppUrl: url,
    note: url ? "Use this exact URL as Upstox Redirect URL after Web App deployment." : "Deploy as Web App first, then run this again.",
  };
}
