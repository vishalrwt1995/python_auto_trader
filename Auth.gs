/***********************
* Auth.gs
* Auto-generate Groww Access Token using API_KEY + API_SECRET
***********************/


function ensureAccessToken() {
 const props = PropertiesService.getScriptProperties();
 const token = props.getProperty('GROWW_ACCESS_TOKEN');
 const expiryIso = props.getProperty('GROWW_ACCESS_TOKEN_EXPIRY_ISO');


 // If token exists and is valid for > 5 minutes, reuse it
 if (token && expiryIso) {
   const expMs = Date.parse(expiryIso);
   if (isFinite(expMs) && (expMs - Date.now() > 5 * 60 * 1000)) {
     return token;
   }
 }


 // Otherwise generate fresh token
 const res = generateAccessTokenViaApproval_();
 props.setProperty('GROWW_ACCESS_TOKEN', res.token);


 if (res.expiry) props.setProperty('GROWW_ACCESS_TOKEN_EXPIRY_ISO', res.expiry);
 else props.deleteProperty('GROWW_ACCESS_TOKEN_EXPIRY_ISO');


 return res.token;
}

function refreshAccessToken_() {
 return ensureAccessToken();
}


function generateAccessTokenViaApproval_() {
 const props = PropertiesService.getScriptProperties();


 const host = mustProp_('GROWW_API_HOST');
 const apiKey = mustProp_('GROWW_API_KEY');
 const secret = mustProp_('GROWW_API_SECRET');


 // epoch seconds (10 digits)
 const ts = String(Math.floor(Date.now() / 1000));


 // checksum = sha256(secret + timestamp)
 const checksum = sha256Hex_(secret + ts);


 const url = host + '/v1/token/api/access';
 const payload = {
   key_type: 'approval',
   checksum: checksum,
   timestamp: ts
 };


 const resp = UrlFetchApp.fetch(url, {
   method: 'post',
   contentType: 'application/json',
   headers: {
     'Authorization': 'Bearer ' + apiKey,
     'Accept': 'application/json',
     'X-API-VERSION': '1.0'
   },
   payload: JSON.stringify(payload),
   muteHttpExceptions: true
 });


 const code = resp.getResponseCode();
 const body = resp.getContentText();


 if (code < 200 || code >= 300) {
   throw new Error('Token gen failed (' + code + '): ' + body);
 }


 const json = JSON.parse(body);
 if (!json.token) throw new Error('Token gen response missing token: ' + body);


 return { token: json.token, expiry: json.expiry || null, raw: json };
}


function sha256Hex_(s) {
 const bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, s, Utilities.Charset.UTF_8);
 return bytes.map(b => ('0' + (b & 255).toString(16)).slice(-2)).join('');
}


function mustProp_(k) {
 const v = PropertiesService.getScriptProperties().getProperty(k);
 if (!v) throw new Error('Missing Script Property: ' + k);
 return v;
}
