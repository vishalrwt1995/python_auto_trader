/************************************
* Indicators.gs — Pure math indicators (SAFE + deterministic)
* NOTE: uses CFG() for EMA/RSI periods in computeIndicators()
************************************/


function calcEMA(data, period) {
 if (!data || data.length === 0) return [];
 if (data.length < period) return data.map(() => data[0]);
 const k = 2 / (period + 1);
 let ema = [data.slice(0, period).reduce((a, b) => a + b, 0) / period];
 for (let i = period; i < data.length; i++) {
   ema.push(data[i] * k + ema[ema.length - 1] * (1 - k));
 }
 const pad = data.length - ema.length;
 return [...Array(pad).fill(ema[0]), ...ema];
}


function calcRSI(closes, period = 14) {
 if (closes.length < period + 1) return Array(closes.length).fill(50);
 let ag = 0, al = 0;
 for (let i = 1; i <= period; i++) {
   const d = closes[i] - closes[i - 1];
   if (d > 0) ag += d; else al -= d;
 }
 ag /= period; al /= period;
 const rsi = [100 - (100 / (1 + ag / (al || 0.001)))];
 for (let i = period + 1; i < closes.length; i++) {
   const d = closes[i] - closes[i - 1];
   ag = (ag * (period - 1) + (d > 0 ? d : 0)) / period;
   al = (al * (period - 1) + (d < 0 ? -d : 0)) / period;
   rsi.push(100 - (100 / (1 + ag / (al || 0.001))));
 }
 return rsi;
}


function calcMACD(closes) {
 const e12 = calcEMA(closes, 12);
 const e26 = calcEMA(closes, 26);
 const macdLine = e12.map((v, i) => v - e26[i]);
 const sig = calcEMA(macdLine, 9);
 const hist = macdLine.map((v, i) => v - sig[i]);
 return { macd: macdLine, signal: sig, hist };
}


function calcATR(candles, period = 14) {
 const trs = [];
 for (let i = 1; i < candles.length; i++) {
   const h = candles[i][2], l = candles[i][3], pc = candles[i - 1][4];
   trs.push(Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc)));
 }
 if (trs.length < period) return trs.length > 0 ? trs[trs.length - 1] : 1;
 let atr = trs.slice(0, period).reduce((a, b) => a + b, 0) / period;
 for (let i = period; i < trs.length; i++) atr = (atr * (period - 1) + trs[i]) / period;
 return atr;
}


function calcSuperTrend(candles, atrP = 10, mult = 3) {
 const closes = candles.map(c => c[4]);
 const highs = candles.map(c => c[2]);
 const lows = candles.map(c => c[3]);
 const atrs = [];
 let runATR = null;


 for (let i = 1; i < candles.length; i++) {
   const tr = Math.max(highs[i] - lows[i], Math.abs(highs[i] - closes[i - 1]), Math.abs(lows[i] - closes[i - 1]));
   runATR = runATR === null ? tr : (runATR * (atrP - 1) + tr) / atrP;
   atrs.push(runATR);
 }


 const vals = Array(candles.length).fill(null);
 const dirs = Array(candles.length).fill(1);


 for (let i = 1; i < candles.length; i++) {
   if (i - 1 >= atrs.length) break;
   const mid = (highs[i] + lows[i]) / 2;
   const up = mid + mult * atrs[i - 1];
   const dn = mid - mult * atrs[i - 1];


   if (i === 1) { vals[i] = up; dirs[i] = 1; continue; }


   if (dirs[i - 1] === 1) {
     vals[i] = closes[i] < (vals[i - 1] || up) ? up : Math.min(up, vals[i - 1] || up);
     dirs[i] = closes[i] < vals[i] ? -1 : 1;
   } else {
     vals[i] = closes[i] > (vals[i - 1] || dn) ? dn : Math.max(dn, vals[i - 1] || dn);
     dirs[i] = closes[i] > vals[i] ? 1 : -1;
   }
 }
 return { values: vals, direction: dirs };
}


function calcVWAP(candles) {
 let cvp = 0, cv = 0;
 return candles.map(c => {
   const tp = (c[2] + c[3] + c[4]) / 3;
   cvp += tp * c[5]; cv += c[5];
   return cv > 0 ? cvp / cv : c[4];
 });
}


function calcOBV(closes, volumes) {
 const obv = [0];
 for (let i = 1; i < closes.length; i++) {
   if (closes[i] > closes[i - 1]) obv.push(obv[i - 1] + volumes[i]);
   else if (closes[i] < closes[i - 1]) obv.push(obv[i - 1] - volumes[i]);
   else obv.push(obv[i - 1]);
 }
 return obv;
}


function calcBB(closes, period = 20, mult = 2) {
 const result = [];
 for (let i = period - 1; i < closes.length; i++) {
   const sl = closes.slice(i - period + 1, i + 1);
   const mean = sl.reduce((a, b) => a + b, 0) / period;
   const std = Math.sqrt(sl.map(v => (v - mean) ** 2).reduce((a, b) => a + b, 0) / period);
   result.push({ upper: mean + mult * std, mid: mean, lower: mean - mult * std });
 }
 return result;
}


function calcStochastic(candles, kP = 14, dP = 3) {
 const highs = candles.map(c => c[2]);
 const lows = candles.map(c => c[3]);
 const closes = candles.map(c => c[4]);
 const kV = [];
 for (let i = kP - 1; i < closes.length; i++) {
   const hh = Math.max(...highs.slice(i - kP + 1, i + 1));
   const ll = Math.min(...lows.slice(i - kP + 1, i + 1));
   kV.push(hh === ll ? 50 : (closes[i] - ll) / (hh - ll) * 100);
 }
 return { k: kV, d: calcEMA(kV, dP) };
}


function computeIndicators(candles) {
 const cfg = CFG();
 if (!candles || candles.length < 80) return null;


 const closes = candles.map(c => c[4]);
 const volumes = candles.map(c => c[5]);
 const n = closes.length - 1;
 const prevN = Math.max(0, n - 1);


 const emaF = calcEMA(closes, cfg.emaFast);
 const emaM = calcEMA(closes, cfg.emaMed);
 const emaS = calcEMA(closes, cfg.emaSlow);
 const ema20 = calcEMA(closes, 20);
 const ema50 = calcEMA(closes, 50);
 const rsi = calcRSI(closes, cfg.rsiPeriod);
 const macd = calcMACD(closes);
 const st = calcSuperTrend(candles, 10, 3);
 const vwap = calcVWAP(candles);
 const obv = calcOBV(closes, volumes);
 const atr = calcATR(candles, 14);
 const bb = calcBB(closes, 20, 2);
 const stoch = calcStochastic(candles, 14, 3);
 const avgVol = volumes.slice(Math.max(0, n - 20), n).reduce((a, b) => a + b, 0) / Math.min(20, n);
 const maxRef = Math.max(...closes.slice(Math.max(0, n - 251), n + 1));
 const distFrom52wHigh = maxRef > 0 ? ((maxRef - closes[n]) / maxRef) * 100 : 0;
 const nearBreakout = closes[n] >= maxRef * 0.98;
 const breakout = closes[n] >= maxRef * 0.999;
 const aboveEMA20 = closes[n] > ema20[n];
 const aboveEMA50 = closes[n] > ema50[n];
 const ema20AboveEMA50 = ema20[n] > ema50[n];
 const emaStack = emaF[n] > emaM[n] && emaM[n] > emaS[n];
 const emaFlip = emaF[n] < emaM[n] && emaM[n] < emaS[n];
 const obvRising = obv[n] > obv[n - 1];


 const range = (candles[n][2] - candles[n][3]) + 0.001;
 const doji = Math.abs(candles[n][1] - candles[n][4]) / range < 0.1;
 const bullEngulf = candles[n][4] > candles[n][1] && candles[n - 1][4] < candles[n - 1][1]
   && candles[n][4] > candles[n - 1][1] && candles[n][1] < candles[n - 1][4];
 const bearEngulf = candles[n][4] < candles[n][1] && candles[n - 1][4] > candles[n - 1][1]
   && candles[n][4] < candles[n - 1][1] && candles[n][1] > candles[n - 1][4];
 const bearCandle = candles[n][4] < candles[n][1];


 const macdCross =
   macd.hist[n] > 0 && (macd.hist[n - 1] || 0) <= 0 ? "BUY" :
   macd.hist[n] < 0 && (macd.hist[n - 1] || 0) >= 0 ? "SELL" : null;


 return {
   close: closes[n], prevClose: closes[n - 1],
   open: candles[n][1], high: candles[n][2], low: candles[n][3],
   emaFast: { curr: emaF[n], prev: emaF[prevN] },
   emaMed: { curr: emaM[n], prev: emaM[prevN] },
   emaSlow: { curr: emaS[n], prev: emaS[prevN] },
   ema20: { curr: ema20[n], prev: ema20[prevN] },
   ema50: { curr: ema50[n], prev: ema50[prevN] },
   rsi: { curr: rsi[rsi.length - 1], prev: rsi[rsi.length - 2] || 50 },
   macd: {
     macd: macd.macd[n], signal: macd.signal[n], hist: macd.hist[n],
     prevHist: macd.hist[n - 1] || 0,
     crossed: macdCross
   },
   supertrend: {
     value: st.values[n], dir: st.direction[n],
     prevDir: st.direction[n - 1], fresh: st.direction[n] !== st.direction[n - 1]
   },
   vwap: vwap[n],
   obv: { curr: obv[n], prev: obv[n - 1] },
   atr,
   bb: bb.length > 0 ? bb[bb.length - 1] : null,
   stoch: { k: stoch.k[stoch.k.length - 1] || 50, d: stoch.d[stoch.d.length - 1] || 50 },
   volume: { curr: volumes[n], avg: avgVol, ratio: avgVol > 0 ? volumes[n] / avgVol : 0 },
   vol: { curr: volumes[n], avg: avgVol, ratio: avgVol > 0 ? volumes[n] / avgVol : 0 }, // legacy alias
   emaStack,
   emaFlip,
   ema20AboveEMA50,
   aboveEMA20,
   aboveEMA50,
   nearBreakout,
   breakout,
   distFrom52wHigh,
   obvRising,
   patterns: { doji, bullEngulf, bearEngulf, bearCandle },
   candles,
 };
}
