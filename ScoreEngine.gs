/************************************
* ScoreEngine.gs — Direction + scoring
************************************/


function determineDirection(ind, regime) {
 if (regime.regime === "AVOID") return "HOLD";
 let bull = 0, bear = 0;


 if (ind.supertrend.dir === 1) bull += 3; else bear += 3;
 if (ind.close > ind.vwap) bull += 2; else bear += 2;
 if (ind.emaFast.curr > ind.emaMed.curr) bull += 2; else bear += 2;
 if (ind.emaMed.curr > ind.emaSlow.curr) bull += 1; else bear += 1;
 if (ind.rsi.curr > 55) bull += 1; else if (ind.rsi.curr < 45) bear += 1;
 if (ind.macd.hist > 0) bull += 2; else bear += 2;
 if (ind.macd.crossed === "BUY") bull += 1;
 if (ind.macd.crossed === "SELL") bear += 1;
 if (ind.patterns.bullEngulf) bull += 1;
 if (ind.patterns.bearEngulf) bear += 1;
 if (regime.bias === "BULLISH") bull += 2;
 if (regime.bias === "BEARISH") bear += 2;


 if (bull > bear + 2) return "BUY";
 if (bear > bull + 2) return "SELL";
 return "HOLD";
}


function scoreSignal(symbol, direction, ind, regime) {
 const cfg = CFG();
 let score = 0;
 const bd = { regime: 0, options: 0, technical: 0, volume: 0, penalty: 0 };


 if (direction === "HOLD" || regime.regime === "AVOID") return { score: 0, breakdown: bd, direction };
 const isBuy = direction === "BUY";


 // Layer 1: Regime (25)
 if ((isBuy && regime.nifty.changePct > 0.1) || (!isBuy && regime.nifty.changePct < -0.1)) bd.regime += 10;
 else if (Math.abs(regime.nifty.changePct) < 0.1) bd.regime += 5;


 if (regime.vix < cfg.vixTrendMax) bd.regime += 8;
 else if (regime.vix < cfg.vixSafeMax) bd.regime += 4;


 if ((isBuy && regime.fii.fii > 500) || (!isBuy && regime.fii.fii < -500)) bd.regime += 7;
 else if (Math.abs(regime.fii.fii) < 500) bd.regime += 3;


 score += bd.regime;


 // Layer 2: Options (20)
 if ((isBuy && regime.pcr.pcr >= cfg.pcrBullMin) || (!isBuy && regime.pcr.pcr <= cfg.pcrBearMax)) bd.options += 10;
 else bd.options += 3;


 if (regime.pcr.maxPain > 0) {
   const mp = regime.pcr.maxPain;
   if ((isBuy && ind.close > mp * 0.998) || (!isBuy && ind.close < mp * 1.002)) bd.options += 10;
   else bd.options += 4;
 } else bd.options += 5;


 score += bd.options;


 // Layer 3: Technical (40)
 if (ind.supertrend.fresh && ((isBuy && ind.supertrend.dir === 1) || (!isBuy && ind.supertrend.dir === -1))) bd.technical += 10;
 else if ((isBuy && ind.supertrend.dir === 1) || (!isBuy && ind.supertrend.dir === -1)) bd.technical += 6;


 if ((isBuy && ind.close > ind.vwap) || (!isBuy && ind.close < ind.vwap)) bd.technical += 8;


 if (isBuy) {
   if (ind.emaFast.curr > ind.emaMed.curr && ind.emaMed.curr > ind.emaSlow.curr) bd.technical += 7;
   else if (ind.emaFast.curr > ind.emaMed.curr) bd.technical += 4;
   else if (ind.emaFast.curr > ind.emaFast.prev) bd.technical += 2;
 } else {
   if (ind.emaFast.curr < ind.emaMed.curr && ind.emaMed.curr < ind.emaSlow.curr) bd.technical += 7;
   else if (ind.emaFast.curr < ind.emaMed.curr) bd.technical += 4;
   else if (ind.emaFast.curr < ind.emaFast.prev) bd.technical += 2;
 }


 const rsi = ind.rsi.curr;
 if ((isBuy && rsi >= cfg.rsiBuyMin && rsi <= cfg.rsiBuyMax) || (!isBuy && rsi >= cfg.rsiSellMin && rsi <= cfg.rsiSellMax)) bd.technical += 7;
 else if ((isBuy && rsi > ind.rsi.prev && rsi < cfg.rsiBuyMax) || (!isBuy && rsi < ind.rsi.prev && rsi > cfg.rsiSellMin)) bd.technical += 3;


 if ((ind.macd.crossed === "BUY" && isBuy) || (ind.macd.crossed === "SELL" && !isBuy)) bd.technical += 8;
 else if ((isBuy && ind.macd.hist > 0) || (!isBuy && ind.macd.hist < 0)) bd.technical += 4;


 if ((isBuy && ind.patterns.bullEngulf) || (!isBuy && ind.patterns.bearEngulf)) bd.technical = Math.min(40, bd.technical + 2);


 bd.technical = Math.min(40, bd.technical);
 score += bd.technical;


 // Layer 4: Volume (15)
 if (ind.volume.ratio >= cfg.volMult) bd.volume += 10;
 else if (ind.volume.ratio >= 1.2) bd.volume += 6;
 else if (ind.volume.ratio >= 1.0) bd.volume += 3;


 if ((isBuy && ind.obv.curr > ind.obv.prev) || (!isBuy && ind.obv.curr < ind.obv.prev)) bd.volume += 5;


 score += bd.volume;


 // Penalties
 if (regime.vix > 18) bd.penalty -= 10;
 if (regime.regime === "RANGE") bd.penalty -= 8;
 if (Math.abs(ind.close - ind.open) / (ind.close || 1) * 100 > 2.5) bd.penalty -= 5;
 if (ind.patterns.doji) bd.penalty -= 3;
 if (ind.bb && isBuy && ind.close > ind.bb.upper * 0.998) bd.penalty -= 5;
 if (ind.bb && !isBuy && ind.close < ind.bb.lower * 1.002) bd.penalty -= 5;
 if (isBuy && ind.stoch.k > 85) bd.penalty -= 4;
 if (!isBuy && ind.stoch.k < 15) bd.penalty -= 4;


 score += bd.penalty;
 score = Math.max(0, Math.min(100, score));


 // (Optional) show best score on Market sheet
 try { SS.getSheetByName(SH.MARKET).getRange("B22").setValue(score); } catch (e) {}


 return { score, breakdown: bd, direction };
}
