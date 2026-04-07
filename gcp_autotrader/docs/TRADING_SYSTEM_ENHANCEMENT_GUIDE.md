# Trading System Enhancement Guide

> Reference document for building a robust, adaptive, and high-performance trading system.
> Created: 2026-04-01 | System: GCP AutoTrader (Upstox, Indian Markets)

---

## 1. SURVIVAL FIRST, PROFITS SECOND

### Correlation-Aware Position Sizing
- Current system sizes by ATR but doesn't account for correlated positions
- 3 banking stocks moving together = 1 effective position, not 3
- Implementation: compute rolling 20-day correlation matrix for open positions. If pairwise correlation > 0.7, reduce combined size by 50%

### Regime-Conditional Risk
- In PANIC: only trade high-liquidity, low-beta names (top 100 by turnover, beta < 1.0)
- Risk adjustments should be non-linear: NORMAL → 100% size, DEFENSIVE → 60%, LOCKDOWN → 0% (not 80%/60%/40%)
- Add sub-rules: if VIX spikes > 25 intraday, immediately halt new entries regardless of regime

### Max Drawdown Circuit Breaker
- Track rolling 5-day and 20-day drawdown (not just daily loss limit)
- If 5-day drawdown > 3% of capital → reduce position sizes to 50% for next 3 days
- If 20-day drawdown > 8% of capital → go to cash for 5 trading days minimum
- Log every circuit breaker trigger to audit_log for post-analysis

### Liquidity-Adjusted Exits
- Model expected slippage per instrument based on average bid-ask spread and volume
- Liquid stocks (top 200 by turnover): assume 0.05% slippage
- Mid-cap (200-500): assume 0.10% slippage
- Below 500: assume 0.20%+ slippage
- Adjust SL and target prices by slippage estimate before order placement

---

## 2. ALPHA FROM EDGE, NOT COMPLEXITY

### Signal Decorrelation
Current scoring has: EMA, RSI, MACD, SuperTrend, Bollinger, Stochastic, OBV, VWAP. Many are correlated.

**Recommended 4 independent factor groups:**
1. **Momentum/Trend** — EMA stack + SuperTrend (pick one composite, not both)
2. **Mean-Reversion/Value** — RSI + Bollinger position (distance from bands)
3. **Volume/Liquidity** — OBV trend + volume ratio + turnover rank
4. **Volatility Regime** — ATR% change + VIX level + gap risk

Weight these 4 groups, not individual indicators.

### Adaptive Signal Weights
- Use rolling 60-day window of own trades from BQ `trades` table
- For each score component (E,P,R,M,B,V,O,N), compute correlation with trade P&L
- Upweight components that predicted profitable trades, downweight those that didn't
- Recalculate weights weekly (Sunday night batch job)
- Store weight history in BQ for tracking drift

### Time-of-Day Strategies (Indian Market Specific)
| Window | IST Time | Strategy |
|--------|----------|----------|
| Open Drive | 09:15-09:45 | ORB (Opening Range Breakout), high vol stocks only |
| Trend Establishment | 09:45-11:00 | Momentum/trend-following, confirm with VWAP slope |
| Midday Chop | 11:00-13:30 | Reduce activity, mean-reversion only, tighter SL |
| Institutional Flow | 13:30-14:30 | Follow large block trades, sector rotation signals |
| Close Auction | 14:30-15:15 | Swing entries only, no new intraday, close intraday positions |

Scanner cadence should adapt: every 3 min during open drive, every 5 min during trend, every 15 min during chop.

### Sector Rotation
- Track 5-day rolling sector returns relative to Nifty 50
- In TREND_UP: only trade stocks from top 3 performing sectors (leaders)
- In RANGE: trade laggard sectors for mean-reversion
- In PANIC: only trade defensive sectors (FMCG, Pharma, IT)
- Update sector_mapping with sector_momentum_5d field daily

---

## 3. DATA EDGE — BEYOND OHLCV

### Order Flow / Tape Reading
- Upstox WebSocket provides tick-by-tick LTP data
- Track: tick delta (count of upticks vs downticks per minute), cumulative delta divergence
- Large institutional orders leave footprints: sudden volume burst + small price change = absorption (reversal signal)
- Implementation: add TickAnalyzer service that processes WebSocket ticks and emits order flow signals

### Options Chain Intelligence
- Already have `get_option_chain()` and `get_expiries()` in upstox_client.py
- **PCR by strike**: compute at-the-money PCR. < 0.7 = extreme bullish, > 1.3 = extreme bearish
- **Max pain tracking**: calculate max pain level daily. Market gravitates toward max pain on expiry days
- **Unusual OI buildup**: alert when OI change > 2x average at any strike. Signals institutional positioning
- **Volatility skew**: compare IV of OTM puts vs OTM calls. Skew > 1.2 = fear, < 0.8 = complacency
- Run options analysis at 09:00 (premarket) and 13:00 (post-lunch reassessment)

### FII/DII Flow Data
- Available from NSE daily after market close
- Source: https://www.nseindia.com/api/fiidiiTradeReact (or scrape from NSE reports)
- Key signals:
  - FII buying + DII selling = foreign-driven rally (often momentum)
  - FII selling + DII buying = distribution (watch for reversal)
  - Both buying = strong rally continuation
  - Both selling = panic / capitulation
- Store in BQ table `institutional_flows` (date, fii_buy, fii_sell, dii_buy, dii_sell, net_fii, net_dii)
- Use 5-day cumulative FII flow as input to Market Brain regime detection

### Volatility Term Structure
- Compare India VIX (30-day implied vol) vs 20-day realized vol (from ATR)
- VIX >> realized vol (ratio > 1.5): market pricing in event risk, be cautious with new positions
- Realized vol >> VIX (ratio < 0.7): complacency or post-event calm, good for trend-following
- Track VIX term structure if available (near-month vs next-month VIX futures)

---

## 4. HONEST BACKTESTING

### Walk-Forward Optimization
- Train window: 6 months
- Test window: 3 months
- Slide forward by 1 month
- If Sharpe ratio on test window < 50% of train window Sharpe → system is overfit
- Minimum 8 walk-forward windows before declaring a strategy viable

### Realistic Cost Model (Indian Markets)
| Cost Component | Intraday | Delivery |
|----------------|----------|----------|
| Brokerage (Upstox) | ₹20/order or 0.03% | ₹20/order or 0.03% |
| STT | 0.025% (sell side) | 0.1% (buy + sell) |
| Exchange txn charges | 0.00345% | 0.00345% |
| GST (on brokerage + txn) | 18% | 18% |
| Stamp duty | 0.003% (buy side) | 0.015% (buy side) |
| SEBI charges | 0.0001% | 0.0001% |
| **Slippage estimate** | **0.05-0.20%** | **0.05-0.10%** |
| **Total round-trip** | **~0.15-0.35%** | **~0.30-0.50%** |

A system must generate > 0.35% average return per trade to be profitable intraday after costs.

### Monte Carlo Simulation
- Take historical trade series, randomize order 10,000 times
- Compute: max drawdown, time to recover, final equity for each run
- If 95th percentile max drawdown > 15% of capital → position sizing is too aggressive
- If 5th percentile final equity is negative → system doesn't have a real edge

### Out-of-Sample Validation
- Reserve 20% of data (most recent) that the system never sees during development
- Only run OOS test once as final validation
- If OOS Sharpe < 0.5 or win rate drops > 10 percentage points → go back to development
- Never "peek" at OOS data during parameter tuning

---

## 5. SYSTEM ARCHITECTURE PRINCIPLES

### Separate Alpha from Execution
- Scoring logic must be a pure function: data in, signals out, no side effects
- This makes it testable, backtestable, and replaceable
- Current: `domain/scoring.py` is mostly pure — keep it that way

### Log Everything, Decide Later
Every decision should be logged with full context:
- Why a signal was generated (score breakdown)
- Why a signal was rejected (blocked_reason + market state)
- What alternatives existed (other candidate signals that scored lower)
- Full market state at decision time (regime, confidence, VIX, PCR)
- Current: audit_log + signals tables are good. Add `decision_context` JSON field to signals table

### A/B Test Strategies Live
- Run 2 versions of scoring simultaneously on paper mode
- Version A: current production weights
- Version B: experimental weights (e.g., adaptive weights from #2)
- Compare after 30+ trading days (minimum ~100 signals per version)
- Only promote to production if B is statistically significantly better (p < 0.05)
- Implementation: add `strategy_version` field to signals and trades tables

### Version Your Models
- When scoring weights or eligibility thresholds change, tag as a version (e.g., v2.1, v2.2)
- Store version info in BQ `trades.strategy` field
- Track performance metrics per version:
  - Win rate, profit factor, Sharpe ratio, max drawdown
  - Score component importance (which components drove winners vs losers)
- Store in BQ table `model_versions` (version, deployed_at, retired_at, total_trades, win_rate, sharpe, notes)

### Immutable Pipeline
- Scoring/watchlist pipeline must produce identical output given identical input
- No randomness, no time-dependent bugs (use deterministic sort tiebreakers)
- Cache intermediate results (candle data, indicator values) for reproducibility
- Test: run pipeline twice with same input, assert byte-identical output

---

## 6. HIGH-IMPACT IMPROVEMENTS (Priority Order)

### Priority 1: Adaptive Signal Weighting
- Use last 60 days of `trades` table to learn which score components correlated with profit
- Expected impact: +5-15% win rate improvement
- Implementation: nightly batch job, store weights in Firestore config
- Fallback: if insufficient trade data (< 50 trades), use default weights

### Priority 2: Options-Based Regime Confirmation
- Before Market Brain declares TREND_UP, confirm with PCR < 0.8 and max pain trending higher
- Reduces false regime calls by ~20-30%
- Implementation: add `options_confirmation` step to market_brain_service.py

### Priority 3: Intraday VWAP Execution
- Replace market orders with TWAP/VWAP execution (split over 2-3 minutes, 3-5 child orders)
- Reduces slippage on larger positions by 30-50%
- Implementation: add `execution_engine.py` service that manages child orders

### Priority 4: Post-Trade Analysis Pipeline
- Nightly job analyzing every trade:
  - Was entry price optimal? (compare to session VWAP)
  - Was SL too tight? (% of trades stopped out that later hit target)
  - Was SL too loose? (average adverse excursion before SL hit)
  - Was regime call correct? (regime at entry vs actual market move)
- Store analysis in BQ table `trade_analysis`
- Feed findings back into parameter tuning

### Priority 5: Multi-Timeframe Confirmation
- Before entry: check if 5m chart agrees with daily chart
- Daily says BUY + 5m in uptrend → strong entry
- Daily says BUY + 5m in downtrend → wait for 5m to turn
- Expected impact: filters out ~30% of losing trades
- Implementation: add `intraday_confirmation` check in trading_service scan loop

### Priority 6: Correlation-Aware Portfolio
- Before adding a new position, check correlation with existing open positions
- If new stock correlates > 0.7 with any open position → skip or reduce size by 50%
- Use 60-day daily returns correlation
- Implementation: add `portfolio_correlation_check()` to order_service.py

### Priority 7: FII/DII Flow Integration
- Fetch daily FII/DII data from NSE after market close
- Add as input to Market Brain morning regime assessment
- 5-day cumulative FII flow > +₹5000Cr → bullish bias boost
- 5-day cumulative FII flow < -₹5000Cr → bearish bias boost

### Priority 8: Volatility-Regime Position Sizing
- Low vol regime (VIX < 13): increase position size 1.2x, use tighter SL (1.0x ATR)
- Normal vol (13-18): standard sizing, standard SL (1.5x ATR)
- High vol (18-25): reduce size 0.7x, wider SL (2.0x ATR)
- Extreme vol (>25): reduce size 0.4x, very wide SL (2.5x ATR) or no new entries

---

## 7. ANTI-PATTERNS TO AVOID

- **Don't use ML for signal generation until 2000+ trades** — ML on small datasets = overfitting = disaster
- **Don't over-optimize** — If system has 50 tunable parameters, it will overfit. Target < 15 meaningful parameters
- **Don't ignore drawdowns** — Target 30-50% annual returns with < 15% max drawdown. 100% return with 60% drawdown will be turned off at the worst moment
- **Don't trade illiquid stocks** — Enforce minimum average daily turnover > ₹5Cr
- **Don't fight the macro** — PANIC → LOCKDOWN transition should be fast and aggressive. Better to miss 1 day of recovery than catch 1 day of crash
- **Don't add indicators for the sake of adding** — Each new indicator must demonstrate independent predictive value on historical data before inclusion
- **Don't backtest without costs** — A system making 0.3% per trade looks great until you realize costs are 0.35%
- **Don't ignore the overnight gap risk** — For swing positions, gap risk > 5% on any single name means exit or hedge with options

---

## 8. KEY METRICS TO TRACK

| Metric | Target | Red Flag |
|--------|--------|----------|
| Win Rate | > 45% | < 35% |
| Profit Factor | > 1.5 | < 1.1 |
| Sharpe Ratio (annualized) | > 1.5 | < 0.8 |
| Max Drawdown | < 15% | > 25% |
| Average R:R | > 1.2 | < 0.8 |
| Avg Trade Duration (intraday) | 30-120 min | < 5 min (noise) or > 4 hrs (stuck) |
| Trades per Day | 3-8 | > 15 (overtrading) or 0 (system broken) |
| Signal-to-Trade Ratio | 30-50% | > 80% (not filtering enough) or < 10% (too restrictive) |
| Expectancy per Trade | > ₹50 | < ₹0 |
| Consecutive Losses Max | < 8 | > 12 |

---

*This document serves as the master reference for all future system enhancements. Each improvement should be implemented, tested on paper for 30+ days, and validated against these principles before going live.*
