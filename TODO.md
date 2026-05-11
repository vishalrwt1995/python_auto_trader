# AutoTrader — Improvement Roadmap

> Verified issues only. Ordered by impact × effort. Each item has a clear scope and expected outcome.

---

## PHASE 1: Bug Fixes (Do First — Broken Logic)

### 1.1 Fix RS Scoring Bug
**Effort:** 1 hour | **Impact:** Eliminates false BUY signals on down days
- **File:** `scoring.py:223-239`
- **Change:** Replace ratio (`stock_chg / nifty_chg`) with difference (`stock_chg - nifty_chg`)
- **Test:** Verify score on scenarios: stock −2% / Nifty −1% should penalize BUY, not reward it
- **Risk:** None — purely additive fix, no side effects

### 1.2 Fix RANGE Dead Zone
**Effort:** 2-3 hours | **Impact:** Enables trading on ~40% of market days currently producing zero trades
- **Option A:** Lower affinity multipliers threshold — drop RANGE VWAP_TREND from 0.7× to 0.9×, PULLBACK from 0.8× to 0.95×
- **Option B:** Loosen MR entry gates in RANGE — RSI ≤ 50 (from ≤ 45), VWAP extension ≥ 0.3% (from ≥ 0.6%)
- **Option C:** Increase stressed RANGE/CHOP discount from -5 to -10 (threshold 72 → 62)
- **Decision needed:** Pick one or combine. Backtest before deploying.
- **Risk:** Too loose = low-quality trades in RANGE. Start conservative (Option A alone), measure.

### 1.3 Fix Regime-Change Tighten Permanence
**Effort:** 1-2 hours | **Impact:** Stops valid positions from being killed by temporary regime blips
- **File:** `ws_monitor_service.py:530-574`
- **Change:** If regime returns to entry regime within 30 min, restore original SL distance
- **Risk:** Low — only widens SL back to what was planned at entry

---

## PHASE 2: Intraday Breadth (Biggest Structural Fix)

### 2.1 Add Intraday Breadth Computation
**Effort:** 6-8 hours | **Impact:** Fixes false TREND_UP classification in flat/down markets
- **Current:** `breadth_score` computed from daily candles (yesterday's close), frozen all day
- **Change:** Add lightweight intraday breadth using live LTP map (already fetched every scan):
  - `% of watchlist stocks where LTP > today's VWAP` (simple, no historical candles needed)
  - Blend: `effective_breadth = 0.4 × daily_breadth + 0.6 × intraday_breadth`
- **Where:** `market_brain_service.py` — new method `_compute_intraday_breadth(live_ltp_map, vwap_map)`
- **Gate:** Use `effective_breadth` for regime classification AND short-block gate (line 1226)
- **Risk:** Medium — changes regime classification. Test with paper trading for 3-5 days before going live.
- **Fallback:** If intraday breadth unavailable (pre-09:45), use daily breadth only

---

## PHASE 3: Time Intelligence (Quick Wins)

### 3.1 Time-of-Day Entry Windows
**Effort:** 2-3 hours | **Impact:** Avoids erratic open entries, respects afternoon liquidity drop
- Add to `trading_service.py` entry qualification:
  - 09:15–09:45: Block all new entries (erratic open phase)
  - 09:45–14:00: Normal entries (best window)
  - 14:00–14:30: Only allow entries with score ≥ 80 (higher bar for afternoon)
  - 14:30–15:10: Block new intraday entries (not enough time to reach target)
- **Risk:** Low — only restricts entries, doesn't change exit logic

### 3.2 Strategy-Aware FLAT_TIMEOUT
**Effort:** 2-3 hours | **Impact:** Stops premature exits on slow strategies, faster exits on quick ones
- **File:** `ws_monitor_service.py:316, 695-702`
- **Change:** Replace single `_FLAT_TIMEOUT_SEC = 120*60` with strategy-specific map:
  - OPEN_DRIVE: 45 min
  - MOMENTUM, BREAKOUT: 60 min
  - PULLBACK, PHASE1_MOMENTUM: 90 min
  - VWAP_TREND, MEAN_REVERSION, PHASE1_REVERSAL: 120 min
- Also: cap effective timeout to `min(strategy_timeout, time_until_EOD - 15_min)` so late-day entries don't rely on EOD blunt close
- **Risk:** Low — each strategy gets a more appropriate timeout

### 3.3 Regime-Aware max_trades_day
**Effort:** 1-2 hours | **Impact:** More trades in strong markets, fewer in chop
- **File:** `settings.py` + `trading_service.py:569-600`
- **Change:** Dynamic cap based on regime:
  - TREND_UP / RECOVERY: 7
  - RANGE: 4
  - CHOP / PANIC: 2
  - TREND_DOWN: 3
- **Risk:** None — only changes the cap, doesn't affect trade quality

---

## PHASE 4: Position Sizing Upgrade

### 4.1 Conviction-Based Position Sizing
**Effort:** 4-6 hours | **Impact:** Higher risk on strong signals, lower risk on marginal ones
- **Current:** Fixed ₹125 per trade regardless of score
- **Change:** Scale risk by adjusted_score:
  - Score 90-100: ₹175 (1.4× base)
  - Score 80-89: ₹150 (1.2× base)
  - Score 72-79: ₹125 (1.0× base — current default)
  - Score 65-71 (swing/defensive): ₹100 (0.8× base)
- **Hard cap:** Never exceed 2% of capital (₹1000) per trade
- **Where:** `market_policy_service.py:size_position_with_market_brain()`
- **Risk:** Low — adds upside scaling, base case unchanged

---

## PHASE 5: Pre-Market Intelligence

### 5.1 GIFT Nifty Gap Prediction
**Effort:** 4-6 hours | **Impact:** 75-90% accuracy on opening direction prediction
- Fetch GIFT Nifty (formerly SGX Nifty) spread at 08:30 IST during premarket job
- Compute `gap_prediction = GIFT_Nifty_close - previous_Nifty_close`
- Use as input to premarket brain:
  - Gap > +50 pts: boost morning_bias to BULLISH
  - Gap < −50 pts: boost morning_bias to BEARISH
  - |Gap| < 25 pts: NEUTRAL
- Feed `morning_bias` into:
  - First-hour strategy selection (OPEN_DRIVE preference on gap days)
  - Direction voting (extra +2 weight aligned with gap)
- **Risk:** Medium — requires reliable GIFT Nifty data source. Start with paper-only.

### 5.2 Expiry-Day Awareness
**Effort:** 3-4 hours | **Impact:** Protects against expiry-day volatility spikes
- Detect weekly expiry (Tuesday for Nifty) and monthly expiry
- On expiry days:
  - Block new entries after 14:00 (gamma squeeze zone)
  - Tighten trailing SL to 0.8× ATR after 14:30
  - Reduce max_positions by 1
- **Risk:** Low — only restricts on expiry days

---

## PHASE 6: Wire Existing Infrastructure

### 6.1 Connect Sentiment/News Gating
**Effort:** 3-4 hours | **Impact:** Block entries on stocks with negative headline sentiment
- Infrastructure exists: `NewsStore`, `aggregate_sentiment()`, schema ready
- Wire into `trading_service.py` as a policy gate:
  - If `aggregate_sentiment(symbol) == BEARISH` with confidence > 0.7 → block BUY entries
  - If `aggregate_sentiment(symbol) == BULLISH` with confidence > 0.7 → block SELL entries
- Enable via flag: `use_news_signals_v1 = True`
- **Risk:** Low — gated behind flag, can disable instantly

### 6.2 Use MFE/MAE Data for Exit Optimization
**Effort:** 4-6 hours | **Impact:** Data-driven exit calibration
- Aggregate MFE/MAE stats per strategy from BigQuery trades table
- Use median MFE per strategy to set strategy-specific R:R targets:
  - If PULLBACK median MFE = 1.4R → set target to 1.3R (capture 90% of typical move)
  - If VWAP_TREND median MFE = 2.1R → keep target at 2.0R
- Add "MFE receded" exit: if MFE reaches 1.5R but current = 0.3R → exit (move came and went)
- **Risk:** Medium — requires enough historical data (50+ trades per strategy)

---

## PHASE 7: Longer-Term Architecture

### 7.1 Walk-Forward Parameter Validation
**Effort:** 1-2 weeks | **Impact:** Prevents "worked in April, died in May"
- Implement rolling 60-90 day backtest windows
- Re-validate: SL multiplier, R:R ratio, score thresholds, affinity multipliers
- Minimum Walk-Forward Efficiency > 50% to keep a strategy active
- Run quarterly; auto-disable strategies that fail WFE check

### 7.2 HMM Regime Detection
**Effort:** 2-3 weeks | **Impact:** Earlier regime transitions, probability-based decisions
- 2-3 state HMM (Bull/Bear/Sideways) using returns + volatility
- 40-60 day lookback, refitted quarterly
- Output: transition probabilities → gradual position adjustment instead of binary regime flip
- Can run parallel to existing rule-based system for validation

### 7.3 Cross-Asset Signal Integration
**Effort:** 1 week | **Impact:** Better macro regime context
- Add data feeds: USD/INR, US 10Y yield, crude oil, S&P 500 futures
- Feed into regime detection and scoring as additional inputs
- Start with simple correlation-based signals, graduate to factor model

---

## CLEAN-UP (Low Priority, Do When Convenient)

- [ ] Remove dead code: RANGE scoring penalty (scoring.py:305-306)
- [ ] Remove redundant direction dampening for hard-blocked strategies
- [ ] Remove BREAKOUT/MORNING_FADE/VWAP_REVERSAL from scan evaluation loop
- [ ] Wire sub_regime/structure_state to trading decisions OR remove computation
- [ ] Add evidence citations to affinity multiplier values
- [ ] Audit paper slippage model against live fills (realized vs planned risk)

---

## SUMMARY

| Phase | Items | Total Effort | Cumulative Impact |
|-------|-------|-------------|-------------------|
| **1: Bug Fixes** | 3 items | 4-6 hours | Fix broken logic, enable RANGE trading |
| **2: Intraday Breadth** | 1 item | 6-8 hours | Fix false regime classification |
| **3: Time Intelligence** | 3 items | 5-8 hours | Avoid bad entries, smarter exits |
| **4: Position Sizing** | 1 item | 4-6 hours | Scale risk by conviction |
| **5: Pre-Market** | 2 items | 7-10 hours | Gap prediction + expiry protection |
| **6: Wire Infrastructure** | 2 items | 7-10 hours | Sentiment gating + MFE-driven exits |
| **7: Architecture** | 3 items | 4-6 weeks | Adaptive params + ML regime + cross-asset |

**Phases 1-3 (15-22 hours) fix the real problems. Phases 4-6 (18-26 hours) add intelligence. Phase 7 is the long-term roadmap.**
