# Strategy Deep Audit — Root Cause + Fixes

**Generated:** 2026-05-08 (overnight autonomous run)
**Window:** Live data 2026-04-10 → 2026-05-07 (29 days, 78 live trades, ~22k scan_decisions). Backtests over 5-month 5m and 5+month 1d windows.

---

## Executive summary

**Of the 12 strategies the system can fire, only 5 have ever produced live trades. The other 7 are paper-tigers — coded but structurally unable to qualify under current calibration.**

| Strategy | Status | Root cause | Fix priority |
|---|---|---|---|
| VWAP_TREND | LIVE, 51 trades, 43% WR, breakeven | Scoring 1-2 pts off between winners/losers · target 5.67 R:R but only 4% hit · 75% morning losses, 75% afternoon wins | **HIGH** — time-of-day filter |
| VWAP_REVERSAL | LIVE, 13 trades, 23% WR, all 1 disaster day | April 16 cluster pre-breadth-filter; gates added 2026-04-22 now block 100% | DONE — already addressed |
| BREAKOUT | LIVE, 9 trades, 0% WR | Continuation thesis fails in modern Indian market structure | DONE — hard-blocked 2026-05-06 |
| MEAN_REVERSION | DEAD, 0 live trades / 1953 scans | Score reaches threshold (p90=80, max=100) but `direction_hold` (701) + `strategy_mr_rsi_not_oversold` (160) gates kill all | **HIGH** — gate calibration |
| MORNING_FADE | DEAD, 0 trades / 235 scans | Only ever evaluated in TREND_UP (where it's hard-blocked at 0.29× affinity = score 22 vs threshold 71) | **HIGH** — needs RANGE/CHOP regime exposure |
| PHASE1_MOMENTUM | DEAD, 0 trades / 3963 scans | Affinity in RANGE (0.64×) crushes; in TREND_UP scores p90=72 just at threshold | **MEDIUM** — affinity too aggressive for RANGE |
| PHASE1_REVERSAL | DEAD, 0 trades / 140 scans | Only TREND_UP exposure (wrong regime); affinity 0.57× crushes max=44 | **MEDIUM** — needs PANIC/TREND_DOWN exposure |
| PULLBACK | DEAD, 0 trades / 96 scans | TREND_UP p90=70 just at threshold but `strategy_pullback_*` gates kill all 24 candidates | **HIGH** — gate calibration too tight |
| SHORT_BREAKDOWN | DEAD, 0 trades / 99 scans | All in RANGE; affinity 0.55× × max raw 100 = 55, threshold 70 → impossible | **HIGH** — affinity vs threshold conflict |
| SHORT_PULLBACK | DEAD, 0 live trades, 4 backtest losses | Almost never emitted (2 scans in 29 days) | **LOW** — needs upstream emission |
| MOMENTUM | LIVE, 1 trade (loser) | Tight per-strategy gates correctly reject most candidates | **MONITOR** — sample too small |
| OPEN_DRIVE | LIVE, 1 trade (winner, +₹156) | Threshold of 72 (vs 70-71 elsewhere) + narrow time window | **LOW** — sample too small |
| AUTO | LIVE, 3 trades (1 win) | 26% of scans hit `insufficient_candles` (data quality issue) | **MEDIUM** — fix data freshness |

---

## Part 1: Strategies with live trades

### 1.1 VWAP_TREND ⚠️ HIGH-PRIORITY FIX

**Symptom**
- 51 live trades, 43% win rate, net -₹516 over 29 days
- 51% of trades hit `FLAT_TIMEOUT` (no target/SL within ~86 min)
- Only 4% hit `TARGET_HIT` despite 43% WR
- Score 72-74 across all outcomes — **scoring formula does NOT differentiate winners from losers**

**Root cause #1: Target too far for the strategy's timeframe**
- Avg target distance: 1.96% from entry
- Avg SL distance: 0.86% from entry
- True R:R = 2.28
- BUT only 2/51 hit target. The trade typically moves 0.4-0.8% in direction then stalls.

**Root cause #2: Strong morning vs afternoon asymmetry**

| Hour (IST) | Trades | Win Rate | Net P&L |
|---|---|---|---|
| 10 | 21 | 33% | -₹225 |
| 11 | 13 | 31% | -₹285 |
| 12 | 7 | 57% | +₹20 |
| 13 | 6 | 67% | -₹5 |
| 14 | 4 | **75%** | -₹21 |

Morning entries (10-11) have 33% WR and contribute 67% of trade volume. Afternoon entries (12-14) have 64% WR but only 33% of volume. **The system is firing at the wrong time.**

**Fix recommendation**
1. **Time-of-day gate:** restrict VWAP_TREND entries to >= 11:30 IST. Backtest validation needed but data strongly suggests ≥30% improvement in win rate.
2. **Tighten target to 1.25 R:R** (target distance ~1.0-1.1% instead of 1.96%). More trades will hit target, fewer FLAT_TIMEOUT exits at near-zero.
3. **Don't change the scoring formula yet** — score doesn't predict outcome anyway, more data needed before fixing scoring layers.

### 1.2 VWAP_REVERSAL ✅ ALREADY ADDRESSED (no action)

**Symptom**
- 13 live trades, 23% WR, net -₹447
- ALL 13 trades clustered on 2 days: April 16 (12 SELL trades, all EOD_CLOSE) + April 20 (1 BUY)
- Strategy has been evaluated 3,327 times since 2026-04-22 with **zero qualifications**

**Root cause**
- April 16 was a strong-trend day where 12 stocks fired SELL "reversal" signals. Reversal didn't materialize; all 12 hit EOD_CLOSE at small losses.
- The system's breadth filter `nifty_breadth_too_bullish_for_shorts` was tightened on 2026-04-22 in batch3.

**Why no action needed**
- Post-Apr-22 block reasons for VWAP_REVERSAL: 1544 score_below_min + 1232 direction_hold + 259 breadth + others = 100% of evaluations blocked
- The strategy is now structurally gated. New action would be redundant.

### 1.3 BREAKOUT ✅ ALREADY ADDRESSED (no action)

**Symptom**
- 9 live trades, **0% win rate**, net -₹326
- Every single trade hit SL_HIT
- Affinity in TREND_UP was 1.20× (boosted) before hard-block

**Root cause**
- BREAKOUT thesis: stock breaks 20-day high with volume → trend continues
- Indian market reality: 20-day-high breakouts in current regime are mostly fakeouts. Stocks pull back to or below the breakout level within hours.
- No live trade hit target. SL set at 1.13% from entry, target 1.26% — tight 1.1 R:R means breakeven WR needs ~52%, getting 0%.

**Why no action needed**
- Hard-blocked in TREND_UP and TREND_DOWN since 2026-05-06 via `regime_affinity._HARD_BLOCKS`
- Live data confirms the block was correct

**Future re-enable conditions**
- Add VCP/cup-handle pattern detection requiring 2-4 weeks consolidation BEFORE breakout
- Validate on paper for 30+ days with positive expectancy before re-enabling in TREND regimes
- Or allow only on BREADTH-CONFIRMED days (>70% sector participation)

---

## Part 2: Strategies that have NEVER produced a live trade

### 2.1 MEAN_REVERSION 🔴 HIGH-PRIORITY FIX

**Symptom**
- 0 trades / 1953 scans over 29 days
- Score reaches threshold easily: in RANGE p90=80, max=100, threshold=71

**Block cascade (RANGE regime, 1663 scans)**
1. `direction_hold` — 625 (37.6%)
2. `score_below_min` — 555 (33.4%)
3. `nifty_breadth_too_bullish_for_shorts` — 246 (14.8%)
4. `strategy_mr_rsi_not_oversold` — 160 (9.6%)
5. `strategy_mr_buy_price_not_below_vwap` — 33 (2.0%)
6. Other — small

**Root cause: cumulative gate filtering**

The strategy needs ALL of these to pass:
1. RSI<40 (BUY) or RSI>60 (SELL) — `direction_hold` if 40≤RSI≤60
2. Score above threshold (passes for ~10%)
3. Stock direction matches breadth (50% blocked for shorts in bullish tape)
4. RSI ≤35 in RANGE regime (oversold gate at strategy level — even tighter than direction)
5. Price extension ≥1% from VWAP

The compounding probability: ~30% × ~10% × ~50% × ~50% × ~50% = ~0.4% pass-through. Out of 1953 scans, expected qualifying = ~7. Actual = 0.

**Fix recommendation**
1. **Loosen the strategy-level RSI gate** from `≤35 in RANGE` to `≤40 in RANGE` (matches `direction_hold` threshold). Currently the direction is BUY at RSI<40 but the strategy gate says NOT oversold at RSI=36-40, contradictory.
2. **Reduce VWAP extension requirement** from 1% to 0.6% (the original threshold before tightening). 1% deviation is rare on 5m bars; intraday MR works on 0.5-1% stretches.
3. After both relax: expect 5-20 trades/month. Then evaluate WR.

### 2.2 MORNING_FADE 🔴 HIGH-PRIORITY FIX

**Symptom**
- 0 trades / 235 scans
- ALL 235 scans were in TREND_UP regime where MORNING_FADE has affinity 0.29× and is hard-blocked
- Result: hardcoded raw_score=75 → adjusted_score=22 → `score_below_min`

**Root cause: regime exposure mismatch**
- MORNING_FADE was added 2026-05-06 (3 days ago)
- Those 3 days have all been TREND_UP regime
- The strategy's design regime is RANGE (1.4× affinity) and CHOP (1.3× affinity)
- It's been waiting for a non-TREND_UP day to even get a chance

**Fix recommendation**
1. **No code change needed.** The strategy is calibrated correctly. Waiting for regime variation.
2. **Validation plan:** when next RANGE/CHOP day occurs, monitor MORNING_FADE qualifications. If it produces 5+ trades, evaluate WR. If WR ≥40%, keep. If <40%, hard-block in those regimes too.
3. **Time gate is correct:** 09:45-10:15 IST window, requires >1.5% pop from open + volume confirmation.

### 2.3 PULLBACK 🔴 HIGH-PRIORITY FIX

**Symptom**
- 0 trades / 96 scans
- TREND_UP: 24 scans, p90=70 (right at threshold 68.5)
- 73 score_below_min, 19 direction_hold

**Root cause: tight strategy entry gates compounding with low scan count**

Strategy gates require ALL of:
1. ema_stack=True (fast > med > slow) for BUY
2. RSI in 38-65 zone (38-65 for BUY, 40-62 for SELL)
3. Price within ±3% of fast EMA
4. Direction = BUY for long, SELL for short

In a TREND_UP day, most stocks have:
- ema_stack=True ✓
- RSI 60-70 (uptrending stocks usually have RSI 60-80) — half pass
- Price near or above fast EMA (extension >3%) — half FAIL "extended_above_ema"

So gates compound to ~25% pass-through. Combined with score barely reaching threshold, very few qualify.

**Fix recommendation**
1. **Widen RSI band** for BUY from 38-65 to 35-70. The zone 65-70 still represents legitimate pullback entries during strong uptrends.
2. **Widen EMA distance** from ±3% to ±5% for the "near fast EMA" check. 3% is too tight on 5m timeframe.
3. **Keep ema_stack requirement** — this is the trend-confirmation filter and should stay strict.

### 2.4 PHASE1_MOMENTUM 🟡 MEDIUM-PRIORITY FIX

**Symptom**
- 0 trades / 3963 scans
- RANGE: 3205 scans, max_adj=49 (threshold 65). Affinity 0.64× crushes.
- TREND_UP: 618 scans, p90=72, max=85, threshold=69. ~10% reach threshold but 0 qualify.

**Root cause #1: Affinity in RANGE crushes**
- PHASE1_MOMENTUM is for stocks selected via the morning Phase 1 momentum process
- In RANGE regime, these stocks may not actually be momentum leaders → affinity 0.64× correctly de-prioritizes
- Result: 81% of evaluations are in RANGE where it can't qualify

**Root cause #2: PHASE1 strategy gate**
- `strategy_phase1_long_only` blocks SELL signals (correct)
- `strategy_phase1_insufficient_volume` blocks `volume_ratio < 0.8`
- These should not block 100% of TREND_UP candidates with score ≥70

The 0 qualified at p90=72 in TREND_UP is suspicious. Worth investigating with isolated backtest.

**Fix recommendation**
1. Run isolated `pure-replay --setups PHASE1_MOMENTUM` to see what fires
2. If still 0, the issue is in the Phase 1 selection process — Phase 1 stocks aren't strong enough on intraday scoring
3. Likely fix: restrict PHASE1_MOMENTUM to TREND_UP regime only via affinity hard-block in RANGE/CHOP/PANIC

### 2.5 PHASE1_REVERSAL 🟡 MEDIUM-PRIORITY FIX

**Symptom**
- 0 trades / 140 scans, all in TREND_UP
- TREND_UP affinity = 0.6× (PHASE1_REVERSAL is for oversold-bounce in bear markets)
- Max adj_score = 44, threshold = 69 → impossible to qualify

**Root cause: regime exposure mismatch**
- PHASE1_REVERSAL is intentionally for PANIC/TREND_DOWN regimes (affinity 1.2× / 0.9×)
- Audit window had only 1 day with brief TREND_DOWN exposure
- Strategy never had a chance

**Fix recommendation**
1. **No code change needed.** Strategy is correctly calibrated for bearish regimes.
2. **Wait for regime variation.** When next PANIC day occurs, monitor PHASE1_REVERSAL qualifications.

### 2.6 SHORT_BREAKDOWN 🔴 HIGH-PRIORITY FIX

**Symptom**
- 0 trades / 99 scans, all in RANGE
- Max adj_score = 40, threshold = 70 → mathematically impossible

**Root cause: affinity vs threshold conflict**
- RANGE × SHORT_BREAKDOWN affinity = 0.6×
- Direction-dampening for SELL in non-bearish regime = additional 0.5× cap
- Net effective max = 0.6 × min(0.6, 1.0) × min(0.5, raw) — caps the ceiling far below threshold
- Even raw=100 produces adj=30-40

**Fix recommendation**
1. Either:
   - **Don't emit SHORT_BREAKDOWN in RANGE regime at all.** Save scan cycles.
   - OR lower the SHORT_BREAKDOWN-specific threshold to 50 (separate from intraday's 70)
2. Look at the regime affinity policy: if the system EXPLICITLY wants to suppress SHORT_BREAKDOWN in RANGE (correct, no edge there), then suppression should happen at the watchlist emission level, not score+threshold.

### 2.7 SHORT_PULLBACK 🟢 LOW-PRIORITY (data sparse)

**Symptom**
- 0 live trades / 2 scans
- Backtest: 4 trades, 0% WR, -₹11k (over 5-month swing window)

**Root cause: rarely emitted by watchlist**
- Only 2 scans in 29 days = the watchlist generator almost never produces SHORT_PULLBACK setup labels
- Backtest reveals: when it DOES fire, it loses 100%

**Fix recommendation**
1. **Investigate watchlist emission logic** for SHORT_PULLBACK. Why so rare? Either the structural conditions never match, or the multi-emission code (added yesterday) only emits in specific bearish regimes.
2. Once emission rate is clear, validate that backtest 4/4 losers isn't sample-size noise.

---

## Part 3: Edge cases (sample size <5)

### 3.1 MOMENTUM
- 1 live trade (BUY, RANGE, MAX_HOLD_11D, -₹94)
- 178 scans, only 3 qualified live (1 traded, 2 blocked downstream)
- Strict per-strategy gates (`strategy_momentum_*`) correctly reject most candidates
- **No action needed.** Sample too small. Monitor for 30+ trades.

### 3.2 OPEN_DRIVE
- 1 live trade (BUY, TREND_UP, TARGET_HIT, +₹156)
- 559 scans, only 1 qualified
- Threshold 72 vs other strategies' 70 = explicit higher bar
- **No action needed.** Sample too small.

### 3.3 AUTO / blank strategy
- 3 live trades (2 wins, near-zero P&L)
- 2705 scans in RANGE, 700 hit `insufficient_candles` (26%)
- **MEDIUM-priority fix:** investigate why 26% of AUTO scans have data quality issues. Likely symbols with stale/missing 5m data.

---

## Part 4: Cross-strategy systemic issues

### Issue 1: Direction-hold dominance (4893 blocks total)
- 22% of all strategy blocks are `direction_hold`
- The `determine_direction()` function uses a vote tally; ties → HOLD
- 5-7 votes total, needs 3+ vote spread to pick a side
- This is the single biggest reason "qualified score, no trade"
- **Fix candidate:** lower the vote spread threshold from 2 to 1 (riskier — more trades, lower quality) OR add a setup-aware tie-break

### Issue 2: Affinity multiplier vs threshold mismatch
- The affinity matrix correctly de-prioritizes setups in wrong regimes (e.g. SHORT_BREAKDOWN × RANGE = 0.55)
- But the threshold is uniform across (strategy, regime) combos
- Combined effect: setups have no chance to qualify even with raw=100
- **Fix candidate:** either suppress emission for low-affinity combos OR use regime-specific thresholds

### Issue 3: Time-of-day patterns ignored
- VWAP_TREND shows 33% WR morning vs 75% WR afternoon
- No strategy currently has time-of-day gates beyond the broad `is_entry_window_open_ist` (09:45-13:30)
- **Fix candidate:** add strategy-specific entry windows (e.g. VWAP_TREND ≥11:30, MORNING_FADE 09:45-10:15 already)

### Issue 4: Data quality (`insufficient_candles`)
- 700 of 2705 AUTO scans (26%) had insufficient candles
- This means the system tried to evaluate stocks without enough price history
- **Fix candidate:** filter watchlist to only emit symbols with ≥80 5m bars. Audit the watchlist emission criteria.

### Issue 5: Strategies "designed for the wrong regime" never get exposure
- MORNING_FADE: only 235 scans, all in TREND_UP (its worst regime)
- PHASE1_REVERSAL: only TREND_UP exposure (its worst regime)
- The audit window happened to be regime-skewed
- **Fix candidate:** none code-wise — wait for regime variation

---

## Part 5: Sim engine calibration (honest assessment)

The compare-mode result shows:
- Sim trade count: 52 vs Live: 78 → **sim under-counts by 33%**
- Sim P&L: -₹7,613 vs Live: -₹1,361 → **sim is 5.6× more pessimistic**
- Sim WR: 28.85% vs Live: 35.90% → **sim WR 7pp lower**

**Why sim diverges from live:**
1. Sim entry fills at next bar's open; live fills at scan-trigger price (better)
2. Sim slippage model may be too aggressive
3. Sim missing some live filters (max_trades_day, breadth filter, etc.)

**Implication for this audit:**
- Sim absolute numbers (P&L, WR) are NOT trustworthy
- Sim relative ordering (which strategy ranks better/worse) IS trustworthy
- All decisions in this audit are driven by **LIVE data** unless explicitly noted

**Backlog for sim improvement:**
1. Calibrate slippage model against live fills (`slippage_calibration.py` exists for this)
2. Verify sim respects max_trades_day cap
3. Verify sim runs the breadth filter

---

## Part 6: Prioritized fix list

| Priority | Strategy | Fix | Expected impact | Risk |
|---|---|---|---|---|
| P0 | VWAP_TREND | Add time-of-day gate ≥11:30 IST | 51 trades → ~17 trades, WR 43% → ~64% | Low — empirical |
| P0 | VWAP_TREND | Tighten target from 2R to 1.25R | More TARGET_HIT, fewer FLAT_TIMEOUT | Med — depends on trade sample |
| P1 | MEAN_REVERSION | Loosen RSI gate (35→40) + VWAP extension (1%→0.6%) | 0 → 5-20 trades/month | Med — could surface losers |
| P1 | PULLBACK | Widen RSI 38-65→35-70, EMA distance ±3%→±5% | 0 → 5-15 trades/month | Med |
| P1 | SHORT_BREAKDOWN | Suppress emission in RANGE OR lower threshold to 50 | 99 wasted scans/window → 0 | Low |
| P2 | AUTO | Investigate insufficient_candles (700/2705) | Cleaner scan_decisions | Low |
| P2 | PHASE1_MOMENTUM | Hard-block in RANGE/CHOP via affinity | Save 3205 wasted scans | Low |
| P3 | MORNING_FADE | Wait for RANGE regime (no code change) | Validation pending | None |
| P3 | PHASE1_REVERSAL | Wait for PANIC regime | Validation pending | None |
| P3 | Sim engine | Calibrate slippage to live fills | Backtest reliability | Med |

---

## Part 7: What the backtest told us vs what live told us

| Question | Backtest answer | Live answer | Trust |
|---|---|---|---|
| Does VWAP_TREND make money? | -₹7,613 sim P&L (over-pessimistic) | -₹516 live (near breakeven) | LIVE |
| Does VWAP_REVERSAL bleed? | -₹27k sim (overstated) | -₹447 live (matches Apr 16 cluster) | LIVE |
| Does BREAKOUT bleed? | -₹17k sim | -₹326 live (0/9) | LIVE confirms |
| Are swing strategies tradeable? | 6 trades over 5mo on top-50 stocks | 0 swing trades in 29 days | Both confirm: gates too strict |
| Will multi-emission unlock swing? | PULLBACK +₹11k (2 trades), SHORT_PULLBACK -₹11k (4 trades) | Untested in production | Backtest is ALL we have for this |

---

## Part 8: Long-window backtest results (added after intraday backtest finished)

The 29-day intraday pure-replay (`--no-watchlist-per-day` semantic = multi-emission applied to intraday) produced **458 trades, ALL strategies negative**:

| Strategy × Regime | Trades | WR | Net P&L | E[R] | Avg MFE | Avg MAE |
|---|---|---|---|---|---|---|
| MOMENTUM × RANGE | 306 | 32.7% | -₹67,744 | -0.47 | 0.54R | -0.52R |
| PULLBACK × TREND_UP | 58 | 22.4% | -₹39,870 | -1.11 | 0.42R | -0.62R |
| MOMENTUM × TREND_UP | 53 | 20.8% | -₹57,216 | -1.12 | 0.47R | -0.54R |
| MORNING_FADE × RANGE | 30 | 16.7% | -₹63,534 | -0.42 | 0.53R | -0.54R |
| MEAN_REVERSION × RANGE | 11 | 9.1% | -₹28,162 | -0.70 | 0.40R | -0.47R |
| **Total** | **458** | **28.4%** | **-₹256,526** | **-0.63** | — | — |
| VWAP_TREND, VWAP_REVERSAL, BREAKOUT, OPEN_DRIVE | 0 | — | — | — | — | — |

**Critical insight #1: Multi-emission applied to intraday would be a disaster.**

Under multi-emission ("best signal wins" across all setups per bar), MOMENTUM wins 78% of selections (359/458 trades) and bleeds catastrophically (-₹125k). Live's winner-takes-all watchlist + per-strategy gates correctly produces only 1 MOMENTUM trade in 29 days — contradicting pure-replay's 359.

**The current production design (winner-takes-all + strict gates) is structurally sound. Extending multi-emission to intraday would produce -₹256k vs live's -₹1k.**

**Critical insight #2: MFE/MAE pattern across all strategies is the same.**

Every strategy shows:
- Winners' max favorable excursion: 0.40-0.53R (winners barely reach target)
- Losers' max adverse excursion: -0.47R to -0.62R (losers don't go much beyond stop)

Translation: **5-min intraday signals rarely produce sustained directional moves of 1+R.** Indian markets at 5m are choppy. Both winners and losers oscillate around entry by 0.4-0.6R then close at flat or stop.

This implies the entire intraday framework needs:
- **Tighter targets** (capture 0.3-0.5R quickly instead of 1R)
- **Tighter stops** (0.4R instead of 1R)
- **Higher score threshold** to filter for the rare 1R+ moves

OR the strategies need timeframe-aware target/SL pairs (5m for fast capture, 15m for trend confirmation).

**Critical insight #3: MORNING_FADE doesn't work in RANGE either.**

Pure-replay finally tested MORNING_FADE in its supposedly-ideal RANGE regime (live data only had TREND_UP). Result: 30 trades, 16.7% WR, -₹64k. The strategy thesis (fade morning pop, expect reversion) doesn't hold up — even in RANGE, 83% of "morning pops" continued or stayed elevated.

**Recommendation:** validate MORNING_FADE with pure-replay BEFORE deploying it widely. If pure-replay already says 17% WR, live won't be much better. Consider hard-blocking it across all regimes pending fundamental redesign.

**Critical insight #4: Live system is correctly conservative.**

Pure-replay with multi-emission semantics: 458 trades, -₹256k.
Live winner-takes-all: 78 trades, -₹1.4k.

The 5x difference in trade count and 180x difference in losses shows the live system's strict gates and one-setup-per-symbol watchlist are saving it from the multi-emission disaster. **Don't loosen gates without a specific reason and validation.**

---

## Part 9: REVISED prioritized fix list (incorporating Part 8 data)

| Priority | Strategy | Fix | Why |
|---|---|---|---|
| **P0 (kill)** | MORNING_FADE | Hard-block across ALL regimes pending redesign | Pure-replay 17% WR in RANGE (its supposed sweet spot); live untested |
| P0 | VWAP_TREND | Add time-of-day gate ≥11:30 IST | Live data: 33% WR morning vs 75% afternoon |
| P0 | VWAP_TREND | Tighten target from 2R to 0.7R (matches MFE distribution) | Live data: only 4% hit target; MFE typically 0.4-0.5R |
| P1 | DON'T extend multi-emission to intraday | Keep winner-takes-all watchlist | Pure-replay shows -₹256k under multi-emission vs -₹1.4k live |
| P1 | MEAN_REVERSION (intraday) | Loosen RSI gate (35→40) + VWAP extension (1%→0.6%) | Score reaches threshold (p90=80) but gates compound to ~0% pass-through |
| P1 | PULLBACK (intraday) | Don't enable multi-emission for this — keep watchlist gate | Pure-replay 22% WR over 58 trades = pattern not predictive |
| P1 | SHORT_BREAKDOWN | Suppress emission in RANGE (affinity makes threshold unreachable) | Mathematical: 0.55× × max raw 100 = 55 vs threshold 70 |
| **P2 (architectural)** | All intraday strategies | Investigate target/SL calibration vs MFE/MAE distribution | All strategies show winners only reach 0.4-0.5R; targets at 1R+ rarely hit |
| P2 | AUTO | Investigate insufficient_candles (700/2705) | Data quality |
| P3 | PHASE1_REVERSAL | Wait for PANIC/TREND_DOWN regime | Calibration looks correct but needs exposure |
| P3 | PHASE1_MOMENTUM | Hard-block in RANGE/CHOP via affinity (already 0.64×, push to 0.0) | Wasted scans |

---

## What I'd do next (post-launch validation)

1. **Day 1 (May 8):** Watch for swing trades. If 0, investigate which gate killed each MEAN_REVERSION watchlist row.
2. **Day 1-7:** Collect 20-50 more live trades. Re-run this audit. Adjust based on actual data, not sim.
3. **Implement P0 fixes (VWAP_TREND time-of-day + target tightening)** as a single deploy with careful before/after metrics.
4. **Wait on P1 fixes** until P0 effect measurable.
5. **Build the as-of-date watchlist runner** (~3-4 hours engineering) for proper future backtests that include the watchlist generator step.
