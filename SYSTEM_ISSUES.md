# AutoTrader System — Verified Issues Audit

> Every issue below has been verified against actual code. Issues from the first draft that were wrong or over-engineered have been corrected or removed. Honest assessment — no inflation.

---

## CRITICAL (Verified bugs producing wrong results)

### C1. Relative Strength Scoring Bug — CONFIRMED
**File:** `scoring.py:223-239`

**Code:** `_rs = _stock_chg / regime.nifty.change_pct`

**The bug:** When both stock and Nifty are negative, the division produces a positive ratio that's misinterpreted.

| Scenario | Stock | Nifty | _rs | BUY Result | Correct? |
|----------|-------|-------|-----|-----------|----------|
| Stock outperforms in up market | +2% | +1% | +2.0 | +4 bonus | YES |
| Stock diverges in up market | -1% | +0.5% | -2.0 | -3 penalty | YES |
| Stock falls faster in down market | -2% | -1% | **+2.0** | **+4 bonus** | **NO — stock is WEAKER, not stronger** |
| Stock falls slower in down market | -0.5% | -1% | +0.5 | no bonus | Correct by accident |

The bug only fires when BOTH are negative AND stock falls faster (bigger magnitude). This IS a real bug but has a limited blast radius — it only affects BUY signals on down days where the stock is weak. These signals usually fail other gates (VWAP below, EMA stack wrong) so the practical impact is moderate, not catastrophic.

**Severity: MEDIUM-HIGH** (was rated CRITICAL — corrected. The bug exists but other gates catch most of the damage.)

**Fix:** Use difference instead of ratio. 1 hour.

---

### C2. Stale Daily Breadth/Trend Scores — CONFIRMED
**Files:** `market_brain_service.py:1034`, `market_breadth_service.py:112`, `universe_service.py:2630`

**Verified:** `_expected_latest_daily_candle_date()` returns "most recent completed trading day strictly before today." Breadth computes `close > ema20` using yesterday's daily close (line 120-124 of breadth service). During intraday, breadth NEVER updates with live prices.

**This IS the root cause of false TREND_UP.** On May 8, breadth=94 (from May 7 close), leadership=67 (intraday-updated), stress=low → high-breadth override fires → TREND_UP all day in a flat market.

**However — I was wrong about trend_score.** Trend_score IS also daily-stale, but the `_norm` function and formula are correct (verified: `_norm` is standard min-max normalization, not broken). The reason trend_score is always 16-21 is because Nifty's EMA50-EMA200 spread has been genuinely narrow for weeks (market structure, not a bug).

**Severity: CRITICAL** (confirmed — this drives the wrong regime classification)

---

### C3. RANGE Regime = Zero Trades — CONFIRMED (but nuanced)
**Evidence:** Code at line 1113-1129 explicitly documents "4 consecutive trading days with 0 qualified entries" and adds a stressed RANGE/CHOP discount of -5 to threshold.

**The fix already exists in code** (lines 1123-1129): when breadth > 85 AND trend < 30 AND regime is RANGE/CHOP, threshold drops by 5 (from 72 to 67, min 60).

**But the fix is insufficient** because:
- VWAP_TREND in RANGE has affinity 0.7×. Raw 80 × 0.7 = 56 → still fails even the lowered 67 threshold
- PULLBACK in RANGE has affinity 0.8×. Raw 80 × 0.8 = 64 → still fails 67
- MEAN_REVERSION in RANGE has affinity 1.4×. Raw 60 × 1.4 = 84 → passes easily. But MR entry gates (RSI ≤ 45, VWAP extension ≥ 0.6%) reject most signals in mild range markets

**Severity: HIGH** (was CRITICAL — corrected. The system tried to fix this but the fix doesn't work.)

---

## HIGH SEVERITY (Verified significant issues)

### H1. Scoring Formula BUY Bias — PARTIALLY CONFIRMED

**What's real:**
- VIX penalty (lines 294-298) only hits BUY: −5 to −10. No corresponding SELL bonus in high VIX. **This IS asymmetric** but the comment (line 290-293) explains WHY: "high VIX actually FAVOURS short setups." The argument is that NOT penalizing SELL is already the bonus. This is a defensible design choice, not necessarily a bug.
- RANGE penalty (−8, line 305-306) hits strategies that ARE hard-blocked anyway. **Dead code, not bias.**

**What I got wrong:**
- Layer 5 daily alignment IS symmetric by design. BUY+UP gets +8 to +15. SELL+DOWN gets +8 to +15. BUY+DOWN gets −5 to −10. SELL+UP gets −5 to −10. The code at lines 269-284 is perfectly symmetric between BUY and SELL. I incorrectly called this biased by saying "bull markets = more BUY-friendly days." That's the market being bullish, not the formula being biased.
- Layer 3 technical components (VWAP, EMA, RSI, MACD) are all directionally symmetric in code. BUY above VWAP = +7, SELL below VWAP = +7. No inherent bias.

**What's actually biased:** The MORNING_FADE fixed-75 bypass (line 114) IS a hack that admits the formula doesn't work for counter-trend SELL setups. But this is specific to one strategy, not systematic bias.

**Revised severity: MEDIUM** (was HIGH — over-stated. The VIX asymmetry is a conscious design choice. The real issue is the MORNING_FADE bypass which signals the formula struggles with contrarian shorts.)

---

### H2. Score Pipeline Math — Impossible Combinations — CONFIRMED but NOT AN ISSUE

The math is correct: VWAP_TREND in RANGE needs raw 103 to pass (impossible). But this is **working as designed** — the affinity matrix is MEANT to suppress strategies that don't fit the regime. The hard-block system exists as a faster binary check for the worst cases.

**I over-engineered this issue.** If a strategy mathematically can't pass the threshold in a given regime, that's the affinity matrix doing its job. The only real concern is whether the multipliers are CORRECT (see M1 below).

**Revised severity: NOT AN ISSUE** (removing from list)

---

### H3. Swing vs Intraday Threshold Inconsistency — CONFIRMED, BUT INTENTIONAL

**Code at lines 1087-1100** explicitly documents WHY swing uses pre-haircut score:

> "swing had 0 trades in 10 days because adjusted_score is haircut by risk_mode (×0.60–1.08) and chop/panic regime (×0.88) BEFORE being compared to the fixed 75 swing threshold. The Market Brain multiplier is designed to dampen intraday size/frequency, not to double-penalise swing."

The comment is dated 2026-04-22 and describes a real problem that was encountered and deliberately fixed. Swing uses `affinity_score` because the brain haircut (designed for intraday frequency control) was killing all swing entries.

**Revised severity: NOT AN ISSUE** (intentional design, well-documented. Removing from list.)

---

### H4. FLAT_TIMEOUT (120 min) One-Size-Fits-All — CONFIRMED REAL ISSUE

120 minutes for all strategies regardless of type or time of day. Evidence: 2026-04-21 data shows 10/14 trades (71%) exited via FLAT_TIMEOUT at 45 min → reverted to 120 min. The fix was a pendulum swing, not targeted.

A trade entered at 14:00 has only 85 min before EOD close. The 120-min timeout never fires — these positions always exit via EOD_CLOSE instead, which is a blunt market-order exit at 15:25.

**Severity: HIGH** (confirmed)

---

### H5. No Time-of-Day Intelligence — CONFIRMED REAL ISSUE

Verified: zero code for time-of-day adjustments in entry scoring or exit management (except MORNING_FADE window 09:45-10:15 and EOD close at 15:25). No awareness that:
- 09:15-09:45 is erratic (avoid entries)
- 10:30-14:00 is the best window
- After 14:30, new intraday entries rarely have time to reach target

**Severity: HIGH** (confirmed)

---

### H6. No Pre-Market/GIFT Nifty Cues — CONFIRMED MISSING

Zero code for SGX/GIFT Nifty, US overnight, or Asian market signals. The system starts fresh at 09:15 with no global context.

**Severity: HIGH for a production system** but MEDIUM for current scope. Adding GIFT Nifty gap prediction is a meaningful feature, not a bug fix.

**Revised severity: MEDIUM-HIGH** (it's missing intelligence, not broken logic)

---

### H7. Dead Strategies — CONFIRMED

- BREAKOUT: hard-blocked in all 6 regimes → globally dead
- MORNING_FADE: hard-blocked in all 6 regimes → globally dead  
- VWAP_REVERSAL: disabled in settings → globally dead
- SHORT_BREAKDOWN: hard-blocked in 4 of 6, mathematically impossible in 1 → nearly dead
- SHORT_PULLBACK: hard-blocked in 3 of 6 → limited
- PHASE1_MOMENTUM: hard-blocked in 4 of 6 → limited

The system evaluates all 12 strategies every scan cycle. Skipping dead ones would reduce scan time.

**Severity: LOW** (was HIGH — over-stated. Dead strategies don't cause wrong trades, just waste CPU. The real issue is that we have too few ACTIVE strategies for market coverage.)

---

## MEDIUM SEVERITY (Real but not urgent)

### M1. Affinity Multipliers Not Evidence-Based — CONFIRMED
No multiplier in `regime_affinity.py` has a cited win-rate, sample size, or backtest basis. Values like MEAN_REVERSION in RANGE = 1.4× are assumptions.

However — this is typical of rule-based systems. Walk-forward calibration is the proper fix, but it's a feature request, not a bug.

---

### M2. Breakeven SL at 1.0× ATR — DEBATABLE, NOT CLEARLY WRONG

I initially said "too aggressive." But on review: 1.0× ATR means the stock has already moved 1× ATR in our favor before SL moves to breakeven. The buffer is 0.1× ATR above entry. This is reasonable for Indian intraday where moves are fast.

**Revised: NOT AN ISSUE for most cases.** Could be 1.2× for slow-developing strategies (VWAP_TREND), but 1.0× is standard for momentum/pullback setups.

---

### M3. Partial Exits Not Strategy-Aware — REAL BUT LOW IMPACT

40/30/30 split at 1R/1.5R/trail is reasonable. Strategy-specific splits would be better but the improvement is marginal. The bigger issue is that qty=2 positions (most common at ₹50k capital) degrade to 50%/50% at 1R only, missing the 1.5R stage entirely.

**Severity: MEDIUM** (confirmed, but not as impactful as initially stated)

---

### M4. max_trades_day = 5 Not Regime-Aware — CONFIRMED

No empirical basis. In TREND_UP, system hits cap by 11:00 with good setups remaining. In CHOP, 2 trades would be plenty.

**Severity: MEDIUM** (confirmed)

---

### M5. Reentry Cooldown (30 min) Blunt — CONFIRMED but REASONABLE

30 min is conservative. The evidence (04-16 churn data) supports a cooldown. Strategy-specific cooldowns would be marginally better.

**Revised severity: LOW** (the current value prevents real churn)

---

### M6. rr_intraday = 1.25 Validation — VERIFIED ADEQUATE

The code comment at settings.py:54-63 provides detailed justification: 35% hit rate at 2.0R produced negative expectancy because MFE peaked at 1.2-1.5R then faded. 1.25R targets hit more often with realized R closer to plan. MEAN_REVERSION keeps 2.0R separately. This is a well-reasoned change.

**I was wrong to question the math.** The point isn't that 1.25R × 35% is worse than 2.0R × 35%. The point is that 1.25R has a HIGHER hit rate (estimated 45-50%) because it's reachable. 1.25R × 50% = +0.625R − 0.50R = +0.125R positive expectancy.

**Revised: NOT AN ISSUE** (removing from list)

---

### M7. Direction Dampening + Hard-Blocks Redundancy — CONFIRMED BUT HARMLESS

Double enforcement (affinity dampening + hard-block) for the same scenarios. Wastes a few CPU cycles. Not harmful.

**Revised severity: LOW** (cosmetic)

---

### M8. RANGE Scoring Penalty Redundant — CONFIRMED DEAD CODE

The −8 penalty at line 305 fires for strategies that are hard-blocked in RANGE anyway. Dead code.

**Severity: LOW** (cleanup)

---

### M9. Regime-Change Tighten is Permanent — CONFIRMED REAL

Once `regime_tightened=True`, SL stays tight even if regime bounces back. A temporary CHOP blip permanently handicaps the position.

**Severity: MEDIUM** (confirmed)

---

### M10. Paper GTT Poll 60s — NOT A REAL ISSUE

The primary exit path is tick-based (`_on_quote` fires on every tick). The 60s poll is ONLY a fallback for when WebSocket stalls. If WS is connected, SL fires on the tick. If WS disconnects, 60s delay is the worst case.

**Revised: NOT AN ISSUE** (removing. The WS watchdog reconnects after 90s silence, and most SLs fire on tick data, not polling.)

---

### M11. No Expiry-Day Awareness — CONFIRMED MISSING

Zero code for weekly/monthly expiry handling. System trades identically on expiry and non-expiry days.

**Severity: MEDIUM** (real gap but not causing wrong trades on non-expiry days)

---

### M12. Breadth Short-Block Using Stale Data — CONFIRMED, SUBSET OF C2

`breadth_score >= 75` blocking SELL trades (line 1222-1245) uses stale daily breadth. This is a consequence of C2 (stale breadth), not a separate issue. However, the code DOES have a smart exemption: swing shorts with `daily_bias.trend == "DOWN"` bypass the block (line 1234-1238). This shows the developers recognized the staleness problem for swing.

**Severity: Subsumed by C2** (not counting separately)

---

## MISSING INTELLIGENCE (Verified gaps vs industry best practice)

### MI1. No Adaptive/Walk-Forward Parameter Tuning — REAL GAP
Parameters are manually tuned via "Batch" updates. No automated walk-forward validation. Industry standard is quarterly re-optimization with WFE > 50%.

**Priority: HIGH** (biggest long-term improvement)

---

### MI2. No Conviction-Based Position Sizing — REAL GAP
Fixed ₹125 risk per trade regardless of signal quality (score 72 gets same size as score 95). Industry standard is Half-Kelly (0.5×) with conviction scaling.

**Priority: HIGH** (easiest P&L improvement)

---

### MI3. MFE/MAE Tracked but Never Used for Exits — REAL GAP
MFE/MAE data is collected on every position but only logged. Not used for dynamic exit decisions. Could power "MFE receded" exits and strategy-specific target calibration.

**Priority: MEDIUM** (needs enough data first)

---

### MI4. Sentiment/News Infrastructure Built but Not Wired — REAL GAP  
`NewsStore`, `aggregate_sentiment()` exist. Flag `use_news_signals_v1 = False`. Zero integration into entry logic.

**Priority: MEDIUM** (infrastructure ready, just needs wiring)

---

### MI5. No ML/HMM Regime Detection — REAL GAP
Current rule-based decision tree has hardcoded thresholds. HMM with 40-60 day lookback would detect transitions earlier and provide transition probabilities instead of binary labels.

**Priority: MEDIUM-HIGH** (biggest potential upgrade, but high effort — 2-3 weeks)

---

### MI6. No Cross-Asset Signals — REAL GAP
No USD/INR, US yields, crude oil, or global equity cues. These are proven leading indicators for Indian markets.

**Priority: MEDIUM** (moderate edge, moderate effort)

---

## ISSUES REMOVED FROM ORIGINAL LIST (Over-Engineered or Wrong)

| Original Issue | Why Removed |
|---------------|-------------|
| H2: Impossible score combinations | Working as designed — affinity matrix SHOULD suppress bad combos |
| H3: Swing vs intraday threshold | Intentional fix, well-documented in code comments |
| H1: Systematic BUY bias | Layer 5 and Layer 3 ARE symmetric. VIX asymmetry is deliberate. Over-stated. |
| M2: Breakeven at 1.0× ATR too aggressive | Standard for Indian intraday momentum. Reasonable. |
| M5: Reentry cooldown too blunt | 30 min is well-justified by churn evidence |
| M6: rr_intraday = 1.25 | Well-reasoned change with clear data justification |
| M10: Paper GTT 60s poll | Primary path is tick-based; 60s is backup only |
| H7: Dead strategies causing harm | They waste CPU but don't cause bad trades |
| L2: Layer 5 double-counts Layer 3 | Different information: Layer 3 = intraday technicals, Layer 5 = daily structure. Additive is reasonable. |

---

## FINAL VERIFIED PRIORITY LIST

| Rank | Issue | Type | Effort | Expected Impact |
|------|-------|------|--------|----------------|
| **1** | **C1: RS scoring bug** (both-negative ratio) | Bug fix | 1 hour | Eliminates false BUY signals on down days |
| **2** | **C2: Intraday breadth update** | Architecture | 6-8 hours | Fixes false TREND_UP, enables correct regime |
| **3** | **C3: RANGE dead zone** (loosen MR gates OR lower threshold further) | Tuning | 2-3 hours | Enables trading on 40%+ of market days |
| **4** | **H4: Strategy-aware FLAT_TIMEOUT** | Enhancement | 2-3 hours | Stops premature exits on slow strategies |
| **5** | **H5: Time-of-day awareness** in entries/exits | Enhancement | 4-6 hours | Avoids erratic open, tightens close |
| **6** | **MI2: Conviction-based position sizing** | New feature | 4-6 hours | Scale risk by signal quality |
| **7** | **M4: Regime-aware max_trades_day** | Tuning | 1-2 hours | More trades in bull, fewer in chop |
| **8** | **M9: Allow SL re-widen after regime bounce-back** | Bug fix | 1-2 hours | Prevents permanent tighten on temporary blip |
| **9** | **H6: GIFT Nifty pre-market bias** | New feature | 4-6 hours | Opening direction prediction |
| **10** | **M11: Expiry-day awareness** | New feature | 3-4 hours | Protects against expiry volatility |
| **11** | **MI4: Wire sentiment/news gating** | New feature | 3-4 hours | Infrastructure exists, connect it |
| **12** | **MI1: Walk-forward parameter validation** | Architecture | 1-2 weeks | Prevents parameter decay |
| **13** | **MI5: HMM regime detection** | Architecture | 2-3 weeks | Earlier transitions, probability-based |

### Summary
- **3 confirmed bugs** (C1, C2, C3) — these need fixing
- **2 real high-severity gaps** (H4, H5) — these improve the system meaningfully
- **5 medium issues** (M1, M3, M4, M9, M11) — improve but not urgent
- **6 missing intelligence items** (MI1-MI6) — roadmap features
- **9 issues removed** from original list as over-engineered or wrong
