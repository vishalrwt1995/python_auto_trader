# AutoTrader System Algorithm — Complete Reference

> **Purpose:** Every logic path, gate, threshold, and decision our system makes — from market open to position close — across all 6 market regimes. Written for review before any code changes.

---

## 1. MARKET REGIME CLASSIFICATION

The Market Brain classifies each trading day into one of 6 regimes using 4 core scores (all 0–100):

| Score | Source | Update Frequency |
|-------|--------|-----------------|
| `trend_score` | Nifty daily candles (EMA50/200 spread 35%, trend direction 25%, close vs EMA200 20%, ATR volatility 20%) | **Once per day** (stale intraday) |
| `breadth_score` | Universe daily candles (8 components: %above EMA20/50, positive returns, near highs/lows, median return, A/D ratio) | **Once per day** (stale intraday) |
| `leadership_score` | Top-decile relative strength concentration | Intraday updates |
| `volatility_stress_score` | VIX 28%, ATR 24%, range expansion 18%, gap 14%, chop risk 10%, VWAP slope 6% | Intraday updates |

### Regime Decision Tree (evaluated top-to-bottom, first match wins)

```
1. PANIC
   stress >= 82  OR  breadth <= 12  OR  data_quality <= 30
   → Everything is broken. Lockdown mode.

2. TREND_UP (two paths)
   Path A (standard):  trend >= 70  AND  breadth >= 62  AND  leadership >= 56  AND  stress <= 48
   Path B (high-breadth override):  breadth >= 80  AND  leadership >= 60  AND  stress <= 48
   → Bull market. Momentum strategies thrive.
   NOTE: Path A has NEVER fired (trend_score stuck at 16-21 for weeks).
         Path B is the ONLY way we enter TREND_UP — via stale daily breadth.

3. TREND_DOWN
   trend <= 36  AND  breadth <= 40  AND  leadership <= 45
   → Bear market. Short-side and oversold bounces.

4. CHOP
   stress >= 62  AND  leadership <= 46  AND  risk_appetite <= 46
   → Noisy, directionless. Only mean-reversion works.

5. RECOVERY (only from PANIC/TREND_DOWN/CHOP)
   prev_regime in {PANIC, TREND_DOWN, CHOP}  AND  trend >= 40  AND  breadth >= 35  AND  leadership >= 40
   → Turning point. Early leaders extend.

6. RANGE (default)
   Nothing above matched.
   → Sideways market. Mean-reversion and selective trend-following.
```

### Hysteresis (prevents regime flipping)
- **PANIC hold:** stays PANIC while stress >= 65 AND breadth <= 22
- **TREND_UP hold:** stays while trend >= 60 AND breadth >= 55 AND leadership >= 50
- **Re-entry cooldown:** minimum 240 seconds between regime transitions

### Risk Mode (layered on top of regime)
| Mode | Condition | Effect |
|------|-----------|--------|
| LOCKDOWN | stress >= 85 OR data_quality <= 35 | Size ×0.40, positions ×0.50, threshold 58 |
| DEFENSIVE | stress >= 65 OR quality <= 55, or regime in {PANIC, CHOP, TREND_DOWN} | Size ×0.65, positions ×0.70, threshold 65 |
| AGGRESSIVE | appetite >= 66 AND stress <= 50 AND quality >= 65, regime in {TREND_UP, RECOVERY} | Size ×1.15–1.50, positions ×1.25–1.50, threshold 75 |
| NORMAL | default | Size ×1.0, positions ×1.0, threshold 72 |

---

## 2. ALL 12 STRATEGIES

### Strategy Taxonomy

| # | Strategy | Direction | Thesis |
|---|----------|-----------|--------|
| 1 | BREAKOUT | LONG | Price breaking above N-bar high near 52-week high with volume surge |
| 2 | SHORT_BREAKDOWN | SHORT | Price breaking below N-bar low with volume surge |
| 3 | PULLBACK | LONG | Pullback to EMA21 in intact uptrend (bull EMA stack) |
| 4 | SHORT_PULLBACK | SHORT | Rally to EMA21 in intact downtrend |
| 5 | MEAN_REVERSION | LONG/SHORT | Bollinger/RSI extreme reversal at VWAP extension |
| 6 | VWAP_REVERSAL | LONG/SHORT | Same as MEAN_REVERSION (alias). **Currently DISABLED** (23% WR, -0.61% avg P&L) |
| 7 | VWAP_TREND | LONG/SHORT | Intraday trend continuation on correct side of VWAP |
| 8 | MOMENTUM | LONG/SHORT | Continuation momentum with ADX expansion and relative strength |
| 9 | OPEN_DRIVE | LONG/SHORT | Opening-range breakout in first hours (09:45–13:30 IST) |
| 10 | PHASE1_MOMENTUM | LONG only | Pre-market selection: top daily momentum + liquidity stocks |
| 11 | PHASE1_REVERSAL | LONG only | Pre-market selection: oversold-bounce candidates |
| 12 | MORNING_FADE | SHORT only | Fade morning pop >1.5% within 09:45–10:15 IST window |

### Strategy-Specific Entry Gates

Every strategy must pass these hard gates BEFORE scoring matters:

**BREAKOUT / SHORT_BREAKDOWN:**
- ADX >= 20
- BUY: within 5% of 52-week high
- SELL: at least 5% off 52-week high (avoids shorting at ATH)
- Volume ratio >= 1.2

**PULLBACK / SHORT_PULLBACK:**
- BUY: intact EMA stack (fast > med > slow)
- SELL: fast EMA < med EMA
- BUY RSI: 35–70 | SELL RSI: 38–65
- Price within ±5% of fast EMA

**MEAN_REVERSION / VWAP_REVERSAL:**
- BUY: price < VWAP (oversold side)
- SELL: price > VWAP (overbought side)
- BUY RSI: <= 45 (RANGE/CHOP) or <= 40 (others)
- SELL RSI: >= 58 (RANGE/CHOP) or >= 60 (others)
- VWAP extension >= 0.6%

**VWAP_TREND:**
- BUY: price > VWAP | SELL: price < VWAP
- ADX >= 18

**MOMENTUM:**
- ADX >= 20
- Volume ratio >= 1.3
- BUY: price > VWAP, RSI 55–75, EMA9 > EMA21
- SELL: price < VWAP, RSI 25–45, EMA9 < EMA21

**OPEN_DRIVE:**
- ADX >= 18
- Volume ratio >= 1.2
- Correct VWAP side
- Time: 09:45–13:30 IST only

**PHASE1_MOMENTUM:**
- LONG only
- Volume ratio >= 0.8

**PHASE1_REVERSAL:**
- LONG only
- RSI <= 55
- Volume ratio >= 0.8

**MORNING_FADE:**
- SHORT only
- Time: 09:45–10:15 IST
- Pop from open >= 1.5%
- Volume ratio >= 1.0

### Swing-Specific Extra Gates (on top of intraday gates)

Swing trades must ALSO pass daily-timeframe confirmations:

| Strategy | Swing Gate |
|----------|-----------|
| BREAKOUT | Daily ADX >= 25, daily trend = UP, volume >= 1.3 |
| PULLBACK | Daily EMA stack intact, daily RSI 40–60 |
| MOMENTUM | LONG only, daily trend UP, daily EMA stack, daily SuperTrend UP, daily ADX >= 20, daily strength >= 50, daily RSI 50–75, volume >= 1.0 |
| MEAN_REVERSION | Daily RSI <= 45 (RANGE) or <= 35 (others) for BUY; price near daily support (within 10%) |

---

## 3. THE SCORING PIPELINE

### Step 1: Raw Score (7-Layer Composition, max 100)

**MORNING_FADE bypasses this entirely** → fixed score of 75 (standard formula is bullish-biased and would give SELL trades ~30-50).

For all other strategies:

| Layer | Max | Components |
|-------|-----|-----------|
| **Regime** | 20 | Nifty direction alignment (+8), VIX level (+7), FII flow alignment (+5) |
| **Options** | 15 | Static PCR (+5), OI change PCR (+3/−2), max-pain proximity (+7) |
| **Technical** | 35 | SuperTrend (+9), VWAP position (+7), EMA stack (+6), RSI zone (+6), MACD (+7), ADX (+4), candlestick pattern (+2), relative strength (+4/−3) |
| **Volume** | 10 | Volume ratio (+7), OBV alignment (+3) |
| **Alignment** | 15 to −10 | Daily bias trend alignment (+8 to +15 with strength), neutral (+5), opposing (−6 to −10) |

**Penalties (subtracted after layers):**
- VIX > 22 on BUY: −10
- VIX > 18 on BUY: −5
- RANGE regime + non-MR strategy: −8
- ADX < 15 + non-RANGE: −5
- Large candle (>2.5% body): −5
- Doji pattern: −3
- Bollinger Band extreme: −5
- Stochastic extreme: −4

**Result: raw_score = clamp(0, 100, sum of all layers − penalties)**

### Step 2: Affinity Multiplier

`affinity_score = raw_score × regime_strategy_multiplier(regime, strategy, direction)`

The multiplier comes from a 6×12 matrix. Key values:

| | TREND_UP | TREND_DOWN | RANGE | CHOP | PANIC | RECOVERY |
|---|---------|-----------|-------|------|-------|----------|
| **PULLBACK** | 1.2 | 0.5 | 0.8 | 0.5 | 0.3 | 1.0 |
| **MEAN_REVERSION** | 0.5 | 0.6 | **1.4** | **1.2** | 0.8 | 0.7 |
| **VWAP_TREND** | 1.1 | 0.7 | 0.7 | 0.4 | 0.2 | 1.0 |
| **MOMENTUM** | **1.4** | 0.3 | 1.1 | 0.4 | 0.2 | **1.3** |
| **PHASE1_REVERSAL** | 0.6 | **1.2** | 1.0 | 0.9 | 0.9 | 1.1 |
| **MORNING_FADE** | 0.3 | 1.0 | **1.4** | **1.3** | 0.6 | 0.7 |

**Direction dampening:** In TREND_UP, SELL direction capped at 0.6× (except counter-trend strategies: MEAN_REVERSION, VWAP_REVERSAL, PHASE1_REVERSAL). In TREND_DOWN, BUY capped at 0.6× (same exceptions).

Range: [0.2, 1.4]

### Step 3: Brain Haircut

`adjusted_score = adjust_signal(affinity_score, brain_state)`

Applies risk-mode and regime penalties:
- DEFENSIVE/LOCKDOWN: 0.60–0.82× haircut
- CHOP/PANIC: additional ~0.88× haircut
- TREND_UP/AGGRESSIVE: no penalty or slight boost

### Step 4: Threshold Check

| Trade Type | Score Used | Threshold |
|-----------|-----------|-----------|
| **Intraday** | adjusted_score (post-haircut) | 72 (NORMAL), 75 (AGGRESSIVE), 65 (DEFENSIVE), 58 (LOCKDOWN) |
| **Swing** | affinity_score (pre-haircut) | 65 |

**Stressed RANGE/CHOP discount:** If regime in {RANGE, CHOP} AND breadth >= 85 AND trend < 30 → threshold drops by 5 (min 60).

---

## 4. HARD BLOCKS (Binary Kill Switch)

These override scoring entirely — strategy is blocked regardless of score:

| Regime | Hard-Blocked Strategies | Allowed Strategies |
|--------|------------------------|-------------------|
| **TREND_UP** | BREAKOUT, MORNING_FADE, SHORT_BREAKDOWN, SHORT_PULLBACK, PHASE1_MOMENTUM | PULLBACK, MEAN_REVERSION, VWAP_TREND, MOMENTUM, OPEN_DRIVE, PHASE1_REVERSAL |
| **TREND_DOWN** | BREAKOUT, MORNING_FADE | SHORT_BREAKDOWN, SHORT_PULLBACK, PULLBACK, MEAN_REVERSION, VWAP_TREND, MOMENTUM, OPEN_DRIVE, PHASE1_MOMENTUM, PHASE1_REVERSAL |
| **RANGE** | BREAKOUT, SHORT_BREAKDOWN, OPEN_DRIVE, PHASE1_MOMENTUM, SHORT_PULLBACK, MORNING_FADE | PULLBACK, MEAN_REVERSION, VWAP_TREND, VWAP_REVERSAL, MOMENTUM, PHASE1_REVERSAL |
| **CHOP** | BREAKOUT, SHORT_BREAKDOWN, PULLBACK, SHORT_PULLBACK, OPEN_DRIVE, PHASE1_MOMENTUM, MOMENTUM, MORNING_FADE | MEAN_REVERSION, VWAP_REVERSAL, VWAP_TREND, PHASE1_REVERSAL |
| **PANIC** | BREAKOUT, PULLBACK, OPEN_DRIVE, PHASE1_MOMENTUM, MOMENTUM, MORNING_FADE | SHORT_BREAKDOWN, SHORT_PULLBACK, MEAN_REVERSION, VWAP_REVERSAL, VWAP_TREND, PHASE1_REVERSAL |
| **RECOVERY** | MORNING_FADE | Everything else allowed |

**Additionally killed globally:** VWAP_REVERSAL (disabled_strategies setting), BREAKOUT (hard-blocked in all 6 regimes — 0/9 live WR), MORNING_FADE (hard-blocked in all 6 regimes — 17% WR in backtest).

---

## 5. THE COMPLETE ENTRY FUNNEL (18 Gates)

A signal must pass ALL of these in order to become a trade:

```
 1. Market open?                     → Must be within IST trading hours
 2. Kill switch off?                 → Manual emergency stop check
 3. Brain state built?               → Market brain computed (stale = cap to DEFENSIVE)
 4. Daily profit target not hit?     → PnL >= +₹375 → stop all trading
 5. Daily loss limit not hit?        → PnL <= -₹300 → block directional (allow MR/PHASE1_REV only)
 6. Daily trade count < 5?           → max_trades_day hard cap
 7. Entry time window open?          → 09:45–15:10 IST (strategy-specific windows exist)
 8. Direction != HOLD?               → Direction voting must produce BUY or SELL
 9. Score >= threshold?              → Intraday: adjusted_score >= 72 | Swing: affinity_score >= 65
10. Strategy not hard-blocked?       → Regime hard-block check
11. Strategy not policy-blocked?     → market_policy.allowed_strategies whitelist
12. Short not disabled?              → Policy short_enabled flag
13. Breadth not too bullish?         → SELL + regime in {RANGE,CHOP,RECOVERY} + breadth >= 75 → blocked
14. SL not too wide?                 → ATR-based SL must allow qty >= 1 within risk budget
15. Signal not stale?                → |live_price − signal_price| < max(1.2%, 1.5×ATR%)
16. Live VWAP guard?                 → BUY must be above VWAP, SELL below (except MR strategies)
17. Strategy entry gate passed?      → check_strategy_entry() / check_swing_entry()
18. Concentration limits OK?         → Max 2 per sector, max 2 per strategy type
```

**Additional gates (applied in order after above):**
- Reentry cooldown (30 min after exiting same symbol)
- Playbook hard-block — M2 flag-gated (strategy + regime + direction + risk_mode, fail-closed)
- Expected edge R gate — M3 flag-gated (blocks if expected_edge_R <= 0)
- Earnings blackout (2 trading days before result date, fail-closed)
- Portfolio book — M4 flag-gated (channel budget + drawdown governors)
- Swing max positions (5 swing positions cap)
- Intraday max positions (3 simultaneous cap)

### Dynamic Score Thresholds

The 72 threshold is the default. It shifts by risk mode:

| Risk Mode | Intraday Threshold | Notes |
|-----------|-------------------|-------|
| AGGRESSIVE | 75 | Bar raised — only best signals in bull runs |
| NORMAL | 72 | Default |
| DEFENSIVE | 65 | Lowered — more entries but lower quality |
| LOCKDOWN | 58 | Very permissive (few signals survive anyway) |

**Stressed RANGE/CHOP discount:** If regime in {RANGE, CHOP} AND breadth >= 85 AND trend < 30 → threshold drops by 5 (min 60).

---

## 6. DIRECTION VOTING

For each signal, 8 technical voters determine BUY/SELL/HOLD:

| Voter | Weight | BUY condition | SELL condition |
|-------|--------|--------------|----------------|
| SuperTrend | 3 | dir = 1 | dir ≠ 1 |
| VWAP position | 2 | close > VWAP | close < VWAP |
| EMA fast vs med | 2 | fast > med | fast < med |
| EMA med vs slow | 1 | med > slow | med < slow |
| RSI momentum | 1 | RSI > 55 | RSI < 45 |
| MACD histogram | 2 | hist > 0 | hist <= 0 |
| MACD cross | 1 | crossed = BUY | crossed = SELL |
| Regime bias | 2 | BULLISH | BEARISH |

**Decision:** BUY if bull > bear + 2 | SELL if bear > bull + 2 | else HOLD (no trade)

**Overrides:**
- MORNING_FADE: forced SELL (ignores voting)
- MR strategies: VWAP voter uses RSI instead (RSI < 40 → BUY, RSI > 60 → SELL)
- Long-only strategies (BREAKOUT, MOMENTUM, PULLBACK, OPEN_DRIVE): SELL result → HOLD
- Short-only strategies: BUY result → HOLD

---

## 7. WATCHLIST CONSTRUCTION

### Intraday Watchlist (2-Phase Pipeline)

**Phase 1 (pre-market, daily data):**
- Universe: top 250–800 stocks by 60-day turnover (tier-dependent)
- PHASE1_MOMENTUM scoring: 40% momentum + 25% liquidity + 20% vol sanity + 15% ADX
- PHASE1_REVERSAL scoring: 25% abs momentum + 20% bounce + 20% RSI reversal + 20% liquidity + 15% vol sanity
- Output: ranked list of momentum leaders and oversold bounce candidates

**Phase 2 (intraday, requires >= 4 bars):**
- Computes live: VWAP slope, volume shock, ORB signals, reversal signals
- Assigns strategy based on conditions:
  - Early session + UP_BREAK + above VWAP + strong volume → OPEN_DRIVE
  - UP_BREAK + above VWAP + moderate volume → BREAKOUT
  - Choppy regime + reversal + extension + below VWAP → MEAN_REVERSION
  - Above VWAP + rising VWAP + moderate volume → PULLBACK
  - Above VWAP → VWAP_TREND
  - Below VWAP → VWAP_REVERSAL
- Phase 2 quality score determines allocation (50–100% of slots)
- Remaining slots filled by Phase 1 fallback rows

**Scanning:** 10 core + 25 rotated symbols per tick (35 total per scan cycle).

**MORNING_FADE overlay:** Between 09:45–10:15 IST, every non-swing watchlist row gets a companion MORNING_FADE row injected. Entry gates filter to only stocks with >1.5% pop from open.

### Swing Watchlist

- Universe: top 500–1500 stocks by turnover (tier-dependent)
- 4 independent component scores per stock: BREAKOUT, PULLBACK, MEAN_REVERSION, MOMENTUM
- Regime-weighted final score:
  - TREND: 30% breakout + 30% momentum + 25% pullback + 15% MR
  - RANGE: 35% MR + 25% pullback + 20% breakout + 20% momentum
  - RISK_OFF: 65% MR + 20% pullback + 10% breakout + 5% momentum
- Multi-setup emission: each component clearing score >= 65 gets its own row
- Scanned FULL BATCH every tick (no rotation)

---

## 8. POSITION SIZING

```
SL distance = ATR × atr_mult (regime/volatility adjusted)
  Base intraday: 1.5 × ATR
  Base swing:    2.5 × ATR

  Regime adjustments to base multiplier:
    LOCKDOWN/PANIC:           × 0.75
    DEFENSIVE/TREND_DOWN/CHOP: × 0.87
    AGGRESSIVE + TREND_UP:     × 1.20
    MR in RANGE/RECOVERY:      × 1.33

  Per-stock ATR% adjustments:
    < 1.5% (low vol):  × 0.87  (tighter SL)
    1.5–3.0% (normal): × 1.20  (wider SL)
    > 3.0% (high vol): unchanged

  Floor: 0.8 × ATR | Ceiling: 3.0 × ATR

Quantity = risk_per_trade / SL_distance
  Intraday risk: ₹125 per trade
  Swing risk:    ₹200 per trade

Target = entry ± (SL_distance × RR_ratio)
  Most strategies:  RR = 1.25 (intraday) | 2.0 (swing)
  MEAN_REVERSION:   RR = 2.0  (intraday) | 2.0 (swing)
```

---

## 9. EXIT LOGIC

### Stop Loss
- **Intraday:** ATR × 1.5 (adjusted by regime/volatility as above)
- **Swing:** ATR × 2.5

### Target
- Hit at RR ratio (1.25× or 2.0× SL distance)
- After target hit → switch to tighter trailing (1.2 × ATR instead of base)

### Breakeven SL
- **Trigger:** price reaches entry + ATR × breakeven_mult
  - Intraday: breakeven_mult = 1.0 (1× ATR move)
  - Swing: breakeven_mult = 1.5 (1.5× ATR move)
- **Action:** SL moves to entry + buffer
  - Intraday buffer: 0.10 × ATR
  - Swing buffer: 0.15 × ATR

### Trailing Stop
- After SL moved to breakeven, trail from best price:
  - Intraday: best − 1.5 × ATR (or 1.2 × ATR after target passed)
  - Swing: best − 2.5 × ATR (or 1.2 × ATR after target passed)
- Only moves UP (BUY) or DOWN (SELL) — never widens

### Partial Exits (intraday only, qty >= 3)
| Stage | Trigger | Action | Qty Exited |
|-------|---------|--------|-----------|
| 1 | Price hits 1R (entry + SL distance) | Exit 40%, SL → breakeven | 40% |
| 2 | Price hits 1.5R | Exit 30%, SL stays | 30% |
| 3 | Trailing from best price | Exit remaining 30% on trail | 30% |

For qty = 2: single stage at 1R, exit 50%, SL → breakeven.
For qty = 1: no partial exits.

### FLAT_TIMEOUT
- **After 120 minutes:** if |current_price − entry| < 0.3 × ATR → exit as FLAT
- Checked continuously on every tick after 120 min elapsed
- Rationale: stock went nowhere for 2 hours, thesis is dead

### Regime-Change Tighten (intraday only)
- If entered in TREND_UP/RECOVERY but regime shifts to CHOP/PANIC/TREND_DOWN:
- Immediately tighten SL to current_price − 0.8 × ATR
- One-shot (doesn't repeat). Swing positions excluded.

### EOD Square-Off
- **15:25 IST:** close all intraday positions (market order)
- **15:30 IST:** shut down WebSocket, end monitoring
- Swing positions persist overnight

### Swing Max Hold
- **10 days:** swing positions auto-exited after 10 calendar days

---

## 10. BRAIN HAIRCUT (adjust_signal) — Exact Formula

After affinity multiplier, the brain applies a risk-mode + regime haircut:

```
base = clamp(affinity_score, 0, 100)
mult = 1.0

Risk-mode multiplier:
  AGGRESSIVE:  mult = 1.08  (slight boost)
  NORMAL:      mult = 1.00  (no change)
  DEFENSIVE:   mult = 0.82  (18% cut)
  LOCKDOWN:    mult = 0.60  (40% cut)

Regime penalty (stacks multiplicatively):
  CHOP or PANIC:  mult × 0.88

adjusted_score = clamp(base × mult, 0, 100)
```

**Example:** raw=80, affinity=1.1 → affinity_score=88, DEFENSIVE → 88 × 0.82 = 72 (just passes threshold).
**Example:** raw=80, affinity=0.8 → affinity_score=64, NORMAL → 64 × 1.0 = 64 (fails 72 threshold).
**Example:** raw=80, affinity=1.2 → affinity_score=96, CHOP+DEFENSIVE → 96 × 0.82 × 0.88 = 69 (fails).

---

## 11. MARKET POLICY SERVICE

Derives trading policy from brain state. Controls what the system is *allowed* to do.

### Policy Derivation by Risk Mode

| Risk Mode | Watchlist Mult | Sector Cap | Special Blocks |
|-----------|---------------|-----------|----------------|
| AGGRESSIVE | 1.10× | 25% | None |
| NORMAL | 1.00× | 20% | None |
| DEFENSIVE | 0.75× | 15% | OPEN_DRIVE disabled |
| LOCKDOWN | 0.60× | 12% | BREAKOUT + OPEN_DRIVE disabled, Phase2 disabled |

### Direction Controls
- **TREND_UP + long_bias >= 0.65:** may disable shorts entirely
- **PANIC:** forces short_enabled = True (shorts are the edge in panic)
- **Breadth >= 75 + regime in {RANGE, CHOP, RECOVERY}:** blocks all SELL trades (stale breadth issue)

### Swing Permission
| Regime | Permission | Effect |
|--------|-----------|--------|
| TREND_UP | ENABLED | All swing strategies allowed |
| RANGE | ENABLED | All swing strategies allowed |
| CHOP, RECOVERY | REDUCED | Blocks momentum-chasing (BREAKOUT, MOMENTUM); allows MR, PULLBACK |
| TREND_DOWN, PANIC | REDUCED | Same as above — never fully DISABLED (MR swings are the edge in bear markets) |

---

## 12. ADVANCED GATES (Flag-Gated, M2–M4)

### Playbook (M2) — `use_playbook_v1`
- **Fail-closed:** Unknown (setup, direction, regime, risk_mode) tuples are DENIED
- If a strategy has no registered Edge entry → blocked with `playbook_no_edge_registered`
- If Edge exists but regime not in allowed_regimes → `playbook_regime_not_allowed`
- If regime allowed but risk_mode blocked → `playbook_risk_mode_not_allowed`

### Expected Edge R (M3) — `use_expected_edge_r_v1`
- Computes expected edge R from signal score + historical priors
- Hard-blocks if expected_edge_R <= 0 (negative expectancy)
- **Stale-prior guard:** if sample count < min_sample_size, does NOT block (allows seed phase)

### Portfolio Book (M4) — `use_portfolio_book_v1`
- Channel-based capital allocation:
  - INTRADAY: 40% of capital
  - SWING: 40%
  - POSITIONAL: 15%
  - HEDGE: 5%
- **Drawdown governors (rolling):**
  - Daily throttle at 1.5% → halve position size (soft)
  - Daily halt at 3.0% → no new entries (hard)
  - Weekly halt at 5.0% rolling 7 days (hard)
  - Monthly halt at 8.0% rolling 30 days (hard)
- Check order: monthly → weekly → daily halt → channel budget → daily throttle

---

## 13. ORDER PLACEMENT (After Qualification)

### Paper Trade Path
1. Apply entry slippage (0.10% adverse direction)
2. Save position to Firestore immediately
3. Create `paper_gtts` row (SL trigger for ws_monitor polling)
4. Return — no broker interaction

### Live Intraday (MIS Product)
1. Place **bracket order** via Upstox: `stop_loss = |entry − SL|`, `square_off = |target − entry|`
2. **25-second fill probe:** poll every 1.2s for fill confirmation
3. If filled → save position, set `mark_fired_today`
4. If rejected → clear fired flag, return error
5. If still pending → save to `pending_orders` for later reconciliation

### Live Swing (CNC/Delivery Product)
1. Place regular **MARKET order** (bracket not supported for delivery)
2. After fill confirms → place **GTT SL order** via Upstox (3 retries, 0.5s/1.0s backoff)
3. If GTT fails after all retries → emergency market exit + `needs_manual_gtt` flag

### SL Distance Floor
- Minimum SL distance: 0.8% of entry price
- If ATR computes tighter → widen SL and proportionally adjust target to preserve R:R ratio

### Idempotency
- `already_fired_today(symbol, side)` prevents duplicate entries same day/side

---

## 14. RECONCILIATION

### Pending Entry Reconciliation (during intraday scans)
- Polls `pending_orders` collection
- Checks order status via Upstox API
- FILLED → create position, delete pending
- REJECTED/CANCELLED → clear fired flag, delete pending
- Still pending → skip until next cycle

### EOD Position Reconciliation (3 passes)
- **15:10 IST:** first pass — close MIS positions (allow transient failures)
- **15:20 IST:** second pass — retry remaining
- **15:29 IST:** final pass — force market exit on everything left
- Swing/CNC positions skip EOD close (persist overnight)
- Writes to BigQuery trades table with net P&L (gross − brokerage)

### Swing Reconciliation (~09:00 IST premarket)
- Re-evaluates CNC positions against fresh daily candles
- Exits if: SuperTrend flips, SL breached overnight, target hit, max_hold exceeded (10 days)
- Updates trailing SL → cancels old GTT, places new one via `refresh_swing_gtt_sl()`

---

## 15. SYSTEM SCHEDULING & INFRASTRUCTURE

### Cloud Run Endpoints (triggered by Cloud Scheduler)

| Time (IST) | Endpoint | What It Does |
|------------|----------|-------------|
| ~08:30 | `/jobs/premarket-precompute` | Build premarket brain, derive policy, recompute universe, build watchlist |
| ~09:00 | `/jobs/swing-reconcile` | Re-evaluate swing positions vs fresh daily candles |
| Every 3–5 min (09:15–15:15) | `/jobs/run-scan-once` | Main entry qualification + order placement loop |
| Every 3–5 min | `/jobs/watchlist-refresh` | Refresh intraday watchlist (Phase2 re-scoring) |
| 15:10, 15:20, 15:29 | `/jobs/eod-position-reconcile` | Close all MIS positions (3 progressively forceful passes) |

### Premarket Workflow (08:30 IST)
1. Acquire distributed lock (prevents concurrent runs, TTL=3600s)
2. Build premarket brain from daily data
3. Derive market policy
4. Recompute universe v2 from GCS candle cache (no API calls — avoids rate limits)
5. Build watchlist (Phase1 only, no Phase2 until market opens and 4+ bars exist)
6. Persist to Firestore + GCS + Pub/Sub
7. Release lock

### WebSocket Tick Feed
- **Protocol:** Upstox streamer v3 (protobuf binary over WSS)
- **Auth flow:** POST authorize → get redirect URI → connect within 60s
- **Monitoring:** ws_monitor_service runs in separate Cloud Run container (min-instances=1)
- **Position refresh:** every 15 seconds from Firestore
- **Watchdog:** warn if no ticks for 30s, force-reconnect after 90s silence
- **Paper GTT poll:** every 60s, checks if LTP crossed paper SL trigger

---

## 16. DATA SOURCES & FRESHNESS

### Core Data Feeds

| Data | Source | Freshness | Fallback |
|------|--------|-----------|----------|
| **5m candles** | GCS cache (primary), BQ (secondary) | Continuous during market hours | Last cached bar |
| **1d candles** | GCS cache (primary), BQ (secondary) | Once daily (stale intraday) | Previous day |
| **VIX** | Upstox live feed | Real-time | Default 15.0 |
| **PCR (static)** | Upstox option chain API | Intraday updates | Cached if < 90 min old, else neutral 1.0 |
| **OI change PCR** | Upstox option chain delta | Intraday updates | Cached |
| **Max-pain** | Computed from option chain OI | Intraday updates | Default +4 pts in scoring |
| **FII flow** | NSE API (`fiidiiTradeReact`) | Daily (previous day's data) | Freshness-decayed: 100% same-day → 25% at 5+ days |
| **Earnings dates** | NSE Corporate Filings API | Weekly refresh (Sunday 08:00 IST) | BSE corporate actions API |

### FII Freshness Decay
| Age | Weight |
|-----|--------|
| Same day | 100% |
| 1 day | 88% |
| 2 days | 72% |
| 3–4 days | 50% |
| 5+ days | 25% |

### Signal Staleness Penalty (PR-1)
- **< 120 seconds:** 0 penalty (fresh)
- **120–900 seconds:** linear interpolation up to 40-point penalty
- **> 900 seconds:** full 40-point penalty subtracted from market_confidence
- Applied to: nifty_age, vix_age, pcr_age (worst of the three)

---

## 17. EARNINGS BLACKOUT

- **Source:** NSE Corporate Filings Event Calendar API
- **Events monitored:** "FINANCIAL RESULT" and "QUARTERLY RESULT"
- **Blackout window:** 2 trading days before result date
- **Storage:** Firestore `config/earnings_blackout` → `{symbol: "YYYY-MM-DD"}`
- **Stale cleanup:** entries > 30 days past result date auto-removed
- **Refresh:** weekly via Cloud Scheduler (Sunday 08:00 IST)
- **Fail-safe:** if earnings read fails during scan → entire scan aborts (fail-closed)

---

## 18. CORRELATION FILTERING & SECTOR DIVERSIFICATION

### Correlation Filtering (watchlist building)
- **Method:** absolute daily returns correlation between candidate and already-selected stocks
- **Threshold (dynamic by regime):**
  - NORMAL: 0.85
  - PANIC: 0.75
  - Cross-sector pairs: min(threshold, 0.72)
  - Clamped to [0.60, 0.95]
- **Action:** if |correlation| >= threshold → candidate blocked from watchlist

### Sector Diversification
- **Watchlist caps:** regime-dependent (12–25% per sector, see §11)
- **Live position caps:** max 2 positions per sector, max 2 per strategy type
- **Coverage floor:** 85% sector coverage minimum in watchlist (prevents over-concentration)

---

## 19. DEGRADED MODE

### Triggers (any one activates)
```
run_degraded_flag = True if:
  data_quality_score < 55
  OR run_integrity_confidence < 55
  OR phase2_confidence < 35
```

### Effects
- Dashboard shows `runDegradedFlag: true`
- Phase2 eligibility may be restricted
- Brain staleness: if brain_state > 90 min old:
  - Falls back to last-known-good cached state from Firestore
  - Caps risk_mode to DEFENSIVE, size ≤ 0.65, positions ≤ 0.70
  - If no cached state available → scan aborts entirely

---

## 20. WHAT WORKS IN EACH MARKET TYPE

### TREND_UP (current default via high-breadth override)
**Active strategies:** PULLBACK (1.2×), VWAP_TREND (1.1×), MOMENTUM (1.4×), OPEN_DRIVE (1.0×), PHASE1_REVERSAL (0.6×)
**Blocked:** BREAKOUT, MORNING_FADE, SHORT_BREAKDOWN, SHORT_PULLBACK, PHASE1_MOMENTUM
**Behavior:** System fires 3–5 trades/day. MOMENTUM and PULLBACK are primary edge. VWAP_TREND provides fill trades.
**Known issue:** Regime may be wrong (breadth stale from daily candles) — system treats flat markets as bullish.

### TREND_DOWN
**Active strategies:** SHORT_BREAKDOWN (1.3×), SHORT_PULLBACK (1.2×), PHASE1_REVERSAL (1.2×), MEAN_REVERSION (0.6×), VWAP_TREND (0.7×)
**Blocked:** BREAKOUT, MORNING_FADE
**Behavior:** Short-side trades dominate. PHASE1_REVERSAL catches oversold bounces.

### RANGE
**Active strategies:** PULLBACK (0.8×), MEAN_REVERSION (1.4×), VWAP_TREND (0.7×), MOMENTUM (1.1×), PHASE1_REVERSAL (1.0×)
**Blocked:** BREAKOUT, SHORT_BREAKDOWN, OPEN_DRIVE, PHASE1_MOMENTUM, SHORT_PULLBACK, MORNING_FADE
**Known issue:** ZERO trades in 4 consecutive RANGE days. Affinity multipliers (0.7–0.8×) push scores below 72 threshold. MR gates (RSI <= 45, VWAP extension >= 0.6%) are too tight for mild range-bound action.

### CHOP
**Active strategies:** MEAN_REVERSION (1.2×), VWAP_TREND (0.4×), PHASE1_REVERSAL (0.9×)
**Blocked:** BREAKOUT, SHORT_BREAKDOWN, PULLBACK, SHORT_PULLBACK, OPEN_DRIVE, PHASE1_MOMENTUM, MOMENTUM, MORNING_FADE
**Behavior:** Very few strategies survive. Only deep mean-reversion works.

### PANIC
**Active strategies:** SHORT_BREAKDOWN (0.8×), SHORT_PULLBACK (0.6×), MEAN_REVERSION (0.8×), VWAP_TREND (0.2×), PHASE1_REVERSAL (0.9×)
**Blocked:** BREAKOUT, PULLBACK, OPEN_DRIVE, PHASE1_MOMENTUM, MOMENTUM, MORNING_FADE
**Behavior:** Size capped at 0.50×. LOCKDOWN risk mode likely. Capitulation bounces via MR/PHASE1_REVERSAL.

### RECOVERY
**Active strategies:** BREAKOUT (1.1×), PULLBACK (1.0×), MEAN_REVERSION (0.7×), VWAP_TREND (1.0×), MOMENTUM (1.3×), OPEN_DRIVE (1.2×), PHASE1_MOMENTUM (1.1×), PHASE1_REVERSAL (1.1×)
**Blocked:** MORNING_FADE only
**Behavior:** Broadest strategy set. Early-recovery leaders tend to extend (MOMENTUM 1.3×).

---

## 21. CAPITAL & RISK PARAMETERS

| Parameter | Value |
|-----------|-------|
| Capital per account | ₹50,000 |
| Risk per intraday trade | ₹125 |
| Risk per swing trade | ₹200 |
| Max daily loss (circuit breaker) | ₹300 |
| Daily profit target (stop trading) | ₹375 |
| Max trades per day | 5 |
| Max intraday positions | 3 |
| Max swing positions | 5 |
| Max per sector | 2 |
| Max per strategy | 2 |
| Reentry cooldown | 30 minutes |
| Paper entry slippage | 0.10% |
| Paper SL slippage | 0.20% |

---

## 22. KNOWN STRUCTURAL ISSUES

1. **Stale breadth/trend scores:** Computed from daily candles, frozen all day. A flat intraday market still shows breadth=94 from yesterday's close, triggering TREND_UP via high-breadth override.

2. **RANGE dead zone:** 4 consecutive zero-trade days in RANGE. Affinity multipliers (0.7–0.8×) combined with brain haircut push most scores below 72. The system is calibrated to only produce qualifying signals in TREND_UP.

3. **Dead strategies (globally blocked):** BREAKOUT (0/9 WR live), MORNING_FADE (17% WR backtest), VWAP_REVERSAL (disabled setting). 3 of 12 strategies are permanently dead.

4. **Breadth short block:** `breadth >= 75` blocks all SELL trades in RANGE/CHOP/RECOVERY. Since breadth is stale and often high (94), this effectively kills all short entries in these regimes even when intraday action is bearish.

5. **max_trades_day = 5 is too low:** On active TREND_UP days, system hits the cap by 11:00 AM with good setups still appearing in the afternoon.

---

## 23. PAPER TRADE vs LIVE TRADE DIFFERENCES

| Aspect | Paper | Live |
|--------|-------|------|
| Entry order | Instant fill (no broker) | Bracket (MIS) or MARKET (CNC) via Upstox |
| Entry slippage | 0.10% adverse applied | Real market slippage |
| SL mechanism | Firestore `paper_gtts` polled every 60s | Broker GTT order (real-time) |
| SL slippage | 0.20% adverse applied at exit | Real market slippage |
| Target exit | No slippage (LIMIT-equivalent) | Bracket leg (MIS) or manual (CNC) |
| Fill latency | 0 | 25s fill probe + pending reconciliation |
| Brokerage | Calculated but not deducted from P&L | Real brokerage deducted |

---

## 24. DAILY BIAS (Swing Alignment Layer)

Computed from 50+ daily candles per stock. Used in scoring Layer 5 and swing entry gates.

| Component | Values | How Computed |
|-----------|--------|-------------|
| `trend` | UP / DOWN / NEUTRAL | 7 technical voters, needs net 3+ margin for stability |
| `strength` | 0–100 | ADX (up to 40) + EMA9/50 spread (up to 30) + RSI distance from 50 (up to 30) |
| `support` | price level | 20-bar swing low |
| `resistance` | price level | 20-bar swing high |
| `ema_stack` | true/false | EMA20 > EMA50 > EMA200 |
| `supertrend_dir` | 1 / −1 | Daily SuperTrend direction |
| `adx_daily` | 0–100 | Daily ADX value |
| `rsi_daily` | 0–100 | Daily RSI(14) |

**Impact on scoring (Layer 5):**
- BUY + daily UP: +8 to +15 pts (scaled by strength)
- BUY + daily NEUTRAL: +5 pts
- BUY + daily DOWN: −6 to −10 pts (scaled by strength)
- SELL mirrors the above inverted

---

## 25. SIGNAL FLOW DIAGRAM (Summary)

```
Universe (500-1500 stocks)
  ↓
Watchlist Build (Phase1 daily → Phase2 intraday)
  ↓
Scan Tick (10 core + 25 rotated per cycle)
  ↓
Per Symbol:
  ├─ Compute indicators (SuperTrend, VWAP, EMA, RSI, MACD, ADX, Bollinger, Stochastic)
  ├─ Direction vote (8 voters → BUY/SELL/HOLD)
  ├─ Strategy entry gate (strategy-specific conditions)
  ├─ Score signal (7 layers → raw_score 0-100)
  ├─ × Affinity multiplier (regime × strategy → 0.2-1.4)
  ├─ × Brain haircut (risk mode → 0.6-1.0)
  ├─ Compare to threshold (72 intraday / 65 swing)
  ├─ Policy gates (18 checks)
  └─ → QUALIFIED or BLOCKED (with reason)
  ↓
Top N qualified signals → Place orders
  ↓
Monitor via WebSocket:
  ├─ SL hit → exit
  ├─ Target hit → trailing mode
  ├─ Breakeven trigger → SL to entry
  ├─ Partial exits (40/30/30 at 1R/1.5R/trail)
  ├─ FLAT_TIMEOUT (120 min) → exit
  ├─ Regime change → tighten SL
  └─ EOD 15:25 → close all intraday
```
