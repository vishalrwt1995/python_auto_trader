# AutoTrader — End-to-End Trading System Documentation

> **Purpose**: Complete technical reference for every algorithm, logic path, and data flow in the AutoTrader system.  
> **Audience**: Any human or LLM that needs to understand, debug, or enhance this system.  
> **Last Updated**: 2026-04-10

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Daily Pipeline Timeline](#2-daily-pipeline-timeline)
3. [Phase 1: Universe Management](#3-phase-1-universe-management)
4. [Phase 2: Candle Cache & Data Pipeline](#4-phase-2-candle-cache--data-pipeline)
5. [Phase 3: Scoring Engine](#5-phase-3-scoring-engine)
6. [Phase 4: Watchlist Generation](#6-phase-4-watchlist-generation)
7. [Phase 5: Market Brain & Regime Detection](#7-phase-5-market-brain--regime-detection)
8. [Phase 6: Market Policy Engine](#8-phase-6-market-policy-engine)
9. [Phase 7: Live Scanner](#9-phase-7-live-scanner)
10. [Phase 8: Indicator Computation](#10-phase-8-indicator-computation)
11. [Phase 9: Direction Voting](#11-phase-9-direction-voting)
12. [Phase 10: Signal Scoring](#12-phase-10-signal-scoring)
13. [Phase 11: Position Sizing & Risk](#13-phase-11-position-sizing--risk)
14. [Phase 12: Order Execution](#14-phase-12-order-execution)
15. [Phase 13: Position Monitoring & Exit](#15-phase-13-position-monitoring--exit)
16. [Phase 14: EOD Reconciliation](#16-phase-14-eod-reconciliation)
17. [Configuration Reference](#17-configuration-reference)
18. [Known Issues & Gaps](#18-known-issues--gaps)
19. [File Reference](#19-file-reference)

---

## 1. System Overview

### Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        GCP Cloud Run                                │
│                                                                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────────────┐  │
│  │ Universe  │→ │  Candle   │→ │  Score   │→ │    Watchlist V2    │  │
│  │ Refresh   │  │  Cache    │  │ Refresh  │  │   (150 stocks)     │  │
│  └──────────┘  └──────────┘  └──────────┘  └────────────────────┘  │
│                                                       │             │
│  ┌──────────────────────────────────────────────────────┘            │
│  │                                                                  │
│  ▼                                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────────────┐  │
│  │  Market   │→ │  Scanner  │→ │  Signal  │→ │   Order Service    │  │
│  │  Brain    │  │  (5m loop)│  │ Scoring  │  │  (Bracket Orders)  │  │
│  └──────────┘  └──────────┘  └──────────┘  └────────────────────┘  │
│                                                       │             │
│                                              ┌────────┘             │
│                                              ▼                      │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────────────────────┐  │
│  │   EOD     │← │    WS     │← │  Firestore Positions           │  │
│  │  Recon    │  │  Monitor  │  │  (OPEN → SL/TARGET → CLOSED)   │  │
│  └──────────┘  └──────────┘  └──────────────────────────────────┘  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
         │              │               │
         ▼              ▼               ▼
    ┌─────────┐   ┌──────────┐   ┌──────────┐
    │Firestore│   │   GCS    │   │ BigQuery │
    │(state)  │   │(candles) │   │(trades)  │
    └─────────┘   └──────────┘   └──────────┘
```

### Tech Stack

| Component | Technology |
|-----------|-----------|
| Runtime | Python 3.11, FastAPI, Uvicorn |
| Hosting | Google Cloud Run (asia-south1) |
| Broker API | Upstox v2/v3 REST + WebSocket |
| State Store | Firestore (positions, watchlist, universe, runtime props) |
| Candle Storage | Google Cloud Storage (JSON files) |
| Trade History | BigQuery (`autotrader.trades`, `autotrader.signals`) |
| Scheduling | Google Cloud Scheduler (HTTP POST triggers) |
| Events | Pub/Sub (position-events, trade-signals) |
| Dashboard | Next.js on Cloud Run (Firebase Auth) |

### Key Settings (defaults)

```
Capital:            ₹50,000
Risk per trade:     ₹125
Max daily loss:     ₹300  (NOT enforced — see Known Issues)
Daily profit target:₹200  (NOT enforced — see Known Issues)
Max positions:      3 (simultaneous open)
Max trades/day:     5
Min signal score:   72 (dynamic, regime-adjusted)
ATR SL multiplier:  1.5x (regime-scaled)
Risk:Reward ratio:  1:1.5
```

---

## 2. Daily Pipeline Timeline

All times IST (UTC+05:30). Weekdays only (Mon-Fri).

```
03:35  ─ Upstox token refresh (daily expiry)
06:15  ─ Universe V2 refresh (raw instruments → canonical build → backfill new)
07:05  ─ Candle cache update pass 1 (1D + 5m candles, retry stale)
07:40  ─ Candle cache update pass 2 (terminalize no-progress symbols)
08:30  ─ Score refresh (v1 scoring from cache, v2 eligibility recompute)
09:00  ─ Watchlist V2 premarket build (150 stocks, diversified)
09:15  ─ Market opens (pre-open auction 09:00-09:15)
09:20  ─ Scanner starts (every 5 minutes until 15:30)
09:30  ─ Watchlist V2 intraday refresh (every 5 min until 10:30, then 15 min)
15:10  ─ EOD position reconciliation pass 1
15:15  ─ Entry window closes (ist_minutes > 915 = 15:15)
15:20  ─ EOD position reconciliation pass 2
15:30  ─ Market closes, EOD reconciliation pass 3, scanner stops
```

### Scanner Cadence
- **09:20-14:55**: Every 5 minutes (`*/5 9-14 * * 1-5`)
- **15:00-15:30**: Every 5 minutes (`0-30/5 15 * * 1-5`)
- Each scan takes ~3-5 seconds (35 symbols × 80ms sleep)

### Watchlist Refresh Cadence
- **09:00**: Premarket build (with premarket=true flag)
- **09:30-09:59**: Every 5 minutes (high-frequency opening period)
- **10:00-10:30**: Every 5 minutes
- **10:45**: Single refresh
- **11:00-13:00**: Every 15 minutes
- **14:45**: Final refresh

---

## 3. Phase 1: Universe Management

**File**: `universe_service.py`  
**Trigger**: `POST /jobs/universe-v2-refresh` at 06:15 IST

### Pipeline Steps

1. **Raw Refresh**: Download complete instruments list from Upstox (`complete.json.gz`)
2. **Canonical Build**: Filter to NSE equity cash-segment instruments
3. **Backfill**: Fetch historical candles for newly appended instruments

### Universe Size
- Total instruments: ~2,469 NSE equities
- Swing eligible: ~880
- Intraday eligible: ~458

### Eligibility Criteria

Each instrument is evaluated for:
- **Exchange**: Must be NSE
- **Segment**: Must be equity/cash
- **Enabled**: Must be marked as enabled in Firestore
- **Liquidity Bucket**: A (highest) through D (lowest), based on median 60-day turnover
- **Bars Available**: Minimum candle history for technical analysis
- **Stale Days**: Instruments with no recent price updates get flagged

### Data Model (Firestore `universe` collection)

Each document contains:
```
symbol, exchange, segment, instrument_key, enabled,
isin, name, lot_size, tick_size, sector, macrosector,
bars_1d, price_last, turnover_med_60d, atr_14, atr_pct_14d,
gap_risk_60d, turnover_rank_60d, stale_days,
swing_eligible, intraday_eligible, liquidity_bucket
```

---

## 4. Phase 2: Candle Cache & Data Pipeline

**File**: `universe_service.py` (candle cache sections)  
**Storage**: Google Cloud Storage (GCS), path format: `candles/{symbol}/{exchange}/{segment}/{timeframe}.json`

### Two Timeframes Cached

| Timeframe | Use | Lookback |
|-----------|-----|----------|
| 1D (daily) | Scoring, 52-week high, trend analysis | 700 trading days (~2.8 years) |
| 5m (5-minute) | Intraday scoring, phase 2 signals | 60 trading days |

### Cache Update Flow (07:05 and 07:40 IST)

**Pass 1 (07:05)** — Full fetch with retries:
```
For each universe symbol:
  1. Read existing cached candles from GCS
  2. Fetch latest candles from Upstox API
  3. Merge new candles into cache (deduplicate by timestamp)
  4. Write merged cache back to GCS
  5. If stale (no new data), retry once
  6. Also fetch 5m intraday candles (same flow)
```

**Pass 2 (07:40)** — Terminalize only:
```
For symbols still stale after pass 1:
  Mark as terminal so scoring doesn't stall waiting for them
```

### API Limits
- Upstox standard tier: 2,000 requests per 30 minutes
- Candle API cap per run: 1,800 (leaves headroom)
- ~500 symbols × 2 calls (1D + 5m) = ~1,000 per pass

---

## 5. Phase 3: Scoring Engine

**File**: `scoring.py` (universe scoring), `universe_service.py` (application)  
**Trigger**: `POST /jobs/score-refresh` at 08:30 IST

### Universe Score (for Watchlist Selection)

Each symbol gets a **0-100 composite score** from these components:

| Component | Code | Max Points | What It Measures |
|-----------|------|-----------|------------------|
| EMA Alignment | `E` | 20 | EMA(9) > EMA(21) > EMA(50) = 20pts, EMA(20) > EMA(50) = 10pts |
| Price Position | `P` | 10 | Above EMA(20) = +5, Above EMA(50) = +5 |
| RSI | `R` | 15 | 50-65 = 15pts (ideal), 40-50 = 8pts, 65-75 = 5pts |
| MACD | `M` | 10 | Histogram > 0 = +5, Fresh BUY cross = +5 |
| Breakout | `B` | 15 | At 52-week high = 15pts, Within 2% = 10pts, Within 10% = 8pts |
| Volume | `V` | 15 | Vol ratio ≥ 1.5x = 15pts, ≥ 1.2x = 10pts, ≥ 1.0x = 5pts |
| OBV | `O` | 5 | OBV rising = 5pts |
| Penalties | `N` | -50 | RSI > 80 (-15), RSI < 35 (-15), Doji (-5), Bear candle (-5), >30% from 52w high (-10) |

**Formula**: `Final = clamp(E + P + R + M + B + V + O + N, 0, 100)`

**Score string format**: `E20|P10|R15|M5|B15|V10|O5|N0|U0|S80`

---

## 6. Phase 4: Watchlist Generation

**File**: `universe_service.py` (lines ~4600-5100)  
**Trigger**: `POST /jobs/watchlist-refresh` (multiple times daily)

### Overview

The watchlist builder takes the scored universe (~880 swing + ~458 intraday eligible) and selects the top 150 stocks, diversified by sector and correlation.

### Step 1: Candidate Building

For each eligible instrument, compute three **strategy sub-scores**:

#### BREAKOUT Score
```python
breakout = (0.30 * relative_strength)      # RS vs Nifty
         + (0.25 * breakout_component)      # proximity to 52-week high
         + (0.15 * volume_component)        # volume surge
         + (0.15 * trend_component)         # EMA alignment
         + (0.15 * adx_component)           # trend strength (ADX)
```

#### PULLBACK Score
```python
pullback = (0.40 * trend_strength)          # EMA stack intact
         + (0.40 * pullback_component)      # retracement depth to support
         + (0.20 * volume_contraction)      # declining volume during pullback
```

#### MEAN_REVERSION Score
```python
mean_rev = (0.30 * mr_component)            # distance from mean
         + (0.20 * rsi_mr_bonus)            # RSI oversold/overbought
         + (0.15 * vol_spike_on_dip)        # volume spike during sell-off
         + (0.10 * bounce_component)        # initial bounce signal
         + (0.10 * support_proximity)       # near key support level
         + (0.15 * vol_sanity)              # liquidity check
```

### Step 2: Strategy Label Assignment

The highest-scoring sub-component determines the label:
```python
setup_scores = {"BREAKOUT": breakout, "PULLBACK": pullback, "MEAN_REVERSION": mean_rev}
setup_label = max(setup_scores.items(), key=lambda kv: kv[1])[0]
```

#### Short-Side Labels (PANIC / TREND_DOWN regimes only)

```python
SHORT_BREAKDOWN = (0.30 * rs_short)         # relative weakness
               + (0.25 * breakdown_component) # breaking support
               + (0.15 * vol_expansion)      # increasing volume
               + (0.15 * trend_short)        # bearish EMA alignment
               + (0.15 * adx_component)      # trend strength

SHORT_PULLBACK = (0.40 * short_trend_str)    # bearish trend intact
              + (0.40 * short_pb_component)  # bounce into resistance
              + (0.20 * vol_expansion)       # volume confirming
```

#### Intraday Labels

- **PHASE1_MOMENTUM**: Assigned to phase 1 intraday candidates
- **VWAP_TREND**: `(0.40 * vwap_slope) + (0.35 * volume_shock) + (0.25 * orb_component)`
- **VWAP_REVERSAL**: `(0.50 * extension) + (0.30 * reversal_signal) + (0.20 * liquidity)` (only in CHOPPY regime)
- **OPEN_DRIVE**: Assigned by market_brain when phase is POST_OPEN

### Step 3: Regime-Dependent Final Score

The final watchlist score blends the three sub-scores with regime-dependent weights:

| Regime | Breakout Weight | Pullback Weight | Mean Reversion Weight |
|--------|----------------|-----------------|----------------------|
| **TREND** (UP/DOWN) | 0.60 | 0.30 | 0.10 |
| **RANGE** | 0.30 | 0.30 | 0.40 |
| **RISK_OFF** (PANIC/DEFENSIVE) | 0.15 | 0.20 | 0.65 |
| **Default** | 0.30 | 0.25 | 0.45 |

Short-side weights (PANIC/TREND_DOWN only):

| Regime | Short Breakdown | Short Pullback | Mean Reversion |
|--------|----------------|----------------|----------------|
| **PANIC** | 0.65 | 0.25 | 0.10 |
| **TREND_DOWN** | 0.50 | 0.35 | 0.15 |

### Step 4: Diversification & Selection

**Function**: `_select_with_diversification_and_corr()`

1. **Sort** all candidates by final_score descending
2. **Sector bucketing**: Each stock assigned to `SECTOR:{macrosector}` bucket
   - If sector coverage < 85%, uses proxy buckets: `PROXY:{liquidity}|{volatility}|{gap_risk}`
3. **Sector cap**: Maximum per-sector = `dynamic_sector_cap_share × target_size`
   - Default cap_share: 0.20 (20%), so max 30 stocks from one sector in a 150-stock watchlist
   - Varies by risk mode (AGGRESSIVE: 0.25, DEFENSIVE: 0.15, LOCKDOWN: 0.12)
4. **Correlation filter**: Pearson correlation of daily returns between candidate and all already-picked stocks
   - If correlation ≥ threshold (default 0.85), candidate is rejected
   - Threshold varies by risk mode (AGGRESSIVE: 0.88, DEFENSIVE: 0.80, LOCKDOWN: 0.75)
5. **Pick** until target_size reached (default 150)

### Step 5: Watchlist Output

Saved to Firestore `watchlist/latest`:
```json
{
  "rows": [
    {
      "symbol": "RELIANCE",
      "exchange": "NSE",
      "enabled": true,
      "setup": "BREAKOUT",
      "macrosector": "ENERGY",
      "beta": 1.15,
      "reason": "E20|P10|R12|M5|B15|V10|O5|N0|S77",
      ...
    }
  ],
  "metadata": {
    "total": 150,
    "swing": 100,
    "intraday": 50,
    "regime": "TREND_UP",
    "build_ts": "2026-04-10T09:00:00+05:30"
  }
}
```

---

## 7. Phase 5: Market Brain & Regime Detection

**File**: `market_brain_service.py`  
**Called by**: Scanner on every scan cycle (every 5 min during market hours)

### What Market Brain Does

The Market Brain is the central intelligence that determines:
1. **Regime**: What type of market environment we're in
2. **Risk Mode**: How aggressively to trade
3. **Allowed Strategies**: Which strategy types can generate signals
4. **Position Sizing Multiplier**: How large positions should be
5. **Max Positions Multiplier**: How many simultaneous positions

### Regime Classification

Based on Nifty 50 index analysis and market-wide indicators:

| Regime | Description | Typical Conditions |
|--------|-------------|-------------------|
| **TREND_UP** | Strong bullish trend | Nifty above EMAs, breadth strong, VIX low |
| **TREND_DOWN** | Strong bearish trend | Nifty below EMAs, breadth weak, VIX elevated |
| **RANGE** | Sideways/balanced | Mixed signals, moderate VIX |
| **CHOP** | High-noise sideways | Conflicting indicators, whipsaws |
| **PANIC** | Extreme stress | VIX spike, breadth collapse, sharp decline |
| **RECOVERY** | Bouncing from lows | Breadth improving, trend turning |
| **AVOID** | Don't trade | Extreme conditions or data quality issues |

### Sub-Regimes

Each primary regime can have a sub-regime for finer control:

| Parent | Sub-Regime | Condition |
|--------|-----------|-----------|
| PANIC | PANIC_TREND | Always (PANIC has one sub-regime) |
| RECOVERY | FALSE_RECOVERY | Trend ≥ 58 but breadth < 48 |
| RECOVERY | RECOVERY_BUILD | Leadership ≥ 55 and breadth ≥ 50 |
| RECOVERY | RECOVERY_TENTATIVE | Default recovery |
| TREND_UP | NARROW_TREND | Structure shows narrow participation |
| TREND_DOWN | SHORT_COVERING_BOUNCE | Vol stress < 55 and breadth improving |
| CHOP | HIGH_NOISE_CHOP | Volatility stress ≥ 62 |
| CHOP | LOW_CONVICTION_CHOP | Default chop |
| RANGE | HEALTHY_RANGE | Vol stress < 45 and data quality ≥ 70 |
| RANGE | LOW_CONVICTION_RANGE | Default range |

### Event States

Special market conditions that override normal behavior:

| Event | Trigger | Impact |
|-------|---------|--------|
| ABNORMAL_GAP | 30m range expansion ≥ 1.6x during POST_OPEN | Extra caution on entries |
| STRESS_EVENT | Volatility stress ≥ 80 | Reduced sizing |
| EXPIRY_SESSION | Thursday (weekly expiry day) | Adjusted thresholds |
| EXECUTION_FRAGILITY | CHOP/PANIC + low liquidity + low risk appetite | Minimal trading |

### Risk Mode Determination

| Risk Mode | Meaning | Size Multiplier | Max Positions Mult |
|-----------|---------|----------------|-------------------|
| **AGGRESSIVE** | Favorable conditions, trade more | ~1.10x | ~1.15x |
| **NORMAL** | Standard conditions | 1.00x | 1.00x |
| **DEFENSIVE** | Cautious conditions | ~0.65x | ~0.70x |
| **LOCKDOWN** | Extreme stress, minimal trading | ~0.40x | ~0.50x |

### Six Component Scores (0-100 each)

The Market Brain computes six scores that feed into all decisions:

1. **Trend Score**: Nifty EMA alignment, slope, distance from key levels
2. **Breadth Score**: Market-wide advance/decline, percentage above EMAs
3. **Leadership Score**: Sector leaders performance, concentration analysis
4. **Volatility Stress Score**: VIX level + rate of change + intraday range
5. **Liquidity Health Score**: Volume breadth, bid-ask spreads, turnover
6. **Data Quality Score**: Cache freshness, pipeline alignment, stale symbols count

### Market Confidence Formula

```python
market_confidence = clamp(
    (0.35 × risk_appetite)
  + (0.15 × trend_score)
  + (0.15 × breadth_score)
  + (0.15 × leadership_score)
  + (0.10 × liquidity_health_score)
  + (0.10 × data_quality_score)
  - (0.15 × volatility_stress_score),
  0, 100
)
```

Note: Risk appetite weights sum to 0.95 (intentional conservative bias — 5% implicit drag).

### Policy Confidence Formula

```python
policy_confidence = clamp(
    (0.40 × market_confidence)
  + (0.20 × data_quality_score)
  + (0.15 × run_integrity_confidence)
  + (0.15 × breadth_confidence)
  + (0.10 × leadership_confidence),
  0, 100
)
```

### Session Phase Detection

```
Time (IST)     Phase
< 09:15        PREMARKET
09:15-10:15    POST_OPEN
10:15-15:30    LIVE
> 15:30        EOD
```

### Intraday State Classification

Based on VWAP slope and range expansion after POST_OPEN:
- **PREOPEN**: Before market open
- **TRENDING**: Clear directional movement
- **CHOPPY**: No clear direction
- **VOLATILE**: Wide swings both ways
- **CALM**: Low activity

---

## 8. Phase 6: Market Policy Engine

**File**: `market_policy_service.py`  
**Called by**: Market Brain on every scan cycle

### What Policy Controls

The Market Policy translates Market Brain state into concrete trading rules:

```python
@dataclass
class MarketPolicy:
    regime: str                      # Current regime
    risk_mode: str                   # AGGRESSIVE/NORMAL/DEFENSIVE/LOCKDOWN
    allowed_strategies: list[str]    # Which strategy types can trade
    swing_permission: str            # ENABLED/REDUCED/DISABLED
    size_multiplier: float           # Position size scaling factor
    max_positions_multiplier: float  # Max simultaneous positions scaling
    watchlist_target_multiplier: float  # Watchlist size scaling
    watchlist_min_score_boost: int   # Added to minimum score requirement
    intraday_phase2_enabled: bool    # Allow phase 2 intraday signals
    breakout_enabled: bool           # Allow breakout trades
    open_drive_enabled: bool         # Allow open drive trades
    long_enabled: bool               # Allow BUY direction
    short_enabled: bool              # Allow SELL direction
    liquidity_bucket_floor: str      # Minimum liquidity tier (A/B/C/D)
    dynamic_sector_cap_share: float  # Max % of watchlist per sector
    correlation_threshold: float     # Max correlation between positions
    policy_confidence: float         # Confidence in current policy
```

### Policy Rules by Risk Mode

| Parameter | AGGRESSIVE | NORMAL | DEFENSIVE | LOCKDOWN |
|-----------|-----------|--------|-----------|----------|
| watchlist_target_multiplier | 1.10 | 1.00 | 0.75 | 0.60 |
| watchlist_min_score_boost | 0 | 2 | 4 | 4 |
| liquidity_bucket_floor | B | B | B | B |
| open_drive_enabled | Yes | Yes | No | No |
| breakout_enabled | Yes | Yes | Yes | No |
| intraday_phase2_enabled | Yes | Yes | Yes | No |
| dynamic_sector_cap_share | 0.25 | 0.20 | 0.15 | 0.12 |
| correlation_threshold | 0.88 | 0.85 | 0.80 | 0.75 |

### Regime Overrides

Applied after risk mode defaults:

| Condition | Override |
|-----------|---------|
| CHOP or PANIC regime | breakout_enabled = False, open_drive_enabled = False |
| TREND_UP/RECOVERY + long_bias ≥ 0.65 | short_enabled = False |
| PANIC regime | short_enabled = True (overrides above) |
| TREND_DOWN/PANIC + data_quality < 45 | intraday_phase2_enabled = False |
| PANIC regime | sector_cap = min(current, 0.15), correlation_threshold = min(current, 0.72) |

### Watchlist Row Filtering

The policy filters watchlist rows through `adjust_watchlist_rows()`:

```
For each watchlist row:
  1. Check liquidity bucket ≥ floor (A=4, B=3, C=2, D=1)
  2. If swing section:
     - Skip if swing_permission = DISABLED
     - Skip BREAKOUT setup if swing_permission = REDUCED
  3. Skip PHASE2_INPLAY if intraday_phase2 disabled
  4. Skip setups containing "BREAKOUT" if breakout disabled
  5. Skip setups containing "OPEN" if open_drive disabled
```

### Signal Score Adjustment

`adjust_signal()` modifies raw signal scores before threshold comparison:

```python
# Base multiplier by risk mode
AGGRESSIVE:  1.08x
NORMAL:      1.00x
DEFENSIVE:   0.82x
LOCKDOWN:    0.60x

# Additional regime multiplier
CHOP/PANIC:  × 0.88 (compounding)

# Example: Raw score 85 in LOCKDOWN + PANIC
# → 85 × 0.60 × 0.88 = 44.88 → 45
```

### Position Sizing with Market Brain

`size_position_with_market_brain()` applies multi-factor scaling:

```python
risk_mult = (
    state.size_multiplier              # 0.10 to 1.40 from Market Brain
  × setup_confidence_multiplier        # 0.40 to 1.40 from signal score
  × liquidity_multiplier              # 0.85 if vol_ratio < 1.0, else 1.0
  × data_quality_multiplier           # 0.40 to 1.20 from data quality score
)
qty = floor(base_qty × risk_mult)

# LOCKDOWN cap: qty cannot exceed half of base qty
if risk_mode == "LOCKDOWN":
    qty = min(qty, base_qty // 2)
```

### Max Positions Limit

```python
effective_max = floor(base_max_positions × max_positions_multiplier)
# Always at least 1
```

---

## 9. Phase 7: Live Scanner

**File**: `trading_service.py` — `run_scan_once()`  
**Trigger**: Cloud Scheduler every 5 min during market hours (09:20-15:30 IST)

### Scanner Flow Diagram

```
run_scan_once()
     │
     ├─ 1. Acquire distributed lock (90s TTL)
     ├─ 2. Check market open (weekday, 09:15-15:30 IST)
     ├─ 3. Reconcile any pending entry orders
     ├─ 4. Build Market Brain state (regime, risk mode, scores)
     ├─ 5. Derive Market Policy from brain state
     ├─ 6. Compute max_positions_limit
     ├─ 7. Read watchlist from Firestore (150 stocks)
     ├─ 8. Slice watchlist for this scan cycle (35 stocks)
     ├─ 9. Fetch instrument keys from universe
     ├─ 10. Batch-fetch live LTP for all symbols (Upstox v3)
     │
     ├─ FOR EACH symbol in slice (35 stocks):
     │   ├─ 11. Fetch 15m candles (GCS cache + live Upstox)
     │   ├─ 12. Compute indicators (EMA, RSI, MACD, SuperTrend, VWAP, etc.)
     │   ├─ 13. Determine direction (7-factor voting → BUY/SELL/HOLD)
     │   ├─ 14. Score signal (4-layer 100-point scoring)
     │   ├─ 15. Adjust score with Market Brain multiplier
     │   ├─ 16. Get live LTP (or candle close as fallback)
     │   ├─ 17. Calculate ATR multiplier (regime-scaled)
     │   ├─ 18. Calculate position size (ATR-based SL/target/qty)
     │   ├─ 19. Apply Market Brain sizing adjustments
     │   ├─ 20. Check qualification gates (see below)
     │   ├─ 21. If qualified → place entry order
     │   └─ sleep(80ms)
     │
     ├─ 22. Save scan results to Firestore (dashboard visibility)
     ├─ 23. Write signals to BigQuery + Pub/Sub
     └─ 24. Release lock
```

### Watchlist Slicing Strategy

The scanner doesn't scan all 150 stocks every cycle. Instead:

```
Total watchlist: 150 stocks
Core symbols:    Top 10 (always scanned every cycle)
Rotated batch:   25 (cycles through remaining 140)
Per scan:        35 symbols total

Rotation cursor stored in Firestore:
  runtime:watchlist_scan_cursor = "50"  → next cycle starts at index 50

If total ≤ 35: scan everything every cycle (no rotation)
```

This means every stock gets scanned at least once every ~6 cycles (30 minutes).

### Candle Fetch for Scanner

```python
def _fetch_candles(symbol, exchange, segment, instrument_key, timeframe="15m", lookback_days=8):
    # 1. Read cached candles from GCS
    cached = gcs.read_candles(path)
    
    # 2. Always try Upstox intraday API for fresh data
    api_candles = upstox.get_intraday_candles_v3(instrument_key, unit="minutes", interval=15)
    cached = gcs.merge_candles(path, api_candles)  # Deduplicate + merge
    
    # 3. Need minimum 80 bars for indicator computation
    if len(cached) >= 80:
        return cached[-80:]  # Last 80 bars
    
    # 4. Fallback: fetch historical range
    api = upstox.get_historical_candles_v3_intraday_range(...)
    return merged[-120:]
```

### Qualification Gates (all must pass)

A signal must pass ALL of these checks to trigger an order:

```
Gate 1: direction != "HOLD"
Gate 2: adjusted_score >= dynamic_min_score
Gate 3: is_entry_window_open_ist()  (weekday, 09:15-15:15 IST)
Gate 4: No policy block:
  4a: If BUY → long_enabled must be True
  4b: If SELL → short_enabled must be True
  4c: Strategy must be in allowed_strategies list
  4d: qualified_count < max_positions_limit
  4e: Live VWAP guard (if live LTP available):
      - BUY: price must be above VWAP (unless MEAN_REVERSION or VWAP_REVERSAL)
      - SELL: price must be below VWAP (unless MEAN_REVERSION or VWAP_REVERSAL)
Gate 5: Idempotency — not already fired today for same symbol+side
```

### Dynamic Minimum Score Thresholds

The scanner adjusts the minimum signal score based on risk mode:

| Risk Mode | Min Score | Rationale |
|-----------|-----------|-----------|
| AGGRESSIVE | 75 | Higher bar — only top signals in bull market |
| NORMAL | 72 | Default threshold |
| DEFENSIVE | 58 | Lower bar because adjust_signal already penalizes (×0.82×0.88) |
| LOCKDOWN | 45 | Much lower bar because adjust_signal crushes scores (×0.60×0.88) |

**Example**: A raw-85 signal in LOCKDOWN + CHOP:
- Adjusted: 85 × 0.60 × 0.88 = 44.88 → 45
- Dynamic threshold: 45
- Result: **Just qualifies** (would be rejected at static 72)

### Strategy Allowance Check

```python
def _strategy_allowed(strategy, allowed_list):
    # "AUTO" or "DEFAULT" → always allowed
    # Exact match → allowed
    # Substring matching for backward compatibility:
    #   "BREAKOUT" in strategy → allowed if "BREAKOUT" in list
    #   "OPEN" in strategy → allowed if "OPEN_DRIVE" or "VWAP_TREND" in list
    #   "MEAN" in strategy → allowed if "MEAN_REVERSION" or "VWAP_REVERSAL" in list
    #   "SHORT_*" → allowed if "MEAN_REVERSION" or "VWAP_REVERSAL" in list
```

---

## 10. Phase 8: Indicator Computation

**File**: `indicators.py` — `compute_indicators()`  
**Input**: List of candles (OHLCV), minimum 80 bars required  
**Output**: `IndicatorSnapshot` with all computed values

### Indicators Computed

#### 1. Exponential Moving Averages (EMA)

| EMA | Period | Purpose |
|-----|--------|---------|
| Fast | 9 | Short-term trend, crossover signals |
| Medium | 21 | Medium-term trend |
| Slow | 50 | Long-term trend |
| EMA(20) | 20 | Universe scoring |
| EMA(50) | 50 | Universe scoring |

**EMA Formula**:
```
k = 2 / (period + 1)
EMA[i] = close[i] × k + EMA[i-1] × (1 - k)
Initial seed = SMA of first `period` values
```

**Derived States**:
- `ema_stack`: EMA(9) > EMA(21) > EMA(50) — bullish alignment
- `ema_flip`: EMA(9) < EMA(21) < EMA(50) — bearish alignment
- `above_ema20`: Close > EMA(20)
- `above_ema50`: Close > EMA(50)

#### 2. Relative Strength Index (RSI)

- Period: 14
- Uses Wilder's smoothing method

```
For each bar:
  gain = close - prev_close (if positive, else 0)
  loss = prev_close - close (if positive, else 0)
  
avg_gain = EMA(gains, period=14)  # Wilder smoothing
avg_loss = EMA(losses, period=14)
RS = avg_gain / avg_loss
RSI = 100 - (100 / (1 + RS))
```

**Key Levels**:
- RSI > 80: Overbought (penalty in scoring)
- RSI 55-65: Bullish confirmation
- RSI 45-55: Neutral
- RSI 35-45: Bearish confirmation
- RSI < 35: Oversold (penalty in scoring, but good for mean reversion)

#### 3. MACD (Moving Average Convergence Divergence)

```
MACD Line = EMA(12) - EMA(26)
Signal Line = EMA(MACD Line, 9)
Histogram = MACD Line - Signal Line
```

**Crossover Detection**:
```python
if hist[current] > 0 and hist[previous] <= 0:  → "BUY" cross
if hist[current] < 0 and hist[previous] >= 0:  → "SELL" cross
```

#### 4. SuperTrend

- ATR Period: 10
- Multiplier: 3.0

```
mid = (high + low) / 2
upper_band = mid + 3.0 × ATR(10)
lower_band = mid - 3.0 × ATR(10)

If previous direction = UP:
  If close < previous_upper_band → direction flips to DOWN
If previous direction = DOWN:
  If close > previous_lower_band → direction flips to UP
```

**Output**:
- `dir`: +1 (bullish/up) or -1 (bearish/down)
- `fresh`: True if direction just changed (flip signal)

#### 5. VWAP (Volume Weighted Average Price)

```python
cumulative_tp_vol += typical_price × volume
cumulative_vol += volume
VWAP = cumulative_tp_vol / cumulative_vol
```

Where `typical_price = (high + low + close) / 3`

> **KNOWN ISSUE**: VWAP is cumulative across ALL candles in the lookback window (8 days of 15m candles). It does NOT reset at each trading day boundary. This means the VWAP used for intraday signals is actually a multi-day VWAP, which can give incorrect signals.

#### 6. On-Balance Volume (OBV)

```python
if close > prev_close:  OBV += volume
if close < prev_close:  OBV -= volume
if close == prev_close: OBV unchanged
```

`obv_rising`: True if current OBV > previous OBV

#### 7. Average True Range (ATR)

- Period: 14
- Uses Wilder's smoothing

```
True Range = max(
    high - low,
    abs(high - prev_close),
    abs(low - prev_close)
)
ATR = Wilder_smooth(TR, period=14)
```

**Used for**: Stop-loss distance, position sizing, volatility measurement

#### 8. Bollinger Bands

- Period: 20
- Multiplier: 2.0

```
Middle Band = SMA(20)
Std = population_std(last 20 closes)
Upper Band = Middle + 2.0 × Std
Lower Band = Middle - 2.0 × Std
```

**Used in**: Penalty scoring (close near upper band on BUY = penalty)

#### 9. Stochastic Oscillator

- %K Period: 14
- %D Period: 3 (EMA of %K)

```
%K = (close - lowest_low_14) / (highest_high_14 - lowest_low_14) × 100
%D = EMA(%K, 3)
```

**Key Levels**:
- %K > 85: Overbought (penalty for BUY signals)
- %K < 15: Oversold (penalty for SELL signals)

#### 10. ADX (Average Directional Index)

- Period: 14
- Uses Wilder's smoothing

```
+DM = high - prev_high (if positive and > -DM, else 0)
-DM = prev_low - low (if positive and > +DM, else 0)
TR = max(high-low, abs(high-prev_close), abs(low-prev_close))

Smoothed +DI = 100 × smooth(+DM, 14) / smooth(TR, 14)
Smoothed -DI = 100 × smooth(-DM, 14) / smooth(TR, 14)
DX = 100 × abs(+DI - -DI) / (+DI + -DI)
ADX = Wilder_smooth(DX, 14)
```

**Result**: ADX value 0-100. Higher = stronger trend.

> **KNOWN ISSUE**: ADX is computed by `compute_indicators()` but the returned `IndicatorSnapshot` does not include it, and the scanner/scoring never uses it. The value is only used in watchlist scoring for strategy label assignment.

#### 11. Candlestick Patterns

```python
# Doji: body < 10% of range
doji = abs(open - close) / (high - low) < 0.1

# Bullish Engulfing: current green candle engulfs previous red candle
bull_engulf = (close > open) and (prev_close < prev_open) and 
              (close > prev_open) and (open < prev_close)

# Bearish Engulfing: current red candle engulfs previous green candle
bear_engulf = (close < open) and (prev_close > prev_open) and 
              (close < prev_open) and (open > prev_close)

# Bear Candle: close < open
bear_candle = close < open
```

#### 12. Derived Metrics

```python
# Volume ratio (current vs 20-bar average)
vol_ratio = current_volume / avg_volume_20

# 52-week high distance
dist_from_52w_high = ((max_252 - close) / max_252) × 100

# Near breakout: within 2% of 52-week high
near_breakout = close >= max_252 × 0.98

# Breakout: within 0.1% of 52-week high
breakout = close >= max_252 × 0.999
```

---

## 11. Phase 9: Direction Voting

**File**: `scoring.py` — `determine_direction()`  
**Input**: IndicatorSnapshot + RegimeSnapshot  
**Output**: "BUY", "SELL", or "HOLD"

### Voting System

A 7-factor weighted voting system where each factor votes BUY or BEAR:

| Factor | BUY Condition | BEAR Condition | Weight |
|--------|--------------|----------------|--------|
| **SuperTrend** | dir = +1 (uptrend) | dir = -1 (downtrend) | **3** |
| **VWAP** | close > VWAP | close ≤ VWAP | **2** |
| **EMA Fast/Med** | EMA(9) > EMA(21) | EMA(9) ≤ EMA(21) | **2** |
| **EMA Med/Slow** | EMA(21) > EMA(50) | EMA(21) ≤ EMA(50) | **1** |
| **RSI** | RSI > 55 | RSI < 45 | **1** |
| **MACD Histogram** | hist > 0 | hist ≤ 0 | **2** |
| **MACD Cross** | fresh BUY cross | fresh SELL cross | **1** |
| **Engulfing** | bullish engulfing | bearish engulfing | **1** |
| **Regime Bias** | BULLISH bias | BEARISH bias | **2** |

**Maximum possible**: Bull = 15, Bear = 15

### Decision Logic

```python
if regime == "AVOID":
    return "HOLD"

if bull > bear + 2:    # Bull leads by more than 2
    return "BUY"
if bear > bull + 2:    # Bear leads by more than 2
    return "SELL"
return "HOLD"          # Too close to call
```

**Example** — Strong uptrend:
```
SuperTrend UP:    bull += 3
Close > VWAP:     bull += 2
EMA(9) > EMA(21): bull += 2
EMA(21) > EMA(50):bull += 1
RSI = 58:         bull += 1
MACD hist > 0:    bull += 2
No cross:         +0
No engulfing:     +0
Regime BULLISH:   bull += 2
─────────────────────────
Bull = 13, Bear = 0
13 > 0 + 2 → BUY
```

---

## 12. Phase 10: Signal Scoring

**File**: `scoring.py` — `score_signal()`  
**Input**: Symbol, direction, IndicatorSnapshot, RegimeSnapshot, StrategySettings  
**Output**: SignalScore (0-100) with breakdown

### Four Scoring Layers

The signal score is a 100-point composite built from 4 layers plus penalties:

#### Layer 1: Regime Score (max 25 points)

| Condition | Points |
|-----------|--------|
| Nifty change aligns with direction (>0.1% for BUY, <-0.1% for SELL) | +10 |
| Nifty change neutral (<0.1%) | +5 |
| VIX < 15 (vix_trend_max) | +8 |
| VIX < 20 (vix_safe_max) | +4 |
| FII flow aligns with direction (>500cr for BUY, <-500cr for SELL) | +7 |
| FII flow neutral | +3 |

#### Layer 2: Options/PCR Score (max 20 points)

| Condition | Points |
|-----------|--------|
| PCR aligns (≥0.8 for BUY, ≤1.2 for SELL) | +10 |
| PCR doesn't align | +3 |
| Price near max pain on correct side | +10 |
| Price not near max pain | +4 |
| Max pain data unavailable | +5 |

#### Layer 3: Technical Score (max 40 points)

| Condition | Points |
|-----------|--------|
| **SuperTrend** fresh flip in direction | +10 |
| SuperTrend aligned (not fresh) | +6 |
| **VWAP** alignment | +8 |
| **EMA triple alignment** (fast > med > slow for BUY) | +7 |
| EMA partial alignment (fast > med only) | +4 |
| EMA turning (fast rising) | +2 |
| **RSI in sweet spot** (45-65 for BUY, 35-55 for SELL) | +7 |
| RSI improving in right direction | +3 |
| **MACD fresh cross** in direction | +8 |
| MACD histogram in direction | +4 |
| **Engulfing pattern** in direction | +2 |

*Technical score capped at 40*

#### Layer 4: Volume Score (max 15 points)

| Condition | Points |
|-----------|--------|
| Volume ratio ≥ 1.5x average | +10 |
| Volume ratio ≥ 1.2x | +6 |
| Volume ratio ≥ 1.0x | +3 |
| OBV confirms direction | +5 |

#### Penalty Layer (negative points)

| Condition | Penalty |
|-----------|---------|
| VIX > 18 | -10 |
| RANGE regime | -8 |
| Large candle (body > 2.5% of price) | -5 |
| Doji pattern | -3 |
| BUY near upper Bollinger (>99.8%) | -5 |
| SELL near lower Bollinger (<100.2%) | -5 |
| BUY with Stochastic %K > 85 | -4 |
| SELL with Stochastic %K < 15 | -4 |

### Score Flow After Raw Computation

```
Raw Score (0-100)
     │
     ▼
Market Brain adjust_signal() — applies risk mode + regime multiplier
     │  AGGRESSIVE: ×1.08
     │  NORMAL:     ×1.00
     │  DEFENSIVE:  ×0.82
     │  LOCKDOWN:   ×0.60
     │  + CHOP/PANIC: ×0.88 (compounds)
     │
     ▼
Adjusted Score (0-100)
     │
     ▼
Compare against dynamic_min_score
     │  AGGRESSIVE: 75
     │  NORMAL:     72
     │  DEFENSIVE:  58
     │  LOCKDOWN:   45
     │
     ▼
QUALIFIED or FILTERED
```

### Score Breakdown Example

**Scenario**: HDFC Bank, BUY direction, TREND_UP regime, NORMAL risk mode

```
Layer 1 (Regime):    Nifty +0.5% → +10, VIX=14 → +8, FII +800cr → +7  = 25
Layer 2 (Options):   PCR=0.85 → +10, Near max pain → +10               = 20
Layer 3 (Technical): ST aligned → +6, VWAP above → +8, EMA triple → +7,
                     RSI=57 → +7, MACD hist>0 → +4, No pattern → 0     = 32
Layer 4 (Volume):    Vol ratio=1.3x → +6, OBV rising → +5              = 11
Penalties:           VIX=14 (ok), not RANGE, small candle                = 0
─────────────────────────────────────────────────────────────────────────
Raw Score: 25 + 20 + 32 + 11 = 88

Market Brain adjustment: 88 × 1.00 (NORMAL) = 88
Dynamic threshold: 72
Result: QUALIFIED (88 ≥ 72)
```

---

## 13. Phase 11: Position Sizing & Risk

**File**: `risk.py` — `calc_position_size()`  
**File**: `market_policy_service.py` — `size_position_with_market_brain()`

### Base Position Sizing Algorithm

```python
# Step 1: Calculate stop-loss distance
sl_dist = max(ATR × atr_sl_mult, entry_price × 0.5%)
# Minimum SL = 0.5% of entry price (prevents tiny SLs on low-ATR stocks)

# Step 2: Calculate SL price and target
if BUY:
    sl_price = entry_price - sl_dist
    target = entry_price + sl_dist × rr_intraday  # 1.5x
if SELL:
    sl_price = entry_price + sl_dist
    target = entry_price - sl_dist × rr_intraday

# Step 3: Calculate quantity from risk budget
qty = risk_per_trade // sl_dist  # ₹125 / sl_dist
qty = min(qty, capital × 15% // entry_price)  # 15% capital cap
qty = max(1, qty)  # At least 1 share

# Step 4: Calculate max loss and gain
brokerage = calc_brokerage(qty, entry_price)
max_loss = qty × sl_dist + brokerage
max_gain = qty × sl_dist × rr_intraday - brokerage
```

### Regime-Scaled ATR Multiplier

The base ATR multiplier (1.5x) is adjusted by regime before position sizing:

| Condition | ATR Multiplier | Rationale |
|-----------|---------------|-----------|
| LOCKDOWN or PANIC | 1.5 × 0.75 = **1.125** | ATR already inflated 3-4x, tighter SL needed |
| DEFENSIVE or TREND_DOWN/CHOP | 1.5 × 0.87 = **1.305** | Slightly tighter |
| AGGRESSIVE + TREND_UP | 1.5 × 1.20 = **1.800** | Give momentum trades room |
| All other cases | **1.500** | Default |

### Market Brain Position Size Adjustment

After base sizing, the Market Brain applies further adjustments:

```python
risk_mult = (
    state.size_multiplier           # From Market Brain (0.10 to 1.40)
  × setup_confidence_multiplier     # (adjusted_score/100 + 0.20), clamped [0.45, 1.30]
  × liquidity_multiplier           # 0.85 if vol_ratio < 1.0, else 1.0
  × data_quality_multiplier        # data_quality_score / 100, clamped [0.60, 1.10]
)

final_qty = floor(base_qty × risk_mult)

# LOCKDOWN hard cap
if risk_mode == "LOCKDOWN":
    final_qty = min(final_qty, base_qty // 2)
```

### Max Positions Limit

```python
effective_max = floor(base_max × max_positions_multiplier)
# base_max = 3 (from settings)
# max_positions_multiplier varies by Market Brain (0.25 to ~1.50)
# Always at least 1
```

### Brokerage Calculation

Includes all Indian market charges (round-trip):

```python
def calc_brokerage(qty, price):
    turnover = qty × price
    broker_fee = min(₹20, turnover × 0.05%)   # Flat ₹20 cap
    stt = turnover × 0.025%                     # Securities Transaction Tax
    nse_charge = turnover × 0.00322%            # Exchange charge
    gst = (broker_fee + nse_charge) × 18%       # GST
    sebi = turnover × 0.0001%                   # SEBI fee
    total_one_way = broker_fee + stt + nse_charge + gst + sebi
    return round(total_one_way × 2, 2)          # Round-trip (entry + exit)
```

---

## 14. Phase 12: Order Execution

**File**: `order_service.py` — `place_entry_order()`

### Entry Order Flow

```
place_entry_order()
     │
     ├─ 1. Check idempotency: already_fired_today(symbol, side)?
     │     If yes → return {skipped: "duplicate_idempotency"}
     │
     ├─ 2. Generate ref_id: "AT-{hex_timestamp}-{random3}"
     │
     ├─ 3. Determine mode: paper = paper_trade setting OR !allow_live_orders
     │
     ├─ PAPER MODE:
     │   ├─ Create position tag: "BOTP:{ref_id}"
     │   ├─ Save position to Firestore (status=OPEN)
     │   ├─ Mark symbol+side as fired today
     │   └─ Return {paper: true, order_id, position_tag}
     │
     └─ LIVE MODE:
         ├─ Place bracket order via Upstox API:
         │   - instrument_token: Upstox instrument key
         │   - transaction_type: BUY or SELL
         │   - quantity: calculated qty
         │   - stop_loss: abs(entry_price - sl_price)  ← DISTANCE, not price
         │   - square_off: abs(target - entry_price)   ← DISTANCE, not price
         │   - order_reference_id: ref_id
         │
         ├─ Poll for fill (_await_fill):
         │   - Poll every 1.2 seconds for up to 25 seconds
         │   - Check Upstox order list for matching order_id
         │   - Return: filled/terminal/timeout
         │
         ├─ IF FILLED:
         │   ├─ Create position tag: "BOT:{order_id}:{ref_id}"
         │   ├─ Save position to Firestore with actual fill_price
         │   ├─ Mark fired today
         │   ├─ Save order record to Firestore
         │   └─ Return {order_id, status=FILLED, fill_price, position_tag}
         │
         ├─ IF TERMINAL (rejected/cancelled):
         │   ├─ Clear fired_today (allow retry)
         │   └─ Return {order_id, status=TERMINAL_NONFILL}
         │
         └─ IF TIMEOUT (still pending):
             ├─ Save to pending_orders collection for reconciliation
             ├─ Mark fired today
             └─ Return {order_id, status=PENDING_RECON}
```

### Bracket Order Details

Upstox bracket orders are a single entry that automatically places:
- **Entry**: LIMIT order at current market price
- **Stop-Loss**: Distance-based (e.g., ₹15 from entry, not absolute price)
- **Target**: Distance-based (e.g., ₹22.50 from entry)

Once the entry fills, both SL and target legs are active. When one hits, the other is auto-cancelled.

### Idempotency Protection

```python
def already_fired_today(symbol, side):
    # Checks Firestore collection "fired_today"
    # Key: "{date}:{symbol}:{side}"
    # Prevents duplicate entries within same trading day
```

### Pending Order Reconciliation

Called at the start of each scanner cycle:

```python
def reconcile_pending_entries(max_items=15):
    # 1. List all pending entry orders from Firestore
    # 2. For each: check Upstox order status
    # 3. If FILLED → create position, delete pending record
    # 4. If REJECTED/CANCELLED → clear fired_today, delete pending
    # 5. If still pending → leave for next cycle
```

---

## 15. Phase 13: Position Monitoring & Exit

**File**: `ws_monitor_service.py` — `WsMonitorService`

### WebSocket Monitor Architecture

```
┌──────────────────────────────────────────────┐
│           WsMonitorService                    │
│                                               │
│  ┌──────────────┐  ┌───────────────────────┐ │
│  │ Position     │  │ Upstox WebSocket      │ │
│  │ Refresh Loop │  │ (real-time ticks)     │ │
│  │ (every 15s)  │  │                       │ │
│  └──────┬───────┘  └───────────┬───────────┘ │
│         │                      │              │
│         ▼                      ▼              │
│  ┌─────────────────────────────────────────┐ │
│  │  Position Map:                          │ │
│  │  instrument_key → {tag, sl, target,     │ │
│  │                     side, ikey}         │ │
│  └─────────────────────┬───────────────────┘ │
│                        │                      │
│                        ▼                      │
│  ┌─────────────────────────────────────────┐ │
│  │  On each tick:                          │ │
│  │  if BUY and ltp ≤ sl_price → SL_HIT    │ │
│  │  if BUY and ltp ≥ target → TARGET_HIT  │ │
│  │  if SELL and ltp ≥ sl_price → SL_HIT   │ │
│  │  if SELL and ltp ≤ target → TARGET_HIT │ │
│  └─────────────────────────────────────────┘ │
│                                               │
│  ┌─────────────────────────────────────────┐ │
│  │  EOD Watchdog:                          │ │
│  │  15:10 IST → force-close all remaining │ │
│  │  15:30 IST → stop WebSocket            │ │
│  └─────────────────────────────────────────┘ │
└──────────────────────────────────────────────┘
```

### Tick Processing Logic

```python
async def _on_quote(instrument_key, ltp, ts):
    pos = positions.get(instrument_key)
    if not pos or tag in exiting_set:
        return
    
    if side == "BUY":
        if ltp <= sl_price:  exit("SL_HIT")
        if ltp >= target:    exit("TARGET_HIT")
    else:  # SELL
        if ltp >= sl_price:  exit("SL_HIT")
        if ltp <= target:    exit("TARGET_HIT")
```

### Exit Order Flow

```python
def place_exit_order(position_tag, instrument_key, exit_reason):
    # 1. Load position from Firestore
    # 2. Determine exit side (opposite of entry)
    
    # PAPER MODE:
    #   Get current LTP from Upstox
    #   Close position at LTP
    
    # LIVE MODE:
    #   Place MARKET order (not bracket) for exit
    #   Poll for fill (10 second timeout)
    #   If filled: use actual fill price
    #   If not filled: use LTP as approximate
    #   Close position in Firestore + write to BigQuery
```

### Position Lifecycle

```
OPEN (Firestore)
  │
  ├── SL_HIT (price hits stop-loss) ──────┐
  ├── TARGET_HIT (price hits target) ─────┤
  ├── EOD_CLOSE (15:10+ IST watchdog) ────┤
  ├── MANUAL (manual exit via API) ────────┤
  └── CLOSED (bracket auto-closed) ────────┤
                                           │
                                           ▼
                                    CLOSED (Firestore)
                                      + BigQuery trade record
                                      + Pub/Sub event
```

---

## 16. Phase 14: EOD Reconciliation

**File**: `order_service.py` — `reconcile_open_positions()`  
**Trigger**: Cloud Scheduler at 15:10, 15:20, 15:30 IST

### Purpose

Catch any positions that weren't closed by:
- Bracket order SL/target hits
- WebSocket monitor exits
- Manual intervention

### EOD Reconciliation Flow

```
reconcile_open_positions()
     │
     ├─ 1. List all OPEN positions from Firestore
     │
     ├─ FOR EACH open position:
     │
     │   ├─ PAPER TRADE:
     │   │   ├─ Get LTP from Upstox
     │   │   └─ Close at LTP with reason "EOD_CLOSE"
     │   │
     │   └─ LIVE TRADE:
     │       ├─ Check Upstox order status for bracket order
     │       │
     │       ├─ IF FILLED (SL/target already hit):
     │       │   ├─ Determine reason by price proximity:
     │       │   │   - Closer to SL price → "SL_HIT"
     │       │   │   - Closer to target → "TARGET_HIT"
     │       │   │   - Very close to entry → "EOD_CLOSE"
     │       │   └─ Close position with actual fill price
     │       │
     │       └─ IF STILL OPEN:
     │           ├─ Place MARKET exit order
     │           └─ Close position at fill price
     │
     └─ Return {checked, closed, remaining, errors}
```

### Three Passes

| Time | Purpose |
|------|---------|
| 15:10 | First pass — catches most bracket-closed positions |
| 15:20 | Second pass — catches late fills and stragglers |
| 15:30 | Final pass — force-closes anything remaining before market close |

---

## 17. Configuration Reference

### StrategySettings (settings.py)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `capital` | 50,000 | Total trading capital (₹) |
| `risk_per_trade` | 125 | Maximum loss per trade (₹) |
| `max_daily_loss` | 300 | Daily loss limit (₹) — **NOT ENFORCED** |
| `daily_profit_target` | 200 | Daily profit target (₹) — **NOT ENFORCED** |
| `max_trades_day` | 5 | Max trades per day |
| `max_positions` | 3 | Max simultaneous open positions |
| `min_signal_score` | 72 | Minimum score for entry (overridden by dynamic threshold) |
| `ema_fast` | 9 | Fast EMA period |
| `ema_med` | 21 | Medium EMA period |
| `ema_slow` | 50 | Slow EMA period |
| `rsi_period` | 14 | RSI lookback period |
| `rsi_buy_min` | 45 | RSI minimum for BUY sweet spot |
| `rsi_buy_max` | 65 | RSI maximum for BUY sweet spot |
| `rsi_sell_min` | 35 | RSI minimum for SELL sweet spot |
| `rsi_sell_max` | 55 | RSI maximum for SELL sweet spot |
| `vol_mult` | 1.5 | Volume multiplier for "high volume" |
| `atr_sl_mult` | 1.5 | ATR multiplier for stop-loss |
| `rr_intraday` | 1.5 | Risk:Reward ratio (1:1.5) |
| `vix_safe_max` | 20 | VIX below this = safe |
| `vix_trend_max` | 15 | VIX below this = trending |
| `pcr_bull_min` | 0.8 | PCR ≥ this = bullish |
| `pcr_bear_max` | 1.2 | PCR ≤ this = bearish |
| `nifty_trend_pct` | 0.3 | Nifty change % threshold for trend |

### Market Hours (time_utils.py)

```
Market Open:    09:15 IST (minute 555)
Market Close:   15:30 IST (minute 930)
Entry Window:   09:15-15:15 IST (minute 555-915)
Weekdays only:  Monday-Friday
```

### Upstox API Limits

| Limit | Value |
|-------|-------|
| Requests per second | 50 |
| Requests per minute | 500 |
| Requests per 30 min | 2,000 |
| Max retries | 4 |

---

## 18. Known Issues & Gaps

### Critical

1. **VWAP Never Resets Daily** (`indicators.py:179`): Cumulative across 8-day candle window. Should reset at each trading day boundary for correct intraday signals.

2. **Strategy Labels Are Decorative**: Watchlist assigns BREAKOUT/PULLBACK/MEAN_REVERSION etc., but the scanner uses identical generic direction voting and signal scoring for ALL strategies. No strategy-specific entry logic.

3. **Daily PnL Limits Not Enforced**: `max_daily_loss` (₹300) and `daily_profit_target` (₹200) are defined in settings but no code checks them before placing trades.

4. **ADX Computed But Not Used in Scanner**: `calc_adx()` runs in `compute_indicators()` but the result is not included in `IndicatorSnapshot` and not used by direction voting or signal scoring.

### Medium

5. **No Trailing Stop / Breakeven SL**: Bracket orders use fixed SL/target. No adjustment after entry.

6. **Single Timeframe**: Scanner only uses 15-minute candles. No higher-timeframe (1H/daily) confirmation.

7. **No Time-Based Exits**: Positions that go flat are held until SL/target/EOD. No "close if flat after N bars" logic.

8. **No Portfolio-Level Risk**: No checks for sector concentration or correlation between open positions.

9. **Direction Voting Weights Hardcoded**: Weights (SuperTrend=3, VWAP=2, etc.) are not validated against actual trade outcomes.

10. **Scanner Uses REST Polling**: Entries depend on 5-minute scan intervals. Fast-moving setups between intervals are missed.

### Low

11. **`risk_appetite` weights sum to 0.95**: Systematic 5% conservative bias in market confidence (intentional design choice).

12. **`_strategy_allowed` uses substring matching**: Could have false positives if strategy names overlap (no current collision).

13. **`allow_live_orders=false` in scanner URL**: Hardcoded paper mode — needs manual change for live trading.

14. **Missing PubSub topic `regime-events`**: Non-blocking warning on every scan, but regime detection works without it.

---

## 19. File Reference

| File | Purpose |
|------|---------|
| `settings.py` | All configurable parameters, environment variable loading |
| `time_utils.py` | IST timezone, market hours, entry window checks |
| `indicators.py` | All technical indicator calculations |
| `scoring.py` | Direction voting, signal scoring, universe scoring |
| `risk.py` | Position sizing, brokerage calculation |
| `trading_service.py` | Live scanner, candle fetching, watchlist slicing |
| `order_service.py` | Entry/exit orders, position management, EOD recon |
| `market_brain_service.py` | Regime detection, 6 market scores, confidence formulas |
| `market_policy_service.py` | Policy rules, signal adjustment, sizing adjustment |
| `regime_service.py` | Nifty analysis, VIX/FII/PCR data, bias calculation |
| `universe_service.py` | Universe management, candle cache, watchlist building |
| `ws_monitor_service.py` | WebSocket position monitoring, real-time SL/target exits |
| `deploy/create_scheduler_jobs.sh` | All Cloud Scheduler cron job definitions |
| `web/api.py` | FastAPI endpoints for jobs and dashboard |
| `domain/models.py` | All data models (dataclasses) |
| `adapters/firestore_state.py` | Firestore read/write operations |
| `adapters/gcs_store.py` | GCS candle storage operations |
| `adapters/upstox_client.py` | Upstox REST API client |
| `adapters/upstox_ws_client.py` | Upstox WebSocket client |
| `adapters/bigquery_client.py` | BigQuery trade/signal writes |
| `adapters/pubsub_client.py` | Pub/Sub event publishing |

---

*Document auto-generated from source code analysis. Verify against latest codebase for accuracy.*
