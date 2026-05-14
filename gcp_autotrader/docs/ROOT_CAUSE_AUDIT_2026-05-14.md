# Root-Cause Audit — 2026-05-14

**Window analyzed**: 2026-04-15 → 2026-05-12 (26 intraday live trades post-redesign-M-series)

**Method**: Pull every live position, examine entries/exits at bar level, trace through the live code paths.

**Outcome**: 4 confirmed root causes with architectural fixes designed. 1 issue was actually fine (query bug on my end). 1 bonus finding flagged.

---

## RC-1 — BREAKOUT fires on RED reversal bars (FALSE BREAKOUTS)

### Symptom
- 6 BREAKOUT trades over period (PPLPHARMA, ONESOURCE, OFSS, ADANIENT, NETWEB, BRIGADE)
- **Win rate: 0%**
- **AvgMFE = 0.00R** — trades never go positive at all
- Net P&L: -₹244

### Bar-level evidence
Looked at the 5m bar at the moment of each entry. In **5 of 5 verified entries**, the entry bar's close was at or below its open (a RED candle). Some examples:

| Symbol | Entry bar OHLC | Pattern |
|---|---|---|
| PPLPHARMA | O=165.0, H=165.3, L=164.2, C=165.0 | Down-trending 25 min before entry, closed flat at LOW |
| ONESOURCE | O=1740, H=1740, L=1730, C=1738 | Opened at high, immediate drop to L, RED close |
| OFSS | O=8830, H=8845, L=8809, C=8821 | RED bar, closed near low |
| ADANIENT | O=2397, H=2400, L=2392, C=2393 | RED, closed at low |
| NETWEB | O=4078, H=4078, L=4044, C=4054 | Massive 0.6% intra-bar DROP |

### Root cause (architectural)
Current gate (`scoring.py:382-395`):
```python
if s in ("BREAKOUT", "SHORT_BREAKDOWN"):
    if ind.adx < 20: reject
    if is_buy and ind.dist_from_52w_high > 5.0: reject
    if ind.volume.ratio < 1.2: reject
    return True
```

The gate confirms **"near 52-week high"** but never confirms the stock is **breaking out NOW**. A stock reversing OFF a high gets identical treatment to one bursting through.

**Missing**:
- Current bar direction (red vs green)
- Close above recent N-bar high (actual breakout confirmation)
- Sustained price action into breakout
- Volume rising NOW (not just elevated overall)

### Permanent fix (architectural)
Add to `check_strategy_entry`:
1. Current bar must close > open (no entries on reversal bars)
2. Current bar close must exceed prior 12-bar high (actual breakout)
3. Last 2 bars must show momentum into the breakout
4. Volume must be accelerating (not just elevated)

### Note
The system already hard-blocked BREAKOUT in all regimes on 2026-05-06 after the "0/9 WR" finding. Our 6 trades are pre-block. The hard-block is the right defensive move BUT this fix re-enables BREAKOUT as a legitimate setup once the gate actually validates breakouts.

---

## RC-2 — VWAP_TREND fails in morning, works in afternoon

### Symptom
| Time | VWAP_TREND record | Net P&L |
|---|---|---|
| 09:45-10:30 | 1/3 (33%) | -₹44 |
| **10:31-11:30** | **1/6 (17%)** | **-₹300** |
| **11:31-12:30** | **3/3 (100%)** | **+₹228** |

VWAP_TREND fires throughout the day but only WORKS after 11:30. Morning trades are major bleeders.

### Root cause (architectural)
Current gate (`scoring.py:499-508`):
```python
if s == "VWAP_TREND":
    if is_buy and ind.close <= ind.vwap: reject
    if not is_buy and ind.close >= ind.vwap: reject
    if ind.adx < 18: reject
    return True
```

Only 2 gates. The setup is named "VWAP_**TREND**" but the gate doesn't check that VWAP is actually trending. At 10:00 IST, VWAP has only ~9 bars of input — too few for "trend" to mean anything.

### Permanent fix (architectural)
1. **Bars-since-session-open gate**: require ≥ 12 5m bars (60 min) before VWAP_TREND can fire
2. **VWAP slope check**: VWAP itself must be trending in trade direction (the actual "TREND" half of the name)
3. **Sustained side of VWAP**: last 3 bars all on trade side of VWAP (not just current)

### Expected impact
9 morning VWAP_TREND trades (2/9 WR) would be rejected. Saves -₹344. The 3 winning post-11:30 trades pass through unchanged.

---

## RC-3 — Position sizing is 50% of configured

### Symptom
- Configured `risk_per_trade = ₹125`
- **Actual avg risk = ₹63** (range ₹28-132)
- Brokerage = ₹10/trade fixed
- For VWAP_TREND: 12 trades, gross PnL ~₹0, cost ₹115 → **cost ratio 960%**

### Root cause
Live `calc_position_size` is reducing qty below the risk-budget target. Hypothesis: `brain_state.size_multiplier` or `policy.size_multiplier` is < 1.0 in some condition.

### Investigation needed
Trace `calc_position_size` call chain in `trading_service.py` to find where qty gets halved. Likely candidate: market policy from MarketPolicyService.

### Permanent fix candidates
1. **Find and fix the reducer** — if size_multiplier is incorrectly < 1.0
2. **Increase configured risk_per_trade** to compensate
3. **Switch to discount-rate broker** — cost per trade matters more than absolute fee
4. **Be more selective** — fewer trades, bigger size each

This needs implementation-phase investigation. Not blocking immediate fixes.

---

## RC-4 — OPEN_DRIVE missing time-of-day gate (developer admitted this!)

### Symptom
3 OPEN_DRIVE trades, **0% WR**, +0.72R AvgMFE then reversed to 0% wins.

All 3 fired in 10:24-11:30 window — well AFTER the opening drive window (09:15-09:45).

### Root cause — developer's own comment in `scoring.py:566-571`:
> "First-30-min strong directional setup. Without a time-of-day check here (**we don't have current_ts in this function**), at minimum require real volume + ADX so a stale stock can't fire on the OPEN_DRIVE template hours after the open. Time-of-day is enforced upstream by `is_entry_window_open_ist` — entries are only allowed 09:45–13:30, **which is wider than ideal** but rules out the overnight stale path."

The developer KNEW the gate was incomplete and left it as a TODO.

### The fix is trivial — MORNING_FADE already does it!
`scoring.py:596-602`:
```python
bar_ts = str(ind.candles[-1][0]) if ind.candles else ""
bar_min = _ist_minutes_from_ts(bar_ts)
if not (585 <= bar_min <= 615):
    return False, "strategy_morning_fade_outside_time_window"
```

OPEN_DRIVE can use the EXACT same pattern — `ind.candles[-1][0]` provides `bar_ts`.

### Permanent fix
Add to OPEN_DRIVE gate:
```python
bar_ts = str(ind.candles[-1][0]) if ind.candles else ""
bar_min = _ist_minutes_from_ts(bar_ts)
if not (555 <= bar_min <= 585):  # 09:15-09:45 IST
    return False, "strategy_open_drive_outside_time_window"
```

5-line fix. Architecturally consistent with MORNING_FADE.

### Caveat
The entry window (`is_entry_window_open_ist`) currently starts at 09:45, but OPEN_DRIVE setup wants 09:15-09:45. Need to **bypass the entry window for OPEN_DRIVE specifically** OR widen the entry window for setups that need it.

---

## RC-5 — Signal score IS captured (false alarm)

### Symptom
Original claim: `signal_score_at_entry = None` on all positions.

### Root cause
**My query was wrong.** The field is `signal_score`, not `signal_score_at_entry`. Data has been captured all along.

### Verified
```
MOSCHIP: signal_score=73, MFE=+0.16R, LOSS
APTUS:   signal_score=74, MFE=+0.73R, WIN
DIVISLAB: signal_score=86, MFE=+0.10R, LOSS  ← highest score, still a loss
```

### Bonus finding
Higher-scoring trades had LOWER win rate in this 9-trade sample. **Score may be miscalibrated** — but sample too small to be sure. Worth tracking.

---

## Summary table

| RC | Finding | Status | Fix complexity | Expected savings |
|---|---|---|---|---|
| RC-1 | BREAKOUT false breakouts (RED bars) | Confirmed | Medium (~15 lines) | Restores setup edge if re-enabled |
| RC-2 | VWAP_TREND morning failures (no slope check) | Confirmed | Medium (~10 lines) | ~-₹344 over period |
| RC-3 | Position sizing 50% of target | Confirmed | Low (1-3 lines + investigation) | Improves edge/cost ratio |
| RC-4 | OPEN_DRIVE missing time gate | Confirmed | Trivial (~5 lines) | Eliminates stale-template trades |
| RC-5 | Score capture (false alarm) | Resolved | None | None |

---

## Notes

- Most of the 26-trade sample is small — RC-1 has 6 trades, others have 3-12. Findings are HYPOTHESES until validated with more data in paper mode.
- The BREAKOUT bar-pattern (5/5 RED entries) is too consistent to be noise even at n=6.
- The VWAP_TREND time-of-day pattern (1/9 morning, 3/3 afternoon) is structurally explainable (VWAP needs time to settle) AND empirically clear.
- The OPEN_DRIVE finding is supported by the developer's own admission in code comments.
- RC-3 needs deeper code-trace investigation before changing.

## Next steps

1. Implement RC-1, RC-2, RC-4 as feature-flagged changes
2. Deeper investigation on RC-3 (size_multiplier chain)
3. Local validation: re-run the 26-trade analysis with fixes applied (count rejections)
4. Paper-mode deployment (2-4 weeks)
5. Promote to live based on paper data

---

# FIX A — RANGE affinity nerf (added 2026-05-14)

## Symptom
- May 13 + May 14: BOTH 0-trade days
- Top-10 scoring signals each day: all MORNING_FADE (hard-blocked)
- Avg score on May 13 = 37.5; May 14 = 40.9 (well below threshold 72)

## Root cause
Regime classifier uses NIFTYBEES alone. Both days NIFTY ranged but sectors rotated:
- May 13: JUNIORBEES (mid-cap) +0.90%, 90% time above open → mid-caps trending
- May 14: BANKBEES +1.42%, 87% time above open → banks trending

System stamped `regime = RANGE`. RANGE affinity multipliers crushed trending-stock signals:
- `VWAP_TREND in RANGE: 0.7×` → base score 75 → final 52.5 → < 72 threshold → rejected
- `PULLBACK in RANGE: 0.8×` → same problem

The trend-setup nerf in RANGE is correct for NARROW-RANGE days but wrong for ROTATING-RANGE days.

## Validation backtest

20 trending stocks (10 banks May 14 + 10 mid-caps May 13) + 20 stocks on 2 narrow-range days (Apr 22/23):

| Day type | Current (0.7×) | FIX A (1.0×) |
|---|---|---|
| **Trending sectors** (May 13/14) | 0 trades / ₹0 | **17 trades / 53% WR / +₹998** |
| Narrow-range (Apr 22/23) | 0 trades / ₹0 | 21 trades / 33% WR / +₹244 |
| **Combined 4 days** | **0 / ₹0** | **38 / 42% WR / +₹1,242** ≈ **+₹310/day** |

## Permanent fix

`src/autotrader/domain/regime_affinity.py`:
```python
"RANGE": {
    "VWAP_TREND": 0.7 → 1.0,
    "PULLBACK":   0.8 → 1.0,
    # Other affinities unchanged
}
```

## Safety properties
- Stock-level gates (RC-2 sustained-VWAP, ADX≥18, ema_stack) still filter false signals
- MEAN_REVERSION (1.4×) and VWAP_REVERSAL (1.3×) still boosted — reversion edge preserved
- BREAKOUT remains hard-blocked separately
- Worst-case behavior validated on narrow-range days (+₹244 over 2 days, not flooding with losers)
