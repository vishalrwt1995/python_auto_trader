# Phase B Intraday Audit — 2026-05-26

Following the Phase A swing audit (MOMENTUM swing re-enabled), Phase B turns
to intraday strategies. Real production data over 56 days (Mar 7 → May 22)
showed intraday losing -₹673 net at ₹50K capital (-1.35% over period =
~-6.5% annualized).

## Intraday strategy P&L attribution (real data)

| Strategy | Trades | Wins | WR | Net | Verdict |
|---|---:|---:|---:|---:|---|
| **VWAP_TREND** | 58 | 5 | **8.6%** | **-₹170** | 🔴 Biggest bleed by trade count |
| VWAP_REVERSAL SELL | 12 | 0 | — | null | 🔴 Pre-fix legacy (data quality) |
| BREAKOUT (forced intraday) | 10 | 1 | 10% | -₹339 | 🔴 Already blocked everywhere |
| MOMENTUM (forced intraday) | 3 | 0 | 0% | -₹153 | 🔴 Will disappear with MOMENTUM swing fix |
| PULLBACK (intraday) | 3 | 1 | 33% | -₹97 | ⚠️ Tiny sample |
| **OPEN_DRIVE** | 3 | 1 | 33% | **+₹106** | 🟢 Only profitable setup |
| PHASE1_MOMENTUM | 2 | 1 | 50% | -₹19 | ⚠️ Tiny sample |

## Key finding: VWAP_TREND gates too loose

VWAP_TREND fired 58 times with 8.6% WR. Investigating the existing gates:

- `bar_min >= 615` (post-10:15 IST) — sensible time gate ✓
- 3-bar sustained side of VWAP — structural confirmation ✓
- ADX ≥ 18 — but data shows ADX 18-22 trades were mostly losers
- Price on correct side of VWAP — required ✓

**Missing gates that should be there:**
1. **Volume confirmation** — VWAP without volume is noise. No vol check.
2. **RSI band** — VWAP_TREND fires on momentum. RSI < 50 = stalling, RSI > 70 = exhausted.
3. **Tighter ADX** — 18 was too generous; 22 selects only genuinely trending tape.

## Fix applied: VWAP_TREND triple-gate tightening

Modified `scoring.py:check_strategy_entry()` for `VWAP_TREND`:

```python
# Before
if ind.adx < 18:
    return False, "strategy_vwap_trend_adx_too_low"

# After
if ind.adx < 22:
    return False, "strategy_vwap_trend_adx_too_low"
if ind.volume.ratio < 1.3:
    return False, "strategy_vwap_trend_volume_insufficient"
if is_buy and not (50 <= ind.rsi.curr <= 70):
    return False, "strategy_vwap_trend_buy_rsi_outside_zone"
if not is_buy and not (30 <= ind.rsi.curr <= 50):
    return False, "strategy_vwap_trend_sell_rsi_outside_zone"
```

**Expected effect:**
- VWAP_TREND trade count drops sharply (probably 60-70% fewer trades)
- WR should improve from 8.6% toward 30-40% range
- Net P&L: should improve (we're cutting net-negative-EV signals)

## Other Phase B findings (NOT shipped)

### SL distance anomalies (need deeper investigation)

Multiple real intraday trades show SL distances FAR below the supposed 0.8% floor:
- NH BUY: 0.33% SL (₹6 on ₹1850)
- TIINDIA BUY: 0.28% SL
- INDUSINDBK BUY: 0.27% SL
- AVANTIFEED BUY: 0.30% SL
- KAYNES BUY: SL ABOVE entry (0.06% inverted!)

The 0.8% floor in `order_service.py:584-604` exists but isn't always firing.
Possible causes:
- SL moved later (breakeven move, trailing stop) and trade table records final SL
- A pre-floor path bypasses the check
- KAYNES inverted-SL bug should have been caught by line 553-560 guard

**Recommendation:** Add audit logging at every SL mutation to trace these.
Not shipped — needs deeper investigation than this session allows.

### VWAP_REVERSAL null PnL (resolved — pre-4/22 legacy)

12 trades show null `net_pnl`. All are pre-4/22 — before the brokerage/net_pnl
formula was added (Batch P0-1, 2026-04-22). Not a current bug; historical
data quality only. No fix needed.

### OPEN_DRIVE — the only winner

3 trades, 33% WR, +₹106. AEROFLEX was the standout (+₹150).

Looking at OPEN_DRIVE gates (`scoring.py:check_strategy_entry`):
- Time window 09:15-09:45 IST strictly enforced ✓
- ADX ≥ 18, volume ratio ≥ 1.2 ✓
- Price on correct side of VWAP ✓

The narrow time window is what makes OPEN_DRIVE work. Late entries (the
2026-05-14 audit found 3/3 losses in 10:24-11:29 window) were the failure mode.
Current gates correctly prevent this.

**No change needed.** OPEN_DRIVE is the gold standard for what intraday gates
should look like.

### FLAT_TIMEOUT 120-min review

32 intraday trades closed at FLAT_TIMEOUT for -₹189 net. Average -₹6/trade.

This isn't a huge bleed but suggests we're entering many low-conviction signals
that don't move enough in 2 hours. With the VWAP_TREND tightening above, these
should reduce naturally.

**No change to FLAT_TIMEOUT for now.** Reassess after VWAP_TREND fix lands.

### BREAKOUT intraday (already disabled)

10 trades, 10% WR, -₹339. All from pre-MOMENTUM-fix era when BREAKOUT swing
got force-retagged to intraday. With the Phase A MOMENTUM fix, these won't
recur.

## Summary

| # | Fix | Status |
|---|---|---|
| B1 | VWAP_TREND gates tightened (ADX 18→22, vol≥1.3, RSI band) | ✅ Shipped |
| B2 | SL distance bug investigation | 📝 Documented, needs deeper trace |
| B3 | VWAP_REVERSAL null PnL | ✅ Confirmed pre-4/22 legacy, no fix needed |
| B4 | OPEN_DRIVE — leave as gold standard | ✅ No change |
| B5 | FLAT_TIMEOUT review | 📝 Watch after VWAP_TREND fix |

## Expected production impact after Phase B deploy

- VWAP_TREND trade count: ↓60-70% (most trades correctly rejected)
- Net VWAP_TREND P&L: ↑ from -₹170 to neutral/positive
- Total intraday volume: ↓ (most was VWAP_TREND)
- Total intraday P&L: ↑ (cutting -EV trades)
- Net swing+intraday: ↑ from -₹673/2.5mo to positive (with Phase A MOMENTUM also live)
