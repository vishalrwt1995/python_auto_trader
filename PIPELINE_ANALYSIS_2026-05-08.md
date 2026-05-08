# Today's Pipeline Analysis — 2026-05-08

**Generated:** during live market session (09:50 IST)
**Status:** 0 trades, 0 signals (so far) — diagnosed below

---

## TL;DR

The pipeline ran correctly start to finish. **One critical bug discovered and fixed mid-session at 09:40 IST** that would have caused zero trades today. **Plus a separate timing limitation** (Phase 2 needs ~30-45 min of intraday bars before it can kick in) that's by design but worth knowing.

---

## Pipeline timeline (2026-05-08)

| Time IST | Event | Status |
|---|---|---|
| 03:35 | Upstox token refresh | ✓ Fired |
| 06:15 | Universe v2 refresh | ✓ Fired (06:20 actual) |
| 07:35 | Score cache prefetch | ✓ Fired |
| 08:30 | Score cron | ✓ Fired (08:37 actual) |
| 08:45 | Score cache update | ✓ Fired |
| 09:00 | Premarket watchlist v2 build | ✓ Fired, finished 09:06 |
| 09:15 | Market open | — |
| 09:21+ | Intraday scan every 3 min | ✓ Running |
| 09:22 | Swing scan | ✓ Fired |
| 09:30 | Watchlist v2 5m rebuild | ✓ Fired, finished 09:42 |
| 09:40 | **EMERGENCY FIX deployed (00223-ffp)** | ✓ Live |
| 09:48 | First post-fix scan | ✓ No more policy blocks |

---

## Critical bug discovered + fixed

### The bug

`market_brain_service.py` builds the `allowed_strategies` whitelist used to gate trades. The list was missing **`PHASE1_MOMENTUM`** and **`PHASE1_REVERSAL`** despite both being valid setup labels emitted by the watchlist generator.

When today's intraday watchlist built at 09:00 IST, it had **150 PHASE1_MOMENTUM rows** (Phase 2 fell back to Phase 1 picks — explained in next section). Without the fix, every single one would have been rejected by `_strategy_allowed()` in `trading_service.py:102` with reason `policy_strategy_blocked`.

**Concrete evidence at 09:30 IST scan, 5 high-score signals were blocked:**
- BSE PHASE1_MOMENTUM BUY score 90 → policy_strategy_blocked
- ADANIGREEN PHASE1_MOMENTUM BUY score 89 → policy_strategy_blocked
- ADANIENT PHASE1_MOMENTUM BUY score 88 → policy_strategy_blocked
- PFC PHASE1_MOMENTUM BUY score 88 → policy_strategy_blocked
- APTUS PHASE1_MOMENTUM BUY score 84 → policy_strategy_blocked

These are top-decile signals that would have been the system's first trades today.

### The fix (commit `6c5f800`)

Added PHASE1_MOMENTUM, PHASE1_REVERSAL (and MORNING_FADE for completeness) to the base `allowed_strategies` list. Applied per-regime filters consistent with existing logic:
- CHOP/PANIC/TREND_DOWN: remove PHASE1_MOMENTUM (chasing strength fails)
- All regimes: keep PHASE1_REVERSAL (oversold-bounce edge in bearish regimes)

### Effect verification

09:42 IST scan (still on revision `00222-pf4`): 6 of 50 evaluations got `policy_strategy_blocked`.
09:48 IST scan (revision `00223-ffp` live): **0 of 70 evaluations got `policy_strategy_blocked`.**

The fix worked. Now stocks fail on legitimate quality gates (`strategy_phase1_insufficient_volume`, `live_price_below_vwap`) which are working as designed.

---

## Why is the watchlist 100% PHASE1, not PHASE2?

You correctly questioned this. **Phase 2 produced 0 candidates today (so far).**

### Phase 2 design

Phase 2 is the intraday-momentum branch — selects stocks based on ACTUAL today's intraday momentum vs. premarket's static daily-frame scoring. To compute that signal, Phase 2 needs:

1. ≥ 4 5-min bars completed today (`today_bars < 4` returns `INSUFFICIENT_INTRADAY_BARS`)
2. ALL of the first 3 ORB slots (09:15, 09:20, 09:25) must have data
3. Plus the last 4 bars (rolling)
4. Historical baseline volume from previous days for the same slots
5. Price + volume normalized vs. that baseline

### Why it failed today

Watchlist build at 09:30:02 IST evaluated Phase 2 candidates while only **3 bars had completed** (09:15, 09:20, 09:25). The build took **12 minutes** to finish — but Phase 2 evaluation runs near the START. Result: 460 of 461 candidates rejected with `INSUFFICIENT_INTRADAY_BARS`.

Phase 2 rejection summary from 09:42 watchlist build log:
```
INSUFFICIENT_INTRADAY_BARS: 460  (99.78%)
STALE_INTRADAY_CACHE:         1
LCD_MISMATCH:                 0
LOW_SLOT_COVERAGE:            0
ZERO_VOLUME_RATE_HIGH:        0
LIQUIDITY_GATE_FAIL:          0
SETUP_GATE_FAIL:              0
MARKET_POLICY_BLOCKED:        0
PHASE2_WINDOW_CLOSED:         0
```

Result: system fell back to Phase 1 (premarket-selected stocks).

### Is this a bug?

**No — this is by design.** Phase 2 logic was last touched 2026-03-05 (2+ months stable). The bar-count threshold of 4 is reasonable: you can't compute "intraday momentum baseline vs today" without enough today data.

But it does mean **the first 30-45 min of every trading day rely on premarket-selected Phase 1 stocks**, even when better intraday-driven candidates would be available later.

### When will Phase 2 kick in today?

The watchlist rebuilds every 5 minutes via crons (`autotrader-watchlist-v2-5m-0930` etc), but each build takes ~12 min and holds a lock blocking subsequent builds. So watchlist refreshes happen ~every 12-15 min in practice.

**Expected next refresh with Phase 2:** the build that starts at ~09:42-09:45 (after the current 09:30 build releases lock). By that time, ~6-7 bars exist (09:15 through 09:45). Phase 2 should produce real candidates.

If today's pattern continues, by 10:00-10:15 IST the watchlist should have Phase 2 setups (BREAKOUT, OPEN_DRIVE, VWAP_TREND, MEAN_REVERSION, PULLBACK, VWAP_REVERSAL) replacing the PHASE1_MOMENTUM-only mix.

### Why is this a real problem worth addressing?

PHASE1_MOMENTUM picks are based on **yesterday's daily-frame momentum** — stocks that gapped up/showed strength end-of-day yesterday. They might NOT be today's actual movers.

Today's data so far suggests this:
- 6 high-scoring PHASE1_MOMENTUM signals (80-90 scores) — high score
- 4 of them blocked by `strategy_phase1_insufficient_volume` — the stocks aren't getting today's volume despite premarket strength
- 4 blocked by `live_price_below_vwap` — they couldn't even sustain above-VWAP

The strategy gates correctly reject these stale-momentum stocks. But the system is firing 0 trades while real movers go untouched.

### Possible improvements (post-launch)

1. **Lower Phase 2 threshold from 4 bars to 3** — fires Phase 2 starting at 09:25 instead of 09:30. Still requires the ORB slots which is the meaningful constraint. ~5-min earlier coverage.

2. **Reduce build duration** — 12 min for a watchlist build is heavy. If we can get it to <5 min, refreshes happen more frequently and Phase 2 catches up faster.

3. **Hybrid emission** — emit Phase 1 picks PLUS Phase 2 setups in parallel. Currently it's "Phase 2 if available, else Phase 1 fallback." A union would give the system more candidates to choose from.

These are post-launch improvements. Current behavior is correct, just data-lag-limited.

---

## Production state

| Service | Revision | Notes |
|---|---|---|
| `autotrader` | **`autotrader-00223-ffp`** | Live with PHASE1_* allowlist fix + all earlier audit fixes |
| `autotrader-dashboard` | `autotrader-dashboard-00063-rhc` | Live |
| `autotrader-ws-monitor` | `autotrader-ws-monitor-00040-n5c` | Live |
| `main` HEAD | `447fd40` | Emergency fix merged |
| Tests | 587 passing | +2 PHASE1 allowlist guard tests |

---

## What to watch next

1. **10:00-10:15 IST:** look for Phase 2 setups in the watchlist (BREAKOUT, VWAP_TREND, MEAN_REVERSION, PULLBACK, etc.). These should be today's actual movers.

2. **First trade:** monitor `trades` BQ table. The PHASE1_MOMENTUM signals at 09:48 had quality-gate blocks; once Phase 2 picks land, signals SHOULD qualify.

3. **15:25 IST:** EOD close + watch for `eod_skip_swing` log if any swing position opened (first time the swing-overnight code runs in production).

---

## What I learned today

1. **The `allowed_strategies` whitelist had a silent bug for weeks.** The audit data flagged it (`policy_strategy_blocked` was the dominant block reason for PHASE1_MOMENTUM in our 29-day window) but the systematic root-cause analysis only happened today. **Lesson: when a setup label has zero qualified trades despite high scan volume, check the strategy whitelist, not just the entry gate.**

2. **Phase 1 vs Phase 2 timing matters.** First 30 min of trading uses stale premarket selections. If those don't reflect today's tape, the system goes signal-poor for that window. Worth optimizing.

3. **Cloud Run revision rollouts have a 5-10 min propagation window.** The 09:42 scan ran on the OLD revision even though the new one was deployed at 09:40. By 09:48 the new revision served all traffic. Plan deploys with this lag in mind.

4. **The audit fixes I shipped overnight (MORNING_FADE kill, MEAN_REVERSION/PULLBACK gate relaxation, hard-blocks) are correctly in production** — they just haven't had a chance to fire yet because the watchlist composition is dominated by Phase 1.
