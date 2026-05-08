# Today's Pipeline — End-to-End Verification

**Generated during live session, 10:00 IST May 8 2026**
**Status:** Pipeline functioning correctly. 2 intraday trades fired post-fix. 0 swing trades (correctly filtered).

---

## Summary

| Channel | Status | Trades today | Verdict |
|---|---|---|---|
| **Intraday** | ✅ Working | **2 trades** (BSE + OLECTRA at 09:54 IST) | Healthy |
| **Swing** | ✅ Correctly filtered | 0 trades | Working as designed |
| **WS Monitor** | ✅ Active | Tracking 5 open positions | Healthy |

---

## INTRADAY pipeline E2E

### Pipeline timeline (verified)

| Time IST | Event | Status |
|---|---|---|
| 03:35 | Upstox token refresh | ✓ |
| 06:15 | Universe v2 refresh | ✓ |
| 07:35 | Score cache prefetch | ✓ |
| 08:30 | Score cron | ✓ |
| 08:45 | Score cache update | ✓ |
| 09:00 | Premarket watchlist v2 build | ✓ (finished 09:06) |
| 09:15 | Market open | — |
| 09:21+ | Intraday scan every 3 min | ✓ |
| 09:33 | Watchlist v2 5m rebuild #1 (Phase 1 fallback) | ✓ |
| **09:40** | **EMERGENCY FIX deployed (00223-ffp)** | ✓ |
| 09:48 | Watchlist v2 5m rebuild #2 (**Phase 2 LIVE**) | ✓ |
| **09:54** | **First 2 trades fired (BSE + OLECTRA)** | ✓ |
| Now | Continuing scans every 3 min | ✓ |

### What fired today

| Symbol | Side | Qty | Strategy | Entry | Target (R:R) | SL | Time IST |
|---|---|---|---|---|---|---|---|
| **BSE** | BUY | 1 | PHASE1_MOMENTUM | 3935.63 | 3978.37 (+1.08%) | 3894.36 (-1.05%) | 09:54:12 |
| **OLECTRA** | BUY | 3 | PHASE1_MOMENTUM | 1279.48 | 1291.88 (+0.97%) | 1267.26 (-0.96%) | 09:54:14 |

Both ~1:1 R:R (target ≈ stop distance). Tighter than the older 2R+ calibration — consistent with the MFE/MAE finding that intraday signals only reach 0.4-0.5R typically.

### Phase 1 vs Phase 2 evolution

**09:00 premarket build:** 100% Phase 1 (Phase 2 window not open).
**09:33 5m rebuild:** Still 100% PHASE1_DAILY_FALLBACK — Phase 2 had only 3 bars when it ran (needs 4+).
**09:48 5m rebuild:** **100% PHASE2_INPLAY** — diverse setup mix:
- VWAP_REVERSAL: 97 (65%) — RANGE-favorable
- PULLBACK: 18 (12%)
- OPEN_DRIVE: 15 (10%)
- MEAN_REVERSION: 11 (7%)
- VWAP_TREND: 9 (6%)

So Phase 2 kicked in at the second rebuild, ~33 min after market open. From now on, scans use today's actual intraday-momentum picks, not yesterday's premarket selections.

### Why only 2 trades fired so far

At 09:54 scan (first scan after the 09:40 fix), 5 high-score PHASE1_MOMENTUM signals were evaluated:

| Symbol | Score | Result |
|---|---|---|
| ENGINERSIN | 84 | strategy_phase1_insufficient_volume |
| ADANIENT | 80 | strategy_phase1_insufficient_volume |
| GVT&D | 77 | live_price_below_vwap |
| ADANIGREEN | 77 | strategy_phase1_insufficient_volume |
| HFCL | 78 | strategy_phase1_insufficient_volume |
| **BSE** | **73** | **✓ QUALIFIED → traded** |
| **OLECTRA** | **72** | **✓ QUALIFIED → traded** |

The higher-score stocks were correctly rejected — they didn't have today's volume confirmation despite premarket strength. BSE + OLECTRA passed all gates.

---

## SWING pipeline E2E

### Pipeline timeline (verified)

| Time IST | Event | Status |
|---|---|---|
| 03:30 | Swing reconciliation | ✓ |
| 09:00 | Premarket watchlist with multi-emission | ✓ — emitted 145 BREAKOUT + **5 MEAN_REVERSION** |
| 09:22 | Swing scan (single run/day) | ✓ |
| Now | No more swing activity until tomorrow | — |

### Swing scan results today

Single scan at 09:22 evaluated 35 of 150 swing watchlist rows (rotational batching — all rows scanned across multiple days):

| Block reason | Count | Verdict |
|---|---|---|
| `regime_strategy_hard_block` (BREAKOUT × TREND_UP) | 27 | ✓ Correct |
| `sl_too_wide_for_risk_budget` | 4 | ✓ Risk gate |
| `score_below_min` | 4 | ✓ Threshold filter |
| **Total** | **35** | All correctly blocked |

The single MEAN_REVERSION row evaluated today: **UBL SELL score 32** → score_below_min. Score way below threshold — correctly rejected.

### Multi-emission validation

Tonight's 5 MEAN_REVERSION rows in the swing watchlist:
- Emission worked (was 0 historically due to winner-takes-all + veto)
- Today's evaluation: 1 of 5 evaluated, scored 32 (poor signal for that stock)
- Other 4 will be evaluated on subsequent days as cursor rotates

**Conclusion:** the multi-emission fix shipped overnight is working — MEAN_REVERSION rows ARE being emitted as swing setups. They just need higher-quality candidates to actually qualify.

### Why 0 swing trades is correct today

In TREND_UP regime:
- BREAKOUT swing: hard-blocked (correct — 0/9 live WR justified)
- MEAN_REVERSION swing: emitted but stock-specific scoring filtered the 1 evaluated
- Other swing-compatible setups (PULLBACK, SHORT_*): not in today's watchlist

System is **correctly conservative** — refuses to fire low-quality signals. Tomorrow's regime + universe might produce different qualified candidates.

---

## WS Monitor — open positions

Currently tracking **5 positions** via WebSocket LTP feed:

### Today's 2 trades (LIVE)
- **BSE** intraday PHASE1_MOMENTUM BUY qty=1 entry 3935.63
- **OLECTRA** intraday PHASE1_MOMENTUM BUY qty=3 entry 1279.48

These will exit on:
- Target hit (BSE 3978.37, OLECTRA 1291.88)
- SL hit (BSE 3894.36, OLECTRA 1267.26)
- EOD close at 15:25 IST (intraday MIS)

### 3 stale paper-trade positions from 2026-04-29 (9 days old)

These are paper-trade artifacts that didn't auto-close because:
1. **Swing positions persist overnight by design** (CNC product, no EOD force-close)
2. **Targets/SLs not yet hit** — large distances on these older positions
3. **`swing_max_hold_days` will close them** at day 11

| Symbol | Days held | Target distance | SL distance |
|---|---|---|---|
| KRN | 9 | +27% | -13% |
| NESTLEIND | 9 | +8.4% | -4.3% |
| KTKBANK | 9 | +17.6% | -8.9% |

These don't affect today's trading — just sit on the books in paper mode. They'll close automatically at max-hold (~tomorrow or day after).

---

## Critical bug found + fixed mid-session

**The `allowed_strategies` whitelist in `market_brain_service.py` was MISSING `PHASE1_MOMENTUM` and `PHASE1_REVERSAL`.** This silently caused every PHASE1_* watchlist row to be rejected with `policy_strategy_blocked` for weeks (3,963 wasted scans in our 29-day audit window).

Today's 09:30 watchlist was 100% PHASE1_MOMENTUM (Phase 2 wasn't ready yet). Without the fix, **the system would have fired 0 intraday trades today.**

Fixed at 09:40 IST in commit `6c5f800`. Deployed to revision `autotrader-00223-ffp`. Verified working at 09:48 (zero policy_strategy_blocked vs 6 at 09:42 on old revision).

The 2 trades that fired at 09:54 (BSE, OLECTRA) **directly resulted from this fix** — they would otherwise have been blocked.

---

## Production state right now

| Component | Status |
|---|---|
| `autotrader` Cloud Run | ✓ Live on `autotrader-00223-ffp` (PHASE1_* fix + all earlier audit fixes) |
| `autotrader-dashboard` | ✓ Live |
| `autotrader-ws-monitor` | ✓ Tracking 5 positions, refreshing every 15s |
| Schedulers | ✓ All ENABLED, firing on schedule |
| Universe v2 freshness | ✓ Refreshed today at 06:15 IST |
| Score cache freshness | ✓ Updated 08:43 IST |
| Watchlist freshness | ✓ Latest 09:48 IST (Phase 2 live) |
| Upstox token | ✓ Refreshed 03:35 IST |
| `main` HEAD | `447fd40` — emergency fix merged |
| Tests | 587 passing |

---

## What to expect for the rest of today

**10:00-11:00 IST:**
- Phase 2 watchlist now live → scans evaluate today's actual movers
- VWAP_REVERSAL (97 stocks) is the dominant setup — will fire if RANGE-like price action emerges
- VWAP_TREND, PULLBACK, MEAN_REVERSION, OPEN_DRIVE all eligible
- Likely 1-3 more intraday trades if any setup qualifies on score+gates

**11:00-14:00 IST:**
- Continued 3-min scans with Phase 2 watchlist
- Watchlist may rebuild every 12-15 min capturing market evolution

**14:45 IST (INTRA_FINAL block):** final watchlist rebuild before EOD

**15:25 IST EOD:**
- Intraday positions (BSE + OLECTRA if still open) force-closed by WS monitor
- Swing positions skipped (eod_skip_swing log fires) — first time this code path runs on real positions in production
- 3 stale Apr 29 swing positions also persist overnight (within max-hold)

**16:00 IST:**
- Score cache update
- EOD reconciliation 15:25-15:29

---

## Bottom line

✅ **Pipeline is working end-to-end correctly.**
✅ **Today's emergency PHASE1_* fix unlocked the first 2 intraday trades** of the session — without it, 0 trades would have fired.
✅ **Phase 2 is now active** — scans now use today's actual intraday-momentum candidates.
✅ **Swing channel correctly conservative** — strict gates appropriately filtering low-quality signals in TREND_UP regime.
⚠️ **3 stale paper positions** from 9 days ago — will auto-close at max-hold; non-blocking.

**No additional action needed for today's session.** Monitor for trade outcomes (target hits, SL hits, EOD close), then assess tomorrow's setup based on closing P&L.
