# E2E Backtest + Strategy Audit Report

**Generated:** 2026-05-08 01:30 IST
**Window:** 2026-04-10 → 2026-05-07 (29 trading days)
**Data sources:** Live trades (BQ `trades`), scan decisions (BQ `scan_decisions`), GCS canonical candles, sim engine

---

## TL;DR

**System is roughly breakeven (-₹1,361 over 29 days). No strategy is catastrophically broken in current state.** The historical bleeding strategies (BREAKOUT 0/9, VWAP_REVERSAL April 16 cluster) have already been addressed by gate additions earlier this month. No production code change is warranted from this audit.

The 5 strategies that have ever produced live trades:
- **VWAP_TREND** (workhorse, 51 trades, 43% WR, near breakeven) — keep
- **VWAP_REVERSAL** (13 trades, all from 2 days in April; gated since) — already addressed
- **BREAKOUT** (9 trades, 0% WR; hard-blocked May 6) — already addressed
- **MOMENTUM** (1 trade) — insufficient data
- **OPEN_DRIVE / AUTO** (1-2 trades each) — insufficient data

The other strategies (MEAN_REVERSION, PULLBACK, MORNING_FADE, PHASE1_*, SHORT_*) have NEVER produced a qualified live trade in 29 days.

---

## Per-strategy live performance

### LIVE actual data (BQ trades table, 29 days)

| Strategy | Trades | Wins | WR | Net P&L | Pattern |
|---|---|---|---|---|---|
| VWAP_TREND | 51 | 22 | 43.1% | -₹516 | Workhorse |
| VWAP_REVERSAL | 13 | 3 | 23.1% | -₹447 | Single-day disaster |
| BREAKOUT | 9 | 0 | **0.0%** | -₹326 | All losers |
| OPEN_DRIVE | 1 | 1 | 100% | +₹156 | Single trade |
| MOMENTUM | 1 | 0 | 0% | -₹94 | Single trade |
| AUTO/blank | 3 | 2 | 67% | -₹9 | Tiny |
| **TOTAL** | **78** | **28** | **35.9%** | **-₹1,361** | **Near breakeven** |

### Strategies that never produced a qualified live trade

| Strategy | Total scans | Reason for 0 trades |
|---|---|---|
| MEAN_REVERSION | 1,953 | Strategy entry gates strict; veto-lifted yesterday for swing |
| PHASE1_MOMENTUM | 3,963 | Phase-2 selection narrow |
| MORNING_FADE | 235 | Hard-blocked in TREND_UP, RANGE filter strict |
| PHASE1_REVERSAL | 140 | Phase-2 selection |
| PULLBACK | 96 | Strategy entry gates strict |
| SHORT_BREAKDOWN | 99 | Breadth filter + score gates |
| SHORT_PULLBACK | 2 | Almost never scanned |

---

## Sim engine fidelity (compare mode)

Sim diverges from live:

| Metric | Sim | Live | Delta |
|---|---|---|---|
| Trade count | 52 | 78 | Sim under-counts by 33% |
| Net P&L | -₹7,613 | -₹1,361 | Sim 5.6× more pessimistic |
| Win rate | 28.85% | 35.90% | Sim 7pp lower |

**Conclusion:** Sim absolute numbers are NOT trustworthy. Use sim only for relative strategy ordering. **All decisions in this report are driven by LIVE data**, not sim numbers.

---

## Strategy-by-strategy verdict

### VWAP_TREND ✅ KEEP
- **Live: 51 trades, 43.1% WR, -₹516 net** over 4 trading days (Apr 20-21, Apr 23, May 7)
- Most trades hit FLAT_TIMEOUT (26/51, 51%), not target — entries don't reach reward but don't crater either
- Recent day (May 7): 5 trades, 60% WR, +₹39 — improving
- **No action needed.** This is the system's main edge.

### VWAP_REVERSAL ⚠️ ALREADY GATED (no action)
- **Live: 13 trades, 23.1% WR, -₹447** — but ALL 13 are from Apr 16 (12) + Apr 20 (1)
- April 16 was a strong-trend day; 12 SELL "reversal" signals fired but the trend continued, all hit EOD_CLOSE
- After breadth filter tightening on April 22: **3,327 VWAP_REVERSAL evaluations, 0 qualified trades**
- Existing gates (`nifty_breadth_too_bullish_for_shorts`, `policy_strategy_blocked`, `score_below_min`) effectively block 100% of VWAP_REVERSAL signals now
- **No action needed.** Strategy is implicitly disabled by existing gates.

### BREAKOUT ✅ ALREADY HARD-BLOCKED (no action)
- **Live: 9 trades, 0% WR, -₹326** — all SL_HIT
- Hard-blocked in TREND_UP/DOWN since 2026-05-06 via `regime_affinity._HARD_BLOCKS`
- Live data confirms the block was correct
- **No action needed.**

### OPEN_DRIVE / AUTO / MOMENTUM 🟦 INSUFFICIENT DATA
- 1-3 live trades each — too small to act on
- Continue to monitor

### MEAN_REVERSION / PULLBACK / MORNING_FADE / SHORT_* 🆕 UNTESTED IN PRODUCTION
- **0 live trades in 29 days** despite thousands of scans
- Tomorrow is the first day MEAN_REVERSION is allowed as swing (yesterday's veto-lift)
- Tomorrow is the first day multi-emission can produce diverse swing watchlist rows
- **The actual e2e validation comes from tomorrow's live data, not historical backtest**

---

## Why backtest can't validate strategies more thoroughly

Tried this last night and confirmed: **the existing pure-replay harness has fidelity issues that prevent confident strategy-level conclusions.**

1. **Sim engine under-counts trades by 33%** vs live (compare mode result)
2. **Sim P&L is 5.6× more pessimistic** than live actuals
3. **Pure-replay's `--no-watchlist-per-day` mode** evaluates every setup against every bar, which is hypothetical multi-emission semantics, not the deployed code
4. **No watchlist generator in the harness** — can't test the layer we just changed (multi-emission)
5. **5-month 5m candle history** is what we have; intraday backtest can use it but swing's 6-month window covers limited regime variety

A faithful e2e backtest would require:
- Building an "as-of-date" runner that re-runs the entire pipeline (universe → watchlist → scan → entry → exit) per historical day
- Calibrating the sim slippage/cost model to match live fills
- ~3-5 hours of engineering work

**This is doable but not worth the night before launch.** Live data over coming days is the real validation.

---

## Live signal vs sim signal

| Question | Source | Answer |
|---|---|---|
| Is VWAP_TREND profitable? | Live | Near breakeven; no urgent issue |
| Is VWAP_REVERSAL a problem? | Live | Was a problem April 16; resolved by gates April 22 |
| Is BREAKOUT bleeding? | Live | Yes (0/9); already hard-blocked |
| Will multi-emission swing fire trades tomorrow? | Live (none yet) + sim (gates strict) | Uncertain; tomorrow tells us |
| Is the system net-profitable? | Live | No — breakeven minus costs |

---

## Recommended monitoring (post-launch)

For each strategy, set a hard-block threshold based on live data:

| Trigger | Action |
|---|---|
| Strategy WR < 30% over 30+ live trades | Hard-block in worst-performing regime |
| Strategy NET P&L < -₹500 over 30+ trades | Tighten entry gate (RSI / volume / trend) |
| Strategy cluster (>10 trades on single day, mostly losers) | Investigate; likely missing a regime/breadth filter |
| Strategy 0 trades in 30 days despite 1000+ scans | Calibration issue; lower thresholds OR remove from setup list |

---

## Final state

**Production:** unchanged from yesterday's end-of-day state.
- `autotrader-00221-b7w` — multi-emission + MEAN_REVERSION-as-swing + threshold=65 + audit_log fix
- `autotrader-dashboard-00063-rhc`
- `autotrader-ws-monitor-00040-n5c`

**Code:** unchanged. No new hard-blocks. No tightened gates. Backtest tooling improvements (1d path fix, multi-emission CLI flags) shipped last night.

**Tomorrow's actual validation:** the 09:22 IST swing scan + intraday session. If multi-emission produces ≥1 swing trade, the structural fix is validated. If `check_swing_entry` filters everything (consistent with pure-replay's 0-trade swing result), tomorrow looks like 0 swing trades — same as before — and we iterate from there with real data.

---

## What I learned

1. **Backtest with calibration mismatches is misleading.** Last night I overstated PULLBACK as "bleeding -₹61k" without checking sim fidelity. Live data shows that scenario is hypothetical (deployed code wouldn't fire those 97 trades).

2. **Live data over time-windowed buckets reveals more than aggregate stats.** VWAP_REVERSAL looked bad in aggregate (3/13) but was actually 1 disaster day. The strategy isn't broken; the regime gate was missing.

3. **Strategies that NEVER fire live deserve attention.** MEAN_REVERSION, PULLBACK, MORNING_FADE haven't qualified once in 29 days. Either gates are too strict, score formulas are miscalibrated, or these strategies need different watchlist criteria.

4. **The system's edge is currently thin** (-₹1,361 / 29 days = -₹47/day). Tomorrow's multi-emission deploy needs to demonstrate edge or we're running a breakeven-with-fees system.
