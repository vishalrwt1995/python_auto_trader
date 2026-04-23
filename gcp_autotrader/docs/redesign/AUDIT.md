# End-to-End Audit — Why the System Does Not Make Money

**Branch:** `redesign/audit-and-design`
**As of:** 2026-04-23 (commit `c4bb62e`)
**Live evidence (last 7 sessions, paper):** 46 trades, **0 targets hit**, 0 partial exits, **27 FLAT_TIMEOUT**, **19 SL_HIT**. Net loss.
**Mandate:** "Code is the source of truth. Every algo, logic, flow covered before re-design."

> This audit scopes the **entire** live pipeline, from Cloud Scheduler tick → universe refresh → scan → scoring → regime/brain/policy → entry decision → order → ws_monitor exit → Firestore/BigQuery persistence → dashboard. Every claim carries a `file:line` citation. Where a claim cannot be grounded in code, it is labelled `UNVERIFIED`.

---

## TL;DR — The System Is Mathematically Unprofitable

1. **Payoff broken by construction.** Intraday target = `1.25 × ATR` (`settings.py:63`), SL trailing post-breakeven = `1.5 × ATR` (`ws_monitor_service.py:491–514`). Trail is **tighter than target**, so once breakeven-SL fires (at only `+1.0 × ATR` MFE, `ws_monitor_service.py:319–343`) the position is mathematically more likely to exit on trail than hit target. Break-even win-rate **≈ 72%**; realised win-rate **0/46 = 0%**.
2. **FLAT_TIMEOUT is a scythe for wounded winners.** Hardcoded 120 min, guard only checks `|ltp - entry| < 0.3 × ATR` (`ws_monitor_service.py:256, 531–537`) — does **not** check MFE. A position that traveled `+1.0 × ATR` and drifted back is exited "flat" alongside legitimately flat ones, masking that the breakeven mover is the real killer.
3. **Payoff math at trade level (E=₹100, ATR=₹1.50, qty=83, entry slip 0.10%, SL slip 0.20%, brokerage ~0.05% rt)** — a trade that kisses `+1R` and reverses to SL exits at **−₹113.75 on a ₹125 risk budget**, i.e. −0.91R; while a "winner" hitting full `+1.25R` nets ≈ +1.12R after costs. You need 72% strike rate on a 1.25R target with mandatory `+1R` touch first. Structurally impossible.
4. **Swing channel is dead.** Batch 6 tightened MOMENTUM/swing gates (strength ≥ 50, supertrend direction agreement) — result: 0 swing entries in 7 days. Intraday MIS is carrying all the loss, unhedged by the structurally-better (lower-cost, higher-RR) swing book.
5. **Policy layer is toothless.** `allowed_strategies` is set off `brain.regime` (daily regime), but the choppy-market block depends on `regimeIntraday` which is **not** wired into the hard-block (`market_policy_service.py`). In a RANGE daily regime + CHOPPY intraday, BREAKOUT/MOMENTUM still fire; those are the majority of our 19 SL_HITs.
6. **Every fail-path "silently continues."** In `trading_service.py` alone, **4 distinct `except` blocks** log a warning and proceed as if nothing happened — brain build, daily PnL, trade count, re-entry cooldown. On a Firestore blip we can fire 10+ trades in a single scan with zero PnL checks (`trading_service.py:424–474`). One Firestore flake can cancel every risk cap simultaneously.
7. **Swing paper positions have no SL of any kind.** GTT is broker-side and live-only (`order_service.py:287`); paper path skips it. If `ws_monitor` crashes, the paper swing book has **zero protection** until the next reconciliation pass.
8. **Exit-price fallback hides losses.** If the live LTP fetch fails at exit, fallback is `entry_price` with `exit_reason="..._NO_FILL_PRICE"` (`order_service.py:731–744`) — realised PnL lands in BigQuery as `₹0`, corrupting every post-mortem aggregation.
9. **Observability is a black box for the one question that matters.** MFE/MAE/breakeven-trigger-price/breakeven-trigger-MFE are **not persisted anywhere** (`order_service.py:152–182`). We cannot post-mortem the 27 FLAT_TIMEOUTs to prove the breakeven-SL theory — the schema was designed to hide this.
10. **Upstox is underused by a factor of ~3×.** No option chain / PCR / OI, no option-greek WS mode, no news API (launched 17-Apr-2026), no portfolio stream (we poll), no GTT for swing paper, no extended read-only token, no brokerage pre-check, no max-pain, and v3 historical (1-min since Jan 2022) is ungrounded for backtesting. Every profit-meaningful edge the broker exposes is one we ignore.

The system is not "losing unlucky trades." It is a well-engineered loss machine — each component works as designed, and the design guarantees loss.

---

## TOC

1. [Deployment Topology](#1-deployment-topology)
2. [Cloud Scheduler Job Inventory](#2-cloud-scheduler-job-inventory)
3. [Universe / Watchlist Pipeline](#3-universe--watchlist-pipeline)
4. [Market Data & Upstox API Surface](#4-market-data--upstox-api-surface)
5. [Scoring Layer — Signals](#5-scoring-layer--signals)
6. [Regime / Brain / Policy Layer](#6-regime--brain--policy-layer)
7. [Setup Catalog](#7-setup-catalog-intraday--swing)
8. [Orchestration — 10-Stage Entry Funnel](#8-orchestration--10-stage-entry-funnel)
9. [Order Placement (Paper + Live)](#9-order-placement-paper--live)
10. [ws_monitor — Real-time Position Management](#10-ws_monitor--real-time-position-management)
11. [Portfolio, Risk, Kill-switches](#11-portfolio-risk-kill-switches)
12. [Persistence & Observability](#12-persistence--observability)
13. [Critical Findings — Why Every Trade Loses](#13-critical-findings--why-every-trade-loses)
14. [Unexploited Upstox API Surface](#14-unexploited-upstox-api-surface)
15. [Dead Code / Drift / Hidden Risk](#15-dead-code--drift--hidden-risk)

---

## 1. Deployment Topology

- **Project:** `grow-profit-machine`, region `asia-south1`, account `vishalrwt1995@gmail.com`.
- **Cloud Run services:**
  - `autotrader` (`autotrader-00210-zg8`) — FastAPI; scan, recon, watchlist, scoring, jobs.
  - `autotrader-ws-monitor` (`autotrader-ws-monitor-00033-prr`) — `min-instances=1`, hosts the Upstox WS tick loop and exit state machine.
  - `autotrader-dashboard` (`autotrader-dashboard-00062-pg7`) — read-only dashboard.
- **Storage:** Firestore (`(default)`) + BigQuery (`grow-profit-machine:autotrader`) + GCS candle cache.
- **Cache:** GCS bucket for daily/intraday candles (universe service ingests into Firestore `universe` + `universe_intraday`).
- **Secrets:** `upstox_access_token` secret, rotated daily at 08:35 IST by `autotrader-upstox-token-request`.

## 2. Cloud Scheduler Job Inventory

Twenty enabled jobs (IST crons, Mon–Fri unless noted):

| Job | Cron | Target |
|---|---|---|
| `autotrader-upstox-token-request` | `35 3 * * 1-5` | refresh Upstox access_token |
| `autotrader-universe-v2-refresh-0615` | `15 6 * * 1-5` | build universe (daily candles + indicators) |
| `autotrader-score-cache-update-close-0705` | `5 7 * * 1-5` | cache precompute |
| `autotrader-score-cache-update-close-0740` | `40 7 * * 1-5` | cache precompute #2 |
| `autotrader-score-0830` | `30 8 * * 1-5` | score ranking for shortlist |
| `autotrader-watchlist-v2-premarket-0900` | `0 9 * * 1-5` | seed watchlist |
| `autotrader-swing-recon-0900` | `30 3 * * 1-5` | **09:00 IST** swing reconciliation (cron is in UTC) |
| `autotrader-watchlist-v2-5m-0930` | `30-59/5 9 * * 1-5` | 5-minute watchlist refresh (first 30 min) |
| `autotrader-watchlist-v2-5m-1000` | `0-30/5 10 * * 1-5` | 5-minute watchlist refresh |
| `autotrader-watchlist-v2-15m-1045` | `45 10 * * 1-5` | 15-minute pass |
| `autotrader-watchlist-v2-15m-11to12` | `0,15,30,45 11-12 * * 1-5` | 15-minute passes |
| `autotrader-watchlist-v2-15m-1300` | `0 13 * * 1-5` | afternoon pass |
| `autotrader-watchlist-v2-final-1445` | `45 14 * * 1-5` | pre-close watchlist |
| `autotrader-scan-intraday-3m` | `21-57/3 9-14 * * 1-5` | **scanner: every 3 min, 09:21–14:57** |
| `autotrader-scan-intraday-1530` | `0-27/3 15 * * 1-5` | scanner: 15:00–15:27 |
| `autotrader-scan-swing-0922` | `22 9 * * 1-5` | one-shot swing scan |
| `autotrader-eod-recon-1525` | `25 15 * * 1-5` | EOD force-close #1 |
| `autotrader-eod-recon-1527` | `27 15 * * 1-5` | EOD force-close #2 |
| `autotrader-eod-recon-1529` | `29 15 * * 1-5` | EOD force-close #3 |
| `refresh-earnings-calendar` | `0 8 * * 0` | Sunday NSE events fetch |

**Observations**
- Scanner fires **~117 times per trading day** (3-min cadence × ~5.85 h) — every scan reruns all gates. A 72-score threshold × 100-symbol watchlist × 117 scans ≈ 11,700 decisions/day; the 46-trade/7-day throughput means rejection rate **≈ 99.97%**.
- Three separate EOD-recon jobs are a belt-and-braces hack, not a primary design — indicates prior single-point-of-failure incidents.
- No scheduler job exists for: Upstox news ingestion, portfolio-stream reconciliation, option-chain snapshots, FII/DII data, backtest runs, or daily metrics rollup.

## 3. Universe / Watchlist Pipeline

**Files:** `services/universe_service.py`, `services/universe_v2.py`

- **Universe seed** (pre-open): top 250–300 liquid NSE equities; fetch daily candles from GCS cache + live Upstox fallback; compute EMA(9/21/50), RSI(14), ATR(14), SuperTrend(10,3.0); write to Firestore `universe`.
- **Intraday universe** (during session): 5-min candles for the top ranked symbols; written to `universe_intraday`.
- **Watchlist**: selected subset (usually ~100 symbols), re-scored at 5/15/30-min cadence; rows carry `score`, `affinity_score`, `direction`, `strategy`, `entry_price`, `sl_price`, `target`, `atr`, `regime_at_score`.
- **Stale-retry fix** (this branch): `_normalize_last_candle_time()` (commit `c4bb62e`) unified ISO vs space-separated timestamps across six prev-row comparison sites; full test suite 247/247.

**Issues**
- Watchlist is **overwritten** every refresh — no history of *why* a symbol was shortlisted, so we cannot correlate "trade outcome" with "shortlist reason" post-hoc.
- No dedupe/idempotency on scores: the same symbol can be rescored with a materially different score mid-session, the scanner re-reads it and may enter after an earlier scan rejected it.
- Universe pipeline has no dependency on volatility regime — in PANIC it still returns the same liquid universe instead of pivoting to defensive names.

## 4. Market Data & Upstox API Surface

**File:** `adapters/upstox_client.py`, `adapters/upstox_ws_client.py`

**Endpoints we use**
- `/v2/login/authorization/token` — auth code → access token.
- `/v2/historical-candle/...` — daily + intraday candles.
- `/v2/market-quote/quotes` — full quote (used sporadically for live LTP).
- `/v2/order/place` — BO intraday, regular CNC swing.
- `/v2/order/cancel`, `/v2/order/modify` — exit + modifications.
- `/v2/order/gtt/place`, `/cancel`, `/modify` — live swing SL.
- WebSocket `wss://wsfeeder-api.upstox.com/market-data-feeder/v3/...` with `full` mode (L5 depth + LTP + quotes) — protobuf-decoded in `upstox_ws_client.py`.

**Rate limits** — 50/s, 500/min, 2000/30min (shared bucket per SEBI 2025-05). We are nowhere near this; the limit is our decision logic, not API throughput.

**Major gaps** — see §14.

## 5. Scoring Layer — Signals

**Files:** `domain/scoring.py`, `domain/indicators.py`, `domain/daily_bias.py`, `domain/regime_affinity.py`, `domain/risk.py`, `domain/models.py`

- **Indicators computed per bar:** EMA(9/21/50), RSI(14), ATR(14), SuperTrend(10,3.0), VWAP (session), Bollinger, Stochastic, MACD, ADX(14), relative volume (vs 20-bar avg).
- **Daily bias vote** (`daily_bias.py`): counts bullish vs bearish signals across ~6 checks (EMA stack, SuperTrend, RSI slope, MACD cross, price vs 50-EMA, volume trend). A delta of **≥3** flips bias; less → NEUTRAL.
- **Scoring** (`scoring.py`) produces a raw `score ∈ [0,100]` per (symbol, direction, strategy) from weighted component sub-scores:
  - Trend alignment (EMA stack + SuperTrend direction)
  - Momentum (RSI zone, MACD histogram)
  - Volatility fit (ATR vs rolling median — avoid ultra-low ATR "noise traps")
  - Volume confirmation (rel vol ≥ 1.2)
  - Regime affinity (`regime_affinity.py`) multiplier — per (strategy × regime) lookup table, 0.2–1.4×.
- **Adjusted score** = raw × regime_affinity × brain haircut (risk_mode-dependent; see §6).
- **Minimum to qualify:**
  - Intraday: `min_signal_score = 72` (`settings.py:43`), modulated per risk_mode: AGGRESSIVE 72, NORMAL 74, DEFENSIVE 76, LOCKDOWN 76.
  - Swing: `swing_min_signal_score = 70` (`settings.py:87`) — checked **pre-haircut**.

**Problems**
- **Hardcoded SuperTrend parameters** (10, 3.0) — not tuned per symbol volatility cluster, so large-cap NIFTY50 and mid-cap Next50 get identical flip sensitivity.
- **EMA-stack boolean gate** (inside scoring) blocks shorts in early downtrends when EMA9 is still above EMA21 one bar after the flip — by the time the stack confirms, the move is half done.
- **Volume rel-vol threshold** uses 20-bar average but in the first 30 min of session the average is distorted by the opening print — first two BREAKOUT/MOMENTUM scans each day are volume-biased high.
- **No persistence** of score components per decision — we log the final adjusted_score but not why it was high/low (trend 30 + momentum 25 + vol 15 + affinity 1.2× = 84 is irrecoverable from `scan_decisions` table).
- **Intraday-reversion `rr` = 2.0** (`settings.py:64`) is disjoint from the main 1.25 RR — so MEAN_REVERSION and VWAP_REVERSAL get a better payoff but fire less often; the mass-volume strategies (BREAKOUT/MOMENTUM) are stuck at 1.25.

## 6. Regime / Brain / Policy Layer

**Files:** `services/regime_service.py`, `services/market_brain_service.py`, `services/market_policy_service.py`, `services/market_breadth_service.py`, `services/market_leadership_service.py`

- **Daily regime** (`regime_service`): TREND_UP, TREND_DOWN, RANGE, CHOP, PANIC, RECOVERY. Computed from NIFTY50 daily bars + breadth (advancers/decliners) + leadership (sectoral top-N). Transitions require `transition_min_age_sec = 240` to avoid flap.
- **Intraday state** (`market_brain_service`): TRENDY, CHOPPY, TRANSITIONAL; computed every 180 s from 5-min NIFTY bars + breadth snapshot. Written to Firestore `market_brain/current` as `{regime, intraday_state, risk_mode, allowed_strategies, min_score_boost, max_positions_limit, last_updated}`.
- **Thresholds** (`settings.RegimeThresholds`): panic_stress ≥ 82, trend_up_trend ≥ 70, chop_stress ≥ 62, recovery_trend ≥ 40, lockdown_stress ≥ 85, defensive_stress ≥ 65, aggressive_appetite ≥ 66.
- **Risk mode** → `min_score_boost`, `max_positions_multiplier`:
  - AGGRESSIVE: +0 score, 1.4× positions
  - NORMAL: +2 score, 1.0× positions
  - DEFENSIVE: +4 score, 0.7× positions
  - LOCKDOWN: +4 score, 0.4× positions
- **Allowed strategies per regime** (from brain):
  - TREND_UP: all
  - RECOVERY: BREAKOUT, MOMENTUM, PULLBACK, OPEN_DRIVE (no reversion)
  - RANGE: MEAN_REVERSION, VWAP_REVERSAL, PULLBACK
  - CHOP: MEAN_REVERSION, VWAP_REVERSAL only
  - PANIC: MEAN_REVERSION only (and shorts in TREND_DOWN)
- **Policy layer** is **pass-through** — `market_policy_service.allowed_strategies()` returns the brain's set with a final `breadth` gate: `SELL` rejected when breadth ≥ threshold.

**The toothless-policy bug**
- `regimeIntraday` (CHOPPY) is computed, persisted, and shown in dashboards — but it is **never** combined with `brain.regime` to hard-block setups. A day that is **RANGE daily + CHOPPY intraday** still allows BREAKOUT/MOMENTUM through the pass-through; those fire, hit SL, and feed the 19 SL_HITs.
- The "dead" substructure in brain — `sub_regime_v2`, `participation`, `event_state`, `structure_state` — is computed per call but never wired into decisions. Pure overhead.

## 7. Setup Catalog (Intraday + Swing)

**Files:** `domain/scoring.py` (check_strategy_entry, check_swing_entry), `services/trading_service.py`, `services/universe_service.py`

### Intraday (MIS)

| Setup | Trigger (simplified) | RR | Volume gate | Cited |
|---|---|---|---|---|
| **BREAKOUT** | Close above prior-day-high + within 0.2% retest; ADX ≥ 20; rel vol ≥ 1.5 | 1.25 | yes | `scoring.py` check_breakout |
| **PULLBACK** | Price tags EMA21 from above after ≥3 up bars; RSI > 45; SuperTrend_dir = 1 | 1.25 | soft | `scoring.py` check_pullback |
| **MEAN_REVERSION** | Price > 2σ Bollinger, RSI < 30 (BUY) or inverse; ATR in mid band | 2.0 | no | `scoring.py` check_mr |
| **OPEN_DRIVE** | First 5 min high/low break in direction of pre-market bias; NIFTY aligned | 1.25 | yes | Batch 5 `universe_service` |
| **MOMENTUM** | 3-bar higher-highs, rel vol ≥ 2, MACD hist > 0, strength ≥ 50 | 1.25 | strict | Batch 6 gate |
| **VWAP_REVERSAL** | Disabled (`disabled_strategies=('VWAP_REVERSAL',)` in settings) | — | — | `settings.py:95` |

### Swing (CNC)

- Batch 6 gate: `supertrend_dir` must agree with direction AND `strength ≥ 50`. Pre-Batch-6 swing fired ~8× more often; post-Batch-6 = **0 entries in 7 days**.
- Setup variants (`check_swing_entry`): BREAKOUT_SWING, PULLBACK_SWING, REVERSAL_SWING, TREND_CONTINUATION.
- Exit logic in swing reconciliation (§11, `swing_reconciliation_service.py`):
  - `MAX_HOLD_10D` — `days_held ≥ 10` (`settings.swing_max_hold_days`).
  - `SL_BREACH_DAILY` — close past SL on a daily candle.
  - `TARGET_HIT_DAILY` — close past target.
  - `DAILY_SUPERTREND_FLIP` — SuperTrend direction against position.

**Problems**
- **Setup overlap.** BREAKOUT + MOMENTUM share many triggers; both can fire the same scan for the same symbol → we size one 1R position that is actually correlated to two bets, but only one slot is used.
- **MEAN_REVERSION has a 2.0 RR but hugs Bollinger 2σ**, which has a low strike rate; because `min_signal_score` boost forces it through only in tight regimes, the hit-rate isn't reported separately in `scan_decisions`.
- **OPEN_DRIVE** cuts off at the first 5-min candle; given our scan cadence of 3 min starting at 09:21, we can miss the setup entirely if the 09:21 watchlist hasn't yet ingested the 09:20 close.
- **No trend-following continuation intraday** — nothing that adds to a winner on a second higher-high (all our wins get chopped by the same 1.25 RR ceiling).

## 8. Orchestration — 10-Stage Entry Funnel

**File:** `services/trading_service.py` (entry path `run_scan_once`, lines 269–1120)

Per scan:

```
Stage 1: market open + lock (timeout 60s)        trading_service.py:283–302
Stage 2: PnL circuit breaker                     trading_service.py:420–484
   - realized_pnl ≤ -₹300  → _pnl_block_reason="daily_loss_limit_hit", CONTINUE restrict (line 456)
   - realized_pnl ≥ +₹375  → HALT (line 454–455)
   - trade_count ≥ 5       → HALT (line 475–484)
Stage 3: load watchlist + recent_exits (30m cooldown)   trading_service.py:486–557
Stage 4: build market_brain (fallback last_known_good)  trading_service.py:310–351, 401–414
Stage 5: position quota (intraday max 2–5; swing 5)    trading_service.py:715–800, 955–957
Stage 6: per-symbol gates (20+ checks)                 trading_service.py:908–1016
   a  SL > risk budget         → "sl_too_wide_for_risk_budget"
   b  recent_exit              → "reentry_cooldown"
   c  long_enabled / short_enabled
   d  breadth-vs-direction
   e  strategy allowed (market_policy)
   f  regime hard-block (regime_affinity)
   g  daily loss gate          → MR/VWAP_REV only (line 959)
   h  candle staleness         → LTP > 1.2–4 % from candle close rejected
   i  VWAP guard               → BUY below VWAP / SELL above VWAP rejected
   j  check_strategy_entry / check_swing_entry (ok_bool, fail_reason)
   k  sector concentration     → max 2 same sector
   l  strategy concentration   → max 2 same strategy
   m  earnings blackout        → result_date ± 2 trading days
Stage 7: adjusted_score ≥ dynamic_min_score            trading_service.py:1019
Stage 8: entry window (09:15–15:25 IST)                trading_service.py:1020
Stage 9: calc_position_size (risk.py)                  trading_service.py:1048–1070
Stage 10: place_entry_order                            trading_service.py:1086
```

**Silent-continue paths in `trading_service.py`** (every one is a loaded gun):

| Line | Branch | On failure |
|---|---|---|
| 311–351 | `build_premarket_market_brain()` | warn, use last_known_good; if stale, cap `max_positions *= 0.7`. **No freshness check vs today's date.** |
| 424–426 | `get_today_realized_pnl()` | warn, continue scan **without any PnL cap**. |
| 471–474 | `get_today_trade_count()` | warn, `_today_trade_count = 0` → the 5-trade hard cap disappears. |
| 545–555 | `get_recently_exited_symbols()` | debug log only, `_recent_exits = {}` → 30-min cooldown vanishes. |

Hit any two of these simultaneously (e.g., Firestore slow) and you've disabled every daily risk cap. This is not a theoretical risk — Firestore latency spikes are routinely 2–5 s on `asia-south1`.

## 9. Order Placement (Paper + Live)

**File:** `services/order_service.py`

### Entry (`place_entry_order`, lines 357–628)

- **Idempotency** (`state.already_fired_today`, line 379): `{symbol}-{side}` key; prevents same-direction dupes in one trading day. **Gotcha:** `mark_fired_today` is only written after Firestore `_save_position_firestore()` (line 489/583/626) — a crash between order placement and save leaves the idempotency key unset and we can fire again on the next scan.
- **SL / qty validation** (lines 385–428, post-mortem 2026-04-21): reject `sl ≤ 0`, `qty ≤ 0`, or inverted SL/target sides. Fail-loud. ✅
- **Paper slippage** (lines 483–488): `paper_entry_slippage_pct = 0.10%` — BUY fills higher, SELL fills lower.
- **Live path** (lines 551+): 25 s fill-poll timeout. If unfilled, write to `pending_orders` for next scan reconcile. **No position save yet**, so no ws_monitor coverage until reconcile happens (up to 3 min later).
- **GTT placement on swing live** (lines 575–582): `self._place_gtt_sl(...)` — return value **not checked**. If GTT fails (network, rate limit), swing position is live with **no broker-side SL**. `_place_gtt_sl` itself is paper-skipped (line 287: `if settings.runtime.paper_trade or not instrument_key: return None`) — the paper swing book has no broker-side SL by design, and the caller does not create any software-side GTT either.

### Exit (`place_exit_order`, lines ~630–900)

- **Routing:** called from ws_monitor, from trading_service eod-recon, from swing_reconciliation.
- **Paper SL/EOD/Timeout/Partial slippage** (lines 668–675): `paper_sl_slippage_pct = 0.20%`, applied only when reason does NOT contain "TARGET".
- **Live exit quote fallback** (lines 731–744): on quote fetch failure → fallback to `entry_price`, tag `..._NO_FILL_PRICE`. **Realized PnL persists as 0** — corrupts BigQuery aggregation and hides the loss.
- **Brokerage:** computed per-leg via `calc_round_trip_brokerage()` helper; partial exits double-bill because each partial calls the helper again with a fresh round-trip assumption.

### Reconciliation (`reconcile_pending_entries`, lines 891–953)

- Polls `pending_orders` via `state.list_pending_orders`; for each: `_extract_order_snapshot` → if FILLED save position, if final-non-fill clear `mark_fired_today`, else leave pending.
- **Upstox API outage stalls the entire reconcile indefinitely** — no timeout or dead-letter queue; `pending_orders` can accumulate forever (`order_service.py:891–953`).

## 10. ws_monitor — Real-time Position Management

**File:** `services/ws_monitor_service.py` (760 lines)

### Lifecycle
- Container stays alive across days (`min-instances=1`, line 1–24 docstring). Each trading day: refresh positions from Firestore every 15 s (`POSITION_REFRESH_INTERVAL = 15`, line 53), subscribe to Upstox WS `full` mode, dispatch ticks to `_on_quote`. WS disconnect → exponential back-off. Signal handlers set `_stop_event`.
- **SL persistence throttling**: trailing updates capped at 1/30 s/position (`_sl_last_persist`, line 83). Breakeven/regime-tighten bypass throttle and write immediately. Best-price persistence throttled separately (`_best_last_persist`, line 88) — added Batch 3.1 post-restart-regression fix.

### Exit precedence (`_on_quote`, lines 258–545)

| # | Trigger | Condition | Slippage |
|---|---|---|---|
| 1 | Breakeven SL mover | `best ≥ entry + 1.0×ATR` (intraday), `1.5×ATR` (swing); moves SL to `entry + 0.1×ATR` / `entry + 0.15×ATR` | none (SL move only) |
| 2 | Target-passed flag | `ltp ≥ target` (BUY) / `ltp ≤ target` (SELL); sets `target_passed=True`, `sl_moved=True`, shifts trail to `1.2×ATR` | none |
| 3 | Partial exit stage 1 | `ltp ≥ entry + sl_dist` if `qty ≥ 3` (tag PARTIAL_1R); or `qty==2` (PARTIAL_1R_QTY2) | SL 0.20% |
| 4 | Partial exit stage 2 | `ltp ≥ entry + sl_dist×1.5` if `qty ≥ 3` (PARTIAL_1_5R) | SL 0.20% |
| 5 | Regime-change tighten | entry in TREND_UP/RECOVERY, current in CHOP/PANIC/TREND_DOWN (intraday only, swing immune per Batch 2.3); SL → `ltp ± 0.8×ATR` | none |
| 6 | Trailing SL | `sl_moved=True` → SL = `best ± (atr × 1.5 or 1.2 post-target)`; one-way ratchet | none |
| 7 | SL_HIT | `ltp ≤ sl` (BUY) / `ltp ≥ sl` (SELL) | SL 0.20% |
| 8 | TARGET_HIT (legacy) | `ltp ≥ target` AND `not target_passed` | **none** — but **unreachable** in practice because (2) always fires first and sets `target_passed=True` |
| 9 | EOD_CLOSE | IST ≥ 15:25 | SL 0.20% |
| 10 | FLAT_TIMEOUT | `elapsed ≥ 120 min` AND `|ltp - entry| < 0.3×ATR` (intraday only) | SL 0.20% |

### The breakeven-SL death trap (the single biggest bug)

Given entry E, ATR A, target = E + 1.25A, intraday:

1. Trade travels to `E + 1.0A` MFE.
2. Breakeven mover fires (line 319–343) → SL becomes `E + 0.1A`, `sl_moved=True`.
3. Trailing kicks in at 1.5A (line 491–514): `sl = best - 1.5A` = `E + 1.0A - 1.5A` = **E − 0.5A** — still wider than 0.1A, but the trail ratchets: as `best` climbs, SL follows at `best − 1.5A`.
4. Target = `E + 1.25A`. To hit target you need `best = E + 1.25A`, at which point trail is `E + 1.25A − 1.5A` = `E − 0.25A`, and any pullback deeper than `1.5A` from best triggers trail-stop exit.
5. **With target at 1.25A and trail at 1.5A, the trail is wider than the target move** — but the asymmetry is that the trail tracks **best**, not **entry**; once above breakeven, the trail stops you out only if price falls 1.5A from its high. Price has to push 1.25A (to target) without ever retracing more than 1.5A. In choppy/post-Batch-6 conditions, retracements of 0.5–1.0A between the breakeven trigger and target are normal; the trailing stop is re-armed each tick, and **a 0.5A pullback from any local high before target can already put the stop below entry + 0.1A** (the original breakeven level).

The "0 targets hit / 27 FLAT_TIMEOUT / 19 SL_HIT" pattern is the fingerprint: winners kiss `+1R`, drift sideways, and exit at FLAT_TIMEOUT (price back near entry) or SL (trail taken out on pullback). Losers go straight to SL. Neither path sees target.

### FLAT_TIMEOUT bug
- `_FLAT_TIMEOUT_SEC = 120 * 60` hardcoded (line 256).
- Guard only checks `|ltp - entry| < 0.3*atr`. Does NOT check whether breakeven-SL has fired. A position that went to +1.0A, triggered breakeven, drifted back to entry ± 0.3A gets exited as "FLAT_TIMEOUT" indistinguishably from a position that never moved. Post-mortem impossible without MFE/MAE.

### Other traps
- **Regime-tighten is one-shot** (`regime_tightened` flag) but uses the CURRENT regime each tick; a single-bar CHOP flap during a genuine TREND_UP day will permanently tighten the SL of every position entered in TREND_UP.
- **Swing paper has no SL path**: the GTT is live-only (see §9). If ws_monitor crashes mid-day, paper swing positions lose every defence until `position_refresh` repopulates 15 s after restart — and even then the SL is only re-read, not re-enforced server-side.
- **EOD_CLOSE at 15:25, market closes 15:30**: exits via `place_exit_order` with 0.20% SL slippage — in the last 5 min with thin depth, actual adverse selection is almost certainly worse than 0.20% for mid-caps.

## 11. Portfolio, Risk, Kill-switches

**Files:** `settings.py` (`StrategySettings`), `domain/risk.py`, `services/trading_service.py`

### Hardcoded caps

| Knob | Value | Where enforced |
|---|---|---|
| `capital` | ₹50,000 | `risk.py` (capital exposure cap) |
| `risk_per_trade` | ₹125 | `risk.py:109` |
| `max_daily_loss` | ₹300 (soft) | `trading_service.py:442` — continues but restricts to MR/VWAP_REV |
| `daily_profit_target` | ₹375 (hard halt) | `trading_service.py:454` |
| `max_trades_day` | 5 | `trading_service.py:475` |
| `max_positions` (intraday) | 3 base × regime mult | `trading_service.py:401–414` |
| `swing_max_positions` | 5 | `trading_service.py:955` |
| `reentry_cooldown_minutes` | 30 | `trading_service.py:546` |
| `swing_max_hold_days` | 10 | `swing_reconciliation_service.py:121` |
| `swing_risk_per_trade` | ₹200 | `risk.py:21` |
| `atr_sl_mult` (intraday) | 1.5 | `settings.py:53` → `risk.py:103` |
| `swing_atr_sl_mult` | 2.5 | `settings.py:74` → `risk.py:16` |
| `rr_intraday` | 1.25 | `settings.py:63` |
| `rr_intraday_reversion` | 2.0 | `settings.py:64` |
| `swing_rr` | 2.0 | `settings.py:75` |
| `paper_entry_slippage_pct` | 0.0010 (0.10%) | `order_service.py:483–488` |
| `paper_sl_slippage_pct` | 0.0020 (0.20%) | `order_service.py:668–675` |

### Position sizing traps (`risk.py`)

- Intraday sizing: `sl_dist = max(atr × 1.5, entry × 0.005)`; `qty = min(risk_per_trade / sl_dist, capital × 0.15 / entry)`. If `qty < 1 AND sl_dist > risk_per_trade × 1.5` (> ₹187.50) → `qty = 0` skip.
- Swing sizing: `sl_dist = max(atr × 2.5, entry × 0.01)`; cap `capital × 0.20 / entry`; if `qty < 1 AND sl_dist > ₹300` → `qty = 0` skip.

**Consequence:** on high-ATR days (the days most likely to produce clean trends), the sizing cap skips the best setups. We're filtering out the fat-tail winners by construction.

### No real kill-switch
- "Kill-switch" is `max_daily_loss` + `max_trades_day`, both of which silently vanish on Firestore failure (§8).
- No server-level "stop-all" flag in Firestore that halts `/jobs/scan-once` on manual admin action.
- No anomaly monitor (e.g., `open_positions_count > max_positions` inconsistency detection).

## 12. Persistence & Observability

**Firestore collections (12 known)**
- `positions/*` — open+closed; schema at `order_service.py:152–182`.
- `orders/*` — placement audit log.
- `watchlist/*` + `watchlist_history/*` — current watchlist snapshots.
- `universe`, `universe_intraday` — daily/intraday indicators.
- `market_brain/current` + `market_brain/history` — regime state.
- `scan_decisions/*` — per-scan per-symbol accept/reject reason.
- `config/earnings_blackout` — NSE results calendar.
- `state/*` — locks, fired-today flags, pending_orders.
- `pending_orders/*` — live order recon queue.
- `idempotency/*` — day-scoped firing keys.
- `audit_log/*` — admin actions.
- `ws_monitor_heartbeat` (presence-only doc, refreshed by ws_monitor run loop).

**BigQuery tables (8 known)**
- `trades` — closed trade P&L (exit_reason, pnl, partial_pnl, net_pnl, strategy, regime, risk_mode, score).
- `signals` — scan-emitted candidates (pre-entry).
- `scan_decisions` — per-symbol accept/reject, but no score-component breakdown.
- `market_brain_history` — regime snapshots.
- `breadth_history`, `leadership_history` — breadth/leadership time-series.
- `orders` — order lifecycle.
- `watchlist_history` — daily snapshots (append-only).

**Position schema (`order_service.py:152–182`) — missing fields**
- `max_favorable_excursion` — absent.
- `max_adverse_excursion` — absent.
- `breakeven_sl_fired` + `breakeven_sl_trigger_mfe` — absent.
- `regime_tightened` — boolean only, no "triggered at what regime/what price".
- `setup_components` (trend/momentum/vol/affinity breakdown) — absent.

**No backtest harness** — `reference_enhancement_guide.md` mentions one but nothing in `src/` creates one (confirmed by file inventory).

**No daily_metrics aggregate table** — dashboard queries re-aggregate from `trades` each load; correlations (e.g., "days with breakeven-SL firing rate > 80%") are not materialised.

**Log_sink** (`services/log_sink.py`) — buffers action/decision events and flushes to BQ. Decision events include regime + strategy, but **not** the raw score vector or the exact gate that tripped on rejects (only the last gate shows up in `scan_decisions.reason`).

## 13. Critical Findings — Why Every Trade Loses

### F1 — Payoff is structurally negative
- Target 1.25R, mandatory breakeven-SL touch at +1.0R, trail 1.5R, slippage 0.30% rt + brokerage 0.05% rt.
- Required breakeven win-rate ≈ 72%; realised 0%.
- **Fix:** target ≥ 2R unless strategy is reversion (then 1.5R minimum); breakeven only at +1.0R **AND** only if >N seconds have elapsed (no mid-bar whipsaw trigger); trail wider than target until target is struck.

### F2 — FLAT_TIMEOUT is a blended scythe
- No MFE guard, no regime-adaptive duration, hardcoded to 120 min.
- Winners that pulled back after +1R touch die alongside genuine flats; both report "FLAT_TIMEOUT".
- **Fix:** capture MFE; branch exit reason (FLAT_TIMEOUT_CLEAN vs FLAT_TIMEOUT_POST_BE); make timeout a function of ATR-normalised intrabar MFE.

### F3 — Silent-except on every risk cap
- Firestore 2-s latency = no PnL cap + no trade cap + no cooldown for the duration of the blip.
- **Fix:** fail-closed — if any cap-reading fails, skip the scan entirely; surface as an alert; require explicit admin override to continue.

### F4 — Policy is pass-through
- `regimeIntraday=CHOPPY` is not combined with `brain.regime` to hard-block BREAKOUT/MOMENTUM in a RANGE+CHOPPY day.
- **Fix:** make policy authoritative; `allowed_strategies = f(regimeDaily, regimeIntraday, breadth, event_state)`; delete brain→policy pass-through.

### F5 — Swing book is zero-contribution
- Batch 6 gate (strength ≥ 50 + supertrend agreement) killed swing entries entirely.
- Swing is the highest-RR, lowest-slippage channel we own; a 0-entry swing book means we only trade the worst payoff structure we have.
- **Fix:** recalibrate swing gate; introduce a second swing flavour ("core position", 15–25 day horizon, broader-universe) alongside the tight Batch-6 flavour.

### F6 — Paper swing has no SL
- `_place_gtt_sl` is live-only; paper path skips it entirely.
- **Fix:** implement a software-side "paper GTT" — a persistent Firestore doc with `trigger_price`, polled by ws_monitor or a periodic cron even if ws_monitor is down.

### F7 — Exit-price fallback to entry hides loss
- `..._NO_FILL_PRICE` rows write `realised_pnl = 0` to BQ.
- **Fix:** on fetch failure, retry 3× with 500 ms backoff; if still null, use last ws tick (ws_monitor has it); never fall back to entry; surface as alert.

### F8 — GTT placement unverified on live swing
- `place_entry_order` doesn't check `_place_gtt_sl` return value.
- **Fix:** assert GTT placed; if fail, immediately submit synthetic "cancel-then-market-exit" fallback and alert.

### F9 — No MFE/MAE anywhere
- Can't prove or disprove any of the above hypotheses from live data.
- **Fix:** add `max_favorable_excursion`, `max_adverse_excursion`, `be_trigger_price`, `be_trigger_mfe`, `trail_peak`, `partial_exit_count` to position schema; backfill from BQ trades + candle data for historical trades.

### F10 — Idempotency race
- `mark_fired_today` written after `_save_position_firestore`; crash between order placement and save → next scan re-fires.
- **Fix:** write `fired_today` **before** place (with `pending`), `confirmed` after save, `expired` after 60 s if no confirmation.

### F11 — Scoring black box
- Component scores (trend/momentum/vol/affinity) not persisted.
- **Fix:** add `score_components` JSON column to `signals` + `scan_decisions`; dashboard chart "which component drove the decision".

### F12 — Double brokerage on partials
- `calc_round_trip_brokerage` called per partial leg without amortising the entry cost.
- **Fix:** amortise entry brokerage across all exit legs proportionally; document the model in `docs/redesign/MODELS.md`.

## 14. Unexploited Upstox API Surface

Ranked by P&L impact (full audit table — see companion agent report):

| # | Capability | Endpoint | Why |
|---|---|---|---|
| 1 | **Put/Call option chain + PCR/OI** | `GET /v2/option/chain?instrument_key=<idx>&expiry_date=<yyyy-mm-dd>` | Institutional flow lives in options before equities; a 1/min poll on NIFTY+BANKNIFTY ATM gives us a 30–60 s leading signal for trend confirmation/fade |
| 2 | **Option greeks batched** | `GET /v3/market-quote/option-greek?instrument_key=<csv≤50>` | Delta/gamma-based index regime signal; gamma-flip = volatility regime change |
| 3 | **WS `option_greeks` mode** | same feed, different mode | Streaming IV spikes on single stocks — early warning for event-driven moves |
| 4 | **Portfolio stream WS** | `wss://.../v2/feed/portfolio-stream-feed?update_types=order,position,holding,gtt_order` | Removes 5–30 s polling lag in order-fill reconcile |
| 5 | **News API** | `GET /v2/news?category=holdings` | Launched 17-Apr-2026; auto-filters to held names; wire a 60-s "news veto" per ISIN blocking MOMENTUM/BREAKOUT |
| 6 | **v3 historical 1-min since Jan 2022** | `GET /v3/historical-candle/{ik}/minutes/1/{to}/{from}` | Unlocks true backtesting (3+ yr 1-min) — the prerequisite for any adaptive-weight retraining |
| 7 | **Sandbox order path** | `POST /v3/order/place` with sandbox token | Replace home-grown fake-fill logic for the order adapter bug class; keep our own simulator for strategy testing |
| 8 | **Extended (1-year) read-only token** | 5 endpoints (positions/holdings/orders/history/book) | Eliminates daily token-refresh-before-open failure class for the read side |
| 9 | **Funds v3 (nested pledge split)** | `GET /v3/...funds-and-margin` | Reveals pledged-margin buying power — roughly 15–25% extra capital we leave idle |
| 10 | **Brokerage pre-check** | `GET /v2/charges/brokerage` | Per-trade exact cost; reject when expected edge < fees |
| 11 | **GTT OCO for swing** | `/v2/order/gtt/place` with OCO | Server-side SL+target for positional; survives Cloud Run restarts |
| 12 | **Market Holidays / Timings** | `/v2/market/holidays`, `/v2/market/timings/{date}` | Replace hardcoded lists; handle half-day sessions automatically |
| 13 | **Multi-leg order** | `POST /v2/order/multi/place` | Atomic 2–4 leg options spreads |

See Upstox-audit companion in agent transcript for full endpoint inventory with rate limits and WS frame specs.

## 15. Dead Code / Drift / Hidden Risk

- **`_append_order_log_sheets`, `_append_position_sheets`** (`order_service.py:68–72`) — no-op stubs still called at lines 465/500/566/930. Harmless; remove.
- **`market_brain_service` substructure** — `sub_regime_v2`, `participation`, `event_state`, `structure_state` all computed per call, persisted, but **never read by the policy or scoring layers**. Cost = ~40% of brain-build time.
- **`earnings_calendar_service._nse_get`** — cookie bootstrap fallback silently returns empty on rate-limit. Refresh quietly no-ops; last_updated unchanged but "symbols" map empty → no blackout in force.
- **`universe_service` dual-source** — Firestore primary / Sheets fallback (`trading_service.py:130 _read_watchlist_with_fallback`) — Sheets can be 12 h stale; no timestamp guard.
- **Batch-history in commit log** — Batch 1 through 7 present as commits; no consolidated "batch changelog" that maps `batch_id → code paths touched → observed effect`. Review/revert becomes archaeology.
- **Dashboard** — read-only, per the deployment; but queries `trades` with no `exit_price` validation → silently renders `₹0` P&L rows for `NO_FILL_PRICE` exits. Operators see "flat day" when we actually lost.
- **Three EOD-recon crons (15:25/27/29)** — defensive redundancy; indicates history of missed EODs; not a root-cause fix.
- **No `test_market_brain_v2.py` coverage** (pre-existing GrowwSettings collection error) — noted during test-universe fix; brain changes ship untested.

---

## Appendix A — Data lineage (signal → trade)

```
Cloud Scheduler cron
    → POST /jobs/scan-once  (web/api.py)
    → TradingService.run_scan_once()
        → read watchlist (Firestore or Sheets fallback)
        → build brain (market_brain_service)
        → for symbol in watchlist:
            → 20+ gates
            → calc_position_size
            → OrderService.place_entry_order()
                → paper: apply entry slippage → save position (Firestore)
                → live: Upstox POST /v2/order/place → poll 25s → save or pending
            → log to signals, scan_decisions (BQ buffered via log_sink)
```

Exit:

```
ws_monitor loop
    → Upstox WS (full mode)
    → tick → _on_quote()
        → update best_price / atr
        → breakeven/partial/regime-tighten/trail/SL/target/EOD/timeout chain
        → _do_exit() → OrderService.place_exit_order()
            → paper: apply SL slippage → mark position CLOSED
            → live: Upstox POST /v2/order/cancel + POST /v2/order/place MARKET
        → persist exit to Firestore; log to trades (BQ)
```

## Appendix B — File index (top files cited)

- `settings.py` — all tunables.
- `services/trading_service.py` — entry orchestrator, 10-stage funnel.
- `services/order_service.py` — entry/exit placement, slippage, paper/live fork.
- `services/ws_monitor_service.py` — tick handler, exit precedence, FLAT_TIMEOUT.
- `services/swing_reconciliation_service.py` — daily swing eval.
- `services/earnings_calendar_service.py` — blackout.
- `services/market_brain_service.py` + `services/market_policy_service.py` + `services/regime_service.py` — regime stack.
- `services/universe_service.py` + `services/universe_v2.py` — universe + intraday refresh.
- `domain/scoring.py` + `domain/indicators.py` + `domain/risk.py` + `domain/regime_affinity.py` — pure logic.
- `adapters/upstox_client.py` + `adapters/upstox_ws_client.py` — broker adapter.
- `adapters/firestore_state.py` + `adapters/bigquery_client.py` — state stores.
- `web/api.py` — FastAPI routes.

---

**End of AUDIT.md.** Redesign → `DESIGN.md`.
