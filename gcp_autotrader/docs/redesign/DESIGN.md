# Redesign — From Loss Machine to Profit Machine

**Branch:** `redesign/audit-and-design`
**Companion:** `AUDIT.md` (grounded in current code, file:line citations)
**Author:** Autonomous redesign pass, 2026-04-23
**Scope:** End-to-end target architecture + migration plan + milestone breakdown.

> Design principle: every decision in the system must be **attributable**, **invertible**, and **testable**. The current system fails all three (see AUDIT §13). The redesign is organised around that triad.

---

## 0. Design principles (non-negotiable)

1. **Profit-capture, not loss-prevention.** Optimise payoff structure first, limit losses second. Current system does the opposite.
2. **Every decision has a cause that lands in BigQuery.** `score_components`, `gate_reason`, `regime_snapshot`, `MFE`, `MAE`, `be_trigger_mfe` — all persisted. Nothing is ephemeral that affects P&L.
3. **Fail-closed on missing data.** Any risk-cap read failure halts the scan. No silent-continue.
4. **Payoff asymmetry is a first-class property.** A setup is described by its `(hit_rate, avg_win_R, avg_loss_R)` triple + a regime-conditional prior, not by a single "score".
5. **Regime drives the entire decision space**, not just the score threshold. Each (regime × time-of-day × volatility-state) combination maps to **one playbook** — a curated set of setups with pre-tuned payoffs.
6. **Edge-before-entry budget.** Every candidate must have `expected_edge(R) > 2 × expected_cost(R)` after real brokerage + slippage + adverse-selection. If not, skip.
7. **Simulation first, live second.** Every change (parameter, setup, exit rule) ships through backtest → paper-trade → live, never straight to live.
8. **Stateful, auditable ws_monitor.** Every SL mutation, every target hit, every MFE peak carries a reason and a timestamp.

---

## 1. Target architecture — abstractions

The redesign introduces five first-class concepts. The current system mashes all of these into `trading_service.py` + `ws_monitor_service.py`.

### 1.1 `Edge` — a testable trading idea

```
Edge = {
  edge_id: "breakout_range_compression_v1",
  setup_fn: f(bars, indicators, regime) -> Optional[Signal],
  entry_rules: [...],
  exit_rules: [...],      # target / stop / time / regime-flip
  size_rule: (side, entry, atr, regime) -> qty,
  prior: {hit_rate, avg_win_R, avg_loss_R, sample_n},  # from backtest
  allowed_regimes: [TREND_UP, RECOVERY, RANGE_HIGH_ATR],
  allowed_times: [(09:20, 10:15), (13:30, 14:45)],
  cost_budget_R: 0.20,   # max acceptable brokerage+slippage
}
```

Every Edge owns its test suite. Edges without a passing backtest prior cannot be enabled in live. Current "strategies" (BREAKOUT/MOMENTUM/etc) become the first generation of Edges, each with its own prior measured on 3 yr of 1-min candles (enabled by v3 history since Jan 2022).

### 1.2 `Thesis` — why we think an Edge will pay today

```
Thesis = {
  regime_daily: TREND_UP,
  regime_intraday: TRENDY,
  breadth_snapshot: {...},
  leadership_snapshot: {...},
  event_state: CLEAN | PRE_FOMC | POST_RESULT | EARNINGS_BLACKOUT,
  options_state: {pcr, atm_iv, gamma_state},
  news_flags: {veto: bool, until_epoch: int},
  derived_at: <ts>,
}
```

A Thesis is built once per minute in the brain service and cached. Edges consult the Thesis (not raw primitives) to decide if they are eligible.

### 1.3 `Playbook` — the map (regime × volatility × time) → Edges

```
Playbook[TREND_UP × TRENDY × 09:20–10:15]
  = [breakout_range_compression_v1, opening_drive_continuation_v1, intraday_pullback_v1]
Playbook[RANGE × CHOPPY × 11:00–14:00]
  = [mean_reversion_bb_v1, vwap_reversal_v2]
Playbook[PANIC × any × any]
  = []  # explicitly no trades
Playbook[TREND_DOWN × TRENDY × 09:20–10:15]
  = [breakdown_continuation_v1, short_pullback_v1]
```

Rendered as a Firestore-backed config doc, versioned, auditable. **Removes the `allowed_strategies` pass-through** of the current brain→policy→scanner chain.

### 1.4 `PortfolioBook` — the capital allocator

Maintains per-channel (Intraday/Swing/Positional/Hedge) budgets:

```
PortfolioBook = {
  total_capital: ₹1,00,000,
  channels: {
    intraday: {budget: 40%, used: 0, max_concurrent: 3, daily_risk_cap_R: 3},
    swing:    {budget: 40%, used: 0, max_concurrent: 5, daily_risk_cap_R: 2},
    position: {budget: 15%, used: 0, max_concurrent: 3, weekly_risk_cap_R: 2},
    hedge:    {budget: 5%,  used: 0, max_concurrent: 2},  # options
  },
  global_kill_switch: false,
  global_daily_dd_cap_R: 3,  # hard stop
}
```

**Each new position checks its channel budget before placement.** The current system only knows "max_positions" — it cannot say "we are over-allocated to intraday and under-allocated to swing."

### 1.5 `AttributionLog` — the post-mortem substrate

Every position carries a full lifecycle record:

```
AttributionLog(position_id) = {
  entry: {edge_id, thesis_snapshot, score_components, expected_edge_R, expected_cost_R},
  size: {qty, sl_dist, capital_used, channel_budget_remaining_pct},
  path: [
    {ts, event: MFE_PEAK, price, mfe_R},
    {ts, event: BE_TRIGGER_FIRED, price, be_sl_new, mfe_at_fire},
    {ts, event: TRAIL_UPDATE, new_sl, best_price},
    {ts, event: PARTIAL_EXIT_1, price, qty, partial_pnl_R},
    ...
  ],
  exit: {reason, price, pnl_R, pnl_inr, slippage_R, brokerage_R},
  attribution: {edge_bucket_R, regime_bucket_R, cost_bucket_R, luck_bucket_R},
}
```

Stored compressed in Firestore + mirrored to BigQuery. Enables "which edges are carrying the book", "which regimes are loss-makers", "how much does breakeven-SL actually cost us" — questions we cannot currently answer.

---

## 2. The target system — components

```
                               ┌───────────────┐
                               │ Cloud Scheduler│
                               └──────┬────────┘
                                      │ cron
             ┌────────────────────────▼────────────────────────────┐
             │           autotrader (FastAPI, Cloud Run)            │
             │  /jobs/scan-once   /jobs/thesis-refresh              │
             │  /jobs/universe    /jobs/watchlist                   │
             │  /jobs/option-chain /jobs/news-pull /jobs/eod-roll   │
             └─┬───────────────┬───────────────┬──────────────────┬┘
               │               │               │                   │
               ▼               ▼               ▼                   ▼
     ┌───────────────┐ ┌───────────────┐ ┌───────────────┐ ┌──────────────┐
     │ Universe +    │ │ Thesis builder│ │ PortfolioBook │ │ Playbook     │
     │ indicators    │ │ (regime +     │ │ (allocation)  │ │ resolver     │
     │ (daily + intr)│ │  breadth +    │ │               │ │              │
     │               │ │  leadership + │ │               │ │              │
     │               │ │  options +    │ │               │ │              │
     │               │ │  news)        │ │               │ │              │
     └──────┬────────┘ └──────┬────────┘ └───────┬───────┘ └──────┬───────┘
            │                 │                  │                │
            └──────┬──────────┴────────┬─────────┘                │
                   │                   │                          │
                   ▼                   ▼                          ▼
           ┌────────────────┐  ┌─────────────────┐       ┌────────────────┐
           │ Edge registry  │  │ Candidate       │       │ Gate chain     │
           │ (setups, prior)│→ │ generator       │  →    │ (hard blocks,  │
           │                │  │ (per symbol)    │       │  cost filter)  │
           └────────────────┘  └─────────────────┘       └───────┬────────┘
                                                                  │
                                                                  ▼
                                                         ┌────────────────┐
                                                         │ Sizer          │
                                                         │ (channel-aware)│
                                                         └───────┬────────┘
                                                                 │
                                                                 ▼
                                                         ┌────────────────┐
                                                         │ OrderService   │
                                                         │ (paper/live)   │
                                                         └───────┬────────┘
                                                                 │
  ┌──────────────────────────────────────────────────────────────┤
  │                                                              │
  ▼                                                              ▼
┌────────────────────┐                              ┌────────────────────┐
│ autotrader-ws      │                              │ Firestore / BQ /   │
│ (Cloud Run, min=1) │                              │ GCS                │
│  - WS full + opts  │                              │  - AttributionLog  │
│  - Tick handler    │                              │  - trades/scan_dec │
│  - Exit state mach.│                              │  - daily_metrics   │
│  - Paper GTT       │                              │  - backtest_runs   │
└─────────┬──────────┘                              └────────────────────┘
          │ upstox portfolio-stream
          ▼
     (Reconcile)
```

### New components (additions)

- **Thesis builder service** — consolidates regime, breadth, leadership, option-chain (PCR, ATM IV, gamma), news veto, event state. Cached 60 s.
- **Edge registry** — each Edge is a module with `setup_fn`, `entry_rules`, `exit_rules`, `prior`, unit tests. Loaded at process start.
- **Playbook resolver** — maps (thesis) → active edges × time-slot filters.
- **PortfolioBook** — channel-aware allocator.
- **Candidate generator** — per-symbol runs all eligible Edges in parallel, emits `(symbol, edge, signal, expected_edge_R, expected_cost_R)`.
- **Gate chain** — thin, fail-closed: `kill_switch_off → channel_has_budget → fresh_data → cost_passes → idempotency`.
- **Sizer (channel-aware)** — knows which channel the candidate belongs to, reads PortfolioBook.
- **News pull service** — 60 s poll of `GET /v2/news?category=holdings`, writes `news_veto` Firestore doc.
- **Option chain service** — 60 s poll of `/v2/option/chain` for NIFTY/BANKNIFTY + top F&O stocks; derives PCR, ATM IV, gamma indicator.
- **Paper-GTT service** — Firestore-backed synthetic GTT for paper swing/positional; polled by ws_monitor every 5 s; survives ws_monitor restart.
- **Daily metrics rollup** — nightly BQ job aggregates AttributionLog by (edge × regime × channel × cost bucket); dashboard reads this, not raw trades.
- **Backtest harness** — offline process; v3 1-min history Jan 2022+; every Edge runs nightly for prior update.

### Reused components (refactored)

- `universe_service`, `universe_v2` — unchanged surface, stricter stale-key normalisation already shipped (commit `c4bb62e`).
- `regime_service`, `market_brain_service` — refactored as upstream providers for the Thesis builder; **no longer own `allowed_strategies`**.
- `upstox_client`, `upstox_ws_client` — add `option_greeks` WS mode, `portfolio-stream-feed` subscription.
- `order_service` — retained, heavily hardened; see §4.
- `ws_monitor_service` — rewritten exit state machine; see §5.

---

## 3. Payoff architecture — the heart of the fix

The current system loses because its payoff structure is impossible. The redesign mandates that no Edge can exist unless its payoff structure is mathematically positive.

### 3.1 The payoff contract

For any Edge to be live, it must satisfy:

```
EV_R = p_win × avg_win_R − (1 − p_win) × avg_loss_R − cost_R > 0.15
```

where:
- `p_win` comes from 3 yr backtest + walk-forward out-of-sample ≥ 12 months
- `avg_win_R` and `avg_loss_R` are both net of slippage + brokerage
- `cost_R` is the Upstox `/v2/charges/brokerage` + modelled slippage (per-symbol liquidity tier)
- `0.15R` is a safety margin against prior decay

**Consequence:** the current 1.25R target × 1.0R breakeven × 1.5R trail × 72% breakeven win-rate × 0% actual win-rate fails this contract and cannot exist.

### 3.2 The four exit regimes (replaces current 10-rank precedence)

| Regime | When | Stop | Target | Time cap |
|---|---|---|---|---|
| **Initial** | From entry to MFE < 0.8R | Fixed at entry − 1R (no moves) | 2.5R–3R | 60 min hardcap (no cost-creep) |
| **Confirmed** | MFE ≥ 0.8R | Move stop to entry − 0.3R (partial loss protection) | 2.5R–3R | 90 min |
| **Runner** | MFE ≥ 1.5R | Trail at 2× ATR from best price | 3R–4R stretch | 120 min |
| **Terminal** | Target hit OR ATR-normalised flatline ≥ N min | Hit | Hit | — |

**Critical shift:** breakeven-SL is *not* automatic at +1R. It activates only after MFE ≥ 0.8R **AND** at least 15 seconds have elapsed since MFE was set (no wick triggers). Even then, SL moves to `entry − 0.3R`, not to entry. The "trail tighter than target" trap is eliminated.

### 3.3 Target ≥ 2R baseline

- Intraday momentum/breakout: target = 2.5R (vs current 1.25R)
- Intraday mean-reversion: target = 1.8R, stop 1R
- Swing: target = 2.5–4R depending on ATR regime, stop 1R
- Positional: target = 3–5R, stop 1R

Targets are **runner-style**: partial at 1R, partial at 2R, last leg rides trail. This matches real retail-retail-flow dynamics in Indian equities (post-9:30 momentum trades historically run 2.5–3.5R in TRENDY/TREND_UP conditions).

### 3.4 FLAT_TIMEOUT redesign

- Time cap depends on Edge's own `expected_time_to_1R` (from backtest): default 60 min if MFE < 0.5R; 90 min if MFE ≥ 0.5R but < 1R; 120 min if MFE ≥ 1R. **Different exit reasons per bucket** — `FLAT_NO_EDGE`, `FLAT_STALL_PRE_1R`, `FLAT_POST_1R`. No more blending.

---

## 4. Order placement redesign (order_service)

- **Idempotency written before place, confirmed after save.** Three-state token: `pending → confirmed | expired(60s)`.
- **GTT assertion** — `place_entry_order` awaits `_place_gtt_sl` return; on failure, queue a synthetic software GTT in Firestore AND raise an alert.
- **Exit quote fallback chain** — Upstox quote (3× retry, 500 ms backoff) → last ws tick → abort with alert. Never `exit_price = entry_price`.
- **Paper GTT** — synthetic Firestore doc polled by ws_monitor *and* a 1-min cron; covers paper swing and paper positional.
- **Brokerage pre-check** — `/v2/charges/brokerage` called during sizing; rejects when `cost_R > 0.25`.
- **Slippage model per liquidity tier** — symbols bucketed Tier A/B/C by rolling 20-day turnover; slippage expectations tied to tier, not a single 0.10/0.20 constant.

---

## 5. ws_monitor redesign — the exit state machine

The current tick handler is a 10-rank precedence switch. The redesign is an **explicit state machine** per position, with every transition logged to AttributionLog.

### 5.1 States

```
INITIAL → CONFIRMED → RUNNER → TERMINAL
       ↘︎              ↘︎
         LOSING ────────→ STOPPED
```

- `INITIAL` — from entry to `MFE < 0.8R`
- `CONFIRMED` — `MFE ≥ 0.8R`, SL advanced to `entry − 0.3R`
- `RUNNER` — `MFE ≥ 1.5R`, partial taken at 2R, trail at 2×ATR from best
- `LOSING` — `ltp < entry − 0.5R` and MFE never hit 0.5R
- `TERMINAL` / `STOPPED` — exit

Each transition: `ts, state_from, state_to, price, mfe_R, reason, new_sl, new_target` logged.

### 5.2 Regime-change tighten

- Only applies in `CONFIRMED` or `RUNNER` (never `INITIAL`).
- Requires **2 consecutive** brain-refresh cycles in adverse regime (no 1-bar flaps).
- Tightens SL to `ltp − 0.5R`, not `ltp − 0.8×ATR` — always in R units.

### 5.3 Swing / positional on ws_monitor

- Swing no longer relies on paper-only "no GTT" path. Paper swings carry a synthetic GTT; ws_monitor polls it every 5 s regardless of WS tick.
- Swing exit regimes mirror above, scaled: `0.8R`/`1.5R` thresholds become `1.0R`/`2.0R`.

### 5.4 EOD handling

- Intraday: force-exit at 15:20 (earlier than current 15:25) with limit-through-bid/ask, not market, and a 15:24 fallback to market. Captures 4 minutes of better-liquidity exit windows.
- Swing/positional: **no EOD** (carry overnight via CNC) unless explicit blackout flag set.

---

## 6. Scoring & selection redesign

### 6.1 Replace "score" with "expected_edge_R"

Every candidate is scored in R-units of expected edge **after costs**:

```
expected_edge_R(candidate) =
    edge.prior.hit_rate × edge.prior.avg_win_R
  − (1 − edge.prior.hit_rate) × edge.prior.avg_loss_R
  × thesis_multiplier(regime, breadth, options_state)
  − cost_R(symbol_tier, side)
```

This replaces the current 0–100 integer score. Candidates are selected on `expected_edge_R` descending, capped at `channel.max_concurrent`.

### 6.2 Score components persisted

Firestore + BQ columns:

```
scan_decisions.score_components = {
  edge_id, prior_hit_rate, prior_avg_win_R, prior_avg_loss_R,
  thesis_multiplier_trend, thesis_multiplier_breadth, thesis_multiplier_options,
  cost_R_brokerage, cost_R_slippage, cost_R_impact,
  expected_edge_R,
  gate_decision, gate_reason_chain   -- all 20+ gates as list, not last one only
}
```

### 6.3 Thresholds

- `min_expected_edge_R = 0.35` (below this, cost eats the edge).
- Regime-mode modifiers: +0.10 in PANIC, +0.05 in CHOP, +0 in TREND_UP/RECOVERY.

---

## 7. Risk & capital redesign

### 7.1 Channel-aware allocation (₹1L → ₹10L → ₹1Cr path)

| Capital tier | Intraday % | Swing % | Positional % | Hedge % |
|---|---|---|---|---|
| ₹1L | 40% | 40% | 15% | 5% |
| ₹5L | 35% | 40% | 20% | 5% |
| ₹10L+ | 30% | 35% | 25% | 10% |
| ₹50L+ | 25% | 30% | 30% | 15% |

Budget per trade:
- Intraday: `0.5R = 0.5% of intraday budget`
- Swing: `0.75R = 0.75% of swing budget`
- Positional: `1R = 1% of positional budget`

### 7.2 Daily / weekly / monthly DD governors

- Daily DD > 3R (intraday channel) → intraday channel closed for the day.
- Daily DD > 2R (swing channel) → swing entries paused, existing positions unaffected.
- 3 consecutive losing days → reduce all risk-per-trade by 50% until 2 consecutive winning days.
- Monthly DD > 10R aggregate → mandatory review + hard halt until admin override.

### 7.3 Kill-switch primitive

- Firestore `control/kill_switch` doc. Scanner reads first; if true, skip entire scan. Dashboard has a button. Alerts on flip.
- **Fail-closed**: inability to read `kill_switch` → treat as active.

---

## 8. Observability redesign

### 8.1 AttributionLog (new, first-class)

Every position writes one `AttributionLog` row. BQ table `attribution_log`:

```
position_id, symbol, side, edge_id, channel,
entry_ts, entry_price, sl_price, target_price, qty,
thesis_json, score_components_json,
path_events_json,         -- MFE peaks, BE triggers, trails, partials
exit_ts, exit_price, exit_reason, pnl_R, pnl_inr,
slippage_R, brokerage_R,
edge_bucket_R, regime_bucket_R, cost_bucket_R, luck_bucket_R
```

### 8.2 Daily rollup (`daily_metrics`)

Scheduled job at 16:00 IST aggregates `attribution_log` into:

```
date, channel, edge_id,
trades, hit_rate, avg_win_R, avg_loss_R, total_R, gross_inr, net_inr,
cost_total_R, cost_pct_of_gross,
regime_breakdown_json,
MFE_distribution_json,
BE_trigger_rate, BE_to_stopout_rate   -- catches the breakeven-SL bug
```

Dashboard reads `daily_metrics`, never `trades` directly.

### 8.3 Dead-code removal

Removed in redesign: `_append_order_log_sheets`, `_append_position_sheets`, `sub_regime_v2`, `participation`, `event_state` inside brain (moved to Thesis), `structure_state`.

### 8.4 Alerts

- Firestore latency > 2 s → page (currently invisible).
- `NO_FILL_PRICE` exit → page.
- GTT placement failure → page.
- Consecutive 3 SL_HITs in one scan → page.
- Daily DD breach → page.
- Kill-switch flip → page.

---

## 9. Upstox API expansion

Wire each of these into a Firestore-backed cache layer; every Edge/Thesis reads from cache, never directly.

| Priority | Capability | Integration point | Effort |
|---|---|---|---|
| P0 | `option_greeks` WS mode | `upstox_ws_client` | 2d |
| P0 | Option chain poll (NIFTY + BANKNIFTY + top F&O) | new service `services/option_chain_service.py` | 2d |
| P0 | News API poll | new service `services/news_service.py`, writes `news_veto` | 1d |
| P0 | Portfolio stream WS | new service `services/portfolio_stream_service.py`, replaces polling | 3d |
| P1 | v3 historical 1-min since Jan 2022 | backtest harness | 1w |
| P1 | Extended (1y) read-only token | config change + secret | 0.5d |
| P1 | Sandbox order path for integration tests | `tests/integration/` | 2d |
| P1 | GTT OCO for live swing/positional | `order_service` | 2d |
| P1 | Brokerage pre-check | `order_service` sizing | 1d |
| P2 | Funds v3 pledged-margin split | `adapters/upstox_client.funds()` | 1d |
| P2 | Market holidays + timings | `universe_service` calendar source | 0.5d |
| P2 | Multi-leg order for hedge channel | `order_service.place_multi_leg` | 3d |

---

## 10. Testing & validation

1. **Unit tests per Edge** — entry rules, exit rules, sizer, cost model. Each Edge ships with ≥10 test cases per regime.
2. **Backtest harness** — v3 1-min + daily history Jan 2022 onward. Runs nightly. Updates priors in Firestore. Prior staleness > 14 days → Edge disabled.
3. **Walk-forward out-of-sample** — holdout last 6 months; Edge is only enabled live if OOS EV_R > 0.20.
4. **Replay tests** — capture 1 full day of WS ticks (Firestore doc + GCS blob); replay through ws_monitor; assert identical exit sequence. Catches state-machine regressions.
5. **Integration tests** — Upstox sandbox for order-placement path; fake WS tick stream for ws_monitor.
6. **Paper-trade phase** — ≥20 trading days of positive cumulative R before a single live trade.
7. **Canary live** — 1/10th size for 10 trading days before full size.

---

## 11. Migration plan — Edge-of-cliff approach

**Guiding rule:** no live trades until AUDIT findings F1–F10 are fully fixed AND paper shows 20 positive days.

### M0 — Safety net (branch: `redesign/m0-safety`, ≤ 3 days)
1. **Kill-switch primitive** (`control/kill_switch`) + scanner check.
2. **Fail-closed on risk-cap reads** — delete the 4 silent-continue paths; scan aborts if Firestore slow.
3. **Exit-price fallback chain** — never `exit_price = entry_price`; add alert.
4. **GTT assertion** on live swing placement.
5. **Paper GTT** — synthetic Firestore-backed SL for paper swing/positional.
6. Add **MFE/MAE capture** to position schema; backfill via BQ + candles.

Success: all positions have MFE/MAE; no `NO_FILL_PRICE` rows; kill-switch works from dashboard.

### M1 — Exit state machine rewrite (branch: `redesign/m1-exit-sm`, ~1 week)
1. Implement INITIAL/CONFIRMED/RUNNER/LOSING/TERMINAL states in ws_monitor.
2. Breakeven only at MFE ≥ 0.8R + 15 s debounce; moves SL to `entry − 0.3R` not entry.
3. Trail at 2×ATR in RUNNER only.
4. Replace FLAT_TIMEOUT with MFE-branched `FLAT_NO_EDGE` / `FLAT_STALL_PRE_1R` / `FLAT_POST_1R`.
5. Regime-tighten requires 2 consecutive bars, only in CONFIRMED/RUNNER.

Success: paper run 10 days, breakeven-SL-to-stopout rate < 20%, hit_rate_at_1R ≥ 40%.

### M2 — Thesis + Playbook + Edge registry (branch: `redesign/m2-thesis`, ~2 weeks)
1. Consolidate regime + breadth + leadership into Thesis.
2. Add option-chain + news polls; Thesis carries options_state + news_veto.
3. Edge registry; migrate the 5 current setups to Edges (BREAKOUT, PULLBACK, MR, OPEN_DRIVE, MOMENTUM).
4. Playbook resolver replaces `allowed_strategies` pass-through.
5. Gate chain refactored to 5 thin gates (kill / budget / fresh / cost / idempotency).
6. Rejection reason = full gate chain list in scan_decisions.

Success: brain refresh writes Thesis; scanner reads Playbook; `regime_intraday=CHOPPY + daily=RANGE` blocks BREAKOUT/MOMENTUM hard.

### M3 — Expected_edge_R scoring (branch: `redesign/m3-scoring`, ~1 week, parallel with M2)
1. Backtest harness offline for the 5 current Edges on 3 yr 1-min data.
2. Emit priors to Firestore `edge_priors` doc.
3. Scanner replaces integer score with `expected_edge_R`; thresholds in R.
4. Cost check (brokerage pre-call + tiered slippage) rejects sub-0.35R candidates.

Success: scan_decisions carries score_components; dashboard shows expected_edge_R histograms per Edge.

### M4 — PortfolioBook + channel allocator (branch: `redesign/m4-portfolio`, ~1 week)
1. Channel budgets in settings; scanner consults PortfolioBook.
2. Daily/weekly DD governors.
3. 3-losing-day auto-halt.

Success: intraday+swing+positional+hedge books visible; per-channel DD tracked.

### M5 — Upstox P0 expansion (branch: `redesign/m5-upstox`, ~1 week)
1. Option greeks WS mode.
2. Option chain service.
3. News service.
4. Portfolio stream (deprecate polling).

Success: Thesis has live PCR/IV/gamma; news_veto tested on an event day; reconcile lag < 1 s.

### M6 — AttributionLog + daily_metrics (branch: `redesign/m6-obs`, ~1 week)
1. AttributionLog table + per-position writer.
2. daily_metrics nightly rollup.
3. Dashboard rewrite to read daily_metrics + AttributionLog.
4. Alerting wiring.

Success: Day-1 post-mortem answerable from 2 dashboard panels.

### M7 — Paper validation (20 days, cannot be skipped)
1. Full paper run with all M0–M6 shipped.
2. Daily positive cumulative R target; weekly R ≥ 0.5; max DD ≤ 3R.
3. If any week fails, revert to review, do not advance.

### M8 — Canary live (10 days, 1/10 size)
1. Live with 10% of planned risk-per-trade.
2. Compare live vs paper priors; reject edges where deviation > 1σ.

### M9 — Full live (gradual ramp over 30 days)
1. Day 1–5: 25% size, 2 channels.
2. Day 6–15: 50% size, 3 channels.
3. Day 16–30: 100% size, all channels.

### Post-M9 — adaptive weights, options (scope for a future DESIGN v2)
- Reinforcement of Edge priors weekly via walk-forward retraining.
- Option-chain-driven edges (delta-neutral, 0DTE index).
- News-driven event edges.

---

## 12. Success criteria (profit-first)

The redesign is profitable if, after M7 paper + M8 canary:

- **Cumulative R per trading day ≥ 0.5R** (median over 30 sessions)
- **Hit rate at target ≥ 35%** (anywhere in 35–55% is healthy; > 55% means target too tight)
- **Cost-to-edge ratio ≤ 30%** (cost_R / gross_edge_R, from AttributionLog)
- **DD max ≤ 5R in any 20-day window**
- **No single Edge contributes > 50% of PnL** (diversification proof)
- **BE-to-stopout rate ≤ 20%** (vs ~80% estimated today)
- **Paper → live prior deviation ≤ 1σ** (model validity)

If these hold, ramp to full capital and add hedge/options layer.

---

## 13. Out-of-scope for this DESIGN (deferred)

- Machine-learning scoring (Edges stay rule-based for at least M0–M9).
- Direct-market-access / co-lo (Upstox API latency is bounded).
- Cross-asset (commodities, FX, bonds) — future DESIGN v2.
- Portfolio hedging with VIX / SGX NIFTY (deferred; partial Upstox coverage).
- Real-time news-sentiment model (start with veto flag, upgrade later).

---

## 14. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Edge priors decay faster than 14-day staleness cap | Walk-forward retrain weekly; auto-disable on 3 consecutive neg-R weeks per Edge |
| Upstox WS disconnects miss ticks during exit window | ws_monitor falls back to REST quote poll at 2 s cadence during disconnect |
| Firestore cost ramps with AttributionLog writes | Batch 50 writes per flush; 99th percentile cost budget: ₹200/day |
| BQ daily_metrics rollup job fails on results day | Retry 3×, alert if still failing; dashboard caches last successful snapshot |
| Paper slippage model underestimates real slippage | M8 canary compares live vs paper; recalibrate tier constants |
| Option-chain poll rate-limits in volatile sessions | Drop to 2-min cadence on rate-limit; Thesis marks `options_state=STALE` |
| Sandbox diverges from live behaviour | Track divergence metrics; treat sandbox only as interface test, never behaviour test |

---

## 15. Immediate post-merge checklist (first 24h)

1. Merge `redesign/audit-and-design` to main (docs only, no code).
2. Create M0 branch; start the 6 M0 tasks.
3. Pause all live orders (set `kill_switch = true` in prod); continue paper for baseline.
4. Backfill MFE/MAE for last 30 days of trades from BQ candles.
5. Re-run the last 7 days through the new metrics lens to produce a baseline report.

---

**End of DESIGN.md.** Implementation starts with M0 immediately after user sign-off.
