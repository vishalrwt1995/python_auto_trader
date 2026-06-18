# Project Knowledge — Auto Trading System

> **Purpose:** Single source of truth for any Claude session, started at any time.
> **Read this file first** in every new chat. It is committed to the repo and updated continuously.
>
> **Last updated:** 2026-06-18 (fresh INTRADAY re-audit → no retail edge, channel parked; swing ₹5L profit profile + improvement-scope; next = fundamental-data edge project, PARKED awaiting user signal) · **Last verified live state:** 2026-06-18 09:42 IST (rev 00257-mn6 serving, PAPER, ₹5L swing; no new swing trades 06-17/18 = regime RANGE_ROTATING gates momentum + MR at 3/3 reserve cap — by design, not a bug)
>
> **If you are a future Claude session reading this:** verify the "Production State" section against live `gcloud` output before asserting current state — drift is possible. Then read the "Recent History" log (newest at top) for context on the last few sessions of work.

---

## 0. Quick start for a new chat

> **NEW (2026-05-29):** the canonical entry point is `/Users/vishalrawat/Auto Trading Python GCP/CLAUDE.md` — read that FIRST, then this file. CLAUDE.md has the deploy hygiene rules, gcloud auth gotchas, working-style conventions, and routes to all knowledge docs. Don't skip it.
>
> Companion: `gcp_autotrader/docs/GLOSSARY.md` decodes project-specific terms (channel, tact, R-multiple, regime, etc.).

The user's preferred bootstrap prompt for any new chat:

```
Read /Users/vishalrawat/Auto Trading Python GCP/CLAUDE.md and
/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/docs/PROJECT_KNOWLEDGE.md first.
Then verify production state and tell me what's open. We'll decide together what to work on.
```

Working agreement (must respect):
- **Collaborative review by default.** Surface findings, propose options. Do not ship fixes autonomously unless the user explicitly says "approved" or "go".
- **No over-engineering.** Don't introduce abstractions, frameworks, or code reorganisations the user didn't ask for.
- **Terse responses.** Senior-developer style — no preamble, no trailing summaries, no emojis.
- **Verify before asserting.** Memory and docs are point-in-time. Code is the source of truth.
- **Update this file.** When work ships, when prod state changes, when a decision is made — append to "Recent History" and patch the relevant section. (See §10.)

---

## 1. What this project is

Enterprise-grade automated trading system for Indian equity markets (NSE/BSE), running on GCP, executing through Upstox.

**Two channels run side-by-side:**
- **Intraday (MIS)** — squared off same day; momentum, breakout, VWAP, mean-reversion, Phase 1/Phase 2 momentum.
- **Swing (CNC)** — held overnight up to 10 days; daily-bias entries, broker-side GTT SL, premarket reconciliation.

**Stack:** Python · FastAPI · Cloud Run · Firestore · BigQuery · GCS · Pub/Sub · Cloud Scheduler · Upstox API v2/v3 (REST + WebSocket) · Next.js dashboard.

User: vishal — comfortable with all of the above, expects senior-level collaboration.

---

## 2. Repository layout

Repo root: `/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader`
GitHub: `https://github.com/vishalrwt1995/python_auto_trader.git`

```
gcp_autotrader/
├── src/autotrader/
│   ├── adapters/               # bigquery_client, firestore_client, pubsub_client, upstox_ws_client, gcs_client
│   ├── domain/                 # scoring.py, regime_affinity.py, indicators, pure logic
│   ├── services/               # trading_service, market_brain_service, universe_service,
│   │                           # ws_monitor_service, order_service, swing_reconciliation_service,
│   │                           # log_sink, market_policy_service
│   ├── backtest/               # pure-replay backtest harness (data.py, harness.py, cli flags)
│   ├── web/                    # FastAPI routes (api.py)
│   ├── scripts/                # one-off scripts (migrations, audits)
│   ├── container.py            # DI wiring
│   ├── settings.py             # all tuning constants + env-var overrides
│   ├── jobs.py                 # scheduler entry points
│   └── main.py                 # FastAPI app
├── tests/                      # 594+ tests, pytest
├── deploy/
│   ├── deploy_cloud_run.sh
│   └── create_scheduler_jobs.sh
├── dashboard/                  # Next.js dashboard (separate Cloud Run)
├── docs/
│   ├── PROJECT_KNOWLEDGE.md    # ← this file
│   ├── ARCHITECTURE.md
│   ├── GCP_PRODUCTION_SETUP.md
│   ├── TRADING_SYSTEM_ENHANCEMENT_GUIDE.md
│   ├── UNIVERSE_RAW_FETCH_AND_BUILD_ALGORITHM.md
│   └── redesign/               # AUDIT.md + DESIGN.md (long-term redesign track)
├── Dockerfile, Dockerfile.ws
└── requirements.txt
```

**Key files (most-touched):**

| File | Purpose |
|---|---|
| `services/trading_service.py` | Scanner loop, 10-stage entry funnel, `_slice_watchlist_for_scan`, `_strategy_allowed` |
| `services/universe_service.py` | Watchlist build, multi-emission setup logic, `_phase2_eligibility` |
| `services/market_brain_service.py` | Regime detection, `allowed_strategies` per regime |
| `services/ws_monitor_service.py` | Real-time SL/target/EOD exits via Upstox WS |
| `services/order_service.py` | Paper + live order placement, GTT for swing |
| `services/swing_reconciliation_service.py` | 03:30 IST premarket exit checks for swing CNC |
| `services/log_sink.py` | audit_log BQ writer (fixed 2026-05-08) |
| `domain/scoring.py` | `score_signal`, `check_strategy_entry`, `check_swing_entry` |
| `domain/regime_affinity.py` | `_HARD_BLOCKS` dict + multiplier matrix |
| `settings.py` | Defaults + env overrides for all tuning constants |
| `deploy/create_scheduler_jobs.sh` | Cloud Scheduler cron definitions |

---

## 3. GCP / Production state

**Verify with:** `gcloud run services list --project grow-profit-machine --region asia-south1`

**Project:** `grow-profit-machine` · region `asia-south1` · account `vishalrwt1995@gmail.com`
**gcloud config name:** `autotrader-groww` · gcloud binary: `/Users/vishalrawat/google-cloud-sdk/bin/gcloud`

> ⚠️ Always pass `--project grow-profit-machine --account vishalrwt1995@gmail.com` to every gcloud command. Active config alone is not sufficient.

| Service | Latest revision (verified 2026-06-17) | Notes |
|---|---|---|
| `autotrader` | `autotrader-00257-mn6` | PAPER; **swing PAPER capital ₹1L→₹5L (`CAPITAL_SWING=500000`, `SWING_RISK_PER_TRADE=7500`; env-only on the PR #25 image, 2026-06-17) for the live PAPER test of the new edges**. Code = **Swing-edges #3 (MR>200-SMA gate) + #7-soft (momentum near-high tilt), PR #25** (universe_service only; shared `domain/swing_signals.py`); on top of Swing overhaul (PR #23) + FSM swing-fix (PR #24) + Phase C v2.1; **₹5L swing** + ₹1L intraday, swing risk ₹7,500, dedup, holiday-aware |
| `autotrader-ws-monitor` | `autotrader-ws-monitor-00042-wv7` | min-instances=1, holds Upstox WS loop, runs the exit FSM (`USE_EXIT_FSM_V1=true`). **Separate image (`cloudbuild.ws.yaml`) — see CLAUDE.md Rule 8; had silently run May-15 code for a month until PR #24.** |
| `autotrader-dashboard` | `autotrader-dashboard-00063-rhc` | Next.js, Firebase Auth |

**Live trading flags (autotrader env, verified 2026-06-17 15:12 IST):**
- `PAPER_TRADE=true` — **PAPER mode** (was `false` on 2026-05-08; flipped to paper since)
- **`CAPITAL=600000` (₹6L total)** · **`CAPITAL_SWING=500000` (₹5L — bumped ₹1L→₹5L 2026-06-17 for the live PAPER test of the new edges; deep-OOS showed the edge cost-crippled at ₹1L, economic ≥₹2L, ~saturated by ₹3L)** · `CAPITAL_INTRADAY=100000` ← Phase C v1 (2026-05-28): per-channel capital separation. Each channel has independent daily loss/profit circuit breakers — bad swing day no longer halts intraday and vice versa.
- **`SWING_RISK_PER_TRADE=7500`** (= 1.5% of ₹5L; scaled WITH capital 2026-06-17 — MUST move with `CAPITAL_SWING` or the 20% per-position cap leaves capital idle. Was 1500 at ₹1L.)
- `RISK_PER_TRADE=250` (intraday, unchanged)
- `MAX_DAILY_LOSS=3000` · `DAILY_PROFIT_TARGET=6000` (LEGACY shared, fallback only — Phase C uses `daily_loss_pct=0.03` / `daily_profit_pct=0.06` per channel: **SWING now 3%/6%×₹5L = ₹15k loss-halt / ₹30k profit-target**; INTRADAY 3%/6%×₹1L = ₹3k/₹6k)
- `SWING_MIN_SIGNAL_SCORE=45`
- `GCP_PROJECT_ID=grow-profit-machine` · `BQ_DATASET=autotrader`

> ⚠️ **Deploy auth (2026-05-28):** the `vishalrwt1995@gmail.com` account has NO on-disk credentials. Deploys work via ADC token:
> `export CLOUDSDK_AUTH_ACCESS_TOKEN=$(gcloud auth application-default print-access-token)` then `gcloud run deploy ...`.
> **Deploy hygiene:** `gcloud run deploy --source` builds from `/Users/.../gcp_autotrader` (the MAIN dir). ALWAYS `git fetch origin main && git merge --ff-only origin/main` in that dir FIRST — on 2026-05-27 a deploy from a stale main dir rolled back a day's work. Verify `git log origin/main..HEAD` is empty before deploying.

**Cloud Scheduler — current jobs (post-2026-05-08 fix):**
- `autotrader-upstox-token-request` — `35 3 * * 1-5` (08:35 IST UTC offset)
- `autotrader-universe-v2-refresh-0615` — `15 6 * * 1-5`
- `autotrader-score-cache-update-close-0705` / `0740` / `score-0830`
- `autotrader-watchlist-v2-premarket-0900` — `0 9 * * 1-5`
- `autotrader-swing-recon-0900` — premarket reconciliation
- `autotrader-watchlist-v2-5m-0930` / `5m-1000` — early-session refresh
- `autotrader-watchlist-v2-15m-1045` / `11to12` / `1300` / `1330` / `1400` / `1415` / `final-1445`
- `autotrader-scan-intraday-3m` — **`*/3 9-14 * * 1-5`** (was `21-57/3`, fixed 2026-05-08)
- `autotrader-scan-intraday-1530` — `0-27/3 15 * * 1-5`
- `autotrader-scan-swing-{0922,1100,1300,1430}` — 4 swing scans/day (was 1; fixed 2026-05-08)
- `autotrader-eod-recon-{1510,1520,1530}`

**Branches:**
- `main` — production (HEAD `5620f7a` as of 2026-05-08)
- `redesign/audit-and-design` — long-term redesign track. Contains `docs/redesign/AUDIT.md` (528 lines, file:line citations) + `docs/redesign/DESIGN.md` (585 lines, target architecture). Pushed.
- `claude/*` — per-session worktree branches (transient)

---

## 4. Data model

### Firestore (database `(default)`)
- `positions` — open + recent closed positions; `wl_type ∈ {intraday, swing}`
- `watchlist` — current watchlist (intraday + swing combined)
- `universe` / `universe_intraday` — daily/intraday candle cache
- `market_brain` — current regime snapshot
- `market_policy` — `allowed_strategies` whitelist + risk caps
- `runtime` — runtime overrides (allow_live_orders, paper_trade flag, etc.)

### BigQuery (`grow-profit-machine:autotrader`)
- `trades` — closed + open trades; `pnl`, `exit_reason`, `hold_minutes`, `entry_ts`, `exit_ts`
- `scan_decisions` — every scan tick row; `qualified`, `blocked_reason`, `setup`, `wl_type`, score components
- `signals` — generated signals
- `audit_log` — action log (writer fixed 2026-05-08; see Recent History)
- `market_brain_history` — regime snapshots over time
- `universe_history` — daily universe builds

---

## 5. Domain — strategies, regimes, scoring

### Strategy taxonomy

**Intraday:** VWAP_TREND, VWAP_REVERSAL, BREAKOUT, PULLBACK, MEAN_REVERSION, MORNING_FADE, MOMENTUM, OPEN_DRIVE, PHASE1_MOMENTUM, PHASE1_REVERSAL
**Short side:** SHORT_BREAKDOWN, SHORT_PULLBACK
**Swing:** BREAKOUT, PULLBACK, MEAN_REVERSION, SHORT_BREAKDOWN, SHORT_PULLBACK (all multi-emission; MOMENTUM excluded for swing)

PHASE1_* = premarket-derived from yesterday's daily-frame momentum (cached at watchlist build)
PHASE2_* = today's intraday momentum, requires ≥3 today bars (5m frame; threshold lowered from 4 on 2026-05-08)

### Regimes
TREND_UP · TREND_DOWN · RANGE · CHOP · PANIC · RECOVERY

### Hard-blocks (`domain/regime_affinity.py:_HARD_BLOCKS`)
Snapshot as of 2026-05-08:
- **TREND_UP** blocks: `BREAKOUT, MORNING_FADE, SHORT_BREAKDOWN, SHORT_PULLBACK, PHASE1_MOMENTUM`
- **RANGE** blocks: `PHASE1_MOMENTUM, MORNING_FADE, SHORT_PULLBACK` (+ others)
- **CHOP / PANIC**: aggressive blocks; PHASE1_MOMENTUM blocked
- **TREND_DOWN / RECOVERY**: PHASE1_MOMENTUM allowed (low Phase 2 confidence here)

### Scoring (`domain/scoring.py:score_signal`)
7-layer composition: Regime 20 + Options 15 + Technical 35 + Volume 10 + Alignment 15 + Penalty.
MORNING_FADE returns hardcoded 75.

### Key tunables (`settings.py`)
- `max_trades_day = 5`
- `swing_min_signal_score = 65`
- `DEFAULT_WATCHLIST_SCAN_BATCH = 25` + `DEFAULT_WATCHLIST_SCAN_CORE = 10` → 35 rows/tick (intraday only; swing scans full watchlist after 2026-05-08 fix)
- `target_atr_mult = 1.25`, `sl_atr_mult = 1.0` (intraday) — under review (see Open Items)
- breakeven trigger at `+1.0 × ATR` MFE, post-breakeven trail `1.5 × ATR`
- FLAT_TIMEOUT 120 min, fires when `|ltp - entry| < 0.3 × ATR` (does not check MFE — flagged in AUDIT.md §10)

### 10-stage entry funnel (`trading_service.py`)
1. Watchlist slice (rotational for intraday, full for swing)
2. Per-row strategy gate (`check_strategy_entry` / `check_swing_entry`)
3. Score threshold (`min_score`)
4. Direction-hold guard (no opposite-side flip within hold window)
5. Regime hard-block (`_HARD_BLOCKS`)
6. Policy strategy whitelist (`market_policy.allowed_strategies`)
7. SL-too-wide for risk budget
8. Phase 1 volume gate (`strategy_phase1_insufficient_volume`)
9. Daily PnL / trade-count cap
10. Order placement (paper or live)

Each gate's rejection writes a `blocked_reason` to `scan_decisions`.

---

## 6. Working style — what's expected of Claude

1. **Collaborative review.** When asked to "analyse" or "investigate" — surface findings, do not propose fixes until requested. The user wants to decide together.
2. **Approval keywords.** Treat "approved", "go", "ship it", "do it" as explicit go-ahead. Anything else → pause and ask.
3. **No emojis in code or docs.** Plain text only unless explicitly requested.
4. **Senior-developer terseness.** No "I'll go ahead and...". No "Great question!". No closing summaries that repeat what was just done.
5. **Verify-before-asserting.** Code is truth. Memory and docs are stale by default; check live state before claiming current behaviour.
6. **Don't introduce frameworks.** No new abstractions, no new dependencies, no over-engineered solutions. Match the codebase's existing style.
7. **Tests are non-negotiable.** Every fix ships with tests. Run the suite (`pytest tests/`) before committing.
8. **Commits sign off as Claude.** `Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>`. Never amend an existing commit unless explicitly asked. Never `--no-verify`.
9. **Production safety.** Ask before any of: deleting positions, modifying live runtime flags, force-pushing, deploying outside market-closed hours.

---

## 7. Open items — under collaborative review

These are observed but not yet resolved. Discuss before acting.

### A. VWAP_TREND target/SL calibration (high priority)
Live evidence 2026-05-08: 3/3 closed VWAP_TREND trades hit SL.
- GAIL VWAP_TREND BUY −₹64.40 / 122 min / SL_HIT (entry 167.80, sl 166.29, tgt 169.31)
- ANGELONE VWAP_TREND BUY −₹72.72 / 53 min / SL_HIT (entry 332.98, sl 329.60, tgt 336.46)
- (Plus historical pattern in AUDIT.md §13.)
Hypothesis: tight 1:1 R:R targets not being reached, stops tighter than typical price oscillation. Needs ATR-vs-true-range analysis before touching.

### B. Stale Apr 29 swing positions (auto-resolves)
3 paper swing positions opened 2026-04-29 still open. Auto-close 2026-05-09 at `swing_max_hold_days=10` boundary. No action needed unless user wants manual close.

### C. BREAKOUT 250 scans / 0 qualified today
BREAKOUT averaged 71.2 score (above threshold) but 0 qualified — likely all hit `regime_strategy_hard_block` (TREND_UP blocks BREAKOUT) or `direction_hold`. Needs per-block-reason breakdown by setup.

### D. MORNING_FADE 235 scans / avg score 21.4 / 0 qualified
Score formula returns 75 hardcoded but observed avg 21.4 — suggests something is overriding. Investigation pending.

### E. Watchlist build duration 11-12 min (CPU=2 should help)
After CPU 1→2 bump, re-measure on next build cycle.

### F. PULLBACK swing gates may need bullish-divergence/reversal-candle confirmation
Per AUDIT.md §7.2 — discussion pending.

### G. BREAKOUT swing needs VCP / cup-handle pattern detection before re-enable
Currently disabled in some regimes. Discussion pending.

### H. Intraday edge → cross-sectional low-vol SWING test (next major thread)
Intraday audit concluded candle-intraday is **cost-walled** (see §8 2026-06-15 Phase A/B). The real edge found is **cross-sectional low-volatility** (gross +32%/yr, robust incl 2026) + regime-conditional range-momentum & reversal — but NET-negative as intraday (flat-EOD forces 100% daily turnover on a slow signal). **Next, on resume:** user is building **2010–2026 swing history**; test low-vol (+ conditional momentum/reversal) as a cross-sectional **market-neutral SWING** strategy on it. Harnesses: `backtest_v2/intraday_alpha_search.py`, `intraday_regime_diag.py`, `intraday_phase_b_gate.py`. Caveats: GROSS-only so far; low-vol **flips in PANIC** (regime-gate required); swing version needs overnight-risk + shortability + its own cost gate. **Executability (India):** retail can't hold overnight equity shorts, so the idealized long-short basket isn't directly tradeable — executable forms are **long-only min-vol** or **long-basket + short-Nifty-futures hedge**; Phase 0 MUST backtest the *executable* form (the +32% gross may not fully transfer), not the stock-vs-stock long-short. **Agreed plan (2026-06-15):** if validated, ship as a NEW `MARKET_NEUTRAL` channel (own dashboard panel + circuit breakers, NOT under swing/intraday), funded by **₹1L repurposed from the parked intraday channel** (total stays ₹2L). Phases: 0 validate (GATE: net-positive + robust 2010-26) → 1 design → 2 build → 3 PAPER → 4 live (explicit go only). **2010-26 deep history now BUILT** (swing audit 2026-06-17 — `gs://…/oos/candles_daily_deep.pkl`, prod's `score_1d` source, 2,506 syms × 2010-26) → **Phase 0 unblocked.** NB: swing-audit's long-only `realizedVol` scan showed ~no edge at 10d-forward — does NOT contradict this (different measurement); Phase 0 must test the *market-neutral, regime-gated, executable* form. Discuss before building.

---

## 8. Recent history (newest first)

> Append-only log. Each entry: date · revision/commit · what shipped · live evidence.

### 2026-06-18 — Fresh INTRADAY re-audit (no retail edge → parked) + swing ₹5L profit profile + improvement-scope (NO prod change)

**Context:** user distrusted the prior-thread intraday audit ("done in another thread, doubt it was correct") → redid it **fresh + independent** (own BigQuery tests on the full 165M-row `candles_5m_full`, not the prior `intraday_baseline.py`). Verdict reproduced AND extended.

1. **INTRADAY = no retail-viable systematic edge (fresh, exhaustive).** Every category negative on NSE liquid equities net of cost:
   - Single-name **directional** (trend-long above-VWAP): forward 60-min return ≈ **0, sub-coin-flip (41-45% pos)** on a ~14M-entry sample — entry carries no directional info.
   - **Cross-sectional** momentum + range-position: weak/unstable, **negative 2024-26**.
   - **Market intraday momentum** (Gao et al., the documented index edge): marginal in India, **below ~3 bps futures cost**; vol-conditioning made it worse.
   - **ORB on stocks-in-play** (Zarattini-Aziz, best-documented retail intraday, US Sharpe 2.4): **NEGATIVE GROSS every year** on NSE (−0.05 to −0.11R).
   - **Root cause: NSE intraday MEAN-REVERTS** (US continuation edges invert & die) + the retail cost wall. Corroborated by prod's real paper trades (**93 trades, 9% WR, −₹687, every month negative**) + SEBI (70% cash / 93% F&O traders lose).
2. **Big-player research:** intraday edge = **data (catalyst/options-OI/L2) + speed + cost**, not a setup. Speed is NOT our blocker (Upstox WS handles non-HFT). Cost can't reach prop at retail (statutory STT). **Only untested lever = options-OI / F&O** (Upstox provides it; India intraday is options-driven) — but that's the **93%-lose arena**, access ≠ edge. **Decision: intraday systematic-on-candles is dead for us; F&O-OI track parked** (data-acquisition R&D, low base rate).
3. **Swing ₹5L profit profile** (shipped #3+#7-soft, deep OOS 2010-26, net of cost): raw **~8.5%/yr** (median 9.2%, **13/17 +years**), **MAX DRAWDOWN 35% (₹175k)**, worst yr −25% (2011), best +38% (2023), return/risk 0.60. **Honest LIVE ≈ +5%/yr (₹25k)** after survivorship/vintage/slippage haircut — *underperforms the ~10-12%/yr cap-weight market on raw return* (its value is lower-beta + diversification, a satellite not a wealth engine). **Improvement scope:** cost = small (already diluted at ₹5L; residual mostly statutory STT); tuning = exhausted/overfit-risky; candle-edges = near-exhausted → **the only step-change is NEW fundamental data** (real PEAD / analyst-revisions / quality-value); a **drawdown/vol-target overlay** is the best *risk-adjusted* lever (cuts the 35% DD, doesn't raise return). **Allocation lever tested + REJECTED:** lifting the MR reserve cap 3→4→5 at ₹5L *cuts* net (8.5%→6.6%→4.9%) and *explodes* DD (35%→52%→57%) — the "idle" slots in RANGE are the system correctly declining marginal 4th/5th-best MR trades. **System is signal-QUALITY-constrained, not slot-constrained** → confirms the lever is better signals (fundamental data), not more positions/capacity. Reserve-2-trend validated; do not change. **Full param surface then swept @₹5L (sector-cap, poscap%, risk%): NONE improve** — sector_cap=3 is a no-op (system already never holds >3/sector), cap=2 hurts (−19% net, same DD); poscap 20% & risk 1.5% near-optimal; *every* exposure-reduction lever cuts return > drawdown (flat/worse Sharpe). **Config is on the risk-adjusted frontier — signal-quality-constrained confirmed a 3rd way.** Only untested risk idea: a *dynamic* drawdown/vol-target overlay (cut size only in adverse regimes — could shallow the 35% DD at better Sharpe; does NOT raise return). Return lever remains fundamental data.
4. **No-trades 06-17/18 = BENIGN, by design.** Regime `RANGE_ROTATING` → momentum/pullback hard-gated (`swing_setup_regime_gate`; 15+ score-qualifying signals blocked); MEAN_REVERSION at the **3/3 reserve-trend cap** (CROMPTON/JAYNECOIND/SAIL open) → `qualified=0`. Scans running, zero errors, deploy healthy. Idle stretches in non-trending regimes are normal — selectivity *is* the edge.

**Next (PARKED — user will signal when to start):** scope the **fundamental-data edge project** — the only step-change shot for swing (needs earnings/fundamentals ingestion + factor modeling). Intraday left as-is (PAPER, deprioritized; zombie-emission cleanup optional). Intraday audit closed (no edge). Scratch harnesses: `~/.autotrader_backtest_cache/oos_*.py` (fresh BQ intraday tests, swing ₹5L profile).

### 2026-06-17 (later) — SHIPPED swing-edges #3 (MR>200-SMA gate) + #7-soft (momentum near-high tilt) — PR #25, PAPER

**Goal:** implement the edges that survived strict walk-forward verification (the locked "verify P0/P1 before implement" gate). Branch `swing-mr-gate-mom-tilt` → PR #25 → merged `c86c030` (change `123513b`) → deployed **`autotrader-00256-g2r`** (was `00255-gnv`), serving 100%, PAPER + env preserved, ws-monitor unchanged.

1. **Verification first (per locked process).** Re-ran the deep 2010-26 OOS under STRICT walk-forward (select-on-train 2010-17 / test-on-unseen 2018-26, robust across 5 split boundaries). **#3 MR>200-SMA gate PASS** (both halves +, +113k test-half Δ@₹5L); **#7-SOFT momentum near-high ranking tilt PASS** (plateau W=1-2, +66k test Δ@₹5L). **#7-HARD (binary 52wk-high gate) FAILED** — train-best threshold 0.92 lost money on the unseen test half (overfit) — and **#2 FAILED**; both dropped. **This corrects the prior entry's framing of #7 as a shippable hard gate** — only the soft ranking tilt generalised. Combined #3+#7-soft: TEST5L 227,980→389,350 (+161k), both halves +.

2. **Shipped (universe_service only).** New pure module `domain/swing_signals.py` — shared by prod AND backtest (the `swing_exit.py` fidelity discipline, so prod can't drift). #3: `MEAN_REVERSION` emitted only when close>200-SMA (`closes[-201:-1]`), at candidate generation + the winner-takes-all fallback. #7-soft: near-52wk-high tilt (W=1) baked into the `MOMENTUM` row's `wl_score` (trading_service already fills the 5-slot book by `wl_score` desc). No `models.py`/`trading_service.py` change. Slice windows mirror the backtest's `bars[ei-1]` as-of convention exactly.

3. **Fidelity-replay (the gate).** Shared module == certified backtest **bit-for-bit over all 239,359 real 2010-26 pool entries** (0 gate + 0 tilt mismatches); walk re-run reproduces the certified NET exactly (baseline TEST5L 227,980, #3+#7-soft 389,350). 20 new unit tests (`tests/test_swing_signals.py`, incl. off-by-one guards); **382 passed** blast-radius+swing/regime suite (before AND after commit). Scratch validator: `~/.autotrader_backtest_cache/oos_fidelity_shared.py`.

4. **Honest calibration unchanged:** thin, trend-dependent, lumpy; improves *selection* (which setups take slots), not exits; absolute ₹ small at ₹1L (economic mainly ≥₹3L). New code first executes at **next swing scan 09:22 IST 2026-06-18** (today's scans ran on old rev). **PAPER stays PAPER.**

**Deploy hygiene:** `autotrader` only (selection change — ws-monitor/exits untouched, Rule 8 N/A). Rule 1 sync verified (`git log origin/main..HEAD` empty), Rule 3 ADC token.

**Capital set for the live PAPER test (2026-06-17, env-only, rev `autotrader-00257-mn6`):** swing ₹1L→**₹5L** (`CAPITAL_SWING=500000`, `SWING_RISK_PER_TRADE=7500`=1.5% — must move together or the 20% per-position cap idles capital; `CAPITAL=600000`); intraday unchanged. Per-capital deep-OOS net (prod-faithful RS, net of Upstox cost; TEST 2018-26 %/yr): **₹1L 5.5% (cost-crippled) → ₹2L 8.0% → ₹3L 8.4% → ₹5L 8.7%** — edge ~saturates by ₹3L; chose ₹5L for max absolute ₹ at ≈equal efficiency (PAPER ⇒ no real cost). Honest: raw is an optimistic ceiling (survivorship/vintage/parity haircuts remain); **lumpy** — 2011 −24% single-year, 2/9 test years negative, and the change itself gave 2022 back vs baseline (+24.7k→−37.4k @₹5L). The 4 open swing positions at change-time (EMAMILTD/CROMPTON/JAYNECOIND/SAIL) are **unaffected** — qty/stop/sl_dist stored at entry; new sizing applies to entries from **09:22 IST 2026-06-18**.

**Next:** PAPER-monitor swing fills vs backtest expectation at ₹5L; bear-tail/PANIC fix; INTRADAY audit (#23-25).

### 2026-06-17 — Swing deep-audit: 2010-21 held-out OOS + setup-by-setup review + edge-discovery (NO prod change; PAPER untouched)

**Goal:** validate the deployed swing config out-of-sample (2010-2021), review each cell, audit for missing/proven setups. **Full handoff + prioritized TODOs: `docs/SWING_EDGE_AUDIT_HANDOFF.md`.** Reusable infra: `backtest_v2/oos_cloud.py` (Cloud Run job `autotrader-oos`, 8 vCPU), `gs://…/oos/candles_daily_deep.pkl` (deep 2010-26, = prod's live `score_1d` source; `candles_daily` BQ is backtest-only).

1. **Held-out OOS built + validated.** Deep pipeline reproduces the prod brain at **97% fidelity** (VIX=15 stub confirmed harmless). Caught + fixed a **sector-map packaging bug** (was 5× sparsifying the pool; now fail-closed). Honest result: **thin trend-dependent edge ~+3%/yr @₹1L, ~+5%/yr @₹5L** — the +8.9% headline was inflated by backtest-RS + non-prod `candles_daily`. Caveats inherent (not fixable with our data): survivorship (mild on the liquidity-filtered traded universe; daily archives since 2026-02-25 make *future* backtests survivorship-free) + 91% regime vintage.

2. **All 3 cells KEEP.** MOMENTUM×TREND_UP (engine, +54.7k@1L, trend-dependent, 2015-17 = −22.5k drawdown); MEAN_REVERSION×RANGE (gross +57k real, cost-crippled @₹1L only → economic ≥₹3L; carries bear-tail risk); PULLBACK×TREND_UP (ugly cell P&L −18k but **+12.7k MARGINAL** — blocks worse marginal trades).

3. **Edge-audit — one refinement found.** ✅ **#7 52-wk-high gate on momentum** (`hi52≥0.85`): OOS-robust (+₹39k OOS@₹5L), removes counter-trend-bounce duds; projected next config **~+4.8→+5.4%/yr OOS@₹5L** (modest); needs fidelity-replay + fresh-split before ship. ❌ rejected: 12-1 momentum (over-extension), short-term reversal (uneconomic), PEAD (weak proxy + needs EPS + wrong horizon); broad 9-family data scan → no new incremental edge. **System is mature; price-data edge hunt at diminishing returns.**

4. **Cross-thread:** the deep history built here **unblocks the intraday thread's low-vol market-neutral SWING Phase 0** (Open Items §7-H).

**Next (TODOs in handoff doc):** ship #7 (test→fidelity→PAPER); test entry-quality floor (highest untested lever); capital→₹3-5L decision; bear-tail/PANIC fix; then the **never-audited INTRADAY channel** (#23-25). PAPER stays the live arbiter.

### 2026-06-15 (later) — INTRADAY EDGE AUDIT (Phase A/B): real cross-sectional edge found, but cost-walled as intraday → belongs at swing horizon

**Goal:** after proving all prod intraday setups edgeless, hunt for ANY real intraday edge on candle data (gross first, then net). New harnesses: `backtest_v2/intraday_alpha_search.py` (23-signal cross-sectional library), `intraday_regime_diag.py` (decay-vs-regime), `intraday_phase_b_gate.py` (exact-Upstox-cost portfolio gate). Logs: `~/.autotrader_backtest_cache/intraday_audit/{alpha_search_allyears,regime_diag,phase_b_gate}.log`. No prod change; PAPER untouched.

1. **Prod setups have NO gross edge** (37,504-entry baseline, gross R *before* cost): MEAN_REVERSION −0.28 / VWAP_TREND −0.38 / MOMENTUM −0.38 / PULLBACK −0.39, ~28% win, negative every year. Sub-1.0 win rate vs 1R stop = negative expectancy by construction. Ignoring cost does **not** rescue them.

2. **Phase A — a real cross-sectional gross edge exists** (lag-honest IC: decide 11:00 / enter 11:15 / hold EOD / market-neutral / per-year 2022-26). THREE edges: **(a) low-vol** (`rvol`,`park`: long calm, short jumpy) — robust, +IC every year incl 2026, the standout; **(b) range-momentum** (`range_pos`) — strong 2022-25, regime-conditional; **(c) multi-day reversal** (`ret_3d/5d`) — conditional on unstable years. Naive momentum (`mom_open`) and VWAP-distance (`dist_vwap`) were strong 2022-25 then collapsed in 2026.

3. **Decay vs regime — mostly REGIME, not permanent decay** (`intraday_regime_diag.py`). 2026 is the most unstable year (intraday mkt vol **0.73%** vs 0.53-0.64 prior; **PANIC 33%** of days vs 11-29%; TREND_UP only 9%). Conditioning IC on the brain regime: `range_pos` still fires on 2026 TREND_UP/RANGE days (+32 vs +37 hist ×1e3 IC) → REGIME; only naive `mom_open` is dead even in trend → genuine decay. Low-vol works (stronger) in RANGE/TREND_UP but **FLIPS in PANIC** (−10.5 hist → +16.2 2026 ×1e3) — not all-weather, needs a panic gate.

4. **Phase B cost gate — NO-GO for intraday** (`intraday_phase_b_gate.py`, exact Upstox cost via `backtest/costs.py`, regime-gated RANGE/TREND_UP, K∈{5,10,20,50}/side × cap ₹1-5L). Regime-gated low-vol **GROSS +31.6%/yr at K=5, positive every year incl 2026 (+47%)** — a real edge. But **NET best is −1.1%/yr (K=5, ₹5L)**, negative 2/5 years, *before* slippage and assuming full shortability; all other configs −17 to −45%/yr. Cost ≈ 0.13%/day ≈ gross ≈ 0.13%/day → cancels. **Structural cause:** intraday flat-by-EOD forces a full round-trip *every day* (100% turnover) on a slow/sticky signal — the worst cost setup for low-vol.

**Conclusion:** candle-based intraday is cost-walled even with the best real edge findable (now proven 4 ways + best-edge gate). The low-vol edge is real but in the **wrong channel** — held as **swing** (multi-day) it's sticky (~10-20%/wk turnover vs 100%/day) → ~6-12× less cost → the +32% gross should clearly survive. Low-vol anomaly is natively multi-day.

**Next (user-driven):** user is building **2010-2026 swing history** (16 yrs, multi-cycle) for a more robust backtest; on resume, test low-vol (+ conditional range-momentum/reversal) as a cross-sectional **market-neutral SWING** strategy. See Open Items §7-H. *(Doc updated this session; not yet committed — awaiting user go-ahead.)*

### 2026-06-15 — First live swing day: deep audit + FSM swing-exit root-cause fix (PR #24, revs autotrader-00255-gnv / ws-monitor-00042-wv7, PAPER)

**Audit of the first live day (RANGE all session, trend 19 / breadth 41).** The SELECTION layer (PR2) worked exactly as designed: premarket build wrote 450 swing rows all carrying wl_score/rs_vs_mkt/breadth_pct; multi-emit live (150 MOMENTUM + 150 PULLBACK + 150 MR, 88 multi-setup symbols); MOMENTUM 266 + PULLBACK 150 blocked by `swing_setup_regime_gate` (correct in RANGE); MR `swing_rs_below_market` fired 87×; **1 MR entry (JAYNECOIND, RSI 39.8, passed RS)**; daily reconcile ran clean (checked=5); zero errors. 2 legacy shorts (SUNDARMFIN, GHCL) exited at SL_HIT correctly.

**BUG FOUND + FIXED (root cause).** The live intraday exit path is the **exit FSM** (`domain/exit_fsm.py`, `USE_EXIT_FSM_V1=true`), reached via `_on_quote → _on_quote_fsm`. **PR1 edited the legacy `_on_quote` handler, which is dead code when the FSM flag is on** (early return) — so PR1's intraday swing-exit changes never took effect, and the FSM (intraday-tuned: breakeven@0.8R + 2R TARGET_HIT + 2×ATR runner) was being applied to swing — the opposite of the validated design. Compounding: **`autotrader-ws-monitor` (separate `cloudbuild.ws.yaml` image) had run May-15 code for a month** — the `--source` deploy never touched it. No divergence had occurred yet (no open swing position reached +0.8R today; all 4 verified `exit_fsm_state=INITIAL`, SL unchanged).

**Fix (PR #24):** `exit_fsm.transition` is now swing-aware — for `is_swing`, SL-only (stay INITIAL, no breakeven/target/runner/flat); `swing_reconciliation_service` owns the daily 1R trail; SL_HIT enforces it. Intraday unchanged. +8 FSM tests; fsm/exit/m1 suite 221 passed. **Deployed BOTH services** (autotrader `--source` → 00255-gnv; ws-monitor `cloudbuild.ws.yaml` build → `run deploy --image` → 00042-wv7, env + min-instances=1 preserved, came up clean: ws_connected, 5 positions subscribed). **Added CLAUDE.md Rule 8** (deploy ws-monitor for exit changes; the FSM is the live exit path, not `_on_quote`).

**Lesson:** PR1 was "fidelity-proven" only for the pure exit function + reconcile — not the live tick path. Always trace which handler is live (`USE_EXIT_FSM_V1`) and that ws-monitor was redeployed before claiming an exit change is live.

### 2026-06-13 — SWING OVERHAUL: trailing exit + validated 3-cell selection (PR #23, rev autotrader-00254-wqk, PAPER)

**Why:** multi-year faithful backtest (2022–2026, 58,430-signal pool) showed the old swing config nets **−56,820 at ₹1L (−12.9%/yr)** — the 0.5R-partial/2R-target exit churns into costs, momentum/pullback fire in edge-less regimes, the single-emit watchlist hides the profitable setups, and slots fill first-come.

**Shipped (merge f141f83; PR1 c10b82e + PR2 dce43a6/1ceacdb):**
1. **Exit → daily 1R trailing** (`domain/swing_exit.py`, single source of truth): ride full size, arm at +1R, trail = stored `sl_dist` below the running peak; 20 **trading-day** max-hold (was 10 calendar). `swing_reconciliation_service` ratchets the stop premarket (dropped TARGET_HIT_DAILY + SUPERTREND_FLIP exits; kept daily-close SL backstop); `ws_monitor` swing path = resting-SL only (0.5R partial + 2R target + intraday ATR-trail removed/gated for swing).
2. **Regime gate** (`swing_setup_allowed_in_regime`): MOMENTUM/PULLBACK only in {TREND_UP, EARLY_TREND_UP}; MEAN_REVERSION only in {RANGE, RANGE_ROTATING}; nothing in PANIC/TREND_DOWN/CHOP/RECOVERY. Block reason `swing_setup_regime_gate`.
3. **Shorts disabled** (`_ALLOW_SHORT_SETUPS=False` in universe_service) — no cash-executable short edge (PANIC V-bounce squeeze).
4. **Multi-emit watchlist**: per-setup diversified slates for {MOMENTUM, PULLBACK, MEAN_REVERSION} (a symbol may appear once per setup); BREAKOUT not emitted (hard-blocked everywhere).
5. **wl_score slot ranking**: regime-weighted `final_score` persisted per row; scan fills the 5 swing slots best-first (`_read_watchlist_with_fallback` re-sorts swing rows desc).
6. **Per-cell filters**: MR+PULLBACK need `rs_vs_mkt>0` (ret60 − universe-mean ret60); PULLBACK needs `breadth_pct≥60` (% of swing universe > 50d SMA). Fail closed on legacy docs until next premarket build. Block reasons `swing_rs_below_market` / `swing_breadth_below_60`.
7. **Reserve-2-trend**: MEAN_REVERSION ≤ 3 of 5 concurrent slots (`SWING_RANGE_GROUP_CAP=3`, block reason `swing_range_slots_full`).

**Evidence:** fidelity gate (`backtest_v2/prod_replay_validate.py`, artifacts log 2026-06-13) — prod predicates reproduce the validated config **to the rupee** (+39,310 ₹1L NET). **Honest deployable expectation** (prod RS = as-of timing, no look-ahead): **+29,969 NET ₹1L (+6.8%/yr)**; ₹2L +57,111 / ₹3L +94,777 / ₹5L +211,595 (+9.6%/yr). Discovery: the backtest RS carried a 1-day look-ahead worth ~−18k; prod's arithmetic-mean leg recovers ~+9k; RS filter still honestly earns +17,949 (no-RS control = +12,020). OOS-realistic: **~+3–5%/yr at ₹1L, lumpy** (2026 H1 negative: bear leg + V-bounce with 1 TREND_UP day; MR dip-buying bleeds in down-RANGE — known weakness, mitigations on). Tests: 399 passed (30 new: `test_swing_exit` incl. exit ≡ backtest on all 58,430 entries, `test_swing_regime_gate`, `test_swing_selection`, `test_ws_monitor_swing_exit`); 4 pre-existing date-dependent `test_watchlist_v2` phase2 failures unrelated (stash-verified; holiday-calendar AttributeError — open item).

**Live verification (2026-06-13 ~07:00 IST, Saturday):** rev `autotrader-00254-wqk` serving 100%; env preserved (PAPER_TRADE=true, CAPITAL_SWING=100000, SWING_RISK_PER_TRADE=1500, SWING_MIN_SIGNAL_SCORE=45); manual `/jobs/swing-reconcile` smoke on the 5 open PAPER positions (4 SHORT_BREAKDOWN + 1 MEAN_REVERSION): `checked=5 errors=0`, no exits (correct: 1–5 days held vs 20d max, no SL breach), no trail updates (correct: none at +1R yet), **no `swing_recon_no_sl_dist` warnings** (all positions trail-capable).

**Watch Monday (2026-06-16):** 08:30 premarket build must write `wl_score`/`rs_vs_mkt`/`breadth_pct` on swing rows (until then RS/breadth gates fail closed); 09:22 swing scan should show new block reasons in scan_decisions; no new shorts ever; open shorts exit via trail/SL/20d max-hold only.

### 2026-05-27/28 — Cost-model correction + swing sizing (live revision autotrader-00244-s52, PAPER)

**Shipped (4 changes, all live):**
1. **MORNING_FADE vwap-guard fix** (PR #16) — `live_price_above_vwap` was blocking the strategy's own entries (price-above-VWAP IS the fade entry condition). Added MORNING_FADE to the exception tuple.
2. **costs.py → Upstox rates** (PR #17, commit e492df4) — was modeling **Zerodha** fees; understated real cost **2–4×**. Now defaults to Upstox (intraday 0.1%/cap₹20, delivery ₹20/order, DP ₹20), `.zerodha()` kept. Backtest-only. Verified RT on ₹20k: intraday ₹54.25 / swing ₹115.25.
3. **Swing sizing `SWING_RISK_PER_TRADE` 600→1500** (env var, rev 00243) — validated on real engine @ ₹1L: net +₹28k/3yr vs +₹4k at ₹600 (costs as % of gross: 84%→51%). Caveat: validated on favorable 2023-26 window; ~27% maxDD.
4. **Same-symbol dedup** (PR #17, commit 52eaeeb, rev 00244) — new gate `symbol_already_held` blocks holding a (symbol,direction) twice (DMART was held 2×). `_MAX_SAME_STRATEGY=2` already existed for setup-concentration.
5. **Daily loss/profit limits raised 300/375 → 3000/6000** (env-var, rev 00245) — stale ₹50K-era defaults would've halted system after first swing trade resolved.
6. **NSE holiday awareness** (PR #18, commit fd0c71a, rev 00246) — `is_market_open_ist()` was weekday+clock only, NO holiday check. Discovered 2026-05-28 Bakri Eid: system thought market was open; only saved by Upstox returning no data. Hardcoded full 2026 NSE calendar (16 dates) in `time_utils.NSE_TRADING_HOLIDAYS` + new `is_trading_day_ist()` helper. Annual maintenance: add next year's list each Dec.
7. **Phase C v1 — per-channel capital separation** (PR #19, commit 54b1a44, rev 00248, activated 2026-05-28 late evening on Bakri Eid market-closed window). New `CAPITAL_SWING` / `CAPITAL_INTRADAY` env vars + `daily_loss_pct=3%` / `daily_profit_pct=6%` per channel + `channel_capital()` helper in settings + `get_today_realized_pnl_by_channel()` state method. trading_service.py replaces shared daily-limit gate with per-channel logic when both channel capitals are set; legacy shared-pool path preserved as fallback. **Total ₹2L = ₹1L swing + ₹1L intraday.** Bad swing day no longer halts intraday and vice versa. Code shipped behind runtime gate (`capital_swing > 0 AND capital_intraday > 0`); activated by env-var update. 13 new tests + 236 regression — all green. **Phase C v2** (full PortfolioBookV1 activation + per-channel position-size caps + per-channel capital-exhausted gate + dashboard) planned for weekend (offline, market closed Sat/Sun).

**KEY FINDINGS (decision-grade, honest):**
- **Swing edge is MARGINAL after real Upstox costs.** 2019-26 backtest: only `swing_50` net-positive; 55/60 lose at all sizes. Edge is thin (~35% WR, +0.06R), lumpy (79% of profit from 2023 alone, 2025 was a losing year), ~4–9%/yr at best on deployed capital. NOT a money-printer.
- **The dominant lever at ₹1L is POSITION SIZE, not gates** — Upstox's flat fees (₹20 brokerage + ₹20 DP) crush small positions. Bigger positions amortize them.
- **Intraday is BROKEN at the strategy level, not just costs.** Old-strategy backtest (2025-12→2026-05): **gross −₹440k BEFORE any brokerage** (−₹595k after). It loses on its own merits — sizing/cost tweaks can't fix negative gross edge. Plus cost-disadvantaged at ₹1L (5 trades/day ≈ 68% annual drag). → Phase E #64 must **gate on proving POSITIVE GROSS edge** (build simulator + test ORB/VWAP_TREND gross-first) before any build/fund decision.
- **Capital decision:** stay at ₹1L for now.

**INCIDENT (resolved):** a `gcloud run deploy --source` from a STALE main working dir (7 commits behind origin/main) silently rolled back the day's work (caught via brain `tactical_trend=NULL`). Fixed by ff-syncing main dir + redeploy. → added deploy-hygiene note in §3.

**OPEN TODOS:** #68 all-weather re-validate ₹1,500 on full 2019-26 (favorable-window risk); #69 watch live paper swing @ ₹1,500 (10-15 days); #64 Phase E intraday (reconsider scope vs cost reality — gate on ORB+VWAP_TREND first); #61 capital split (deferred until intraday proven).

### 2026-05-22 20:15 IST — V2 swing exit logic DEPLOYED (live revision autotrader-00235-gtb)
**PR #10 merged · Revision `autotrader-00235-gtb` · 100% traffic on latest**

Shipped two related changes:
1. **100% production-match replica fix** (`prod_replica_v2.py`): use `MarketRegimeService.from_market_brain_state()` instead of `snap.to_regime_snapshot()` to construct the RegimeSnapshot. This matches production's behavior (which doesn't populate vix/fii/nifty.change_pct, defaulting them to 0, scoring regime layer at constant +13 for every BUY scan). Validated on 200/200 random scans = 100% direction + 100% raw_score match.
2. **V2 swing exit logic** (`ws_monitor_service.py`): scale out 50% of swing position at 0.5R, hold remaining 50% to TARGET/SL/MAX_HOLD. Also disabled breakeven SL move for swing (V3 variant tested moving SL to BE = -₹125k loss vs baseline). Intraday positions unaffected.

**Backtest evidence (Phase E, bar-by-bar simulation, 4 windows):**
| Window | Trades | V0 baseline | V2 (now live) | Improvement |
|---|---|---|---|---|
| 1 month | 480 | -₹8,948 | +₹11,037 | +₹19,985 |
| 3 months | 530 | -₹1,918 | +₹15,972 | +₹17,890 |
| 6 months | 1,795 | -₹41,420 | +₹39,693 | +₹81,113 |
| **1 year** | **5,775** | **+₹11,478** | **+₹204,071** | **+₹192,593** |

Per-strategy 1-year improvement: PULLBACK +₹127k, MOMENTUM +₹50k, MEAN_REVERSION +₹15k.

**Variants rejected:**
- V1 (exit ALL at 0.5R): -₹13,936 over 1yr — cuts winners short.
- V3 (trail SL to breakeven after 0.5R partial): -₹125,042 — BE knocked off by normal noise.

**Root cause why V2 works:** Swing trades reach +0.54R to +0.61R MFE on average but realize only +0.04R because 2R targets are rarely hit. Locking in 50% at 0.5R captures the structural edge that's already there.

**Post-deploy validation (e2e):** Equivalence test unchanged (65% gate-ordering noise, expected). Phase E 6mo re-run confirmed V2 still +₹85,569 over baseline. Code-level diff confirmed backtest V2 = deployed ws_monitor swing block (same formula, qty split, trigger, no SL movement).

**Monitor tomorrow (Monday 2026-05-25):**
- First swing scan at 09:22 IST. When any swing position reaches 0.5R, expect log line: `swing_partial_exit_0_5R tag=... exit_qty=... at_0.5R=...`
- `signals` table: entries with `entry_placed=true` continue as before.
- `attribution` table: new exit reason `SWING_PARTIAL_0_5R` for the partial-fill leg.
- `trades` table: partial-fill PnL recorded; full-close PnL recorded when remaining 50% exits via SL/TARGET/MAX_HOLD.
- Intraday positions: no behavior change (still use 1R/1.5R tiered partials).

**Rollback:** revert commit + redeploy (paper mode = zero real-money risk).

### 2026-05-22 03:50 IST — Pre-market readiness verified (before Friday open)
**Live revision:** `autotrader-00234-7rt` · 100% traffic on latest · 0 errors in last 12h

**Verified production state (via direct Cloud APIs — gcloud auth was expired, used ADC refresh token):**
- `PAPER_TRADE=true`, `USE_PLAYBOOK_V1=true`, `SWING_MIN_SIGNAL_SCORE=45` — all set correctly
- `VIX_TREND_MAX` is **unset** in env → code default `15` applies (`settings.py:69`). Earlier session notes had this as `18 production override` — that was incorrect carry-over. Backtest `prod_replica_v2.py` uses 18; should be reconciled to 15 for accuracy.
- 28 Cloud Scheduler jobs ENABLED (swing scans at 09:22/11:00/13:00/14:30, intraday `*/3` 9-14, brain snaps, EOD reconcile, etc.)
- Brain snapshots: 56 days available, May 21 had 59 snapshots (last at 15:21 IST close)

**0 trades in last 3 days (May 19, 20, 21) — verified CORRECT, not a bug:**
- Brain was RANGE → DEFENSIVE risk_mode all week
- Market confidence 33-39 (very low), participation WEAK
- May 20: 2,137 scans, 0 qualified (1,598 score_below_min, 466 direction_hold, 47 regime_hard_block)
- May 21: 2,200 scans, 0 qualified (1,571 score_below_min, 424 direction_hold, 67 morning_fade_outside_time_window)
- Last signals placed: May 19, 2× POWERGRID BUY @ score 63 in RANGE/NORMAL

**Backtest progress this session:**
- Built `phase7_v2_with_gates.py` applying real production gates (hard_block, affinity multiplier, adjust_signal, MarketPolicy.allowed_strategies)
- Short test (May 1-21, 3 weeks, 47 days): -₹5,874 net (PULLBACK -₹5,315 dominant)
- Medium test (Mar 9 - May 21, 47 days, 100% real brain snapshots): **-₹549 net (essentially flat)**, PULLBACK +₹2, MOMENTUM +₹241, MR -₹793
- PULLBACK 3-week loss was regime outlier, not structural

### 2026-05-08 — Mid-session pipeline batch (8 fixes)
**Commit `4cb36ab`** (merged into `5620f7a` on main) · **Revision `00225-zjt`** · 594 tests passing

| # | Fix | File | Live evidence |
|---|---|---|---|
| 1 | Intraday cron `21-57/3` → `*/3` (recovered 95 min/day = 29% trading time) | `deploy/create_scheduler_jobs.sh` | 10:00-10:21 IST had zero scans pre-fix |
| 2a | Swing channel scans full watchlist (no rotation) | `trading_service.py:_slice_watchlist_for_scan` | 5 MEAN_REVERSION rows; only 1 evaluated pre-fix |
| 2b | Swing scan 4×/day (09:22, 11:00, 13:00, 14:30) | `deploy/create_scheduler_jobs.sh` | was 09:22 only |
| 3 | Cloud Run min-instances 0→1 | gcloud | cold-start aborts |
| 4 | Cloud Run CPU 1→2 | gcloud | watchlist build 11-12 min |
| 5 | PHASE1_MOMENTUM hard-block in TREND_UP | `regime_affinity.py:_HARD_BLOCKS` | OLECTRA PHASE1_MOMENTUM SL_HIT in 8 min, −₹47 |
| 6 | Watchlist refresh crons 13:30, 14:00, 14:15 | `deploy/create_scheduler_jobs.sh` | 13:00→14:45 was 105 min gap |
| 7 | Stale Apr 29 paper positions — NO ACTION (auto-close at 10d max-hold) | — | — |
| 8 | Phase 2 bar threshold `< 4` → `< 3` (recovers 5 min at open) | `universe_service.py:_phase2_eligibility` | 09:30 build rejected 460/461 phase2 candidates pre-fix |

**Tests added:** `tests/test_pipeline_fixes_2026_05_08_midsession.py` (7 tests), `tests/test_strategy_audit_fixes_2026_05_08.py` (15 tests).

### 2026-05-08 — Emergency PHASE1_* allowlist fix
**Commit `6c5f800`** · Revision `00223-ffp`
PHASE1_MOMENTUM, PHASE1_REVERSAL were missing from `market_policy.allowed_strategies` → 100% of intraday watchlist blocked with `policy_strategy_blocked` (5 high-score signals lost). Added to `market_brain_service.py:1180`. Verified 0 policy_blocked at 09:48 IST vs 6 at 09:42 IST.

### 2026-05-08 — Strategy audit recalibration
**Commit `9813d73`** · 4 strategies recalibrated based on backtest:
- MEAN_REVERSION: RSI ≤45/40 RANGE/other (was ≤40/35), VWAP extension ≥0.6% (was 1.0%)
- PULLBACK: RSI 35-70 BUY / 38-65 SELL (was 38-65/40-62), EMA distance ±5% (was ±3%)
- TREND_UP regime added MORNING_FADE / SHORT_BREAKDOWN / SHORT_PULLBACK to hard-blocks
- RECOVERY regime added (was missing → silent allow)

### 2026-05-08 — audit_log BQ writer fix
**Commit (within `9813d73` chain)**
`LogSink.flush_actions()` was clearing buffer without writing to BQ since launch. Added module-level `_DEFAULT_BQ` + `set_default_bq()` helper, wired in `container.py`. 9 new tests in `tests/test_audit_log_persistence.py`.

### 2026-05-08 — Backtest 1d candle path fix
Path was `cache/score_1d/` (stopped refreshing 2026-02-27). Fixed to `cache/candles/1d/` in `backtest/data.py`.

### 2026-04-23 — Redesign branch + AUDIT.md/DESIGN.md
**Commit `b3af94c` on `redesign/audit-and-design`**
End-to-end audit grounded in code with file:line citations. Companion DESIGN.md with target architecture (Edge / Thesis / Playbook / RiskCap abstractions). Pushed. Long-term track — not a near-term migration commitment.

### 2026-04-10 — Phase 8 complete (swing/intraday split + intelligence layer)
Multi-timeframe scoring, regime-strategy affinity matrix, swing entry gates, GTT broker SL on every swing, AMO premarket exit orders.

### Prior phases (1A–7)
GCP migration, Upstox order methods, Pub/Sub event bus, EOD reconcile, WebSocket monitor, BQ candle migration, Sheets removal from critical reads, Next.js dashboard. See `memory/project_phases_status.md` for detail.

---

## 9. Quick reference

### Today's scan distribution (verify per-day)
```sql
SELECT setup, COUNT(*) AS total, COUNTIF(qualified) AS qualified, ROUND(AVG(adjusted_score),1) AS avg_score
FROM autotrader.scan_decisions
WHERE run_date = CURRENT_DATE('Asia/Kolkata')
GROUP BY setup ORDER BY total DESC
```

### Today's closed trades
```sql
SELECT symbol, strategy, side, exit_reason, ROUND(pnl,2) AS pnl, hold_minutes,
       FORMAT_TIMESTAMP('%H:%M', entry_ts, 'Asia/Kolkata') AS entry_ist,
       FORMAT_TIMESTAMP('%H:%M', exit_ts, 'Asia/Kolkata') AS exit_ist
FROM autotrader.trades
WHERE trade_date = CURRENT_DATE('Asia/Kolkata') AND exit_ts IS NOT NULL
ORDER BY exit_ts
```

### Open positions
```sql
SELECT symbol, strategy, side, qty, ROUND(entry_price,2) entry, ROUND(target,2) tgt, ROUND(sl_price,2) sl,
       FORMAT_TIMESTAMP('%m-%d %H:%M', entry_ts, 'Asia/Kolkata') AS entry_ist
FROM autotrader.trades WHERE exit_ts IS NULL ORDER BY entry_ts
```

### Block-reason distribution
```sql
SELECT blocked_reason, COUNT(*) n
FROM autotrader.scan_decisions
WHERE run_date = CURRENT_DATE('Asia/Kolkata') AND NOT qualified AND blocked_reason IS NOT NULL
GROUP BY blocked_reason ORDER BY n DESC
```

### Run command shorthand
```bash
# bq with PATH set
export PATH="/Users/vishalrawat/google-cloud-sdk/bin:$PATH"
bq --project_id=grow-profit-machine query --use_legacy_sql=false --format=pretty "<SQL>"

# tests
cd "/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader" && pytest tests/

# deploy
gcloud run deploy autotrader \
  --source "/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader" \
  --region asia-south1 --project grow-profit-machine --account vishalrwt1995@gmail.com
```

### Service URLs
- autotrader: `https://autotrader-147177395303.asia-south1.run.app`
- dashboard: `https://autotrader-dashboard-147177395303.asia-south1.run.app`
- ws-monitor: internal only

---

## 10. Update protocol — keep this file alive

This file becomes useless if it goes stale. Update rules:

| Trigger | What to update |
|---|---|
| **Code shipped to main** | Append entry to §8 Recent History (newest first). Update §3 if revision changed. Update §5 if a tunable changed. Bump "Last updated" timestamp at top. |
| **Cloud Run deploy** | Update §3 revision row + bump timestamp. |
| **Scheduler change** | Update §3 cron list + add §8 entry. |
| **New strategy / hard-block / regime** | Update §5 + add §8 entry. |
| **Architecture change** | Update §2 file table if a service moved/added. Update `docs/ARCHITECTURE.md` for the deeper view. |
| **Open issue resolved** | Move from §7 to §8 with the resolution. |
| **New open issue surfaced** | Add to §7 with date + evidence. |
| **End of every working session** | At least: bump "Last verified live state" at top, even if nothing else changed. Confirms doc is fresh. |

Keep §8 Recent History dense — bullets, file:line, live-evidence. No retrospectives, no narrative; this is a load-bearing doc.

When a section grows past ~200 lines, factor out to its own doc under `docs/` and link from here. The goal is "one file you can read in 10 minutes and have full context."

**Companion docs (do not duplicate; link only):**
- `docs/ARCHITECTURE.md` — deep architectural view
- `docs/GCP_PRODUCTION_SETUP.md` — provisioning runbook
- `docs/UNIVERSE_RAW_FETCH_AND_BUILD_ALGORITHM.md` — universe pipeline detail
- `docs/redesign/AUDIT.md` + `docs/redesign/DESIGN.md` — long-term redesign track
- `docs/TRADING_SYSTEM_ENHANCEMENT_GUIDE.md` — enhancement roadmap (adaptive weights, options, execution, risk)

---

## 11. Cross-session memory

Claude's auto-loaded memory lives at `/Users/vishalrawat/.claude/projects/-Users-vishalrawat-Auto-Trading-Python-GCP/memory/`. The index is `MEMORY.md`. Each file there has a `system-reminder` showing its age — treat anything > 7 days as potentially stale and verify against this file.

**Authoritative ordering when sources disagree:**
1. Live `gcloud` / `bq` output (right now)
2. Repo code at `main` HEAD
3. This file (`docs/PROJECT_KNOWLEDGE.md`)
4. Memory files in `~/.claude/projects/.../memory/`
5. User's spoken statement in chat (often the latest intent — but verify the others before acting)

If sources disagree, surface the disagreement to the user before deciding.
