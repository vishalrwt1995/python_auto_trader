# Project Knowledge — Auto Trading System

> **Purpose:** Single source of truth for any Claude session, started at any time.
> **Read this file first** in every new chat. It is committed to the repo and updated continuously.
>
> **Last updated:** 2026-06-27 (Phase 1+2 regime cleanup committed — CHOP/EARLY_TREND removed, RECOVERY time-based fix; 874 tests green; NOT yet deployed to Cloud Run. See §8.) · Prior: 2026-06-26 (new Mac setup complete; gcloud v574 @ `/opt/homebrew/bin/gcloud`; prod verified: autotrader `00275-xt9` [PR #47 swing-regime-fold + PR #46 EOD-squareoff-fix], dashboard `00069-pjk`, ws-monitor `00044-rmw`. Prior: 2026-06-22 (**DASHBOARD 5-CHANNEL UI shipped (Phases 0-2, PAPER, additive): backend per-channel endpoints — PR #38, rev `autotrader-00273-hf9` (`/dashboard/channels/overview`,`/positions/by-channel`,`/core/basket`,`/gap-fade/shorts`; pure rollup, 401-verified) — plus a new `/channels` cockpit with click-to-expand drill-down [CORE 30-name basket+weights, gap-fade shorts, per-channel positions] — PR #39/#40, rev `autotrader-dashboard-00067-whw`. Channel model generalized swing/intraday→6 (`inferTradeChannel` routes PEAD/CORE/gap-fade by strategy). tsc + next build clean (17/17 pages); routes 200 live; local browser preview blocked (stale system node v15 + Firebase login-gate) → verified via prod next build. Residual polish (Command-Center strip, Settings per-channel, Positions/Journal/Analytics channel tabs) deferred — cockpit supersedes most. Prior same day: DAILY-ANALYSIS FIX shipped — PR #36, rev `autotrader-00272-t49`, PAPER.** First daily-ops pass caught swing+intraday **silently halted all session**: the CORE quarterly rebalance's 30 buy-and-hold entries (stamped today) tripped the GLOBAL `max_trades_day=5` counter → every swing/intraday scan SKIPped `max_trades_day_hit` (no entries, no `scan_decisions`). Root: `get_today_trade_count` counted ALL channels — Phase C made the daily PnL limits per-channel but left this trade-count gate global. **Fix:** channel-scoped the gate (`get_today_trade_count(channels=…)`; the scan passes `{swing}`/`{intraday}`/both, so core/pead/corp/gap_fade can't starve the active scanners) + repaired 2 gap-fade first-fire bugs (breaker called nonexistent `_channel_realized_today`/`list_closed_positions_today` → nan-trip blocking all entries; Upstox v3 `market-quote/ohlc` nests OHLC under `live_ohlc`, unparsed → `snapshot=0`). 271-test blast radius green; **live-verified** post-deploy: `scan_decisions` resumed 0→25 (13:52 IST), scans complete with real work (no skip), env byte-identical (PAPER + 5 capitals + CORE_ENABLED), ws-monitor/dashboard untouched (Rule 1 honored — origin/main was d910f99, additive merge). **gap-fade live-validates Tue 06-23 09:16.** See §8. **Prior 2026-06-21:** SHIPPED + **ENABLED** **CORE channel** — PR #34, revs `autotrader-00270-llh` + `ws-monitor-00044-rmw`, PAPER, **₹3L NEW capital [total now ₹12L]**. The system's **BETA engine**: quarterly buy-and-HOLD of the large-cap top-30 by a **momentum+low-vol rank-blend**, long-only CNC, own isolated channel [`channel`/`wl_type="core"`]. Validated deep-daily 2010-26: ~11% CAGR / −35% maxDD / Calmar 0.32 [survivor-inflated → **real ~9-10%**]; honest — it's **beta not alpha**, the return comes WITH a −35-40% drawdown stock-only can't hedge. **ENABLED:** `CAPITAL_CORE`=300000 + `CORE_ENABLED`=true + quarterly scheduler `autotrader-core-rebalance` [`30 9 1 1,4,7,10 *` IST, first fire **Jul 1 09:30**]. **e2e PROOF:** manually triggered `/jobs/core-rebalance` Sun 06-21 → universe=211, basket=30, **bought=27** [27 paper positions tagged `channel=core`, catastrophe-stops ~0.40×entry, ws-monitor holds them not EOD-squares]. **CASH-DRAG FIXED same session (PR #35, rev `autotrader-00271-v9k`):** the first run left ₹54k/18% idle (integer-share sizing skipped names >₹10k/slice + rounding residual). Shipped a **residual-cash sweep** (`plan_core_rebalance`: equal-weight base, then greedily deploy leftover cash into the most-underweight names, admitting names up to 1.5×slice/cap). **Book reset (27 paper positions closed, CORE_RESET) + re-triggered → 30 names / ₹300,251 / 100% deployed**, weights 2.5-4.5% (target 3.33%), MARUTI/POLYCAB/BAJAJ-AUTO now included, no name over the ₹15k cap. **Three half-built-I/O bugs caught+fixed pre-merge** [wrong `place_exit_order`/`place_entry_order` sigs would've rejected every order; 130d fetch window would've emptied the basket]. ws_monitor: `wl_type="core"` added to overnight-SL-only set [Rule 8 → ws-monitor redeployed `00044-rmw`]. **Monday 06-22 validation TODO: confirm the 27 core positions persist [NOT EOD-squared] + are untouched by swing/intraday/gap-fade scans.** See §8. **Prior (same day):** SHIPPED + **ENABLED** **GAP_FADE channel** — PR #32 + #33, rev `autotrader-00268-rtj`, PAPER, **LIVE Monday-armed**. The system's first validated systematic short: intraday-MIS short of NSE F&O >5% gap-ups, cover at the 15:25 EOD squareoff, 3% protective buy-stop. Own isolated channel [`channel`/`wl_type="gap_fade"`] — swing/intraday/PEAD/corp **byte-untouched**. Exit reuses the side-aware FSM + EOD watchdog = **zero new exit code** [verified]. Economics: OOS 2018-26 +0.58%/trade, ~+6.7%/yr per ₹1L @0.20 pilot, 6/9 yrs +. **ENABLED:** `CAPITAL_GAPFADE`=100000 + `GAPFADE_MAX_POSITIONS`=3 + scheduler `autotrader-gapfade-scan-0916` (Mon-Fri 09:16 IST, ENABLED). **GF-8b finished the live I/O** (PR #33): `upstox.get_ohlc_v3` (real open/high/low) + prev_close from `get_ltp_v3` cp [FRESH — candles_daily lags ~2.5wk, must not be the gap denominator] + F&O universe/keys from the Upstox master (`fetch_fno_universe`, 211 live). Prod-verified `/jobs/gapfade-scan` HTTP 200 → universe=211, candidates=0, entered=0 [Sunday, fails-closed clean]. **MONDAY 06-22 PREDICTION: 0–1 gap-fade shorts** (>5% F&O gaps are ~0.3/day; fires only if a name gaps >5%). **Monday validation TODO: confirm `get_ohlc_v3` returns live data at the 09:16 fire (snapshot>0) — its first live test.** ws-monitor `00043-sql` unchanged. See §8 + `docs/GAP_FADE_CHANNEL_PLAN.md`. **Prior 2026-06-20:** SHIPPED + ENABLED corp-action bonus/split sub-strategy — PR #31, revs `autotrader-00265-h2d` + `ws-monitor-00043-sql`, PAPER; live + Monday-ready [0 EVENT trades predicted Mon — corp: no qualifying events; PEAD: dormant on NIFTY −gate]; + ws_monitor Rule 8 overnight-SL-only fix [latent PEAD EOD-squareoff bug]. See §8 + `docs/EVENT_CHANNEL_CORP_ACTION_PLAN.md`. Prior: SHIPPED EVENT/PEAD channel — PR #27, rev 00260-b58, PAPER, ₹2L NEW capital; additive 3rd channel, swing/intraday byte-identical; NIFTY-50 −5% market gate [no-gate documented/env-flippable]; ~4% live/yr / ~23% MTM DD; fidelity-proven [2,042 candidates, 0 unexplained]; dormant until first scheduler fire Mon 06-23 ~08:40 IST; + dashboard backend PR #28 [pead_watchlist persist + /dashboard/pead endpoints; frontend tab deferred to Monday's data]; earlier: BREAKOUT rejected, swing arm 1.75R PR #26) · **Last verified live state:** 2026-06-19 ~16:40 IST (rev 00263-d74 serving 100%, PAPER, ₹5L swing + ₹1L intraday + ₹2L PEAD; PEAD GRIND-V2 live [anti-pump 0.75 / max-hold 60, OOS +42%]; scan HTTP 200 still correctly DORMANT [NIFTY −8.2%]; swing/intraday env unchanged, swing routes 401 — nothing affected)
>
> **If you are a future Claude session reading this:** verify the "Production State" section against live `gcloud` output before asserting current state — drift is possible. Then read the "Recent History" log (newest at top) for context on the last few sessions of work.

---

## 0. Quick start for a new chat

> **NEW (2026-05-29):** the canonical entry point is `/Users/apple/Projects_Migrated/Auto Trading Python GCP/CLAUDE.md` — read that FIRST, then this file. CLAUDE.md has the deploy hygiene rules, gcloud auth gotchas, working-style conventions, and routes to all knowledge docs. Don't skip it.
>
> Companion: `gcp_autotrader/docs/GLOSSARY.md` decodes project-specific terms (channel, tact, R-multiple, regime, etc.).

The user's preferred bootstrap prompt for any new chat:

```
Read /Users/apple/Projects_Migrated/Auto Trading Python GCP/CLAUDE.md and
/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/docs/PROJECT_KNOWLEDGE.md first.
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

Repo root: `/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader`
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
**gcloud config name:** `autotrader-groww` · gcloud binary: `/opt/homebrew/bin/gcloud` (v574.0.0, installed 2026-06-26)

> ⚠️ Always pass `--project grow-profit-machine --account vishalrwt1995@gmail.com` to every gcloud command. Active config alone is not sufficient.

| Service | Latest revision (verified 2026-06-26) | Notes |
|---|---|---|
| `autotrader` | `autotrader-00278-bhh` | **2026-06-27 (env-var fix): `SWING_MAX_HOLD_DAYS=20` set — `from_env()` was defaulting to 10 while backtest validated 20; positions were being cut 10 days early. No code rebuild.** **PR #48 (2026-06-27): swing MR Fix1 — RSI entry threshold 45→35 (universal, RANGE exception removed) + SELL disabled; 16 new tests; OOS Calmar 0.04→0.28 at ₹5L.** Prior: **breadth≥70% EMA200 gate + pb_slot (direct commits to main, 2026-06-26, rev 00276-n9k) — MOMENTUM/PULLBACK blocked when <70% of universe is above EMA200; last swing slot reserved for PULLBACK; 293 tests, no regressions.** Prior: **PR #47 (2026-06-26): swing regime fold — refined regimes collapsed to CORE-4 at the gates to restore backtest parity (`regime_affinity.py` + `trading_service.py` + `universe_service.py`; 85-test lock in `test_swing_core4_fold.py`). PR #46 (2026-06-22): EOD squareoff exemption broadened — CORE/PEAD/corp now exempt (not just swing) via `order_service.py`; 107-test lock in `test_eod_recon_overnight_skip.py`.** Prior: PAPER; **Dashboard per-channel API endpoints added (PR #38, 2026-06-22): 4 additive read-only GETs (`/dashboard/channels/overview`, `/positions/by-channel`, `/core/basket`, `/gap-fade/shorts`) for the channels UI — pure rollup (`build_channel_overview`, 4 tests), 401-verified, zero trading-path/schema/env change.** **Per-channel `max_trades_day` fix LIVE (PR #36, 2026-06-22): daily trade-count cap is now channel-scoped (`get_today_trade_count(channels=…)`) so CORE's 30-position rebalance can't halt swing/intraday; gap-fade breaker uses real `list_all_positions` + Upstox v3 `live_ohlc` parse fixed. Live-verified (scan_decisions resumed 0→25); zero env/channel changes; ws-monitor/dashboard untouched.** **CORE channel LIVE (PR #34 + #35, 2026-06-21/22): the BETA engine — quarterly large-cap top-30 momentum+low-vol buy-and-HOLD, own channel `channel`/`wl_type="core"`, `CAPITAL_CORE=300000` + `CORE_ENABLED=true`, scheduler `core-rebalance` [`30 9 1 1,4,7,10 *` IST, first fire Jul 1]; e2e-validated 27/30 paper positions [~18% cash drag at ₹3L granularity — see Open Items §7]; catastrophe-stop/unreachable-target so a stopless hold satisfies the entry-order contract. Three half-built-I/O bugs caught+fixed pre-merge. Zero edits to swing/intraday/PEAD/corp/gap-fade.** Prior: GAP_FADE channel LIVE (PR #32/#33). **CORP-ACTION sub-strategy LIVE (PR #31, 2026-06-20): bonus/split pre-meeting drift = 2nd EVENT sub-strategy sharing the ₹2L pool via channel="pead" (corp≤2 of 5 slots), own hard meeting-exit (wl_type="corp_action"), `CORP_MAX_POSITIONS=2`, seeded `corp_event_history` (856 keys), scheduler `corp-recon-0843`/`corp-scan-0847`, live-pipeline validated 0-orders; shipped-edge +1.54% net/event (look-ahead-free, robust IS+OOS). Zero edits to PEAD/swing/intraday.** Prior: EVENT/PEAD channel live + dormant + GRIND-V2 (PR #27/#28/#29/#30, 2026-06-19; rev 00263 = anti-pump 0.50→0.75 + max-hold 40→60, OOS-validated +42%)** — `CAPITAL_PEAD=200000`, `PEAD_RISK_PER_TRADE=3000`; daily scan `/jobs/pead-scan` + exit `/jobs/pead-reconcile` (scheduler `pead-scan-0845`/`pead-recon-0840`, Mon-Fri IST; first fire Mon 06-23). **Dashboard backend (PR #28):** scan persists candidates to `pead_watchlist` Firestore + `/dashboard/pead/watchlist` + `/dashboard/pead/summary` endpoints (frontend PEAD tab deferred → build vs Monday's real data). Additive — swing/intraday byte-identical (verified: brain updating post-deploy, swing routes 401, env unchanged). Prior code = swing arm 1.75R (PR #26) + edges #3/#7-soft (PR #25) + overhaul (PR #23) + FSM fix (PR #24) + Phase C v2.1; **₹5L swing** + ₹1L intraday + **₹2L PEAD**, swing risk ₹7,500, dedup, holiday-aware |
| `autotrader-ws-monitor` | `autotrader-ws-monitor-00044-rmw` | min-instances=1, holds Upstox WS loop, runs the exit FSM (`USE_EXIT_FSM_V1=true`). **2026-06-21 (PR #34): overnight SL-only set broadened to {swing, pead, corp_action, core} — CORE is a pure buy-and-HOLD; ws-monitor must NOT EOD-square it. Redeployed (Rule 8).** **2026-06-20 (PR #31): overnight SL-only set broadened to {swing, pead, corp_action} — EOD watchdog + FSM no longer square off EVENT-channel positions (fixed a latent PEAD EOD-squareoff bug; only pead/corp behaviour changed, none live → swing/intraday byte-identical).** **Separate image (`cloudbuild.ws.yaml`) — see CLAUDE.md Rule 8; had silently run May-15 code for a month until PR #24.** |
| `autotrader-dashboard` | `autotrader-dashboard-00070-gdz` | Next.js, Firebase Auth. **2026-06-26: CORE positions display fix — Target shows "HOLD · Jul 1" (not 11x placeholder), SL shows "CATASTROPHE" label, R:R bar hidden, badge corrected to CNC. URL: `https://autotrader-dashboard-147177395303.asia-south1.run.app`. Auth domain added to Firebase authorized domains.** **2026-06-22 (PR #39+#40+#42+#44): new `/channels` cockpit — per-channel cards (capital / today P&L / open positions / R-at-risk / slot cap / breaker state) + ₹12L totals header + click-to-expand drill-down (CORE basket + weights, gap-fade shorts, per-channel positions); Settings gained a per-channel capital-allocation section (PR #42); Positions/Journal/Analytics channel-filter tabs now cover all funded channels + Command-Center per-channel capital strip (PR #44). Channel model generalized to all 6. tsc + next build clean; routes 200 live.** |

**Live trading flags (autotrader env, verified 2026-06-17 15:12 IST):**
- `PAPER_TRADE=true` — **PAPER mode** (was `false` on 2026-05-08; flipped to paper since)
- **`CAPITAL=600000` (₹6L total)** · **`CAPITAL_SWING=500000` (₹5L — bumped ₹1L→₹5L 2026-06-17 for the live PAPER test of the new edges; deep-OOS showed the edge cost-crippled at ₹1L, economic ≥₹2L, ~saturated by ₹3L)** · `CAPITAL_INTRADAY=100000` ← Phase C v1 (2026-05-28): per-channel capital separation. Each channel has independent daily loss/profit circuit breakers — bad swing day no longer halts intraday and vice versa.
- **`SWING_RISK_PER_TRADE=7500`** (= 1.5% of ₹5L; scaled WITH capital 2026-06-17 — MUST move with `CAPITAL_SWING` or the 20% per-position cap leaves capital idle. Was 1500 at ₹1L.)
- `RISK_PER_TRADE=250` (intraday, unchanged)
- `MAX_DAILY_LOSS=3000` · `DAILY_PROFIT_TARGET=6000` (LEGACY shared, fallback only — Phase C uses `daily_loss_pct=0.03` / `daily_profit_pct=0.06` per channel: **SWING now 3%/6%×₹5L = ₹15k loss-halt / ₹30k profit-target**; INTRADAY 3%/6%×₹1L = ₹3k/₹6k)
- `SWING_MIN_SIGNAL_SCORE=45`
- **`SWING_MAX_HOLD_DAYS=20`** (set 2026-06-27 — was missing, causing `from_env()` default of 10 to be used; backtest validated 20)
- **Per-channel capital roster (verified 2026-06-21):** `CAPITAL_SWING=500000` + `CAPITAL_INTRADAY=100000` + `CAPITAL_PEAD=200000` + `CAPITAL_GAPFADE=100000` + **`CAPITAL_CORE=300000`** = **₹12L committed across 5 channels** (shared fallback `CAPITAL=600000` unchanged). **`CORE_ENABLED=true`** (CORE master switch; `GAPFADE_MAX_POSITIONS=3`). PAPER throughout.
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
- `autotrader-gapfade-scan-0916` — `16 9 * * 1-5` (gap-fade short scan, just after open)
- **`autotrader-core-rebalance`** — **`30 9 1 1,4,7,10 *`** (Asia/Kolkata; quarterly CORE buy-and-hold rebalance, 1st of Jan/Apr/Jul/Oct 09:30 IST; first fire 2026-07-01)

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

### C. ~~BREAKOUT 250 scans / 0 qualified~~ — MOOT 2026-06-18
Moot: breakout was screened → **no net edge** (see §7-G + §8 2026-06-18 BREAKOUT entry). The 0-qualified was benign; breakout stays disabled by design — there's no edge to enable.

### D. MORNING_FADE 235 scans / avg score 21.4 / 0 qualified
Score formula returns 75 hardcoded but observed avg 21.4 — suggests something is overriding. Investigation pending.

### E. Watchlist build duration 11-12 min (CPU=2 should help)
After CPU 1→2 bump, re-measure on next build cycle.

### F. PULLBACK swing gates may need bullish-divergence/reversal-candle confirmation
Per AUDIT.md §7.2 — discussion pending.

### G. ~~BREAKOUT swing needs VCP / cup-handle detection before re-enable~~ — RESOLVED 2026-06-18
**Screened → REJECTED.** Canonical Donchian N-day-high breakout (the low-overfit form) is net-negative after cost (gross +0.05R/trade < ~0.09R swing cost wall) and catastrophic as a portfolio (102–354% DD; negative at every capital/gate/era). VCP not worth building — the simple form is decisively cost-negative + VCP adds overfit risk. **Do NOT re-enable breakout: no net edge.** See §8 2026-06-18 BREAKOUT entry.

### H. Intraday edge → cross-sectional low-vol SWING test (next major thread)
Intraday audit concluded candle-intraday is **cost-walled** (see §8 2026-06-15 Phase A/B). The real edge found is **cross-sectional low-volatility** (gross +32%/yr, robust incl 2026) + regime-conditional range-momentum & reversal — but NET-negative as intraday (flat-EOD forces 100% daily turnover on a slow signal). **Next, on resume:** user is building **2010–2026 swing history**; test low-vol (+ conditional momentum/reversal) as a cross-sectional **market-neutral SWING** strategy on it. Harnesses: `backtest_v2/intraday_alpha_search.py`, `intraday_regime_diag.py`, `intraday_phase_b_gate.py`. Caveats: GROSS-only so far; low-vol **flips in PANIC** (regime-gate required); swing version needs overnight-risk + shortability + its own cost gate. **Executability (India):** retail can't hold overnight equity shorts, so the idealized long-short basket isn't directly tradeable — executable forms are **long-only min-vol** or **long-basket + short-Nifty-futures hedge**; Phase 0 MUST backtest the *executable* form (the +32% gross may not fully transfer), not the stock-vs-stock long-short. **Agreed plan (2026-06-15):** if validated, ship as a NEW `MARKET_NEUTRAL` channel (own dashboard panel + circuit breakers, NOT under swing/intraday), funded by **₹1L repurposed from the parked intraday channel** (total stays ₹2L). Phases: 0 validate (GATE: net-positive + robust 2010-26) → 1 design → 2 build → 3 PAPER → 4 live (explicit go only). **2010-26 deep history now BUILT** (swing audit 2026-06-17 — `gs://…/oos/candles_daily_deep.pkl`, prod's `score_1d` source, 2,506 syms × 2010-26) → **Phase 0 unblocked.** NB: swing-audit's long-only `realizedVol` scan showed ~no edge at 10d-forward — does NOT contradict this (different measurement); Phase 0 must test the *market-neutral, regime-gated, executable* form. Discuss before building.

### I. EVENT-channel corp-action (bonus/split) — SHIPPED + ENABLED (PAPER) 2026-06-20 · monitor first trade
Built + deployed + enabled (PR #31; see §8). Live shipped-edge **+1.54% net/event** (look-ahead-free, next-open, eq-weight benchmark, robust IS+OOS) — ~½ the +2.48% offset-headline after the look-ahead fix + NIFTY→eq-weight + next-open. **OPEN:** (1) monitor the FIRST live corp trade end-to-end (intimation→next-open entry, eq-weight anti-pump, hard meeting-day exit, wide protective SL, the ws_monitor overnight-skip) when a first-time bonus/split in an uptrend appears (~5/yr; none Monday). (2) Smart-entry (+0.45%, needs an intraday near-close order path) deferred. (3) Corp dashboard tab deferred (#56 — `corp_watchlist` Firestore + reuse the pead panel). (4) After a few live trades, consider folding to one EVENT capital pool / relaxing liquidity (≥2cr) if real slippage is benign.

---

## 8. Recent history (newest first)

> Append-only log. Each entry: date · revision/commit · what shipped · live evidence.

### 2026-06-27 (latest) — Regime cleanup Phase 1+2: remove CHOP/EARLY_TREND, fix RECOVERY (commits 48a347a + feb6a3d, PAPER — NOT yet deployed to Cloud Run)

**Goal:** reduce live brain's 9-regime set to 6 validated regimes. Three regimes were dead or broken: CHOP (structurally unreachable — TREND_DOWN elif fires before it), EARLY_TREND_UP (0.1% live emission, 8.2% for DOWN — miscalibrated thresholds), EARLY_TREND_DOWN (concept valid but miscalibrated). RECOVERY was dead because when PANIC finally exits, scores jump directly to RANGE/TREND_UP, bypassing RECOVERY.

**Phase 1 shipped (commit 48a347a) — 19 files, 874 tests pass:**
- `domain/models.py`: MarketRegimeV2 Literal trimmed to 6: TREND_UP, TREND_DOWN, RANGE, PANIC, RECOVERY, RANGE_ROTATING
- `domain/regime_affinity.py`: _AFFINITY, _HARD_BLOCKS (CHOP block removed), _SWING_SETUP_REGIMES (EARLY_TREND_UP removed from MOMENTUM/PULLBACK), _CORE4_FOLD (now single entry: RANGE_ROTATING→RANGE)
- `services/market_brain_service.py`: CHOP detection block removed; EARLY_TREND_UP/DOWN upgrade block removed; RECOVERY now requires prev in {PANIC, TREND_DOWN} only
- `services/settings.py`: chop_stress_min/leadership_max/appetite_max params removed
- `backtest_v2/brain_reconstruct.py`: CORE_MAP = {RANGE_ROTATING: RANGE}
- `scoring.py`, `edge.py`, `market_policy_service.py`, `trading_service.py`, `ws_monitor_service.py`: all CHOP/EARLY_TREND refs cleaned
- `tests/test_phase_d_tactical_trend.py`: deleted (tested removed regimes)

**Phase 2 shipped (commit feb6a3d) — 2 files:**
- `market_brain_service.py`: After PANIC guard passes, force regime="RECOVERY" (no score bypass). RECOVERY holds for 4 calendar days (~4 trading sessions) using regime_age_seconds before promoting to RANGE/TREND_UP.
- `tests/test_market_brain_pr1.py`: 2 new tests for Phase 2 RECOVERY behavior.

**Next steps:** Phase 3 (regenerate regime_v2_2015.json with fixed brain) and Phase 4 (revalidate backtest on new regime data) — pending user go-ahead.

**Deploy note:** These are code commits to main. Deploy to Cloud Run requires `gcloud run deploy` per CLAUDE.md Rule 1. Not yet deployed.

---

### 2026-06-27 — Prod alignment: SWING_MAX_HOLD_DAYS=20 + script Fix1 patch (rev autotrader-00278-bhh, PAPER)

**Goal:** full prod-vs-backtest alignment audit. Found one real gap: `swing_max_hold_days` defaulted to 10 via `from_env()` even though the dataclass default and backtest both use 20. Prod was cutting swing positions 10 days early.

**What shipped:**
- **Env-var only:** `gcloud run services update --update-env-vars SWING_MAX_HOLD_DAYS=20` → `autotrader-00278-bhh`. ~90s, no rebuild.
- **`settings.py:428`:** `from_env()` default updated `10 → 20` so future deploys without env var set also get 20.
- **`scripts/redesign/swing_prod_faithful.py`:** `mr_direction()` updated to Fix1 logic (RSI ≤35 → BUY, SELL disabled; regime arg kept for signature compatibility but unused). Script was previously simulating pre-Fix1 behavior; now matches live `scoring.py`.

**Evidence:** `SWING_MAX_HOLD_DAYS=20` confirmed in `gcloud` env output post-deploy. All 20 other swing alignment checks passed in the same audit (regime gates, entry gates, sizing, exit arm/trail, capital, breadth/b200/rs_vs_mkt/mr_above_200 gates, pb_slot, RANGE_GROUP_CAP).

---

### 2026-06-27 — Swing MR Fix1: RSI ≤35 BUY-only (PR #48, rev autotrader-00277-2rv, PAPER)

**Goal:** fix swing MEAN_REVERSION drag. OOS 2020-2026: RSI 35–45 zone drove 4,006 SL-exit trades at WR 0% (-₹60L gross). Tightening to RSI ≤35 + disabling SELL flips MR from -₹50k drag to +₹48k contributor at ₹5L.

**What shipped (1 file, 16 new tests):**
- `domain/scoring.py`: two aligned changes — `determine_direction()` swing MR branch: early-return "BUY" if daily RSI ≤35 else "HOLD" (no regime exception, no SELL). `check_swing_entry()` MR gate: SELL always returns `swing_mr_sell_disabled`; BUY gate tightened to `rsi_daily > 35` → `swing_mr_daily_rsi_not_oversold`.
- `tests/test_swing_mr_fix1.py`: 16 tests covering SELL-blocked (all regimes), RSI boundary 35/36, RANGE exception removed (RSI 44 now blocked), intraday path unchanged.

**Backtest evidence (OOS 2020-2026):** ₹5L net +₹1,12,229 (was +₹26,475); CAGR 3.3% (was 0.8%); DD -11% (was -22%); Calmar 0.28 (was 0.04). IS 2015-2019 Calmar unchanged at 0.07 — not overfitting.

**Deploy:** PR #48 merged squash → `gcloud run deploy autotrader` → `autotrader-00277-2rv` serving 100%. ws-monitor and dashboard untouched (no exit-logic change; Rule 8 not triggered). 16 new tests + 426 prior passing.

**First-deploy behavior:** swing MR will fire less often (RSI 35–45 trades now HOLD). Expect fewer MR signals vs current; any MR entry in prod will be RSI ≤35 BUY only.

---

### 2026-06-26 — Breadth≥70% EMA200 gate + pb_slot (direct commits to main, rev autotrader-00276-n9k, PAPER)

**Goal:** ship the validated breadth gate from the 11-yr survivorship-free swing backtest (OOS 2020-2026 Calmar 0.79 vs 0.10 unfiltered baseline; IS/OOS split confirmed; step-up gradient 66-71% validates the threshold isn't a cliff).

**What shipped (6 files, 293 tests, 0 regressions):**
- `market_breadth_service.py`: compute `aboveEma200Pct` per stock — stocks with ≥201 daily bars counted above their EMA200; 0.0 sentinel when no eligible stocks.
- `domain/models.py`: `breadth_ema200_pct: float = 50.0` on `MarketBrainState` (backward-safe default; `0.0` from old Firestore docs → gate bypassed on first deploy).
- `market_brain_service.py`: wire field — Firestore `_state_from_dict` + `_build_state` from breadth snapshot + BQ row (unknown column silently dropped until ALTER TABLE).
- `trading_service.py`: two new swing `policy_block_reason` cases:
  - `swing_breadth_ema200_below_70`: MOMENTUM+PULLBACK blocked when `breadth_ema200_pct < 70.0` and `> 0.0` (0.0 = not-yet-populated).
  - `swing_pb_slot_reserved`: non-PULLBACK strategies limited to `swing_max_positions - 1` slots; last slot reserved for PULLBACK.
- New test file `tests/test_breadth_service.py` (3 tests); `tests/test_swing_selection.py` updated with 2 structural guard assertions.

**Deploy:** commit `c15c9d0` pushed to `vishalrwt1995/python_auto_trader main` → `gcloud run deploy autotrader` → `autotrader-00276-n9k` serving 100% traffic. Zero errors in startup logs. Dashboard and ws-monitor untouched.

**Env vars (no change needed):** `SWING_MIN_SIGNAL_SCORE=45` was already set in prod.

**First-deploy behavior:** `breadth_ema200_pct` field missing from current Firestore brain doc → reads as `0.0` → gate bypassed → MOMENTUM/PULLBACK trading continues unchanged. Gate activates after the next brain premarket run (08:30 IST) which will write the field for the first time.

**Expected prod impact (next trading day):** `scan_decisions.blocked_reason="swing_breadth_ema200_below_70"` will appear on low-breadth days; `swing_pb_slot_reserved` will appear rarely (PULLBACK fires ~5/yr). Calmar improvement from ~0.10 baseline to ~0.67-0.79 OOS is the validated backtest expectation over many months of trading.

---

### 2026-06-26 — Score-cache-0705 OOM root-cause identified + scheduler fix applied (env-only, no code deploy)

**Problem:** Cloud Run instance was dead/restarting from ~07:39-08:20 IST. All early-morning critical jobs (pead, corp, gap_fade 09:16, swing-0922) ran during/after recovery. Root cause: the `score-cache-update-close-0705` scheduler job has `attemptDeadline=1800s` (30 min), but was configured with `api_cap=1800&run_intraday_update=true&intraday_api_cap=1800`. At 07:05 IST, the expectedLCD flips overnight by one trading day (yesterday's close becomes the new expected), making all ~2,600 universe symbols stale simultaneously. At ~1 API call/sec, 1800 calls = 30 min exactly → scheduler disconnects at 1800s with status 4 (UNKNOWN/504), Cloud Run (timeoutSeconds=3600) keeps processing as a zombie. The instance accumulates memory loading candle data for ~2,600 symbols over the remaining 13+ min and OOM-crashes (confirmed: early logs missing, later scans visible, UNREACHABLE responses for 0820 job → zombie + no SIGTERM). Lock `score_cache_update_close` (TTL=3600s) also blocks the 0820 job's lock check until ~08:05 IST.

**Why reducing api_cap is safe:**
1. At scan time (09:22 IST), `trading_service._prefetch_candles_parallel` fetches **fresh 1D candles live from Upstox** (lookback_days=120) — NOT from the GCS score cache. `daily_bias` and `check_swing_entry` are computed from this live fetch. Score cache staleness does NOT affect signal quality.
2. The GCS score cache is only used by `build_watchlist` for universe ranking (`universe_score`). One-day-stale data on RSI/EMA over 700 bars = negligible ranking change.
3. The 0820 job (api_cap=600, `attemptDeadline=1800s`) covers 600 more symbols by 08:30 IST — still well before swing scan at 09:22.
4. The 1600 job (api_cap=1800) does the definitive full-universe refresh each EOD.
5. `run_intraday_update=false` is safe: at 07:05 IST, today's 5m data doesn't exist (market hasn't opened), and the previous 1600 run already populated yesterday's 5m cache. The 0820 job handles any remaining 5m gaps.

**Fix (env-only, via `gcloud scheduler jobs update`):**
- **Before:** `api_cap=1800&run_intraday_update=true&intraday_api_cap=1800` → ~2600 API calls → 43 min → OOM
- **After:** `api_cap=400&run_intraday_update=false` → ~400 API calls → ~7 min → clean finish within 1800s deadline
- No code deploy needed (Rule 4). Applied 2026-06-26 ~01:00 IST. Verified via `gcloud scheduler jobs describe`.

**Today's health check (normal operation post-crash, ~10:42+ IST onward):**
- Brain: RANGE_ROTATING all day, tact=77.9, trend_score=25.5, risk=NORMAL. Writing to BQ normally.
- All 33 positions (30 CORE + 3 swing) held correctly — EOD squareoff fix (PR #46) confirmed working (15:25/27/29 IST logs showed CORE/swing correctly skipped).
- Scans running normally from 10:42 IST onward after instance recovery.

**Pending / still-open:**
- `scan_decisions` BQ table showed 0 rows today — separate issue from the outage, BQ write path not yet investigated.
- gap_fade 09:16 live validation still unconfirmed (2nd consecutive day — UNREACHABLE during crash window).

---

### 2026-06-26 — New Mac setup + prod sync (no code change)

**Goal:** migrated to new Mac (username: apple). gcloud SDK installed (`/opt/homebrew/bin/gcloud` v574.0.0). ADC configured. All memory files restored. CLAUDE.md + PROJECT_KNOWLEDGE.md paths updated. Prod state verified: autotrader `00275-xt9`, dashboard `00069-pjk`, ws-monitor `00044-rmw`.

---

### 2026-06-26 — Swing regime fold to CORE-4 (PR #47, rev autotrader-00275-xt9, PAPER)

**Goal:** restore backtest parity — refined intermediate regimes were blocking valid swing setups at the gate. Collapsed to CORE-4 (`TREND_UP`, `TREND_DOWN`, `RANGE`, `CHOP`) in `regime_affinity.py` + `trading_service.py` + `universe_service.py`. 85-test lock in `test_swing_core4_fold.py`. Dashboard + ws-monitor untouched.

---

### 2026-06-22 — EOD squareoff exemption broadened (PR #46, rev autotrader-00274-?, PAPER)

**Goal:** CORE/PEAD/corp positions were being EOD-squared despite being long-hold channels. Fixed `order_service.py` to exempt all three (not just swing). 107-test lock in `test_eod_recon_overnight_skip.py`.

---

### 2026-06-22 — Dashboard 5-channel UI, Phases 0-2 (PR #38/#39/#40; revs autotrader-00273-hf9 + autotrader-dashboard-00067-whw, PAPER)

**Goal:** the algo grew to 5 channels / ₹12L but the dashboard still modelled only swing/intraday (even mislabeling CORE/PEAD/gap-fade). User: "make the best and no issues." Shipped Phases 0-2 of a dashboard channels update; additive, no trading-path impact.

- **Phase 0 — backend (PR #38, autotrader-00273-hf9):** 4 additive read-only GETs feeding the UI, all data already computed server-side: `/dashboard/channels/overview` (per-channel capital, today P&L, open positions, R-at-risk, slot cap, daily-breaker state), `/positions/by-channel`, `/core/basket` (holdings + entry-notional weights), `/gap-fade/shorts`. `build_channel_overview` is a pure rollup (4 unit tests). Firebase-auth like the rest; routes 401-verified live. Zero trading-path / schema / env change.
- **Phase 1 — cockpit (PR #39, dashboard-00066):** new `/channels` page + nav entry — per-channel cards + ₹12L totals. Channel model generalized swing/intraday→6 (`Channel` type, `CHANNEL_META`/`CHANNEL_ORDER`, api methods). `inferTradeChannel` now routes PEAD/GAP_FADE/CORE/corp by `strategy` instead of mis-bucketing them into swing/intraday (analytics accumulator made channel-agnostic to match — caught by tsc).
- **Phase 2 — drill-down (PR #40, dashboard-00067-whw):** cockpit cards expand to detail — CORE 30-name basket + weights, gap-fade shorts (15:25 cover), per-channel open positions. Reuses the Phase-0 endpoints.
- **Verification:** `tsc --noEmit` clean + `next build` clean (17/17 static pages, /channels built) under node v24 (Cloud Build runs node 20). **Local browser preview blocked** — the stale system node (v15) can't run `next dev` and the page is Firebase-login-gated, so a fresh preview can't show real data; the prod `next build` is the equivalent gate. Live routes 200 (`/`, `/channels`, `/positions`). Rule 1 honored on every deploy.
- **Phase 3 — Settings (PR #42, dashboard-00068-l8t):** added a per-channel capital-allocation section to the Settings page (capital / slot cap / daily stop per channel + ₹12L total, from `/channels/overview`) — corrects the single global `capital` figure shown above it.
- **Residual SHIPPED (PR #44, dashboard-00069-pjk):** Command-Center per-channel capital + today-P&L strip (links to /channels) + Positions/Journal/Analytics channel-filter tabs made dynamic over all funded channels present in the data (CHANNEL_META-labeled, channel-colored). The dashboard 5-channel UI is now complete — overview cockpit + drill-down + per-page channel tabs + Settings allocation. tsc + next build clean; all routes 200.

### 2026-06-22 — Daily analysis: per-channel max_trades_day fix + gap-fade repairs (PR #36, rev autotrader-00272-t49, PAPER)

**Context:** user repurposed this thread for DAILY live-system analysis (channels working? pipelines ran? trades missed? data/pipeline broken?). First pass, Mon 06-22 mid-session.

**Found — 2 bugs:**
- **Swing + intraday silently HALTED all session.** The CORE quarterly rebalance entered **30 buy-and-hold positions stamped today**; the GLOBAL `max_trades_day=5` counter (`get_today_trade_count`) counted all 30 → `run_scan_once` SKIPped `max_trades_day_hit` on every swing/intraday scan (no entries, and **zero `scan_decisions`** — the skip precedes the decision loop). Phase C made the daily PnL limits per-channel but left this trade-count gate global. Would auto-heal next day, but recurs every CORE rebalance.
- **gap-fade first armed fire broken** (caught from the 09:16 log): (1) `_channel_realized_today` called `pead_trading_service._channel_realized_today` + `state.list_closed_positions_today` — **neither exists** → breaker returned `nan` → would block all entries; (2) Upstox v3 `market-quote/ohlc` nests OHLC under `live_ohlc`, which `_extract_quote_from_row` (v2 `ohlc`-only) ignored → open=0 → open-snapshot dropped all 211 symbols (`snapshot=0`). Live-probed the v3 response to confirm.

**Fix (PR #36, +215/−21, +8 tests):** `get_today_trade_count(channels=…)` + the scan gate passes `{swing}`/`{intraday}`/both → passive channels (core/pead/corp/gap_fade) can never consume the active scanners' budget; gap-fade breaker uses real `state.list_all_positions` (mirrors `_pead_realized_today`, fail-closed); parser reads v3 `live_ohlc`/`prev_ohlc`. Blast radius **271 passed / 0 failed**.

**Ship:** PR #36 merged (c22c4f2) → main dir synced (**Rule 1** — origin/main was d910f99, additive, all channel work intact) → deployed autotrader **00271-v9k → 00272-t49** (ws-monitor `00044-rmw` / dashboard untouched — no exit change). **Live-verified post-deploy:** a scan on 00272 proceeded past the gate (no skip) and `scan_decisions` resumed **0 → 25 rows (13:52 IST, intraday, scanned=25/150)**; env byte-identical (PAPER + CAPITAL_SWING/INTRADAY/PEAD/GAPFADE/CORE + CORE_ENABLED + risk). PAPER throughout.

**OPEN:** gap-fade live-validates at the next 09:16 fire (Tue 06-23) — confirm `snapshot>0` + breaker reads a real number, not nan. Healthy 06-22: brain writing (RANGE_ROTATING), all scheduler jobs HTTP 200, PEAD/corp correctly dormant (NIFTY −8.8% < −5% gate), CORE 30 positions held + not squared.

### 2026-06-21 — CORE channel SHIPPED + ENABLED (PR #34, revs autotrader-00270-llh + ws-monitor-00044-rmw, PAPER, ₹3L NEW)

**Goal:** the user challenged "6-7%/yr is less than a bank FD; my Groww portfolio did >20% last year — what are we lacking?" → the **BETA insight**: every channel we'd built is thin *alpha* (~5-6%/yr, market-adjusted); the missing piece is owning *beta* (buy-and-hold). Decided **stock-only** (no options/derivatives/gold). Built the CORE channel: the system's beta engine.

**What shipped — CORE channel (large-cap momentum+low-vol buy-and-HOLD):**
- **Edge/economics:** each quarter rank the ~211 F&O large-caps → top-100 by turnover → **top-30 by a momentum(12-1) + low-vol(60d) rank-blend** → buy equal-weight, hold, rebalance. Long-only CNC. Validated deep-daily 2010-26: **~11% CAGR / −35% maxDD / Calmar 0.32**; survivor-inflated → **real ~9-10%**. Honest: this is **beta, not alpha** — the return comes WITH a −35-40% drawdown that stock-only CANNOT hedge (200-DMA/vol-target/abs-mom all *lower* Calmar — Indian crashes are V-shaped). It is the price of the return; size to tolerance.
- **Code (PR #34, +596/−2, 14 CORE tests):** `domain/core_signals.py` (PURE selection + `catastrophe_stop`/`unreachable_target` helpers, fidelity-replayed against the deep cache — reproduces the blend) + `core_signal_service` (target-basket builder, **CORE-sized 600d** daily window) + `core_trading_service` (quarterly `plan_core_rebalance` PURE + live PAPER wrapper) + settings (`capital_core`/`core_enabled` + `channel_capital('core')`) + `container.run_core_rebalance` + `/jobs/core-rebalance` + ws_monitor (`wl_type="core"` → overnight-SL-only set, **Rule 8**).
- **THREE half-built-I/O bugs caught + fixed pre-merge** (the e2e discipline working): (1) `place_exit_order` called with the wrong signature → every sell would throw; (2) `place_entry_order` hard-rejects `sl<=0` → every stopless CORE buy would be rejected [fixed with catastrophe-stop entry×0.40 + unreachable-target entry×11, protective-only ~0 fidelity impact]; (3) reused pead's 130d fetch (<252 momentum lookback) → empty basket every quarter [rewrote to 600d].
- **Ship:** PR #34 merged (0f91ab6) → main synced (Rule 1) → deployed autotrader (00269-2bk) **+ ws-monitor (00044-rmw, Rule 8 — overnight set changed)** → enabled `CAPITAL_CORE=300000` + `CORE_ENABLED=true` (00270-llh) + scheduler `autotrader-core-rebalance` (`30 9 1 1,4,7,10 *` IST, first fire Jul 1). Blast-radius **463 passed** (4 pre-existing date-dependent test_watchlist_v2 fails, unrelated).
- **e2e PROOF (manual trigger, Sun 06-21):** `/jobs/core-rebalance` → universe=211, basket=30, **bought=27** (27 paper positions tagged `channel=core`, catastrophe-stops ~0.40×entry verified; pre-enable the route correctly returned `skipped:core_disabled`). PAPER preserved; swing/intraday/PEAD/corp/gap-fade env **byte-untouched**.
- **CAVEAT → FIXED same session (PR #35, rev 00271-v9k):** the first run deployed only ₹246k/₹3L (**18% idle**) — 3 names >₹10k/slice skipped (MARUTI/POLYCAB/BAJAJ-AUTO) + rounding residual on the 1-share ₹4.5-8.5k names. Shipped a **residual-cash sweep** in `plan_core_rebalance` (equal-weight base → greedily deploy leftover into the most-underweight names, admit up to 1.5×slice cap, exclude >cap). User authorized "reset + re-trigger": closed the 27 paper positions (CORE_RESET, flat) + re-ran → **30 names / ₹300,251 / 100% deployed**, weights 2.5-4.5% (target 3.33%), no name over the ₹15k cap. 8 CORE-trading tests (incl. sweep + cap-exclusion); deployed BUY-idempotency safe (date-scoped `fired_key` rolled to 06-22).
- **Monday 06-22 validation TODO:** confirm the 27 `core` positions persist (NOT EOD-squared by ws-monitor `00044-rmw`) and are untouched by the swing/intraday/gap-fade scans (channel isolation).

### 2026-06-21 — GAP_FADE channel SHIPPED (PR #32, rev autotrader-00266-j4z, PAPER, DEFAULT-OFF)

**Goal:** after a deep swing dig concluded swing is near-optimal/tuning-resistant (5 disciplined "improvements" all failed validation — pbTU-cut, cover-timing trail, market-filter, earnings-tilt, parameter-mine), pivot to **uncorrelated channels**. User: "do both 1-by-1 e2e" (gap-fade first, then momentum-neutral). Then "open PR, review, test, merge, deploy and test on prod."

**What shipped — GAP_FADE channel (the system's first validated systematic SHORT):**
- **Edge:** intraday-MIS short of NSE F&O stocks gapping **>5%** at the open, cover at the **15:25 EOD squareoff**, **3% protective buy-stop**. Validated deep daily 2010-26 (F&O, price≥₹30, net 0.27% MIS + 0.25% slip): **OOS 2018-26 +0.58%/trade, ~+6.7%/yr per ₹1L @0.20 pilot, 6/9 yrs +**. Diversifying (earns in the down tape that hurts the long book). Cover-timing/trailing/market-filter all tested on the real 1m intraday path (BQ candles_1m, 2022-26, 475 events) and **rejected** — hold-to-close optimal.
- **Code (PR #32, +940/−0 additive, 37 tests):** `domain/gap_fade_signals.py` (pure gates + fade economics, fidelity-replayed) + `gap_fade_signal_service` (open snapshot → ranked shorts) + `gap_fade_trading_service` (own-channel book: own capital/slot-cap/3%-6% breaker, MIS SELL @0.20×cap, 3% buy-stop) + settings (`capital_gapfade` + `gapfade_*` + `channel_capital('gap_fade')`, default-off) + `container.run_gap_fade_scan` + `/jobs/gapfade-scan`.
- **Exit = ZERO new code (verified):** the exit FSM is already side-aware (`_crossed_sl`: `ltp≥sl` for SELL); `gap_fade` is NOT in `_OVERNIGHT_SL_ONLY_WL` → ws_monitor SL-monitors intraday + EOD-covers at 15:25; `exit_side` opposite-of-entry (short → BUY cover). So **no ws_monitor redeploy needed** (Rule 8: no exit-logic change). Elegant reuse.
- **Ship:** PR #32 merged (a89a8a2) → main dir synced (Rule 1) → deployed autotrader (00265→00266-j4z) [ws-monitor 00043-sql unchanged]. Blast-radius **374 passed** (4 pre-existing date-dependent test_watchlist_v2 fails, unrelated — diff doesn't touch those files).
- **Prod-verified:** `/jobs/gapfade-scan` HTTP 200 → `{"skipped":"gap_fade_disabled"}` = route live + channel safely **dormant** (no orders). PAPER preserved; CAPITAL_SWING/INTRADAY/PEAD + CORP_MAX_POSITIONS **unchanged**; CAPITAL_GAPFADE/GAPFADE_MAX_POSITIONS unset (=0, off).
- **ENABLED (same day, PR #33 + GF-8b):** the "enable" turned out to need a **live-I/O finish, not just an env flip** — the channel shipped with placeholder I/O (`get_ohlc_quotes` didn't exist; `fno_underlyings` table didn't exist). User feedback: *"do not ship half-built features; every change e2e + a Monday-validation result."* (see `feedback_e2e_no_half_built`). GF-8b (PR #33, rev 00267→00268): added `upstox.get_ohlc_v3` (real open/high/low), prev_close from `get_ltp_v3` cp (FRESH — candles_daily lags ~2.5wk), F&O universe+keys from the Upstox master (`fetch_fno_universe`, 211 live-validated). Then enabled: `CAPITAL_GAPFADE`=100000 + `GAPFADE_MAX_POSITIONS`=3 + scheduler `autotrader-gapfade-scan-0916` (ENABLED, Mon-Fri 09:16 IST). Prod scan HTTP 200 → universe=211, candidates=0, entered=0 (Sunday, fails-closed clean). **Monday 06-22 prediction: 0–1 gap-fade shorts.** **Open Monday-validation: confirm `get_ohlc_v3` returns live data at 09:16 (snapshot>0) — its first live test;** monitor any short's SL/EOD cover.
- **Channel 2 NEXT (per "both 1-by-1"): FACTOR/MOMENTUM-NEUTRAL.** Fresh-validated this session: low-vol REJECTED (no alpha, −4%/yr neutral), but **cross-sectional 12-1 momentum has real neutral alpha** (+7.9%/yr F&O, 13/16 yrs, bear-positive +0.76%/mo, recent-robust 2023-26) — survivorship-inflated magnitude (real likely ~+3-5%), needs futures-hedge (new instrument class) + deeper survivorship check before build. Scope → validate → futures decision → build (gated).

### 2026-06-20 — Corp-action sub-strategy SHIPPED + ENABLED live (PR #31, revs autotrader-00265-h2d + ws-monitor-00043-sql, PAPER)

**Goal:** user said review + merge + deploy the locked corp-action (bonus/split) edge, make ready for Monday, predict trades. Shipped as the **2nd sub-strategy of the EVENT/PEAD channel.**

- **Code (PR #31, +1,118, 39 unit tests):** `domain/corp_action_signals.py` (pure) + `corp_action_{signal,trading,reconciliation}_service.py` + settings/container/web wiring (`/jobs/corp-{scan,reconcile}`). Shares the EVENT ₹2L/5-slot pool via `channel="pead"` (PEAD already counts book+breaker by channel → **zero edits to PEAD/swing/intraday**). Exit isolated via `wl_type="corp_action"` (hard meeting-day close exit, NOT PEAD's trail; wide protective SL backstop). Anti-pump market-adjusted by the **eq-weight universe** (right benchmark for small/mid; NIFTY-50 under-adjusts). **Shipped-edge +1.54% net/event, robust IS+OOS** (~½ the +2.48% offset-headline after look-ahead fix [dist at intimation-close not entry-close] + NIFTY→eq-weight + next-open-vs-smart).
- **Rule 8 fix (caught in review):** the ws_monitor EOD watchdog + exit FSM only exempted `wl_type=="swing"` → pead/corp would be EOD-squared at 15:25 + intraday-managed = **latent PEAD bug** (never fired — pead dormant). Broadened the overnight SL-only set to `{swing, pead, corp_action}` (`_is_overnight_sl_only`); deployed ws-monitor (Rule 8). Only pead/corp behaviour changed (none live) → swing/intraday byte-identical. Updated the EOD tripwire test.
- **Deploy + enable:** merged PR #31 → main (Rule 1 sync) → deployed autotrader (00263→00264 code → 00265 enable) + ws-monitor (00042→00043). Full suite **404 passed** (4 pre-existing date-dependent `test_watchlist_v2` failures, proven unrelated via stash). Enabled: seeded `corp_event_history/all` (856 first-time keys), created scheduler `corp-recon-0843`/`corp-scan-0847` (Mon-Fri IST), flipped `CORP_MAX_POSITIONS=2`. **PAPER preserved.**
- **Live validation:** `/jobs/corp-scan` full pipeline ran clean (last_session 2026-06-19, events=2, candidates=0, entered=0, **no crash** — I/O wrapper proven, unlike the PEAD HTTP-500 bug history); reconcile checked=0. **Monday (06-22) prediction: 0 EVENT trades** — corp: no first-time bonus/split *entering* Monday; PEAD: dormant (NIFTY >5% drawdown). Swing/intraday run as usual. First corp trade fires on the next qualifying first-time bonus/split in an uptrend (~5/yr liquid).
- **OPEN:** monitor the first live corp trade lifecycle; smart-entry (+0.45%) + corp dashboard tab deferred. See §7-I.

### 2026-06-20 — Corporate-action edge sweep + bonus/split LOCKED (RESEARCH, no prod change)

**Goal:** user asked to hunt more EVENT-channel profit. Exhaustively swept the NSE corporate-action calendar with OOS discipline. All local scratch (`~/.autotrader_backtest_cache/`), **no prod changes — swing/intraday/PEAD byte-identical, still PAPER.**

- **Bonus/split pre-meeting run-up = the one tradeable LONG corporate-action edge.** Event-specific (placebo control: random windows on same names +0.20% vs +1.30% into the meeting), OOS-robust. Crude pooled +0.73% → **SELECTED +2.48% net/event** (liquid ≥10cr, IS +2.65 / OOS +2.43) via first-time + uptrend(≥40% above 52w-low) + anti-pump(<+6% 20d) + lead≥4d + smart entry + meeting-close exit. Built board-meeting archive **with intimation timestamps** (`/api/corporate-board-meetings` `bm_timestamp`; median ~7-day lead) — the tradeability key. **Lesson:** fixed-offset window-sweep OVERSTATED the edge 4× vs the exact-intimation backtest; always backtest real intimation dates.
- **Combined EVENT backtest** (PEAD grind-v2 + corp, shared ₹2L/5-slot pool, 2010–26): corp adds **~+2.4–3.4%/yr**, **monthly corr ≈ 0** with PEAD, only 15 slot-collisions/16y, **smooths weak PEAD years** (2024 PEAD +₹1.1k → combined +₹29.4k). Correction (caught via breaker-off check): it does NOT cut DD on its own — raises gross deployment (clean DD 21%→27%); the daily −3%/+6% breaker tames it → **size modestly (cap corp at 2 of 5 slots).**
- **REJECTED:** buyback (negative mkt-adj); **merger/delist/fundraise/dividend on the LONG side** (duds — "selected" winners were n=3–9 overfit noise, OOS-negative; discipline caught the trap). Only other real edge = **fundraise dilution-FADE** (short, robust on F&O subset but thin +0.3–0.95% and needs F&O infra → below bar).
- **Verdict:** the long-only EVENT channel is **at its ceiling (PEAD + bonus/split)**. Next profit = the **intraday 5m hunt** (proposed) or committing to F&O for the fade.

**Locked:** `docs/EVENT_CHANNEL_CORP_ACTION_PLAN.md` (full build spec + locked config). Memory: `project_corp_action_edge.md`. Build pending user go (sequence after PEAD's first live trade, or build isolated).

### 2026-06-19 — PEAD GRIND-V2 shipped (PR #30, rev 00263-d74, PAPER) — +42% OOS-validated

**Goal:** user asked to grind PEAD for max profit. Did an OOS-disciplined one-at-a-time sweep (IS 2010-17 / OOS 2018-26) over all knobs — guarded against overfit (we only trust changes that help BOTH halves + form a smooth plateau, given the prior forensic "over-filtering hurts"). Scratch: `~/.autotrader_backtest_cache/pead_param_grind.py` + `pead_grind_combo.py`.

**Two robust, stacking, economically-motivated wins** (rejected arm/trail/slots — those only added return via higher DD; gate/atr/surprise already optimal):
- **anti_pump 0.50 → 0.75** — 0.50 was over-filtering legit momentum names; 0.75 still blocks pump-and-dumps.
- **max_hold 40 → 60** — 40 cut the PEAD drift short; 60 = textbook drift horizon.

**At ₹2L (NIFTY-50 −5% gate): TOT ₹234k → ₹332k (+42%), MTM DD ~unchanged (22.8%→23.6%), 14/17 +yrs (was 11/16).** Robust, not overfit: gains **concentrated out-of-sample** (OOS +71% vs IS only +24% — the opposite of curve-fitting), **smooth plateau** across 0.60-0.90 × 50-70. Fidelity intact (live==backtest set-equality exact); look-ahead-free counts 1494→1626 (eq-weight) / 2042→2171 (NIFTY) = the +legit movers. 67 PEAD tests pass. **Deploy verified:** rev 00263-d74 serving, PAPER, swing/intraday env unchanged, swing routes 401, grind-v2 scan HTTP 200 (still correctly dormant, NIFTY −8.2%). **Honest caveat: still backtest — real test is live-forward.**

### 2026-06-19 (latest) — SHIPPED EVENT/PEAD channel (PR #27, rev 00260-b58, PAPER, ₹2L) — additive 3rd channel

**Goal:** build + deploy the validated PEAD (post-earnings-announcement-drift) edge as a NEW channel with its own ₹2L PAPER capital, without touching swing/intraday. User: "Capital 2L, Paper mode. Make sure nothing breaks in swing... build e2e in a single go. Test everything. Separate branch."

**Honest economics (look-ahead-free, through the shipped code, ₹2L):** ~6.7% raw / ~4% live per yr (~₹8k), ~23% mark-to-market max DD, 11/16 +yrs. NOT the ~8.2%/20% headline cited earlier — that had look-ahead (next-open price floor + announce-date drawdown), both removed. Thin, lumpy, orthogonal long-term diversifier; dormant in corrections.

**Market-state gate (audited decision):** chose **NIFTY-50 −5% drawdown** (1 index fetch/day) over the validated equal-weight index (needs ~2,000 daily bars, can't be fed live — prod has no fresh full-universe daily feed). NIFTY-50 backtests as well/better (₹231k vs ₹228k, 22.8% vs 27.9% MTM DD, 11/16 vs 10/14) and stayed profitable through 2022 (+₹7k) + 2025 (+₹27k) where no-gate bled (2025-26 −₹43k). **No-gate documented + env-flippable** via `PEAD_MARKET_DD_GATE=-1.0` (highest lifetime total ₹303k but trades through corrections — future exploration).

**Fidelity:** live signal provably reproduces the backtest selection event-for-event (NIFTY-50 −5% → 2,042 candidates, 0 unexplained divergences; eq-weight reference 1,494). Shared `domain/pead_signals` + `domain/pead_book` + `swing_exit.trailed_stop` = single source of truth (prod == backtest).

**Architecture (hybrid):** NEW thin services (`pead_signal_service`, `pead_trading_service`, `pead_reconciliation_service`) for signal+book+entry+exit; REUSE `order_service`/positions/`swing_exit` trail math via `channel="pead"` (additive explicit-channel param) + distinct `wl_type="pead"` (keeps PEAD invisible to swing_reconciliation, which matches `"swing"` exactly). Daily scan `/jobs/pead-scan` (08:45 IST) after exit `/jobs/pead-reconcile` (08:40 IST), Mon-Fri scheduler. NO ws_monitor change (PEAD resting SL uses the existing channel-agnostic paper_gtt poll — verify Monday).

**Tests:** PEAD suite 40 (both fidelity replays) + book/trading/recon 28 unit + full blast-radius **463 passed, 0 failed**. **Deploy verified:** rev 00260-b58 serving, PAPER=true, CAPITAL_SWING/INTRADAY unchanged, CAPITAL_PEAD=200000 + PEAD_RISK_PER_TRADE=3000 set, swing routes 401 (intact), PEAD routes 401 (live), **brain updating 13:27 IST post-deploy** (swing/intraday healthy). Deployed mid-session (Fri 13:30 IST) — low risk (additive + rolling, clear of 14:30 swing scan); PEAD makes **no trades until Mon 06-23 ~08:40 IST** (weekend buffer).

**Dashboard backend (PR #28, rev 00261-t8s):** scan now persists the annotated daily candidate list to a SEPARATE `pead_watchlist` Firestore collection (rows tagged ENTERED/ALREADY_HELD/BREAKER_HALT/PLANNED_NOT_FILLED/NOT_SELECTED) + `/dashboard/pead/watchlist` + `/dashboard/pead/summary` endpoints (Firebase-auth, mirror `/scan/latest`). Positions+trades already channel-tagged (`channel=pead`/`strategy=PEAD`). **Frontend PEAD tab deferred** — build the Next.js tab (`./dashboard`) against Monday's real data.

**Live I/O validated 2026-06-19 (rev 00262-j2t, PR #29):** a post-close manual `/jobs/pead-scan` smoke test caught + fixed two `run_pead_scan_once` bugs the unit tests missed (they test the pure core, not the I/O wrapper): (1) a `market_dd_gate` NameError in `_persist_pead_watchlist` → every scan 500'd; (2) reaction-date used `asof=today`, but at the 08:45 premarket run today's bar doesn't exist → 0 candidates (channel never trades). Fix: `reaction_date = nifty_daily[-1][0]` (last completed session — works premarket+post-close); `fetch_result_events()` returns filing dates; pure `_select_reaction_symbols()` keeps only true reaction-day reactors (faithful to the backtest's per-event bisect). Re-test: **HTTP 200, I/O clean end-to-end** (NIFTY + NSE event-cal [3 reporters] + BQ keys + fresh-daily + build + persist), `pead_watchlist/latest` persisted, and **`nifty_dd=−8.2%` → market_ok=False → correctly DORMANT** (NIFTY in correction — the thesis working live).

**Open:** monitor first scheduled PEAD scan Mon 06-23 08:45 (gate likely still closed if NIFTY stays <−5% — expect dormant); confirm ws_monitor enforces PEAD paper_gtt SLs once a position opens; build the dashboard frontend PEAD tab vs real data. Scratch harnesses: `~/.autotrader_backtest_cache/pead_*.py` (gap_diag, lookahead_free_pnl, nifty_gate_pnl, gate_audit).

### 2026-06-18 — BREAKOUT screened → REJECTED (candle-data chapter closed; NO prod change)

**Goal:** test the one genuinely-untested long-only candle setup (volatility breakout, open item §7-G) before any fundamental-data lift — the disciplined "exhaust the cheap existing-data options first" move. User: "lets move to breakout, make sure we do not make mistakes."

**Method (anti-mistake, look-ahead-free):** generated Donchian N-day-high breakouts (N=20/50) over the deep daily data (2506 syms, 2010-26); fill at NEXT open; SAME liquid universe (top-1000 60d-turnover + price≥30 — no penny/illiquid slippage inflation), SAME sizing (sl_dist=max(2.5×ATR14, 1%)), SAME exit (1R trail arm 1.75), SAME Upstox cost+booking (`exit_lab.book_cap`) as the validated momentum/pullback/MR pool. Started with the canonical 1-param Donchian (NOT multi-param VCP) as the overfit-guard. Memory-light (candle pickle only, NO 427MB stats dict → 8GB-safe). Scratch: `~/.autotrader_backtest_cache/oos_breakout_screen.py`.

**Result — decisive reject:**
1. **Signal-level cost wall:** 114K breakouts, gross avg **+0.048R** (45% win) — a faint *real* tendency — but swing cost ≈0.09R/trade (0.58% delivery ÷ entry/sl_dist) → **net −0.04R/trade**. Structurally cost-negative; R-scale-invariant (no N/stop/gate fixes it — a tighter stop raises cost-in-R as fast as gross-in-R).
2. **Portfolio (breakout-only book) negative everywhere:** −5 to −17%/yr @₹5L with **102–354% drawdowns**; negative at every N (20/50), gate (ungated/TREND-gated), capital (₹1L–5L), and in ~10–12 of 17 years → every held-out post-2015 window negative (no positive edge for a walk-forward to even stress). TREND-gating slows the bleed, stays negative. (A 300-symbol smoke showed a lone "positive" ungated line — an artifact, gone on the full universe.)
3. **VCP not pursued:** the canonical form is decisively cost-negative; a multi-param VCP would need ~2× the gross edge just to break even, plus heavy overfit risk + far fewer signals → dead end ("simple form has no edge → complex form won't robustly survive").

**Takeaway:** breakout joins intraday as a **retail-cost casualty** (real gross tendency, killed by the cost wall). **5th independent OOS confirmation of the candle-data ceiling** (reserve-cap, param-sweep, overlay, vol-filter, breakout). Swing is at the frontier of what price/volume data supports. **The only remaining step-change is new (fundamental) data.**

**Next:** the fork is the **fundamental-data edge project** (PEAD / analyst-revisions / quality-value — the real step-change, gated on data procurement) vs **bank-and-observe** the shipped ₹5L + arm-1.75 system in PAPER. Awaiting user signal.

### 2026-06-18 (later) — SHIPPED swing trail arm-threshold 1.0R→1.75R — PR #26, PAPER

**Goal:** ship the first validated swing improvement since #3/#7-soft. The daily 1R trailing stop now ARMS at +1.75R instead of +1.0R (`domain/swing_exit.py:DEFAULT_ACTIVATE_R` 1.0→1.75) — winners ride longer before the stop ratchets up (trail width unchanged at 1R). Branch `swing-arm-1.75` → PR #26 → merged `d9d37e4` (commit `8112e6b`) → deployed **`autotrader-00258-t7d`** (was `00257-mn6`), serving 100%, PAPER + ₹5L env preserved, ws-monitor unchanged.

1. **Validation (deep OOS 2010-26, 239,359 entries, net of Upstox cost, 5-slot book).** **Plateau, not peak:** arm 1.4-1.75R ALL beat 1.0R on the held-out TEST half at every walk-forward split boundary (2015/17/19/21/23); 2.0R FAILS (trail arms too late to ever activate — the plateau's upper wall). 1.75 is the plateau's high end with the strongest recent-boundary margin (≥2023: **+31,119 vs 1.5's thin +6,251** → most robust forward). **@₹5L: +28% full net (723,517→924,853), 8.5%→10.9%/yr, AND a lower drawdown (35%→33%)** — dominates the 1.5 alternative on both return and DD at every capital ≥₹2L. **All three setups improve** (signal-level net-R: MOMENTUM +583→+1006, PULLBACK +364→+514, MEAN_REVERSION +178→+236) → uniform value, no setup-specific carve-out. Finalized 1.75 over 1.5 (data dominates; PAPER lets us monitor the plateau edge).

2. **Fidelity + tests.** `simulate_exit`/`trailed_stop` were already parameterized on `activate_R`; the prod trail (`swing_reconciliation_service:281`) consumes `DEFAULT_ACTIVATE_R`, so the one-line default change flows straight to the live trail. `tests/test_swing_exit.py` **15 passed** incl the **exit_lab fidelity-replay over ~58K entries** (implementations agree at the policy arm; the 1.75 geometry is the same formula + the OOS walk drove `simulate_exit` at 1.75 directly → prod≡backtest by construction). 5 default-reliant unit tests pinned to explicit `activate_R=1.0`; +3 new tests cover the 1.75 default. **401 passed** blast-radius (trading/watchlist/policy/swing/recon/exit/fsm).

3. **Open positions SAFE (ratchet-only).** Reconciliation does `sl_price = max(sl_price, new_stop)` (BUY) — the higher arm can NEVER lower an already-raised stop. The 4 open swing positions at deploy time (EMAMILTD short, CROMPTON/JAYNECOIND/SAIL MR) were all **unarmed** (`sl_moved` empty, stops at entry-SL) → zero impact. New arm effective at the next premarket `swing-recon-0900` (06-19).

**Honest calibration:** the +28% is raw backtest; after the survivorship/vintage/slippage haircut expect a modest live uplift (~5%→~6-6.5%/yr) PLUS the shallower DD. **The first validated profit improvement since #3/#7-soft** (after the swing audit closed: params/allocation/sector/sizing/overlays all rejected OOS). PAPER stays PAPER.

**Deploy hygiene:** `autotrader` ONLY (Rule 8 verified — arm computed in the daily reconciliation; ws-monitor/FSM swing path is SL-only, no `trailed_stop(` call). Rule 1 sync clean (`git log origin/main..HEAD` empty), Rule 3 ADC token. **Git-auth fixed mid-session** (`gh auth setup-git` → active `vishalrwt1995`; the prior push 403 was a stale `vishal01012` osxkeychain token, not a permission loss). Scratch: `~/.autotrader_backtest_cache/oos_arm_*.py`.

**Next:** PAPER-monitor swing trail behavior at arm 1.75R vs backtest expectation; fundamental-data edge project (PARKED, awaiting user signal).

### 2026-06-18 — Fresh INTRADAY re-audit (no retail edge → parked) + swing ₹5L profit profile + improvement-scope (NO prod change)

**Context:** user distrusted the prior-thread intraday audit ("done in another thread, doubt it was correct") → redid it **fresh + independent** (own BigQuery tests on the full 165M-row `candles_5m_full`, not the prior `intraday_baseline.py`). Verdict reproduced AND extended.

1. **INTRADAY = no retail-viable systematic edge (fresh, exhaustive).** Every category negative on NSE liquid equities net of cost:
   - Single-name **directional** (trend-long above-VWAP): forward 60-min return ≈ **0, sub-coin-flip (41-45% pos)** on a ~14M-entry sample — entry carries no directional info.
   - **Cross-sectional** momentum + range-position: weak/unstable, **negative 2024-26**.
   - **Market intraday momentum** (Gao et al., the documented index edge): marginal in India, **below ~3 bps futures cost**; vol-conditioning made it worse.
   - **ORB on stocks-in-play** (Zarattini-Aziz, best-documented retail intraday, US Sharpe 2.4): **NEGATIVE GROSS every year** on NSE (−0.05 to −0.11R).
   - **Root cause: NSE intraday MEAN-REVERTS** (US continuation edges invert & die) + the retail cost wall. Corroborated by prod's real paper trades (**93 trades, 9% WR, −₹687, every month negative**) + SEBI (70% cash / 93% F&O traders lose).
2. **Big-player research:** intraday edge = **data (catalyst/options-OI/L2) + speed + cost**, not a setup. Speed is NOT our blocker (Upstox WS handles non-HFT). Cost can't reach prop at retail (statutory STT). **Only untested lever = options-OI / F&O** (Upstox provides it; India intraday is options-driven) — but that's the **93%-lose arena**, access ≠ edge. **Decision: intraday systematic-on-candles is dead for us; F&O-OI track parked** (data-acquisition R&D, low base rate).
3. **Swing ₹5L profit profile** (shipped #3+#7-soft, deep OOS 2010-26, net of cost): raw **~8.5%/yr** (median 9.2%, **13/17 +years**), **MAX DRAWDOWN 35% (₹175k)**, worst yr −25% (2011), best +38% (2023), return/risk 0.60. **Honest LIVE ≈ +5%/yr (₹25k)** after survivorship/vintage/slippage haircut — *underperforms the ~10-12%/yr cap-weight market on raw return* (its value is lower-beta + diversification, a satellite not a wealth engine). **Improvement scope:** cost = small (already diluted at ₹5L; residual mostly statutory STT); tuning = exhausted/overfit-risky; candle-edges = near-exhausted → **the only step-change is NEW fundamental data** (real PEAD / analyst-revisions / quality-value); a **drawdown/vol-target overlay** is the best *risk-adjusted* lever (cuts the 35% DD, doesn't raise return). **Allocation lever tested + REJECTED:** lifting the MR reserve cap 3→4→5 at ₹5L *cuts* net (8.5%→6.6%→4.9%) and *explodes* DD (35%→52%→57%) — the "idle" slots in RANGE are the system correctly declining marginal 4th/5th-best MR trades. **System is signal-QUALITY-constrained, not slot-constrained** → confirms the lever is better signals (fundamental data), not more positions/capacity. Reserve-2-trend validated; do not change. **Full param surface then swept @₹5L (sector-cap, poscap%, risk%): NONE improve** — sector_cap=3 is a no-op (system already never holds >3/sector), cap=2 hurts (−19% net, same DD); poscap 20% & risk 1.5% near-optimal; *every* exposure-reduction lever cuts return > drawdown (flat/worse Sharpe). **Config is on the risk-adjusted frontier — signal-quality-constrained confirmed a 3rd way.** Dynamic risk overlay then TESTED (vol-target + drawdown-governor, reduce-only): **no Sharpe win** — vol-target fails (cuts return not DD; DDs not vol-predictable), dd-governor whipsaws (recoveries snap back → cutting size after the fall misses the bounce; full-pause = −0.4%/yr). DD *reducible* only at proportional return cost (best: dd-gov >20%→½ size = DD 35%→28% for return 8.5%→7.2%, ret/risk 0.57<0.60 — a risk-*preference* dial, not an improvement). **The 35% DD is intrinsic to the edge.** SWING AUDIT FULLY CLOSED — params, allocation, sector, sizing, overlays all tested, none beat current; config on the risk-adjusted frontier. Return lever remains fundamental data (the only step-change, parked). **Universe-pipeline audit (2026-06-18):** edge-concentration diagnostic flagged the highest-volatility quartile as net-negative (in-sample, all setups); but the high-vol-EXCLUSION filter **FAILED multi-split robustness** — thresholds 3.0/3.5/4.0% all beat baseline 2015-2021 yet go **negative vs baseline at the ≥2023 boundary** (regime-dependent: high-vol names were losers in the 2010s, winners in the 2023-25 momentum tape). REJECTED (in-sample/threshold mirage, #7-hard pattern). (Liquidity/price slices showed low-liq/low-price "edge" — dismissed as slippage-underestimate artifact.) **Universe stage clean. 4th OOS confirmation of the candle-data ceiling** (reserve-cap, param-sweep, overlay, vol-filter all reject OOS).
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
export PATH="# gcloud not installed — run: brew install --cask google-cloud-sdk"
bq --project_id=grow-profit-machine query --use_legacy_sql=false --format=pretty "<SQL>"

# tests
cd "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader" && pytest tests/

# deploy
gcloud run deploy autotrader \
  --source "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader" \
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

Claude's auto-loaded memory lives at `/Users/apple/.claude/projects/-Users-apple-Auto-Trading-Python-GCP/memory/`. The index is `MEMORY.md`. Each file there has a `system-reminder` showing its age — treat anything > 7 days as potentially stale and verify against this file.

**Authoritative ordering when sources disagree:**
1. Live `gcloud` / `bq` output (right now)
2. Repo code at `main` HEAD
3. This file (`docs/PROJECT_KNOWLEDGE.md`)
4. Memory files in `~/.claude/projects/.../memory/`
5. User's spoken statement in chat (often the latest intent — but verify the others before acting)

If sources disagree, surface the disagreement to the user before deciding.
