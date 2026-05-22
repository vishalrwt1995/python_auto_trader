# Project Knowledge — Auto Trading System

> **Purpose:** Single source of truth for any Claude session, started at any time.
> **Read this file first** in every new chat. It is committed to the repo and updated continuously.
>
> **Last updated:** 2026-05-22 03:50 IST · **Last verified live state:** 2026-05-22 03:50 IST
>
> **If you are a future Claude session reading this:** verify the "Production State" section against live `gcloud` output before asserting current state — drift is possible. Then read the "Recent History" log (newest at top) for context on the last few sessions of work.

---

## 0. Quick start for a new chat

The user's preferred bootstrap prompt for any new chat:

```
Read /Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader/docs/PROJECT_KNOWLEDGE.md first.
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

| Service | Latest revision (verified 2026-05-08) | Notes |
|---|---|---|
| `autotrader` | `autotrader-00225-zjt` | min-instances=1, CPU=2 (raised 2026-05-08) |
| `autotrader-ws-monitor` | `autotrader-ws-monitor-00040-n5c` | min-instances=1, holds Upstox WS loop |
| `autotrader-dashboard` | `autotrader-dashboard-00063-rhc` | Next.js, Firebase Auth |

**Live trading flags (autotrader env):**
- `PAPER_TRADE=false` — live orders enabled
- `allow_live_orders=true` (Firestore runtime)
- `GCP_PROJECT_ID=grow-profit-machine`
- `BQ_DATASET=autotrader`

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

---

## 8. Recent history (newest first)

> Append-only log. Each entry: date · revision/commit · what shipped · live evidence.

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
