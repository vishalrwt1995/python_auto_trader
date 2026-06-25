# Auto Trading System — CLAUDE.md

> **You are an LLM accessing this codebase. Read this file first, then `gcp_autotrader/docs/PROJECT_KNOWLEDGE.md`. Don't skip.**

## What this project is

Algorithmic trading system for Indian equities. Runs on GCP (Cloud Run + Firestore + BigQuery). Trades via Upstox API. **Currently PAPER mode.** ~₹2L total capital split 50/50 between SWING (1–10 day holds, ₹1,500 risk/trade) and INTRADAY (same-day, ₹250 risk/trade) channels.

---

## 🚨 Critical rules — violating any of these has broken production before

### Rule 1 — Deploy hygiene (sync main dir BEFORE every code deploy)
`gcloud run deploy --source "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader"` builds from the **main working directory**. If that directory is behind `origin/main`, you'll silently roll back commits. **Always run before deploying:**
```bash
cd "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader"
git fetch origin main
git log origin/main..HEAD                 # MUST be empty
git merge --ff-only origin/main            # if behind
```
Incident: 2026-05-27, a stale-dir deploy rolled back a full day's work. Caught via `tact=NULL` brain regression. See `docs/PROJECT_KNOWLEDGE.md` §8.

### Rule 2 — Commit in worktrees via Python subprocess, NOT `git commit` directly
Claude SDK worktrees (`.claude/worktrees/*`) hold a lock on the index. `git commit` via the Bash tool fails with `index.lock: File exists`. Use:
```python
import subprocess
subprocess.run(['git', 'commit', '-m', 'msg'], cwd='/path/to/worktree', capture_output=True, text=True)
```
`git add`, `git push`, `git status` work fine from Bash — only `git commit` is affected.

### Rule 3 — gcloud auth uses an ADC access token
The `vishalrwt1995@gmail.com` account has NO on-disk credentials. Before any `gcloud run deploy`:
```bash
export CLOUDSDK_AUTH_ACCESS_TOKEN=$(gcloud auth application-default print-access-token)
gcloud run deploy autotrader --source "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader" \
  --region asia-south1 --project grow-profit-machine --quiet
```
Without the env var, the deploy fails with `does not have any valid credentials`.

### Rule 4 — Env-var updates ≠ code deploys
- **Env-only change** (e.g., `SWING_RISK_PER_TRADE=1500`): use `gcloud run services update --update-env-vars KEY=VALUE`. ~30 sec, no rebuild, all other env vars preserved, no stale-dir risk.
- **Code change**: requires `gcloud run deploy --source` (rebuild). Apply Rule 1.

### Rule 5 — PAPER mode is sacred until told otherwise
`PAPER_TRADE=true` is currently set. Do NOT flip to live without explicit user direction. All risk numbers in this codebase are simulated until that env var changes.

### Rule 6 — Backtest-first for any new strategy or sizing change
This codebase has 78 days × 2,638 symbols × 5.89M bars of real 5m candle data in BigQuery `candles_5m`. Any sizing / strategy / threshold change must be backtest-validated on it before shipping. Use `costs.py` (now Upstox rates by default — see Rule 7) for net-of-cost analysis.

### Rule 7 — Costs are Upstox by default (not Zerodha)
`src/autotrader/backtest/costs.py` defaults to Upstox rates as of 2026-05-28. Round-trip cost on a ₹20K position: intraday **₹54.25 (0.27%)**, swing **₹115.25 (0.58%)**. For comparison, call `CostConfig.zerodha()`. Source: `https://upstox.com/brokerage-charges/`.

### Rule 8 — Exit-logic changes must deploy the ws-monitor service AND target the FSM
Incident: 2026-06-15, the swing exit overhaul shipped but the *intraday* half had **no effect** for two reasons. Both must be checked for any exit-logic change:
1. **Two services, two deploys.** The intraday tick-exit loop runs in the **separate** `autotrader-ws-monitor` service, built from `Dockerfile.ws` via `cloudbuild.ws.yaml` (entrypoint `python -m autotrader.services.ws_monitor_service`). `gcloud run deploy autotrader --source` does **NOT** touch it. To ship a ws_monitor/exit change:
   ```bash
   cd "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader"
   gcloud builds submit --config cloudbuild.ws.yaml --project grow-profit-machine --region asia-south1 .
   gcloud run deploy autotrader-ws-monitor --image gcr.io/grow-profit-machine/autotrader-ws-monitor:latest \
     --region asia-south1 --project grow-profit-machine   # preserves env + min-instances=1
   ```
   Verify both services' revisions after deploy; ws-monitor had silently run May-15 code for a month.
2. **The live exit path is the FSM, not `_on_quote`.** With `USE_EXIT_FSM_V1=true` (set in prod), `ws_monitor._on_quote` returns early and delegates to `_on_quote_fsm` → **`domain/exit_fsm.py:transition`**. Editing the legacy `_on_quote` body is dead code. Swing exits are SL-only in the FSM (2026-06); `swing_reconciliation_service` owns the daily 1R trail. Any exit change must target `exit_fsm.py` (and stay swing/intraday-aware) — and prove which handler is live before claiming it works.

---

## Authoritative knowledge files (read in this order)

| Order | File | What it has |
|---|---|---|
| 1 | **`gcp_autotrader/docs/PROJECT_KNOWLEDGE.md`** | **Source of truth.** Production state (live revision, env vars), recent history, open items. Updated continuously. Always read after CLAUDE.md. |
| 2 | This `CLAUDE.md` | Rules, gotchas, conventions (you are reading it) |
| 3 | `gcp_autotrader/docs/ARCHITECTURE.md` | High-level system map |
| 4 | `gcp_autotrader/docs/GLOSSARY.md` | Domain terms (channel, regime, tact, R-multiple, etc.) |
| 5 | `gcp_autotrader/docs/PHASE_C_CAPITAL_SEPARATION_PLAN.md` | Per-channel design (shipped 2026-05-28) |
| 6 | `gcp_autotrader/docs/PHASE_E_INTRADAY_PLAN.md` | Intraday redesign plan (unbuilt) |
| 7 | `gcp_autotrader/docs/BACKTEST_USER_GUIDE.md` | Backtest tooling |
| 8 | `~/.claude/projects/.../memory/MEMORY.md` | User memory + feedback files (commit hygiene, gcloud config, dashboard, working style) |

---

## Architecture in one paragraph

Python trading service on Cloud Run (`autotrader` service). Cloud Scheduler triggers: premarket at 08:30 IST, swing scans at 09:22/11:00/13:00/14:30 IST, intraday scans `*/3` minutes from 09:15-14:00 IST. The **Brain** (`market_brain_service.py`) computes a regime (TREND_UP/RANGE/CHOP/etc.) + tactical_trend_score (Phase D) and writes to Firestore `market_brain/latest` + BigQuery `market_brain_history`. The **Universe service** builds a watchlist from BQ `candles_5m`. The **Scanner** emits signals; **trading_service.py:run_scan_once** applies a layered policy gate (cap → strategy → portfolio book → daily limit → entry); **order_service.py** places paper/live orders via Upstox. Two **channels** (logical capital pools): SWING (5 slots, hold 1–10 days, ₹1L allocated) + INTRADAY (3 slots, same-day, ₹1L allocated). Per-channel daily loss/profit circuit breakers (3% / 6%). Currently PAPER.

---

## Working-style conventions (the user prefers these)

1. **Honest calibration over confidence theater** — distinguish "validated by backtest" from "extrapolated" from "intuition." Use numbers; show your work.
2. **Collaborative review by default** — propose plans, get sign-off, then ship. Exception: explicit "go autonomous" direction.
3. **No silent fallbacks** — fail closed when state can't be read. Pattern: `return {"skipped": "<reason>"}` and log it.
4. **Backtest evidence before alpha claims** — if no number, say "I don't know."
5. **PR + merge + sync + deploy** for code changes; **env-var update only** for config tweaks. Never skip the sync step.
6. **Commit messages**: imperative mood, focused scope, "why" not "what," include test count. End with `Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>`.
7. **Calibrate expectations** — this is a thin-edge system. Realistic: ~9% CAGR on swing at ₹1L, 30–60% peak drawdown possible. Don't oversell.

---

## Session bootstrap (every new chat)

```bash
# 1. Set ADC token (needed for any gcloud read/write)
export PATH="/opt/homebrew/bin:$PATH"  # gcloud at /opt/homebrew/bin/gcloud (installed 2026-06-25)
export CLOUDSDK_AUTH_ACCESS_TOKEN=$(gcloud auth application-default print-access-token)

# 2. Verify production state
gcloud run services describe autotrader \
  --project=grow-profit-machine --region=asia-south1 \
  --format='value(status.traffic[0].revisionName)'

# 3. Compare with PROJECT_KNOWLEDGE.md §3 "Latest revision (verified ...)"
#    Drift means PROJECT_KNOWLEDGE is stale — re-verify with live before any action.

# 4. Optional: pre-flight (env vars, errors, open positions)
gcloud run services describe autotrader \
  --project=grow-profit-machine --region=asia-south1 \
  --format='value(spec.template.spec.containers[0].env)' | tr ';' '\n' | \
  grep -iE "PAPER|CAPITAL|SWING|RISK|DAILY"
```

---

## When you finish a session (mandatory)

Update `gcp_autotrader/docs/PROJECT_KNOWLEDGE.md`:
1. Bump `Last updated` + `Last verified live state` timestamps in the header
2. Update §3 table if you deployed (revision, notes)
3. Update §3 env-var block if you changed env vars
4. **Append** to §8 "Recent History" (newest first) — include date, what shipped, evidence
5. Move resolved items out of §7 "Open Items"
6. Commit + push the doc update to `main` (direct, no PR — docs are continuous)

Why this matters: tomorrow's session will read PROJECT_KNOWLEDGE.md first. If it's stale, decisions are made on outdated facts. **Always close the loop.**

---

## Common patterns (copy-paste templates)

### Read open positions + their channel attribution
```python
import json, urllib.request, os
TOK = os.environ['CLOUDSDK_AUTH_ACCESS_TOKEN']
URL = 'https://firestore.googleapis.com/v1/projects/grow-profit-machine/databases/(default)/documents/positions?pageSize=300'
req = urllib.request.Request(URL, headers={'Authorization': f'Bearer {TOK}'})
data = json.loads(urllib.request.urlopen(req).read())
for doc in data.get('documents', []):
    f = doc.get('fields', {})
    def gv(k):
        v = f.get(k, {})
        return v.get('stringValue', v.get('integerValue', v.get('doubleValue', v.get('booleanValue', ''))))
    if str(gv('status')).upper() == 'OPEN':
        print(gv('symbol'), gv('channel') or gv('wl_type'), gv('strategy'), gv('max_loss'))
```

### Query latest brain state from BigQuery
```bash
bq --project_id=grow-profit-machine --location=asia-south1 query --use_legacy_sql=false \
  "SELECT FORMAT_TIMESTAMP('%H:%M:%S', asof_ts, 'Asia/Kolkata') AS t, regime, \
   ROUND(trend_score,1) AS trend, ROUND(tactical_trend_score,1) AS tact \
   FROM grow-profit-machine.autotrader.market_brain_history \
   WHERE DATE(asof_ts,'Asia/Kolkata')=CURRENT_DATE('Asia/Kolkata') ORDER BY asof_ts DESC LIMIT 5"
```

### Run the test suite
```bash
cd "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader"
PYTHONPATH=src /Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/.venv/bin/python3.13 -m pytest tests/ \
  -k "trading or watchlist or vwap or guard or policy or pnl or phase_c or m4_portfolio or backtest or time" \
  --ignore=tests/test_api_watchlist_logging.py --ignore=tests/test_market_brain_v2.py -q
```
Should yield 240+ passed.

---

## File layout (quick reference)

```
/Users/apple/Projects_Migrated/Auto Trading Python GCP/      ← project root, you are here
├── CLAUDE.md                                     ← this file
├── .claude/                                      ← Claude Code settings + worktrees
├── gcp_autotrader/                               ← THE CODE
│   ├── src/autotrader/
│   │   ├── settings.py                           ← env reads + StrategySettings dataclass
│   │   ├── time_utils.py                         ← is_market_open_ist + NSE_TRADING_HOLIDAYS
│   │   ├── domain/
│   │   │   ├── risk.py                           ← position sizing
│   │   │   ├── portfolio_book.py                 ← M4 channel budgets + DD governors
│   │   │   ├── regime_affinity.py                ← regime × strategy hard-blocks
│   │   │   ├── scoring.py                        ← signal scoring
│   │   │   └── models.py
│   │   ├── services/
│   │   │   ├── trading_service.py                ← THE HOT PATH — run_scan_once
│   │   │   ├── market_brain_service.py           ← regime + tactical_trend
│   │   │   ├── universe_service.py               ← watchlist builder
│   │   │   ├── order_service.py                  ← order placement (paper/live)
│   │   │   └── ws_monitor_service.py             ← live WS tick handler
│   │   ├── adapters/
│   │   │   ├── firestore_state.py                ← Firestore reads/writes
│   │   │   ├── bigquery_client.py
│   │   │   └── upstox_client.py
│   │   ├── backtest/                             ← bar-by-bar replay engine
│   │   │   ├── costs.py                          ← Upstox rate model (DEFAULT)
│   │   │   ├── engine.py
│   │   │   └── replay_pure.py
│   │   └── backtest_v2/                          ← portfolio-aware sim
│   ├── tests/                                    ← 600+ tests
│   ├── docs/                                     ← *.md knowledge files
│   ├── scripts/redesign/                         ← backtest sweep runners
│   ├── backtests/                                ← backtest outputs (cached)
│   │   └── _cache/                               ← bars_5m.pkl, daily_bars.pkl
│   └── cloudbuild.yaml                           ← Cloud Build config
└── docs PDFs + older markdown files              ← from earlier phases, mostly stale
```

---

## Last-checked production state (re-verify before action)

See `gcp_autotrader/docs/PROJECT_KNOWLEDGE.md` §3. As of last update there: `autotrader-00252-v7w` · PAPER · ₹2L (₹1L per channel) · risk ₹1,500 swing / ₹250 intraday · daily limits 3%/6% per channel · Phase C v2.1 complete.

---

## Update policy for this CLAUDE.md

- Add a Critical Rule only when a real incident proves one is needed.
- Update the file-layout map when the directory structure changes.
- Update working-style notes when the user explicitly states a new preference.
- Otherwise leave it stable. PROJECT_KNOWLEDGE.md is the moving target; this file is the frame.
