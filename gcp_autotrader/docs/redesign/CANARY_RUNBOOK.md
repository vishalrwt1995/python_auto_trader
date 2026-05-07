# M8 — Canary Runbook (Paper + Live)

**Purpose:** safely flip the M1–M6 feature flags ON in production with
per-flag rollback and daily checkpoints. Each day either advances a
flag or halts the ramp — there is no "we'll check next week" state.

**Audience:** the single operator of this stack (i.e. you). No handoffs,
no on-call rotation. This runbook is a checklist you execute alone.

**Prerequisite state:**
- Branch `redesign/audit-and-design` merged to `main` (or deployed
  directly from branch in Cloud Run).
- Test suite green: `pytest -q` reports 340+ passing.
- All seven redesign flags default **OFF** in Cloud Run env. Flipping
  is a `gcloud run services update --update-env-vars` away.
- `PAPER_TRADE=true` in `autotrader` service. Live is gated separately
  by `allow_live_orders` (existing; untouched by this rollout).

---

## Flag inventory

| Env var | Milestone | Behaviour when OFF | Behaviour when ON |
|---|---|---|---|
| `USE_EXIT_FSM_V1` | M1 | legacy exit precedence | 5-state FSM with 0.8R-debounce confirm |
| `USE_PLAYBOOK_V1` | M2 | pass-through on unknown regime/setup | hard-block unless edge registered |
| `USE_EXPECTED_EDGE_R_V1` | M3 | signal_score alone gates entry | also requires prior.expected_edge_r > 0 |
| `USE_PORTFOLIO_BOOK_V1` | M4 | only `max_daily_loss` applies | channel budgets + daily/weekly/monthly DD governors |
| `USE_OPTION_ANALYTICS_V1` | M5 | option_metrics Firestore doc stale | polled every 5m during market hours |
| `USE_NEWS_SIGNALS_V1` | M5 | no news read | scanner reads recent_for_symbol |
| `USE_PORTFOLIO_STREAM_V1` | M5 | portfolio stream disabled | ws_monitor subscribes to portfolio feed |
| `USE_ATTRIBUTION_LOG_V1` | M6 | only `trades` row on close | also writes to `attribution` table |

Note: M0 (safety net) and M7 (tests) ship unconditionally; no flag.

---

## Ramp schedule (10 market days)

One flag flip per day. Weekend days are rest/review, not flip days.
If any **Halt trigger** fires, stop the ramp, revert the latest flip,
and hold the current set until the root cause is identified.

| Day | Action | Success gate (end of day) |
|---|---|---|
| **D0** | Pre-flight: deploy branch, confirm all flags OFF. Run `pytest -q` against deployed tag. | 340+ tests green; Firestore `flags/{svc}` shows all 8 flags `false` |
| **D1** | Paper only: `USE_EXIT_FSM_V1=true` on `autotrader-ws-monitor` | ≥3 closed trades; 0 invalid state transitions in logs; no FLAT_TIMEOUT regression vs D−5 avg |
| **D2** | Paper only: `USE_PLAYBOOK_V1=true` on `autotrader` | scan_decisions table shows a non-zero `blocked_by_playbook` count **and** ≥1 entry still fired (both gates working) |
| **D3** | Paper only: `USE_EXPECTED_EDGE_R_V1=true` on `autotrader` | scan_decisions shows `blocked_by_expected_edge` count; priors file unchanged (hot-reload disabled) |
| **D4** | Paper only: `USE_PORTFOLIO_BOOK_V1=true` on `autotrader` | portfolio log-line appears every scan with `channels={intraday,swing,...}` populated; no `portfolio_unknown_channel` denials |
| **D5** | Weekend: review — compute daily_metrics for D1–D4, compare against legacy week | `mean_r_delta` not worse than legacy by > 0.5R; `alerts` tuple empty every day |
| **D6** | Paper only: `USE_OPTION_ANALYTICS_V1=true` + `USE_NEWS_SIGNALS_V1=true` (bundle — both are read-side, no order impact) | Firestore `option_metrics/NIFTY` doc age ≤ 7 min during market hours |
| **D7** | Paper only: `USE_PORTFOLIO_STREAM_V1=true` on `autotrader-ws-monitor` | ws_monitor logs show `portfolio_stream_subscribed`; position updates arrive without REST poll |
| **D8** | Paper only: `USE_ATTRIBUTION_LOG_V1=true` on `autotrader` | `attribution` BQ table gets ≥1 row per close; row count == `trades` row count for the day |
| **D9** | Rest + observe. Do not flip anything. | 340+ tests still green against the deployed HEAD; `daily_metrics` table has D1–D8 rollups |
| **D10** | **Promotion gate** — see below | ALL success gates D1–D8 were met and no halt triggers fired |

After D10, proceed to M9 (full-live ramp runbook) only if the
promotion gate passes.

---

## Halt triggers (revert immediately)

A halt trigger is an **automatic** stop. No deliberation — the latest
flag flip gets reverted, the ramp pauses.

1. **Trade count collapse** — fewer than 30% of the prior-week baseline
   qualified entries in a trading day after a flag flip. (Exception:
   USE_PLAYBOOK_V1 and USE_EXPECTED_EDGE_R_V1 are *expected* to reduce
   entries; collapse means <20% for those.)
2. **Win-rate collapse** — ≥5 trades in a day with win_rate ≤ 0.20
   (M6 alert: `win_rate_below_0.25` fires).
3. **R-lag** — daily `mean_r_delta ≤ −1.0` over 5+ trades (M6 alert:
   `realized_r_lags_expected_by_1.0R` fires).
4. **Stop overrun** — any single trade with `|mae_r| > 1.5` (M6 alert:
   `mae_over_1.5R_stop_overrun` fires). Stop overrun = SL was breached
   but fill landed more than 1.5× the planned sl_dist away — a broker
   latency or gap event. One instance is enough to halt.
5. **Exception burst** — `logger.error` count > 20 in any 10-minute
   window in `autotrader` or `autotrader-ws-monitor` logs.
6. **Firestore/BQ write failure** — `bq_trade_insert_failed_permanent`
   log line for any closed position. Trades table is the audit of
   record; a gap is a halt.
7. **Kill-switch trip** — the existing kill-switch fires. Do not
   re-arm it before root cause is understood.

---

## Rollback procedure (per flag)

Flipping any flag OFF is one command. Positions already opened under
the new path continue to close under whatever the exit path now reads
(the exit FSM state lives on the position doc; the legacy path reads
it too and treats unknown states as INITIAL — verified in M7 S10).

```bash
# Example: revert M4 on autotrader.
gcloud run services update autotrader \
  --region asia-south1 \
  --update-env-vars USE_PORTFOLIO_BOOK_V1=false
```

After revert:
1. Check `gcloud run services describe autotrader --region asia-south1 --format='value(spec.template.spec.containers[0].env)'` — confirm the flag shows `false`.
2. Tail `gcloud run services logs read autotrader --region asia-south1 --limit 100` — confirm scanner log line no longer contains `portfolio_book=...`.
3. Update the ramp log (`docs/redesign/CANARY_LOG.md`, append-only) with the halt reason + revert timestamp.

---

## Daily checklist (D1–D9)

Run this every day at **15:45 IST** (15 min after close):

- [ ] BigQuery `trades` row count for today is ≥ yesterday's −20%
- [ ] BigQuery `attribution` row count (if flag on) == `trades` count
- [ ] Run `python scripts/redesign/compute_daily_metrics.py --since <today>` and read the `alerts` tuple — must be empty
- [ ] `gcloud run services logs read autotrader --region asia-south1 --limit 500 | grep -iE 'error|failed|kill'` returns only known benign patterns
- [ ] Firestore `kill_switch/active` doc shows `active=false`
- [ ] Eyeball the dashboard: open positions count, today P&L, any stuck OPEN-for->6h position (intraday)

If every checkbox passes → tomorrow is a flip day. If any fails → today was the last flip day; investigate before flipping more.

---

## Promotion gate (D10)

All of:
- D1–D8 each met their success gate (table above).
- No halt triggers fired during the ramp.
- `daily_metrics` table shows 10 consecutive days of non-empty rows.
- `mean_r_delta` averaged over D1–D8 ≥ 0 (realized matches or beats expected).
- Exception count in any 24h window ≤ 5 across both services.

If any bullet fails: **do not** advance to M9. Capture the failure in
`CANARY_LOG.md`, leave the current flag set in place for one more
full week, and re-evaluate.

---

## What this runbook explicitly does NOT do

- Does not touch `allow_live_orders`. Live rollout is M9.
- Does not modify the paper slippage defaults (Batch 7) — already
  calibrated; flipping M-flags does not re-tune them.
- Does not roll out a new scanner setup or a new edge. The edge
  registry is frozen at M2's seed list for the duration of the canary.
- Does not change capital, risk_per_trade, or max_daily_loss. Those
  are production-constant during the ramp; tune separately after M9.

---

## Emergency contact surface

- Upstox broker support: dashboard → help → chat (live-hours only).
- GCP support: standard console — Cloud Run SLA covers service uptime,
  not trading behaviour.
- The trading-bot error channel (existing): monitored by the operator;
  critical alerts already route there via the existing audit pipeline.
