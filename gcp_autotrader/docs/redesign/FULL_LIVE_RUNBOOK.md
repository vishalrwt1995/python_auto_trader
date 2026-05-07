# M9 — Full-Live Ramp Runbook

**Purpose:** move the redesigned stack from paper-only to live orders
in a 30-day ramp, sized from ₹5k/trade to full ₹125k/trade capital.
Every gate from the paper canary (M8) stays in place; this runbook
adds the live-order gates only.

**Prerequisite:** M8 canary passed the promotion gate on D10. All
eight flags are ON in the `autotrader` and `autotrader-ws-monitor`
services, and have been on for at least 5 consecutive market days in
paper with no halt triggers. `daily_metrics.alerts` has been empty for
those 5 days.

**Audience:** single operator (you). Same as M8 — no handoff.

---

## Guiding principle

> The scariest number in algo trading is not "today's drawdown". It is
> "yesterday's paper trade that would have been a live trade, but live
> got a worse fill". Full-live is a **paper+live shadow** ramp, not a
> replacement. Keep paper running in parallel; compare realized-R
> between the two channels every evening for the entire 30 days.

---

## Pre-flight (D−1)

- [ ] Confirm Upstox access_token is valid through the ramp window
      (30 market days ≈ 6 calendar weeks). Rotate if expiry < 7 weeks.
- [ ] Confirm broker funds ≥ ₹1.5 lakh (covers 1.25L position +
      brokerage float).
- [ ] Verify `allow_live_orders=false` today. We flip it on D1 at
      09:10 IST exactly (after the 09:00 open-drive settles).
- [ ] Run `pytest -q` one more time. 340+ tests green or halt.
- [ ] Snapshot Firestore + BQ state: `scripts/redesign/snapshot_state.py`
      (if present) or manual BQ export of `trades`, `attribution`,
      `daily_metrics`, `positions`.
- [ ] Tell nobody. Sounds silly — it matters. The pressure of "people
      are watching" breaks discipline.

---

## Size ladder (30 market days)

The ladder is ₹-per-trade max risk. Position size derives from
`risk_per_trade / sl_dist`. Capital of ₹50k stays constant in settings;
we only adjust `RISK_PER_TRADE`. One flip per milestone-day, all others
are observation days.

| Day range | `RISK_PER_TRADE` | Max concurrent positions | Description |
|---|---|---|---|
| **D1–D3** | ₹500 | 1 | Toe in the water. Live can fire 1 order at a time. |
| **D4–D7** | ₹1,500 | 2 | Tripled but still bounded. |
| **D8–D14** | ₹3,000 | 3 | Production-shape concurrency at 24% sizing. |
| **D15–D21** | ₹6,000 | 3 | Half-sized. |
| **D22–D28** | ₹10,000 | 3 | Four-fifths. |
| **D29–D30** | ₹12,500 | 3 | Full production sizing. |

Corresponding `max_daily_loss` tracks at 3× `RISK_PER_TRADE` (matches
M4 `daily_throttle 1.5%` on 50k capital at D29+). `daily_profit_target`
tracks at 3× too. Update both whenever `RISK_PER_TRADE` changes.

---

## Day-0 live flip (D1, 09:10 IST)

1. Confirm market is not halted (NSE site, or first regime snapshot
   must show a non-PANIC label).
2. `gcloud run services update autotrader --region asia-south1 \
     --update-env-vars ALLOW_LIVE_ORDERS=true,RISK_PER_TRADE=500,MAX_DAILY_LOSS=1500,DAILY_PROFIT_TARGET=1500`
3. Tail the log: `gcloud run services logs tail autotrader --region asia-south1`
4. Wait for the first live entry — could be minutes, could be hours.
   Do **not** trigger a manual scan. Let the scheduler do its thing.
5. When the first live entry fires:
   - Log into Upstox dashboard, confirm the order appears with the
     right qty and ref_id (`AT-…`).
   - Confirm the position doc in Firestore has `paper=false`.
   - Confirm the attribution row (once the position closes) has
     `paper=false`.
6. If any of (4)/(5) fail → revert: `ALLOW_LIVE_ORDERS=false`. No
   exceptions.

---

## Shadow comparison (runs every day D1–D30)

Every evening at 15:50 IST, run:

```bash
python scripts/redesign/shadow_compare.py --date $(date +%F)
```

(If the script doesn't yet exist, the comparison is a hand-SQL on BQ
`attribution` grouped by `paper`.)

Expected output:
```
date       live_n  live_mean_R  paper_n  paper_mean_R  drift_R
2026-05-15 3       +1.20        4        +1.35         -0.15
```

- `drift_R` = `live_mean_R − paper_mean_R`. This is the slippage &
  execution-quality gap.
- Target: `|drift_R| ≤ 0.15` on any single day.
- If `|drift_R| > 0.25` for 2 consecutive days → halt and re-tune
  paper slippage (`PAPER_ENTRY_SLIPPAGE_PCT`, `PAPER_SL_SLIPPAGE_PCT`)
  until paper tracks live again. Paper is only useful as a forward
  test if it agrees with live.

---

## Halt triggers (revert to `ALLOW_LIVE_ORDERS=false`)

All M8 halt triggers remain in force, plus these live-only triggers:

1. **Broker reject loop** — ≥3 consecutive REJECTED orders on live.
   Root-cause (freeze quantities? funds? product type?) before resuming.
2. **Fill-price gap** — any live fill where `|fill_price − ref_price|
   / ref_price > 0.005` (50 bps). One slip that big is a liquidity
   event; revert until you've read the tape at that timestamp.
3. **Shadow drift** — `|drift_R| > 0.5` on any single day, OR
   `|drift_R| > 0.25` for 2 days in a row.
4. **Realised drawdown** — live P&L curve below −2× `RISK_PER_TRADE`
   on the day after any single trade closes.
5. **Kill-switch trip** — as in M8.

When any trigger fires:
- Revert `ALLOW_LIVE_ORDERS=false`.
- Do **not** re-flip the same day. The ramp loses that day; you pick
  back up the next market morning at the same size tier (not the next
  tier).

---

## Daily checklist (D1–D30)

Add to the M8 daily checklist (run at 15:50 IST):

- [ ] Live order count today matches Upstox dashboard.
- [ ] Every live position has a matching Firestore `paper=false` doc.
- [ ] `shadow_compare` shows `|drift_R| ≤ 0.15`.
- [ ] Brokerage deducted in `trade_row.brokerage` is within ±5% of
      Upstox-reported brokerage (already reconciled by
      `calc_round_trip_brokerage`; confirm with a spot check).
- [ ] No open-to-CLOSED transition took more than 5 min after SL/TARGET
      hit (live exit latency sanity).

---

## Size-tier advance gate (applied on D3, D7, D14, D21, D28)

Advance to the next size tier only if **all** of:
- The current tier ran for its full range with zero halt triggers.
- `mean_r_delta` averaged over the tier ≥ 0.
- Shadow drift averaged over the tier is within `±0.15`.
- `daily_metrics.alerts` was empty every day of the tier.
- Available broker funds ≥ 1.5× next-tier full exposure
  (next-tier `risk_per_trade` × max_positions × 5 — rough SL-loss
  buffer).

If any bullet fails, **hold at the current tier** for another full
tier duration before re-evaluating. Do not skip tiers, ever.

---

## D30 — Declaring the ramp complete

On D30 close:
- [ ] `daily_metrics` shows 30 consecutive live trading days.
- [ ] Shadow drift cumulative average within ±0.10.
- [ ] Weekly `mean_r_delta` ≥ −0.15 across all four weeks.
- [ ] Kill-switch never tripped.
- [ ] No halt triggers fired in the final 10 trading days.

If all pass: remove this runbook from the top-of-mind stack. The
system is in steady-state live. Continue the daily checklist at
15:50 IST indefinitely — that's the ops contract from here on.

If anything failed: roll back `ALLOW_LIVE_ORDERS=false`, stay on paper
for 10 full trading days, then restart this runbook from D1.

---

## What this runbook explicitly does NOT do

- Does not add new strategies. New edges go through their own
  backtest-priors-paper cycle after the full-live ramp settles.
- Does not scale capital past ₹50k. Capital scaling is a separate
  decision with its own runbook (not in this doc).
- Does not enable overnight positions. Swing positions already work
  in paper; a separate swing-live ramp runbook follows M9 completion.
- Does not authorise manual trade intervention during market hours.
  If you see something wrong → flip `ALLOW_LIVE_ORDERS=false`; do not
  place a manual order to "fix" it.

---

## Post-M9 (after D30 passes)

- Move `RISK_PER_TRADE` and `MAX_DAILY_LOSS` from env vars into a
  committed YAML/JSON so future changes are code-reviewed, not shell
  commands.
- Re-run priors rebuild (`scripts/redesign/compute_priors_from_bq.py`)
  against the live data — the D1–D30 trades are the first live
  priors batch.
- Archive this runbook into `docs/redesign/completed/` and start the
  next milestone's runbook if there is one.
