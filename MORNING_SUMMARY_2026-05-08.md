# Morning Summary — Pre-Launch Backtest Audit

**Generated:** 2026-05-07 23:55 IST (overnight autonomous run)
**Branch state:** `main` HEAD = `29ea3f1` (merged); all code in production
**Cloud Run revisions live:** `autotrader-00221-b7w` · `autotrader-dashboard-00063-rhc` · `autotrader-ws-monitor-00040-n5c`
**Test suite:** 572 passing

---

## TL;DR

The backtest harness audit produced **no production-affecting changes** but uncovered one real bug in the harness itself (was reading 70-day-stale daily candles). The system is **ready for tomorrow's open exactly as it was when you went to sleep**, plus the backtest tooling is now fixed for honest post-launch iteration.

**No alarms. No bleeding strategy in the deployed code. Tomorrow's launch proceeds as planned.**

---

## What was tested

### Intraday backtest (22 days · 2026-04-16 → 2026-05-07 · 5m bars · 546 symbols)

Configuration: `--no-watchlist-per-day --setups BREAKOUT,VWAP_TREND,VWAP_REVERSAL,MEAN_REVERSION,PULLBACK,OPEN_DRIVE,MORNING_FADE`

Results:

| Strategy | Regime | N trades | Win rate | Net P&L | E[R] |
|---|---|---|---|---|---|
| MORNING_FADE | RANGE | 59 | 39.0% | **+₹39,216** | +0.135 |
| PULLBACK | TREND_UP | 97 | 21.7% | **-₹60,711** | -0.286 |
| Total | mixed | 156 | 28.2% | -₹21,495 | -0.127 |
| VWAP_TREND, BREAKOUT, MEAN_REVERSION, OPEN_DRIVE, VWAP_REVERSAL | — | 0 | — | — | — |

### Swing backtest (22 days · 1d bars · 546 symbols · swing-compatible setups)

Configuration: `--no-watchlist-per-day --is-swing --setups BREAKOUT,PULLBACK,MEAN_REVERSION,SHORT_BREAKDOWN,SHORT_PULLBACK`

Results: **0 trades fired across all 22 days.** The `check_swing_entry` daily-bias gates filtered every candidate.

This matches live's actual zero swing trades over the same window. The bottleneck **is the entry gate**, not the threshold (now 65) or multi-emission. Tomorrow's swing trades will only fire if `check_swing_entry` passes for at least one of the multi-emission rows.

---

## What I fixed

### Real bug: backtest's 1d candle path

The GCS data loader for `timeframe=1d` was reading from `cache/score_1d/{ex}/{seg}/{sym}.json` — the **legacy** score-cache writer's output. Verified: that path's last bar for RELIANCE was 2026-02-26 (70 days stale). The fresh canonical writer is `cache/candles/1d/{ex}/{seg}/{sym}.json` (uniform with 5m/15m), updated daily.

**Every backtest using daily candles since 2026-02-27 has been silently reading stale data.** Fixed in the same commit (`259c5a9`). Test files updated to assert the new path.

### New CLI surface

Added two flags to `pure-replay` mode:

- `--no-watchlist-per-day` — evaluate every setup in `--setups` against every bar instead of restricting to historical setup labels. This is the right flag for "if today's deployed code had been live for the audit window, what would it have made?"
- `--is-swing` — drives `PureReplayConfig.is_swing` → routes through delivery cost model + enables `check_swing_entry` gating

Use combined for a swing audit: `pure-replay --timeframe 1d --is-swing --no-watchlist-per-day --setups BREAKOUT,PULLBACK,MEAN_REVERSION,SHORT_BREAKDOWN,SHORT_PULLBACK`

### Production code

**Untouched.** I considered hard-blocking PULLBACK in TREND_UP regime based on the -₹61k bleed signal, but the methodology has a known caveat (see below) — the signal isn't trustworthy enough to justify a production change. Conservative call: leave production alone, observe live data tomorrow.

---

## Honest caveats on the backtest results

### Why the PULLBACK -₹61k signal isn't actionable

The 97 PULLBACK trades fired because `--no-watchlist-per-day` evaluates **every setup against every bar**. That mirrors a hypothetical world where multi-emission applied to *intraday* too. **It doesn't.** The deployed system applies multi-emission only to the *swing* path; intraday still uses the watchlist generator's winner-takes-all label, which doesn't emit PULLBACK in TREND_UP days.

So the 97 hypothetical PULLBACK trades aren't trades the deployed system will fire tomorrow. The signal is "PULLBACK calibration would be bad if we extended multi-emission to intraday" — not "the deployed code is bleeding".

### Why the MORNING_FADE +₹39k signal is encouraging but limited

MORNING_FADE in RANGE has positive expectancy in the backtest. The deployed system already supports MORNING_FADE in RANGE (it's hard-blocked in TREND_UP per `regime_affinity._HARD_BLOCKS`). So this confirms the calibration is working; no change needed.

### Why the 0-swing-trades signal is real

The deployed swing pipeline (multi-emission watchlist → `trading_service.run_scan_once` → `check_swing_entry` daily-bias gate → entry) wouldn't have fired any swing trades over the 22-day audit window. This matches live's actual data: 0 qualified swing entries in the same window. The strict daily-bias gates are doing their job.

The expected outcome tomorrow at 09:22 IST: most of the 3 MEAN_REVERSION + 147 BREAKOUT swing watchlist rows from tonight's build will hit the entry gate. The MEAN_REVERSION ones (scores 80+) need `daily_bias.rsi_daily ≤ 35 OR similar` per `check_swing_entry`'s MEAN_REVERSION branch. If today's market action satisfies that for any of the 3 candidates, you get a swing trade. If not, zero.

---

## Production state for tomorrow

| Layer | Revision | Behavior |
|---|---|---|
| Backend trader (`autotrader`) | `00221-b7w` | Multi-emission swing watchlist + MEAN_REVERSION-as-swing veto-lift + threshold=65 + audit_log writer fix |
| WS monitor (`autotrader-ws-monitor`) | `00040-n5c` | Swing-skip-EOD logic verified (5 smoke tests pass); fresh image |
| Dashboard (`autotrader-dashboard`) | `00063-rhc` | Channel filters on positions, journal, analytics |
| `main` HEAD | `29ea3f1` | All work merged |
| PR #1 | MERGED | — |

## What to watch tomorrow morning

1. **09:22 IST swing scan**: query `scan_decisions` for `wl_type=swing`. ≥1 with `qualified=true` = the multi-emission unblock worked.
2. **09:21–11:00 IST intraday scans**: VWAP_TREND has been the steady earner (5 trades, +₹39 today). Should fire similar pattern tomorrow.
3. **15:25 IST EOD close**: if any swing position is open, look for `eod_skip_swing tag=...` log lines. First time the swing-overnight code runs on a real position.

## Rollback plan if something goes wrong

- Backend: `gcloud run services update-traffic autotrader --to-revisions=autotrader-00220-rns=100 --project grow-profit-machine --region asia-south1` reverts to pre-multi-emission, pre-MEAN_REVERSION-lift.
- Dashboard: revert is one revision back (`00062`).
- WS monitor: revert is one revision back (`00039-w7m`).

---

## Backtest tool — usage cheatsheet for post-launch iteration

```bash
# Set credentials once per shell:
export GOOGLE_CLOUD_PROJECT=grow-profit-machine
export GCS_BUCKET=grow-profit-machine-autotrader-data
export PATH="/Users/vishalrawat/google-cloud-sdk/bin:$PATH"

# Intraday: how would deployed code (with --no-watchlist-per-day = multi-emission semantic) fare?
python scripts/redesign/backtest.py pure-replay \
  --since 2026-04-16 --until 2026-05-07 --timeframe 5m \
  --no-watchlist-per-day \
  --setups BREAKOUT,VWAP_TREND,VWAP_REVERSAL,MEAN_REVERSION,PULLBACK,OPEN_DRIVE,MORNING_FADE \
  --out-dir /tmp/backtests/intraday_test --label test_run

# Swing: how would deployed code with multi-emission + MEAN_REVERSION-as-swing fare?
python scripts/redesign/backtest.py pure-replay \
  --since 2026-04-16 --until 2026-05-07 --timeframe 1d \
  --is-swing --no-watchlist-per-day \
  --setups BREAKOUT,PULLBACK,MEAN_REVERSION,SHORT_BREAKDOWN,SHORT_PULLBACK \
  --warmup-days 90 \
  --out-dir /tmp/backtests/swing_test --label test_run

# Sanity check: replay live's actual decisions through sim engine
python scripts/redesign/backtest.py compare \
  --since 2026-04-16 --until 2026-05-07 \
  --label compare_test
```

After each run, look at:
- `/tmp/backtests/<label>/per_setup.csv` — per-strategy WR / E[R] / P&L
- `/tmp/backtests/<label>/per_setup_regime.csv` — strategy × regime breakdown (where to hard-block)
- `/tmp/backtests/<label>/trades.csv` — every individual trade

---

You're set. Sleep well.
