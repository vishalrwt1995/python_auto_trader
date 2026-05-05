# Backtester — design + smoke-run findings

**Branch**: `redesign/audit-and-design`
**Status**: MVP shipped (live-decision replay). Pure replay deferred.
**Date**: 2026-05-04

---

## 1. Why a backtester now

The live system has been running since 2026-04-16 with the redesigned scoring +
exit FSM. After ~3 weeks we have:

| metric | value |
|---|---|
| live trades booked | 73 |
| net P&L (₹) | -1,368 |
| win rate (%) | 34.25 |

Win-rate is below target and we lack the introspection to answer: *is the
scoring weak, are exits leaving R on the table, or are costs eroding the
edge?* The scan_decisions table records every signal (qualified + blocked)
with run-date partitioning, so a replay strategy can drive a deterministic
sim engine and isolate which layer is the culprit.

## 2. Architecture (one screen)

```
┌──────────────────┐   ┌──────────────────┐   ┌────────────────┐
│  scan_decisions  │   │   candles_5m     │   │ market_brain_  │
│  (live signals)  │   │   candles_1d     │   │   history      │
└────────┬─────────┘   └────────┬─────────┘   └────────┬───────┘
         │ via data.py loaders                         │
         ▼                       ▼                     ▼
   ┌─────────────────────────────────────────────────────┐
   │ LiveDecisionStrategy   ←── BrainTimeline.asof(ts)   │
   │  (replay_live.py)                                   │
   └────────────┬────────────────────────────────────────┘
                │ on_bar / on_fill
                ▼
   ┌─────────────────────────┐    ┌──────────────────────┐
   │   BacktestEngine        │───▶│   SimAccount         │
   │  (engine.py)            │    │  (account.py)        │
   │   • bar walk            │    │   • orders/fills     │
   │   • exit FSM tick       │    │   • positions/cash   │
   │   • mark-to-market      │    │   • equity curve     │
   └────────────┬────────────┘    └──────────────────────┘
                │ uses                ▲
                ▼                     │
   ┌─────────────────────────┐    ┌──────────────────────┐
   │  CostConfig (Indian-    │    │   SlippageModel      │
   │  market intraday +      │    │   (BarRangePct       │
   │  delivery rates)        │    │    default)          │
   └─────────────────────────┘    └──────────────────────┘
                │ output
                ▼
   ┌─────────────────────────────────────────────────────┐
   │ metrics.py + reports.py                             │
   │   • summary.json + summary.txt + per-bucket CSVs    │
   │   • Sharpe/Sortino (daily-bucketed × √252)          │
   │   • Monte Carlo bootstrap (montecarlo.py)           │
   │   • walk-forward harness (walkforward.py)           │
   └─────────────────────────────────────────────────────┘
```

### Module layout

```
src/autotrader/backtest/
├── __init__.py            module overview + honesty disclosures
├── types.py               Bar, Order, Fill, Position, SimTrade, EquityPoint
├── costs.py               Indian-market round-trip cost calculator
├── slippage.py            NoSlippage / FixedBps / BarRangePct
├── account.py             SimAccount: order matching, fills, equity
├── data.py                BQ loaders (candles, scan_decisions, brain)
├── engine.py              BacktestEngine: bar walk + FSM tick + MTM
├── replay_live.py         LiveDecisionStrategy: scan_decisions → orders
├── metrics.py             summary, per_setup_*, per_regime_*
├── reports.py             write CSV / JSON / TXT bundle
├── montecarlo.py          bootstrap_trades — CIs on Sharpe / DD / VaR
├── walkforward.py         rolling train/test fold harness + holdout OOS
└── runner.py              end-to-end orchestrator (RunSpec → BacktestResult)

scripts/redesign/
└── backtest.py            CLI: live-replay | compare | walk-forward

tests/backtest/             48 tests — costs, slippage, account, engine,
                            replay, metrics, walk-forward, Monte Carlo
```

## 3. Determinism / no-look-ahead contract

The engine guarantees:

1. **Bars are processed in `(ts, symbol)` order** — strategy can't peek.
2. **`Strategy.on_bar` is called BEFORE order resolution** — a signal at bar T
   places a MARKET order that fills at bar T+1's open.
3. **FSM ticks AFTER order resolution** — same-bar SL/target hits resolve as
   exit fills before FSM evolves state. Mirrors live: ticks flow through the
   matcher first, FSM reads post-fill.
4. **Mark-to-market runs LAST per bar** — equity reflects all fills produced
   by that bar.
5. **Pessimistic bar-internal precedence** — when a single bar would hit BOTH
   SL and target, SL wins. Bias is downward — the right way to be wrong.

## 4. What the MVP DOES measure

* **Exit FSM behavior** under sim execution + costs — answers "is the FSM
  leaving R on the table or stopping out too early?"
* **Cost erosion** — STT, exchange, SEBI, GST, stamp duty, DP charges
  (intraday and delivery branches both).
* **Counterfactual gate unblocking** — `--unblock blocked_reason` includes
  signals that were blocked for that reason as if they'd qualified, lets you
  A/B individual gates without code changes.
* **Per-setup / per-regime breakdowns** — find which slices print money and
  which lose.
* **Statistical significance** — Monte Carlo bootstrap of trade reshuffles
  (`bootstrap_trades`) gives 5%/95% CIs on total-PnL and max-DD, plus
  P(loss) and P(2× capital).

## 5. What the MVP does NOT measure (deferred, per honesty disclosure)

* **Scoring calibration** — needs pure-replay (decoupled from live adapters).
  Estimated 11h of refactoring per the audit. Tracked as TODO.
* **Universe selection** — backtester replays signals; doesn't re-run watchlist.
* **Options-flow signals** — historical options chain not backfilled in BQ.
* **News-sentiment gates** — `news_store` writes are not history-backed.
* **Slippage realism** — modeled (BarRangePct), not measured. Recalibrate
  once we have ≥50 real fills with quote-time prices.

## 6. Smoke run results (2026-04-16 → 2026-05-04, 5m)

```
$ python scripts/redesign/backtest.py compare \
    --project grow-profit-machine --dataset autotrader \
    --since 2026-04-16 --until 2026-05-04 \
    --max-concurrent 10 --out-dir backtests/ --label smoke_v2_all
```

|                  | sim       | live    |
|------------------|-----------|---------|
| n_trades         | 24        | 73      |
| net_pnl (₹)      | -45,854   | -1,368  |
| win_rate (%)     | 12.5      | 34.25   |

**Decisions in window**: 17,576. **Sim trades**: 24 (0.14% conversion).

### Findings

1. **Pyramid-guard / hold-time mismatch**: Live booked 3× more trades on the
   same scan stream. Reason: live exits faster, freeing the (symbol, side)
   slot for re-entry. Sim positions hold longer due to FSM exit settings. We
   need to reconcile FSM debounce + give-back vs the live config.

2. **Same-bar SL hits dominate**: Many trades (e.g. WELSPUNLIV, VEDL,
   TIINDIA) entry-and-exit in the SAME 5m bar. Cause: ATR×atr_mult SL
   distance is tight (₹0.69 on ₹124 stock = ~0.55%), so the bar's
   high/low covers it. Either lift `default_atr_mult` from 1.74 or use 15m
   for entries.

3. **Win rate gap is bigger than n-trade gap**: 12.5% sim vs 34.25% live —
   even of the 24 sim trades, a higher fraction stop out. This points at
   the exit FSM, not selection.

4. **Costs are NOT the killer**: ₹5.3k of costs on -₹45.8k loss = 12% — so
   even zero-cost would still leave a -₹40k loss. The edge problem isn't
   friction.

### Next experiments (post-merge)

* Sweep `default_atr_mult` ∈ {2.0, 2.5, 3.0} → expectancy_r curve.
* Sweep `default_rr` ∈ {1.5, 2.0, 2.5} → win-rate vs avg-win tradeoff.
* Per-setup walk-forward to identify which setups converge on positive EV.
* Pure replay (after decoupling) to validate scoring calibration.

## 7. Reproducibility

Every run writes a `meta` block to `summary.json` capturing: label, date
range, timeframe, n_decisions, n_symbols, qualified_only, unblock_reasons,
max_concurrent, per_trade_risk_inr. Re-running with the same args produces
byte-identical output (engine sorts deterministically; bootstrap uses fixed
seed 1729).

## 8. Tests

```
$ PYTHONPATH=src .venv/bin/pytest tests/backtest/ -v
========================== 48 passed in 0.08s ==========================
```

Coverage:
* `test_costs.py` — 7 tests on Indian-market cost math (STT sides, brokerage
  cap, swing vs intraday)
* `test_slippage.py` — 5 tests across three models (No / FixedBps / BarRangePct)
* `test_account.py` — 7 tests on order matching (MARKET, LIMIT, STOP, no-look-ahead)
* `test_engine.py` — 5 tests on full bar→trade flow with synthetic bars
* `test_replay_live.py` — 7 tests including pyramid guard, max-concurrent,
  unblock counterfactual, regression test for per-instance `_pending_meta`
* `test_metrics.py` — 8 tests on Sharpe, drawdown, expectancy, per-bucket
* `test_walkforward_montecarlo.py` — 9 tests on fold geometry + bootstrap

## 9. CLI cheatsheet

```bash
# Smoke (no out-dir; just print summary)
python scripts/redesign/backtest.py live-replay \
  --project grow-profit-machine --dataset autotrader \
  --since 2026-04-16 --until 2026-05-04

# Counterfactual: include signals blocked by VWAP_REVERSAL policy
python scripts/redesign/backtest.py live-replay \
  --since 2026-04-16 --until 2026-05-04 \
  --unblock policy_strategy_blocked --label vwap_unblock \
  --out-dir backtests/

# Compare to live trades
python scripts/redesign/backtest.py compare \
  --since 2026-04-16 --until 2026-05-04 --out-dir backtests/

# Walk-forward (15-day train, 5-day test, 5-day holdout)
python scripts/redesign/backtest.py walk-forward \
  --since 2026-02-01 --until 2026-05-04 \
  --train-days 15 --test-days 5 --holdout-days 5
```

## 10. Known gaps (recorded for follow-up)

| gap | severity | tracked |
|---|---|---|
| pure-replay strategy not built (~11h decoupling) | medium | future PR |
| per-day position re-entry semantics differ from live | medium | tune FSM |
| same-bar SL/target on tight ATR (5m) | low-med | use 15m or wider mult |
| no plotting (PNG/HTML output) | low | dependency-free for now |
| BQ-only data path (no GCS cache fallback) | low | doc'd in `data.py` |
| no per-trade risk caps in account (strategy's job) | doc'd | by design |

## 11. Calibration findings (2026-05-05)

### 11.1 Engine clock units bug — fixed

**Symptom**: sim showed 0 FLAT_TIMEOUT exits and few CONFIRMED FSM transitions
even though live had both. Root cause was a units mismatch in the engine.

* `engine.run()` incremented `self._sim_epoch += 1.0` per bar — a bar counter.
* `_make_position_view` set `entry_epoch=0.0` for every position.
* `tick.ts` passed to `domain.exit_fsm.transition()` was therefore the bar
  counter, but `FsmConfig.flat_timeout_s=7200` and `confirm_debounce_s=15`
  are in seconds.

The 15-second debounce became 15-bar (~75 minutes at 5m timeframe), so
positions almost never reached CONFIRMED. The 7200-second flat timeout
was unreachable until ~7200 bars had been processed, even though the
gate compared `tick.ts - entry_epoch=0` (cumulative bar count, not
elapsed since entry).

**Fix**: parse `bar.ts` (ISO-8601) to epoch seconds and use the entry
fill's `entry_ts` for `entry_epoch`. New helper `_iso_to_epoch()` is
LRU-cached. Two regression tests added in `test_engine.py`:
`test_engine_clock_is_real_epoch_seconds` and
`test_engine_fsm_debounce_uses_real_seconds`.

### 11.2 Live's 27 FLAT_TIMEOUTs were pre-flag-flip artifacts

The first attempt to calibrate the smoke window (2026-04-16 to 05-04)
showed live with 27 FLAT_TIMEOUT exits but sim with 0. Per-day grouping
of the live `trades` table reveals:

| date range | dominant exit reasons | mode |
|---|---|---|
| 2026-04-16 | EOD_CLOSE | legacy (different bug) |
| 2026-04-20 to 04-23 | **FLAT_TIMEOUT (27), SL_HIT (19)** | legacy `_on_quote_legacy` |
| 2026-04-28 onward | SL_HIT, occasional TARGET_HIT_BACKFILL | FSM `_on_quote_fsm` |

The `USE_EXIT_FSM_V1` Cloud Run env var was flipped on between 04-23
and 04-28. The 27 FLAT_TIMEOUTs live booked are from when it was off
and the legacy non-FSM tick handler ran. The post-flip distribution
matches sim's — both produce mostly SL_HIT and TARGET_HIT, no
FLAT_TIMEOUT.

### 11.3 FSM FLAT_TIMEOUT from CONFIRMED is mathematically unreachable

A separate finding: `domain.exit_fsm.transition()` only fires
`FLAT_TIMEOUT` from the CONFIRMED branch (line 307–320). For that to
fire we need:

1. `peak_mfe_r ≥ 0.8` (to enter CONFIRMED at all)
2. `current_r ≥ peak / 2` (otherwise the pullback gate fires first
   and transitions to LOSING, which has no FLAT_TIMEOUT path)
3. `|ltp − entry| < atr × 0.3` (the "flat" check)

With `sl_dist = atr × atr_mult` and `atr_mult = 1.74` (default), the
flat gate constrains `|current_r| < 0.3 / 1.74 ≈ 0.17`. But staying in
CONFIRMED requires `current_r ≥ peak/2 ≥ 0.4`. Contradiction — the
pullback transition fires first, every time.

This is a real FSM design bug surfaced by the calibration work. **Not
fixed in this PR** — it changes live behavior. Tracked as a follow-up.
The legacy `_on_quote_legacy` had FLAT_TIMEOUT as a state-independent
check that fired regardless of CONFIRMED/RUNNER/LOSING/INITIAL; the
FSM rewrite tucked it under CONFIRMED only, which was a regression
that's been silently masked because pullback-to-LOSING dominates.

### 11.4 Calibration gates met

After the units fix, with `per_trade_risk=40` and `max_concurrent=50`:

| metric | sim | live | gap |
|---|---|---|---|
| n_trades | 65 | 73 | 11% (gate < 20%) ✓ |
| win_rate (%) | 38.46 | 34.25 | 4.2pp (gate < 5pp) ✓ |
| net_pnl (₹) | +140 | -1,368 | sim slightly profitable |

---

*Generated as part of the redesign/audit-and-design branch. Sources of
truth: code under `src/autotrader/backtest/`, tests under `tests/backtest/`,
CLI under `scripts/redesign/backtest.py`.*
