# INTRADAY AUDIT — session handoff

> **Mission:** apply the SWING audit playbook to the INTRADAY channel (₹1L, ₹250
> risk/trade). Replicate prod 100% e2e for backtesting, run the multi-year
> baseline, review every (setup × regime) cell, fix or remove, fidelity-gate,
> ship to PAPER. Started 2026-06-13; status: **data inventory done, everything
> else pending.**

## Why this audit

The swing audit (completed 2026-06-13, see PROJECT_KNOWLEDGE §8) found the old
swing config was losing **−12.9%/yr net** under plausible-looking logic, and a
validated fix-set now expects ~+6.8%/yr in-sample (deployed rev
`autotrader-00254-wqk`, PAPER). The intraday channel — the other ₹1L — has
**never had this audit**. Assume nothing; measure everything.

## Data assets (inventoried 2026-06-13 — full multi-year coverage ✓)

| BQ table (`grow-profit-machine.autotrader`) | Window | Days | Symbols | Rows |
|---|---|---|---|---|
| `candles_5m_full` (cols: symbol, instrument_key, **bar_ts**, trade_date, OHLCV) | 2022-01-03 → 2026-06-04 | 1,092 | 2,638 | 165.6M |
| `candles_1m` (cols: **candle_ts**, trade_date, symbol, OHLCV, resolution) | 2022-01-03 → 2026-06-04 | 1,096 | 2,638 | 781M |
| `candles_5m` (operational, recent only) | 89 days | — | 2,649 | 7.7M |

Also available: `candles_daily_all.pkl` (130MB local cache, 2,638 symbols daily),
regime timeline 2022-2026 (`backtest_v2/artifacts/regime_timeline_2022_2026.jsonl`,
core-4 regimes), sector mapping, swing harness/infra in `src/autotrader/backtest_v2/`
(esp. `exit_lab.py` booking/cost patterns, `prod_replay_validate.py` fidelity-gate
pattern). Costs: `backtest/costs.py` Upstox default — intraday RT on ₹20k ≈
**₹54.25 (0.27%)**; with ₹250 risk/trade, positions are SMALL → cost % is the
first thing to measure.

## The prod intraday system (map before trusting — verify in code)

- **Scans:** Cloud Scheduler `*/3` min, 09:15–14:00 IST → `trading_service.run_scan_once`
  (same hot path as swing; intraday rows ranked/thresholded by `adjusted_score`
  post-brain-haircut vs risk-mode-tiered min score).
- **Watchlist:** `universe_service.build_watchlist` intraday section — Phase 1
  (premarket momentum/reversal lists, `PHASE1_MOMENTUM`/`PHASE1_REVERSAL`) and
  Phase 2 (intraday 5m rescoring during market, `phase2eligibility`, its own
  window/policy gates + rejection counters).
- **Setups (enumerate exactly from code):** VWAP_TREND, VWAP_REVERSAL, OPEN_DRIVE,
  MORNING_FADE, PHASE1_MOMENTUM, PHASE1_REVERSAL (+ BREAKOUT/short variants where
  applicable). Regime gating via `regime_affinity._HARD_BLOCKS`/`_AFFINITY`
  (note 2026-05-20 Batch H: MORNING_FADE re-enabled in CHOP/RANGE/TREND_DOWN to
  gather live evidence; PHASE1_MOMENTUM blocked in TREND_UP/RANGE/CHOP).
- **Sizing:** `RISK_PER_TRADE=250`, MIS product, 3 slots (`max_signals_allowed`),
  channel capital ₹1L, daily halts 3%/6% per channel (Phase C).
- **Exits (`ws_monitor_service`, intraday-only paths):** 3-stage partials (40% at
  1R + SL→breakeven, 30% at 1.5R, rest trails), degraded qty==2 partial, breakeven
  at 1.0×ATR, trail 1.5×ATR (1.2× after target-passed), regime-change tighten
  (0.8×ATR), FLAT_TIMEOUT (2h, <0.3×ATR move), EOD square-off
  (`eod-position-reconcile`). **This exit stack is complex and tick-driven — the
  harness must model it on 5m (validate against 1m), and exit fidelity is the
  first gate, as with swing.**

## Playbook (mirror the swing audit — tasks #23–25)

1. **Inventory** (#23, in progress): enumerate every (setup × regime) cell from
   code; document every gate and why it exists.
2. **Faithful harness + baseline** (#24): replicate watchlist→scan→size→exit on
   `candles_5m_full`; certify fidelity vs prod code; baseline with CURRENT prod
   config: **GROSS and NET, every year separately, multiple capitals
   (₹1L/2L/3L/5L), per setup × regime. No truncated reports.**
3. **Cell-by-cell** (#25): per cell — what's wrong → fix → validate → keep or
   remove after best effort. Avoid over-engineering: economic rationale, few
   params, robust plateaus, OOS walk-forward before believing anything.
4. **Fidelity replay gate** → implement in prod code → tests → market-closed
   PAPER deploy (CLAUDE.md Rules 1–5 — sync main dir, ADC token, PAPER sacred).

## Hard-won lessons from the swing audit (apply from day 1)

1. **Cost share kills thin edges**: swing's V2 exit churned into −12.9%/yr.
   Intraday positions are ~6× smaller (₹250 vs ₹1500 risk) — expect cost % of
   gross to be the dominant question. Compute it cell-by-cell first.
2. **Look-ahead hides in innocuous places**: the swing backtest's RS filter used
   entry-day close (worth a fake +₹9.3k/yr). For intraday, beware same-bar fills,
   bar-close indicators used at bar-open, and Phase-2 selection timing.
3. **Selection > exit**: swing's profit came from trade selection (regime gating,
   ranking, filters), not exit polish. Expect the same.
4. **Regime vocabulary**: backtests run on core-4 folded regimes
   (`brain_reconstruct.CORE_MAP`: EARLY_TREND_UP→TREND_UP etc.); prod emits the
   refinement regimes — gate on the bucket, not the literal label.
5. **Parameter plateaus, not peaks**: accept a value only if neighbors are ~as
   good (cf. swing max-hold: 10d→+2.5k, 20d→+30.0k, 25d→+26.3k, 30d→+19.7k —
   deployed 20d sits on the plateau).
6. **Exit-fidelity-first**: build the pure exit function + prove it equals the
   harness on every signal before touching anything else
   (`tests/test_swing_exit.py::test_fidelity_matches_exit_lab` pattern).
7. **Account-level walks** (slots, halts, ranking), not signal-level sums — slot
   competition changes everything.
8. **Validate 5m exits against 1m** for a sample (swing did 5m validation of the
   daily trail; intraday needs the inverse rigor).

## Session bootstrap for the audit (new chat)

Read in order: root `CLAUDE.md` → `docs/PROJECT_KNOWLEDGE.md` → this file.
Local python: `/usr/local/bin/python3.13` (pytest 9.0.2); backtest scratch can use
`/opt/homebrew/bin/python3.13`. ADC: `export CLOUDSDK_AUTH_ACCESS_TOKEN=$(gcloud
auth application-default print-access-token)`. Cache dir:
`~/.autotrader_backtest_cache/` (reuse; pull 5m data per-year to manage size).

## Status checklist

- [x] Data inventory (multi-year 5m + 1m confirmed)
- [ ] System inventory: cells, gates, exit stack (task #23)
- [ ] Faithful harness + certified fidelity (task #24)
- [ ] Baseline: gross/net × year × capital × cell (task #24)
- [ ] Cell-by-cell verdicts (task #25)
- [ ] Fix-set validated + OOS + fidelity replay
- [ ] Prod implementation + tests + PAPER deploy

> Meanwhile: swing PAPER validation is running (deployed 2026-06-13). Monday
> 2026-06-16 first-cycle verification = task #26 (build fields, scan block
> reasons, reconcile on the 5 legacy positions).
