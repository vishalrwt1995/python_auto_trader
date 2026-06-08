# Multi-Year Backtest — Session Handoff (resume here)

> Written 2026-06-08 to continue the multi-year backtest/optimization work in a fresh thread.
> Read this + `docs/PROJECT_KNOWLEDGE.md` + `CLAUDE.md` first.

## Mission
Make the system profitable via a **production-faithful multi-year backtest** (Indian equities, Upstox,
PAPER mode, ₹2L = ₹1L swing + ₹1L intraday), THEN optimize. **Sequence the user wants:** cover all
caveats → full "real-system-working" backtest → only then optimize for profit. **Never change prod
without explicit approval. `PAPER_TRADE=true` is sacred. Backtest = edge-finder; paper confirms before real money.**

## CURRENT STATE — immediate next step
We just wrote `src/autotrader/backtest_v2/swing_portfolio_sim.py` (account-level swing portfolio sim).
**IT HAS NOT BEEN RUN YET — that is the next action.** Run:
```
cd "/Users/vishalrawat/Auto Trading Python GCP/gcp_autotrader"
export PATH="/Users/vishalrawat/google-cloud-sdk/bin:$PATH"
PYTHONPATH=src python3 -u -m autotrader.backtest_v2.swing_portfolio_sim 2>&1 | grep -avE "FutureWarning|warnings.warn"
```
It reads `~/.autotrader_backtest_cache/phase9_trades_2022-01-01_2026-06-04.json` + the regime timeline
(see artifacts) and produces the **₹1L account-level** swing result (5-position cap, ₹3k daily halt,
faithful-regime affinity gating, score-ranked selection, ₹1,500 sizing, full Upstox cost). NOTE: it reads
`/tmp/regime_timeline.jsonl` — if `/tmp` was cleared, first copy it back:
`cp src/autotrader/backtest_v2/artifacts/regime_timeline_2022_2026.jsonl /tmp/regime_timeline.jsonl`

## ENVIRONMENT / RUN (critical — get this right or nothing works)
- **gcloud**: use the **ADC token** for project `grow-profit-machine`: `gcloud auth application-default print-access-token`
  (has BQ/Firestore/GCS/CloudRun-admin access). The active gcloud account `vishal@eloelo.in` is on a DIFFERENT
  project (`protrade-prod-2026`) and lacks access — do NOT use `gcloud run/bq` directly; use the ADC token via the BQ/Run REST APIs.
- **Python**: system `python3` (3.9.6) HAS the deps (google.cloud, etc.). `/usr/local/bin/python3.13` does NOT
  (missing google.cloud.firestore). Always `PYTHONPATH=src python3`.
- **Env vars**: `source /tmp/recon_env.sh` before running scripts that build AppContainer (brain_reconstruct).
  It mirrors the live Cloud Run env. **If `/tmp/recon_env.sh` is cleared, regenerate** from the Cloud Run service
  env via the Run Admin API (GET `https://run.googleapis.com/v2/projects/grow-profit-machine/locations/asia-south1/services/autotrader`,
  emit `export K='V'` per env). Key vars: `GCP_PROJECT_ID=grow-profit-machine`, `GCS_BUCKET=grow-profit-machine-autotrader-data`,
  `BQ_DATASET=autotrader`, the 6 `UPSTOX_*_SECRET_NAME`s, `PAPER_TRADE=true`.
- **Long runs**: launch **fully detached** (`nohup … & disown`), NOT `run_in_background` (harness reaps bg tasks ~5 min).
  Foreground `sleep` is blocked (use Monitor/poll the log file).
- BQ location is always `asia-south1`. gcloud bin: `/Users/vishalrawat/google-cloud-sdk/bin`.

## DATA ASSETS (BigQuery: grow-profit-machine.autotrader)
- `candles_1m` — 781M rows, 2,638 symbols, 2022-01-03→2026-06-04, full-universe 1-minute. AUDITED CLEAN
  (0 dups; ~0 bad OHLC; out-of-hours = legit Diwali Muhurat/special sessions; 1m→5m cross-validated to
  production `candles_5m` to the share). `CLUSTER BY symbol, trade_date`.
- `candles_daily` — NEW this session: 1m→daily resample (regular session 09:15–15:29), 2,638 syms, 2022→2026.
- `candles_5m_full` — NEW: 1m→5m resample, 165.6M rows, 2,638 syms, 2022→2026. (RELIANCE 2026-05-06 verified vs known daily.)
- `candles_indices` — NIFTY 50 + India VIX (1m), 2022+.
- `market_brain_history` — production brain log, **2026-04-02→2026-06-05 (48 days, ~60 snaps/day)** = regime validation ground truth.
- `scan_decisions` — production scan decisions (for equivalence test).

## REPO ARTIFACTS (persist these — /tmp gets cleared)
- `src/autotrader/backtest_v2/artifacts/regime_timeline_2022_2026.jsonl` — **the 4-HOUR faithful daily regime**
  (1,091 days, core-4). Dist: RANGE 726, PANIC 203, TREND_UP 159, TREND_DOWN 3. **Do not recompute unless inputs change.**
- `~/.autotrader_backtest_cache/phase9_trades_2022-01-01_2026-06-04.json` — 45,571 swing trades (prod_replica).
  Fields per trade: as_of, setup, direction, entry_price, sl, exit_price, qty, holding_days, gross_pnl,
  net_pnl (**BROKERAGE-ONLY — see caveat**), r_realized, raw_score, adjusted_score, entry_regime, config_tags.

## CODE WRITTEN (src/autotrader/backtest_v2/)
- `brain_reconstruct.py` — runs the REAL `MarketBrainService._build_state()` with BQ candles injected
  point-in-time (zero reimplementation). `BQHistoricalGCS` serves daily (candles_daily) + 5m (candles_5m_full)
  in-memory; `daily_only` flag skips 5m for fast daily-regime. Modes: `full`, `core4`, `diag`, `timeline`.
  `core(regime)` folds EARLY_*→TREND_*, RANGE_ROTATING→RANGE. Run `timeline` to rebuild the regime timeline:
  `source /tmp/recon_env.sh; PYTHONPATH=src python3 -u -m autotrader.backtest_v2.brain_reconstruct timeline` (~2-4h detached).
- `phase_e_multiyear.py` — multi-year intraday MORNING_FADE screen on candles_5m_full (rolling-window
  `collect_one_windowed`, production `check_strategy_entry`+`simulate_exit`+Upstox cost). Run: `… phase_e_multiyear [N]`.
- `swing_portfolio_sim.py` — **NEW, NOT YET RUN** — account-level swing portfolio sim (see CURRENT STATE).
- (Reused existing: `phase9_prod_replica.py` = the multi-year swing harness; `phase5_trade_sim.py` = `simulate_swing_trade`.)

## VERIFIED LIVE PRODUCTION CONFIG (Cloud Run Admin API, deployed ~2026-06-01)
CAPITAL=200000, CAPITAL_SWING=100000, CAPITAL_INTRADAY=100000, RISK_PER_TRADE=250 (intraday),
SWING_RISK_PER_TRADE=1500, MAX_DAILY_LOSS=3000, DAILY_PROFIT_TARGET=6000, SWING_MIN_SIGNAL_SCORE=45, PAPER_TRADE=true.
Other knobs = code defaults (MAX_POSITIONS=3 intraday, SWING_MAX_POSITIONS=5, swing ATR×2.5, swing RR 2.0, max hold 10d).
⚠️ The Firestore `config` collection has STALE display-label docs (₹50k/₹125/"Swing Trading"=FALSE) that the
settings loader IGNORES — env vars are authoritative. (Dashboard shows wrong numbers — harmless, fix later.)

## KEY FINDINGS

### Brain / regime
- **PCR & FII/DII do NOT affect the regime** (computed at line ~1380, AFTER `_map_regime` is called at ~1237).
  So regime is exactly reconstructable from price+breadth+VIX. Verified.
- Exact-match to the production log is capped by: production-log artifacts (dq-glitch PANICs), code-version drift
  (RANGE_ROTATING only existed Apr–May14 2026; high-breadth-alt TREND_UP added ~May 2026), and leadership being
  boundary-sensitive (~15pts high, faithful but not exact). **Decision: use CORE-4 regime (fold the recent
  EARLY_*/RANGE_ROTATING/high-breadth additions) applied uniformly** — correct for a 2022-2026 backtest.
- Reconstructor bugs found+fixed: asof str() vs isoformat() (fell back to now); daily loaded from 2025 not 2022;
  expected_lcd resolver ignores its arg (monkeypatched); fresh=False blocked breadth (forced True); by-IK vs
  by-symbol daily path; dq forcing enabled EARLY override (core4 sets dq=59).

### INTRADAY (MORNING_FADE only, so far)
- Small size: cost-killed every cycle (gross ~+0.045R, **net −0.44R**, −₹76k). Leveraged+selective: net −0.065R,
  only 2024 positive → NOT robust. **MORNING_FADE is not a robust edge.**
- ⬜ Other intraday setups (ORB / VWAP_TREND / MOMENTUM / OPEN_DRIVE / MEAN_REVERSION) NOT yet screened multi-year (task #81/#64).

### SWING (the main result) — at ₹1,500 sizing + FULL Upstox cost
- Total **+₹1.08M net BUT entirely 2023** (+₹3.76M); 2022 −1.55M, 2024 −249k, 2025 −599k, 2026 −277k → **NOT robust** (bull-year-dependent).
- Faithful-regime re-gate (hard-blocks) barely changed it (+₹1.26M) — **hypothesis that regime-mislabel caused the
  losses was REFUTED.** The losing trades are PULLBACK/MOMENTUM **longs on RANGE days, which RANGE permits (affinity ~1.0).**
- **PER-SETUP × REGIME (the killer table, ₹1,500, full cost):**
  | setup | TREND_UP | RANGE | PANIC | total net | verdict |
  |---|---|---|---|---|---|
  | PULLBACK (70% of trades) | +1.29M | −552k | +3k | +745k | trend-only edge |
  | MOMENTUM (28%) | +685k | −131k | −179k | +380k | trend-only edge |
  | MEAN_REVERSION (3%) | −25k | −34k | +13k | −45k | net-negative → KILL |
  | BREAKOUT/SHORT_BREAKDOWN | ~0 trades | | | | dormant (gates too strict) |
  | VWAP_REVERSAL | disabled in config | | | | |
- **The edge is TREND-CONDITIONAL.** ~₹680k of avoidable losses come from trading PULLBACK/MOMENTUM in RANGE/PANIC.
- **Leading fix (to test after full backtest): gate PULLBACK & MOMENTUM to TREND_UP** (block/haircut in RANGE+PANIC)
  → keeps +₹1.97M trend profit, drops the bleed → "makes money in trends, sits flat otherwise" = robust. **Kill MEAN_REVERSION.**

### Cost & sizing are decisive (caught two verdict-flipping bugs)
- `phase9` net was **BROKERAGE-ONLY** (`calc_round_trip_brokerage`, ~₹40) — NOT full Upstox CNC (STT+DP+GST+stamp, ~₹109/trade at ₹1,500).
- `phase9` also used the **DEFAULT ₹300 sizing**, not live ₹1,500. At ₹300 full-cost net was −₹2.3M; at ₹1,500 it's +₹1.08M.
- ALWAYS recost with `compute_round_trip_cost(qty, entry, exit, is_swing=True, cfg=CostConfig.upstox())` at live sizing.

## PLAN — covering the caveats → full faithful backtest → optimize
Caveats still to cover for a true "real-system" backtest:
1. **Portfolio limits** (5 positions, ₹1L capital, ₹3k halt / ₹6k target, score-ranked) → **S1 = `swing_portfolio_sim.py` (RUN IT NEXT).**
2. **Faithful watchlist** (production trades score-ranked/diversified **150** from top-1000-turnover eligible, NOT
   top-300 turnover) → **S2 = task #85** (heavy: score ~1,000 names/day × 1,084 days; reuse `build_watchlist` /
   `_select_with_diversification_and_corr` in universe_service.py, candles_daily). Then re-scan + portfolio-sim = complete replica.
3. **Full affinity re-score** (not just hard-blocks) — fold into the re-scan.
Then: **optimize** (lead candidate = trend-gating PULLBACK/MOMENTUM + kill MEAN_REVERSION), validate cross-cycle, ship to paper.
Also pending: intraday all-setups multi-year screen; tighten equivalence test onto BQ-1m (old GCS harness drifts on the forming-bar).

### Production watchlist generation (how the live system picks swing names) — for S2
- Layer 1 (daily 06:15 `/jobs/universe-v2-refresh`): top-N by 60d turnover + filters → `eligibleSwing` (BALANCED top-1000).
- Layer 2 (`/jobs/watchlist-refresh`, target 150, premarket + every 5 min): `build_watchlist` → `_select_with_diversification_and_corr`
  = **sort by signal SCORE desc** + sector cap + correlation filter, regime/policy-aware. (universe_service.py:4455 / :3333)

## TASKS (in the task list)
#78 Phase 0 harness, #79 brain feasibility/timeline (≈done), #80 truth-pass (in progress), #81 intraday economics,
#82 amplify swing, #83 real-time exec, #84 ship-to-paper, #85 faithful watchlist replica (queued). #64 Phase E intraday (in progress).
#69/#73 swing watch / regime-aware sizing (pending).

## CONSTRAINTS
No prod changes without explicit "approved". PAPER_TRADE stays true. Collaborative review. Honest about confidence
(backtest = edge-finder + robustness; paper = final gate before real money). User probes fidelity hard — keep covering caveats.
