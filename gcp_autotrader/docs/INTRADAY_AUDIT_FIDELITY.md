# INTRADAY HARNESS — fidelity certification (audit #24a)

> What the backtest harness reproduces of production, proven with numbers, and
> the one place it deliberately does NOT (and why that's correct). Read with
> `INTRADAY_AUDIT_INVENTORY.md`. Generated 2026-06-13.

## TL;DR

| Layer | Method | Result | Verdict |
|---|---|---|---|
| **Exit logic** | replay 88 real paper trades through pure exit fn on 5m + 1m | FSM-era (n=26): 77% reason-match 5m / 81% 1m; 4/4 TARGET_HIT exact; median gross Δ +₹0.2; median \|px Δ\| 0.033% | **CERTIFIED** |
| **Scan logic** | my `compute_indicators`+`score_signal` on prod's OWN 15m bars vs `scan_decisions` | rsi 42.42 vs 42.6, atr 13.26 vs 12.96, adx 22.29 vs 21.8, vwap 1741.9 vs 1744.0 (all <2%) | **CERTIFIED** |
| **5m→15m data** | 5m_full resample vs prod's 15m cache, bar-by-bar | 35/36 bars exact (price AND volume to the share); the 1 = prod's own mid-bar cache snapshot | **CERTIFIED** |
| **Bit-level prod-decision replay** | full 5m_full pipeline vs `scan_decisions` (1435 rows) | rsi 32%, atr 38% — diverges | **NOT ACHIEVABLE — by design (see §3)** |

Net: the harness is a faithful implementation of prod's exit + scan **logic**
on **bit-exact data**, but it is a **clean-room, look-ahead-free** backtest, not
a tick-replay of prod's exact historical decisions. For an edge audit this is
the correct and sounder target. (The swing audit made the identical choice —
clean daily bars, not prod's exact intraday cache.)

## 1. Exit-side certification (lesson 6 + 8)

`backtest_v2/intraday_exit.py` — two pure simulators:
- `simulate_intraday_exit_fsm` — the path prod runs (`USE_EXIT_FSM_V1=true` on
  ws-monitor since 2026-04-24; see inventory §0.1). No partials; real
  TARGET_HIT; confirm-stop entry∓0.3R after 0.8R MFE; LOSING tighten; FLAT/EOD.
- `simulate_intraday_exit` — the legacy partial/breakeven/trail stack, for
  completeness and the pre-2026-04-24 era.

26 unit tests (`tests/test_intraday_exit_sim.py`) pin every mechanism.

Replay (`backtest_v2/intraday_exit_validate.py`) of all 88 real intraday paper
trades (2026-04-16..05-12) on BQ 5m AND 1m bars, era-split:
- **FSM era (n=26, the gate):** exit-reason match 77% (5m) / 81% (1m); all 4
  TARGET_HITs reproduced to the paisa; median per-trade gross Δ **+₹0.2**;
  5m-vs-1m bias median **₹0.0**, mean −₹1.6; median \|final-px Δ\| **0.033%**.
- Legacy era (n=50, informational — older tunables: 45-min flat-timeout pre
  04-21, no paper slippage pre 04-23): 54% reason match, used only to confirm
  the legacy sim is wired, not as a gate.

Residual FSM-era mismatches (6) are all explained: 1 manual `TARGET_HIT_BACKFILL`
label, 1 pure 5m-granularity case (1m agrees with prod), 4 FLAT-vs-EOD boundary
cases consistent with the WS-tick-vs-exchange-candle feed difference. The
5m-vs-1m bias band (±₹10–30 on boundary trades, ₹0 median) is the documented
granularity noise the baseline carries.

## 2. Scan-side certification

`backtest_v2/intraday_scan_validate.py`. Two separable questions, both answered:

**(a) Is the scan LOGIC faithful?** Feed my `compute_indicators` →
`determine_direction` → `score_signal` → affinity × brain-haircut prod's OWN
cached 15m bars (`~/.autotrader_backtest_cache/cache__candles__15m__*.json`),
truncated to the scan timestamp, and compare to the recorded `scan_decisions`
row. ADANIPORTS 2026-05-12 09:36:34:

```
metric     scan_decisions   my-code-on-prod-bars
rsi              42.6              42.42
atr              12.96             13.26
adx              21.8              22.29
vwap           1743.99           1741.89
```

All within 2%; the sub-1% residual is prod's real-time forming bar (the 09:30
bar at 09:36 held ~6 min of data; the cache stored its completed form). → the
indicator/score/affinity code path is a faithful reimplementation. (This is the
intraday analog of swing's `prod_replica_v2` 200/200.)

**(b) Is the 5m→15m DATA faithful?** `candles_5m_full` resampled to 15m
(start-labeled: the 09:15 15m bar = {09:15,09:20,09:25} 5m bars) vs prod's 15m
cache, bar-by-bar over 2026-05-08..05-12 for ADANIPORTS: **35/36 bars match
exactly on O/H/L/C AND volume** (e.g. 09:15 bar O1749.8 H1757.4 L1719.0 C1723.5
V760653 — identical to the share). The single diff is prod's last cached bar,
written mid-formation. → the resample is exact where both sources exist.

## 3. Why bit-level prod-decision replay is NOT achievable (and not the goal)

Running the FULL pipeline on dense `candles_5m_full`→15m vs the recorded
`scan_decisions` (1435 rows, 5 days) gives rsi 32% / atr 38% / vol_ratio 15%
match — despite (a) and (b) above. The reconciliation:

1. **Prod's indicator INPUT was a gappy, live-fetched 15m series.** Prod fetches
   today's 15m via `get_intraday_candles_v3` and merges with a GCS 15m cache
   (`_fetch_candles`, `need=80`). That cache is sparse: liquid names carry only
   ~27–32 trading DAYS over a 60+-day span (ADANIPORTS 624 bars / 32 present
   days / 2026-02-19..05-21), with day-level holes. ATR/RSI/ADX(14) and EMA50
   computed over a gappy window ≠ the same indicators over a dense window, even
   when the individual bars are identical.
2. **Prod decided on a real-time FORMING bar.** At a 09:36:34 tick the 09:30
   15m bar held ~6 min of ticks. A historical replay has at best 5-min
   granularity and, to stay look-ahead-free, must use bars completed by the
   tick — it cannot reconstruct a sub-5m partial bar. (Bucketing match-rate by
   minutes-into-the-forming-bar showed no clean gradient, confirming the gappy
   window in (1) dominates, not the forming bar alone.)
3. **`ltp` is the live Upstox quote**, exogenous to candles (median 0.11% /
   p90 2.86% from the 15m close) — it drives entry price and the stale/VWAP
   guards.

None of these are reproducible from clean historical data, and **none are the
strategy's edge** — they are artifacts of prod's live execution (cache gaps,
3-min forming-bar cadence, quote timing). A backtest that faithfully reproduced
them would be reproducing noise *and* would carry look-ahead in the forming
bar. The swing audit reached the same fork and chose clean settled bars.

## 4. The harness the baseline will use (the sound choice)

- **Data:** `candles_5m_full` → 15m start-labeled, DENSE, look-ahead-free.
- **Decision cadence:** at each COMPLETED 15m bar within the entry window
  (09:45–13:30), apply the full prod entry pipeline (direction → score →
  affinity → brain-haircut → the ~17-gate funnel → sizing). Decisions use only
  bars closed by the decision time. (Prod scanned every 3 min on forming bars;
  the baseline scans every 15m on completed bars — same logic, cleaner timing,
  ~0–14 min later entry. Documented as a known, deliberate difference.)
- **Regimes:** real `market_brain_history` where present; reconstructed core-4 +
  risk-mode for 2022–2025 (inventory §9). Gate on the bucket, not the literal.
- **Exits:** `simulate_intraday_exit_fsm` (prod path), validated above. Sizing,
  costs (`costs.py` Upstox), slippage (0.10%/0.20%) per inventory §5/§6.

**Consequence for validation:** the baseline picks DIFFERENT trades than prod
made (different exact inputs/timing), so there is no trade-by-trade entry
replay against prod's 88 trades — only the exit side was replayable (§1). The
baseline answers "what edge does THIS logic have on clean 2022–2026 data," not
"can I reproduce prod's cache gaps." That is the right question for an audit.

## 5. Honest limitations carried into the baseline

- Entry timing lags prod by ≤1 15m bar (completed-bar cadence).
- 5m→1m exit granularity noise: ₹0 median, ±₹10–30 on boundary trades.
- `ltp`-driven gates (stale-price, live-VWAP) are evaluated on the 15m close,
  not a live quote — a small number of prod blocks/qualifications will differ.
- Regime reconstruction for 2022–2025 is core-4 + modelled risk-mode (no
  AGGRESSIVE/LOCKDOWN in those years; DEFENSIVE/NORMAL only — matches what
  `market_brain_history` recorded).
- 5m_full universe coverage of scanned names ≈ 83% (incomplete recent tail +
  newly-listed); baseline caps at 2026-06-03.
