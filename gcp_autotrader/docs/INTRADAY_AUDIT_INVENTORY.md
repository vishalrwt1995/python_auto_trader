# INTRADAY SYSTEM INVENTORY — audit task #23

> Code-grounded inventory of the INTRADAY channel as deployed in rev
> `autotrader-00254-wqk` (verified live 2026-06-13, PAPER, ₹1L channel capital,
> `RISK_PER_TRADE=250`). Every claim below carries a file:line citation against
> `main` HEAD. This is the "map before trusting" deliverable; the faithful
> harness (#24) must reproduce exactly what is documented here.
>
> Companion: `INTRADAY_AUDIT_HANDOFF.md` (mission + playbook + swing lessons).

---

## 0. REVISION (2026-06-13, same day) — live-flag corrections

The first draft of this inventory assumed the M1–M4 feature flags were at
their code defaults (OFF). **Checking the live Cloud Run env proved otherwise**
— these corrections override the sections below where they conflict:

1. **`USE_EXIT_FSM_V1=true` on `autotrader-ws-monitor`** (since rev 00038,
   2026-04-24). Production exits run `domain/exit_fsm.py::transition` via
   `_on_quote_fsm` — NOT the legacy path documented in §6. Live FSM behavior:
   - **No partials** (the 40%/30% stages are legacy-only, dormant).
   - **TARGET_HIT is REAL**: books full qty at the 1.25R/2.0R target from
     INITIAL or CONFIRMED states (limit fill, no slippage).
   - Confirm at peak-MFE ≥ 0.8R (+15s debounce; peak is monotone so the
     debounce is a pure delay, not a sustain test) → SL = entry ∓ 0.3R.
   - RUNNER (2R MFE, 2.0×ATR trail) is **unreachable** for any trade with a
     target ≤ 2R — target fires first. All current intraday trades qualify.
   - LOSING: peak ∈ [0.8R, 2R) and price retraces ≥50% of peak → SL = ltp ∓
     1.0×ATR (ratchet).
   - FLAT_TIMEOUT (120min, <0.3×ATR) any state; EOD 15:25 watchdog unchanged.
   - **No breakeven-at-1.0×ATR, no 1.5×/1.2× trail, no regime tighten** —
     all legacy-only.
   Sim: `backtest_v2/intraday_exit.py::simulate_intraday_exit_fsm` (validated
   vs the 26 FSM-era real trades: 77% exit-reason match on 5m, 81% on 1m,
   4/4 TARGET_HITs exact, median gross Δ +₹0.2, median |px Δ| 0.033%).
2. **`USE_PLAYBOOK_V1=true` on `autotrader`** — the M2 playbook gate (§3.2
   #14) is ACTIVE and fail-closed against `domain/edge.py`'s registry. Net
   effect on the cell grid (aliases: VWAP_TREND→MOMENTUM, PHASE1_MOMENTUM→
   MOMENTUM, VWAP_REVERSAL/PHASE1_REVERSAL→MEAN_REVERSION):
   - **MORNING_FADE has no Edge → `playbook_no_edge_registered` everywhere.**
     The Batch-H re-enable and the §2.3 arithmetic are moot; the setup is dead.
   - Registry regimes are the 6 base literals only → **every intraday setup is
     playbook-blocked when the brain emits EARLY_TREND_UP, EARLY_TREND_DOWN or
     RANGE_ROTATING** (`playbook_regime_not_allowed`).
   - LOCKDOWN risk mode: no edge opts in → all entries blocked in LOCKDOWN
     (the 58 threshold tier is unreachable).
   - Effective long grid: VWAP_TREND/PULLBACK/OPEN_DRIVE {TREND_UP, RECOVERY};
     MEAN_REVERSION {RANGE, CHOP, PANIC, RECOVERY} (+SELL in RANGE/CHOP);
     BREAKOUT+PHASE1_MOMENTUM {RECOVERY} (after hard-blocks);
     PHASE1_REVERSAL {RANGE, CHOP, PANIC, RECOVERY} — **not TREND_DOWN**,
     which is where it's emitted; live cell ≈ PANIC only (+stale rows).
     VWAP_TREND/PULLBACK in RANGE/RANGE_ROTATING — killed by playbook despite
     affinity 1.0–1.2 (supersedes the §2.3 matrix).
3. **`USE_EXPECTED_EDGE_R_V1=true`** — M3 active but currently a NO-OP:
   no prior in `config/priors/priors_v1.json` has n ≥ 30 with edge ≤ 0.
4. **`USE_PORTFOLIO_BOOK_V1=true`** — M4 channel budgets + DD governors live
   (daily-DD soft throttle halves qty; weekly/monthly DD hard-block).
5. **Deployment drift**: serving ws-monitor revision is `00041-gfl`
   (2026-05-15) — PROJECT_KNOWLEDGE §3 lists 00040-n5c (stale), and the
   2026-06-13 swing-overhaul edits to `ws_monitor_service.py` are NOT running
   there. Worse: under the FSM flag those edits are ineffective even after a
   redeploy — the swing "resting-SL only" gating lives in the legacy path,
   while `_on_quote_fsm` processes swing positions through the full FSM
   (is_swing only exempts FLAT_TIMEOUT). Live swing positions therefore get
   FSM confirm-stops (entry−0.3R after 0.8R) and 2R TARGET_HIT exits — not
   the validated daily-1R-trail-only design. Surfaced to the user 2026-06-13;
   resolution pending.

---

## 1. How an intraday trade happens (end-to-end, one paragraph)

Cloud Scheduler hits `/scan` every 3 min 09:15–14:00 IST (+ a 15:00–15:27 tail
that can't enter — entry window is 09:45–13:30, `time_utils.py:115-148`). The
scanner (`trading_service.run_scan_once`) reads the intraday watchlist (built
premarket + refreshed ~12×/day by `universe_service.build_watchlist`), slices
**35 rows/tick** (10 core + 25 rotated, `trading_service.py:172-228`), injects
MORNING_FADE companion rows 09:15–10:15 (`trading_service.py:751-765`), computes
indicators on **15-minute candles** (`trading_service.py:1028-1030`,
`timeframe="15m"`, needs ≥80 bars), votes a direction, scores 7 layers, applies
the regime-affinity multiplier and the brain risk-mode haircut, walks a ~17-gate
elif chain, sizes off ₹250 risk with regime/ATR%-scaled SL width and brain size
multipliers, and fires a paper MARKET order at live LTP +0.10% slippage
(`order_service.py:646-651`). Exits are tick-driven in `ws_monitor_service`
(partials at 1R/1.5R, breakeven at 1×ATR, 1.5×ATR trail, regime tighten, 2h flat
timeout, 15:25 EOD), with a 60s paper-GTT poll as SL backstop. Paper exit fills
at LTP −0.20% (non-target reasons); net P&L books `risk.py` brokerage.

---

## 2. The cell grid — (setup × regime) reachability

### 2.1 Setups: where each label comes from

| Setup | Emitted by | Emission condition (build time) |
|---|---|---|
| `PHASE1_MOMENTUM` | Phase 1 premarket/daily scoring | canonical regime NOT in {PANIC, TREND_DOWN} (`universe_service.py:5029,5066-5069`) |
| `PHASE1_REVERSAL` | Phase 1 | canonical regime IN {PANIC, TREND_DOWN} (`universe_service.py:5029,5046-5065`) |
| `OPEN_DRIVE` | Phase 2 in-play | before 10:45 + ORB UP_BREAK + above VWAP + volume_shock ≥ 0.50 + policy `open_drive_enabled` (`universe_service.py:5223-5232,5286`) |
| `BREAKOUT` | Phase 2 | ORB UP_BREAK + above VWAP + volume_shock ≥ 0.35 + policy `breakout_enabled` (`universe_service.py:5233-5236,5279`) |
| `MEAN_REVERSION` | Phase 2 | regimeIntraday == CHOPPY + reversal fired + extension ≥ 0.60 + below VWAP (`universe_service.py:5237-5247`) |
| `PULLBACK` | Phase 2 | above VWAP + rising VWAP slope ≥ 0.35 + volume ≥ 0.35 + no ORB DOWN_BREAK (`universe_service.py:5248-5256`) |
| `VWAP_TREND` | Phase 2 catch-all | above VWAP (`universe_service.py:5257-5258`) |
| `VWAP_REVERSAL` | Phase 2 catch-all | below VWAP (`universe_service.py:5259-5260`) — **kill-switched, see §2.3** |
| `MORNING_FADE` | Scanner overlay | injected against every intraday row 09:15–10:15 (`trading_service.py:727-765`) |

Phase 2 needs ≥3 today 5m bars + volume-slot baseline (`_phase2_eligibility`,
`universe_service.py:3443-3560`); rows that fail fall back to their Phase 1
label (`PHASE1_DAILY_FALLBACK`, `universe_service.py:5319-5343`). Watchlist
target ≤150 rows, scaled 0.50–1.0 by phase2 quality (`universe_service.py:5345-5382`)
and by policy `watchlist_target_multiplier` (0.60 LOCKDOWN / 0.75 DEFENSIVE).
Universe = top-N by 60d turnover (N = 250/500/800 by mode,
`universe_service.py:729-755`, gate at `:4609-4616`).

`SHORT_BREAKDOWN` / `SHORT_PULLBACK` are **never emitted intraday** — they only
existed as swing labels (now disabled). Intraday SELL trades can only arise as:
MEAN_REVERSION SELL (RSI ≥58/60 forced direction), VWAP_REVERSAL SELL (dead),
VWAP_TREND SELL (democratic vote — but its gate then needs sustained-below-VWAP
+ RSI 30–50), MORNING_FADE (forced SELL).

### 2.2 Gate layers that decide reachability (scan time)

A (setup, regime) cell is live only if it passes ALL of:

1. **Emission** (§2.1) — uses BUILD-time regime; rows persist across intraday
   regime flips, so a cell can be hit at scan time in a regime that would never
   have emitted it. The harness must model build-time vs scan-time regime
   separately.
2. **Brain allowlist** `allowed_strategies` (`market_brain_service.py:1313-1359`):
   full 12-setup list, minus {BREAKOUT, OPEN_DRIVE, MOMENTUM, PHASE1_MOMENTUM}
   when scan-regime ∈ {CHOP, PANIC, TREND_DOWN}; minus `disabled_strategies =
   ("VWAP_REVERSAL",)` always (`settings.py:146`). NOTE: EARLY_TREND_DOWN is
   NOT pruned (only literal "TREND_DOWN" is) — an asymmetry worth flagging.
   Enforced as `policy_strategy_blocked` via `_strategy_allowed`
   (`trading_service.py:123-149`) which has fuzzy substring matching.
3. **Hard blocks** `_HARD_BLOCKS` (`regime_affinity.py:279-374`), reason
   `regime_strategy_hard_block`.
4. **Direction policy**: `short_enabled=False` in TREND_UP/RECOVERY when
   long_bias ≥ 0.65 (`market_policy_service.py:70-71`; TREND_UP bias 0.78,
   RECOVERY 0.68, capped to 0.60 if participation WEAK —
   `market_brain_service.py:1083-1099`); plus
   `nifty_breadth_too_bullish_for_shorts` when SELL + regime ∈ {RANGE, CHOP,
   RECOVERY} + breadth ≥ 75 (`trading_service.py:1468-1491`; the daily-trend
   exemption is swing-only).
5. **Score arithmetic** (§4) — affinity × brain haircut vs risk-mode threshold
   can make a cell *mathematically dead* even when "allowed" (see MORNING_FADE
   below).

### 2.3 Reachability matrix (setup × scan-regime)

Legend: ✅ reachable · ❌H hard-block · ❌P policy allowlist · ❌K kill-switch ·
❌E never emitted in that build regime (reachable only via stale rows after a
flip) · ⚠️ reachable but score-dead or sliver (explained below).

| Setup \ Regime | TREND_UP | EARLY_TU | RANGE | RANGE_ROT | CHOP | TREND_DOWN | EARLY_TD | PANIC | RECOVERY |
|---|---|---|---|---|---|---|---|---|---|
| VWAP_TREND | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| VWAP_REVERSAL | ❌K | ❌K | ❌K | ❌K | ❌K | ❌K | ❌K | ❌K | ❌K |
| PULLBACK (P2) | ✅ | ✅ | ✅ | ✅ | ❌H | ✅ | ✅ | ❌H | ✅ |
| MEAN_REVERSION (P2) | ✅⚠️E | ✅⚠️E | ✅⚠️E | ✅⚠️E | ✅ | ✅⚠️E | ✅⚠️E | ✅ | ✅⚠️E |
| BREAKOUT (P2) | ❌H | ❌H | ❌H | ❌H | ❌H+P | ❌H+P | ❌H | ❌H+P | ✅ (only cell!) |
| OPEN_DRIVE (P2) | ✅⚠️sliver | ✅⚠️sliver | ❌H | ✅⚠️sliver | ❌H+P | ❌P | ✅⚠️sliver | ❌H+P | ✅⚠️sliver |
| MORNING_FADE | ❌H | ❌H | ✅ | ❌H | ✅ (71 vs 65, tight) | ⚠️dead (62<65) | ✅ at NORMAL only | ❌H | ❌H |
| PHASE1_MOMENTUM | ❌H | ❌H | ❌H | ❌H | ❌H+P | ❌P (❌E) | ✅(❌E?) | ❌H+P (❌E) | ✅ |
| PHASE1_REVERSAL | ✅⚠️E | ✅⚠️E | ✅⚠️E | ✅⚠️E | ✅⚠️E | ✅ | ✅(E?) | ✅ | ✅⚠️E |

Notes (all verified in code):

- **OPEN_DRIVE in RANGE/RANGE_ROTATING is ❌H** (`regime_affinity.py:299-308,350-356`).
  Where allowed, it is a **sliver cell**: its gate requires the last 15m bar
  timestamp in 09:15–09:45 (`scoring.py:716-721`) but the entry window opens
  09:45 (`time_utils.py:148`) — only scans landing ~09:45–10:00 whose last bar
  is the 09:45 (or stale 09:30) bar can fire. Additionally policy
  `open_drive_enabled=False` in DEFENSIVE/LOCKDOWN risk modes
  (`market_policy_service.py:44,55`) — and TREND_DOWN/CHOP force DEFENSIVE
  (`market_brain_service.py:1067-1073`), so EARLY_TD reachability depends on
  risk mode at scan.
- **MORNING_FADE viability is pure score arithmetic** (raw hardcoded 75,
  `scoring.py:178-179`): RANGE → 75×1.4(affinity)=clamp 100, ×1.0 NORMAL = 100
  vs 72 ✓ comfortable. CHOP → 75×1.3=98, ×0.82(DEFENSIVE forced by CHOP)
  ×0.88(CHOP) = 71 vs 65 ✓ by 6 points. TREND_DOWN → 75×1.0=75, ×0.82 = 62
  vs 65 ✗ **mathematically dead** despite Batch H re-enabling it there.
  EARLY_TREND_DOWN → 75×1.0, alive only when risk mode is NORMAL (75 ≥ 72).
  The harness must reproduce this arithmetic per cell rather than trust labels.
- **BREAKOUT×RECOVERY is the only BREAKOUT cell** (RECOVERY hard-blocks only
  MORNING_FADE, `regime_affinity.py:344`). Emission still requires Phase 2
  ORB UP_BREAK + `breakout_enabled` (False in LOCKDOWN/CHOP/PANIC only).
- **PHASE1_MOMENTUM** is hard/policy-blocked everywhere except RECOVERY and
  EARLY_TREND_DOWN (`regime_affinity.py:329-373`, allowlist prune
  `market_brain_service.py:1333-1346`). Emission requires a non-bearish build
  regime, so the EARLY_TD cell is only hit via stale rows or if canonical-regime
  folding maps EARLY_TD → non-bearish at build (verify `canonical_regime`
  folding in harness).
- **PHASE1_REVERSAL** is never hard-blocked and never pruned — its real cells
  are PANIC/TREND_DOWN (emission); elsewhere only via stale rows after a flip.
- **MEAN_REVERSION (Phase 2 label)** is only emitted when regimeIntraday ==
  CHOPPY (`universe_service.py:5237-5247`) — but the brain scan-regime gate
  never blocks it (not in any hard-block set; affinity 1.4 in RANGE). So its
  live cells depend on the CHOPPY classification × scan regime joint
  distribution. Note MR can also reach scans as a swing label — out of scope.

**Audit implication:** the *theoretical* grid is 9 setups × 9 regimes = 81
cells, but ≤ ~25 are reachable, several only via stale-row dynamics or in
slivers, and at least one (VWAP_REVERSAL) is globally dead yet still consumes
watchlist slots and scan cycles (`universe_service.py:5259-5260` keeps emitting
rows that `policy_strategy_blocked` then rejects every tick). For the baseline,
report per-cell counts of scans/qualifications/trades so dead-but-emitting
cells are visible as waste.

---

## 3. Every gate in the funnel, in exact order, and why it exists

### 3.1 Scan-level (once per tick) — `run_scan_once` (`trading_service.py:379-...`)

| # | Gate | Effect | Why (provenance) |
|---|---|---|---|
| S1 | lock `run_scan_once_intraday` TTL 180s (`:403-408`) | skip tick | 2026-04-22: swing/intraday lock collision starved swing |
| S2 | kill switch, fail-closed (`:414-420`) | halt entries | M0.1 |
| S3 | `is_market_open_ist()` — weekday+clock+holiday (`:422-424`, `time_utils.py:106-112`) | skip | 2026-05-28 Bakri Eid incident |
| S4 | brain build; stale fallback caps DEFENSIVE if >90min old (`:431-473`) | haircut all | no-silent-fallback rule |
| S5 | per-channel daily PnL: loss ≥3% → channel restricted to counter-trend setups {MEAN_REVERSION, VWAP_REVERSAL, PHASE1_REVERSAL}; profit ≥6% → channel blocked (`:574-633`, applied per-row `:1563-1575`) | circuit breaker | Phase C (2026-05-28) |
| S6 | **`max_trades_day=5` — GLOBAL across BOTH channels** (`:645-666`; counts ALL positions entered today, `firestore_state.py:243-262`) | skip entire scan (incl. the other channel's tick) | Batch 1.1; cost control. ⚠️ swing entries consume intraday budget and vice versa |
| S7 | re-entry cooldown map — symbols exited <30min ago, ANY channel (`:943-968`, `firestore_state.py:205`) | per-row block | Batch 2.1 churn fix |
| S8 | earnings blackout doc, fail-closed (`:906-929`) | per-row block | gap risk |
| S9 | watchlist slice: 10 core + 25 rotated per tick, cursor in Firestore (`:172-228`) | coverage cadence | Upstox rate budget |
| S10 | MORNING_FADE overlay injection 09:15–10:15 (`:743-765`) | adds rows | 2026-05-06 (universe doesn't emit MF) |
| S11 | `max_signals_allowed` = max(2, floor(3 × max_pos_mult)), raised to ≥5 in TREND_UP/RECOVERY (`:523-536`) | cap **qualifications per tick** (NOT open positions) | regime-adaptive slots |

⚠️ **There is no standing open-position cap for intraday.** "3 slots" from the
docs is actually the per-tick qualification cap (2–5). Concurrency is bounded
in practice by S6 (5 entries/day global), one-per-(symbol,side)-per-day (§3.3),
max-2-per-setup, max-2-per-sector, and channel capital exhaustion.

### 3.2 Per-row elif chain (first match wins → `blocked_reason`) (`trading_service.py:1430-1665`)

Pre-chain computation: indicators on 15m (≥80 bars else `insufficient_candles`),
direction vote (§4.1), raw score → ×affinity → clamp 0–100 → ×brain haircut →
`adjusted_score`; position sized (§5) BEFORE gates so `sl_too_wide` can fire.

| # | `blocked_reason` | Condition | Why |
|---|---|---|---|
| 1 | `sl_too_wide_for_risk_budget` | qty==0 from sizing | risk contract |
| 2 | `channel_capital_exhausted` | channel cap + today channel PnL − channel open risk < new max_loss (`:1444-1453`) | Layer 21.6 solvency |
| 3 | `reentry_cooldown` | symbol exited <30 min ago | churn |
| 4 | `policy_long_disabled` / `policy_short_disabled` | from MarketPolicy (long never disabled in code today) | regime direction policy |
| 5 | `nifty_breadth_too_bullish_for_shorts` | SELL + {RANGE,CHOP,RECOVERY} + breadth≥75 | short squeeze protection |
| 6 | `policy_strategy_blocked` | `_strategy_allowed` vs brain allowlist (incl. VWAP_REVERSAL kill-switch + CHOP/PANIC/TREND_DOWN pruning) | strategy kill-switch |
| 7 | `regime_strategy_hard_block` | `_HARD_BLOCKS` | dead-cell elimination |
| 8 | `symbol_already_held` | open (symbol, direction) | DMART double-hold fix |
| 9 | `policy_max_positions_reached` | qualified-this-tick ≥ max_signals_allowed | per-tick throttle |
| 10 | `intraday_daily_loss_limit_strategy_restricted` / `intraday_daily_profit_target_hit` | channel halt from S5 | Phase C |
| 11 | `stale_signal_price_moved` | LTP vs 15m-close drift > max(1.2%, min(4%, 1.5×ATR%)) (`:1580-1583`) | don't chase |
| 12 | `live_price_below_vwap` / `live_price_above_vwap` | LTP wrong side of VWAP after 09:30; exempt MR/VWAP_REV (both sides), MORNING_FADE (SELL) (`:1586-1599`) | entry-quality guard; MF exemption 2026-05-27 |
| 13 | per-setup gate from `check_strategy_entry` (§4.2) | setup-specific | thesis validation |
| 14 | `playbook_*` / `expected_edge_non_positive` | **ACTIVE — see §0.2/§0.3** (flags ON in live env) | M2 fail-closed edge registry; M3 priors gate (currently no-op) |
| 15 | `portfolio_sector_concentrated` | ≥2 open positions same sector | correlation |
| 16 | `portfolio_strategy_concentrated` | ≥2 open positions same setup | correlation |
| 17 | `earnings_blackout_*` | ±2 trading days of results | gap risk |

Qualification (`:1670`): `direction != HOLD` AND `adjusted_score ≥
dynamic_min_score` AND `is_entry_window_open_ist("intraday")` (09:45–13:30) AND
no block. Thresholds (`:1297-1302`): AGGRESSIVE 75 / NORMAL 72 / DEFENSIVE 65 /
LOCKDOWN 58; −5 (floor 60) when regime ∈ {RANGE, CHOP} + breadth>85 + trend<30
(`:1347-1353`).

### 3.3 Order layer (`order_service.place_entry_order`)

- **One entry per (symbol, side) per trading day** — atomic `fired_signals`
  acquire (`order_service.py:536`), released only on error paths, never on
  normal close. Stronger than the 30-min cooldown; harness must model it.
- SL sanity (side-inversion fail-loud, `:561-591`); **SL floor 0.8% of entry**
  (widen + re-scale target to preserve R:R, `:596-613`).
- Paper fill = scan LTP × (1 + 0.10%) adverse (`:646-651`).

---

## 4. Signal logic (what the harness must reproduce bit-for-bit)

### 4.1 Direction vote (`scoring.py:12-156`)
Forced: MORNING_FADE → SELL (`:32-33`). MR/VWAP_REVERSAL → pure intraday-RSI
rule: RANGE/CHOP BUY ≤45 / SELL ≥58, else BUY ≤40 / SELL ≥60 (`:47-84`).
Everything else: weighted bull/bear vote (supertrend 3, VWAP side 2, EMA9>21 2,
EMA21>50 1, RSI>55/<45 1, MACD hist 2 + cross 1, engulf 1, regime bias 2) with
**margin >2** for intraday (`:143-144`); long-only setups {BREAKOUT, MOMENTUM,
OPEN_DRIVE, PULLBACK} veto SELL→HOLD; short-only {SHORT_*, MORNING_FADE} veto
BUY→HOLD (`:97-98,149-155`).

### 4.2 Per-setup entry gates (`check_strategy_entry`, `scoring.py:425-767`)

| Setup | Gates (exact) |
|---|---|
| BREAKOUT | ADX≥20; dist_52w ≤5% (BUY); vol_ratio≥1.2; ≥13 bars; green entry bar; close > prior-12-bar high; cur vol > 1.1× prior-4-bar avg (`:446-497`) ⚠️ on 15m bars "52w high" = max of last 252 **15m** closes ≈ **10-day high** (`indicators.py:283-284`) |
| PULLBACK | BUY: ema_stack required; RSI 35–70; price within ±5% of EMA9 (`:499-545`) |
| MEAN_REVERSION / VWAP_REVERSAL | BUY: below VWAP + RSI ≤45 (RANGE/CHOP) or ≤40; SELL: above VWAP + RSI ≥58/≥60; VWAP deviation ≥0.6% (`:547-599`) |
| VWAP_TREND | last-bar ts ≥10:15; last 3 closes same side of VWAP; correct side; ADX≥22; vol≥1.3; RSI 50–70 BUY / 30–50 SELL (`:601-651`) |
| PHASE1_MOMENTUM | long-only; vol_ratio≥0.8 (`:653-662`) |
| PHASE1_REVERSAL | long-only; RSI ≤55; vol_ratio≥0.8 (`:664-676`) |
| MOMENTUM | ADX≥20; vol≥1.3; above VWAP; RSI 55–75; EMA9>EMA21 (`:678-706`) — only reachable intraday if a row carries label MOMENTUM (universe emits it as swing); flag in harness |
| OPEN_DRIVE | last-bar ts 09:15–09:45; ADX≥18; vol≥1.2; correct side of VWAP (`:708-730`) |
| MORNING_FADE | SELL-only; last-bar ts 09:45–10:15; pop ≥ +1.5% from session open; vol_ratio ≥1.0 (`:732-764`) |

### 4.3 Score (`score_signal`, `scoring.py:159-388`)
MORNING_FADE: hardcoded 75. Else 7 layers: Regime 20 (nifty chg / VIX<15/<20 /
FII) + Options 15 (PCR, OI-change PCR, max-pain) + Technical 35 (supertrend,
VWAP side, EMA stack, RSI band 45–65/35–55, MACD, ADX≥20/30, engulf, RS vs
Nifty) + Volume 10 (ratio ≥1.5/1.2/1.0 + OBV) + Alignment ±15 (daily_bias trend
× strength) + Penalties (VIX>18/22 BUY-only, RANGE −8 for non-range setups, ADX<15
−5, bar range >2.5% −5, doji −3, BB-pierce −5, stoch −4).
**Production quirk (verified 2026-05-22, PROJECT_KNOWLEDGE §8):** prod's
RegimeSnapshot leaves vix/fii/nifty.change_pct = 0 → Layer 1 scores a constant
+13 for BUY scans (vix=0 < 15 → +7; |chg|<0.1 → +4; |fii|<500 → +2); PCR layer
also degrades to defaults. The harness must replicate THIS, not the idealized
formula — `prod_replica_v2.py` already validated 200/200 scans match using
`MarketRegimeService.from_market_brain_state()`.

### 4.4 Score adjustment chain (`trading_service.py:1176-1184`)
`raw` → × `regime_strategy_multiplier(regime, setup, direction)` (clamped
0.2–1.4, SELL dampened to ≤0.6 in TREND_UP/RANGE_ROTATING except counter-trend
setups; `regime_affinity.py:195-247`) → clamp 0–100 → × brain haircut
(AGGRESSIVE 1.08 / NORMAL 1.0 / DEFENSIVE 0.82 / LOCKDOWN 0.60, ×0.88 extra if
regime ∈ {CHOP, PANIC}; `market_policy_service.py:159-172`) → clamp → compare
to risk-mode threshold.

---

## 5. Sizing — the ₹250 chain (`trading_service.py:1189-1254`, `risk.py:93-144`)

1. **ATR mult**: base 1.5 → regime scale: LOCKDOWN/PANIC ×0.75; DEFENSIVE or
   {TREND_DOWN, CHOP} ×0.87; AGGRESSIVE+TREND_UP ×1.20; MR/VWAP_REV in
   {RANGE, RECOVERY} ×1.33 (≈2.0 eff) → ATR%-tier: <1.5% ×0.87; 1.5–3% ×1.20;
   >3% ×1.0 → clamp [0.8, 3.0].
2. `sl_dist = max(atr_mult × ATR15m, 0.5% × price)` (`risk.py:115`); R:R 1.25
   (2.0 for MR/VWAP_REV); `raw_qty = floor(250 / sl_dist)`.
3. Cap: `qty ≤ floor(0.15 × ₹1L / price)` → **₹15K max position**
   (`risk.py:122-124`).
4. **Over-risk quirk** (`risk.py:128-131`): if raw_qty==0 but sl_dist ≤ ₹375
   (1.5×250), force qty=1 → risk up to ₹375 = 1.5× budget. Only skip when
   sl_dist > ₹375.
5. Brain sizing (`market_policy_service.py:174-213`): qty × size_mult
   (AGG 1.15→×1.30 TREND_UP cap 1.50 / NORM 1.0 / DEF 0.65 / LOCK 0.40, PANIC
   cap 0.50) × **min**(setup_conf, liq, dq) where setup_conf =
   clamp(score/100+0.20, 0.45, 1.30), liq = 1.0 or 0.85 (vol_ratio<1), dq =
   clamp(dq/100, 0.6, 1.1); floor(qty), min 1; LOCKDOWN additionally halves.
6. **Effective economics**: typical position ₹6K–15K. Upstox real round trip on
   ₹15K ≈ ₹46–54 (intraday); on DEFENSIVE-sized ₹6K ≈ ₹25–35. Vs ₹250 (or
   ₹162 DEFENSIVE-effective) risk → **costs are 15–25% of risk per trade
   before any edge**. This is the cost-share-first number to compute per cell.
7. **Cost-model discrepancy**: live paper P&L books `risk.py:calc_brokerage_leg`
   (min(₹20, 0.05%) + STT 0.025% both legs) — that is neither Upstox real
   (0.1% cap ₹20 brokerage, STT sell-side 0.025% intraday) nor `costs.py`.
   On a ₹12K position risk.py books ≈ ₹19.6 RT vs Upstox-real ≈ ₹40.7 RT.
   **Paper NET P&L understates real costs ~2×.** Baseline must use `costs.py`
   (Upstox default) and report the delta vs prod-booked numbers.

---

## 6. The intraday exit stack — LEGACY path (⚠️ DORMANT since 2026-04-24: prod runs the FSM, see §0.1)

Evaluation order per tick (`_on_quote`, `:323-737`), tick = Upstox WS LTP:

| # | Mechanism | Trigger | Action | Cite |
|---|---|---|---|---|
| 1 | MFE/MAE tracking | every tick | bookkeeping (used by attribution) | `:364-413` |
| 2 | Emergency SL | sl_price==0 | SL = entry ∓ 2.0×ATR | `:415-428` |
| 3 | Breakeven move | best ≥ entry + **1.0×ATR** (not 1R!) | SL = entry + 0.1×ATR; one-shot `sl_moved` | `:452-525` |
| 4 | Target-passed switch | LTP crosses target (1.25R) | NO exit — `target_passed=True`, trail tightens to 1.2×ATR | `:527-543` |
| 5 | Regime tighten | entry regime ∈ {TREND_UP, RECOVERY} and current ∈ {CHOP, PANIC, TREND_DOWN} | SL = LTP ∓ 0.8×ATR; one-shot | `:545-591` |
| 6 | Partial 1 (qty≥3) | LTP ≥ entry + 1.0×sl_dist (=1R) | exit 40% (`max(1, int(0.4×qty))`), SL→BE (entry±0.1×ATR) | `:593-632` |
| 7 | Partial 2 (qty≥3) | LTP ≥ entry + 1.5×sl_dist | exit 30% | `:634-642` |
| 8 | Degraded partial (qty==2) | 1R | exit 1 share, SL→BE | `:644-671` |
| 9 | Trailing (once sl_moved) | every tick | SL = best ∓ 1.5×ATR (1.2× post-target); ratchet-only | `:683-706` |
| 10 | SL_HIT | LTP ≤ SL (BUY) | full exit | `:708-720` |
| 11 | TARGET_HIT | effectively **dead** — #4 sets target_passed in the same tick before exit check | — | `:713-720` |
| 12 | FLAT_TIMEOUT | ≥120 min AND \|LTP−entry\| < 0.3×ATR | full exit | `:321,722-729` |
| 13 | EOD_CLOSE | 15:25 watchdog (intraday only); hard stop 15:30 | full exit | `:40-41,950-971` |
| 14 | Paper GTT reconciler | 60s REST poll vs SL trigger | SL_HIT_PAPER_GTT backstop | `:977-1025` |
| 15 | EOD recon crons | 15:10/15:20/15:30 `reconcile_open_positions` | paper close at LTP, EOD_CLOSE | `order_service.py:1220-1338` |

Notes for the exit-fidelity gate:
- All thresholds are in **ATR units of the 15m ATR stored at entry** (static),
  except partials which use **sl_dist** (= atr_mult_eff × ATR, so 1R). Breakeven
  (1.0×ATR) fires before partial-1 (1R = atr_mult_eff×ATR) whenever
  atr_mult_eff > 1.0, e.g. mid-vol tier 1.8×ATR → BE arms at 0.56R.
- Tick-driven: the 5m-bar harness must adopt explicit, conservative intra-bar
  ordering conventions (e.g. adverse-first: SL evaluated before favorable
  triggers within a bar) and **validate against 1m bars** (handoff lesson 8). MFE-based triggers (BE, trail) depend on tick path, not
  bar OHLC alone — 5m introduces systematic optimism/pessimism that must be
  measured, not assumed.
- Paper exit fill = LTP at detection ∓ 0.20% for ALL non-TARGET reasons
  (`order_service.py:881-890`) including partials; TARGET fills clean (but
  TARGET_HIT is dead per #11 — in practice every exit pays 0.20%).
- Brokerage on partials: each partial books `calc_round_trip_brokerage(exit_qty,
  entry, exit)` (`order_service.py:1075`), final close books remaining-qty RT +
  accumulated partial brokerage (`:258-264`).

---

## 7. Scheduler timing (entry-relevant)

- Scans: `*/3 9-14` + `0-27/3 15` IST (PROJECT_KNOWLEDGE §3). Effective entry
  ticks: 09:45–13:30 (~76 ticks/day). Watchlist builds: premarket 09:00, 5m
  blocks 09:30/10:00, 15m blocks 10:45, 11:00–12:00, 13:00, 13:30, 14:00,
  14:15, final 14:45.
- Brain snapshots are produced inside each scan tick (build_post_open) and at
  watchlist builds; `market_brain_history` BQ has the real regime/risk_mode
  sequence (verify coverage window — needed for replay vs reconstruction).

---

## 8. Known dead paths / quirks the audit must quantify (found during inventory)

1. **VWAP_REVERSAL zombie rows** — emitted by Phase 2, kill-switched at policy.
   Burned watchlist slots + scan cycles; also MR-SELL-shaped edge is therefore
   untested in prod.
2. **TARGET_HIT is unreachable intraday** — 1.25R "target" is actually a
   trail-tightening trigger; realized R distribution is governed by partials +
   1.2×ATR trail + 0.20% slippage on every exit leg.
3. **Breakeven at 1.0×ATR vs sl_dist mismatch** — arms as early as 0.56R
   (mid-vol tier), guaranteeing scratch exits on normal oscillation for the
   60% runner.
4. **₹375 over-risk forced qty=1** (`risk.py:128-131`) on wide-SL names.
5. **max_trades_day=5 shared with swing** — swing fires first at 09:22; on
   active swing days intraday can be starved (and vice versa); FLAT/EOD churn
   burns budget.
6. **OPEN_DRIVE sliver window** (gate 09:15–09:45 bars vs 09:45 entry open).
7. **15m "52w-high" is a 10-day high** for BREAKOUT gates (`indicators.py:283`).
8. **Cost model triple-mismatch**: prod books risk.py costs (~½ of real);
   backtests must use costs.py Upstox; live Upstox differs again on STT side.
9. **EARLY_TREND_DOWN asymmetry**: inherits TREND_DOWN hard-blocks but NOT the
   allowed_strategies pruning → OPEN_DRIVE/PHASE1_MOMENTUM/MOMENTUM allowed
   there by policy (mostly moot via emission, but stale rows can hit it).
10. **Stale-row × regime-flip dynamics**: emission regime ≠ scan regime; rows
    persist between builds. The per-cell attribution must record BOTH.

---

## 9. What the faithful harness must therefore model (input to #24)

1. **Data**: `candles_5m_full` (2022-01→2026-06) resampled to 15m for scanner
   indicators (alignment 09:15 anchor; resolve the partial-last-bar question by
   replaying recent days against recorded `scan_decisions` rows — they store
   rsi/vwap/atr/adx/vol_ratio/ltp per scan); 5m for Phase 2 build signals; 1m
   (`candles_1m`) for exit-path validation; LTP ≈ bar close of the active 5m
   bar at tick time (+ slippage model).
2. **Regime/risk-mode timeline**: real `market_brain_history` where it exists;
   reconstructed core-4 (`brain_reconstruct.CORE_MAP`) + risk-mode rules for
   2022+ — risk mode drives thresholds (75/72/65/58), size (1.15/1.0/0.65/0.40)
   and DEFENSIVE-forcing in CHOP/TREND_DOWN, so its reconstruction fidelity is
   load-bearing. Gate on the bucket, not the literal label (lesson 4).
3. **Watchlist**: Phase 1 scoring from daily candles + Phase 2 from 5m at each
   build cron; selection with diversification/correlation caps; ≤150 rows;
   35-row rotation per tick with persistent cursor.
4. **Account walk** (lesson 7): 5 global trades/day shared with swing (model
   swing's consumption or document the assumption), per-tick qualified cap,
   fired-today (symbol,side), 30-min cooldown, sector/strategy concentration,
   channel capital + 3%/6% halts, capital-exhaustion.
5. **Exits**: pure exit function on 5m with explicit intra-bar conventions,
   validated against 1m on a sample; then prove harness ≡ pure function on
   every simulated position (lesson 6 — `test_swing_exit` fidelity pattern).
6. **Costs**: `costs.py` Upstox default everywhere; report GROSS and NET;
   per-cell cost share of gross (lesson 1); slippage 0.10%/0.20% as prod
   models it, with a sensitivity row at 0/half/double.

---

*Inventory complete 2026-06-13. Next: #24 harness design (see audit handoff
checklist). Do not trust this document over code — re-verify any line that a
fix will depend on.*
