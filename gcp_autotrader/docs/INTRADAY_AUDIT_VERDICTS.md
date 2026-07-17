# INTRADAY AUDIT — cell-by-cell verdicts (running log)

> Per-cell findings from the clean-room, prod-faithful baseline (#24b → #25).
> Harness: `backtest_v2/intraday_baseline.py` (multi-emit per-setup edge scan,
> top-500 BALANCED universe, FSM exit, Upstox costs, per-capital ₹1L/2L/3L/5L),
> 2022–2026, 37,504 trades. Reconstructed regime/risk-mode from `market_brain_history`.
> See INTRADAY_AUDIT_FIDELITY.md for why this is clean-room (not bit-replay).
>
> **Universal test applied to each cell** (the "can it be made profitable" gate):
> 1. **Gross R** per trade, per year — is the *entry* positive before any cost?
> 2. **Path-aware MFE** — favorable excursion *before* the 1R stop, and the
>    expectancy of every fixed take-profit. Proves whether an *exit* fix exists.
> 3. **Sub-slice** (time-of-day / ADX / RSI / volume) — does any sub-condition
>    have robust (multi-year) positive-gross edge? Proves whether a *selection*
>    fix exists.
> A cell can only be "made profitable" if (2) or (3) finds something; costs/
> sizing never rescue a negative-gross entry.

---

## Baseline overview (all cells, 2022–2026, prod ₹1L)

| Cell (setup × regime) | n | WR% | gross R | NET ₹1L | NET ₹5L | cost%G |
|---|---|---|---|---|---|---|
| PULLBACK × TREND_UP | 16,594 | 26 | −0.39 | −888,076 | −1,102,215 | 94% |
| MOMENTUM × TREND_UP | 14,430 | 28 | −0.38 | −775,781 | −962,124 | 94% |
| MEAN_REVERSION × RANGE | 4,712 | 21 | −0.28 | −202,099 | −208,439 | 83% |
| VWAP_TREND × TREND_UP | 1,687 | 26 | −0.38 | −88,270 | −109,759 | 98% |
| OPEN_DRIVE × TREND_UP | 80 | 32 | −0.19 | −3,224 | −3,440 | 150% |
| PHASE1_REVERSAL × RANGE | 1 | 100 | +1.16 | +86 | +86 | (n=1) |

- **Every cell negative gross**, every year. Capital scales the loss ₹1L→₹2L
  (cap binds, positions ~double), then saturates ₹2L→₹5L (₹250 risk binds).
- MOMENTUM is a swing label (not a standard prod intraday emission) — edge-
  discovery only. PHASE1_REVERSAL n=1 = noise. Per-year totals: every year net
  negative (2022 −129k … 2023 −902k … 2026 −119k at ₹1L).

---

## A1 — VWAP_TREND × TREND_UP   →   VERDICT: REMOVE (unfixable)

Prod's primary intraday cell (most-qualified in `scan_decisions`). 1,687 trades,
2022-08 → 2026-05.

- **Gross R −0.38, WR 26%, negative every year** (gross R −0.34 to −0.42, dead
  stable). Net −88k @₹1L (−110k @₹5L), 98% cost-share.
- **Exit fix? NO.** Path-aware MFE +0.96R avg / +0.50R median (drift exists),
  but every fixed take-profit is negative expectancy: TP 0.3R → −0.195R, 0.5R →
  −0.25R, 0.8R → −0.31R, 1.0R → −0.36R. Favorable excursion (+0.96R) ≈ adverse
  (MAE −1.33R) → **noise, not edge**. No exit/stop/trail/partial turns it positive.
- **Selection fix? NO.** No sub-slice is robustly positive: time-of-day all
  −0.32..−0.54 (0–1/5 +years); ADX all −0.36..−0.39 (0/5); RSI all −0.26..−0.41
  (0–1/5); volume all −0.37..−0.39 (0/5).
- **Conclusion:** the entry carries no directional information — structurally a
  coin flip. Cannot be tuned into profit (no exit/selection/cost lever exists).
  Profitability would require a *new entry signal*, not a parameter change.

---

## A2 — PULLBACK × TREND_UP   →   VERDICT: REMOVE (unfixable)

Highest-volume cell. 16,594 trades, 2022-08 → 2026-05.

- **Gross R −0.39, WR 26%, negative every year** (−0.30 to −0.51). Net −888k
  @₹1L (−1,102k @₹5L), 94% cost-share. Exit mix SL 42% / FLAT 28% / TARGET 24%.
- **Exit fix? NO.** Path-aware MFE +1.05R avg / +0.50R median; every fixed TP
  negative (best TP 0.3R → −0.217R; 0.5R −0.25R; 1.0R −0.32R). Favorable ≈
  adverse → noise.
- **Selection fix? NO.** Every sub-slice negative, 0–1/5 +years: hour −0.34..−0.46;
  ADX −0.31..−0.41; RSI −0.30..−0.40; volume −0.36..−0.41.
- **Conclusion:** identical to A1 — coin-flip entry, no tuning lever. REMOVE.

## A3 — MEAN_REVERSION × RANGE   →   VERDICT: REMOVE (unfixable)

4,712 trades, 2022-05 → 2026-05. The RANGE counter-trend cell.

- **Gross R −0.28, WR 21%, negative every year** (−0.13 to −0.35). Net −202k
  @₹1L, 83% cost-share. Exit mix SL 37% / FLAT 31% / EOD 26% / TARGET only 6%
  (rarely reaches the 2R reversion target).
- **Exit fix? NO.** Path-aware MFE +0.79R avg; every fixed TP negative (best
  TP 0.3R → −0.153R). "Least bad" of the cells but still solidly negative.
- **Selection fix? NO.** Full sub-slice (all 4,712): every bucket negative,
  0–1/5 +years (hour −0.21..−0.38; ADX −0.23..−0.30; RSI −0.14..−0.34;
  vol −0.25..−0.37).
- **Conclusion:** REMOVE — same coin-flip pattern, no lever.

## A4 — MEAN_REVERSION × {CHOP, PANIC, RECOVERY}   →   N/A (no trades)

These regimes are near-absent as the daily modal regime in the core-4
reconstruction (`market_brain_history` 2022–2025 modal ∈ {RANGE, TREND_UP,
TREND_DOWN, PANIC}; CHOP/RECOVERY rarely the daily modal). MR materialized only
in RANGE (A3). No cell to audit.

## MOMENTUM × TREND_UP   →   VERDICT: REMOVE (unfixable)

2nd-biggest cell, 14,430 trades. (MOMENTUM is a swing label, not a standard prod
*intraday* emission — edge-discovery scan; audited for completeness.)

- **Gross R −0.38, WR 28%, negative every year** (−0.33 to −0.41). Net −776k
  @₹1L, 94% cost-share. Exit mix SL 46% / TARGET 26% / FLAT 22%.
- **Exit fix? NO** (best TP 0.3R → −0.215R). **Selection fix? NO** (every
  sub-slice −0.35..−0.50, 0/5 +years).
- **Conclusion:** REMOVE — identical coin-flip pattern.

## B/C tier (small cells)

- **OPEN_DRIVE × TREND_UP** — 80 trades, gross R −0.19, WR 32%, negative; net
  −3.2k @₹1L, **150% cost-share** (tiny positions, cost > gross). Too few for a
  robust sub-slice but consistent with the pattern. VERDICT: REMOVE.
- **PHASE1_REVERSAL × RANGE** — n=1 (+0.086 net). Noise; not a cell. N/A.
- **BREAKOUT / PHASE1_MOMENTUM × {TREND_UP,RANGE,...}** — ~0 trades under the
  current config (hard-blocked everywhere except RECOVERY, which is near-absent
  in the reconstruction). Nothing to audit. Confirmed dead-by-config.

## DEAD — MORNING_FADE, VWAP_REVERSAL

Confirmed 0 entries across 2022–2026 (MORNING_FADE: playbook `no_edge_registered`;
VWAP_REVERSAL: `disabled_strategies` kill-switch). They still consume scan cycles
+ watchlist slots in prod (zombie emission) — a cleanup, not an edge, item.

---

## ★ CHANNEL-WIDE VERDICT (cell sweep complete)

**Every reachable intraday cell has negative gross edge, every year, and is
unfixable by tuning.** Summary of the fixability gate across all real cells:

| Cell | n | gross R | best fixed-TP exp | positive sub-slice? | verdict |
|---|---|---|---|---|---|
| PULLBACK × TREND_UP | 16,594 | −0.39 | −0.217R | none | REMOVE |
| MOMENTUM × TREND_UP | 14,430 | −0.38 | −0.215R | none | REMOVE |
| MEAN_REVERSION × RANGE | 4,712 | −0.28 | −0.153R | none | REMOVE |
| VWAP_TREND × TREND_UP | 1,687 | −0.38 | −0.195R | none | REMOVE |
| OPEN_DRIVE × TREND_UP | 80 | −0.19 | (n too small) | — | REMOVE |

- **No exit/stop/trail/partial/target lever** turns any cell positive (every
  fixed take-profit is −EV; favorable excursion ≈ adverse → noise, not edge).
- **No selection lever** (time-of-day / ADX / RSI / volume) finds a robustly
  positive sub-condition in any cell.
- **Costs make it worse but are not the cause** — every cell is negative GROSS.
- Net at ₹1L across the channel ≈ **−₹1.97M** over 2022–2026 (the deployable
  loss is ~2× at ₹2L+, where positions stop being cap-limited).

**The intraday channel has no demonstrable alpha in its current form.** This is
not a parameter/cost problem — the entry signals carry no directional
information. The 2026-05 PROJECT_KNOWLEDGE suspicion ("intraday broken at the
strategy level, not just costs") is now proven cell-by-cell, 2022–2026, on
prod-faithful clean-room data.

### Three independent confirmations the verdict is real (not a backtest artifact)

1. **My clean-room backtest** — every cell negative gross, every year (above).
2. **Prod's REAL live-paper trades** (BQ `trades`, 88 intraday trades 2026-04-16→
   05-12): **9% win-rate, gross −₹1,278, net −₹573.** VWAP_TREND 5/58 wins,
   VWAP_REVERSAL 0/13, BREAKOUT 0/9. Prod's actual fills lost — independent of
   the harness. (Prod's 9% WR < backtest's 26% — the harness is, if anything,
   *generous*.)
3. **Stop/exit/cost-independent forward return** (32,711 long trend-cell
   entries): avg price move after entry +10min +0.003%, +30min +0.002%, +60min
   +0.002%, EOD −0.041%; %positive 42–45% (< coin-flip 50%). The price is a
   ~martingale after these entries — there is no drift for any stop/exit to
   capture, and it's ~0.27% short of just covering cost.

→ The strategies are implemented correctly (they ARE prod's code, verified
<1% vs `scan_decisions`); the backtest is correct (corroborated by prod's real
losses AND the assumption-free forward-return test). The entries simply have no
predictive edge on liquid Indian equities at 5m/15m — they are textbook retail
technical setups, heavily arbitraged, ~50/50 on forward direction.

---

## ★★ PHASE 1 EDGE SEARCH — COMPLETE (cross-sectional, deep re-audit)

After the single-name verdict, we built a cross-sectional IC harness
(`backtest_v2/intraday_xsec.py`) and ran an exhaustive search, 2022–2026,
market-neutral, lag-validated, net of cost. Findings:

| Signal | IC t-stat | lag-robust? | gross | net @retail cost | verdict |
|---|---|---|---|---|---|
| short-horizon reversal (5-30m) | −21 | **NO — collapses on 1-bar lag** | — | — | bid-ask BOUNCE (untradeable) |
| intraday momentum (open→11:00) | +3.2 | yes | +0.06%/day | −0.14% | real, sub-cost |
| range-position | +7.7 | yes | +0.10%/day | −0.10% | real, sub-cost |
| overnight-gap reversal | −5.0 | yes | +0.12%/day | ~0.00% | real, ~breakeven |
| **COMBINED multi-factor alpha** | **+7.6** | yes | **+0.11–0.13%/day (~+28–33%/yr)** | **−0.07 to −0.09%/day (~−22%/yr)** | **REAL EDGE, sub-cost** |

Tail concentration (2.5/5/10%) does NOT close the gap (best 5% tail = +33%/yr
gross, still −22%/yr net). Independent corroboration: prod's REAL paper trades
lost (9% WR, net −₹573).

### The definitive conclusion
**A real cross-sectional intraday edge EXISTS — ~+28–33%/yr GROSS, t=7.6, robust
across years and entry lags.** The market is not perfectly efficient. BUT the
edge (~11–13 bps/day) is **structurally below the retail cost floor** (~20–24 bps
round-trip/day; statutory STT 2.5bps + exchange/GST/stamp + brokerage, ×2 legs,
×full daily turnover from EOD square-off).

- **Break-even cost ≈ 13 bps/day round-trip.** Retail floor ≈ 15–22 bps → loses.
  Institutional/prop/exchange-member ≈ 5–10 bps → the SAME signal nets
  **~+15–25%/yr**.
- It is **neither the backtest nor the algo** that fails: the backtest *found*
  the edge; the algo (cross-sectional multi-factor) is sound. **The wall is the
  retail cost structure** — quantified.

### What makes intraday profitable (precise, not vague)
1. **Lower cost structure** (the real answer): prop desk / exchange membership /
   sub-₹10 brokerage → cost < 13 bps → the +28%/yr-gross edge turns net-positive.
2. **Lower turnover**: hold overnight (net day-to-day) instead of EOD square-off
   → far less cost, but it stops being intraday and converges toward swing +
   overnight risk.
3. NOT achievable by signal/exit/sizing tuning at retail cost — proven.

### Decision options (for the user):
0. **(edge-search outcome)** Pursue a lower-cost structure (prop/member rates) to
   harvest the real +28%/yr-gross edge; OR run it overnight-netted (≈ swing).
   Both are real-world/structure changes, not code tweaks.
1. **Stop / shrink the ₹1L intraday channel** — it cannot be tuned into profit at retail cost;
   redeploy that capital (e.g. to swing, which has a thin but positive validated
   edge). Lowest-risk given the evidence.
2. **New-alpha research track** — a genuinely new intraday entry signal with
   positive *path-aware* expectancy net of cost (order-flow, options positioning,
   index/sector lead-lag, ML on the 5m tape). Research, not a tweak; honest prior
   that it's hard, but it's the only route to a profitable intraday channel.
3. **Cleanup regardless** — kill the VWAP_REVERSAL zombie emission + MORNING_FADE
   overlay (wasted scan/watchlist cycles), independent of the capital decision.


## DEAD — MORNING_FADE, VWAP_REVERSAL   →   (confirm 0 trades + quantify waste)

---

## Emerging channel-wide read (preliminary, pending remaining cells)

Every cell is negative gross; A1 (cleanest, prod-primary) is *provably* edgeless
(no exit/selection lever). The intraday channel's problem is **missing alpha,
not parameters** — confirmed cell-by-cell. The 2026-05 PROJECT_KNOWLEDGE note
("intraday is broken at the strategy level, not just costs") is now quantified.
Capital decision (stop/shrink ₹1L intraday vs new-alpha research) deferred until
all A-tier cells confirmed.
