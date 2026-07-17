# INTRADAY SETUP / STRATEGY INVENTORY — cell-by-cell audit roster

> The complete list of intraday setups, the regime cells each can reach, the
> direction + entry logic, and the order we'll audit them ONE BY ONE (swing
> playbook). Grounded in code at `main` HEAD; cross-ref `INTRADAY_AUDIT_INVENTORY.md`
> (full gate funnel) and `INTRADAY_AUDIT_FIDELITY.md` (harness fidelity).
>
> Per cell, per setup, we deliver: capital-wise (₹1L/2L/3L/5L) × year-wise
> (2022–2026) × {GROSS (no cost), NET (Upstox cost)}, plus WR, R-multiple,
> cost-share, exit-reason mix, max-DD — no truncated reports — then a verdict
> (keep / fix / remove) with OOS walk-forward before believing anything.

## The 10 intraday setups (everything the scanner can emit/evaluate)

| # | Setup | Emitted by | Side | Direction logic | Entry gate (`check_strategy_entry`) | Reachable regime cells (after hard-block + playbook + kill-switch) |
|---|---|---|---|---|---|---|
| 1 | **VWAP_TREND** | Phase 2 (above-VWAP catch-all) | long+short | democratic vote (margin>2) | 10:15+, last 3 closes same side VWAP, ADX≥22, vol≥1.3, RSI 50-70 BUY / 30-50 SELL | TREND_UP, RECOVERY (playbook MOMENTUM-alias) |
| 2 | **PULLBACK** | Phase 2 (above-VWAP, rising slope) | long-only intraday | long-only veto | EMA stack, RSI 35-70, price ±5% of EMA9 | TREND_UP, RECOVERY |
| 3 | **MEAN_REVERSION** | Phase 2 (CHOPPY + below-VWAP) | long+short | RSI rule (≤45/40 BUY, ≥58/60 SELL) | below VWAP + RSI oversold (BUY) / above + overbought (SELL), VWAP dev ≥0.6% | RANGE, CHOP, PANIC, RECOVERY (+ SELL in RANGE/CHOP) |
| 4 | **OPEN_DRIVE** | Phase 2 (early ORB break) | long+short | democratic vote | bar 09:15-09:45, ADX≥18, vol≥1.2, correct side VWAP | TREND_UP, RECOVERY, EARLY_TU — **sliver** (gate window 09:15-09:45 vs entry-open 09:45) |
| 5 | **BREAKOUT** | Phase 2 (ORB break + vol) | long+short | democratic vote | ADX≥20, ≤5% from 52w(≈10d on 15m) high, vol≥1.2, green entry bar, close>prior-12-bar high, vol accel | **RECOVERY only** (hard-blocked all other regimes) |
| 6 | **PHASE1_MOMENTUM** | Phase 1 (premarket, bullish regime) | long-only | long-only | vol≥0.8 | **RECOVERY only** (hard-block + bearish-prune elsewhere) |
| 7 | **PHASE1_REVERSAL** | Phase 1 (premarket, bearish regime) | long-only | long-only | RSI≤55, vol≥0.8 | PANIC (emission), RANGE/CHOP/RECOVERY (playbook) — **not TREND_DOWN** (playbook MR-alias excludes it) |
| 8 | **MORNING_FADE** | scanner overlay 09:15-10:15 | short-only | forced SELL | bar 09:45-10:15, pop ≥+1.5% from open, vol≥1.0 | **NONE — playbook `no_edge_registered` everywhere** (dead despite Batch-H re-enable) |
| 9 | **VWAP_REVERSAL** | Phase 2 (below-VWAP catch-all) | long+short | RSI rule | below VWAP + RSI rule + VWAP dev ≥0.6% | **NONE — `disabled_strategies` kill-switch** (zombie rows still emitted) |
| 10 | **MOMENTUM** | (swing label; reachable if a row carries it) | long-only intraday | long-only | ADX≥20, vol≥1.3, above VWAP, RSI 55-75, EMA9>EMA21 | TREND_UP, RECOVERY (rarely emitted intraday) |

Short setups **SHORT_BREAKDOWN / SHORT_PULLBACK are never emitted intraday**
(swing-only labels, now disabled). Intraday SELL exposure comes only from
MEAN_REVERSION-SELL, VWAP_TREND-SELL, MORNING_FADE (dead), OPEN_DRIVE/BREAKOUT-SELL.

## Live-reachable cell roster (what actually has trades to audit)

After hard-blocks + playbook (live `USE_PLAYBOOK_V1=true`) + kill-switch, the
cells that can produce trades — the audit work-list:

| Priority | Cell (setup × regime) | Why it matters |
|---|---|---|
| **A1** | VWAP_TREND × TREND_UP | highest-volume intraday cell (prod's dominant trade) |
| **A2** | PULLBACK × TREND_UP | 2nd-highest volume; May'26 probe showed −0.20R gross |
| **A3** | MEAN_REVERSION × RANGE | the range-regime workhorse (+ MR×CHOP, MR×PANIC) |
| **A4** | MEAN_REVERSION × CHOP / PANIC / RECOVERY | counter-trend bounce cells |
| **B1** | VWAP_TREND / PULLBACK × RECOVERY | low-frequency trend cells |
| **B2** | PHASE1_REVERSAL × PANIC | capitulation-bounce cell |
| **B3** | OPEN_DRIVE × TREND_UP/RECOVERY | sliver-window cell (quantify if it ever fires) |
| **C1** | BREAKOUT × RECOVERY | the only BREAKOUT cell |
| **C2** | PHASE1_MOMENTUM × RECOVERY | the only PHASE1_MOMENTUM cell |
| **DEAD** | MORNING_FADE (all), VWAP_REVERSAL (all) | confirm 0 trades; quantify wasted scan/watchlist slots |

## Audit order (one cell at a time, swing-style)

1. **Fidelity milestone first** — run the prod scan code path on prod's own
   recent 15m cache vs `scan_decisions` across many scans; report match %
   (target ~swing's 200/200). Certifies the entry code path before any P&L.
2. **A-tier cells** (highest volume → most reliable edge signal): A1 → A2 → A3
   → A4. Per cell: GROSS/NET × year × capital, exit-reason mix, cost-share,
   then verdict + OOS walk-forward.
3. **B-tier**, then **C-tier** (low-frequency; flag if sample too thin to judge).
4. **DEAD** cells: confirm zero trades and measure the waste (scan cycles +
   watchlist slots consumed by VWAP_REVERSAL zombie rows + MORNING_FADE overlay).
5. Synthesize: which cells carry positive NET edge at which capital; propose
   the fixed config; re-validate OOS; fidelity-replay gate; PAPER deploy.

## Per-cell report template (no truncation)

```
CELL: <setup> × <regime>
  trades: <n>   (per year: 2022 .. 2026)
  WR%: <overall>   avg R (gross): <r>
  GROSS:  ₹1L <..>  ₹2L <..>  ₹3L <..>  ₹5L <..>   (per year breakdown)
  NET:    ₹1L <..>  ₹2L <..>  ₹3L <..>  ₹5L <..>   (per year breakdown)
  cost-share (cost / |gross|): <..>%
  exit mix: SL_HIT/TARGET_HIT/FLAT_TIMEOUT/EOD_CLOSE %
  max-DD: <..>   median hold: <..> min
  VERDICT: keep / fix(<what>) / remove   + OOS walk-forward result
```
