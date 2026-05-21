# Backtest Plan v2 — Production Replica with Validation Gates

**Supersedes** `BACKTEST_PLAN.md` (which was written before key discoveries about missing production gates and unavailable historical data).

## Goal (One Sentence)

Build a backtest that runs EXACT production scoring + gating logic on historical
data, validated by equivalence test against BigQuery `scan_decisions`, with HARD STOP
gates at each phase.

---

## Definition of Success

The replica is "production-equivalent" ONLY when:

1. **Equivalence test passes** for a representative historical day:
   - Qualified count within ±10% of BQ
   - Each `blocked_reason` count within ±20% of BQ (or accounted for as gate-order)
   - No `blocked_reason` appearing in BQ but missing from replay
2. **Trade outcome simulation** matches BQ `trades` table within ±30% for overlap window
3. **Brain regime** matches BQ `market_brain_history` on ≥90% of days

Anything less is "approximate" and must be labeled as such in reports.

---

## Phase A — 10-Week TRUE Replica (Week 1, ~5 days)

**Period covered:** March 7 – May 21, 2026 (where stored brain snapshots exist)

**Why this first:** We can do this with data we already have. No external sourcing needed.

### Deliverables
- `backtest_v2/brain_loader.py` — reads stored brain snapshots from `state/market_brain/history/{date}/{time}.json`
- `backtest_v2/prod_replica_v2.py` — runs full production scoring pipeline (all 4 gates: hard_block + affinity multiplier + adjust_signal haircut + score floor + policy_block + playbook checks)
- `backtest_v2/equivalence_v2.py` — comprehensive test that compares ALL outcome fields (qualified, raw_score, adjusted_score, blocked_reason) to BQ row-by-row

### Hard Gate A — must pass before Phase B
- Equivalence test on **1 historical day** (pick a swing-active day):
  - Qualified count within ±10%
  - Each blocked_reason within ±20%
  - No reason in BQ missing from replay
- **If fail:** stop, diagnose the gap, fix, retry. Do not proceed.

### Estimated effort: 3-5 days

---

## Phase B — External Data Sourcing (Week 2, ~3-4 days)

**Why:** Multi-year replica requires VIX/PCR/FII/DII history that production never archived.

### Deliverables
- `data_sourcing/nse_vix.py` — scrapes India VIX historical from NSE archives (2009→today)
- `data_sourcing/nse_bhavcopy.py` — downloads F&O bhavcopy, computes daily PCR for NIFTY/BANKNIFTY (2010→today)
- `data_sourcing/nse_fii_dii.py` — scrapes daily FII/DII reports (going back years)
- All output stored in GCS at `cache/historical/{vix,pcr,fii_dii}/{date}.json` (consistent with existing cache layout)

### Hard Gate B — must pass before Phase C
For each sourced data source, must validate against existing GCS data:
- Today's VIX value matches Upstox live value
- Today's PCR (computed from bhavcopy) matches `market_brain_history` PCR column for last 10 days within ±5%
- FII/DII flows match BQ `market_brain_history` flow scores for last 10 days

If validation fails on any source: **stop**. Either fix scraper or find different source.

### Estimated effort: 1-2 days

---

## Phase C — Brain Reconstruction (Week 2-3, ~3-4 days)

**Why:** With historical inputs sourced, we can compute brain state for any historical date.

### Deliverables
- `backtest_v2/brain_reconstruction.py` — runs `MarketBrainService` logic on historical inputs (instead of live calls), producing brain state per date
- Reuses existing production `MarketBrainService` code — just swaps live data calls for historical lookups

### Hard Gate C — must pass before Phase D
- Reconstructed brain for the **10-week stored window (March 7 – May 21, 2026)** must match stored snapshots:
  - Regime class match ≥90% of days
  - Score components (trend_score, breadth_score, etc.) within ±5
- **If fail:** the reconstruction logic has a bug; stop and find it.

### Estimated effort: 2-3 days

---

## Phase D — Multi-Year Replica + Validation (Week 3-4, ~3-4 days)

**Why:** Extend replica back to wherever data permits (2010+ for bhavcopy, 2009+ for VIX).

### Deliverables
- `backtest_v2/multiyear_replica.py` — full multi-year backtest using:
  - Reconstructed brain (from Phase C)
  - Recomputed watchlist (from Phase 7 logic, already exists)
  - Production scoring code (imports as-is)
  - All 4 gates (hard_block, affinity, adjust_signal haircut, score floor, policy, playbook)
  - Trade simulation (from Phase 5 logic, already exists)

### Hard Gate D — final validation
- For the period that overlaps with live BQ data (Apr 16 – May 12, 2026, 95 trades):
  - Replica predicts ±30% trade count
  - Replica predicts ±50% P&L
- **If fail:** identify why predictions diverge from reality. Document limitations.

### Estimated effort: 2-3 days

---

## Validation Strategy (Cross-Phase)

### The equivalence test (mandatory pre-flight for any backtest run)
Already built: `backtest_v2/equivalence_test.py`. Will be enhanced in Phase A.

Validates that for any historical day from BQ `scan_decisions`:
- Replay produces same `qualified` decision per (symbol, setup) pair
- Replay produces same `raw_score` and `adjusted_score`
- Replay produces same `blocked_reason` OR a documented gate-order alternative

### Live BQ trades comparison (final validation)
For the period when live paper trading ran (Apr 16 – May 12, 2026, 95 trades):
- Replica predictions must approximate live trades
- Documented variance must be within stated tolerance
- If gap >50% → document as "approximate, not equivalent"

---

## Realistic Total Effort

| Phase | Best Case | Realistic | Risk |
|---|---|---|---|
| A: 10-week replica | 3 days | 5 days | Discovery of more gates |
| B: Data sourcing | 1 day | 2 days | NSE format changes |
| C: Brain reconstruction | 2 days | 3 days | Brain has un-audited inputs |
| D: Multi-year + validation | 2 days | 4 days | Live BQ comparison fails |
| **TOTAL** | **8 days** | **14 days** | **3 weeks calendar** |

Realistic: **2-3 weeks of focused work.**

---

## Things That Will NOT Be Solved Even After This Plan

These are honest limitations, documented up front:

1. **Production code has evolved.** Backtesting 2024 with TODAY's code = "what would current production have done" not "what 2024 production actually did." Unfixable.

2. **Intraday tick precision.** Production sees live ticks; backtest sees 5-min OHLC bars. Intra-bar SL/target hits will differ. Bounded approximation.

3. **Symbol survivorship.** Delisted symbols may be missing from current GCS cache. Universe at historical date approximated by "symbols present at that date."

4. **Sector mapping is current-only.** A symbol's sector today may differ from its sector in 2024. We use today's mapping throughout (documented approximation).

5. **Real-time data races.** Production may have queued/delayed data; backtest assumes clean inputs. Edge cases will differ.

6. **MarketPolicy + playbook config from Firestore.** We use current settings throughout (documented). Historical config changes are not captured.

These are accepted approximations. If equivalence test passes despite these, the replica is "production-equivalent within documented limits."

---

## Commitments

1. **No declaring success past a failed gate.** Each phase has a hard gate. I stop if it fails.

2. **No backtest results without equivalence-test receipt.** Every backtest report I produce will reference which equivalence-test run validated it.

3. **No silent approximations.** Any place we use a proxy/approximation will be logged in the report.

4. **Daily check-ins.** Each working day I summarize: where I am, what gate I'm targeting, blockers if any.

5. **Hard stop if track record continues.** If we hit two failed gates in a row that I cannot diagnose, I will recommend stopping the backtest path entirely and switching to live-data-only analysis.

---

## What This Plan Does NOT Do

- It does not promise a backtest that exactly matches production. It promises a backtest that passes a defined equivalence test, with documented limits.
- It does not promise specific P&L predictions. It produces relative comparisons (this rule change adds/subtracts X% P&L vs the validated baseline).
- It does not test the dominant production strategy (VWAP_TREND) in 2024-2025 because intraday 5m only exists from Dec 2025 onwards. VWAP_TREND backtest is limited to ~5 months of data.

---

## First Concrete Action

**Phase A, Day 1, Step 1:** Build `brain_loader.py` that reads stored brain snapshots
from `state/market_brain/history/{date}/{time}.json` and produces a regime + risk_mode + brain inputs object that matches `RegimeSnapshot`/`MarketBrainState` interface.

**Validation:** load 5 random brain snapshots, verify they parse correctly and match BQ `market_brain_history` for the same timestamp.

Estimated effort: 2-3 hours.
