# Backtest User Guide — Swing Replica v2

**Status:** validated against production BQ scan_decisions
**Accuracy:** 99.5% qualified-compat, 78% direction match, 28% raw_score exact match
**Use for:** Hypothesis testing of gate/regime changes on swing strategies
**Window:** March 7 – May 21, 2026 (~10 weeks where brain snapshots exist)
**Scope:** SWING wl_type rows only — BREAKOUT, MEAN_REVERSION (PULLBACK + MOMENTUM are intraday-only per production data)

---

## TL;DR — Three commands to run a backtest

```bash
cd "/Users/vishalrawat/Auto Trading Python GCP/.claude/worktrees/zen-feynman-447e04/gcp_autotrader"

# 1. Validate the replica (sanity check — run this once)
PYTHONPATH=src GOOGLE_CLOUD_PROJECT=grow-profit-machine \
    python3 -m autotrader.backtest_v2.validate_symbols

# 2. Run hypothesis tests
PYTHONPATH=src GOOGLE_CLOUD_PROJECT=grow-profit-machine \
    python3 -m autotrader.backtest_v2.hypothesis_tester 2026-04-10 2026-05-21

# 3. Inspect results
cat ~/.autotrader_backtest_cache/hypothesis_results_*.json | python3 -m json.tool | head -100
```

---

## What you get back

### Output 1 — Hypothesis comparison table

```
Hypothesis                                NewQ  Wins     WR    AvgR     NetPnL
baseline_production                      0 newly qualified (matches prod)
H1_unblock_breakout_trend_up                95    37  38.9%  -0.11R ₹    -3077
H2_unblock_breakout_range                    2     0   0.0%  -0.57R ₹     -356
H2b_unblock_breakout_range_strong_only       2     0   0.0%  -0.57R ₹     -356
H3_floor_affinity_range_08                   2     0   0.0%  -0.57R ₹     -356
H4_combined_unblock_brkout_all_trends       95    37  38.9%  -0.11R ₹    -3077
```

Columns:
- **NewQ**: Trades that would have NEWLY qualified under this hypothesis (production blocked, replica unblocked)
- **Wins**: How many made money
- **WR**: Win rate of newly-qualified trades
- **AvgR**: Average R-multiple per trade (positive = profitable, negative = losing)
- **NetPnL**: Net rupee P&L if you had taken these trades

### Output 2 — Per-trade details (JSON)

`~/.autotrader_backtest_cache/hypothesis_results_<dates>.json` contains every newly-qualified trade with:
- Symbol, scan_date, direction
- Raw score, why production blocked it
- Simulated trade outcome (entry, exit, P&L, exit reason)

---

## What questions you can answer reliably

✅ **"Would unblocking BREAKOUT in TREND_UP help?"**
   - hypothesis_tester says yes/no with simulated P&L

✅ **"Which production rules are correctly blocking losing trades?"**
   - If unblocking a rule produces negative P&L, the rule is doing its job

✅ **"Would disabling PULLBACK / changing a setup-regime matrix improve P&L?"**
   - Add the rule change as a HypothesisConfig, re-run

✅ **"Is the system over-restrictive in RANGE / TREND_UP / etc.?"**
   - Test "unblock X in Y regime" and see if newly-fired trades win

## What questions you CANNOT answer reliably yet

⚠️ **"What's the exact 2-year P&L of production?"**
   - Multi-year backtest before March 7, 2026 has no brain snapshots → approximate brain only

⚠️ **"What if score floor was 67 instead of 65?"**
   - Raw score precision is ±5-10 typical, fine-tuning thresholds has uncertainty

⚠️ **"What's the optimal SL / target?"**
   - Score-precision dependent, won't get reliable answer

❌ **"What about intraday setups (VWAP_TREND etc.)?"**
   - Intraday replica only 50-65% match. Don't use for those.

❌ **"What happened in 2024?"**
   - Brain snapshots only go back to March 7, 2026

---

## Adding your own hypothesis

Edit `src/autotrader/backtest_v2/hypothesis_tester.py`:

```python
HYPOTHESES = [
    HypothesisConfig(name="baseline_production"),

    # ADD YOUR HYPOTHESIS HERE
    HypothesisConfig(
        name="H5_my_idea",
        # Choose what to override:
        unblock_pairs={("TREND_UP", "BREAKOUT")},  # remove regime hard-block
        affinity_floor_range=0.8,                   # raise floor in RANGE
        score_floor_override=70,                    # different score gate
        require_strong_stock=True,                  # only strong-trend stocks
    ),
]
```

Then re-run `hypothesis_tester.py`.

---

## Understanding the output

### When NewQ = 0
The hypothesis doesn't change any production decisions. Either:
- The rule you're changing wasn't blocking anything in this window
- Or score gates blocked the trades anyway (hard-block change irrelevant)

### When NewQ is positive but P&L is negative
Production was correctly blocking those trades. Don't deploy the change.

### When NewQ is positive AND P&L is positive
The change adds profit. Worth testing in paper mode for real.

### When you see "newly_blocked"
Trades production took that hypothesis would have rejected. If those trades LOST money in production, blocking them is good.

---

## What's in your toolbox

| File | Purpose |
|---|---|
| `prod_replica_v2.py` | The validated swing scanner replica |
| `brain_loader.py` | Loads production brain snapshots from GCS |
| `data.py` | Reads historical daily + intraday candles |
| `hypothesis_tester.py` | Compares hypotheses to baseline + simulates trades |
| `validate_symbols.py` | Sanity-check that replica matches BQ (run anytime) |
| `phase5_trade_sim.py` | Trade outcome simulator (used by hypothesis_tester) |
| `equivalence_v2.py` | Full-day equivalence test against BQ |
| `backfill_intraday_1m.py` | Pulls 1m NIFTY/VIX from Upstox (already done) |

---

## Known limitations (documented honestly)

1. **MEAN_REVERSION/swing**: Direction match only ~34% — RSI threshold sensitivity. Use BREAKOUT-only hypotheses for highest confidence.

2. **SBICARD MR matches**: 34% direction match. Specific symbols may have edge-case behavior; skip them in hypothesis tests if results look noisy.

3. **VIX_TREND_MAX=18 production override**: We've baked this into prod_replica's default cfg. If production changes this env var, update `prod_replica_v2.py:__init__`.

4. **Brain snapshot lag**: We use the snapshot at-or-before scan_ts. If snapshot is more than 5 min stale, replica may produce different VIX/PCR/FII than production saw.

5. **Portfolio state gates not modeled**: capital_exhausted, swing_max_positions_reached, etc. These affect ~5% of production qualifications.

6. **Score precision ±5-10**: Don't fine-tune thresholds. Use for big rule changes only.

---

## When to come back here

- After running a hypothesis: read the NetPnL column. Decision: deploy or skip.
- After deploying a change to live: compare live results to predicted P&L. Difference > 30% → revisit replica calibration.
- Every month: re-run validate_symbols.py to catch any drift from production code changes.
