# EVENT channel — Corporate-Action (bonus/split) sub-strategy — LOCKED build spec

> **Status: LOCKED spec, validated, build pending.** Decided 2026-06-20. No prod code/deploy yet.
> All evidence is backtest (2010–2026, deep daily pkl); local scratch in `~/.autotrader_backtest_cache/`.
> Edge is a *second sub-strategy* inside the existing EVENT/PEAD channel — **zero change to PEAD's own
> selection or exit logic.** Stays PAPER. Build sequenced after PEAD's first live trades (or fully isolated).

---

## 1. The edge (what & why)

NSE-listed companies must intimate a board meeting ≥2 working days ahead (SEBI LODR Reg.29);
the intimation carries a timestamp and the meeting date (feed: `/api/corporate-board-meetings`,
fields `bm_timestamp` + `bm_date`, available historically to 2010). For **bonus / split** events
the stock drifts **up into the meeting** (informed/speculative pre-positioning) then sells the news.
We capture only the **pre-meeting run-up** (long), the tradeable, no-look-ahead slice.

- **Event-specific, not momentum** (placebo control: same names in random 2-day windows = +0.20%; into the meeting = +1.30%).
- **OOS-robust** and improvable by selection (crude pooled +0.73% → selected **+2.48%** net, liquid).
- **Buyback REJECTED** (negative market-adjusted). **Merger/delist/fundraise/dividend REJECTED on the long side** (duds; "selected" winners were n=3–9 overfit noise). Bonus/split is the *only* tradeable long corporate-action edge. (Fundraise has a real *dilution-fade* short, but it needs F&O and is thin — out of scope here.)

## 2. LOCKED config (live-computable thresholds)

| Knob | Locked value | Notes |
|---|---|---|
| Event types | **bonus, split** | from board-meeting intimation `bm_purpose`/`bm_desc` |
| Universe | **liquid**: 20-day avg turnover ≥ **₹10 cr**, price ≥ **₹30** | conservative; 50bps cost is realistic here |
| First-time | symbol's **first** bonus/split in rolling history | serial repeats are noise (+1.78% vs +0.42%) |
| Uptrend | close ≥ **1.40 × 252-day low** (≥40% above 52w low) | calibrated from ~47% in-sample median; **fidelity-replay must confirm** the fixed threshold reproduces the median-based backtest |
| Anti-pump | 20-day **market-adjusted** run-up < **+6%** | drop already-pumped names (tertile cutoff was ~+5.7%) |
| Lead | intimation→meeting ≥ **4 trading days** | enough warning to enter |
| Entry | **smart**: intimation-day **close** if `bm_timestamp` hour < 14:00 IST, else **next open** | captures ~½ day more; no look-ahead (news already public) |
| Exit | **hard exit at meeting-day close** (~3-day hold) | **NOT** PEAD's ATR-trail — the run-up peaks at the meeting, reverses after |
| Sizing | modest notional; **cap corp at 2 of the EVENT channel's 5 slots** | manages the gross-deployment DD increase (see §4) |
| Capital | **shares the ₹2L EVENT/PEAD pool** + the daily −3%/+6% breaker | no new capital |

## 3. Validated economics (backtest)

- **Per-event: +2.48% net** market-adjusted (liquid, 50bps), **IS +2.65 / OOS +2.43, robust**; ~5/yr (broad ≥2cr: +3.0%, ~8/yr).
- **Combined EVENT-channel backtest** (PEAD grind-v2 + this, shared ₹2L/5-slot pool, 2010–26):
  - Adds **~+2.4–3.4%/yr** to the channel, **monthly correlation ≈ 0** with PEAD, only **15 slot collisions in 16y** (no cannibalization).
  - **Smooths weak PEAD years** (clean per-year): 2024 PEAD +₹1.1k → combined +₹29.4k; 2016 −₹19.2k → −₹6.5k.
  - **Does NOT reduce drawdown on its own** — it deploys more idle capital, so clean (breaker-off) DD rises 21%→27%; the deployed daily breaker tames it. **Therefore size modestly** (the 2-slot cap).

## 4. Honest caveats (hold these)

- Backtest, not live. Small/mid-cap **slippage on 3-day holds** is the key unproven risk → PAPER must measure it.
- **Lumpy** (~5/yr) and **correction-sensitive** (negative through 2022–23 mid-cap correction, like PEAD).
- "First-time" has pre-2010 left-censoring; OOS (2018+) robustness shows it's not a censoring artifact.
- It's a **return-add that increases gross deployment**, not a free DD reducer.

## 5. Build plan (when greenlit)

1. `domain/corp_action_signals.py` — **pure**, fidelity-tested (mirror of `pead_signals.py`): the §2 filters + smart-entry timing.
2. `services/corp_action_signal_service.py` — fetch board-meeting intimations, maintain rolling first-time history, compute features, emit candidates.
3. **Exit** = own hard meeting-date close. ⚠️ **Rule 8 risk**: wire as an event-type branch in the EVENT exit path; prove PEAD's exit is byte-identical.
4. Share the 5-slot book + daily breaker (score-ranked); enforce the **2-slot corp cap**.
5. Cloud Scheduler daily intimation scan; dashboard EVENT panel extension (ties to #56).
6. **Gates before any deploy:** fidelity-replay (live selection == this backtest's trades, exactly) · unit tests per filter + meeting-exit + slot-cap · full blast-radius suite green (PEAD/swing/intraday untouched).

## 6. Sequencing

1. **Validate PEAD live first** (deployed but dormant on the NIFTY −8% gate; no real trade yet).
2. Then build this: pure module + tests/fidelity **first** → entry → meeting-exit (most careful) → PAPER deploy → monitor live slippage vs the +2.48% backtest.
Alternative if not waiting on PEAD: build **fully isolated** (own tiny paper channel) so it cannot affect PEAD/swing.

## 7. Reproduce the numbers

Scratch (all single-process, no prod): `corp_bm_build.py` (archive w/ intimation ts) · `intimation_backtest.py` (exact no-look-ahead) · `composite_v2.py` / `subtype_vol.py` (selection) · `combined_event_channel.py` (PEAD+corp shared pool, breaker on/off). Memory: `project_corp_action_edge.md`.
