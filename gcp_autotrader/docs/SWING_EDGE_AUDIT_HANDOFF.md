# Swing deep-audit + edge-discovery — session handoff (2026-06-17)

> **Mission this session:** held-out OOS validation of the deployed swing config (2010–2021),
> setup-by-setup review, and a deep edge-audit ("are we missing proven setups?"). **No prod
> changes** — PAPER untouched (`autotrader-00255-gnv` / ws-monitor `00042-wv7`). This doc is
> the TODO handoff for the next coding session.

## What was established

**1. Deep held-out OOS built (2010–2026) — infra reusable.**
- Source: `score_1d_by_instrument` GCS cache (= **prod's real live daily source**; `candles_daily` BQ is backtest-only). Deep pickle: 2,506 symbols × 2010–2026 → `gs://grow-profit-machine-autotrader-data/oos/candles_daily_deep.pkl`.
- Pipeline: `src/autotrader/backtest_v2/oos_cloud.py` (timeline → pool → walk), runs as Cloud Run job `autotrader-oos` (8 vCPU). **A sector-map packaging bug** (silently 5× sparsified the pool) was found + fixed (now fail-closed).
- Regime reconstruction = unchanged prod brain on deep data, **97% faithful** to the certified timeline (VIX=15 stub confirmed harmless: 0/6 regime flips on extreme days).

**2. Honest OOS result (prod-faithful, variant A):** thin, trend-dependent edge.
- ~**+3%/yr at ₹1L, ~+5%/yr at ₹5L** (consistent across 2017–21 OOS and 2022–26 in-sample). The +8.9% headline was inflated (used backtest-RS + non-prod data).
- Trend-harvester: wins 2014/2020/2023, bleeds bears/crashes (2011 −31k, 2018 −13k).
- **Two inherent caveats** (not fixable with our data): **survivorship** (mild on the *traded* liquidity-filtered universe, ~5–15%, mixed-sign; daily archives since 2026-02-25 make *future* backtests survivorship-free) and **91% regime vintage**.

**3. Setup-by-setup — all 3 cells are KEEPS.**
- **MOMENTUM×TREND_UP** (+54,753@₹1L): the engine, clean keep; trend-dependent (2015–17 = −22.5k drawdown).
- **MEAN_REVERSION×RANGE** (−4,485@₹1L → +104k@₹5L): gross +57k is real; **cost-crippled at ₹1L only** (cost 108% of gross), economic ≥₹3L. Carries the bear-tail risk.
- **PULLBACK×TREND_UP** (−18,352@₹1L): ugly cell P&L but **+12.7k MARGINAL** — it blocks worse marginal trades. Keep (counterintuitive).

**4. Edge-audit — tested hard, one refinement found.**
- ✅ **#7 52-wk-high gate on momentum** (`hi52 ≥ 0.85`) — QUALIFIED PASS. Removes counter-trend-bounce duds; OOS-robust across thresholds (+₹39k OOS@₹5L), but in-sample noisy → conservative threshold + fresh-split needed. Projected next config: ~+4.8 → +5.4%/yr OOS@₹5L (modest).
- ❌ #6 12-1 momentum — rejected (our picks already strong; more = over-extension, hurts).
- ❌ #8 short-term reversal — uneconomic (0.17%/10d < cost).
- ⏸️ PEAD — proxy alpha weak + wrong horizon (20–40d) + needs EPS data.
- Broad 9-family data scan (vol-contraction, low-vol, trend-quality, volume, accel, gaps…) → **no genuinely-new incremental edge**; "robust" hits all overlap momentum or are artifacts.
- **Verdict: system is mature; price-data edge hunt has hit diminishing returns.**

## TODOs — next coding session (prioritized)

**P0 — ship the one validated win (test → fidelity-replay → PAPER):**
1. **#7 52-wk-high gate** (`hi52 ≥ 0.85`) on MOMENTUM selection in `universe_service`. Gate: sub-period hold-out (tune 2010–17 / confirm 2018–26) + fidelity-replay (prod ≡ backtest) + PAPER. *Modest; don't oversell.*

**P1 — highest-value UNTESTED lever:**
2. **Entry-quality floor** — the PULLBACK marginal analysis showed the system books net-losing trades in its lowest-`wl_score` slots. Test raising the marginal-slot bar (account-walk test); may help system-wide + supersede PULLBACK's slot-blocking.

**P1 — decide (test/config):**
3. **MR `>200-SMA` gate** — in-sample-profitable (+18k) but OOS slightly worse; test incremental + fresh-split, then keep/skip.
4. **Capital → ₹3–5L** — MR + system cost-economic there; fills clean to ~₹5L (ADV analysis). Config (`CAPITAL_SWING` + risk scaling) + allocation decision.
5. **Per-name ADV cap (~2–3%)** — only if scaling >₹10L (fills degrade past there). Defer until #4.

**P2 — system-level / bigger fish:**
6. **Bear-tail / PANIC-detection** — 2011/2018 MR losses come from the brain calling declining markets RANGE → MR catches knives. Improve PANIC detection (brain/regime work; biggest tail-risk reducer).
7. **INTRADAY AUDIT (#23–25)** — the *other* ₹1L, **never audited.** Highest-value unaudited exposure. See `INTRADAY_AUDIT_HANDOFF.md`.

**P2 — cross-thread (now unblocked):**
8. **Low-vol market-neutral SWING (Open Items §7-H)** — the parallel intraday thread found cross-sectional low-vol is real (gross +32%/yr) but cost-walled as intraday → belongs at swing horizon. **It was waiting on the 2010–26 history, which is now BUILT** (`oos/candles_daily_deep.pkl`). Phase 0 can proceed (validate the *executable* form: long-only min-vol or long-basket + short-Nifty-hedge — retail can't short equity overnight). NB: this session's long-only swing scan showed `realizedVol` ≈ no edge at 10d-forward — that does *not* contradict the market-neutral/regime-gated finding (different measurement); Phase 0 must test the market-neutral, regime-gated, executable form.

**P3 — parked (low-EV/blocked):** PEAD/SUE (needs EPS data + longer-hold cell), sector momentum (overlaps), bhavcopy survivorship-free history (low-value for liquidity-filtered strategy).

## Reusable infra (this session)
- `oos_cloud.py` — deep OOS pipeline (Cloud Run job `autotrader-oos`).
- `gs://…/oos/` — deep pickle, regime timeline, candidate pool, sector map.
- Scratch analysis scripts in `~/.autotrader_backtest_cache/`: `oos_setup_breakdown.py`, `oos_mr_improve.py`, `oos_hi52.py`, `oos_with7.py`, `oos_factor_scan.py`/`_scan2.py`, `oos_pead_proxy.py`, `oos_data_audit.py`.

## Hard rules (don't skip)
Nothing ships without: economic rationale + OOS-robust (both periods) + plateau (not peak) + **fidelity-replay** (prod code ≡ backtest) + PAPER. This session re-confirmed the discipline kills most "improvements" — only #7 survived.
