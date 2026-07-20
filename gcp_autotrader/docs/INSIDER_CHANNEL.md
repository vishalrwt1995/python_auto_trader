# Insider Cluster-Buy Channel — proposal, build, and deploy runbook

> Status: **CODE COMPLETE + TESTED on branch `feat/insider-cluster-buy-channel`. NOT deployed.**
> PAPER-only. Deploy steps (§5) are GATED on explicit user cost/deploy confirmation.
> Validated 2026-07-20 (god-mode grind). Author: Claude Opus 4.8.

## 1. The edge

NSE SEBI-PIT disclosures of **informed open-market BUYS** (promoter / promoter group / director /
KMP / immediate relative), each leg ≥ ₹5 lakh, aggregated per (symbol, dissemination-day). A
**cluster** = ≥2 such qualifying legs the same day for the same symbol (genuine conviction; a
single leg is noise). Entry the **next session's open after the public disclosure** (never the
private transaction date — no look-ahead).

Informed buying crushes an all-liquid baseline in **both** IS (≤2020) and OOS (≥2021): fwd20
+1.71%/+2.98% (base −0.10/+0.56), fwd60 +6.18%/+7.46% (base +1.46/+3.30); directors & KMP/relatives
strongest (dose-response). ~1,500 raw signals/yr → real capacity. Genuinely distinct: **3.2%
overlap** with the momentum channel's top-20.

## 2. God-mode validation (locked config)

The un-engineered edge was high-return/high-DD (Calmar 0.51, −44% DD). The DD-killer was a
**double macro gate** + the **cluster** signal:

| | CAGR | maxDD | Calmar | IS Cal / OOS Cal |
|---|---|---|---|---|
| un-engineered | +21.2% | −37.6% | 0.56 | 0.81 / 0.61 |
| **LOCKED (god-mode)** | **+23.0%** | **−12.5%** | **1.84** | **2.85 / 1.75** |

Robust **plateau** (every hold×slots neighbor Calmar 1.0–2.07); cross-checked with a
tactical-score 2nd gate (still Calmar ~1.2 — it's the *principle* of two independent macro
confirmations, not a lucky gate). Per-year: the old −154k (2018) / −129k (2022) disasters become
small **positives**; worst year −29k. Survivorship-safe (incl. delisted), full Upstox costs +
slippage, sizing-robust (1–2% risk all Calmar 1.6+). Liquidity: return concentrates in 10–25cr
mid-caps (>25cr = +13.6%/Cal 0.73 — still clears the bar; fills realistic, ₹20–40k in ₹10cr/day
≈ 0.03% of volume).

**Locked parameters** (env-overridable, defaults in `settings.py`):
- signal: informed open-market buy, ≥₹5L/leg, **cluster ≥2 buyers/symbol/day**
- **double macro gate: breadth `b200 > 50` AND Nifty `> 100DMA`** (channel-level, per scan)
- universe: 20d-mean turnover **≥ ₹10cr** (no upper cap), price ≥ ₹30, ETFs excluded
- **fixed 90-session hold** + 2.5×ATR protective stop (NO trail — a trail whipsaws this drift)
- **10 slots**, 1.5% equity risk, notional cap = capital/10, 2% participation cap
- ranking (slot priority): more buyers, then larger total value
- capital: `CAPITAL_INSIDER` (proposed ₹2L to start, PAPER)

## 3. Build manifest (all on branch `feat/insider-cluster-buy-channel`)

Mirrors the delivery channel (hybrid: new thin services, reuse order_service/positions/exit via
`channel="insider"`/`wl_type="insider"`, `product="CNC"`). Zero change to swing/intraday/other
channels.

**New files (5):**
- `domain/insider_signals.py` — pure gates, `aggregate_clusters`, `macro_gate_ok`, slot selection
- `services/insider_ingest_service.py` — NSE corporates-pit rolling window → BQ `nse_insider_daily`
- `services/insider_signal_service.py` — cluster candidates (fidelity-shared with backtest)
- `services/insider_trading_service.py` — scan → double-gate → plan → CNC entries
- `services/insider_reconciliation_service.py` — 90d max-hold + SL-breach backstop (no trail)

**Integration edits:**
- `settings.py` — `capital_insider` + 10 `insider_*` fields + `channel_capital()` + env reads
- `order_service.py` + `ws_monitor_service.py` — **both** EOD overnight-exemption lists (verified 2 hits)
- `container.py` — DI: `run_insider_scan` / `run_insider_ingest` / `insider_reconciliation_service`
- `web/api.py` — routes `/jobs/insider-{ingest,reconcile,scan}`
- `web/dashboard_api.py` — `_CHANNELS` + `max_pos`
- dashboard `types.ts` / `constants.ts` / `positions/page.tsx` / `utils.ts` (+ fixed delivery's
  `_KNOWN_CHANNELS`/`inferTradeChannel` attribution gap in passing)

**Macro live reads** (fail-closed): b200 = brain `breadth_ema200_pct` (Firestore `market_brain/latest`);
Nifty>100DMA = **reuses** `momentum_signal_service.fetch_nifty_regime`.

**Tests:** `test_insider_{signals,trading,reconcile,ingest}.py` (51 tests) + sync-guard tests
updated. **Full suite: 1065 passed / 5 skipped, no regressions.**

## 4. Data feed

- **Backfill (one-time):** the cached pull `~/.autotrader_backtest_cache/insider_pit/*.json`
  (341,175 rows, 2015-11 → 2026-05) loads into BQ `nse_insider_daily`.
- **Daily (prod):** `insider-ingest` (evening) refetches a rolling 7-day corporates-pit window
  (absorbs SEBI's ≤2-day disclosure lag + late filings), idempotent upsert.
- **Known caveat:** the corporates-pit API returned 0 rows for the *very recent* Jun–Jul 2026
  window during recon (2025 + through-May-2026 pulled clean). Must be re-checked at deploy so the
  live daily feed is confirmed non-empty before funding.

## 5. DEPLOY PLAN — GATED (needs explicit cost + deploy confirmation, per the GCP-cost rule)

1. **BQ** (cost: tiny — one table, ~50MB load): create `nse_insider_daily` (date, symbol, acq_name,
   person_category, transaction_type, acq_mode, sec_val, bef_pct, after_pct, disseminated_ts);
   backfill from the cached JSONs.
2. **Confirm live feed:** run `insider-ingest` once, verify `MAX(date)` is current + rows > 0.
3. **Deploy `autotrader`** (`gcloud run deploy --source`, Rule 1 sync first) — ships domain,
   services, routes, dashboard_api.
4. **Deploy `autotrader-ws-monitor`** (Rule 8: `cloudbuild.ws.yaml` → deploy image) — ships the
   `_OVERNIGHT_SL_ONLY_WL` insider addition. **Both** services must show new revisions.
5. **Deploy `autotrader-dashboard`** — ships the frontend (separate deploy; frontend edits don't
   ship with autotrader).
6. **Env:** `gcloud run services update autotrader --update-env-vars CAPITAL_INSIDER=200000`
   (PAPER; keep PAPER_TRADE=true).
7. **Schedulers (3, by hand, mirror delivery):**
   - `autotrader-insider-ingest-1930` `30 19 * * 1-5` → `/jobs/insider-ingest`
   - `autotrader-insider-recon-0905` `5 9 * * 1-5` → `/jobs/insider-reconcile`
   - `autotrader-insider-scan-0910` `10 9 * * 1-5` → `/jobs/insider-scan`

## 6. Forward-validation prediction (first live session)

- First scan: macro gate likely **OFF** right now (b200 ≈ 62 > 50 ✓ but Nifty is in a correction /
  RANGE — if Nifty < 100DMA the gate blocks all entries → `macro_gate_ok=false`, 0 orders). That is
  **correct behaviour**, not a bug — the gate is the DD-killer.
- When the gate opens (breadth>50 AND Nifty>100DMA), expect ~20 entries/yr, 90-day CNC holds,
  cluster-ranked, ₹20–40k positions. Watch: `insider_scan_summary` (clusters/candidates/entered),
  positions persist overnight (EOD fix — verify not squared), fills <0.15% slip, overlap <10%.
- Scale ₹2L → ₹5L only after a few positions complete a full 90d cycle with backtest-consistent behaviour.
