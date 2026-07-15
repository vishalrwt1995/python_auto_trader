# Delivery-Accumulation Channel — Integration Proposal (for sign-off)

> **Status:** validated candidate, NOT yet built. Design for approval before any prod change.
> **Isolation:** nothing in prod touched. Evidence from isolated read-only backtests
> (`delivery_alpha_check.py`, `delivery_lock.py`, `delivery_overlap.py`, `delivery_profit.py`).
> **Date:** 2026-07-14 · PAPER (ships PAPER like every channel).

> ### ⚠ Correction history (read this — the config + numbers changed twice)
> 1. **Flat-slippage KILL was too blunt.** The original grind killed delivery on a *flat* 0.25–0.5%/leg
>    slippage. Slippage is size-dependent; a small position in a thin name fills at ~half-spread. The
>    size-aware re-grind revived it.
> 2. **An unverified number reached an earlier draft.** A "₹5L → 10.3%" figure was cited that was never
>    cleanly reproduced (the real hold10+dip config was ~1–4%). Removed. Every number below is from a
>    logged run.
> 3. **The dip filter was wrong.** The alpha/beta isolation test showed `deliv≥75 & ret5≤0` *underperforms*
>    `deliv≥75` alone — the dip requirement threw away good accumulation-on-strength trades. Config
>    corrected to **`deliv≥75`, no dip filter, hold ~20d**.

---

## 1. What it is

A next channel that buys **mid-cap stocks showing genuine accumulation** — days with high delivery-%
(≥75% of traded volume taken to delivery = real buyers, not intraday churn) — and holds ~3-4 weeks for the
forward drift. A different *signal family* (delivery microstructure, not price momentum / earnings /
events) and a different *universe tier* (25–50cr mid-caps).

**Validated economics** (survivorship-safe universe incl. delisted, size-aware fills, IS ≤2022 / OOS ≥2023):

| capital | CAGR | Calmar | maxDD | ₹/yr | IS | OOS | +years |
|---|---|---|---|---|---|---|---|
| ₹2L | 11.9% | 0.75 | −16.0% | ₹32k | +15.8 | +12.9 | **7/7** |
| **₹5L** | **12.8%** | 0.82 | −15.5% | **₹88k** | +16.9 | +14.0 | **7/7** |
| ₹5L (hold18 variant) | 13.3% | **1.33** | **−10.0%** | ₹92k | +15.0 | +16.1 | 7/7 |

**Every year positive 2020–2026**, including 2022 (the bear, +15k). IS≈OOS. Not concentrated.

### The signal is real alpha, not beta (the decisive test)
Identical 5-slot walks, same 25–50cr band, three signals:

| hold | **`deliv≥75`** | `ret5≤0` (dip control) | any-day (pure beta) |
|---|---|---|---|
| 12 | **+11.4%** (IS+17 OOS+10) | −1.2% | −34% |
| 20 | **+13.4%** (IS+16 OOS+15) | +2.3% | −9% (blows up, −80% DD) |

Random mid-cap entry **destroys capital** (this survivorship-safe universe is full of falling knives), and
dip-buying alone loses. Only the delivery-% signal is robustly profitable — beating beta by **10–40 points**
both halves. It's doing genuine selection.

---

## 2. Locked config (data-driven, robust plateau — not a knife-edge)

| param | value | why |
|---|---|---|
| **Signal** | `deliv_pct ≥ 75` (**no dip filter**) | plateau: 70–75 all ~12–13% both halves; dip filter *hurts* |
| **Universe band** | **[25cr, 50cr]** turnover | 25–50 / 25–60 robust; 30–60 fails OOS; 20–50 weaker |
| **Price floor** | ≥ ₹30 | standard |
| **Entry** | next-day **open** | signal EOD day-T → enter T+1 (realistic, matches data) |
| **Exit** | ATR **2.5×** stop, arm **1.75R**, trail **1.0R**, **hold ~20d** | reuse `domain/swing_exit.py` (no new exit code). Plateau 15–22d; **hold15–18 give higher Calmar (1.3–1.45) / lower DD (−10%)** if you prefer risk-adjusted over headline |
| **Slots** | 5 | slots 5–7 both ~12.8%; 3 too few |
| **Capacity guard** | position ≤ **2%** of daily turnover | backtest realized participation ~0.01% → fills realistic |
| **Cost** | full Upstox delivery round-trip + 0.10%/leg paper slip | matches every channel |

**Open decision — capital + risk/trade** (§6).

---

## 3. Diversification (earns its own capital)

Concurrent-holding overlap (same name, same time) on the corrected config:

| vs | name overlap | **concurrent** |
|---|---|---|
| momentum | 31.7% | **7.8%** |
| pead | 29.3% | **1.6%** |
| core | 9.6% | **0.0%** |

Total ~9.4% — still **below momentum's own 0.23 (23%) ship credential**. Turnover tier 33cr vs momentum
99cr / core 360cr. Longer holds nudged momentum overlap up (5.1→7.8%) but it remains a genuine diversifier.
Swing not simulated (heavy engine; different signal, TREND_UP-gated, larger-cap) → confirm concurrency live.

---

## 4. Integration design — additive, zero blast radius

Mirrors the **momentum channel ship (PR #59)**: gated behind its own capital + enable flag, existing
channels byte-untouched.

**a) `settings.py`** — new fields + env (same pattern as `capital_momentum`):
```
capital_delivery: float = 0.0            # CAPITAL_DELIVERY
delivery_enabled: bool  = False          # DELIVERY_ENABLED
delivery_risk_per_trade: float = 0.0     # DELIVERY_RISK_PER_TRADE (1.5% of cap)
delivery_max_positions: int = 5          # DELIVERY_MAX_POSITIONS
delivery_turnover_min_cr: float = 25.0   # DELIVERY_TURNOVER_MIN_CR
delivery_turnover_max_cr: float = 50.0   # DELIVERY_TURNOVER_MAX_CR
delivery_deliv_min: float = 75.0         # DELIVERY_DELIV_MIN
delivery_hold_days: int = 20             # DELIVERY_HOLD_DAYS
delivery_notional_cap_pct: float = 0.20  # per-position notional cap × capital_delivery
```
+ `channel_capital("delivery")` branch.

**b) `domain/portfolio_book.py`** — add `ChannelName.DELIVERY`; budget + 3%/6% breaker reuse existing machinery.

**c) New services** (mirror pead — a daily channel):
- `services/delivery_signal_service.py` — daily: read yesterday's delivery + bars, apply §2 signal, emit
  candidates (rank by delivery-%).
- `services/delivery_trading_service.py` — 5-slot walk, size (risk + slot cap + 2%-participation cap),
  place **CNC** orders (buy-hold), reuse `swing_exit` for exits + daily reconciliation. **No new exit path**
  (Rule 8: no ws-monitor change).

**d) Data ingestion — the one net-new infra piece (task #12):**
`nse_delivery_daily` is a stale historical load (last 2026-06-22), not live-fed. Need a **daily job**:
fetch NSE `sec_bhavdata_full_DDMMYYYY.csv` post-close → parse `DELIV_QTY`/`DELIV_PER` → append to BQ. URL
known (`bhavcopy_scope.py`), BQ write path exists (`bigquery_client.insert_rows_json`). Cloud Scheduler
trigger (~evening IST).

**e) Scan schedule** — one daily premarket scan (~08:30 IST) on the prior session's delivery, enter at open.

**f) Dashboard** — add `delivery` to `CHANNEL_ORDER` + `CHANNEL_META`.

**Blast radius:** gated behind `DELIVERY_ENABLED=false` + `CAPITAL_DELIVERY=0` by default → zero effect on
live channels until explicitly enabled. No existing channel file modified.

---

## 5. Remaining work to ship (build checklist)

1. Daily `sec_bhavdata_full` ingestion job + Cloud Scheduler trigger (main new infra).
2. `delivery_signal_service` + `delivery_trading_service` (mirror pead), settings + portfolio_book wiring.
3. Prod-faithful `delivery_final.py` engine importing the new domain signal (parity check, like
   swing_final/momentum_grind) — backtest can't drift from prod.
4. Unit tests (signal gate, sizing, slot walk, capacity cap) — match the ~960-test bar.
5. Dashboard channel entry.
6. Deploy PAPER (single-service `autotrader` deploy; ws-monitor untouched per Rule 8), verify revision.

---

## 6. Capital allocation — ✅ LOCKED: ₹2L to start

**Decision (2026-07-14): `CAPITAL_DELIVERY=2,00,000`** — parity with how pead/momentum launched.
→ 11.9% CAGR / ₹32k-yr / 7-of-7 years. Roster ~₹13L → ~₹15L.
Risk/trade: **1.5% of capital = `DELIVERY_RISK_PER_TRADE=3,000`**.
**Scale-up path:** bump to ₹5L (→12.8% / ₹88k-yr) after PAPER forward-validation confirms real fills on
25–50cr mid-caps + the edge holds live. Env-only change (`gcloud run services update`), no redeploy.

---

## 7. Forward-validation prediction (first live PAPER sessions)

- Fires on **mid-caps (25–50cr turnover)** with delivery-% ≥75 — watch `scan_decisions` for delivery rows
  in that band, NOT large-caps.
- **~1–1.5 entries/week** (backtest ~67/yr taken at hold20).
- Concurrent overlap with momentum should stay **<10%** — live check on the diversification claim.
- Realized fills **<0.15% slippage** (mid-cap, small size) — if materially worse, re-size down.
- Every calendar year was positive in backtest — a losing *year* live would be the first red flag.

---

## 8. Honest caveats

- **Exact CAGR ±a couple points**: absolute number is sensitive to the turnover/sizing definition
  (`delivery_profit` vs `delivery_alpha_check` disagreed by ~2pts). The *relative* result — beats beta by
  10–40pts, robust both halves, 7/7 years — is rock-solid; treat "~12–13%" as the honest range.
- **Threshold plateau, but real edges beyond it**: deliv 70–75 robust; ≥78 drops off (still positive).
- **Recent years lighter**: 2024 +43k / 2026 (partial) +44k vs 2021/2023 ~+117k — some year-to-year
  variance; monitor for decay.
- **Short history**: delivery data from 2020-07 → ~3.5yr OOS. Solid, not decade-long.
- **Ingestion dependency**: live edge needs the new daily `sec_bhavdata_full` job; a missed fetch = no
  signal that day (fail-closed + log — no silent fallback).
