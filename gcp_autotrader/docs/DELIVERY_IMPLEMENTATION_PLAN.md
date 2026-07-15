# Delivery Channel — Turnkey Implementation Plan (build spec)

> **For the build session.** Validated + finalized; this is the execution guide. Read alongside
> `DELIVERY_CHANNEL_PROPOSAL.md` (the why + the numbers). Config is LOCKED (below). Wiring is faithful to a
> read-only map of the live codebase (2026-07-14).
>
> **Guardrails:** additive-only · PAPER · gated behind `CAPITAL_DELIVERY` (0 ⇒ channel off) · **no
> swing/intraday/core/momentum/pead path modified** · Deploy Rules 1 (sync main), 3 (ADC token), **8
> (ws-monitor is a second deploy — see Step 10)**.

---

## 0. Locked config (final)

`deliv_pct ≥ 75` (no dip filter) · **25–50cr** turnover band · **hold 20d** · **5 slots** · ATR **2.5×** stop ·
arm **1.75R** / trail **1.0R** (reuse `swing_exit`) · next-open entry · 2%-participation cap · **CAPITAL_DELIVERY
= ₹2,00,000** · **DELIVERY_RISK_PER_TRADE = ₹3,000** (1.5%). CNC buy-hold. PAPER.

Validated: 12.8% CAGR @₹5L / 11.9% @₹2L, Calmar 0.82, 7/7 positive years, beats pure-beta control by
10–40pts both halves (real alpha), diversifier (concurrent overlap ≤7.8%).

---

## 1. The ONE genuinely-new piece: daily delivery-% ingestion

**Why it's needed (and why PEAD didn't need it):** PEAD pulls price bars on-demand from Upstox candles.
But **Upstox candles are OHLCV only — they do NOT carry delivery-%.** The delivery signal's *entire* input
(`deliv_pct`) exists only in NSE's `sec_bhavdata_full`. So delivery needs a daily job that PEAD doesn't:

- **New service** `services/delivery_ingest_service.py`: fetch
  `https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_DDMMYYYY.csv` for the latest trading
  day → parse `SYMBOL, SERIES, DELIV_QTY, DELIV_PER, TTL_TRD_QNTY` → keep `SERIES=EQ` → **append to BQ
  `nse_delivery_daily`** (schema already exists: `date, symbol, ttl_trd_qty, deliv_qty, deliv_pct`).
  - **Idempotent**: dedupe by `date` (delete-then-insert the day, or `MERGE`). BQ write path exists:
    `adapters/bigquery_client.py:insert_rows_json`.
  - **Fail-closed**: fetch fail → log + no-op (no delivery signal next day = no trades = safe). No silent
    fallback (memory rule).
  - **NSE fetch caveat**: `nsearchives.nseindia.com` may require a browser-like `User-Agent` + a warm-up
    cookie from `nseindia.com`. `scripts/redesign/bhavcopy_scope.py` already has the URL; the build session
    validates the fetch handshake first.
- **Endpoint** `POST /jobs/delivery-ingest` in `web/api.py` (auth via `_auth(job_trigger_token)`, mirror
  `/jobs/pead-scan` at api.py:1825).
- **Scheduler** `autotrader-delivery-ingest-1900` (~19:00 IST — after NSE publishes EOD). Signal on day-T
  delivery → enter T+1 open, so the scan (Step 6, ~09:10) reads the prior evening's load.

**Acceptance:** run the endpoint manually → `nse_delivery_daily` max(date) advances to the latest session,
row count ≈ EQ-series count (~1,800), `deliv_pct` populated, re-running the same day doesn't duplicate.

---

## 2. Domain signal (new, pure) — `domain/delivery_signals.py`

Pure functions + constants (mirror `domain/pead_signals.py`'s shape so a parity engine can import them):
```python
DELIV_MIN = 75.0                 # deliv_pct floor
TURNOVER_MIN_CR, TURNOVER_MAX_CR = 25.0, 50.0   # 20d-median turnover band
PRICE_MIN = 30.0
MAX_HOLD_DAYS = 20
ATR_SL_MULT, ACTIVATE_R, TRAIL_R = 2.5, 1.75, 1.0

def passes_delivery_gates(deliv_pct, turnover_cr, close) -> bool:
    return (deliv_pct >= DELIV_MIN and TURNOVER_MIN_CR <= turnover_cr < TURNOVER_MAX_CR
            and close >= PRICE_MIN)

def turnover_20d_cr(closes, vols, i) -> float:   # trailing 20d EXCLUDING day i, in cr
    w = [closes[j]*vols[j] for j in range(max(0, i-20), i)]
    return (sum(w)/len(w))/1e7 if w else 0.0
```
**Acceptance:** unit tests for the gate boundaries (74.9 vs 75.0; 24.9 / 50.0 / 50.1 cr; price 29.9).

---

## 3. `settings.py` wiring (exact change points)

- **Dataclass field** after `capital_momentum` (~line 49): `capital_delivery: float = 0.0`
- **7 delivery params** after `pead` block (~line 194):
  ```python
  # ── DELIVERY channel (daily CNC buy-hold, added 2026-07-14) ──
  delivery_risk_per_trade: float = 0.0    # DELIVERY_RISK_PER_TRADE (1.5% of cap)
  delivery_max_positions: int = 5         # DELIVERY_MAX_POSITIONS
  delivery_max_hold_days: int = 20        # DELIVERY_MAX_HOLD_DAYS
  delivery_atr_sl_mult: float = 2.5       # DELIVERY_ATR_SL_MULT
  delivery_notional_cap_pct: float = 0.20 # per-position notional cap × capital_delivery
  delivery_activate_r: float = 1.75       # arm trail at +1.75R
  delivery_trail_r: float = 1.0           # trail distance in R
  delivery_deliv_min: float = 75.0        # DELIVERY_DELIV_MIN
  delivery_turnover_min_cr: float = 25.0  # DELIVERY_TURNOVER_MIN_CR
  delivery_turnover_max_cr: float = 50.0  # DELIVERY_TURNOVER_MAX_CR
  ```
- **`from_env()`** after `capital_momentum=` (~line 436): `capital_delivery=_env_float("CAPITAL_DELIVERY", 0.0),`
  and after the pead env block (~line 490) the matching `_env_float/_env_int` reads for all 10 params above.
- **`channel_capital()`** after the momentum branch (~line 262):
  ```python
  if ch == "delivery" and self.capital_delivery > 0:
      return self.capital_delivery
  ```

**Acceptance:** `StrategySettings.from_env()` with `CAPITAL_DELIVERY=200000 DELIVERY_RISK_PER_TRADE=3000`
returns the right values; `channel_capital("delivery")==200000`; defaults (unset) leave channel off.

---

## 4. `domain/portfolio_book.py` (exact)

- **ChannelName enum** (~line 39): add `DELIVERY = "delivery"`
- **DEFAULT_CHANNEL_PCT** (~line 50): add `ChannelName.DELIVERY.value: 0.00` (0 ⇒ off; capital comes from
  `capital_delivery` via `channel_capital`, so the % default stays 0 and doesn't rebalance others).
- Daily 3%/6% breaker: **no code change** — `check_can_open` already applies `daily_loss_pct/daily_profit_pct`
  to `channel_capital(ch)`; delivery inherits it via `capital_delivery`.

**Acceptance:** `check_can_open(ChannelName.DELIVERY, ...)` gates on ₹2L (loss 3% = ₹6k, profit 6% = ₹12k);
existing channels' budgets unchanged (assert byte-identical behavior for swing/pead in a test).

---

## 5. `services/delivery_signal_service.py` (new — mirror `pead_signal_service.py`)

Pure candle+delivery → candidate logic (no I/O):
- Input: latest-session delivery rows (`{symbol: deliv_pct}` for that date, deliv_pct≥75) + per-symbol daily
  bars.
- For each delivery symbol: compute `turnover_20d_cr`, ATR14, `passes_delivery_gates` → candidate with
  `entry_price` (next open proxy = reaction close), `sl_price = close − 2.5·ATR`, `atr`, `score = deliv_pct`.
- **Rank by `deliv_pct` desc** (the entry-ranking lever), return top candidates.

**Acceptance:** given a synthetic delivery+bars fixture, emits exactly the gate-passing names, ranked by
delivery-%, with correct sl/atr.

---

## 6. `services/delivery_trading_service.py` (new — mirror `pead_trading_service.py:run_pead_scan_once`)

`run_delivery_scan_once(settings, upstox, state, order_service, bq, reaction_date)`:
1. **Gate off** if `settings.capital_delivery <= 0` → return `{"skipped": "delivery_disabled"}`.
2. Read yesterday's delivery from BQ `nse_delivery_daily` (date = last session), deliv_pct≥75.
3. Resolve instrument keys (BQ `candles_daily`, mirror pead `_resolve_instrument_keys`).
4. Pull daily bars per candidate on-demand (`_fetch_symbol_daily`, pead pattern) for turnover/ATR/entry.
5. `delivery_signal_service.scan(...)` → ranked candidates.
6. Per-channel daily breaker (mirror pead: 3%/6% on `capital_delivery`) + `PortfolioBook.check_can_open`.
7. Size: `qty = min(risk//sl_dist, (capital/slots)//entry, 0.02·turnover//entry)` (**2%-participation cap**);
   cap by `delivery_notional_cap_pct`.
8. Place orders: `order_service.place_entry_order(..., product="CNC", reason="DELIVERY",
   strategy="DELIVERY", wl_type="delivery", channel="delivery", allow_live_orders=False)`.

**Acceptance:** dry-run against a captured day → correct # entries (≤5), CNC, tagged `channel/wl_type=delivery`,
participation ≤2%, breaker respected. **No order when `CAPITAL_DELIVERY=0`.**

---

## 7. `services/delivery_reconciliation_service.py` (new — mirror `pead_reconciliation_service.py`)

Daily premarket exit (the live exit path — NOT ws_monitor):
- Filter open positions `wl_type == "delivery"`.
- Per position: fetch daily bars, evaluate max-hold **20d** + **1R trail armed at +1.75R** + daily SL backstop,
  reusing `swing_exit.trailed_stop` (single source of truth). Close via AMO / update GTT SL.

**Acceptance:** a position past 20d closes; an armed +1.75R position ratchets the 1R stop; SL-breach forces
close. Uses the same trail math as swing/pead (assert parity).

---

## 8. `web/api.py` — endpoints (mirror pead at 1825 / 1854)

- `POST /jobs/delivery-ingest` → `container.delivery_ingest_service().run()` (Step 1)
- `POST /jobs/delivery-reconcile` → `container.delivery_reconciliation_service().run(asof)`
- `POST /jobs/delivery-scan` → `container.run_delivery_scan()` → `run_delivery_scan_once(...)`
All behind `_auth(job_trigger_token)`; parse `X-CloudScheduler-JobName` for sched_ctx (mirror pead).

## 9. `container.py` — factories (mirror pead ~line 169–189)

Add `delivery_signal_service`, `delivery_trading_service.run_delivery_scan`, `delivery_ingest_service`,
`_delivery_reconciliation_service` + factory.

## 10. `ws_monitor_service.py` — Rule 8 touchpoint ⚠

- **One line**: add `"delivery"` to `_OVERNIGHT_SL_ONLY_WL` frozenset (line ~67) so delivery positions are
  **excluded from the intraday tick-exit loop** (they exit via daily reconciliation only).
- **This means a ws-monitor deploy is REQUIRED** (Rule 8): `cloudbuild.ws.yaml` build +
  `gcloud run deploy autotrader-ws-monitor`. Without it, ws-monitor could tick-exit delivery holds wrongly.

---

## 11. Parity engine — `scripts/redesign/delivery_final.py`

Rewrite the isolated backtest to **import `domain/delivery_signals.py`** (the shipped gates), reproduce the
12.8%/₹5L (11.9%/₹2L) numbers → proves backtest = prod (like `swing_final.py` / `momentum_grind.py`).
**Acceptance:** reproduces ~12–13% CAGR, 7/7 years within tolerance; any drift ⇒ a real prod/backtest gap.

## 12. Tests (match the ~960-test bar)

`test_delivery_signals.py` (gate boundaries), `test_delivery_sizing.py` (risk/slot/participation caps),
`test_delivery_scan.py` (slot walk, disabled-when-capital-0, breaker), `test_delivery_reconcile.py`
(max-hold/trail/SL parity with swing_exit), `test_delivery_ingest.py` (parse + dedupe). Additive; assert
existing channel tests still pass.

## 13. Dashboard

`dashboard/src/lib/constants.ts`: add `"delivery"` to `CHANNEL_ORDER` + a `CHANNEL_META` entry (mirror
momentum). Verify in browser preview (memory rule).

---

## 14. Cloud Scheduler jobs

| Job | ~IST | Endpoint |
|---|---|---|
| `autotrader-delivery-ingest-1900` | 19:00 | `/jobs/delivery-ingest` |
| `autotrader-delivery-reconcile-0900` | 09:00 | `/jobs/delivery-reconcile` |
| `autotrader-delivery-scan-0912` | 09:12 | `/jobs/delivery-scan` (after pead-scan) |

## 15. Deploy sequence (Rules 1 / 3 / 8)

1. `git fetch origin main && git merge --ff-only origin/main` (Rule 1 — sync before `--source`).
2. Test suite green (all delivery tests + full existing suite unchanged).
3. `export CLOUDSDK_AUTH_ACCESS_TOKEN=$(gcloud auth application-default print-access-token)` (Rule 3).
4. **Main service**: `gcloud run deploy autotrader --source ...` (the endpoints, services, settings).
5. **ws-monitor** (Rule 8): `gcloud builds submit --config cloudbuild.ws.yaml ...` +
   `gcloud run deploy autotrader-ws-monitor --image ...` (the frozenset one-liner).
6. Set env (env-only update, preserves the rest):
   `CAPITAL_DELIVERY=200000 DELIVERY_RISK_PER_TRADE=3000 DELIVERY_MAX_POSITIONS=5 DELIVERY_MAX_HOLD_DAYS=20
   DELIVERY_ATR_SL_MULT=2.5 DELIVERY_DELIV_MIN=75 DELIVERY_TURNOVER_MIN_CR=25 DELIVERY_TURNOVER_MAX_CR=50`.
7. Create the 3 Cloud Scheduler jobs (Step 14).
8. **Verify**: both service revisions updated; `/jobs/delivery-ingest` populates BQ; a manual `/jobs/delivery-scan`
   returns candidates (or a clean `skipped` if no signal that day).

---

## 16. Forward-validation checklist (first live PAPER sessions)

- Delivery `scan_decisions`/orders are **25–50cr mid-caps** with deliv_pct≥75 (NOT large-caps).
- ~1–1.5 entries/week; positions CNC, tagged `channel=delivery`.
- Concurrent overlap with momentum stays **<10%** (the live diversification check).
- Realized fills **<0.15% slippage**; if worse, re-size down.
- Every backtest year was positive — a losing calendar year live = first red flag.

## 17. Rollback (instant, no code)

`gcloud run services update autotrader --update-env-vars CAPITAL_DELIVERY=0` → `channel_capital("delivery")`
returns 0 → scan no-ops, no new entries. Existing delivery positions exit normally via reconciliation.
(Same instant-off lever gapfade used.)
