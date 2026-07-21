# PLEDGE channel — promoter pledge-release (channel #8)

> Built 2026-07-21. Reuses the insider PIT/XBRL feed + `nse_insider_daily` BQ table (NO new ingest,
> NO new table, NO new NSE feed). Additive, channel-isolated, PAPER. See `domain/pledge_signals.py`.

## Thesis
A promoter **pledge REVOKE** (un-pledging shares) = deleveraging / reduced financial distress = a
bullish informed action. Distinct from insider cluster-buys (15.6% temporal overlap; the edge
survives with NO insider buy nearby — validated additive). Two-sided validation: revoke = bullish,
invocation (forced sale) = strongly bearish (−11.5% OOS f60).

## Validated FINAL config (2026-07-21)
`promoter pledge-revoke` + `px > 200DMA` (falling-knife filter) + **DOUBLE MACRO GATE** `b200>50 AND
Nifty>100DMA` + `turnover_20d ≥ 25cr` + `price ≥ 30` + `hold 60 sessions` + `10 slots (cap 10%/pos,
no-leverage)` + `ATR14×2.0 fixed-hold stop (NO trail)` + `1.5% risk` + CNC.

**Backtest 2016–26** (survivorship-safe incl. delisted, full Upstox cost+slip, IS≤2020/OOS≥2021):
+25.0% CAGR / −11.5% maxDD / Calmar 2.18 (IS 1.96 / OOS 4.17) / ~39 trades/yr / all-years-positive.
**Honest forward expectation: ~15–20% CAGR normal regimes (mid-20s in bull-breadth), Calmar ~1.5–2.0,
DD ~−10–12%.** The full-sample figure is bull-inflated (OOS 2021–24). A thin-but-real additive
diversifier, not a standalone CAGR-mover.

### What the grind established (and killed)
- **Kept (robust IS+OOS):** px>200DMA (Calmar 1.26→1.62), diversified cap-10% deployment (harvests
  idle capital → 2.16), tighter 2.0×ATR stop (IS Calmar 1.09→1.96), turnover≥25cr (liquid/fillable).
- **Killed (OOS-fail / overfit):** release-size / release-% / holding-% magnitude filters (negative
  OOS excess); the h60 Calmar peak (compounding artifact — hold surface is noisy, h60 is the robust
  *central* choice, not a peak); trailing / 50DMA-break exits (whipsaw the drift); looser gates.

## Architecture (mirrors insider; reuses its plumbing)
- **Data:** `nse_insider_daily` (insider ingest writes every leg incl. pledge/revoke verbatim). No
  separate ingest. Verified live 2026-07-21: `Pledge Revoke` rows present (latest 2026-07-17).
- **Domain:** `domain/pledge_signals.py` (pure; qualifies_revoke / aggregate_revokes / 200DMA gate /
  macro_gate / liquidity-ranked select).
- **Services:** `pledge_signal_service` (BQ read + pure candidate build), `pledge_trading_service`
  (`run_pledge_scan_once` → macro gate → revokes → dailies → gates → CNC entries; fail-closed on
  `capital_pledge<=0`), `pledge_reconciliation_service` (60d max-hold + SL-breach, no trail).
- **Wiring:** container (`run_pledge_scan`, `pledge_reconciliation_service`), api (`/jobs/pledge-scan`,
  `/jobs/pledge-reconcile`), EOD exemptions ×2 (order_service + ws_monitor), dashboard_api
  (`/dashboard/pledge/watchlist`, `_CHANNELS`, `max_pos`), frontend (types/constants/utils/api +
  channels drill-down + positions labels).
- **Isolation:** distinct `channel="pledge"` / `wl_type="pledge"` — invisible to swing/intraday/other
  reconciliation + the intraday tick path. 1100 tests green, no regression.

## Env (set to enable; PAPER)
`CAPITAL_PLEDGE` (0 = disabled/no-op). Optional overrides: `PLEDGE_MAX_HOLD_DAYS=60`,
`PLEDGE_ATR_SL_MULT=2.0`, `PLEDGE_NOTIONAL_CAP_PCT=0.10`, `PLEDGE_TURNOVER_MIN_CR=25`,
`PLEDGE_MAX_POSITIONS=10`, `PLEDGE_B200_MIN=50`, `PLEDGE_RISK_PER_TRADE` (0 → 1.5% of capital).

## Deploy runbook (3 services + 2 schedulers) — GATED
1. `gcloud run services update autotrader --update-env-vars CAPITAL_PLEDGE=<amt>` (+ any PLEDGE_*).
2. Deploy **autotrader** (main, `--source`) — Rule 1 sync-first.
3. Deploy **autotrader-ws-monitor** (`cloudbuild.ws.yaml` → image) — Rule 8 (EOD exemption lives here).
4. Deploy **autotrader-dashboard** (frontend drill-down).
5. Cloud Scheduler: `autotrader-pledge-recon-0907` (POST /jobs/pledge-reconcile, ~09:07 IST) +
   `autotrader-pledge-scan-0912` (POST /jobs/pledge-scan, ~09:12 IST, after insider ingest + scan).
6. Verify all 3 revisions; confirm `pledge_scan_summary` logs + `pledge_watchlist/latest` gate doc;
   confirm the other 7 channels' positions/scan unaffected.

Monday validation: first premarket scan writes the gate doc; entries fire only when a promoter
revoke lands on the latest disclosure day AND both macro gates are open (rare — ~15–40/yr, lumpy).
