# NSE Data → Channel Catalog (channel pipeline)

> **Purpose:** the master list of every data source (what we have + what NSE publishes that we
> don't yet) and the candidate channel each could power. We grind → build → deploy **one at a
> time, top-down**, ticking items off here. Created 2026-07-21.
>
> **Honest-odds legend:** ★★★ tight fit w/ our proven pattern (informed accumulation, niche
> alt-data) · ★★ plausible · ★ long shot / crowded. **Status:** LIVE · BACKTEST-NOW ·
> NEEDS-HISTORY · NEEDS-FEED · KILLED.
>
> **What has actually worked (the pattern):** niche NSE alt-data capturing *informed accumulation*
> (delivery-% footprint, insider open-market buys). What has died: raw OHLCV patterns (breakout,
> overnight, gap-fade, intraday, MR) — arbitraged/cost-killed; and thin high-freq. New candidates
> are ranked against that reality. Every grind stays READ-ONLY + isolated; every ship additive.

---

## A. Data we HAVE (in BQ / cached) → candidate channels

| Data source | What it is / coverage | Candidate channel(s) | Status · odds |
|---|---|---|---|
| `nse_delivery_daily` | delivery-% per stock, daily, current | **Delivery-accumulation** | **LIVE** |
| insider PIT (cached 341k + `nse_insider_daily`) — **BUYS** | informed open-market buys, 2015→now | **Insider Cluster-Buy** | **LIVE** |
| insider PIT — **PLEDGE** events (create/release/invoke, ~38k rows in cache) | promoter share-pledging, 2015→now, survivorship-safe | **Promoter-Pledge** (release=deleverage/bullish; creation=stress/bearish/avoid) | **BACKTEST-NOW · ★★★** |
| `nse_corp_actions.subject` — buyback | ex-date only (`broadcast_date` NULL for ALL); ~100 rows, ~53 fillable | ~~Buyback~~ | **KILLED 2026-07-21** (full-set BQ test, ~₹0) |
| `nse_corp_actions` — dividend / rights | special-dividend; rights (dilutive) | dividend-signal (thin); rights = avoid-filter | BACKTEST-NOW · ★ |
| `nse_fii_deriv` | FII F&O buy/sell/OI daily (→06-22) | **FII-flow regime overlay** (smarter gate than Nifty>100DMA → lifts ALL channels) or directional | BACKTEST-NOW · ★★ |
| `nse_participant_oi` | FII/DII/Pro/Client long-short OI daily (→06-22) | **Smart-money positioning** (Pro/FII net bias) — timing/long-bias | BACKTEST-NOW · ★★ |
| `nse_short_selling` | daily short qty per stock (→06-22) | short-interest (rising=bearish avoid; covering=bullish) — adjacent to killed squeeze | BACKTEST-NOW · ★ |
| `nse_fo_futures` (+`nse_fo_pcr` USED by brain) | stock/index futures OI, PCR | OI-buildup (price×OI long/short buildup) | BACKTEST-NOW · ★ |
| `fundamentals_financials` | revenue/profit/margins, 337 cos × 2003-25 (annual) | **Growth / Quality / Value factor** | BACKTEST-NOW (survivor-biased + arbitraged) · ★ |
| `fundamentals_holdings` | promoter/FII/DII/MF % holdings — **only Apr–Sep 2025** | Ownership-accumulation (promoter/FII stake ↑) | **NEEDS-HISTORY** (6mo too short; accumulate live) · ★★ |
| `fundamentals_ratios` | P/E, ROE, … company vs sector — **single snapshot** | live value/quality screen input | NEEDS-HISTORY (no time series) · ★ |
| candles 5m/1m/1d, bhavcopy | OHLCV | breakout / overnight / gap-fade / intraday / MR | **KILLED** (arbitraged/cost) |
| bulk/block deals | large trades | deal-follow | **KILLED** (no OOS edge) |

## B. NSE publishes but we DON'T ingest yet (Phase 2 — needs a new feed like insider) → candidate channels

| Data (NSE page) | Candidate channel | odds |
|---|---|---|
| **SAST disclosures** (Reg 29 — >5% acquisitions, open offers, creeping acquisition) | **SAST-accumulation** (strategic/PE/promoter large-stake buying — bigger-conviction cousin of insider) | ★★★ |
| Credit-rating changes (upgrades/downgrades) | rating-momentum event | ★★ |
| Index reconstitution (Nifty 50/100/500 inclusion-exclusion) | index-inclusion front-run (F&O-inclusion was thin; Nifty-inclusion untested) | ★★ |
| Board-meeting outcomes / order wins / capex / fund-raising announcements | announcement-event channels | ★ |
| ASM/GSM surveillance lists | avoid-filter / post-exit mean-reversion | ★ |
| New listings / IPO | post-listing drift | ★ |
| MF monthly portfolio disclosures | MF-accumulation (monthly) | ★★ |

*(Phase 2 confirmed via user's NSE screenshots → verify each feed's server-side reachability + backtest data before building.)*

---

## C. Build queue (grind → build → deploy, one at a time)

1. **Promoter-Pledge** — ✅ **VALIDATED + BUILT 2026-07-21** (Calmar 2.18 / +25% CAGR bull-inflated → ~15-20% honest; reuses insider feed; 1100 tests green). Deploy GATED. See `docs/PLEDGE_CHANNEL.md`.
2. ~~Buyback~~ — **KILLED 2026-07-21** (full-set BQ test, ~₹0): `nse_corp_actions` has NO announcement date (`broadcast_date` NULL for all buybacks) — only ex-dates (already-priced tender-eligibility); ex-date fwd-return at/below baseline OOS, f60 median −3.9%/WR 44% = outlier noise; tender premium is captured on announcement not ex-date. Un-testable + un-tradeable with our data. Scripts: `scripts/redesign/buyback_diag.py`.
3. **FII/DII flow** ← **NEXT** — as a regime overlay first (could lift existing channels) then maybe standalone.
4. **Fundamental Growth/Quality** — only if 1-3 thin; survivorship + arbitrage headwinds; do survivorship-honest.
5. *(Phase 2)* **SAST-accumulation** — needs a new NSE feed; highest-odds of the not-yet-ingested set.

Deferred (data-limited): ownership-holdings (6mo history), ratios-value (snapshot). Revisit once we accumulate live history or find a longer source.

## D. Per-item bar (every candidate must clear before build)
Survivorship-safe (incl. delisted) · IS + OOS both positive · not one-year-carried · additive/low-overlap with the 7 live channels · full Upstox cost · plateau not peak · realistic fills. Kill honestly if it doesn't. Thin-but-real event channels (pead/corp tier) are acceptable as diversifiers; needle-movers are rare.
