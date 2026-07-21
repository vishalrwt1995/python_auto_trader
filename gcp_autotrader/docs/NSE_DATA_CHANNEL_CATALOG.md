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
| `nse_corp_actions` — dividend / rights | special-dividend; rights (dilutive) | ~~dividend-capture / rights~~ | **KILLED 2026-07-21** (sweep): dividend high-yield≥3% f60 drift beat baseline both halves (t=3.4/6.4) but adversarial verify REFUTED — beta-neutral IS alpha insignificant (t=1.5), OOS win is 2021-23 PSU/value melt-up, DEAD 2.5yr (2024-26 excess −0.3%). Rights = short/avoid only (dilutive), n=19 IS, not long-actionable |
| `nse_fii_deriv` / `nse_participant_oi` | FII/DII/Pro/Client index-fut+opt long-short + flow, 2015-26 (→06-19, ~1mo stale) | ~~FII-flow regime overlay / smart-money positioning~~ | **KILLED 2026-07-21** — real signal, no tradeable edge. HARD grind (30-signal screen → 8 OOS-robust survivors → composite): there IS a genuine OOS-robust CONTRARIAN signal (fade Pro/retail/DII-option crowding + follow FII futures flow; composite quintile spread IS +1.9% / OOS +1.1%/20d, holds both halves). BUT no config monetizes it — best-config sweep (cash-threshold + exposure-tilt) shows **every** setting UNDERPERFORMS buy-hold Nifty on CAGR *and* Calmar (signal ~1%/20d < beta drift ~0.9%/20d; acting on it costs more beta than it saves). Overlay adds only +0.3-0.7%/20d within the up-regime. Textbook widely-watched→arbitraged: real signal, un-monetizable. (First-pass note: plain FII index-fut net DOES reverse OOS; the robust part is the contrarian cluster.) Scripts `fii_diag/grind/grind2/grind3.py` |
| `nse_short_selling` | daily short qty per stock, 2015-26 | ~~short-interest~~ | **KILLED 2026-07-21** (sweep): IS-positive (covering/rising) configs FLIP OOS; heavy-short is informed-BEARISH (short/avoid, not long); 2024-26 reporting-regime explosion confounds IS/OOS |
| `nse_fo_futures` (+`nse_fo_pcr` USED by brain) | stock/index futures OI, 2016-26 | ~~OI-buildup~~ | **KILLED 2026-07-21** (sweep): 4-quadrant price×OI is a beta/momentum proxy — non-monotonic (collapses at big builds), f60 UNDERPERFORMS OOS baseline, regime-pocket doesn't replicate |
| `fundamentals_financials` | annual income/bal/cashflow — **snapshot 2026-06-28, dense only FY2023+** | ~~Growth/Quality/Value factor~~ | **KILLED 2026-07-21** (data-limited): only 337 current survivors + history dense only FY2023+ (≤9 cos/yr before) → NO IS period → un-backtestable; + survivorship + factor-arbitrage |
| `fundamentals_holdings` | promoter/FII/DII/MF % — **single snapshot 2026-06-28** | ~~Ownership-accumulation~~ | **KILLED 2026-07-21** (data-limited): 1 distinct date, no time series → un-backtestable |
| `fundamentals_ratios` | P/E, ROE — **single snapshot 2026-06-28** | ~~value/quality screen~~ | **KILLED 2026-07-21** (data-limited): 1 distinct date → un-backtestable |
| candles 5m/1m/1d, bhavcopy | OHLCV | breakout / overnight / gap-fade / intraday / MR | **KILLED** (arbitraged/cost; re-confirmed 2026-07-21 sweep: overnight drift real but cost-killed, gap-ups fade, MR regime-reverses, breakout=beta) |
| bulk/block deals | large trades | deal-follow | **KILLED** (no OOS edge; re-confirmed 2026-07-21: bulk buys=distribution neg-IS, block net-buy structurally untestable) |

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
3. ~~FII/DII flow~~ — **KILLED 2026-07-21** (diag: IS→OOS sign-reversal, regime-unstable; worsens the price gate OOS; no cash flow / per-stock data). See §A.
4. ~~Fundamental Growth/Quality~~ + ~~short-interest~~ + ~~OI-buildup~~ + ~~dividend/rights~~ — **ALL KILLED 2026-07-21** (exhaustive multi-agent sweep). See §A.
5. *(Phase 2)* **SAST-accumulation** ← **highest remaining odds** — the informed-accumulation cousin (★★★), needs a new NSE feed (user screenshots).

**State (2026-07-21) — CACHED DATA EXHAUSTIVELY SWEPT.** An exhaustive multi-agent workflow (13 dataset-hypotheses × dozens of configs, adversarially verified, ~646k tokens) rule-in/out'd EVERY remaining cached NSE dataset → **ZERO tradeable long edges** beyond the 3 live channels. The sweep found genuine market phenomena (overnight drift, gap-fade, informed shorting, high-yield value drift) but NONE survives long-only + full 0.7% cost + both-halves-OOS. Data-limited sets (fundamentals — single 2026-06-28 snapshot, no IS history) are un-backtestable. **Conclusion: the 3 live channels (delivery, insider, pledge) captured everything monetizable in the cached data. The only remaining frontier is Phase 2 — NSE data we do NOT ingest yet (SAST etc.), via user screenshots.** Sweep scripts: `scripts/redesign/{fii_diag,fii_grind,fii_grind2,fii_grind3,buyback_diag,fund_grind}.py` + the workflow-agent grinds.

Deferred (data-limited): ownership-holdings (6mo history), ratios-value (snapshot). Revisit once we accumulate live history or find a longer source.

## D. Per-item bar (every candidate must clear before build)
Survivorship-safe (incl. delisted) · IS + OOS both positive · not one-year-carried · additive/low-overlap with the 7 live channels · full Upstox cost · plateau not peak · realistic fills. Kill honestly if it doesn't. Thin-but-real event channels (pead/corp tier) are acceptable as diversifiers; needle-movers are rare.
