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
| `nse_short_selling` | daily short qty per stock, 2015-26 | ~~short-interest~~ | **KILLED 2026-07-21** (triage + DEEP re-grind `short_grind.py`): coverage-invariant grind (short/vol ratio, per-stock z, cross-sectional daily percentile) confirms high-short-intensity → underperformance (OOS Q5 −0.75% vs Q1 −0.17%, monotonic = real informed shorting) BUT unusable: (a) it's a SHORT/avoid direction (we're stock-only-long); (b) as a long avoid-FILTER it adds ~nothing (OOS −0.31→−0.29%, +0.02pp — whole short-reported set is weak); (c) IS un-testably sparse (3 days ≥10 names) → can't validate across regimes. Real signal, not long-monetizable |
| `nse_fo_futures` (+`nse_fo_pcr` USED by brain) | stock/index futures OI, 2016-26 | ~~OI-buildup~~ | **KILLED 2026-07-21** (sweep): 4-quadrant price×OI is a beta/momentum proxy — non-monotonic (collapses at big builds), f60 UNDERPERFORMS OOS baseline, regime-pocket doesn't replicate |
| `fundamentals_financials` | annual income/bal/cashflow — **snapshot 2026-06-28, dense only FY2023+** | ~~Growth/Quality/Value factor~~ | **KILLED 2026-07-21** (data-limited): only 337 current survivors + history dense only FY2023+ (≤9 cos/yr before) → NO IS period → un-backtestable; + survivorship + factor-arbitrage |
| `fundamentals_holdings` | promoter/FII/DII/MF % — **single snapshot 2026-06-28** | ~~Ownership-accumulation~~ | **KILLED 2026-07-21** (data-limited): 1 distinct date, no time series → un-backtestable |
| `fundamentals_ratios` | P/E, ROE — **single snapshot 2026-06-28** | ~~value/quality screen~~ | **KILLED 2026-07-21** (data-limited): 1 distinct date → un-backtestable |
| candles 5m/1m/1d, bhavcopy | OHLCV | breakout / overnight / gap-fade / intraday / MR | **KILLED** (arbitraged/cost; re-confirmed 2026-07-21 sweep: overnight drift real but cost-killed, gap-ups fade, MR regime-reverses, breakout=beta) |
| bulk/block deals | large trades | deal-follow | **KILLED** (no OOS edge; re-confirmed 2026-07-21: bulk buys=distribution neg-IS, block net-buy structurally untestable) |

## B. Phase 2 — NSE Corporate Filings FETCHED (2026-07-23) → grind queue

All 42 Corporate-Filings (Equity) menu items were inventoried against the user's copied sidebar. Real
API endpoints were **read from the pages' compiled JS bundles** (not guessed) — the reusable pattern is
`/api/<name>?index=equities&from_date=DD-MM-YYYY&to_date=DD-MM-YYYY` via the insider NSE handshake
(session warmup → browser UA + Referer). Each feed below was fetched to `scratchpad/*.json` and profiled
for real multi-year history (row count alone ≠ history — SAST Reg30 taught that). GRIND all 7, top-down.

| # | Data (endpoint) | Rows | Span | Candidate channel | odds |
|---|---|---|---|---|---|
| 1 | **SAST Reg 29 Promoter** (`corporate-shareholding-disclosure?type=reg29`) | 15,531 | 2017–26 | promoter large-stake open-mkt buys — insider's bigger-conviction cousin | ★★★ |
| 2 | **Shareholding Patterns** (`corporate-share-holdings-master`) | 89,073 | 2015–26 | promoter/FII/DII stake-change accumulation — **revives killed ownership idea, now REAL history** | ★★★ |
| 3 | **SAST Reg 31 pledge** (`corporate-shareholding-disclosure?type=reg31`) | 88,622 | 2016–26 | richer pledge feed (lender+reason+before/after%) — overlap-check vs live pledge | ★★ |
| 4 | **Board Meetings** (`corporate-board-meetings`) | 135,569 | 2015–26 | purpose-tagged intimations (fundraise/buyback/bonus) | ★★ |
| 5 | **Financial Results** (`corporates-financial-results` + `integrated-filing-results`) | 116,629 | 2015–26 | earnings-surprise / fundamental-momentum (numbers behind per-row XBRL) | ★★ |
| 6 | **Related Party Txns** (`related-party-transactions-master`) | 11,308 | 2022–26 | governance red-flag (IS-thin, 2022+ only) | ★ |
| 7 | **Voting Results** (`corporate-voting-results`) | 28,798 | ~2015–26 | shareholder dissent (metadata-nested, needs parse) | ★ |

**Phase-2 DEAD (fetched, data-limited, skip):** SAST Reg 7 (`type=reg7`, 2017–2020 only — feed
discontinued) · SAST Reg 30 (`type=reg30`, 2017+2022 sparse annual snapshot, `typeOfEvent`/`regulations`
null on every row — not an event stream) · Insider Trading-Plan (`TradingPlandata` — returns [] in bulk,
per-symbol only). **Could not self-discover endpoint (stopped guessing per no-hallucination rule):** SAST
Reg 29 Non-Promoter (`type=reg29np` → confirmed-invalid error shape), base SDD Credit-Rating.

**Still menu-listed but SKIP (admin / wrong-asset-class):** Annual Reports, BRSR, Company Directory,
Corporate Governance, all Debt Centralised DB items, Debt Liquidity Window, Debt Reg 50, Event Calendar,
Investor Complaints, Issuer Offer Docs, Loss/Dup Certificate, all 6 Mutual Fund PIT items, MF Updates,
Secretarial Compliance, Share Transfers, Statement of Deviation, Statement on Audit Qualification,
Trading Window Disclosure, Unitholding Patterns (REIT/InvIT), + Further-Issues sub-items (ADR/GDR/FCCB;
QIP/Preferential deferred as ★ leads if queue dries up).

---

## C. Build queue (grind → build → deploy, one at a time)

1. **Promoter-Pledge** — ✅ **VALIDATED + BUILT 2026-07-21** (Calmar 2.18 / +25% CAGR bull-inflated → ~15-20% honest; reuses insider feed; 1100 tests green). Deploy GATED. See `docs/PLEDGE_CHANNEL.md`.
2. ~~Buyback~~ — **KILLED 2026-07-21** (full-set BQ test, ~₹0): `nse_corp_actions` has NO announcement date (`broadcast_date` NULL for all buybacks) — only ex-dates (already-priced tender-eligibility); ex-date fwd-return at/below baseline OOS, f60 median −3.9%/WR 44% = outlier noise; tender premium is captured on announcement not ex-date. Un-testable + un-tradeable with our data. Scripts: `scripts/redesign/buyback_diag.py`.
3. ~~FII/DII flow~~ — **KILLED 2026-07-21** (diag: IS→OOS sign-reversal, regime-unstable; worsens the price gate OOS; no cash flow / per-stock data). See §A.
4. ~~Fundamental Growth/Quality~~ + ~~short-interest~~ + ~~OI-buildup~~ + ~~dividend/rights~~ — **ALL KILLED 2026-07-21** (exhaustive multi-agent sweep). See §A.
5. *(Phase 2 — ALL FETCHED 2026-07-23, see §B)* grind top-down: **① SAST Reg 29 Promoter (★★★, diag done — messy, needs proper grind)** → **② Shareholding Patterns (★★★, revives ownership w/ real history)** → ③ SAST Reg 31 pledge → ④ Board Meetings → ⑤ Financial Results → ⑥ Related-Party → ⑦ Voting Results. Every grind READ-ONLY + isolated; per-item bar in §D.

**State (2026-07-21) — CACHED DATA EXHAUSTIVELY SWEPT.** An exhaustive multi-agent workflow (13 dataset-hypotheses × dozens of configs, adversarially verified, ~646k tokens) rule-in/out'd EVERY remaining cached NSE dataset → **ZERO tradeable long edges** beyond the 3 live channels. The sweep found genuine market phenomena (overnight drift, gap-fade, informed shorting, high-yield value drift) but NONE survives long-only + full 0.7% cost + both-halves-OOS. Data-limited sets (fundamentals — single 2026-06-28 snapshot, no IS history) are un-backtestable. **Conclusion: the 3 live channels (delivery, insider, pledge) captured everything monetizable in the cached data. The only remaining frontier is Phase 2 — NSE data we do NOT ingest yet (SAST etc.), via user screenshots.** Sweep scripts: `scripts/redesign/{fii_diag,fii_grind,fii_grind2,fii_grind3,buyback_diag,fund_grind}.py` + the workflow-agent grinds.

**Deeper follow-up (2026-07-21, cheap targeted — the two genuinely-unexplored angles):** (a) **cross-dataset COMBINATIONS** (conjunctions of insider-buy / short-covering / OI-buildup / px>200DMA / macro) → KILLED: best combo beats f20 baseline both halves but only ~0.4pp/20d, WR~50% = momentum/beta tilt, not alpha (`overlay_combo_grind.py`). (b) **OVERLAYS on live channels** (does a stock px>trend filter improve the insider channel?) → event-level screen looked promising, but re-run through the CANONICAL `insider_engine.walk` (reproduces the validated +23%/Cal1.84 baseline exactly) it is a lucky CELL not a plateau — only px>50DMA at the exact locked h90/s10 helps both halves; 100/200DMA hurt OOS, 50DMA hurts IS at h60/s8 → overfit, NO live change (`insider_trend_parity.py`). Both unexplored angles now closed: still zero shippable edge in cached data.

Deferred (data-limited): ownership-holdings (6mo history), ratios-value (snapshot). Revisit once we accumulate live history or find a longer source.

## D. Per-item bar (every candidate must clear before build)
Survivorship-safe (incl. delisted) · IS + OOS both positive · not one-year-carried · additive/low-overlap with the 7 live channels · full Upstox cost · plateau not peak · realistic fills. Kill honestly if it doesn't. Thin-but-real event channels (pead/corp tier) are acceptable as diversifiers; needle-movers are rare.
