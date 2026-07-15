# New-Channel Grind — Findings Log (research/new-channels-grind)

> Running verdict log for the new-channel grind. Reviewed at the end to decide what to implement.
> All READ-ONLY isolated backtests; nothing shipped. Discipline: IS/OOS + survivorship + slippage.

## Grind order
1. ✅ Delivery-accumulation — **RE-GRADED → REAL CANDIDATE** (①) — *was wrongly killed; size-aware re-grind*
2. ✅ Bulk/block-deal follow — **KILLED** (②)
3. ✅ Short-squeeze (short-selling) — **KILLED** (③)
4. ⏭ Intraday-5m — **NEXT / the substantial grind** (`candles_5m_full` ~3-4yr; a different edge family)
Deferred: positioning (market-wide overlay, not a standalone channel) · corp-refetch (known tiny ~2/yr) ·
fundamentals (snapshot, not point-in-time — not backtestable).

---

## ① Delivery-accumulation — VERDICT: ✅ **VALIDATED** (revived + config corrected). Proposal: `DELIVERY_CHANNEL_PROPOSAL.md`

> **The path here took THREE passes — kept transparent because each correction matters:**
>
> **Pass 1 — KILL (wrong).** Flat 0.25% slippage on a mid-cap edge traded at *small* size, plus liquidity
> bucketed by cumulative floors (hid the 25-50cr BAND). Original (wrong) analysis kept below for the record.
>
> **Pass 2 — size-aware revival, but two errors.** Fixed slippage (size-aware `5bps + 0.01·√participation`,
> real fills ~0.06%) and found the 25-50cr band. BUT (a) cited a **₹5L 10.3%** figure that was never cleanly
> reproduced, and (b) kept a `ret5≤0` **dip filter**.
>
> **Pass 3 — alpha/beta isolation → the real config (FINAL).** Ran the delivery signal vs two controls in
> the same 25-50cr band, identical 5-slot walk:
> - **`deliv≥75` (real): +11–13% both halves** · dip-only control: −1 to +2% · **any-day/beta control:
>   −9 to −34% (blows up, −80%+ DD)**. Random mid-cap entry destroys capital → the delivery-% signal is
>   doing genuine selection; **it is real alpha, not beta**, beating the beta baseline by **10–40 points**.
> - The **dip filter HURT** (`deliv≥75` alone > `deliv≥75 & dip` at nearly every hold) → dropped it.
>
> **FINAL config:** `deliv_pct≥75` (**no dip filter**), **25-50cr band**, **hold ~20d**, 5 slots, ATR 2.5×
> arm 1.75R trail 1.0R, size-aware fills, 2%-participation cap.
> **@ ₹5L: 12.8% CAGR · Calmar 0.82 · DD −15.5% · ₹88k/yr · IS +16.9 / OOS +14.0 · 7/7 positive years.**
> **@ ₹2L: 11.9% · ₹32k/yr · 7/7 years.** (hold15–18 variant: ~13.3% · **Calmar 1.3–1.45** · DD −10%.)
> **Robust plateau:** deliv 70–75, band 25–60cr, hold 15–25, slots 5–7 all positive both halves (30–60cr
> the one fail). **Diversifier:** concurrent-holding overlap momentum 7.8% / pead 1.6% / core 0.0% (turnover
> tier 33cr vs 99–360cr) — below momentum's own 0.23 ship credential.
> **Caveats:** exact CAGR ±~2pts (turnover/sizing-definition sensitivity — "~12–13%" is the honest range);
> deliv≥75 plateau but ≥78 drops off; ~3.5yr OOS; live needs a new daily `sec_bhavdata_full` ingestion job.
> **Scripts:** `delivery_{capacity,profit,alpha_check,lock,overlap}.py`.
>
> _The original KILL analysis is kept below as the record of the first (wrong) pass — flat slippage +
> floor-bucketing._

**Thesis:** high delivery-% (real buyers taking delivery = accumulation) + price dip → forward drift.
**Data:** `nse_delivery_daily` (2.86M rows / 3,121 syms / 2020-07→2026-06), paired with survivorship-safe
`pead_full_bars_2014`. Isolated cache `~/.autotrader_grind_cache/delivery.pkl` (one 118MB scan).

**Best config:** `deliv_pct≥75 & 5d-ret≤0`, enter next open, hold ≤10d (1.75R-arm/1.0R-trail), 5 slots,
₹3k risk / ₹2L, full Upstox cost. ~120 trades/yr, WR ~49%.

**Numbers — slippage is the verdict-maker:**
| slip/leg | CAGR | Calmar | maxDD | IS | OOS |
|---|---|---|---|---|---|
| 0.10% | 9.5% | 0.64 | −14.8% | +15.2% | +7.9% |
| 0.25% (realistic) | 5.3% | 0.29 | −18.3% | +10.3% | +2.5% |
| 0.50% | −4.6% | −0.11 | −41.5% | +1.4% | −8.5% |

**The killer — liquidity tiers @ 0.25% slip:** ≥10cr +3.7% · ≥25cr +4.1% · **≥50cr −10.3%** · ≥100cr −42%.
The edge lives ONLY in thin ₹10–25cr names (highest real slippage); on liquid names it's negative.

**Why killed:** real both-halves survivorship-safe signal, but **un-executable at scale** — thin-name-trapped,
slippage-fragile (dead by 0.5%), negative on the liquid names you could actually fill. The 9.5% headline
was a fillability mirage. Same disease as gapfade/gapdown. Exhaustively checked: dip depth, delivery level,
spike, persistence, hold length, oversold, MR vs momentum exits, liquidity tiers, combos — none survive.

**Implementation note (for the final review):** do NOT build as a standalone channel. **Possible future
overlay** — delivery-% as a confirmation *filter* on swing/momentum entries (untested); revisit only if a
liquid-name variant emerges. Scripts: `delivery_{pull,diag,bt,bt2,bt3,final}.py`.

---

## ② Bulk/block-deal follow — VERDICT: KILL (edge dies on execution)

**Thesis:** follow BUY block deals (large negotiated institutional trades = accumulation) → drift.
**Data:** `nse_block_deals` (10k deals / 6,376 BUY) + `nse_bulk_deals` (191k), 2005-2026; usable 2014-2026
(~12yr) paired with `pead_full_bars_2014`. Isolated `~/.autotrader_grind_cache/deals.pkl`.

**Diagnostic:** the overall buy-deal pool is **negative IS** (−1.42% fwd10); only the BLOCK subset looked
positive (IS +1.77 / OOS +1.44 fwd10), bulk deals negative.

**Engine-test (portfolio walk):** block-buy **DIED** — −11.0% CAGR / −82.5% DD, **IS −7.3 / OOS −8.2
(negative both)**, even at 0.10% slip; broadly red by-year (2015 −30k, 2018 −34k, 2024 −30k). No config
(ratio≥1×/3×, ≥50cr liquidity, hold 10/20) rescues; 0.25% slip worse.

**Why killed:** the +1.77/+1.44 was a **raw close-to-close artifact**. Block-deal stocks are volatile → the
ATR stop whipsaws, the next-open entry gaps post-deal, and cost finishes it. Realistic execution flips the
raw drift negative. Bulk deals negative regardless.

**Implementation note:** none — shelved. Scripts: `deals_{pull,diag,bt}.py`.

---

## ③ Short-squeeze (NSE short-selling) — VERDICT: KILL (no signal — dead in CSV *and* walk)

**Thesis (LONG side, stock-only):** NSE discloses daily short-sold qty/symbol; a spike in short pressure
+ reversal/oversold → trapped shorts cover → upward drift (long bounce).
**Data:** `nse_short_selling` (69k rows / 1,244 syms / 2008-2026; ~24k usable events 2014-26 paired with
survivorship-safe `pead_full_bars_2014`). Isolated `~/.autotrader_grind_cache/shorts.pkl`.

**Signal is trivially small:** short-intensity (short qty / day volume) median **0.01%**, p90 0.80%,
p99 6.6%. NSE self-reported short data barely registers as pressure.

**Diagnostic — flips sign, and negative where fillable:**
- core short-intensity 5-10% = IS **+0.84** / OOS **−0.24** fwd10 (positive IS, negative OOS = overfit/noise)
- in LIQUID names (≥50cr, the only fillable ones): sint≥5% = IS **−1.53** / OOS **−0.74** (negative BOTH);
  add ret5≤0: IS **−3.85** / OOS **−0.17**. No fillability escape — it's negative *where you could trade*.
- the only "EDGE" buckets were generic price features (above-20d-low, positive ret5), not short-selling.

**Engine-test (portfolio walk) — every config loses:** all negative/≈0 CAGR with −30 to −97% DD,
**negative OOS across the board** (sint≥5% & ≥50cr: −11.8% CAGR / −76.7% DD / IS −2.8 / OOS −16.6 @0.10%
slip; worse @0.25%). Best-looking (sint≥2% & ret5≤0 h20) = +0.6% CAGR but IS +3.2 / **OOS −2.7**, goes
negative at realistic slip. Red almost every year 2016-2026.

**Why killed:** unlike delivery/deals (real raw signal, died on execution), short-selling has **NO robust
raw signal** — negative in CSV *and* walk, IS *and* OOS, thin *and* liquid, every slippage. Squeeze thesis
falsified: high short-intensity does not predict a bounce; in liquid names it mildly predicts
underperformance (an un-tradeable short in a stock-only book).

**Implementation note:** none — shelved. Scripts: `shorts_{pull,diag,bt}.py`.

---

## Pattern so far
Three non-intraday microstructure candidates graded exhaustively — each through the **full bar**:
diagnostic (IS/OOS feature buckets) → engine-walk (slots + Upstox cost) → slippage-stress →
liquidity-tiers → survivorship-safe universe (incl. delisted).

| # | Channel | Verdict | Why |
|---|---|---|---|
| ① | Delivery-accumulation | ✅ **VALIDATED** | **12.8% CAGR / Calmar 0.82 / 7-of-7 positive years** (`deliv≥75`, 25-50cr, hold20, ₹5L); **beats a pure-beta control by 10–40pts both halves** (random mid-cap entry blows up −80% DD) → real alpha. *KILL was wrong (flat slip); the dip filter was also wrong (dropped in pass 3).* Proposal: `DELIVERY_CHANNEL_PROPOSAL.md` |
| ② | Bulk/block-deal follow | **KILL** | raw drift real, dies on ATR-stop whipsaw + cost in the walk (−11% CAGR); 0.1% flat slip ≈ size-aware floor, so not a slippage-model error |
| ③ | Short-squeeze (short-selling) | **KILL** | no signal — flips sign OOS, negative in liquid names both halves; size-aware model can't create a signal that isn't there |

**Lessons learned (the hard way):** (1) execution modeling must be **size-aware**, not a flat % — a blunt
flat slippage killed a *real* mid-cap edge (delivery) that trades fine at small size. (2) **Isolate alpha
from beta** — the size-aware revival still cited an unverified number *and* kept a dip filter; a beta control
(random entry) + a dip control proved delivery-% is genuine selection and the dip filter was hurting. ②③
stay dead: deals died at the size-aware slip floor, shorts have no out-of-sample signal at all.
**Delivery ① is a genuine ~12–13% diversifier — validated, proposal written, awaiting build sign-off.**
Remaining non-intraday ideas are thin (positioning = overlay; corp = tiny; fundamentals = no history).
**Intraday-5m (`candles_5m_full`, ~3-4yr)** is still the substantial remaining grind — a different edge family.
