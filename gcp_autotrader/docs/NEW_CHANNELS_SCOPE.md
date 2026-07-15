# New-Channel Grind — Scope & Ranking (2026-07-14)

> Branch `research/new-channels-grind`. **Isolation (hard constraints):** (1) nothing touches
> running prod configs/implementation; (2) no existing-channel backtest is edited or perturbed;
> (3) every artifact here is a NEW additive file. New channels get brand-new scripts + a SEPARATE
> local data cache. BQ pulls are cost-confirmed, partition-pruned, local-once.

## Why new channels (the gap)

The productive book — **SWING (9.7%), CORE (~13%), MOMENTUM (14.8%)** — is entirely
**momentum-family** (all long trend/relative-strength). PEAD is the only non-momentum edge, and
it's small. So the highest-value new channels are **genuine diversifiers**: non-momentum edges
with low correlation to the existing book. Ranking weights that heavily.

## Grindable data (has real history)

| Dataset | Rows | Est. coverage | Powers |
|---|---|---|---|
| `nse_delivery_daily` | 2.86M | ~4 yr | delivery/accumulation |
| `candles_5m_full` | 165M | ~3–4 yr | intraday |
| `nse_bulk_deals` / `block_deals` | 191k / 10k | multi-yr | institutional footprints |
| `nse_corp_actions` | 42.6k | multi-yr | corp events (proper data) |
| `nse_participant_oi` / `fii_deriv` / `fo_pcr` | 11k / 18k / 6k | daily aggregates | positioning **overlay** |
| `fundamentals_ratios` / `financials` | 2.4k / 10k | **snapshot / ~few qtrs** | factors (data too shallow) |

Exact date ranges confirmed at pull time (cost-gated). **Not grindable yet:** news (scaffold,
no history), option greeks/IV (on-demand), `capture_index_positioning` (~28 days, accumulating).

## Ranking (edge × diversification × data-readiness × stock-only executability)

| # | Channel | Edge | Diversif. | Data | Exec | Verdict |
|---|---|---|---|---|---|---|
| **1** | **Delivery-accumulation** | High (documented IN-mkt edge) | **High** (microstructure, non-momentum) | High (~4yr) | High (stock-long, liquid) | ★★★ **grind first** |
| **2** | **Intraday-5m rebuild** | Med (thin, cost-sensitive) | Med (different horizon) | High (3–4yr 5m) | Med (MIS cost/slip) | ★★ strong (user priority) |
| 3 | Bulk/block-deal follow | Med (footprint drift) | High | Med (191k) | Med (post-deal drift) | ★★ candidate |
| — | Fundamental factor (value/quality) | High | **Highest** | **Low (snapshot only)** | High | **DEFER** — need point-in-time history |
| — | Corp-refetch (faithful) | Real but **rare** | High | High | Low (~2–3/yr) | LOW — known tiny ceiling |
| — | Positioning (OI/FII/PCR) | Med | High | Med | overlay-only | **OVERLAY**, not standalone |

## Recommendation

**Grind #1 = Delivery-accumulation.** Highest expected value: a well-documented Indian-market
microstructure edge (high delivery-% = real buyers taking delivery, not intraday churn →
accumulation → forward drift), it's the **strongest genuine diversifier** from the momentum-heavy
book, the data is ready (~4 yr), and it's cleanly executable stock-long in liquid names.

**Grind #2 = Intraday-5m rebuild** (your stated priority) — data is ready, but honest expectation
is modest: intraday edges are thin and cost/slippage-sensitive (the gapfade lesson). Tackle after
the more-tractable delivery edge.

**Defer:** fundamentals (data too shallow — a snapshot, not point-in-time), corp-refetch (ceiling
already known tiny), positioning (better as a filter/overlay on other channels than standalone).

## Per-channel grind loop (each candidate)

1. Cost-confirmed local data pull (partition-pruned, once) into the isolated cache.
2. Baseline entry/exit in a NEW script → IS/OOS (net/CAGR/Calmar/DD/trades-yr, full cost).
3. Feature/selection grind → engine-test survivors (both-halves rule, no curve-fit).
4. Full validation bar: survivorship + walk-forward + slippage stress + per-year.
5. Verdict keep/kill → if keep, additive prod-integration proposal (sign-off + PR + PAPER).
