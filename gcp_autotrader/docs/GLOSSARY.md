# Glossary — domain terms for the Auto Trading System

> If you see a term in code or docs you don't recognize, look here first.
> Last updated 2026-05-29.

---

## Channels (logical capital pools)

| Term | Meaning |
|---|---|
| **channel** | A logical bucket of capital + entry rules. Two channels: `swing` and `intraday`. Stored on each position as `position.channel`. Set at entry time. |
| **wl_type** | "Watchlist type" — same routing key as channel ("swing" or "intraday"). Position docs carry both; channel was added later (M4), wl_type predates it. Back-compat fallback: `channel = "swing" if wl_type == "swing" else "intraday"`. |
| **swing** | 1–10 day holds (CNC). 5 concurrent slots. Risk ₹1,500/trade. ₹1L allocation. ATR×2.5 SL, 2R target. |
| **intraday** | Same-day (MIS). 3 concurrent slots. Risk ₹250/trade. ₹1L allocation. ATR×1.5 SL, 1.25R target (2.0R for MEAN_REVERSION/VWAP_REVERSAL). |
| **per-channel limits** | Daily loss/profit halts evaluated *per channel* when `CAPITAL_SWING > 0 AND CAPITAL_INTRADAY > 0`. Defaults: 3% loss (₹3K of ₹1L) / 6% profit (₹6K). Bad swing day doesn't halt intraday and vice versa. Phase C v1 shipped 2026-05-28. |

---

## Regimes (brain output)

| Regime | When | Strategy effect |
|---|---|---|
| **TREND_UP** | Sustained uptrend (EMA50 > EMA200, positive trend score) | Favours MOMENTUM, PULLBACK; blocks BREAKOUT (needs VCP detection — not yet built), SHORT_BREAKDOWN |
| **TREND_DOWN** | Sustained downtrend | Favours SHORT_BREAKDOWN, SHORT_PULLBACK; blocks long entries |
| **RANGE** | Sideways, ADX < threshold | Favours MEAN_REVERSION, VWAP_REVERSAL; blocks trend setups |
| **CHOP** | Volatile sideways | Blocks most setups (worst regime) |
| **PANIC** | High VIX, breadth collapse | Halts trading (`risk_mode = LOCKDOWN`) |
| **RECOVERY** | Post-panic stabilization | Limited entries |
| **EARLY_TREND_UP** | Phase D — fast-trend signal flips bullish before slow trend score | Allows MOMENTUM/PULLBACK earlier than waiting for TREND_UP confirmation |
| **EARLY_TREND_DOWN** | Phase D — fast-trend bearish | Allows SHORT setups earlier |

See `src/autotrader/domain/regime_affinity.py` for the full `_HARD_BLOCKS` + `_AFFINITY` matrices.

---

## Brain scores

| Term | Meaning |
|---|---|
| **trend_score** | 0–100. Slow trend signal from EMA50/EMA200 + multi-day return. Used as the primary regime driver. Lags 10–15 sessions. |
| **tactical_trend_score** (tact) | 0–100. Fast trend signal from EMA20/EMA50 + 10-day return. Added in Phase D (2026-05-27). Catches regime transitions 5–10 days before `trend_score`. Drives `EARLY_TREND_UP/DOWN`. |
| **breadth_score** | 0–100. % of universe with positive returns. NIFTY breadth proxy. |
| **leadership_score** | 0–100. Are the leading stocks (top-quartile) outperforming? |
| **volatility_stress_score** | 0–100. VIX + ATR-expansion + range-expansion. >65 = elevated stress. |
| **data_quality_score** | 0–100. % of expected candles received. <60 = data issues. |
| **market_confidence** | Aggregate of the above, weighted. Drives `risk_mode`. |

---

## Strategies / setups

### Swing setups
- **PULLBACK** — buy dips in an uptrend (RSI oversold + trend filter)
- **BREAKOUT** — buy new highs (currently *hard-blocked* in TREND_UP, awaiting VCP / cup-handle detector)
- **MEAN_REVERSION** — buy oversold or sell overbought against a regime backdrop
- **MOMENTUM** — re-enabled 2026-05-27. Buy strength.
- **SHORT_BREAKDOWN** — short on bearish breakdown. Pre-10am block (R1) to avoid noisy opening 30min.
- **SHORT_PULLBACK** — short on weak retracements

### Intraday setups
- **OPEN_DRIVE** — 09:15–09:45. Continuation of strong open.
- **MORNING_FADE** — 09:30–10:30. Short gap-ups that popped above VWAP, fade back. Live-fixed 2026-05-27 (vwap-guard was blocking it).
- **VWAP_TREND** — 10:15–13:30. Trend continuation along VWAP.
- **VWAP_REVERSAL** — Counter-trend bounce off VWAP.
- **MEAN_REVERSION_INTRA** — 10:30–13:30. 5m RSI extremes + VWAP distance.
- **PHASE1_MOMENTUM / PHASE1_REVERSAL** — specific stock lists, narrow scope.
- **ORB / VWAP_BOUNCE / AFTERNOON_TREND** — NOT YET BUILT. Phase E redesign plan in `docs/PHASE_E_INTRADAY_PLAN.md`.

---

## Risk + position-sizing terms

| Term | Meaning |
|---|---|
| **R / R-multiple** | Risk units. 1R = max_loss. 2R = 2× target. "+0.5R MFE" = best price was 0.5× SL distance favourable. |
| **MFE / MAE** | Max Favourable Excursion / Max Adverse Excursion. How far the price went in your favour / against you before exit. Tracked live by `ws_monitor`. |
| **max_loss** | qty × |entry - SL| (gross, no brokerage). Stored on position. Used by capital-exhausted gate + PortfolioBook channel-budget. |
| **sl_dist** | |entry_price - sl_price|. Per-share SL distance. |
| **risk_per_trade** | Max ₹ lost if a single trade hits SL. `cfg.risk_per_trade` (intraday=₹250) or `cfg.swing_risk_per_trade` (₹1,500). |
| **channel_capital(ch)** | Returns swing/intraday channel allocation (₹1L each) when `CAPITAL_SWING/INTRADAY` env set, else falls back to total `CAPITAL`. Method on `StrategySettings`. |
| **position size cap** | swing: 20% of channel capital (₹20K @ ₹1L). intraday: 15% of channel capital (₹15K @ ₹1L). |
| **expected_edge_r** | Bayesian per-setup edge estimate (M3). Gated by `USE_EXPECTED_EDGE_R_V1`. |

---

## Gates / policy layers

The entry decision in `trading_service.py:run_scan_once` walks a layered `if/elif` chain. Earlier layers short-circuit later ones. Order:

1. **Daily limit gate** (line ~552) — per-channel since Phase C v1. Blocks `swing` if swing today_pnl ≤ -₹3K; same for intraday.
2. **Max trades day cap** (`max_trades_day=5`).
3. **PortfolioBook channel budget** (M4) — `channel.open_risk + new_risk ≤ channel.capital × pct_of_capital`. Currently 50/50 of ₹2L.
4. **PortfolioBook DD halts** — daily 3% / weekly 5% / monthly 8% of total capital (NOT yet per-channel; that's Phase C v2.2 work).
5. **Same-symbol dedup** (line ~1393) — block if (symbol, direction) already in `_open_symbol_dirs`.
6. **Swing max positions** (5) / intraday max (3).
7. **Strategy concentration cap** (`_MAX_SAME_STRATEGY=2`).
8. **Regime hard-blocks** (`regime_affinity.regime_hard_blocks_strategy`).
9. **VWAP guards** — block LONG below VWAP / SHORT above VWAP (MEAN_REVERSION + VWAP_REVERSAL + MORNING_FADE exempted).
10. **Capital-exhausted gate** — channel-scoped since Phase C v2.
11. **Strategy entry gates** (`check_swing_entry` / `check_strategy_entry`).
12. **PlaybookV1** (`USE_PLAYBOOK_V1=true`) — regime × edge × risk_mode.

If any layer fires, `policy_block_reason` is set + a `BLOCK` log is emitted with the specific reason. Useful for diagnostics: query BQ `scan_decisions` table.

---

## Exit logic

| Exit reason | When |
|---|---|
| **TARGET_HIT** | Price reaches target |
| **SL_HIT** | Price reaches SL |
| **MAX_HOLD** | Held > swing_max_hold_days (10 days) |
| **EOD_CLOSE** | Intraday position force-closed at 15:25 |
| **FLAT_TIMEOUT** | Intraday position not in profit after 120 min |
| **REGIME_FLIP_SOFT_EXIT** | R4 (2026-05-26) — brain flips to a regime that hard-blocks this setup AND position has MAE > 0.5R → soft close. Only swing positions ≤2 days old. |
| **V2_PARTIAL_EXIT_1 / EXIT_2** | Swing scale-out: 50% at 0.5R, rest holds (V2 exit logic shipped 2026-05-22) |
| **MANUAL** | User intervention |

---

## Brokerage / costs

| Term | Meaning |
|---|---|
| **brokerage_drag** | Annualized friction cost as % of capital. At ₹1L on Upstox doing 5/day intraday = ~68%/yr; that's why high-frequency intraday is dead at ₹1L. |
| **STT** | Securities Transaction Tax. Intraday: 0.025% sell-side. Delivery: 0.1% both sides. Statutory. |
| **DP charges** | Depository Participant charges. Delivery-sell only. Upstox: ₹20/scrip flat. |
| **CostConfig.upstox()** | Default cost model (`backtest/costs.py`). Round-trip on ₹20K: intraday ₹54 / swing ₹115. |
| **CostConfig.zerodha()** | Comparison model. Round-trip ₹20K: intraday ₹21 / swing ₹60. Used pre-2026-05-28 (caused inflated profitability estimates). |

---

## Feature flags (`settings.runtime.use_*_v1`)

| Flag | What it enables |
|---|---|
| `USE_PLAYBOOK_V1` | Regime × edge × risk_mode hard-blocks |
| `USE_PORTFOLIO_BOOK_V1` | M4 channel budgets + DD governors (currently ON since 2026-05-27 — was already ON before Phase C work began) |
| `USE_EXPECTED_EDGE_R_V1` | Bayesian per-setup edge scoring |
| `USE_EXIT_FSM_V1` | Exit state machine (M5) |
| `USE_OPTION_ANALYTICS_V1` | Options Greeks (not yet wired) |
| `USE_NEWS_SIGNALS_V1` | News integration (placeholder) |
| `USE_ATTRIBUTION_LOG_V1` | Per-trade attribution logging |

---

## GCP infrastructure quick map

| Component | Where |
|---|---|
| Cloud Run service | `autotrader` (project `grow-profit-machine`, region `asia-south1`) |
| Service URL | `https://autotrader-147177395303.asia-south1.run.app` |
| Firestore collections | `market_brain` (latest brain state), `positions` (open + closed), `config` (runtime overrides), `audit_log` |
| BigQuery dataset | `grow-profit-machine.autotrader` |
| Key BQ tables | `candles_5m` (5.89M bars), `candles_1d`, `market_brain_history`, `scan_decisions`, `trades`, `signals`, `audit_log` |
| GCS bucket | `grow-profit-machine-autotrader-data` |
| Cloud Scheduler | All jobs prefixed `autotrader-*` in `asia-south1` |

---

## Common abbreviations in code comments

| Abbrev | Meaning |
|---|---|
| `IST` | Indian Standard Time (UTC+5:30). All trading times are IST. |
| `LCD` | Last Completed Day (last trading day before today) |
| `MIS` | Margin Intraday Square-off (intraday product code) |
| `CNC` | Cash & Carry (delivery / swing product code) |
| `wl` | Watchlist |
| `ATR` | Average True Range (volatility measure) |
| `EOD` | End of Day |
| `DD` | Drawdown |
| `R` | Risk multiple (see above) |
| `cfg` | `self.settings.strategy` (StrategySettings instance) |
| `_dc_replace` | `dataclasses.replace` — for frozen dataclass mutation |
