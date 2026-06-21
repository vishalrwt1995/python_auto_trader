# GAP-FADE SHORT CHANNEL — Design & Build Plan

> Status: **SCOPE — awaiting sign-off (no code yet).** PAPER from day one (Rule 5).
> Created 2026-06-21. Sibling doc to `EVENT_CHANNEL_CORP_ACTION_PLAN.md` (same channel pattern).

## 1. What it is + the validated edge

Short an NSE **F&O** stock that **gaps up >5%** at the open; cover the same day at the
close. Pure intraday (MIS), no overnight risk. It is the system's **first *validated*
systematic short** — anti-correlated with the entire long book (swing + PEAD + corp), so
it earns most in the broad-market spikes that hurt everything else (diversification is the
strategic case; the absolute ₹ is modest).

**Validated** on 2010–2026 deep daily data (`candles_daily_deep.pkl`), restricted to the
211 current NSE F&O underlyings (reliably MIS-shortable), **price ≥ ₹30**, with a 3%
protective stop + realistic costs (0.27% MIS + 0.25% slippage + 0.15% stop-slippage):

| OOS 2018–2026 (F&O, gap>5%, price≥₹30, 3% stop) | value |
|---|---|
| Net / trade | **+0.58%** — robust to the price floor (≈ +0.57–0.66% at any floor) |
| Per ₹1L / yr @ **0.33×cap** (backtest sizing) | **+₹11.1k (+11.1%/yr)**, **6/9 OOS years positive** |
| Per ₹1L / yr @ **0.20×cap** (pilot sizing) | **~+₹6.7k (+6.7%/yr)** |
| Trades / yr | ~57 (~1.2/week) |

Carried by 2018/2020/2023/2024 (+₹17–30k @0.33); **2019/2025/2026 slightly negative**
(−₹0.9–1.6k each, all <₹2k). **Honest correction (2026-06-21):** the no-floor result
(+₹112.9k OOS, 9/9 positive) leaned on **penny F&O names** (IDEA/SUZLON/ADANIPOWER/YESBANK…
= 13% of trades at +1.05%/trade) that are **partly a historical-price artifact** (cheap in
2012–18, since re-priced — the forward-bias caveat) and carry squeeze/spread risk the cost
model understates. `PRICE_MIN=30` excludes them for forward honesty; the per-trade edge
survives (~+0.58%) but the year-consistency is **6/9, not 9/9**, and the ₹ is lower.

**Honest caveats (carried into live):**
- **Forward-biased F&O universe** — today's 211 names applied across 2010–26 (point-in-time
  membership unavailable); even OOS is mildly optimistic, 2010–15 most so. Anchor on recent
  years.
- **Daily-bar stop is slightly optimistic on fill** (squeezes can gap the stop; 0.15%
  stop-slippage added but real fast moves can exceed it).
- **Low frequency → lumpy.** ~1.5 trades/week; a live year can disappoint though all 9
  backtested positive.
- **Open-execution slippage** is the load-bearing cost assumption (0.25% RT) — must be
  watched live.
- **3% stop is a sensible default, lightly fit** to OOS smoothness (grid showed 3–8% all
  cluster ~+0.5–0.6%/trade).

## 2. Capital & risk (decided)

- **NEW ₹1L** — `CAPITAL_GAPFADE=100000`. Total system capital → ₹9L. Existing intraday
  channel **unchanged at ₹1L**.
- Own per-channel **daily breaker 3% loss / 6% profit** on ₹1L (reuse the Phase-C governor;
  `daily_loss_pct`/`daily_profit_pct` applied to `capital_gapfade`).
- **`channel="gap_fade"`** (book + breaker attribution), **`wl_type="gap_fade"`** (exit
  isolation — invisible to swing/PEAD/corp reconciliation, exactly like corp's
  `wl_type="corp_action"`).
- Sizing: notional **0.33 × capital per position**, **K=3 concurrent** slots (recycles
  daily; gap>5% averages <1/day so K rarely binds). Stop **3% above entry**.

## 3. Signal (fires once, just after the open)

At the first scan after 09:15 (target **09:16–09:20 IST**):
1. For each of the ~211 F&O underlyings: `gap = open / prev_close − 1`.
2. Keep `gap > 5%`, 20-day turnover ≥ ₹10cr, **not locked-limit** (skip if no intraday
   range yet / circuit), price floor.
3. Rank by gap (largest = highest edge), take top **K=3**.
4. F&O list sourced from the **Upstox instrument master** (`NSE_FO` `FUT` underlyings),
   refreshed periodically (membership changes ~quarterly).

## 4. Entry

- **MIS SELL bracket order**: market SELL + broker **buy-stop 3% above** entry. The bracket
  puts the stop **broker-side**, which sidesteps custom short-side FSM/ws-monitor logic.
  *(Build-time verification: confirm Upstox bracket supports a MIS SELL entry +
  above-entry stop; `order_service` already has a BRACKET path for non-delivery.)*
- Notional `0.33 × capital_gapfade`; qty = notional / open.

## 5. Exit

- **Protective stop**: the broker bracket's buy-stop, 3% above entry. Hard tail cap.
- **Cover at close**: scheduled square-off ~**15:15 IST** (`/jobs/gapfade-squareoff`) for any
  position not stopped — mirrors the backtest's close-cover.
- **No overnight risk** by construction (MIS, same-day).
- **Rule 8**: exit logic here is intraday → *if* any custom tick-monitoring is required
  beyond the broker bracket, the **ws-monitor service must be deployed** (cloudbuild.ws.yaml)
  and the FSM made short-aware. **Design intent: avoid this** by using the broker bracket +
  scheduled square-off, so no ws-monitor change is needed (verify in GF-4).

## 6. Architecture — where it plugs in (mirror PEAD/corp thin-service pattern)

- **`domain/gap_fade_signals.py`** (NEW, pure): gap calc, gates, ranking, fade/stop math.
- **`services/gap_fade_signal_service.py`** (NEW): open snapshot → ranked short candidates.
- **`services/gap_fade_trading_service.py`** (NEW): channel book gate + MIS bracket short
  entry; tags `channel="gap_fade"`, `wl_type="gap_fade"`.
- **`services/gap_fade_squareoff`** (NEW, thin): 15:15 cover of any open gap_fade position.
- **`settings.py`**: `capital_gapfade`, `gapfade_notional_cap_pct` (0.33),
  `gapfade_stop_pct` (0.03), `gapfade_max_positions` (3), `gapfade_min_gap` (0.05),
  `gapfade_min_turnover` (1e8) + env loaders. Default **0** (channel off until enabled).
- **`container.py`**: factories + `run_gap_fade_scan` / `run_gap_fade_squareoff`.
- **`web/api.py`**: `/jobs/gapfade-scan` + `/jobs/gapfade-squareoff`.
- **Scheduler**: `gapfade-scan-0916` + `gapfade-squareoff-1515` (Mon–Fri IST).
- **`order_service.py`**: MIS bracket SELL path (verify/extend for above-entry stop).
- **F&O universe loader**: fetch/cache the Upstox `NSE_FO` `FUT` underlyings.

## 7. Risk & safety

- **PAPER first**, sacred until explicit live direction (Rule 5). Channel default-off
  (`CAPITAL_GAPFADE=0`) until enabled with explicit authorization (the corp-flip precedent).
- **Squeeze tail** capped by the hard broker buy-stop (3%).
- **Locked-limit / circuit**: skip names with no intraday range; F&O names have wide/relaxing
  bands so they rarely lock — a reason F&O is the right filter.
- **F&O ban-period** does not block this: it bars new *F&O* positions, not **cash** MIS
  intraday shorts (squared off same day).
- **No overnight / SLB** dependence (intraday cash short).

## 8. Tests + fidelity (the load-bearing gate)

- **Fidelity replay**: drive `gap_fade_signals.py` over the deep daily pool and reproduce the
  backtest **+12.5%/yr OOS / 9-9 positive** within tolerance (report exact ₹ + any gap).
- Unit tests: gap calc, gates (gap/turnover/locked-limit), ranking, fade/stop math, sizing,
  channel book gate, breaker, square-off.
- Full blast-radius suite green (swing/intraday/PEAD/corp byte-identical — additive only).

## 9. Honest caveats (live ≠ backtest)

Repeating §1: forward-biased F&O list; daily-bar stop optimism; open-execution slippage is
the key live unknown; low-frequency lumpiness; **first live short → execution unproven**
(bracket-short fills, square-off timing, paper-fill realism all need live observation before
any scaling).

## 10. Build sequence (GF-1 … GF-8, mirrors the corp-action ship)

1. **GF-1** — pure `gap_fade_signals.py` + unit tests + **fidelity replay == backtest**.
2. **GF-2** — `gap_fade_signal_service` (open snapshot → candidates) + tests.
3. **GF-3** — `gap_fade_trading_service` (MIS bracket short entry) + channel registration
   (settings/portfolio_book) + tests.
4. **GF-4** — exit: broker-bracket stop + 15:15 square-off job; **prove no ws-monitor change
   needed** (Rule 8 check) + tests.
5. **GF-5** — `settings` + `container` + `web/api` routes + scheduler jobs.
6. **GF-6** — full blast-radius suite + fidelity green.
7. **GF-7** — PR + review (**no deploy**).
8. **GF-8** — deploy PAPER + verify (**explicit per-round authorization**, channel stays
   off until `CAPITAL_GAPFADE` flipped with sign-off).

---

### Open design questions for sign-off
1. **Cover timing — RESOLVED (2026-06-21).** Investigated on the **real 1m intraday path**
   (BigQuery, 2022–26, 475 F&O gap>5% events): time-of-day covers ≈ close (noise),
   profit-targets **worse**, and a properly-simulated (whipsaw-modeled) trailing buy-stop
   **+0.52% ≈ hold-to-close +0.465%**. The +1.69% "perfect cover at the low" ceiling is
   **unreachable** — fades are choppy so trails whipsaw out early. (An optimistic day-low
   trail *proxy* showed +1.15% but the honest sequential sim refuted it.) **Decision:
   hold-to-close 15:15 square-off, no target.** Optional: a 2% trailing buy-stop is
   marginally smoother (5/5 vs 4/5 yrs) at ~same return — not adopted (complexity > +0.07%/t).
2. **Stop = 3%** broker bracket (validated default) — confirm, vs naked (higher avg but
   un-runnable live) or wider.
3. **K=3 / notional 0.33×cap** — confirm sizing, or start smaller (e.g., 0.20×cap) for the
   live pilot.
