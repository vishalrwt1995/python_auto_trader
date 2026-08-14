from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, default)).strip())
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(str(os.getenv(name, default)).strip())
    except Exception:
        return default


@dataclass(frozen=True)
class StrategySettings:
    capital: float = 50_000.0
    # Phase C (2026-05-28): per-channel capital allocation. When both non-zero,
    # PortfolioBookV1 routes each channel's risk gates, daily DD halts, and
    # position-size caps against its own allocation instead of the shared
    # `capital`. Both default 0 → falls back to shared `capital` (back-compat).
    # Typical config: CAPITAL_SWING=100000, CAPITAL_INTRADAY=100000, CAPITAL=200000.
    capital_swing: float = 0.0
    capital_intraday: float = 0.0
    capital_pead: float = 0.0          # EVENT/PEAD channel (Phase C, added 2026-06-19)
    capital_gapfade: float = 0.0       # GAP_FADE channel (Phase C, added 2026-06-21)
    capital_core: float = 0.0          # CORE momentum-hold channel (Phase C, added 2026-06-21)
    capital_momentum: float = 0.0      # Momentum x Low-Vol channel (monthly rebalance, added 2026-07-10)
    capital_delivery: float = 0.0      # Delivery-accumulation channel (daily CNC mid-caps, added 2026-07-14)
    capital_insider: float = 0.0       # Insider cluster-buy channel (daily CNC, added 2026-07-20)
    capital_pledge: float = 0.0        # Promoter pledge-release channel (daily CNC, added 2026-07-21)
    # Phase C (2026-05-28): per-channel daily loss/profit limits as a fraction
    # of channel capital. Used by the per-channel daily-limit gate in
    # trading_service. Default 3% loss / 6% profit (= 2x swing risk / 4x).
    # When 0, falls back to absolute max_daily_loss / daily_profit_target.
    daily_loss_pct: float = 0.03
    daily_profit_pct: float = 0.06

    # ── Forward-test epoch (fixed 2026-08-07; env FORWARD_TEST_START) ──────────────
    # THE canonical start of the honest PAPER forward test. The user's ground truth is
    # that the last wrong-logic trades executed 2026-07-24 (a Friday), so the first clean
    # session is Monday 2026-07-27. Everything before it is the pre-revamp system
    # (206 closed trades, −₹12,614) and must NOT be mixed into forward-test results.
    #
    # Attribution rule: a trade belongs to the forward test iff its **entry_ts >= this
    # date**. Entry-based, never exit-based — at the cutoff 34 old-logic positions were
    # still open (core 30 entered Jun-23..Jul-01, delivery 4 entered Jul-16..Jul-20) and
    # they close *inside* the window, so an exit-date rule would credit their P&L here.
    #
    # DO NOT move this date to flatter a result. Moving it forward discards real
    # forward evidence; moving it back re-imports old-logic losses. If it must change,
    # record why in PROJECT_KNOWLEDGE §8.
    forward_test_start: str = "2026-07-27"

    risk_per_trade: float = 125.0
    max_daily_loss: float = 300.0
    daily_profit_target: float = 375.0
    max_trades_day: int = 5
    max_positions: int = 3
    min_signal_score: int = 72
    ema_fast: int = 9
    ema_med: int = 21
    ema_slow: int = 50
    rsi_period: int = 14
    rsi_buy_min: float = 45.0
    rsi_buy_max: float = 65.0
    rsi_sell_min: float = 35.0
    rsi_sell_max: float = 55.0
    vol_mult: float = 1.5
    atr_sl_mult: float = 1.5
    # Batch 4.1 (2026-04-22): dropped 2.0 → 1.25. Post-mortem review of recent
    # trades showed winners routinely peaked at 1.2-1.5R MFE then faded; the
    # 2R target meant those winners tripped the trailing-stop post-target
    # logic instead of booking a clean TARGET_HIT, realizing less than the
    # plan. At 35% hit rate × 1.3R actual capture vs 65% × 1R loss, 2R was
    # NEGATIVE expectancy despite the headline R:R. 1.25R target should hit
    # more often (more trades resolve cleanly) and realized R closer to plan.
    # MEAN_REVERSION keeps a higher target (see rr_intraday_reversion) — fade
    # setups need meaningful excursion to be worth the counter-trend risk.
    rr_intraday: float = 1.25
    # Per-strategy R:R override: MEAN_REVERSION / VWAP_REVERSAL fades need
    # wider targets because the "snap back" on oversold names routinely does
    # 2-3R; a 1.25R target cuts them off right where the move is accelerating.
    rr_intraday_reversion: float = 2.0
    vix_safe_max: float = 20.0
    vix_trend_max: float = 15.0
    pcr_bull_min: float = 0.8
    pcr_bear_max: float = 1.2
    nifty_trend_pct: float = 0.3
    # Swing-specific settings
    swing_atr_sl_mult: float = 2.5
    swing_rr: float = 2.0
    # Audit 2026-05-16 (Batch D): bumped from 200 to 300. 122 sl_too_wide
    # rejects/week — daily ATR × 2.5 (swing SL mult) frequently exceeds
    # 200/qty for stocks priced > 1000. Bumping risk_per_trade widens the
    # qty math envelope without changing the SL distance per share.
    swing_risk_per_trade: float = 300.0
    swing_max_positions: int = 5
    # Compounding (2026-07-03): when > 0, swing risk/trade = this %% of ROLLING
    # equity (= CAPITAL_SWING base + all-time realized swing net_pnl) instead of
    # the flat swing_risk_per_trade. 0 = OFF (flat sizing, legacy behavior).
    swing_compound_pct: float = 0.0
    # Liquidity cap (2026-07-03): when > 0, cap each swing position at this %% of
    # the symbol's 60d median daily turnover (₹). Keeps order size in the
    # low-impact regime so backtest fills == prod fills. 0 = OFF (no cap).
    swing_liq_cap_pct: float = 0.0
    # 2026-06: 10 → 20. The backtest-validated daily 1R trailing exit
    # (domain/swing_exit) lets winners ride; a 20-bar max-hold gives trend trades
    # room while the trail caps give-back. Counted in TRADING days in
    # swing_reconciliation_service (holiday/weekend-proof via daily-bar count).
    swing_max_hold_days: int = 20
    # P1 (2026-04-22): dropped 75 → 70 after live observation that scorer-eligible
    # daily-uptrending names (WELCORP daily_strength=82, LLOYDSME=84, STLTECH=88)
    # cluster at adjusted_score 62–73 in RANGE/NORMAL regimes. Intraday uses a
    # risk-mode-tiered threshold (58–75) and adjusted_score post brain-haircut;
    # swing uses _affinity_score (pre-haircut) against this single threshold.
    # A 3–10 day swing trade's edge is the daily trend — over-filtering at 75
    # on intraday-composite scoring kills the sample size (see 2026-04-22:
    # 35 evaluations → 1 qualified at 76/75, 1-point margin).
    #
    # 2026-05-07 audit (live data 2026-04-23 → 2026-05-07, 305 swing scans):
    # Score distribution showed only 19% of swing scans scored ≥70 (57 of 305),
    # and just 4 actually qualified across 14 trading days — effectively zero
    # swing trades fired. The 65-69 band held 27 scans (9% of total) — the
    # natural sweet spot for the swing-on-intraday-formula gap. Lowered to 65
    # to unlock real swing trade volume; subsequent gates (volume, RSI zone,
    # daily-bias, sl_too_wide) still filter low-quality candidates. Track
    # qualified rate post-deploy: target 1-3 swing trades/day.
    # Audit 2026-05-16 (Batch D): 65 → 60. 1424 score_below_min rejects in
    # the past trading week — the score distribution peaks in the 58-67
    # band; dropping the bar to 60 captures ~30-40% of that cluster while
    # the strategy gates (check_swing_entry: daily trend, ADX, RSI band,
    # supertrend) still filter the lowest-quality candidates.
    #
    # Alpha-finder audit 2026-05-21: BACKTEST suggests 60 → 45 would
    # capture +₹12k of alpha (40-49 band has +₹6,445 / 48.5% WR; 50-59
    # band has +₹6,144 / 50.8% WR). NOT applied to default to keep
    # production-replica validation accurate; deploy via env var override
    # `SWING_MIN_SIGNAL_SCORE=45` after paper-mode validation.
    swing_min_signal_score: int = 60
    # Batch 2.1 (2026-04-22): re-entry cooldown. When a position closes
    # (SL hit, target hit, or timeout), the scanner should NOT immediately
    # re-stage the same symbol on the next 3-min cycle. The watchlist will
    # naturally re-score that name as a strong setup (price just moved
    # through SL/target), and without a cooldown the bot would enter the
    # same trade again — compounding a losing thesis. Empirically 04-16
    # showed multiple symbols churned 2-3 times in under 30 min. 30-min
    # default chosen to be > 1 intraday-candle (15m) so the next signal
    # comes from a fresh candle cycle, not the SL breakout bar.
    reentry_cooldown_minutes: int = 30
    # P0-2 (2026-04-22): strategy kill-switch. Strategies listed here are
    # stripped from `allowed_strategies` regardless of regime. Used to disable
    # known-bad strategies surfaced by live P&L analysis.
    # Current blocklist:
    #   VWAP_REVERSAL — 13 trades over 30d, 23% win-rate, -0.61% avg P&L,
    #     12/13 closed at EOD never reaching target or SL. Negative expectancy.
    # Re-enable only after backtest or replay proves the strategy has edge.
    disabled_strategies: tuple[str, ...] = ("VWAP_REVERSAL",)
    # Batch 7 (2026-04-23): paper-trade slippage modeling. Paper fills used to
    # assume LTP=fill-price with zero cost, but live execution pays bid-ask +
    # impact cost. Un-modelled slippage flattered paper P&L vs live by roughly
    # 0.15-0.25% per round-trip (measured on 2026-02 to 2026-03 trade ledger
    # comparing paper-tagged vs live-tagged same-setup trades). These two
    # percentages shift paper fills adversely so paper P&L tracks live.
    # Entry slippage: MARKET order fills through the spread + momentum kick.
    # SL slippage: market-order exit triggered mid-bar, fills further through
    # the L2 book when multiple traders hit the same level.
    # Target slippage is zero — target orders are LIMIT, so fills happen AT
    # the price or not at all.
    paper_entry_slippage_pct: float = 0.0010   # 0.10%
    paper_sl_slippage_pct: float = 0.0020      # 0.20%

    # ── EVENT/PEAD channel (added 2026-06-19) ─────────────────────────────────
    # Post-earnings-announcement-drift channel — own capital (capital_pead),
    # separate daily breakers, NSE event-calendar signal, NIFTY-50 −5% market gate.
    # All default to the validated config; absolute risk is set per-deploy (1.5% of
    # capital_pead = Rs3,000 at Rs2L). The daily loss/profit breaker reuses
    # daily_loss_pct / daily_profit_pct (3%/6%) applied to capital_pead.
    pead_risk_per_trade: float = 0.0           # Rs/trade (PEAD_RISK_PER_TRADE; 1.5% of cap)
    pead_max_positions: int = 5                # surprise-ranked 5-slot book
    pead_max_hold_days: int = 60               # PEAD drift horizon (grind v2 40->60; vs swing 20)
    pead_atr_sl_mult: float = 2.5              # stop = ATR14 × this
    pead_notional_cap_pct: float = 0.20        # per-position notional cap (× capital_pead)
    pead_activate_r: float = 1.75              # arm the 1R trail at +1.75R (matches swing)
    pead_trail_r: float = 1.0                  # trail distance in R once armed
    # Market-state gate on NIFTY-50 252-day drawdown (validated −0.05). Set very
    # negative (e.g. −1.0 via PEAD_MARKET_DD_GATE) to run the documented NO-GATE
    # variant (higher lifetime total, trades through corrections — PROJECT_KNOWLEDGE §8).
    pead_market_dd_gate: float = -0.05
    # Run-up FLOOR (2026-07-09 grind): require pre-event run-up >= this. Drops falling-knife
    # reactions (downtrending names) — validated edge (Calmar 0.18->0.68, maxDD -28%->-11%,
    # both halves + survivorship-robust). Default 0.0 (floor on); PEAD_MIN_RUNUP=-1.0 disables.
    pead_min_runup: float = 0.0
    # Corp-action (bonus/split) sub-strategy of the EVENT/PEAD channel (added 2026-06-20).
    # SHARES the PEAD capital pool + 5-slot cap + daily breaker (both tag channel="pead");
    # corp is sub-capped at corp_max_positions concurrent. Own meeting-day exit
    # (wl_type="corp_action"). Validated look-ahead-free +1.54% net/event, robust IS+OOS.
    # Set CORP_MAX_POSITIONS=0 to disable the sub-strategy entirely. PAPER.
    corp_max_positions: int = 0                # 0 = disabled; 2 = enable (cap of the shared 5)
    corp_notional_cap_pct: float = 0.20        # per-position notional (× channel capital), matches backtest
    corp_protective_stop_pct: float = 0.15     # wide disaster backstop only (backtest holds to meeting, no stop)
    # GAP_FADE channel (added 2026-06-21) — intraday SHORT of F&O >5% gap-ups, cover at the
    # 15:15 squareoff, 3% protective buy-stop ABOVE entry. OWN channel (capital_gapfade), own
    # slot cap + 3%/6% breaker; tagged channel="gap_fade"/wl_type="gap_fade" (isolated from
    # swing/pead/corp). The system's first validated systematic short. Validated OOS
    # +0.58%/trade, ~+6.7%/yr @0.20 notional, 6/9 years. Set GAPFADE_MAX_POSITIONS=0 to
    # disable. Intraday (MIS) — exit via the side-aware FSM (short SL + EOD cover). PAPER.
    gapfade_max_positions: int = 0             # 0 = disabled; 3 = enable (concurrent same-day shorts)
    gapfade_notional_cap_pct: float = 0.20     # per-position notional (× channel capital), pilot
    gapfade_stop_pct: float = 0.03             # protective buy-stop this far ABOVE entry (short)
    # CORE channel (added 2026-06-21) — large-cap top-30 momentum+low-vol blend, quarterly
    # buy-and-HOLD, long-only CNC. OWN channel (capital_core); pure beta engine (~10% real /
    # -35-40% DD — sized to tolerance). Tagged channel="core"/wl_type="core" (held overnight,
    # never EOD-squared — ws_monitor _OVERNIGHT_SL_ONLY_WL). STOCK-ONLY. Set CORE_ENABLED=true
    # + CAPITAL_CORE to turn on. PAPER.
    core_enabled: bool = False                 # master on/off for the CORE channel
    core_notional_cap_pct: float = 1.0         # CORE deploys its full channel capital (equal-weight top-30)
    # 2026-07-08: size CORE off current NAV (reinvest gains) instead of the FIXED channel_capital,
    # which left ~30% idle cash. Backtest: ~9.5% -> ~13% CAGR, -35% DD, beats Nifty on all axes,
    # OOS-robust + survives 3x cost (see memory project_core_channel_grind). Compounds the channel.
    # Env kill-switch CORE_COMPOUND_SIZING=false reverts to the prior fixed-capital sizing.
    core_compound_sizing: bool = True

    # 2026-07-09: when true, build_watchlist writes SWING rows only (no intraday rows) to
    # watchlist/latest. Used to halt the intraday channel cleanly — the intraday scan jobs
    # are paused AND the dashboard watchlist stops showing inert intraday names. Default off
    # (no behavior change). Env WATCHLIST_SWING_ONLY=true. Reversible; swing rows unaffected.
    watchlist_swing_only: bool = False

    # Momentum x Low-Vol channel (2026-07-10): monthly-rebalanced top-20 cross-sectional
    # momentum + low-vol blend (>=Rs10cr, buffer x1.5, Nifty-100DMA regime overlay), buy-and-HOLD
    # CNC, own channel (never EOD-squared). Validated ~14% CAGR / -16% DD / Calmar ~0.85, low-corr
    # to CORE (scripts/redesign/factor_*.py). Set MOMENTUM_ENABLED=true + CAPITAL_MOMENTUM. PAPER,
    # STOCK-ONLY. Compounds off current NAV (MOMENTUM_COMPOUND_SIZING=false reverts to fixed).
    momentum_enabled: bool = False
    momentum_compound_sizing: bool = True

    # Delivery-accumulation channel (2026-07-14): daily CNC buy-hold of 25-50cr mid-cap STOCKS
    # (ETFs excluded) showing NSE delivery-% >= 75 (real accumulation), hold ~20d. Own channel
    # (capital_delivery), separate 3%/6% breaker; tagged channel/wl_type="delivery" (held overnight,
    # never EOD-squared — ws_monitor _OVERNIGHT_SL_ONLY_WL). Signal from a daily sec_bhavdata_full
    # ingest -> BQ nse_delivery_daily. Reuses swing_exit trail + a daily reconciliation (no tick path).
    # Validated ~11.8% CAGR / Calmar 0.86 / 6-of-7 yrs, beats a pure-beta control (real stock alpha,
    # not ETF-beta). Set CAPITAL_DELIVERY>0 to enable. PAPER, STOCK-ONLY. (docs/DELIVERY_CHANNEL_PROPOSAL.md)
    delivery_risk_per_trade: float = 0.0        # Rs/trade (DELIVERY_RISK_PER_TRADE; 1.5% of cap)
    delivery_max_positions: int = 5             # delivery-%-ranked 5-slot book
    delivery_max_hold_days: int = 20            # hold horizon (validated plateau 15-22)
    delivery_atr_sl_mult: float = 2.5           # stop = ATR14 × this
    delivery_notional_cap_pct: float = 0.20     # per-position notional cap (× capital_delivery)
    delivery_activate_r: float = 1.75           # arm the 1R trail at +1.75R (matches swing/pead)
    delivery_trail_r: float = 1.0               # trail distance in R once armed
    delivery_deliv_min: float = 75.0            # delivery-% floor (validated plateau 70-75)
    delivery_turnover_min_cr: float = 25.0      # 20d-mean turnover band low (crore)
    delivery_turnover_max_cr: float = 50.0      # 20d-mean turnover band high (crore)

    # Insider cluster-buy channel (2026-07-20, GOD-MODE validated: +23% CAGR / -12.5% DD /
    # Calmar 1.84, IS 2.85 / OOS 1.75). Signal: >=2 informed open-market buys (promoter/director/
    # KMP/relative), each >=Rs5L, same symbol+day; DOUBLE MACRO GATE b200>50 AND Nifty>100DMA;
    # fixed 90d hold (NO trail). Set CAPITAL_INSIDER>0 to enable. PAPER, STOCK-ONLY.
    insider_risk_per_trade: float = 0.0         # Rs/trade (INSIDER_RISK_PER_TRADE; 1.5% of cap)
    insider_max_positions: int = 10             # cluster-strength-ranked 10-slot book
    insider_max_hold_days: int = 90             # FIXED hold horizon (validated plateau 60-90)
    insider_atr_sl_mult: float = 2.5            # protective disaster stop = ATR14 × this (no trail)
    insider_notional_cap_pct: float = 0.10      # per-position notional cap (× capital_insider; =1/slots)
    insider_min_buyers: int = 2                 # cluster threshold (>=2 informed buy legs/day)
    insider_min_leg_value: float = 500000.0     # each disclosure leg >= Rs 5 lakh
    insider_turnover_min_cr: float = 10.0       # 20d-mean turnover floor (crore; no upper cap)
    insider_b200_min: float = 50.0              # breadth macro-gate floor (brain breadth_ema200_pct)
    # PLEDGE (2026-07-21): promoter pledge-REVOKE (deleveraging = bullish) reusing nse_insider_daily;
    # px>200DMA (falling-knife filter) + DOUBLE MACRO GATE b200>50 AND Nifty>100DMA; fixed 60d hold
    # (NO trail). Set CAPITAL_PLEDGE>0 to enable. PAPER, STOCK-ONLY. See domain/pledge_signals.
    pledge_risk_per_trade: float = 0.0          # Rs/trade (PLEDGE_RISK_PER_TRADE; 1.5% of cap)
    pledge_max_positions: int = 10              # liquidity-ranked 10-slot book (cap10% => no-leverage)
    pledge_max_hold_days: int = 60              # FIXED hold horizon (validated robust-central)
    pledge_atr_sl_mult: float = 2.0             # protective disaster stop = ATR14 × this (tighter; no trail)
    pledge_notional_cap_pct: float = 0.10       # per-position notional cap (× capital_pledge; diversified)
    pledge_turnover_min_cr: float = 25.0        # 20d-mean turnover floor (crore; liquid/fillable)
    pledge_b200_min: float = 50.0               # breadth macro-gate floor (brain breadth_ema200_pct)

    def channel_capital(self, channel: str) -> float:
        """Return capital allocated to a channel (Phase C 2026-05-28).

        When `capital_swing` / `capital_intraday` are non-zero, each channel
        operates on its own logical capital pool — used by PortfolioBookV1
        DD math, position-size caps in `risk.py`, and the capital-exhausted
        gate. When unset (0), falls back to the shared `capital` field for
        backward compatibility with single-pool deployments.

        Unknown channel names also fall back to shared capital (fail-open
        on routing — the channel gate itself handles unknown channels).
        """
        ch = str(channel or "").strip().lower()
        if ch == "swing" and self.capital_swing > 0:
            return self.capital_swing
        if ch == "intraday" and self.capital_intraday > 0:
            return self.capital_intraday
        if ch == "pead" and self.capital_pead > 0:
            return self.capital_pead
        if ch == "gap_fade" and self.capital_gapfade > 0:
            return self.capital_gapfade
        if ch == "core" and self.capital_core > 0:
            return self.capital_core
        if ch == "momentum" and self.capital_momentum > 0:
            return self.capital_momentum
        if ch == "delivery" and self.capital_delivery > 0:
            return self.capital_delivery
        if ch == "insider" and self.capital_insider > 0:
            return self.capital_insider
        if ch == "pledge" and self.capital_pledge > 0:
            return self.capital_pledge
        return self.capital

    # Explicit per-channel allocation map — reporting only.
    _CHANNEL_CAPITAL_FIELDS = {
        "swing": "capital_swing", "intraday": "capital_intraday", "pead": "capital_pead",
        "gap_fade": "capital_gapfade", "core": "capital_core", "momentum": "capital_momentum",
        "delivery": "capital_delivery", "insider": "capital_insider", "pledge": "capital_pledge",
    }

    def channel_capital_allocated(self, channel: str) -> float:
        """Explicit per-channel allocation; **0 when unfunded**.

        Unlike `channel_capital()`, this does NOT fall back to the shared `capital`
        pool. That fallback is deliberate for the trading path (single-pool deploys,
        risk.py sizing, PortfolioBook DD) and must stay — but it makes REPORTING lie:
        a killed channel with CAPITAL_GAPFADE=0 was inheriting the Rs6L global pool, so
        the cockpit showed Rs25L allocated instead of the real Rs19L, credited Rs6L to
        gap_fade, computed breaker limits off phantom capital, and (because `enabled`
        is `capital > 0`) badged a KILLED channel as ACTIVE. Use this for any display
        or attribution; use `channel_capital()` for sizing/risk.
        """
        f = self._CHANNEL_CAPITAL_FIELDS.get(str(channel or "").strip().lower())
        return float(getattr(self, f, 0.0) or 0.0) if f else 0.0


@dataclass(frozen=True)
class UpstoxSettings:
    api_v2_host: str
    api_v3_host: str
    client_id_secret_name: str
    client_secret_secret_name: str
    access_token_secret_name: str
    access_token_expiry_secret_name: str
    redirect_uri: str = ""
    auth_code_secret_name: str = ""
    # Long-lived Upstox Analytics token (1-year, read-only scope: historical
    # candles, LTP, option chain, market holidays). When set, all read-side
    # API calls route through this token and survive daily 03:30 IST rotation
    # of `access_token_secret_name`. Order placement / portfolio / funds
    # endpoints always use the daily access token regardless.
    analytics_token_secret_name: str = ""
    notifier_shared_secret: str = ""
    instruments_complete_url: str = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
    requests_per_second: int = 50
    max_per_minute: int = 500
    max_per_30min: int = 2000
    max_retries: int = 4
    nifty50_instrument_key: str = "NSE_INDEX|Nifty 50"
    india_vix_instrument_key: str = "NSE_INDEX|India VIX"
    pcr_underlying_instrument_key: str = "NSE_INDEX|Nifty 50"
    pcr_expiry_date: str = ""


@dataclass(frozen=True)
class GcpSettings:
    project_id: str
    region: str
    bucket_name: str
    spreadsheet_id: str = ""
    firestore_database: str = "(default)"
    bq_dataset: str = "autotrader"
    pubsub_topic_positions: str = "position-events"
    pubsub_topic_signals: str = "trade-signals"
    pubsub_topic_regime: str = "regime-events"


@dataclass(frozen=True)
class RuntimeSettings:
    paper_trade: bool
    job_trigger_token: str
    log_level: str
    timezone_name: str = "Asia/Kolkata"
    # ── Redesign feature flags (default OFF — flip to opt in) ──
    # M1: 5-state exit FSM (INITIAL/CONFIRMED/RUNNER/LOSING/TERMINAL).
    # When False, the legacy exit precedence in ws_monitor runs.
    use_exit_fsm_v1: bool = False
    # M2: Playbook hard-block layer + Edge registry. When False, the legacy
    # scorer decides entries directly.
    use_playbook_v1: bool = False
    # M3: expected_edge_R scoring (backtest-derived R priors). When False,
    # the legacy signal_score drives entry ranking.
    use_expected_edge_r_v1: bool = False
    # M4: PortfolioBook channel budgets + DD governors. When False, the
    # legacy max_positions / risk_per_trade gates apply.
    use_portfolio_book_v1: bool = False
    # M5: Upstox P0 expansion (option analytics poll, news signal ingest,
    # portfolio-stream WS). Independently flagged so each primitive can
    # roll out separately during the canary.
    use_option_analytics_v1: bool = False
    use_news_signals_v1: bool = False
    use_portfolio_stream_v1: bool = False
    # M6: Per-trade AttributionLog row + daily-metrics rollup. When False,
    # the close-position path only writes the legacy `trades` row; the
    # `attribution` table stays empty and the weekly review script no-ops.
    use_attribution_log_v1: bool = False


@dataclass(frozen=True)
class RegimeThresholds:
    """Market-Brain regime classification thresholds.

    Defaults match the magic numbers previously hard-coded in
    `MarketBrainService._map_regime` / `_map_risk_mode` (PR-1, 2026-04-20).
    Externalising them lets us tune without redeploying code, and the
    table-driven regime tests lock behaviour to these defaults so any
    env override is an explicit, reviewable change.
    """
    # PANIC entry
    panic_stress_min: float = 82.0
    panic_breadth_max: float = 12.0
    panic_dq_max: float = 30.0
    # 2026-07-08: suppress the data-quality PANIC during the market-open warmup
    # (PREMARKET/POST_OPEN), where a low data_quality just means intraday bars
    # haven't accumulated yet — not a broken pipeline. A genuine mid-session
    # outage still trips dq-PANIC in the LIVE phase; vol/breadth PANIC fire in
    # every phase. Prevents the daily ~09:20 false PANIC that (via the Phase-2
    # force-RECOVERY hold) locked the regime in RECOVERY indefinitely.
    panic_dq_warmup_suppress: bool = True
    # TREND_UP entry (standard)
    trend_up_trend_min: float = 70.0
    trend_up_breadth_min: float = 62.0
    trend_up_leadership_min: float = 56.0
    trend_up_stress_max: float = 48.0
    # TREND_UP entry (high-breadth alternative)
    trend_up_hi_breadth_min: float = 80.0
    trend_up_hi_leadership_min: float = 60.0
    trend_up_hi_stress_max: float = 48.0
    # RANGE_ROTATING entry (audit 2026-05-15, Layer 4 Option A).
    # Captures days when NIFTY itself ranges (so trend_score stays low)
    # but mid-caps/sectors are trending — breadth + leadership clear a
    # mid-range bar while index trend doesn't. Uses TREND_UP affinity
    # multipliers so VWAP_TREND / PULLBACK / MOMENTUM aren't haircut
    # the way they are in plain RANGE. Stress ceiling kept identical
    # to TREND_UP's so PANIC always takes precedence.
    range_rotating_breadth_min: float = 65.0
    range_rotating_leadership_min: float = 55.0
    range_rotating_stress_max: float = 55.0
    # TREND_DOWN entry
    trend_down_trend_max: float = 36.0
    trend_down_breadth_max: float = 40.0
    trend_down_leadership_max: float = 45.0
    # RECOVERY entry
    recovery_trend_min: float = 40.0
    recovery_breadth_min: float = 35.0
    recovery_leadership_min: float = 40.0
    # PANIC exit guard (stay-in-PANIC conditions)
    panic_exit_stress_above: float = 65.0
    panic_exit_breadth_below: float = 22.0
    # TREND_UP hysteresis (stay-in)
    trend_up_hold_trend_min: float = 60.0
    trend_up_hold_breadth_min: float = 55.0
    trend_up_hold_leadership_min: float = 50.0
    # TREND_UP hysteresis (entry after absence)
    trend_up_reenter_trend_min: float = 74.0
    trend_up_reenter_breadth_min: float = 66.0
    trend_up_reenter_leadership_min: float = 58.0
    trend_up_reenter_hi_breadth_min: float = 82.0
    trend_up_reenter_hi_leadership_min: float = 62.0
    trend_up_reenter_hi_stress_max: float = 45.0
    # General transition damper (sub-threshold age in seconds)
    transition_min_age_sec: float = 240.0
    # Risk mode thresholds
    lockdown_stress_min: float = 85.0
    lockdown_dq_max: float = 35.0
    defensive_stress_min: float = 65.0
    defensive_dq_max: float = 55.0
    aggressive_appetite_min: float = 66.0
    aggressive_stress_max: float = 50.0
    aggressive_dq_min: float = 65.0
    # Signal-staleness decay (PR-1)
    signal_fresh_max_sec: float = 120.0     # < this → 0 penalty
    signal_stale_full_sec: float = 900.0    # > this → full penalty (40 pts)
    signal_max_penalty: float = 40.0
    # Pubsub emission cadence (PR-1)
    pubsub_heartbeat_sec: float = 300.0     # emit even without transition after this long


@dataclass(frozen=True)
class AppSettings:
    gcp: GcpSettings
    upstox: UpstoxSettings
    runtime: RuntimeSettings
    strategy: StrategySettings
    regime_thresholds: RegimeThresholds = RegimeThresholds()

    @staticmethod
    def from_env() -> "AppSettings":
        strategy = StrategySettings(
            capital=_env_float("CAPITAL", 50000),
            capital_swing=_env_float("CAPITAL_SWING", 0.0),
            capital_intraday=_env_float("CAPITAL_INTRADAY", 0.0),
            capital_pead=_env_float("CAPITAL_PEAD", 0.0),
            capital_gapfade=_env_float("CAPITAL_GAPFADE", 0.0),
            capital_core=_env_float("CAPITAL_CORE", 0.0),
            capital_momentum=_env_float("CAPITAL_MOMENTUM", 0.0),
            capital_delivery=_env_float("CAPITAL_DELIVERY", 0.0),
            capital_insider=_env_float("CAPITAL_INSIDER", 0.0),
            capital_pledge=_env_float("CAPITAL_PLEDGE", 0.0),
            daily_loss_pct=_env_float("DAILY_LOSS_PCT", 0.03),
            daily_profit_pct=_env_float("DAILY_PROFIT_PCT", 0.06),
            forward_test_start=_env("FORWARD_TEST_START", "2026-07-27"),
            risk_per_trade=_env_float("RISK_PER_TRADE", 125),
            max_daily_loss=_env_float("MAX_DAILY_LOSS", 300),
            daily_profit_target=_env_float("DAILY_PROFIT_TARGET", 375),
            max_trades_day=_env_int("MAX_TRADES_DAY", 5),
            max_positions=_env_int("MAX_POSITIONS", 3),
            min_signal_score=_env_int("MIN_SIGNAL_SCORE", 72),
            ema_fast=_env_int("EMA_FAST", 9),
            ema_med=_env_int("EMA_MED", 21),
            ema_slow=_env_int("EMA_SLOW", 50),
            rsi_period=_env_int("RSI_PERIOD", 14),
            rsi_buy_min=_env_float("RSI_BUY_MIN", 45),
            rsi_buy_max=_env_float("RSI_BUY_MAX", 65),
            rsi_sell_min=_env_float("RSI_SELL_MIN", 35),
            rsi_sell_max=_env_float("RSI_SELL_MAX", 55),
            vol_mult=_env_float("VOL_MULT", 1.5),
            atr_sl_mult=_env_float("ATR_SL_MULT", 1.5),
            # Batch 4.1 (2026-04-22): default aligned to dataclass (1.25)
            rr_intraday=_env_float("RR_INTRADAY", 1.25),
            rr_intraday_reversion=_env_float("RR_INTRADAY_REVERSION", 2.0),
            vix_safe_max=_env_float("VIX_SAFE_MAX", 20),
            vix_trend_max=_env_float("VIX_TREND_MAX", 15),
            pcr_bull_min=_env_float("PCR_BULL_MIN", 0.8),
            pcr_bear_max=_env_float("PCR_BEAR_MAX", 1.2),
            nifty_trend_pct=_env_float("NIFTY_TREND_PCT", 0.3),
            swing_atr_sl_mult=_env_float("SWING_ATR_SL_MULT", 2.5),
            swing_rr=_env_float("SWING_RR", 2.0),
            swing_risk_per_trade=_env_float("SWING_RISK_PER_TRADE", 300),
            swing_compound_pct=_env_float("SWING_COMPOUND_PCT", 0.0),
            swing_liq_cap_pct=_env_float("SWING_LIQ_CAP_PCT", 0.0),
            swing_max_positions=_env_int("SWING_MAX_POSITIONS", 5),
            swing_max_hold_days=_env_int("SWING_MAX_HOLD_DAYS", 20),
            # Batch 1.3 (2026-04-22): default aligned to dataclass (70). Prior
            # divergence (dataclass=70, env default=75) meant production — which
            # constructs StrategySettings via from_env() — silently used the OLD
            # pre-P1 threshold while unit tests constructing StrategySettings()
            # directly saw the P1 value. The P1 swing-threshold calibration only
            # takes effect because no SWING_MIN_SIGNAL_SCORE env var is set in
            # Cloud Run today, so from_env's default must be authoritative.
            swing_min_signal_score=_env_int("SWING_MIN_SIGNAL_SCORE", 60),
            reentry_cooldown_minutes=_env_int("REENTRY_COOLDOWN_MINUTES", 30),
            paper_entry_slippage_pct=_env_float("PAPER_ENTRY_SLIPPAGE_PCT", 0.0010),
            paper_sl_slippage_pct=_env_float("PAPER_SL_SLIPPAGE_PCT", 0.0020),
            # EVENT/PEAD channel (2026-06-19)
            pead_risk_per_trade=_env_float("PEAD_RISK_PER_TRADE", 0.0),
            pead_max_positions=_env_int("PEAD_MAX_POSITIONS", 5),
            pead_max_hold_days=_env_int("PEAD_MAX_HOLD_DAYS", 60),
            pead_atr_sl_mult=_env_float("PEAD_ATR_SL_MULT", 2.5),
            pead_notional_cap_pct=_env_float("PEAD_NOTIONAL_CAP_PCT", 0.20),
            pead_activate_r=_env_float("PEAD_ACTIVATE_R", 1.75),
            pead_trail_r=_env_float("PEAD_TRAIL_R", 1.0),
            pead_market_dd_gate=_env_float("PEAD_MARKET_DD_GATE", -0.05),
            pead_min_runup=_env_float("PEAD_MIN_RUNUP", 0.0),
            # Corp-action (bonus/split) sub-strategy of the EVENT/PEAD channel (2026-06-20)
            corp_max_positions=_env_int("CORP_MAX_POSITIONS", 0),       # 0 = disabled until enabled
            corp_notional_cap_pct=_env_float("CORP_NOTIONAL_CAP_PCT", 0.20),
            corp_protective_stop_pct=_env_float("CORP_PROTECTIVE_STOP_PCT", 0.15),
            gapfade_max_positions=_env_int("GAPFADE_MAX_POSITIONS", 0),   # 0 = disabled until enabled
            gapfade_notional_cap_pct=_env_float("GAPFADE_NOTIONAL_CAP_PCT", 0.20),
            gapfade_stop_pct=_env_float("GAPFADE_STOP_PCT", 0.03),
            core_enabled=_env_bool("CORE_ENABLED", False),
            core_notional_cap_pct=_env_float("CORE_NOTIONAL_CAP_PCT", 1.0),
            core_compound_sizing=_env_bool("CORE_COMPOUND_SIZING", True),
            watchlist_swing_only=_env_bool("WATCHLIST_SWING_ONLY", False),
            momentum_enabled=_env_bool("MOMENTUM_ENABLED", False),
            momentum_compound_sizing=_env_bool("MOMENTUM_COMPOUND_SIZING", True),
            # Delivery-accumulation channel (2026-07-14)
            delivery_risk_per_trade=_env_float("DELIVERY_RISK_PER_TRADE", 0.0),
            delivery_max_positions=_env_int("DELIVERY_MAX_POSITIONS", 5),
            delivery_max_hold_days=_env_int("DELIVERY_MAX_HOLD_DAYS", 20),
            delivery_atr_sl_mult=_env_float("DELIVERY_ATR_SL_MULT", 2.5),
            delivery_notional_cap_pct=_env_float("DELIVERY_NOTIONAL_CAP_PCT", 0.20),
            delivery_activate_r=_env_float("DELIVERY_ACTIVATE_R", 1.75),
            delivery_trail_r=_env_float("DELIVERY_TRAIL_R", 1.0),
            delivery_deliv_min=_env_float("DELIVERY_DELIV_MIN", 75.0),
            delivery_turnover_min_cr=_env_float("DELIVERY_TURNOVER_MIN_CR", 25.0),
            delivery_turnover_max_cr=_env_float("DELIVERY_TURNOVER_MAX_CR", 50.0),
            # Insider cluster-buy channel (2026-07-20)
            insider_risk_per_trade=_env_float("INSIDER_RISK_PER_TRADE", 0.0),
            insider_max_positions=_env_int("INSIDER_MAX_POSITIONS", 10),
            insider_max_hold_days=_env_int("INSIDER_MAX_HOLD_DAYS", 90),
            insider_atr_sl_mult=_env_float("INSIDER_ATR_SL_MULT", 2.5),
            insider_notional_cap_pct=_env_float("INSIDER_NOTIONAL_CAP_PCT", 0.10),
            insider_min_buyers=_env_int("INSIDER_MIN_BUYERS", 2),
            insider_min_leg_value=_env_float("INSIDER_MIN_LEG_VALUE", 500000.0),
            insider_turnover_min_cr=_env_float("INSIDER_TURNOVER_MIN_CR", 10.0),
            insider_b200_min=_env_float("INSIDER_B200_MIN", 50.0),
            pledge_risk_per_trade=_env_float("PLEDGE_RISK_PER_TRADE", 0.0),
            pledge_max_positions=_env_int("PLEDGE_MAX_POSITIONS", 10),
            pledge_max_hold_days=_env_int("PLEDGE_MAX_HOLD_DAYS", 60),
            pledge_atr_sl_mult=_env_float("PLEDGE_ATR_SL_MULT", 2.0),
            pledge_notional_cap_pct=_env_float("PLEDGE_NOTIONAL_CAP_PCT", 0.10),
            pledge_turnover_min_cr=_env_float("PLEDGE_TURNOVER_MIN_CR", 25.0),
            pledge_b200_min=_env_float("PLEDGE_B200_MIN", 50.0),
        )
        return AppSettings(
            gcp=GcpSettings(
                project_id=_env("GCP_PROJECT_ID"),
                region=_env("GCP_REGION", "asia-south1"),
                bucket_name=_env("GCS_BUCKET"),
                spreadsheet_id=os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", ""),
                firestore_database=_env("FIRESTORE_DATABASE", "(default)"),
                bq_dataset=_env("BQ_DATASET", "autotrader"),
                pubsub_topic_positions=_env("PUBSUB_TOPIC_POSITIONS", "position-events"),
                pubsub_topic_signals=_env("PUBSUB_TOPIC_SIGNALS", "trade-signals"),
                pubsub_topic_regime=_env("PUBSUB_TOPIC_REGIME", "regime-events"),
            ),
            upstox=UpstoxSettings(
                api_v2_host=_env("UPSTOX_API_V2_HOST", "https://api.upstox.com/v2").rstrip("/"),
                api_v3_host=_env("UPSTOX_API_V3_HOST", "https://api.upstox.com/v3").rstrip("/"),
                client_id_secret_name=_env("UPSTOX_CLIENT_ID_SECRET_NAME"),
                client_secret_secret_name=_env("UPSTOX_CLIENT_SECRET_SECRET_NAME"),
                access_token_secret_name=_env("UPSTOX_ACCESS_TOKEN_SECRET_NAME"),
                access_token_expiry_secret_name=_env("UPSTOX_ACCESS_TOKEN_EXPIRY_SECRET_NAME"),
                redirect_uri=_env("UPSTOX_REDIRECT_URI", ""),
                auth_code_secret_name=_env("UPSTOX_AUTH_CODE_SECRET_NAME", ""),
                analytics_token_secret_name=_env("UPSTOX_ANALYTICS_TOKEN_SECRET_NAME", ""),
                notifier_shared_secret=_env("UPSTOX_NOTIFIER_SHARED_SECRET", ""),
                instruments_complete_url=_env(
                    "UPSTOX_INSTRUMENTS_COMPLETE_URL",
                    "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz",
                ),
                requests_per_second=max(1, _env_int("UPSTOX_REQUESTS_PER_SECOND", 50)),
                max_per_minute=max(1, _env_int("UPSTOX_MAX_PER_MINUTE", 500)),
                max_per_30min=max(1, _env_int("UPSTOX_MAX_PER_30MIN", 2000)),
                max_retries=max(1, _env_int("UPSTOX_MAX_RETRIES", 4)),
                nifty50_instrument_key=_env("UPSTOX_NIFTY50_INSTRUMENT_KEY", "NSE_INDEX|Nifty 50"),
                india_vix_instrument_key=_env("UPSTOX_INDIA_VIX_INSTRUMENT_KEY", "NSE_INDEX|India VIX"),
                pcr_underlying_instrument_key=_env("UPSTOX_PCR_UNDERLYING_INSTRUMENT_KEY", "NSE_INDEX|Nifty 50"),
                pcr_expiry_date=_env("UPSTOX_PCR_EXPIRY_DATE", ""),
            ),
            runtime=RuntimeSettings(
                paper_trade=_env_bool("PAPER_TRADE", True),
                job_trigger_token=_env("JOB_TRIGGER_TOKEN"),
                log_level=_env("LOG_LEVEL", "INFO"),
                timezone_name=_env("TZ", "Asia/Kolkata"),
                use_exit_fsm_v1=_env_bool("USE_EXIT_FSM_V1", False),
                use_playbook_v1=_env_bool("USE_PLAYBOOK_V1", False),
                use_expected_edge_r_v1=_env_bool("USE_EXPECTED_EDGE_R_V1", False),
                use_portfolio_book_v1=_env_bool("USE_PORTFOLIO_BOOK_V1", False),
                use_option_analytics_v1=_env_bool("USE_OPTION_ANALYTICS_V1", False),
                use_news_signals_v1=_env_bool("USE_NEWS_SIGNALS_V1", False),
                use_portfolio_stream_v1=_env_bool("USE_PORTFOLIO_STREAM_V1", False),
                # 2026-05-06: Default flipped to True. The attribution table
                # is the only place MAE/MFE per trade is captured. With it OFF,
                # every diagnostic question ("did this trade ever go +0.5R
                # before reverting?", "what's the median pull-back-to-entry
                # ratio for losers?") is unanswerable. Flag stays togglable
                # via env so canary runbook can still validate it cleanly.
                use_attribution_log_v1=_env_bool("USE_ATTRIBUTION_LOG_V1", True),
            ),
            strategy=strategy,
            regime_thresholds=RegimeThresholds(
                panic_stress_min=_env_float("REGIME_PANIC_STRESS_MIN", 82.0),
                panic_breadth_max=_env_float("REGIME_PANIC_BREADTH_MAX", 12.0),
                panic_dq_max=_env_float("REGIME_PANIC_DQ_MAX", 30.0),
                panic_dq_warmup_suppress=_env_bool("REGIME_PANIC_DQ_WARMUP_SUPPRESS", True),
                trend_up_trend_min=_env_float("REGIME_TREND_UP_TREND_MIN", 70.0),
                trend_up_breadth_min=_env_float("REGIME_TREND_UP_BREADTH_MIN", 62.0),
                trend_up_leadership_min=_env_float("REGIME_TREND_UP_LEADERSHIP_MIN", 56.0),
                trend_up_stress_max=_env_float("REGIME_TREND_UP_STRESS_MAX", 48.0),
                trend_up_hi_breadth_min=_env_float("REGIME_TREND_UP_HI_BREADTH_MIN", 80.0),
                trend_up_hi_leadership_min=_env_float("REGIME_TREND_UP_HI_LEADERSHIP_MIN", 60.0),
                trend_up_hi_stress_max=_env_float("REGIME_TREND_UP_HI_STRESS_MAX", 48.0),
                range_rotating_breadth_min=_env_float("REGIME_RANGE_ROTATING_BREADTH_MIN", 65.0),
                range_rotating_leadership_min=_env_float("REGIME_RANGE_ROTATING_LEADERSHIP_MIN", 55.0),
                range_rotating_stress_max=_env_float("REGIME_RANGE_ROTATING_STRESS_MAX", 55.0),
                trend_down_trend_max=_env_float("REGIME_TREND_DOWN_TREND_MAX", 36.0),
                trend_down_breadth_max=_env_float("REGIME_TREND_DOWN_BREADTH_MAX", 40.0),
                trend_down_leadership_max=_env_float("REGIME_TREND_DOWN_LEADERSHIP_MAX", 45.0),
                recovery_trend_min=_env_float("REGIME_RECOVERY_TREND_MIN", 40.0),
                recovery_breadth_min=_env_float("REGIME_RECOVERY_BREADTH_MIN", 35.0),
                recovery_leadership_min=_env_float("REGIME_RECOVERY_LEADERSHIP_MIN", 40.0),
                panic_exit_stress_above=_env_float("REGIME_PANIC_EXIT_STRESS_ABOVE", 65.0),
                panic_exit_breadth_below=_env_float("REGIME_PANIC_EXIT_BREADTH_BELOW", 22.0),
                trend_up_hold_trend_min=_env_float("REGIME_TREND_UP_HOLD_TREND_MIN", 60.0),
                trend_up_hold_breadth_min=_env_float("REGIME_TREND_UP_HOLD_BREADTH_MIN", 55.0),
                trend_up_hold_leadership_min=_env_float("REGIME_TREND_UP_HOLD_LEADERSHIP_MIN", 50.0),
                trend_up_reenter_trend_min=_env_float("REGIME_TREND_UP_REENTER_TREND_MIN", 74.0),
                trend_up_reenter_breadth_min=_env_float("REGIME_TREND_UP_REENTER_BREADTH_MIN", 66.0),
                trend_up_reenter_leadership_min=_env_float("REGIME_TREND_UP_REENTER_LEADERSHIP_MIN", 58.0),
                trend_up_reenter_hi_breadth_min=_env_float("REGIME_TREND_UP_REENTER_HI_BREADTH_MIN", 82.0),
                trend_up_reenter_hi_leadership_min=_env_float("REGIME_TREND_UP_REENTER_HI_LEADERSHIP_MIN", 62.0),
                trend_up_reenter_hi_stress_max=_env_float("REGIME_TREND_UP_REENTER_HI_STRESS_MAX", 45.0),
                transition_min_age_sec=_env_float("REGIME_TRANSITION_MIN_AGE_SEC", 240.0),
                lockdown_stress_min=_env_float("REGIME_LOCKDOWN_STRESS_MIN", 85.0),
                lockdown_dq_max=_env_float("REGIME_LOCKDOWN_DQ_MAX", 35.0),
                defensive_stress_min=_env_float("REGIME_DEFENSIVE_STRESS_MIN", 65.0),
                defensive_dq_max=_env_float("REGIME_DEFENSIVE_DQ_MAX", 55.0),
                aggressive_appetite_min=_env_float("REGIME_AGGRESSIVE_APPETITE_MIN", 66.0),
                aggressive_stress_max=_env_float("REGIME_AGGRESSIVE_STRESS_MAX", 50.0),
                aggressive_dq_min=_env_float("REGIME_AGGRESSIVE_DQ_MIN", 65.0),
                signal_fresh_max_sec=_env_float("REGIME_SIGNAL_FRESH_MAX_SEC", 120.0),
                signal_stale_full_sec=_env_float("REGIME_SIGNAL_STALE_FULL_SEC", 900.0),
                signal_max_penalty=_env_float("REGIME_SIGNAL_MAX_PENALTY", 40.0),
                pubsub_heartbeat_sec=_env_float("REGIME_PUBSUB_HEARTBEAT_SEC", 300.0),
            ),
        )
