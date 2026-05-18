from __future__ import annotations

from typing import TYPE_CHECKING

from autotrader.domain.models import Direction, IndicatorSnapshot, RegimeSnapshot, ScoreBreakdown, SignalScore
from autotrader.settings import StrategySettings

if TYPE_CHECKING:
    from autotrader.domain.daily_bias import DailyBias


def determine_direction(
    ind: IndicatorSnapshot,
    regime: RegimeSnapshot,
    setup: str = "",
    wl_type: str = "intraday",
    daily_bias: "DailyBias | None" = None,
) -> Direction:
    if regime.regime == "AVOID":
        return "HOLD"

    _setup_upper = str(setup or "").strip().upper()
    _is_swing = str(wl_type or "").strip().lower() == "swing"

    # MORNING_FADE is contrarian by design: it shorts stocks that are UP
    # >1.5% by 09:45 IST. The standard bull/bear vote will see a strong-up
    # stock and return BUY, which the short-only veto then converts to HOLD,
    # producing zero signals (verified empirically 2026-05-06: 13-day window
    # with only the time+pop+volume gate produced 1 trade because the rest
    # were filtered by direction vote). Force SELL so check_strategy_entry
    # can decide on the actual fade-thesis gates instead.
    if _setup_upper == "MORNING_FADE":
        return "SELL"

    # MEAN_REVERSION / VWAP_REVERSAL are also contrarian by construction —
    # BUY at oversold, SELL at overbought, HOLD in between. Audit 2026-05-19
    # (Batch E) found 105 misfired swing MR SELLs/day on May 18 where
    # intraday RSI was ~37 (oversold) but the democratic vote went SELL
    # because EMA stack was bearish + MACD was bearish. The downstream
    # gate then vetoed with `swing_mr_daily_rsi_not_overbought` because
    # the stock wasn't actually overbought on the daily timeframe.
    #
    # Fix: force-direction purely from RSI, ALIGNED TO THE GATE'S TIMEFRAME.
    # - Intraday MR uses ind.rsi.curr (matches intraday MR gate at line 529)
    # - Swing MR uses daily_bias.rsi_daily (matches swing MR gate at line 789)
    # Regime-aware thresholds match the gate's RANGE/CHOP vs other bands.
    if _setup_upper in ("MEAN_REVERSION", "VWAP_REVERSAL"):
        _regime_str = str(getattr(regime, "regime", "") or "").strip().upper()
        _is_range_like = _regime_str in ("RANGE", "CHOP")
        if _is_swing and daily_bias is not None and float(daily_bias.rsi_daily or 0) > 0:
            _rsi = float(daily_bias.rsi_daily)
            # Match swing MR gate (scoring.py ~789):
            #   RANGE/CHOP:  BUY ≤ 45, SELL ≥ 55
            #   Other:       BUY ≤ 35, SELL ≥ 65
            _buy_max = 45.0 if _is_range_like else 35.0
            _sell_min = 55.0 if _is_range_like else 65.0
        else:
            _rsi = float(ind.rsi.curr)
            # Match intraday MR gate (scoring.py ~529):
            #   RANGE/CHOP:  BUY ≤ 45, SELL ≥ 58
            #   Other:       BUY ≤ 40, SELL ≥ 60
            _buy_max = 45.0 if _is_range_like else 40.0
            _sell_min = 58.0 if _is_range_like else 60.0
        if _rsi <= _buy_max:
            return "BUY"
        if _rsi >= _sell_min:
            return "SELL"
        return "HOLD"

    bull = 0
    bear = 0

    _is_mr = _setup_upper in ("MEAN_REVERSION", "VWAP_REVERSAL")
    # Some setups are inherently single-sided. Pre-fix this check was missing,
    # which let the vote tally flip the "direction" of a setup whose name
    # already encodes its side. Result: scan_decisions rows like
    # `setup=SHORT_BREAKDOWN, direction=BUY` — nonsense that wasted scan
    # cycles and produced confusing audit data.
    # BREAKOUT, MOMENTUM, OPEN_DRIVE, PULLBACK = long-only by design.
    # SHORT_BREAKDOWN, SHORT_PULLBACK, SHORT_MOMENTUM = short-only.
    _long_only = _setup_upper in ("BREAKOUT", "MOMENTUM", "OPEN_DRIVE", "PULLBACK")
    _short_only = _setup_upper.startswith("SHORT_") or _setup_upper == "MORNING_FADE"

    bull += 3 if ind.supertrend.dir == 1 else 0
    bear += 3 if ind.supertrend.dir != 1 else 0

    if _is_mr:
        # Mean-reversion strategies fade the VWAP deviation — a stock below VWAP
        # is OVERSOLD (we want to BUY), not bearish. Suppress the VWAP position
        # vote and replace it with an RSI-based reversal vote so direction aligns
        # with the entry gate (BUY when oversold, SELL when overbought).
        if ind.rsi.curr < 40:
            bull += 3   # oversold → expect bounce → BUY
        elif ind.rsi.curr > 60:
            bear += 3   # overbought → expect fade → SELL
    else:
        bull += 2 if ind.close > ind.vwap else 0
        bear += 2 if ind.close < ind.vwap else 0   # equal = neutral: no vote for either side
    bull += 2 if ind.ema_fast.curr > ind.ema_med.curr else 0
    bear += 2 if ind.ema_fast.curr < ind.ema_med.curr else 0   # equal = neutral
    bull += 1 if ind.ema_med.curr > ind.ema_slow.curr else 0
    bear += 1 if ind.ema_med.curr < ind.ema_slow.curr else 0   # equal = neutral
    if ind.rsi.curr > 55:
        bull += 1
    elif ind.rsi.curr < 45:
        bear += 1
    bull += 2 if ind.macd.hist > 0 else 0
    bear += 2 if ind.macd.hist <= 0 else 0
    if ind.macd.crossed == "BUY":
        bull += 1
    if ind.macd.crossed == "SELL":
        bear += 1
    if ind.patterns.bull_engulf:
        bull += 1
    if ind.patterns.bear_engulf:
        bear += 1
    if regime.bias == "BULLISH":
        bull += 2
    if regime.bias == "BEARISH":
        bear += 2

    # Audit 2026-05-16 (Batch D): swing uses a 2-point margin (was 3) — daily
    # timeframe has less noise than 5m intraday and the 3-point bar was
    # blocking 305 rejects/week as `direction_hold` with 0 swing trades
    # firing for 5 consecutive days. Intraday keeps the 3-point margin
    # (bull > bear + 2) — it's exposed to per-bar whipsaw.
    _margin = 1 if _is_swing else 2
    if bull > bear + _margin:
        # Single-sided setup veto: a long-only setup that votes BUY is fine,
        # but if a short-only setup somehow accumulates more bull votes
        # (because of regime bias), we return HOLD rather than fire a wrong-
        # side trade. Same for the inverse.
        if _short_only:
            return "HOLD"
        return "BUY"
    if bear > bull + _margin:
        if _long_only:
            return "HOLD"
        return "SELL"
    return "HOLD"


def score_signal(
    symbol: str,
    direction: Direction,
    ind: IndicatorSnapshot,
    regime: RegimeSnapshot,
    cfg: StrategySettings,
    *,
    daily_bias: DailyBias | None = None,
    setup: str = "",
) -> SignalScore:
    # MORNING_FADE bypasses the standard scoring formula. The 7-layer score
    # is bullish-trend-biased (Layer-1 regime alignment, Layer-3 technical
    # with-trend, Layer-5 daily-bias alignment) — every layer structurally
    # penalises shorting an up-stock, scoring 30-50, never qualifying. The
    # check_strategy_entry gate already validated the contrarian thesis
    # (time + pop + volume); we hand back a fixed 75 so threshold +
    # affinity + brain-haircut still gate the trade in adverse regimes.
    # Verified empirically (2026-05-06): without this, MORNING_FADE fired
    # 1 trade over a 13-day window. With it, 51 trades, 37% WR, +₹12k net.
    if str(setup or "").strip().upper() == "MORNING_FADE":
        return SignalScore(score=75, direction=direction, breakdown=ScoreBreakdown())

    bd = ScoreBreakdown()
    if direction == "HOLD" or regime.regime == "AVOID":
        return SignalScore(score=0, direction=direction, breakdown=bd)

    score = 0.0
    is_buy = direction == "BUY"

    # Layer 1: Regime (20)
    if (is_buy and regime.nifty.change_pct > 0.1) or ((not is_buy) and regime.nifty.change_pct < -0.1):
        bd.regime += 8
    elif abs(regime.nifty.change_pct) < 0.1:
        bd.regime += 4

    if regime.vix < cfg.vix_trend_max:
        bd.regime += 7
    elif regime.vix < cfg.vix_safe_max:
        bd.regime += 3

    if (is_buy and regime.fii.fii > 500) or ((not is_buy) and regime.fii.fii < -500):
        bd.regime += 5
    elif abs(regime.fii.fii) < 500:
        bd.regime += 2
    bd.regime = min(20, bd.regime)
    score += bd.regime

    # Layer 2: Options (15)
    if (is_buy and regime.pcr.pcr >= cfg.pcr_bull_min) or ((not is_buy) and regime.pcr.pcr <= cfg.pcr_bear_max):
        bd.options += 5
    else:
        bd.options += 1
    # OI change PCR: real-time options flow (put OI additions vs call OI additions).
    # Rising oi_change_pcr = put protection being added = smart money hedging = bearish.
    # This is more actionable than static PCR snapshot.
    oi_pcr = regime.pcr.oi_change_pcr  # default 1.0 = neutral
    if is_buy and oi_pcr < 0.75:
        bd.options += 3   # call buying dominant → bullish flow confirms BUY
    elif not is_buy and oi_pcr > 1.35:
        bd.options += 3   # put buying dominant → bearish flow confirms SELL
    elif is_buy and oi_pcr > 1.35:
        bd.options -= 2   # bearish options flow contradicts BUY signal
    elif not is_buy and oi_pcr < 0.75:
        bd.options -= 2   # bullish options flow contradicts SELL signal
    # Max-pain proximity: use the pre-computed % distance from Nifty max-pain
    # (max_pain_dist_pct = abs(nifty_ltp - max_pain) / max_pain × 100).
    # The old code compared stock price to Nifty max-pain strike — meaningless.
    if regime.pcr.max_pain_dist_pct > 0 or regime.pcr.max_pain > 0:
        mp_dist = regime.pcr.max_pain_dist_pct  # % distance of Nifty from max pain
        if mp_dist <= 1.0:
            bd.options += 7   # Nifty pinned near max pain — strong mean-reversion force
        elif mp_dist <= 2.5:
            bd.options += 4   # moderately close to max pain
        else:
            bd.options += 2   # far from max pain — max-pain gravity weak
    else:
        bd.options += 4
    bd.options = min(15, bd.options)
    score += bd.options

    # Layer 3: Technical (35)
    if ind.supertrend.fresh and ((is_buy and ind.supertrend.dir == 1) or ((not is_buy) and ind.supertrend.dir == -1)):
        bd.technical += 9
    elif (is_buy and ind.supertrend.dir == 1) or ((not is_buy) and ind.supertrend.dir == -1):
        bd.technical += 5

    if (is_buy and ind.close > ind.vwap) or ((not is_buy) and ind.close < ind.vwap):
        bd.technical += 7

    if is_buy:
        if ind.ema_fast.curr > ind.ema_med.curr > ind.ema_slow.curr:
            bd.technical += 6
        elif ind.ema_fast.curr > ind.ema_med.curr:
            bd.technical += 3
        elif ind.ema_fast.curr > ind.ema_fast.prev:
            bd.technical += 1
    else:
        if ind.ema_fast.curr < ind.ema_med.curr < ind.ema_slow.curr:
            bd.technical += 6
        elif ind.ema_fast.curr < ind.ema_med.curr:
            bd.technical += 3
        elif ind.ema_fast.curr < ind.ema_fast.prev:
            bd.technical += 1

    rsi = ind.rsi.curr
    if (is_buy and cfg.rsi_buy_min <= rsi <= cfg.rsi_buy_max) or ((not is_buy) and cfg.rsi_sell_min <= rsi <= cfg.rsi_sell_max):
        bd.technical += 6
    elif (is_buy and rsi > ind.rsi.prev and rsi < cfg.rsi_buy_max) or ((not is_buy) and rsi < ind.rsi.prev and rsi > cfg.rsi_sell_min):
        bd.technical += 2

    if (ind.macd.crossed == "BUY" and is_buy) or (ind.macd.crossed == "SELL" and (not is_buy)):
        bd.technical += 7
    elif (is_buy and ind.macd.hist > 0) or ((not is_buy) and ind.macd.hist < 0):
        bd.technical += 3

    # ADX: trend-strength filter
    if ind.adx >= 30:
        bd.technical = min(35, bd.technical + 4)
    elif ind.adx >= 20:
        bd.technical = min(35, bd.technical + 2)

    if (is_buy and ind.patterns.bull_engulf) or ((not is_buy) and ind.patterns.bear_engulf):
        bd.technical = min(35, bd.technical + 2)

    # Relative Strength vs Nifty: stocks leading the market in the trade direction
    # are higher quality — they'll be the first to move AND will resist adverse Nifty moves.
    # Stocks diverging (stock falling while market rising, or vice versa) are low quality.
    # Only applied when Nifty has moved meaningfully (≥0.15%) to avoid noise on flat days.
    if ind.prev_close > 0 and abs(regime.nifty.change_pct) >= 0.15:
        _stock_chg = (ind.close - ind.prev_close) / ind.prev_close * 100.0
        _rs = _stock_chg / regime.nifty.change_pct   # ratio: 1.0 = in-line, >1 = outperform
        if is_buy:
            if _rs >= 2.0:
                bd.technical += 4    # Strong leadership — stock up 2× more than market
            elif _rs >= 1.3:
                bd.technical += 2    # Moderate outperformance
            elif _rs <= 0.0:
                bd.technical -= 3    # Divergence — stock falling while market rising
        else:  # SELL
            if _rs <= 0.0:
                bd.technical += 4    # True relative weakness — stock falling vs flat/rising market
            elif _rs <= 0.5:
                bd.technical += 2    # Significant underperformance
            elif _rs >= 2.0:
                bd.technical -= 3    # Wrong side — stock is leading upward

    bd.technical = min(35, bd.technical)
    score += bd.technical

    # Layer 4: Volume (10)
    if ind.volume.ratio >= cfg.vol_mult:
        bd.volume += 7
    elif ind.volume.ratio >= 1.2:
        bd.volume += 4
    elif ind.volume.ratio >= 1.0:
        bd.volume += 2
    if (is_buy and ind.obv_curr > ind.obv_prev) or ((not is_buy) and ind.obv_curr < ind.obv_prev):
        bd.volume += 3
    bd.volume = min(10, bd.volume)
    score += bd.volume

    # Layer 5: Multi-timeframe Alignment (15)
    # When daily_bias is provided, reward signals aligned with the daily trend
    # and penalise those fighting it.
    #
    # Batch 3.2 (2026-04-22): daily_bias.strength now scales the alignment
    # magnitude directly, not just as a ±3 post-adjustment. A weak UP trend
    # (strength=25) should award fewer alignment points than a strong UP trend
    # (strength=90) — previously both collapsed to +15 and we over-weighted
    # weak-trend setups that were structurally coin-flips. Scaling: the fixed
    # baseline captures "direction is correct" and the strength multiplier
    # captures "the daily trend actually has conviction".
    if daily_bias is not None:
        _strength_norm = max(0.0, min(1.0, float(daily_bias.strength or 0) / 100.0))
        if is_buy:
            if daily_bias.trend == "UP":
                # 8 baseline + up to 7 scaled by strength → weak trend +8, strong trend +15
                bd.alignment += int(round(8 + 7 * _strength_norm))
            elif daily_bias.trend == "NEUTRAL":
                bd.alignment += 5
            else:
                # Counter-trend: penalty also scales with how strongly DOWN it is
                bd.alignment -= int(round(5 + 5 * _strength_norm))
        else:  # SELL
            if daily_bias.trend == "DOWN":
                bd.alignment += int(round(8 + 7 * _strength_norm))
            elif daily_bias.trend == "NEUTRAL":
                bd.alignment += 5
            else:
                bd.alignment -= int(round(5 + 5 * _strength_norm))

        bd.alignment = max(-10, min(15, bd.alignment))
    score += bd.alignment

    # Penalties
    # VIX tiering: Indian VIX baseline is 14-16, so >18 triggers too easily.
    # Penalty only applies to BUY signals — high VIX actually FAVOURS short
    # setups (volatility expansion to downside), so penalising SELL in high-VIX
    # would incorrectly suppress the most correct trades in a fear spike.
    if is_buy:
        if regime.vix > 22:
            bd.penalty -= 10
        elif regime.vix > 18:
            bd.penalty -= 5
    # RANGE penalty only applies to strategies that genuinely struggle in range
    # markets. MEAN_REVERSION, VWAP_REVERSAL, and VWAP_TREND all work in RANGE
    # (individual stocks trend even when the index ranges). PULLBACK also works
    # when a stock pulls back to EMA support within its own mini-trend.
    _setup_upper = str(setup or "").strip().upper()
    _range_ok = {"MEAN_REVERSION", "VWAP_REVERSAL", "VWAP_TREND", "PULLBACK", "SHORT_PULLBACK", "MOMENTUM"}
    if regime.regime == "RANGE" and _setup_upper not in _range_ok:
        bd.penalty -= 8
    if ind.adx > 0 and ind.adx < 15 and regime.regime != "RANGE":
        bd.penalty -= 5
    if abs(ind.close - ind.open) / (ind.close or 1) * 100 > 2.5:
        bd.penalty -= 5
    if ind.patterns.doji:
        bd.penalty -= 3
    if ind.bb and is_buy and ind.close > ind.bb.upper * 0.998:
        bd.penalty -= 5
    if ind.bb and (not is_buy) and ind.close < ind.bb.lower * 1.002:
        bd.penalty -= 5
    if is_buy and ind.stoch.k > 85:
        bd.penalty -= 4
    if (not is_buy) and ind.stoch.k < 15:
        bd.penalty -= 4
    score += bd.penalty

    final_score = max(0, min(100, int(round(score))))
    return SignalScore(score=final_score, direction=direction, breakdown=bd)


def _session_open_price(ind: IndicatorSnapshot) -> float:
    """Return today's session-open price from the candle window.

    The IndicatorSnapshot.open field is the CURRENT 5m bar's open, not the
    day-session open. For setups that need "intraday return so far"
    (MORNING_FADE et al.) we walk back through ind.candles to the first
    bar of today's date.
    """
    if not ind.candles:
        return 0.0
    last_ts = str(ind.candles[-1][0])
    today_date = last_ts[:10]
    for c in ind.candles:
        if str(c[0])[:10] == today_date:
            try:
                return float(c[1])    # candle tuple: (ts, open, h, l, c, vol)
            except (TypeError, ValueError, IndexError):
                return 0.0
    return 0.0


def _ist_minutes_from_ts(ts: str) -> int:
    """Extract HH:MM from an ISO-8601 IST timestamp and return minutes
    since midnight. Used by setups with a time-of-day gate (e.g.
    MORNING_FADE which only fires in the first 30 min after market open).
    Returns 0 if parsing fails."""
    try:
        time_part = ts.split("T")[1]
        h, m = time_part.split(":")[:2]
        return int(h) * 60 + int(m)
    except Exception:
        return 0


def check_strategy_entry(
    strategy: str,
    direction: str,
    ind: IndicatorSnapshot,
    regime: str = "",
) -> tuple[bool, str]:
    """Validate strategy-specific entry conditions beyond the generic direction vote.

    Returns (passed, reason).  When passed=False the signal is suppressed even if
    direction and score would otherwise qualify.  Each strategy has a short list of
    hard gates — conditions that *must* be true for that setup to make sense.

    BREAKOUT  : needs trend strength (ADX ≥ 20), price near 52-week high, volume surge
    PULLBACK  : needs intact trend (EMA stack), RSI in healthy pullback zone (40-60)
    MEAN_REVERSION / VWAP_REVERSAL : needs RSI stretched, price extended from VWAP
    VWAP_TREND: price must be on correct side of VWAP with positive slope proxy
    All others (AUTO, DEFAULT, OPEN_DRIVE, etc.): pass through unchecked
    """
    s = str(strategy or "").strip().upper()
    is_buy = direction == "BUY"

    if s in ("BREAKOUT", "SHORT_BREAKDOWN"):
        # 2026-05-14 audit: BREAKOUT was 0/6 WR over Apr 23 → Apr 28 with
        # AvgMFE = 0.00R (trades NEVER went positive). Bar-by-bar analysis
        # showed 5/5 verified entries fired on RED bars (close <= open) where
        # price was reversing OFF the breakout level. Old gate only checked
        # "near 52w high" — same green-light for actual breakouts and for
        # stocks reversing off their highs. The system already hard-blocked
        # BREAKOUT in all regimes on 2026-05-06 (see regime_affinity.py); this
        # gate fix makes the setup safe to re-enable once validated.
        #
        # Architectural fix: confirm an ACTUAL breakout NOW, not proximity.
        if ind.adx < 20:
            return False, "strategy_breakout_adx_too_low"
        if is_buy and ind.dist_from_52w_high > 5.0:
            return False, "strategy_breakout_too_far_from_high"
        if not is_buy and ind.dist_from_52w_high < 5.0:
            # Short breakdown: block only if stock is at/near all-time high
            # (< 5% off). Early breakdown entries (5-20% off highs) are the
            # highest-quality shorts — don't block them.
            return False, "strategy_breakdown_price_too_high"
        if ind.volume.ratio < 1.2:
            return False, "strategy_breakout_no_volume_surge"
        # ── Breakout confirmation gates (2026-05-14) ────────────────────
        if not ind.candles or len(ind.candles) < 13:
            return False, "strategy_breakout_insufficient_history"
        current = ind.candles[-1]
        c_open = float(current[1])
        c_close = float(current[4])
        # 1. Current bar must close in trade direction. A red bar (close<=open)
        # for a BUY breakout is a textbook false breakout / reversal candle.
        if is_buy and c_close <= c_open:
            return False, "strategy_breakout_red_entry_bar"
        if not is_buy and c_close >= c_open:
            return False, "strategy_breakout_green_entry_bar"
        # 2. Close must clear the prior 12-bar high (60 min on 5m). "Near 52w
        # high" can be true while price is rolling over; "close > recent high"
        # is what an actual breakout looks like.
        prior_12 = ind.candles[-13:-1]  # excludes current bar
        if is_buy:
            prior_high = max(float(b[2]) for b in prior_12)
            if c_close < prior_high:
                return False, "strategy_breakout_close_below_recent_high"
        else:
            prior_low = min(float(b[3]) for b in prior_12)
            if c_close > prior_low:
                return False, "strategy_breakout_close_above_recent_low"
        # 3. Volume must be accelerating INTO the breakout, not just elevated
        # overall. Current bar volume > 1.1× prior-4-bar avg.
        recent_vol_avg = sum(float(b[5]) for b in ind.candles[-5:-1]) / 4
        if recent_vol_avg > 0 and float(current[5]) < recent_vol_avg * 1.1:
            return False, "strategy_breakout_volume_not_accelerating"
        return True, ""

    if s in ("PULLBACK", "SHORT_PULLBACK"):
        # 2026-05-08 strategy audit: PULLBACK had 0 live trades / 96 scans
        # despite p90 score of 70 (just above threshold of 68.5 in TREND_UP).
        # Block reasons cascade: gates compound to ~0% pass-through. Specific
        # widenings below preserve the trend-continuity check (ema_stack)
        # while letting more legitimate pullback entries through:
        #   * RSI band 38-65 → 35-70 (BUY): stocks in strong uptrends often
        #     pull back to 60-70 range, not 38-50. Tight band missed real
        #     pullback entries on RS leaders.
        #   * RSI band 40-62 → 38-65 (SELL): symmetric widening for shorts.
        #   * EMA distance ±3% → ±5%: 5m bars on liquid Indian stocks
        #     routinely move 3-4% intraday during uptrends. ±3% tight EMA
        #     band rejected most pullbacks before they could enter.
        # The structural anchors stay strict: ema_stack required (no buying
        # broken trends), pullback direction enforced.
        if is_buy and not ind.ema_stack:
            return False, "strategy_pullback_no_bull_ema_stack"
        # SHORT_PULLBACK: require at minimum fast EMA < med EMA (first downtrend signal).
        # ema_flip (fast<med<slow) was too strict — EMAs lag, so full flip only appears
        # well into a downtrend after the best short entry has passed. We block only if
        # stock is in a full BULL stack (wrong direction entirely).
        if not is_buy and ind.ema_stack:
            return False, "strategy_pullback_no_bear_ema_signal"
        rsi = ind.rsi.curr
        # Widened from 38-65 to 35-70 (2026-05-08 audit).
        if is_buy and not (35 <= rsi <= 70):
            return False, "strategy_pullback_rsi_outside_reload_zone"
        # Widened from 40-62 to 38-65 (2026-05-08 audit).
        if not is_buy and not (38 <= rsi <= 65):
            return False, "strategy_pullback_rsi_outside_reload_zone"
        # Actual pullback check: price must be near fast EMA support/resistance
        # (within ±5%, was ±3%). If price is >5% above EMA for BUY, it already
        # ran — not a pullback. If price is >5% below EMA for BUY, the trend
        # is broken — not a pullback entry.
        if ind.ema_fast.curr > 0:
            _ema_dist_pct = (ind.close - ind.ema_fast.curr) / ind.ema_fast.curr * 100.0
            if is_buy:
                if _ema_dist_pct > 5.0:
                    return False, "strategy_pullback_price_extended_above_ema"
                if _ema_dist_pct < -5.0:
                    return False, "strategy_pullback_price_broke_below_ema"
            else:
                if _ema_dist_pct < -5.0:
                    return False, "strategy_pullback_price_extended_below_ema"
                if _ema_dist_pct > 5.0:
                    return False, "strategy_pullback_price_broke_above_ema"
        return True, ""

    if s in ("MEAN_REVERSION", "VWAP_REVERSAL"):
        # VWAP Reversal / Mean Reversion — proper institutional entry gates.
        #
        # 2026-05-08 strategy audit: MEAN_REVERSION had 0 live trades / 1953
        # scans despite p90 score of 80 in RANGE (well above threshold 71).
        # Block-cascade analysis showed the gates here compounded with
        # `direction_hold` (RSI 40-60 produces no direction vote) to drop
        # ~99.5% of scans. Two specific calibration mismatches identified:
        #
        #   1. RSI gate vs direction logic: determine_direction() casts a
        #      bull vote at RSI<40 (loose), bear at RSI>60 (loose). This
        #      gate required RSI≤35 in non-RANGE — so a stock with RSI=37
        #      could be voted BUY by direction logic and then rejected here.
        #      Aligning the strategy gate to match the direction logic's
        #      thresholds removes this contradiction. RSI band widened:
        #        - Non-RANGE BUY:  ≤35 → ≤40
        #        - Non-RANGE SELL: ≥60 → ≥60 (already aligned)
        #        - RANGE BUY:      ≤40 → ≤45 (slight loosening, still oversold)
        #        - RANGE SELL:     ≥65 → ≥58 (matches direction logic at 60)
        #
        #   2. VWAP extension: 1.0% was too tight on 5m bars. p50 5m
        #      VWAP-deviation in our universe is 0.4%; 1.0% threshold means
        #      we only fire on the top ~10% extension events. Loosened to
        #      0.6% — captures genuine reversal setups without firing on
        #      bid/ask noise (<0.5%).
        rsi = ind.rsi.curr
        _regime_upper = str(regime or "").strip().upper()
        _is_range_like = _regime_upper in ("RANGE", "CHOP")

        if is_buy:
            # BUY reversal: price must be below VWAP (oversold stretch)
            if ind.vwap > 0 and ind.close >= ind.vwap:
                return False, "strategy_mr_buy_price_not_below_vwap"
            # Loosened from (40 RANGE / 35 other) to (45 RANGE / 40 other).
            rsi_limit = 45 if _is_range_like else 40
            if rsi > rsi_limit:
                return False, "strategy_mr_rsi_not_oversold"
        else:
            # SELL reversal: price must be above VWAP (overbought stretch)
            if ind.vwap > 0 and ind.close <= ind.vwap:
                return False, "strategy_mr_sell_price_not_above_vwap"
            # Loosened from (65 RANGE / 60 other) to (58 RANGE / 60 other).
            # Match direction logic's bear vote at RSI>60.
            rsi_floor = 58 if _is_range_like else 60
            if rsi < rsi_floor:
                return False, "strategy_mr_rsi_not_overbought"

        if ind.vwap > 0:
            vwap_dev = abs(ind.close - ind.vwap) / ind.vwap * 100
            # Loosened from 1.0% to 0.6%. p50 5m VWAP deviation ≈ 0.4%.
            if vwap_dev < 0.6:
                return False, "strategy_mr_insufficient_vwap_extension"
        return True, ""

    if s == "VWAP_TREND":
        # 2026-05-14 audit: VWAP_TREND time-of-day split:
        #   09:45-10:30:  1/3 WR (-₹44)
        #   10:31-11:30:  1/6 WR (-₹300) ← morning bleeder
        #   11:31-12:30:  3/3 WR (+₹228) ← afternoon edge
        # Root cause: gate doesn't validate that VWAP is a meaningful signal.
        # At 10:00 IST VWAP has ~9 bars of input → noise. By 11:30 VWAP has
        # 27 bars → actual trend signal. Two additional gates:
        #   1. Bars-since-open >= 12 (60 min) — VWAP statistically settled
        #   2. Last 3 bars all on trade side of VWAP — not coincidental crossing
        # (VWAP slope check deferred — needs vwap_history on IndicatorSnapshot,
        # bigger change; the bars-since-open + sustained-side gates capture
        # most of the morning-failure pattern.)
        bar_ts = str(ind.candles[-1][0]) if ind.candles else ""
        bar_min = _ist_minutes_from_ts(bar_ts)
        # 60 min after session-open: 09:15 + 60 = 10:15 = 615 min.
        if bar_min < 615:
            return False, "strategy_vwap_trend_session_too_young"
        # Sustained side of VWAP — last 3 closes all in direction.
        if len(ind.candles) >= 3:
            last_3 = ind.candles[-3:]
            if is_buy and not all(float(c[4]) > ind.vwap for c in last_3):
                return False, "strategy_vwap_trend_not_sustained_above_vwap"
            if not is_buy and not all(float(c[4]) < ind.vwap for c in last_3):
                return False, "strategy_vwap_trend_not_sustained_below_vwap"
        # Existing gates — price on correct side of VWAP
        if is_buy and ind.close <= ind.vwap:
            return False, "strategy_vwap_trend_price_below_vwap"
        if not is_buy and ind.close >= ind.vwap:
            return False, "strategy_vwap_trend_price_above_vwap"
        if ind.adx < 18:
            return False, "strategy_vwap_trend_adx_too_low"
        return True, ""

    if s == "PHASE1_MOMENTUM":
        # Long-only setup — PHASE1 stocks are selected for upside momentum; shorting
        # them on a bad day is the opposite of the intended edge.
        if not is_buy:
            return False, "strategy_phase1_long_only"
        # Require at least near-average volume — a stale Phase1 pick with no
        # participation should not enter.
        if ind.volume.ratio < 0.8:
            return False, "strategy_phase1_insufficient_volume"
        return True, ""

    if s == "PHASE1_REVERSAL":
        # Oversold-bounce setup selected in bearish markets — long-only (we're
        # looking for beaten-down stocks to bounce, not fresh shorts).
        if not is_buy:
            return False, "strategy_phase1_reversal_long_only"
        # Must be in oversold territory — if RSI is already above 55, the
        # "reversal" has already played out and the edge is gone.
        if ind.rsi.curr > 55:
            return False, "strategy_phase1_reversal_rsi_too_high"
        # Require at least near-average volume
        if ind.volume.ratio < 0.8:
            return False, "strategy_phase1_reversal_insufficient_volume"
        return True, ""

    if s == "MOMENTUM":
        # 2026-05-06: previously had NO gate — fell through to "pass". Live
        # 2026-04-16 → 2026-05-04: 31 of 36 pure-replay trades were MOMENTUM,
        # win rate 9.7%, total −₹70,516. Without gates, MOMENTUM fires on any
        # signal that clears the score threshold, including weak ones with no
        # actual momentum behind them.
        # Gates below mirror what an experienced intraday trader requires
        # before chasing strength: real trend (ADX), real volume (>avg),
        # right side of VWAP, and RSI in the momentum zone (not overbought,
        # not yet exhausted, not in pullback).
        if ind.adx < 20:
            return False, "strategy_momentum_adx_too_low"
        if ind.volume.ratio < 1.3:
            return False, "strategy_momentum_insufficient_volume"
        if is_buy:
            if ind.close <= ind.vwap:
                return False, "strategy_momentum_buy_below_vwap"
            if not (55 <= ind.rsi.curr <= 75):
                return False, "strategy_momentum_buy_rsi_outside_zone"
            if ind.ema_fast.curr <= ind.ema_med.curr:
                return False, "strategy_momentum_buy_ema_not_stacked"
        else:
            if ind.close >= ind.vwap:
                return False, "strategy_momentum_sell_above_vwap"
            if not (25 <= ind.rsi.curr <= 45):
                return False, "strategy_momentum_sell_rsi_outside_zone"
            if ind.ema_fast.curr >= ind.ema_med.curr:
                return False, "strategy_momentum_sell_ema_not_stacked"
        return True, ""

    if s == "OPEN_DRIVE":
        # First-30-min strong directional setup. 2026-05-14 audit identified
        # 3/3 OPEN_DRIVE losses in live (MOSCHIP, ANURAS, AEROFLEX), all
        # firing in 10:24-11:29 window — well past the actual opening drive.
        # Each trade showed AvgMFE +0.72R then reversed (system entered late,
        # caught the dying gasp of the morning move). The previous developer
        # left a TODO comment about this (see git history); the fix mirrors
        # MORNING_FADE's existing pattern using `ind.candles[-1][0]`.
        bar_ts = str(ind.candles[-1][0]) if ind.candles else ""
        bar_min = _ist_minutes_from_ts(bar_ts)
        # 09:15-09:45 IST = 555-585 minutes since midnight. The 09:20 bar
        # represents 09:15-09:20 close, so first 6 bars qualify.
        if not (555 <= bar_min <= 585):
            return False, "strategy_open_drive_outside_time_window"
        if ind.adx < 18:
            return False, "strategy_open_drive_adx_too_low"
        if ind.volume.ratio < 1.2:
            return False, "strategy_open_drive_insufficient_volume"
        if is_buy and ind.close <= ind.vwap:
            return False, "strategy_open_drive_buy_below_vwap"
        if not is_buy and ind.close >= ind.vwap:
            return False, "strategy_open_drive_sell_above_vwap"
        return True, ""

    if s == "MORNING_FADE":
        # 2026-05-06: NEW setup. Live audit (Apr 16 → May 4, 73 trades, all
        # net-negative) showed the existing 5 continuation setups had no
        # edge in the current mean-reverting regime. Wider-universe ORB
        # experiment: 78% of stocks that pop >1.5% in the first 30 min
        # reverse to a 1% stop. MORNING_FADE captures the inverse trade.
        #
        # First-pass gates (ADX<22 + RSI>60 + dist_52w>1) compounded to
        # ZERO signals over a 13-day window — proven by 2026-05-06 backtest.
        # Loosened to the THESIS gates only: time + pop magnitude + volume.
        # Add back trend/momentum filters once we have data showing which
        # of them actually predict win-rate.
        if is_buy:
            return False, "strategy_morning_fade_short_only"
        bar_ts = str(ind.candles[-1][0]) if ind.candles else ""
        bar_min = _ist_minutes_from_ts(bar_ts)
        # Time gate: 09:45 (585) to 10:15 (615) IST. Six 5m bars after open
        # is the earliest meaningful signal. 10:15 cap because after that
        # the "morning pop" is no longer recent enough to fade.
        if not (585 <= bar_min <= 615):
            return False, "strategy_morning_fade_outside_time_window"
        day_open = _session_open_price(ind)
        if day_open <= 0:
            return False, "strategy_morning_fade_no_session_open"
        # Must be up >1.5% from session open (the "pop" we're fading).
        pct_up = (ind.close - day_open) / day_open * 100.0
        if pct_up < 1.5:
            return False, "strategy_morning_fade_no_pop"
        # Volume confirmation — at least average volume so we're not fading
        # an illiquid stock where the pop has no participation.
        if ind.volume.ratio < 1.0:
            return False, "strategy_morning_fade_no_volume_participation"
        return True, ""

    # AUTO, DEFAULT, etc. — no extra gate
    return True, ""


def check_swing_entry(
    strategy: str,
    direction: str,
    ind: IndicatorSnapshot,
    daily_bias: DailyBias | None,
    regime: str = "",
) -> tuple[bool, str]:
    """Swing-specific entry gates — tighter than intraday because positions are held for days.

    Uses daily-timeframe indicators from daily_bias when available, falls back to
    the intraday IndicatorSnapshot for basic checks.
    """
    if daily_bias is None:
        return False, "swing_no_daily_data"  # Swing trades need daily context — skip without it

    s = str(strategy or "").strip().upper()
    is_buy = direction == "BUY"

    if s in ("BREAKOUT", "SHORT_BREAKDOWN"):
        # Swing breakout: needs strong daily trend + daily ADX ≥ 25
        if daily_bias.adx_daily < 25:
            return False, "swing_breakout_daily_adx_too_low"
        # Daily trend must align with direction
        if is_buy and daily_bias.trend != "UP":
            return False, "swing_breakout_daily_trend_not_up"
        if not is_buy and daily_bias.trend != "DOWN":
            return False, "swing_breakout_daily_trend_not_down"
        if ind.volume.ratio < 1.3:
            return False, "swing_breakout_volume_insufficient"
        return True, ""

    if s in ("PULLBACK", "SHORT_PULLBACK"):
        # Swing pullback: daily EMA stack intact, daily RSI in reload zone
        if is_buy and not daily_bias.ema_stack:
            return False, "swing_pullback_daily_ema_not_stacked"
        # Swing SHORT_PULLBACK: block only if daily EMA stack is fully bullish
        # (ema_flip required full bear stack — too strict, misses early downtrend entries)
        if not is_buy and daily_bias.ema_stack:
            return False, "swing_pullback_daily_ema_not_flipped"
        if is_buy and not (40 <= daily_bias.rsi_daily <= 60):
            return False, "swing_pullback_daily_rsi_outside_zone"
        # SHORT_PULLBACK RSI: 38–62 (was 45–60, blocking best early-downtrend entries)
        if not is_buy and not (38 <= daily_bias.rsi_daily <= 62):
            return False, "swing_pullback_daily_rsi_outside_zone"
        return True, ""

    if s == "MOMENTUM":
        # Swing relative-strength momentum: buy the strongest stocks in a
        # healthy uptrend and ride them for days/weeks. Distinct from BREAKOUT
        # which requires a tight consolidation + 20-day-high breakout event —
        # MOMENTUM fires on ongoing strength with no base requirement.
        # Only long-side (SELL would be shorting strength = structurally wrong).
        if not is_buy:
            return False, "swing_momentum_sell_not_supported"
        if daily_bias.trend != "UP":
            return False, "swing_momentum_daily_trend_not_up"
        if not daily_bias.ema_stack:
            return False, "swing_momentum_daily_ema_not_stacked"
        # Batch 6.3 (2026-04-23): daily SuperTrend must agree with the EMA
        # stack. ema_stack can be True momentarily while SuperTrend has
        # already flipped down — that's early-distribution, not momentum, and
        # entries here systematically rolled over within 2-3 sessions.
        if daily_bias.supertrend_dir != 1:
            return False, "swing_momentum_daily_supertrend_not_up"
        if daily_bias.adx_daily < 20:
            return False, "swing_momentum_daily_adx_too_low"
        # Batch 6.3: composite trend-strength floor. `strength` is 0-100 and
        # bakes in ADX + EMA spread + RSI distance from 50. <50 means the
        # uptrend is structurally weak even if the individual components
        # each clear their own floor — we were buying tiered-pass but low-
        # conviction names that consistently underperformed the index.
        if float(daily_bias.strength or 0.0) < 50.0:
            return False, "swing_momentum_daily_strength_too_low"
        # RSI 50–75 = momentum zone. <50 means the stock has cooled off (PULLBACK
        # setup, not MOMENTUM). >75 is overbought → poor risk/reward for new entries.
        if not (50 <= daily_bias.rsi_daily <= 75):
            return False, "swing_momentum_daily_rsi_outside_zone"
        # Require at least modest relative-strength confirmation via intraday volume
        if ind.volume.ratio < 1.0:
            return False, "swing_momentum_volume_insufficient"
        return True, ""

    if s in ("MEAN_REVERSION", "VWAP_REVERSAL"):
        # Swing mean-reversion: daily RSI threshold depends on regime
        # RANGE: stock pulled back to lower portion of range → RSI ≤ 45 is good enough
        # Other regimes: need truly stretched daily RSI (≤ 35) for multi-day bounce
        _regime_upper = str(regime or "").strip().upper()
        swing_mr_buy_limit = 45 if _regime_upper in ("RANGE", "CHOP") else 35
        swing_mr_sell_floor = 55 if _regime_upper in ("RANGE", "CHOP") else 65
        if is_buy and daily_bias.rsi_daily > swing_mr_buy_limit:
            return False, "swing_mr_daily_rsi_not_oversold"
        if not is_buy and daily_bias.rsi_daily < swing_mr_sell_floor:
            return False, "swing_mr_daily_rsi_not_overbought"
        # Price should be near daily BB band (use support/resistance as proxy)
        # Support proximity: 10% band (was 3% — too tight, rejected stocks in the
        # bottom 30% of their range that are still 5-10% above the absolute 20-day low)
        if is_buy and daily_bias.support > 0 and ind.close > daily_bias.support * 1.10:
            return False, "swing_mr_price_not_near_support"
        if not is_buy and daily_bias.resistance > 0 and ind.close < daily_bias.resistance * 0.97:
            return False, "swing_mr_price_not_near_resistance"
        return True, ""

    # AUTO, DEFAULT, VWAP_TREND, OPEN_DRIVE — pass through for swing
    return True, ""


def compute_universe_score_breakdown(ind: IndicatorSnapshot) -> tuple[int, dict[str, int]]:
    parts = {"E": 0, "P": 0, "R": 0, "M": 0, "B": 0, "V": 0, "O": 0, "N": 0}

    if ind.ema_stack:
        parts["E"] += 20
    elif ind.ema20_above_ema50:
        parts["E"] += 10
    if ind.above_ema20:
        parts["P"] += 5
    if ind.above_ema50:
        parts["P"] += 5

    rsi = ind.rsi.curr
    if 50 <= rsi <= 65:
        parts["R"] += 15
    elif 40 <= rsi < 50:
        parts["R"] += 8
    elif 65 < rsi <= 75:
        parts["R"] += 5
    if ind.macd.hist > 0:
        parts["M"] += 5
    if ind.macd.crossed == "BUY":
        parts["M"] += 5

    if ind.breakout:
        parts["B"] += 15
    elif ind.near_breakout:
        parts["B"] += 10
    elif ind.dist_from_52w_high < 10:
        parts["B"] += 8

    if ind.volume.ratio >= 1.5:
        parts["V"] += 15
    elif ind.volume.ratio >= 1.2:
        parts["V"] += 10
    elif ind.volume.ratio >= 1.0:
        parts["V"] += 5
    if ind.obv_rising:
        parts["O"] += 5

    if rsi > 80:
        parts["N"] -= 15
    if rsi < 35:
        parts["N"] -= 15
    if ind.patterns.doji:
        parts["N"] -= 5
    if ind.patterns.bear_candle:
        parts["N"] -= 5
    if ind.dist_from_52w_high > 30:
        parts["N"] -= 10

    raw_score = sum(parts.values())
    final = max(0, min(100, round(raw_score)))
    return final, parts


def format_universe_score_calc_short(score: int, parts: dict[str, int], *, priority_bonus: float = 0.0) -> str:
    pb = max(0.0, min(5.0, float(priority_bonus or 0.0)))
    return (
        f"E{int(parts.get('E', 0))}|P{int(parts.get('P', 0))}|R{int(parts.get('R', 0))}|"
        f"M{int(parts.get('M', 0))}|B{int(parts.get('B', 0))}|V{int(parts.get('V', 0))}|"
        f"O{int(parts.get('O', 0))}|N{int(parts.get('N', 0))}|U{pb:g}|S{int(score)}"
    )


def compute_universe_score(ind: IndicatorSnapshot) -> int:
    score, _parts = compute_universe_score_breakdown(ind)
    return score
