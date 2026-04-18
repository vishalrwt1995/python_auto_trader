from __future__ import annotations

from typing import TYPE_CHECKING

from autotrader.domain.models import Direction, IndicatorSnapshot, RegimeSnapshot, ScoreBreakdown, SignalScore
from autotrader.settings import StrategySettings

if TYPE_CHECKING:
    from autotrader.domain.daily_bias import DailyBias


def determine_direction(ind: IndicatorSnapshot, regime: RegimeSnapshot, setup: str = "") -> Direction:
    if regime.regime == "AVOID":
        return "HOLD"
    bull = 0
    bear = 0

    _setup_upper = str(setup or "").strip().upper()
    _is_mr = _setup_upper in ("MEAN_REVERSION", "VWAP_REVERSAL")

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

    if bull > bear + 2:
        return "BUY"
    if bear > bull + 2:
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
    if daily_bias is not None:
        if is_buy:
            if daily_bias.trend == "UP":
                # Perfect alignment: intraday BUY + daily uptrend
                bd.alignment += 15
            elif daily_bias.trend == "NEUTRAL":
                bd.alignment += 5
            else:
                # Counter-trend: intraday BUY against daily downtrend
                bd.alignment -= 10
        else:  # SELL
            if daily_bias.trend == "DOWN":
                bd.alignment += 15
            elif daily_bias.trend == "NEUTRAL":
                bd.alignment += 5
            else:
                bd.alignment -= 10

        # Strength bonus: stronger daily trend = more alignment weight
        if daily_bias.strength >= 70 and bd.alignment > 0:
            bd.alignment = min(15, bd.alignment + 3)
        elif daily_bias.strength < 30 and bd.alignment > 0:
            bd.alignment = max(0, bd.alignment - 3)

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
    _range_ok = {"MEAN_REVERSION", "VWAP_REVERSAL", "VWAP_TREND", "PULLBACK", "SHORT_PULLBACK"}
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
        # Must have trend strength and be near the high (or low for shorts)
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
        return True, ""

    if s in ("PULLBACK", "SHORT_PULLBACK"):
        # Pullback needs the higher-TF trend intact (EMA stack) and RSI in reload zone
        if is_buy and not ind.ema_stack:
            return False, "strategy_pullback_no_bull_ema_stack"
        # SHORT_PULLBACK: require at minimum fast EMA < med EMA (first downtrend signal).
        # ema_flip (fast<med<slow) was too strict — EMAs lag, so full flip only appears
        # well into a downtrend after the best short entry has passed. We block only if
        # stock is in a full BULL stack (wrong direction entirely).
        if not is_buy and ind.ema_stack:
            return False, "strategy_pullback_no_bear_ema_signal"
        rsi = ind.rsi.curr
        if is_buy and not (38 <= rsi <= 65):
            return False, "strategy_pullback_rsi_outside_reload_zone"
        if not is_buy and not (40 <= rsi <= 62):
            return False, "strategy_pullback_rsi_outside_reload_zone"
        # Actual pullback check: price must be near fast EMA support/resistance
        # (within ±3%). If price is >3% above EMA for BUY, it already ran — not a pullback.
        # If price is >3% below EMA for BUY, the trend is broken — not a pullback entry.
        if ind.ema_fast.curr > 0:
            _ema_dist_pct = (ind.close - ind.ema_fast.curr) / ind.ema_fast.curr * 100.0
            if is_buy:
                if _ema_dist_pct > 3.0:
                    return False, "strategy_pullback_price_extended_above_ema"
                if _ema_dist_pct < -3.0:
                    return False, "strategy_pullback_price_broke_below_ema"
            else:
                if _ema_dist_pct < -3.0:
                    return False, "strategy_pullback_price_extended_below_ema"
                if _ema_dist_pct > 3.0:
                    return False, "strategy_pullback_price_broke_above_ema"
        return True, ""

    if s in ("MEAN_REVERSION", "VWAP_REVERSAL"):
        # VWAP Reversal / Mean Reversion — proper institutional entry gates:
        #
        # BUY (fade the selloff):
        #   - Price must be BELOW VWAP (stretched down, expecting bounce back to VWAP)
        #   - RSI must be oversold: ≤ 40 in RANGE, ≤ 35 in trending regimes
        #
        # SELL (fade the rally):
        #   - Price must be ABOVE VWAP (stretched up, expecting reversion back to VWAP)
        #   - RSI must be overbought: ≥ 65 in RANGE, ≥ 60 in trending regimes
        #
        # Extension: price must be ≥ 1.5% from VWAP (was 0.5% — too close to noise)
        # A 0.5% deviation is inside normal intraday bid/ask noise and produces
        # false signals. Real reversions need at least 1.5% stretch.
        rsi = ind.rsi.curr
        _regime_upper = str(regime or "").strip().upper()
        _is_range_like = _regime_upper in ("RANGE", "CHOP")

        if is_buy:
            # BUY reversal: price must be below VWAP (oversold stretch)
            if ind.vwap > 0 and ind.close >= ind.vwap:
                return False, "strategy_mr_buy_price_not_below_vwap"
            rsi_limit = 40 if _is_range_like else 35
            if rsi > rsi_limit:
                return False, "strategy_mr_rsi_not_oversold"
        else:
            # SELL reversal: price must be above VWAP (overbought stretch)
            if ind.vwap > 0 and ind.close <= ind.vwap:
                return False, "strategy_mr_sell_price_not_above_vwap"
            rsi_floor = 65 if _is_range_like else 60
            if rsi < rsi_floor:
                return False, "strategy_mr_rsi_not_overbought"

        if ind.vwap > 0:
            vwap_dev = abs(ind.close - ind.vwap) / ind.vwap * 100
            if vwap_dev < 1.0:
                return False, "strategy_mr_insufficient_vwap_extension"
        return True, ""

    if s == "VWAP_TREND":
        # Price must be on the correct side of VWAP
        if is_buy and ind.close <= ind.vwap:
            return False, "strategy_vwap_trend_price_below_vwap"
        if not is_buy and ind.close >= ind.vwap:
            return False, "strategy_vwap_trend_price_above_vwap"
        # Needs moderate trend strength
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

    # OPEN_DRIVE, AUTO, DEFAULT, etc. — no extra gate
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
