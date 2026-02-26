from __future__ import annotations

from autotrader.domain.models import Direction, IndicatorSnapshot, RegimeSnapshot, ScoreBreakdown, SignalScore
from autotrader.settings import StrategySettings


def determine_direction(ind: IndicatorSnapshot, regime: RegimeSnapshot) -> Direction:
    if regime.regime == "AVOID":
        return "HOLD"
    bull = 0
    bear = 0

    bull += 3 if ind.supertrend.dir == 1 else 0
    bear += 3 if ind.supertrend.dir != 1 else 0
    bull += 2 if ind.close > ind.vwap else 0
    bear += 2 if ind.close <= ind.vwap else 0
    bull += 2 if ind.ema_fast.curr > ind.ema_med.curr else 0
    bear += 2 if ind.ema_fast.curr <= ind.ema_med.curr else 0
    bull += 1 if ind.ema_med.curr > ind.ema_slow.curr else 0
    bear += 1 if ind.ema_med.curr <= ind.ema_slow.curr else 0
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
) -> SignalScore:
    bd = ScoreBreakdown()
    if direction == "HOLD" or regime.regime == "AVOID":
        return SignalScore(score=0, direction=direction, breakdown=bd)

    score = 0.0
    is_buy = direction == "BUY"

    # Layer 1: Regime (25)
    if (is_buy and regime.nifty.change_pct > 0.1) or ((not is_buy) and regime.nifty.change_pct < -0.1):
        bd.regime += 10
    elif abs(regime.nifty.change_pct) < 0.1:
        bd.regime += 5

    if regime.vix < cfg.vix_trend_max:
        bd.regime += 8
    elif regime.vix < cfg.vix_safe_max:
        bd.regime += 4

    if (is_buy and regime.fii.fii > 500) or ((not is_buy) and regime.fii.fii < -500):
        bd.regime += 7
    elif abs(regime.fii.fii) < 500:
        bd.regime += 3
    score += bd.regime

    # Layer 2: Options (20)
    if (is_buy and regime.pcr.pcr >= cfg.pcr_bull_min) or ((not is_buy) and regime.pcr.pcr <= cfg.pcr_bear_max):
        bd.options += 10
    else:
        bd.options += 3
    if regime.pcr.max_pain > 0:
        mp = regime.pcr.max_pain
        if (is_buy and ind.close > mp * 0.998) or ((not is_buy) and ind.close < mp * 1.002):
            bd.options += 10
        else:
            bd.options += 4
    else:
        bd.options += 5
    score += bd.options

    # Layer 3: Technical (40)
    if ind.supertrend.fresh and ((is_buy and ind.supertrend.dir == 1) or ((not is_buy) and ind.supertrend.dir == -1)):
        bd.technical += 10
    elif (is_buy and ind.supertrend.dir == 1) or ((not is_buy) and ind.supertrend.dir == -1):
        bd.technical += 6

    if (is_buy and ind.close > ind.vwap) or ((not is_buy) and ind.close < ind.vwap):
        bd.technical += 8

    if is_buy:
        if ind.ema_fast.curr > ind.ema_med.curr > ind.ema_slow.curr:
            bd.technical += 7
        elif ind.ema_fast.curr > ind.ema_med.curr:
            bd.technical += 4
        elif ind.ema_fast.curr > ind.ema_fast.prev:
            bd.technical += 2
    else:
        if ind.ema_fast.curr < ind.ema_med.curr < ind.ema_slow.curr:
            bd.technical += 7
        elif ind.ema_fast.curr < ind.ema_med.curr:
            bd.technical += 4
        elif ind.ema_fast.curr < ind.ema_fast.prev:
            bd.technical += 2

    rsi = ind.rsi.curr
    if (is_buy and cfg.rsi_buy_min <= rsi <= cfg.rsi_buy_max) or ((not is_buy) and cfg.rsi_sell_min <= rsi <= cfg.rsi_sell_max):
        bd.technical += 7
    elif (is_buy and rsi > ind.rsi.prev and rsi < cfg.rsi_buy_max) or ((not is_buy) and rsi < ind.rsi.prev and rsi > cfg.rsi_sell_min):
        bd.technical += 3

    if (ind.macd.crossed == "BUY" and is_buy) or (ind.macd.crossed == "SELL" and (not is_buy)):
        bd.technical += 8
    elif (is_buy and ind.macd.hist > 0) or ((not is_buy) and ind.macd.hist < 0):
        bd.technical += 4

    if (is_buy and ind.patterns.bull_engulf) or ((not is_buy) and ind.patterns.bear_engulf):
        bd.technical = min(40, bd.technical + 2)
    bd.technical = min(40, bd.technical)
    score += bd.technical

    # Layer 4: Volume (15)
    if ind.volume.ratio >= cfg.vol_mult:
        bd.volume += 10
    elif ind.volume.ratio >= 1.2:
        bd.volume += 6
    elif ind.volume.ratio >= 1.0:
        bd.volume += 3
    if (is_buy and ind.obv_curr > ind.obv_prev) or ((not is_buy) and ind.obv_curr < ind.obv_prev):
        bd.volume += 5
    score += bd.volume

    # Penalties
    if regime.vix > 18:
        bd.penalty -= 10
    if regime.regime == "RANGE":
        bd.penalty -= 8
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

    if ind.near_breakout:
        parts["B"] += 10
    if ind.breakout:
        parts["B"] += 15
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
