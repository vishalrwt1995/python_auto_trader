from __future__ import annotations

import math
from typing import Iterable

from autotrader.domain.models import (
    BollingerState,
    Candle,
    IndicatorLine,
    IndicatorSnapshot,
    MacdState,
    Patterns,
    StochasticState,
    SuperTrendState,
    VolumeState,
)
from autotrader.settings import StrategySettings


def _as_candle(row: Iterable[object]) -> Candle | None:
    try:
        vals = list(row)
        if len(vals) < 6:
            return None
        ts = str(vals[0]).strip()
        o = float(vals[1])
        h = float(vals[2])
        l = float(vals[3])
        c = float(vals[4])
        v = float(vals[5])
        if not ts:
            return None
        return (ts, o, h, l, c, v)
    except Exception:
        return None


def normalize_candles(candles: Iterable[Iterable[object]]) -> list[Candle]:
    out: list[Candle] = []
    for row in candles:
        c = _as_candle(row)
        if c is not None:
            out.append(c)
    return out


def calc_ema(data: list[float], period: int) -> list[float]:
    if not data:
        return []
    if len(data) < period:
        return [data[0]] * len(data)
    k = 2 / (period + 1)
    ema = [sum(data[:period]) / period]
    for i in range(period, len(data)):
        ema.append(data[i] * k + ema[-1] * (1 - k))
    pad = len(data) - len(ema)
    return [ema[0]] * pad + ema


def calc_rsi(closes: list[float], period: int = 14) -> list[float]:
    if len(closes) < period + 1:
        return [50.0] * len(closes)
    ag = 0.0
    al = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        if d > 0:
            ag += d
        else:
            al -= d
    ag /= period
    al /= period
    rsis = [100 - (100 / (1 + ag / (al or 0.001)))]
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        ag = (ag * (period - 1) + (d if d > 0 else 0)) / period
        al = (al * (period - 1) + (-d if d < 0 else 0)) / period
        rsis.append(100 - (100 / (1 + ag / (al or 0.001))))
    return rsis


def calc_macd(closes: list[float]) -> tuple[list[float], list[float], list[float]]:
    e12 = calc_ema(closes, 12)
    e26 = calc_ema(closes, 26)
    macd_line = [a - b for a, b in zip(e12, e26)]
    signal = calc_ema(macd_line, 9)
    hist = [m - s for m, s in zip(macd_line, signal)]
    return macd_line, signal, hist


def calc_atr(candles: list[Candle], period: int = 14) -> float:
    trs: list[float] = []
    for i in range(1, len(candles)):
        h = candles[i][2]
        l = candles[i][3]
        pc = candles[i - 1][4]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(trs) < period:
        return trs[-1] if trs else 1.0
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr


def calc_adx(candles: list[Candle], period: int = 14) -> float:
    """Compute ADX using Wilder's smoothing. Returns last ADX value (0-100)."""
    if len(candles) < period * 2 + 1:
        return 25.0
    plus_dm_list: list[float] = []
    minus_dm_list: list[float] = []
    tr_list: list[float] = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i][2], candles[i][3], candles[i - 1][4]
        ph, pl = candles[i - 1][2], candles[i - 1][3]
        up_move = h - ph
        down_move = pl - l
        plus_dm_list.append(up_move if (up_move > down_move and up_move > 0) else 0.0)
        minus_dm_list.append(down_move if (down_move > up_move and down_move > 0) else 0.0)
        tr_list.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(tr_list) < period:
        return 25.0
    s_tr = sum(tr_list[:period])
    s_plus = sum(plus_dm_list[:period])
    s_minus = sum(minus_dm_list[:period])
    dx_list: list[float] = []
    for i in range(period, len(tr_list)):
        s_tr = s_tr - (s_tr / period) + tr_list[i]
        s_plus = s_plus - (s_plus / period) + plus_dm_list[i]
        s_minus = s_minus - (s_minus / period) + minus_dm_list[i]
        if s_tr == 0:
            continue
        plus_di = 100.0 * s_plus / s_tr
        minus_di = 100.0 * s_minus / s_tr
        di_sum = plus_di + minus_di
        dx_list.append(100.0 * abs(plus_di - minus_di) / di_sum if di_sum > 0 else 0.0)
    if not dx_list:
        return 25.0
    adx = sum(dx_list[:period]) / min(period, len(dx_list))
    for dx in dx_list[period:]:
        adx = (adx * (period - 1) + dx) / period
    return round(adx, 2)


def calc_supertrend(candles: list[Candle], atr_p: int = 10, mult: float = 3.0) -> tuple[list[float | None], list[int]]:
    closes = [c[4] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    atrs: list[float] = []
    run_atr: float | None = None
    for i in range(1, len(candles)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        run_atr = tr if run_atr is None else (run_atr * (atr_p - 1) + tr) / atr_p
        atrs.append(run_atr)

    vals: list[float | None] = [None] * len(candles)
    dirs: list[int] = [1] * len(candles)
    for i in range(1, len(candles)):
        if i - 1 >= len(atrs):
            break
        mid = (highs[i] + lows[i]) / 2
        up = mid + mult * atrs[i - 1]
        dn = mid - mult * atrs[i - 1]
        if i == 1:
            vals[i] = up
            dirs[i] = 1
            continue
        prev_val = vals[i - 1] if vals[i - 1] is not None else up
        if dirs[i - 1] == 1:
            vals[i] = up if closes[i] < prev_val else min(up, prev_val)
            dirs[i] = -1 if closes[i] < (vals[i] or up) else 1
        else:
            prev_val = vals[i - 1] if vals[i - 1] is not None else dn
            vals[i] = dn if closes[i] > prev_val else max(dn, prev_val)
            dirs[i] = 1 if closes[i] > (vals[i] or dn) else -1
    return vals, dirs


def calc_vwap(candles: list[Candle]) -> list[float]:
    """Compute VWAP, resetting cumulative sums at each new trading day.

    The timestamp in each candle (index 0) is expected to be an ISO-8601
    string.  The first 10 characters give the YYYY-MM-DD date, which is
    used to detect day boundaries.  Without this reset the VWAP drifts
    across multiple days and gives incorrect intraday signals.
    """
    cvp = 0.0
    cv = 0.0
    out: list[float] = []
    prev_date = ""
    for c in candles:
        # Detect day boundary and reset cumulative sums
        ts_date = str(c[0])[:10]
        if ts_date != prev_date:
            cvp = 0.0
            cv = 0.0
            prev_date = ts_date
        tp = (c[2] + c[3] + c[4]) / 3
        cvp += tp * c[5]
        cv += c[5]
        out.append(cvp / cv if cv > 0 else c[4])
    return out


def calc_obv(closes: list[float], volumes: list[float]) -> list[float]:
    obv = [0.0]
    for i in range(1, len(closes)):
        if closes[i] > closes[i - 1]:
            obv.append(obv[i - 1] + volumes[i])
        elif closes[i] < closes[i - 1]:
            obv.append(obv[i - 1] - volumes[i])
        else:
            obv.append(obv[i - 1])
    return obv


def calc_bb(closes: list[float], period: int = 20, mult: float = 2.0) -> list[BollingerState]:
    out: list[BollingerState] = []
    for i in range(period - 1, len(closes)):
        sl = closes[i - period + 1 : i + 1]
        mean = sum(sl) / period
        std = math.sqrt(sum((v - mean) ** 2 for v in sl) / period)
        out.append(BollingerState(upper=mean + mult * std, mid=mean, lower=mean - mult * std))
    return out


def calc_stochastic(candles: list[Candle], k_p: int = 14, d_p: int = 3) -> tuple[list[float], list[float]]:
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    k_vals: list[float] = []
    for i in range(k_p - 1, len(closes)):
        hh = max(highs[i - k_p + 1 : i + 1])
        ll = min(lows[i - k_p + 1 : i + 1])
        k_vals.append(50.0 if hh == ll else ((closes[i] - ll) / (hh - ll) * 100))
    return k_vals, calc_ema(k_vals, d_p) if k_vals else []


def compute_indicators(candles: list[Candle], cfg: StrategySettings) -> IndicatorSnapshot | None:
    candles = normalize_candles(candles)
    if len(candles) < 80:
        return None

    closes = [c[4] for c in candles]
    volumes = [c[5] for c in candles]
    n = len(closes) - 1
    prev_n = max(0, n - 1)

    ema_f = calc_ema(closes, cfg.ema_fast)
    ema_m = calc_ema(closes, cfg.ema_med)
    ema_s = calc_ema(closes, cfg.ema_slow)
    ema20 = calc_ema(closes, 20)
    ema50 = calc_ema(closes, 50)
    rsi = calc_rsi(closes, cfg.rsi_period)
    macd_line, macd_sig, macd_hist = calc_macd(closes)
    st_vals, st_dirs = calc_supertrend(candles, 10, 3)
    vwap = calc_vwap(candles)
    obv = calc_obv(closes, volumes)
    atr = calc_atr(candles, 14)
    bb = calc_bb(closes, 20, 2)
    stoch_k, stoch_d = calc_stochastic(candles, 14, 3)

    vol_slice = volumes[max(0, n - 20) : n]
    avg_vol = (sum(vol_slice) / max(1, min(20, n))) if n > 0 else volumes[n]
    max_ref = max(closes[max(0, n - 251) : n + 1])
    dist_from_52w_high = ((max_ref - closes[n]) / max_ref) * 100 if max_ref > 0 else 0
    near_breakout = closes[n] >= max_ref * 0.98
    breakout = closes[n] >= max_ref * 0.999
    above_ema20 = closes[n] > ema20[n]
    above_ema50 = closes[n] > ema50[n]
    ema20_above_ema50 = ema20[n] > ema50[n]
    ema_stack = ema_f[n] > ema_m[n] > ema_s[n]
    ema_flip = ema_f[n] < ema_m[n] < ema_s[n]
    obv_rising = obv[n] > obv[n - 1] if n > 0 else False

    rng = (candles[n][2] - candles[n][3]) + 0.001
    doji = abs(candles[n][1] - candles[n][4]) / rng < 0.1
    bull_engulf = (
        n > 0
        and candles[n][4] > candles[n][1]
        and candles[n - 1][4] < candles[n - 1][1]
        and candles[n][4] > candles[n - 1][1]
        and candles[n][1] < candles[n - 1][4]
    )
    bear_engulf = (
        n > 0
        and candles[n][4] < candles[n][1]
        and candles[n - 1][4] > candles[n - 1][1]
        and candles[n][4] < candles[n - 1][1]
        and candles[n][1] > candles[n - 1][4]
    )
    bear_candle = candles[n][4] < candles[n][1]

    macd_cross = None
    prev_hist = macd_hist[n - 1] if n > 0 and n - 1 < len(macd_hist) else 0
    if macd_hist[n] > 0 and prev_hist <= 0:
        macd_cross = "BUY"
    elif macd_hist[n] < 0 and prev_hist >= 0:
        macd_cross = "SELL"

    return IndicatorSnapshot(
        close=closes[n],
        prev_close=closes[n - 1] if n > 0 else closes[n],
        open=candles[n][1],
        high=candles[n][2],
        low=candles[n][3],
        ema_fast=IndicatorLine(curr=ema_f[n], prev=ema_f[prev_n]),
        ema_med=IndicatorLine(curr=ema_m[n], prev=ema_m[prev_n]),
        ema_slow=IndicatorLine(curr=ema_s[n], prev=ema_s[prev_n]),
        ema20=IndicatorLine(curr=ema20[n], prev=ema20[prev_n]),
        ema50=IndicatorLine(curr=ema50[n], prev=ema50[prev_n]),
        rsi=IndicatorLine(curr=rsi[-1], prev=rsi[-2] if len(rsi) > 1 else 50.0),
        macd=MacdState(
            macd=macd_line[n],
            signal=macd_sig[n],
            hist=macd_hist[n],
            prev_hist=prev_hist,
            crossed=macd_cross,
        ),
        supertrend=SuperTrendState(
            value=st_vals[n],
            dir=st_dirs[n],
            prev_dir=st_dirs[n - 1] if n > 0 else st_dirs[n],
            fresh=(st_dirs[n] != st_dirs[n - 1]) if n > 0 else False,
        ),
        vwap=vwap[n],
        obv_curr=obv[n],
        obv_prev=obv[n - 1] if n > 0 else obv[n],
        atr=atr,
        bb=bb[-1] if bb else None,
        stoch=StochasticState(k=(stoch_k[-1] if stoch_k else 50.0), d=(stoch_d[-1] if stoch_d else 50.0)),
        volume=VolumeState(curr=volumes[n], avg=avg_vol, ratio=(volumes[n] / avg_vol if avg_vol > 0 else 0.0)),
        ema_stack=ema_stack,
        ema_flip=ema_flip,
        ema20_above_ema50=ema20_above_ema50,
        above_ema20=above_ema20,
        above_ema50=above_ema50,
        near_breakout=near_breakout,
        breakout=breakout,
        dist_from_52w_high=dist_from_52w_high,
        obv_rising=obv_rising,
        patterns=Patterns(
            doji=doji,
            bull_engulf=bull_engulf,
            bear_engulf=bear_engulf,
            bear_candle=bear_candle,
        ),
        candles=candles,
    )

