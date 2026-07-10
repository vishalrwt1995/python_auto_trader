"""Momentum x Low-Vol channel signal logic — monthly cross-sectional momentum tilted by low
volatility, buy-and-HOLD, with a Nifty-100DMA regime overlay + a hysteresis (turnover) buffer.
Single source of truth shared by the production rebalance service AND the backtest
(scripts/redesign/factor_*.py) so production can never drift from the validated config — same
fidelity discipline as domain/core_signals + gap_fade_signals.

WHAT IT IS: a monthly-rebalanced, long-only equity basket. Rank the >=Rs10cr-turnover liquid NSE
universe by a momentum(12-1) + low-vol(126d) RANK-BLEND, hold the top-20 equal-weight, BUFFER the
turnover (keep a held name until it drops out of the top-30, not top-20), and go to CASH when
Nifty closes below its 100-day SMA. Distinct from CORE (quarterly, F&O large-cap, no overlay):
this is MONTHLY, BROADER (mid+large), and REGIME-TIMED — 0.23 position overlap with CORE.

VALIDATED (survivorship-safe full-universe daily 2015-2026, net of delivery cost, DAILY-marked):
~14% CAGR / ~-16% maxDD / Calmar ~0.85 / Sharpe ~1.3, both IS+OOS positive, walk-forward-stable
(same params train-optimal in every fold). Regime-dependent: ~5-8% in momentum-hostile stretches
(2018-2020), ~20% when momentum works. Full grind: scripts/redesign/factor_{recon,stress,
deepdive,push,push2,max,walkforward}.py.

All functions PURE, fail-closed. rank_blend_select() is what the fidelity replay reproduces."""
from __future__ import annotations

import statistics
from typing import Any, Sequence

# ── Validated config (do NOT change without a fresh walk-forward / risk-adjusted sweep) ──
TOPN: int = 20                     # hold top-20 blend names, equal-weight
MOM_LOOKBACK: int = 252            # 12-month momentum window
MOM_SKIP: int = 21                 # skip the most recent month (standard 12-1 momentum)
VOL_LOOKBACK: int = 126            # realized-vol window (6m) — the low-vol leg
TURNOVER_LOOKBACK: int = 60        # median-turnover window
TURNOVER_MIN: float = 1e8          # >= Rs10cr 60d MEDIAN turnover (the fillable mid+large tier)
PRICE_MIN: float = 30.0            # price floor
BUFFER_MULT: float = 1.5           # hysteresis: keep a held name until it exits top-(TOPN*1.5)=top-30
REGIME_MA: int = 100               # Nifty regime SMA window: hold only when close > 100DMA

# Buy-and-HOLD like CORE: the validated backtest has no per-name stop, but order_service
# hard-rejects a BUY without 0<sl<entry<target. So carry a deep CATASTROPHE stop + unreachable
# target — protective (fraud/halt/delisting) only, ~0 fidelity impact, never taken. (Copied
# intent from core_signals: a rebalancing buy-and-hold basket exits only at the monthly rebalance.)
CATASTROPHE_STOP_PCT: float = 0.60     # SL at entry*(1-0.60); backstop only
UNREACHABLE_TARGET_MULT: float = 10.0  # target at entry*(1+10) = +1000%, never hit
MAX_WEIGHT_MULT: float = 1.5           # residual-cash sweep: a name may hold up to 1.5x its slice


def catastrophe_stop(entry: float) -> float:
    """Deep protective SL for a HOLD buy (entry*(1-CATASTROPHE_STOP_PCT)); satisfies the live
    entry-order 0<sl<entry contract for an otherwise stopless monthly-rebalanced hold."""
    return round(entry * (1.0 - CATASTROPHE_STOP_PCT), 2)


def unreachable_target(entry: float) -> float:
    """Unreachable target (entry*(1+UNREACHABLE_TARGET_MULT)); the channel never profit-takes,
    it HOLDS to the monthly rebalance. Satisfies the entry-order target>entry contract."""
    return round(entry * (1.0 + UNREACHABLE_TARGET_MULT), 2)


def momentum_score(closes: Sequence[float], idx: int,
                   lookback: int = MOM_LOOKBACK, skip: int = MOM_SKIP) -> float | None:
    """12-1 momentum = ``close[idx-skip] / close[idx-lookback] - 1``. None on bad history."""
    if idx - lookback < 0 or idx - skip < 0:
        return None
    base = closes[idx - lookback]
    if base is None or base <= 0 or closes[idx - skip] is None or closes[idx - skip] <= 0:
        return None
    return closes[idx - skip] / base - 1.0


def realized_vol(rets: Sequence[float], idx: int, window: int = VOL_LOOKBACK) -> float | None:
    """Trailing realized vol (stdev of daily returns over ``window``). None on short history."""
    if idx - window < 0:
        return None
    seg = rets[idx - window:idx]
    return statistics.pstdev(seg) if len(seg) >= 2 else None


def median_turnover(closes: Sequence[float], vols: Sequence[float], idx: int,
                    window: int = TURNOVER_LOOKBACK) -> float | None:
    """Median daily turnover (close*volume) over the trailing ``window`` (fillability gate)."""
    if idx - window < 0:
        return None
    seg = [closes[k] * vols[k] for k in range(idx - window, idx)]
    return statistics.median(seg) if seg else None


def passes_universe_gates(price: float, turnover_med: float, has_history: bool,
                          price_min: float = PRICE_MIN, turnover_min: float = TURNOVER_MIN) -> bool:
    """Eligibility: price floor + >=Rs10cr median turnover (fillable) + enough history. Fail-closed."""
    if not has_history or price is None or turnover_med is None:
        return False
    return price >= price_min and turnover_med >= turnover_min


def nifty_regime_ok(nifty_closes: Sequence[float], ma_window: int = REGIME_MA) -> bool:
    """Regime overlay: True (HOLD the basket) iff the latest Nifty close > its ``ma_window`` SMA;
    False (go to CASH) when at/below. Fail-closed: too little history -> False (don't deploy)."""
    if not nifty_closes or len(nifty_closes) < ma_window:
        return False
    ma = sum(nifty_closes[-ma_window:]) / float(ma_window)
    last = nifty_closes[-1]
    return last is not None and ma > 0 and last > ma


def rank_blend_select(candidates: Sequence[dict[str, Any]],
                      prev_holds: Sequence[str] = (),
                      topn: int = TOPN, buffer_mult: float = BUFFER_MULT,
                      regime_ok: bool = True) -> list[str]:
    """PURE selection. ``candidates`` = ``[{symbol, momentum, vol, turnover}]`` (already
    universe-gated to >=Rs10cr). Steps (exactly matching the validated backtest):
      1. regime overlay: if ``regime_ok`` is False -> return [] (go to CASH);
      2. rank-blend: momentum-descending rank + vol-ascending rank, sort by the summed rank
         (high momentum AND low vol first);
      3. hysteresis buffer: KEEP a name from ``prev_holds`` if it's still within the
         top-(topn*buffer_mult); then fill the remaining slots from the top of the blended order,
         up to ``topn``. This cuts turnover/cost and rides winners (validated +Calmar)."""
    if not regime_ok:
        return []
    elig = [c for c in candidates if c.get("momentum") is not None and c.get("vol") and c["vol"] > 0]
    if len(elig) < topn:
        return []
    mom_rank = {c["symbol"]: r for r, c in enumerate(sorted(elig, key=lambda c: -c["momentum"]))}
    vol_rank = {c["symbol"]: r for r, c in enumerate(sorted(elig, key=lambda c: c["vol"]))}
    order = [str(c["symbol"]).strip().upper()
             for c in sorted(elig, key=lambda c: mom_rank[c["symbol"]] + vol_rank[c["symbol"]])]
    rankpos = {s: i for i, s in enumerate(order)}
    prev = {str(s).strip().upper() for s in prev_holds}
    buf_cut = int(topn * float(buffer_mult))
    sel: list[str] = []
    if buffer_mult > 1.0:                       # keep stayers still within the buffer band
        for s in order:
            if len(sel) >= topn:
                break
            if s in prev and rankpos[s] < buf_cut:
                sel.append(s)
    for s in order:                             # fill remaining slots from the top
        if len(sel) >= topn:
            break
        if s not in sel:
            sel.append(s)
    return sel


def target_weights(symbols: Sequence[str]) -> dict[str, float]:
    """Equal-weight target ``{symbol: 1/n}``. Empty for an empty basket (cash)."""
    n = len(symbols)
    if n <= 0:
        return {}
    return {str(s).strip().upper(): 1.0 / n for s in symbols}


def position_qty(price: float, capital: float, weight: float) -> int:
    """Share qty for a target weight = ``floor(weight * capital / price)``. 0 on bad input."""
    if price is None or price <= 0 or capital is None or capital <= 0 or weight <= 0:
        return 0
    return int((weight * capital) // price)
