"""CORE channel signal logic — large-cap momentum + low-vol blend, quarterly buy-and-HOLD.
Single source of truth for the CORE channel, shared by the production rebalance service AND
the backtest so production can never drift from the validated backtest (same fidelity
discipline as domain/gap_fade_signals + corp_action_signals).

WHAT IT IS: the CORE is the system's long-biased BETA engine — it captures the equity risk
premium + a momentum/low-vol tilt by HOLDING (not trading). Every quarter: rank the large-cap
universe, buy the top-30 blend names equal-weight, hold the quarter, rebalance. Long-only,
cash (CNC). It is deliberately NOT a thin-alpha trading channel — it owns the market, tilted.

VALIDATED (deep daily 2010-2026, large-cap, net of quarterly turnover cost): the mom+low-vol
rank-blend is the best RISK-ADJUSTED long-only stock config found (CAGR ~11% / maxDD ~-35% /
Calmar 0.32; survivor-inflated -> real ~9-10%). Pure momentum returns more (13.6%) but draws
down deeper (-46%); the low-vol blend trades ~2% return for materially lower drawdown.

HONEST: this is BETA — the ~10% comes WITH a -35-40% drawdown (deeper in a 2008-type crash).
Stock-only has no way to remove that (proven); it is the price of the return. Size to tolerance.

All functions PURE. Fail-closed. The selection (rank_blend_select) is what the backtest
fidelity-replay reproduces."""
from __future__ import annotations

import statistics
from typing import Any, Sequence

# ── Validated config (do not change without a fresh OOS/risk-adjusted sweep) ─────
UNIVERSE_TOP: int = 100            # large-cap = top-N by 20d turnover (delisting~0, less survivorship)
TOPN: int = 30                     # hold top-30 blend names, equal-weight
MOM_LOOKBACK: int = 252            # 12-month momentum window
MOM_SKIP: int = 21                 # skip most recent month (standard 12-1 momentum)
VOL_LOOKBACK: int = 60             # realized-vol window (the low-vol leg)
TURNOVER_MIN: float = 1e8          # 20d avg turnover floor (Rs10cr — liquid)
PRICE_MIN: float = 30.0            # price floor

# CORE is a pure buy-and-HOLD: the validated backtest had NO stop. But the live
# order path (order_service.place_entry_order) hard-rejects a BUY without a valid
# 0<sl<entry<target. So CORE carries a CATASTROPHE stop far below entry — deep
# enough to never interfere with the strategy's designed -35-50% single-name
# drawdowns, but to still exit a fraud/halt/delisting collapse the survivor-biased
# backtest never saw. This is a deliberate, protective, ~0-fidelity-impact safe
# deviation (a -60% intraday move on a top-100-by-turnover large-cap is a disaster
# you WANT out of). The target is set unreachably high (no profit-taking — it HOLDS
# to the quarterly rebalance). Both exist only to satisfy the entry-order contract.
CATASTROPHE_STOP_PCT: float = 0.60  # SL at entry*(1-0.60); fraud/halt backstop only
UNREACHABLE_TARGET_MULT: float = 10.0  # target at entry*(1+10) = +1000%, never hit

# Residual-cash sweep (fixes the ~18% idle-cash drag at small capital). At capital/TOPN
# per name, integer-share sizing both skips names priced > the slice AND leaves big
# rounding residuals. The sweep deploys the leftover cash greedily into the most-underweight
# names, and admits names priced up to MAX_WEIGHT_MULT * slice (1 share). The cap stops any
# single name (e.g. a ₹25k+ large-cap that can't be equal-weighted at ₹3L) from ballooning.
MAX_WEIGHT_MULT: float = 1.5       # a name may hold up to 1.5x its equal-weight slice


def catastrophe_stop(entry: float) -> float:
    """SL price for a CORE BUY: a deep catastrophe backstop (entry*(1-CATASTROPHE_STOP_PCT)).
    Not a trading stop — only fires on a fraud/halt-scale collapse. Satisfies the live
    entry-order's ``0<sl<entry`` contract for an otherwise stopless buy-and-hold."""
    return round(entry * (1.0 - CATASTROPHE_STOP_PCT), 2)


def unreachable_target(entry: float) -> float:
    """Target price for a CORE BUY: unreachably high (entry*(1+UNREACHABLE_TARGET_MULT)).
    CORE never takes profit — it HOLDS to the quarterly rebalance. Satisfies the live
    entry-order's ``target>entry`` contract without ever triggering a target exit."""
    return round(entry * (1.0 + UNREACHABLE_TARGET_MULT), 2)


def momentum_score(closes: Sequence[float], idx: int,
                   lookback: int = MOM_LOOKBACK, skip: int = MOM_SKIP) -> float | None:
    """12-1 momentum = ``close[idx-skip] / close[idx-lookback] - 1``. None on bad history."""
    if idx - lookback < 0 or idx - skip < 0:
        return None
    base = closes[idx - lookback]
    if base is None or base <= 0 or closes[idx - skip] <= 0:
        return None
    return closes[idx - skip] / base - 1.0


def realized_vol(rets: Sequence[float], idx: int, window: int = VOL_LOOKBACK) -> float | None:
    """Trailing realized vol (stdev of daily returns over ``window``). None on short history."""
    if idx - window < 0:
        return None
    seg = rets[idx - window:idx]
    return statistics.pstdev(seg) if len(seg) >= 2 else None


def passes_universe_gates(price: float, turnover_20d: float, has_history: bool,
                          price_min: float = PRICE_MIN, turnover_min: float = TURNOVER_MIN) -> bool:
    """Eligibility for the CORE universe: price floor, liquidity, enough history. Fail-closed."""
    if not has_history or price is None or turnover_20d is None:
        return False
    return price >= price_min and turnover_20d >= turnover_min


def rank_blend_select(candidates: Sequence[dict[str, Any]],
                      topn: int = TOPN, universe_top: int = UNIVERSE_TOP) -> list[str]:
    """PURE selection. ``candidates`` = ``[{symbol, momentum, vol, turnover}]`` (already
    universe-gated). Restrict to the ``universe_top`` most-liquid (large-cap), then rank-blend
    (momentum descending rank + vol ascending rank — high momentum AND low vol), return the
    top-``topn`` symbols. This is the validated best risk-adjusted construction."""
    elig = [c for c in candidates if c.get("momentum") is not None and c.get("vol") and c["vol"] > 0]
    if len(elig) < topn:
        return []
    large = sorted(elig, key=lambda c: -float(c["turnover"]))[:universe_top]
    if len(large) < topn:
        return []
    mom_rank = {c["symbol"]: r for r, c in enumerate(sorted(large, key=lambda c: -c["momentum"]))}
    vol_rank = {c["symbol"]: r for r, c in enumerate(sorted(large, key=lambda c: c["vol"]))}
    blended = sorted(large, key=lambda c: mom_rank[c["symbol"]] + vol_rank[c["symbol"]])
    return [str(c["symbol"]).strip().upper() for c in blended[:topn]]


def target_weights(symbols: Sequence[str]) -> dict[str, float]:
    """Equal-weight target for the basket. ``{symbol: 1/n}``. Empty for an empty basket."""
    n = len(symbols)
    if n <= 0:
        return {}
    return {str(s).strip().upper(): 1.0 / n for s in symbols}


def position_qty(price: float, capital: float, weight: float) -> int:
    """Share qty for a target weight = ``floor(weight * capital / price)``. 0 on bad input."""
    if price is None or price <= 0 or capital is None or capital <= 0 or weight <= 0:
        return 0
    return int((weight * capital) // price)
