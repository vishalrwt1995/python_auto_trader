"""Gap-fade short signal logic — single source of truth for the GAP_FADE channel,
shared by the production signal service AND the backtest so production can never drift
from the validated backtest (same ``domain/corp_action_signals.py`` fidelity discipline).

Validated config — deep daily 2010-2026 + 1m intraday-path study 2022-2026, net of cost
(build 2026-06-21; see ``docs/GAP_FADE_CHANNEL_PLAN.md``):
  - SIGNAL: an NSE **F&O** stock that **gaps up > GAP_MIN (5%)** at the open. Short it at
    the open; cover the same day at the close (intraday MIS, no overnight risk). It is the
    system's first *validated* systematic short — anti-correlated with the long book.
  - UNIVERSE — F&O only: F&O underlyings are reliably MIS-shortable and have wide/relaxing
    circuit bands (so they rarely lock limit-up -> you can cover). The edge survives the
    restriction (>5% gap: OOS +0.9-1.3%/trade vs +1.0-2.5% on all-liquid). Membership is
    supplied by the caller (Upstox NSE_FO FUT underlyings).
  - GATE — liquidity (20d avg turnover >= TURNOVER_MIN) + price floor + NOT locked-limit
    (no intraday range yet = circuit/halt -> unshortable, skip).
  - ENTRY: market SELL at the open (first session bar). Sizing NOTIONAL_CAP_PCT x capital.
  - STOP: hard protective **buy-stop INITIAL_STOP_PCT (3%) ABOVE** entry (a short's stop is
    above). Caps the squeeze tail; turns every OOS year positive (9/9). Best as a broker
    bracket (broker-side -> no custom ws_monitor short logic).
  - EXIT: cover at the **close** (~15:15 square-off). Cover-timing was investigated on the
    real 1m path (time-of-day, profit-targets, trailing stops) — NONE beats hold-to-close;
    the intraday-low ceiling is unreachable (choppy fades whipsaw a trail). So: close-cover.

Economics (F&O, gap>5%, 3% stop, net of 0.27% MIS + 0.25% slippage): OOS 2018-26
**+0.57%/trade, +12.5%/yr per Rs1L, 9/9 years positive**, ~67 trades/yr (~1.5/week). Lumpy
and low-frequency; carried by 2018/2020/2023/2024. Forward-biased F&O universe (today's list
applied historically) -> anchor recent years. The value is *diversification* (earns in the
broad-market spikes that hurt the long book), not the modest absolute Rs.

All functions are PURE. Fail-closed: any missing/bad input -> no signal / no trade, never a
silent default. The fade economics (``fade_net``) are the canonical implementation the
backtest fidelity-replay reproduces; production sizing uses ``position_qty``.
"""
from __future__ import annotations

from typing import Sequence

# ── Validated config constants (do not change without a fresh OOS walk) ─────────
GAP_MIN: float = 0.05               # min open-vs-prev-close gap-up to fade (>5%)
INITIAL_STOP_PCT: float = 0.03      # protective buy-stop this far ABOVE entry (short)
TURNOVER_MIN: float = 1e8           # 20d avg turnover floor (Rs10cr); F&O already implies liquidity
PRICE_MIN: float = 30.0             # entry price floor
MAX_POSITIONS: int = 3              # concurrent same-day shorts (slots; recycles daily)
NOTIONAL_CAP_PCT: float = 0.20      # per-position notional (x channel capital); pilot size
# Cost model (net-of-cost economics; matches the validated backtest) ─────────────
MIS_COST: float = 0.0027            # intraday MIS round-trip brokerage+taxes (Upstox)
SLIPPAGE: float = 0.0025            # round-trip market-impact/spread on the volatile open
STOP_SLIPPAGE: float = 0.0015       # extra fast-move slippage when the protective stop fires


def gap_pct(open_price: float, prev_close: float) -> float | None:
    """Open-vs-prior-close gap = ``open / prev_close - 1``. Returns ``None`` on bad input."""
    if open_price is None or prev_close is None or prev_close <= 0 or open_price <= 0:
        return None
    return open_price / prev_close - 1.0


def is_locked_limit(high: float, low: float) -> bool:
    """``True`` when the bar has no intraday range (``high <= low``) — a circuit-locked /
    halted / single-print session that cannot be shorted-and-covered. Fail-closed: treat
    bad data as locked (unshortable)."""
    if high is None or low is None or high <= 0 or low <= 0:
        return True
    return high <= low


def passes_gap_gates(
    gap: float | None,
    turnover_20d: float,
    high: float,
    low: float,
    price: float,
    is_fno: bool,
    gap_min: float = GAP_MIN,
    turnover_min: float = TURNOVER_MIN,
    price_min: float = PRICE_MIN,
) -> bool:
    """All gap-fade entry gates. ``is_fno`` (F&O membership) is supplied by the caller.
    Returns ``True`` only if every gate passes; fail-closed on any missing input."""
    if gap is None or not is_fno:
        return False
    if gap <= gap_min:
        return False
    if turnover_20d is None or turnover_20d < turnover_min:
        return False
    if price is None or price < price_min:
        return False
    if is_locked_limit(high, low):
        return False
    return True


def short_stop_price(entry: float, stop_pct: float = INITIAL_STOP_PCT) -> float | None:
    """Protective buy-stop price for a short = ``entry * (1 + stop_pct)`` (ABOVE entry)."""
    if entry is None or entry <= 0:
        return None
    return entry * (1.0 + stop_pct)


def position_qty(entry: float, capital: float, notional_cap_pct: float = NOTIONAL_CAP_PCT) -> int:
    """Share qty for one short = ``floor(notional_cap_pct * capital / entry)``. 0 on bad input."""
    if entry is None or entry <= 0 or capital is None or capital <= 0:
        return 0
    return int((notional_cap_pct * capital) // entry)


def fade_net(
    entry: float,
    day_high: float,
    day_close: float,
    stop_pct: float = INITIAL_STOP_PCT,
    mis_cost: float = MIS_COST,
    slippage: float = SLIPPAGE,
    stop_slippage: float = STOP_SLIPPAGE,
) -> float | None:
    """Net return of the gap-fade SHORT (the canonical economics the backtest reproduces).
    Short at ``entry`` (open); if the day's ``high`` reaches the ``stop_pct`` buy-stop the
    trade is covered there for a capped loss; otherwise it covers at ``day_close``.
      stopped : ``-stop_pct - mis_cost - slippage - stop_slippage``
      else    : ``-(day_close/entry - 1) - mis_cost - slippage``
    Positive = profit (price fell). Returns ``None`` on bad input."""
    if entry is None or entry <= 0 or day_high is None or day_close is None:
        return None
    if day_high >= entry * (1.0 + stop_pct):
        return -stop_pct - mis_cost - slippage - stop_slippage
    return -(day_close / entry - 1.0) - mis_cost - slippage
