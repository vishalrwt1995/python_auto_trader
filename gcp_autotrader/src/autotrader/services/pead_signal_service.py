"""PEAD live signal service — produces EVENT/PEAD channel candidates for a date.

Flow:  NSE event-calendar (just-reported results)  ->  per-symbol earnings surprise
+ anti-pump run-up + broad-market drawdown  ->  domain/pead_signals gates  ->
qualifying candidates (symbol, surprise rank, ATR for sizing).

Design: the candidate-selection core (`build_candidates`, `compute_market_dd`) is
PURE and data-injected, so it is fidelity-tested against the exact backtest data;
the NSE fetch (`fetch_result_symbols`) is the thin live-I/O wrapper. All eligibility
constants and computations mirror the validated Config B backtest byte-for-byte, so
the live signal cannot drift from the backtest (the swing_signals/swing_exit
fidelity discipline).

Bars convention: per symbol, a list of ``[date, open, high, low, close, volume]``
rows sorted ascending by date (the prod candle/BQ shape). The "reaction day" is the
first full session after results are filed; the surprise is that day's move; entry
is the next session's open (computed at order time live).
"""
from __future__ import annotations

import bisect
import logging
from typing import Any, Sequence

from autotrader.domain import pead_signals

logger = logging.getLogger(__name__)

# Backtest-validated eligibility floors (Config B) — must match the OOS harness.
PRICE_MIN: float = 30.0               # entry price floor (no penny names)
TURNOVER_MIN: float = 2e7             # 20-day avg turnover floor (Rs 2 cr, liquid)
TURNOVER_WINDOW: int = 20
ATR_WINDOW: int = 14
MARKET_DD_WINDOW: int = 252           # trailing peak window for the market index
MARKET_RET_CLIP: float = 0.30         # clip daily returns when building the index (glitch guard)
MARKET_MIN_STOCKS: int = 50           # min stocks/day for a valid index point
MARKET_MIN_HISTORY: int = 300         # min total bars for a symbol to enter the index
#                                       (established names only — matches the validated
#                                       Config B backtest index universe; without it the
#                                       index admits ~500 short-history small-caps and the
#                                       drawdown flips ~11% of candidates at the -5% gate)


def _simple_atr(highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], ri: int,
                window: int = ATR_WINDOW) -> float | None:
    """14-day mean True Range ending at ``ri`` — matches the backtest sizing exactly
    (simple mean of TR, not Wilder). Returns None without enough history."""
    if ri - (window - 1) < 1:
        return None
    trs = []
    for k in range(ri - (window - 1), ri + 1):
        trs.append(max(highs[k] - lows[k], abs(highs[k] - closes[k - 1]), abs(lows[k] - closes[k - 1])))
    return sum(trs) / window


def compute_market_dd(candles: dict[str, list[list]], asof: str,
                      window: int = MARKET_DD_WINDOW) -> float | None:
    """Broad-market drawdown at ``asof`` = equal-weight index level / trailing-``window``
    peak − 1 (<= 0). Equal-weight index = cumulative cross-sectional mean daily return
    of the universe. Mirrors the backtest's market-state measure exactly.
    """
    rsum: dict[str, float] = {}
    rcnt: dict[str, int] = {}
    for bars in candles.values():
        if len(bars) < MARKET_MIN_HISTORY:
            continue
        for i in range(1, len(bars)):
            d = bars[i][0]
            if d > asof:
                break
            pc = bars[i - 1][4]
            if pc > 0:
                r = bars[i][4] / pc - 1.0
                if -MARKET_RET_CLIP < r < MARKET_RET_CLIP:
                    rsum[d] = rsum.get(d, 0.0) + r
                    rcnt[d] = rcnt.get(d, 0) + 1
    days = sorted(d for d in rsum if rcnt[d] >= MARKET_MIN_STOCKS)
    if not days:
        return None
    lvl: list[float] = []
    ix = 1.0
    for d in days:
        ix *= (1 + rsum[d] / rcnt[d])
        lvl.append(ix)
    peak = max(lvl[max(0, len(lvl) - window):])
    return lvl[-1] / peak - 1.0 if peak > 0 else None


def build_candidates(reaction_date: str, result_symbols: Sequence[str],
                     candles: dict[str, list[list]], market_dd: float | None) -> list[dict[str, Any]]:
    """Pure Config B candidate selection for one reaction day.

    For each just-reported symbol whose reaction is ``reaction_date``: apply the
    liquidity/price floors, compute the surprise + anti-pump run-up (via
    ``pead_signals``), apply the gates (incl. the broad-market ``market_dd``), and
    emit qualifying candidates. ``surprise`` is the slot-ranking key; ``atr`` is the
    sizing basis (entry + sl_dist are finalised at order time on the next open).
    """
    out: list[dict[str, Any]] = []
    for sym in result_symbols:
        bars = candles.get(sym)
        if not bars or len(bars) < ATR_WINDOW + 2:
            continue
        dates = [b[0] for b in bars]
        ri = bisect.bisect_left(dates, reaction_date)
        if ri >= len(bars) or dates[ri] != reaction_date or ri < 1:
            continue
        highs = [b[2] for b in bars]; lows = [b[3] for b in bars]; closes = [b[4] for b in bars]; vols = [b[5] for b in bars]
        # liquidity + price floors (next-open price approximated by reaction close premarket)
        if closes[ri] < PRICE_MIN:
            continue
        if ri + 1 - TURNOVER_WINDOW < 0:
            continue
        tov = sum(closes[k] * vols[k] for k in range(ri + 1 - TURNOVER_WINDOW, ri + 1)) / TURNOVER_WINDOW
        if tov < TURNOVER_MIN:
            continue
        surprise = pead_signals.earnings_surprise(closes, ri)
        runup = pead_signals.pre_event_runup(closes, ri)
        if not pead_signals.passes_pead_gates(surprise, runup, market_dd):
            continue
        atr = _simple_atr(highs, lows, closes, ri)
        if atr is None or atr <= 0:
            continue
        out.append({
            "symbol": sym,
            "channel": "pead",
            "reaction_date": reaction_date,
            "surprise": surprise,
            "runup": runup,
            "atr": atr,
            "reaction_close": closes[ri],
            "max_hold_days": pead_signals.MAX_HOLD_DAYS,
        })
    out.sort(key=lambda c: -c["surprise"])      # surprise-ranked (slot priority)
    return out


def fetch_result_symbols(asof: str, lookback_days: int = 3) -> list[str]:
    """Live NSE event-calendar: symbols whose 'Financial Results' board meeting fell
    in [asof - lookback_days, asof] (i.e. results just filed). Thin I/O wrapper;
    fail-closed (returns [] on error — no silent bad signals)."""
    from datetime import date, timedelta
    try:
        from autotrader.services.earnings_calendar_service import _nse_get
        a = date.fromisoformat(asof)
        frm = (a - timedelta(days=lookback_days)).strftime("%d-%m-%Y")
        to = a.strftime("%d-%m-%Y")
        data = _nse_get(f"/api/event-calendar?index=equities&from_date={frm}&to_date={to}")
        events = data if isinstance(data, list) else data.get("data", [])
        syms = set()
        for e in events:
            if "result" in str(e.get("purpose", "")).lower():
                s = str(e.get("symbol", "")).strip().upper()
                if s:
                    syms.add(s)
        logger.info("pead_result_symbols asof=%s count=%d", asof, len(syms))
        return sorted(syms)
    except Exception as exc:
        logger.error("pead_fetch_result_symbols_failed asof=%s err=%s", asof, exc)
        return []


def scan(reaction_date: str, candles: dict[str, list[list]],
         result_symbols: Sequence[str] | None = None) -> list[dict[str, Any]]:
    """Top-level: compute market_dd from ``candles`` and return PEAD candidates for
    ``reaction_date``. If ``result_symbols`` is None, fetch them live from NSE."""
    market_dd = compute_market_dd(candles, reaction_date)
    if market_dd is None:
        logger.warning("pead_scan_no_market_dd reaction_date=%s — skipped", reaction_date)
        return []
    if result_symbols is None:
        result_symbols = fetch_result_symbols(reaction_date)
    cands = build_candidates(reaction_date, result_symbols, candles, market_dd)
    logger.info("pead_scan reaction_date=%s market_dd=%.3f candidates=%d", reaction_date, market_dd, len(cands))
    return cands
