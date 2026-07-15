"""DELIVERY live signal service — produces delivery-accumulation candidates for a date.

Flow:  NSE ``sec_bhavdata_full`` delivery-% (ingested to BQ ``nse_delivery_daily``)  ->
per-symbol daily bars (Upstox, on-demand)  ->  domain/delivery_signals gates  ->
qualifying candidates (symbol, delivery-% rank, ATR for sizing).

Design mirrors ``pead_signal_service``: the candidate-selection core (``build_candidates``,
``scan``) is PURE and data-injected so it is fidelity-tested against the exact backtest;
the BQ reads (``fetch_delivery_rows``, ``latest_reaction_date``) are thin fail-closed I/O.
All gates/computations mirror the validated backtest byte-for-byte (import
``domain/delivery_signals``), so the live signal cannot drift from the backtest.

Bars convention: per symbol, a list of ``[date, open, high, low, close, volume]`` rows
sorted ascending by date (the prod candle/BQ shape). The "reaction day" is the EOD session
whose delivery-% fired the signal; entry is the NEXT session's open (finalised at order
time — the trading service uses the reaction-day close as the premarket proxy). NO
market-drawdown gate (unlike PEAD) — delivery is a pure microstructure signal.

Validated config (2026-07-14, STOCKS-ONLY, ETFs excluded, survivorship-safe, IS+OOS):
deliv_pct >= 75, 25-50cr 20d-mean turnover band, price >= 30, hold 20d, 5 slots, rank by
delivery-%  ->  ~11.8% CAGR / Calmar 0.86 / -13.7% DD / 6-of-7 positive years.
"""
from __future__ import annotations

import bisect
import logging
from typing import Any, Sequence

from autotrader.domain import delivery_signals

logger = logging.getLogger(__name__)


def build_candidates(
    reaction_date: str,
    delivery_rows: dict[str, dict[str, Any]],
    candles: dict[str, list[list]],
    deliv_min: float = delivery_signals.DELIV_MIN,
    turnover_min_cr: float = delivery_signals.TURNOVER_MIN_CR,
    turnover_max_cr: float = delivery_signals.TURNOVER_MAX_CR,
    price_min: float = delivery_signals.PRICE_MIN,
) -> list[dict[str, Any]]:
    """Pure delivery candidate selection for one reaction day.

    For each symbol with a delivery-% on ``reaction_date`` (``delivery_rows`` =
    ``{sym: {"deliv_pct": ..}}``): find the reaction-day bar, compute the trailing
    20d-mean turnover (``turnover_20d_cr``) + ATR14 (``atr14``), and apply the full
    delivery gate (``passes_delivery_gates`` — deliv floor, turnover band, price floor,
    ETF exclusion). Emits qualifying candidates; ``deliv_pct`` is the slot-ranking key,
    ``atr`` is the sizing basis, ``reaction_close`` is the next-open premarket proxy.

    Note (differs from PEAD): the reaction bar only needs enough history for turnover
    + ATR (``MIN_BARS``); no ``ri+1`` bar is required here (entry is handled at order
    time on the live next open, not read from the bars).
    """
    out: list[dict[str, Any]] = []
    for sym, row in delivery_rows.items():
        bars = candles.get(sym)
        if not bars or len(bars) < delivery_signals.MIN_BARS:
            continue
        dates = [b[0] for b in bars]
        ri = bisect.bisect_left(dates, reaction_date)
        if ri >= len(bars) or dates[ri] != reaction_date:
            continue
        # need a full 20d trailing window + ATR14 ending at the reaction bar
        if ri < 20 or ri < delivery_signals.ATR_WINDOW:
            continue
        closes = [b[4] for b in bars]
        vols = [b[5] for b in bars]
        deliv_pct = float(row.get("deliv_pct", 0.0))
        turnover_cr = delivery_signals.turnover_20d_cr(closes, vols, ri)
        if not delivery_signals.passes_delivery_gates(
            deliv_pct, turnover_cr, closes[ri], sym,
            deliv_min=deliv_min, turnover_min_cr=turnover_min_cr,
            turnover_max_cr=turnover_max_cr, price_min=price_min,
        ):
            continue
        atr_series = delivery_signals.atr14(bars)
        atr = atr_series[ri] if ri < len(atr_series) else None
        if atr is None or atr <= 0:
            continue
        out.append({
            "symbol": sym,
            "channel": "delivery",
            "reaction_date": reaction_date,
            "deliv_pct": deliv_pct,
            "atr": atr,
            "reaction_close": closes[ri],
            "turnover_cr": turnover_cr,
            "max_hold_days": delivery_signals.MAX_HOLD_DAYS,
        })
    out.sort(key=lambda c: -c["deliv_pct"])       # delivery-%-ranked (slot priority)
    return out


def scan(
    reaction_date: str,
    delivery_rows: dict[str, dict[str, Any]],
    candles: dict[str, list[list]],
    deliv_min: float = delivery_signals.DELIV_MIN,
    turnover_min_cr: float = delivery_signals.TURNOVER_MIN_CR,
    turnover_max_cr: float = delivery_signals.TURNOVER_MAX_CR,
    price_min: float = delivery_signals.PRICE_MIN,
) -> list[dict[str, Any]]:
    """Top-level (thin): return delivery candidates for ``reaction_date``.

    No market-state gate (delivery is a pure accumulation-microstructure signal — unlike
    PEAD there is deliberately NO nifty drawdown fetch). Just delegates to the pure
    ``build_candidates`` and logs.
    """
    cands = build_candidates(reaction_date, delivery_rows, candles,
                             deliv_min=deliv_min, turnover_min_cr=turnover_min_cr,
                             turnover_max_cr=turnover_max_cr, price_min=price_min)
    logger.info("delivery_scan reaction_date=%s deliv_rows=%d candidates=%d",
                reaction_date, len(delivery_rows), len(cands))
    return cands


# ── live I/O (validated in PAPER, not unit-tested — fail-closed) ──────────────
def fetch_delivery_rows(bq, reaction_date: str, deliv_prefilter: float = 70.0) -> dict[str, dict[str, Any]]:
    """Read one session's delivery-% from BQ ``nse_delivery_daily``.

    Returns ``{symbol: {"deliv_pct", "close", "turnover_cr"}}`` for rows on
    ``reaction_date`` with ``deliv_pct >= deliv_prefilter`` (cheap pre-cut below the 75
    gate). Fail-closed: ``{}`` on any error (no silent bad signal)."""
    try:
        q = (f"SELECT symbol, deliv_pct, close_price, turnover_cr "
             f"FROM `grow-profit-machine.autotrader.nse_delivery_daily` "
             f"WHERE date = '{reaction_date}' AND deliv_pct >= {float(deliv_prefilter)}")
        out: dict[str, dict[str, Any]] = {}
        for r in bq.query(q):
            sym = str(r.get("symbol") or "").strip().upper()
            if not sym:
                continue
            out[sym] = {
                "deliv_pct": float(r.get("deliv_pct") or 0.0),
                "close": float(r.get("close_price") or 0.0),
                "turnover_cr": float(r.get("turnover_cr") or 0.0),
            }
        logger.info("delivery_fetch_rows date=%s prefilter=%.1f rows=%d",
                    reaction_date, deliv_prefilter, len(out))
        return out
    except Exception as exc:
        logger.error("delivery_fetch_rows_failed date=%s err=%s", reaction_date, exc)
        return {}


def latest_reaction_date(bq) -> str:
    """Latest available session in BQ ``nse_delivery_daily`` (``MAX(date)``). This is the
    delivery reaction day — entry is the next open. Fail-closed: ``""`` on error/empty."""
    try:
        rows = bq.query("SELECT CAST(MAX(date) AS STRING) AS d "
                        "FROM `grow-profit-machine.autotrader.nse_delivery_daily`")
        d = str((rows[0].get("d") if rows else "") or "")[:10]
        return d
    except Exception as exc:
        logger.error("delivery_latest_reaction_date_failed err=%s", exc)
        return ""
