"""INSIDER live signal service — produces cluster-buy candidates for a reaction (dissemination) day.

Flow:  NSE SEBI-PIT disclosures (ingested to BQ ``nse_insider_daily``) -> ``aggregate_clusters``
(domain: informed open-market buys, >=2 legs/symbol/day) -> per-symbol daily bars (Upstox, on-
demand) -> ``domain/insider_signals`` liquidity/price gates -> qualifying candidates (symbol,
cluster strength for slot rank, ATR for sizing, reaction_close as the next-open proxy).

Design mirrors ``delivery_signal_service`` / ``pead_signal_service``: the candidate core
(``build_candidates`` / ``scan``) is PURE + data-injected (fidelity-tested vs the exact backtest);
the BQ reads (``fetch_disclosure_rows``, ``latest_reaction_date``) are thin fail-closed I/O. All
gates mirror the validated backtest byte-for-byte via ``domain/insider_signals`` so the live
signal cannot drift.

The DOUBLE MACRO GATE (breadth b200>50 AND Nifty>100DMA) is CHANNEL-LEVEL and is applied once
per scan by the trading service (``macro_gate_ok``), not per-candidate — bad regime => no entries.

Validated config (2026-07-20, GOD-MODE): cluster>=2, turnover>=10cr, price>=30, hold 90d, 10
slots, ATR14x2.5 fixed-hold stop -> +23% CAGR / -12.5% DD / Calmar 1.84. See ``domain/insider_signals``.
"""
from __future__ import annotations

import bisect
import logging
from typing import Any

from autotrader.domain import insider_signals

logger = logging.getLogger(__name__)


def build_candidates(
    reaction_date: str,
    clusters: dict[str, dict[str, Any]],
    candles: dict[str, list[list]],
    turnover_min_cr: float = insider_signals.TURNOVER_MIN_CR,
    price_min: float = insider_signals.PRICE_MIN,
) -> list[dict[str, Any]]:
    """Pure insider candidate selection for one reaction (dissemination) day.

    ``clusters`` = ``aggregate_clusters`` output ``{sym: {n_buyers, total_val, dpct, category}}``
    (already cluster-gated at >=2 informed open-market buy legs). For each clustered symbol:
    find the reaction-day bar, compute trailing 20d-mean turnover + ATR14, apply the liquidity/
    price/ETF gate (``passes_insider_gates``). Emits qualifying candidates; ``n_buyers``/
    ``total_val`` are the slot-ranking keys, ``atr`` the sizing basis, ``reaction_close`` the
    next-open proxy.
    """
    out: list[dict[str, Any]] = []
    for sym, info in clusters.items():
        bars = candles.get(sym)
        if not bars or len(bars) < insider_signals.MIN_BARS:
            continue
        dates = [b[0] for b in bars]
        ri = bisect.bisect_left(dates, reaction_date)
        if ri >= len(bars) or dates[ri] != reaction_date:
            continue
        if ri < 20 or ri < insider_signals.ATR_WINDOW:
            continue
        closes = [b[4] for b in bars]
        vols = [b[5] for b in bars]
        turnover_cr = insider_signals.turnover_20d_cr(closes, vols, ri)
        if not insider_signals.passes_insider_gates(
            turnover_cr, closes[ri], sym,
            turnover_min_cr=turnover_min_cr, price_min=price_min,
        ):
            continue
        atr_series = insider_signals.atr14(bars)
        atr = atr_series[ri] if ri < len(atr_series) else None
        if atr is None or atr <= 0:
            continue
        out.append({
            "symbol": sym,
            "channel": "insider",
            "reaction_date": reaction_date,
            "n_buyers": int(info.get("n_buyers", 0)),
            "total_val": float(info.get("total_val", 0.0)),
            "category": str(info.get("category", "")),
            "atr": atr,
            "reaction_close": closes[ri],
            "turnover_cr": turnover_cr,
            "max_hold_days": insider_signals.MAX_HOLD_DAYS,
        })
    # slot priority: strongest cluster first (more buyers, then larger value)
    out.sort(key=lambda c: (-c["n_buyers"], -c["total_val"]))
    return out


def scan(
    reaction_date: str,
    clusters: dict[str, dict[str, Any]],
    candles: dict[str, list[list]],
    turnover_min_cr: float = insider_signals.TURNOVER_MIN_CR,
    price_min: float = insider_signals.PRICE_MIN,
) -> list[dict[str, Any]]:
    """Top-level (thin): return insider cluster-buy candidates for ``reaction_date``. The
    channel-level macro double-gate is applied by the trading service, not here."""
    cands = build_candidates(reaction_date, clusters, candles,
                             turnover_min_cr=turnover_min_cr, price_min=price_min)
    logger.info("insider_scan reaction_date=%s clusters=%d candidates=%d",
                reaction_date, len(clusters), len(cands))
    return cands


# ── live I/O (validated in PAPER, not unit-tested — fail-closed) ──────────────
def fetch_disclosure_rows(bq, reaction_date: str) -> list[dict[str, Any]]:
    """Read one dissemination day's raw PIT rows from BQ ``nse_insider_daily`` (for
    ``domain.aggregate_clusters``). Fail-closed: ``[]`` on any error."""
    try:
        q = (f"SELECT symbol, person_category, transaction_type, acq_mode, sec_val, "
             f"bef_pct, after_pct FROM `grow-profit-machine.autotrader.nse_insider_daily` "
             f"WHERE date = '{reaction_date}'")
        rows = [dict(r) for r in bq.query(q)]
        logger.info("insider_fetch_rows date=%s rows=%d", reaction_date, len(rows))
        return rows
    except Exception as exc:
        logger.error("insider_fetch_rows_failed date=%s err=%s", reaction_date, exc)
        return []


def latest_reaction_date(bq) -> str:
    """Latest dissemination day in BQ ``nse_insider_daily`` (``MAX(date)``). Fail-closed: ``""``."""
    try:
        rows = bq.query("SELECT CAST(MAX(date) AS STRING) AS d "
                        "FROM `grow-profit-machine.autotrader.nse_insider_daily`")
        return str((rows[0].get("d") if rows else "") or "")[:10]
    except Exception as exc:
        logger.error("insider_latest_reaction_date_failed err=%s", exc)
        return ""
