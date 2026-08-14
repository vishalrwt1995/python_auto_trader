"""PLEDGE live signal service — promoter pledge-revoke candidates for a reaction (dissemination) day.

Flow:  NSE SEBI-PIT disclosures (ingested to BQ ``nse_insider_daily`` — the SAME table as insider;
revoke legs are already written verbatim) -> ``pledge_signals.aggregate_revokes`` (promoter
pledge-revoke legs, >=1/symbol — a single promoter revoke IS the signal, no cluster) -> per-symbol
daily bars (Upstox, on-demand) -> ``domain/pledge_signals`` gates (turnover>=25cr, price>=30,
px>200DMA) -> qualifying candidates (symbol, ATR for sizing, reaction_close as next-open proxy).

Design mirrors ``insider_signal_service`` / ``delivery_signal_service``: the candidate core
(``build_candidates`` / ``scan``) is PURE + data-injected (fidelity-tested vs the exact backtest);
the BQ reads (``fetch_revoke_rows``, ``latest_reaction_date``) are thin fail-closed I/O. All gates
mirror the validated backtest via ``domain/pledge_signals`` so the live signal cannot drift.

The DOUBLE MACRO GATE (breadth b200>50 AND Nifty>100DMA) is CHANNEL-LEVEL and is applied once per
scan by the trading service (``macro_gate_ok``), not per-candidate — bad regime => no entries.

Validated FINAL config (2026-07-21): promoter revoke + px>200DMA + turnover>=25cr + price>=30,
hold 60d, 10 slots (cap10% no-leverage), ATR14x2.0 fixed-hold stop -> +25% CAGR / -11.5% DD /
Calmar 2.18 (IS 1.96 / OOS 4.17). See ``domain/pledge_signals``.
"""
from __future__ import annotations

import bisect
import logging
from typing import Any

from autotrader.domain import pledge_signals

logger = logging.getLogger(__name__)


def build_candidates(
    reaction_date: str,
    revokes: dict[str, dict[str, Any]],
    candles: dict[str, list[list]],
    turnover_min_cr: float = pledge_signals.TURNOVER_MIN_CR,
    price_min: float = pledge_signals.PRICE_MIN,
) -> list[dict[str, Any]]:
    """Pure pledge candidate selection for one reaction (dissemination) day.

    ``revokes`` = ``aggregate_revokes`` output ``{sym: {n_revokes, category}}``. For each symbol:
    find the reaction-day bar, compute trailing 20d-mean turnover + ATR14 + the 200-day SMA, apply
    the liquidity/price/uptrend/ETF gate (``passes_pledge_gates``). Emits qualifying candidates;
    ``turnover_cr`` is the slot-ranking key (liquidity), ``atr`` the sizing basis, ``reaction_close``
    the next-open proxy.
    """
    out: list[dict[str, Any]] = []
    for sym, info in revokes.items():
        bars = candles.get(sym)
        if not bars or len(bars) < pledge_signals.MIN_BARS:
            continue
        dates = [b[0] for b in bars]
        ri = bisect.bisect_left(dates, reaction_date)
        if ri >= len(bars) or dates[ri] != reaction_date:
            continue
        if ri < pledge_signals.MA_DAYS - 1 or ri < 20:      # need 200d SMA + 20d turnover
            continue
        closes = [b[4] for b in bars]
        vols = [b[5] for b in bars]
        turnover_cr = pledge_signals.turnover_20d_cr(closes, vols, ri)
        s200 = pledge_signals.sma(closes, pledge_signals.MA_DAYS, ri)
        above_200dma = s200 is not None and closes[ri] > s200
        if not pledge_signals.passes_pledge_gates(
            turnover_cr, closes[ri], above_200dma, sym,
            turnover_min_cr=turnover_min_cr, price_min=price_min,
        ):
            continue
        atr_series = pledge_signals.atr14(bars)
        atr = atr_series[ri] if ri < len(atr_series) else None
        if atr is None or atr <= 0:
            continue
        out.append({
            "symbol": sym,
            "channel": "pledge",
            "reaction_date": reaction_date,
            "n_revokes": int(info.get("n_revokes", 0)),
            "category": str(info.get("category", "")),
            "atr": atr,
            "reaction_close": closes[ri],
            "turnover_cr": turnover_cr,
            "max_hold_days": pledge_signals.MAX_HOLD_DAYS,
        })
    # slot priority: most liquid first (magnitude/release-% were killed as OOS-invalid)
    out.sort(key=lambda c: -c["turnover_cr"])
    return out


def scan(
    reaction_date: str,
    revokes: dict[str, dict[str, Any]],
    candles: dict[str, list[list]],
    turnover_min_cr: float = pledge_signals.TURNOVER_MIN_CR,
    price_min: float = pledge_signals.PRICE_MIN,
) -> list[dict[str, Any]]:
    """Top-level (thin): return pledge-revoke candidates for ``reaction_date``. The channel-level
    macro double-gate is applied by the trading service, not here."""
    cands = build_candidates(reaction_date, revokes, candles,
                             turnover_min_cr=turnover_min_cr, price_min=price_min)
    logger.info("pledge_scan reaction_date=%s revokes=%d candidates=%d",
                reaction_date, len(revokes), len(cands))
    return cands


# ── live I/O (validated in PAPER, not unit-tested — fail-closed) ──────────────
def fetch_revoke_rows(bq, reaction_date: str) -> list[dict[str, Any]]:
    """Read one dissemination day's promoter pledge-REVOKE rows from BQ ``nse_insider_daily``
    (for ``domain.aggregate_revokes``). Filters at the query for revoke + promoter to keep the
    payload tiny. Fail-closed: ``[]`` on any error."""
    try:
        q = (f"SELECT symbol, person_category, transaction_type, shares "
             f"FROM `grow-profit-machine.autotrader.nse_insider_daily` "
             f"WHERE date = '{reaction_date}' "
             f"AND LOWER(transaction_type) LIKE '%revok%' "
             f"AND LOWER(person_category) LIKE '%promoter%'")
        rows = [dict(r) for r in bq.query(q)]
        logger.info("pledge_fetch_rows date=%s rows=%d", reaction_date, len(rows))
        return rows
    except Exception as exc:
        logger.error("pledge_fetch_rows_failed date=%s err=%s", reaction_date, exc)
        return []


def latest_reaction_date(bq) -> str:
    """Latest dissemination day in BQ ``nse_insider_daily`` (``MAX(date)``). Fail-closed: ``""``."""
    try:
        rows = bq.query("SELECT CAST(MAX(date) AS STRING) AS d "
                        "FROM `grow-profit-machine.autotrader.nse_insider_daily`")
        return str((rows[0].get("d") if rows else "") or "")[:10]
    except Exception as exc:
        logger.error("pledge_latest_reaction_date_failed err=%s", exc)
        return ""


def available_reaction_dates(bq, lookback_days: int = 10) -> list[str]:
    """Ascending DISTINCT dissemination days in BQ ``nse_insider_daily`` over the trailing
    ``lookback_days``. Feeds ``domain.reaction_dates.classify_pending_dates`` so a scan settles
    EVERY date it still owes rather than only ``MAX(date)`` — a single-date read silently dropped
    weekend-dated filings and rows that landed after their own date's scan. Fail-closed: ``[]``."""
    try:
        rows = bq.query(
            "SELECT DISTINCT CAST(date AS STRING) AS d "
            "FROM `grow-profit-machine.autotrader.nse_insider_daily` "
            f"WHERE date >= DATE_SUB(CURRENT_DATE('Asia/Kolkata'), INTERVAL {int(lookback_days)} DAY) "
            "ORDER BY d")
        return [str(r.get("d"))[:10] for r in rows if r.get("d")]
    except Exception as exc:
        logger.error("pledge_available_reaction_dates_failed err=%s", exc)
        return []
