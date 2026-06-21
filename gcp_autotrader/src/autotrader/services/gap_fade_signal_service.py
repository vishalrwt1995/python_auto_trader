"""Gap-fade short live signal service — produces gap-fade short candidates at the open,
the GAP_FADE channel's signal layer. Mirrors corp_action_signal_service's pure-core +
thin-I/O structure: the selection core (`build_candidates`) is PURE + data-injected
(fidelity-tested via `domain/gap_fade_signals`); `fetch_open_snapshot` is the live Upstox
quote wrapper. Fail-closed (any bad input -> no candidate).

Flow: at ~09:16 IST (just after the open), for each NSE F&O underlying get today's open,
prior close, today's high/low (locked-limit/circuit check) and 20d avg turnover ->
`gap_fade_signals` gates (gap>5%, liquid, price floor, not locked) -> rank by gap size ->
top-K short candidates. Each candidate: short at the open, cover at the 15:15 squareoff,
3% protective buy-stop (broker bracket). UNLIKE corp/PEAD this is INTRADAY (same-day MIS,
no overnight) and fires at the open, not premarket."""
from __future__ import annotations

import logging
from typing import Any

from autotrader.domain import gap_fade_signals as gf

logger = logging.getLogger(__name__)


def build_candidates(
    snapshot: dict[str, dict[str, float]],
    max_positions: int = gf.MAX_POSITIONS,
    gap_min: float = gf.GAP_MIN,
    turnover_min: float = gf.TURNOVER_MIN,
    price_min: float = gf.PRICE_MIN,
) -> list[dict[str, Any]]:
    """PURE gap-fade candidate selection. ``snapshot`` = ``{symbol: {open, prev_close,
    high, low, turnover_20d}}`` restricted to NSE F&O underlyings (caller guarantees the
    universe). Apply the validated gap gates, rank by gap size (largest = highest edge),
    return the top-K short candidates. Each is tagged channel/wl_type/strategy for isolated
    book + exit handling, with the entry (open) and the 3% protective buy-stop."""
    out: list[dict[str, Any]] = []
    for sym, q in snapshot.items():
        if not q:
            continue
        op = q.get("open"); pc = q.get("prev_close")
        gap = gf.gap_pct(op, pc)
        if not gf.passes_gap_gates(gap, q.get("turnover_20d", 0.0), q.get("high"), q.get("low"),
                                   op, True, gap_min=gap_min, turnover_min=turnover_min, price_min=price_min):
            continue
        entry = float(op)
        out.append({
            "symbol": str(sym).strip().upper(),
            "channel": "gap_fade",
            "wl_type": "gap_fade",
            "strategy": "GAP_FADE",
            "side": "SELL",                                   # gap-fade is a SHORT
            "gap": round(gap, 4),
            "ref_open": round(entry, 2),                      # short entry ~ the open
            "stop_price": round(gf.short_stop_price(entry), 2),   # 3% protective buy-stop (above)
        })
    out.sort(key=lambda c: -c["gap"])                          # largest gap-up first (highest edge)
    return out[:max_positions]


def fetch_open_snapshot(
    fno_symbols: list[str],
    upstox: Any,
    turnover_map: dict[str, float],
) -> dict[str, dict[str, float]]:
    """Live I/O: today's OHLC for each F&O underlying via Upstox quotes + the caller-supplied
    20d turnover (from candles/universe). ``{symbol: {open, prev_close, high, low,
    turnover_20d}}``. Thin wrapper; fail-closed (skip a symbol on any bad/missing quote)."""
    snap: dict[str, dict[str, float]] = {}
    try:
        quotes = upstox.get_ohlc_quotes(fno_symbols) if hasattr(upstox, "get_ohlc_quotes") else {}
    except Exception as exc:
        logger.error("gap_fade_fetch_quotes_failed err=%s", exc)
        return {}
    for sym in fno_symbols:
        q = quotes.get(sym) or quotes.get(str(sym).upper())
        if not q:
            continue
        try:
            op = float(q.get("open") or 0.0); pc = float(q.get("prev_close") or 0.0)
            hi = float(q.get("high") or 0.0); lo = float(q.get("low") or 0.0)
        except (TypeError, ValueError):
            continue
        if op <= 0 or pc <= 0:
            continue
        snap[str(sym).strip().upper()] = {
            "open": op, "prev_close": pc, "high": hi, "low": lo,
            "turnover_20d": float(turnover_map.get(sym, turnover_map.get(str(sym).upper(), 0.0))),
        }
    return snap


def scan(
    fno_symbols: list[str],
    upstox: Any = None,
    turnover_map: dict[str, float] | None = None,
    snapshot: dict[str, dict[str, float]] | None = None,
    max_positions: int = gf.MAX_POSITIONS,
) -> list[dict[str, Any]]:
    """Top-level: return gap-fade short candidates for the open. ``fno_symbols`` = the NSE
    F&O underlying universe (the shortable set). If ``snapshot`` is None, fetch live via
    ``upstox`` + ``turnover_map``. Returns the top-K (by gap) short candidates."""
    if snapshot is None:
        snapshot = fetch_open_snapshot(fno_symbols, upstox, turnover_map or {})
    cands = build_candidates(snapshot, max_positions=max_positions)
    logger.info("gap_fade_scan universe=%d snapshot=%d candidates=%d",
                len(fno_symbols), len(snapshot), len(cands))
    return cands
