"""DELIVERY channel trading service — daily premarket scan -> 5-slot book -> PAPER CNC entries.

Hybrid architecture (mirrors the PEAD ship): a NEW thin service that owns the delivery
signal + book + entry, but REUSES the proven order_service / positions / exit plumbing via
``channel="delivery"`` + ``wl_type="delivery"``. It does NOT touch the swing/intraday hot
path or any other channel. The distinct ``wl_type="delivery"`` keeps delivery positions
invisible to swing_reconciliation (matches ``wl_type=="swing"``) and to the intraday tick
exits (``ws_monitor`` excludes it via ``_OVERNIGHT_SL_ONLY_WL``).

Daily flow (premarket, ~09:12 IST, on the prior evening's ingested delivery-%):
  1. ``latest_reaction_date`` (BQ ``nse_delivery_daily`` MAX(date)) = the reaction day.
  2. ``fetch_delivery_rows`` (deliv_pct >= 70 pre-cut) -> candidate symbols.
  3. resolve instrument keys + fresh daily bars per name (Upstox)  ->  turnover + ATR.
  4. ``delivery_signal_service.scan``  ->  qualifying candidates (delivery-%-ranked).
  5. ``plan_delivery_entries`` (pure): daily breaker + 5-slot cap + risk/notional/participation
     sizing  ->  order specs.
  6. ``order_service.place_entry_order(channel="delivery", wl_type="delivery", product="CNC")``.

NO market-drawdown gate (delivery is a pure accumulation-microstructure signal). Exit (the
daily 1R-trail armed at +1.75R + 20-day max-hold + 2.5xATR stop) is owned by
``delivery_reconciliation_service``. PAPER until explicitly flipped (Rule 5).

Validated config (2026-07-14, STOCKS-ONLY): ~11.8% CAGR @ deliv>=75, 25-50cr band, hold20,
5 slots, 2%-participation cap. See ``domain/delivery_signals`` + ``docs/DELIVERY_CHANNEL_PROPOSAL.md``.
"""
from __future__ import annotations

import logging
from typing import Any, Sequence

from autotrader.domain import delivery_signals, pead_book
from autotrader.services import delivery_signal_service

logger = logging.getLogger(__name__)

DELIVERY_EXCHANGE = "NSE"
DELIVERY_SEGMENT = "CASH"
SYMBOL_HISTORY_DAYS = 130         # > MIN_BARS (20d turnover + ATR14 + margin)
TARGET_BACKSTOP_R = 10.0          # far placeholder target; the real exit is the daily trail
PARTICIPATION_PCT = 0.02          # position <= 2% of the reaction-day traded turnover (capacity guard)


def plan_delivery_entries(
    candidates: Sequence[dict[str, Any]],
    open_delivery_symbols: Sequence[str],
    realized_today: float,
    channel_capital: float,
    cfg: Any,
) -> list[dict[str, Any]]:
    """PURE: given today's qualifying delivery candidates and the current book state,
    return the order specs to place (PAPER).

    Applies, in order: the per-channel daily loss/profit breaker (3%/6% of channel
    capital), exclusion of names already held, the 5-slot delivery-%-ranked cap, risk +
    notional sizing, and finally the **2%-participation cap** (position <= 2% of the
    reaction-day traded turnover) — the capacity guard that keeps fills realistic on
    25-50cr mid-caps. Returns ``[]`` when the breaker is tripped, capital is zero, or
    nothing fits. Side-effect-free + fully unit-testable.

    Entry price uses the reaction-day close as the premarket proxy for the next open
    (the live order fills market-on-open). ``target`` is a far backstop — the real exit
    is the daily 1R-trail managed by ``delivery_reconciliation_service``.
    """
    if channel_capital <= 0:
        logger.warning("delivery_plan_no_capital — channel_capital=%s, skipping", channel_capital)
        return []
    loss_limit = -abs(cfg.daily_loss_pct) * channel_capital
    profit_limit = abs(cfg.daily_profit_pct) * channel_capital
    if pead_book.daily_breaker_tripped(realized_today, loss_limit, profit_limit):
        logger.info("delivery_daily_breaker_tripped realized=%.0f loss_lim=%.0f profit_lim=%.0f — no entries",
                    realized_today or 0.0, loss_limit, profit_limit)
        return []

    held = {str(s).strip().upper() for s in open_delivery_symbols}
    fresh = [c for c in candidates if str(c.get("symbol", "")).strip().upper() not in held]
    risk = cfg.delivery_risk_per_trade if cfg.delivery_risk_per_trade > 0 else 0.015 * channel_capital
    notional_cap = cfg.delivery_notional_cap_pct * channel_capital
    picked = delivery_signals.select_for_slots(fresh, len(held), cfg.delivery_max_positions)

    specs: list[dict[str, Any]] = []
    for c in picked:
        entry = float(c["reaction_close"])
        sl_dist = pead_book.sl_distance(float(c["atr"]), entry, cfg.delivery_atr_sl_mult)
        qty = pead_book.position_size(entry, sl_dist, risk, notional_cap)
        # 2%-participation capacity cap: position notional <= 2% of the day's traded
        # turnover (turnover_cr is in crore -> *1e7 to rupees). Keeps fills realistic.
        turnover_cr = float(c.get("turnover_cr", 0.0) or 0.0)
        if turnover_cr > 0 and entry > 0:
            part_cap_qty = int(PARTICIPATION_PCT * (turnover_cr * 1e7) / entry)
            qty = min(qty, part_cap_qty)
        if qty < 1:
            logger.info("delivery_skip_qty_zero sym=%s entry=%.2f sl_dist=%.2f risk=%.0f cap=%.0f tov=%.1fcr",
                        c.get("symbol"), entry, sl_dist, risk, notional_cap, turnover_cr)
            continue
        specs.append({
            "symbol": str(c["symbol"]).strip().upper(),
            "qty": qty,
            "entry_price": round(entry, 2),
            "sl_price": round(entry - sl_dist, 2),
            "target": round(entry + sl_dist * TARGET_BACKSTOP_R, 2),
            "atr": round(float(c["atr"]), 4),
            "deliv_pct": float(c["deliv_pct"]),
            "instrument_key": str(c.get("instrument_key", "")),
            "strategy": "DELIVERY",
            "reaction_date": c.get("reaction_date", ""),
        })
    logger.info("delivery_plan candidates=%d held=%d planned=%d", len(candidates), len(held), len(specs))
    return specs


# ── live I/O wrapper (validated in PAPER, not unit-tested — fail-closed) ───────
def _fetch_symbol_daily(upstox, instrument_key: str, asof: str) -> list[list]:
    """Fresh daily bars [[date,o,h,l,c,v], ...] ascending for one symbol up to ``asof``."""
    from datetime import date, timedelta
    try:
        frm = (date.fromisoformat(asof) - timedelta(days=int(SYMBOL_HISTORY_DAYS * 1.6))).isoformat()
        rows = upstox.get_historical_candles_v3_days(instrument_key, to_date=asof, from_date=frm, interval_days=1)
        bars = [[str(r[0])[:10], float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])]
                for r in rows if isinstance(r, list) and len(r) >= 6]
        bars.sort(key=lambda b: b[0])
        return bars
    except Exception as exc:
        logger.error("delivery_fetch_symbol_failed key=%s err=%s", instrument_key, exc)
        return []


def _resolve_instrument_keys(symbols: Sequence[str], bq) -> dict[str, str]:
    """symbol -> Upstox instrument_key via BQ candles_daily (deep, has the keys even
    when recent bars are stale). Fail-closed: returns {} on error."""
    try:
        syms = ",".join("'" + str(s).strip().upper().replace("'", "") + "'" for s in symbols)
        q = (f"SELECT symbol, ANY_VALUE(instrument_key) ik "
             f"FROM `grow-profit-machine.autotrader.candles_daily` "
             f"WHERE UPPER(symbol) IN ({syms}) AND instrument_key IS NOT NULL GROUP BY symbol")
        return {str(r["symbol"]).strip().upper(): str(r["ik"]) for r in bq.query(q)}
    except Exception as exc:
        logger.error("delivery_resolve_keys_failed err=%s", exc)
        return {}


def _delivery_realized_today(state, asof: str) -> float:
    """Sum realized P&L of DELIVERY positions closed today (for the daily breaker).
    Fail-closed: returns NaN on error so ``daily_breaker_tripped`` halts entries (an
    unreadable ledger must never silently ENABLE trading)."""
    try:
        total = 0.0
        for p in state.list_all_positions(limit=500):
            if str(p.get("channel", "")).strip().lower() != "delivery":
                continue
            if str(p.get("status", "")).upper() != "CLOSED":
                continue
            if str(p.get("exit_ts", ""))[:10] != asof:
                continue
            total += float(p.get("pnl", 0.0) or 0.0)
        return total
    except Exception as exc:
        logger.error("delivery_realized_today_failed err=%s — failing closed (breaker trips)", exc)
        return float("nan")


def _candidate_status(sym, planned_syms, entered_syms, held, breaker):
    """Annotate why each candidate was/wasn't taken (for the dashboard watchlist)."""
    if sym in entered_syms:
        return "ENTERED"
    if sym in held:
        return "ALREADY_HELD"
    if breaker:
        return "BREAKER_HALT"
    if sym in planned_syms:
        return "PLANNED_NOT_FILLED"     # order rejected/skipped (e.g. dup idempotency)
    return "NOT_SELECTED"               # below the 5-slot delivery-% rank


def _persist_delivery_watchlist(state, asof, channel_capital, realized_today,
                                candidates, planned_syms, entered_syms, held, cfg):
    try:
        loss_limit = -abs(cfg.daily_loss_pct) * channel_capital
        profit_limit = abs(cfg.daily_profit_pct) * channel_capital
        breaker = pead_book.daily_breaker_tripped(realized_today, loss_limit, profit_limit)
        rows = [{
            "symbol": c["symbol"],
            "deliv_pct": round(float(c["deliv_pct"]), 2),
            "turnover_cr": round(float(c.get("turnover_cr") or 0.0), 2),
            "atr": round(float(c["atr"]), 4),
            "reaction_close": round(float(c["reaction_close"]), 2),
            "reaction_date": c.get("reaction_date", asof),
            "status": _candidate_status(c["symbol"], planned_syms, entered_syms, held, breaker),
        } for c in candidates]
        payload = {
            "asof": asof,
            "channel": "delivery",
            "breaker_tripped": breaker,
            "candidates": len(rows),
            "entered": len(entered_syms),
            "held_before": len(held),
            "rows": rows,
        }
        state.set_json("delivery_watchlist", asof, payload, merge=False)
        state.set_json("delivery_watchlist", "latest", payload, merge=False)
    except Exception:
        logger.warning("delivery_watchlist_persist_failed asof=%s — non-critical", asof, exc_info=True)


def run_delivery_scan_once(*, settings, upstox, state, order_service, bq=None,
                           reaction_date: str | None = None) -> dict[str, Any]:
    """Live DELIVERY daily scan + entry (PAPER). Fail-closed at every step.

    Returns a summary dict (also used by the HTTP endpoint). Reuses the existing Upstox
    client, Firestore state, and order_service — no swing/intraday/other-channel path.
    """
    from autotrader.time_utils import now_ist
    cfg = settings.strategy
    asof = reaction_date or now_ist().strftime("%Y-%m-%d")
    channel_capital = cfg.channel_capital("delivery")
    if channel_capital <= 0:
        return {"skipped": "delivery_capital_zero", "asof": asof}
    if bq is None:
        return {"skipped": "no_bq", "asof": asof}

    reaction_target = reaction_date or delivery_signal_service.latest_reaction_date(bq)
    if not reaction_target:
        return {"skipped": "no_delivery_data", "asof": asof}

    rows = delivery_signal_service.fetch_delivery_rows(bq, reaction_target, 70.0)
    # cheap turnover pre-cut around the 25-50cr band (loose bounds; the exact 20d-mean
    # band gate is applied in build_candidates on real bars). Skips large-caps/penny early.
    rows = {s: v for s, v in rows.items() if 10.0 <= float(v.get("turnover_cr") or 0.0) <= 120.0}
    # ETFs are excluded by build_candidates anyway (delivery_signals.is_etf), but dropping them
    # here too (free, string-only check) avoids a wasted Upstox fetch + noisy error log for known
    # ETF names that recur daily in nse_delivery_daily (e.g. PSUBANK -> invalid instrument_key).
    rows = {s: v for s, v in rows.items() if not delivery_signals.is_etf(s)}
    if not rows:
        return {"asof": asof, "reaction_date": reaction_target, "delivery_rows": 0,
                "candidates": 0, "entered": 0}

    # resolve instrument keys + fetch fresh dailies for the delivery names
    key_map = _resolve_instrument_keys(sorted(rows.keys()), bq)
    candles: dict[str, list[list]] = {}
    ik_for: dict[str, str] = {}
    for sym in sorted(rows.keys()):
        ik = key_map.get(sym)
        if not ik:
            continue
        bars = _fetch_symbol_daily(upstox, ik, reaction_target)
        if len(bars) >= delivery_signals.MIN_BARS:
            candles[sym] = bars
            ik_for[sym] = ik

    candidates = delivery_signal_service.scan(
        reaction_target, rows, candles,
        deliv_min=cfg.delivery_deliv_min,
        turnover_min_cr=cfg.delivery_turnover_min_cr,
        turnover_max_cr=cfg.delivery_turnover_max_cr,
        price_min=delivery_signals.PRICE_MIN,
    )
    for c in candidates:
        c["instrument_key"] = ik_for.get(c["symbol"], "")

    open_positions = [p for p in state.list_open_positions()
                      if str(p.get("channel", "")).strip().lower() == "delivery"]
    open_syms = [p.get("symbol") for p in open_positions]
    realized_today = _delivery_realized_today(state, asof)

    specs = plan_delivery_entries(candidates, open_syms, realized_today, channel_capital, cfg)
    planned_syms = {s["symbol"] for s in specs}
    entered_syms: set[str] = set()
    for s in specs:
        try:
            res = order_service.place_entry_order(
                symbol=s["symbol"], exchange=DELIVERY_EXCHANGE, segment=DELIVERY_SEGMENT,
                side="BUY", qty=s["qty"], entry_price=s["entry_price"], sl_price=s["sl_price"],
                target=s["target"], atr=s["atr"], product="CNC", score=int(round(s["deliv_pct"] * 100)),
                reason="DELIVERY", instrument_key=s["instrument_key"], strategy="DELIVERY",
                wl_type="delivery", channel="delivery",
            )
            if res and not res.get("error") and not res.get("skipped"):
                entered_syms.add(s["symbol"])
        except Exception:
            logger.exception("delivery_place_entry_failed sym=%s", s["symbol"])

    # persist the day's candidate watchlist (annotated) so the dashboard can show delivery
    # the same way as swing/intraday. Separate `delivery_watchlist` collection. Best-effort.
    _persist_delivery_watchlist(state, reaction_target, channel_capital, realized_today,
                                candidates, planned_syms, entered_syms, set(open_syms), cfg)

    summary = {"asof": asof, "reaction_date": reaction_target, "delivery_rows": len(rows),
               "candidates": len(candidates), "planned": len(specs),
               "entered": len(entered_syms), "open_before": len(open_syms)}
    logger.info("delivery_scan_summary %s", summary)
    return summary
