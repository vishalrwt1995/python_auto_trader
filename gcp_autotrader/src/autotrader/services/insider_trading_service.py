"""INSIDER Cluster-Buy trading service — daily premarket scan -> 10-slot book -> PAPER CNC entries.

Hybrid architecture (mirrors delivery/PEAD): a NEW thin service that owns the insider signal +
book + entry, but REUSES the proven order_service / positions / exit plumbing via
``channel="insider"`` + ``wl_type="insider"``. It does NOT touch the swing/intraday hot path or
any other channel. The distinct ``wl_type="insider"`` keeps insider positions out of swing_recon
(``wl_type=="swing"``) and the intraday tick exits (``ws_monitor`` excludes it via
``_OVERNIGHT_SL_ONLY_WL``).

Daily flow (premarket, ~09:10 IST, on the prior evening's ingested disclosures):
  1. **DOUBLE MACRO GATE** (the DD-killer): breadth ``b200 > 50`` (brain ``breadth_ema200_pct``,
     Firestore) AND Nifty ``> 100DMA`` (``momentum_signal_service.fetch_nifty_regime``). BOTH
     must hold or the whole scan is skipped (no entries) — fail-closed if either is unreadable.
  2. ``latest_reaction_date`` (BQ ``nse_insider_daily`` MAX(date)) = the dissemination day.
  3. ``fetch_disclosure_rows`` -> ``insider_signals.aggregate_clusters`` -> clustered symbols
     (>=2 informed open-market buy legs, each >= Rs 5L).
  4. resolve instrument keys + fresh daily bars per name (Upstox) -> turnover + ATR.
  5. ``insider_signal_service.scan`` -> qualifying candidates (cluster-strength ranked).
  6. ``plan_insider_entries`` (pure): daily breaker + 10-slot cap + risk/notional/participation.
  7. ``order_service.place_entry_order(channel="insider", wl_type="insider", product="CNC")``.

Exit is a FIXED 90-session hold + 2.5xATR protective stop (NO trail — validated), owned by
``insider_reconciliation_service``. PAPER until explicitly flipped (Rule 5).

Validated GOD-MODE config (2026-07-20): cluster>=2 + b200>50 + Nifty>100DMA, turnover>=10cr,
price>=30, hold 90d, 10 slots, ATR14x2.5 -> +23% CAGR / -12.5% DD / Calmar 1.84. See
``domain/insider_signals`` + ``docs/INSIDER_CHANNEL_PROPOSAL.md``.
"""
from __future__ import annotations

import logging
from typing import Any, Sequence

from autotrader.domain import insider_signals, pead_book
from autotrader.services import insider_signal_service

logger = logging.getLogger(__name__)

INSIDER_EXCHANGE = "NSE"
INSIDER_SEGMENT = "CASH"
SYMBOL_HISTORY_DAYS = 130         # > MIN_BARS (20d turnover + ATR14 + margin)
TARGET_BACKSTOP_R = 20.0          # far placeholder target; real exit is 90d hold + SL
PARTICIPATION_PCT = 0.02          # position <= 2% of reaction-day traded turnover (capacity guard)


def plan_insider_entries(
    candidates: Sequence[dict[str, Any]],
    open_insider_symbols: Sequence[str],
    realized_today: float,
    channel_capital: float,
    cfg: Any,
) -> list[dict[str, Any]]:
    """PURE: given today's qualifying insider cluster candidates and the current book, return
    the order specs to place (PAPER). Applies (in order): per-channel daily loss/profit breaker,
    exclusion of held names, the 10-slot cluster-strength-ranked cap, risk + notional sizing, and
    the 2%-participation cap. Returns ``[]`` when the breaker trips / capital is zero / nothing
    fits. Side-effect-free + unit-testable. Entry price = reaction-day close (next-open proxy);
    target is a far backstop (real exit is the 90d hold managed by the reconciliation service)."""
    if channel_capital <= 0:
        logger.warning("insider_plan_no_capital — channel_capital=%s, skipping", channel_capital)
        return []
    loss_limit = -abs(cfg.daily_loss_pct) * channel_capital
    profit_limit = abs(cfg.daily_profit_pct) * channel_capital
    if pead_book.daily_breaker_tripped(realized_today, loss_limit, profit_limit):
        logger.info("insider_daily_breaker_tripped realized=%.0f loss_lim=%.0f profit_lim=%.0f — no entries",
                    realized_today or 0.0, loss_limit, profit_limit)
        return []

    held = {str(s).strip().upper() for s in open_insider_symbols}
    fresh = [c for c in candidates if str(c.get("symbol", "")).strip().upper() not in held]
    risk = cfg.insider_risk_per_trade if cfg.insider_risk_per_trade > 0 else 0.015 * channel_capital
    notional_cap = (cfg.insider_notional_cap_pct * channel_capital if cfg.insider_notional_cap_pct > 0
                    else channel_capital / max(1, cfg.insider_max_positions))
    picked = insider_signals.select_for_slots(fresh, len(held), cfg.insider_max_positions)

    specs: list[dict[str, Any]] = []
    for c in picked:
        entry = float(c["reaction_close"])
        sl_dist = pead_book.sl_distance(float(c["atr"]), entry, cfg.insider_atr_sl_mult)
        qty = pead_book.position_size(entry, sl_dist, risk, notional_cap)
        turnover_cr = float(c.get("turnover_cr", 0.0) or 0.0)
        if turnover_cr > 0 and entry > 0:
            part_cap_qty = int(PARTICIPATION_PCT * (turnover_cr * 1e7) / entry)
            qty = min(qty, part_cap_qty)
        if qty < 1:
            logger.info("insider_skip_qty_zero sym=%s entry=%.2f sl_dist=%.2f risk=%.0f cap=%.0f tov=%.1fcr",
                        c.get("symbol"), entry, sl_dist, risk, notional_cap, turnover_cr)
            continue
        specs.append({
            "symbol": str(c["symbol"]).strip().upper(),
            "qty": qty,
            "entry_price": round(entry, 2),
            "sl_price": round(entry - sl_dist, 2),
            "target": round(entry + sl_dist * TARGET_BACKSTOP_R, 2),
            "atr": round(float(c["atr"]), 4),
            "n_buyers": int(c.get("n_buyers", 0)),
            "total_val": float(c.get("total_val", 0.0)),
            "instrument_key": str(c.get("instrument_key", "")),
            "strategy": "INSIDER",
            "reaction_date": c.get("reaction_date", ""),
        })
    logger.info("insider_plan candidates=%d held=%d planned=%d", len(candidates), len(held), len(specs))
    return specs


# ── live I/O wrapper (validated in PAPER, not unit-tested — fail-closed) ───────
def _read_b200(state) -> float | None:
    """Live breadth (% of universe above EMA200) from the brain Firestore doc. None on error."""
    try:
        brain = state.get_market_brain() or {}
        v = brain.get("breadth_ema200_pct")
        return float(v) if v is not None else None
    except Exception as exc:
        logger.error("insider_read_b200_failed err=%s", exc)
        return None


def _macro_gate(state, upstox, cfg, asof: str) -> tuple[bool, dict[str, Any]]:
    """CHANNEL-LEVEL double macro gate: b200 > floor AND Nifty > 100DMA. Fail-closed: if either
    input is unreadable, gate is CLOSED (no entries). Reuses momentum's proven nifty-regime read."""
    from autotrader.services import momentum_signal_service
    b200 = _read_b200(state)
    b200_ok = b200 is not None and b200 > cfg.insider_b200_min
    try:
        nifty_ok = momentum_signal_service.fetch_nifty_regime(
            upstox, getattr(cfg, "nifty50_instrument_key", "NSE_INDEX|Nifty 50"), asof)
    except Exception as exc:
        logger.error("insider_nifty_regime_failed err=%s — failing closed", exc)
        nifty_ok = False
    ctx = {"b200": b200, "b200_ok": b200_ok, "nifty_above_100dma": bool(nifty_ok)}
    return (b200_ok and bool(nifty_ok)), ctx


def _fetch_symbol_daily(upstox, instrument_key: str, asof: str) -> list[list]:
    from datetime import date, timedelta
    try:
        frm = (date.fromisoformat(asof) - timedelta(days=int(SYMBOL_HISTORY_DAYS * 1.6))).isoformat()
        rows = upstox.get_historical_candles_v3_days(instrument_key, to_date=asof, from_date=frm, interval_days=1)
        bars = [[str(r[0])[:10], float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])]
                for r in rows if isinstance(r, list) and len(r) >= 6]
        bars.sort(key=lambda b: b[0])
        return bars
    except Exception as exc:
        logger.error("insider_fetch_symbol_failed key=%s err=%s", instrument_key, exc)
        return []


def _resolve_instrument_keys(symbols: Sequence[str], bq) -> dict[str, str]:
    try:
        syms = ",".join("'" + str(s).strip().upper().replace("'", "") + "'" for s in symbols)
        q = (f"SELECT symbol, ANY_VALUE(instrument_key) ik "
             f"FROM `grow-profit-machine.autotrader.candles_daily` "
             f"WHERE UPPER(symbol) IN ({syms}) AND instrument_key IS NOT NULL GROUP BY symbol")
        return {str(r["symbol"]).strip().upper(): str(r["ik"]) for r in bq.query(q)}
    except Exception as exc:
        logger.error("insider_resolve_keys_failed err=%s", exc)
        return {}


def _insider_realized_today(state, asof: str) -> float:
    """Sum realized P&L of INSIDER positions closed today (daily breaker). Fail-closed -> NaN
    (breaker trips) so an unreadable ledger can never silently ENABLE trading."""
    try:
        total = 0.0
        for p in state.list_all_positions(limit=500):
            if str(p.get("channel", "")).strip().lower() != "insider":
                continue
            if str(p.get("status", "")).upper() != "CLOSED":
                continue
            if str(p.get("exit_ts", ""))[:10] != asof:
                continue
            total += float(p.get("pnl", 0.0) or 0.0)
        return total
    except Exception as exc:
        logger.error("insider_realized_today_failed err=%s — failing closed (breaker trips)", exc)
        return float("nan")


def _candidate_status(sym, planned_syms, entered_syms, held, breaker, macro_ok):
    if not macro_ok:
        return "MACRO_GATE_OFF"
    if sym in entered_syms:
        return "ENTERED"
    if sym in held:
        return "ALREADY_HELD"
    if breaker:
        return "BREAKER_HALT"
    if sym in planned_syms:
        return "PLANNED_NOT_FILLED"
    return "NOT_SELECTED"


def _persist_insider_watchlist(state, asof, channel_capital, realized_today, candidates,
                               planned_syms, entered_syms, held, macro_ok, macro_ctx, cfg):
    try:
        loss_limit = -abs(cfg.daily_loss_pct) * channel_capital
        profit_limit = abs(cfg.daily_profit_pct) * channel_capital
        breaker = pead_book.daily_breaker_tripped(realized_today, loss_limit, profit_limit)
        rows = [{
            "symbol": c["symbol"],
            "n_buyers": int(c.get("n_buyers", 0)),
            "total_val_cr": round(float(c.get("total_val") or 0.0) / 1e7, 3),
            "category": c.get("category", ""),
            "turnover_cr": round(float(c.get("turnover_cr") or 0.0), 2),
            "atr": round(float(c["atr"]), 4),
            "reaction_close": round(float(c["reaction_close"]), 2),
            "reaction_date": c.get("reaction_date", asof),
            "status": _candidate_status(c["symbol"], planned_syms, entered_syms, held, breaker, macro_ok),
        } for c in candidates]
        payload = {"asof": asof, "channel": "insider", "macro_gate_ok": macro_ok,
                   "macro": macro_ctx, "breaker_tripped": breaker, "candidates": len(rows),
                   "entered": len(entered_syms), "held_before": len(held), "rows": rows}
        state.set_json("insider_watchlist", asof, payload, merge=False)
        state.set_json("insider_watchlist", "latest", payload, merge=False)
    except Exception:
        logger.warning("insider_watchlist_persist_failed asof=%s — non-critical", asof, exc_info=True)


def run_insider_scan_once(*, settings, upstox, state, order_service, bq=None,
                          reaction_date: str | None = None) -> dict[str, Any]:
    """Live INSIDER daily scan + entry (PAPER). Fail-closed at every step. Reuses the existing
    Upstox client, Firestore state, and order_service — no swing/intraday/other-channel path."""
    from autotrader.time_utils import now_ist
    cfg = settings.strategy
    asof = reaction_date or now_ist().strftime("%Y-%m-%d")
    channel_capital = cfg.channel_capital("insider")
    if channel_capital <= 0:
        return {"skipped": "insider_capital_zero", "asof": asof}
    if bq is None:
        return {"skipped": "no_bq", "asof": asof}

    # 1. DOUBLE MACRO GATE (evaluated once; bad regime => no entries)
    macro_ok, macro_ctx = _macro_gate(state, upstox, cfg, asof)

    reaction_target = reaction_date or insider_signal_service.latest_reaction_date(bq)
    if not reaction_target:
        return {"skipped": "no_insider_data", "asof": asof, "macro": macro_ctx}

    raw_rows = insider_signal_service.fetch_disclosure_rows(bq, reaction_target)
    clusters = insider_signals.aggregate_clusters(
        raw_rows, min_buyers=cfg.insider_min_buyers, min_leg_value=cfg.insider_min_leg_value)
    if not clusters:
        return {"asof": asof, "reaction_date": reaction_target, "macro_gate_ok": macro_ok,
                "clusters": 0, "candidates": 0, "entered": 0}

    # resolve instrument keys + fresh dailies for the clustered names
    key_map = _resolve_instrument_keys(sorted(clusters.keys()), bq)
    candles: dict[str, list[list]] = {}
    ik_for: dict[str, str] = {}
    for sym in sorted(clusters.keys()):
        ik = key_map.get(sym)
        if not ik:
            continue
        bars = _fetch_symbol_daily(upstox, ik, reaction_target)
        if len(bars) >= insider_signals.MIN_BARS:
            candles[sym] = bars
            ik_for[sym] = ik

    candidates = insider_signal_service.scan(
        reaction_target, clusters, candles,
        turnover_min_cr=cfg.insider_turnover_min_cr, price_min=insider_signals.PRICE_MIN)
    for c in candidates:
        c["instrument_key"] = ik_for.get(c["symbol"], "")

    open_positions = [p for p in state.list_open_positions()
                      if str(p.get("channel", "")).strip().lower() == "insider"]
    open_syms = [p.get("symbol") for p in open_positions]
    realized_today = _insider_realized_today(state, asof)

    # macro gate closed => plan nothing (but still persist the watchlist for visibility)
    specs = (plan_insider_entries(candidates, open_syms, realized_today, channel_capital, cfg)
             if macro_ok else [])
    planned_syms = {s["symbol"] for s in specs}
    entered_syms: set[str] = set()
    for s in specs:
        try:
            res = order_service.place_entry_order(
                symbol=s["symbol"], exchange=INSIDER_EXCHANGE, segment=INSIDER_SEGMENT,
                side="BUY", qty=s["qty"], entry_price=s["entry_price"], sl_price=s["sl_price"],
                target=s["target"], atr=s["atr"], product="CNC", score=int(s["n_buyers"]),
                reason="INSIDER", instrument_key=s["instrument_key"], strategy="INSIDER",
                wl_type="insider", channel="insider",
            )
            if res and not res.get("error") and not res.get("skipped"):
                entered_syms.add(s["symbol"])
        except Exception:
            logger.exception("insider_place_entry_failed sym=%s", s["symbol"])

    _persist_insider_watchlist(state, reaction_target, channel_capital, realized_today, candidates,
                               planned_syms, entered_syms, set(open_syms), macro_ok, macro_ctx, cfg)

    summary = {"asof": asof, "reaction_date": reaction_target, "macro_gate_ok": macro_ok,
               "macro": macro_ctx, "clusters": len(clusters), "candidates": len(candidates),
               "planned": len(specs), "entered": len(entered_syms), "open_before": len(open_syms)}
    logger.info("insider_scan_summary %s", summary)
    return summary
