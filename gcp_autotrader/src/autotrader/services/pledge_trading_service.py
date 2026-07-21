"""PLEDGE (promoter pledge-release) trading service — daily premarket scan -> 10-slot book -> PAPER CNC.

Hybrid architecture (mirrors insider/delivery): a NEW thin service that owns the pledge signal +
book + entry, but REUSES the proven order_service / positions / exit plumbing via
``channel="pledge"`` + ``wl_type="pledge"``. It does NOT touch the swing/intraday hot path or any
other channel. The distinct ``wl_type="pledge"`` keeps pledge positions out of swing_recon
(``wl_type=="swing"``) and the intraday tick exits (``ws_monitor`` excludes it via
``_OVERNIGHT_SL_ONLY_WL``).

Reuses the SAME ingest + BQ table as insider (``nse_insider_daily`` — the ingest writes every
transaction leg incl. pledge/revoke verbatim), so there is NO new ingest and NO new table; pledge
only reads with a ``transaction_type ~ revoke`` + promoter filter.

Daily flow (premarket, ~09:12 IST, on the prior evening's ingested disclosures):
  1. **DOUBLE MACRO GATE** (the DD-killer): breadth ``b200 > 50`` (brain, Firestore) AND Nifty
     ``> 100DMA``. BOTH must hold or the whole scan is skipped — fail-closed if either unreadable.
  2. ``latest_reaction_date`` (BQ ``nse_insider_daily`` MAX(date)) = the dissemination day.
  3. ``fetch_revoke_rows`` -> ``pledge_signals.aggregate_revokes`` -> promoter revoke symbols.
  4. resolve instrument keys + fresh daily bars per name (Upstox) -> turnover + ATR + 200DMA.
  5. ``pledge_signal_service.scan`` -> qualifying candidates (px>200DMA; liquidity-ranked).
  6. ``plan_pledge_entries`` (pure): daily breaker + 10-slot cap + risk/notional/participation.
  7. ``order_service.place_entry_order(channel="pledge", wl_type="pledge", product="CNC")``.

Exit is a FIXED 60-session hold + 2.0xATR protective stop (NO trail — validated), owned by
``pledge_reconciliation_service``. PAPER until explicitly flipped (Rule 5).

Validated FINAL config (2026-07-21): promoter revoke + px>200DMA + b200>50 + Nifty>100DMA,
turnover>=25cr, price>=30, hold 60d, 10 slots, ATR14x2.0, cap10% -> +25% CAGR / -11.5% DD /
Calmar 2.18. See ``domain/pledge_signals`` + ``docs/PLEDGE_CHANNEL.md``.
"""
from __future__ import annotations

import logging
from typing import Any, Sequence

from autotrader.domain import pledge_signals, pead_book
from autotrader.services import pledge_signal_service

logger = logging.getLogger(__name__)

PLEDGE_EXCHANGE = "NSE"
PLEDGE_SEGMENT = "CASH"
SYMBOL_HISTORY_DAYS = 320         # > MIN_BARS (need 200d SMA + ATR14 + 20d turnover + margin)
TARGET_BACKSTOP_R = 20.0          # far placeholder target; real exit is 60d hold + SL
PARTICIPATION_PCT = 0.02          # position <= 2% of reaction-day traded turnover (capacity guard)


def plan_pledge_entries(
    candidates: Sequence[dict[str, Any]],
    open_pledge_symbols: Sequence[str],
    realized_today: float,
    channel_capital: float,
    cfg: Any,
) -> list[dict[str, Any]]:
    """PURE: given today's qualifying pledge-revoke candidates and the current book, return the
    order specs to place (PAPER). Applies (in order): per-channel daily loss/profit breaker,
    exclusion of held names, the 10-slot liquidity-ranked cap, risk + notional (cap10%) sizing, and
    the 2%-participation cap. Returns ``[]`` when the breaker trips / capital is zero / nothing fits.
    Side-effect-free + unit-testable. Entry price = reaction-day close (next-open proxy); target is
    a far backstop (real exit is the 60d hold managed by the reconciliation service)."""
    if channel_capital <= 0:
        logger.warning("pledge_plan_no_capital — channel_capital=%s, skipping", channel_capital)
        return []
    loss_limit = -abs(cfg.daily_loss_pct) * channel_capital
    profit_limit = abs(cfg.daily_profit_pct) * channel_capital
    if pead_book.daily_breaker_tripped(realized_today, loss_limit, profit_limit):
        logger.info("pledge_daily_breaker_tripped realized=%.0f loss_lim=%.0f profit_lim=%.0f — no entries",
                    realized_today or 0.0, loss_limit, profit_limit)
        return []

    held = {str(s).strip().upper() for s in open_pledge_symbols}
    fresh = [c for c in candidates if str(c.get("symbol", "")).strip().upper() not in held]
    risk = cfg.pledge_risk_per_trade if cfg.pledge_risk_per_trade > 0 else 0.015 * channel_capital
    notional_cap = (cfg.pledge_notional_cap_pct * channel_capital if cfg.pledge_notional_cap_pct > 0
                    else channel_capital / max(1, cfg.pledge_max_positions))
    picked = pledge_signals.select_for_slots(fresh, len(held), cfg.pledge_max_positions)

    specs: list[dict[str, Any]] = []
    for c in picked:
        entry = float(c["reaction_close"])
        sl_dist = pead_book.sl_distance(float(c["atr"]), entry, cfg.pledge_atr_sl_mult)
        qty = pead_book.position_size(entry, sl_dist, risk, notional_cap)
        turnover_cr = float(c.get("turnover_cr", 0.0) or 0.0)
        if turnover_cr > 0 and entry > 0:
            part_cap_qty = int(PARTICIPATION_PCT * (turnover_cr * 1e7) / entry)
            qty = min(qty, part_cap_qty)
        if qty < 1:
            logger.info("pledge_skip_qty_zero sym=%s entry=%.2f sl_dist=%.2f risk=%.0f cap=%.0f tov=%.1fcr",
                        c.get("symbol"), entry, sl_dist, risk, notional_cap, turnover_cr)
            continue
        specs.append({
            "symbol": str(c["symbol"]).strip().upper(),
            "qty": qty,
            "entry_price": round(entry, 2),
            "sl_price": round(entry - sl_dist, 2),
            "target": round(entry + sl_dist * TARGET_BACKSTOP_R, 2),
            "atr": round(float(c["atr"]), 4),
            "n_revokes": int(c.get("n_revokes", 0)),
            "turnover_cr": turnover_cr,
            "instrument_key": str(c.get("instrument_key", "")),
            "strategy": "PLEDGE",
            "reaction_date": c.get("reaction_date", ""),
        })
    logger.info("pledge_plan candidates=%d held=%d planned=%d", len(candidates), len(held), len(specs))
    return specs


# ── live I/O wrapper (validated in PAPER, not unit-tested — fail-closed) ───────
def _read_b200(state) -> float | None:
    """Live breadth (% of universe above EMA200) from the brain Firestore doc. The raw doc nests it
    at ``context.breadthSnapshot.aboveEma200Pct``; falls back to a flat/parsed key. None on
    error/missing -> fail-closed gate."""
    try:
        brain = state.get_market_brain() or {}
        snap = (brain.get("context") or {}).get("breadthSnapshot") or {}
        v = snap.get("aboveEma200Pct")
        if v is None:
            v = brain.get("breadth_ema200_pct")   # fallback (parsed/flat shape)
        return float(v) if v is not None else None
    except Exception as exc:
        logger.error("pledge_read_b200_failed err=%s", exc)
        return None


def _macro_gate(state, upstox, cfg, asof: str) -> tuple[bool, dict[str, Any]]:
    """CHANNEL-LEVEL double macro gate: b200 > floor AND Nifty > 100DMA. Fail-closed: if either
    input is unreadable, gate is CLOSED (no entries). Reuses momentum's proven nifty-regime read."""
    from autotrader.services import momentum_signal_service
    b200 = _read_b200(state)
    b200_ok = b200 is not None and b200 > cfg.pledge_b200_min
    try:
        nifty_ok = momentum_signal_service.fetch_nifty_regime(
            upstox, getattr(cfg, "nifty50_instrument_key", "NSE_INDEX|Nifty 50"), asof)
    except Exception as exc:
        logger.error("pledge_nifty_regime_failed err=%s — failing closed", exc)
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
        logger.error("pledge_fetch_symbol_failed key=%s err=%s", instrument_key, exc)
        return []


def _resolve_instrument_keys(symbols: Sequence[str], bq) -> dict[str, str]:
    try:
        syms = ",".join("'" + str(s).strip().upper().replace("'", "") + "'" for s in symbols)
        q = (f"SELECT symbol, ANY_VALUE(instrument_key) ik "
             f"FROM `grow-profit-machine.autotrader.candles_daily` "
             f"WHERE UPPER(symbol) IN ({syms}) AND instrument_key IS NOT NULL GROUP BY symbol")
        return {str(r["symbol"]).strip().upper(): str(r["ik"]) for r in bq.query(q)}
    except Exception as exc:
        logger.error("pledge_resolve_keys_failed err=%s", exc)
        return {}


def _pledge_realized_today(state, asof: str) -> float:
    """Sum realized P&L of PLEDGE positions closed today (daily breaker). Fail-closed -> NaN
    (breaker trips) so an unreadable ledger can never silently ENABLE trading."""
    try:
        total = 0.0
        for p in state.list_all_positions(limit=500):
            if str(p.get("channel", "")).strip().lower() != "pledge":
                continue
            if str(p.get("status", "")).upper() != "CLOSED":
                continue
            if str(p.get("exit_ts", ""))[:10] != asof:
                continue
            total += float(p.get("pnl", 0.0) or 0.0)
        return total
    except Exception as exc:
        logger.error("pledge_realized_today_failed err=%s — failing closed (breaker trips)", exc)
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


def _persist_pledge_watchlist(state, asof, channel_capital, realized_today, candidates,
                              planned_syms, entered_syms, held, macro_ok, macro_ctx, cfg):
    try:
        loss_limit = -abs(cfg.daily_loss_pct) * channel_capital
        profit_limit = abs(cfg.daily_profit_pct) * channel_capital
        breaker = pead_book.daily_breaker_tripped(realized_today, loss_limit, profit_limit)
        rows = [{
            "symbol": c["symbol"],
            "n_revokes": int(c.get("n_revokes", 0)),
            "category": c.get("category", ""),
            "turnover_cr": round(float(c.get("turnover_cr") or 0.0), 2),
            "atr": round(float(c["atr"]), 4),
            "reaction_close": round(float(c["reaction_close"]), 2),
            "reaction_date": c.get("reaction_date", asof),
            "status": _candidate_status(c["symbol"], planned_syms, entered_syms, held, breaker, macro_ok),
        } for c in candidates]
        payload = {"asof": asof, "channel": "pledge", "macro_gate_ok": macro_ok,
                   "macro": macro_ctx, "breaker_tripped": breaker, "candidates": len(rows),
                   "entered": len(entered_syms), "held_before": len(held), "rows": rows}
        state.set_json("pledge_watchlist", asof, payload, merge=False)
        state.set_json("pledge_watchlist", "latest", payload, merge=False)
    except Exception:
        logger.warning("pledge_watchlist_persist_failed asof=%s — non-critical", asof, exc_info=True)


def _persist_pledge_status(state, asof, reaction_date, macro_ok, macro_ctx, revoke_syms,
                           candidates=None):
    """Persist a compact pledge gate/status doc to Firestore ``pledge_watchlist/latest`` on EVERY
    scan — including gated-off / 0-revoke days — so the dashboard drill-down always shows the
    current macro-gate state (why it is/isn't trading). Best-effort, read-only downstream."""
    try:
        rows = [{
            "symbol": c["symbol"], "n_revokes": int(c.get("n_revokes", 0)),
            "category": c.get("category", ""),
            "turnover_cr": round(float(c.get("turnover_cr") or 0.0), 2),
            "reaction_close": round(float(c.get("reaction_close") or 0.0), 2),
            "reaction_date": c.get("reaction_date", reaction_date),
            "status": "MACRO_GATE_OFF" if not macro_ok else "NOT_SELECTED",
        } for c in (candidates or [])]
        payload = {"asof": asof, "reaction_date": reaction_date, "channel": "pledge",
                   "macro_gate_ok": bool(macro_ok), "macro": macro_ctx or {},
                   "revoke_symbols": int(revoke_syms), "candidates": len(rows), "entered": 0, "rows": rows}
        state.set_json("pledge_watchlist", reaction_date or asof or "latest", payload, merge=False)
        state.set_json("pledge_watchlist", "latest", payload, merge=False)
    except Exception:
        logger.warning("pledge_status_persist_failed asof=%s — non-critical", asof, exc_info=True)


def run_pledge_scan_once(*, settings, upstox, state, order_service, bq=None,
                         reaction_date: str | None = None) -> dict[str, Any]:
    """Live PLEDGE daily scan + entry (PAPER). Fail-closed at every step. Reuses the existing Upstox
    client, Firestore state, and order_service — no swing/intraday/other-channel path."""
    from autotrader.time_utils import now_ist
    cfg = settings.strategy
    asof = reaction_date or now_ist().strftime("%Y-%m-%d")
    channel_capital = cfg.channel_capital("pledge")
    # fail-closed: no-op unless the channel's OWN capital is funded (channel_capital falls back to
    # shared capital for single-pool compat — must not trade on it when CAPITAL_PLEDGE is unset).
    if cfg.capital_pledge <= 0 or channel_capital <= 0:
        return {"skipped": "pledge_capital_zero", "asof": asof}
    if bq is None:
        return {"skipped": "no_bq", "asof": asof}

    # 1. DOUBLE MACRO GATE (evaluated once; bad regime => no entries)
    macro_ok, macro_ctx = _macro_gate(state, upstox, cfg, asof)

    reaction_target = reaction_date or pledge_signal_service.latest_reaction_date(bq)
    if not reaction_target:
        summary = {"asof": asof, "reaction_date": "", "macro_gate_ok": macro_ok, "macro": macro_ctx,
                   "revoke_symbols": 0, "candidates": 0, "entered": 0, "skipped": "no_insider_data"}
        _persist_pledge_status(state, asof, "", macro_ok, macro_ctx, 0)
        logger.info("pledge_scan_summary %s", summary)
        return summary

    raw_rows = pledge_signal_service.fetch_revoke_rows(bq, reaction_target)
    revokes = pledge_signals.aggregate_revokes(raw_rows)
    if not revokes:
        summary = {"asof": asof, "reaction_date": reaction_target, "macro_gate_ok": macro_ok,
                   "macro": macro_ctx, "revoke_symbols": 0, "candidates": 0, "entered": 0}
        _persist_pledge_status(state, asof, reaction_target, macro_ok, macro_ctx, 0)
        logger.info("pledge_scan_summary %s", summary)
        return summary

    # resolve instrument keys + fresh dailies for the candidate names (need >=200 bars for 200DMA)
    key_map = _resolve_instrument_keys(sorted(revokes.keys()), bq)
    candles: dict[str, list[list]] = {}
    ik_for: dict[str, str] = {}
    for sym in sorted(revokes.keys()):
        ik = key_map.get(sym)
        if not ik:
            continue
        bars = _fetch_symbol_daily(upstox, ik, reaction_target)
        if len(bars) >= pledge_signals.MIN_BARS:
            candles[sym] = bars
            ik_for[sym] = ik

    candidates = pledge_signal_service.scan(
        reaction_target, revokes, candles,
        turnover_min_cr=cfg.pledge_turnover_min_cr, price_min=pledge_signals.PRICE_MIN)
    for c in candidates:
        c["instrument_key"] = ik_for.get(c["symbol"], "")

    open_positions = [p for p in state.list_open_positions()
                      if str(p.get("channel", "")).strip().lower() == "pledge"]
    open_syms = [p.get("symbol") for p in open_positions]
    realized_today = _pledge_realized_today(state, asof)

    # macro gate closed => plan nothing (but still persist the watchlist for visibility)
    specs = (plan_pledge_entries(candidates, open_syms, realized_today, channel_capital, cfg)
             if macro_ok else [])
    planned_syms = {s["symbol"] for s in specs}
    entered_syms: set[str] = set()
    for s in specs:
        try:
            res = order_service.place_entry_order(
                symbol=s["symbol"], exchange=PLEDGE_EXCHANGE, segment=PLEDGE_SEGMENT,
                side="BUY", qty=s["qty"], entry_price=s["entry_price"], sl_price=s["sl_price"],
                target=s["target"], atr=s["atr"], product="CNC", score=int(s["n_revokes"]),
                reason="PLEDGE", instrument_key=s["instrument_key"], strategy="PLEDGE",
                wl_type="pledge", channel="pledge",
            )
            if res and not res.get("error") and not res.get("skipped"):
                entered_syms.add(s["symbol"])
        except Exception:
            logger.exception("pledge_place_entry_failed sym=%s", s["symbol"])

    _persist_pledge_watchlist(state, reaction_target, channel_capital, realized_today, candidates,
                              planned_syms, entered_syms, set(open_syms), macro_ok, macro_ctx, cfg)

    summary = {"asof": asof, "reaction_date": reaction_target, "macro_gate_ok": macro_ok,
               "macro": macro_ctx, "revoke_symbols": len(revokes), "candidates": len(candidates),
               "planned": len(specs), "entered": len(entered_syms), "open_before": len(open_syms)}
    logger.info("pledge_scan_summary %s", summary)
    return summary
