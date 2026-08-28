"""Corp-action (bonus/split) trading service — daily premarket scan -> SHARED EVENT-pool
entry (PAPER), the second sub-strategy of the EVENT/PEAD channel.

Shares PEAD's capital pool, 5-slot cap, and daily breaker WITHOUT touching PEAD's code:
both tag ``channel="pead"``, and PEAD already counts the book + breaker by ``channel``,
so corp positions are automatically counted toward the shared 5-cap and the shared
−3%/+6% breaker. Corp is additionally capped at ``corp_max_positions`` (2) concurrent.
Exit isolation: corp positions are ``wl_type="corp_action"`` (NOT "pead"), so
pead_reconciliation / swing_reconciliation never touch them — corp_action_reconciliation
owns the hard meeting-day exit. The protective resting SL is wide (disaster backstop only;
the backtest holds to the meeting with no stop).

Sizing: fixed notional (``corp_notional_cap_pct`` of channel capital), qty = notional/price.
Entry: market-on-open next session (premarket sizing proxy = intimation-day close). All
constants mirror the validated look-ahead-free backtest (+1.54% net/event, robust IS+OOS).
PAPER until explicitly flipped (Rule 5). The live I/O wrapper is fail-closed.
"""
from __future__ import annotations

import logging
from typing import Any, Sequence

from autotrader.adapters import instrument_keys
from autotrader.domain import etf_filter
from autotrader.domain import pead_book
from autotrader.services import corp_action_signal_service, pead_trading_service

logger = logging.getLogger(__name__)

PRIOR_KEYS_DOC = "corp_event_history"        # Firestore: persisted (symbol|type) first-time history
TARGET_BACKSTOP_R = 10.0


def plan_corp_entries(
    candidates: Sequence[dict[str, Any]],
    open_channel_symbols: Sequence[str],
    open_corp_count: int,
    realized_today: float,
    channel_capital: float,
    cfg: Any,
) -> list[dict[str, Any]]:
    """PURE: given today's corp candidates + shared-channel book state, return order specs.

    Applies, in order: the SHARED channel daily breaker (3%/6% of channel capital), the
    SHARED 5-slot cap (counting ALL channel="pead" positions = pead + corp), the corp
    sub-cap (``corp_max_positions``), exclusion of names already held in the channel, and
    fixed-notional sizing with a wide protective stop. Candidates are pre-ranked by uptrend.
    Side-effect-free + unit-testable.
    """
    if channel_capital <= 0:
        return []
    loss_limit = -abs(cfg.daily_loss_pct) * channel_capital
    profit_limit = abs(cfg.daily_profit_pct) * channel_capital
    if pead_book.daily_breaker_tripped(realized_today, loss_limit, profit_limit):
        logger.info("corp_daily_breaker_tripped realized=%.0f — no entries", realized_today or 0.0)
        return []

    held = {str(s).strip().upper() for s in open_channel_symbols}
    channel_room = max(0, cfg.pead_max_positions - len(held))          # shared 5-cap (pead + corp)
    corp_room = max(0, cfg.corp_max_positions - max(0, open_corp_count))  # corp sub-cap
    room = min(channel_room, corp_room)
    if room <= 0:
        logger.info("corp_no_room channel_held=%d corp_open=%d", len(held), open_corp_count)
        return []

    notional = cfg.corp_notional_cap_pct * channel_capital
    specs: list[dict[str, Any]] = []
    for c in candidates:
        if len(specs) >= room:
            break
        sym = str(c["symbol"]).strip().upper()
        if sym in held:
            continue
        entry = float(c["ref_close"])
        if entry <= 0:
            continue
        qty = int(notional // entry)
        if qty < 1:
            logger.info("corp_skip_qty_zero sym=%s entry=%.2f notional=%.0f", sym, entry, notional)
            continue
        sl = round(entry * (1.0 - cfg.corp_protective_stop_pct), 2)    # wide disaster backstop
        specs.append({
            "symbol": sym,
            "qty": qty,
            "entry_price": round(entry, 2),
            "sl_price": sl,
            "target": round(entry * (1.0 + cfg.corp_protective_stop_pct * TARGET_BACKSTOP_R), 2),
            "atr": round(entry * cfg.corp_protective_stop_pct, 4),
            "event_type": c.get("event_type", ""),
            "meeting_date": c.get("meeting_date", ""),
            "dist_low": float(c.get("dist_low", 0.0)),
            "instrument_key": str(c.get("instrument_key", "")),
            "strategy": "CORP_ACTION",
        })
    logger.info("corp_plan candidates=%d channel_held=%d room=%d planned=%d",
                len(candidates), len(held), room, len(specs))
    return specs


# ── live I/O wrapper (validated in PAPER, not unit-tested — fail-closed) ───────
def _fetch_eqweight_market(bq, asof: str, days: int = 70) -> dict[str, float]:
    """Equal-weight universe index level by date (the anti-pump benchmark), computed in BQ
    over the last ``days`` for established names (>=300 bars). Returns ``{date: level}``;
    fail-closed -> ``{}`` (then the anti-pump gate sees the raw run-up, the conservative
    direction). Mirrors the backtest's eq-weight construction (the RIGHT small/mid
    benchmark — NIFTY-50 under-adjusts)."""
    if bq is None:
        return {}
    try:
        q = (
            "WITH est AS (SELECT symbol FROM `grow-profit-machine.autotrader.candles_daily` "
            "GROUP BY symbol HAVING COUNT(*) >= 300), "
            "r AS (SELECT trade_date, SAFE_DIVIDE(close, LAG(close) OVER "
            "(PARTITION BY symbol ORDER BY trade_date)) - 1 AS ret "
            "FROM `grow-profit-machine.autotrader.candles_daily` "
            "WHERE symbol IN (SELECT symbol FROM est) "
            f"AND trade_date >= DATE_SUB(DATE('{asof}'), INTERVAL {days} DAY)) "
            "SELECT FORMAT_DATE('%Y-%m-%d', trade_date) d, "
            "AVG(IF(ABS(ret) < 0.30, ret, NULL)) mret, COUNT(ret) n "
            "FROM r WHERE ret IS NOT NULL GROUP BY trade_date HAVING n >= 50 ORDER BY d"
        )
        lvl: dict[str, float] = {}
        ix = 1.0
        for row in bq.query(q):
            ix *= (1.0 + float(row["mret"]))
            lvl[str(row["d"])] = ix
        return lvl
    except Exception as exc:
        logger.error("corp_eqweight_market_failed asof=%s err=%s — anti-pump uses raw runup", asof, exc)
        return {}


def _load_prior_keys(state) -> set[tuple[str, str]]:
    """Persisted (symbol, type) first-time history (bootstrapped from the archive at
    deploy). Empty set on miss -> everything looks first-time (over-includes until seeded)."""
    try:
        doc = state.get_json(PRIOR_KEYS_DOC, "all") or {}
        return {(str(k).split("|")[0], str(k).split("|")[1]) for k in doc.get("keys", []) if "|" in str(k)}
    except Exception as exc:
        logger.error("corp_load_prior_keys_failed err=%s", exc)
        return set()


def _save_prior_keys(state, keys: set[tuple[str, str]]) -> None:
    try:
        state.set_json(PRIOR_KEYS_DOC, "all", {"keys": sorted(f"{s}|{t}" for s, t in keys)}, merge=False)
    except Exception:
        logger.warning("corp_save_prior_keys_failed — non-critical", exc_info=True)


def _channel_realized_today(state, asof: str) -> float:
    """Shared EVENT-channel realized P&L today (pead + corp, by channel) for the shared
    breaker. Reuses PEAD's channel-keyed accounting. NaN on read failure -> breaker trips."""
    return pead_trading_service._pead_realized_today(state, asof)


def run_corp_action_scan_once(*, settings, upstox, state, order_service, bq=None,
                              entry_date: str | None = None) -> dict[str, Any]:
    """Live corp-action daily scan + entry (PAPER). Shares the EVENT pool; fail-closed.

    Mirrors PEAD's premarket flow: anchor on the last completed NIFTY session, enter
    qualifying intimations at the next open, store the meeting date for the hard exit.
    """
    from autotrader.time_utils import now_ist
    cfg = settings.strategy
    asof = entry_date or now_ist().strftime("%Y-%m-%d")
    channel_capital = cfg.channel_capital("pead")
    if channel_capital <= 0:
        return {"skipped": "pead_capital_zero", "asof": asof}
    if getattr(cfg, "corp_max_positions", 0) <= 0:
        return {"skipped": "corp_disabled", "asof": asof}

    nifty_daily = pead_trading_service._fetch_nifty_daily(upstox, asof)
    if len(nifty_daily) < 2:
        return {"skipped": "no_session_reference", "asof": asof}
    last_session = nifty_daily[-1][0]          # last completed session; entry = next open

    events = corp_action_signal_service.fetch_corp_events(last_session)
    if not events:
        return {"asof": asof, "last_session": last_session, "events": 0, "candidates": 0, "entered": 0}

    ev_syms = sorted({e["symbol"] for e in events})
    key_map = (instrument_keys.resolve_instrument_keys(ev_syms, bq, "corp_action")
               if bq else {})
    # STOCK-ONLY (2026-08-28): drop fund/ETF units by ISIN before any bar fetch. Name matching
    # alone missed NSE_EQ|INF740KA1SW3 in delivery and it was stopped only by an Upstox HTTP 400 --
    # luck, not design. NSE company equity is INE..., mutual-fund units (an ETF is one) are INF...
    # Fail-safe: a missing key returns False, so nothing is dropped on absent data.
    _fund_syms = [s for s, k in key_map.items() if etf_filter.is_fund_instrument_key(k)]
    if _fund_syms:
        logger.warning("corp_action_stock_only_dropped_funds n=%d syms=%s -- fund ISIN (INF...), "
                       "excluded before fetch", len(_fund_syms),
                       ",".join(sorted(_fund_syms)[:20]))
        _drop = set(_fund_syms)
        key_map = {s: k for s, k in key_map.items() if s not in _drop}
    candles: dict[str, list[list]] = {}
    ik_for: dict[str, str] = {}
    for sym in ev_syms:
        ik = key_map.get(sym)
        if not ik:
            continue
        bars = pead_trading_service._fetch_symbol_daily(upstox, ik, last_session)
        if len(bars) >= 62:
            candles[sym] = bars
            ik_for[sym] = ik

    market_level = _fetch_eqweight_market(bq, last_session)
    prior_keys = _load_prior_keys(state)
    candidates = corp_action_signal_service.scan(last_session, candles, market_level, prior_keys, corp_events=events)
    for c in candidates:
        c["instrument_key"] = ik_for.get(c["symbol"], "")

    open_positions = state.list_open_positions()
    open_channel = [p for p in open_positions if str(p.get("channel", "")).strip().lower() == "pead"]
    open_channel_syms = [p.get("symbol") for p in open_channel]
    open_corp_count = sum(1 for p in open_channel if str(p.get("wl_type", "")).strip().lower() == "corp_action")
    realized_today = _channel_realized_today(state, asof)

    specs = plan_corp_entries(candidates, open_channel_syms, open_corp_count, realized_today, channel_capital, cfg)
    entered: list[str] = []
    for s in specs:
        try:
            res = order_service.place_entry_order(
                symbol=s["symbol"], exchange="NSE", segment="CASH", side="BUY", qty=s["qty"],
                entry_price=s["entry_price"], sl_price=s["sl_price"], target=s["target"], atr=s["atr"],
                product="CNC", score=int(round(s["dist_low"] * 100)), reason="CORP_ACTION",
                instrument_key=s["instrument_key"], strategy="CORP_ACTION",
                wl_type="corp_action", channel="pead",
            )
            if res and not res.get("error") and not res.get("skipped"):
                tag = str(res.get("position_tag") or res.get("tag") or "")
                if tag and s.get("meeting_date"):
                    try:
                        state.update_position(tag, {"meeting_date": s["meeting_date"], "event_type": s["event_type"]})
                    except Exception:
                        logger.warning("corp_meeting_date_stamp_failed sym=%s tag=%s", s["symbol"], tag)
                entered.append(s["symbol"])
                prior_keys.add((s["symbol"], str(s.get("event_type") or "")))
        except Exception:
            logger.exception("corp_place_entry_failed sym=%s", s["symbol"])

    if entered:
        _save_prior_keys(state, prior_keys)
    _persist_corp_watchlist(state, last_session, channel_capital, realized_today, candidates, set(entered), cfg)

    summary = {"asof": asof, "last_session": last_session, "events": len(events),
               "candidates": len(candidates), "planned": len(specs), "entered": len(entered),
               "channel_open_before": len(open_channel), "corp_open_before": open_corp_count}
    logger.info("corp_scan_summary %s", summary)
    return summary


def _persist_corp_watchlist(state, asof, channel_capital, realized_today, candidates, entered, cfg):
    """Persist the day's corp candidates (annotated) for the dashboard EVENT panel.
    Separate `corp_watchlist` collection; best-effort."""
    try:
        loss_limit = -abs(cfg.daily_loss_pct) * channel_capital
        profit_limit = abs(cfg.daily_profit_pct) * channel_capital
        breaker = pead_book.daily_breaker_tripped(realized_today, loss_limit, profit_limit)
        rows = [{
            "symbol": c["symbol"], "event_type": c.get("event_type", ""),
            "dist_low": round(float(c.get("dist_low", 0.0)), 4),
            "runup_adj": round(float(c.get("runup_adj", 0.0)), 4),
            "lead": int(c.get("lead", 0)), "meeting_date": c.get("meeting_date", ""),
            "ref_close": round(float(c.get("ref_close", 0.0)), 2),
            "status": "ENTERED" if c["symbol"] in entered else ("BREAKER_HALT" if breaker else "NOT_SELECTED"),
        } for c in candidates]
        payload = {"asof": asof, "channel": "pead", "sub_strategy": "CORP_ACTION",
                   "breaker_tripped": breaker, "candidates": len(rows), "entered": len(entered), "rows": rows}
        state.set_json("corp_watchlist", asof, payload, merge=False)
        state.set_json("corp_watchlist", "latest", payload, merge=False)
    except Exception:
        logger.warning("corp_watchlist_persist_failed asof=%s — non-critical", asof, exc_info=True)
