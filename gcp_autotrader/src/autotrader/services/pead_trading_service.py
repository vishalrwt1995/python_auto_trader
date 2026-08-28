"""PEAD / EVENT channel trading service — daily premarket scan → 5-slot book → PAPER entries.

Hybrid architecture (2026-06-19, decided after a full gate audit): a NEW thin service
that owns PEAD signal + book + entry, but REUSES the proven order_service / positions /
exit plumbing via ``channel="pead"`` + ``wl_type="pead"``. It does NOT touch the
swing/intraday hot path (universe_service / trading_service.run_scan_once). The distinct
``wl_type="pead"`` keeps PEAD positions invisible to swing_reconciliation
(matches ``wl_type=="swing"`` exactly) and to the intraday tick exits.

Daily flow (premarket, ~09:10 IST, after the reaction-day close is known):
  1. NIFTY-50 252-day drawdown  ->  market-state gate.
  2. NSE event-calendar  ->  just-reported symbols.
  3. fresh daily history per reported name (Upstox)  ->  surprise + anti-pump + ATR.
  4. ``pead_signal_service.scan``  ->  qualifying Config-B candidates (surprise-ranked).
  5. ``plan_pead_entries`` (pure): daily breaker + 5-slot cap + sizing  ->  order specs.
  6. ``order_service.place_entry_order(channel="pead", wl_type="pead", product="CNC")``.

Exit (the daily 1R-trail + 40-day max-hold + 2.5×ATR stop) is owned by
``pead_reconciliation_service`` — mirrors swing's split (reconciliation owns the trail,
the resting paper-GTT/stop owns the SL). PAPER until explicitly flipped (Rule 5).
"""
from __future__ import annotations

import logging
from typing import Any, Sequence

from autotrader.adapters import instrument_keys
from autotrader.domain import etf_filter
from autotrader.domain import pead_book, pead_signals
from autotrader.services import pead_signal_service

logger = logging.getLogger(__name__)

NIFTY_INSTRUMENT_KEY = "NSE_INDEX|Nifty 50"
NIFTY_HISTORY_DAYS = 400          # > 252 trailing-peak window with a comfortable margin
SYMBOL_HISTORY_DAYS = 130         # > 80-bar min (surprise/runup/ATR) with margin
PEAD_EXCHANGE = "NSE"
PEAD_SEGMENT = "CASH"
TARGET_BACKSTOP_R = 10.0          # far placeholder target; real exit is the daily trail


def plan_pead_entries(
    candidates: Sequence[dict[str, Any]],
    open_pead_symbols: Sequence[str],
    realized_today: float,
    channel_capital: float,
    cfg: Any,
) -> list[dict[str, Any]]:
    """PURE: given today's qualifying PEAD candidates and the current book state,
    return the order specs to place (PAPER).

    Applies, in order: the per-channel daily loss/profit breaker (3%/6% of
    channel capital), exclusion of names already held, the 5-slot surprise-ranked
    cap, and risk/notional sizing. Returns ``[]`` when the breaker is tripped or
    nothing fits. Side-effect-free and fully unit-testable; the I/O wrapper
    (``run_pead_scan_once``) feeds it live data and places the resulting orders.

    Entry price uses the reaction-day close as the premarket proxy for the next
    open (the live order fills market-on-open; this is the same premarket estimate
    the swing path makes). ``target`` is a far backstop — the real exit is the daily
    1R-trail managed by ``pead_reconciliation_service``.
    """
    if channel_capital <= 0:
        logger.warning("pead_plan_no_capital — channel_capital=%s, skipping", channel_capital)
        return []
    loss_limit = -abs(cfg.daily_loss_pct) * channel_capital
    profit_limit = abs(cfg.daily_profit_pct) * channel_capital
    if pead_book.daily_breaker_tripped(realized_today, loss_limit, profit_limit):
        logger.info("pead_daily_breaker_tripped realized=%.0f loss_lim=%.0f profit_lim=%.0f — no entries",
                    realized_today or 0.0, loss_limit, profit_limit)
        return []

    held = {str(s).strip().upper() for s in open_pead_symbols}
    fresh = [c for c in candidates if str(c.get("symbol", "")).strip().upper() not in held]
    risk = cfg.pead_risk_per_trade if cfg.pead_risk_per_trade > 0 else 0.015 * channel_capital
    notional_cap = cfg.pead_notional_cap_pct * channel_capital
    picked = pead_book.select_for_slots(fresh, len(held), cfg.pead_max_positions)

    specs: list[dict[str, Any]] = []
    for c in picked:
        entry = float(c["reaction_close"])
        sl_dist = pead_book.sl_distance(float(c["atr"]), entry, cfg.pead_atr_sl_mult)
        qty = pead_book.position_size(entry, sl_dist, risk, notional_cap)
        if qty < 1:
            logger.info("pead_skip_qty_zero sym=%s entry=%.2f sl_dist=%.2f risk=%.0f cap=%.0f",
                        c.get("symbol"), entry, sl_dist, risk, notional_cap)
            continue
        specs.append({
            "symbol": str(c["symbol"]).strip().upper(),
            "qty": qty,
            "entry_price": round(entry, 2),
            "sl_price": round(entry - sl_dist, 2),
            "target": round(entry + sl_dist * TARGET_BACKSTOP_R, 2),
            "atr": round(float(c["atr"]), 4),
            "surprise": float(c["surprise"]),
            "instrument_key": str(c.get("instrument_key", "")),
            "strategy": "PEAD",
            "reaction_date": c.get("reaction_date", ""),
        })
    logger.info("pead_plan candidates=%d held=%d planned=%d", len(candidates), len(held), len(specs))
    return specs


# ── live I/O wrapper (validated in PAPER, not unit-tested — fail-closed) ───────
def _fetch_nifty_daily(upstox, asof: str) -> list[tuple[str, float]]:
    """NIFTY-50 daily ``[(date, close)]`` up to ``asof`` (Upstox; one call)."""
    from datetime import date, timedelta
    try:
        frm = (date.fromisoformat(asof) - timedelta(days=int(NIFTY_HISTORY_DAYS * 1.6))).isoformat()
        rows = upstox.get_historical_candles_v3_days(NIFTY_INSTRUMENT_KEY, to_date=asof, from_date=frm, interval_days=1)
        out = [(str(r[0])[:10], float(r[4])) for r in rows if isinstance(r, list) and len(r) >= 6]
        out.sort(key=lambda x: x[0])          # Upstox returns newest-first; ascend for nifty_drawdown
        return out
    except Exception as exc:
        logger.error("pead_fetch_nifty_failed asof=%s err=%s", asof, exc)
        return []


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
        logger.error("pead_fetch_symbol_failed key=%s err=%s", instrument_key, exc)
        return []


def _select_reaction_symbols(events: Sequence[tuple[str, str]],
                             candles: dict[str, list[list]], reaction_target: str) -> list[str]:
    """PURE: keep symbols whose reaction day (first daily session strictly AFTER their
    filing date) == ``reaction_target``. Mirrors the backtest's per-event reaction
    (``bisect_right(bars, announce)``) so we price only the true reaction-day move and
    skip names that reacted on a different session. De-duplicated."""
    import bisect
    out: list[str] = []
    seen: set[str] = set()
    for sym, filing in events:
        if sym in seen:
            continue
        bars = candles.get(sym)
        if not bars:
            continue
        dates = [b[0] for b in bars]
        ri = bisect.bisect_right(dates, filing)   # first bar strictly after filing
        if ri < len(dates) and dates[ri] == reaction_target:
            out.append(sym); seen.add(sym)
    return out


def _classify_reaction_drops(events: Sequence[tuple[str, str]],
                             candles: dict[str, list[list]], reaction_target: str):
    """PURE DIAGNOSTIC (2026-08-27) — explains WHY each event missed eligibility.

    Behaviour-neutral: nothing here feeds a decision. It exists because PEAD reads a single
    reaction session with no catch-up and, unlike pledge/insider after PR #70, logged NOTHING when
    a session was dropped — so silent losses were unmeasurable.

    Mirrors ``_select_reaction_symbols`` exactly, including its quirk that ``seen`` is populated
    only on a MATCH (so a symbol with several filings is retried). Therefore ``on_target`` here
    equals ``eligible`` there, and the buckets are a true partition of the same loop.

    Returns ``(on_target, past, future, nobars)`` where ``past`` is the number that matters:
    events whose reaction session is ALREADY GONE. Those are unreachable without breaking
    backtest parity, and until now vanished without a trace.
    """
    import bisect
    on_target: list[str] = []
    past: list[tuple[str, str]] = []
    future: list[tuple[str, str]] = []
    nobars: list[str] = []
    seen: set[str] = set()
    for sym, filing in events:
        if sym in seen:
            continue
        bars = candles.get(sym)
        if not bars:
            nobars.append(sym)
            continue
        dates = [b[0] for b in bars]
        ri = bisect.bisect_right(dates, filing)
        if ri >= len(dates):
            future.append((sym, "beyond-bars"))
        elif dates[ri] == reaction_target:
            on_target.append(sym); seen.add(sym)
        elif dates[ri] < reaction_target:
            past.append((sym, dates[ri]))
        else:
            future.append((sym, dates[ri]))
    return on_target, past, future, nobars


def run_pead_scan_once(*, settings, upstox, state, order_service, bq=None,
                       reaction_date: str | None = None) -> dict[str, Any]:
    """Live PEAD daily scan + entry (PAPER). Fail-closed at every step.

    Returns a summary dict (also used by the HTTP endpoint). Reuses the existing
    Upstox client, Firestore state, and order_service — no swing/intraday code path.
    """
    from autotrader.time_utils import now_ist
    cfg = settings.strategy
    asof = reaction_date or now_ist().strftime("%Y-%m-%d")
    channel_capital = cfg.channel_capital("pead")
    if channel_capital <= 0:
        return {"skipped": "pead_capital_zero", "asof": asof}

    nifty_daily = _fetch_nifty_daily(upstox, asof)
    if len(nifty_daily) < 2:
        return {"skipped": "no_nifty_market_state", "asof": asof}
    # Reaction day = the LAST COMPLETED market session (NIFTY's last bar). This works
    # at premarket (today's bar not yet formed -> last = the prior session) AND
    # post-close (last = today). We enter names that reacted on this session at the
    # NEXT open — exactly the backtest's reaction+1 entry. Using `asof` directly was
    # wrong: at premarket the asof-day bar doesn't exist, so nothing qualified.
    reaction_target = nifty_daily[-1][0]

    events = pead_signal_service.fetch_result_events(reaction_target)
    if not events:
        return {"asof": asof, "reaction_date": reaction_target, "result_events": 0,
                "candidates": 0, "entered": 0}

    # resolve instrument keys + fetch fresh dailies for the reported names
    ev_syms = sorted({s for s, _ in events})
    key_map = (instrument_keys.resolve_instrument_keys(ev_syms, bq, "pead")
               if bq else {})
    # STOCK-ONLY (2026-08-28): drop fund/ETF units by ISIN before any bar fetch. Name matching
    # alone missed NSE_EQ|INF740KA1SW3 and it was stopped only by an Upstox HTTP 400 -- luck, not
    # design. NSE equity is INE..., fund units (an ETF is one) are INF... Fail-safe: an unknown
    # key is KEPT, so a resolver outage cannot empty the book.
    key_map, _fund_syms = etf_filter.split_fund_keys(key_map)
    if _fund_syms:
        logger.warning("pead_stock_only_dropped_funds n=%d syms=%s -- fund ISIN (INF...), "
                       "excluded before fetch", len(_fund_syms), ",".join(_fund_syms[:20]))
    candles: dict[str, list[list]] = {}
    ik_for: dict[str, str] = {}
    _n_nokey = _n_short = 0
    _newest_bar = ""
    _nokey_syms: list[str] = []
    for sym in ev_syms:
        ik = key_map.get(sym)
        if not ik:
            _n_nokey += 1
            _nokey_syms.append(str(sym))
            continue
        bars = _fetch_symbol_daily(upstox, ik, reaction_target)
        if len(bars) >= pead_signal_service.ATR_WINDOW + 2:
            candles[sym] = bars
            ik_for[sym] = ik
            if bars and str(bars[-1][0]) > _newest_bar:
                _newest_bar = str(bars[-1][0])
        else:
            _n_short += 1
    # Funnel visibility (2026-08-20): `events=N -> eligible=0` was previously indistinguishable
    # from a quiet market, because nothing recorded how many symbols actually had usable bars
    # or how fresh those bars were. `newest_bar` vs `reaction_target` is the decisive pair.
    logger.info("pead_fetch_funnel events=%d syms=%d no_key=%d short_bars=%d candles=%d "
                "newest_bar=%s reaction_target=%s", len(events), len(ev_syms), _n_nokey,
                _n_short, len(candles), _newest_bar or "-", reaction_target)
    if _nokey_syms:
        # NAME them — a count alone is not actionable. These are symbols the event feed
        # reported but neither key source could resolve, so they can never be traded.
        logger.warning("pead_no_key_symbols n=%d syms=%s", len(_nokey_syms),
                       ",".join(sorted(_nokey_syms)[:40]))

    # keep only names whose reaction day (first session after their filing) == target —
    # the faithful per-event reaction filter (vs naively pricing every recent reporter)
    eligible = _select_reaction_symbols(events, candles, reaction_target)
    # Behaviour-neutral split of the SAME loop — quantifies the silent reaction-date loss that
    # pead has always had (single session, no catch-up, no stale log). `past` is the number that
    # matters: sessions already gone, unreachable without breaking parity.
    _ont, _past, _fut, _nb = _classify_reaction_drops(events, candles, reaction_target)
    logger.info("pead_reaction_split target=%s on_target=%d past=%d future=%d nobars=%d "
                "events=%d", reaction_target, len(_ont), len(_past), len(_fut), len(_nb),
                len(events))
    if _past:
        # A symbol may appear in BOTH buckets: an older filing whose session has gone, plus a
        # newer one reacting today. The selector trades it on the newer filing, so that is NOT a
        # loss. `unrecovered` is the honest count — past events whose symbol was never traded.
        _ont_set = set(_ont)
        _unrec = [(sy, rd) for sy, rd in _past if sy not in _ont_set]
        logger.warning("pead_reaction_past n=%d unrecovered=%d — reacted BEFORE the target "
                       "session, unreachable without breaking backtest parity. `unrecovered` is "
                       "the true silent loss (the rest were traded on a newer filing). Until now "
                       "this was dropped with no log at all: the pead half of the PR#70 gap "
                       "(PROJECT_KNOWLEDGE section 7). unrecovered_syms=%s",
                       len(_past), len(_unrec),
                       ",".join(f"{sy}@{rd}" for sy, rd in sorted(_unrec)[:25]) or "none")
    if candles and not eligible:
        # Eligibility is an EXACT date match on `reaction_target` (set from nifty_daily[-1]).
        # State the facts and let the reader judge: if newest_bar < reaction_target it IS a
        # data-freshness skew; if they are equal the shortfall is upstream (few filings landed
        # on the qualifying session, and/or no_key/short_bars thinned the candle set).
        # 2026-08-20/21 were the equal case — an earlier version of this message asserted the
        # skew as the cause and was wrong.
        logger.warning("pead_eligible_zero events=%d candles=%d newest_bar=%s reaction_target=%s "
                       "stale_bars=%s — no symbol's first bar after its filing equals the target; "
                       "compare newest_bar vs reaction_target (see pead_fetch_funnel for no_key/"
                       "short_bars) before assuming a cause",
                       len(events), len(candles), _newest_bar or "-", reaction_target,
                       bool(_newest_bar and _newest_bar < reaction_target))
    candidates = pead_signal_service.scan(reaction_target, candles, nifty_daily,
                                          result_symbols=eligible,
                                          market_dd_gate=cfg.pead_market_dd_gate,
                                          min_runup=cfg.pead_min_runup)
    for c in candidates:
        c["instrument_key"] = ik_for.get(c["symbol"], "")

    open_positions = [p for p in state.list_open_positions()
                      if str(p.get("channel", "")).strip().lower() == "pead"]
    open_syms = [p.get("symbol") for p in open_positions]
    realized_today = _pead_realized_today(state, asof)

    specs = plan_pead_entries(candidates, open_syms, realized_today, channel_capital, cfg)
    planned_syms = {s["symbol"] for s in specs}
    entered_syms: set[str] = set()
    for s in specs:
        try:
            res = order_service.place_entry_order(
                symbol=s["symbol"], exchange=PEAD_EXCHANGE, segment=PEAD_SEGMENT,
                side="BUY", qty=s["qty"], entry_price=s["entry_price"], sl_price=s["sl_price"],
                target=s["target"], atr=s["atr"], product="CNC", score=int(round(s["surprise"] * 1000)),
                reason="PEAD", instrument_key=s["instrument_key"], strategy="PEAD",
                wl_type="pead", channel="pead",
            )
            if res and not res.get("error") and not res.get("skipped"):
                entered_syms.add(s["symbol"])
        except Exception:
            logger.exception("pead_place_entry_failed sym=%s", s["symbol"])

    # ── #1: persist the day's candidate watchlist (annotated) so the dashboard can
    # show PEAD the same way as swing/intraday. Separate `pead_watchlist` collection —
    # never touches the swing/intraday watchlist. Best-effort (won't fail the scan).
    nifty_dd = pead_signal_service.nifty_drawdown(nifty_daily, reaction_target)
    _persist_pead_watchlist(state, reaction_target, nifty_dd, cfg.pead_market_dd_gate, channel_capital,
                            realized_today, candidates, planned_syms, entered_syms,
                            set(open_syms), cfg)

    summary = {"asof": asof, "reaction_date": reaction_target, "result_events": len(events),
               "eligible": len(eligible), "candidates": len(candidates),
               "planned": len(specs), "entered": len(entered_syms), "open_before": len(open_syms),
               "nifty_dd": round(nifty_dd or 0.0, 4)}
    logger.info("pead_scan_summary %s", summary)
    return summary


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
    return "NOT_SELECTED"               # below the 5-slot surprise rank


def _persist_pead_watchlist(state, asof, nifty_dd, market_dd_gate, channel_capital,
                            realized_today, candidates, planned_syms, entered_syms, held, cfg):
    try:
        loss_limit = -abs(cfg.daily_loss_pct) * channel_capital
        profit_limit = abs(cfg.daily_profit_pct) * channel_capital
        breaker = pead_book.daily_breaker_tripped(realized_today, loss_limit, profit_limit)
        rows = [{
            "symbol": c["symbol"],
            "surprise": round(float(c["surprise"]), 4),
            "runup": round(float(c.get("runup") or 0.0), 4),
            "atr": round(float(c["atr"]), 4),
            "reaction_close": round(float(c["reaction_close"]), 2),
            "reaction_date": c.get("reaction_date", asof),
            "status": _candidate_status(c["symbol"], planned_syms, entered_syms, held, breaker),
        } for c in candidates]
        payload = {
            "asof": asof,
            "channel": "pead",
            "market_dd": round(nifty_dd, 4) if nifty_dd is not None else None,
            "market_dd_gate": market_dd_gate,
            "market_ok": (nifty_dd is not None and nifty_dd > market_dd_gate),
            "breaker_tripped": breaker,
            "candidates": len(rows),
            "entered": len(entered_syms),
            "held_before": len(held),
            "rows": rows,
        }
        state.set_json("pead_watchlist", asof, payload, merge=False)
        state.set_json("pead_watchlist", "latest", payload, merge=False)
    except Exception:
        logger.warning("pead_watchlist_persist_failed asof=%s — non-critical", asof, exc_info=True)


def _pead_realized_today(state, asof: str) -> float:
    """Sum realized P&L of PEAD positions closed today (for the daily breaker).
    Fail-closed: returns a tripped-high value's caller handles None → 0.0 here is safe
    because an unreadable ledger should not silently *enable* trading — but the breaker
    treats None as tripped, so we return 0.0 only when we can read an empty/none set."""
    try:
        total = 0.0
        for p in state.list_all_positions(limit=500):
            if str(p.get("channel", "")).strip().lower() != "pead":
                continue
            if str(p.get("status", "")).upper() != "CLOSED":
                continue
            if str(p.get("exit_ts", ""))[:10] != asof:
                continue
            total += float(p.get("pnl", 0.0) or 0.0)
        return total
    except Exception as exc:
        logger.error("pead_realized_today_failed err=%s — failing closed (breaker trips)", exc)
        return float("nan")        # NaN -> daily_breaker_tripped returns True (no entries)
