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

from autotrader.adapters import instrument_keys
from autotrader.domain import etf_filter
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
                              planned_syms, entered_syms, held, macro_ok, macro_ctx, cfg,
                              revoke_syms: int = 0):
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
        # ``asof`` here IS the reaction date (the caller passes reaction_target). Both keys are
        # written because the success path used to omit reaction_date/revoke_symbols entirely,
        # so days that DID find revokes still rendered as "0 revokes" on the dashboard.
        payload = {"asof": asof, "reaction_date": asof, "channel": "pledge",
                   "macro_gate_ok": macro_ok, "macro": macro_ctx, "breaker_tripped": breaker,
                   "revoke_symbols": int(revoke_syms), "candidates": len(rows),
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
                         reaction_date: str | None = None,
                         asof: str | None = None) -> dict[str, Any]:
    """Live PLEDGE daily scan + entry for ONE reaction date (PAPER). Fail-closed at every step.
    Reuses the existing Upstox client, Firestore state, and order_service — no swing/intraday/
    other-channel path.

    ``asof`` is the *scan* day and ``reaction_date`` the disclosure day being settled. They differ
    when ``run_pledge_scan_catchup`` replays a date the previous scan missed; keeping them separate
    matters because the daily loss/profit breaker must read TODAY's realised P&L, not the
    reaction day's. Default preserves the original single-arg behaviour."""
    from autotrader.time_utils import now_ist
    cfg = settings.strategy
    asof = asof or reaction_date or now_ist().strftime("%Y-%m-%d")
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
    key_map = instrument_keys.resolve_instrument_keys(
        sorted(revokes.keys()), bq, "pledge")
    # STOCK-ONLY (2026-08-28): drop fund/ETF units by ISIN before any bar fetch. Name matching
    # alone missed NSE_EQ|INF740KA1SW3 in delivery and it was stopped only by an Upstox HTTP 400 --
    # luck, not design. NSE company equity is INE..., mutual-fund units (an ETF is one) are INF...
    # Fail-safe: a missing key returns False, so nothing is dropped on absent data.
    _fund_syms = [s for s, k in key_map.items() if etf_filter.is_fund_instrument_key(k)]
    if _fund_syms:
        logger.warning("pledge_stock_only_dropped_funds n=%d syms=%s -- fund ISIN (INF...), "
                       "excluded before fetch", len(_fund_syms),
                       ",".join(sorted(_fund_syms)[:20]))
        _drop = set(_fund_syms)
        key_map = {s: k for s, k in key_map.items() if s not in _drop}
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
    # Name what was rejected (2026-08-20) — mirrors insider. MASTERTR (08-12) was dropped on the
    # Rs25cr turnover floor and nothing recorded it; only the aggregate count survived.
    if len(candidates) < len(revokes):
        _kept = {str(c.get("symbol", "")).upper() for c in candidates}
        for _sym in sorted(revokes):
            if str(_sym).upper() in _kept:
                continue
            _bars = candles.get(_sym) or []
            # measured-vs-threshold (2026-08-21). Pledge additionally needs px > 200DMA, so
            # log the SMA too — otherwise a name above the turnover floor but below its MA
            # is indistinguishable from a thin one.
            _tov = _px = _sma = -1.0
            if _bars:
                try:
                    _dts = [b[0] for b in _bars]
                    _i = _dts.index(reaction_target) if reaction_target in _dts else len(_bars) - 1
                    _cl = [float(b[4]) for b in _bars]
                    _tov = pledge_signals.turnover_20d_cr(_cl, [float(b[5]) for b in _bars], _i)
                    _px = _cl[_i]
                    _sma = pledge_signals.sma(_cl, pledge_signals.MA_DAYS, _i) or -1.0
                except Exception:            # diagnostic only — never break the scan
                    pass
            logger.info("pledge_revoke_dropped sym=%s n_revokes=%s has_key=%s bars=%d/min%d "
                        "turnover_cr=%.2f/min%.1f close=%.2f/min%.1f sma%d=%.2f above_ma=%s "
                        "newest_bar=%s target=%s",
                        _sym, (revokes.get(_sym) or {}).get("n_revokes", "?"),
                        bool(ik_for.get(_sym)), len(_bars), pledge_signals.MIN_BARS,
                        _tov, cfg.pledge_turnover_min_cr, _px, pledge_signals.PRICE_MIN,
                        pledge_signals.MA_DAYS, _sma, (_px > _sma if _sma > 0 else "?"),
                        (str(_bars[-1][0]) if _bars else "-"), reaction_target)
    if revokes and not candidates:
        logger.warning("pledge_candidates_zero revokes=%d candles=%d target=%s — every revoke was "
                       "dropped downstream; see pledge_revoke_dropped lines for the reason",
                       len(revokes), len(candles), reaction_target)
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
                              planned_syms, entered_syms, set(open_syms), macro_ok, macro_ctx, cfg,
                              revoke_syms=len(revokes))

    summary = {"asof": asof, "reaction_date": reaction_target, "macro_gate_ok": macro_ok,
               "macro": macro_ctx, "revoke_symbols": len(revokes), "candidates": len(candidates),
               "planned": len(specs), "entered": len(entered_syms), "open_before": len(open_syms)}
    logger.info("pledge_scan_summary %s", summary)
    return summary


# ---------------------------------------------------------------------------------------------
# Reaction-date catch-up (2026-08-14). The scheduled scan used to read only MAX(date) and never
# revisit it, so weekend-dated filings and rows that landed after their own date's scan were lost
# permanently — 2 of 6 qualifying promoter revokes in 2026-07-20..08-13 (Sat 07-25 RAMCOIND, and
# 07-31 EMBDL whose row reached BQ after the 08-03 morning scan). See domain/reaction_dates.py.
# ---------------------------------------------------------------------------------------------
_SCAN_STATE_COLL = "pledge_scan_state"
_PROCESSED_KEY = "processed"
_KEEP_DATES = 40


def _completed_reaction_dates(state) -> list[str] | None:
    """Reaction dates this channel has already settled. ``[]`` when the ledger is simply absent
    (first run), but ``None`` on a READ FAILURE — and the caller must then do nothing. Treating an
    unreadable ledger as empty is the one path that could re-enter every date in the window."""
    try:
        doc = state.get_json(_SCAN_STATE_COLL, _PROCESSED_KEY)
        if doc is None:
            return []
        return [str(d)[:10] for d in (doc.get("dates") or [])]
    except Exception:
        logger.error("pledge_completed_dates_read_failed — skipping catch-up (fail-closed)",
                     exc_info=True)
        return None


def _mark_reaction_date_done(state, day: str) -> None:
    """Append ``day`` to the processed ledger, newest ``_KEEP_DATES`` retained."""
    try:
        prior = _completed_reaction_dates(state)
        done = sorted(set(prior or []) | {str(day)[:10]})[-_KEEP_DATES:]
        state.set_json(_SCAN_STATE_COLL, _PROCESSED_KEY, {"dates": done}, merge=False)
    except Exception:
        logger.warning("pledge_mark_date_done_failed day=%s", day, exc_info=True)


def run_pledge_scan_catchup(*, settings, upstox, state, order_service, bq=None,
                            lookback_days: int = 10) -> dict[str, Any]:
    """Scheduled PLEDGE entry point: settle EVERY reaction date owed today, not just ``MAX(date)``.

    A date is owed on the first trading day after it — precisely where the backtest enters it — so
    a Saturday filing is settled on Monday and parity holds. Dates whose entry window has already
    passed are logged as ``pledge_missed_stale`` and deliberately NOT entered: filling them today
    would use a price the backtest never validated. Fail-closed at every step; each date is settled
    by the unchanged single-date scan, and only marked done once it completes."""
    import datetime as _dt

    from autotrader.domain import reaction_dates
    from autotrader.time_utils import NSE_TRADING_HOLIDAYS, now_ist

    asof = now_ist().strftime("%Y-%m-%d")
    if bq is None:
        return {"skipped": "no_bq", "asof": asof}
    if settings.strategy.capital_pledge <= 0:
        return {"skipped": "pledge_capital_zero", "asof": asof}

    completed = _completed_reaction_dates(state)
    if completed is None:
        return {"skipped": "completed_ledger_unreadable", "asof": asof}

    available = pledge_signal_service.available_reaction_dates(bq, lookback_days)
    if not available:
        return {"skipped": "no_insider_data", "asof": asof, "processed": [], "stale": []}

    start = (_dt.date.fromisoformat(asof) - _dt.timedelta(days=lookback_days + 10)).isoformat()
    trading_days = reaction_dates.trading_days_between(start, asof, NSE_TRADING_HOLIDAYS)
    to_process, stale = reaction_dates.classify_pending_dates(available, completed, asof, trading_days)
    if stale:
        logger.warning("pledge_missed_stale asof=%s dates=%s — entry window passed; NOT entering "
                       "late (backtest parity)", asof, stale)

    runs: list[dict[str, Any]] = []
    for day in to_process:
        try:
            res = run_pledge_scan_once(settings=settings, upstox=upstox, state=state,
                                       order_service=order_service, bq=bq,
                                       reaction_date=day, asof=asof) or {}
            runs.append(res)
            if not res.get("skipped"):
                _mark_reaction_date_done(state, day)
        except Exception:
            logger.exception("pledge_catchup_date_failed day=%s — left unmarked for retry", day)

    def _sum(key: str) -> int:
        return sum(int(r.get(key, 0) or 0) for r in runs)

    summary = {"asof": asof, "processed": to_process, "stale": stale,
               "revoke_symbols": _sum("revoke_symbols"), "candidates": _sum("candidates"),
               "planned": _sum("planned"), "entered": _sum("entered"), "runs": runs}
    logger.info("pledge_catchup_summary %s", {k: v for k, v in summary.items() if k != "runs"})
    return summary
