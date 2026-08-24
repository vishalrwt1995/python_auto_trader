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
    """Live breadth (% of universe above EMA200) from the brain Firestore doc. The raw doc nests
    it at ``context.breadthSnapshot.aboveEma200Pct`` (the source of MarketBrainState.breadth_ema200_pct);
    falls back to a flat/parsed key. None on error/missing -> fail-closed gate."""
    try:
        brain = state.get_market_brain() or {}
        snap = (brain.get("context") or {}).get("breadthSnapshot") or {}
        v = snap.get("aboveEma200Pct")
        if v is None:
            v = brain.get("breadth_ema200_pct")   # fallback (parsed/flat shape)
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
    """symbol -> Upstox instrument_key, from the FRESH source first, then the DEEP one.

    Why two sources (2026-08-24): `candles_daily` stopped being written on **2026-06-07**, so
    it silently fails to resolve any symbol that listed — or entered the universe — after that
    date. Measured on pead's event set the day this was found: **no_key 15% (08-20) -> 78%
    (08-21) -> 64% (08-24)** — which reads in the logs as "quiet market", not "broken lookup".

    `candles_5m` is written daily (partitioned by trade_date, clustered by symbol, so a
    date+symbol probe prunes hard) BUT its 30-day window is NARROWER, because it only carries
    actively-traded intraday names. Measured coverage:
        candles_daily 2,638 syms · candles_5m(30d) 2,440 · gained 224 · **LOST 422 if replaced**
    So a straight swap would be a net loss dressed up as a fix. The union — fresh wins on
    conflict, deep fills the gaps — gains 224 and loses nothing (~2,862 resolvable).

    Deliberately two queries merged in Python rather than one clever UNION: each is
    independently debuggable, and if the fresh probe fails we still degrade to the deep table
    instead of losing everything. Fail-closed overall: {} only if BOTH fail.

    Still a per-channel copy rather than a shared helper (2026-08-24): all four copies are now
    byte-identical apart from the log prefix, so extracting one is a mechanical follow-up —
    but doing it in this same change would have put four live entry paths on one blast radius.
    """
    syms = ",".join("'" + str(s).strip().upper().replace("'", "") + "'" for s in symbols)
    if not syms:
        return {}
    deep: dict[str, str] = {}
    fresh: dict[str, str] = {}
    try:
        q = (f"SELECT symbol, ANY_VALUE(instrument_key) ik "
             f"FROM `grow-profit-machine.autotrader.candles_daily` "
             f"WHERE UPPER(symbol) IN ({syms}) AND instrument_key IS NOT NULL GROUP BY symbol")
        deep = {str(r["symbol"]).strip().upper(): str(r["ik"]) for r in bq.query(q)}
    except Exception as exc:
        logger.error("insider_resolve_keys_deep_failed err=%s", exc)
    try:
        q = (f"SELECT symbol, ANY_VALUE(instrument_key) ik "
             f"FROM `grow-profit-machine.autotrader.candles_5m` "
             f"WHERE trade_date >= DATE_SUB(CURRENT_DATE('Asia/Kolkata'), INTERVAL 30 DAY) "
             f"AND UPPER(symbol) IN ({syms}) AND instrument_key IS NOT NULL GROUP BY symbol")
        fresh = {str(r["symbol"]).strip().upper(): str(r["ik"]) for r in bq.query(q)}
    except Exception as exc:
        logger.error("insider_resolve_keys_fresh_failed err=%s", exc)
    merged = {**deep, **fresh}                      # fresh wins on conflict
    logger.info("insider_resolve_keys asked=%d deep=%d fresh=%d merged=%d fresh_only=%d",
                len(symbols), len(deep), len(fresh), len(merged), len(set(fresh) - set(deep)))
    return merged


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
                               planned_syms, entered_syms, held, macro_ok, macro_ctx, cfg,
                               cluster_count: int = 0):
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
        # ``asof`` here IS the reaction date (the caller passes reaction_target). Both keys are
        # written because the success path used to omit reaction_date/clusters entirely, so days
        # that DID find clusters still rendered as "0 clusters" on the dashboard.
        payload = {"asof": asof, "reaction_date": asof, "channel": "insider",
                   "macro_gate_ok": macro_ok, "macro": macro_ctx, "breaker_tripped": breaker,
                   "clusters": int(cluster_count), "candidates": len(rows),
                   "entered": len(entered_syms), "held_before": len(held), "rows": rows}
        state.set_json("insider_watchlist", asof, payload, merge=False)
        state.set_json("insider_watchlist", "latest", payload, merge=False)
    except Exception:
        logger.warning("insider_watchlist_persist_failed asof=%s — non-critical", asof, exc_info=True)


def _persist_insider_status(state, asof, reaction_date, macro_ok, macro_ctx, clusters,
                            candidates=None):
    """Persist a compact insider gate/status doc to Firestore ``insider_watchlist/latest`` on
    EVERY scan — including gated-off / 0-cluster days — so the dashboard drill-down always shows
    the current macro-gate state (why it is/isn't trading). Best-effort, read-only downstream."""
    try:
        rows = [{
            "symbol": c["symbol"], "n_buyers": int(c.get("n_buyers", 0)),
            "total_val_cr": round(float(c.get("total_val") or 0.0) / 1e7, 3),
            "category": c.get("category", ""),
            "turnover_cr": round(float(c.get("turnover_cr") or 0.0), 2),
            "reaction_close": round(float(c.get("reaction_close") or 0.0), 2),
            "reaction_date": c.get("reaction_date", reaction_date),
            "status": "MACRO_GATE_OFF" if not macro_ok else "NOT_SELECTED",
        } for c in (candidates or [])]
        payload = {"asof": asof, "reaction_date": reaction_date, "channel": "insider",
                   "macro_gate_ok": bool(macro_ok), "macro": macro_ctx or {},
                   "clusters": int(clusters), "candidates": len(rows), "entered": 0, "rows": rows}
        state.set_json("insider_watchlist", reaction_date or asof or "latest", payload, merge=False)
        state.set_json("insider_watchlist", "latest", payload, merge=False)
    except Exception:
        logger.warning("insider_status_persist_failed asof=%s — non-critical", asof, exc_info=True)


def run_insider_scan_once(*, settings, upstox, state, order_service, bq=None,
                          reaction_date: str | None = None,
                          asof: str | None = None) -> dict[str, Any]:
    """Live INSIDER daily scan + entry for ONE reaction date (PAPER). Fail-closed at every step.
    Reuses the existing Upstox client, Firestore state, and order_service — no swing/intraday/
    other-channel path.

    ``asof`` is the *scan* day and ``reaction_date`` the disclosure day being settled. They differ
    when ``run_insider_scan_catchup`` replays a date the previous scan missed; keeping them separate
    matters because the daily loss/profit breaker must read TODAY's realised P&L, not the reaction
    day's. Default preserves the original single-arg behaviour."""
    from autotrader.time_utils import now_ist
    cfg = settings.strategy
    asof = asof or reaction_date or now_ist().strftime("%Y-%m-%d")
    channel_capital = cfg.channel_capital("insider")
    if channel_capital <= 0:
        return {"skipped": "insider_capital_zero", "asof": asof}
    if bq is None:
        return {"skipped": "no_bq", "asof": asof}

    # 1. DOUBLE MACRO GATE (evaluated once; bad regime => no entries)
    macro_ok, macro_ctx = _macro_gate(state, upstox, cfg, asof)

    reaction_target = reaction_date or insider_signal_service.latest_reaction_date(bq)
    if not reaction_target:
        summary = {"asof": asof, "reaction_date": "", "macro_gate_ok": macro_ok, "macro": macro_ctx,
                   "clusters": 0, "candidates": 0, "entered": 0, "skipped": "no_insider_data"}
        _persist_insider_status(state, asof, "", macro_ok, macro_ctx, 0)
        logger.info("insider_scan_summary %s", summary)
        return summary

    # Pass 1 (pre-price): informed open-market buy legs -> candidate symbols with >=2 legs.
    raw_rows = insider_signal_service.fetch_disclosure_rows(bq, reaction_target)
    legs = insider_signals.aggregate_legs(raw_rows, min_buyers=cfg.insider_min_buyers)
    if not legs:
        summary = {"asof": asof, "reaction_date": reaction_target, "macro_gate_ok": macro_ok,
                   "macro": macro_ctx, "clusters": 0, "candidates": 0, "entered": 0}
        _persist_insider_status(state, asof, reaction_target, macro_ok, macro_ctx, 0)
        logger.info("insider_scan_summary %s", summary)
        return summary

    # resolve instrument keys + fresh dailies for the candidate names
    key_map = _resolve_instrument_keys(sorted(legs.keys()), bq)
    candles: dict[str, list[list]] = {}
    ik_for: dict[str, str] = {}
    for sym in sorted(legs.keys()):
        ik = key_map.get(sym)
        if not ik:
            continue
        bars = _fetch_symbol_daily(upstox, ik, reaction_target)
        if len(bars) >= insider_signals.MIN_BARS:
            candles[sym] = bars
            ik_for[sym] = ik

    # Pass 2 (post-price): value each leg = shares × reaction-close, re-apply the >=Rs5L per-leg
    # gate + re-count >=2 (the filer sec_val is unreliable -> never trusted). See domain docstring.
    from bisect import bisect_left as _bl
    price_map: dict[str, float] = {}
    for sym, bars in candles.items():
        dts = [b[0] for b in bars]
        ri = _bl(dts, reaction_target)
        if ri < len(bars) and dts[ri] == reaction_target:
            price_map[sym] = float(bars[ri][4])
    clusters = insider_signals.finalize_clusters(
        legs, price_map, min_buyers=cfg.insider_min_buyers, min_leg_value=cfg.insider_min_leg_value)
    if not clusters:
        summary = {"asof": asof, "reaction_date": reaction_target, "macro_gate_ok": macro_ok,
                   "macro": macro_ctx, "clusters": 0, "candidates": 0, "entered": 0}
        _persist_insider_status(state, asof, reaction_target, macro_ok, macro_ctx, 0)
        logger.info("insider_scan_summary %s", summary)
        return summary

    candidates = insider_signal_service.scan(
        reaction_target, clusters, candles,
        turnover_min_cr=cfg.insider_turnover_min_cr, price_min=insider_signals.PRICE_MIN)
    # Name what was rejected (2026-08-20). Previously only the COUNTS were emitted, so a
    # `clusters=2 -> candidates=0` day left no record of WHICH symbols dropped or WHY —
    # the same blind spot that let the pledge reaction-date leak hide for three weeks.
    if len(candidates) < len(clusters):
        _kept = {str(c.get("symbol", "")).upper() for c in candidates}
        for _sym in sorted(clusters):
            if str(_sym).upper() in _kept:
                continue
            _bars = candles.get(_sym) or []
            # measured-vs-threshold, so the reader can see WHICH gate bit rather than
            # inferring it. (2026-08-21: the first version logged only the thresholds,
            # which named the symbol but still left the reason ambiguous.)
            _tov = _px = -1.0
            if _bars:
                try:
                    _dts = [b[0] for b in _bars]
                    _i = _dts.index(reaction_target) if reaction_target in _dts else len(_bars) - 1
                    _tov = insider_signals.turnover_20d_cr(
                        [float(b[4]) for b in _bars], [float(b[5]) for b in _bars], _i)
                    _px = float(_bars[_i][4])
                except Exception:            # diagnostic only — never break the scan
                    pass
            logger.info("insider_cluster_dropped sym=%s n_buyers=%s has_key=%s bars=%d/min%d "
                        "turnover_cr=%.2f/min%.1f close=%.2f/min%.1f newest_bar=%s target=%s",
                        _sym, (clusters.get(_sym) or {}).get("n_buyers", "?"),
                        bool(ik_for.get(_sym)), len(_bars), insider_signals.MIN_BARS,
                        _tov, cfg.insider_turnover_min_cr, _px, insider_signals.PRICE_MIN,
                        (str(_bars[-1][0]) if _bars else "-"), reaction_target)
    if clusters and not candidates:
        logger.warning("insider_candidates_zero clusters=%d candles=%d target=%s — every cluster "
                       "was dropped downstream; see insider_cluster_dropped lines for the reason",
                       len(clusters), len(candles), reaction_target)
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
                               planned_syms, entered_syms, set(open_syms), macro_ok, macro_ctx, cfg,
                               cluster_count=len(clusters))

    summary = {"asof": asof, "reaction_date": reaction_target, "macro_gate_ok": macro_ok,
               "macro": macro_ctx, "clusters": len(clusters), "candidates": len(candidates),
               "planned": len(specs), "entered": len(entered_syms), "open_before": len(open_syms)}
    logger.info("insider_scan_summary %s", summary)
    return summary


# ---------------------------------------------------------------------------------------------
# Reaction-date catch-up (2026-08-14) — mirrors pledge_trading_service. The scheduled scan used to
# read only MAX(date) and never revisit it, so weekend-dated filings and rows that landed after
# their own date's scan were lost permanently. See domain/reaction_dates.py for the parity rule.
# ---------------------------------------------------------------------------------------------
_SCAN_STATE_COLL = "insider_scan_state"
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
        logger.error("insider_completed_dates_read_failed — skipping catch-up (fail-closed)",
                     exc_info=True)
        return None


def _mark_reaction_date_done(state, day: str) -> None:
    """Append ``day`` to the processed ledger, newest ``_KEEP_DATES`` retained."""
    try:
        prior = _completed_reaction_dates(state)
        done = sorted(set(prior or []) | {str(day)[:10]})[-_KEEP_DATES:]
        state.set_json(_SCAN_STATE_COLL, _PROCESSED_KEY, {"dates": done}, merge=False)
    except Exception:
        logger.warning("insider_mark_date_done_failed day=%s", day, exc_info=True)


def run_insider_scan_catchup(*, settings, upstox, state, order_service, bq=None,
                             lookback_days: int = 10) -> dict[str, Any]:
    """Scheduled INSIDER entry point: settle EVERY reaction date owed today, not just ``MAX(date)``.

    A date is owed on the first trading day after it — precisely where the backtest enters it — so a
    Saturday filing is settled on Monday and parity holds. Dates whose entry window has already
    passed are logged as ``insider_missed_stale`` and deliberately NOT entered: filling them today
    would use a price the backtest never validated. Fail-closed at every step; each date is settled
    by the unchanged single-date scan, and only marked done once it completes."""
    import datetime as _dt

    from autotrader.domain import reaction_dates
    from autotrader.time_utils import NSE_TRADING_HOLIDAYS, now_ist

    asof = now_ist().strftime("%Y-%m-%d")
    if bq is None:
        return {"skipped": "no_bq", "asof": asof}
    if settings.strategy.channel_capital("insider") <= 0:
        return {"skipped": "insider_capital_zero", "asof": asof}

    completed = _completed_reaction_dates(state)
    if completed is None:
        return {"skipped": "completed_ledger_unreadable", "asof": asof}

    available = insider_signal_service.available_reaction_dates(bq, lookback_days)
    if not available:
        return {"skipped": "no_insider_data", "asof": asof, "processed": [], "stale": []}

    start = (_dt.date.fromisoformat(asof) - _dt.timedelta(days=lookback_days + 10)).isoformat()
    trading_days = reaction_dates.trading_days_between(start, asof, NSE_TRADING_HOLIDAYS)
    to_process, stale = reaction_dates.classify_pending_dates(available, completed, asof, trading_days)
    if stale:
        logger.warning("insider_missed_stale asof=%s dates=%s — entry window passed; NOT entering "
                       "late (backtest parity)", asof, stale)

    runs: list[dict[str, Any]] = []
    for day in to_process:
        try:
            res = run_insider_scan_once(settings=settings, upstox=upstox, state=state,
                                        order_service=order_service, bq=bq,
                                        reaction_date=day, asof=asof) or {}
            runs.append(res)
            if not res.get("skipped"):
                _mark_reaction_date_done(state, day)
        except Exception:
            logger.exception("insider_catchup_date_failed day=%s — left unmarked for retry", day)

    def _sum(key: str) -> int:
        return sum(int(r.get(key, 0) or 0) for r in runs)

    summary = {"asof": asof, "processed": to_process, "stale": stale,
               "clusters": _sum("clusters"), "candidates": _sum("candidates"),
               "planned": _sum("planned"), "entered": _sum("entered"), "runs": runs}
    logger.info("insider_catchup_summary %s", {k: v for k, v in summary.items() if k != "runs"})
    return summary
