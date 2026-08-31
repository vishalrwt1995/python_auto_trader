"""Dashboard API router.

Provides all frontend-facing endpoints under /dashboard.
Authentication is performed via Firebase ID tokens (Bearer scheme).
"""
from __future__ import annotations

import logging
import os
import re
import threading
import time
from datetime import date, timedelta
from typing import Any

import google.auth.transport.requests
import google.oauth2.id_token
import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Query

from autotrader.container import get_container
from autotrader.time_utils import now_ist

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Firebase token cache — avoid blocking network call on every request
# ---------------------------------------------------------------------------

_TOKEN_CACHE: dict[str, tuple[dict[str, Any], float]] = {}  # token → (claims, exp_ts)
_TOKEN_CACHE_LOCK = threading.Lock()
_TOKEN_CACHE_TTL = 3600  # seconds — re-verify after 1 hour


def _cached_verify_firebase_token(id_token: str) -> dict[str, Any]:
    """Verify a Firebase token, using an in-process cache to avoid a network
    call on every request.  Cache entries expire after 1 hour."""
    now = time.time()
    with _TOKEN_CACHE_LOCK:
        entry = _TOKEN_CACHE.get(id_token)
        if entry and now < entry[1]:
            return entry[0]
    # Not cached or expired — do the actual verification
    claims: dict[str, Any] = google.oauth2.id_token.verify_firebase_token(
        id_token,
        google.auth.transport.requests.Request(),
    )
    exp_ts = now + _TOKEN_CACHE_TTL
    with _TOKEN_CACHE_LOCK:
        # Evict stale entries to avoid unbounded growth
        if len(_TOKEN_CACHE) > 500:
            stale = [k for k, v in _TOKEN_CACHE.items() if now >= v[1]]
            for k in stale:
                del _TOKEN_CACHE[k]
        _TOKEN_CACHE[id_token] = (claims, exp_ts)
    return claims


# ---------------------------------------------------------------------------
# Input sanitisation helpers
# ---------------------------------------------------------------------------

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SAFE_STR_RE = re.compile(r"^[A-Za-z0-9_\-]+$")


def _safe_date(value: str | None, default: str) -> str:
    """Return value if it's a valid YYYY-MM-DD date string, else default."""
    if not value:
        return default
    if not _DATE_RE.match(value):
        raise HTTPException(status_code=400, detail=f"Invalid date format: {value!r}")
    try:
        date.fromisoformat(value)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid date value: {value!r}")
    return value


def _safe_str(value: str | None) -> str | None:
    """Return value only if it contains safe characters, else raise 400."""
    if value is None:
        return None
    if not _SAFE_STR_RE.match(value):
        raise HTTPException(status_code=400, detail=f"Invalid parameter value: {value!r}")
    return value

router = APIRouter(prefix="/dashboard", tags=["dashboard"])

# ---------------------------------------------------------------------------
# Auth dependency
# ---------------------------------------------------------------------------

_firebase_request_adapter = google.auth.transport.requests.Request()


def verify_firebase_token(
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    """Validate a Firebase ID token from the Authorization header."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=401,
            detail="Missing or malformed Authorization header.",
        )

    id_token = authorization.removeprefix("Bearer ").strip()
    if not id_token:
        raise HTTPException(status_code=401, detail="Empty Bearer token")

    try:
        claims: dict[str, Any] = _cached_verify_firebase_token(id_token)
    except Exception as exc:
        logger.warning("Firebase token validation failed: %s", exc)
        raise HTTPException(status_code=401, detail="Invalid or expired Firebase token") from exc

    return {
        "uid": str(claims.get("uid") or claims.get("sub") or ""),
        "email": str(claims.get("email") or ""),
        "role": str(claims.get("role") or "viewer"),
    }


def _require_admin(user: dict[str, Any] = Depends(verify_firebase_token)) -> dict[str, Any]:
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin role required")
    return user


# ---------------------------------------------------------------------------
# Trades
# ---------------------------------------------------------------------------


@router.get("/trades/summary")
def get_trades_summary(
    from_date: str | None = Query(default=None, alias="from"),
    to_date: str | None = Query(default=None, alias="to"),
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Aggregated P&L, win rate, etc from BQ trades table."""
    c = get_container()
    today = now_ist().strftime("%Y-%m-%d")
    fd = from_date or today
    td = to_date or today

    # P&L is NET of costs (`IFNULL(net_pnl, pnl)`) — gross `pnl` understates every loss by
    # the round-trip cost and made this page disagree with /channels on the same trade
    # (JSWCEMENT: gross -2,334.15 vs net -2,491.25, brokerage 157.10). Fallback to `pnl`
    # covers pre-cost-model rows that have no net_pnl.
    # biggest_win/biggest_loss are SIGN-GUARDED: a bare MAX(pnl) over a loss-only period
    # returned the least-bad LOSS as "Best Win" (and the mirror bug for an all-wins period).
    q = f"""
        WITH t AS (
            SELECT exit_reason, IFNULL(net_pnl, pnl) AS p
            FROM `{c.settings.gcp.project_id}.{c.settings.gcp.bq_dataset}.trades`
            WHERE trade_date BETWEEN '{fd}' AND '{td}'
              AND exit_reason != 'EOD_CLOSE_NO_QUOTE'
              AND {_BQ_VALID_TRADE}
        )
        SELECT
            COUNT(*) as total_trades,
            -- exit_reason stays the primary win/loss signal so pnl=0 rows can't inflate win_rate
            COUNTIF(exit_reason = 'TARGET_HIT' OR (exit_reason NOT IN ('SL_HIT','EOD_CLOSE_NO_QUOTE') AND p > 0)) as wins,
            COUNTIF(exit_reason = 'SL_HIT' OR (exit_reason NOT IN ('TARGET_HIT','EOD_CLOSE_NO_QUOTE') AND p < 0)) as losses,
            COALESCE(SUM(p), 0) as total_pnl,
            COALESCE(AVG(CASE WHEN p > 0 THEN p END), 0) as avg_win,
            COALESCE(AVG(CASE WHEN p < 0 THEN p END), 0) as avg_loss,
            COALESCE(MAX(CASE WHEN p > 0 THEN p END), 0) as biggest_win,
            COALESCE(MIN(CASE WHEN p < 0 THEN p END), 0) as biggest_loss,
            COALESCE(SUM(CASE WHEN p > 0 THEN p ELSE 0 END), 0) as gross_profit,
            COALESCE(ABS(SUM(CASE WHEN p < 0 THEN p ELSE 0 END)), 0) as gross_loss
        FROM t
    """
    try:
        rows = c.bq.query(q)
        r = rows[0] if rows else {}
        total = r.get("total_trades", 0)
        wins = r.get("wins", 0)
        win_rate = (wins / total * 100) if total else 0
        gross_profit = r.get("gross_profit", 0)
        gross_loss = r.get("gross_loss", 0)
        # None signals "no losing trades" — frontend renders as ∞
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (None if gross_profit > 0 else 0.0)
        avg_win = r.get("avg_win", 0)
        avg_loss = abs(r.get("avg_loss", 0))
        expectancy = (win_rate / 100 * avg_win) - ((1 - win_rate / 100) * avg_loss) if total else 0

        return {
            "total_pnl": r.get("total_pnl", 0),
            "total_trades": total,
            "win_rate": round(win_rate, 1),
            "biggest_win": r.get("biggest_win", 0),
            "biggest_loss": r.get("biggest_loss", 0),
            "profit_factor": round(profit_factor, 2) if profit_factor is not None else None,
            "expectancy": round(expectancy, 2),
            "avg_rr": round(avg_win / avg_loss, 2) if avg_loss else 0,
        }
    except Exception as exc:
        logger.error("trades/summary query failed: %s", exc)
        return {"total_pnl": 0, "total_trades": 0, "win_rate": 0, "error": str(exc)}


@router.get("/trades/equity-curve")
def get_trades_equity_curve(
    from_date: str | None = Query(default=None, alias="from"),
    to_date: str | None = Query(default=None, alias="to"),
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Daily cumulative P&L series."""
    c = get_container()
    today = now_ist().strftime("%Y-%m-%d")
    td = to_date or today
    fd = from_date or (date.fromisoformat(td) - timedelta(days=90)).isoformat()

    q = f"""
        SELECT trade_date, SUM(IFNULL(net_pnl, pnl)) as daily_pnl   -- NET of costs (see stats note)
        FROM `{c.settings.gcp.project_id}.{c.settings.gcp.bq_dataset}.trades`
        WHERE trade_date BETWEEN '{fd}' AND '{td}'
          AND exit_reason != 'EOD_CLOSE_NO_QUOTE'
          AND {_BQ_VALID_TRADE}
        GROUP BY trade_date
        ORDER BY trade_date
    """
    try:
        rows = c.bq.query(q)
        cum = 0.0
        series = []
        for r in rows:
            cum += float(r.get("daily_pnl") or 0)
            series.append({"date": str(r.get("trade_date", "")), "pnl": round(cum, 2)})
        return {"series": series}
    except Exception as exc:
        logger.error("trades/equity-curve query failed: %s", exc)
        return {"series": [], "error": str(exc)}


@router.get("/trades/list")
def get_trades_list(
    from_date: str | None = Query(default=None, alias="from"),
    to_date: str | None = Query(default=None, alias="to"),
    strategy: str | None = Query(default=None),
    symbol: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Paginated trade list from BQ."""
    c = get_container()
    today = now_ist().strftime("%Y-%m-%d")
    td = _safe_date(to_date, today)
    fd = _safe_date(from_date, (date.fromisoformat(td) - timedelta(days=30)).isoformat())
    strategy_safe = _safe_str(strategy)
    symbol_safe = _safe_str(symbol)

    where = f"trade_date BETWEEN '{fd}' AND '{td}'"
    if strategy_safe:
        where += f" AND strategy = '{strategy_safe}'"
    if symbol_safe:
        where += f" AND UPPER(symbol) = '{symbol_safe.upper()}'"

    q = f"""
        SELECT * REPLACE(
            IFNULL(net_pnl, pnl) AS pnl,              -- canonical `pnl` is NET of costs
            IFNULL(net_pnl_pct, pnl_pct) AS pnl_pct
        ), pnl AS gross_pnl, pnl_pct AS gross_pnl_pct
        FROM `{c.settings.gcp.project_id}.{c.settings.gcp.bq_dataset}.trades`
        WHERE {where} AND {_BQ_VALID_TRADE}
        ORDER BY trade_date DESC, entry_ts DESC
        LIMIT {limit} OFFSET {offset}
    """
    try:
        rows = c.bq.query(q)
        for r in rows:
            for k, v in r.items():
                if hasattr(v, "isoformat"):
                    r[k] = v.isoformat()
        return {"trades": rows, "limit": limit, "offset": offset}
    except Exception as exc:
        logger.error("trades/list query failed: %s", exc)
        return {"trades": [], "error": str(exc)}


@router.post("/trades/export")
def post_trades_export(
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Trigger a CSV export. Stub for now."""
    return {"status": "ok", "message": "Export not yet implemented"}


# ---------------------------------------------------------------------------
# Signals
# ---------------------------------------------------------------------------


@router.get("/signals/today")
def get_signals_today(
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Today's signals from BQ."""
    c = get_container()
    today = now_ist().strftime("%Y-%m-%d")
    q = f"""
        SELECT *
        FROM `{c.settings.gcp.project_id}.{c.settings.gcp.bq_dataset}.signals`
        WHERE run_date = '{today}'
        ORDER BY scan_ts DESC
    """
    try:
        rows = c.bq.query(q)
        for r in rows:
            for k, v in r.items():
                if hasattr(v, "isoformat"):
                    r[k] = v.isoformat()
        return {"date": today, "signals": rows}
    except Exception as exc:
        logger.error("signals/today query failed: %s", exc)
        return {"date": today, "signals": [], "error": str(exc)}


@router.get("/scan/latest")
def get_scan_latest(
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Latest scan audit trail from Firestore — every scanned symbol with direction,
    score, and reason. Updated after each scan run (every 5 min during market hours)."""
    c = get_container()
    try:
        doc = c.state.get_json("scan_results", "latest")
        if not doc:
            return {"rows": [], "scanned": 0, "qualified": 0, "regime": "", "risk_mode": "", "scan_ts": ""}
        # Sanitize timestamps
        for k in ("scan_ts", "updated_at"):
            v = doc.get(k)
            if v is not None and hasattr(v, "isoformat"):
                doc[k] = v.isoformat()
        return doc
    except Exception as exc:
        logger.error("scan/latest query failed: %s", exc)
        return {"rows": [], "scanned": 0, "qualified": 0, "error": str(exc)}


# ---------------------------------------------------------------------------
# EVENT / PEAD channel (2026-06-19) — shown the same way as swing/intraday
# ---------------------------------------------------------------------------
@router.get("/pead/watchlist")
def get_pead_watchlist(
    date: str | None = Query(default=None),
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """PEAD daily candidate watchlist (surprise-ranked, annotated ENTERED/HELD/
    NOT_SELECTED/BREAKER) — written by the pead-scan job. `date` defaults to latest."""
    c = get_container()
    try:
        key = _safe_date(date, "latest") if date else "latest"
        doc = c.state.get_json("pead_watchlist", key)
        if not doc:
            return {"asof": "", "rows": [], "candidates": 0, "entered": 0, "market_ok": None}
        v = doc.get("updated_at")
        if v is not None and hasattr(v, "isoformat"):
            doc["updated_at"] = v.isoformat()
        return doc
    except Exception as exc:
        logger.error("pead/watchlist query failed: %s", exc)
        return {"asof": "", "rows": [], "candidates": 0, "entered": 0, "error": str(exc)}


@router.get("/insider/watchlist")
def get_insider_watchlist(
    date: str | None = Query(default=None),
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """INSIDER daily gate + candidate-cluster watchlist — the double macro-gate state
    (macro_gate_ok + b200 + nifty>100DMA) and cluster rows (symbol, n_buyers, category, status),
    written by the insider-scan job on EVERY run incl. gated-off days. `date` defaults to latest.
    Read-only; mirrors /pead/watchlist."""
    c = get_container()
    try:
        key = _safe_date(date, "latest") if date else "latest"
        doc = c.state.get_json("insider_watchlist", key)
        if not doc:
            return {"asof": "", "rows": [], "clusters": 0, "candidates": 0,
                    "macro_gate_ok": None, "macro": {}}
        v = doc.get("updated_at")
        if v is not None and hasattr(v, "isoformat"):
            doc["updated_at"] = v.isoformat()
        return doc
    except Exception as exc:
        logger.error("insider/watchlist query failed: %s", exc)
        return {"asof": "", "rows": [], "clusters": 0, "candidates": 0, "error": str(exc)}


@router.get("/pledge/watchlist")
def get_pledge_watchlist(
    date: str | None = Query(default=None),
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """PLEDGE daily gate + candidate watchlist — the double macro-gate state (macro_gate_ok +
    b200 + nifty>100DMA) and promoter pledge-revoke rows (symbol, n_revokes, category, turnover,
    status), written by the pledge-scan job on EVERY run incl. gated-off days. `date` defaults to
    latest. Read-only; mirrors /insider/watchlist."""
    c = get_container()
    try:
        key = _safe_date(date, "latest") if date else "latest"
        doc = c.state.get_json("pledge_watchlist", key)
        if not doc:
            return {"asof": "", "rows": [], "revoke_symbols": 0, "candidates": 0,
                    "macro_gate_ok": None, "macro": {}}
        v = doc.get("updated_at")
        if v is not None and hasattr(v, "isoformat"):
            doc["updated_at"] = v.isoformat()
        return doc
    except Exception as exc:
        logger.error("pledge/watchlist query failed: %s", exc)
        return {"asof": "", "rows": [], "revoke_symbols": 0, "candidates": 0, "error": str(exc)}


@router.get("/pead/summary")
def get_pead_summary(
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """PEAD channel summary — open positions + slots, realized P&L (strategy=PEAD),
    market-state, breaker — the per-channel view (parity with swing/intraday)."""
    c = get_container()
    out: dict[str, Any] = {"channel": "pead", "capital": c.settings.strategy.channel_capital("pead"),
                           "max_slots": c.settings.strategy.pead_max_positions}
    try:
        open_pos = [p for p in c.state.list_open_positions()
                    if str(p.get("channel", "")).strip().lower() == "pead"]
        out["open_positions"] = len(open_pos)
        out["slots_used"] = len(open_pos)
        out["open"] = [{"symbol": p.get("symbol"), "qty": p.get("qty"), "entry_price": p.get("entry_price"),
                        "sl_price": p.get("sl_price"), "strategy": p.get("strategy"),
                        "entry_ts": str(p.get("entry_ts", ""))[:19]} for p in open_pos]
    except Exception as exc:
        logger.error("pead/summary positions failed: %s", exc)
        out["open_positions"] = None; out["error_positions"] = str(exc)
    try:
        wl = c.state.get_json("pead_watchlist", "latest") or {}
        out["market_dd"] = wl.get("market_dd")
        out["market_ok"] = wl.get("market_ok")
        out["breaker_tripped"] = wl.get("breaker_tripped")
        out["last_scan"] = wl.get("asof")
    except Exception as exc:
        logger.error("pead/summary market-state failed: %s", exc)
    try:
        q = (f"SELECT COUNT(*) n, COALESCE(SUM(pnl),0) total_pnl, "
             f"COUNTIF(pnl>0) wins, COUNTIF(pnl<0) losses "
             f"FROM `{c.settings.gcp.project_id}.{c.settings.gcp.bq_dataset}.trades` "
             f"WHERE strategy='PEAD' AND {_BQ_VALID_TRADE}")
        r = next(iter(c.bq.query(q)), None)
        if r:
            out["closed_trades"] = int(r.get("n") or 0)
            out["realized_pnl"] = round(float(r.get("total_pnl") or 0.0), 2)
            out["wins"] = int(r.get("wins") or 0)
            out["losses"] = int(r.get("losses") or 0)
    except Exception as exc:
        logger.error("pead/summary trades failed: %s", exc)
        out["realized_pnl"] = None; out["error_trades"] = str(exc)
    return out


# ---------------------------------------------------------------------------
# Channels — per-channel overview (Phase 0, 2026-06-22)
# ---------------------------------------------------------------------------
#
# The system runs five FUNDED channels (swing, intraday, pead, gap_fade, core)
# under a Rs12L PAPER book. corp_action positions are tagged channel="pead"
# (they share the EVENT pool), so they fold into the pead card. These GETs are
# read-only + additive — no trading-path, schema, or env change.

_CHANNELS: tuple[str, ...] = ("swing", "intraday", "pead", "gap_fade", "core", "momentum", "delivery", "insider", "pledge")


def _position_channel(p: dict[str, Any]) -> str:
    """Derive a position's channel — mirrors firestore_state's routing: explicit
    `channel`, else `wl_type`, else legacy default 'intraday'."""
    return str(p.get("channel") or p.get("wl_type") or "intraday").strip().lower()


def build_channel_overview(
    channels: Any,
    positions: Any,
    pnl_by_channel: Any,
    risk_by_channel: Any,
    capital_of: Any,
    max_positions_of: Any,
    daily_loss_pct: float,
    daily_profit_pct: float,
    realized_by_channel: Any = None,
    unrealized_by_channel: Any = None,
    daily_move_by_channel: Any = None,
    forward_by_channel: Any = None,
    halted_by_channel: Any = None,
) -> dict[str, Any]:
    """PURE per-channel rollup (no I/O — unit-tested). For each channel: capital,
    enabled, open-position count + symbols, today's realized P&L, open R-at-risk,
    slot cap, daily-breaker state, AND (2026-08) all-time realized P&L + trade count +
    win-rate, open cost basis, unrealized P&L (open marked to last close), overall
    P&L (realized + unrealized), and today's MTM move (open book marked last vs prev
    close — the meaningful daily P&L for a hold book where realized-from-exits is ~0 on
    non-exit days). `realized_by_channel` maps ch -> {realized, closed, wins};
    `unrealized_by_channel` / `daily_move_by_channel` map ch -> ₹ (all optional — omitted
    → 0). Unfunded channels (capital 0) are `enabled=False` and never trip the breaker.

    `realized_by_channel` is the ALL-TIME clean record (invalid-tagged rows already
    excluded upstream) — it is what the cards lead with. `forward_by_channel` carries the
    same shape restricted to the forward-test epoch and is surfaced as the secondary
    `fwd_*` fields. Rationale (2026-08-10): making the epoch the primary number blanked
    every card to ₹0/0 trades, hiding the whole trade history for no gain."""
    grouped: dict[str, list] = {}
    for p in positions or []:
        grouped.setdefault(_position_channel(p), []).append(p)
    realized_by_channel = realized_by_channel or {}
    unrealized_by_channel = unrealized_by_channel or {}
    daily_move_by_channel = daily_move_by_channel or {}
    forward_by_channel = forward_by_channel or {}
    halted_by_channel = halted_by_channel or {}
    rows: list[dict[str, Any]] = []
    tot_cap = tot_pnl = tot_risk = tot_real = tot_unreal = tot_openval = tot_move = 0.0
    tot_fwd_real = 0.0
    tot_open = tot_closed = tot_fwd_closed = 0
    for ch in channels:
        cap = float(capital_of(ch) or 0.0)
        pnl = round(float((pnl_by_channel or {}).get(ch, 0.0)), 2)
        risk = round(float((risk_by_channel or {}).get(ch, 0.0)), 2)
        poss = grouped.get(ch, [])
        rstat = realized_by_channel.get(ch, {}) or {}
        realized = round(float(rstat.get("realized", 0.0)), 2)
        closed = int(rstat.get("closed", 0) or 0)
        wins = int(rstat.get("wins", 0) or 0)
        win_rate = round(100.0 * wins / closed, 1) if closed else None
        unreal = round(float(unrealized_by_channel.get(ch, 0.0)), 2)
        today_move = round(float(daily_move_by_channel.get(ch, 0.0)), 2)
        halt = str(halted_by_channel.get(ch, "") or "")   # non-empty => channel is stopped
        fstat = forward_by_channel.get(ch, {}) or {}
        fwd_real = round(float(fstat.get("realized", 0.0)), 2)
        fwd_closed = int(fstat.get("closed", 0) or 0)
        open_value = round(sum(float(p.get("entry_price") or 0) * float(p.get("qty") or 0) for p in poss), 2)
        overall = round(realized + unreal, 2)
        loss_limit = round(-abs(daily_loss_pct) * cap, 2)
        profit_limit = round(abs(daily_profit_pct) * cap, 2)
        hit_loss = cap > 0 and pnl <= loss_limit
        hit_profit = cap > 0 and pnl >= profit_limit
        rows.append({
            "channel": ch,
            "capital": round(cap, 2),
            "enabled": cap > 0,
            "open_positions": len(poss),
            "open_symbols": [str(p.get("symbol") or "") for p in poss],
            "today_pnl": pnl,
            "today_move": today_move,
            "realized_pnl": realized,
            "unrealized_pnl": unreal,
            "overall_pnl": overall,
            "closed_trades": closed,
            "win_rate": win_rate,
            "fwd_realized_pnl": fwd_real,     # forward-test epoch only (secondary line)
            "fwd_closed_trades": fwd_closed,
            "halted": bool(halt),             # intentionally stopped (halted/killed)
            "halt_reason": halt or None,
            "open_value": open_value,
            "open_risk": risk,
            "max_positions": max_positions_of(ch),
            "daily_loss_limit": loss_limit,
            "daily_profit_limit": profit_limit,
            "breaker_tripped": bool(hit_loss or hit_profit),
            "breaker_reason": ("daily_loss_limit_hit" if hit_loss
                               else "daily_profit_target_hit" if hit_profit else None),
        })
        if cap > 0:
            tot_cap += cap
        tot_pnl += pnl
        tot_risk += risk
        tot_real += realized
        tot_unreal += unreal
        tot_openval += open_value
        tot_move += today_move
        tot_open += len(poss)
        tot_closed += closed
        tot_fwd_real += fwd_real
        tot_fwd_closed += fwd_closed
    return {
        "channels": rows,
        "totals": {
            "capital": round(tot_cap, 2),
            "today_pnl": round(tot_pnl, 2),
            "today_move": round(tot_move, 2),
            "realized_pnl": round(tot_real, 2),
            "unrealized_pnl": round(tot_unreal, 2),
            "overall_pnl": round(tot_real + tot_unreal, 2),
            "open_value": round(tot_openval, 2),
            "open_positions": tot_open,
            "closed_trades": tot_closed,
            "fwd_realized_pnl": round(tot_fwd_real, 2),
            "fwd_closed_trades": tot_fwd_closed,
            "open_risk": round(tot_risk, 2),
        },
    }


# Cache: {symbol: [latest_close, prev_close]} newest-first (up to 2). ONE BQ query feeds
# BOTH the unrealized mark (latest close vs entry) and today's MTM move (latest − prev).
# BQ trades exclusion (2026-08-07): rows tagged data_quality='INVALID' are the
# EOD-squareoff bug exits (core 30 + delivery 4) — never strategy outcomes, so they must
# not reach any P&L/win-rate aggregate. NULL-safe via IFNULL (SQL `!=` drops NULLs).
_BQ_VALID_TRADE = "IFNULL(data_quality,'') != 'INVALID'"


_MARK_CACHE: dict[str, Any] = {"ts": 0.0, "recent": {}}


def _recent_closes(c: Any, symbols: list[str]) -> dict[str, list[float]]:
    """Last two daily closes per symbol from nse_delivery_daily (newest-first), cached
    ~10 min in-process so the cockpit never hits BQ on every page load. Best-effort: a BQ
    failure returns the last good cache (or {}) rather than raising.

    Source = nse_delivery_daily (the live NSE EOD feed the delivery channel ingests daily:
    ~2,400 symbols/session, plain tickers, current through the prior session). candles_1d
    is deliberately NOT used — it stopped populating the traded universe (only 1-2 stray
    symbols/day), so it marks every held name to nothing (verified 2026-08-05)."""
    import time
    syms = sorted({str(s).strip().upper() for s in symbols if s})
    now = time.time()
    cached = _MARK_CACHE["recent"]
    if now - float(_MARK_CACHE["ts"]) < 600 and cached and all(x in cached for x in syms):
        return cached
    if not syms:
        return cached or {}
    frm = (date.fromisoformat(now_ist().strftime("%Y-%m-%d")) - timedelta(days=12)).isoformat()
    inlist = ",".join("'" + s.replace("'", "") + "'" for s in syms)
    q = (f"SELECT symbol, date, close_price FROM `{c.settings.gcp.project_id}.{c.settings.gcp.bq_dataset}.nse_delivery_daily` "
         f"WHERE symbol IN ({inlist}) AND date >= '{frm}' "
         f"QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) <= 2")
    try:
        rows = [r for r in c.bq.query(q) if r.get("close_price") is not None]
        rows.sort(key=lambda r: str(r.get("date")), reverse=True)   # newest first
        recent: dict[str, list[float]] = {}
        for r in rows:
            recent.setdefault(str(r["symbol"]).strip().upper(), []).append(float(r["close_price"]))
        _MARK_CACHE["ts"] = now
        _MARK_CACHE["recent"] = recent
        return recent
    except Exception as exc:
        logger.error("channel mark query failed: %s", exc)
        return cached or {}


def _unrealized_by_channel(c: Any, positions: list[dict[str, Any]]) -> dict[str, float]:
    """Unrealized P&L per channel = Σ (last_close − entry) × qty over open positions,
    marked to the latest candles_1d close. Symbols absent from candles (e.g. some ETFs)
    contribute 0. Channel routing reuses `_position_channel`."""
    recent = _recent_closes(c, [str(p.get("symbol") or "") for p in positions or []])
    out: dict[str, float] = {}
    for p in positions or []:
        closes = recent.get(str(p.get("symbol") or "").strip().upper())
        if not closes:
            continue
        entry = float(p.get("entry_price") or 0)
        qty = float(p.get("qty") or 0)
        ch = _position_channel(p)
        out[ch] = out.get(ch, 0.0) + (closes[0] - entry) * qty
    return {k: round(v, 2) for k, v in out.items()}


def _daily_move_by_channel(c: Any, positions: list[dict[str, Any]]) -> dict[str, float]:
    """Today's mark-to-market move per channel = Σ (last_close − prev_close) × qty over
    open positions — the day-to-day P&L of a buy-and-hold book (realized-from-exits is ~0
    on non-exit days). Positions with <2 available closes (just entered / illiquid / ETF
    absent from candles) contribute 0. Channel routing reuses `_position_channel`."""
    recent = _recent_closes(c, [str(p.get("symbol") or "") for p in positions or []])
    out: dict[str, float] = {}
    for p in positions or []:
        closes = recent.get(str(p.get("symbol") or "").strip().upper())
        if not closes or len(closes) < 2:
            continue
        qty = float(p.get("qty") or 0)
        ch = _position_channel(p)
        out[ch] = out.get(ch, 0.0) + (closes[0] - closes[1]) * qty
    return {k: round(v, 2) for k, v in out.items()}


def _halted_channels(s: Any) -> dict[str, str]:
    """Channels intentionally stopped, so the cockpit can badge them HALTED instead of
    ACTIVE. `enabled` alone is `capital > 0`, which cannot express "funded but switched
    off" (intraday) or "killed" (gap_fade). Derived from live config only — no guessing:
      * intraday  -> WATCHLIST_SWING_ONLY=true means its scan path is off (halted 07-09)
      * gap_fade  -> zero allocation == killed (07-14); its scheduler is PAUSED too
    """
    out: dict[str, str] = {}
    if bool(getattr(s, "watchlist_swing_only", False)):
        out["intraday"] = "halted (WATCHLIST_SWING_ONLY)"
    if float(s.channel_capital_allocated("gap_fade") or 0) <= 0:
        out["gap_fade"] = "killed (unfunded)"
    return out


@router.get("/channels/overview")
def get_channels_overview(
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Per-channel cockpit: capital, today's P&L, open positions, R-at-risk,
    slot cap, and daily-breaker state for each of the five funded channels."""
    c = get_container()
    s = c.settings.strategy
    today = now_ist().strftime("%Y-%m-%d")
    try:
        positions = c.state.list_open_positions()
    except Exception as exc:
        logger.error("channels/overview positions failed: %s", exc)
        positions = []
    try:
        pnl_by = c.state.get_today_realized_pnl_by_channel(today)
    except Exception as exc:
        logger.error("channels/overview pnl failed: %s", exc)
        pnl_by = {}
    try:
        risk_by = c.state.get_open_risk_by_channel()
    except Exception as exc:
        logger.error("channels/overview risk failed: %s", exc)
        risk_by = {}
    # Forward-test epoch: the cockpit reports the REVAMPED system's own record. Trades
    # entered before settings.forward_test_start belong to the pre-revamp system and are
    # excluded (entry-based, so old positions closing inside the window don't leak in).
    fwd_start = str(getattr(s, "forward_test_start", "") or "").strip()
    try:
        realized_by = c.state.get_realized_stats_by_channel(since_entry=fwd_start)
    except Exception as exc:
        logger.error("channels/overview realized failed: %s", exc)
        realized_by = {}
    try:                       # all-time kept alongside so nothing is hidden
        alltime_by = c.state.get_realized_stats_by_channel()
    except Exception as exc:
        logger.error("channels/overview all-time realized failed: %s", exc)
        alltime_by = {}
    try:
        unrealized_by = _unrealized_by_channel(c, positions)
    except Exception as exc:
        logger.error("channels/overview unrealized failed: %s", exc)
        unrealized_by = {}
    try:
        move_by = _daily_move_by_channel(c, positions)
    except Exception as exc:
        logger.error("channels/overview daily-move failed: %s", exc)
        move_by = {}
    max_pos = {"swing": s.swing_max_positions, "intraday": s.max_positions,
               "pead": s.pead_max_positions, "gap_fade": s.gapfade_max_positions,
               "core": None, "delivery": s.delivery_max_positions,
               "insider": s.insider_max_positions, "pledge": s.pledge_max_positions}
    out = build_channel_overview(
        _CHANNELS, positions, pnl_by, risk_by,
        # ALLOCATED capital, not the trading fallback: channel_capital() fail-opens to
        # the shared pool, which credited killed gap_fade with Rs6L and inflated the
        # cockpit total to Rs25L vs the real Rs19L (and badged it ACTIVE via capital>0).
        capital_of=s.channel_capital_allocated,
        max_positions_of=lambda ch: max_pos.get(ch),
        daily_loss_pct=s.daily_loss_pct, daily_profit_pct=s.daily_profit_pct,
        realized_by_channel=alltime_by, unrealized_by_channel=unrealized_by,
        daily_move_by_channel=move_by, forward_by_channel=realized_by,
        halted_by_channel=_halted_channels(s),
    )
    out["asof"] = today
    # PRIMARY on the cards = the ALL-TIME clean record (`realized_pnl` / `overall_pnl` /
    # `closed_trades`); the forward-test epoch rides along as `fwd_*` + this block.
    # (2026-08-10: the epoch was briefly primary, which blanked every card to ₹0/0 trades
    # and hid the whole trade history — corrected.)
    at_real = round(float(out["totals"].get("realized_pnl", 0.0)), 2)
    at_closed = int(out["totals"].get("closed_trades", 0) or 0)
    fw_closed = int(out["totals"].get("fwd_closed_trades", 0) or 0)
    fw_real = round(float(out["totals"].get("fwd_realized_pnl", 0.0)), 2)
    out["forward_test"] = {
        "start": fwd_start,
        "basis": "entry_ts >= start (entry-based, so pre-cutoff positions closing inside "
                 "the window are excluded)",
        "realized_pnl": fw_real,
        "closed_trades": fw_closed,
        "open_positions_in_epoch": sum(
            1 for p in (positions or [])
            if fwd_start and str(p.get("entry_ts", "") or "")[:10] >= fwd_start),
        "all_time_realized_pnl": at_real,
        "all_time_closed_trades": at_closed,
        "pre_epoch_realized_pnl": round(at_real - fw_real, 2),
        "pre_epoch_closed_trades": at_closed - fw_closed,
    }
    return out


@router.get("/positions/by-channel")
def get_positions_by_channel(
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Open positions grouped by channel, with per-position detail."""
    c = get_container()
    try:
        positions = c.state.list_open_positions()
    except Exception as exc:
        logger.error("positions/by-channel failed: %s", exc)
        return {"channels": {}, "total": 0, "error": str(exc)}
    grouped: dict[str, list] = {}
    for p in positions:
        grouped.setdefault(_position_channel(p), []).append({
            "symbol": p.get("symbol"),
            "side": p.get("side"),
            "qty": p.get("qty"),
            "entry_price": p.get("entry_price"),
            "sl_price": p.get("sl_price"),
            "target": p.get("target"),
            "max_loss": p.get("max_loss"),
            "strategy": p.get("strategy"),
            "wl_type": p.get("wl_type"),
            "entry_ts": str(p.get("entry_ts", ""))[:19],
        })
    return {"channels": grouped, "total": len(positions)}


@router.get("/core/basket")
def get_core_basket(
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """CORE buy-and-hold basket — current holdings + entry-notional weights."""
    c = get_container()
    s = c.settings.strategy
    try:
        positions = [p for p in c.state.list_open_positions()
                     if _position_channel(p) == "core"]
    except Exception as exc:
        logger.error("core/basket failed: %s", exc)
        return {"channel": "core", "holdings": [], "count": 0, "error": str(exc)}
    holdings: list[dict[str, Any]] = []
    for p in positions:
        qty = float(p.get("qty") or 0.0)
        entry = float(p.get("entry_price") or 0.0)
        holdings.append({
            "symbol": p.get("symbol"),
            "qty": int(qty),
            "entry_price": entry,
            "sl_price": p.get("sl_price"),
            "entry_ts": str(p.get("entry_ts", ""))[:19],
            "notional": round(qty * entry, 2),
        })
    total = sum(h["notional"] for h in holdings) or 0.0
    for h in holdings:
        h["weight_pct"] = round(100.0 * h["notional"] / total, 2) if total else 0.0
    holdings.sort(key=lambda h: -h["notional"])
    return {
        "channel": "core",
        "capital": s.channel_capital("core"),
        "enabled": bool(s.core_enabled),
        "count": len(holdings),
        "deployed_notional": round(total, 2),
        "holdings": holdings,
    }


@router.get("/momentum/basket")
def get_momentum_basket(
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Momentum x Low-Vol buy-and-hold basket — current holdings + entry-notional weights."""
    c = get_container()
    s = c.settings.strategy
    try:
        positions = [p for p in c.state.list_open_positions()
                     if _position_channel(p) == "momentum"]
    except Exception as exc:
        logger.error("momentum/basket failed: %s", exc)
        return {"channel": "momentum", "holdings": [], "count": 0, "error": str(exc)}
    holdings: list[dict[str, Any]] = []
    for p in positions:
        qty = float(p.get("qty") or 0.0)
        entry = float(p.get("entry_price") or 0.0)
        holdings.append({
            "symbol": p.get("symbol"),
            "qty": int(qty),
            "entry_price": entry,
            "sl_price": p.get("sl_price"),
            "entry_ts": str(p.get("entry_ts", ""))[:19],
            "notional": round(qty * entry, 2),
        })
    total = sum(h["notional"] for h in holdings) or 0.0
    for h in holdings:
        h["weight_pct"] = round(100.0 * h["notional"] / total, 2) if total else 0.0
    holdings.sort(key=lambda h: -h["notional"])
    return {
        "channel": "momentum",
        "capital": s.channel_capital("momentum"),
        "enabled": bool(s.momentum_enabled),
        "count": len(holdings),
        "deployed_notional": round(total, 2),
        "holdings": holdings,
    }


@router.get("/gap-fade/shorts")
def get_gap_fade_shorts(
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """GAP_FADE channel — open intraday shorts (covered at the 15:25 squareoff)."""
    c = get_container()
    s = c.settings.strategy
    try:
        positions = [p for p in c.state.list_open_positions()
                     if _position_channel(p) == "gap_fade"]
    except Exception as exc:
        logger.error("gap-fade/shorts failed: %s", exc)
        return {"channel": "gap_fade", "shorts": [], "count": 0, "error": str(exc)}
    shorts = [{
        "symbol": p.get("symbol"),
        "qty": p.get("qty"),
        "entry_price": p.get("entry_price"),
        "sl_price": p.get("sl_price"),
        "strategy": p.get("strategy"),
        "entry_ts": str(p.get("entry_ts", ""))[:19],
    } for p in positions]
    cap = s.channel_capital("gap_fade")
    return {
        "channel": "gap_fade",
        "capital": cap,
        "max_positions": s.gapfade_max_positions,
        "enabled": bool(cap > 0 and s.gapfade_max_positions > 0),
        "count": len(shorts),
        "squareoff_ist": "15:25",
        "shorts": shorts,
    }


# ---------------------------------------------------------------------------
# Market Brain — Tier-1 (PR-2)
# ---------------------------------------------------------------------------
#
# Three routes feed the dashboard Market-Brain page:
#   /market-brain/latest   — full Firestore doc (state + context + policy +
#                            narrative). Fast Firestore read, no BQ.
#   /market-brain/explain  — composed breakdown (per-component weights and
#                            contributions + confidence drivers + secondary
#                            signals + narrative). Pure over the Firestore
#                            doc, no recomputation.
#   /market-brain/history  — BQ-backed timeseries of scores for the chart.
#
# The Firestore doc is updated by the market-brain background jobs
# (premarket-precompute, watchlist-refresh, scan-market-5m). These GETs
# never trigger a recompute.


def _sanitize_brain_doc(doc: dict[str, Any]) -> dict[str, Any]:
    """Convert Firestore timestamps in the brain doc to ISO strings."""
    if not isinstance(doc, dict):
        return {}
    out = dict(doc)
    for k in ("updated_at", "asof_ts"):
        v = out.get(k)
        if v is not None and hasattr(v, "isoformat"):
            out[k] = v.isoformat()
    # Sanitize nested timestamps one level deep
    for section in ("state", "context", "policy", "narrative"):
        v = out.get(section)
        if isinstance(v, dict):
            for k, x in list(v.items()):
                if hasattr(x, "isoformat"):
                    v[k] = x.isoformat()
    return out


@router.get("/market-brain/latest")
def get_market_brain_latest(
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Return the latest full market-brain document from Firestore —
    state + context + policy + narrative. Cheap: no BQ, no recompute.
    The same doc the real-time Firestore listener sees; this GET exists
    for SSR/hydration paths and for non-Firebase clients that cannot
    open a websocket."""
    c = get_container()
    try:
        doc = c.state.get_json("market_brain", "latest")
        if not doc:
            return {"state": {}, "context": {}, "policy": {}, "narrative": {}, "empty": True}
        return _sanitize_brain_doc(doc)
    except Exception as exc:
        logger.error("market-brain/latest query failed: %s", exc)
        return {"state": {}, "context": {}, "policy": {}, "narrative": {}, "error": str(exc)}


@router.get("/market-brain/explain")
def get_market_brain_explain(
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Return a structured per-component explanation of the latest state.

    Uses the persisted Firestore doc (no recompute) and composes a
    transparent breakdown: each score with its weight, contribution,
    delta from the previous snapshot, qualitative band, and a short
    rationale. Also returns confidence drivers and secondary signals.
    Safe even if the narrative was never persisted (will be built on
    the fly from state + context)."""
    c = get_container()
    try:
        doc = c.state.get_json("market_brain", "latest")
        if not doc:
            return {"empty": True}
        brain = c.market_brain_service()
        state_dict = doc.get("state") if isinstance(doc.get("state"), dict) else {}
        context_dict = doc.get("context") if isinstance(doc.get("context"), dict) else {}
        narrative_dict = doc.get("narrative") if isinstance(doc.get("narrative"), dict) else {}
        # Rehydrate a MarketBrainState; tolerant of missing fields (default-safe).
        state = brain._state_from_dict(state_dict)
        if state is None:
            return {"empty": True}
        narrative = narrative_dict or brain.build_narrative(state, context_dict)
        explain = brain.build_explain_payload(
            state,
            context=context_dict,
            policy=None,  # policy dict already in doc; _state_from_dict doesn't hydrate MarketPolicy
            narrative=narrative,
        )
        # Attach the raw policy dict directly — no need to rehydrate for a read-only view.
        explain["policy"] = doc.get("policy") if isinstance(doc.get("policy"), dict) else {}
        return explain
    except Exception as exc:
        logger.exception("market-brain/explain failed")
        return {"empty": True, "error": str(exc)}


_HISTORY_SCORE_COLUMNS: tuple[str, ...] = (
    "asof_ts",
    "regime",
    "risk_mode",
    "participation",
    "trend_score",
    "breadth_score",
    "volatility_stress_score",
    "data_quality_score",
    "options_positioning_score",
    "flow_score",
    "breadth_roc_score",
    "market_confidence",
    "breadth_confidence",
    "leadership_confidence",
    "prev_regime",
    "regime_age_seconds",
    "regime_transitions_today",
    "signal_age_penalty",
)


@router.get("/market-brain/history")
def get_market_brain_history(
    days: int = Query(default=1, ge=1, le=7),
    limit: int = Query(default=500, ge=10, le=2000),
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """BQ-backed timeseries of market-brain scores for charting.

    Returns up to `limit` rows from the last `days` calendar days,
    ordered chronologically so charts render left-to-right. Bounded
    hard: days∈[1,7], limit∈[10,2000]. The table has one row per
    persist_market_brain_state call (typically every 60-180s during
    market hours, plus premarket + EOD snapshots)."""
    c = get_container()
    today = now_ist().strftime("%Y-%m-%d")
    start = (date.fromisoformat(today) - timedelta(days=max(0, int(days) - 1))).isoformat()
    cols_sql = ", ".join(_HISTORY_SCORE_COLUMNS)
    q = f"""
        SELECT {cols_sql}
        FROM `{c.settings.gcp.project_id}.{c.settings.gcp.bq_dataset}.market_brain_history`
        WHERE run_date BETWEEN '{start}' AND '{today}'
        ORDER BY asof_ts ASC
        LIMIT {int(limit)}
    """
    try:
        rows = c.bq.query(q)
        # Sanitize timestamps
        for r in rows:
            for k, v in list(r.items()):
                if hasattr(v, "isoformat"):
                    r[k] = v.isoformat()
        return {
            "series": rows,
            "meta": {
                "days": int(days),
                "limit": int(limit),
                "row_count": len(rows),
                "from_date": start,
                "to_date": today,
            },
        }
    except Exception as exc:
        logger.error("market-brain/history query failed: %s", exc)
        return {"series": [], "meta": {"days": int(days), "limit": int(limit), "row_count": 0, "error": str(exc)}}


# ---------------------------------------------------------------------------
# Universe
# ---------------------------------------------------------------------------


def _fmt_date(v: Any) -> str:
    """Convert Firestore Timestamp, datetime, or ISO string to YYYY-MM-DD string."""
    if v is None:
        return ""
    if hasattr(v, "date"):  # datetime or Firestore DatetimeWithNanoseconds
        try:
            return v.date().isoformat()
        except Exception:
            pass
    s = str(v)
    return s[:10] if len(s) >= 10 else s


def _derive_eligibility(data: dict[str, Any]) -> tuple[bool, bool]:
    """Return swing/intraday eligibility — uses real fields if present, falls back to allowed_product."""
    if "eligible_swing" in data or "eligible_intraday" in data:
        return bool(data.get("eligible_swing")), bool(data.get("eligible_intraday"))
    ap = str(data.get("allowed_product", "BOTH")).upper()
    swing = ap in ("BOTH", "SWING")
    intraday = ap in ("BOTH", "INTRADAY")
    return swing, intraday


@router.get("/universe/stats")
def get_universe_stats(
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Eligible counts and score distribution."""
    c = get_container()
    try:
        rows = c.state.list_universe(limit=3000)
        total = len(rows)
        swing = 0
        intraday = 0
        both = 0
        for data in rows:
            s, i = _derive_eligibility(data)
            if s:
                swing += 1
            if i:
                intraday += 1
            if s and i:
                both += 1
        return {
            "total_symbols": total,
            "eligible_swing": swing,
            "eligible_intraday": intraday,
            "neither": total - swing - intraday + both,
        }
    except Exception as exc:
        logger.error("universe/stats failed: %s", exc)
        return {"total_symbols": 0, "error": str(exc)}


@router.get("/universe/list")
def get_universe_list(
    sector: str | None = Query(default=None),
    eligible: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Full universe list with filters."""
    c = get_container()
    try:
        rows = c.state.list_universe(limit=3000)
        symbols = []
        for data in rows:
            s, i = _derive_eligibility(data)
            symbols.append({
                "symbol": data.get("symbol", data.get("_id", "")),
                "exchange": data.get("exchange", "NSE"),
                "sector": data.get("sector", ""),
                "beta": data.get("beta", 0),
                "eligible_swing": s,
                "eligible_intraday": i,
                "universe_score": data.get("universe_score"),       # 0-100 computed indicator score
                "score_calc": data.get("score_calc", ""),            # E|P|R|M|B|V|O|N|S breakdown string
                "priority": data.get("priority"),                    # manual priority (legacy field)
                # Tradability metrics
                "price_last": data.get("price_last"),
                "atr_pct_14d": data.get("atr_pct_14d"),
                "atr_14": data.get("atr_14"),
                "turnover_med_60d": data.get("turnover_med_60d"),
                "turnover_rank_60d": data.get("turnover_rank_60d"),
                "liquidity_bucket": data.get("liquidity_bucket", ""),
                "gap_risk_60d": data.get("gap_risk_60d"),
                "bars_1d": data.get("bars_1d"),
                "last_1d_date": _fmt_date(data.get("last_1d_date")),
                # Data quality
                "data_quality_flag": data.get("data_quality_flag", ""),
                "stale_days": data.get("stale_days"),
                "disable_reason": data.get("disable_reason", ""),
                # Config
                "allowed_product": data.get("allowed_product", ""),
                "strategy_pref": data.get("strategy_pref", ""),
                "enabled": data.get("enabled", True),
            })

        # Apply filters
        if sector:
            symbols = [s for s in symbols if s["sector"] == sector]
        if eligible == "swing":
            symbols = [s for s in symbols if s["eligible_swing"]]
        elif eligible == "intraday":
            symbols = [s for s in symbols if s["eligible_intraday"]]
        elif eligible == "both":
            symbols = [s for s in symbols if s["eligible_swing"] and s["eligible_intraday"]]

        total = len(symbols)
        symbols = symbols[offset : offset + limit]
        return {"symbols": symbols, "total": total, "limit": limit, "offset": offset}
    except Exception as exc:
        logger.error("universe/list failed: %s", exc)
        return {"symbols": [], "error": str(exc)}


# ---------------------------------------------------------------------------
# Sectors
# ---------------------------------------------------------------------------


@router.get("/sectors/summary")
def get_sectors_summary(
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Per-sector aggregated metrics derived from universe + sector_mapping."""
    c = get_container()
    try:
        uni_rows = c.state.list_universe(limit=3000)

        # Build sector_mapping lookup: symbol → {macro_sector, industry, basic_industry}
        try:
            sm_rows = c.state.list_sector_mapping(limit=3000)
        except Exception:
            sm_rows = []
        sm_lookup: dict[str, dict[str, Any]] = {}
        for r in sm_rows:
            sym = r.get("symbol") or r.get("_id") or ""
            if sym:
                sm_lookup[sym] = r

        # Aggregate
        sectors: dict[str, Any] = {}
        for data in uni_rows:
            sym = data.get("symbol") or data.get("_id") or ""
            sector = data.get("sector") or "Unknown"
            sm = sm_lookup.get(sym, {})
            macro = sm.get("macro_sector") or data.get("macro_sector") or "Other"
            industry = sm.get("industry") or ""

            if sector not in sectors:
                sectors[sector] = {
                    "sector": sector,
                    "macro_sector": macro,
                    "total": 0,
                    "eligible_swing": 0,
                    "eligible_intraday": 0,
                    "both": 0,
                    "beta_sum": 0.0,
                    "beta_count": 0,
                    "atr_sum": 0.0,
                    "atr_count": 0,
                    "turnover_sum": 0.0,
                    "bucket_a": 0,
                    "bucket_b": 0,
                    "bucket_c": 0,
                    "bucket_d": 0,
                    "gap_risk_sum": 0.0,
                    "gap_risk_count": 0,
                    "dq_good": 0,
                    "dq_stale": 0,
                    "dq_missing": 0,
                    "dq_other": 0,
                    "industries": set(),
                }

            sd = sectors[sector]
            sd["total"] += 1
            sw, intra = _derive_eligibility(data)
            if sw:
                sd["eligible_swing"] += 1
            if intra:
                sd["eligible_intraday"] += 1
            if sw and intra:
                sd["both"] += 1

            if data.get("beta"):
                sd["beta_sum"] += float(data["beta"])
                sd["beta_count"] += 1
            if data.get("atr_pct_14d"):
                sd["atr_sum"] += float(data["atr_pct_14d"])
                sd["atr_count"] += 1
            if data.get("turnover_med_60d"):
                sd["turnover_sum"] += float(data["turnover_med_60d"])

            bucket = data.get("liquidity_bucket") or ""
            if bucket == "A":
                sd["bucket_a"] += 1
            elif bucket == "B":
                sd["bucket_b"] += 1
            elif bucket == "C":
                sd["bucket_c"] += 1
            elif bucket == "D":
                sd["bucket_d"] += 1

            if data.get("gap_risk_60d"):
                sd["gap_risk_sum"] += float(data["gap_risk_60d"])
                sd["gap_risk_count"] += 1

            dq = data.get("data_quality_flag") or ""
            if dq == "GOOD":
                sd["dq_good"] += 1
            elif dq == "STALE":
                sd["dq_stale"] += 1
            elif dq == "MISSING":
                sd["dq_missing"] += 1
            else:
                sd["dq_other"] += 1

            if industry:
                sd["industries"].add(industry)

        result = []
        for sd in sectors.values():
            total = sd["total"] or 1
            eligible_any = max(sd["eligible_swing"], sd["eligible_intraday"])
            liq_weighted = (
                sd["bucket_a"] * 4 + sd["bucket_b"] * 3 + sd["bucket_c"] * 2 + sd["bucket_d"]
            )
            liq_with_data = sd["bucket_a"] + sd["bucket_b"] + sd["bucket_c"] + sd["bucket_d"]
            result.append(
                {
                    "sector": sd["sector"],
                    "macro_sector": sd["macro_sector"],
                    "total": sd["total"],
                    "eligible_swing": sd["eligible_swing"],
                    "eligible_intraday": sd["eligible_intraday"],
                    "both": sd["both"],
                    "neither": sd["total"] - sd["eligible_swing"] - sd["eligible_intraday"] + sd["both"],
                    "eligible_pct": round(eligible_any / total * 100, 1),
                    "avg_beta": round(sd["beta_sum"] / sd["beta_count"], 2) if sd["beta_count"] else None,
                    "avg_atr_pct": round(sd["atr_sum"] / sd["atr_count"], 4) if sd["atr_count"] else None,
                    "total_turnover": sd["turnover_sum"],
                    "bucket_a": sd["bucket_a"],
                    "bucket_b": sd["bucket_b"],
                    "bucket_c": sd["bucket_c"],
                    "bucket_d": sd["bucket_d"],
                    "liq_score": round(liq_weighted / liq_with_data, 2) if liq_with_data else 0.0,
                    "avg_gap_risk": round(sd["gap_risk_sum"] / sd["gap_risk_count"], 4) if sd["gap_risk_count"] else None,
                    "dq_good": sd["dq_good"],
                    "dq_stale": sd["dq_stale"],
                    "dq_missing": sd["dq_missing"],
                    "dq_other": sd["dq_other"],
                    "dq_score": round(sd["dq_good"] / total * 100, 1),
                    "industries": sorted(sd["industries"]),
                }
            )

        result.sort(key=lambda x: x["total_turnover"], reverse=True)
        macro_sectors = sorted({r["macro_sector"] for r in result})
        return {"sectors": result, "total_sectors": len(result), "macro_sectors": macro_sectors}
    except Exception as exc:
        logger.error("sectors/summary failed: %s", exc)
        return {"sectors": [], "error": str(exc)}


@router.get("/sectors/detail/{sector}")
def get_sector_detail(
    sector: str,
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Top symbols and industry breakdown for a specific sector."""
    c = get_container()
    try:
        uni_rows = c.state.list_universe(limit=3000)
        try:
            sm_rows = c.state.list_sector_mapping(limit=3000)
        except Exception:
            sm_rows = []
        sm_lookup: dict[str, dict[str, Any]] = {
            (r.get("symbol") or r.get("_id") or ""): r for r in sm_rows
        }

        symbols = []
        for data in uni_rows:
            if (data.get("sector") or "Unknown") != sector:
                continue
            sym = data.get("symbol") or data.get("_id") or ""
            sm = sm_lookup.get(sym, {})
            sw, intra = _derive_eligibility(data)
            symbols.append(
                {
                    "symbol": sym,
                    "industry": sm.get("industry") or "",
                    "basic_industry": sm.get("basic_industry") or "",
                    "eligible_swing": sw,
                    "eligible_intraday": intra,
                    "price_last": data.get("price_last"),
                    "turnover_med_60d": data.get("turnover_med_60d"),
                    "atr_pct_14d": data.get("atr_pct_14d"),
                    "liquidity_bucket": data.get("liquidity_bucket") or "",
                    "beta": data.get("beta"),
                    "gap_risk_60d": data.get("gap_risk_60d"),
                    "data_quality_flag": data.get("data_quality_flag") or "",
                    "disable_reason": data.get("disable_reason") or "",
                }
            )

        symbols.sort(key=lambda x: x.get("turnover_med_60d") or 0, reverse=True)

        industries: dict[str, int] = {}
        for s in symbols:
            ind = s["industry"] or "Unknown"
            industries[ind] = industries.get(ind, 0) + 1

        return {
            "sector": sector,
            "total": len(symbols),
            "symbols": symbols,
            "industries": sorted(industries.items(), key=lambda x: -x[1]),
        }
    except Exception as exc:
        logger.error("sectors/detail failed: %s", exc)
        return {"sector": sector, "symbols": [], "error": str(exc)}


# ---------------------------------------------------------------------------
# History / Data Freshness
# ---------------------------------------------------------------------------


def _normalize_status_1d(flag: str) -> str:
    f = (flag or "").upper()
    if f in ("GOOD", "FRESH"): return "FRESH"  # "FRESH" was written by an earlier bug; treat as alias
    if f == "STALE": return "STALE"
    if f == "MISSING": return "MISSING"
    if "INVALID" in f: return "INVALID"
    return "OTHER"


def _normalize_status_5m(s: str) -> str:
    s = (s or "").upper()
    if s == "FRESH_READY": return "FRESH"
    if "STALE" in s: return "STALE"
    if "MISSING" in s: return "MISSING"
    if "INVALID" in s: return "INVALID"
    if not s: return "NO_DATA"
    return "OTHER"


@router.get("/history/summary")
def get_history_summary(user: dict[str, Any] = Depends(verify_firebase_token)) -> dict[str, Any]:
    c = get_container()
    try:
        rows = c.state.list_universe(limit=3000)
        total = len(rows)
        d1: dict[str, int] = {"FRESH": 0, "STALE": 0, "MISSING": 0, "INVALID": 0, "OTHER": 0}
        d5: dict[str, int] = {"FRESH": 0, "STALE": 0, "MISSING": 0, "INVALID": 0, "NO_DATA": 0, "OTHER": 0}
        last_1d_dates: list[str] = []
        last_5m_dates: list[str] = []

        for row in rows:
            # normalize returns uppercase ("FRESH", "STALE"…); store as lowercase to match frontend keys
            s1d = _normalize_status_1d(row.get("data_quality_flag", ""))  # returns uppercase
            d1[s1d] = d1.get(s1d, 0) + 1
            s5m = _normalize_status_5m(row.get("status_5m", ""))  # returns uppercase
            d5[s5m] = d5.get(s5m, 0) + 1
            d = _fmt_date(row.get("last_1d_date"))
            if d: last_1d_dates.append(d)
            d5m = str(row.get("last_5m_date") or "")
            if d5m: last_5m_dates.append(d5m)

        # Use system-computed expected LCD (holiday-aware) — don't derive from stale Firestore dates
        try:
            expected_lcd = str(c.universe_service()._expected_lcd_context().get("expectedLCD") or "")
        except Exception:
            expected_lcd = max(last_1d_dates) if last_1d_dates else ""
        last_5m_run = max(last_5m_dates) if last_5m_dates else ""
        return {
            "total": total,
            "expected_lcd": expected_lcd,
            "last_5m_run": last_5m_run,
            "status_1d": d1,
            "status_5m": d5,
            "fresh_pct_1d": round(d1["FRESH"] / total * 100, 1) if total else 0,
            "fresh_pct_5m": round(d5["FRESH"] / total * 100, 1) if total else 0,
            "issues_1d": d1["STALE"] + d1["MISSING"] + d1["INVALID"],
            "issues_5m": d5["STALE"] + d5["MISSING"] + d5["INVALID"] + d5.get("NO_DATA", 0),
        }
    except Exception as exc:
        logger.error("history/summary failed: %s", exc)
        return {"total": 0, "error": str(exc)}


@router.get("/history/symbols")
def get_history_symbols(
    status_1d: str = Query(default=""),
    status_5m: str = Query(default=""),
    search: str = Query(default=""),
    limit: int = Query(default=3000, ge=1, le=5000),
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    c = get_container()
    try:
        rows = c.state.list_universe(limit=3000)
        result = []
        filter_1d = status_1d.upper() if status_1d else ""
        filter_5m = status_5m.upper() if status_5m else ""
        search_q = search.lower()

        for row in rows:
            sym = row.get("symbol") or row.get("_id") or ""
            if not sym:
                continue
            s1d = _normalize_status_1d(row.get("data_quality_flag", ""))
            s5m = _normalize_status_5m(row.get("status_5m", ""))
            if filter_1d and s1d != filter_1d:
                continue
            if filter_5m and s5m != filter_5m:
                continue
            if search_q and search_q not in sym.lower():
                continue
            # Coerce stale_days to int — older Firestore docs occasionally
            # hold it as a string, which breaks the sort below with
            # "bad operand type for unary -: 'str'" and causes the whole
            # symbols list to disappear on the Data History page.
            try:
                stale_days_val = int(row.get("stale_days") or 0)
            except (TypeError, ValueError):
                stale_days_val = 0
            result.append({
                "symbol": sym,
                "exchange": row.get("exchange", "NSE"),
                "sector": row.get("sector", ""),
                "last_1d_date": _fmt_date(row.get("last_1d_date")),
                "bars_1d": row.get("bars_1d"),
                "status_1d": s1d,
                "stale_days": stale_days_val,
                "last_5m_date": str(row.get("last_5m_date") or ""),
                "bars_5m": row.get("bars_5m"),
                "status_5m": s5m,
            })

        _order = {"MISSING": 0, "INVALID": 1, "NO_DATA": 2, "STALE": 3, "OTHER": 4, "FRESH": 5}
        result.sort(key=lambda r: (
            _order.get(r["status_1d"], 4),
            -int(r.get("stale_days") or 0),
        ))
        return {"symbols": result[:limit], "total": len(result)}
    except Exception as exc:
        logger.error("history/symbols failed: %s", exc)
        return {"symbols": [], "total": 0, "error": str(exc)}


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


def _parse_action_log(text: str) -> dict[str, str] | None:
    """Parse a log_sink action log line into structured fields.

    Input format (from logger.info):
      ...log_sink action module=X action=Y status=Z message=... ctx={...} execId=ABC
    """
    marker = "action module="
    idx = text.find(marker)
    if idx < 0:
        return None
    tail = text[idx + len("action module="):]

    # Split off ctx= and execId= from the right (ctx is always a JSON object)
    idx_ctx = tail.rfind(" ctx=")
    idx_exec = tail.rfind(" execId=")
    if idx_ctx < 0 or idx_exec < 0:
        return None

    header = tail[:idx_ctx]
    ctx_str = tail[idx_ctx + len(" ctx="):idx_exec].strip()
    exec_id = tail[idx_exec + len(" execId="):].strip()

    # header looks like: "Universe action=score_refresh status=DONE message=some text here"
    parts = header.split()
    module = parts[0] if parts else ""
    action = status = ""
    for part in parts[1:]:
        if part.startswith("action="):
            action = part[7:]
        elif part.startswith("status="):
            status = part[7:]

    # message= is everything after "status=XXX message=" until end of header
    msg_prefix = f"status={status} message="
    msg_pos = header.find(msg_prefix)
    message = header[msg_pos + len(msg_prefix):].strip() if msg_pos >= 0 else ""

    # Extract schedulerJob from ctx JSON so the frontend can match specific scheduled runs
    scheduler_job = ""
    try:
        import json as _json
        ctx_obj = _json.loads(ctx_str)
        scheduler_job = ctx_obj.get("schedulerJob", "")
    except Exception:
        pass

    # Normalize log_sink status values to what the frontend expects
    status_map = {"DONE": "success", "START": "running", "ERROR": "error", "SKIP": "skipped", "LOCK_BUSY": "skipped"}
    status_norm = status_map.get(status.upper(), status.lower())

    return {
        "module": module,
        "action": action,
        "status": status_norm,
        "message": message,
        "exec_id": exec_id,
        "scheduler_job": scheduler_job,
    }


@router.get("/pipeline/status")
def get_pipeline_status(
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Recent pipeline audit log entries read from Cloud Logging."""
    c = get_container()
    today = now_ist().strftime("%Y-%m-%d")
    try:
        import google.auth
        import google.auth.transport.requests as ga_requests

        # On Cloud Run, default() returns Compute credentials with cloud-platform scope
        # which already covers logging.logEntries.list — don't restrict with custom scopes
        creds, _ = google.auth.default()
        creds.refresh(ga_requests.Request())

        filter_str = (
            'resource.type="cloud_run_revision" '
            'resource.labels.service_name="autotrader" '
            'textPayload=~"log_sink action module=" '
            f'timestamp>="{today}T00:00:00+05:30"'
        )
        resp = httpx.post(
            "https://logging.googleapis.com/v2/entries:list",
            headers={"Authorization": f"Bearer {creds.token}"},
            json={
                "resourceNames": [f"projects/{c.settings.gcp.project_id}"],
                "filter": filter_str,
                "orderBy": "timestamp desc",
                "pageSize": 100,
            },
            timeout=15,
        )
        resp.raise_for_status()

        entries = []
        for entry in resp.json().get("entries", []):
            tp = entry.get("textPayload", "")
            parsed = _parse_action_log(tp)
            if parsed:
                # Use Cloud Logging timestamp (UTC ISO) — browser's new Date() handles it fine
                parsed["log_ts"] = entry.get("timestamp", "")
                entries.append(parsed)

        return {"date": today, "entries": entries}
    except Exception as exc:
        logger.error("pipeline/status query failed: %s", exc)
        return {"date": today, "entries": [], "error": str(exc)}


# ---------------------------------------------------------------------------
# Symbol detail (aggregated single-call endpoint)
# ---------------------------------------------------------------------------


@router.get("/symbol/{symbol}")
def get_symbol_detail(
    symbol: str,
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Aggregated symbol detail: universe + watchlist + position + signals + recent trades."""
    c = get_container()
    sym = symbol.upper().strip()
    today = now_ist().strftime("%Y-%m-%d")
    from_d = (date.fromisoformat(today) - timedelta(days=365)).isoformat()

    # 1. Universe row from Firestore
    universe_row: dict[str, Any] = {}
    try:
        row = c.state.get_universe_row(sym)
        if row:
            universe_row = {k: v for k, v in row.items() if not hasattr(v, "read")}
    except Exception:
        logger.warning("symbol_detail universe read failed sym=%s", sym, exc_info=True)

    # 2. Watchlist entry (scan latest watchlist doc)
    watchlist_entry: dict[str, Any] | None = None
    try:
        wl = c.state.get_watchlist()
        if wl:
            rows = wl.get("rows", [])
            for r in rows:
                if str(r.get("symbol", "")).upper() == sym:
                    watchlist_entry = {k: v for k, v in r.items() if not hasattr(v, "read")}
                    break
    except Exception:
        logger.warning("symbol_detail watchlist read failed sym=%s", sym, exc_info=True)

    # 3. Open position (filter in memory)
    position: dict[str, Any] | None = None
    try:
        positions = c.state.list_open_positions()
        for p in positions:
            if str(p.get("symbol", "")).upper() == sym:
                position = {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in p.items()}
                break
    except Exception:
        logger.warning("symbol_detail position read failed sym=%s", sym, exc_info=True)

    # 4. Today's signals for this symbol from BQ
    signals_today: list[dict] = []
    try:
        q = f"""
            SELECT *
            FROM `{c.settings.gcp.project_id}.{c.settings.gcp.bq_dataset}.signals`
            WHERE run_date = '{today}' AND UPPER(symbol) = '{sym}'
            ORDER BY scan_ts DESC
            LIMIT 20
        """
        rows = c.bq.query(q)
        for r in rows:
            for k, v in r.items():
                if hasattr(v, "isoformat"):
                    r[k] = v.isoformat()
        signals_today = rows
    except Exception:
        logger.warning("symbol_detail signals query failed sym=%s", sym, exc_info=True)

    # 5. Recent trades for this symbol from BQ (last 12 months)
    recent_trades: list[dict] = []
    try:
        q = f"""
            SELECT * REPLACE(
                IFNULL(net_pnl, pnl) AS pnl,              -- canonical `pnl` is NET of costs
                IFNULL(net_pnl_pct, pnl_pct) AS pnl_pct
            ), pnl AS gross_pnl, pnl_pct AS gross_pnl_pct
            FROM `{c.settings.gcp.project_id}.{c.settings.gcp.bq_dataset}.trades`
            WHERE UPPER(symbol) = '{sym}' AND trade_date BETWEEN '{from_d}' AND '{today}'
            AND {_BQ_VALID_TRADE}
            ORDER BY trade_date DESC, entry_ts DESC
            LIMIT 50
        """
        rows = c.bq.query(q)
        for r in rows:
            for k, v in r.items():
                if hasattr(v, "isoformat"):
                    r[k] = v.isoformat()
        recent_trades = rows
    except Exception:
        logger.warning("symbol_detail trades query failed sym=%s", sym, exc_info=True)

    return {
        "symbol": sym,
        "universe": universe_row,
        "watchlist": watchlist_entry,
        "position": position,
        "signals_today": signals_today,
        "recent_trades": recent_trades,
    }


# ---------------------------------------------------------------------------
# Market data
# ---------------------------------------------------------------------------


def _gcs_candles_1d(c: Any, symbol: str, from_d: str, today: str) -> list[dict]:
    """Read daily candles from the GCS score cache — the PRIMARY 1d source (2026-08-07).

    This is the canonical store the live system reads at scan time. It replaced BQ
    `candles_1d` as primary because that table went cold ~2026-04 (see the retirement note
    on `get_candles`), which made 1d charts silently truncate.

    **Path order mirrors `universe_service._read_score_cache_with_migration`: the
    INSTRUMENT-KEY path first, then the legacy by-symbol path.** That order matters — the
    live job migrated to instrument-key keying, so the legacy `cache/score_1d/<EX>/<SEG>/
    <SYM>.json` files are frozen (verified 2026-08-07: SBIN legacy last bar 2026-02-26 vs
    2026-08-06 under `score_1d_by_instrument/`). Reading legacy first would serve charts
    ~5 months stale.

    Raw format per candle: [iso_ts, open, high, low, close, volume]
    """
    sym = symbol.upper()
    candidate_paths: list[str] = []
    # Preferred: instrument-key path (what the live pipeline writes). Resolve the key from
    # the universe row; best-effort — a lookup failure just falls through to legacy.
    try:
        row = c.state.get_universe_row(sym) or {}
        ik = str(row.get("instrument_key") or "").strip()
        exch = str(row.get("exchange") or "NSE").strip().upper() or "NSE"
        seg = str(row.get("segment") or "CASH").strip().upper() or "CASH"
        if ik:
            candidate_paths.append(c.gcs.score_cache_1d_path_by_instrument_key(ik, exch, seg))
    except Exception:
        logger.info("candles_1d ik-path lookup failed symbol=%s — using legacy", sym, exc_info=True)
    candidate_paths += [                       # legacy by-symbol (frozen; last-resort)
        f"cache/score_1d/NSE/CASH/{sym}.json",
        f"cache/score_1d/NSE/EQ/{sym}.json",
        f"cache/score_1d/BSE/CASH/{sym}.json",
    ]
    raw: list[list] = []
    for path in candidate_paths:
        try:
            data = c.gcs.read_candles(path)
            if data:
                raw = data
                break
        except Exception:
            continue

    if not raw:
        return []

    rows: list[dict] = []
    for candle in raw:
        try:
            ts_str = str(candle[0])
            trade_date = ts_str[:10]  # "YYYY-MM-DD"
            if trade_date < from_d or trade_date > today:
                continue
            rows.append({
                "time": trade_date,
                "open": float(candle[1]),
                "high": float(candle[2]),
                "low": float(candle[3]),
                "close": float(candle[4]),
                "volume": float(candle[5]) if len(candle) > 5 else 0.0,
            })
        except Exception:
            continue
    return rows


@router.get("/candles/{symbol}")
def get_candles(
    symbol: str,
    interval: str = Query(default="1d"),
    days: int = Query(default=90, ge=1, le=365),
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Candle data for charting. 1d reads GCS score cache (primary); 5m reads BQ candles_5m.

    **BQ `candles_1d` is RETIRED as a live source (2026-08-07).** It was only ever written
    as a side effect of an Upstox fetch inside `prefetch_score_cache_batch`, so once the GCS
    score cache went warm (~2026-04) the writes stopped (~1-2 stray symbols/day; liquid names
    like SBIN/RELIANCE/LUPIN absent entirely). The previous BQ-primary order carried a latent
    bug: the GCS fallback fired only on `not rows`, but the table still holds 1.19M historical
    rows, so a 1d chart got a NON-empty yet truncated series ending ~April and silently never
    fell back. GCS score_1d is the canonical full+fresh store the live system itself reads, so
    it is now primary; BQ is kept only as a last-resort net if GCS is unreachable. The table
    and its rows are retained (nothing deleted) — see `backfill_candles_1d_to_bq` to refill
    on demand if a future consumer needs SQL-side daily bars.
    """
    c = get_container()
    is_daily = interval in ("1d", "day", "daily")
    today = now_ist().strftime("%Y-%m-%d")
    from_d = (date.fromisoformat(today) - timedelta(days=days)).isoformat()

    if is_daily:
        try:
            rows = sorted(_gcs_candles_1d(c, symbol, from_d, today), key=lambda r: str(r["time"]))
            if rows:
                return {"symbol": symbol, "interval": interval, "candles": rows}
            logger.info("candles_gcs_empty symbol=%s — trying retired BQ candles_1d", symbol)
        except Exception as exc:
            logger.error("candles gcs read failed symbol=%s: %s", symbol, exc)
        # Last-resort net only (retired table — may be stale/truncated for recent dates)
        q = f"""
            SELECT trade_date as time, open, high, low, close, volume
            FROM `{c.settings.gcp.project_id}.{c.settings.gcp.bq_dataset}.candles_1d`
            WHERE symbol = '{symbol}' AND trade_date BETWEEN '{from_d}' AND '{today}'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY trade_date ORDER BY trade_date) = 1
            ORDER BY trade_date
        """
    else:
        q = f"""
            SELECT candle_ts as time, open, high, low, close, volume
            FROM `{c.settings.gcp.project_id}.{c.settings.gcp.bq_dataset}.candles_5m`
            WHERE symbol = '{symbol}' AND trade_date BETWEEN '{from_d}' AND '{today}'
            ORDER BY candle_ts
        """

    try:
        rows = c.bq.query(q)
        for r in rows:
            for k, v in r.items():
                if hasattr(v, "isoformat"):
                    r[k] = v.isoformat()
        return {"symbol": symbol, "interval": interval, "candles": rows}
    except Exception as exc:
        logger.error("candles query failed: %s", exc)
        return {"symbol": symbol, "candles": [], "error": str(exc)}


@router.get("/ltp")
def get_ltp(
    symbols: str = Query(description="Comma-separated symbols"),
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Batch LTP from Upstox with 5-second cache."""
    c = get_container()
    sym_list = [s.strip() for s in symbols.split(",") if s.strip()]
    if not sym_list:
        return {"prices": {}}

    try:
        # Look up instrument keys from Firestore universe
        prices: dict[str, float] = {}
        instrument_keys = []
        sym_to_key: dict[str, str] = {}

        for sym in sym_list:
            doc = c.state.get_json("universe", sym)
            if doc and doc.get("instrument_key"):
                key = doc["instrument_key"]
                instrument_keys.append(key)
                sym_to_key[key] = sym

        if instrument_keys:
            quotes = c.upstox.get_ltp_v3(instrument_keys)
            for key, quote in quotes.items():
                sym = sym_to_key.get(key, key)
                prices[sym] = quote.ltp

        return {"prices": prices}
    except Exception as exc:
        logger.error("ltp fetch failed: %s", exc)
        return {"prices": {}, "error": str(exc)}


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@router.get("/health/upstox")
def get_health_upstox(
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Upstox integration health status."""
    c = get_container()
    try:
        token_expiry = c.secrets.get_secret(
            c.settings.upstox.access_token_expiry_secret_name
        )
        return {
            "token_expires_at": token_expiry,
            "token_valid": bool(token_expiry),
        }
    except Exception as exc:
        logger.error("health/upstox failed: %s", exc)
        return {"token_valid": False, "error": str(exc)}


# ---------------------------------------------------------------------------
# Config (admin only)
# ---------------------------------------------------------------------------


@router.post("/config/update")
def post_config_update(
    payload: dict[str, Any],
    admin: dict[str, Any] = Depends(_require_admin),
) -> dict[str, Any]:
    """Update Firestore config values. Admin only."""
    c = get_container()
    key = payload.get("key")
    value = payload.get("value")
    if not key:
        raise HTTPException(status_code=400, detail="Missing 'key' field")
    try:
        c.state.set_json("config", key, {"key": key, "value": str(value)})
        return {"status": "ok", "key": key, "value": str(value)}
    except Exception as exc:
        logger.error("config/update failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Admin actions
# ---------------------------------------------------------------------------


@router.post("/admin/exit-position")
def post_admin_exit_position(
    payload: dict[str, Any],
    admin: dict[str, Any] = Depends(_require_admin),
) -> dict[str, Any]:
    """Force-exit an open position. Admin only."""
    position_tag = payload.get("position_tag")
    if not position_tag:
        raise HTTPException(status_code=400, detail="Missing 'position_tag'")

    c = get_container()
    try:
        # Look up instrument key from position doc
        pos = c.state.get_position(position_tag)
        if not pos:
            raise HTTPException(status_code=404, detail="Position not found")

        symbol = str(pos.get("symbol") or "")
        # Get instrument key from universe
        uni_doc = c.state.get_json("universe", symbol)
        instrument_key = (uni_doc or {}).get("instrument_key", "")
        if not instrument_key:
            raise HTTPException(status_code=400, detail=f"No instrument_key for {symbol}")

        result = c.order_service().place_exit_order(
            position_tag=position_tag,
            instrument_key=instrument_key,
            exit_reason="MANUAL_EXIT",
        )
        logger.info("admin exit_position tag=%s by=%s result=%s", position_tag, admin["email"], result)
        return {"status": "ok", **result}
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("admin/exit-position failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/admin/toggle-paper-mode")
def post_admin_toggle_paper_mode(
    payload: dict[str, Any],
    admin: dict[str, Any] = Depends(_require_admin),
) -> dict[str, Any]:
    """Toggle paper/live trading mode. Admin only."""
    enabled = payload.get("paper_trade")
    if enabled is None:
        raise HTTPException(status_code=400, detail="Missing 'paper_trade' (bool)")

    c = get_container()
    try:
        c.state.set_json("config", "runtime", {"paper_trade": bool(enabled)})
        c.settings.runtime.paper_trade = bool(enabled)
        logger.info("paper_mode toggled to %s by %s", enabled, admin["email"])
        return {"status": "ok", "paper_trade": bool(enabled)}
    except Exception as exc:
        logger.error("toggle-paper-mode failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/config/paper-mode")
def get_paper_mode(
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Get current paper/live mode status."""
    c = get_container()
    return {"paper_trade": c.settings.runtime.paper_trade}


@router.get("/config/settings")
def get_settings(
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Return all live strategy + runtime settings for the Settings page."""
    c = get_container()
    s = c.settings.strategy
    r = c.settings.runtime
    return {
        "capital": s.capital,
        "risk_per_trade": s.risk_per_trade,
        "max_daily_loss": s.max_daily_loss,
        "daily_profit_target": s.daily_profit_target,
        "max_trades_day": s.max_trades_day,
        "max_positions": s.max_positions,
        "min_signal_score": s.min_signal_score,
        "atr_sl_mult": s.atr_sl_mult,
        "rr_intraday": s.rr_intraday,
        "ema_fast": s.ema_fast,
        "ema_med": s.ema_med,
        "ema_slow": s.ema_slow,
        "rsi_period": s.rsi_period,
        "rsi_buy_min": s.rsi_buy_min,
        "rsi_buy_max": s.rsi_buy_max,
        "rsi_sell_min": s.rsi_sell_min,
        "rsi_sell_max": s.rsi_sell_max,
        "vol_mult": s.vol_mult,
        "vix_safe_max": s.vix_safe_max,
        "pcr_bull_min": s.pcr_bull_min,
        "pcr_bear_max": s.pcr_bear_max,
        "paper_trade": r.paper_trade,
    }


_JOB_TOKEN = os.environ.get("JOB_TRIGGER_TOKEN", "")

# Maps dashboard job names → internal job endpoint path + query params
_JOB_ROUTES: dict[str, str] = {
    "token_refresh":       "/jobs/upstox-token-request",
    "universe_refresh":    "/jobs/universe-v2-refresh?replace=false&build_limit=0&candle_api_cap=1800&run_full_backfill=true&write_v2_eligibility=false&run_intraday_appended_backfill=true&intraday_api_cap=1800&intraday_lookback_trading_days=60",
    "candle_cache":        "/jobs/score-cache-update-close?api_cap=1800&lookback_days=700&min_bars=320&retry_stale_terminal_today=true&run_intraday_update=true&intraday_api_cap=1800&intraday_lookback_trading_days=60",
    "candle_finalize":     "/jobs/score-cache-update-close?api_cap=600&lookback_days=700&min_bars=320&retry_stale_terminal_today=false&run_intraday_update=true&intraday_api_cap=600&intraday_lookback_trading_days=60",
    "score_refresh":       "/jobs/score-refresh?api_cap=0&cache_only=true&require_fresh_cache=true&fresh_hours=0",
    "premarket_watchlist": "/jobs/watchlist-refresh?target_size=150&premarket=true&intraday_timeframe=5m",
    "watchlist_5m":        "/jobs/watchlist-refresh?target_size=150&premarket=false&intraday_timeframe=5m",
    "scanner":             "/jobs/scan-once?force=true&allow_live_orders=false",
    "eod_recon":           "/jobs/eod-position-reconcile",
    "swing_recon":         "/jobs/swing-reconcile",
}


def _fire_job_async(path: str) -> None:
    """Call internal job endpoint in a background thread (fire-and-forget)."""
    base = os.environ.get("INTERNAL_SERVICE_URL", "http://localhost:8080")
    url = f"{base}{path}"
    try:
        httpx.post(
            url,
            headers={"Content-Type": "application/json", "X-Job-Token": _JOB_TOKEN},
            json={},
            timeout=None,  # jobs run for minutes; don't time out the background thread
        )
    except Exception as exc:
        logger.warning("trigger-job background call failed: %s", exc)


@router.post("/admin/trigger-job")
def post_admin_trigger_job(
    payload: dict[str, Any],
    admin: dict[str, Any] = Depends(_require_admin),
) -> dict[str, Any]:
    """Manually trigger a pipeline job. Admin only."""
    job_name = payload.get("job", "")
    path = _JOB_ROUTES.get(job_name)
    if not path:
        raise HTTPException(status_code=400, detail=f"Unknown job: {job_name!r}. Valid jobs: {list(_JOB_ROUTES)}")
    logger.info("manual trigger: job=%s path=%s by=%s", job_name, path, admin.get("email"))
    threading.Thread(target=_fire_job_async, args=(path,), daemon=True).start()
    return {"status": "triggered", "job": job_name, "path": path}


@router.post("/admin/force-token-refresh")
def post_admin_force_token_refresh(
    admin: dict[str, Any] = Depends(_require_admin),
) -> dict[str, Any]:
    """Force Upstox token refresh. Admin only."""
    c = get_container()
    try:
        result = c.upstox.request_access_token_v3()
        return {"status": "ok", "result": result}
    except Exception as exc:
        logger.error("force-token-refresh failed: %s", exc)
        return {"status": "error", "error": str(exc)}


@router.post("/admin/backfill-candles-1d")
def post_admin_backfill_candles_1d(
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Backfill candles_1d BQ table from GCS score_1d cache. Runs in background."""
    c = get_container()

    def _run() -> None:
        try:
            result = c.universe_service().backfill_candles_1d_to_bq()
            logger.info("admin_backfill_candles_1d_done result=%s", result)
        except Exception:
            logger.error("admin_backfill_candles_1d_failed", exc_info=True)

    threading.Thread(target=_run, daemon=True).start()
    return {"status": "started", "message": "Backfill running in background — check server logs for progress"}
