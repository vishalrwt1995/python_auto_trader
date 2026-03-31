"""Dashboard API router.

Provides all frontend-facing endpoints under /dashboard.
Authentication is performed via Firebase ID tokens (Bearer scheme).
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

import google.auth.transport.requests
import google.oauth2.id_token
from fastapi import APIRouter, Depends, Header, HTTPException, Query

from autotrader.container import get_container
from autotrader.time_utils import now_ist

logger = logging.getLogger(__name__)

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
        claims: dict[str, Any] = google.oauth2.id_token.verify_firebase_token(
            id_token,
            _firebase_request_adapter,
        )
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

    q = f"""
        SELECT
            COUNT(*) as total_trades,
            COUNTIF(pnl > 0) as wins,
            COUNTIF(pnl < 0) as losses,
            COALESCE(SUM(pnl), 0) as total_pnl,
            COALESCE(AVG(CASE WHEN pnl > 0 THEN pnl END), 0) as avg_win,
            COALESCE(AVG(CASE WHEN pnl < 0 THEN pnl END), 0) as avg_loss,
            COALESCE(MAX(pnl), 0) as biggest_win,
            COALESCE(MIN(pnl), 0) as biggest_loss,
            COALESCE(SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END), 0) as gross_profit,
            COALESCE(ABS(SUM(CASE WHEN pnl < 0 THEN pnl ELSE 0 END)), 1) as gross_loss
        FROM `{c.settings.gcp.project_id}.{c.settings.gcp.bq_dataset}.trades`
        WHERE trade_date BETWEEN '{fd}' AND '{td}'
    """
    try:
        rows = list(c.bq.client.query(q).result())
        r = dict(rows[0]) if rows else {}
        total = r.get("total_trades", 0)
        wins = r.get("wins", 0)
        win_rate = (wins / total * 100) if total else 0
        gross_profit = r.get("gross_profit", 0)
        gross_loss = r.get("gross_loss", 1)
        profit_factor = gross_profit / gross_loss if gross_loss else 0
        avg_win = r.get("avg_win", 0)
        avg_loss = abs(r.get("avg_loss", 0))
        expectancy = (win_rate / 100 * avg_win) - ((1 - win_rate / 100) * avg_loss) if total else 0

        return {
            "total_pnl": r.get("total_pnl", 0),
            "total_trades": total,
            "win_rate": round(win_rate, 1),
            "biggest_win": r.get("biggest_win", 0),
            "biggest_loss": r.get("biggest_loss", 0),
            "profit_factor": round(profit_factor, 2),
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
        SELECT trade_date, SUM(pnl) as daily_pnl
        FROM `{c.settings.gcp.project_id}.{c.settings.gcp.bq_dataset}.trades`
        WHERE trade_date BETWEEN '{fd}' AND '{td}'
        GROUP BY trade_date
        ORDER BY trade_date
    """
    try:
        rows = list(c.bq.client.query(q).result())
        cum = 0.0
        series = []
        for r in rows:
            cum += float(r.daily_pnl or 0)
            series.append({"date": str(r.trade_date), "pnl": round(cum, 2)})
        return {"series": series}
    except Exception as exc:
        logger.error("trades/equity-curve query failed: %s", exc)
        return {"series": [], "error": str(exc)}


@router.get("/trades/list")
def get_trades_list(
    from_date: str | None = Query(default=None, alias="from"),
    to_date: str | None = Query(default=None, alias="to"),
    strategy: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Paginated trade list from BQ."""
    c = get_container()
    today = now_ist().strftime("%Y-%m-%d")
    td = to_date or today
    fd = from_date or (date.fromisoformat(td) - timedelta(days=30)).isoformat()

    where = f"trade_date BETWEEN '{fd}' AND '{td}'"
    if strategy:
        where += f" AND strategy = '{strategy}'"

    q = f"""
        SELECT *
        FROM `{c.settings.gcp.project_id}.{c.settings.gcp.bq_dataset}.trades`
        WHERE {where}
        ORDER BY trade_date DESC, entry_ts DESC
        LIMIT {limit} OFFSET {offset}
    """
    try:
        rows = [dict(r) for r in c.bq.client.query(q).result()]
        # Serialize dates
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
        rows = [dict(r) for r in c.bq.client.query(q).result()]
        for r in rows:
            for k, v in r.items():
                if hasattr(v, "isoformat"):
                    r[k] = v.isoformat()
        return {"date": today, "signals": rows}
    except Exception as exc:
        logger.error("signals/today query failed: %s", exc)
        return {"date": today, "signals": [], "error": str(exc)}


# ---------------------------------------------------------------------------
# Universe
# ---------------------------------------------------------------------------


@router.get("/universe/stats")
def get_universe_stats(
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Eligible counts and score distribution."""
    c = get_container()
    try:
        docs = list(c.sheets.db.collection("universe").stream())
        total = len(docs)
        swing = 0
        intraday = 0
        for d in docs:
            data = d.to_dict()
            if data.get("eligible_swing"):
                swing += 1
            if data.get("eligible_intraday"):
                intraday += 1
        return {
            "total_symbols": total,
            "eligible_swing": swing,
            "eligible_intraday": intraday,
            "neither": total - swing - intraday + len([d for d in docs if d.to_dict().get("eligible_swing") and d.to_dict().get("eligible_intraday")]),
        }
    except Exception as exc:
        logger.error("universe/stats failed: %s", exc)
        return {"total_symbols": 0, "error": str(exc)}


@router.get("/universe/list")
def get_universe_list(
    sector: str | None = Query(default=None),
    eligible: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
    offset: int = Query(default=0, ge=0),
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Full universe list with filters. Stub — reads from Firestore."""
    return {"symbols": [], "limit": limit, "offset": offset, "message": "Full implementation pending"}


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


@router.get("/pipeline/status")
def get_pipeline_status(
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Recent audit log entries for pipeline monitoring."""
    c = get_container()
    today = now_ist().strftime("%Y-%m-%d")
    q = f"""
        SELECT log_ts, module, action, status, message, exec_id
        FROM `{c.settings.gcp.project_id}.{c.settings.gcp.bq_dataset}.audit_log`
        WHERE run_date = '{today}'
        ORDER BY log_ts DESC
        LIMIT 50
    """
    try:
        rows = [dict(r) for r in c.bq.client.query(q).result()]
        for r in rows:
            for k, v in r.items():
                if hasattr(v, "isoformat"):
                    r[k] = v.isoformat()
        return {"date": today, "entries": rows}
    except Exception as exc:
        logger.error("pipeline/status query failed: %s", exc)
        return {"date": today, "entries": [], "error": str(exc)}


# ---------------------------------------------------------------------------
# Market data
# ---------------------------------------------------------------------------


@router.get("/candles/{symbol}")
def get_candles(
    symbol: str,
    interval: str = Query(default="1d"),
    days: int = Query(default=90, ge=1, le=365),
    user: dict[str, Any] = Depends(verify_firebase_token),
) -> dict[str, Any]:
    """Candle data from BQ for charting."""
    c = get_container()
    table = "candles_1d" if interval in ("1d", "day", "daily") else "candles_5m"
    today = now_ist().strftime("%Y-%m-%d")
    from_d = (date.fromisoformat(today) - timedelta(days=days)).isoformat()

    if table == "candles_1d":
        q = f"""
            SELECT trade_date as time, open, high, low, close, volume
            FROM `{c.settings.gcp.project_id}.{c.settings.gcp.bq_dataset}.{table}`
            WHERE symbol = '{symbol}' AND trade_date BETWEEN '{from_d}' AND '{today}'
            ORDER BY trade_date
        """
    else:
        q = f"""
            SELECT candle_ts as time, open, high, low, close, volume
            FROM `{c.settings.gcp.project_id}.{c.settings.gcp.bq_dataset}.{table}`
            WHERE symbol = '{symbol}' AND trade_date BETWEEN '{from_d}' AND '{today}'
            ORDER BY candle_ts
        """

    try:
        rows = [dict(r) for r in c.bq.client.query(q).result()]
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

        result = c.order_service.place_exit_order(
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


@router.post("/admin/trigger-job")
def post_admin_trigger_job(
    payload: dict[str, Any],
    admin: dict[str, Any] = Depends(_require_admin),
) -> dict[str, Any]:
    """Manually trigger a pipeline job. Admin only. Stub."""
    job_name = payload.get("job")
    return {"status": "ok", "job": job_name, "message": "Job trigger not yet wired"}


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
