"""One-time migration: Google Sheets → Firestore + BigQuery.

Usage:
    python -m autotrader.scripts.migrate_sheets_to_gcp \\
        --project grow-profit-machine \\
        --spreadsheet-id <ID> \\
        --dry-run          # print what would be written, no actual writes

Run this ONCE before cutting over to Firestore/BigQuery as primary stores.
Safe to re-run (upserts overwrite existing docs).

What gets migrated
──────────────────
Sheet                        → Destination
─────────────────────────────────────────────────────────────────────
⚙️ Config                   → Firestore config/{key}
🧾 Universe Instruments      → Firestore universe/{symbol}
🗂️ Sector Mapping           → Firestore sector_mapping/{symbol}
💼 Positions                 → Firestore positions/{position_tag}
📦 Orders                   → Firestore orders/{ref_id}
🧠 Market Brain              → Firestore market_brain/latest  +  BigQuery market_brain_history
📋 Watchlist / V2 tabs       → Firestore watchlist/latest     +  BigQuery watchlist_history
🎯 Signals                   → BigQuery signals
🧩 Project Log / 📝 Logs    → BigQuery audit_log
💰 P&L Tracker               → BigQuery trades  (closed positions)
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(str(v).strip().replace(",", "")) if v not in (None, "", "N/A", "-") else default
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(str(v).strip().replace(",", ""))) if v not in (None, "", "N/A", "-") else default
    except Exception:
        return default


def _safe_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    return str(v).strip().upper() in {"1", "TRUE", "YES", "Y", "ENABLED"}


def _parse_ts(v: Any) -> str:
    """Return ISO timestamp string or empty string."""
    if not v or str(v).strip() in ("", "N/A", "-"):
        return ""
    return str(v).strip()


def _today_str() -> str:
    return date.today().isoformat()


def _now_str() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _header_map(headers: list[str]) -> dict[str, int]:
    """col-name → 0-based column index (case-insensitive strip)."""
    return {h.strip().lower(): i for i, h in enumerate(headers)}


def _row_dict(row: list[str], hmap: dict[str, int]) -> dict[str, str]:
    out: dict[str, str] = {}
    for name, idx in hmap.items():
        out[name] = row[idx].strip() if idx < len(row) else ""
    return out


# ---------------------------------------------------------------------------
# Per-sheet migrators
# ---------------------------------------------------------------------------

def migrate_config(
    sheets_rows: list[list[str]],
    state: Any,
    dry_run: bool,
) -> int:
    """Config sheet: row[0]=Key, row[1]=Value (starting row 4, skipping header)."""
    count = 0
    for row in sheets_rows:
        if len(row) < 2:
            continue
        key = str(row[0]).strip()
        value = str(row[1]).strip() if len(row) > 1 else ""
        if not key or key.startswith("#"):
            continue
        logger.info("  config %s = %s", key, value)
        if not dry_run:
            state.set_config(key, value)
        count += 1
    return count


def migrate_universe(
    sheets_rows: list[list[str]],
    headers: list[str],
    state: Any,
    dry_run: bool,
) -> int:
    hmap = _header_map(headers)
    count = 0
    for row in sheets_rows:
        r = _row_dict(row, hmap)
        symbol = r.get("symbol", "").upper()
        if not symbol:
            continue
        # Parse raw instrument JSON if available
        raw_json: dict = {}
        raw_csv = r.get("raw csv (json)", "")
        if raw_csv:
            try:
                raw_json = json.loads(raw_csv)
            except Exception:
                pass
        instrument_key = r.get("instrument key", "") or raw_json.get("instrument_key", "")
        payload: dict[str, Any] = {
            # --- Identity ---
            "symbol": symbol,
            "exchange": r.get("exchange", "NSE"),
            "segment": r.get("segment", "CASH"),
            "security_type": r.get("security type", "EQ"),
            "isin": raw_json.get("isin", ""),
            "canonical_id": r.get("canonical id", ""),
            "primary_exchange": r.get("primary exchange", ""),
            "secondary_exchange": r.get("secondary exchange", ""),
            "secondary_instrument_key": r.get("secondary instrument key", ""),
            # --- Trading config ---
            "allowed_product": r.get("allowed product", "BOTH"),
            "strategy_pref": r.get("strategy", "AUTO"),
            "enabled": _safe_bool(r.get("enabled"), default=True),
            "priority": _safe_float(r.get("priority")),
            "notes": r.get("notes", ""),
            "provider": r.get("data provider", "upstox"),
            "instrument_key": instrument_key,
            "source_segment": r.get("source segment", ""),
            # --- Sector ---
            "sector": r.get("sector", ""),
            "sector_source": r.get("sector source", ""),
            "sector_updated_at": _parse_ts(r.get("sector updated at")),
            # --- Risk ---
            "beta": _safe_float(r.get("beta")),
            # --- Tradability metrics ---
            "bars_1d": _safe_int(r.get("bars 1d")),
            "last_1d_date": _parse_ts(r.get("last 1d date")),
            "price_last": _safe_float(r.get("price last")),
            "turnover_med_60d": _safe_float(r.get("turnover med 60d")),
            "atr_14": _safe_float(r.get("atr 14")),
            "atr_pct_14d": _safe_float(r.get("atr pct 14d")),
            "gap_risk_60d": _safe_float(r.get("gap risk 60d")),
            "turnover_rank_60d": _safe_int(r.get("turnover rank 60d")),
            "liquidity_bucket": r.get("liquidity bucket", ""),
            # --- Data quality ---
            "data_quality_flag": r.get("data quality flag", ""),
            "stale_days": _safe_int(r.get("stale days")),
            # --- Eligibility ---
            "eligible_swing": _safe_bool(r.get("eligible swing")),
            "eligible_intraday": _safe_bool(r.get("eligible intraday")),
            "disable_reason": r.get("disable reason", ""),
            "universe_mode": r.get("universe mode", ""),
            "universe_v2_updated_at": _parse_ts(r.get("universe v2 updated at")),
        }
        logger.info("  universe %s", symbol)
        if not dry_run:
            state.save_universe_row(symbol, payload)
        count += 1
    return count


def migrate_sector_mapping(
    sheets_rows: list[list[str]],
    headers: list[str],
    state: Any,
    dry_run: bool,
) -> int:
    hmap = _header_map(headers)
    count = 0
    for row in sheets_rows:
        r = _row_dict(row, hmap)
        symbol = r.get("symbol", "").upper()
        if not symbol:
            continue
        payload: dict[str, Any] = {
            "symbol": symbol,
            "macro_sector": r.get("macrosector", ""),
            "sector": r.get("sector", ""),
            "industry": r.get("industry", ""),
            "basic_industry": r.get("basicindustry", ""),
            "source": r.get("source", "sheets"),
            "updated_at": r.get("updatedat", _now_str()),
        }
        logger.info("  sector_mapping %s", symbol)
        if not dry_run:
            state.save_sector_mapping(symbol, payload)
        count += 1
    return count


def migrate_positions(
    sheets_rows: list[list[str]],
    headers: list[str],
    state: Any,
    dry_run: bool,
) -> int:
    """Positions: tag is symbol+side+entry_ts hash, or use ref_id column if available."""
    hmap = _header_map(headers)
    count = 0
    for row in sheets_rows:
        r = _row_dict(row, hmap)
        symbol = r.get("symbol", "").upper()
        if not symbol:
            continue
        # Build a stable position_tag
        ref_id = r.get("ref_id", "") or r.get("order_id", "") or r.get("ref id", "")
        side = r.get("side", "BUY").upper()
        entry_ts = r.get("entry_ts", "") or r.get("entry ts", "") or _now_str()
        position_tag = ref_id if ref_id else f"MIGRATED:{symbol}:{side}:{entry_ts[:10]}"
        status = str(r.get("status", "CLOSED")).upper()
        payload: dict[str, Any] = {
            "position_tag": position_tag,
            "symbol": symbol,
            "exchange": r.get("exchange", "NSE"),
            "segment": r.get("segment", "CASH"),
            "side": side,
            "qty": _safe_int(r.get("qty")),
            "entry_price": _safe_float(r.get("entry_price", "") or r.get("entry price", "")),
            "sl_price": _safe_float(r.get("sl_price", "") or r.get("sl price", "")),
            "target": _safe_float(r.get("target")),
            "strategy": r.get("strategy", ""),
            "smart_order_id": r.get("smart_order_id", "") or r.get("order id", ""),
            "status": status,
            "exit_price": _safe_float(r.get("exit_price", "") or r.get("exit price", "")),
            "exit_reason": r.get("exit_reason", "") or r.get("exit reason", ""),
            "entry_ts": entry_ts,
            "exit_ts": r.get("exit_ts", "") or r.get("exit ts", ""),
            "pnl": _safe_float(r.get("pnl")),
            "regime": r.get("regime", ""),
            "risk_mode": r.get("risk_mode", "") or r.get("risk mode", ""),
            "signal_score": _safe_int(r.get("signal_score", "") or r.get("score", "")),
            "migrated_from_sheets": True,
        }
        logger.info("  position %s %s", position_tag, status)
        if not dry_run:
            state.save_position(position_tag, payload)
        count += 1
    return count


def migrate_orders(
    sheets_rows: list[list[str]],
    headers: list[str],
    state: Any,
    dry_run: bool,
) -> int:
    hmap = _header_map(headers)
    count = 0
    for row in sheets_rows:
        r = _row_dict(row, hmap)
        ref_id = r.get("ref_id", "") or r.get("ref id", "") or r.get("order_id", "") or r.get("order id", "")
        symbol = r.get("symbol", "").upper()
        if not ref_id and not symbol:
            continue
        if not ref_id:
            ref_id = f"MIGRATED:{symbol}:{r.get('sent_at', _now_str())[:19]}"
        payload: dict[str, Any] = {
            "ref_id": ref_id,
            "symbol": symbol,
            "side": r.get("side", "BUY").upper(),
            "qty": _safe_int(r.get("qty")),
            "order_type": r.get("order_type", "") or r.get("order type", "MARKET"),
            "entry_price": _safe_float(r.get("entry_price", "") or r.get("entry price", "")),
            "sl_price": _safe_float(r.get("sl_price", "") or r.get("sl price", "")),
            "target": _safe_float(r.get("target")),
            "status": r.get("status", ""),
            "smart_order_id": r.get("smart_order_id", "") or r.get("smart order id", ""),
            "paper": _safe_bool(r.get("paper")),
            "sent_at": r.get("sent_at", "") or r.get("sent at", ""),
            "migrated_from_sheets": True,
        }
        logger.info("  order %s", ref_id)
        if not dry_run:
            state.save_order(ref_id, payload)
        count += 1
    return count


def migrate_market_brain(
    sheets_rows: list[list[str]],
    headers: list[str],
    state: Any,
    bq: Any,
    dry_run: bool,
) -> int:
    """Each row is a historical snapshot. Last row → Firestore latest. All → BigQuery."""
    hmap = _header_map(headers)
    bq_rows: list[dict] = []
    latest_payload: dict[str, Any] | None = None
    count = 0
    for row in sheets_rows:
        r = _row_dict(row, hmap)
        asof = r.get("asof_ts", "") or r.get("timestamp", "") or r.get("run ts", "")
        if not asof:
            continue
        regime = r.get("regime", "RANGE")
        risk_mode = r.get("risk_mode", "") or r.get("risk mode", "NORMAL")
        run_date = asof[:10] if len(asof) >= 10 else _today_str()
        payload: dict[str, Any] = {
            "asof_ts": asof,
            "run_date": run_date,
            "regime": regime,
            "risk_mode": risk_mode,
            "participation": r.get("participation", ""),
            "market_confidence": _safe_float(r.get("market_confidence", "") or r.get("market confidence", "")),
            "breadth_confidence": _safe_float(r.get("breadth_confidence", "") or r.get("breadth confidence", "")),
            "leadership_confidence": _safe_float(r.get("leadership_confidence", "") or r.get("leadership confidence", "")),
            "trend_score": _safe_float(r.get("trend_score", "") or r.get("trend score", "")),
            "breadth_score": _safe_float(r.get("breadth_score", "") or r.get("breadth score", "")),
            "volatility_stress_score": _safe_float(r.get("volatility_stress_score", "") or r.get("vol stress", "")),
            "data_quality_score": _safe_float(r.get("data_quality_score", "") or r.get("data quality", "")),
            "selected_watchlist_count": _safe_int(r.get("selected_watchlist_count", "") or r.get("selected", "")),
        }
        bq_rows.append(payload)
        latest_payload = payload
        count += 1
    logger.info("  market_brain: %d rows → BQ, latest → Firestore", count)
    if not dry_run:
        if latest_payload:
            state.save_market_brain(latest_payload)
        if bq_rows:
            bq.insert_market_brain_batch(bq_rows)
    return count


def migrate_watchlist(
    sheets_rows: list[list[str]],
    headers: list[str],
    state: Any,
    bq: Any,
    dry_run: bool,
    run_date: str | None = None,
) -> int:
    hmap = _header_map(headers)
    symbols: list[str] = []
    rows_for_firestore: list[dict] = []
    run_ts = ""
    regime = ""
    risk_mode = ""
    for row in sheets_rows:
        r = _row_dict(row, hmap)
        sym = r.get("symbol", "").upper()
        if not sym:
            continue
        if not run_ts:
            run_ts = r.get("runts", "") or r.get("run ts", "") or _now_str()
        if not regime:
            regime = r.get("canonicalregime", "") or r.get("regime", "")
        if not risk_mode:
            risk_mode = r.get("riskmode", "") or r.get("risk mode", "")
        symbols.append(sym)
        rows_for_firestore.append({k: v for k, v in r.items()})
    rd = run_date or (run_ts[:10] if len(run_ts) >= 10 else _today_str())
    firestore_payload: dict[str, Any] = {
        "generated_at": run_ts or _now_str(),
        "run_date": rd,
        "regime": regime,
        "risk_mode": risk_mode,
        "selected": len(symbols),
        "symbols": symbols,
        "rows": rows_for_firestore,
    }
    bq_payload: dict[str, Any] = {
        "generated_at": run_ts or _now_str(),
        "run_date": rd,
        "regime": regime,
        "risk_mode": risk_mode,
        "selected": len(symbols),
        "symbols": symbols,
    }
    logger.info("  watchlist: %d symbols → Firestore + BQ", len(symbols))
    if not dry_run:
        state.save_watchlist(firestore_payload)
        bq.insert_watchlist_snapshot(bq_payload)
    return len(symbols)


def migrate_signals(
    sheets_rows: list[list[str]],
    headers: list[str],
    bq: Any,
    dry_run: bool,
) -> int:
    hmap = _header_map(headers)
    rows: list[dict] = []
    for row in sheets_rows:
        r = _row_dict(row, hmap)
        symbol = r.get("symbol", "").upper()
        if not symbol:
            continue
        scan_ts = r.get("scan_ts", "") or r.get("scan ts", "") or r.get("timestamp", "") or _now_str()
        rows.append({
            "scan_ts": scan_ts,
            "run_date": scan_ts[:10] if len(scan_ts) >= 10 else _today_str(),
            "symbol": symbol,
            "direction": r.get("direction", r.get("side", "BUY")).upper(),
            "score": _safe_int(r.get("score")),
            "ltp": _safe_float(r.get("ltp", "") or r.get("price", "")),
            "sl": _safe_float(r.get("sl")),
            "target": _safe_float(r.get("target")),
            "qty": _safe_int(r.get("qty")),
            "regime": r.get("regime", ""),
            "risk_mode": r.get("risk_mode", "") or r.get("risk mode", ""),
            "entry_placed": _safe_bool(r.get("entry_placed", "") or r.get("entry placed", "")),
            "blocked_reason": r.get("blocked_reason", "") or r.get("blocked reason", ""),
            "scanner_run_id": r.get("scanner_run_id", "") or r.get("scanner run id", "MIGRATED"),
        })
    logger.info("  signals: %d rows → BQ", len(rows))
    if not dry_run and rows:
        bq.insert_signals_batch(rows)
    return len(rows)


def migrate_audit_log(
    sheets_rows: list[list[str]],
    headers: list[str],
    bq: Any,
    dry_run: bool,
    module: str = "SHEETS_MIGRATION",
) -> int:
    hmap = _header_map(headers)
    rows: list[dict] = []
    for row in sheets_rows:
        r = _row_dict(row, hmap)
        # Try several possible column names used across log sheets
        log_ts = (
            r.get("log_ts") or r.get("log ts") or r.get("timestamp") or
            r.get("run ts") or r.get("runts") or _now_str()
        )
        message = (
            r.get("message") or r.get("msg") or r.get("action") or
            r.get("note") or r.get("notes") or ""
        )
        if not message:
            # fallback: join all non-empty cells
            message = " | ".join(v for v in row if v.strip())
        if not message:
            continue
        rows.append({
            "log_ts": log_ts,
            "run_date": log_ts[:10] if len(log_ts) >= 10 else _today_str(),
            "module": r.get("module", module),
            "action": r.get("action", "LOG"),
            "status": r.get("status", "INFO"),
            "message": message,
            "context": None,
            "exec_id": r.get("exec_id", "") or r.get("exec id", "MIGRATED"),
        })
    logger.info("  audit_log: %d rows → BQ", len(rows))
    if not dry_run and rows:
        # insert in batches of 500
        for i in range(0, len(rows), 500):
            bq.insert_audit_log_batch(rows[i : i + 500])
    return len(rows)


def migrate_pnl(
    sheets_rows: list[list[str]],
    headers: list[str],
    bq: Any,
    dry_run: bool,
) -> int:
    """P&L tracker rows → BigQuery trades table."""
    hmap = _header_map(headers)
    rows: list[dict] = []
    for row in sheets_rows:
        r = _row_dict(row, hmap)
        symbol = r.get("symbol", "").upper()
        if not symbol:
            continue
        entry_ts = r.get("entry_ts", "") or r.get("entry ts", "") or r.get("entry time", "")
        trade_date = entry_ts[:10] if len(entry_ts) >= 10 else _today_str()
        rows.append({
            "trade_date": trade_date,
            "position_tag": r.get("position_tag", "") or r.get("ref id", "") or f"MIGRATED:{symbol}:{trade_date}",
            "symbol": symbol,
            "side": r.get("side", "BUY").upper(),
            "qty": _safe_int(r.get("qty")),
            "entry_price": _safe_float(r.get("entry_price", "") or r.get("entry price", "")),
            "exit_price": _safe_float(r.get("exit_price", "") or r.get("exit price", "")),
            "sl_price": _safe_float(r.get("sl_price", "") or r.get("sl price", "")),
            "target": _safe_float(r.get("target")),
            "pnl": _safe_float(r.get("pnl")),
            "pnl_pct": _safe_float(r.get("pnl_pct", "") or r.get("pnl%", "") or r.get("pnl pct", "")),
            "exit_reason": r.get("exit_reason", "") or r.get("exit reason", ""),
            "strategy": r.get("strategy", ""),
            "entry_ts": entry_ts,
            "exit_ts": r.get("exit_ts", "") or r.get("exit ts", ""),
            "hold_minutes": _safe_int(r.get("hold_minutes", "") or r.get("hold mins", "")),
            "regime": r.get("regime", ""),
            "risk_mode": r.get("risk_mode", "") or r.get("risk mode", ""),
            "market_confidence": _safe_float(r.get("market_confidence", "") or r.get("mkt conf", "")),
            "signal_score": _safe_int(r.get("signal_score", "") or r.get("score", "")),
        })
    logger.info("  trades (pnl): %d rows → BQ", len(rows))
    if not dry_run and rows:
        for i in range(0, len(rows), 500):
            bq.insert_trades_batch(rows[i : i + 500])
    return len(rows)


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def run_migration(
    project_id: str,
    spreadsheet_id: str,
    firestore_database: str = "(default)",
    bq_dataset: str = "autotrader",
    dry_run: bool = False,
) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    logger.info("=== migrate_sheets_to_gcp started project=%s dry_run=%s ===", project_id, dry_run)

    # Lazy imports so script can be called without all deps installed
    from autotrader.adapters.firestore_state import FirestoreStateStore
    from autotrader.adapters.bigquery_client import BigQueryClient
    from autotrader.adapters.sheets_repository import GoogleSheetsRepository, SheetNames

    state = FirestoreStateStore(project_id, firestore_database)
    bq = BigQueryClient(project_id, bq_dataset)
    sheets = GoogleSheetsRepository(spreadsheet_id)

    totals: dict[str, int] = {}

    # ── Config ──────────────────────────────────────────────────────────────
    logger.info("--- Config ---")
    try:
        rows = sheets.read_sheet_rows(SheetNames.CONFIG, start_row=2)
        totals["config"] = migrate_config(rows, state, dry_run)
    except Exception:
        logger.exception("config migration failed")

    # ── Universe ─────────────────────────────────────────────────────────────
    logger.info("--- Universe ---")
    try:
        hdrs = sheets.read_sheet_headers(SheetNames.UNIVERSE, header_row=3)
        rows = sheets.read_sheet_rows(SheetNames.UNIVERSE, start_row=4)
        totals["universe"] = migrate_universe(rows, hdrs, state, dry_run)
    except Exception:
        logger.exception("universe migration failed")

    # ── Sector Mapping ───────────────────────────────────────────────────────
    logger.info("--- Sector Mapping ---")
    try:
        hdrs = sheets.read_sheet_headers(SheetNames.SECTOR_MAPPING, header_row=3)
        rows = sheets.read_sheet_rows(SheetNames.SECTOR_MAPPING, start_row=4)
        totals["sector_mapping"] = migrate_sector_mapping(rows, hdrs, state, dry_run)
    except Exception:
        logger.exception("sector_mapping migration failed")

    # ── Positions ────────────────────────────────────────────────────────────
    logger.info("--- Positions ---")
    try:
        hdrs = sheets.read_sheet_headers(SheetNames.POSITIONS, header_row=3)
        rows = sheets.read_sheet_rows(SheetNames.POSITIONS, start_row=4)
        totals["positions"] = migrate_positions(rows, hdrs, state, dry_run)
    except Exception:
        logger.exception("positions migration failed")

    # ── Orders ───────────────────────────────────────────────────────────────
    logger.info("--- Orders ---")
    try:
        hdrs = sheets.read_sheet_headers(SheetNames.ORDERS, header_row=3)
        rows = sheets.read_sheet_rows(SheetNames.ORDERS, start_row=4)
        totals["orders"] = migrate_orders(rows, hdrs, state, dry_run)
    except Exception:
        logger.exception("orders migration failed")

    # ── Market Brain ─────────────────────────────────────────────────────────
    logger.info("--- Market Brain ---")
    try:
        hdrs = sheets.read_sheet_headers(SheetNames.MARKET, header_row=3)
        rows = sheets.read_sheet_rows(SheetNames.MARKET, start_row=4)
        totals["market_brain"] = migrate_market_brain(rows, hdrs, state, bq, dry_run)
    except Exception:
        logger.exception("market_brain migration failed")

    # ── Watchlist (Intraday V2 preferred, fallback to main watchlist) ─────────
    logger.info("--- Watchlist ---")
    for tab in (SheetNames.WATCHLIST_INTRADAY_V2, SheetNames.WATCHLIST_SWING_V2, SheetNames.WATCHLIST):
        try:
            hdrs = sheets.read_sheet_headers(tab, header_row=3)
            rows = sheets.read_sheet_rows(tab, start_row=4)
            if not rows:
                continue
            totals[f"watchlist_{tab}"] = migrate_watchlist(rows, hdrs, state, bq, dry_run)
            break  # Only migrate the first tab that has data
        except Exception:
            logger.warning("watchlist tab %s failed, trying next", tab)

    # ── Signals ──────────────────────────────────────────────────────────────
    logger.info("--- Signals ---")
    try:
        hdrs = sheets.read_sheet_headers(SheetNames.SIGNALS, header_row=3)
        rows = sheets.read_sheet_rows(SheetNames.SIGNALS, start_row=4)
        totals["signals"] = migrate_signals(rows, hdrs, bq, dry_run)
    except Exception:
        logger.exception("signals migration failed")

    # ── P&L Tracker → BQ trades ──────────────────────────────────────────────
    logger.info("--- P&L Tracker ---")
    try:
        hdrs = sheets.read_sheet_headers(SheetNames.PNL, header_row=3)
        rows = sheets.read_sheet_rows(SheetNames.PNL, start_row=4)
        totals["trades"] = migrate_pnl(rows, hdrs, bq, dry_run)
    except Exception:
        logger.exception("pnl/trades migration failed")

    # ── Project Log → BQ audit_log ───────────────────────────────────────────
    logger.info("--- Project Log ---")
    try:
        hdrs = sheets.read_sheet_headers(SheetNames.ACTIONS, header_row=3)
        rows = sheets.read_sheet_rows(SheetNames.ACTIONS, start_row=4)
        totals["audit_actions"] = migrate_audit_log(rows, hdrs, bq, dry_run, module="PROJECT_LOG")
    except Exception:
        logger.exception("project_log migration failed")

    # ── Logs → BQ audit_log ──────────────────────────────────────────────────
    logger.info("--- Logs ---")
    try:
        hdrs = sheets.read_sheet_headers(SheetNames.LOGS, header_row=3)
        rows = sheets.read_sheet_rows(SheetNames.LOGS, start_row=4)
        totals["audit_logs"] = migrate_audit_log(rows, hdrs, bq, dry_run, module="APP_LOG")
    except Exception:
        logger.exception("logs migration failed")

    logger.info("=== Migration complete. Summary: %s ===", totals)


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate Google Sheets data to Firestore + BigQuery")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--spreadsheet-id", required=True, help="Google Sheets spreadsheet ID")
    parser.add_argument("--firestore-database", default="(default)", help="Firestore database name")
    parser.add_argument("--bq-dataset", default="autotrader", help="BigQuery dataset name")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be written without writing")
    args = parser.parse_args()
    run_migration(
        project_id=args.project,
        spreadsheet_id=args.spreadsheet_id,
        firestore_database=args.firestore_database,
        bq_dataset=args.bq_dataset,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
