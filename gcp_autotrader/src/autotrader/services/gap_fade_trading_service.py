"""Gap-fade short trading service — at-the-open scan -> own GAP_FADE channel entry (PAPER).

Unlike corp (which shares the EVENT pool), gap-fade is its OWN channel: own capital
(``channel_capital("gap_fade")``), own ``gapfade_max_positions`` slot cap, own −3%/+6%
daily breaker on the channel capital. Tagged ``channel="gap_fade"`` + ``wl_type="gap_fade"``
so swing/pead/corp reconciliation never touch it (gap_fade exit owns it).

It is a SHORT, intraday MIS: entry = market SELL at the open; protective **buy-stop 3%
ABOVE** entry (a short's stop is above) placed as a broker bracket; cover at the 15:15
squareoff. Sizing = ``gapfade_notional_cap_pct`` of channel capital (0.20 pilot). All
selection mirrors the validated backtest via ``domain/gap_fade_signals``. PAPER until
explicitly flipped (Rule 5); the live I/O wrapper is fail-closed."""
from __future__ import annotations

import logging
from typing import Any, Sequence

from autotrader.domain import gap_fade_signals as gf
from autotrader.domain import pead_book
from autotrader.services import gap_fade_signal_service

logger = logging.getLogger(__name__)


def plan_gap_fade_entries(
    candidates: Sequence[dict[str, Any]],
    open_channel_symbols: Sequence[str],
    open_count: int,
    realized_today: float,
    channel_capital: float,
    cfg: Any,
) -> list[dict[str, Any]]:
    """PURE: given the open's gap-fade candidates + GAP_FADE book state, return SHORT order
    specs. Applies the channel daily breaker (3%/6% of channel capital), the slot cap
    (``gapfade_max_positions``), exclusion of names already held, and fixed-notional sizing
    with the 3% protective buy-stop ABOVE entry. Candidates are pre-ranked by gap size.
    Side-effect-free + unit-testable."""
    if channel_capital <= 0:
        return []
    loss_limit = -abs(cfg.daily_loss_pct) * channel_capital
    profit_limit = abs(cfg.daily_profit_pct) * channel_capital
    if pead_book.daily_breaker_tripped(realized_today, loss_limit, profit_limit):
        logger.info("gap_fade_daily_breaker_tripped realized=%.0f — no entries", realized_today or 0.0)
        return []

    held = {str(s).strip().upper() for s in open_channel_symbols}
    room = max(0, cfg.gapfade_max_positions - max(0, open_count))
    if room <= 0:
        logger.info("gap_fade_no_room open=%d cap=%d", open_count, cfg.gapfade_max_positions)
        return []

    notional_pct = cfg.gapfade_notional_cap_pct
    stop_pct = cfg.gapfade_stop_pct
    specs: list[dict[str, Any]] = []
    for c in candidates:
        if len(specs) >= room:
            break
        sym = str(c["symbol"]).strip().upper()
        if sym in held:
            continue
        entry = float(c.get("ref_open") or 0.0)
        if entry <= 0:
            continue
        qty = gf.position_qty(entry, channel_capital, notional_pct)
        if qty < 1:
            logger.info("gap_fade_skip_qty_zero sym=%s entry=%.2f", sym, entry)
            continue
        sl = round(gf.short_stop_price(entry, stop_pct), 2)               # buy-stop ABOVE entry
        specs.append({
            "symbol": sym,
            "qty": qty,
            "side": "SELL",                                               # SHORT
            "entry_price": round(entry, 2),
            "sl_price": sl,
            "atr": round(entry * stop_pct, 4),
            "gap": float(c.get("gap", 0.0)),
            "instrument_key": str(c.get("instrument_key", "")),
            "strategy": "GAP_FADE",
        })
    logger.info("gap_fade_plan candidates=%d held=%d room=%d planned=%d",
                len(candidates), len(held), room, len(specs))
    return specs


# ── live I/O wrapper (validated in PAPER, not unit-tested — fail-closed) ───────
def _channel_realized_today(state, asof: str) -> float:
    """GAP_FADE channel realized P&L today (channel="gap_fade") for the daily breaker.
    Mirrors PEAD's channel-keyed accounting (`_pead_realized_today`); NaN on read
    failure -> breaker trips (fail-closed)."""
    try:
        total = 0.0
        for p in state.list_all_positions(limit=500):
            if str(p.get("channel", "")).strip().lower() != "gap_fade":
                continue
            if str(p.get("status", "")).upper() != "CLOSED":
                continue
            if str(p.get("exit_ts", ""))[:10] != asof:
                continue
            total += float(p.get("pnl", 0.0) or 0.0)
        return total
    except Exception as exc:
        logger.error("gap_fade_realized_today_failed asof=%s err=%s — breaker trips", asof, exc)
        return float("nan")


def run_gap_fade_scan_once(*, settings, upstox, state, order_service, bq=None,
                           entry_date: str | None = None,
                           keymap: dict[str, str] | None = None,
                           turnover_map: dict[str, float] | None = None) -> dict[str, Any]:
    """Live at-the-open gap-fade scan + SHORT entry (PAPER). Own GAP_FADE channel; fail-closed.

    Runs just after the open (~09:16 IST): snapshot the F&O universe's open/prev-close/range,
    select >5% gap-ups, short the top-K (MIS, broker-bracket buy-stop 3% above). Cover is the
    15:15 squareoff (gap_fade_reconciliation). Channel default-off (CAPITAL_GAPFADE=0 /
    gapfade_max_positions=0) until enabled with explicit authorization."""
    from autotrader.time_utils import now_ist
    cfg = settings.strategy
    asof = entry_date or now_ist().strftime("%Y-%m-%d")
    channel_capital = cfg.channel_capital("gap_fade")
    if channel_capital <= 0:
        return {"skipped": "gap_fade_capital_zero", "asof": asof}
    if getattr(cfg, "gapfade_max_positions", 0) <= 0:
        return {"skipped": "gap_fade_disabled", "asof": asof}

    km = keymap if keymap is not None else fetch_fno_universe()
    if not km:
        return {"skipped": "no_fno_universe", "asof": asof}
    tm = turnover_map if turnover_map is not None else _fetch_turnover_map(bq, asof, list(km))

    candidates = gap_fade_signal_service.scan(km, upstox=upstox, turnover_map=tm,
                                              max_positions=cfg.gapfade_max_positions)
    for c in candidates:
        c["instrument_key"] = km.get(c["symbol"], "")

    open_positions = state.list_open_positions()
    open_channel = [p for p in open_positions if str(p.get("channel", "")).strip().lower() == "gap_fade"]
    open_syms = [p.get("symbol") for p in open_channel]
    realized_today = _channel_realized_today(state, asof)

    specs = plan_gap_fade_entries(candidates, open_syms, len(open_channel), realized_today, channel_capital, cfg)
    entered: list[str] = []
    for s in specs:
        try:
            res = order_service.place_entry_order(
                symbol=s["symbol"], exchange="NSE", segment="CASH", side="SELL", qty=s["qty"],
                entry_price=s["entry_price"], sl_price=s["sl_price"],
                target=round(s["entry_price"] * (1.0 - 0.5), 2),         # nominal far cover target; real cover = 15:15 squareoff
                atr=s["atr"], product="I", score=int(round(s["gap"] * 100)), reason="GAP_FADE",
                instrument_key=s["instrument_key"], strategy="GAP_FADE",
                wl_type="gap_fade", channel="gap_fade",
            )
            if res and not res.get("error") and not res.get("skipped"):
                entered.append(s["symbol"])
        except Exception:
            logger.exception("gap_fade_place_entry_failed sym=%s", s["symbol"])

    summary = {"asof": asof, "universe": len(km), "candidates": len(candidates),
               "planned": len(specs), "entered": len(entered), "open_before": len(open_channel)}
    logger.info("gap_fade_scan_summary %s", summary)
    return summary


# Upstox NSE instrument master (F&O underlyings + their NSE_EQ instrument keys, in one fetch)
_FNO_MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
_INDEX_UNDERLYINGS = frozenset({"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50", "NIFTYIT"})


def fetch_fno_universe() -> dict[str, str]:
    """NSE F&O underlyings + their NSE_EQ instrument keys, from the Upstox instrument master
    (the FUT rows carry ``underlying_symbol`` + ``underlying_key``). Returns
    ``{symbol: nse_eq_instrument_key}`` — universe AND quote-keys in one fetch, always-current
    (no stale table to maintain). Index futures excluded. Fail-closed -> ``{}``."""
    import urllib.request
    import gzip
    import json as _json
    try:
        raw = urllib.request.urlopen(_FNO_MASTER_URL, timeout=45).read()
        data = _json.loads(gzip.decompress(raw))
        out: dict[str, str] = {}
        for d in data:
            if d.get("segment") != "NSE_FO" or d.get("instrument_type") != "FUT":
                continue
            sym = str(d.get("underlying_symbol") or d.get("asset_symbol") or "").strip().upper()
            uk = str(d.get("underlying_key") or "").strip()
            if sym and uk and sym not in _INDEX_UNDERLYINGS and (d.get("underlying_type") or "").upper() != "INDEX":
                out[sym] = uk
        logger.info("gap_fade_fno_universe symbols=%d", len(out))
        return out
    except Exception as exc:
        logger.error("gap_fade_fno_universe_failed err=%s", exc)
        return {}


def _fetch_turnover_map(bq, asof: str, symbols: Sequence[str]) -> dict[str, float]:
    """``{symbol: turnover_20d}`` (20d avg close*volume) from candles_daily — the liquidity
    gate input. Liquidity is stable, so a slightly-stale value is fine (unlike prev_close, which
    must be fresh and comes from the live ``cp``). Fail-closed -> ``{}`` (turnover gate then
    fails closed = no candidate, the safe direction)."""
    if bq is None:
        return {}
    try:
        q = ("WITH r AS (SELECT symbol, close*volume AS cv, "
             "ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date DESC) rn "
             "FROM `grow-profit-machine.autotrader.candles_daily` "
             f"WHERE trade_date >= DATE_SUB(DATE('{asof}'), INTERVAL 45 DAY)) "
             "SELECT symbol, AVG(IF(rn<=20, cv, NULL)) AS tov FROM r GROUP BY symbol")
        return {str(row["symbol"]).strip().upper(): float(row["tov"] or 0.0) for row in bq.query(q)}
    except Exception as exc:
        logger.error("gap_fade_turnover_map_failed err=%s", exc)
        return {}
