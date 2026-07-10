"""Momentum x Low-Vol channel signal service — monthly target-basket builder. Mirrors the
pure-core + thin-I/O pattern of core_signal_service: ``build_target_basket`` is PURE +
data-injected (fidelity-tested via domain/momentum_signals); the ``fetch_*`` helpers are the
live wrappers. Fail-closed.

Flow each month: pre-filter the broad NSE universe (Firestore) to the liquid subset by its
stored 60d turnover -> fetch daily history via Upstox -> compute 12-1 momentum + 126d vol + 60d
median turnover -> >=Rs10cr gate -> rank-blend with hysteresis buffer -> Nifty-100DMA regime
overlay (empty basket = go to CASH). The trade layer (momentum_trading_service) rebalances
holdings to this target. Long-only, cash (CNC), buy-and-HOLD — no stops, no intraday exits."""
from __future__ import annotations

import logging
from typing import Any, Sequence

from autotrader.domain import momentum_signals as ms

logger = logging.getLogger(__name__)

# Loose pre-filter on the universe's stored turnover so we only fetch daily bars for plausibly
# liquid names (bounds ~2,665 -> ~liquid subset). The EXACT >=Rs10cr gate is re-applied in
# build_target_basket on the freshly-fetched daily bars (the stored value can be slightly stale).
PREFILTER_TURNOVER: float = 5e7    # >= Rs5cr (loose; below the Rs10cr live gate to avoid edge misses)


def build_target_basket(history: dict[str, list[list]], prev_holds: Sequence[str] = (),
                        regime_ok: bool = True) -> list[dict[str, Any]]:
    """PURE: ``history`` = ``{symbol: daily_bars}`` (ascending ``[date,o,h,l,c,v]``) as of the
    rebalance. Compute momentum/vol/turnover at the last bar, apply the >=Rs10cr universe gate,
    then rank-blend (with the hysteresis buffer vs ``prev_holds``) under the ``regime_ok``
    overlay -> top-20 target. Returns ``[{symbol, ref_price}]`` (equal-weight; ref_price = last
    close, the sizing proxy). Empty basket when ``regime_ok`` is False (go to CASH)."""
    cand: list[dict[str, Any]] = []
    ref: dict[str, float] = {}
    for sym, bars in history.items():
        if not bars or len(bars) <= ms.MOM_LOOKBACK:
            continue
        closes = [b[4] for b in bars]
        vols = [b[5] for b in bars]
        i = len(bars) - 1
        rets = [0.0] + [closes[k] / closes[k - 1] - 1.0 if closes[k - 1] > 0 else 0.0
                        for k in range(1, len(closes))]
        tov = ms.median_turnover(closes, vols, i)
        if not ms.passes_universe_gates(closes[i], tov, i >= ms.MOM_LOOKBACK):
            continue
        mom = ms.momentum_score(closes, i)
        vol = ms.realized_vol(rets, i)
        if mom is None or vol is None:
            continue
        s = str(sym).strip().upper()
        cand.append({"symbol": s, "momentum": mom, "vol": vol, "turnover": tov})
        ref[s] = closes[i]
    picks = ms.rank_blend_select(cand, prev_holds=prev_holds, regime_ok=regime_ok)
    return [{"symbol": s, "ref_price": round(ref.get(s, 0.0), 2)} for s in picks]


def fetch_universe(state, prefilter_turnover: float = PREFILTER_TURNOVER) -> dict[str, str]:
    """Live I/O: the broad NSE universe + NSE_EQ instrument keys from Firestore
    (``state.list_universe``), pre-filtered to plausibly-liquid names by the stored
    ``turnover_med_60d``. Returns ``{symbol: instrument_key}``. Fail-closed -> ``{}``."""
    try:
        docs = state.list_universe(limit=3000) or []
    except Exception:
        logger.warning("momentum_universe_fetch_failed", exc_info=True)
        return {}
    out: dict[str, str] = {}
    for doc in docs:
        if str(doc.get("enabled", "Y")).strip().upper() in ("N", "NO", "FALSE", "0"):
            continue
        sym = str(doc.get("symbol") or "").strip().upper()
        ik = str(doc.get("instrument_key") or "").strip()
        if not sym or not ik:
            continue
        try:
            tov = float(doc.get("turnover_med_60d") or 0.0)
        except (TypeError, ValueError):
            tov = 0.0
        if tov < prefilter_turnover:
            continue
        out[sym] = ik
    logger.info("momentum_universe symbols=%d (prefilter>=Rs%.0fcr)", len(out), prefilter_turnover / 1e7)
    return out


def fetch_universe_history(universe: list[str], instrument_keys: dict[str, str], upstox: Any,
                           asof: str, lookback_days: int = 600) -> dict[str, list[list]]:
    """Live I/O: daily bars per universe symbol via Upstox v3 over a window comfortably larger
    than MOM_LOOKBACK (252) + vol/turnover. Mirrors core_signal_service.fetch_universe_history
    (do NOT reuse pead's short window — it would silently empty the basket). Fail-closed per
    symbol. ``instrument_keys`` = {symbol: ik}."""
    from datetime import date, timedelta
    if not asof:
        return {}
    frm = (date.fromisoformat(asof) - timedelta(days=int(lookback_days))).isoformat()
    out: dict[str, list[list]] = {}
    for sym in universe:
        ik = instrument_keys.get(sym)
        if not ik:
            continue
        try:
            rows = upstox.get_historical_candles_v3_days(ik, to_date=asof, from_date=frm, interval_days=1)
            bars = [[str(r[0])[:10], float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])]
                    for r in rows if isinstance(r, (list, tuple)) and len(r) >= 6]
            bars.sort(key=lambda b: b[0])
            if len(bars) > ms.MOM_LOOKBACK:
                out[sym] = bars
        except Exception:
            logger.warning("momentum_fetch_daily_failed sym=%s", sym, exc_info=True)
    return out


def fetch_nifty_regime(upstox: Any, nifty_key: str, asof: str, ma_window: int = ms.REGIME_MA) -> bool:
    """Live I/O: fetch Nifty daily closes and return the regime overlay state (True = HOLD when
    close > ``ma_window``-DMA, False = CASH). Fail-closed: on any fetch error -> False (don't
    deploy into an unknown regime). ``nifty_key`` = settings.nifty50_instrument_key."""
    from datetime import date, timedelta
    if not asof or not nifty_key:
        return False
    frm = (date.fromisoformat(asof) - timedelta(days=ma_window * 2 + 60)).isoformat()
    try:
        rows = upstox.get_historical_candles_v3_days(nifty_key, to_date=asof, from_date=frm, interval_days=1)
        closes = [float(r[4]) for r in sorted(rows, key=lambda r: str(r[0]))
                  if isinstance(r, (list, tuple)) and len(r) >= 6]
        ok = ms.nifty_regime_ok(closes, ma_window)
        logger.info("momentum_regime nifty_bars=%d ma=%d regime_ok=%s", len(closes), ma_window, ok)
        return ok
    except Exception:
        logger.warning("momentum_nifty_regime_fetch_failed", exc_info=True)
        return False


def scan(history: dict[str, list[list]] | None = None, *, universe=None, instrument_keys=None,
         upstox=None, asof: str | None = None, prev_holds: Sequence[str] = (),
         regime_ok: bool = True) -> list[dict[str, Any]]:
    """Top-level: return the monthly target basket (top-20, equal-weight) under the regime
    overlay + buffer. If ``history`` is None, fetch live for ``universe`` via ``upstox``."""
    if history is None:
        history = fetch_universe_history(universe or [], instrument_keys or {}, upstox, asof or "")
    basket = build_target_basket(history, prev_holds=prev_holds, regime_ok=regime_ok)
    logger.info("momentum_scan universe=%d basket=%d regime_ok=%s", len(history), len(basket), regime_ok)
    return basket
