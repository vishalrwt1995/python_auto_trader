"""CORE channel signal service — quarterly target-basket builder, the CORE channel's signal
layer. Mirrors the pure-core + thin-I/O pattern: ``build_target_basket`` is PURE + data-injected
(fidelity-tested via ``domain/core_signals``); ``fetch_universe_history`` is the live wrapper.
Fail-closed.

Flow: each quarter-end, for the large-cap liquid universe compute 12-1 momentum + 60d vol +
20d turnover from daily history -> universe gates -> rank-blend -> top-30 target basket
(equal-weight). The trade layer (core_trading_service) rebalances current holdings to this
target. Long-only, cash (CNC), buy-and-HOLD — no stops, no intraday exits."""
from __future__ import annotations

import logging
from typing import Any

from autotrader.domain import core_signals as cs

logger = logging.getLogger(__name__)


def build_target_basket(history: dict[str, list[list]]) -> list[dict[str, Any]]:
    """PURE: ``history`` = ``{symbol: daily_bars}`` (bars ascending ``[date,o,h,l,c,v]``) for the
    candidate universe, as of the rebalance. For each symbol compute momentum/vol/turnover at
    the last bar, apply universe gates, then rank-blend -> top-30. Returns the target basket as
    ``[{symbol, ref_price}]`` (equal-weight; ref_price = last close, the sizing proxy)."""
    cand = []
    ref = {}
    for sym, bars in history.items():
        if not bars or len(bars) <= cs.MOM_LOOKBACK:
            continue
        closes = [b[4] for b in bars]; vols = [b[5] for b in bars]
        i = len(bars) - 1
        rets = [0.0] + [closes[k] / closes[k - 1] - 1.0 if closes[k - 1] > 0 else 0.0 for k in range(1, len(closes))]
        tov = sum(closes[k] * vols[k] for k in range(i - 19, i + 1)) / 20.0 if i >= 19 else 0.0
        if not cs.passes_universe_gates(closes[i], tov, i >= cs.MOM_LOOKBACK):
            continue
        mom = cs.momentum_score(closes, i); vol = cs.realized_vol(rets, i)
        if mom is None or vol is None:
            continue
        s = str(sym).strip().upper()
        cand.append({"symbol": s, "momentum": mom, "vol": vol, "turnover": tov})
        ref[s] = closes[i]
    picks = cs.rank_blend_select(cand)
    return [{"symbol": s, "ref_price": round(ref.get(s, 0.0), 2)} for s in picks]


def fetch_universe_history(universe: list[str], instrument_keys: dict[str, str], upstox: Any,
                           asof: str, lookback_days: int = 600) -> dict[str, list[list]]:
    """Live I/O: daily bars for each universe symbol via Upstox v3, over a CORE-sized window.
    CORE needs > MOM_LOOKBACK (252) trading bars for the 12-1 momentum; ``lookback_days`` is
    calendar days, defaulting to 600 (~410 trading bars — comfortable buffer over 252+21+vol).
    NOTE: we do NOT reuse pead's ``_fetch_symbol_daily`` — its window (SYMBOL_HISTORY_DAYS*1.6
    ≈ 208 cal days ≈ 140 trading bars) is far too short for CORE and would silently produce an
    EMPTY basket every quarter. Thin wrapper; fail-closed (skip a symbol on any fetch error).
    ``instrument_keys`` = {symbol: ik}."""
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
            if len(bars) > cs.MOM_LOOKBACK:
                out[sym] = bars
        except Exception:
            logger.warning("core_fetch_daily_failed sym=%s", sym, exc_info=True)
    return out


def scan(history: dict[str, list[list]] | None = None, *, universe=None, instrument_keys=None,
         upstox=None, asof: str | None = None) -> list[dict[str, Any]]:
    """Top-level: return the quarterly target basket (top-30, equal-weight). If ``history`` is
    None, fetch live for ``universe`` via ``upstox``."""
    if history is None:
        history = fetch_universe_history(universe or [], instrument_keys or {}, upstox, asof or "")
    basket = build_target_basket(history)
    logger.info("core_scan universe=%d basket=%d", len(history), len(basket))
    return basket
