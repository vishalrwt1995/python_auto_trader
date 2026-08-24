"""Shared ``symbol -> Upstox instrument_key`` resolution for the event-driven channels.

Extracted 2026-08-24 from five call sites that had drifted into four byte-identical copies of
the same function plus one cross-service private import
(``corp_action`` called ``pead_trading_service._resolve_instrument_keys``). Four copies of one
function is the duplication shape that let the EOD-exemption bug ship **twice** (CORE 06-22,
delivery 07-16): a fix applied to one copy silently leaves the others wrong.

Why two sources (the substance of the 2026-08-24 fix). ``candles_daily`` stopped being written
on **2026-06-07**, so on its own it silently fails to resolve any symbol that listed -- or
entered a channel's universe -- after that date. Such a symbol can never be traded, and before
this was instrumented nothing logged it. Measured on pead's event set:
**no_key 15% (08-20) -> 78% (08-21) -> 64% (08-24)**, which reads in the logs as "quiet market"
rather than "broken lookup".

``candles_5m`` is written daily (partitioned by ``trade_date``, clustered by ``symbol``, so a
date+symbol probe prunes hard) BUT its 30-day window is NARROWER, because it only carries
actively-traded intraday names. Measured coverage:

    candles_daily 2,638 syms | candles_5m(30d) 2,440 | gained 224 | LOST 422 if replaced

So a straight table swap would have been a net loss dressed up as a fix. The union -- fresh wins
on conflict, deep fills the gaps -- gains 224 and loses nothing (~2,862 resolvable).

Deliberately two queries merged in Python rather than one clever ``UNION``: each is
independently debuggable, and if the fresh probe fails we still degrade to the deep table
instead of losing everything. Fail-closed overall: ``{}`` only if BOTH fail, which leaves the
caller with no candidates (the safe direction) rather than a partial book.

``channel`` only sets the log prefix. It is a required positional argument on purpose -- these
lines are the sole live evidence that key resolution is healthy, so a caller must not be able to
fall back to some other channel's name by omitting it.
"""
from __future__ import annotations

import logging
from typing import Sequence

logger = logging.getLogger(__name__)

_DEEP_TABLE = "grow-profit-machine.autotrader.candles_daily"
_FRESH_TABLE = "grow-profit-machine.autotrader.candles_5m"
_FRESH_WINDOW_DAYS = 30


def resolve_instrument_keys(symbols: Sequence[str], bq, channel: str) -> dict[str, str]:
    """``{symbol: instrument_key}`` from the FRESH source first, then the DEEP one.

    ``symbols`` is matched case-insensitively and returned upper-cased. ``bq`` may be ``None`` or
    a broken client -- both queries are individually guarded, so that degrades to ``{}`` rather
    than raising into the caller's scan.
    """
    syms = ",".join("'" + str(s).strip().upper().replace("'", "") + "'" for s in symbols)
    if not syms:
        return {}
    deep: dict[str, str] = {}
    fresh: dict[str, str] = {}
    try:
        q = (f"SELECT symbol, ANY_VALUE(instrument_key) ik "
             f"FROM `{_DEEP_TABLE}` "
             f"WHERE UPPER(symbol) IN ({syms}) AND instrument_key IS NOT NULL GROUP BY symbol")
        deep = {str(r["symbol"]).strip().upper(): str(r["ik"]) for r in bq.query(q)}
    except Exception as exc:
        logger.error("%s_resolve_keys_deep_failed err=%s", channel, exc)
    try:
        q = (f"SELECT symbol, ANY_VALUE(instrument_key) ik "
             f"FROM `{_FRESH_TABLE}` "
             f"WHERE trade_date >= DATE_SUB(CURRENT_DATE('Asia/Kolkata'), "
             f"INTERVAL {_FRESH_WINDOW_DAYS} DAY) "
             f"AND UPPER(symbol) IN ({syms}) AND instrument_key IS NOT NULL GROUP BY symbol")
        fresh = {str(r["symbol"]).strip().upper(): str(r["ik"]) for r in bq.query(q)}
    except Exception as exc:
        logger.error("%s_resolve_keys_fresh_failed err=%s", channel, exc)
    merged = {**deep, **fresh}                      # fresh wins on conflict
    logger.info("%s_resolve_keys asked=%d deep=%d fresh=%d merged=%d fresh_only=%d",
                channel, len(symbols), len(deep), len(fresh), len(merged),
                len(set(fresh) - set(deep)))
    return merged
