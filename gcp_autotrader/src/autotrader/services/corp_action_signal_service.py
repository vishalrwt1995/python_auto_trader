"""Corp-action (bonus/split) live signal service — produces corp-action candidates for
a scan session, the second sub-strategy of the EVENT/PEAD channel.

Flow:  NSE corporate-board-meetings (bonus/split INTIMATIONS, with bm_timestamp)  ->
per-symbol uptrend + anti-pump (eq-weight market-adjusted) + first-time + lead gates
(domain/corp_action_signals)  ->  qualifying candidates (enter NEXT OPEN after the
intimation, hard-exit at the meeting-day close).

Design mirrors pead_signal_service: the selection core (`build_candidates`,
`build_market_level`) is PURE and data-injected (fidelity-tested against the exact
backtest); the NSE fetch (`fetch_corp_events`) is the thin live-I/O wrapper. All
constants/computations mirror the validated look-ahead-free backtest (+1.54% net/event,
robust IS+OOS) so live cannot drift from it. Fail-closed.

Premarket timing (mirrors PEAD's reaction_target): the scan anchors on the LAST COMPLETED
session (``last_session``); a candidate is an intimation whose last session on/before it
== ``last_session`` (so the next open — the upcoming session — is the entry). Features are
computed at that completed session (look-ahead-free); the order fills market-on-open and
the position stores the meeting date for the hard exit.

Bars: per symbol ``[date, open, high, low, close, volume]`` ascending. The anti-pump
run-up is market-adjusted by the EQUAL-WEIGHT universe return (the right benchmark for
these small/mid names; NIFTY-50 under-adjusts). Entry is NEXT-OPEN (v1; the +0.45%
smart-entry refinement is a future upgrade needing an intraday order path).
"""
from __future__ import annotations

import bisect
import logging
from typing import Any, Sequence

from autotrader.domain import corp_action_signals as ca

logger = logging.getLogger(__name__)

# Eq-weight universe index construction (matches pead_signal_service.compute_market_dd)
MARKET_RET_CLIP: float = 0.30
MARKET_MIN_STOCKS: int = 50
MARKET_MIN_HISTORY: int = 300


def build_market_level(universe_candles: dict[str, list[list]], upto: str) -> dict[str, float]:
    """Equal-weight universe index LEVEL by date (cumulative cross-sectional mean daily
    return) up to ``upto``. Same construction as the backtest and
    pead_signal_service.compute_market_dd — the anti-pump benchmark. ``{date: level}``."""
    rsum: dict[str, float] = {}
    rcnt: dict[str, int] = {}
    for bars in universe_candles.values():
        if len(bars) < MARKET_MIN_HISTORY:
            continue
        for i in range(1, len(bars)):
            d = bars[i][0]
            if d > upto:
                break
            pc = bars[i - 1][4]
            if pc > 0:
                r = bars[i][4] / pc - 1.0
                if -MARKET_RET_CLIP < r < MARKET_RET_CLIP:
                    rsum[d] = rsum.get(d, 0.0) + r
                    rcnt[d] = rcnt.get(d, 0) + 1
    days = sorted(d for d in rsum if rcnt[d] >= MARKET_MIN_STOCKS)
    lvl: dict[str, float] = {}
    ix = 1.0
    for d in days:
        ix *= (1 + rsum[d] / rcnt[d])
        lvl[d] = ix
    return lvl


def market_ret(market_level: dict[str, float], d0: str, d1: str) -> float:
    """Equal-weight market return between two dates (0.0 if either is missing — the
    anti-pump gate then sees the raw run-up, the conservative direction)."""
    a, b = market_level.get(d0), market_level.get(d1)
    if not a or not b or a <= 0:
        return 0.0
    return b / a - 1.0


def build_candidates(
    last_session: str,
    corp_events: Sequence[dict[str, Any]],
    candles: dict[str, list[list]],
    market_level: dict[str, float],
    prior_keys: set[tuple[str, str]],
) -> list[dict[str, Any]]:
    """PURE corp-action candidate selection anchored on ``last_session`` (the last
    completed market session; entry is the NEXT open).

    A candidate is a bonus/split intimation whose last session on/before its intimation
    date == ``last_session`` (so its first-session-after — the upcoming open — is the
    entry). At that completed session ``ai`` compute (look-ahead-free): uptrend
    (``dist_above_52w_low``), eq-weight market-adjusted 20d run-up, liquidity/price floors.
    First-time from ``prior_keys``. Apply ``corp_action_signals`` gates. Rank by uptrend.
    ``ref_close`` is the premarket sizing proxy; ``meeting_date`` is the hard exit date.
    """
    out: list[dict[str, Any]] = []
    for ev in corp_events:
        typ = str(ev.get("type") or "").strip().lower()
        if typ not in ca.EVENT_TYPES:
            continue
        sym = str(ev.get("symbol") or "").strip().upper()
        bars = candles.get(sym)
        if not bars or len(bars) < 62:
            continue
        dates = [b[0] for b in bars]
        ai = bisect.bisect_right(dates, ev["intim_dt"]) - 1       # last session on/before the intimation
        if ai < 61 or ai >= len(dates) or dates[ai] != last_session:
            continue                                              # entry (next open) is the upcoming session iff ai is last_session
        lows = [b[3] for b in bars]; closes = [b[4] for b in bars]; vols = [b[5] for b in bars]
        if closes[ai] < ca.PRICE_MIN or closes[ai - ca.RUNUP_LOOKBACK] <= 0:
            continue
        if sum(closes[k] * vols[k] for k in range(ai - 19, ai + 1)) / 20.0 < ca.TURNOVER_MIN:
            continue
        dist = ca.dist_above_52w_low(closes, lows, ai)            # look-ahead-free: completed-session close
        raw = closes[ai] / closes[ai - ca.RUNUP_LOOKBACK] - 1.0   # 20d run-up ending at the completed session
        runup_adj = raw - market_ret(market_level, dates[ai - ca.RUNUP_LOOKBACK], dates[ai])
        is_first = (sym, typ) not in prior_keys
        lead = ev.get("lead")
        if not ca.passes_corp_gates(typ, is_first, dist, runup_adj, lead):
            continue
        out.append({
            "symbol": sym,
            "channel": "pead",
            "wl_type": "corp_action",
            "strategy": "CORP_ACTION",
            "event_type": typ,
            "meeting_date": str(ev["meeting_dt"]),
            "dist_low": round(dist, 4),
            "runup_adj": round(runup_adj, 4),
            "lead": int(lead),
            "ref_close": round(closes[ai], 2),     # premarket-known proxy for the next open (sizing)
        })
    out.sort(key=lambda c: -c["dist_low"])      # rank by uptrend strength (slot priority)
    return out


def _parse_nse_dt(ds: str) -> str | None:
    """Parse '08-May-2026' / '08-May-2026 19:18:13' / '2026-05-08' to ISO date; None on fail."""
    from datetime import datetime
    ds = str(ds or "").strip()
    if not ds:
        return None
    head = ds.split()[0]
    for fmt in ("%d-%b-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(head, fmt).date().isoformat()
        except Exception:
            continue
    return None


def fetch_corp_events(asof: str, lookback_days: int = 6) -> list[dict[str, Any]]:
    """Live NSE corporate-board-meetings: bonus/split INTIMATIONS in
    ``[asof - lookback_days, asof]``, each ``{symbol, type, intim_dt, meeting_dt, lead}``.
    Routine dividend-with-results is excluded (that is PEAD's earnings meeting). Thin I/O
    wrapper; fail-closed (``[]`` on error)."""
    from datetime import date, timedelta
    try:
        from autotrader.services.earnings_calendar_service import _nse_get
        a = date.fromisoformat(asof)
        frm = (a - timedelta(days=lookback_days)).strftime("%d-%m-%Y")
        to = a.strftime("%d-%m-%Y")
        data = _nse_get(f"/api/corporate-board-meetings?index=equities&from_date={frm}&to_date={to}")
        rows = data if isinstance(data, list) else (data.get("data") if isinstance(data, dict) else [])
        out: list[dict[str, Any]] = []
        for e in rows:
            blob = (str(e.get("bm_purpose", "")) + " " + str(e.get("bm_desc", ""))).lower()
            typ = "bonus" if "bonus" in blob else ("split" if "split" in blob else None)
            if typ is None:
                continue
            sym = str(e.get("bm_symbol", "")).strip().upper()
            meeting = _parse_nse_dt(e.get("bm_date"))
            intim = _parse_nse_dt(e.get("bm_timestamp"))
            if not (sym and meeting and intim):
                continue
            lead = (date.fromisoformat(meeting) - date.fromisoformat(intim)).days
            out.append({"symbol": sym, "type": typ, "intim_dt": intim, "meeting_dt": meeting, "lead": lead})
        logger.info("corp_action_events asof=%s count=%d", asof, len(out))
        return out
    except Exception as exc:
        logger.error("corp_action_fetch_events_failed asof=%s err=%s", asof, exc)
        return []


def scan(
    last_session: str,
    candles: dict[str, list[list]],
    market_level: dict[str, float],
    prior_keys: set[tuple[str, str]],
    corp_events: Sequence[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Top-level: return corp-action candidates whose entry (next open) is the session
    after ``last_session``. ``candles`` = per-symbol daily bars for the intimated names;
    ``market_level`` = eq-weight universe index level (anti-pump benchmark, caller-built);
    ``prior_keys`` = persisted (symbol,type) history for first-time. If ``corp_events`` is
    None, fetch live from NSE."""
    if corp_events is None:
        corp_events = fetch_corp_events(last_session)
    cands = build_candidates(last_session, corp_events, candles, market_level, prior_keys)
    logger.info("corp_action_scan last_session=%s events=%d candidates=%d",
                last_session, len(corp_events), len(cands))
    return cands
