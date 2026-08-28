"""INSIDER Cluster-Buy channel — pure signal gates, cluster aggregation + slot selection.

Shared by the live insider trading service AND the parity backtest (scripts/redesign/insider_god*.py)
so live selection/sizing cannot drift from the validated config (same discipline as
delivery_signals / pead_signals). All functions pure + side-effect-free.

Signal: NSE SEBI-PIT disclosures of INFORMED OPEN-MARKET BUYS (promoter / promoter group /
director / KMP / immediate relative), each leg >= Rs 5 lakh, aggregated per (symbol,
disclosure-day). A "cluster" = >= 2 such qualifying legs on the same day for the same symbol
(genuine conviction; a single leg is noise). Entry the NEXT session's open after the public
disclosure (never the private transaction date — no look-ahead).

Validated GOD-MODE config (2026-07-20; survivorship-safe incl. delisted; IS<=2020 / OOS>=2021;
full Upstox costs + slippage; robust plateau + tactical-gate cross-check):
  cluster (>=2 buyers)  ·  DOUBLE MACRO GATE: breadth b200 > 50 AND Nifty > 100-day-MA  ·
  turnover_20d >= Rs 10cr  ·  price >= Rs 30  ·  hold 90 sessions  ·  10 slots  ·
  ATR14 x 2.5 protective stop, FIXED-hold (NO trail — a trail whipsaws this drift edge)  ·
  1.5% equity risk.
  -> +23% CAGR / -12.5% maxDD / Calmar 1.84 (IS 2.85 / OOS 1.75) / ~20 trades/yr /
     3.2% momentum overlap (additive). Un-engineered was Calmar 0.51 — the double macro-gate +
     cluster is what cut the DD from -44% to -12.5% while lifting return.
Sizing / daily-breaker reuse domain/pead_book (generic). Costs/ATR reuse shared Upstox code.
The macro double-gate is CHANNEL-LEVEL (evaluated once per scan) — bad regime => no entries.
"""
from __future__ import annotations

from autotrader.domain import etf_filter

from typing import Any, Sequence

# ── validated thresholds (env-overridable in settings; these are the defaults) ──
MIN_BUYERS = 2                         # "cluster": >= 2 qualifying informed-buy legs same day
MIN_LEG_VALUE = 500_000.0             # each disclosure leg >= Rs 5 lakh (per-leg pre-filter)
TURNOVER_MIN_CR = 10.0                # 20d-mean turnover floor (crore); NO upper cap (edge
#                                       concentrates 10-25cr but works >=10cr; large-caps
#                                       merely dilute — restricting to >=25cr drops CAGR 23->14%)
PRICE_MIN = 30.0
MAX_HOLD_DAYS = 90                    # fixed-hold exit (drift edge; a trail whipsaws it)
ATR_SL_MULT = 2.5                    # protective disaster stop only (not a trail)
ATR_WINDOW = 14
MIN_BARS = 22                        # need 20d turnover + ATR14 + a prior close
MAX_POSITIONS = 10
# double macro gate (channel-level, evaluated once per scan day)
B200_MIN = 50.0                      # market breadth (% of universe > EMA200) floor
NIFTY_MA_DAYS = 100                  # Nifty must be above its 100-day SMA

# informed insider categories (SEBI-PIT personCategory), lower-cased substring match
_INFORMED = ("promoter", "director", "key managerial", "immediate relative", "promoter group")

# ETF guard (belt-and-suspenders; PIT disclosures are company-only so ETFs shouldn't appear).
# Superseded 2026-08-28 by domain/etf_filter.ETF_CURATED (the union of the three
# drifted copies). Kept as an alias so nothing that referenced it breaks.
_ETF_CURATED = etf_filter.ETF_CURATED


def is_etf(symbol: str) -> bool:
    """True for an NSE ETF / fund unit. Delegates to the shared STOCK-ONLY filter.

    Was a local copy with its own curated list until 2026-08-28, when the three copies were found
    to have DRIFTED: this module's list and insider/pledge's differed by 20 names, so insider and
    pledge were missing GOLDSHARE / LIQUIDCASE / LIQUIDADD and 14 others — none of which match any
    name pattern. See ``domain/etf_filter`` for the consolidation and the ISIN layer.

    Prefer ``etf_filter.is_non_equity(symbol, instrument_key)`` at any call site that has the
    instrument key: names alone cannot catch a fund whose ticker looks like an ordinary stock.
    """
    return etf_filter.is_etf_symbol(symbol)


def is_informed(category: str) -> bool:
    """True for the informed personCategory set (promoter/director/KMP/relative)."""
    c = str(category or "").lower()
    return any(k in c for k in _INFORMED)


def is_open_market_buy(transaction_type: str, acq_mode: str) -> bool:
    """True for an open-MARKET BUY (excludes ESOP, off-market, gift, pledge, inter-se, etc.)."""
    t = str(transaction_type or "").lower()
    m = str(acq_mode or "").lower()
    return ("buy" in t) and ("market" in m) and ("off" not in m)


def _fnum(x: Any) -> float | None:
    try:
        return float(str(x).replace(",", ""))
    except (TypeError, ValueError):
        return None


def qualifies_leg(row: dict[str, Any]) -> bool:
    """True if one raw PIT disclosure leg is an informed OPEN-MARKET BUY with shares > 0.

    The per-leg VALUE gate (>= Rs 5L) is NOT applied here — the NSE feed's filer-entered value
    is unreliable (the ``corporates-pit-gg`` XBRL showed Rs 1 for a 10M-share buy), so value is
    computed as shares × reaction-close in ``finalize_clusters`` once a price is available.
    """
    if not is_informed(row.get("person_category")):
        return False
    if not is_open_market_buy(row.get("transaction_type"), row.get("acq_mode")):
        return False
    return (_fnum(row.get("shares")) or 0.0) > 0.0


def _best_category(cats: set[str]) -> str:
    low = {c.lower() for c in cats}
    if any("promoter" in c for c in low):
        return "promoter"
    if any("director" in c for c in low):
        return "director"
    return "kmp/rel"


def aggregate_legs(
    rows: Sequence[dict[str, Any]],
    min_buyers: int = MIN_BUYERS,
) -> dict[str, list[dict[str, Any]]]:
    """PURE pass 1 (pre-price): group qualifying informed open-market buy legs by symbol.

    Returns ``{symbol: [leg, ...]}`` for symbols with ``>= min_buyers`` legs (leg =
    ``{shares, category, dpct}``). NO value gate yet — that needs a price (``finalize_clusters``).
    Counts legs (not distinct names), matching the validated backtest so live cannot drift.
    """
    by: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        if not qualifies_leg(r):
            continue
        sym = str(r.get("symbol") or "").strip().upper()
        if not sym or is_etf(sym):
            continue
        by.setdefault(sym, []).append({
            "shares": _fnum(r.get("shares")) or 0.0,
            "category": str(r.get("person_category") or ""),
            "dpct": (_fnum(r.get("after_pct")) or 0.0) - (_fnum(r.get("bef_pct")) or 0.0),
        })
    return {s: legs for s, legs in by.items() if len(legs) >= min_buyers}


def finalize_clusters(
    legs_by_symbol: dict[str, list[dict[str, Any]]],
    price_by_symbol: dict[str, float],
    min_buyers: int = MIN_BUYERS,
    min_leg_value: float = MIN_LEG_VALUE,
) -> dict[str, dict[str, Any]]:
    """PURE pass 2 (post-price): apply the per-leg value gate as value = shares × reaction-close,
    then keep symbols still holding ``>= min_buyers`` qualifying legs (the cluster gate).

    This preserves the validated backtest semantics (old ``secVal`` ~= shares × transaction price)
    while ignoring the unreliable filer value. Returns ``{symbol: {n_buyers, total_val, dpct,
    category}}`` — the exact shape ``insider_signal_service.build_candidates`` consumes.
    """
    out: dict[str, dict[str, Any]] = {}
    for sym, legs in legs_by_symbol.items():
        px = price_by_symbol.get(sym)
        if not px or px <= 0:
            continue
        kept = [lg for lg in legs if lg["shares"] * px >= min_leg_value]
        if len(kept) < min_buyers:
            continue
        out[sym] = {
            "n_buyers": len(kept),
            "total_val": round(sum(lg["shares"] * px for lg in kept), 2),
            "dpct": round(max(lg["dpct"] for lg in kept), 4),
            "category": _best_category({lg["category"] for lg in kept}),
        }
    return out


def atr14(bars: Sequence[Sequence[float]]) -> list[float | None]:
    """SMA-of-TR over 14 bars (o[i] set for i>=13). Matches the validated backtest ATR.
    bars = [date,o,h,l,c,v]."""
    h = [b[2] for b in bars]
    l = [b[3] for b in bars]
    c = [b[4] for b in bars]
    if not c:
        return []
    tr = [h[0] - l[0]]
    for i in range(1, len(c)):
        tr.append(max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])))
    out: list[float | None] = [None] * len(c)
    s = 0.0
    for i in range(len(tr)):
        s += tr[i]
        if i >= ATR_WINDOW:
            s -= tr[i - ATR_WINDOW]
        if i >= ATR_WINDOW - 1:
            out[i] = s / ATR_WINDOW
    return out


def turnover_20d_cr(closes: Sequence[float], vols: Sequence[float], i: int) -> float:
    """Trailing 20-session mean of close×volume EXCLUDING day ``i``, in crore (matches backtest)."""
    if i < 20:
        return 0.0
    w = [closes[j] * vols[j] for j in range(i - 20, i)]
    return (sum(w) / len(w)) / 1e7 if w else 0.0


def passes_insider_gates(turnover_cr: float, close: float, symbol: str,
                         turnover_min_cr: float = TURNOVER_MIN_CR,
                         price_min: float = PRICE_MIN) -> bool:
    """Per-symbol liquidity/price/ETF gate (the cluster + category + value gate is applied in
    ``aggregate_clusters``). NO upper turnover cap — see TURNOVER_MIN_CR note."""
    return (turnover_cr >= turnover_min_cr
            and close >= price_min
            and not is_etf(symbol))


def macro_gate_ok(b200: float | None, nifty_close: float | None, nifty_ma: float | None,
                  b200_min: float = B200_MIN) -> bool:
    """CHANNEL-LEVEL double macro gate: breadth b200 > floor AND Nifty > its 100-day MA.
    Fail-CLOSED: if either input is missing/unreadable, returns False (no entries) — an
    unreadable regime must never silently ENABLE trading."""
    if b200 is None or nifty_close is None or nifty_ma is None:
        return False
    return float(b200) > b200_min and float(nifty_close) > float(nifty_ma)


def select_for_slots(candidates: Sequence[dict[str, Any]], open_count: int,
                     max_slots: int) -> list[dict[str, Any]]:
    """Top-(free) candidates by cluster strength: more buyers first, then larger total value.
    ``free = max(0, max_slots - open_count)``. Pure — places no orders. (Slot priority only
    binds when same-day clusters exceed free slots — rare at ~20 signals/yr.)"""
    free = max(0, max_slots - open_count)
    if free <= 0:
        return []
    ranked = sorted(candidates,
                    key=lambda c: (-int(c.get("n_buyers", 0)), -float(c.get("total_val", 0.0))))
    return ranked[:free]
