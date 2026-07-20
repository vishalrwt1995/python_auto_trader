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
_ETF_CURATED = frozenset({
    "MON100", "MOM100", "MOM50", "ICICIB22", "LIQUIDBEES", "NASDAQ", "SETFNIF50", "SETFGOLD",
    "CPSEETF", "PSUBANK", "GOLDBEES", "NIFTYBEES", "BANKBEES",
})


def is_etf(symbol: str) -> bool:
    s = str(symbol).strip().upper()
    return s.endswith("BEES") or "ETF" in s or s in _ETF_CURATED


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


def qualifies_leg(row: dict[str, Any], min_leg_value: float = MIN_LEG_VALUE) -> bool:
    """True if one raw PIT disclosure row is a qualifying informed open-market buy >= min value."""
    if not is_informed(row.get("person_category")):
        return False
    if not is_open_market_buy(row.get("transaction_type"), row.get("acq_mode")):
        return False
    val = _fnum(row.get("sec_val")) or _fnum(row.get("buy_value")) or 0.0
    return val >= min_leg_value


def _best_category(cats: set[str]) -> str:
    low = {c.lower() for c in cats}
    if any("promoter" in c for c in low):
        return "promoter"
    if any("director" in c for c in low):
        return "director"
    return "kmp/rel"


def aggregate_clusters(
    rows: Sequence[dict[str, Any]],
    min_buyers: int = MIN_BUYERS,
    min_leg_value: float = MIN_LEG_VALUE,
) -> dict[str, dict[str, Any]]:
    """PURE: aggregate one day's raw PIT disclosure rows into per-symbol CLUSTERS.

    Keeps only qualifying informed open-market buy legs (``qualifies_leg``), groups by symbol,
    counts qualifying legs (``n_buyers`` — matches the validated backtest which counted legs,
    not distinct names, so live cannot drift), sums leg value, keeps the max holding-% delta,
    and returns only symbols with ``n_buyers >= min_buyers`` (the cluster gate).

    Returns ``{symbol: {n_buyers, total_val, dpct, category}}``.
    """
    agg: dict[str, dict[str, Any]] = {}
    for r in rows:
        if not qualifies_leg(r, min_leg_value):
            continue
        sym = str(r.get("symbol") or "").strip().upper()
        if not sym or is_etf(sym):
            continue
        a = agg.setdefault(sym, {"n_buyers": 0, "total_val": 0.0, "dpct": 0.0, "cats": set()})
        a["n_buyers"] += 1
        a["total_val"] += (_fnum(r.get("sec_val")) or _fnum(r.get("buy_value")) or 0.0)
        db = (_fnum(r.get("after_pct")) or 0.0) - (_fnum(r.get("bef_pct")) or 0.0)
        a["dpct"] = max(a["dpct"], db)
        a["cats"].add(str(r.get("person_category") or ""))
    out: dict[str, dict[str, Any]] = {}
    for sym, a in agg.items():
        if a["n_buyers"] >= min_buyers:
            out[sym] = {"n_buyers": a["n_buyers"], "total_val": round(a["total_val"], 2),
                        "dpct": round(a["dpct"], 4), "category": _best_category(a["cats"])}
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
