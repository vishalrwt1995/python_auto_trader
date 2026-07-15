"""DELIVERY-accumulation channel — pure signal gates + slot selection.

Shared by the live delivery trading service AND the parity backtest (scripts/redesign/delivery_final.py)
so live sizing/selection can't drift from the validated config (same discipline as pead_signals/pead_book).
All functions pure + side-effect-free.

Validated 2026-07-14 (STOCKS-ONLY — ETFs excluded), survivorship-safe, size-aware fills, IS≤2022/OOS≥2023:
  deliv_pct ≥ 75  ·  25–50cr 20d-median turnover band  ·  price ≥ ₹30  ·  hold 20d  ·  5 slots  ·
  ATR14×2.5 stop, arm 1.75R / trail 1.0R (domain/swing_exit geometry)  ·  rank by delivery-%.
  → ~11.8% CAGR / Calmar 0.86 / −13.7% DD / 6-of-7 positive years; beats a pure-beta (random-entry)
  control by 10–40 pts both halves (genuine stock-accumulation alpha, NOT ETF-beta or dip mean-reversion).
Sizing / daily-breaker reuse domain/pead_book (generic). Costs/exit reuse the shared Upstox/swing_exit code.
"""
from __future__ import annotations

from typing import Any, Sequence

# ── validated thresholds (env-overridable in settings; these are the defaults) ──
DELIV_MIN = 75.0                       # delivery-% floor (plateau 70–75; ≥78 drops off)
TURNOVER_MIN_CR, TURNOVER_MAX_CR = 25.0, 50.0   # 20d-median turnover band (crore)
PRICE_MIN = 30.0
MAX_HOLD_DAYS = 20
ATR_SL_MULT, ACTIVATE_R, TRAIL_R = 2.5, 1.75, 1.0
ATR_WINDOW = 14
MIN_BARS = 22                          # need 20d turnover + ATR + a prior close

# ── ETF exclusion ─────────────────────────────────────────────────────────────
# The edge is STOCK accumulation. NSE lists ETFs in the EQ series and they carry
# structurally ~100% delivery-% every day (no churn) + drift with their index — in
# the backtest they were 13% of trades but a −6% net DRAG. Name filter catches them
# (BEES suffix / ETF substring / curated pure-name ETFs). The trading service adds a
# belt-and-suspenders check: intersect candidates with the equity instrument master.
_ETF_CURATED = frozenset({
    "MON100", "MOM100", "MOM50", "MOM30", "ICICIB22", "LIQUID", "LIQUIDCASE", "LIQUIDADD",
    "NASDAQ", "N100", "MASPTOP50", "MAFANG", "SETFNIF50", "SETFNIFBK", "SETFNIFTY", "SETFGOLD",
    "QGOLDHALF", "QNIFTY", "GOLDSHARE", "HDFCLIQUID", "KOTAKNIFTY", "KOTAKGOLD", "AXISNIFTY",
    "AXISGOLD", "UTINIFTETF", "TATAGOLD", "GROWWGOLD", "CPSEETF", "PSUBANK",
})


def is_etf(symbol: str) -> bool:
    """True for NSE ETFs (excluded from the delivery stock universe)."""
    s = str(symbol).strip().upper()
    return s.endswith("BEES") or "ETF" in s or "IETF" in s or s in _ETF_CURATED


def atr14(bars: Sequence[Sequence[float]]) -> list[float | None]:
    """SMA-of-TR over 14 bars, aligned to ``bars`` (o[i] set for i≥13). EXACTLY matches the
    validated backtest's ATR (scripts/redesign/delivery_lock.py:atr14). bars = [date,o,h,l,c,v]."""
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
    """Trailing 20-session mean of close×volume EXCLUDING day ``i``, in crore. Matches the
    validated backtest (``mean(c[j]*v[j] for j in range(i-20, i)) / 1e7``)."""
    if i < 20:
        return 0.0
    w = [closes[j] * vols[j] for j in range(i - 20, i)]
    return (sum(w) / len(w)) / 1e7 if w else 0.0


def passes_delivery_gates(deliv_pct: float, turnover_cr: float, close: float, symbol: str,
                          deliv_min: float = DELIV_MIN,
                          turnover_min_cr: float = TURNOVER_MIN_CR,
                          turnover_max_cr: float = TURNOVER_MAX_CR,
                          price_min: float = PRICE_MIN) -> bool:
    """The full delivery entry gate (env-overridable thresholds). ETFs always excluded."""
    return (deliv_pct >= deliv_min
            and turnover_min_cr <= turnover_cr < turnover_max_cr
            and close >= price_min
            and not is_etf(symbol))


def select_for_slots(candidates: Sequence[dict[str, Any]], open_count: int,
                     max_slots: int) -> list[dict[str, Any]]:
    """Top-(free) candidates by delivery-% descending (the validated entry ranking).
    ``free = max(0, max_slots - open_count)``. Pure — places no orders."""
    free = max(0, max_slots - open_count)
    if free <= 0:
        return []
    ranked = sorted(candidates, key=lambda c: -float(c.get("deliv_pct", 0.0)))
    return ranked[:free]
