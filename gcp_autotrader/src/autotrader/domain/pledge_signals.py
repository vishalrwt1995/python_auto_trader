"""PLEDGE (promoter pledge-release) channel — pure signal gates + slot selection.

Signal: an NSE SEBI-PIT disclosure of a promoter PLEDGE REVOKE (un-pledging shares = deleveraging,
a bullish informed action). REUSES the SAME feed + BQ table as the insider channel
(``nse_insider_daily``) — the ingest already writes every transaction leg (buy / sell / pledge /
revoke) verbatim, so pledge only needs a different read filter (``transaction_type ~ revoke`` +
promoter). Entry the NEXT session open after public disclosure (never the private txn date — no
look-ahead). Distinct from insider cluster-buys (15.6% temporal overlap; the edge survives with NO
insider buy nearby — validated additive).

Shared by the live pledge trading service AND the parity backtest (scripts/redesign/pledge_*.py) so
live selection/sizing cannot drift from the validated config (same discipline as insider/delivery/
pead signals). All functions pure + side-effect-free. Self-contained (copies the small pure helpers
rather than importing insider_signals) to keep the channel isolated.

Validated FINAL config (2026-07-21; survivorship-safe incl. delisted; IS<=2020 / OOS>=2021; full
Upstox cost + slippage; robust across the completeness sweep — stop / gate / universe / exit all
tested):
  promoter pledge-revoke  ·  px > 200DMA (uptrend confirm — the falling-knife filter; magnitude /
  release-% filters were KILLED for failing OOS)  ·  DOUBLE MACRO GATE: breadth b200 > 50 AND
  Nifty > 100-day-MA  ·  turnover_20d >= Rs 25cr (liquid/fillable)  ·  price >= Rs 30  ·  hold 60
  sessions  ·  10 slots  ·  ATR14 x 2.0 protective stop, FIXED-hold (NO trail — a trail whipsaws
  this drift edge)  ·  1.5% equity risk  ·  10% notional cap/position (no-leverage; harvests the
  otherwise-idle ~40% capital into more small decorrelated positions).
  -> +25% CAGR (bull-inflated; honest ~15-20% normal regimes) / -11.5% maxDD / Calmar 2.18
     (IS 1.96 / OOS 4.17) / ~39 trades/yr (declining) / 15.6% insider overlap (additive diversifier).
Sizing / daily-breaker reuse domain/pead_book (generic). The tighter 2.0xATR stop + px>200DMA +
25cr turnover were the robust improvers over the un-engineered baseline (Calmar 1.26 -> 2.18).
The macro double-gate is CHANNEL-LEVEL (evaluated once per scan) — bad regime => no entries.
"""
from __future__ import annotations

from autotrader.domain import etf_filter

from typing import Any, Sequence

# ── validated thresholds (env-overridable in settings; these are the defaults) ──
TURNOVER_MIN_CR = 25.0               # 20d-mean turnover floor (crore) — liquid/fillable names only
PRICE_MIN = 30.0
MA_DAYS = 200                        # px must be above its 200-day SMA (uptrend / anti-falling-knife)
MAX_HOLD_DAYS = 60                   # fixed-hold exit (drift edge; a trail whipsaws it)
ATR_SL_MULT = 2.0                    # protective disaster stop only (not a trail) — tighter than 2.5
ATR_WINDOW = 14
MIN_BARS = MA_DAYS + 1               # need 200d SMA + ATR14 + 20d turnover + a prior close
MAX_POSITIONS = 10
# double macro gate (channel-level, evaluated once per scan day)
B200_MIN = 50.0                      # market breadth (% of universe > EMA200) floor
NIFTY_MA_DAYS = 100                  # Nifty must be above its 100-day SMA

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

def is_promoter(category: str) -> bool:
    """True for a promoter / promoter-group filer. The 'any-promoter' set is the validated one —
    promoters-only was much weaker OOS, promoter-group-only slightly weaker; the union wins."""
    return "promoter" in str(category or "").lower()


def is_pledge_revoke(transaction_type: str) -> bool:
    """True for a pledge REVOKE (un-pledge / release). Matches ``Pledge Revoke`` (live pit-gg feed)
    and ``Revokation of Pledge`` (legacy) via the ``revok`` substring. Excludes pledge CREATION
    (bearish) and INVOCATION (forced sale — bearish)."""
    return "revok" in str(transaction_type or "").lower()


def _fnum(x: Any) -> float | None:
    try:
        return float(str(x).replace(",", ""))
    except (TypeError, ValueError):
        return None


def qualifies_revoke(row: dict[str, Any]) -> bool:
    """True if one raw PIT disclosure leg is a promoter pledge-revoke with shares > 0.
    NO value / release-% gate (both failed OOS in the grind — magnitude does not predict)."""
    if not is_promoter(row.get("person_category")):
        return False
    if not is_pledge_revoke(row.get("transaction_type")):
        return False
    return (_fnum(row.get("shares")) or 0.0) > 0.0


def _best_category(cats: set[str]) -> str:
    low = {c.lower() for c in cats}
    if any("group" in c for c in low) and not any(c == "promoters" for c in low):
        return "promoter group"
    return "promoter"


def aggregate_revokes(rows: Sequence[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """PURE: group qualifying promoter pledge-revoke legs by symbol. Unlike insider (which needs a
    >=2 CLUSTER), a single promoter revoke IS the signal — so every symbol with >=1 qualifying leg
    is a candidate. Returns ``{symbol: {n_revokes, category}}``."""
    by: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        if not qualifies_revoke(r):
            continue
        sym = str(r.get("symbol") or "").strip().upper()
        if not sym or is_etf(sym):
            continue
        by.setdefault(sym, []).append({"category": str(r.get("person_category") or "")})
    return {s: {"n_revokes": len(legs), "category": _best_category({lg["category"] for lg in legs})}
            for s, legs in by.items()}


def sma(closes: Sequence[float], n: int, i: int) -> float | None:
    """Simple moving average of the last ``n`` closes ending at index ``i`` (inclusive)."""
    if i < n - 1:
        return None
    w = closes[i - n + 1:i + 1]
    return sum(w) / len(w) if len(w) == n else None


def atr14(bars: Sequence[Sequence[float]]) -> list[float | None]:
    """SMA-of-TR over 14 bars (set for i>=13). Matches the validated backtest ATR. bars=[date,o,h,l,c,v]."""
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


def passes_pledge_gates(turnover_cr: float, close: float, above_200dma: bool, symbol: str,
                        turnover_min_cr: float = TURNOVER_MIN_CR,
                        price_min: float = PRICE_MIN) -> bool:
    """Per-symbol gate: liquidity floor (>=25cr) + price floor + px>200DMA (uptrend/anti-falling-
    knife) + not an ETF. The macro double-gate is applied once per scan by the trading service."""
    return (turnover_cr >= turnover_min_cr
            and close >= price_min
            and bool(above_200dma)
            and not is_etf(symbol))


def macro_gate_ok(b200: float | None, nifty_close: float | None, nifty_ma: float | None,
                  b200_min: float = B200_MIN) -> bool:
    """CHANNEL-LEVEL double macro gate: breadth b200 > floor AND Nifty > its 100-day MA.
    Fail-CLOSED: if any input is missing/unreadable, returns False (no entries) — an unreadable
    regime must never silently ENABLE trading."""
    if b200 is None or nifty_close is None or nifty_ma is None:
        return False
    return float(b200) > b200_min and float(nifty_close) > float(nifty_ma)


def select_for_slots(candidates: Sequence[dict[str, Any]], open_count: int,
                     max_slots: int) -> list[dict[str, Any]]:
    """Top-(free) candidates by LIQUIDITY (largest 20d turnover first) — magnitude/release-% were
    killed as OOS-invalid, so liquidity is the safe, fillable slot-priority tiebreak. ``free =
    max(0, max_slots - open_count)``. Pure — places no orders. (Priority only binds when same-day
    revokes exceed free slots — rare.)"""
    free = max(0, max_slots - open_count)
    if free <= 0:
        return []
    ranked = sorted(candidates, key=lambda c: -float(c.get("turnover_cr", 0.0)))
    return ranked[:free]
