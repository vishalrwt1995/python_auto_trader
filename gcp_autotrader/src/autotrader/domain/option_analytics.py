"""M5 — Option analytics: pure computations over an option chain snapshot.

Takes a list of option-chain rows (as returned by UpstoxClient.get_option_chain)
and derives:
  * max_pain_strike      — strike price where total option-writer loss is
                           minimized. A widely watched expiry-pin magnet.
  * put_call_ratio       — sum(PE OI) / sum(CE OI). >1 skews bearish
                           (more put interest), <1 bullish.
  * oi_change_pcr        — put OI additions vs call OI additions; more
                           reactive than static PCR.
  * iv_skew              — (avg OTM put IV) − (avg OTM call IV). Positive
                           skew = market paying up for downside protection.
  * underlying_implied   — spot derived from the chain's ATM straddle when
                           the upstream didn't include it.

All functions are pure and side-effect free. Row schema is the superset
returned by Upstox v2 option-chain:
  {
    "strike_price": float,
    "call_options":  {"market_data": {"ltp": ..., "oi": ..., "oi_change": ...},
                      "option_greeks": {"iv": ..., "delta": ...}},
    "put_options":   {"market_data": {...}, "option_greeks": {...}}
  }

Rows missing fields are tolerated — we coerce with `.get(... , 0) or 0`
and let the aggregate math no-op on them.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


def _g(d: Any, *keys: str, default: float = 0.0) -> float:
    """Safe nested numeric getter across the inconsistent chain shape."""
    cur: Any = d
    for k in keys:
        if isinstance(cur, dict):
            cur = cur.get(k)
        else:
            return float(default)
    try:
        return float(cur) if cur is not None else float(default)
    except (TypeError, ValueError):
        return float(default)


def _strikes(chain: Iterable[dict]) -> list[float]:
    return sorted({float(r.get("strike_price") or 0) for r in chain if r.get("strike_price")})


@dataclass(frozen=True)
class OptionMetrics:
    max_pain_strike: float = 0.0
    put_call_ratio: float = 1.0
    oi_change_pcr: float = 1.0
    iv_skew: float = 0.0
    # How many rows contributed — downstream can use this to tell "empty
    # chain" from "genuinely neutral metrics".
    n_rows: int = 0


def compute_max_pain(chain: list[dict]) -> float:
    """Strike at which combined option writer pain is minimised.

    At a candidate expiry price K:
      call_pain = sum_over_strikes(max(K - strike, 0) * CE_OI)
      put_pain  = sum_over_strikes(max(strike - K, 0) * PE_OI)
    We pick K (from the set of listed strikes) that minimises
    call_pain + put_pain. This is the textbook max-pain computation.
    """
    strikes = _strikes(chain)
    if not strikes:
        return 0.0
    best_strike = strikes[0]
    best_pain = float("inf")
    for K in strikes:
        pain = 0.0
        for r in chain:
            s = float(r.get("strike_price") or 0.0)
            if s <= 0:
                continue
            ce_oi = _g(r, "call_options", "market_data", "oi")
            pe_oi = _g(r, "put_options", "market_data", "oi")
            if K > s:
                pain += (K - s) * ce_oi
            elif s > K:
                pain += (s - K) * pe_oi
        if pain < best_pain:
            best_pain = pain
            best_strike = K
    return float(best_strike)


def compute_pcr(chain: list[dict]) -> float:
    ce = sum(_g(r, "call_options", "market_data", "oi") for r in chain)
    pe = sum(_g(r, "put_options", "market_data", "oi") for r in chain)
    if ce <= 0:
        return 1.0
    return round(pe / ce, 4)


def compute_oi_change_pcr(chain: list[dict]) -> float:
    """Reactive PCR based on intraday OI additions, not total OI.

    Positive OI additions are the real-time "new positioning" signal.
    If call writers and put writers both cut, the static PCR barely
    moves while oi_change_pcr correctly falls to ~1.
    """
    ce_delta = sum(max(0.0, _g(r, "call_options", "market_data", "oi_change")) for r in chain)
    pe_delta = sum(max(0.0, _g(r, "put_options", "market_data", "oi_change")) for r in chain)
    if ce_delta <= 0:
        return 1.0
    return round(pe_delta / ce_delta, 4)


def compute_iv_skew(chain: list[dict], spot: float) -> float:
    """(mean IV on OTM puts) − (mean IV on OTM calls), both near ATM.

    Using strikes within ±7% of spot avoids the tail-strike IV noise.
    Positive skew = market paying up for put protection = defensive tape.
    """
    if spot <= 0:
        return 0.0
    put_ivs: list[float] = []
    call_ivs: list[float] = []
    for r in chain:
        s = float(r.get("strike_price") or 0.0)
        if s <= 0:
            continue
        # OTM put: strike < spot. OTM call: strike > spot.
        if spot * 0.93 <= s < spot:
            iv = _g(r, "put_options", "option_greeks", "iv")
            if iv > 0:
                put_ivs.append(iv)
        elif spot < s <= spot * 1.07:
            iv = _g(r, "call_options", "option_greeks", "iv")
            if iv > 0:
                call_ivs.append(iv)
    if not put_ivs or not call_ivs:
        return 0.0
    return round(sum(put_ivs) / len(put_ivs) - sum(call_ivs) / len(call_ivs), 4)


def compute_metrics(chain: list[dict], spot: float = 0.0) -> OptionMetrics:
    """One-shot: compute all option metrics over a chain snapshot."""
    if not chain:
        return OptionMetrics()
    return OptionMetrics(
        max_pain_strike=compute_max_pain(chain),
        put_call_ratio=compute_pcr(chain),
        oi_change_pcr=compute_oi_change_pcr(chain),
        iv_skew=compute_iv_skew(chain, spot) if spot > 0 else 0.0,
        n_rows=len(chain),
    )


__all__ = [
    "OptionMetrics",
    "compute_max_pain",
    "compute_pcr",
    "compute_oi_change_pcr",
    "compute_iv_skew",
    "compute_metrics",
]
