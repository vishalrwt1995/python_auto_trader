"""Slippage models for the backtester.

Slippage is the gap between the bar-derived theoretical fill price and what
you'd actually have got in production — adverse fill that comes from spread,
queue position, and impact. We model it conservatively (always against you).

Three pluggable models:
  * `NoSlippage` — for pure debugging.
  * `FixedBps`   — N basis points adverse, regardless of bar.
  * `BarRangePct`— X% of (high-low) adverse, capped at Y bps. The default —
                   matches the empirical observation that fills on volatile
                   bars are worse than on quiet bars.

All models return *adverse* slippage in ₹/share, signed so that:
  * BUY fills get adjusted UP by `slippage_per_share` (you pay more)
  * SELL fills get adjusted DOWN (you receive less)

The engine applies this consistently — see `account.SimAccount.fill_at`.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from autotrader.backtest.types import Bar


class SlippageModel(Protocol):
    """Interface for slippage models. `quote_fill_price` returns the actual
    fill price the simulator should use, given the theoretical price and
    the bar context."""

    def adjust(self, *, theoretical: float, side: str, bar: Bar) -> float:
        """Return adjusted fill price (≥ theoretical for BUY, ≤ for SELL)."""
        ...


@dataclass(frozen=True)
class NoSlippage:
    def adjust(self, *, theoretical: float, side: str, bar: Bar) -> float:
        return theoretical


@dataclass(frozen=True)
class FixedBps:
    """Constant bps adverse fill. bps = 5 ⇒ 0.05%."""
    bps: float = 5.0

    def adjust(self, *, theoretical: float, side: str, bar: Bar) -> float:
        adj = theoretical * (self.bps / 10_000.0)
        if side.upper() == "BUY":
            return round(theoretical + adj, 2)
        return round(theoretical - adj, 2)


@dataclass(frozen=True)
class BarRangePct:
    """Adverse fill = `pct_of_range × (high - low)`, capped at `cap_bps`.

    Defaults model "you usually pay roughly half the spread plus a bit; on
    wide-range bars you pay more, but never more than 25 bps total."
    """
    pct_of_range: float = 0.10     # 10% of bar range, against you
    cap_bps: float = 25.0          # never worse than 25 bps
    floor_bps: float = 1.0         # at least 1 bp (you never get a free fill)

    def adjust(self, *, theoretical: float, side: str, bar: Bar) -> float:
        bar_range = max(0.0, bar.high - bar.low)
        adj_from_range = bar_range * self.pct_of_range
        cap = theoretical * (self.cap_bps / 10_000.0)
        floor = theoretical * (self.floor_bps / 10_000.0)
        adj = max(floor, min(cap, adj_from_range))
        if side.upper() == "BUY":
            return round(theoretical + adj, 2)
        return round(theoretical - adj, 2)


def default_model() -> SlippageModel:
    """The default for fresh backtests. Recalibrate after you have ≥50 real
    fills (`calibrate_from_fills` — TODO when fills are available)."""
    return BarRangePct()


__all__ = ["SlippageModel", "NoSlippage", "FixedBps", "BarRangePct", "default_model"]
