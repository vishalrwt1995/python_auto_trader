"""M4 — PortfolioBook: channel budgets + drawdown governors.

Central risk accounting. Two orthogonal jobs:

  1. Channel budgets — capital is partitioned between four "channels"
     (intraday 40% / swing 40% / positional 15% / hedge 5%). Each
     channel can allocate at most its share in R-at-risk across its
     open positions. Prevents one hot intraday day from starving the
     swing book or vice versa.

  2. Drawdown governors — daily, weekly, monthly rolling P&L vs
     capital. Crossing a threshold either THROTTLES position sizing
     (halves it) or HALTS new entries entirely. Recovers automatically
     once the next calendar period rolls over.

Pure dataclasses + pure functions. The service layer fetches the
inputs (open risk per channel, rolling pnls, capital) and calls
`check_can_open` before authorizing a new entry.

DESIGN.md §7 rationale: existing `max_daily_loss` is a single number
that's easy to blow past on a multi-position day — several positions
can all open simultaneously with combined max_loss > max_daily_loss.
The PortfolioBook fixes this by gating on COMMITTED risk, not just
realized loss.

Rollout: gated behind settings.runtime.use_portfolio_book_v1. When
OFF the service continues using only max_daily_loss (DESIGN.md §9).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class ChannelName(str, Enum):
    INTRADAY = "intraday"
    SWING = "swing"
    POSITIONAL = "positional"
    HEDGE = "hedge"


# Default capital allocation — the numbers from DESIGN.md §7. Swing gets
# equal weight to intraday because the historical data shows swing
# trades deliver higher per-trade R but at lower frequency; equal
# capital ≈ equal expected-R contribution.
DEFAULT_CHANNEL_PCT: dict[str, float] = {
    ChannelName.INTRADAY.value: 0.40,
    ChannelName.SWING.value: 0.40,
    ChannelName.POSITIONAL.value: 0.15,
    ChannelName.HEDGE.value: 0.05,
}


# DD governor thresholds — as a fraction of capital. Halt levels are
# intentionally lower than the historical max_daily_loss because the
# book tracks COMMITTED risk too; hitting 3% realized with another 2%
# committed = actual 5% exposure.
@dataclass(frozen=True)
class DdThresholds:
    daily_throttle_pct: float = 0.015   # 1.5% of capital → halve size
    daily_halt_pct: float = 0.030       # 3.0% of capital → no new entries
    weekly_halt_pct: float = 0.050      # 5% of capital in a rolling week
    monthly_halt_pct: float = 0.080     # 8% of capital in a rolling month


@dataclass(frozen=True)
class ChannelBudget:
    name: str
    pct_of_capital: float
    # Currently committed open R-at-risk (max_loss summed over open positions).
    open_risk: float = 0.0


@dataclass(frozen=True)
class DrawdownState:
    capital: float
    daily_pnl: float = 0.0       # realized today
    weekly_pnl: float = 0.0      # realized in last 7 calendar days
    monthly_pnl: float = 0.0     # realized in last 30 calendar days

    @property
    def daily_dd_pct(self) -> float:
        return abs(min(0.0, self.daily_pnl)) / self.capital if self.capital > 0 else 0.0

    @property
    def weekly_dd_pct(self) -> float:
        return abs(min(0.0, self.weekly_pnl)) / self.capital if self.capital > 0 else 0.0

    @property
    def monthly_dd_pct(self) -> float:
        return abs(min(0.0, self.monthly_pnl)) / self.capital if self.capital > 0 else 0.0


@dataclass(frozen=True)
class PortfolioBook:
    capital: float
    channels: dict[str, ChannelBudget]
    dd: DrawdownState
    thresholds: DdThresholds = field(default_factory=DdThresholds)

    def channel(self, name: str) -> ChannelBudget | None:
        return self.channels.get(str(name or "").strip().lower())

    def channel_budget_r(self, name: str) -> float:
        """Absolute R budget for a channel (capital × pct)."""
        ch = self.channel(name)
        if not ch:
            return 0.0
        return round(self.capital * ch.pct_of_capital, 2)


@dataclass(frozen=True)
class OpenDecision:
    allowed: bool
    reason: str            # "" when allowed
    size_multiplier: float = 1.0   # 1.0 = full size, 0.5 = throttled
    # Expose the book snapshot so log lines / attribution get the numbers.
    book: PortfolioBook | None = None


def build_book(
    capital: float,
    open_risk_by_channel: dict[str, float],
    daily_pnl: float = 0.0,
    weekly_pnl: float = 0.0,
    monthly_pnl: float = 0.0,
    channel_pct: dict[str, float] | None = None,
    thresholds: DdThresholds | None = None,
) -> PortfolioBook:
    pct = dict(channel_pct or DEFAULT_CHANNEL_PCT)
    channels: dict[str, ChannelBudget] = {}
    for name, p in pct.items():
        risk = float(open_risk_by_channel.get(name, 0.0) or 0.0)
        channels[name] = ChannelBudget(name=name, pct_of_capital=float(p), open_risk=risk)
    return PortfolioBook(
        capital=float(capital or 0.0),
        channels=channels,
        dd=DrawdownState(
            capital=float(capital or 0.0),
            daily_pnl=float(daily_pnl or 0.0),
            weekly_pnl=float(weekly_pnl or 0.0),
            monthly_pnl=float(monthly_pnl or 0.0),
        ),
        thresholds=thresholds or DdThresholds(),
    )


def check_can_open(
    book: PortfolioBook,
    channel: str,
    risk_amount: float,
) -> OpenDecision:
    """Decide whether a new entry on `channel` with `risk_amount` is allowed.

    Order of checks (first-fail wins):
      1. Monthly halt — serious multi-week bleed; review.
      2. Weekly halt — bad week; stop until next week.
      3. Daily halt — bad day; stop until tomorrow.
      4. Channel budget — this specific channel would exceed its cap.
      5. Daily throttle — warn + halve size (still ALLOWED, but smaller).

    Note 4 vs 5: channel-budget is a HARD block, the daily-throttle is
    SOFT (continues trading at 50%). That matches the "size down before
    you size out" principle from the original enhancement guide.
    """
    dd = book.dd
    t = book.thresholds

    if dd.monthly_dd_pct >= t.monthly_halt_pct:
        return OpenDecision(False, "portfolio_monthly_dd_halt", book=book)
    if dd.weekly_dd_pct >= t.weekly_halt_pct:
        return OpenDecision(False, "portfolio_weekly_dd_halt", book=book)
    if dd.daily_dd_pct >= t.daily_halt_pct:
        return OpenDecision(False, "portfolio_daily_dd_halt", book=book)

    ch_name = str(channel or "").strip().lower() or ChannelName.INTRADAY.value
    ch = book.channel(ch_name)
    if ch is None:
        # Unknown channel is fail-closed — if we start emitting a new
        # channel name without updating the registry, block until it's
        # properly allocated.
        return OpenDecision(False, "portfolio_unknown_channel", book=book)

    budget = book.channel_budget_r(ch_name)
    if ch.open_risk + float(risk_amount or 0.0) > budget:
        return OpenDecision(False, "portfolio_channel_budget_exceeded", book=book)

    # Soft throttle: daily DD over the throttle line halves sizing.
    if dd.daily_dd_pct >= t.daily_throttle_pct:
        return OpenDecision(True, "", size_multiplier=0.5, book=book)

    return OpenDecision(True, "", size_multiplier=1.0, book=book)


__all__ = [
    "ChannelName",
    "DEFAULT_CHANNEL_PCT",
    "DdThresholds",
    "ChannelBudget",
    "DrawdownState",
    "PortfolioBook",
    "OpenDecision",
    "build_book",
    "check_can_open",
]
