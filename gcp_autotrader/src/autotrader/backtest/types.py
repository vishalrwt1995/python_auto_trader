"""Core data types for the backtester. All immutable-by-convention dataclasses.

The wire format between the data layer, the engine, and the simulator. Keep
this module dependency-free (pure stdlib) so it loads in any context.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Literal


# ── Bar / candle ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Bar:
    """Single OHLCV bar. ts is ISO-8601 (with timezone) for determinism.

    Convention: all bars are CLOSED — `ts` is the bar's open time, OHLC reflects
    the full interval [ts, ts+timeframe). Engine never reads a bar before its
    close — enforces no-look-ahead.
    """
    symbol: str
    ts: str            # ISO-8601, sortable
    open: float
    high: float
    low: float
    close: float
    volume: float
    timeframe: str     # "15m" | "1d" | "5m"


# ── Orders / fills ────────────────────────────────────────────────────────


OrderSide = Literal["BUY", "SELL"]
OrderType = Literal["MARKET", "LIMIT", "STOP", "STOP_LIMIT"]


class OrderStatus(str, Enum):
    PENDING = "PENDING"      # accepted by sim, awaiting fill
    FILLED = "FILLED"
    PARTIAL = "PARTIAL"      # not used yet — full-fill model
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


@dataclass
class Order:
    """A simulated order. Mutable state (status/filled_*) updates as the engine
    matches it against incoming bars."""
    order_id: str
    symbol: str
    side: OrderSide
    qty: int
    order_type: OrderType
    limit_price: float = 0.0       # for LIMIT / STOP_LIMIT
    stop_price: float = 0.0        # for STOP / STOP_LIMIT
    placed_ts: str = ""            # ISO-8601 (bar ts when placed)
    parent_tag: str = ""           # links exits to their entry (position_tag)
    purpose: str = "ENTRY"         # "ENTRY" | "EXIT_SL" | "EXIT_TARGET" | "EXIT_FSM" | "EOD"
    status: OrderStatus = OrderStatus.PENDING
    filled_price: float = 0.0
    filled_qty: int = 0
    filled_ts: str = ""
    reject_reason: str = ""


@dataclass
class Fill:
    """A simulated fill — one bar's resolution of one order."""
    order_id: str
    symbol: str
    side: OrderSide
    qty: int
    price: float
    ts: str
    parent_tag: str
    costs: float = 0.0          # all-in friction (commissions + taxes + slippage cost)


# ── Positions ─────────────────────────────────────────────────────────────


@dataclass
class Position:
    """An open simulated position. Mirrors the live PositionView shape so the
    same exit-FSM code can drive it.

    Mutable fields (entry_*, exits_*, pnl_*) are updated by the engine as
    bars are consumed. SL/target start as the planned values; the FSM may
    move SL via `current_sl`.
    """
    tag: str
    symbol: str
    side: OrderSide
    qty: int
    setup: str                  # "BREAKOUT" | "MEAN_REVERSION" | etc.
    is_swing: bool

    # Entry
    entry_price: float
    entry_ts: str
    entry_atr: float
    entry_regime: str

    # Risk plan
    initial_sl: float           # the planned SL at entry — never mutates
    target: float               # planned target — never mutates
    sl_dist: float              # |entry - initial_sl|

    # Mutable FSM state
    current_sl: float = 0.0     # may move via FSM (CONFIRMED give-back, RUNNER trail)
    fsm_state: str = "INITIAL"  # mirrors ExitState; string for serialization
    best_price: float = 0.0
    peak_mfe_r: float = 0.0
    confirm_started_epoch: float = 0.0

    # Exit
    exit_price: float = 0.0
    exit_ts: str = ""
    exit_reason: str = ""

    # P&L (computed at exit, gross of costs is gross_pnl, net is net_pnl)
    gross_pnl: float = 0.0
    costs: float = 0.0
    net_pnl: float = 0.0
    realized_r: float = 0.0     # net_pnl / (sl_dist * qty), signed

    # Diagnostics
    mfe_r: float = 0.0          # max favorable excursion (R)
    mae_r: float = 0.0          # max adverse excursion (R, negative)
    bars_held: int = 0


@dataclass
class SimTrade:
    """A closed simulated trade — the row that goes into the trade journal CSV.

    This is the analytics-friendly flat shape; `Position` is the runtime carrier."""
    trade_id: str
    symbol: str
    side: OrderSide
    qty: int
    setup: str
    is_swing: bool

    entry_ts: str
    entry_price: float
    exit_ts: str
    exit_price: float

    initial_sl: float
    target: float
    sl_dist: float

    gross_pnl: float
    costs: float
    net_pnl: float
    realized_r: float

    mfe_r: float
    mae_r: float
    bars_held: int

    exit_reason: str
    regime_at_entry: str
    score_at_entry: float = 0.0
    blocked_reason: str = ""    # always "" for executed trades; useful for "what-if" replays


# ── Engine snapshot types ─────────────────────────────────────────────────


@dataclass
class EquityPoint:
    """One sample of the equity curve. Engine emits one per bar tick."""
    ts: str
    equity: float
    cash: float
    positions_value: float
    drawdown_pct: float
    open_positions: int


@dataclass
class BacktestResult:
    """Top-level result returned by the engine. `trades` is the journal,
    `equity_curve` is per-bar, `metrics` is the summary, `meta` records
    config + run hash for reproducibility."""
    trades: list[SimTrade]
    equity_curve: list[EquityPoint]
    metrics: dict[str, float] = field(default_factory=dict)
    meta: dict[str, str] = field(default_factory=dict)
    per_setup: dict[str, dict[str, float]] = field(default_factory=dict)
    per_regime: dict[str, dict[str, float]] = field(default_factory=dict)
    per_setup_regime: dict[str, dict[str, float]] = field(default_factory=dict)


__all__ = [
    "Bar",
    "Order",
    "OrderSide",
    "OrderType",
    "OrderStatus",
    "Fill",
    "Position",
    "SimTrade",
    "EquityPoint",
    "BacktestResult",
]
