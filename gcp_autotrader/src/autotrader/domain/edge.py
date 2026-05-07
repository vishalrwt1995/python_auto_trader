"""M2 — Edge registry.

An Edge is a named, versioned rule-set that combines:
  * a setup family (BREAKOUT / PULLBACK / MEAN_REVERSION / OPEN_DRIVE /
    MOMENTUM / VWAP_REVERSAL — matches the existing StrategySetup IDs),
  * the regimes in which it's allowed to fire (the Playbook consumes this),
  * a "priors" handle that maps (regime, setup) → expected R / win-rate.
    The priors file lives in config/priors/; M3 plugs the backtest
    harness into these entries.

The registry is deliberately static at startup — no database, no hot
reload. Adding a new edge is a code change + review + deploy.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Edge:
    """A named trading edge. Immutable — mutate by registering a v2."""
    name: str
    version: str
    setup: str                    # e.g. "BREAKOUT", "PULLBACK"
    direction: str                # "LONG" | "SHORT" | "BOTH"
    allowed_regimes: tuple[str, ...]
    allowed_risk_modes: tuple[str, ...] = (
        "AGGRESSIVE",
        "NORMAL",
        "DEFENSIVE",
        # LOCKDOWN is opt-in only — an edge must list it explicitly.
    )
    # Priors key used by M3 to look up expected_R etc.
    priors_key: str = ""
    # Human-readable one-liner for attribution logs + dashboards.
    summary: str = ""


_REGISTRY: dict[str, Edge] = {}


def register(edge: Edge) -> None:
    """Register an edge. Last-write-wins by (name, version); duplicates log."""
    _REGISTRY[f"{edge.name}:{edge.version}"] = edge


def get(name: str, version: str = "latest") -> Edge | None:
    if version == "latest":
        # Latest = highest-sorting version string among registered edges with name.
        candidates = [(k, v) for k, v in _REGISTRY.items() if v.name == name]
        if not candidates:
            return None
        candidates.sort(key=lambda kv: kv[1].version, reverse=True)
        return candidates[0][1]
    return _REGISTRY.get(f"{name}:{version}")


def all_edges() -> list[Edge]:
    return list(_REGISTRY.values())


def reset_for_tests() -> None:
    """Only for tests — clears registry so test cases can build a fresh one."""
    _REGISTRY.clear()


# ──────────────────────────────────────────────────────────────────────────
# Default registrations. These match the setups the scanner currently emits.
# Regime lists here are INTENTIONALLY restrictive — a setup firing in a
# regime that doesn't appear below is blocked by the Playbook. This is the
# "hard-block replaces pass-through" change from DESIGN.md §5.
# ──────────────────────────────────────────────────────────────────────────


def _register_defaults() -> None:
    register(Edge(
        name="breakout_long",
        version="v1",
        setup="BREAKOUT",
        direction="LONG",
        allowed_regimes=("TREND_UP", "RECOVERY"),
        summary="Momentum breakout above N-bar high; only in bullish trend regimes.",
        priors_key="BREAKOUT:LONG",
    ))
    register(Edge(
        name="breakout_short",
        version="v1",
        setup="BREAKOUT",
        direction="SHORT",
        allowed_regimes=("TREND_DOWN",),
        summary="Breakdown below N-bar low; only in trend-down regime.",
        priors_key="BREAKOUT:SHORT",
    ))
    register(Edge(
        name="pullback_long",
        version="v1",
        setup="PULLBACK",
        direction="LONG",
        allowed_regimes=("TREND_UP", "RECOVERY"),
        summary="Pullback to EMA21 in an uptrend; buys the retrace.",
        priors_key="PULLBACK:LONG",
    ))
    register(Edge(
        name="pullback_short",
        version="v1",
        setup="PULLBACK",
        direction="SHORT",
        allowed_regimes=("TREND_DOWN",),
        summary="Pullback to EMA21 in a downtrend; sells the bounce.",
        priors_key="PULLBACK:SHORT",
    ))
    register(Edge(
        name="mean_reversion_long",
        version="v1",
        setup="MEAN_REVERSION",
        direction="LONG",
        # Mean-reversion plays the bounces best in PANIC / RANGE. Trend regimes
        # chew up mean-reversion signals (you're fading the dominant move).
        allowed_regimes=("RANGE", "CHOP", "PANIC", "RECOVERY"),
        summary="Bollinger/RSI extreme reversal long; capitulation bounces.",
        priors_key="MEAN_REVERSION:LONG",
    ))
    register(Edge(
        name="mean_reversion_short",
        version="v1",
        setup="MEAN_REVERSION",
        direction="SHORT",
        allowed_regimes=("RANGE", "CHOP"),
        summary="Overbought fade short; only in non-trending tape.",
        priors_key="MEAN_REVERSION:SHORT",
    ))
    register(Edge(
        name="open_drive_long",
        version="v1",
        setup="OPEN_DRIVE",
        direction="LONG",
        allowed_regimes=("TREND_UP", "RECOVERY"),
        summary="Opening-range breakout long; first-hour momentum.",
        priors_key="OPEN_DRIVE:LONG",
    ))
    register(Edge(
        name="open_drive_short",
        version="v1",
        setup="OPEN_DRIVE",
        direction="SHORT",
        allowed_regimes=("TREND_DOWN",),
        summary="Opening-range breakdown short.",
        priors_key="OPEN_DRIVE:SHORT",
    ))
    register(Edge(
        name="momentum_long",
        version="v1",
        setup="MOMENTUM",
        direction="LONG",
        allowed_regimes=("TREND_UP", "RECOVERY"),
        summary="Continuation momentum long on ADX expansion.",
        priors_key="MOMENTUM:LONG",
    ))
    register(Edge(
        name="momentum_short",
        version="v1",
        setup="MOMENTUM",
        direction="SHORT",
        allowed_regimes=("TREND_DOWN",),
        summary="Continuation momentum short.",
        priors_key="MOMENTUM:SHORT",
    ))


_register_defaults()


__all__ = ["Edge", "register", "get", "all_edges", "reset_for_tests"]
