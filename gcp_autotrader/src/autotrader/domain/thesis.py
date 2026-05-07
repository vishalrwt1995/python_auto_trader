"""M2 — Thesis: entry-time expectation record.

A Thesis is a frozen snapshot of what we believed when we opened the
position: which Edge fired, what R we expected, how long we planned to
hold, the price at which the thesis is INVALIDATED (not the stop — the
stop is the execution exit; invalidation is "the reason we entered no
longer applies, cut even if stop hasn't hit"), and the regime + risk
mode at entry.

Stored alongside the position doc (field name: `thesis`) and consumed by:
  * ExitFSM/monitors — for mode-aware exits (invalidation can fire an
    early exit even before a stop breach).
  * AttributionLog (M6) — realized R vs expected R, actual hold vs
    expected hold, regime drift, etc.

Design notes:
  * Pure dataclass — no I/O. Construction is a single function
    `build_thesis` that stays easy to unit-test.
  * Values default to sensible "unknown" sentinels (0.0 / "") so that
    un-filled fields round-trip through Firestore without KeyErrors.
  * `to_dict` + `from_dict` keep the Firestore layer dumb — just a
    dict, no custom codec.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class Thesis:
    """Entry-time expectation snapshot — immutable once created."""

    edge_name: str
    edge_version: str
    setup: str
    direction: str                   # "BUY" | "SELL" (matches position.side)
    entry_price: float
    expected_r: float                # R we expected to realize (priors lookup, M3)
    expected_hold_minutes: int       # intraday ≈ 30-120, swing ≈ 1-5 days in minutes
    invalidation_price: float        # thesis-invalid (structural), not stop (risk)
    regime_at_entry: str
    risk_mode_at_entry: str
    ts_epoch: float                  # entry epoch seconds (for attribution join)
    # Optional free-form confirmation levels the FSM can arm on
    # (e.g. "break 102.30 to confirm"). Empty tuple if not used.
    confirmation_levels: tuple[float, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        # Firestore dislikes tuples — flatten to list.
        d["confirmation_levels"] = list(self.confirmation_levels)
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Thesis":
        return cls(
            edge_name=str(d.get("edge_name", "") or ""),
            edge_version=str(d.get("edge_version", "") or ""),
            setup=str(d.get("setup", "") or ""),
            direction=str(d.get("direction", "") or ""),
            entry_price=float(d.get("entry_price", 0.0) or 0.0),
            expected_r=float(d.get("expected_r", 0.0) or 0.0),
            expected_hold_minutes=int(d.get("expected_hold_minutes", 0) or 0),
            invalidation_price=float(d.get("invalidation_price", 0.0) or 0.0),
            regime_at_entry=str(d.get("regime_at_entry", "") or ""),
            risk_mode_at_entry=str(d.get("risk_mode_at_entry", "") or ""),
            ts_epoch=float(d.get("ts_epoch", 0.0) or 0.0),
            confirmation_levels=tuple(float(x) for x in (d.get("confirmation_levels") or [])),
        )


# Default expected-R / expected-hold heuristics. M3 replaces these with
# real priors from the backtest harness; M2 ships the defaults so the
# thesis field is populated on every entry from day one.
_DEFAULT_INTRADAY_HOLD_MIN = 90        # median intraday hold in minutes
_DEFAULT_SWING_HOLD_MIN = 3 * 24 * 60  # 3 days
_DEFAULT_EXPECTED_R = 1.25             # matches rr_intraday default in settings


def build_thesis(
    *,
    setup: str,
    direction: str,
    entry_price: float,
    sl_price: float,
    regime: str,
    risk_mode: str,
    ts_epoch: float,
    is_swing: bool = False,
    edge_name: str = "",
    edge_version: str = "v1",
    expected_r: float | None = None,
    expected_hold_minutes: int | None = None,
    invalidation_price: float | None = None,
    confirmation_levels: tuple[float, ...] = (),
) -> Thesis:
    """Assemble a Thesis from the values available at entry time.

    `edge_name` can be left blank — we fill it from the playbook's
    matching_edges lookup in `trading_service`, but the Thesis is still
    usable for attribution without it (setup + direction are enough to
    join priors).

    Invalidation defaults to the SL price if not given; M3 will override
    with per-edge structural invalidation (e.g. "breakout invalidates
    if price closes back inside the range", not just "stop hit").
    """
    if expected_r is None:
        expected_r = _DEFAULT_EXPECTED_R
    if expected_hold_minutes is None:
        expected_hold_minutes = _DEFAULT_SWING_HOLD_MIN if is_swing else _DEFAULT_INTRADAY_HOLD_MIN
    if invalidation_price is None:
        # Default: if stop is the invalidation too. Non-zero so attribution
        # queries don't need to special-case missing values.
        invalidation_price = float(sl_price or 0.0)

    return Thesis(
        edge_name=str(edge_name or ""),
        edge_version=str(edge_version or "v1"),
        setup=str(setup or "").strip().upper(),
        direction=str(direction or "").strip().upper(),
        entry_price=float(entry_price or 0.0),
        expected_r=float(expected_r),
        expected_hold_minutes=int(expected_hold_minutes),
        invalidation_price=float(invalidation_price),
        regime_at_entry=str(regime or "").strip().upper(),
        risk_mode_at_entry=str(risk_mode or "NORMAL").strip().upper(),
        ts_epoch=float(ts_epoch or 0.0),
        confirmation_levels=tuple(float(x) for x in confirmation_levels),
    )


__all__ = ["Thesis", "build_thesis"]
