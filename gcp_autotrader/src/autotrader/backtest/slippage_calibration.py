"""Slippage calibration harness — fits `BarRangePct` parameters from real fills.

Why this exists
---------------
The default `BarRangePct(pct_of_range=0.10, cap_bps=25, floor_bps=1)` was
chosen by intuition, not measurement. As soon as you have enough live
fills, this module measures what slippage *actually* looked like and
hands back a recalibrated model. Run it weekly to keep the sim's P&L
magnitudes honest.

What it does
------------
For each live trade (entry leg only — exits are handled separately), we
need three numbers to compute realized slippage:

    1. **Theoretical fill price** — the LTP the scanner saw when it
       decided to enter (`scan_decisions.ltp` at the matched scan_ts).
       This is what the strategy *thought* it was getting.
    2. **Actual fill price** — `trades.entry_price`, the price the
       broker actually executed at.
    3. **Bar context** — the 5m candle covering the entry. The bar's
       (high - low) is the range the slippage scales against.

From these:

    realized_adverse_per_share = entry_price - ltp   (BUY)
                              = ltp - entry_price    (SELL)
    realized_adverse_bps       = adverse / ltp × 10_000
    realized_pct_of_range      = adverse / max(0.01, bar.high - bar.low)

We fit `BarRangePct` by taking the **median** of `pct_of_range` (robust
to outliers from halt-day fills, news shocks, etc.) and the **p95** of
realized_adverse_bps as a cap-floor sanity check.

Why median, not mean
--------------------
A single slippage event during a circuit-up gap can be 200+ bps and
would skew a mean estimator. The median is a sturdier central
tendency for P&L magnitude calibration.

Usage
-----

    >>> from autotrader.backtest.slippage_calibration import (
    ...     calibrate_from_fills, load_calibration_data,
    ... )
    >>> data = load_calibration_data(project="...", dataset="...",
    ...                              since="2026-04-16", until="2026-05-04")
    >>> result = calibrate_from_fills(data)
    >>> print(result.summary)
    >>> # plug `result.model` into RunSpec.slippage for the next backtest

Or via the CLI: `python scripts/redesign/calibrate_slippage.py ...`
"""
from __future__ import annotations

import logging
import statistics
from dataclasses import dataclass, field
from typing import Any

from autotrader.backtest.slippage import BarRangePct, SlippageModel

log = logging.getLogger(__name__)


# Below this fill count, calibration confidence intervals are wide
# enough that the recalibrated model is barely better than the default.
# Below this, we still produce a fit but flag it as low-confidence.
MIN_RELIABLE_N = 30
# Below this, refuse to produce a model at all — too few samples to
# distinguish signal from noise.
MIN_USABLE_N = 5


@dataclass
class CalibrationFill:
    """One observation for slippage calibration. All three components must
    be present and self-consistent — drop the row before constructing if
    any value is missing or zero."""
    symbol: str
    side: str                    # "BUY" / "SELL"
    entry_ts: str                # ISO-8601 IST
    theoretical_price: float     # the LTP scanner saw at decision time
    actual_price: float          # broker's actual entry fill
    bar_high: float              # 5m bar containing the entry
    bar_low: float

    def adverse_per_share(self) -> float:
        """Return signed adverse slippage in ₹/share (≥0 = adverse).
        For BUY, paying more than theoretical is adverse; for SELL,
        receiving less is adverse. We measure absolute adversity here —
        favorable fills (rare) get clamped to 0 since the slippage model
        only models the adverse direction."""
        if self.side.upper() == "BUY":
            adv = self.actual_price - self.theoretical_price
        else:
            adv = self.theoretical_price - self.actual_price
        return max(0.0, adv)

    def adverse_bps(self) -> float:
        if self.theoretical_price <= 0:
            return 0.0
        return (self.adverse_per_share() / self.theoretical_price) * 10_000.0

    def bar_range(self) -> float:
        return max(0.0, self.bar_high - self.bar_low)

    def pct_of_range(self) -> float:
        rng = self.bar_range()
        if rng <= 0:
            return 0.0
        return self.adverse_per_share() / rng


@dataclass
class CalibrationResult:
    """What a fit produces: the model + the numbers that justify it."""
    model: SlippageModel
    n_fills: int
    median_pct_of_range: float
    p95_adverse_bps: float
    median_adverse_bps: float
    max_adverse_bps: float
    confidence: str              # "low" | "moderate" | "high"
    summary: str = ""
    raw_metrics: dict[str, Any] = field(default_factory=dict)


def calibrate_from_fills(fills: list[CalibrationFill]) -> CalibrationResult:
    """Fit `BarRangePct` to a set of real fills.

    The recipe:
      1. Drop fills with bar_range = 0 (pre-market or halted bars give
         no signal — pct_of_range is undefined).
      2. Take the median pct_of_range as the new `pct_of_range` parameter.
         Median is robust to circuit-day outliers.
      3. Take the p95 of adverse_bps as the new `cap_bps`. We want the
         cap to bind on tail bars but not the typical bar.
      4. Take the p10 of adverse_bps (clamped at 0.5) as `floor_bps`.

    Returns a `CalibrationResult`; the `.model` is plug-in compatible with
    the rest of the backtest stack (use as `RunSpec.slippage`).
    """
    valid = [f for f in fills if f.bar_range() > 0 and f.theoretical_price > 0]
    n = len(valid)
    raw = {
        "n_input": len(fills),
        "n_valid": n,
        "n_dropped_zero_range": len([f for f in fills if f.bar_range() <= 0]),
        "n_dropped_bad_price": len([f for f in fills if f.theoretical_price <= 0]),
    }

    if n < MIN_USABLE_N:
        # Not enough data — return the default with a clear warning.
        return CalibrationResult(
            model=BarRangePct(),
            n_fills=n,
            median_pct_of_range=0.0,
            p95_adverse_bps=0.0,
            median_adverse_bps=0.0,
            max_adverse_bps=0.0,
            confidence="low",
            summary=(
                f"INSUFFICIENT DATA: only {n} valid fills (need ≥{MIN_USABLE_N}). "
                f"Returning default BarRangePct model."
            ),
            raw_metrics=raw,
        )

    pct_samples = sorted(f.pct_of_range() for f in valid)
    bps_samples = sorted(f.adverse_bps() for f in valid)

    median_pct = statistics.median(pct_samples)
    p95_bps = _percentile(bps_samples, 0.95)
    p10_bps = _percentile(bps_samples, 0.10)
    median_bps = statistics.median(bps_samples)
    max_bps = bps_samples[-1] if bps_samples else 0.0

    # Sanity floors: never let calibration produce a model that says "fills
    # are free" (floor_bps=0) or "infinite slippage" (cap_bps>1000).
    floor_bps = max(0.5, min(p10_bps, 5.0))
    cap_bps = min(max(p95_bps, 5.0), 100.0)
    # Also clip pct_of_range — pathological inputs can produce >1.0 which
    # would mean we're paying more than the entire bar range.
    pct_of_range = min(max(median_pct, 0.0), 0.5)

    model = BarRangePct(
        pct_of_range=round(pct_of_range, 4),
        cap_bps=round(cap_bps, 2),
        floor_bps=round(floor_bps, 2),
    )

    confidence = "high" if n >= MIN_RELIABLE_N * 2 else (
        "moderate" if n >= MIN_RELIABLE_N else "low"
    )

    summary_lines = [
        f"slippage calibration over {n} valid fills (input={len(fills)})",
        f"  fitted: BarRangePct(pct_of_range={pct_of_range:.4f}, "
        f"cap_bps={cap_bps:.2f}, floor_bps={floor_bps:.2f})",
        f"  median pct_of_range: {median_pct:.4f}",
        f"  adverse bps: median={median_bps:.2f}, p95={p95_bps:.2f}, max={max_bps:.2f}",
        f"  confidence: {confidence}",
    ]
    if confidence == "low":
        summary_lines.append(
            f"  WARNING: n={n} below the {MIN_RELIABLE_N}-fill reliability "
            f"threshold; treat the fit as a starting estimate, not ground truth."
        )

    return CalibrationResult(
        model=model,
        n_fills=n,
        median_pct_of_range=round(median_pct, 4),
        p95_adverse_bps=round(p95_bps, 2),
        median_adverse_bps=round(median_bps, 2),
        max_adverse_bps=round(max_bps, 2),
        confidence=confidence,
        summary="\n".join(summary_lines),
        raw_metrics={
            **raw,
            "median_pct_of_range": median_pct,
            "median_adverse_bps": median_bps,
            "p10_adverse_bps": p10_bps,
            "p95_adverse_bps": p95_bps,
            "max_adverse_bps": max_bps,
        },
    )


def _percentile(sorted_xs: list[float], p: float) -> float:
    """Return the p-th percentile of a SORTED list. p ∈ [0,1]. Linear
    interpolation between the two nearest ranks."""
    if not sorted_xs:
        return 0.0
    if p <= 0:
        return sorted_xs[0]
    if p >= 1:
        return sorted_xs[-1]
    idx = p * (len(sorted_xs) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(sorted_xs) - 1)
    frac = idx - lo
    return sorted_xs[lo] * (1 - frac) + sorted_xs[hi] * frac


# ── BigQuery loader ─────────────────────────────────────────────────────


def load_calibration_data(
    *,
    project: str,
    dataset: str,
    since: str,
    until: str,
    match_window_seconds: int = 90,
) -> list[CalibrationFill]:
    """Pull (trade, scan_decision, 5m bar) join from BigQuery.

    The match between a trade and the scan_decision that triggered it is
    fuzzy: the scanner emits a decision at scan_ts and the broker fills
    some seconds later. We accept any decision-trade pair within
    `match_window_seconds` of each other on the same (symbol, side).

    Trades that don't match any decision (manual entries, post-flag-flip
    re-routes) are dropped. So are decisions without a matching trade.

    Returns: list of CalibrationFill, ready to feed `calibrate_from_fills`.
    """
    from autotrader.backtest.data import _query  # internal helper

    sql = f"""
        WITH scans AS (
            SELECT
                symbol, direction AS side,
                scan_ts, ltp,
                FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S+05:30', scan_ts, 'Asia/Kolkata') AS scan_ts_iso
            FROM `{project}.{dataset}.scan_decisions`
            WHERE run_date BETWEEN '{since}' AND '{until}'
              AND qualified = TRUE
              AND ltp > 0
        ),
        trades AS (
            SELECT
                symbol, side,
                entry_ts, entry_price,
                FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S+05:30', entry_ts, 'Asia/Kolkata') AS entry_ts_iso,
                trade_date
            FROM `{project}.{dataset}.trades`
            WHERE trade_date BETWEEN '{since}' AND '{until}'
              AND entry_price > 0
        ),
        bars AS (
            SELECT
                symbol, candle_ts,
                high AS bar_high, low AS bar_low,
                TIMESTAMP_SECONDS(
                    CAST(FLOOR(UNIX_SECONDS(candle_ts) / 300) * 300 AS INT64)
                ) AS bucket_ts
            FROM `{project}.{dataset}.candles_5m`
            WHERE trade_date BETWEEN '{since}' AND '{until}'
        )
        SELECT
            t.symbol AS symbol,
            t.side AS side,
            t.entry_ts_iso AS entry_ts,
            s.ltp AS theoretical_price,
            t.entry_price AS actual_price,
            b.bar_high AS bar_high,
            b.bar_low AS bar_low
        FROM trades t
        JOIN scans s
          ON s.symbol = t.symbol AND s.side = t.side
         AND ABS(TIMESTAMP_DIFF(s.scan_ts, t.entry_ts, SECOND)) <= {match_window_seconds}
        JOIN bars b
          ON b.symbol = t.symbol
         AND b.bucket_ts = TIMESTAMP_SECONDS(
              CAST(FLOOR(UNIX_SECONDS(t.entry_ts) / 300) * 300 AS INT64)
            )
    """
    rows = _query(project, sql)
    log.info("calibration_rows_loaded n=%d", len(rows))

    out: list[CalibrationFill] = []
    for r in rows:
        try:
            out.append(CalibrationFill(
                symbol=str(r["symbol"]).upper(),
                side=str(r["side"]).upper(),
                entry_ts=str(r["entry_ts"]),
                theoretical_price=float(r["theoretical_price"]),
                actual_price=float(r["actual_price"]),
                bar_high=float(r["bar_high"]),
                bar_low=float(r["bar_low"]),
            ))
        except (TypeError, ValueError, KeyError) as e:
            log.warning("calibration_row_skipped reason=%s row=%s", e, r)
            continue
    return out


__all__ = [
    "CalibrationFill",
    "CalibrationResult",
    "calibrate_from_fills",
    "load_calibration_data",
    "MIN_RELIABLE_N",
    "MIN_USABLE_N",
]
