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
    gcs_bucket: str = "grow-profit-machine-autotrader-data",
    gcs_exchange: str = "NSE",
    gcs_segment: str = "CASH",
) -> list[CalibrationFill]:
    """Pull trade + scan_decision pairs from BQ, then look up the
    enclosing 5m bar from the GCS cache (the canonical store the live
    system writes at scan time).

    Why GCS for the 5m bars? The BQ `candles_5m` table is a best-effort
    dual-write with incomplete coverage on traded symbols/dates — in
    practice 0/73 of trade-symbol-dates had bars in BQ when this script
    was first run. GCS has every (symbol, date) the universe writer
    touched, including all traded symbols on their trade days. Reading
    from GCS makes calibration faithful to the same data the live
    scoring engine saw.

    The trade↔decision match is fuzzy: scanner emits a decision at
    scan_ts and the broker fills some seconds later. We accept any
    decision-trade pair within `match_window_seconds` of each other on
    the same (symbol, side).

    Returns: list of CalibrationFill, ready to feed `calibrate_from_fills`.
    """
    from autotrader.backtest.data import (
        _gcs_path_for,
        _query,
    )
    from autotrader.adapters.gcs_store import GoogleCloudStorageStore

    # ── 1. Scans from BQ (cheap; just filtered by run_date) ──────────
    scans_sql = f"""
        SELECT
            symbol, direction AS side,
            UNIX_SECONDS(scan_ts) AS scan_epoch,
            ltp
        FROM `{project}.{dataset}.scan_decisions`
        WHERE run_date BETWEEN '{since}' AND '{until}'
          AND qualified = TRUE
          AND ltp > 0
    """
    scan_rows = _query(project, scans_sql)
    log.info("calibration_scans_loaded n=%d", len(scan_rows))

    # ── 2. Trades from BQ ────────────────────────────────────────────
    trades_sql = f"""
        SELECT
            symbol, side,
            UNIX_SECONDS(entry_ts) AS entry_epoch,
            FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S+05:30', entry_ts, 'Asia/Kolkata') AS entry_ts_iso,
            entry_price
        FROM `{project}.{dataset}.trades`
        WHERE trade_date BETWEEN '{since}' AND '{until}'
          AND entry_price > 0
    """
    trade_rows = _query(project, trades_sql)
    log.info("calibration_trades_loaded n=%d", len(trade_rows))

    if not trade_rows or not scan_rows:
        return []

    # ── 3. Match trades ↔ scans (Python) ─────────────────────────────
    # Index scans by (symbol, side) → list of (epoch, ltp) sorted by epoch.
    scans_by_key: dict[tuple[str, str], list[tuple[int, float]]] = {}
    for r in scan_rows:
        key = (str(r["symbol"]).upper(), str(r["side"]).upper())
        scans_by_key.setdefault(key, []).append((int(r["scan_epoch"]), float(r["ltp"])))
    for v in scans_by_key.values():
        v.sort()

    matched: list[dict[str, Any]] = []
    for tr in trade_rows:
        key = (str(tr["symbol"]).upper(), str(tr["side"]).upper())
        cands = scans_by_key.get(key, ())
        if not cands:
            continue
        entry_epoch = int(tr["entry_epoch"])
        # Pick the closest scan within window.
        best: tuple[int, float] | None = None
        best_dt = match_window_seconds + 1
        for s_epoch, ltp in cands:
            dt = abs(s_epoch - entry_epoch)
            if dt <= match_window_seconds and dt < best_dt:
                best_dt = dt
                best = (s_epoch, ltp)
        if best is None:
            continue
        matched.append({
            "symbol": key[0],
            "side": key[1],
            "entry_epoch": entry_epoch,
            "entry_ts_iso": str(tr["entry_ts_iso"]),
            "theoretical_price": best[1],
            "actual_price": float(tr["entry_price"]),
        })
    log.info("calibration_trades_matched n=%d (of %d trades)",
             len(matched), len(trade_rows))
    if not matched:
        return []

    # ── 4. Resolve enclosing 5m bar from GCS, per matched fill ───────
    # Cache 5m candle JSONs per symbol so we don't re-fetch.
    gcs = GoogleCloudStorageStore(bucket_name=gcs_bucket)
    symbol_bar_cache: dict[str, list[tuple[int, float, float]]] = {}
    # cache value: list of (bucket_epoch, high, low) sorted by bucket_epoch

    def _load_bars(sym: str) -> list[tuple[int, float, float]]:
        if sym in symbol_bar_cache:
            return symbol_bar_cache[sym]
        path = _gcs_path_for("5m", sym, gcs_exchange, gcs_segment)
        try:
            raw = gcs.read_candles(path)
        except Exception:
            log.warning("calibration_bar_read_failed sym=%s path=%s", sym, path,
                        exc_info=True)
            raw = []
        bucket_list: list[tuple[int, float, float]] = []
        for c in raw:
            if not isinstance(c, (list, tuple)) or len(c) < 6:
                continue
            try:
                # Live cache stores bar-open ts (already a 5-min boundary in
                # IST). Convert to epoch-seconds via Python datetime.
                ts_str = str(c[0])
                # Normalize "+05:30" → "+0530" for fromisoformat compat on 3.9.
                from datetime import datetime as _dt
                _dt_obj = _dt.fromisoformat(ts_str)
                bucket_epoch = int(_dt_obj.timestamp())
                bucket_list.append((bucket_epoch, float(c[2]), float(c[3])))
            except (ValueError, TypeError, KeyError):
                continue
        bucket_list.sort()
        symbol_bar_cache[sym] = bucket_list
        return bucket_list

    out: list[CalibrationFill] = []
    n_no_bar = 0
    for m in matched:
        bars = _load_bars(m["symbol"])
        if not bars:
            n_no_bar += 1
            continue
        # 5m bucket boundary = floor(entry_epoch / 300) * 300.
        entry_epoch = m["entry_epoch"]
        bucket_epoch = (entry_epoch // 300) * 300
        # Linear scan is fine; per-symbol bar lists are small (~94 days * 75 bars/day).
        bar_high: float | None = None
        bar_low: float | None = None
        for be, hi, lo in bars:
            if be == bucket_epoch:
                bar_high, bar_low = hi, lo
                break
            if be > bucket_epoch:
                break
        if bar_high is None or bar_low is None:
            n_no_bar += 1
            continue
        try:
            out.append(CalibrationFill(
                symbol=m["symbol"],
                side=m["side"],
                entry_ts=m["entry_ts_iso"],
                theoretical_price=float(m["theoretical_price"]),
                actual_price=float(m["actual_price"]),
                bar_high=float(bar_high),
                bar_low=float(bar_low),
            ))
        except (TypeError, ValueError) as e:
            log.warning("calibration_row_skipped reason=%s sym=%s", e, m["symbol"])
            continue

    log.info("calibration_fills_built n=%d (matched=%d, missing_bar=%d)",
             len(out), len(matched), n_no_bar)
    return out


__all__ = [
    "CalibrationFill",
    "CalibrationResult",
    "calibrate_from_fills",
    "load_calibration_data",
    "MIN_RELIABLE_N",
    "MIN_USABLE_N",
]
