"""Phase 1 spike — use REAL historical data from production GCS cache.

This is the same as phase0_spike but instead of synthetic data, reads from
`cache/score_1d/...` — the exact files the live system reads.

PASS: script runs, finds candles, produces a decision for a real symbol on
a real historical date.

FAIL: cache doesn't have the symbol/date, or production functions choke on
real data.

Run:
    python -m autotrader.backtest_v2.phase1_real_data RELIANCE 2026-04-15
"""
from __future__ import annotations

import json
import sys

from autotrader.backtest_v2.data import HistoricalDataset
from autotrader.domain.daily_bias import compute_daily_bias
from autotrader.domain.indicators import compute_indicators
from autotrader.domain.models import NiftySnapshot, RegimeSnapshot
from autotrader.domain.scoring import check_swing_entry, determine_direction, score_signal
from autotrader.settings import StrategySettings


def _build_regime_snapshot() -> RegimeSnapshot:
    """Plausible regime for Phase 1 (no historical brain replay yet — that's Phase 2)."""
    return RegimeSnapshot(
        regime="TREND_UP",
        bias="BULLISH",
        vix=14.0,
        nifty=NiftySnapshot(change_pct=0.5, ltp=22000.0),
        confidence=0.8,
        data_health=0.9,
        source_quality=0.95,
    )


def run_spike(symbol: str, as_of: str) -> dict:
    result: dict = {
        "phase": "1",
        "symbol": symbol,
        "as_of": as_of,
        "steps": [],
        "outcome": "UNKNOWN",
        "errors": [],
    }

    cfg = StrategySettings()
    ds = HistoricalDataset()

    # Step A: confirm cache has the symbol
    if not ds.has_daily(symbol):
        result["errors"].append(f"GCS cache missing daily file for {symbol} — try a different symbol")
        result["outcome"] = "FAIL"
        return result
    result["steps"].append(f"GCS cache has daily file for {symbol}")

    # Step B: load daily candles up to (and including) as_of date
    try:
        daily = ds.daily_candles(symbol, end_date=as_of)
    except Exception as exc:
        result["errors"].append(f"daily_candles raised: {type(exc).__name__}: {exc}")
        result["outcome"] = "FAIL"
        return result
    if not daily:
        result["errors"].append(f"daily_candles returned 0 rows for {symbol} as-of {as_of}")
        result["outcome"] = "FAIL"
        return result
    result["steps"].append(
        f"loaded {len(daily)} daily candles "
        f"({str(daily[0][0])[:10]} to {str(daily[-1][0])[:10]})"
    )

    # Step C: try intraday (Phase 1 is OK if intraday cache is missing — that's a Phase 1.5 concern)
    intraday = ds.intraday_candles(symbol, end_date=as_of)
    if intraday:
        result["steps"].append(f"loaded {len(intraday)} intraday candles")
    else:
        result["steps"].append("intraday cache missing — will use synthetic intraday for spike")
        # For the spike, synthesize intraday from the last daily close
        last_close = float(daily[-1][4]) if daily else 100.0
        intraday = []
        price = last_close * 0.998
        for i in range(120):
            o = price
            c = price * 1.0002
            h = max(o, c) * 1.001
            l = min(o, c) * 0.999
            intraday.append([f"{as_of}T{9 + i // 12:02d}:{(i % 12) * 5:02d}:00+05:30", o, h, l, c, 5000, 0])
            price = c

    # Step D: daily bias
    try:
        db = compute_daily_bias(daily)
        if db is None:
            result["errors"].append("compute_daily_bias returned None (need ≥50 daily candles)")
            result["outcome"] = "FAIL"
            return result
        result["steps"].append(
            f"daily_bias: trend={db.trend} strength={db.strength:.0f} "
            f"adx={db.adx_daily:.0f} rsi={db.rsi_daily:.0f} stack={db.ema_stack}"
        )
    except Exception as exc:
        result["errors"].append(f"compute_daily_bias raised: {type(exc).__name__}: {exc}")
        result["outcome"] = "FAIL"
        return result

    # Step E: indicator snapshot
    try:
        ind = compute_indicators(intraday, cfg)
        if ind is None:
            result["errors"].append(f"compute_indicators returned None ({len(intraday)} bars; need >= 80)")
            result["outcome"] = "FAIL"
            return result
        result["steps"].append(
            f"ind: close={ind.close:.2f} rsi={ind.rsi.curr:.1f} adx={ind.adx:.1f} "
            f"vwap={ind.vwap:.2f} stack={ind.ema_stack}"
        )
    except Exception as exc:
        result["errors"].append(f"compute_indicators raised: {type(exc).__name__}: {exc}")
        result["outcome"] = "FAIL"
        return result

    # Step F-H: regime → direction → score → gate
    regime = _build_regime_snapshot()
    try:
        direction = determine_direction(ind, regime, setup="MOMENTUM", wl_type="swing", daily_bias=db)
        result["steps"].append(f"direction: {direction}")
        sig = score_signal(symbol, direction, ind, regime, cfg, daily_bias=db, setup="MOMENTUM")
        result["steps"].append(f"score: {sig.score} (threshold {cfg.swing_min_signal_score})")
        gate_ok, gate_reason = check_swing_entry("MOMENTUM", direction, ind, db, regime=regime.regime)
        result["steps"].append(f"gate: ok={gate_ok} reason='{gate_reason}'")
    except Exception as exc:
        result["errors"].append(f"scoring chain raised: {type(exc).__name__}: {exc}")
        result["outcome"] = "FAIL"
        return result

    # Decision
    if direction == "HOLD":
        result["decision"] = "HOLD (no direction)"
    elif not gate_ok:
        result["decision"] = f"BLOCKED ({gate_reason})"
    elif sig.score < cfg.swing_min_signal_score:
        result["decision"] = f"BLOCKED (score {sig.score} < {cfg.swing_min_signal_score})"
    else:
        result["decision"] = f"QUALIFIED — {direction} score={sig.score}"

    result["outcome"] = "PASS"
    return result


def main() -> int:
    symbol = sys.argv[1] if len(sys.argv) > 1 else "INFY"
    as_of = sys.argv[2] if len(sys.argv) > 2 else "2025-09-25"

    print("=" * 70)
    print(f"Phase 1 — real-data spike: {symbol} as of {as_of}")
    print("=" * 70)
    result = run_spike(symbol, as_of)
    print()
    for s in result.get("steps", []):
        print(f"  ✓ {s}")
    if result.get("errors"):
        print("\nERRORS:")
        for e in result["errors"]:
            print(f"  ✗ {e}")
    print(f"\nDecision: {result.get('decision', '?')}")
    print(f"Outcome:  {result['outcome']}")
    print()
    if result["outcome"] != "PASS":
        print(json.dumps(result, indent=2, default=str))
    return 0 if result["outcome"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
