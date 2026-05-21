"""Phase 0 spike — does the import-prod-code approach work?

Goal: prove that we can take production scoring + gate code, feed it
historical candle data, and get a sensible decision back. Single stock,
single day, MOMENTUM swing setup.

PASS CRITERION: script runs without errors, produces a decision (BUY/SELL/HOLD
+ qualified/blocked-reason).

FAIL CRITERION: import fails, or the functions can't be called without the
full container, or the output makes no sense.

If this script fails, the entire backtest plan must be redesigned. We stop
and pick a different approach (paper observation or walk-away) rather than
spend weeks trying to make import-prod work.

Run:
    python -m autotrader.backtest_v2.phase0_spike
"""
from __future__ import annotations

import json
import sys
from datetime import date

# Production code imports — the whole point of this spike is to prove
# these load cleanly and can be called.
from autotrader.domain.daily_bias import compute_daily_bias
from autotrader.domain.indicators import compute_indicators
from autotrader.domain.models import (
    NiftySnapshot,
    RegimeSnapshot,
)
from autotrader.domain.scoring import (
    check_swing_entry,
    determine_direction,
    score_signal,
)
from autotrader.settings import StrategySettings


# --------------------------------------------------------------------------
# Synthetic data for the spike
# --------------------------------------------------------------------------
# We do NOT yet need a real candle pipeline. The spike's job is to prove
# the prod functions can be invoked end-to-end. We feed plausible synthetic
# data; if everything wires up, we move to Phase 1 (real BQ-backed data).


def _synth_daily_candles(start_price: float = 2000.0, n_days: int = 100) -> list[list]:
    """Generate a synthetic uptrending daily candle series.

    Format: list of [ts_iso, open, high, low, close, volume, ...]
    Matches Upstox v3 daily candle shape.
    """
    candles: list[list] = []
    price = start_price
    for i in range(n_days):
        drift = 0.4   # +0.4% per day average — synthetic uptrend (MOMENTUM thesis)
        open_p = price
        close_p = price * (1.0 + drift / 100.0)
        high_p = max(open_p, close_p) * 1.005
        low_p = min(open_p, close_p) * 0.997
        vol = 1_000_000
        ts = f"2026-01-{(i % 28) + 1:02d}T00:00:00+05:30"
        candles.append([ts, open_p, high_p, low_p, close_p, vol, 0])
        price = close_p
    return candles


def _synth_intraday_candles(daily: list[list], n_bars: int = 100) -> list[list]:
    """Generate synthetic 5m intraday candles for the most-recent daily."""
    last_close = float(daily[-1][4])
    bars: list[list] = []
    price = last_close * 0.998
    for i in range(n_bars):
        open_p = price
        # mild uptrend intraday
        close_p = price * (1.0 + 0.02 / 100.0)
        high_p = max(open_p, close_p) * 1.001
        low_p = min(open_p, close_p) * 0.999
        vol = 5000
        ts = f"2026-04-15T{9 + (i // 12):02d}:{(i % 12) * 5:02d}:00+05:30"
        bars.append([ts, open_p, high_p, low_p, close_p, vol, 0])
        price = close_p
    return bars


def _build_regime_snapshot() -> RegimeSnapshot:
    """Plausible RegimeSnapshot for a TREND_UP day."""
    return RegimeSnapshot(
        regime="TREND_UP",
        bias="BULLISH",
        vix=14.0,
        nifty=NiftySnapshot(
            change_pct=0.5,
            ltp=22000.0,
        ),
        confidence=0.8,
        data_health=0.9,
        source_quality=0.95,
    )


# --------------------------------------------------------------------------
# Run the production functions
# --------------------------------------------------------------------------


def run_spike() -> dict:
    """Execute the spike. Returns a structured result dict."""
    result: dict = {
        "phase": "0",
        "symbol": "RELIANCE",
        "setup": "MOMENTUM",
        "wl_type": "swing",
        "as_of": "2026-04-15",
        "steps": [],
        "outcome": "UNKNOWN",
        "errors": [],
    }

    cfg = StrategySettings()
    result["steps"].append("imported StrategySettings OK")

    # Build synthetic candles
    daily_candles = _synth_daily_candles(start_price=2000.0, n_days=100)
    intraday_candles = _synth_intraday_candles(daily_candles, n_bars=100)
    result["steps"].append(f"synth candles: {len(daily_candles)} daily, {len(intraday_candles)} intraday")

    # Step 1: compute daily bias
    try:
        daily_bias = compute_daily_bias(daily_candles)
        if daily_bias is None:
            result["errors"].append("compute_daily_bias returned None — insufficient data?")
            result["outcome"] = "FAIL"
            return result
        result["steps"].append(
            f"daily_bias: trend={daily_bias.trend} strength={daily_bias.strength:.0f} "
            f"adx={daily_bias.adx_daily:.0f} rsi={daily_bias.rsi_daily:.0f} "
            f"ema_stack={daily_bias.ema_stack} supertrend_dir={daily_bias.supertrend_dir}"
        )
    except Exception as exc:
        result["errors"].append(f"compute_daily_bias raised: {type(exc).__name__}: {exc}")
        result["outcome"] = "FAIL"
        return result

    # Step 2: compute indicator snapshot from intraday candles
    try:
        ind = compute_indicators(intraday_candles, cfg)
        if ind is None:
            result["errors"].append("compute_indicators returned None — insufficient bars?")
            result["outcome"] = "FAIL"
            return result
        result["steps"].append(
            f"ind: close={ind.close:.1f} rsi={ind.rsi.curr:.1f} "
            f"adx={ind.adx:.1f} ema_stack={ind.ema_stack} vwap={ind.vwap:.1f}"
        )
    except Exception as exc:
        result["errors"].append(f"compute_indicators raised: {type(exc).__name__}: {exc}")
        result["outcome"] = "FAIL"
        return result

    # Step 3: regime snapshot
    regime = _build_regime_snapshot()
    result["steps"].append(f"regime: {regime.regime}/{regime.bias} vix={regime.vix}")

    # Step 4: determine direction (swing + daily bias)
    try:
        direction = determine_direction(
            ind, regime, setup="MOMENTUM",
            wl_type="swing",
            daily_bias=daily_bias,
        )
        result["steps"].append(f"determine_direction → {direction}")
    except Exception as exc:
        result["errors"].append(f"determine_direction raised: {type(exc).__name__}: {exc}")
        result["outcome"] = "FAIL"
        return result

    # Step 5: score_signal
    try:
        signal_score = score_signal(
            "RELIANCE", direction, ind, regime, cfg,
            daily_bias=daily_bias, setup="MOMENTUM",
        )
        result["steps"].append(f"score_signal → score={signal_score.score} direction={signal_score.direction}")
        result["score"] = signal_score.score
    except Exception as exc:
        result["errors"].append(f"score_signal raised: {type(exc).__name__}: {exc}")
        result["outcome"] = "FAIL"
        return result

    # Step 6: check_swing_entry
    try:
        gate_ok, gate_reason = check_swing_entry(
            "MOMENTUM", direction, ind, daily_bias, regime=regime.regime,
        )
        result["steps"].append(f"check_swing_entry → ok={gate_ok} reason='{gate_reason}'")
        result["gate_ok"] = gate_ok
        result["gate_reason"] = gate_reason
    except Exception as exc:
        result["errors"].append(f"check_swing_entry raised: {type(exc).__name__}: {exc}")
        result["outcome"] = "FAIL"
        return result

    # Decision
    if direction == "HOLD":
        result["decision"] = "HOLD (no direction vote)"
    elif not gate_ok:
        result["decision"] = f"BLOCKED ({gate_reason})"
    elif signal_score.score < cfg.swing_min_signal_score:
        result["decision"] = f"BLOCKED (score {signal_score.score} < threshold {cfg.swing_min_signal_score})"
    else:
        result["decision"] = f"QUALIFIED — direction={direction} score={signal_score.score}"

    result["outcome"] = "PASS"
    return result


def main() -> int:
    print("=" * 70)
    print("Phase 0 Spike — does import-prod-code work for backtest?")
    print("=" * 70)
    result = run_spike()
    print()
    for s in result.get("steps", []):
        print(f"  ✓ {s}")
    print()
    if result.get("errors"):
        print("ERRORS:")
        for e in result["errors"]:
            print(f"  ✗ {e}")
    print()
    print(f"Decision: {result.get('decision', '?')}")
    print(f"Outcome:  {result['outcome']}")
    print()
    if result["outcome"] == "PASS":
        print("✅ Phase 0 PASSED. Import-prod-code approach is viable.")
        print("   → Proceed to Phase 1 (data layer + real BQ candles).")
        return 0
    else:
        print("❌ Phase 0 FAILED. Import-prod-code approach has unresolvable issues.")
        print("   → STOP. Revisit plan; do not proceed to Phase 1.")
        # Also dump structured result for debugging
        print()
        print("Full result:")
        print(json.dumps(result, indent=2, default=str))
        return 1


if __name__ == "__main__":
    sys.exit(main())
