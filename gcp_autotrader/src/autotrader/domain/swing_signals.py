"""Pure swing-signal predicates validated on the 2010-2026 deep held-out OOS.

Two edges, both confirmed by walk-forward (TRAIN 2010-17 / TEST 2018-26, and
robust across five split boundaries — see docs/SWING_EDGE_AUDIT_HANDOFF.md):

  #3  MEAN_REVERSION 200-SMA gate — mean-revert ONLY above the 200-day SMA
      (buy dips inside uptrends, never catch a falling knife below trend).
      Walk-forward: both halves net-positive, +113k TEST-half delta at Rs.5L.

  #7-soft  MOMENTUM near-52wk-high tilt — add a small ranking bonus to a
      momentum candidate whose close sits within 15% of its 252-day high, so
      leaders win slots over laggards. Walk-forward plateau at weight 1-2;
      weight 1.0 generalises (TEST delta +66k at Rs.5L). NOTE the HARD version
      (a binary 52wk-high gate) FAILED walk-forward — train-best threshold
      0.92 lost money on the unseen TEST half — and was deliberately dropped.

These predicates are kept pure + dependency-free so the production scanner
(services/universe_service) and the fidelity-replay backtest call the SAME
code. Re-implementing them twice is how an off-by-one in a slice silently
deletes the edge; sharing one tested module is the only way to guarantee
prod == the validated backtest (same lesson as domain/swing_exit.py).

Index convention — both inputs end at the last COMPLETED daily bar (the scan's
"as-of" bar), i.e. ``closes[-1]`` / ``highs[-1]`` are the as-of close / high.
This reproduces the backtest's ``bars[ei-1]`` as-of convention exactly (the
entry fills on ``bars[ei]``; signals are computed on the prior bar ``j=ei-1``).
"""

from __future__ import annotations

# Window sizes chosen to reproduce the backtest's slices bar-for-bar.
SMA200_WINDOW = 200      # mean of the 200 closes ENDING the bar before the as-of close
HIGH252_WINDOW = 252     # 252-day high == max high over the 253 bars incl. the as-of bar
NEAR_HIGH_FLOOR = 0.85   # tilt activates once close is within 15% of the 252-day high
DEFAULT_TILT_WEIGHT = 1.0  # validated weight (plateau 1-2; 1.0 generalises on TEST)


def mean_reversion_above_200sma(closes: list[float]) -> bool:
    """#3 gate: True iff the as-of close is above its 200-day SMA.

    The SMA is the mean of the 200 closes ENDING the day before the as-of close
    (``closes[-201:-1]``), matching the backtest's ``range(j-200, j)`` where
    ``j = ei-1`` is the as-of bar. Returns ``False`` on insufficient history
    (< 201 closes) — fail-closed, so MEAN_REVERSION simply is not emitted,
    exactly as the backtest skips the trade (``if j < 200: continue``).
    """
    if len(closes) < SMA200_WINDOW + 1:
        return False
    sma200 = sum(closes[-(SMA200_WINDOW + 1):-1]) / float(SMA200_WINDOW)
    return sma200 > 0.0 and closes[-1] > sma200


def near_high_tilt(
    closes: list[float],
    highs: list[float],
    weight: float = DEFAULT_TILT_WEIGHT,
) -> float:
    """#7-soft tilt: ranking bonus for a momentum leader near its 52-week high.

    ``hi52 = as_of_close / max(high over the 253 bars ending at the as-of bar)``
    where ``highs[-253:]`` == the backtest's ``bars[j-252:j+1]`` (the max
    includes the as-of bar's own high). The bonus is
    ``weight * max(0, hi52 - 0.85) * 100`` and is added to the candidate's
    ``wl_score`` (the slot-ranking key). Returns ``0.0`` on insufficient
    history (< 253 bars) — no tilt, matching the backtest's ``h is None`` path
    (the momentum trade still happens, just without the bonus).
    """
    n = HIGH252_WINDOW + 1
    if len(closes) < n or len(highs) < n:
        return 0.0
    window_high = max(highs[-n:])
    if window_high <= 0.0:
        return 0.0
    hi52 = closes[-1] / window_high
    return weight * max(0.0, hi52 - NEAR_HIGH_FLOOR) * 100.0
