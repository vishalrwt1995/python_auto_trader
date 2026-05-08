"""Regime-strategy affinity matrix.

Maps (regime, strategy, direction) to a score multiplier.  Strategies that
are well-suited to the current market regime get a boost (up to 1.4x),
while mismatched strategies get penalised (down to 0.2x).  This prevents
the system from firing breakout entries in choppy markets or mean-reversion
in strong trends — the #1 cause of false signals.
"""
from __future__ import annotations

# Matrix: regime → {strategy: multiplier}
# For directional strategies the multiplier applies when direction aligns with regime.
# "short" variants use the mirror multiplier from the corresponding bearish regime.

_AFFINITY: dict[str, dict[str, float]] = {
    "TREND_UP": {
        "BREAKOUT": 1.3,
        "SHORT_BREAKDOWN": 0.4,   # shorting in uptrend is dangerous
        "PULLBACK": 1.2,
        "SHORT_PULLBACK": 0.5,
        "MEAN_REVERSION": 0.5,
        "VWAP_REVERSAL": 0.5,
        "VWAP_TREND": 1.1,
        "OPEN_DRIVE": 1.0,
        "PHASE1_MOMENTUM": 1.2,
        "PHASE1_REVERSAL": 0.6,   # oversold-bounce picks are wrong in a bull market
        "MOMENTUM": 1.4,          # swing relative-strength leaders — ideal setup for trending markets
        "MORNING_FADE": 0.3,      # fading the pop in a strong uptrend = catching knives
        "AUTO": 1.0,
        "DEFAULT": 1.0,
    },
    "TREND_DOWN": {
        "BREAKOUT": 0.4,          # buying breakouts in downtrend rarely works
        "SHORT_BREAKDOWN": 1.3,
        "PULLBACK": 0.5,
        "SHORT_PULLBACK": 1.2,
        "MEAN_REVERSION": 0.6,
        "VWAP_REVERSAL": 0.6,
        "VWAP_TREND": 0.7,        # SELL path structurally unreachable (label=above-VWAP); BUY in downtrend is low-quality
        "OPEN_DRIVE": 0.8,
        "PHASE1_MOMENTUM": 0.8,
        "PHASE1_REVERSAL": 1.2,   # oversold bounces are the primary edge in a downtrend
        "MOMENTUM": 0.3,          # buying strength in a downtrend almost always fails
        "MORNING_FADE": 1.0,      # bear regime + morning pop = decent fade, but mean-reversion is weaker than RANGE
        "AUTO": 0.9,
        "DEFAULT": 0.9,
    },
    "RANGE": {
        "BREAKOUT": 0.6,
        "SHORT_BREAKDOWN": 0.6,
        "PULLBACK": 0.8,
        "SHORT_PULLBACK": 0.8,
        "MEAN_REVERSION": 1.4,
        "VWAP_REVERSAL": 1.3,
        "VWAP_TREND": 0.7,
        "OPEN_DRIVE": 0.8,
        "PHASE1_MOMENTUM": 0.7,
        "PHASE1_REVERSAL": 1.0,   # decent — individual oversold stocks can bounce in a range
        "MOMENTUM": 1.1,          # leaders can outperform even in a ranging index
        "MORNING_FADE": 1.4,      # ideal regime — RANGE means morning pops mean-revert
        "AUTO": 1.0,
        "DEFAULT": 1.0,
    },
    "CHOP": {
        "BREAKOUT": 0.3,
        "SHORT_BREAKDOWN": 0.3,
        "PULLBACK": 0.5,
        "SHORT_PULLBACK": 0.5,
        "MEAN_REVERSION": 1.2,
        "VWAP_REVERSAL": 1.1,
        "VWAP_TREND": 0.4,
        "OPEN_DRIVE": 0.5,
        "PHASE1_MOMENTUM": 0.4,
        "PHASE1_REVERSAL": 0.9,   # choppy index can still produce oversold individual-stock bounces
        "MOMENTUM": 0.4,          # momentum persistence breaks down in chop
        "MORNING_FADE": 1.3,      # CHOP = pure mean-reverting; fades work great
        "AUTO": 0.7,
        "DEFAULT": 0.7,
    },
    "PANIC": {
        "BREAKOUT": 0.2,
        "SHORT_BREAKDOWN": 0.8,
        "PULLBACK": 0.3,
        "SHORT_PULLBACK": 0.6,
        "MEAN_REVERSION": 0.8,   # capitulation bounces can be profitable
        "VWAP_REVERSAL": 0.8,
        "VWAP_TREND": 0.2,
        "OPEN_DRIVE": 0.3,
        "PHASE1_MOMENTUM": 0.3,
        "PHASE1_REVERSAL": 0.9,   # capitulation + oversold = strong reversal candidate
        "MOMENTUM": 0.2,          # strongest stocks fall hardest in panics
        "MORNING_FADE": 0.6,      # PANIC opens often gap down, not up — fade rarely triggers, low conviction when it does
        "AUTO": 0.5,
        "DEFAULT": 0.5,
    },
    "RECOVERY": {
        "BREAKOUT": 1.1,
        "SHORT_BREAKDOWN": 0.4,
        "PULLBACK": 1.0,
        "SHORT_PULLBACK": 0.5,
        "MEAN_REVERSION": 0.7,
        "VWAP_REVERSAL": 0.7,
        "VWAP_TREND": 1.0,
        "OPEN_DRIVE": 1.2,
        "PHASE1_MOMENTUM": 1.1,
        "PHASE1_REVERSAL": 1.1,   # recovery is the ideal environment for oversold-stock bounces
        "MOMENTUM": 1.3,          # early-recovery leaders tend to extend
        "MORNING_FADE": 0.7,      # RECOVERY = continuation regime; fading the pop fights the trend
        "AUTO": 1.0,
        "DEFAULT": 1.0,
    },
}

# Floor and ceiling to prevent extreme distortion
_MIN_MULT = 0.2
_MAX_MULT = 1.4


def regime_strategy_multiplier(
    regime: str,
    strategy: str,
    direction: str = "BUY",
) -> float:
    """Return a score multiplier for the (regime, strategy, direction) combination.

    The multiplier is applied to the raw signal score to boost strategies
    that match the regime and suppress those that don't.

    Args:
        regime: Market regime from brain_state (TREND_UP, TREND_DOWN, RANGE, etc.)
        strategy: Watchlist setup (BREAKOUT, PULLBACK, MEAN_REVERSION, etc.)
        direction: BUY or SELL

    Returns:
        float in [0.2, 1.4] — multiply by raw score
    """
    regime_upper = str(regime or "RANGE").strip().upper()
    strategy_upper = str(strategy or "AUTO").strip().upper()

    regime_map = _AFFINITY.get(regime_upper)
    if regime_map is None:
        # Unknown regime — no adjustment
        return 1.0

    mult = regime_map.get(strategy_upper, 1.0)

    # Direction alignment bonus/penalty for directional regimes
    # In TREND_UP, BUY gets the full multiplier; SELL gets a dampening
    # In TREND_DOWN, SELL gets the full multiplier; BUY gets dampening
    # Exception: PHASE1_REVERSAL and MEAN_REVERSION/VWAP_REVERSAL are
    # explicitly counter-trend — their BUY scores in TREND_DOWN should
    # NOT be penalised because buying oversold bounces IS the edge here.
    _counter_trend_strategies = {"PHASE1_REVERSAL", "MEAN_REVERSION", "VWAP_REVERSAL"}
    if regime_upper == "TREND_UP" and direction == "SELL":
        if strategy_upper not in _counter_trend_strategies:
            mult = min(mult, 0.6)
    elif regime_upper == "TREND_DOWN" and direction == "BUY":
        if strategy_upper not in _counter_trend_strategies:
            mult = min(mult, 0.6)

    return max(_MIN_MULT, min(_MAX_MULT, round(mult, 2)))


# Hard blocks: strategies that should never fire in certain regimes, regardless
# of score. This is a stronger gate than the affinity multiplier — the multiplier
# can still let a 90-score signal sneak through at 0.3× = 27, but hard-block
# eliminates the strategy entirely so we don't waste a slot.
#
# 2026-05-06: BREAKOUT added to ALL regime hard-blocks (was already blocked in
# CHOP/RANGE/PANIC; adding to TREND_UP/TREND_DOWN). Live data 2026-04-16 →
# 2026-05-04: BREAKOUT BUY went 0/9 (zero wins, total P&L −₹392). Even in the
# regimes where the affinity matrix favoured it (TREND_UP=1.3×, RANGE_BULL=1.1×),
# real follow-through was absent. Until a root-cause is identified — e.g.
# narrowing-breadth fakeouts, the volume-surge gate is too lax, or the 52w-high
# proximity is wrong for current market structure — BREAKOUT is parked across
# the board. Re-enable on a regime-by-regime basis when a controlled re-test
# (paper or canary) shows positive expectancy.
# 2026-05-08 strategy audit: hard-block updates based on live + backtest data.
#   * MORNING_FADE: 30-trade backtest in RANGE (its supposed sweet spot at
#     1.4× affinity) showed 17% WR / -₹64k. The thesis (fade morning pop)
#     doesn't hold up — 83% of pops continued or stayed elevated. Strategy
#     is now hard-blocked in EVERY regime pending fundamental redesign with
#     bearish-confirmation gates (rejection candle, RSI>70, volume fade).
#     Was previously blocked only in TREND_UP. Live live-trade count: 0.
#   * SHORT_PULLBACK: 4/4 losses in 5-month backtest. Already blocked in CHOP.
#     Adding TREND_UP and RANGE — fading rallies in non-bearish regimes is
#     structurally wrong (you're shorting STRENGTH). Allow in TREND_DOWN
#     (affinity 1.2× — the actual sweet spot), PANIC (0.6×), RECOVERY (0.5×).
#   * SHORT_BREAKDOWN: adding TREND_UP to hard-blocks for consistency. Was
#     already affinity-suppressed (0.4× × 0.6 SELL-dampening = 0.24×
#     effective in TREND_UP) but the explicit hard-block prevents wasted
#     scan cycles. Allowed in TREND_DOWN (1.3× sweet spot) and PANIC (0.8×).
_HARD_BLOCKS: dict[str, set[str]] = {
    # CHOP: block high-risk momentum strategies. Keep VWAP_REVERSAL and
    # VWAP_TREND — individual stocks can still trend/reverse even on choppy
    # index days, and these provide the best edge in low-conviction markets.
    "CHOP": {
        "BREAKOUT", "SHORT_BREAKDOWN", "PULLBACK", "SHORT_PULLBACK",
        "OPEN_DRIVE", "PHASE1_MOMENTUM",
        "MOMENTUM",    # relative-strength leaders fail when index whipsaws
        "MORNING_FADE",  # 2026-05-08: backtest 17% WR even in 1.3× affinity regime
    },
    # RANGE: block pure-breakout strategies (fakeouts common) and OPEN_DRIVE
    # (needs gap/momentum at open). Allow VWAP_TREND — individual stocks trend
    # within a ranging index all the time, and blocking it leaves the system
    # unable to trade on broadly bullish range days (breadth=100%).
    "RANGE": {
        "BREAKOUT", "SHORT_BREAKDOWN",
        "OPEN_DRIVE", "PHASE1_MOMENTUM",
        "SHORT_PULLBACK",  # 2026-05-08: 4/4 backtest losses; shorting in RANGE without bearish structure
        "MORNING_FADE",  # 2026-05-08: backtest 17% WR / -₹64k in 1.4× affinity regime
    },
    # PANIC: only allow counter-trend oversold bounces (MR) or short-breakdown
    # continuation. Everything else gets shredded.
    "PANIC": {
        "BREAKOUT", "PULLBACK",
        "OPEN_DRIVE", "PHASE1_MOMENTUM",
        "MOMENTUM",    # chasing strength into a panic = catching a knife
        "MORNING_FADE",  # 2026-05-08: PANIC opens often gap-down, fade thesis inverted
    },
    # TREND_UP / TREND_DOWN: BREAKOUT parked here too (0/9 live WR, see comment
    # block above). Other strategies still allowed — TREND regimes are where
    # VWAP_TREND and MOMENTUM are designed to shine.
    # MORNING_FADE blocked in TREND_UP — fading strong rallies is a knife-catch.
    "TREND_UP":   {
        "BREAKOUT",
        "MORNING_FADE",
        "SHORT_BREAKDOWN",   # 2026-05-08: explicit block for consistency (affinity already 0.24× effective)
        "SHORT_PULLBACK",    # 2026-05-08: shorting strength in uptrend is structurally wrong
    },
    "TREND_DOWN": {"BREAKOUT", "MORNING_FADE"},  # 2026-05-08: MORNING_FADE thesis dead in all regimes
    # 2026-05-08: RECOVERY added to enforce MORNING_FADE block consistently.
    # Pre-fix RECOVERY had no entry in _HARD_BLOCKS → silently allowed all
    # strategies. Add MORNING_FADE explicitly.
    "RECOVERY":   {"MORNING_FADE"},
}


def regime_hard_blocks_strategy(regime: str, strategy: str) -> bool:
    """Return True if this (regime, strategy) combination should be hard-blocked.

    Unlike the affinity multiplier (which softens scores), this is a binary
    allow/deny gate applied as a policy block in the scanner.
    """
    regime_upper = str(regime or "").strip().upper()
    strategy_upper = str(strategy or "").strip().upper()
    if not strategy_upper or strategy_upper in ("AUTO", "DEFAULT"):
        return False
    return strategy_upper in _HARD_BLOCKS.get(regime_upper, set())
