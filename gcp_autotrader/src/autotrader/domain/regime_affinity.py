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
        # 2026-05-14 audit (FIX A): PULLBACK lifted 0.8 → 1.0. On NIFTY-RANGE
        # days with sector rotation (mid-caps May 13, banks May 14), individual
        # trending stocks were CRUSHED by 0.8× multiplier — base scores ~75
        # × 0.8 = 60 < threshold 72 → rejected as `score_below_min`. Live
        # had 0 trades on both days despite clear sector trends. Individual
        # setup gates (ema_stack, RSI band, EMA distance) already validate
        # the stock is actually trending — the affinity nerf was redundant.
        "PULLBACK": 1.0,
        "SHORT_PULLBACK": 0.8,
        "MEAN_REVERSION": 1.4,
        "VWAP_REVERSAL": 1.3,
        # 2026-05-14 audit (FIX A): VWAP_TREND lifted 0.7 → 1.0. Same root
        # cause as PULLBACK above. Backtest validation (May 13/14 trending
        # + Apr 22/23 narrow-range): FIX A produces +₹1,242 net over 4 days
        # vs ₹0 currently. Trending days +53% WR, narrow days +33% WR/+0.18R
        # avg — net positive on both day types. Stock-level gates (RC-2 fix:
        # bars_since_open ≥ 60 + sustained-side-of-VWAP for 3 bars) prevent
        # firing on noise-VWAP-crossing signals.
        "VWAP_TREND": 1.0,
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
    # RANGE_ROTATING (audit 2026-05-15, Layer 4 Option A).
    # NIFTY ranges but mid-caps / one sector trends — May 13/14/15 pattern.
    # Trend setups should get TREND_UP-ish boosts (so they aren't haircut
    # the way they are in plain RANGE), but BREAKOUT stays hard-blocked
    # downstream because false breakouts are the most common loser at
    # range edges. Mean-reversion setups kept at a moderate 0.9 — they
    # can still work on the index half of a rotating day, but they're
    # not the primary edge here.
    "RANGE_ROTATING": {
        "BREAKOUT": 1.0,          # hard-blocked downstream; multiplier kept neutral
        "SHORT_BREAKDOWN": 0.5,
        "PULLBACK": 1.2,          # individual trending stocks pulling back are the bread-and-butter setup
        "SHORT_PULLBACK": 0.5,
        "MEAN_REVERSION": 0.9,    # works on the index half — not the rotating sector half
        "VWAP_REVERSAL": 0.9,
        "VWAP_TREND": 1.2,        # primary edge — stocks trending against a ranging index
        "OPEN_DRIVE": 1.0,
        "PHASE1_MOMENTUM": 1.0,
        "PHASE1_REVERSAL": 0.8,
        "MOMENTUM": 1.3,          # leaders in the rotating sector extend
        "MORNING_FADE": 0.5,      # hard-blocked downstream
        "AUTO": 1.0,
        "DEFAULT": 1.0,
    },
    # Phase D 2026-05-26 — EARLY_TREND_UP / EARLY_TREND_DOWN.
    # Multipliers ~70-80% of full TREND_* values. Rationale: fast tactical
    # signal confirms direction but structural EMAs haven't yet — direction
    # is plausible but not confirmed. Use moderate multipliers (not 1.4×)
    # to size into the move conservatively. If structural catches up,
    # regime becomes full TREND_UP/DOWN automatically.
    "EARLY_TREND_UP": {
        "BREAKOUT": 1.0,          # hard-blocked downstream (no pattern detector yet)
        "SHORT_BREAKDOWN": 0.4,   # shorting nascent rally = wrong side
        "PULLBACK": 1.1,          # buying dips in early uptrend is sound
        "SHORT_PULLBACK": 0.5,
        "MEAN_REVERSION": 0.7,    # not the primary edge but acceptable
        "VWAP_REVERSAL": 0.6,
        "VWAP_TREND": 1.0,        # neutral — let scoring decide
        "OPEN_DRIVE": 1.0,
        "PHASE1_MOMENTUM": 1.0,
        "PHASE1_REVERSAL": 0.8,
        "MOMENTUM": 1.2,          # near-full bonus — leading stocks extend
        "MORNING_FADE": 0.4,
        "AUTO": 1.0,
        "DEFAULT": 1.0,
    },
    "EARLY_TREND_DOWN": {
        "BREAKOUT": 0.5,          # hard-blocked downstream
        "SHORT_BREAKDOWN": 1.1,   # primary edge but conservative until structural confirms
        "PULLBACK": 0.6,
        "SHORT_PULLBACK": 1.1,
        "MEAN_REVERSION": 0.8,
        "VWAP_REVERSAL": 0.7,
        "VWAP_TREND": 0.7,
        "OPEN_DRIVE": 0.8,
        "PHASE1_MOMENTUM": 0.7,
        "PHASE1_REVERSAL": 1.1,   # oversold bounces work in early decline
        "MOMENTUM": 0.4,          # chasing strength in early decline is wrong
        "MORNING_FADE": 1.0,
        "AUTO": 0.9,
        "DEFAULT": 0.9,
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
    elif regime_upper == "RANGE_ROTATING" and direction == "SELL":
        # Audit 2026-05-15 (Layer 4 Option A): RANGE_ROTATING fires on
        # bullish-breadth + leadership-up days (the sector that's rotating
        # is rotating UP — that's why breadth and leadership are elevated
        # while NIFTY itself ranges). SELL trades in that environment are
        # counter-trend by definition; dampen them the same way TREND_UP
        # does, except for the explicit counter-trend setups whose edge
        # IS shorting strength.
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
        # 2026-05-20 (Batch H): MORNING_FADE re-enabled in CHOP. The
        # 2026-05-08 17%-WR backtest was 30 trades — small sample, possibly
        # tainted by the buggy replay infrastructure. Live data 2026-05-20
        # produced 10 score-100 MORNING_FADE signals that ALL got blocked
        # by hard_block — zero shot at validating in live paper. Re-enable
        # in CHOP/RANGE (the mean-reverting regimes where the fade thesis
        # is structurally correct) to gather real evidence.
    },
    # RANGE: block pure-breakout strategies (fakeouts common) and OPEN_DRIVE
    # (needs gap/momentum at open). Allow VWAP_TREND — individual stocks trend
    # within a ranging index all the time, and blocking it leaves the system
    # unable to trade on broadly bullish range days (breadth=100%).
    "RANGE": {
        "BREAKOUT", "SHORT_BREAKDOWN",
        "OPEN_DRIVE", "PHASE1_MOMENTUM",   # 2026-05-08 mid-session: confirmed via
                                            #   live OLECTRA loss + audit data (3963
                                            #   PHASE1_MOMENTUM scans, 0 qualified)
        "SHORT_PULLBACK",  # 2026-05-08: 4/4 backtest losses; shorting in RANGE without bearish structure
        # 2026-05-20 (Batch H): MORNING_FADE re-enabled in RANGE (see CHOP
        # comment). The strategy was hard-blocked everywhere on a 30-trade
        # backtest; live evidence is needed.
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
    # E2E audit 2026-05-26: considered re-enabling BREAKOUT in TREND_UP but
    # the regression test guard (test_breakout_still_hard_blocked_in_trend_regimes)
    # documents that VCP/cup-handle pattern detection is required before
    # re-enable. Current check_swing_entry() gates (ADX≥25, trend=UP, vol≥1.3)
    # check that trend exists but NOT that the stock is breaking out of a
    # base. MOMENTUM swing (re-enabled in this audit) serves as the trend-
    # following entry that BREAKOUT was meant to be — without the base
    # requirement — see comment at universe_service.py:4824.
    "TREND_UP":   {
        "BREAKOUT",
        "MORNING_FADE",
        "SHORT_BREAKDOWN",   # 2026-05-08: explicit block for consistency (affinity already 0.24× effective)
        "SHORT_PULLBACK",    # 2026-05-08: shorting strength in uptrend is structurally wrong
        "PHASE1_MOMENTUM",   # 2026-05-08 mid-session: stale Phase-1 selections fire on
                             #   yesterday's momentum, not today's tape (OLECTRA SL_HIT
                             #   in 8 min). Phase 2 is the proper signal in TREND_UP.
                             #   Allowed in PANIC/TREND_DOWN/RECOVERY where Phase 2 may
                             #   struggle and Phase 1 fallback adds value.
    },
    "TREND_DOWN": {"BREAKOUT"},  # 2026-05-20 (Batch H): MORNING_FADE re-enabled — a pop in a down market is a legitimate fade thesis
    # 2026-05-08: RECOVERY added to enforce MORNING_FADE block consistently.
    # Pre-fix RECOVERY had no entry in _HARD_BLOCKS → silently allowed all
    # strategies. Add MORNING_FADE explicitly.
    "RECOVERY":   {"MORNING_FADE"},
    # RANGE_ROTATING (audit 2026-05-15): block the same loser set as TREND_UP
    # so the regime-loosening doesn't silently re-admit known-bad strategies.
    # MORNING_FADE block matches all other regimes (thesis dead). BREAKOUT
    # blocked because false-breakouts at range edges are this regime's
    # signature failure mode.
    "RANGE_ROTATING": {
        "BREAKOUT",
        "SHORT_BREAKDOWN",
        "SHORT_PULLBACK",
        "PHASE1_MOMENTUM",
        "MORNING_FADE",
    },
    # Phase D 2026-05-26 — EARLY_TREND_UP / EARLY_TREND_DOWN.
    # New regimes detected when the fast tactical_trend_score confirms
    # direction before structural trend_score does (3-5d lead vs EMA50/200
    # framework). Inherit the same blocks as their parent TREND_* regimes
    # so the system can't accidentally short in EARLY_TREND_UP or chase
    # breakouts in EARLY_TREND_DOWN.
    "EARLY_TREND_UP": {
        "BREAKOUT",        # mirrors TREND_UP block; pattern detection still missing
        "MORNING_FADE",    # fading early rallies is structurally wrong
        "SHORT_BREAKDOWN", # no shorting strength in confirmed up-move
        "SHORT_PULLBACK",  # ditto
        "PHASE1_MOMENTUM", # stale picks — Phase 2 is correct path here
    },
    "EARLY_TREND_DOWN": {
        "BREAKOUT",        # mirrors TREND_DOWN block
        # NB: MORNING_FADE intentionally NOT blocked (Batch H rule for TREND_DOWN)
    },
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


# ── 2026-06 swing-config: per-setup regime gate ─────────────────────────────
# The multi-year backtest validated exactly three long swing cells:
#   MOMENTUM × TREND_UP, PULLBACK × TREND_UP, MEAN_REVERSION × RANGE.
# It reconstructed regimes in "core-4" mode (backtest_v2/brain_reconstruct.CORE_MAP),
# folding the live brain's refinement regimes into their base:
#   EARLY_TREND_UP → TREND_UP,  EARLY_TREND_DOWN → TREND_DOWN,  RANGE_ROTATING → RANGE
# So to reproduce the backtest LIVE, momentum/pullback must fire in the UPTREND
# bucket {TREND_UP, EARLY_TREND_UP} and mean-reversion in the RANGE bucket
# {RANGE, RANGE_ROTATING}. In every other regime (PANIC, TREND_DOWN, CHOP,
# RECOVERY, EARLY_TREND_DOWN) none of the three fire — matching the config, which
# attributes zero profit to those regimes. Gating on the literal "TREND_UP" only
# would under-trade live, since the live brain splits backtest-TREND_UP days into
# TREND_UP + EARLY_TREND_UP. This is a SWING-ONLY gate layered on top of
# _HARD_BLOCKS (which still governs BREAKOUT, the shorts and intraday setups).
_SWING_SETUP_REGIMES: dict[str, set[str]] = {
    "MOMENTUM":       {"TREND_UP", "EARLY_TREND_UP"},
    "PULLBACK":       {"TREND_UP", "EARLY_TREND_UP"},
    "MEAN_REVERSION": {"RANGE", "RANGE_ROTATING"},
}


def swing_setup_allowed_in_regime(setup: str, regime: str) -> bool:
    """Return True if this swing setup may fire in this regime (2026-06 config).

    Only the three backtest-validated long cells are gated here; any other label
    (BREAKOUT, the shorts, intraday setups) returns True and is governed by
    _HARD_BLOCKS / affinity instead. {TREND_UP, EARLY_TREND_UP} is the uptrend
    bucket; {RANGE, RANGE_ROTATING} the range bucket (see note above).
    """
    allowed = _SWING_SETUP_REGIMES.get(str(setup or "").strip().upper())
    if allowed is None:
        return True
    return str(regime or "").strip().upper() in allowed


# ── CORE-4 regime fold (2026-06-24) ─────────────────────────────────────────
# Every validated backtest gates on the 4 BASE regimes. The live brain v4
# (Phase D) emits refinements — EARLY_TREND_UP/DOWN, RANGE_ROTATING — that the
# validated gate stack never saw (swing_s2_canonical loads a pre-folded core-4
# regime artifact; intraday_baseline folds via CORE_MAP at load). Live
# trading_service was passing the REFINED regime straight into the gates, so the
# (intended, core-4-validated) playbook vetoed validated cells on refined-regime
# days. Folding the regime back to its base BEFORE the swing gates restores
# exact parity with the backtest. Value-identical to
# backtest_v2.brain_reconstruct.CORE_MAP (drift-guarded by test).
_CORE4_FOLD: dict[str, str] = {
    "EARLY_TREND_UP": "TREND_UP",
    "EARLY_TREND_DOWN": "TREND_DOWN",
    "RANGE_ROTATING": "RANGE",
}


def core4_regime(regime: str) -> str:
    """Fold a refined brain regime to its CORE-4 base (identity for base regimes).

    Mirrors backtest_v2.brain_reconstruct.CORE_MAP so live gating matches the
    validated backtests, which all gate on the folded core-4 regime set.
    """
    r = str(regime or "").strip().upper()
    return _CORE4_FOLD.get(r, r)


# Reserve-2-trend slot allocation (2026-06 swing-config): the RANGE-bucket cell
# (MEAN_REVERSION) may hold at most 3 of the 5 swing slots concurrently, keeping
# 2 free for the TREND-bucket cells (MOMENTUM/PULLBACK). Backtest evidence: MR
# signals are ~5× more frequent and otherwise crowd out the trend trades exactly
# when a new uptrend starts — the reserve added ~+8k NET at ₹1L vs no cap.
SWING_RANGE_GROUP_CAP = 3
_SWING_RANGE_GROUP = {"MEAN_REVERSION"}


def swing_setup_group(setup: str) -> str:
    """Slot-allocation group: 'RANGE' for MEAN_REVERSION, 'TREND' for the rest."""
    return "RANGE" if str(setup or "").strip().upper() in _SWING_RANGE_GROUP else "TREND"
