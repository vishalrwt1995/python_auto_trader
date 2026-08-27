"""Final prod-faithful swing backtest engine.

All entry/exit/sizing layers use REAL prod domain code with correct deployed config.
No approximations except one documented gap (max_pain_dist_pct unavailable historically).

DEPLOYED PROD CONFIG (as of 2026-06-29):
    SWING_RISK_PER_TRADE = 7500   (₹7,500 flat — NOT 1.5% formula)
    SWING_MIN_SIGNAL_SCORE = 45
    SWING_MAX_HOLD_DAYS = 20
    CAPITAL_SWING = 500000        (₹5L)
    swing_atr_sl_mult = 2.5       (code default, no env override)

EMPIRICAL VALIDATION (2026-06-29, 8,271 May-2026 prod scan_decisions):
    Affinity multiplier   99.99% match (8,270/8,271; 1 miss = HOLD direction)
    ATR multiplier        100.0% match (8,271/8,271; 6-regime era only)
    Score qualification   99.86% match (2,115/2,118 score-decisive rows)
    — confirms scoring + sizing layers are faithful to deployed prod code

EMPIRICAL VALIDATION v2 (2026-07-01, widened to full April-July scan_decisions,
corrected methodology — see PROJECT_KNOWLEDGE.md for the full investigation):
    Two methodology bugs found + fixed before trusting any number:
      1. scan_decisions.adjusted_score is ALWAYS the brain-haircut-inclusive score
         (trading_service.py:1201); swing's real gate qualifies on _affinity_score
         (haircut-free, :1721). Comparing against the haircut-inclusive column
         understates match rate whenever risk_mode != NORMAL — fixed by
         reconstructing the correct reference value.
      2. Prod's own code changed mid-window (regime taxonomy dropped
         EARLY_TREND_UP/DOWN + CHOP on 2026-06-27; swing_setup_allowed_in_regime()
         and the adaptive ATR mult were themselves shipped inside this window).
         Comparing current code to older logged decisions shows drift from prod
         being a different codebase then — not a backtest fidelity gap.
    On the stable post-06-27 window (current deployed config, 1,517 rows,
    1,473 score-decisive): full pipeline (affinity+haircut) match 100.00%,
    swing-qualification match (regime-allowlist + score vs prod `qualified`)
    100.00%. Stronger and more current than the v1 May-only number above.
    Side-finding (informational, NOT fixed — prod code, out of scope): prod's
    own blocked_reason label at trading_service.py:1813 checks `adjusted_score`
    instead of the swing-aware `_score_for_threshold` — cosmetic log mislabel
    only, does not affect actual qualification (line 1722 uses the right var).

FAITHFULNESS AUDIT vs prod (2026-06-29, post entry/exit deep audit):
    Universe eligibility   ✅  BALANCED gates: top-1000 turnover, ≥180 bars,
                               price ≥30, ATR% ≤12%, gap-risk ≤6% (UNIVERSE_MODE
                               default = BALANCED, confirmed not overridden in prod)
    Data                   ✅  bt_bhavcopy_adj: split/bonus-adjusted, survivorship-free
    Regime                 ✅  regime_faithful_2015.json: byte-identical 2015-2026
                               reconstruction running REAL _build_state/_map_regime
    Entry pre-filter (T4)  ✅  run() accepts emit_floor param. Prod watchlist floor≈1
                               (all ~150+ names checked); EMIT_FLOOR=45 was too tight →
                               missed 36 PULLBACK signals/yr. T4 sweep (2022-2026 MOM+PB):
                               floor=45 → CAGR=0.5%/PB=11t; floor=10 → CAGR=5.0%/PB=47t
                               (Calmar=0.43). PULLBACK is the alpha driver (+₹141k at
                               floor=10 vs +₹19k at 45). Optimal floor=10 for backtests.
    Direction              ✅  determine_direction() — real prod, all setups incl. MR
    Entry gates            ✅  check_swing_entry() — byte-exact
    Score (Layer 1)        ✅  VIX + nifty_pct + FII from market_inputs_2015.json
    Score (Layer 2)        ⚠   PCR + oi_change_pcr ✅; max_pain_dist_pct ≡ 0 (no
                               historical max-pain → neutral 4 pts / 12 pts max)
    Score (Layers 3-5)     ✅  compute_indicators + compute_daily_bias on daily bars
    Score threshold        ✅  adj_score = score * regime_multiplier ≥ 45 (affinity
                               score, no brain risk-mode haircut — matches swing path)
    Swing regime gate      ✅  swing_setup_allowed_in_regime(): MOMENTUM/PULLBACK →
                               TREND_UP only; MR → RANGE/RANGE_ROTATING/RECOVERY only
    Hard blocks            ✅  regime_hard_blocks_strategy()
    Sizing                 ✅  calc_swing_position_size(), RISK=7500; adaptive ATR SL
                               mult (risk_mode×regime×ATR%-band, →2.5 fallback at base
                               1.5) — 100% ATR match on 8.3k May-2026 6-regime scans
                               (prior 90.5% miss was pre-2026-06-24 RANGE_ROTATING rows)
    Exit                   ✅  simulate_exit(): arm 1.75R, trail 1.0R, max 20d, gap-fill
                               — shares trailed_stop() with live swing_reconciliation
    Costs                  ✅  CostConfig.upstox() — Upstox round-trip rates
    Paper slippage         ✅  0.10%/leg adverse on entry+exit fills (order_service)
    Daily breaker          ✅  per-channel 3% loss (→ MR-only) / 6% profit (→ full block)
    Slot model             ✅  5 slots, last reserved for PULLBACK, RANGE-group cap 3,
                               (sym) dedup, candidates sorted by wl_score
    Candidate ordering (T7) ✅ Stage 2 sorts by wl_score (pre-filter component score)
                               matching prod's watchlist sort. adj_score stored for
                               diagnostics but not used for slot priority.
    Sector divers. (T5)    ✅  USE_PLAYBOOK_V1=true in prod DISABLES sector/strategy-
                               concentration gates → correctly omitted here. RANGE-group
                               cap 3 already models the MR slot limit.
    RS sector-blend (T6)   ✅  score_signal uses universe-Z RS from compute_indicators;
                               0.6·z_sector+0.4·z_univ would need sector map. Not needed:
                               99.86% empirical score match (2,115/2,118) confirms
                               negligible sector-blend gap.
    Breadth gates (T10)    ✅  b200<70 blocks MOMENTUM/PULLBACK (trading_service.py:1564);
                               breadth<60 blocks PULLBACK (line 1554). Both computed
                               per day from the universe (same formula as prod brain).
                               Comment: "OOS Calmar 0.79 vs 0.10 baseline" in prod code.
    DD governor (T3)       ⚠   Weekly-5%/monthly-8% halt modeled from SWING-ONLY
                               realized P&L; real check_can_open() (portfolio_book.py)
                               sums daily/weekly/monthly P&L GLOBALLY across intraday+
                               swing+positional+hedge (get_realized_pnl_since has no
                               channel filter). Swing-only proxy is an approximation,
                               not a replication — bias direction unknown (depends on
                               intraday's contribution on any given day). Daily 1.5%
                               qty-halve NOT modeled at all (see residual gaps: 2026-07-01
                               investigation found it's global-scoped + likely near-zero
                               impact for swing given 09:22 scan timing — deliberately
                               left unmodeled rather than approximated).
    max_trades_day (T8)    ✅  5 entries/day/channel cap (matches prod gate)
    Daily-breaker (T9)     ✅  Uses prev-day realized P&L (swing exits EOD, entries
                               at scan time → no same-day look-ahead)

KNOWN RESIDUAL GAPS (not modeled — all small/structural, bias noted):
    DD governor 1.5%-daily-DD throttle (halve qty): NOT modeled. Investigated
        2026-07-01: portfolio_book.check_can_open() scopes daily/weekly/monthly P&L
        GLOBALLY (intraday+swing combined, no channel filter) — faithfully modeling
        it here would need intraday P&L history that doesn't exist for 2015-2026.
        Separately, swing's single scan fires at 09:22 IST (~7min post-open), when
        same-day realized P&L from either channel is essentially always ~0 — so the
        gate likely rarely binds for swing regardless of scope. Given the data gap
        and low apparent impact, decided NOT to build a swing-only-proxy
        approximation (would add complexity of unproven value). Same global-scope
        caveat applies to the T3 weekly/monthly halt above, which IS modeled but
        only as a swing-only proxy for what's really a global metric.
        Channel-budget risk cap (₹37.5k max open risk vs ₹5L budget) never binds
        → safely ignored.
    Real intraday entry timing (T11): prod enters at scan-time live LTP (4 scans/day
        at 09:22/11:00/13:00/14:30 IST); here at next-day OPEN (daily-bar limitation).
        Bias ambiguous. Implementing T11 requires 5m GCS candle data per signal day.
        Stretch goal — omitted in current engine.
    Playbook (USE_PLAYBOOK_V1=true): disables prod's sector/strategy-concentration gates
        "to preserve backtest parity" → correctly omitted here.

MR DIAGNOSTIC (2026-06-29, 2022-2026):
    MOMENTUM+PULLBACK, floor=45: 79 trades, WR=46.8%, NET=+₹10,638, CAGR=+0.5%, maxDD=−11%
    MR only (RANGE):             73 trades, WR=43.8%, NET=−₹65,709, CAGR=−2.5%, maxDD=−13%
    Removing MR = +3.1pp CAGR improvement, maxDD nearly halved. MR has wildly negative
    expectancy in 2022-2026 (2024 alone: −₹62k). Regime = 47% RANGE, 22% TREND_UP
    (post core4-fold). High RANGE share forces MR as the dominant setup despite
    negative edge. ⚠ Needs user approval before disabling MR in prod.

T4 RESULTS (2026-06-29, MOM+PB only, 2022-2026, adj_score sort — T7 not applied):
    floor=45: 79t, PB=11t, MOM=68t, NET=+₹10,638, CAGR=0.5%, Calmar=0.04
    floor=30: 88t, PB=36t, MOM=52t, NET=−₹43,290, CAGR=−2.1%, Calmar=−0.11  ← worse
    floor=20: 97t, PB=47t, MOM=50t, NET=+₹65,770, CAGR=+2.9%, Calmar=+0.24
    floor=10: 97t, PB=47t, MOM=50t, NET=+₹117,111, CAGR=+5.0%, Calmar=+0.43  ← BEST
    floor=1:  95t, PB=43t, MOM=52t, NET=+₹48,323,  CAGR=+2.2%, Calmar=+0.21
    PULLBACK is the alpha driver: floor 45→10 grows PB from 11→47 trades and PB alone
    earns +₹141k (vs +₹19k at 45). MOMENTUM is marginally negative at all floors.
    Optimal floor = 10 for MOM+PB backtests. Need to validate on 2015-2026 (--long).

DATA NOTE:
    swing_adj_bars.pkl rebuilt 2014-01-01 → effective backtest start ~2015-01
    (180-bar warmup). Use --long flag + 2015+ pickle for full 11-year runs.
"""
from __future__ import annotations

import collections
import json
import random as _random
import math
import os
import statistics
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from autotrader.domain.daily_bias import compute_daily_bias
from autotrader.domain.indicators import compute_indicators
from autotrader.domain.models import FiiDiiSnapshot, NiftySnapshot, PcrSnapshot, RegimeSnapshot
from autotrader.domain.regime_affinity import (
    SWING_RANGE_GROUP_CAP,
    core4_regime,
    regime_hard_blocks_strategy,
    regime_strategy_multiplier,
    swing_setup_allowed_in_regime,
    swing_setup_group,
)
from autotrader.domain.risk import calc_swing_position_size
from autotrader.domain.scoring import check_swing_entry, determine_direction, score_signal
from autotrader.domain.swing_exit import (
    DEFAULT_ACTIVATE_R,
    DEFAULT_MAX_HOLD_DAYS,
    DEFAULT_TRAIL_R,
    simulate_exit,
)
from autotrader.backtest.costs import CostConfig, compute_leg_cost
from autotrader.settings import StrategySettings

CACHE = os.path.expanduser("~/.autotrader_backtest_cache")
BARS_PKL = os.path.join(CACHE, "swing_adj_bars.pkl")
REGIME_JSON = os.path.join(CACHE, "regime_faithful_2015.json")
MARKET_INPUTS_JSON = os.path.join(CACHE, "market_inputs_2015.json")
SECTOR_MAPPING_JSON = os.path.join(CACHE, "sector_mapping.json")
UPSTOX = CostConfig.upstox()


def _load_sym_sector() -> dict:
    """sym -> sector, from the cached ISIN-keyed sector_mapping.json (2,685 ISINs)."""
    try:
        raw = json.load(open(SECTOR_MAPPING_JSON))
    except Exception:
        return {}
    out = {}
    for _isin, _row in raw.items():
        _sym = _row.get("sym"); _sec = _row.get("sector")
        if _sym and _sec:
            out[_sym] = _sec
    return out


SYM_SECTOR = _load_sym_sector()

# ── Deployed prod config (2026-06-29) ────────────────────────────────────────
RISK = 7500.0                        # SWING_RISK_PER_TRADE env override (flat, not 1.5%)
CAP = 500_000.0                      # CAPITAL_SWING env override
ATR_SL_MULT = 2.5                    # swing_atr_sl_mult (code default)
EMIT_FLOOR = 45.0                    # SWING_MIN_SIGNAL_SCORE env override
CAND_SORT = None                     # None = prod behaviour (sort by -wl_score).
                                     # "random" = shuffle instead, to test whether the
                                     # scorer's slot-ordering role carries any signal.
_CAND_RNG = None                     # set to random.Random(seed) by the caller per run
MAX_HOLD = DEFAULT_MAX_HOLD_DAYS     # = 20 — SWING_MAX_HOLD_DAYS env override
ACTIVATE_R = DEFAULT_ACTIVATE_R      # = 1.75
TRAIL_R = DEFAULT_TRAIL_R            # = 1.0
SLIP = 0.0010                        # paper_entry/exit_slippage_pct (order_service.py:161) — 0.10%/leg adverse
# --- 2026-08-18 grind knobs (defaults reproduce prod EXACTLY; overridden via CLI like SLIP) ---
MOM_B200_FLOOR = 70.0                # prod gate (trading_service.py:1614). --mom-b200-floor to sweep.
MOM_B200_SUSTAIN = 0                 # HYSTERESIS: require b200 >= floor for this many CONSECUTIVE
#                                      days before a new MOMENTUM entry. 0/1 = off (prod behaviour).
#                                      Motivated by 2025: all 6 trades entered at b200 71.0-71.1,
#                                      i.e. AT the gate, and 4 of 6 stopped out — while the 80+
#                                      bucket averaged +Rs2,126/trade vs +Rs479 for 70-80. Raising the
#                                      threshold failed the plateau test (85 -> Calmar 0.03), so the
#                                      hypothesis is that the problem is entering AT the boundary,
#                                      not the boundary's level.
_B200_SEQ: list[tuple[str, float]] = []   # (day, b200) for the CURRENT run; self-resets per run
MOM_TOV_EXCL_REGIMES = ("TREND_UP",)  # cells the turnover dead-zone applies to. Prod = TREND_UP only;
#                                      the in-code warning says all 3 TU filters sign-flip on RANGE,
#                                      so broadening this is a RE-VALIDATION test, not a fix.
ATR_BASE_MULT = 1.5                  # intraday atr_sl_mult; adaptive swing mult built off this (trading_service.py:1226-1234)

# ── Universe gates (BALANCED, from universe_service.py) ──────────────────────
SWING_TOPN_TURNOVER = 1000
MIN_BARS_SWING = 180
MIN_PRICE_SWING = 30.0
MAX_ATR_PCT_SWING = 0.12
MAX_GAP_RISK_SWING = 0.06

MULTI_EMIT = ("MOMENTUM", "PULLBACK", "MEAN_REVERSION")


# ── Sym: fast per-symbol pre-computed series for eligibility + pre-filter ─────

def _clip01(v: float) -> float:
    return 0.0 if not math.isfinite(v) else max(0.0, min(1.0, v))


def _norm(v: float, lo: float, hi: float) -> float:
    return 0.0 if hi <= lo or not math.isfinite(v) else _clip01((v - lo) / (hi - lo))


def _ema_series(c: list[float], period: int) -> list[float]:
    if not c:
        return []
    a = 2.0 / (period + 1.0)
    out = [c[0]]
    for x in c[1:]:
        out.append(a * x + (1 - a) * out[-1])
    return out


def _atr_series(o, h, l, c) -> list[float]:
    n = len(c)
    out = [0.0] * n
    trs = [0.0] * n
    for i in range(1, n):
        trs[i] = max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1]))
    if n <= 14:
        return out
    atr = sum(trs[1:15]) / 14.0
    out[14] = atr
    for i in range(15, n):
        atr = (atr * 13 + trs[i]) / 14.0
        out[i] = atr
    return out


def _adx_series(o, h, l, c) -> list[float]:
    n = len(c)
    out = [25.0] * n
    if n < 30:
        return out
    pdm = [0.0] * n; mdm = [0.0] * n; tr = [0.0] * n
    for i in range(1, n):
        up = h[i] - h[i - 1]; dn = l[i - 1] - l[i]
        pdm[i] = up if (up > dn and up > 0) else 0.0
        mdm[i] = dn if (dn > up and dn > 0) else 0.0
        tr[i] = max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1]))
    s_tr = sum(tr[1:15]); s_p = sum(pdm[1:15]); s_m = sum(mdm[1:15])
    dx = []; dx_idx = []
    for i in range(15, n):
        s_tr = s_tr - s_tr / 14 + tr[i]
        s_p = s_p - s_p / 14 + pdm[i]
        s_m = s_m - s_m / 14 + mdm[i]
        if s_tr == 0:
            continue
        pdi = 100 * s_p / s_tr; mdi = 100 * s_m / s_tr
        dsum = pdi + mdi
        dx.append(100 * abs(pdi - mdi) / dsum if dsum > 0 else 0.0)
        dx_idx.append(i)
    if len(dx) < 14:
        return out
    adx = sum(dx[:14]) / 14.0
    out[dx_idx[13]] = round(adx, 2)
    for j in range(14, len(dx)):
        adx = (adx * 13 + dx[j]) / 14.0
        out[dx_idx[j]] = round(adx, 2)
    return out


def _rsi_series(c: list[float]) -> list[float]:
    n = len(c)
    out = [50.0] * n
    if n < 15:
        return out
    ag = al = 0.0
    for i in range(1, 15):
        d = c[i] - c[i - 1]
        ag += d if d > 0 else 0.0
        al += -d if d < 0 else 0.0
    ag /= 14; al /= 14
    out[14] = 100 - 100 / (1 + ag / (al or 0.001))
    for i in range(15, n):
        d = c[i] - c[i - 1]
        ag = (ag * 13 + (d if d > 0 else 0)) / 14
        al = (al * 13 + (-d if d < 0 else 0)) / 14
        out[i] = 100 - 100 / (1 + ag / (al or 0.001))
    return out


class Sym:
    __slots__ = ("bars", "d", "o", "h", "l", "c", "v", "idx", "ema20", "ema50",
                 "ema200", "atr", "adx", "rsi", "turn", "turnmed60",
                 "high20", "low20", "vol20med", "sma200")

    def __init__(self, bars):
        self.bars = bars
        self.d = [b[0] for b in bars]
        self.o = [b[1] for b in bars]; self.h = [b[2] for b in bars]
        self.l = [b[3] for b in bars]; self.c = [b[4] for b in bars]
        self.v = [b[5] for b in bars]
        self.idx = {dt: i for i, dt in enumerate(self.d)}
        self.ema20 = _ema_series(self.c, 20)
        self.ema50 = _ema_series(self.c, 50)
        self.ema200 = _ema_series(self.c, 200)
        self.atr = _atr_series(self.o, self.h, self.l, self.c)
        self.adx = _adx_series(self.o, self.h, self.l, self.c)
        self.rsi = _rsi_series(self.c)
        self.turn = [self.c[i] * self.v[i] for i in range(len(bars))]
        self.turnmed60 = [0.0] * len(bars)
        for i in range(len(bars)):
            w = self.turn[max(0, i - 59): i + 1]
            self.turnmed60[i] = statistics.median(w) if w else 0.0

        # ── PRECOMPUTED ROLLING STATS (2026-08-26) ────────────────────────────────
        # These four were recomputed inside _component_scores / the day loop for every
        # (symbol, day) — 6.3M times PER ARM. Sym is built once and shared across arms,
        # so hoisting them here makes each arm dramatically cheaper. Windows are
        # asymmetric; see the comments — they are reproduced exactly.
        n = len(bars)
        # rolling max/min over the last 20 bars INCLUDING j, via monotonic deques -> O(n)
        self.high20 = [0.0] * n
        self.low20 = [0.0] * n
        _dqh: collections.deque = collections.deque()   # indices, decreasing h
        _dql: collections.deque = collections.deque()   # indices, increasing l
        for i in range(n):
            lo_bound = max(0, i - 19)
            while _dqh and _dqh[0] < lo_bound:
                _dqh.popleft()
            while _dql and _dql[0] < lo_bound:
                _dql.popleft()
            hi = self.h[i]
            while _dqh and self.h[_dqh[-1]] <= hi:
                _dqh.pop()
            _dqh.append(i)
            lw = self.l[i]
            while _dql and self.l[_dql[-1]] >= lw:
                _dql.pop()
            _dql.append(i)
            self.high20[i] = self.h[_dqh[0]]
            self.low20[i] = self.l[_dql[0]]
        # median volume over the 20 bars EXCLUDING j (0.0 when the window is empty).
        # Left as a slice median: it is the smaller term and exactness matters more here.
        self.vol20med = [0.0] * n
        for i in range(n):
            w = self.v[max(0, i - 20): i]
            self.vol20med[i] = statistics.median(w) if w else 0.0
        # 200-bar SMA over bars EXCLUDING j, 0.0 before warmup, via prefix sums -> O(n)
        _pref = [0.0] * (n + 1)
        for i in range(n):
            _pref[i + 1] = _pref[i] + self.c[i]
        self.sma200 = [((_pref[i] - _pref[i - 200]) / 200.0) if i >= 200 else 0.0
                       for i in range(n)]


def _ret(c, j, n):
    return (c[j] / c[j - n] - 1.0) if j >= n and c[j - n] > 0 else 0.0


def _component_scores(s: Sym, j, ret_mean, ret_std):
    """4-component watchlist score used as a fast pre-filter before compute_indicators.
    Same formula as swing_prod_faithful.py component_scores() — universe-level RS
    uses the universe-z fallback (no sector map needed for pre-filtering).
    """
    c = s.c[j]
    ret60 = _ret(s.c, j, 60) or _ret(s.c, j, 20)
    z_u = (ret60 - ret_mean) / ret_std if ret_std > 1e-9 else 0.0
    rs = _norm(max(-3.0, min(3.0, z_u)), -3.0, 3.0)
    high20 = s.high20[j]; low20 = s.low20[j]      # precomputed (was O(20) slice+scan)
    volmed = s.vol20med[j]                        # precomputed (was a sorting median)
    vr = (s.v[j] / volmed) if volmed > 0 else 1.0
    ema20, ema50, ema200 = s.ema20[j], s.ema50[j], s.ema200[j]
    ema50p = s.ema50[j - 20] if j >= 20 else ema50
    atr = s.atr[j]; atr_pct = atr / c if c > 0 else 0.0
    adx = s.adx[j]; rsi = s.rsi[j]
    breakout_c = max(0.0, 1.0 - (((high20 - c) / high20) if high20 > 0 else 0.0) * 5.0)
    vol_c = min(2.0, vr) / 2.0
    trend_c = 1.0 if (c > ema50 > ema200) else 0.0
    adx_c = _norm(adx, 15.0, 40.0)
    breakout = _clip01(0.30 * rs + 0.25 * breakout_c + 0.15 * vol_c + 0.15 * trend_c + 0.15 * adx_c) * 100
    if ema50 > ema200 and atr > 0:
        pb_depth = _clip01((ema20 - c) / atr / 2.0)
        slope = (ema50 - ema50p) / 20.0
        ts = _clip01(slope / (max(1e-6, atr_pct) * c))
    else:
        pb_depth = 0.0; ts = 0.0
    vl3 = statistics.mean(s.v[max(0, j - 2): j + 1]) if j >= 2 else s.v[j]
    vp3 = statistics.mean(s.v[max(0, j - 5): j - 2]) if j >= 5 else vl3
    vcon = 1.0 if (vp3 > 0 and vl3 < vp3 * 0.85) else (0.5 if vr < 1.0 else 0.0)
    pullback = _clip01(0.40 * ts + 0.40 * pb_depth + 0.20 * vcon) * 100
    rden = max(1e-6, high20 - low20); rpos = (c - low20) / rden
    mr_c = _clip01(1.0 - rpos)
    vsan = 1.0 if 0.01 <= atr_pct <= 0.035 else 0.0
    rsi_b = 1.0 if 25 <= rsi <= 40 else 0.7 if rsi < 25 else 0.3 if 40 < rsi <= 50 else 0.0
    vspike = _clip01(vr / 2.0) if rpos < 0.35 else 0.0
    ret5 = _ret(s.c, j, 5)
    bounce = _clip01(ret5 / 0.05) if ret5 > 0 else 0.0
    low252 = min(s.l[max(0, j - 251): j + 1])
    sdist = (c - low252) / c if c > 0 else 1.0
    supp = max(0.0, 1.0 - sdist * 8.0)
    mean_rev = _clip01(0.30 * mr_c + 0.20 * rsi_b + 0.15 * vspike + 0.10 * bounce + 0.10 * supp + 0.15 * vsan) * 100
    mtrend = 1.0 if (c > ema200 > 0 and ema50 > ema200) else 0.0
    mpers = _clip01(ret5 / 0.03) if ret5 > 0 else 0.0
    momentum = _clip01(0.50 * rs + 0.20 * mtrend + 0.15 * adx_c + 0.10 * mpers + 0.05 * vol_c) * 100
    return {"MOMENTUM": momentum, "PULLBACK": pullback, "MEAN_REVERSION": mean_rev, "BREAKOUT": breakout}


def eligible(s: Sym, j):
    if j + 1 < MIN_BARS_SWING:
        return False
    if s.c[j] < MIN_PRICE_SWING:
        return False
    atr_pct = s.atr[j] / s.c[j] if s.c[j] > 0 else 1.0
    if atr_pct > MAX_ATR_PCT_SWING:
        return False
    gaps = [abs(s.o[i] / s.c[i - 1] - 1.0) for i in range(max(1, j - 59), j + 1) if s.c[i - 1] > 0]
    if gaps and (sum(gaps) / len(gaps)) > MAX_GAP_RISK_SWING:
        return False
    return True


# ── RegimeSnapshot builder — populated from real historical market inputs ──────

def build_regime_snapshot(regime_label: str, mi: dict) -> RegimeSnapshot:
    """Build RegimeSnapshot from faithful regime + per-day market inputs.

    Layers populated in score_signal:
      Layer 1 (Regime, 20pts): VIX ✅  nifty_pct ✅  FII ✅
      Layer 2 (Options, 15pts): PCR ✅  oi_change_pcr ✅  max_pain_dist_pct ≡ 0 (⚠ neutral 4pts)
      Layers 3-5: computed from ind + daily_bias (passed separately)
    """
    vix = float(mi.get("vix") or 14.0)
    nifty_pct = float(mi.get("nifty_pct") or 0.0)
    nifty_close = float(mi.get("nifty_close") or 22000.0)
    pcr = float(mi.get("pcr") or 1.0)
    oi_change_pcr = float(mi.get("oi_change_pcr") or 1.0)
    fii = float(mi.get("fii") or 0.0)

    regime_upper = str(regime_label or "").upper()
    if regime_upper in ("TREND_UP", "RECOVERY"):
        bias = "BULLISH"
    elif regime_upper in ("TREND_DOWN", "PANIC"):
        bias = "BEARISH"
    else:
        bias = "NEUTRAL"

    return RegimeSnapshot(
        regime=regime_upper,
        bias=bias,
        vix=vix,
        nifty=NiftySnapshot(change_pct=nifty_pct, ltp=nifty_close),
        pcr=PcrSnapshot(pcr=pcr, oi_change_pcr=oi_change_pcr),
        fii=FiiDiiSnapshot(fii=fii),
        confidence=0.8,
        data_health=0.9,
    )


def adaptive_atr_mult(reg: str, risk_mode: str, setup: str, atr: float, px: float) -> float:
    """Prod's adaptive swing SL multiplier — replicates trading_service.py:1218-1257.

    Built off the 1.5 intraday base (NOT swing's 2.5), scaled by a risk_mode/regime
    tier then an ATR%-band, clamped [0.8, 3.0]. Validated against 27,741 logged prod
    swing scans (90.5% exact; misses are pre-2026-06-24 old-code rows). The caller
    passes this as atr_mult_override UNLESS it lands on 1.5 — in which case prod falls
    back to swing_atr_sl_mult=2.5 (override=None), so 2.5 IS used for base-tier names.
    """
    base = ATR_BASE_MULT
    is_rev = setup in ("MEAN_REVERSION", "VWAP_REVERSAL")
    if risk_mode == "LOCKDOWN" or reg == "PANIC":
        m = round(base * 0.75, 3)
    elif risk_mode == "DEFENSIVE" or reg == "TREND_DOWN":
        m = round(base * 0.87, 3)
    elif risk_mode == "AGGRESSIVE" and reg == "TREND_UP":
        m = round(base * 1.20, 3)
    elif is_rev and reg in ("RANGE", "RECOVERY"):
        m = round(base * 1.33, 3)
    else:
        m = base
    if px > 0 and atr > 0:                       # ATR%-band (matches prod's gate on ltp/atr)
        p = atr / px
        if p < 0.015:
            m = round(m * 0.87, 3)
        elif p <= 0.030:
            m = round(m * 1.20, 3)
        m = max(0.8, min(3.0, m))
    return m


# ── DD governor helpers (T3) ─────────────────────────────────────────────────

def _week_key(d: str) -> str:
    """ISO year-week key for DD governor weekly halt."""
    from datetime import date as _date
    dt = _date.fromisoformat(d)
    y, w, _ = dt.isocalendar()
    return f"{y}-W{w:02d}"


def _month_key(d: str) -> str:
    """Calendar month key for DD governor monthly halt."""
    return d[:7]   # YYYY-MM


# ── Data-gap guard (2026-07-02 finding) ─────────────────────────────────────
# bt_bhavcopy_adj has real holes for some symbols (suspension/illiquidity) —
# consecutive bars in `s.d` can jump weeks/months with no warning. A trade
# whose hold window spans one of these gets a phantom entry/exit price pair
# and an R-multiple with no basis in real trading (found: -3.46R to +5.35R,
# vs every genuine trade in this engine landing in the normal -1.1R..+3R
# band). Detected directly on the date sequence, not inferred from hold-vs-
# calendar-span after the fact, so it catches the gap wherever it sits in
# the holding window, not just at entry or exit.
_MAX_INTRABAR_GAP_DAYS = 6   # generous: covers a long-weekend + one holiday


def _spans_data_gap(dates: list, lo: int, hi: int) -> bool:
    """True if any two consecutive bars in dates[lo:hi] are >6 calendar days
    apart — a real trading gap (weekend+holiday) never gets close to this;
    it means bars are simply missing, not that no trading happened."""
    from datetime import date as _date
    for k in range(lo, hi):
        d0 = _date.fromisoformat(dates[k])
        d1 = _date.fromisoformat(dates[k + 1])
        if (d1 - d0).days > _MAX_INTRABAR_GAP_DAYS:
            return True
    return False


# ── Main entry ────────────────────────────────────────────────────────────────

def run(symdata, regime: dict, market_inputs: dict,
        d0="2022-01-03", d1="2026-12-31",
        long_only=False, verbose=True,
        setups=None, emit_floor=None,
        trades_out=None,
        pb_month_block=None,
        pb_b200_floor=None,
        pb_month_block_maxb200=None,
        setup_daily_cap=None,
        mom_month_block=None,
        mom_exit_override=None,
        mom_turnover_exclude=None,
        setup_daily_cap_rank_by=None,
        mom_regimes=None,
        mom_range_max_off_high=None,
        mom_range_max_dsp=None,
        pb_regimes=None,
        pb_rsi_floor=None,
        pb_exit_override=None,
        mr_month_block=None,
        mr_exit_override=None,
        mr_regimes=None,
        range_bucket_by_regime=False,
        range_group_cap=None,
        total_slots=None,
        tu_slot_cap=None,
        compound_pct=None,
        liq_cap_pct=None,
        bk_regimes=None):
    """Run the final prod-faithful swing backtest.

    Args:
        symdata:       {sym: Sym}  — pre-built from swing_adj_bars.pkl
        regime:        {date: {regime: str, ...}}  — regime_faithful_2015.json
        market_inputs: {date: {vix, nifty_pct, pcr, oi_change_pcr, fii}}
        d0, d1:        backtest window (both inclusive)
        long_only:     if True, suppress SELL signals
        verbose:       print results
        setups:        tuple of setup names to include (default: all MULTI_EMIT)
        emit_floor:    component-score pre-filter floor (default: EMIT_FLOOR=45).
                       T4: lower (e.g. 20) to match prod's ~1 watchlist floor and
                       recover candidates that pass check_swing_entry but were
                       dropped by the tight pre-filter.
    """
    _setups = tuple(setups) if setups else MULTI_EMIT
    _emit_floor = emit_floor if emit_floor is not None else EMIT_FLOOR
    cfg = StrategySettings(
        capital_swing=CAP,
        swing_risk_per_trade=RISK,
        swing_atr_sl_mult=ATR_SL_MULT,
        swing_rr=2.0,
    )
    loss_lim = CAP * 0.03         # ₹15,000 daily loss limit (3%)
    profit_lim = CAP * 0.06       # ₹30,000 daily profit limit (6%)
    cal = sorted(d for d in regime if d0 <= d <= d1)

    # ── Stage 1: generate all qualified signals (portfolio-independent) ──────
    signals = []
    _trend_up_streak = 0   # consecutive TREND_UP days leading into signal day
    _prev_reg = ""
    _last_panic_d = None   # most recent PANIC day seen so far (for days-since-PANIC filter)
    from datetime import date as _dsp_date
    for d in cal:
        regime_entry = regime[d]
        reg = regime_entry.get("regime", "RANGE")
        reg = core4_regime(reg)           # fold 6-label to 4-label
        risk_mode = str(regime_entry.get("risk_mode", "NORMAL"))  # for adaptive ATR mult (T1)
        _trend_up_streak = (_trend_up_streak + 1) if reg == "TREND_UP" else 0
        _prev_reg = reg
        if reg == "PANIC":
            _last_panic_d = d
        _days_since_panic = (
            (_dsp_date.fromisoformat(d) - _dsp_date.fromisoformat(_last_panic_d)).days
            if _last_panic_d else 9999
        )

        mi = market_inputs.get(d, {})
        regime_snap = build_regime_snapshot(reg, mi)

        # eligible universe today + cross-sectional stats for RS computation
        elig = []
        for sym, s in symdata.items():
            j = s.idx.get(d)
            if j is None or not eligible(s, j):
                continue
            elig.append((sym, s, j, s.turnmed60[j]))
        if not elig:
            continue
        elig.sort(key=lambda x: -x[3])
        elig = elig[:SWING_TOPN_TURNOVER]

        ret60s = [_ret(s.c, j, 60) or _ret(s.c, j, 20) for _, s, j, _ in elig]
        ret_mean = statistics.mean(ret60s) if ret60s else 0.0
        ret_std = statistics.pstdev(ret60s) if len(ret60s) > 1 else 0.0
        above = sum(1 for _, s, j, _ in elig if j >= 49 and s.c[j] > sum(s.c[j - 49:j + 1]) / 50.0)
        breadth = 100.0 * above / len(elig) if elig else 0.0
        b200_above = sum(1 for _, s, j, _ in elig if j >= 200 and s.c[j] > s.ema200[j])
        b200_elig = sum(1 for _, s, j, _ in elig if j >= 200)
        b200 = (b200_above * 100.0 / b200_elig) if b200_elig else 0.0
        # hysteresis bookkeeping: dates are ascending within a run, so a non-increasing date
        # means a NEW run started -> reset. Written before any gate reads it, so look-backs
        # only ever see days already processed in this same run (no cross-run leakage).
        if _B200_SEQ and d <= _B200_SEQ[-1][0]:
            _B200_SEQ.clear()
        _B200_SEQ.append((d, b200))

        for sym, s, j, _ in elig:
            if j + 1 >= len(s.c):
                continue

            # Per-symbol values used in multiple setup checks
            ret60 = _ret(s.c, j, 60) or _ret(s.c, j, 20)
            rs_vs_mkt = ret60 - ret_mean
            sma200 = s.sma200[j]                  # precomputed (was O(200) slice+sum)

            # ── Fast pre-filter: component scores (cheap, pre-computed series) ──
            sc = _component_scores(s, j, ret_mean, ret_std)

            for setup in _setups:
                comp = sc[setup]
                if comp < _emit_floor:
                    continue                  # fast reject — saves compute_indicators call

                # ── Swing-only HARD regime gate (trading_service.py:1534) ────
                # MOMENTUM/PULLBACK → TREND_UP only; MR → RANGE/RANGE_ROTATING/
                # RECOVERY only. Prod's _SWING_SETUP_REGIMES allowlist; stronger
                # than regime_hard_blocks_strategy (which doesn't block MOM in RANGE).
                if setup == "MOMENTUM" and mom_regimes is not None:
                    # regime-cell exploration: override becomes the single source of
                    # truth for MOMENTUM's regime permission (allowlist + hard-block)
                    if reg not in mom_regimes:
                        continue
                elif setup == "PULLBACK" and pb_regimes is not None:
                    if reg not in pb_regimes:
                        continue
                elif setup == "MEAN_REVERSION" and mr_regimes is not None:
                    if reg not in mr_regimes:
                        continue
                elif setup == "BREAKOUT" and bk_regimes is not None:
                    if reg not in bk_regimes:
                        continue
                elif not swing_setup_allowed_in_regime(setup, reg):
                    continue

                # Regime-level breadth gates (mirrors swing_prod_faithful.py)
                if setup == "MEAN_REVERSION":
                    if j < 200 or s.c[j] <= sma200:
                        continue             # mr_above_200 gate
                    if rs_vs_mkt <= 0.0:
                        continue             # rs_vs_mkt > 0
                if setup == "PULLBACK":
                    if rs_vs_mkt <= 0.0:
                        continue
                    if breadth < 60.0:
                        continue
                    if pb_month_block and int(d[5:7]) in pb_month_block:
                        # conditional: override the block when market is very strong
                        _max_b200 = pb_month_block_maxb200 if pb_month_block_maxb200 is not None else 100.0
                        if b200 < _max_b200:
                            continue
                if setup == "MOMENTUM" and b200 > 0.0 and b200 < MOM_B200_FLOOR:
                    continue
                if (setup == "MOMENTUM" and MOM_B200_SUSTAIN > 1 and b200 > 0.0
                        and not (len(_B200_SEQ) >= MOM_B200_SUSTAIN
                                 and all(v >= MOM_B200_FLOOR
                                         for _dd, v in _B200_SEQ[-MOM_B200_SUSTAIN:]))):
                    continue      # hysteresis: gate must have HELD, not just been crossed today
                # MOMENTUM filters below are scoped to TREND_UP: they were validated
                # on TREND_UP trades only (2026-07-02); the RANGE cell showed
                # sign-flip/no-transfer for all three (Jan, turnover dead-zone,
                # same-day cap) — do not broaden without re-validation per cell.
                if setup == "MOMENTUM" and reg == "TREND_UP" and mom_month_block and int(d[5:7]) in mom_month_block:
                    continue          # candidate filter: Jan consistently bad IS+OOS (2026-07-02 grind)
                if setup == "MOMENTUM" and reg in MOM_TOV_EXCL_REGIMES and mom_turnover_exclude:
                    _tlo, _thi = mom_turnover_exclude
                    if _tlo <= (s.turnmed60[j] / 1e7) < _thi:
                        continue      # candidate filter: 5-40cr "dead zone" bad IS+OOS (2026-07-02 grind)
                if setup == "MOMENTUM" and reg == "RANGE" and mom_range_max_off_high is not None:
                    # candidate filter (George-Hwang 52w-high effect): RANGE-cell momentum
                    # >15% below its 52w high is consistently BAD IS+OOS; near-high is best.
                    # Monotonic both periods on n=219 (2026-07-02 grind). RANGE-scoped.
                    # ENGINE TEST RESULT: FAILED — slot reallocation swamps the ~20k direct
                    # gain (raw +335,288 vs filtered +314,351). Kept for reference only.
                    _hi52 = max(s.h[max(0, j - 251): j + 1])
                    if _hi52 > 0 and (s.c[j] / _hi52 - 1.0) < -mom_range_max_off_high / 100.0:
                        continue
                if setup == "MOMENTUM" and reg == "RANGE" and mom_range_max_dsp is not None:
                    # candidate filter: RANGE-cell momentum with no PANIC in the last N days
                    # was consistently BAD IS+OOS (>90d: IS -0.15R n=26 / OOS -0.05R n=44).
                    # Weakest mechanism grounding of the three — engine test decides.
                    if _days_since_panic > mom_range_max_dsp:
                        continue
                if setup == "PULLBACK" and b200 < (pb_b200_floor if pb_b200_floor is not None else 70.0):
                    continue
                if setup == "MEAN_REVERSION" and mr_month_block and int(d[5:7]) in mr_month_block:
                    continue          # candidate filter: Jan BAD both periods, 4th cross-setup confirmation (2026-07-02)

                # ── Prod entry pipeline: real indicators → real gate stack ────
                win = s.bars[max(0, j - 299): j + 1]
                try:
                    ind = compute_indicators(win, cfg)
                    db = compute_daily_bias(win)
                except Exception:
                    continue
                if ind is None or db is None:
                    continue
                if setup == "PULLBACK" and pb_rsi_floor is not None and ind.rsi.curr < pb_rsi_floor:
                    # candidate filter (2026-07-02 grind): within PB's 40-60 daily-RSI
                    # reload zone, the sub-47 half is consistently BAD IS+OOS (n=50,
                    # -0.08/-0.20 avgR) — deepest dips keep dipping; upper half is
                    # uniformly good. Engine test decides.
                    continue

                direction = determine_direction(
                    ind, regime_snap, setup=setup, wl_type="swing", daily_bias=db
                )
                if direction == "HOLD":
                    continue
                if long_only and direction == "SELL":
                    continue

                ok, _ = check_swing_entry(setup, direction, ind, db, regime=reg)
                if not ok:
                    continue

                sig = score_signal(
                    sym, direction, ind, regime_snap, cfg, daily_bias=db, setup=setup
                )
                raw_score = int(sig.score)
                mult = regime_strategy_multiplier(reg, setup, direction)
                adj_score = max(0, min(100, int(round(raw_score * mult))))
                if adj_score < EMIT_FLOOR:
                    continue
                if regime_hard_blocks_strategy(reg, setup):
                    _override = (
                        (setup == "MOMENTUM" and mom_regimes is not None and reg in mom_regimes)
                        or (setup == "PULLBACK" and pb_regimes is not None and reg in pb_regimes)
                        or (setup == "MEAN_REVERSION" and mr_regimes is not None and reg in mr_regimes)
                        or (setup == "BREAKOUT" and bk_regimes is not None and reg in bk_regimes)
                    )
                    if not _override:
                        continue

                # ── Sizing (prod risk.py + adaptive ATR mult, T1) ────────────
                ei = j + 1
                entry_px = s.o[ei]
                if entry_px <= 0:
                    continue
                # Adaptive SL mult off signal-day price; ==1.5 → None → swing's 2.5×
                _amult = adaptive_atr_mult(reg, risk_mode, setup, ind.atr, ind.close)
                _override = _amult if _amult != ATR_BASE_MULT else None
                pos = calc_swing_position_size(
                    entry_px, ind.atr, direction, cfg, atr_mult_override=_override
                )
                if pos.qty < 1 or pos.sl_price <= 0:
                    continue
                sl_dist = abs(entry_px - pos.sl_price)
                if sl_dist <= 0:
                    continue

                # ── Exit (prod simulate_exit) ────────────────────────────────
                is_buy = direction == "BUY"
                _max_hold, _trail_r, _activate_r = MAX_HOLD, TRAIL_R, ACTIVATE_R
                if setup == "MOMENTUM" and mom_exit_override:
                    _max_hold = mom_exit_override.get("max_hold", _max_hold)
                    _trail_r = mom_exit_override.get("trail_R", _trail_r)
                    _activate_r = mom_exit_override.get("activate_R", _activate_r)
                if setup == "PULLBACK" and pb_exit_override:
                    _max_hold = pb_exit_override.get("max_hold", _max_hold)
                    _trail_r = pb_exit_override.get("trail_R", _trail_r)
                    _activate_r = pb_exit_override.get("activate_R", _activate_r)
                if setup == "MEAN_REVERSION" and mr_exit_override:
                    _max_hold = mr_exit_override.get("max_hold", _max_hold)
                    _trail_r = mr_exit_override.get("trail_R", _trail_r)
                    _activate_r = mr_exit_override.get("activate_R", _activate_r)
                off, exit_px, reason = simulate_exit(
                    s.bars, ei, is_buy, sl_dist, _max_hold,
                    trail_R=_trail_r, activate_R=_activate_r,
                )
                exit_i = min(ei + off, len(s.bars) - 1)
                if _spans_data_gap(s.d, ei, exit_i):
                    continue        # data-gap artifact — phantom R, not a real trade

                # Paper-mode fill slippage (prod order_service.py: 0.10%/leg, adverse).
                # Sizing stays on the clean price (prod sizes on LTP, fills on LTP±slip).
                entry_fill = entry_px * (1 + SLIP) if is_buy else entry_px * (1 - SLIP)
                exit_fill = exit_px * (1 - SLIP) if is_buy else exit_px * (1 + SLIP)

                gross = ((exit_fill - entry_fill) if is_buy else (entry_fill - exit_fill)) * pos.qty
                cost = (
                    compute_leg_cost(side="BUY" if is_buy else "SELL",
                                     qty=pos.qty, price=entry_fill, is_swing=True, cfg=UPSTOX)
                    + compute_leg_cost(side="SELL" if is_buy else "BUY",
                                       qty=pos.qty, price=exit_fill, is_swing=True, cfg=UPSTOX)
                )
                net = gross - cost
                signals.append({
                    "sig_d": d, "entry_d": s.d[ei], "exit_d": s.d[exit_i],
                    "sym": sym, "setup": setup, "dir": direction,
                    "group": ("RANGE" if (range_bucket_by_regime and reg == "RANGE")
                              else swing_setup_group(setup)), "regime": reg,
                    "risk_mode": risk_mode,
                    "qty": pos.qty, "entry": entry_px, "exit": exit_px,
                    "sl_price": pos.sl_price,
                    "risk": sl_dist * pos.qty, "notional": entry_px * pos.qty,
                    "gross": gross, "net": net, "reason": reason,
                    "hold": off,
                    "R": net / (sl_dist * pos.qty) if sl_dist * pos.qty > 0 else 0.0,
                    "adj_score": adj_score, "raw_score": raw_score,
                    "wl_score": comp,
                    "breadth": round(breadth, 1),
                    "b200": round(b200, 1),
                    "atr_pct": round(100.0 * ind.atr / entry_px, 2) if entry_px > 0 else 0.0,
                    "sl_pct": round(100.0 * sl_dist / entry_px, 2) if entry_px > 0 else 0.0,
                    "trend_streak": _trend_up_streak,
                    "month": int(d[5:7]),
                    "year": int(d[:4]),
                    "rsi": round(ind.rsi.curr, 1),
                    "adx_daily": round(db.adx_daily, 1),
                    "volume_ratio": round(ind.volume.ratio, 2),
                    "sector": SYM_SECTOR.get(sym, "UNKNOWN"),
                    "turnover_cr": round(s.turnmed60[j] / 1e7, 2),
                    "strength": round(float(db.strength or 0.0), 1),
                })

    # ── Stage 2: portfolio walk (5 slots, 1 PULLBACK reserve, daily breaker) ─
    # T3 DD governor thresholds (PortfolioBook, USE_PORTFOLIO_BOOK_V1=true)
    DD_WEEK_HALT  = CAP * 0.05   # 5%  weekly loss  → halt all entries
    DD_MONTH_HALT = CAP * 0.08   # 8%  monthly loss → halt all entries

    by_sig = collections.defaultdict(list)
    for sg in signals:
        by_sig[sg["sig_d"]].append(sg)
    open_pos = []
    realized_day = collections.defaultdict(float)
    week_pnl     = collections.defaultdict(float)  # T3: {week_key: pnl}
    month_pnl    = collections.defaultdict(float)  # T3: {month_key: pnl}
    taken = []
    held_syms = set()
    prev_d = None                                   # T9: previous trading day
    _equity = CAP                                   # compounding: rolling realized equity
    for d in cal:
        still = []
        for p in open_pos:
            if p["exit_d"] < d:
                net = p["net"]
                realized_day[p["exit_d"]] += net
                week_pnl[_week_key(p["exit_d"])]   += net  # T3
                month_pnl[_month_key(p["exit_d"])] += net  # T3
                held_syms.discard(p["sym"])
                _equity += net
            else:
                still.append(p)
        open_pos = still
        # compounding mode: breaker/DD thresholds scale with equity (else 1.0 = flat)
        _hs = (_equity / CAP) if compound_pct else 1.0

        # T9: per-channel daily breaker (trading_service.py:1604-1614).
        # Swing exits happen at EOD reconciliation; entries at scan time → use
        # PREVIOUS day's realized P&L so we don't look into same-day exits.
        prev_pnl = realized_day.get(prev_d, 0.0) if prev_d else 0.0
        if prev_pnl >= profit_lim * _hs:    # profit target → full block
            prev_d = d; continue
        _loss_day = prev_pnl <= -loss_lim * _hs   # loss limit → MR-only

        # T3: DD governor weekly/monthly halts
        if week_pnl.get(_week_key(d), 0.0)   <= -DD_WEEK_HALT * _hs:
            prev_d = d; continue
        if month_pnl.get(_month_key(d), 0.0) <= -DD_MONTH_HALT * _hs:
            prev_d = d; continue

        if CAND_SORT == "random":
            cands = list(by_sig.get(d, []))
            (_CAND_RNG or _random.Random(0)).shuffle(cands)
        else:
            cands = sorted(by_sig.get(d, []), key=lambda x: -x["wl_score"])  # T7: prod sorts by wl_score
        entries_today = 0                           # T8: max_trades_day=5
        _setup_today = collections.defaultdict(int)  # candidate filter: same-day-per-setup cap
        # candidate filter: rank same-day cap survivors by a different key than
        # wl_score (e.g. ADX) while cross-setup slot priority stays wl_score-sorted.
        _cap_ok_ids = None
        if setup_daily_cap and setup_daily_cap_rank_by:
            _cap_ok_ids = set()
            _by_setup_today = collections.defaultdict(list)
            for sg in cands:
                _by_setup_today[sg["setup"]].append(sg)
            for _su, _lst in _by_setup_today.items():
                _rank_key = setup_daily_cap_rank_by.get(_su)
                _cap_n = setup_daily_cap.get(_su, len(_lst))
                _ranked = sorted(_lst, key=lambda x: -x.get(_rank_key, 0.0)) if _rank_key else _lst
                for sg in _ranked[:_cap_n]:
                    _cap_ok_ids.add(id(sg))
        _tslots = total_slots if total_slots is not None else 5
        for sg in cands:
            if entries_today >= 5 or len(open_pos) >= _tslots:  # T8 (entries/day stays 5 = prod gate)
                break
            if _loss_day and sg["setup"] != "MEAN_REVERSION":
                continue                # loss-limit day: counter-trend (MR) only
            # TU sub-book cap (additive-slots mode): non-RANGE-bucket positions are
            # capped at tu_slot_cap (default = total slots → no-op), with the last
            # TU slot reserved for PULLBACK (generalizes the original >=4-of-5 rule;
            # counts only non-RANGE positions, identical to legacy when bucket off).
            _nonrange_open = sum(1 for p in open_pos if p["group"] != "RANGE")
            if sg["group"] != "RANGE":
                _tu_cap = tu_slot_cap if tu_slot_cap is not None else _tslots
                if _nonrange_open >= _tu_cap:
                    continue
                if sg["setup"] not in ("PULLBACK",) and _nonrange_open >= _tu_cap - 1:
                    continue            # last TU slot reserved for PULLBACK
            if sg["sym"] in held_syms:
                continue
            if sg["group"] == "RANGE" and sum(1 for p in open_pos if p["group"] == "RANGE") >= (range_group_cap if range_group_cap is not None else SWING_RANGE_GROUP_CAP):
                continue
            if compound_pct and not sg.get("_scaled"):
                # %-of-rolling-equity sizing: rescale this signal from flat RISK to
                # compound_pct% of current realized equity. Linear scale of qty-
                # proportional fields (costs ≈ linear in qty; ₹20 brokerage cap makes
                # real costs sub-linear → linear is slightly conservative). R invariant.
                _f = (_equity * compound_pct / 100.0) / RISK
                for _k in ("qty", "notional", "risk", "gross", "net"):
                    sg[_k] = sg[_k] * _f
                sg["_scaled"] = True
            if liq_cap_pct and not sg.get("_liqcapped"):
                # LIQUIDITY CAP (prod-replicable): never take more than liq_cap_pct%
                # of the symbol's TRAILING 60d median daily turnover (known pre-trade,
                # no look-ahead — prod computes the identical cap). Shrink qty to fit;
                # P&L scales linearly, R invariant. Keeps sizing in the low-impact
                # regime where the flat 0.10% slippage is actually accurate, so the
                # backtest fill == the prod fill by construction. Compounding then
                # auto-plateaus at the edge's real capacity instead of assuming
                # un-fillable orders.
                _cap_notional = (liq_cap_pct / 100.0) * float(sg.get("turnover_cr", 0.0)) * 1e7
                if _cap_notional > 0 and sg["notional"] > _cap_notional:
                    _shrink = _cap_notional / sg["notional"]
                    for _k in ("qty", "notional", "risk", "gross", "net"):
                        sg[_k] = sg[_k] * _shrink
                elif _cap_notional <= 0:
                    continue   # no turnover data → cannot size safely, skip (fail-closed)
                sg["_liqcapped"] = True
            if sum(p["notional"] for p in open_pos) + sg["notional"] > CAP * _hs:
                continue
            # cap keys may be regime-scoped ("MOMENTUM@TREND_UP") or plain ("MOMENTUM");
            # the scoped key wins when present — validated per-cell, not per-setup.
            _cap_key = None
            if setup_daily_cap:
                _scoped = f"{sg['setup']}@{sg['regime']}"
                _cap_key = _scoped if _scoped in setup_daily_cap else (sg["setup"] if sg["setup"] in setup_daily_cap else None)
            if _cap_key is not None and _setup_today[_cap_key] >= setup_daily_cap[_cap_key]:
                continue                # candidate filter: same-day clustering cap (IS+OOS validated for MOMENTUM×TREND_UP)
            if _cap_ok_ids is not None and sg["setup"] in setup_daily_cap_rank_by and id(sg) not in _cap_ok_ids:
                continue                # candidate filter: excluded by the custom cap-selection ranking key
            open_pos.append(sg); held_syms.add(sg["sym"]); taken.append(sg)
            entries_today += 1  # T8
            if _cap_key is not None:
                _setup_today[_cap_key] += 1
        prev_d = d  # T9

    for p in open_pos:
        net = p["net"]
        realized_day[p["exit_d"]] += net
        week_pnl[_week_key(p["exit_d"])]   += net
        month_pnl[_month_key(p["exit_d"])] += net

    if trades_out is not None:
        trades_out.extend(taken)

    return _report(taken, realized_day, cal, verbose)


def _report(taken, realized_day, cal, verbose):
    if not taken:
        if verbose:
            print("  (no trades)")
        return {"net": 0.0, "n": 0}
    net = sum(t["net"] for t in taken)
    gross = sum(t["gross"] for t in taken)
    wins = sum(1 for t in taken if t["net"] > 0)
    eq = CAP; peak = CAP; mdd = 0.0
    for d in cal:
        eq += realized_day.get(d, 0.0)
        peak = max(peak, eq); mdd = min(mdd, eq / peak - 1.0)
    years = len(cal) / 252.0
    cap_for_report = CAP
    cagr = ((eq / cap_for_report) ** (1 / years) - 1) * 100 if years > 0 and eq > 0 else 0.0
    calmar = cagr / abs(mdd * 100) if mdd < 0 else float("inf")
    by_year = collections.defaultdict(float)
    by_cell = collections.defaultdict(lambda: [0, 0.0])
    by_reg = collections.defaultdict(lambda: [0, 0.0])
    by_dir = collections.defaultdict(lambda: [0, 0.0])
    for t in taken:
        by_year[t["exit_d"][:4]] += t["net"]
        by_cell[t["setup"]][0] += 1; by_cell[t["setup"]][1] += t["net"]
        by_reg[t["regime"]][0] += 1; by_reg[t["regime"]][1] += t["net"]
        dd = t.get("dir", "BUY")
        by_dir[dd][0] += 1; by_dir[dd][1] += t["net"]
    if verbose:
        print(f"  cap=₹{CAP/1e5:.0f}L  risk=₹{RISK:.0f}  trades={len(taken)}  "
              f"WR={100*wins/len(taken):.1f}%  GROSS=₹{gross:,.0f}  NET=₹{net:,.0f}  "
              f"({100*net/cap_for_report/(len(cal)/252):.1f}%/yr)  "
              f"maxDD={mdd*100:.0f}%  CAGR={cagr:.1f}%  Calmar={calmar:.2f}")
        print("    per-year NET:", {y: f"₹{v:,.0f}" for y, v in sorted(by_year.items())})
        print("    per-cell    :", {k: f"n={v[0]} ₹{v[1]:,.0f}" for k, v in by_cell.items()})
        print("    by-direction:", {k: f"n={v[0]} ₹{v[1]:,.0f}" for k, v in by_dir.items()})
        print("    by-regime   :", {k: f"n={v[0]} ₹{v[1]:,.0f}" for k, v in by_reg.items()})
    return {
        "net": net, "n": len(taken), "wr": 100 * wins / len(taken),
        "mdd": mdd * 100, "cagr": cagr, "calmar": calmar,
        "by_year": dict(by_year),
        "by_dir": {k: v[1] for k, v in by_dir.items()},
        "n_sell": by_dir.get("SELL", [0])[0],
    }


def main():
    import pickle
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--pkl", default=BARS_PKL, help="bars pickle path")
    ap.add_argument("--regime", default=REGIME_JSON, help="regime JSON path")
    ap.add_argument("--long", action="store_true", help="run 2015-2026 full history baseline")
    ap.add_argument("--setups", default=None, help="comma-sep setup names, e.g. MOMENTUM,PULLBACK")
    ap.add_argument("--floor", type=float, default=None, help="emit_floor override")
    ap.add_argument("--by-year", action="store_true", help="year-by-year sweep 2015-2026 for --setups")
    ap.add_argument("--d0", default=None, help="start date YYYY-MM-DD (single custom run)")
    ap.add_argument("--d1", default=None, help="end date YYYY-MM-DD (single custom run)")
    ap.add_argument("--mining", action="store_true", help="full setup×year matrix: all setups, every year, 2015-2026")
    ap.add_argument("--trades-out", default=None, help="write executed trades to this CSV path for diagnosis")
    ap.add_argument("--pb-month-block", default=None, help="months to skip for PULLBACK only, e.g. 1,4,7")
    ap.add_argument("--pb-b200-floor", type=float, default=None, help="b200 floor for PULLBACK (default 70)")
    ap.add_argument("--pb-month-block-maxb200", type=float, default=None,
                    help="override month-block when b200 >= this (e.g. 85 = allow Jan/Apr/Jul in very strong markets)")
    ap.add_argument("--setup-daily-cap", default=None,
                    help="cap same-day entries per setup, e.g. MOMENTUM:1 or MOMENTUM:2,PULLBACK:1")
    ap.add_argument("--mom-month-block", default=None, help="months to skip for MOMENTUM only, e.g. 1")
    ap.add_argument("--mom-activate-r", type=float, default=None, help="MOMENTUM-only trail-arm threshold (default 1.75)")
    ap.add_argument("--mom-trail-r", type=float, default=None, help="MOMENTUM-only trail width in R (default 1.0)")
    ap.add_argument("--mom-max-hold", type=int, default=None, help="MOMENTUM-only max hold days (default 20)")
    ap.add_argument("--mom-turnover-exclude", default=None, help="MOMENTUM-only: exclude turnover_cr range lo,hi e.g. 5,40")
    ap.add_argument("--setup-daily-cap-rank-by", default=None, help="rank cap-selection by this field instead of wl_score, e.g. MOMENTUM:adx_daily")
    ap.add_argument("--mom-regimes", default=None,
                    help="regime-cell exploration: override MOMENTUM's allowed regimes (allowlist+hard-block), e.g. RANGE or TREND_UP,RANGE")
    ap.add_argument("--mom-range-max-off-high", type=float, default=None,
                    help="RANGE-cell MOMENTUM only: block entries more than this %% below the 52w high, e.g. 15")
    ap.add_argument("--mom-range-max-dsp", type=int, default=None,
                    help="RANGE-cell MOMENTUM only: block entries when no PANIC in the last N days, e.g. 90")
    ap.add_argument("--pb-regimes", default=None,
                    help="regime-cell exploration: override PULLBACK's allowed regimes (allowlist+hard-block), e.g. RANGE")
    ap.add_argument("--pb-rsi-floor", type=float, default=None,
                    help="PULLBACK only: block entries with signal-day RSI below this (e.g. 47)")
    ap.add_argument("--pb-activate-r", type=float, default=None, help="PULLBACK-only trail-arm threshold (default 1.75)")
    ap.add_argument("--pb-max-hold", type=int, default=None, help="PULLBACK-only max hold days (default 20)")
    ap.add_argument("--mr-month-block", default=None, help="months to skip for MEAN_REVERSION only, e.g. 1")
    ap.add_argument("--mr-activate-r", type=float, default=None, help="MR-only trail-arm threshold (default 1.75)")
    ap.add_argument("--mr-max-hold", type=int, default=None, help="MR-only max hold days (default 20)")
    ap.add_argument("--mr-regimes", default=None,
                    help="regime-cell exploration: override MR's allowed regimes (allowlist+hard-block), e.g. PANIC")
    ap.add_argument("--range-bucket-by-regime", action="store_true",
                    help="slot partition: any RANGE-regime trade joins the RANGE slot bucket (capped)")
    ap.add_argument("--range-group-cap", type=int, default=None,
                    help="max concurrent RANGE-bucket positions (default SWING_RANGE_GROUP_CAP=3)")
    ap.add_argument("--total-slots", type=int, default=None, help="total concurrent positions (default 5)")
    ap.add_argument("--tu-slot-cap", type=int, default=None,
                    help="cap on non-RANGE-bucket positions (additive-slots mode, e.g. 5 with --total-slots 7)")
    ap.add_argument("--risk", type=float, default=None, help="risk per trade in ₹ (default 7500 = prod)")
    ap.add_argument("--compound-pct", type=float, default=None,
                    help="size risk as this %% of rolling realized equity (e.g. 2.0); default = flat RISK")
    ap.add_argument("--bk-regimes", default=None,
                    help="regime-cell exploration: override BREAKOUT's hard-blocks, e.g. TREND_UP,RANGE")
    ap.add_argument("--slippage", type=float, default=None,
                    help="override per-leg slippage fraction (default 0.0010 = 0.10%%); e.g. 0.003 for liquidity stress")
    ap.add_argument("--liq-cap-pct", type=float, default=None,
                    help="cap position at this %% of trailing 60d daily turnover (prod-replicable liquidity cap), e.g. 1.0")
    ap.add_argument("--mom-b200-floor", type=float, default=None,
                    help="MOMENTUM breadth-EMA200 gate (prod=70). e.g. 80 to tighten")
    ap.add_argument("--mom-tov-excl-regimes", default=None,
                    help="comma-sep cells the turnover dead-zone applies to (prod=TREND_UP); "
                         "e.g. TREND_UP,RANGE — RE-VALIDATION test, see in-code warning")
    args = ap.parse_args()
    if getattr(args, 'slippage', None) is not None:
        globals()['SLIP'] = args.slippage
    if getattr(args, 'mom_b200_floor', None) is not None:
        globals()['MOM_B200_FLOOR'] = args.mom_b200_floor
        print(f"[knob] MOM_B200_FLOOR = {args.mom_b200_floor}")
    if getattr(args, 'mom_tov_excl_regimes', None):
        globals()['MOM_TOV_EXCL_REGIMES'] = tuple(
            x.strip().upper() for x in args.mom_tov_excl_regimes.split(",") if x.strip())
        print(f"[knob] MOM_TOV_EXCL_REGIMES = {globals()['MOM_TOV_EXCL_REGIMES']}")
        print(f"[slippage override] SLIP = {args.slippage:.4f} ({args.slippage*100:.2f}%/leg)")
    if args.risk is not None:
        global RISK
        RISK = float(args.risk)

    print("loading bars + regime + market_inputs ...")
    raw = pickle.load(open(args.pkl, "rb"))
    regime = json.load(open(args.regime))
    market_inputs = json.load(open(MARKET_INPUTS_JSON))
    print(f"  {len(raw)} symbols  |  regime: {len(regime)} days ({os.path.basename(args.regime)})  |  market_inputs: {len(market_inputs)} days")

    print("building indicator series ...")
    symdata = {sym: Sym(bars) for sym, bars in raw.items() if len(bars) >= MIN_BARS_SWING}
    print(f"  {len(symdata)} symbols with ≥{MIN_BARS_SWING} bars\n")

    _setups_arg = tuple(s.strip().upper() for s in args.setups.split(",")) if args.setups else None
    _floor = args.floor
    _pb_month_block = set(int(m.strip()) for m in args.pb_month_block.split(",")) if getattr(args, 'pb_month_block', None) else None
    _pb_b200_floor = getattr(args, 'pb_b200_floor', None)
    _pb_month_block_maxb200 = getattr(args, 'pb_month_block_maxb200', None)
    _setup_daily_cap = None
    if getattr(args, 'setup_daily_cap', None):
        _setup_daily_cap = {}
        for _pair in args.setup_daily_cap.split(","):
            _k, _v = _pair.split(":")
            _setup_daily_cap[_k.strip().upper()] = int(_v.strip())
    _mom_month_block = set(int(m.strip()) for m in args.mom_month_block.split(",")) if getattr(args, 'mom_month_block', None) else None
    _mom_exit_override = None
    if getattr(args, 'mom_activate_r', None) is not None or getattr(args, 'mom_trail_r', None) is not None or getattr(args, 'mom_max_hold', None) is not None:
        _mom_exit_override = {}
        if args.mom_activate_r is not None: _mom_exit_override["activate_R"] = args.mom_activate_r
        if args.mom_trail_r is not None: _mom_exit_override["trail_R"] = args.mom_trail_r
        if args.mom_max_hold is not None: _mom_exit_override["max_hold"] = args.mom_max_hold
    _mom_turnover_exclude = None
    if getattr(args, 'mom_turnover_exclude', None):
        _lo, _hi = args.mom_turnover_exclude.split(",")
        _mom_turnover_exclude = (float(_lo), float(_hi))
    _setup_daily_cap_rank_by = None
    if getattr(args, 'setup_daily_cap_rank_by', None):
        _setup_daily_cap_rank_by = {}
        for _pair in args.setup_daily_cap_rank_by.split(","):
            _k, _v = _pair.split(":")
            _setup_daily_cap_rank_by[_k.strip().upper()] = _v.strip()
    _mom_regimes = set(r.strip().upper() for r in args.mom_regimes.split(",")) if getattr(args, 'mom_regimes', None) else None
    _mom_range_max_off_high = getattr(args, 'mom_range_max_off_high', None)
    _mom_range_max_dsp = getattr(args, 'mom_range_max_dsp', None)
    _pb_regimes = set(r.strip().upper() for r in args.pb_regimes.split(",")) if getattr(args, 'pb_regimes', None) else None
    _pb_rsi_floor = getattr(args, 'pb_rsi_floor', None)
    _pb_exit_override = None
    if getattr(args, 'pb_activate_r', None) is not None or getattr(args, 'pb_max_hold', None) is not None:
        _pb_exit_override = {}
        if args.pb_activate_r is not None: _pb_exit_override["activate_R"] = args.pb_activate_r
        if args.pb_max_hold is not None: _pb_exit_override["max_hold"] = args.pb_max_hold
    _mr_month_block = set(int(m.strip()) for m in args.mr_month_block.split(",")) if getattr(args, 'mr_month_block', None) else None
    _mr_exit_override = None
    if getattr(args, 'mr_activate_r', None) is not None or getattr(args, 'mr_max_hold', None) is not None:
        _mr_exit_override = {}
        if args.mr_activate_r is not None: _mr_exit_override["activate_R"] = args.mr_activate_r
        if args.mr_max_hold is not None: _mr_exit_override["max_hold"] = args.mr_max_hold
    _mr_regimes = set(r.strip().upper() for r in args.mr_regimes.split(",")) if getattr(args, 'mr_regimes', None) else None
    _range_bucket = bool(getattr(args, 'range_bucket_by_regime', False))
    _range_cap = getattr(args, 'range_group_cap', None)
    _total_slots = getattr(args, 'total_slots', None)
    _tu_slot_cap = getattr(args, 'tu_slot_cap', None)
    _compound_pct = getattr(args, 'compound_pct', None)
    _liq_cap_pct = getattr(args, 'liq_cap_pct', None)
    _bk_regimes = set(r.strip().upper() for r in args.bk_regimes.split(",")) if getattr(args, 'bk_regimes', None) else None

    # ── --by-year: year-by-year sweep for a specific setup group ─────────────
    if args.by_year:
        import csv
        label = "+".join(s[:3] for s in _setups_arg) if _setups_arg else "ALL"
        floor_label = int(_floor) if _floor is not None else "default(45)"
        filt_parts = []
        if _pb_month_block: filt_parts.append(f"pb_block={sorted(_pb_month_block)}")
        if _pb_b200_floor is not None: filt_parts.append(f"pb_b200>={int(_pb_b200_floor)}")
        if _pb_month_block_maxb200 is not None: filt_parts.append(f"override_if_b200>={int(_pb_month_block_maxb200)}")
        if _setup_daily_cap: filt_parts.append(f"daily_cap={_setup_daily_cap}")
        filt_label = ("  " + "  ".join(filt_parts)) if filt_parts else ""
        print(f"=== YEAR-BY-YEAR: {label}, floor={floor_label}{filt_label}  {os.path.basename(args.regime)} ===")
        _collector = [] if args.trades_out else None
        for y in range(2015, 2027):
            print(f"\n-- {y} --")
            run(symdata, regime, market_inputs, d0=f"{y}-01-01", d1=f"{y}-12-31",
                setups=_setups_arg, emit_floor=_floor, trades_out=_collector,
                pb_month_block=_pb_month_block, pb_b200_floor=_pb_b200_floor,
                pb_month_block_maxb200=_pb_month_block_maxb200, setup_daily_cap=_setup_daily_cap, mom_month_block=_mom_month_block, mom_exit_override=_mom_exit_override, mom_turnover_exclude=_mom_turnover_exclude, setup_daily_cap_rank_by=_setup_daily_cap_rank_by, mom_regimes=_mom_regimes, mom_range_max_off_high=_mom_range_max_off_high, mom_range_max_dsp=_mom_range_max_dsp, pb_regimes=_pb_regimes, pb_rsi_floor=_pb_rsi_floor, pb_exit_override=_pb_exit_override, mr_month_block=_mr_month_block, mr_exit_override=_mr_exit_override, mr_regimes=_mr_regimes, range_bucket_by_regime=_range_bucket, range_group_cap=_range_cap, total_slots=_total_slots, tu_slot_cap=_tu_slot_cap, compound_pct=_compound_pct, liq_cap_pct=_liq_cap_pct, bk_regimes=_bk_regimes)
        print(f"\n-- 2015-2026 COMBINED --")
        run(symdata, regime, market_inputs, d0="2015-01-01", d1="2026-12-31",
            setups=_setups_arg, emit_floor=_floor,
            pb_month_block=_pb_month_block, pb_b200_floor=_pb_b200_floor,
            pb_month_block_maxb200=_pb_month_block_maxb200, setup_daily_cap=_setup_daily_cap, mom_month_block=_mom_month_block, mom_exit_override=_mom_exit_override, mom_turnover_exclude=_mom_turnover_exclude, setup_daily_cap_rank_by=_setup_daily_cap_rank_by, mom_regimes=_mom_regimes, mom_range_max_off_high=_mom_range_max_off_high, mom_range_max_dsp=_mom_range_max_dsp, pb_regimes=_pb_regimes, pb_rsi_floor=_pb_rsi_floor, pb_exit_override=_pb_exit_override, mr_month_block=_mr_month_block, mr_exit_override=_mr_exit_override, mr_regimes=_mr_regimes, range_bucket_by_regime=_range_bucket, range_group_cap=_range_cap, total_slots=_total_slots, tu_slot_cap=_tu_slot_cap, compound_pct=_compound_pct, liq_cap_pct=_liq_cap_pct, bk_regimes=_bk_regimes)
        if _collector and args.trades_out:
            fields = ["sig_d","entry_d","exit_d","sym","setup","dir","regime","risk_mode",
                      "year","month","hold","adj_score","raw_score","wl_score",
                      "entry","exit","sl_price","sl_pct","atr_pct",
                      "qty","notional","risk","gross","net","R","reason",
                      "breadth","b200","trend_streak","rsi","adx_daily","volume_ratio","strength",
                      "sector","turnover_cr"]
            with open(args.trades_out, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
                w.writeheader(); w.writerows(_collector)
            print(f"\n[trades-out] {len(_collector)} trades → {args.trades_out}")
        return

    # ── --mining: full setup×year matrix ─────────────────────────────────────
    if args.mining:
        floor_val = _floor if _floor is not None else 10.0
        setup_groups = [
            ("PULLBACK",),
            ("MOMENTUM",),
            ("MEAN_REVERSION",),
            ("MOMENTUM", "PULLBACK"),
            ("MOMENTUM", "PULLBACK", "MEAN_REVERSION"),
        ]
        for sg in setup_groups:
            label = "+".join(s[:3] for s in sg)
            fl = floor_val if "MEAN_REVERSION" not in sg else (floor_val if _floor is not None else 45.0)
            print(f"\n{'='*65}")
            print(f"=== SETUP: {label}  floor={int(fl)} ===")
            print(f"{'='*65}")
            for y in range(2015, 2027):
                print(f"\n-- {y} --")
                run(symdata, regime, market_inputs, d0=f"{y}-01-01", d1=f"{y}-12-31",
                    setups=sg, emit_floor=fl)
            print(f"\n-- 2015-2026 COMBINED --")
            run(symdata, regime, market_inputs, d0="2015-01-01", d1="2026-12-31",
                setups=sg, emit_floor=fl)
        return

    # ── single custom date range ──────────────────────────────────────────────
    if args.d0 or args.d1:
        d0 = args.d0 or "2015-01-01"
        d1 = args.d1 or "2026-12-31"
        label = "+".join(s[:3] for s in _setups_arg) if _setups_arg else "ALL"
        print(f"=== {label}, floor={int(_floor) if _floor else 'default'}, {d0}→{d1} ===")
        run(symdata, regime, market_inputs, d0=d0, d1=d1, setups=_setups_arg, emit_floor=_floor,
            pb_month_block=_pb_month_block, pb_b200_floor=_pb_b200_floor,
            pb_month_block_maxb200=_pb_month_block_maxb200, setup_daily_cap=_setup_daily_cap, mom_month_block=_mom_month_block, mom_exit_override=_mom_exit_override, mom_turnover_exclude=_mom_turnover_exclude, setup_daily_cap_rank_by=_setup_daily_cap_rank_by, mom_regimes=_mom_regimes, mom_range_max_off_high=_mom_range_max_off_high, mom_range_max_dsp=_mom_range_max_dsp, pb_regimes=_pb_regimes, pb_rsi_floor=_pb_rsi_floor, pb_exit_override=_pb_exit_override, mr_month_block=_mr_month_block, mr_exit_override=_mr_exit_override, mr_regimes=_mr_regimes, range_bucket_by_regime=_range_bucket, range_group_cap=_range_cap, total_slots=_total_slots, tu_slot_cap=_tu_slot_cap, compound_pct=_compound_pct, liq_cap_pct=_liq_cap_pct, bk_regimes=_bk_regimes)
        return

    # ── default: original baseline runs ──────────────────────────────────────
    print(f"=== SWING FINAL — prod-faithful, {os.path.basename(args.regime)}, ₹5L/₹7500 ===")
    print("\n-- 2022-2026 (current pickle coverage, faithful regime) --")
    run(symdata, regime, market_inputs, d0="2022-01-03", d1="2026-12-31")

    print("\n-- 2021-2026 (full pickle coverage) --")
    run(symdata, regime, market_inputs, d0="2021-01-01", d1="2026-12-31")

    print("\n-- DIAGNOSTIC: 2022-2026 MOMENTUM+PULLBACK only (excl MR) --")
    run(symdata, regime, market_inputs, d0="2022-01-03", d1="2026-12-31",
        setups=("MOMENTUM", "PULLBACK"))

    print("\n-- DIAGNOSTIC: 2022-2026 MR only (RANGE signal quality) --")
    run(symdata, regime, market_inputs, d0="2022-01-03", d1="2026-12-31",
        setups=("MEAN_REVERSION",))

    print("\n=== T4: EMIT_FLOOR sweep (MOM+PB only, 2022-2026) ===")
    for floor in [45, 30, 20, 10, 1]:
        print(f"\n-- emit_floor={floor} --")
        run(symdata, regime, market_inputs, d0="2022-01-03", d1="2026-12-31",
            setups=("MOMENTUM", "PULLBACK"), emit_floor=floor)

    if args.long:
        print("\n=== 2015-2026 Full (T4: floor=10, MOM+PB only) ===")
        run(symdata, regime, market_inputs, d0="2015-01-01", d1="2026-12-31",
            setups=("MOMENTUM", "PULLBACK"), emit_floor=10)


if __name__ == "__main__":
    main()
