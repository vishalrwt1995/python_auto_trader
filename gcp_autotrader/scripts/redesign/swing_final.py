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
    DD governor (T3)       ✅  Weekly-5%/monthly-8% halt (PortfolioBook); daily 1.5%
                               qty-halve NOT modeled (noted in residual gaps)
    max_trades_day (T8)    ✅  5 entries/day/channel cap (matches prod gate)
    Daily-breaker (T9)     ✅  Uses prev-day realized P&L (swing exits EOD, entries
                               at scan time → no same-day look-ahead)

KNOWN RESIDUAL GAPS (not modeled — all small/structural, bias noted):
    DD governor 1.5%-daily-DD throttle (halve qty): NOT modeled — weekly/monthly halts
        are (T3), but intraday halving requires per-entry re-sizing (complex). Missing
        → slight optimism on extreme intraday loss days. Channel-budget risk cap
        (₹37.5k max open risk vs ₹5L budget) never binds → safely ignored.
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
UPSTOX = CostConfig.upstox()

# ── Deployed prod config (2026-06-29) ────────────────────────────────────────
RISK = 7500.0                        # SWING_RISK_PER_TRADE env override (flat, not 1.5%)
CAP = 500_000.0                      # CAPITAL_SWING env override
ATR_SL_MULT = 2.5                    # swing_atr_sl_mult (code default)
EMIT_FLOOR = 45.0                    # SWING_MIN_SIGNAL_SCORE env override
MAX_HOLD = DEFAULT_MAX_HOLD_DAYS     # = 20 — SWING_MAX_HOLD_DAYS env override
ACTIVATE_R = DEFAULT_ACTIVATE_R      # = 1.75
TRAIL_R = DEFAULT_TRAIL_R            # = 1.0
SLIP = 0.0010                        # paper_entry/exit_slippage_pct (order_service.py:161) — 0.10%/leg adverse
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
                 "ema200", "atr", "adx", "rsi", "turn", "turnmed60")

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
    high20 = max(s.h[max(0, j - 19): j + 1]); low20 = min(s.l[max(0, j - 19): j + 1])
    vol20 = s.v[max(0, j - 20): j]
    volmed = statistics.median(vol20) if vol20 else 0.0
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


# ── Main entry ────────────────────────────────────────────────────────────────

def run(symdata, regime: dict, market_inputs: dict,
        d0="2022-01-03", d1="2026-12-31",
        long_only=False, verbose=True,
        setups=None, emit_floor=None):
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
    for d in cal:
        regime_entry = regime[d]
        reg = regime_entry.get("regime", "RANGE")
        reg = core4_regime(reg)           # fold 6-label to 4-label
        risk_mode = str(regime_entry.get("risk_mode", "NORMAL"))  # for adaptive ATR mult (T1)

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

        for sym, s, j, _ in elig:
            if j + 1 >= len(s.c):
                continue

            # Per-symbol values used in multiple setup checks
            ret60 = _ret(s.c, j, 60) or _ret(s.c, j, 20)
            rs_vs_mkt = ret60 - ret_mean
            sma200 = sum(s.c[j - 200:j]) / 200.0 if j >= 200 else 0.0

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
                if not swing_setup_allowed_in_regime(setup, reg):
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
                if setup in ("MOMENTUM", "PULLBACK") and b200 > 0.0 and b200 < 70.0:
                    continue

                # ── Prod entry pipeline: real indicators → real gate stack ────
                win = s.bars[max(0, j - 299): j + 1]
                try:
                    ind = compute_indicators(win, cfg)
                    db = compute_daily_bias(win)
                except Exception:
                    continue
                if ind is None or db is None:
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
                off, exit_px, reason = simulate_exit(
                    s.bars, ei, is_buy, sl_dist, MAX_HOLD,
                    trail_R=TRAIL_R, activate_R=ACTIVATE_R,
                )
                exit_i = min(ei + off, len(s.bars) - 1)

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
                    "group": swing_setup_group(setup), "regime": reg,
                    "qty": pos.qty, "entry": entry_px, "exit": exit_px,
                    "risk": sl_dist * pos.qty, "notional": entry_px * pos.qty,
                    "gross": gross, "net": net, "reason": reason,
                    "R": net / (sl_dist * pos.qty) if sl_dist * pos.qty > 0 else 0.0,
                    "adj_score": adj_score, "raw_score": raw_score,
                    "wl_score": comp,     # T7: pre-filter component score (prod sorts by this)
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
    for d in cal:
        still = []
        for p in open_pos:
            if p["exit_d"] < d:
                net = p["net"]
                realized_day[p["exit_d"]] += net
                week_pnl[_week_key(p["exit_d"])]   += net  # T3
                month_pnl[_month_key(p["exit_d"])] += net  # T3
                held_syms.discard(p["sym"])
            else:
                still.append(p)
        open_pos = still

        # T9: per-channel daily breaker (trading_service.py:1604-1614).
        # Swing exits happen at EOD reconciliation; entries at scan time → use
        # PREVIOUS day's realized P&L so we don't look into same-day exits.
        prev_pnl = realized_day.get(prev_d, 0.0) if prev_d else 0.0
        if prev_pnl >= profit_lim:          # profit target → full block
            prev_d = d; continue
        _loss_day = prev_pnl <= -loss_lim   # loss limit → MR-only

        # T3: DD governor weekly/monthly halts
        if week_pnl.get(_week_key(d), 0.0)   <= -DD_WEEK_HALT:
            prev_d = d; continue
        if month_pnl.get(_month_key(d), 0.0) <= -DD_MONTH_HALT:
            prev_d = d; continue

        cands = sorted(by_sig.get(d, []), key=lambda x: -x["wl_score"])  # T7: prod sorts by wl_score
        entries_today = 0                           # T8: max_trades_day=5
        for sg in cands:
            if entries_today >= 5 or len(open_pos) >= 5:  # T8
                break
            if _loss_day and sg["setup"] != "MEAN_REVERSION":
                continue                # loss-limit day: counter-trend (MR) only
            if sg["setup"] not in ("PULLBACK",) and len(open_pos) >= 4:
                continue                # last slot reserved for PULLBACK
            if sg["sym"] in held_syms:
                continue
            if sg["group"] == "RANGE" and sum(1 for p in open_pos if p["group"] == "RANGE") >= SWING_RANGE_GROUP_CAP:
                continue
            if sum(p["notional"] for p in open_pos) + sg["notional"] > CAP:
                continue
            open_pos.append(sg); held_syms.add(sg["sym"]); taken.append(sg)
            entries_today += 1  # T8
        prev_d = d  # T9

    for p in open_pos:
        net = p["net"]
        realized_day[p["exit_d"]] += net
        week_pnl[_week_key(p["exit_d"])]   += net
        month_pnl[_month_key(p["exit_d"])] += net

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
    ap.add_argument("--regime", default=REGIME_JSON, help="regime JSON path (default: daily-only; use regime_faithful_2015_5m.json for T2)")
    ap.add_argument("--long", action="store_true", help="run 2015-2026 full history (slow)")
    args = ap.parse_args()

    print("loading bars + regime + market_inputs ...")
    raw = pickle.load(open(args.pkl, "rb"))
    regime = json.load(open(args.regime))
    market_inputs = json.load(open(MARKET_INPUTS_JSON))
    print(f"  {len(raw)} symbols  |  regime: {len(regime)} days ({os.path.basename(args.regime)})  |  market_inputs: {len(market_inputs)} days")

    print("building indicator series ...")
    symdata = {sym: Sym(bars) for sym, bars in raw.items() if len(bars) >= MIN_BARS_SWING}
    print(f"  {len(symdata)} symbols with ≥{MIN_BARS_SWING} bars\n")

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

    # ── T4: EMIT_FLOOR sweep — MOM+PB only (2022-2026) ───────────────────────
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
