"""Prod-faithful swing backtest on survivorship-free, split/bonus-adjusted data.

DRIVES THE ACTUAL PRODUCTION DOMAIN CODE for every load-bearing decision:
  - compute_indicators + compute_daily_bias (domain) on DAILY bars  [prod: swing
    ind & bias are both computed on daily candles — trading_service.py:1101/1106/1114]
  - check_swing_entry (domain.scoring)            structural gates  [byte-exact]
  - core4_regime / swing_setup_allowed_in_regime / swing_setup_group (regime_affinity)
  - calc_swing_position_size (domain.risk)         sizing            [byte-exact]
  - simulate_exit (domain.swing_exit)              exit              [arm 1.75 / trail 1.0 / 20d]
  - compute_leg_cost (backtest.costs, Upstox)      costs             [byte-exact]

REPLICATED (pure math, mirrors universe_service formulas) — documented approximations:
  - universe eligibility (BALANCED) + the 4 component scores (emit floor 45, wl_score rank)
  - rs_vs_mkt = ret60 - eligible-universe-mean(ret60); SMA-50 breadth
  - rs_component sector z-score FALLS BACK to universe z (no historical sector map for
    delisted names) -> affects momentum/breakout RANKING only, never qualification
  - ATR/ADX component-score series are Wilder-seeded-from-start (converged ~ prod's
    trailing-window calc for the >=180-bar eligible names); EMA(_ema_last)/RSI are exact
  - the intraday scan-time score_signal>=45 threshold has no daily analog -> we apply the
    daily-computable component emit-floor (45) + the full structural gate stack instead

Regime  = market_brain_history folded to core-4 (94.3% match vs the validated artifact),
          one as-of/day. Data = bt_bhavcopy_adj -> swing_adj_bars.pkl. Single-process.
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
from autotrader.domain.indicators import compute_indicators, calc_atr, calc_adx, calc_rsi
from autotrader.domain.scoring import check_swing_entry
from autotrader.domain.regime_affinity import (
    core4_regime, swing_setup_allowed_in_regime, swing_setup_group, SWING_RANGE_GROUP_CAP,
)
from autotrader.domain.risk import calc_swing_position_size
from autotrader.domain.swing_exit import simulate_exit, DEFAULT_ACTIVATE_R, DEFAULT_TRAIL_R, DEFAULT_MAX_HOLD_DAYS
from autotrader.backtest.costs import CostConfig, compute_leg_cost
from autotrader.settings import StrategySettings

CACHE = os.path.expanduser("~/.autotrader_backtest_cache")
BARS_PKL = os.path.join(CACHE, "swing_adj_bars.pkl")
REGIME_JSON = os.path.join(CACHE, "regime_core4.json")
UPSTOX = CostConfig.upstox()

# ── BALANCED universe gates (universe_service.py / universe_v2.py) ───────────
SWING_TOPN_TURNOVER = 1000
MIN_BARS_SWING = 180
MIN_PRICE_SWING = 30.0
MAX_ATR_PCT_SWING = 0.12
MAX_GAP_RISK_SWING = 0.06
EMIT_FLOOR = 45.0                 # SWING_MIN_SIGNAL_SCORE (live env)
MULTI_EMIT = ("MOMENTUM", "PULLBACK", "MEAN_REVERSION")


def _clip01(v: float) -> float:
    return 0.0 if not math.isfinite(v) else max(0.0, min(1.0, v))


def _norm(v: float, lo: float, hi: float) -> float:
    return 0.0 if hi <= lo or not math.isfinite(v) else _clip01((v - lo) / (hi - lo))


def _ema_series(c: list[float], period: int) -> list[float]:
    """Match universe_service._ema_last per index (first-value seed)."""
    if not c:
        return []
    a = 2.0 / (period + 1.0)
    out = [c[0]]
    for x in c[1:]:
        out.append(a * x + (1 - a) * out[-1])
    return out


def _atr_series(o, h, l, c) -> list[float]:
    """Wilder ATR(14) aligned per index; converged ~ calc_atr(daily[-260:],14)."""
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
    """Wilder ADX(14) aligned per index (matches calc_adx algorithm, full seed)."""
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
    # Wilder smoothed sums seeded over first 14 (indices 1..14)
    s_tr = sum(tr[1:15]); s_p = sum(pdm[1:15]); s_m = sum(mdm[1:15])
    dx = []           # dx[k] corresponds to candle index 14+len so far
    dx_idx = []
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
    """Wilder RSI(14) aligned per index (matches calc_rsi values)."""
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
        # rolling 60d median turnover
        self.turnmed60 = [0.0] * len(bars)
        for i in range(len(bars)):
            w = self.turn[max(0, i - 59): i + 1]
            self.turnmed60[i] = statistics.median(w) if w else 0.0


def _ret(c, j, n):
    return (c[j] / c[j - n] - 1.0) if j >= n and c[j - n] > 0 else 0.0


def component_scores(s: Sym, j, ret_mean, ret_std):
    """The 4 universe_service blends at index j (rs sector-tilt -> universe fallback)."""
    c = s.c[j]
    ret60 = _ret(s.c, j, 60) or _ret(s.c, j, 20)
    z_u = (ret60 - ret_mean) / ret_std if ret_std > 1e-9 else 0.0
    rs = _norm(max(-3.0, min(3.0, z_u)), -3.0, 3.0)              # sector->universe fallback
    high20 = max(s.h[max(0, j - 19): j + 1]); low20 = min(s.l[max(0, j - 19): j + 1])
    vol20 = s.v[max(0, j - 20): j]
    volmed = statistics.median(vol20) if vol20 else 0.0
    vr = (s.v[j] / volmed) if volmed > 0 else 1.0
    ema20, ema50, ema200 = s.ema20[j], s.ema50[j], s.ema200[j]
    ema50p = s.ema50[j - 20] if j >= 20 else ema50
    atr = s.atr[j]; atr_pct = atr / c if c > 0 else 0.0
    adx = s.adx[j]; rsi = s.rsi[j]
    # breakout (disabled downstream, kept for completeness)
    breakout_c = max(0.0, 1.0 - (((high20 - c) / high20) if high20 > 0 else 0.0) * 5.0)
    vol_c = min(2.0, vr) / 2.0
    trend_c = 1.0 if (c > ema50 > ema200) else 0.0
    adx_c = _norm(adx, 15.0, 40.0)
    breakout = _clip01(0.30 * rs + 0.25 * breakout_c + 0.15 * vol_c + 0.15 * trend_c + 0.15 * adx_c) * 100
    # pullback
    if ema50 > ema200 and atr > 0:
        pb_depth = (ema20 - c) / atr
        pb_c = _clip01(pb_depth / 2.0)
        slope = (ema50 - ema50p) / 20.0
        ts = _clip01(slope / (max(1e-6, atr_pct) * c))
    else:
        pb_c = 0.0; ts = 0.0
    vl3 = statistics.mean(s.v[max(0, j - 2): j + 1]) if j >= 2 else s.v[j]
    vp3 = statistics.mean(s.v[max(0, j - 5): j - 2]) if j >= 5 else vl3
    if vp3 > 0 and vl3 < vp3 * 0.85:
        vcon = 1.0
    elif vr < 1.0:
        vcon = 0.5
    else:
        vcon = 0.0
    pullback = _clip01(0.40 * ts + 0.40 * pb_c + 0.20 * vcon) * 100
    # mean reversion
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
    # momentum
    mtrend = 1.0 if (c > ema200 > 0 and ema50 > ema200) else 0.0
    mpers = _clip01(ret5 / 0.03) if ret5 > 0 else 0.0
    momentum = _clip01(0.50 * rs + 0.20 * mtrend + 0.15 * adx_c + 0.10 * mpers + 0.05 * vol_c) * 100
    return {"MOMENTUM": momentum, "PULLBACK": pullback, "MEAN_REVERSION": mean_rev, "BREAKOUT": breakout}


def near_high_tilt(s: Sym, j):
    n = 253
    if j + 1 < n:
        return 0.0
    wh = max(s.h[j - 252: j + 1])
    return max(0.0, s.c[j] / wh - 0.85) * 100.0 if wh > 0 else 0.0


def mr_above_200(s: Sym, j):
    if j < 201:
        return False
    sma = sum(s.c[j - 200:j]) / 200.0
    return sma > 0 and s.c[j] > sma


def mr_direction(db, regime):
    """Fix1 (2026-06): RSI ≤ 35 → BUY; SELL disabled universally.
    Matches scoring.py ~54 (live prod). MOMENTUM/PULLBACK long-only → BUY."""
    return "BUY" if db.rsi_daily <= 35.0 else "HOLD"


def eligible(s: Sym, j):
    if j + 1 < MIN_BARS_SWING:                       # bars (incl. current)
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


def run(symdata, regime, cap, activate_r, trail_r=DEFAULT_TRAIL_R, max_hold=DEFAULT_MAX_HOLD_DAYS,
        d0="2022-01-03", d1="2026-12-31", long_only=False, verbose=True):
    risk = 1500.0 * cap / 1e5                          # prod: 1.5% of capital (₹7,500 @ ₹5L)
    cfg = StrategySettings(capital_swing=cap, swing_risk_per_trade=risk,
                           swing_atr_sl_mult=2.5, swing_rr=2.0)
    loss_lim = cap * 0.03; profit_lim = cap * 0.06
    cal = sorted(d for d in regime if d0 <= d <= d1)

    # ---- Stage 1: generate all qualified swing signals (portfolio-independent) ----
    signals = []                                       # each: dict
    for d in cal:
        reg = regime[d]                                # already core-4 folded
        active = [st for st in MULTI_EMIT if swing_setup_allowed_in_regime(st, reg)]
        if not active:
            continue                                   # PANIC/TREND_DOWN/CHOP/etc.
        # eligible universe today + cross-sectional stats
        elig = []
        for sym, s in symdata.items():
            j = s.idx.get(d)
            if j is None or not eligible(s, j):
                continue
            elig.append((sym, s, j, s.turnmed60[j]))
        if not elig:
            continue
        elig.sort(key=lambda x: -x[3])                 # turnover rank desc
        elig = elig[:SWING_TOPN_TURNOVER]              # top-1000
        ret60s = [_ret(s.c, j, 60) or _ret(s.c, j, 20) for _, s, j, _ in elig]
        ret_mean = statistics.mean(ret60s) if ret60s else 0.0
        ret_std = statistics.pstdev(ret60s) if len(ret60s) > 1 else 0.0
        above = sum(1 for _, s, j, _ in elig if j >= 49 and s.c[j] > sum(s.c[j - 49:j + 1]) / 50.0)
        breadth = 100.0 * above / len(elig) if elig else 0.0
        b200_above = sum(1 for _, s, j, _ in elig if j >= 200 and s.c[j] > s.ema200[j])
        b200_eligible = sum(1 for _, s, j, _ in elig if j >= 200)
        b200 = (b200_above * 100.0 / b200_eligible) if b200_eligible else 0.0
        for sym, s, j, _ in elig:
            if j + 1 >= len(s.c):
                continue                               # need next-day open to enter
            sc = component_scores(s, j, ret_mean, ret_std)
            ret60 = _ret(s.c, j, 60) or _ret(s.c, j, 20)
            rs_vs_mkt = ret60 - ret_mean
            for setup in active:
                comp = sc[setup]
                if comp < EMIT_FLOOR:
                    continue
                if setup == "MEAN_REVERSION" and not mr_above_200(s, j):
                    continue                           # #3 gate (emission requires mrAbove200Sma)
                if setup in ("MEAN_REVERSION", "PULLBACK") and rs_vs_mkt <= 0.0:
                    continue                           # rs_vs_mkt > 0
                if setup == "PULLBACK" and breadth < 60.0:
                    continue                           # breadth >= 60
                if setup in ("MOMENTUM", "PULLBACK") and b200 > 0.0 and b200 < 70.0:
                    continue                           # b200 gate (2026-06): ≥70% above EMA200
                # ---- PROD structural gate (exact): compute_indicators + daily_bias ----
                win = s.bars[max(0, j - 299): j + 1]
                ind = compute_indicators(win, cfg)
                if ind is None:
                    continue
                db = compute_daily_bias(win)
                if db is None:
                    continue
                # prod direction: MOMENTUM/PULLBACK long-only (scoring._long_only);
                # MEAN_REVERSION via the prod RSI rule -> can flip SELL (overnight short)
                direction = mr_direction(db, reg) if setup == "MEAN_REVERSION" else "BUY"
                if direction == "HOLD":
                    continue
                if long_only and direction == "SELL":
                    continue
                ok, _ = check_swing_entry(setup, direction, ind, db, regime=reg)
                if not ok:
                    continue
                is_buy = direction == "BUY"
                # ---- size (prod risk.py) using daily ind.atr at entry = next open ----
                ei = j + 1
                entry_px = s.o[ei]
                if entry_px <= 0:
                    continue
                pos = calc_swing_position_size(entry_px, ind.atr, direction, cfg)
                if pos.qty < 1 or pos.sl_price <= 0:
                    continue
                sl_dist = abs(entry_px - pos.sl_price)
                if sl_dist <= 0:
                    continue
                off, exit_px, reason = simulate_exit(s.bars, ei, is_buy, sl_dist,
                                                     max_hold, trail_R=trail_r, activate_R=activate_r)
                exit_i = min(ei + off, len(s.bars) - 1)
                gross = ((exit_px - entry_px) if is_buy else (entry_px - exit_px)) * pos.qty
                cost = (compute_leg_cost(side=("BUY" if is_buy else "SELL"), qty=pos.qty, price=entry_px, is_swing=True, cfg=UPSTOX)
                        + compute_leg_cost(side=("SELL" if is_buy else "BUY"), qty=pos.qty, price=exit_px, is_swing=True, cfg=UPSTOX))
                net = gross - cost
                wl = comp + (near_high_tilt(s, j) if setup == "MOMENTUM" else 0.0)
                signals.append({
                    "sig_d": d, "entry_d": s.d[ei], "exit_d": s.d[exit_i], "sym": sym,
                    "setup": setup, "dir": direction, "group": swing_setup_group(setup), "regime": reg,
                    "qty": pos.qty, "entry": entry_px, "exit": exit_px, "risk": sl_dist * pos.qty,
                    "notional": entry_px * pos.qty, "gross": gross, "net": net, "reason": reason,
                    "R": net / (sl_dist * pos.qty) if sl_dist * pos.qty > 0 else 0.0, "wl": wl,
                })

    # ---- Stage 2: portfolio walk (5 slots, reserve-2-trend, daily breaker) ----
    by_sig = collections.defaultdict(list)
    for sg in signals:
        by_sig[sg["sig_d"]].append(sg)
    open_pos = []                                       # list of dicts with exit_d
    realized_day = collections.defaultdict(float)
    taken = []
    held_syms = set()
    for d in cal:
        # close positions exiting strictly before today (free slots), accrue to exit day
        still = []
        for p in open_pos:
            if p["exit_d"] < d:
                realized_day[p["exit_d"]] += p["net"]
                held_syms.discard(p["sym"])
            else:
                still.append(p)
        open_pos = still
        # breaker on today's realized so far
        if realized_day[d] <= -loss_lim or realized_day[d] >= profit_lim:
            continue
        cands = sorted(by_sig.get(d, []), key=lambda x: -x["wl"])
        for sg in cands:
            if len(open_pos) >= 5:
                break
            if sg["setup"] not in ("PULLBACK",) and len(open_pos) >= 4:
                continue                              # pb_slot: last slot reserved for PULLBACK
            if sg["sym"] in held_syms:
                continue
            if sg["group"] == "RANGE" and sum(1 for p in open_pos if p["group"] == "RANGE") >= SWING_RANGE_GROUP_CAP:
                continue
            if sum(p["notional"] for p in open_pos) + sg["notional"] > cap:
                continue
            open_pos.append(sg); held_syms.add(sg["sym"]); taken.append(sg)
    for p in open_pos:                                  # close survivors at the end
        realized_day[p["exit_d"]] += p["net"]

    return _report(taken, realized_day, cal, cap, activate_r, verbose)


def _report(taken, realized_day, cal, cap, arm, verbose):
    if not taken:
        if verbose:
            print("  (no trades)")
        return {"net": 0.0, "n": 0}
    net = sum(t["net"] for t in taken)
    gross = sum(t["gross"] for t in taken)
    wins = sum(1 for t in taken if t["net"] > 0)
    # equity curve / maxDD from daily realized
    eq = cap; peak = cap; mdd = 0.0
    for d in cal:
        eq += realized_day.get(d, 0.0)
        peak = max(peak, eq); mdd = min(mdd, eq / peak - 1.0)
    years = len(cal) / 252.0
    cagr = ((eq / cap) ** (1 / years) - 1) * 100 if years > 0 and eq > 0 else 0.0
    calmar = cagr / abs(mdd * 100) if mdd < 0 else float("inf")
    by_year = collections.defaultdict(float); by_cell = collections.defaultdict(lambda: [0, 0.0])
    by_reg = collections.defaultdict(lambda: [0, 0.0]); by_dir = collections.defaultdict(lambda: [0, 0.0])
    by_celldir = collections.defaultdict(lambda: [0, 0.0])
    for t in taken:
        by_year[t["exit_d"][:4]] += t["net"]
        by_cell[t["setup"]][0] += 1; by_cell[t["setup"]][1] += t["net"]
        by_reg[t["regime"]][0] += 1; by_reg[t["regime"]][1] += t["net"]
        dd = t.get("dir", "BUY")
        by_dir[dd][0] += 1; by_dir[dd][1] += t["net"]
        by_celldir[(t["setup"], dd)][0] += 1; by_celldir[(t["setup"], dd)][1] += t["net"]
    if verbose:
        print(f"  arm={arm}  cap=₹{cap/1e5:.0f}L  trades={len(taken)}  WR={100*wins/len(taken):.1f}%  "
              f"GROSS=₹{gross:,.0f}  NET=₹{net:,.0f}  ({100*net/cap/(len(cal)/252):.1f}%/yr)  "
              f"maxDD={mdd*100:.0f}%  CAGR={cagr:.1f}%  Calmar={calmar:.2f}")
        print("    per-year NET:", {y: f"₹{v:,.0f}" for y, v in sorted(by_year.items())})
        print("    per-cell    :", {k: f"n={v[0]} ₹{v[1]:,.0f}" for k, v in by_cell.items()})
        print("    by-direction:", {k: f"n={v[0]} ₹{v[1]:,.0f}" for k, v in by_dir.items()})
        print("    by cell+dir :", {f"{k[0]}/{k[1]}": f"n={v[0]} ₹{v[1]:,.0f}" for k, v in by_celldir.items()})
    return {"net": net, "n": len(taken), "wr": 100 * wins / len(taken), "mdd": mdd * 100,
            "cagr": cagr, "calmar": calmar, "by_year": dict(by_year),
            "by_dir": {k: v[1] for k, v in by_dir.items()}, "n_sell": by_dir.get("SELL", [0])[0]}


def main():
    import pickle
    print("loading bars + regime ...")
    raw = pickle.load(open(BARS_PKL, "rb"))
    regime = json.load(open(REGIME_JSON))
    print(f"  {len(raw)} symbols; building indicator series ...")
    symdata = {sym: Sym(bars) for sym, bars in raw.items() if len(bars) >= MIN_BARS_SWING}
    print(f"  {len(symdata)} symbols with >= {MIN_BARS_SWING} bars\n")
    print("=== PROD-FAITHFUL SWING — 2022-2026, survivorship-free adjusted ===")
    print("\n-- prod-live config: ₹5L, arm 1.75 (DEFAULT_ACTIVATE_R) --")
    run(symdata, regime, 500_000, DEFAULT_ACTIVATE_R)
    print("\n-- arm sensitivity @ ₹5L --")
    run(symdata, regime, 500_000, 1.0)
    print("\n-- ₹1L (compare vs validated +39,310@1.0 / ₹32,406@1.75) --")
    run(symdata, regime, 100_000, DEFAULT_ACTIVATE_R)
    run(symdata, regime, 100_000, 1.0)


if __name__ == "__main__":
    main()
