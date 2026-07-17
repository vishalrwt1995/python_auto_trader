"""SWING RANGE-momentum diagnostic — is the b200<70 hard-gate well-calibrated, or overly
conservative? MOMENTUM x RANGE affinity = 1.1x (mild BOOST, "leaders can outperform even in a
ranging index") -- but a SEPARATE unconditional gate (`if b200<70: continue`) in swing_final.py
throws away every MOMENTUM candidate below that line regardless of individual signal quality.
This diagnostic reconstructs the REAL prod MOMENTUM pipeline (same domain imports: scoring,
regime_affinity, risk, swing_exit) WITHOUT that b200 gate, tags each qualifying signal with its
actual b200 at signal time, and buckets forward R-multiple by b200 range, IS(<=2022)/OOS(>=2023).
Answers: does edge decay gradually with b200 (a floor could be LOWERED, cheap fix) or does it
cliff at 70 (the current gate is correct, need a genuinely different signal for RANGE instead)?
Does NOT touch swing_final.py. Prod-faithful via import (not duplicated logic) wherever possible.
READ-ONLY, single-process, thread-capped."""
import os
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "4"
import sys, json, pickle, math, statistics
from collections import defaultdict
sys.path.insert(0, "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")

from autotrader.domain.daily_bias import compute_daily_bias
from autotrader.domain.indicators import compute_indicators
from autotrader.domain.models import FiiDiiSnapshot, NiftySnapshot, PcrSnapshot, RegimeSnapshot
from autotrader.domain.regime_affinity import (
    core4_regime, regime_hard_blocks_strategy, regime_strategy_multiplier, swing_setup_allowed_in_regime,
)
from autotrader.domain.risk import calc_swing_position_size
from autotrader.domain.scoring import check_swing_entry, determine_direction, score_signal
from autotrader.domain.swing_exit import DEFAULT_ACTIVATE_R, DEFAULT_MAX_HOLD_DAYS, DEFAULT_TRAIL_R, simulate_exit
from autotrader.backtest.costs import CostConfig, compute_leg_cost
from autotrader.settings import StrategySettings

CACHE = os.path.expanduser("~/.autotrader_backtest_cache")
UPSTOX = CostConfig.upstox()
RISK, CAP, ATR_SL_MULT, EMIT_FLOOR, SLIP = 7500.0, 500_000.0, 2.5, 45.0, 0.0010
SWING_TOPN_TURNOVER, MIN_BARS_SWING, MIN_PRICE_SWING = 1000, 180, 30.0
MAX_ATR_PCT_SWING, MAX_GAP_RISK_SWING = 0.12, 0.06
D0, D1 = "2018-01-01", "2026-06-19"

# ── same small helpers swing_final.py uses (copied locally per isolation rule) ──
def _ema_series(c, period):
    if not c: return []
    a = 2.0/(period+1.0); out=[c[0]]
    for x in c[1:]: out.append(a*x+(1-a)*out[-1])
    return out
def _atr_series(o,h,l,c):
    n=len(c); out=[0.0]*n; trs=[0.0]*n
    for i in range(1,n): trs[i]=max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1]))
    if n<=14: return out
    atr=sum(trs[1:15])/14.0; out[14]=atr
    for i in range(15,n): atr=(atr*13+trs[i])/14.0; out[i]=atr
    return out
def _adx_series(o,h,l,c):
    n=len(c); out=[25.0]*n
    if n<30: return out
    pdm=[0.0]*n; mdm=[0.0]*n; tr=[0.0]*n
    for i in range(1,n):
        up=h[i]-h[i-1]; dn=l[i-1]-l[i]
        pdm[i]=up if (up>dn and up>0) else 0.0
        mdm[i]=dn if (dn>up and dn>0) else 0.0
        tr[i]=max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1]))
    s_tr=sum(tr[1:15]); s_p=sum(pdm[1:15]); s_m=sum(mdm[1:15])
    dx=[]; dx_idx=[]
    for i in range(15,n):
        s_tr=s_tr-s_tr/14+tr[i]; s_p=s_p-s_p/14+pdm[i]; s_m=s_m-s_m/14+mdm[i]
        if s_tr==0: continue
        pdi=100*s_p/s_tr; mdi=100*s_m/s_tr; dsum=pdi+mdi
        dx.append(100*abs(pdi-mdi)/dsum if dsum>0 else 0.0); dx_idx.append(i)
    if len(dx)<14: return out
    adx=sum(dx[:14])/14.0; out[dx_idx[13]]=round(adx,2)
    for j in range(14,len(dx)): adx=(adx*13+dx[j])/14.0; out[dx_idx[j]]=round(adx,2)
    return out
def _rsi_series(c):
    n=len(c); out=[50.0]*n
    if n<15: return out
    ag=al=0.0
    for i in range(1,15):
        d=c[i]-c[i-1]; ag+=d if d>0 else 0.0; al+=-d if d<0 else 0.0
    ag/=14; al/=14; out[14]=100-100/(1+ag/(al or 0.001))
    for i in range(15,n):
        d=c[i]-c[i-1]; ag=(ag*13+(d if d>0 else 0))/14; al=(al*13+(-d if d<0 else 0))/14
        out[i]=100-100/(1+ag/(al or 0.001))
    return out
def _ret(c,j,n): return (c[j]/c[j-n]-1.0) if j>=n and c[j-n]>0 else 0.0

class Sym:
    __slots__=("bars","d","o","h","l","c","v","idx","ema20","ema50","ema200","atr","adx","rsi","turn","turnmed60")
    def __init__(self, bars):
        self.bars=bars; self.d=[b[0] for b in bars]; self.o=[b[1] for b in bars]
        self.h=[b[2] for b in bars]; self.l=[b[3] for b in bars]; self.c=[b[4] for b in bars]; self.v=[b[5] for b in bars]
        self.idx={dt:i for i,dt in enumerate(self.d)}
        self.ema20=_ema_series(self.c,20); self.ema50=_ema_series(self.c,50); self.ema200=_ema_series(self.c,200)
        self.atr=_atr_series(self.o,self.h,self.l,self.c); self.adx=_adx_series(self.o,self.h,self.l,self.c)
        self.rsi=_rsi_series(self.c); self.turn=[self.c[i]*self.v[i] for i in range(len(bars))]
        self.turnmed60=[0.0]*len(bars)
        for i in range(len(bars)):
            w=self.turn[max(0,i-59):i+1]; self.turnmed60[i]=statistics.median(w) if w else 0.0

def _momentum_component(s, j, ret_mean, ret_std):
    c=s.c[j]; ret60=_ret(s.c,j,60) or _ret(s.c,j,20)
    z_u=(ret60-ret_mean)/ret_std if ret_std>1e-9 else 0.0
    rs=max(0.0,min(1.0,(max(-3.0,min(3.0,z_u))+3.0)/6.0))
    ema200=s.ema200[j]; ema50=s.ema50[j]
    mtrend=1.0 if (c>ema200>0 and ema50>ema200) else 0.0
    adx=s.adx[j]; adx_c=max(0.0,min(1.0,(adx-15.0)/25.0))
    vol20=s.v[max(0,j-20):j]; volmed=statistics.median(vol20) if vol20 else 0.0
    vr=(s.v[j]/volmed) if volmed>0 else 1.0; vol_c=min(2.0,vr)/2.0
    ret5=_ret(s.c,j,5); mpers=max(0.0,min(1.0,ret5/0.03)) if ret5>0 else 0.0
    return max(0.0,min(1.0,0.50*rs+0.20*mtrend+0.15*adx_c+0.10*mpers+0.05*vol_c))*100

def build_regime_snapshot(regime_label, mi):
    vix=float(mi.get("vix") or 14.0); nifty_pct=float(mi.get("nifty_pct") or 0.0)
    nifty_close=float(mi.get("nifty_close") or 22000.0); pcr=float(mi.get("pcr") or 1.0)
    oi_change_pcr=float(mi.get("oi_change_pcr") or 1.0); fii=float(mi.get("fii") or 0.0)
    ru=str(regime_label or "").upper()
    bias="BULLISH" if ru in ("TREND_UP","RECOVERY") else "BEARISH" if ru in ("TREND_DOWN","PANIC") else "NEUTRAL"
    return RegimeSnapshot(regime=ru, bias=bias, vix=vix, nifty=NiftySnapshot(change_pct=nifty_pct, ltp=nifty_close),
                          pcr=PcrSnapshot(pcr=pcr, oi_change_pcr=oi_change_pcr), fii=FiiDiiSnapshot(fii=fii),
                          confidence=0.8, data_health=0.9)

def eligible(s, j):
    if j+1<MIN_BARS_SWING: return False
    if s.c[j]<MIN_PRICE_SWING: return False
    atr_pct=s.atr[j]/s.c[j] if s.c[j]>0 else 1.0
    if atr_pct>MAX_ATR_PCT_SWING: return False
    gaps=[abs(s.o[i]/s.c[i-1]-1.0) for i in range(max(1,j-59),j+1) if s.c[i-1]>0]
    if gaps and (sum(gaps)/len(gaps))>MAX_GAP_RISK_SWING: return False
    return True

print("loading data ...", flush=True)
raw = pickle.load(open(f"{CACHE}/swing_adj_bars_2015.pkl", "rb"))
regime = json.load(open(f"{CACHE}/regime_faithful_2015.json"))
market_inputs = json.load(open(f"{CACHE}/market_inputs_2015.json"))
SYM = {s: Sym(b) for s, b in raw.items() if b and len(b) >= MIN_BARS_SWING}
print(f"  {len(SYM)} symbols\n", flush=True)
del raw
cfg = StrategySettings(capital_swing=CAP, swing_risk_per_trade=RISK, swing_atr_sl_mult=ATR_SL_MULT, swing_rr=2.0)

cal = sorted(d for d in regime if D0 <= d <= D1)
signals = []
print("Stage 1: generating MOMENTUM candidates WITHOUT the b200<70 gate ...", flush=True)
for di, d in enumerate(cal):
    if di % 400 == 0: print(f"  {d} ({di}/{len(cal)})", flush=True)
    regime_entry = regime[d]; reg = core4_regime(regime_entry.get("regime", "RANGE"))
    if not swing_setup_allowed_in_regime("MOMENTUM", reg):
        continue   # still respect the real allowlist (TREND_UP, RANGE) -- just not the b200 gate
    mi = market_inputs.get(d, {}); regime_snap = build_regime_snapshot(reg, mi)
    elig = []
    for sym, s in SYM.items():
        j = s.idx.get(d)
        if j is None or not eligible(s, j): continue
        elig.append((sym, s, j, s.turnmed60[j]))
    if not elig: continue
    elig.sort(key=lambda x: -x[3]); elig = elig[:SWING_TOPN_TURNOVER]
    ret60s = [_ret(s.c, j, 60) or _ret(s.c, j, 20) for _, s, j, _ in elig]
    ret_mean = statistics.mean(ret60s) if ret60s else 0.0
    ret_std = statistics.pstdev(ret60s) if len(ret60s) > 1 else 0.0
    b200_above = sum(1 for _, s, j, _ in elig if j >= 200 and s.c[j] > s.ema200[j])
    b200_elig = sum(1 for _, s, j, _ in elig if j >= 200)
    b200 = (b200_above * 100.0 / b200_elig) if b200_elig else 0.0

    for sym, s, j, _ in elig:
        if j + 1 >= len(s.c): continue
        comp = _momentum_component(s, j, ret_mean, ret_std)
        if comp < EMIT_FLOOR: continue
        # NOTE: deliberately NO "if b200<70: continue" here -- this is the whole point of the test
        ret60 = _ret(s.c, j, 60) or _ret(s.c, j, 20); rs_vs_mkt = ret60 - ret_mean
        win = s.bars[max(0, j-299): j+1]
        try:
            ind = compute_indicators(win, cfg); db = compute_daily_bias(win)
        except Exception:
            continue
        if ind is None or db is None: continue
        direction = determine_direction(ind, regime_snap, setup="MOMENTUM", wl_type="swing", daily_bias=db)
        if direction != "BUY": continue   # long-only, matches the live long-side focus
        ok, _ = check_swing_entry("MOMENTUM", direction, ind, db, regime=reg)
        if not ok: continue
        sig = score_signal(sym, direction, ind, regime_snap, cfg, daily_bias=db, setup="MOMENTUM")
        mult = regime_strategy_multiplier(reg, "MOMENTUM", direction)
        adj_score = max(0, min(100, int(round(int(sig.score) * mult))))
        if adj_score < EMIT_FLOOR: continue
        if regime_hard_blocks_strategy(reg, "MOMENTUM"): continue
        ei = j + 1; entry_px = s.o[ei]
        if entry_px <= 0: continue
        pos = calc_swing_position_size(entry_px, ind.atr, direction, cfg)
        if pos.qty < 1 or pos.sl_price <= 0: continue
        sl_dist = abs(entry_px - pos.sl_price)
        if sl_dist <= 0: continue
        off, exit_px, _reason = simulate_exit(s.bars, ei, True, sl_dist, DEFAULT_MAX_HOLD_DAYS,
                                              trail_R=DEFAULT_TRAIL_R, activate_R=DEFAULT_ACTIVATE_R)
        exit_i = min(ei + off, len(s.bars) - 1)
        entry_fill = entry_px * (1 + SLIP); exit_fill = exit_px * (1 - SLIP)
        gross = (exit_fill - entry_fill) * pos.qty
        cost = (compute_leg_cost(side="BUY", qty=pos.qty, price=entry_fill, is_swing=True, cfg=UPSTOX)
                + compute_leg_cost(side="SELL", qty=pos.qty, price=exit_fill, is_swing=True, cfg=UPSTOX))
        net = gross - cost
        signals.append({"sig_d": d, "entry_d": s.d[ei], "exit_d": s.d[exit_i], "sym": sym,
                        "regime": reg, "b200": round(b200, 1), "adj_score": adj_score,
                        "R": net / (sl_dist * pos.qty) if sl_dist * pos.qty > 0 else 0.0, "net": net})

print(f"\ntotal MOMENTUM candidates (no b200 gate, TREND_UP+RANGE): {len(signals):,}\n", flush=True)
pickle.dump(signals, open(f"{CACHE}/swing_range_momentum_signals.pkl", "wb"))

def bucket(lo, hi, lbl):
    a = [s for s in signals if lo <= s["b200"] < hi and s["sig_d"] <= "2022-12-31"]
    b = [s for s in signals if lo <= s["b200"] < hi and s["sig_d"] >= "2023-01-01"]
    if len(a) < 15 and len(b) < 15:
        print(f"  {lbl:12} n=IS{len(a):>4}/OOS{len(b):>4}  (thin)"); return
    def m(t):
        if not t: return "  n/a  "
        avgR = statistics.mean(x["R"] for x in t); wr = 100*sum(1 for x in t if x["net"]>0)/len(t)
        return f"avgR={avgR:+.3f} WR={wr:4.1f}%"
    print(f"  {lbl:12} IS(n={len(a):>4}): {m(a)}   |   OOS(n={len(b):>4}): {m(b)}", flush=True)

print("=== avg R-multiple + win-rate by b200 bucket, IS(<=2022)/OOS(>=2023) ===", flush=True)
for lo, hi, lbl in [(0,40,"<40"), (40,50,"40-50"), (50,60,"50-60"), (60,70,"60-70 <- CURRENTLY BLOCKED"),
                    (70,80,"70-80 (current floor)"), (80,101,">=80")]:
    bucket(lo, hi, lbl)

print("\n=== same buckets, RANGE regime only (isolates the actual b200<70 candidate pool) ===", flush=True)
range_sigs = [s for s in signals if s["regime"] == "RANGE"]
print(f"  RANGE-regime candidates: {len(range_sigs):,}")
def bucket_range(lo, hi, lbl):
    a = [s for s in range_sigs if lo <= s["b200"] < hi and s["sig_d"] <= "2022-12-31"]
    b = [s for s in range_sigs if lo <= s["b200"] < hi and s["sig_d"] >= "2023-01-01"]
    if len(a) < 10 and len(b) < 10:
        print(f"  {lbl:12} n=IS{len(a):>4}/OOS{len(b):>4}  (thin)"); return
    def m(t):
        if not t: return "  n/a  "
        avgR = statistics.mean(x["R"] for x in t); wr = 100*sum(1 for x in t if x["net"]>0)/len(t)
        return f"avgR={avgR:+.3f} WR={wr:4.1f}%"
    print(f"  {lbl:12} IS(n={len(a):>4}): {m(a)}   |   OOS(n={len(b):>4}): {m(b)}", flush=True)
for lo, hi, lbl in [(0,50,"<50"), (50,60,"50-60"), (60,70,"60-70 <- BLOCKED"), (70,101,">=70")]:
    bucket_range(lo, hi, lbl)

print("\nRead: if avgR stays positive/flat below 70 (esp. 60-70) both halves -> the gate is overly", flush=True)
print("conservative, lowering it is a cheap fix. If avgR craters below 70 -> gate is correctly", flush=True)
print("calibrated; a genuinely different RANGE signal (not just a lower floor) is needed.", flush=True)
