"""SWING true b200-breadth history — replicates swing_final.py's exact eligible-universe +
breadth computation (top-1000-turnover, price>=30, ATR%<=12%, gap-risk<=6%) to build the REAL
per-day b200 (%% of eligible universe above EMA200) series 2015-2026. Corrects the coarse
"regime label" view: MOMENTUM is allowed in {TREND_UP, RANGE} (swing_setup_allowed_in_regime),
but is ADDITIONALLY hard-gated by an unconditional b200<70 check regardless of regime. This
answers: how much of the calendar does b200 actually clear 70%, and how has that changed
recently vs 2015-2026 average? Read-only, does not touch swing_final.py. Same source
(swing_adj_bars_2015.pkl) + same eligible() logic, duplicated locally (isolation rule)."""
import os
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "4"
import sys, pickle, statistics
from collections import defaultdict

C = os.path.expanduser("~/.autotrader_backtest_cache")
SWING_TOPN_TURNOVER, MIN_BARS_SWING, MIN_PRICE_SWING = 1000, 180, 30.0
MAX_ATR_PCT_SWING, MAX_GAP_RISK_SWING = 0.12, 0.06

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

print("loading swing_adj_bars_2015.pkl ...", flush=True)
raw = pickle.load(open(f"{C}/swing_adj_bars_2015.pkl", "rb"))
SYM = {}
for s, bars in raw.items():
    if not bars or len(bars) < MIN_BARS_SWING: continue
    d=[b[0] for b in bars]; o=[b[1] for b in bars]; h=[b[2] for b in bars]
    l=[b[3] for b in bars]; c=[b[4] for b in bars]; v=[b[5] for b in bars]
    SYM[s] = {"d": d, "o": o, "h": h, "l": l, "c": c, "v": v,
              "ema200": _ema_series(c, 200), "atr": _atr_series(o,h,l,c),
              "idx": {dt: i for i, dt in enumerate(d)}}
print(f"  {len(SYM)} symbols\n", flush=True)
del raw

all_dates = sorted({d for S in SYM.values() for d in S["d"]})
print(f"trading days: {len(all_dates)} ({all_dates[0]} -> {all_dates[-1]})\n", flush=True)

def eligible(S, j):
    if j+1 < MIN_BARS_SWING: return False
    if S["c"][j] < MIN_PRICE_SWING: return False
    atr_pct = S["atr"][j]/S["c"][j] if S["c"][j] > 0 else 1.0
    if atr_pct > MAX_ATR_PCT_SWING: return False
    gaps = [abs(S["o"][i]/S["c"][i-1]-1.0) for i in range(max(1,j-59), j+1) if S["c"][i-1] > 0]
    if gaps and (sum(gaps)/len(gaps)) > MAX_GAP_RISK_SWING: return False
    return True

b200_by_date = {}
for d in all_dates:
    elig = []
    for s, S in SYM.items():
        j = S["idx"].get(d)
        if j is None or not eligible(S, j): continue
        turn = S["c"][j]*S["v"][j]
        elig.append((s, S, j, turn))
    if not elig: continue
    elig.sort(key=lambda x: -x[3]); elig = elig[:SWING_TOPN_TURNOVER]
    above = sum(1 for _, S, j, _ in elig if j >= 200 and S["c"][j] > S["ema200"][j])
    tot = sum(1 for _, S, j, _ in elig if j >= 200)
    b200_by_date[d] = (above*100.0/tot) if tot else 0.0

print("=== b200 distribution, full history 2015-2026 ===", flush=True)
vals = list(b200_by_date.values())
print(f"  n={len(vals)}  mean={statistics.mean(vals):.1f}  median={statistics.median(vals):.1f}")
print(f"  >=70: {100*sum(1 for v in vals if v>=70)/len(vals):.1f}% of days")
print(f"  >=60: {100*sum(1 for v in vals if v>=60)/len(vals):.1f}% of days")
print(f"  <60:  {100*sum(1 for v in vals if v<60)/len(vals):.1f}% of days\n")

print("=== by year: %% of days b200>=70 (MOMENTUM's real gate, any regime) ===", flush=True)
by_yr = defaultdict(list)
for d, v in b200_by_date.items(): by_yr[d[:4]].append(v)
for y in sorted(by_yr):
    vv = by_yr[y]
    print(f"  {y}: mean_b200={statistics.mean(vv):>5.1f}  days>=70={100*sum(1 for x in vv if x>=70)/len(vv):>5.1f}%  n={len(vv)}")

print("\n=== recent 60 trading days (is the current 57-66 stretch typical or an outlier?) ===", flush=True)
recent = sorted(b200_by_date.keys())[-60:]
rv = [b200_by_date[d] for d in recent]
print(f"  last 60d: mean={statistics.mean(rv):.1f}  min={min(rv):.1f}  max={max(rv):.1f}  days>=70: {sum(1 for x in rv if x>=70)}/60")
streak = 0; maxstreak = 0
for d in sorted(b200_by_date.keys()):
    if b200_by_date[d] < 70: streak += 1; maxstreak = max(maxstreak, streak)
    else: streak = 0
print(f"  longest-ever b200<70 streak (any period, 2015-2026): {maxstreak} trading days (~{maxstreak/21:.1f} months)")
# current streak (trailing from the most recent day)
cur = 0
for d in sorted(b200_by_date.keys(), reverse=True):
    if b200_by_date[d] < 70: cur += 1
    else: break
print(f"  CURRENT trailing b200<70 streak (as of {sorted(b200_by_date.keys())[-1]}): {cur} trading days (~{cur/21:.1f} months)")

pickle.dump(b200_by_date, open(f"{C}/swing_b200_history.pkl", "wb"))
print(f"\nsaved -> {C}/swing_b200_history.pkl (for reuse by the RANGE-signal grind)", flush=True)
