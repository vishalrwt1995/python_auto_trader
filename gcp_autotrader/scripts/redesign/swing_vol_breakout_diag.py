"""SWING volatility-contraction-breakout diagnostic — the 5th and final RANGE-regime idea,
mechanistically distinct from all 4 that failed (MR=buy-weakness; off-high-filter and
lower-b200=whole-market RS; sector-relative-RS=leadership vs peers -- INVERTED). This one
compares a stock ONLY to its OWN volatility history: a coiling range (ATR well below its own
60d average) that then expands via a fresh 20d-high breakout with volume confirmation. No
market or sector comparison at all -- tests a completely different mechanism (vol-clustering +
breakout) restricted to the exact dark pool (RANGE regime + b200<70) currently invisible to
swing. Reuses domain/swing_exit (real exit) + backtest/costs (real cost model); does NOT run
the full compute_indicators/score_signal stack (this is a fast diagnostic pass, matching the
delivery/deals/shorts pattern: raw signal + real exit fidelity first, full wiring only if this
survives). READ-ONLY, single-process, thread-capped."""
import os
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "4"
import sys, json, pickle, statistics
from collections import defaultdict
sys.path.insert(0, "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.domain.swing_exit import DEFAULT_ACTIVATE_R, DEFAULT_MAX_HOLD_DAYS, DEFAULT_TRAIL_R, simulate_exit
from autotrader.backtest.costs import CostConfig, compute_leg_cost

CACHE = os.path.expanduser("~/.autotrader_backtest_cache")
UPSTOX = CostConfig.upstox()
RISK, CAP, ATR_SL_MULT, SLIP, SLOTS = 7500.0, 500_000.0, 2.5, 0.0010, 5
MIN_BARS_SWING, MIN_PRICE_SWING, MAX_ATR_PCT_SWING, MAX_GAP_RISK_SWING, TOPN = 180, 30.0, 0.12, 0.06, 1000

print("loading bars + regime + true b200 history ...", flush=True)
raw = pickle.load(open(f"{CACHE}/swing_adj_bars_2015.pkl", "rb"))
regime = json.load(open(f"{CACHE}/regime_faithful_2015.json"))
b200_hist = pickle.load(open(f"{CACHE}/swing_b200_history.pkl", "rb"))
sys.path.insert(0, "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.domain.regime_affinity import core4_regime

def _atr_series(o,h,l,c):
    n=len(c); out=[0.0]*n; trs=[0.0]*n
    for i in range(1,n): trs[i]=max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1]))
    if n<=14: return out
    atr=sum(trs[1:15])/14.0; out[14]=atr
    for i in range(15,n): atr=(atr*13+trs[i])/14.0; out[i]=atr
    return out

SYM = {}
for s, b in raw.items():
    if not b or len(b) < MIN_BARS_SWING: continue
    d=[x[0] for x in b]; o=[x[1] for x in b]; h=[x[2] for x in b]; l=[x[3] for x in b]; c=[x[4] for x in b]; v=[x[5] for x in b]
    SYM[s] = {"bars": b, "d": d, "o": o, "h": h, "l": l, "c": c, "v": v, "atr": _atr_series(o,h,l,c), "idx": {dt: i for i, dt in enumerate(d)}}
print(f"  {len(SYM)} symbols", flush=True)
del raw

def eligible(S, j):
    if j+1 < MIN_BARS_SWING: return False
    if S["c"][j] < MIN_PRICE_SWING: return False
    atr_pct = S["atr"][j]/S["c"][j] if S["c"][j] > 0 else 1.0
    if atr_pct > MAX_ATR_PCT_SWING: return False
    gaps = [abs(S["o"][i]/S["c"][i-1]-1.0) for i in range(max(1,j-59), j+1) if S["c"][i-1] > 0]
    if gaps and (sum(gaps)/len(gaps)) > MAX_GAP_RISK_SWING: return False
    return True

# dark-pool dates: RANGE regime AND b200<70 (from the true history, not the coarse label)
dark_dates = sorted(d for d in b200_hist if b200_hist[d] < 70.0
                    and core4_regime(regime.get(d, {}).get("regime", "RANGE")) == "RANGE"
                    and "2018-01-01" <= d <= "2026-06-19")
print(f"  dark-pool dates (RANGE & b200<70): {len(dark_dates):,}\n", flush=True)

def _lookback_has_gap(dates, lo, hi):
    """A data hole in the LOOKBACK window (used to compute the squeeze/breakout features)
    would make a trading-halt look like a genuine squeeze (near-zero ATR = no real trades)."""
    from datetime import date as _date
    for k in range(max(0, lo), hi):
        d0 = _date.fromisoformat(dates[k]); d1 = _date.fromisoformat(dates[k+1])
        if (d1 - d0).days > _MAX_INTRABAR_GAP_DAYS: return True
    return False
_MAX_INTRABAR_GAP_DAYS = 6

print("scanning for squeeze+breakout candidates ...", flush=True)
cands = []   # (d, sym, j, atr_ratio, vol_ratio, is_breakout)
gap_skipped = 0
for di, d in enumerate(dark_dates):
    if di % 300 == 0: print(f"  {d} ({di}/{len(dark_dates)})", flush=True)
    for sym, S in SYM.items():
        j = S["idx"].get(d)
        if j is None or j < 80 or j + 1 >= len(S["c"]) or not eligible(S, j):
            continue
        if _lookback_has_gap(S["d"], j-60, j):
            gap_skipped += 1
            continue   # squeeze/breakout features would be a trading-halt artifact
        atr = S["atr"][j]
        if not atr or atr <= 0: continue
        atr_hist = [x for x in S["atr"][max(14, j-60):j] if x and x > 0]
        if len(atr_hist) < 30: continue
        atr_ratio = atr / statistics.mean(atr_hist)
        prior_hi = max(S["h"][j-20:j])
        is_breakout = S["c"][j] >= prior_hi
        vol_avg = statistics.mean(S["v"][j-20:j]) if any(S["v"][j-20:j]) else 0.0
        vol_ratio = (S["v"][j] / vol_avg) if vol_avg > 0 else 0.0
        cands.append({"d": d, "sym": sym, "j": j, "atr_ratio": atr_ratio, "vol_ratio": vol_ratio, "is_breakout": is_breakout})
print(f"\ntotal eligible dark-pool observations: {len(cands):,}\n", flush=True)

_MAX_INTRABAR_GAP_DAYS = 6   # same guard swing_final.py uses: >6 calendar days between
                             # consecutive bars = a real data hole (suspension/illiquidity),
                             # not a weekend/holiday -- produces phantom entry/exit R multiples.
def _spans_data_gap(dates, lo, hi):
    from datetime import date as _date
    for k in range(lo, hi):
        d0 = _date.fromisoformat(dates[k]); d1 = _date.fromisoformat(dates[k+1])
        if (d1 - d0).days > _MAX_INTRABAR_GAP_DAYS: return True
    return False

def sim_trade(sym, j):
    S = SYM[sym]; ei = j + 1; entry_px = S["o"][ei]
    if entry_px <= 0: return None
    sl_dist = ATR_SL_MULT * S["atr"][j]
    if sl_dist <= 0: return None
    qty = int(RISK // sl_dist)
    if qty < 1: return None
    if qty * entry_px > CAP / SLOTS: qty = int((CAP / SLOTS) // entry_px)
    if qty < 1: return None
    off, exit_px, _ = simulate_exit(S["bars"], ei, True, sl_dist, DEFAULT_MAX_HOLD_DAYS,
                                    trail_R=DEFAULT_TRAIL_R, activate_R=DEFAULT_ACTIVATE_R)
    exit_i = min(ei + off, len(S["bars"]) - 1)
    if _spans_data_gap(S["d"], ei, exit_i):
        return None   # data-gap artifact -- phantom R, not a real trade (matches swing_final.py)
    ef = entry_px * (1 + SLIP); xf = exit_px * (1 - SLIP)
    gross = (xf - ef) * qty
    cost = (compute_leg_cost(side="BUY", qty=qty, price=ef, is_swing=True, cfg=UPSTOX)
            + compute_leg_cost(side="SELL", qty=qty, price=xf, is_swing=True, cfg=UPSTOX))
    net = gross - cost
    return {"ed": S["d"][ei], "xd": S["d"][exit_i], "R": net / (sl_dist * qty), "net": net}

def bucket(pool, lbl):
    a = [sim_trade(c["sym"], c["j"]) for c in pool if c["d"] <= "2022-12-31"]
    b = [sim_trade(c["sym"], c["j"]) for c in pool if c["d"] >= "2023-01-01"]
    a = [x for x in a if x]; b = [x for x in b if x]
    if len(a) < 15 and len(b) < 15:
        print(f"  {lbl:34} n=IS{len(a):>4}/OOS{len(b):>4}  (thin)"); return
    def m(t):
        if not t: return "  n/a  "
        avgR = statistics.mean(x["R"] for x in t); wr = 100*sum(1 for x in t if x["net"]>0)/len(t)
        return f"avgR={avgR:+.3f} WR={wr:4.1f}%"
    print(f"  {lbl:34} IS(n={len(a):>4}): {m(a)}   |   OOS(n={len(b):>4}): {m(b)}", flush=True)

print("=== component isolation (which ingredient carries information, if any) ===", flush=True)
bucket([c for c in cands if c["is_breakout"]], "breakout only (any vol)")
bucket([c for c in cands if c["atr_ratio"] <= 0.75], "squeeze only (any breakout)")
bucket([c for c in cands if c["is_breakout"] and c["atr_ratio"] <= 0.75], "squeeze + breakout")
bucket([c for c in cands if c["is_breakout"] and c["atr_ratio"] <= 0.75 and c["vol_ratio"] >= 1.3],
      "squeeze + breakout + volume>=1.3x")
bucket([c for c in cands if c["is_breakout"] and c["atr_ratio"] <= 0.60], "tight squeeze(<=0.6) + breakout")

print("\n=== squeeze-ratio buckets (any breakout) -- is there a monotonic relationship? ===", flush=True)
for lo, hi, lbl in [(0,0.5,"<0.5 (very tight)"), (0.5,0.75,"0.5-0.75"), (0.75,1.0,"0.75-1.0"), (1.0,10,">=1.0 (expanding already)")]:
    bucket([c for c in cands if c["is_breakout"] and lo <= c["atr_ratio"] < hi], f"squeeze {lbl}")

print("\nRead: robust positive avgR both halves on squeeze+breakout = a genuine 5th-idea edge.", flush=True)
print("Flat/negative like the other 4 -> RANGE regime has no capturable edge with signals tried.", flush=True)
