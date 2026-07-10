"""WALK-FORWARD validation of the Mom x Low-Vol channel — the pre-build gate.
For each OOS test block, RE-PICK the params (topN, buffer, regime-MA, liquidity floor) using
ONLY data before that block, then test forward. If walk-forward OOS ~ the fixed MAX config,
the params are robust (not cumulative-tuning fiction). Also rolling sub-period stability.
READ-ONLY, local, single-process, zero cost."""
from __future__ import annotations
import os, json, pickle, math
from bisect import bisect_right
from statistics import mean, pstdev
from collections import defaultdict

C = os.path.expanduser("~/.autotrader_backtest_cache")
PRICE_MIN = 30.0; START = "2015-01-01"; COST_RT = 0.0058

print("loading bars ...")
bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
mkt = json.load(open(f"{C}/market_inputs_2015.json"))
md = sorted(d for d in mkt if mkt[d].get("nifty_close"))
nc = {d: float(mkt[d]["nifty_close"]) for d in md}
SYM = {}
for s, b in bars.items():
    if not b or len(b) < 300: continue
    SYM[s] = {"d": [x[0] for x in b], "c": [float(x[4]) for x in b], "v": [float(x[5]) for x in b]}
ser = [nc[d] for d in md]
MAW = {w: {md[i]: (mean(ser[i-w+1:i+1]) if i >= w-1 else None) for i in range(len(md))} for w in (100, 150, 200)}
month_end = [md[i-1] for i in range(1, len(md)) if md[i][:7] != md[i-1][:7]] + [md[-1]]
MONTHLY = [d for d in month_end if d >= START]

def idx_le(S, dt):
    i = bisect_right(S["d"], dt) - 1
    return i if i >= 0 else None

_panel = {}
def panel_at(dt):
    if dt in _panel: return _panel[dt]
    row = {}
    for s, S in SYM.items():
        i = idx_le(S, dt)
        if i is None or i < 252: continue
        c, v = S["c"], S["v"]
        if c[i] < PRICE_MIN or c[i-252] <= 0: continue
        turns = sorted(c[k]*v[k] for k in range(i-60, i)); mt = turns[len(turns)//2]
        rets = [c[k]/c[k-1]-1.0 for k in range(i-126, i) if c[k-1] > 0]
        if len(rets) <= 40: continue
        row[s] = {"MOM12": c[i-21]/c[i-252]-1.0, "LOWVOL": -pstdev(rets), "turn": mt}
    _panel[dt] = row
    return row

def rank_pick(dt, cfg, prev):
    p = panel_at(dt)
    uni = [s for s, r in p.items() if r["turn"] >= cfg["min_turn"]]
    if len(uni) < cfg["topn"]: return []
    ranks = defaultdict(float)
    for f in ("MOM12", "LOWVOL"):
        for rk, s in enumerate(sorted(uni, key=lambda s: -p[s][f])): ranks[s] += rk
    order = sorted(uni, key=lambda s: ranks[s]); N = cfg["topn"]; buf = cfg["buffer"]
    rankpos = {s: k for k, s in enumerate(order)}; sel = []
    if buf > 1.0:
        for s in order:
            if len(sel) >= N: break
            if s in prev and rankpos[s] < N*buf: sel.append(s)
    for s in order:
        if len(sel) >= N: break
        if s not in sel: sel.append(s)
    return sel

def curve(cfg, lo, hi):
    reb = [d for d in MONTHLY if lo <= d <= hi]
    if len(reb) < 4: return []
    holds = {}; pv = 1.0; prev = set(); out = []
    dates = [d for d in md if reb[0] <= d <= reb[-1]]; rebset = set(reb)
    for k, t in enumerate(dates):
        if holds and k > 0:
            tp = dates[k-1]
            for s in list(holds):
                S = SYM[s]; i = idx_le(S, t); ip = idx_le(S, tp)
                if i is not None and ip is not None and i != ip and S["c"][ip] > 0:
                    holds[s] *= S["c"][i]/S["c"][ip]
            pv = sum(holds.values())
        if t in rebset:
            on = MAW[cfg["regime_ma"]].get(t) is not None and nc[t] > MAW[cfg["regime_ma"]][t]
            picks = set(rank_pick(t, cfg, prev)) if on else set()
            pv *= (1 - 0.5*len(picks ^ prev)/max(1, cfg["topn"])*COST_RT)
            holds = {s: pv/len(picks) for s in picks} if picks else {}
            prev = picks
        out.append((t, pv))
    return out

def stats(cv):
    if not cv: return None
    pv = [p for _, p in cv]; ds = [d for d, _ in cv]
    dr = [pv[i]/pv[i-1]-1.0 for i in range(1, len(pv)) if pv[i-1] > 0]
    yrs = len(pv)/252; cagr = (pv[-1]/pv[0])**(1/yrs)-1 if pv[0] > 0 else 0
    peak = -1e9; mdd = 0.0
    for p in pv:
        peak = max(peak, p); mdd = min(mdd, p/peak-1)
    sd = pstdev(dr) if len(dr) > 2 else 0
    return dict(cagr=100*cagr, mdd=100*mdd, calmar=(cagr/abs(mdd) if mdd < 0 else 0),
                sharpe=(mean(dr)/sd*math.sqrt(252) if sd > 0 else 0))

GRID = [dict(topn=tn, buffer=bf, regime_ma=ma, min_turn=mt)
        for tn in (20, 30) for bf in (1.0, 1.5) for ma in (100, 150, 200) for mt in (1e8, 2.5e8)]
MAXCFG = dict(topn=20, buffer=1.5, regime_ma=100, min_turn=1e8)
FOLDS = [("2020-01-01", "2021-12-31"), ("2022-01-01", "2023-12-31"), ("2024-01-01", "2026-12-31")]

print("building panel + walk-forward (re-pick params on past data only) ...\n")
print(f"{'test block':22} {'picked cfg (train-optimal)':34} {'WF-picked OOS':22} {'fixed-MAX OOS':22}")
wf_c = []; fx_c = []
for lo, hi in FOLDS:
    train_hi = md[bisect_right(md, lo)-2]                       # day before test block
    best = None
    for cfg in GRID:
        s = stats(curve(cfg, START, train_hi))
        if s and (best is None or s["calmar"] > best[1]["calmar"]):
            best = (cfg, s)
    bcfg = best[0]
    wf = stats(curve(bcfg, lo, hi)); fx = stats(curve(MAXCFG, lo, hi))
    wf_c.append(wf); fx_c.append(fx)
    tag = f"top{bcfg['topn']} buf{bcfg['buffer']} {bcfg['regime_ma']}DMA {int(bcfg['min_turn']/1e7)}cr"
    print(f"{lo[:7]}..{hi[:7]:12} {tag:34} "
          f"CAGR{wf['cagr']:>5.1f}% DD{wf['mdd']:>6.1f}% Cal{wf['calmar']:>4.2f}   "
          f"CAGR{fx['cagr']:>5.1f}% DD{fx['mdd']:>6.1f}% Cal{fx['calmar']:>4.2f}")

def agg(cs):
    eq = 1.0; yrs = 0
    for lo, hi in [(f[0], f[1]) for f in FOLDS]: pass
    cagrs = [c["cagr"] for c in cs]; dds = [c["mdd"] for c in cs]
    return mean(cagrs), min(dds), mean([c["calmar"] for c in cs]), mean([c["sharpe"] for c in cs])

wc, wd, wcal, wsh = agg(wf_c); fc, fd, fcal, fsh = agg(fx_c)
print(f"\nWALK-FORWARD (params chosen only from past) : avg CAGR {wc:.1f}%  worst-block DD {wd:.1f}%  avg Calmar {wcal:.2f}  avg Sharpe {wsh:.2f}")
print(f"FIXED MAX config on same OOS blocks        : avg CAGR {fc:.1f}%  worst-block DD {fd:.1f}%  avg Calmar {fcal:.2f}  avg Sharpe {fsh:.2f}")

print("\n=== rolling 3yr windows, FIXED MAX config (consistency) ===")
for y in range(2015, 2024):
    s = stats(curve(MAXCFG, f"{y}-01-01", f"{y+2}-12-31"))
    if s: print(f"  {y}-{y+2}: CAGR {s['cagr']:>5.1f}%  DD {s['mdd']:>6.1f}%  Calmar {s['calmar']:>4.2f}  Sharpe {s['sharpe']:>4.2f}")

print("\nVERDICT: robust if walk-forward OOS ~ fixed-MAX OOS AND rolling windows are consistently positive.")
