"""FACTOR PUSH round 2 — new base = base + buffer1.5 (the confirmed winner: 13.5% / Calmar 0.60).
Fixes the sector map (ISIN-keyed {sym,sector}) and tests the loose ends: sector caps (now live),
blend+buffer stack, regime-MA window (100/150/200), topN with buffer, best stack.
Discipline: keep a lever only if Calmar up AND both halves positive. READ-ONLY, local, zero cost."""
from __future__ import annotations
import os, json, pickle, math
from bisect import bisect_right
from statistics import mean, pstdev
from collections import defaultdict

C = os.path.expanduser("~/.autotrader_backtest_cache")
PRICE_MIN = 30.0; START = "2015-01-01"; COST_RT = 0.0058; MIN_TURN = 2.5e8

print("loading bars + sector map (fixed) ...")
bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
mkt = json.load(open(f"{C}/market_inputs_2015.json"))
md = sorted(d for d in mkt if mkt[d].get("nifty_close"))
nc = {d: float(mkt[d]["nifty_close"]) for d in md}
_sm = json.load(open(f"{C}/sector_map.json"))
SECTOR = {v["sym"]: v["sector"] for v in _sm.values()
          if isinstance(v, dict) and v.get("sym") and v.get("sector")}
SYM = {}
for s, b in bars.items():
    if not b or len(b) < 300: continue
    SYM[s] = {"d": [x[0] for x in b], "c": [float(x[4]) for x in b], "v": [float(x[5]) for x in b]}
print(f"sector coverage: {sum(1 for s in SYM if s in SECTOR)}/{len(SYM)} symbols mapped")

ser = [nc[d] for d in md]
MA = {w: {md[i]: (mean(ser[i-w+1:i+1]) if i >= w-1 else None) for i in range(len(md))} for w in (100, 150, 200)}
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
        if c[i] < PRICE_MIN or c[i-252] <= 0 or c[i-126] <= 0 or c[i-63] <= 0: continue
        turns = sorted(c[k]*v[k] for k in range(i-60, i)); mt = turns[len(turns)//2]
        rets = [c[k]/c[k-1]-1.0 for k in range(i-126, i) if c[k-1] > 0]
        if len(rets) <= 40: continue
        row[s] = {"MOM12": c[i-21]/c[i-252]-1.0, "MOM6": c[i-21]/c[i-126]-1.0,
                  "MOM3": c[i-21]/c[i-63]-1.0, "LOWVOL": -pstdev(rets), "turn": mt}
    _panel[dt] = row
    return row

def rank_pick(dt, cfg, prev):
    p = panel_at(dt)
    uni = [s for s, r in p.items() if r["turn"] >= MIN_TURN]
    if len(uni) < cfg["topn"]: return []
    ranks = defaultdict(float)
    for f in cfg["facs"]:
        for rk, s in enumerate(sorted(uni, key=lambda s: -p[s][f])): ranks[s] += rk
    order = sorted(uni, key=lambda s: ranks[s])
    N = cfg["topn"]; cap = cfg.get("sector_cap"); buf = cfg.get("buffer", 1.0)
    rankpos = {s: k for k, s in enumerate(order)}
    sel = []; sec_count = defaultdict(int)
    def try_add(s):
        if cap:
            sec = SECTOR.get(s)
            if sec and sec_count[sec] >= cap: return False
            if sec: sec_count[sec] += 1
        sel.append(s); return True
    if buf > 1.0:
        for s in order:
            if len(sel) >= N: break
            if s in prev and rankpos[s] < N*buf: try_add(s)
    for s in order:
        if len(sel) >= N: break
        if s not in sel: try_add(s)
    return sel

def daily_curve(cfg):
    rebset = set(MONTHLY); holds = {}; pv = 1.0; prev = set(); curve = []
    dates = [d for d in md if MONTHLY[0] <= d <= MONTHLY[-1]]
    maw = cfg.get("regime_ma", 200)
    for k, t in enumerate(dates):
        if holds and k > 0:
            tp = dates[k-1]
            for s in list(holds):
                S = SYM[s]; i = idx_le(S, t); ip = idx_le(S, tp)
                if i is not None and ip is not None and i != ip and S["c"][ip] > 0:
                    holds[s] *= S["c"][i]/S["c"][ip]
            pv = sum(holds.values())
        if t in rebset:
            on = (not cfg.get("regime")) or (MA[maw].get(t) is not None and nc[t] > MA[maw][t])
            picks = set(rank_pick(t, cfg, prev)) if on else set()
            turnover = len(picks ^ prev)/max(1, cfg["topn"])
            pv *= (1 - 0.5*turnover*COST_RT)
            holds = {s: pv/len(picks) for s in picks} if picks else {}
            prev = picks
        curve.append((t, pv))
    return curve

def stats(curve):
    ds = [d for d, _ in curve]; pv = [p for _, p in curve]
    dr = [(ds[i], pv[i]/pv[i-1]-1.0) for i in range(1, len(pv)) if pv[i-1] > 0]
    yrs = len(pv)/252; cagr = (pv[-1]/pv[0])**(1/yrs)-1 if pv[0] > 0 else 0
    peak = -1e9; mdd = 0.0
    for p in pv:
        peak = max(peak, p); mdd = min(mdd, p/peak-1)
    rr = [r for _, r in dr]; sd = pstdev(rr) if len(rr) > 2 else 0
    sharpe = mean(rr)/sd*math.sqrt(252) if sd > 0 else 0
    IS = [r for d, r in dr if d <= "2020-12-31"]; OOS = [r for d, r in dr if d >= "2021-01-01"]
    def ann(x): return 100*(math.prod(1+r for r in x)**(252/len(x))-1) if x else 0
    return dict(cagr=100*cagr, mdd=100*mdd, calmar=(cagr/abs(mdd) if mdd < 0 else 0),
                sharpe=sharpe, isc=ann(IS), oosc=ann(OOS))

PUSHED = dict(topn=20, facs=("MOM12", "LOWVOL"), regime=True, regime_ma=200, buffer=1.5)
def v(**kw): d = dict(PUSHED); d.update(kw); return d
def line(lbl, s, b):
    tag = "  BETTER" if (s["calmar"] > b["calmar"]+0.005 and s["isc"] > 0 and s["oosc"] > 0) else ("  worse" if s["calmar"] < b["calmar"]-0.005 else "  =")
    print(f"  {lbl:36} CAGR {s['cagr']:>5.1f}%  maxDD {s['mdd']:>6.1f}%  Calmar {s['calmar']:>5.2f}  "
          f"Sharpe {s['sharpe']:>4.2f}  IS {s['isc']:>+5.1f}% OOS {s['oosc']:>+5.1f}%{tag}")

print("\n=== NEW BASE = base + buffer1.5 ===")
pb = stats(daily_curve(PUSHED)); line("pushed base (buffer1.5)", pb, pb)
print("\n=== sector caps (now live) on pushed base ===")
for cp in [2, 3, 4]: line(f"+ sector cap {cp}/sec", stats(daily_curve(v(sector_cap=cp))), pb)
print("\n=== momentum blend + buffer (return x efficiency) ===")
line("+ blend 3+6+12", stats(daily_curve(v(facs=("MOM3", "MOM6", "MOM12", "LOWVOL")))), pb)
line("+ blend + sector cap 3", stats(daily_curve(v(facs=("MOM3", "MOM6", "MOM12", "LOWVOL"), sector_cap=3))), pb)
print("\n=== regime-MA window ===")
for w in [100, 150, 200]: line(f"regime {w}DMA", stats(daily_curve(v(regime_ma=w))), pb)
print("\n=== topN (with buffer1.5) ===")
for tn in [15, 20, 25, 30]: line(f"top{tn}", stats(daily_curve(v(topn=tn))), pb)
print("\n=== regime-MA robustness (is 100DMA a plateau or a lucky peak?) ===")
for w in [75, 100, 125]:
    MA.setdefault(w, {md[i]: (mean(ser[i-w+1:i+1]) if i >= w-1 else None) for i in range(len(md))})
    line(f"regime {w}DMA (buf1.5)", stats(daily_curve(v(regime_ma=w))), pb)

print("\n=== CLEAN STACK of the winners: buffer1.5 + sector-cap-2 + fast regime ===")
line("buf1.5 + sec2 + 100DMA", stats(daily_curve(v(sector_cap=2, regime_ma=100))), pb)
line("buf1.5 + sec2 + 125DMA", stats(daily_curve(v(sector_cap=2, regime_ma=125))), pb)
line("buf1.5 + sec3 + 100DMA", stats(daily_curve(v(sector_cap=3, regime_ma=100))), pb)
line("buf1.5 + sec2 + 100DMA + blend", stats(daily_curve(v(sector_cap=2, regime_ma=100, facs=("MOM3","MOM6","MOM12","LOWVOL")))), pb)
print("\nKeep a lever ONLY if Calmar up AND both halves positive. Report the honest best config.")
