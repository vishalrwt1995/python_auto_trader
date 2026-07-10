"""FACTOR PUSH — try to lift the Momentum x Low-Vol channel above ~13% NET without overfitting.
Base = monthly, top20, >=Rs25cr, MOM12-1 + LOWVOL rank-blend, Nifty-200DMA overlay, DAILY-marked.
Each lever tested INDEPENDENTLY vs base; keep only levers that improve BOTH halves; then stack
survivors. Levers: weighting (EW vs inverse-vol), vol-targeting (causal, Barroso crash-fix),
buffering/hysteresis, sector caps, momentum blend(3,6,12). READ-ONLY, local, zero cost."""
from __future__ import annotations
import os, json, pickle, math
from bisect import bisect_right
from statistics import mean, pstdev
from collections import defaultdict

C = os.path.expanduser("~/.autotrader_backtest_cache")
PRICE_MIN = 30.0; START = "2015-01-01"; COST_RT = 0.0058; MIN_TURN = 2.5e8

print("loading bars + sector map ...")
bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
mkt = json.load(open(f"{C}/market_inputs_2015.json"))
md = sorted(d for d in mkt if mkt[d].get("nifty_close"))
nc = {d: float(mkt[d]["nifty_close"]) for d in md}
try:
    _sm = json.load(open(f"{C}/sector_map.json"))
    if _sm and isinstance(next(iter(_sm.values())), list):     # {sector:[syms]} -> invert
        SECTOR = {s: sec for sec, syms in _sm.items() for s in syms}
    else:
        SECTOR = {k: str(v) for k, v in _sm.items()}
except Exception as e:
    SECTOR = {}; print("no sector map:", e)
SYM = {}
for s, b in bars.items():
    if not b or len(b) < 300: continue
    SYM[s] = {"d": [x[0] for x in b], "c": [float(x[4]) for x in b], "v": [float(x[5]) for x in b]}

ser = [nc[d] for d in md]
ma200 = {md[i]: (mean(ser[i-199:i+1]) if i >= 199 else None) for i in range(len(md))}
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
        vol = pstdev(rets)
        row[s] = {"MOM12": c[i-21]/c[i-252]-1.0, "MOM6": c[i-21]/c[i-126]-1.0,
                  "MOM3": c[i-21]/c[i-63]-1.0, "LOWVOL": -vol, "vol": vol, "turn": mt}
    _panel[dt] = row
    return row

def rank_pick(dt, cfg, prev):
    p = panel_at(dt)
    uni = [s for s, r in p.items() if r["turn"] >= MIN_TURN]
    if len(uni) < cfg["topn"]: return [], p
    facs = cfg["facs"]
    ranks = defaultdict(float)
    for f in facs:
        for rk, s in enumerate(sorted(uni, key=lambda s: -p[s][f])): ranks[s] += rk
    order = sorted(uni, key=lambda s: ranks[s])
    N = cfg["topn"]; cap = cfg.get("sector_cap"); buf = cfg.get("buffer", 1.0)
    rankpos = {s: k for k, s in enumerate(order)}
    sel = []; sec_count = defaultdict(int)
    def try_add(s):
        if cap:
            sec = SECTOR.get(s, "?")
            if sec != "?" and sec_count[sec] >= cap: return False
            sec_count[sec] += 1
        sel.append(s); return True
    # 1) keep prior holds still within top N*buffer (hysteresis)
    if buf > 1.0:
        for s in order:
            if len(sel) >= N: break
            if s in prev and rankpos[s] < N*buf: try_add(s)
    # 2) fill remaining from the top
    for s in order:
        if len(sel) >= N: break
        if s in sel: continue
        try_add(s)
    return sel, p

def daily_curve(cfg):
    rebset = set(MONTHLY); holds = {}; pv = 1.0; prev = set(); curve = []
    book_rets = []                        # fully-invested book daily returns (for vol-target)
    dates = [d for d in md if MONTHLY[0] <= d <= MONTHLY[-1]]
    vt = cfg.get("vol_target")            # annualized target or None
    vt_daily = (vt/math.sqrt(252)) if vt else None
    for k, t in enumerate(dates):
        book_ret = 0.0
        if holds and k > 0:
            tp = dates[k-1]; tot = sum(holds.values())
            newtot = 0.0
            for s in list(holds):
                S = SYM[s]; i = idx_le(S, t); ip = idx_le(S, tp)
                if i is not None and ip is not None and i != ip and S["c"][ip] > 0:
                    holds[s] *= S["c"][i]/S["c"][ip]
                newtot += holds[s]
            book_ret = (newtot/tot - 1.0) if tot > 0 else 0.0
        # causal exposure from trailing book vol
        expo = 1.0
        if vt_daily and len(book_rets) >= 20:
            rv = pstdev(book_rets[-20:])
            expo = min(1.0, vt_daily/rv) if rv > 0 else 1.0
        realized = expo*book_ret
        pv *= (1 + realized)
        book_rets.append(book_ret)
        if t in rebset:
            on = (not cfg.get("regime")) or (ma200.get(t) is not None and nc[t] > ma200[t])
            picks, p = rank_pick(t, cfg, prev) if on else ([], panel_at(t))
            picks = set(picks)
            turnover = len(picks ^ prev)/max(1, cfg["topn"])
            pv *= (1 - 0.5*turnover*COST_RT)
            if picks:
                if cfg.get("weight") == "invvol":
                    w = {s: 1.0/max(1e-6, p[s]["vol"]) for s in picks}; sw = sum(w.values())
                    holds = {s: pv*w[s]/sw for s in picks}
                else:
                    holds = {s: pv/len(picks) for s in picks}
            else:
                holds = {}
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

BASE = dict(topn=20, facs=("MOM12", "LOWVOL"), regime=True, weight="ew", buffer=1.0)
def variant(**kw): d = dict(BASE); d.update(kw); return d

def line(lbl, s, b=None):
    tag = ""
    if b: tag = "  BETTER" if (s["calmar"] > b["calmar"] and s["isc"] > 0 and s["oosc"] > 0) else ("  worse" if s["calmar"] < b["calmar"] else "")
    print(f"  {lbl:34} CAGR {s['cagr']:>5.1f}%  maxDD {s['mdd']:>6.1f}%  Calmar {s['calmar']:>5.2f}  "
          f"Sharpe {s['sharpe']:>4.2f}  IS {s['isc']:>+5.1f}% OOS {s['oosc']:>+5.1f}%{tag}")

print("building panel (once) ...")
base = stats(daily_curve(BASE))
print("\n=== BASE (monthly top20 >=25cr MOM12+LOWVOL +200DMA) ==="); line("base", base)

print("\n=== LEVERS (each independent vs base; keep only BOTH-halves improvers) ===")
line("weight: inverse-vol", stats(daily_curve(variant(weight="invvol"))), base)
for vt in [0.12, 0.15, 0.20]:
    line(f"vol-target {int(vt*100)}%", stats(daily_curve(variant(vol_target=vt))), base)
for bf in [1.5, 2.0]:
    line(f"buffer x{bf}", stats(daily_curve(variant(buffer=bf))), base)
for cp in [2, 3, 4]:
    line(f"sector cap {cp}/sec", stats(daily_curve(variant(sector_cap=cp))), base)
line("momentum blend 3+6+12", stats(daily_curve(variant(facs=("MOM3", "MOM6", "MOM12", "LOWVOL")))), base)
line("no regime overlay", stats(daily_curve(variant(regime=False))), base)

print("\n=== STACK candidates (combine the independent winners) ===")
line("invvol + vol-target15", stats(daily_curve(variant(weight="invvol", vol_target=0.15))), base)
line("invvol + voltgt15 + buffer1.5", stats(daily_curve(variant(weight="invvol", vol_target=0.15, buffer=1.5))), base)
line("invvol + voltgt15 + buf1.5 + sec3", stats(daily_curve(variant(weight="invvol", vol_target=0.15, buffer=1.5, sector_cap=3))), base)
line("blend + invvol + voltgt15 + buf1.5", stats(daily_curve(variant(facs=("MOM3","MOM6","MOM12","LOWVOL"), weight="invvol", vol_target=0.15, buffer=1.5))), base)

print(f"\nsector coverage: {sum(1 for s in SYM if s in SECTOR)}/{len(SYM)} symbols mapped")
print("Keep a lever ONLY if it lifts Calmar with BOTH halves positive. Stacking must still hold OOS.")
