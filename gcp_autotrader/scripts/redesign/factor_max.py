"""FACTOR MAX — leave no stone unturned for profit on the Mom x Low-Vol channel.
Pushed base = monthly, top20, buffer1.5, Nifty-100DMA overlay, MOM12-1+LOWVOL, >=25cr, DAILY-marked
(~13.9% / -16% / Calmar 0.86). Test the remaining PROFIT levers, each vs base:
  1) LIQUIDITY FLOOR sweep (5/10/25/50/100cr) — biggest lever, + fillability at Rs1-2L capital
  2) momentum-weighted sizing   3) dual-momentum (drop negative absolute-mom -> cash)
  4) residual momentum (net of market beta)   5) frog-in-the-pan (smoothness) quality tilt
Keep a lever only if Calmar up AND both halves positive. READ-ONLY, local, zero cost."""
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
niftyret = {md[i]: nc[md[i]]/nc[md[i-1]]-1.0 for i in range(1, len(md)) if nc[md[i-1]] > 0}
SYM = {}
for s, b in bars.items():
    if not b or len(b) < 300: continue
    SYM[s] = {"d": [x[0] for x in b], "c": [float(x[4]) for x in b], "v": [float(x[5]) for x in b]}

ser = [nc[d] for d in md]
MA100 = {md[i]: (mean(ser[i-99:i+1]) if i >= 99 else None) for i in range(len(md))}
month_end = [md[i-1] for i in range(1, len(md)) if md[i][:7] != md[i-1][:7]] + [md[-1]]
MONTHLY = [d for d in month_end if d >= START]

def idx_le(S, dt):
    i = bisect_right(S["d"], dt) - 1
    return i if i >= 0 else None

_panel = {}
def panel_at(dt):
    if dt in _panel: return _panel[dt]
    i_md = bisect_right(md, dt) - 1
    nmom12 = (nc[md[i_md-21]]/nc[md[i_md-252]]-1.0) if i_md >= 252 else 0.0
    row = {}
    for s, S in SYM.items():
        i = idx_le(S, dt)
        if i is None or i < 252: continue
        c, v, dd = S["c"], S["v"], S["d"]
        if c[i] < PRICE_MIN or c[i-252] <= 0: continue
        turns = sorted(c[k]*v[k] for k in range(i-60, i)); mt = turns[len(turns)//2]
        rets = [c[k]/c[k-1]-1.0 for k in range(i-126, i) if c[k-1] > 0]
        if len(rets) <= 40: continue
        mom12 = c[i-21]/c[i-252]-1.0
        pos = sum(1 for r in [c[k]/c[k-1]-1.0 for k in range(i-252, i) if c[k-1] > 0] if r > 0)
        tot = sum(1 for k in range(i-252, i) if c[k-1] > 0)
        fip = (pos/tot - (1 - pos/tot)) if tot else 0.0          # %pos - %neg over 252d
        # beta over 126d (aligned with nifty)
        sr = []; nr = []
        for k in range(i-126, i):
            d1 = dd[k]; nrk = niftyret.get(d1)
            if nrk is not None and c[k-1] > 0:
                sr.append(c[k]/c[k-1]-1.0); nr.append(nrk)
        if len(nr) > 60:
            mn = mean(nr); msr = mean(sr)
            var = mean((x-mn)**2 for x in nr)
            beta = (mean((x-mn)*(y-msr) for x, y in zip(nr, sr))/var) if var > 0 else 1.0
        else:
            beta = 1.0
        resmom = mom12 - beta*nmom12
        row[s] = {"MOM12": mom12, "LOWVOL": -pstdev(rets), "turn": mt,
                  "RESMOM": resmom, "FIP": fip, "absmom": mom12}
    _panel[dt] = row
    return row

def rank_pick(dt, cfg, prev):
    p = panel_at(dt)
    uni = [s for s, r in p.items() if r["turn"] >= cfg["min_turn"]]
    if cfg.get("dualmom"):
        uni = [s for s in uni if p[s]["absmom"] > 0]         # drop negative absolute momentum
    if len(uni) < cfg["topn"]: return [], p
    ranks = defaultdict(float)
    for f in cfg["facs"]:
        for rk, s in enumerate(sorted(uni, key=lambda s: -p[s][f])): ranks[s] += rk
    order = sorted(uni, key=lambda s: ranks[s])
    N = cfg["topn"]; buf = cfg.get("buffer", 1.0); rankpos = {s: k for k, s in enumerate(order)}
    sel = []
    if buf > 1.0:
        for s in order:
            if len(sel) >= N: break
            if s in prev and rankpos[s] < N*buf: sel.append(s)
    for s in order:
        if len(sel) >= N: break
        if s not in sel: sel.append(s)
    return sel, {s: rankpos[s] for s in sel}

def daily_curve(cfg):
    rebset = set(MONTHLY); holds = {}; pv = 1.0; prev = set(); curve = []
    dates = [d for d in md if MONTHLY[0] <= d <= MONTHLY[-1]]
    for k, t in enumerate(dates):
        if holds and k > 0:
            tp = dates[k-1]
            for s in list(holds):
                S = SYM[s]; i = idx_le(S, t); ip = idx_le(S, tp)
                if i is not None and ip is not None and i != ip and S["c"][ip] > 0:
                    holds[s] *= S["c"][i]/S["c"][ip]
            pv = sum(holds.values())
        if t in rebset:
            on = MA100.get(t) is not None and nc[t] > MA100[t]
            picks, rp = rank_pick(t, cfg, prev) if on else ([], {})
            picks = list(picks); ps = set(picks)
            turnover = len(ps ^ prev)/max(1, cfg["topn"])
            pv *= (1 - 0.5*turnover*COST_RT)
            if picks:
                if cfg.get("weight") == "mom":                # momentum-weighted: more to top ranks
                    w = {s: (cfg["topn"] - rp[s]) for s in picks}; sw = sum(w.values())
                    holds = {s: pv*w[s]/sw for s in picks}
                else:
                    holds = {s: pv/len(picks) for s in picks}
            else:
                holds = {}
            prev = ps
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

BASE = dict(topn=20, facs=("MOM12", "LOWVOL"), buffer=1.5, min_turn=2.5e8, weight="ew")
def v(**kw): d = dict(BASE); d.update(kw); return d
def line(lbl, s, b, extra=""):
    tag = "  BETTER" if (s["calmar"] > b["calmar"]+0.01 and s["isc"] > 0 and s["oosc"] > 0) else ("  worse" if s["calmar"] < b["calmar"]-0.01 else "  =")
    print(f"  {lbl:32} CAGR {s['cagr']:>5.1f}%  maxDD {s['mdd']:>6.1f}%  Calmar {s['calmar']:>5.2f}  "
          f"Sharpe {s['sharpe']:>4.2f}  IS {s['isc']:>+5.1f}% OOS {s['oosc']:>+5.1f}%{tag}{extra}")

print("building panel (beta+fip, ~1-2min) ...")
b = stats(daily_curve(BASE))
print("\n=== PUSHED BASE (buffer1.5 + 100DMA, top20 >=25cr) ==="); line("base", b, b)

print("\n=== 1) LIQUIDITY FLOOR sweep (profit lever) + fillability at Rs2L (top20 = Rs10k/name) ===")
for mt, lbl in [(5e7, ">=5cr"), (1e8, ">=10cr"), (2.5e8, ">=25cr"), (5e8, ">=50cr"), (1e9, ">=100cr")]:
    fillpct = 10000.0/mt*100        # Rs10k position as % of daily turnover
    line(f"floor {lbl}", stats(daily_curve(v(min_turn=mt))), b, extra=f"   (Rs10k = {fillpct:.3f}% of ADV)")

print("\n=== 2) momentum-weighted sizing ===")
line("mom-weighted", stats(daily_curve(v(weight="mom"))), b)
print("\n=== 3) dual momentum (drop negative absolute-mom) ===")
line("dual-momentum", stats(daily_curve(v(dualmom=True))), b)
print("\n=== 4) residual momentum (net of market beta) ===")
line("resmom + LOWVOL", stats(daily_curve(v(facs=("RESMOM", "LOWVOL")))), b)
line("resmom + MOM12 + LOWVOL", stats(daily_curve(v(facs=("RESMOM", "MOM12", "LOWVOL")))), b)
print("\n=== 5) frog-in-the-pan smoothness tilt ===")
line("mom + lowvol + FIP", stats(daily_curve(v(facs=("MOM12", "LOWVOL", "FIP")))), b)
print("\n=== best-profit stacks ===")
line("floor10cr + dualmom", stats(daily_curve(v(min_turn=1e8, dualmom=True))), b)
line("floor10cr + mom-weight", stats(daily_curve(v(min_turn=1e8, weight="mom"))), b)
line("floor10cr + dualmom + FIP", stats(daily_curve(v(min_turn=1e8, dualmom=True, facs=("MOM12", "LOWVOL", "FIP")))), b)
print("\nProfit lever kept ONLY if Calmar up + both halves + genuinely fillable at target capital.")
