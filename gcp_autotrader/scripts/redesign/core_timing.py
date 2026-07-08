"""CORE timing grind — can a LOW-TOGGLE overlay cut drawdown net of cost?

The raw daily-PANIC overlay was cost-blind (104 toggles). Test two low-toggle signals on the
compound engine (EW-daily ~= fully deployed) for the two best selections (blend top30, pure_mom
top20), NET of realistic toggle cost (0.3%/switch):
  none         = always invested (baseline)
  raw_panic    = cash on any PANIC/TREND_DOWN day (the cost-blind one, for reference)
  debounce3    = cash only after 3 consecutive risk-off days; re-enter after 3 risk-on
  nifty_sma200 = cash when Nifty50 < its 200-day SMA (classic trend filter)
IS 2016-21 / OOS 2022-26, vs Nifty50. Isolated, read-only, cached (free).
"""
from __future__ import annotations
import os, sys, pickle, json
from bisect import bisect_left, bisect_right
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
from autotrader.domain.core_signals import (
    momentum_score, realized_vol, passes_universe_gates, MOM_LOOKBACK, UNIVERSE_TOP,
)
C = os.path.expanduser("~/.autotrader_backtest_cache")
raw = pickle.load(open(f"{C}/swing_adj_bars_2015.pkl", "rb"))
mkt = json.load(open(f"{C}/market_inputs_2015.json"))
rg = json.load(open(f"{C}/regime_faithful_2015.json"))
def reg_of(d):
    v = rg.get(d); return (v.get("regime") if isinstance(v, dict) else v) if v else None
RISK_OFF = {"PANIC", "TREND_DOWN"}

sym = {}
for s, bars in raw.items():
    if not bars or len(bars) < MOM_LOOKBACK + 5: continue
    dates = [b[0] for b in bars]; closes = [float(b[4]) for b in bars]; vols = [float(b[5]) for b in bars]
    rets = [0.0] + [(closes[i]/closes[i-1]-1.0) if closes[i-1] > 0 else 0.0 for i in range(1, len(closes))]
    tov = []; _rs = 0.0; _w = []
    for i in range(len(closes)):
        x = closes[i]*vols[i]; _w.append(x); _rs += x
        if len(_w) > 20: _rs -= _w.pop(0)
        tov.append(_rs/len(_w))
    sym[s] = {"dates": dates, "closes": closes, "rets": rets, "tov": tov, "rbd": {dates[i]: rets[i] for i in range(len(dates))}}
all_dates = sorted({d for v in sym.values() for d in v["dates"]})
gidx = {d: i for i, d in enumerate(all_dates)}
def first_ge(iso):
    k = bisect_left(all_dates, iso); return all_dates[k] if k < len(all_dates) else None
rebal_dates = [d for d in sorted({first_ge(f"{y}-{m:02d}-01") for y in range(2016,2027) for m in (1,4,7,10)} - {None}) if d]

def i_le(s, d):
    j = bisect_right(sym[s]["dates"], d) - 1; return j if j >= 0 else None
def cand_at(d):
    out = []
    for s, v in sym.items():
        i = i_le(s, d)
        if i is None or i - MOM_LOOKBACK < 0: continue
        if not passes_universe_gates(v["closes"][i], v["tov"][i], has_history=True): continue
        mom = momentum_score(v["closes"], i); vol = realized_vol(v["rets"], i)
        if mom is None or vol is None: continue
        out.append({"symbol": s, "momentum": mom, "vol": vol, "turnover": v["tov"][i]})
    return out
def select(cands, topn, mode):
    large = sorted([c for c in cands if c["vol"] > 0], key=lambda c: -c["turnover"])[:UNIVERSE_TOP]
    if len(large) < topn: return []
    if mode == "pure_mom": rank = sorted(large, key=lambda c: -c["momentum"])
    else:
        mr = {c["symbol"]: r for r, c in enumerate(sorted(large, key=lambda c: -c["momentum"]))}
        vr = {c["symbol"]: r for r, c in enumerate(sorted(large, key=lambda c: c["vol"]))}
        rank = sorted(large, key=lambda c: mr[c["symbol"]] + vr[c["symbol"]])
    return [c["symbol"] for c in rank[:topn]]
cc = {d: cand_at(d) for d in rebal_dates}
bask = {("blend", 30): {d: select(cc[d], 30, "blend") for d in rebal_dates},
        ("pure_mom", 20): {d: select(cc[d], 20, "pure_mom") for d in rebal_dates}}

# ---- timing signals (day -> risk_off bool) ----
none_sig = {d: False for d in all_dates}
raw_sig = {d: (reg_of(d) in RISK_OFF) for d in all_dates}
def debounce(N):
    sig = {}; off = False; ro_run = 0; on_run = 0
    for d in all_dates:
        if reg_of(d) in RISK_OFF: ro_run += 1; on_run = 0
        else: on_run += 1; ro_run = 0
        if not off and ro_run >= N: off = True
        elif off and on_run >= N: off = False
        sig[d] = off
    return sig
deb_sig = debounce(3)
# Nifty 200-SMA
nser = []; last = None
for d in all_dates:
    if d in mkt and mkt[d].get("nifty_close"): last = mkt[d]["nifty_close"]
    nser.append(last)
sma_sig = {}; _q = []; _s = 0.0
for i, d in enumerate(all_dates):
    if nser[i] is not None:
        _q.append(nser[i]); _s += nser[i]
        if len(_q) > 200: _s -= _q.pop(0)
        sma = _s/len(_q); sma_sig[d] = (len(_q) >= 200 and nser[i] < sma)
    else:
        sma_sig[d] = False
print(f"{len(sym)} syms | risk-off days: raw={sum(raw_sig.values())} debounce3={sum(deb_sig.values())} sma200={sum(sma_sig.values())}")

def sim(basket_by_date, topn, roff, toggle=0.003):
    rebs = [d for d in rebal_dates if basket_by_date[d]]
    eq = [(rebs[0], 1.0)]; val = 1.0; prev = set(); invested = True; toggles = 0
    for k, d in enumerate(rebs):
        basket = set(basket_by_date[d])
        if invested:
            val *= (1 - (len(basket ^ prev)/(2*topn) if prev else 1.0)*0.006)
        prev = basket
        gi = gidx[d]; gj = gidx[rebs[k+1]] if k+1 < len(rebs) else len(all_dates)-1
        for g in range(gi, gj):
            day = all_dates[g]; ro = roff.get(day, False)
            if ro and invested: val *= (1-toggle); invested = False; toggles += 1
            elif (not ro) and not invested: val *= (1-toggle); invested = True; toggles += 1
            if invested and not ro:
                rs = [sym[s]["rbd"][day] for s in basket if day in sym[s]["rbd"]]
                if rs: val *= (1 + sum(rs)/len(rs))
            eq.append((day, val))
    return eq, toggles
def met(eq, lo=None, hi=None):
    pts = [(d, v) for d, v in eq if (lo is None or d >= lo) and (hi is None or d <= hi)]
    vs = [v for _, v in pts]; ds = [d for d, _ in pts]
    y = (date.fromisoformat(ds[-1])-date.fromisoformat(ds[0])).days/365.25
    cg = (vs[-1]/vs[0])**(1/y)-1; pk=-1e18; md=0.0
    for v in vs: pk=max(pk,v); md=min(md, v/pk-1)
    return cg, md, (cg/abs(md) if md else 0)

nv = [nser[i] for i in range(len(all_dates)) if all_dates[i] >= rebal_dates[0] and nser[i]]
ny = (date.fromisoformat(all_dates[-1])-date.fromisoformat(rebal_dates[0])).days/365.25
ncg = (nv[-1]/nv[0])**(1/ny)-1; npk=-1e18; nmd=0.0
for v in nv: npk=max(npk,v); nmd=min(nmd, v/npk-1)
def lag1(sig):   # act on YESTERDAY's signal (no look-ahead): you know today's close only at EOD
    out = {all_dates[0]: False}
    for i in range(1, len(all_dates)):
        out[all_dates[i]] = sig.get(all_dates[i-1], False)
    return out

print(f"\n{'strategy (signal LAGGED 1d, realistic)':38} {'CAGR':>6} {'maxDD':>7} {'Calmar':>6} | {'IS':>6} {'OOS':>6}  tog")
print("-"*90)
print(f"{'NIFTY50 buy-hold':38} {ncg*100:5.1f}% {nmd*100:6.1f}% {ncg/abs(nmd):6.2f} |")
for (mode, tn) in (("blend", 30), ("pure_mom", 20)):
    print("-"*90)
    for name, sig in (("none", none_sig), ("debounce3", deb_sig), ("nifty_sma200", sma_sig)):
        eq, tg = sim(bask[(mode, tn)], tn, lag1(sig))
        c, m, cal = met(eq); isc = met(eq, hi="2021-12-31")[0]; oos = met(eq, lo="2022-01-01")[0]
        print(f"{mode+' top'+str(tn)+' | '+name:38} {c*100:5.1f}% {m*100:6.1f}% {cal:6.2f} | {isc*100:5.1f}% {oos*100:5.1f}%  {tg}")
    # show the look-ahead (unlagged) sma200 for contrast — the illusory number
    eq0, _ = sim(bask[(mode, tn)], tn, sma_sig)
    c0, m0, cal0 = met(eq0)
    print(f"{mode+' top'+str(tn)+' | sma200 LOOK-AHEAD (bogus)':38} {c0*100:5.1f}% {m0*100:6.1f}% {cal0:6.2f} |  <- inflated, ignore")
print("\nWin = LAGGED Calmar beats 'none' with OOS holding. Gap between lagged & look-ahead = the bias we removed.")
