"""PEAD grind-harder — SELECTION-ALPHA feature diagnostic. READ-ONLY, local, zero cost.

For every base-qualified reaction (surprise>=5%, anti-pump<75%, mkt-dd>-5%), simulate the
standalone drift in R (prod simulate_exit, arm 1.75R, 60d), then bucket by candidate
features and report mean-R / win-rate / n split IS(2015-20) vs OOS(2021-26). A feature is
an EDGE only if a bucket beats the pool mean in BOTH halves (consistent, not a period proxy).
Features (all free from bars): reaction size, gap-fraction, volume conviction, pre-event
run-up shape, turnover (size), ATR%, month/season. Winners here become filters to engine-test.
"""
from __future__ import annotations
import os, sys, json, pickle
from bisect import bisect_left
from statistics import mean

sys.path.insert(0, "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.domain.pead_signals import earnings_surprise, pre_event_runup, passes_pead_gates, ANTI_PUMP_LOOKBACK, ATR_SL_MULT
from autotrader.domain.swing_exit import simulate_exit

C = os.path.expanduser("~/.autotrader_backtest_cache")
ev = json.load(open(f"{C}/pead_nse_result_dates_2012_2026.json"))["events"]
bars = pickle.load(open(f"{C}/swing_adj_bars_2015.pkl", "rb"))
mkt = json.load(open(f"{C}/market_inputs_2015.json"))
md = sorted(d for d in mkt if mkt[d].get("nifty_close"))
pk = -1e18; ddd = {}
for d in md:
    v = float(mkt[d]["nifty_close"]); pk = max(pk, v); ddd[d] = v / pk - 1.0
def market_dd(d):
    i = bisect_left(md, d) - 1
    return ddd[md[i]] if i >= 0 else None

def atr14(hi, lo, cl):
    tr = [hi[0] - lo[0]]
    for i in range(1, len(cl)):
        tr.append(max(hi[i]-lo[i], abs(hi[i]-cl[i-1]), abs(lo[i]-cl[i-1])))
    out = [None]*len(cl); s = 0.0
    for i in range(len(tr)):
        s += tr[i]
        if i >= 14: s -= tr[i-14]
        if i >= 13: out[i] = s/14.0
    return out

SYM = {}
for s, b in bars.items():
    if not b or len(b) < ANTI_PUMP_LOOKBACK + 20: continue
    d=[x[0] for x in b]; o=[float(x[1]) for x in b]; hi=[float(x[2]) for x in b]
    lo=[float(x[3]) for x in b]; cl=[float(x[4]) for x in b]; vol=[float(x[5]) for x in b]
    SYM[s]={"d":d,"o":o,"cl":cl,"vol":vol,"bars":b,"atr":atr14(hi,lo,cl)}

# build qualified trades with features + standalone R
trades = []
for e in ev:
    sy = SYM.get(e["symbol"])
    if sy is None or e["date"] < "2015-01-01": continue
    dl = sy["d"]; ri = bisect_left(dl, e["date"])
    if ri >= len(dl) or ri < ANTI_PUMP_LOOKBACK+1 or ri+1 >= len(sy["cl"]): continue
    sp = earnings_surprise(sy["cl"], ri); ru = pre_event_runup(sy["cl"], ri); m = market_dd(dl[ri])
    atr = sy["atr"][ri]
    if not passes_pead_gates(sp, ru, m) or not atr or atr <= 0: continue
    ei = ri+1; epx = sy["o"][ei]; sl = ATR_SL_MULT*atr
    if epx <= 0 or sl <= 0: continue
    off, xpx, _ = simulate_exit(sy["bars"], ei, True, sl, 60, trail_R=1.0, activate_R=1.75)
    R = (xpx - epx)/sl
    gap = sy["o"][ri]/sy["cl"][ri-1] - 1.0            # opening gap on reaction day
    v20 = mean(sy["vol"][ri-20:ri]) if ri >= 20 else sy["vol"][ri]
    vratio = sy["vol"][ri]/v20 if v20 > 0 else 0.0    # volume conviction
    turn = sy["cl"][ri]*sy["vol"][ri]                 # reaction-day turnover (size proxy, Rs)
    atrpct = atr/sy["cl"][ri]
    trades.append({"d": dl[ei], "R": R, "sp": sp, "gap": gap, "gapfrac": (gap/sp if sp else 0),
                   "vr": vratio, "ru": ru, "turn": turn, "atrpct": atrpct, "mon": dl[ei][5:7]})

IS = [t for t in trades if t["d"] <= "2020-12-31"]; OOS = [t for t in trades if t["d"] >= "2021-01-01"]
def stat(ts): return (len(ts), mean(t["R"] for t in ts) if ts else 0.0,
                       100*sum(1 for t in ts if t["R"]>0)/len(ts) if ts else 0.0)
print(f"qualified trades: {len(trades)} | IS {len(IS)} OOS {len(OOS)}")
print(f"POOL MEAN R: IS {stat(IS)[1]:+.3f} (WR {stat(IS)[2]:.0f}%)  OOS {stat(OOS)[1]:+.3f} (WR {stat(OOS)[2]:.0f}%)")
print("=  edge = bucket mean-R beats pool in BOTH halves  =" )

def bucket(name, keyf, edges, labels):
    print(f"\n[{name}]  (n_IS meanR_IS | n_OOS meanR_OOS)")
    poolIS, poolOOS = stat(IS)[1], stat(OOS)[1]
    for lab, lo, hi in zip(labels, edges[:-1], edges[1:]):
        bIS = [t for t in IS if lo <= keyf(t) < hi]; bOOS = [t for t in OOS if lo <= keyf(t) < hi]
        if not bIS and not bOOS: continue
        rIS = mean(t["R"] for t in bIS) if bIS else 0.0; rOOS = mean(t["R"] for t in bOOS) if bOOS else 0.0
        flag = "  <== EDGE" if (bIS and bOOS and rIS > poolIS and rOOS > poolOOS) else \
               ("  (bad both)" if (bIS and bOOS and rIS < poolIS and rOOS < poolOOS) else "")
        print(f"  {lab:14} {len(bIS):4} {rIS:+.3f} | {len(bOOS):4} {rOOS:+.3f}{flag}")

bucket("reaction size", lambda t: t["sp"], [0.05,0.07,0.10,0.15,0.25,9], ["5-7%","7-10%","10-15%","15-25%","25%+"])
bucket("gap fraction (gap/reaction)", lambda t: t["gapfrac"], [-9,0.3,0.6,0.9,9], ["<30% gap","30-60%","60-90%","90%+ (all-gap)"])
bucket("volume conviction (x20d)", lambda t: t["vr"], [0,1.5,3,6,999], ["<1.5x","1.5-3x","3-6x","6x+"])
bucket("pre-event run-up", lambda t: t["ru"], [-9,0,0.15,0.35,0.75], ["negative","0-15%","15-35%","35-75%"])
bucket("turnover Rs (size)", lambda t: t["turn"], [0,5e7,2.5e8,1e9,9e12], ["<5cr","5-25cr","25-100cr","100cr+"])
bucket("ATR% (stock vol)", lambda t: t["atrpct"], [0,0.03,0.05,0.08,9], ["<3%","3-5%","5-8%","8%+"])
