"""CORE channel — PROD-FAITHFUL backtest (2016-2026) on cached daily bars.

Drives the backtest through the ACTUAL production functions (read-only imports; prod
code byte-untouched):
  - selection: domain.core_signals.rank_blend_select  (12-1 mom + 60d vol, top-100 turnover -> top-30)
  - sizing:    services.core_trading_service.plan_core_rebalance  (integer shares + residual
               sweep + per-name cap at CAPITAL_CORE=Rs3L -> excludes names priced > 1.5*slice)
  - catastrophe stop: core_signals CATASTROPHE_STOP_PCT (-60%)
  - cost:      backtest.costs full-Upstox delivery round-trip, booked per holding-episode

Quarterly Jan/Apr/Jul/Oct rebalance, buy-and-hold, add/drop. Cash tracked (integer-share
book leaves residual cash). Free (cached bars, no BQ), single-process, read-only.

RESIDUAL fidelity caveat (cannot close without cost-gated data): universe = top-100-by-turnover
from the 2,397-sym cached set (a close proxy for prod's live F&O list; arguably LESS survivor-
biased since it's not the current-F&O membership), and the bars are survivor-ish (names alive
through 2026). True point-in-time-F&O + delisting-inclusive needs a BQ pull.
"""
from __future__ import annotations
import os, sys, pickle
from bisect import bisect_right

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
from autotrader.domain.core_signals import (
    momentum_score, realized_vol, passes_universe_gates, rank_blend_select,
    MOM_LOOKBACK, CATASTROPHE_STOP_PCT,
)
from autotrader.services.core_trading_service import plan_core_rebalance
from autotrader.backtest.costs import CostConfig, compute_round_trip_cost

CAPITAL = 300000.0                      # CAPITAL_CORE (prod)
_UP = CostConfig.upstox()
def rt_cost(qty, entry, exit_):         # full-Upstox delivery round-trip for one episode
    if qty <= 0: return 0.0
    return compute_round_trip_cost(qty=qty, entry_price=entry, exit_price=exit_, is_swing=True, cfg=_UP)

CACHE = os.path.expanduser("~/.autotrader_backtest_cache/swing_adj_bars_2015.pkl")
raw = pickle.load(open(CACHE, "rb"))    # {sym: [[date,o,h,l,c,vol], ...]}

sym = {}
for s, bars in raw.items():
    if not bars or len(bars) < MOM_LOOKBACK + 5:
        continue
    dates = [b[0] for b in bars]
    lows = [float(b[3]) for b in bars]
    closes = [float(b[4]) for b in bars]
    vols = [float(b[5]) for b in bars]
    rets = [0.0] + [(closes[i]/closes[i-1]-1.0) if closes[i-1] > 0 else 0.0 for i in range(1, len(closes))]
    tov = []; _rs = 0.0; _win = []
    for i in range(len(closes)):
        x = closes[i]*vols[i]; _win.append(x); _rs += x
        if len(_win) > 20: _rs -= _win.pop(0)
        tov.append(_rs/len(_win))
    sym[s] = {"dates": dates, "lows": lows, "closes": closes, "rets": rets, "tov": tov}
print(f"loaded {len(sym)} symbols")

all_dates = sorted({d for v in sym.values() for d in v["dates"]})
from bisect import bisect_left
def first_ge(iso):
    k = bisect_left(all_dates, iso)
    return all_dates[k] if k < len(all_dates) else None
rebal_dates = sorted({first_ge(f"{y}-{m:02d}-01") for y in range(2016, 2027) for m in (1,4,7,10)} - {None})
rebal_dates = [d for d in rebal_dates if d <= all_dates[-1]]
print(f"{len(rebal_dates)} rebalances: {rebal_dates[0]} .. {rebal_dates[-1]}  (CAPITAL_CORE=Rs{CAPITAL:.0f})")

def i_at(s, d):
    ds = sym[s]["dates"]; i = bisect_right(ds, d)-1
    return i if i >= 0 else None
def close_at(s, d):
    i = i_at(s, d); return sym[s]["closes"][i] if i is not None else None
def low_at(s, d):
    ds = sym[s]["dates"]; i = bisect_right(ds, d)-1
    return sym[s]["lows"][i] if i >= 0 and ds[i] == d else None   # only if traded on day d

def select_basket(d):
    cand = []
    for s, v in sym.items():
        i = i_at(s, d)
        if i is None or i - MOM_LOOKBACK < 0: continue
        price = v["closes"][i]; tov = v["tov"][i]
        if not passes_universe_gates(price, tov, has_history=True): continue
        mom = momentum_score(v["closes"], i); vol = realized_vol(v["rets"], i)
        if mom is None or vol is None: continue
        cand.append({"symbol": s, "momentum": mom, "vol": vol, "turnover": tov})
    return rank_blend_select(cand)

cash = CAPITAL
book = {}            # sym -> {"qty", "entry"}
equity = []
total_cost = 0.0
excluded_examples = set()

for k, d in enumerate(rebal_dates):
    basket = select_basket(d)
    if not basket: continue
    target = [{"symbol": s, "ref_price": close_at(s, d), "instrument_key": s} for s in basket if close_at(s, d)]
    holdings = [{"symbol": s, "qty": b["qty"], "entry_price": b["entry"], "instrument_key": s} for s, b in book.items()]
    plan = plan_core_rebalance(target, holdings, CAPITAL, cfg=None)
    for sell in plan["sells"]:
        s = sell["symbol"]; q = book[s]["qty"]; px = close_at(s, d) or book[s]["entry"]
        c = rt_cost(q, book[s]["entry"], px); total_cost += c
        cash += q*px - c; del book[s]
    for buy in plan["buys"]:
        s = buy["symbol"]; q = int(buy["qty"]); px = float(buy["entry_price"])
        cash -= q*px; book[s] = {"qty": q, "entry": px}
    # track names excluded by the price cap (priced > 1.5*slice = 1.5*Rs10k = Rs15k)
    for t in target:
        if t["symbol"] not in book and t["ref_price"] and t["ref_price"] > 1.5*(CAPITAL/30):
            excluded_examples.add(f"{t['symbol']}(Rs{t['ref_price']:.0f})")
    d_next = rebal_dates[k+1] if k+1 < len(rebal_dates) else all_dates[-1]
    for dd in all_dates:
        if dd < d or dd > d_next: continue
        for s in list(book.keys()):                       # catastrophe stop (-60%)
            lo = low_at(s, dd)
            stop = book[s]["entry"]*(1-CATASTROPHE_STOP_PCT)
            if lo is not None and lo <= stop:
                q = book[s]["qty"]; c = rt_cost(q, book[s]["entry"], stop); total_cost += c
                cash += q*stop - c; del book[s]
        mtm = cash + sum(b["qty"]*(close_at(s, dd) or b["entry"]) for s, b in book.items())
        equity.append((dd, mtm))

# terminal net: subtract round-trip cost to close remaining open positions
fin = all_dates[-1]
open_close_cost = sum(rt_cost(b["qty"], b["entry"], close_at(s, fin) or b["entry"]) for s, b in book.items())
equity[-1] = (equity[-1][0], equity[-1][1] - open_close_cost)
total_cost += open_close_cost

from datetime import date
vals = [v for _, v in equity]; dts = [d for d, _ in equity]
start, end = CAPITAL, vals[-1]
yrs = (date.fromisoformat(dts[-1]) - date.fromisoformat(dts[0])).days/365.25
cagr = (end/start)**(1/yrs)-1
peak = -1e18; mdd = 0.0
for v in vals:
    peak = max(peak, v); mdd = min(mdd, v/peak-1)
yr_first = {}; yr_last = {}
for dd, v in equity:
    y = dd[:4]; yr_first.setdefault(y, v); yr_last[y] = v
print(f"\n=== CORE PROD-FAITHFUL backtest {dts[0]} .. {dts[-1]} ({yrs:.1f}y) ===")
print(f"CAGR   = {cagr*100:.1f}%")
print(f"maxDD  = {mdd*100:.1f}%")
print(f"Calmar = {cagr/abs(mdd):.2f}")
print(f"end    = Rs{end:,.0f}  from Rs{start:,.0f}  (total {(end/start-1)*100:.0f}%)")
print(f"cost   = Rs{total_cost:,.0f} lifetime ({total_cost/CAPITAL*100:.1f}% of capital over {yrs:.0f}y)")
print(f"names excluded by Rs15k price-cap (sample): {sorted(excluded_examples)[:8]}")
print("\nper-year:")
for y in sorted(yr_last):
    print(f"  {y}: {(yr_last[y]/yr_first[y]-1)*100:+.1f}%")
print("\nvs docstring claim: ~11% CAGR / -35% DD / Calmar 0.32 (survivor-inflated -> real ~9-10%)")
