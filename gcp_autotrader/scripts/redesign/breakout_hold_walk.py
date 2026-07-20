"""Breakout-hold full account walk (Edge A, decisive test) -- gives the 52wk-high breakout the
fair trail-based framing the fixed-hold diagnostic denied it (fat-tailed trends need a trail,
not a fixed exit). Real prod exit (swing_exit: 2.5xATR SL, 1.75R arm, 1.0R trail, long hold),
real Upstox swing costs, slippage, 5-slot compounding account, Nifty>200DMA regime overlay
(breakouts fail in downtrends). IS(<=2020)/OOS(>=2021). PLUS the decisive additivity check:
what fraction of breakout entries are ALREADY top-20 12-1 momentum names (i.e. names the
MOMENTUM channel would hold anyway)? A channel that clears the return bar but is ~redundant
with MOMENTUM is not a new edge. Survivorship-safe (pead_full_bars_2014). READ-ONLY, single
-process, thread-capped, cached only (zero GCP cost). Imports prod modules read-only; edits none."""
import os
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "4"
import sys, pickle, json, statistics
from collections import deque, defaultdict
from bisect import bisect_left
sys.path.insert(0, "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.domain.swing_exit import simulate_exit
from autotrader.backtest.costs import compute_leg_cost, CostConfig

C = os.path.expanduser("~/.autotrader_backtest_cache")
UPSTOX = CostConfig.upstox()
CAP0, SLOTS, RISK_PCT, SLIP = 200_000.0, 5, 0.015, 0.001
ATR_MULT, TRAIL_R, ACT_R, MAX_HOLD = 2.5, 1.0, 1.75, 60
MIN_PRICE, TURN_MIN, MIN_HIST = 30.0, 10e7, 273
IS_END = "2020-12-31"

print("loading bars + nifty ...", flush=True)
bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
mkt = json.load(open(f"{C}/market_inputs_2015.json"))
mdates = sorted(d for d in mkt if mkt[d].get("nifty_close"))
nclose = [float(mkt[d]["nifty_close"]) for d in mdates]
nma200 = [None] * len(nclose); run = 0.0
for i in range(len(nclose)):
    run += nclose[i]
    if i >= 200: run -= nclose[i-200]
    if i >= 199: nma200[i] = run / 200.0
def nifty_uptrend(d):
    i = bisect_left(mdates, d) - 1
    if i < 0 or nma200[i] is None: return True   # pre-history: allow
    return nclose[i] > nma200[i]

def atr14(h, l, c):
    tr = [h[0]-l[0]]
    for i in range(1, len(c)): tr.append(max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])))
    out = [None]*len(c); s = 0.0
    for i in range(len(tr)):
        s += tr[i]
        if i >= 14: s -= tr[i-14]
        if i >= 13: out[i] = s/14.0
    return out
def prior_max(a, n):
    out = [None]*len(a); dq = deque()
    for i in range(len(a)):
        while dq and dq[0] < i-n: dq.popleft()
        out[i] = a[dq[0]] if dq else None
        while dq and a[dq[-1]] <= a[i]: dq.pop()
        dq.append(i)
    return out

SYM = {}
cands = []   # (entry_date, sym, entry_idx, sl_dist)
for s, b in bars.items():
    n = len(b)
    if n < MIN_HIST + 5: continue
    d=[x[0] for x in b]; o=[x[1] for x in b]; h=[x[2] for x in b]; l=[x[3] for x in b]; c=[x[4] for x in b]; v=[x[5] for x in b]
    atr = atr14(h, l, c); pm252 = prior_max(c, 252)
    turn=[None]*n; run=0.0
    for i in range(n):
        if i>=1: run += c[i-1]*v[i-1]
        if i>=21: run -= c[i-21]*v[i-21]
        if i>=21: turn[i]=run/20.0
    SYM[s] = {"d":d,"o":o,"c":c,"bars":b,"idx":{dt:i for i,dt in enumerate(d)}}
    for j in range(MIN_HIST, n-1):
        if c[j] < MIN_PRICE or turn[j] is None or turn[j] < TURN_MIN: continue
        if pm252[j] is None or atr[j] is None or atr[j] <= 0: continue
        if c[j] >= pm252[j]:                          # NEW 52wk closing high
            cands.append((d[j+1], s, j+1, ATR_MULT*atr[j]))   # enter next open
cands.sort()
print(f"  {len(SYM):,} symbols | {len(cands):,} breakout entry candidates\n", flush=True)

# --- account walk: chronological, 5 slots, compounding, Nifty>200DMA overlay ---
def walk(use_overlay):
    equity = CAP0; free = [""]*SLOTS; openpos = []   # (exit_date, pnl)
    trades = []; peak = equity; maxdd = 0.0
    for entry_d, s, ei, sl_dist in cands:
        # settle exits due by entry_d
        still = []
        for xd, pnl in openpos:
            if xd <= entry_d:
                equity += pnl; peak = max(peak, equity); maxdd = min(maxdd, equity/peak-1.0)
            else: still.append((xd, pnl))
        openpos = still
        for k in range(SLOTS):
            if free[k] and free[k] <= entry_d: free[k] = ""
        if use_overlay and not nifty_uptrend(entry_d): continue
        slot = next((k for k in range(SLOTS) if not free[k]), None)
        if slot is None: continue
        S = SYM[s]; entry_px = S["o"][ei]
        if entry_px <= 0: continue
        risk = RISK_PCT * equity
        qty = int(risk // sl_dist)
        if qty < 1: continue
        if qty*entry_px > equity/SLOTS: qty = int((equity/SLOTS)//entry_px)
        if qty < 1: continue
        off, exit_px, _ = simulate_exit(S["bars"], ei, True, sl_dist, MAX_HOLD, trail_R=TRAIL_R, activate_R=ACT_R)
        xi = min(ei+off, len(S["bars"])-1); xd = S["d"][xi]
        ef = entry_px*(1+SLIP); xf = exit_px*(1-SLIP)
        gross = (xf-ef)*qty
        cost = (compute_leg_cost(side="BUY", qty=qty, price=ef, is_swing=True, cfg=UPSTOX)
                + compute_leg_cost(side="SELL", qty=qty, price=xf, is_swing=True, cfg=UPSTOX))
        pnl = gross - cost
        free[slot] = xd; openpos.append((xd, pnl))
        trades.append({"ed":entry_d, "xd":xd, "sym":s, "pnl":pnl, "R":pnl/(sl_dist*qty)})
    for xd, pnl in openpos:
        equity += pnl; peak = max(peak, equity); maxdd = min(maxdd, equity/peak-1.0)
    return equity, maxdd, trades

def summarize(tag, use_overlay):
    eq, maxdd, tr = walk(use_overlay)
    if not tr: print(f"  {tag}: no trades"); return
    yrs = (max(t["xd"] for t in tr)[:4], min(t["ed"] for t in tr)[:4])
    span = int(yrs[0]) - int(yrs[1]) + 1
    cagr = (eq/CAP0)**(1/span) - 1 if span > 0 else 0
    wr = 100*sum(1 for t in tr if t["pnl"]>0)/len(tr)
    calmar = cagr/abs(maxdd) if maxdd < 0 else float('inf')
    isr = [t["R"] for t in tr if t["ed"] <= IS_END]; oosr = [t["R"] for t in tr if t["ed"] > IS_END]
    print(f"  {tag}", flush=True)
    print(f"    final=Rs{eq:,.0f}  CAGR={cagr*100:+.1f}%  maxDD={maxdd*100:.1f}%  Calmar={calmar:.2f}  "
          f"trades={len(tr)} (~{len(tr)/span:.0f}/yr)  WR={wr:.1f}%", flush=True)
    print(f"    avgR: IS(n={len(isr)})={statistics.mean(isr):+.3f}  OOS(n={len(oosr)})={statistics.mean(oosr):+.3f}", flush=True)
    return tr

print("=== ACCOUNT WALK (trail exit, 5-slot compounding, real Upstox costs) ===\n", flush=True)
print(f"  Nifty buy-hold over span: reference ~10-12% CAGR\n", flush=True)
summarize("NO overlay (all regimes)", False)
print(flush=True)
tr = summarize("Nifty>200DMA overlay", True)

# --- decisive additivity: are breakout entries already top-20 12-1 momentum names? ---
print("\n=== additivity vs MOMENTUM channel (overlap of entries w/ top-20 12-1 momentum) ===", flush=True)
def mom_top(entry_dates, K=20):
    hits = 0; tot = 0
    cache = {}
    for d, s in entry_dates:
        if d not in cache:
            uni = []
            for sym, S in SYM.items():
                j = S["idx"].get(d)
                if j is None or j < 273: continue
                c = S["c"]
                if c[j-273] <= 0: continue
                uni.append((sym, c[j-21]/c[j-273]-1.0))
            uni.sort(key=lambda x: -x[1]); cache[d] = {sym for sym,_ in uni[:K]}
        tot += 1; hits += 1 if s in cache[d] else 0
    return 100.0*hits/tot if tot else 0.0
if tr:
    ed = [(t["ed"], t["sym"]) for t in tr]
    print(f"  entries already in top-20 12-1 momentum: {mom_top(ed,20):.1f}%", flush=True)
    print(f"  entries already in top-50 12-1 momentum: {mom_top(ed,50):.1f}%", flush=True)
    print("\n  READ: high overlap (>50% top-20) -> redundant with MOMENTUM, kill even if profitable.", flush=True)
    print("  Low overlap + clears bar (Calmar>=0.6, CAGR>>FD, DD sane) -> a real additive channel.", flush=True)
