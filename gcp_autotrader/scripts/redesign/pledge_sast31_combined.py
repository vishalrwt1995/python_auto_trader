"""VALIDATION: can the broad SAST Reg 31 'Release' feed improve the LIVE pledge channel's candidate
sourcing? Reuses the exact pledge_final engine (revoke cands from insider PIT, gated b200>50 & Nifty>100DMA,
60d hold, 10 slots, 2.5xATR stop, 1.5% risk, compounding, Upstox cost). Adds SAST Reg 31 'Release' events
(the parallel SAST-feed pledge-release) as extra candidates. Tests: (A) pledge-revoke baseline [must ~repro
Calmar 2.18], (B) SAST31-release-only channel, (C) COMBINED revoke + SAST31-distinct. Overlap measured.
Verdict: additive only if combined Calmar > baseline BOTH halves; else the broad feed dilutes (cf. SAST29
booster hurt insider). READ-ONLY, cached."""
import os
for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "4"
import sys, json, glob, pickle
from bisect import bisect_right, bisect_left
from datetime import datetime
sys.path.insert(0, "/Users/apple/Projects_Migrated/Auto Trading Python GCP/gcp_autotrader/src")
from autotrader.backtest.costs import compute_leg_cost, CostConfig

C = os.path.expanduser("~/.autotrader_backtest_cache"); PIT = os.path.join(C, "insider_pit")
SCR = "/private/tmp/claude-501/-Users-apple-Projects-Migrated-Auto-Trading-Python-GCP/439e48e8-a413-4a1d-9d0a-530e53a5e277/scratchpad"
UPSTOX = CostConfig.upstox(); CAP0, SLIP, IS_END = 200_000.0, 0.001, "2020-12-31"
# LIVE pledge config (PROJECT_KNOWLEDGE §3): px>200DMA + turnover>=25cr + ATR14x2.0 + cap10% + gated + 60d
TURN_MIN, PRICE_MIN, ATR_MULT, RISK_PCT, B200_MIN, HOLD, SLOTS = 25e7, 30.0, 2.0, 0.015, 50.0, 60, 10

def atr14(h, l, c):
    tr = [h[0]-l[0]]
    for i in range(1, len(c)): tr.append(max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])))
    out = [None]*len(c); s = 0.0
    for i in range(len(tr)):
        s += tr[i]
        if i >= 14: s -= tr[i-14]
        if i >= 13: out[i] = s/14.0
    return out
def fnum(x):
    try: return float(str(x).replace(",", ""))
    except Exception: return None
def dparse(x, fmts):
    s = str(x).split()[0] if x else ""
    for f in fmts:
        try: return datetime.strptime(s, f).strftime("%Y-%m-%d")
        except Exception: pass
    return None

bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
SYM = {}
for s, b in bars.items():
    if len(b) < 70: continue
    d = [x[0] for x in b]; o = [x[1] for x in b]; h = [x[2] for x in b]; l = [x[3] for x in b]; c = [x[4] for x in b]; v = [x[5] for x in b]
    turn = [None]*len(c); run = 0.0; sma200 = [None]*len(c); rs = 0.0
    for i in range(len(c)):
        if i >= 1: run += c[i-1]*v[i-1]
        if i >= 21: run -= c[i-21]*v[i-21]
        if i >= 21: turn[i] = run/20.0
        rs += c[i]
        if i >= 200: rs -= c[i-200]
        if i >= 199: sma200[i] = rs/200.0
    SYM[s] = {"d": d, "o": o, "c": c, "atr": atr14(h, l, c), "turn": turn, "sma200": sma200, "idx": {x: i for i, x in enumerate(d)}}
b200h = pickle.load(open(f"{C}/swing_b200_history.pkl", "rb")); bdd = sorted(b200h)
mkt = json.load(open(f"{C}/market_inputs_2015.json")); md = sorted(x for x in mkt if mkt[x].get("nifty_close")); nc = [float(mkt[x]["nifty_close"]) for x in md]
ma = [None]*len(nc); run = 0.0
for i in range(len(nc)):
    run += nc[i]
    if i >= 100: run -= nc[i-100]
    if i >= 99: ma[i] = run/100.0
def nifty_ok(dt):
    i = bisect_left(md, dt)-1; return i < 0 or ma[i] is None or nc[i] > ma[i]
def b200_at(dt):
    i = bisect_right(bdd, dt)-1; return b200h[bdd[i]] if i >= 0 else 0.0

def mkcand(sym, dd, shares):
    S = SYM.get(sym)
    if not S or not dd: return None
    ref = bisect_right(S["d"], dd)
    if ref >= len(S["c"]) or ref < 1 or S["atr"][ref-1] is None or S["atr"][ref-1] <= 0: return None
    if S["turn"][ref] is None or S["turn"][ref] < TURN_MIN or S["o"][ref] < PRICE_MIN: return None
    if S["sma200"][ref] is None or S["c"][ref] <= S["sma200"][ref]: return None   # px>200DMA falling-knife filter
    ed = S["d"][ref]
    return {"ed": ed, "sym": sym, "ref": ref, "sl": ATR_MULT*S["atr"][ref-1],
            "relval": (shares or 0.0)*S["o"][ref], "b200": b200_at(ed), "nifty_ok": nifty_ok(ed)}

# (A) pledge revokes from insider PIT
recs = []
for fn in sorted(glob.glob(os.path.join(PIT, "*.json"))):
    try: recs.extend(json.load(open(fn)))
    except Exception: pass
revoke = []
for r in recs:
    if "revoke" not in str(r.get("tdpTransactionType", "")).lower(): continue
    if "promoter" not in str(r.get("personCategory", "")).lower(): continue
    c = mkcand(str(r.get("symbol") or "").strip().upper(), dparse(r.get("date"), ("%d-%b-%Y",)), fnum(r.get("secAcq")))
    if c: revoke.append(c)
# (B) SAST31 releases
s31 = json.load(open(f"{SCR}/sast_reg31.json"))
sast_all, sast_loan = [], []
for r in s31:
    if "release" not in str(r.get("typeOfEvent", "")).lower(): continue
    c = mkcand(str(r.get("symbol") or "").strip().upper(), dparse(r.get("broadcastDateTime"), ("%d-%b-%Y", "%d-%B-%Y")), fnum(r.get("numofShares")))
    if not c: continue
    sast_all.append(c)
    if "margin pledge" not in str(r.get("reasonForEncumbrance", "")).lower(): sast_loan.append(c)

# overlap: SAST31-loan release within +-10 trading days of a revoke on same sym
rev_by = {}
for c in revoke: rev_by.setdefault(c["sym"], []).append(c["ref"])
def is_dupe(c):
    lst = rev_by.get(c["sym"], []); return any(abs(x-c["ref"]) <= 10 for x in lst)
sast_distinct = [c for c in sast_loan if not is_dupe(c)]
print(f"revokes {len(revoke)} | SAST31 releases: all {len(sast_all)} / loan-only {len(sast_loan)} | SAST31-loan distinct-from-revoke {len(sast_distinct)} (overlap {100*(len(sast_loan)-len(sast_distinct))/max(1,len(sast_loan)):.0f}%)", flush=True)

def seg(curve, s0, y0, y1):
    if not curve or s0 <= 0 or y1 < y0: return 0.0, 0.0
    peak = s0; mdd = 0.0
    for _, e in curve: peak = max(peak, e); mdd = min(mdd, e/peak-1)
    return (curve[-1][1]/s0)**(1/max(1, y1-y0+1))-1, mdd
def walk(cands, tag):
    cands = sorted(cands, key=lambda x: (x["ed"], -x["relval"]))
    equity = CAP0; free = [""]*SLOTS; openp = []; closed = []; held = set()
    for c in cands:
        if c["b200"] <= B200_MIN or not c["nifty_ok"]: continue
        ed = c["ed"]; still = []
        for xd, pnl, sy in openp:
            if xd <= ed: equity += pnl; closed.append((xd, pnl)); held.discard(sy)
            else: still.append((xd, pnl, sy))
        openp = still
        for k in range(SLOTS):
            if free[k] and free[k] <= ed: free[k] = ""
        slot = next((k for k in range(SLOTS) if not free[k]), None)
        if slot is None or c["sym"] in held: continue
        S = SYM[c["sym"]]; ref = c["ref"]; epx = S["o"][ref]
        if epx <= 0: continue
        qty = int((RISK_PCT*equity)//c["sl"])
        if qty < 1: continue
        if qty*epx > equity/SLOTS: qty = int((equity/SLOTS)//epx)
        if qty < 1: continue
        xi = min(ref+HOLD, len(S["c"])-1); xpx = S["c"][xi]; stop = epx-c["sl"]
        for k in range(ref+1, xi+1):
            if k < len(S["c"]) and S["c"][k] <= stop: xpx = stop; xi = k; break
        xd = S["d"][xi]; ef = epx*(1+SLIP); xf = xpx*(1-SLIP)
        pnl = (xf-ef)*qty-(compute_leg_cost(side="BUY", qty=qty, price=ef, is_swing=True, cfg=UPSTOX)+compute_leg_cost(side="SELL", qty=qty, price=xf, is_swing=True, cfg=UPSTOX))
        free[slot] = xd; openp.append((xd, pnl, c["sym"])); held.add(c["sym"])
    for xd, pnl, sy in openp: equity += pnl; closed.append((xd, pnl))
    if len(closed) < 10: print(f"  {tag:34} thin (n={len(closed)})"); return
    closed.sort(); eq = CAP0; curve = []
    for xd, pnl in closed: eq += pnl; curve.append((xd, eq))
    y0, y1 = int(closed[0][0][:4]), int(closed[-1][0][:4])
    isp = [x for x in curve if x[0] <= IS_END]; oosp = [x for x in curve if x[0] > IS_END]; eqis = isp[-1][1] if isp else CAP0
    fc, fdd = seg(curve, CAP0, y0, y1); ic, idd = seg(isp, CAP0, y0, 2020); oc, odd = seg(oosp, eqis, 2021, y1)
    span = y1-y0+1; wins = sum(1 for _, p in closed if p > 0)
    print(f"  {tag:34} CAGR{fc*100:+5.1f}% DD{fdd*100:6.1f}% Cal{fc/abs(fdd) if fdd else 0:5.2f} | IS{ic/abs(idd) if idd else 0:5.2f} OOS{oc/abs(odd) if odd else 0:5.2f} | n{len(closed):>4} WR{100*wins/len(closed):3.0f}% {len(closed)/span:.0f}/yr", flush=True)

print("\n=== pledge channel: baseline vs SAST31 feed (locked config) ===", flush=True)
walk(revoke, "A. revoke ONLY (= live baseline)")
walk(sast_loan, "B. SAST31-loan-release ONLY")
walk(revoke + sast_distinct, "C. COMBINED revoke + SAST31-distinct")
walk(revoke + sast_all, "D. COMBINED revoke + ALL SAST31 (incl margin+overlap)")
print("\nREAD: A must ~repro Calmar 2.18. SAST31 improves sourcing ONLY if C's Calmar > A's in BOTH halves; else it dilutes.", flush=True)
