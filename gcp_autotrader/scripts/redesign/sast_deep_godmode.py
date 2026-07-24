"""SAST Reg 29 DEEP — god-mode grind (pass 2). Builds on sast_deep_grind.py pass-1 finding:
promoter open-market ACQUISITION beats baseline both halves. Now the channel-level questions:
  (1) OVERLAP vs the live insider channel — is this the same trades wearing a SAST hat? (make-or-break)
  (2) HOLD surface (10/20/40/60) — confirm the f20 sweet spot
  (3) PORTFOLIO WALK — K concurrent slots, equal notional, full 0.7% cost, daily MTM equity curve ->
      CAGR / maxDD / Calmar / Sharpe / trades-yr / win-rate / YEAR-WISE return
  (4) SLOT sensitivity (5/10/15)
Signal = promoter OM acquisition, fillable (turn>=10cr, px>=30), entry = next trading day after `timestamp`
dissemination (look-ahead-free). Survivorship-safe bars. READ-ONLY, single-process, cached."""
import os, json, pickle, statistics
from bisect import bisect_right
from datetime import datetime
from collections import defaultdict, Counter

C = os.path.expanduser("~/.autotrader_backtest_cache")
S = "/private/tmp/claude-501/-Users-apple-Projects-Migrated-Auto-Trading-Python-GCP/439e48e8-a413-4a1d-9d0a-530e53a5e277/scratchpad"
COST, IS_END, TURN_MIN, PRICE_MIN = 0.007, "2020-12-31", 10e7, 30.0
CAP = 1_000_000.0

rows = json.load(open(f"{S}/sast_reg29_sdd_deep.json"))
bars = pickle.load(open(f"{C}/pead_full_bars_2014.pkl", "rb"))
SYM = {}
for s, b in bars.items():
    if len(b) < 70:
        continue
    d = [x[0] for x in b]; c = [x[4] for x in b]; v = [x[5] for x in b]
    turn = [None] * len(c); run = 0.0
    for i in range(len(c)):
        if i >= 1: run += c[i - 1] * v[i - 1]
        if i >= 21: run -= c[i - 21] * v[i - 21]
        if i >= 21: turn[i] = run / 20.0
    SYM[s] = {"d": d, "c": c, "turn": turn, "idx": {dd: i for i, dd in enumerate(d)}}

def dt(x):
    try: return datetime.strptime(str(x).split()[0], "%d-%b-%Y").strftime("%Y-%m-%d")
    except Exception: return None

def tl(r, f): return str(r.get(f, "")).lower()
def is_signal(r):
    return ("open market" in tl(r, "acquisitionMode") and tl(r, "acqSaleType") == "acquisition"
            and str(r.get("promoterType", "")).upper() == "Y")

# --- build fillable signal events: (entry_date, sym, ref_idx) ---
events = []
for r in rows:
    if not is_signal(r): continue
    sym = str(r.get("symbol") or "").strip().upper(); dd = dt(r.get("timestamp"))
    Sd = SYM.get(sym)
    if not Sd or not dd: continue
    ref = bisect_right(Sd["d"], dd)
    if ref >= len(Sd["c"]) or ref < 1: continue
    if Sd["turn"][ref] is None or Sd["turn"][ref] < TURN_MIN or Sd["c"][ref] < PRICE_MIN: continue
    events.append({"date": Sd["d"][ref], "sym": sym, "ref": ref})
events.sort(key=lambda e: e["date"])
print(f"promoter-OM-acq fillable events: {len(events)}  ({events[0]['date']}..{events[-1]['date']})", flush=True)

# ============ (1) OVERLAP vs live insider channel ============
ins = pickle.load(open(f"{C}/insider_cands_enriched.pkl", "rb"))["cands"]
ins_by_sym = defaultdict(list)
for c in ins:
    s = str(c.get("sym", "")).upper(); e = str(c.get("ed", ""))[:10]
    if s and e: ins_by_sym[s].append(e)
for s in ins_by_sym: ins_by_sym[s].sort()
def within(sym, date, win=5):
    lst = ins_by_sym.get(sym)
    if not lst: return False
    ds = SYM[sym]["idx"].get(date)
    if ds is None: return False
    for e in lst:
        di = SYM[sym]["idx"].get(e)
        if di is not None and abs(di - ds) <= win: return True
    return False
ov = sum(1 for e in events if within(e["sym"], e["date"], 5))
print(f"\n(1) OVERLAP vs insider channel (same sym, insider event within +-5 trading days):")
print(f"    {ov}/{len(events)} = {100*ov/len(events):.1f}% overlap  -> {'REDUNDANT' if ov/len(events)>0.5 else 'mostly DISTINCT'} with insider", flush=True)

# ============ (2) HOLD surface ============
def netret(sym, ref, hold):
    Sd = SYM[sym]
    if ref + hold >= len(Sd["c"]) or Sd["c"][ref] <= 0: return None
    return Sd["c"][ref + hold] / Sd["c"][ref] - 1.0 - COST
print("\n(2) HOLD surface (avg net% / median / WR / ann-proxy = avg*252/hold):", flush=True)
for h in (10, 20, 40, 60):
    rs = [netret(e["sym"], e["ref"], h) for e in events]; rs = [x for x in rs if x is not None]
    ann = statistics.mean(rs) * 252 / h * 100
    print(f"    hold {h:>2d}d: avg={statistics.mean(rs)*100:+5.2f}% med={statistics.median(rs)*100:+5.2f}% WR={100*sum(1 for x in rs if x>0)/len(rs):3.0f}% n={len(rs)}  ann~{ann:+.1f}%", flush=True)

# ============ (3) PORTFOLIO WALK (daily MTM equity curve) ============
cal = sorted({d for s in SYM.values() for d in s["d"]})
cal_idx = {d: i for i, d in enumerate(cal)}
def close_on(sym, t_date):
    Sd = SYM[sym]; i = Sd["idx"].get(t_date)
    return Sd["c"][i] if i is not None else None

def walk(hold, K):
    ev_by_date = defaultdict(list)
    for e in events: ev_by_date[e["date"]].append(e)
    notional = CAP / K
    open_pos = []            # {sym, entry_date, exit_cal, entry_px}
    held_syms = set()
    realized = 0.0; ntr = 0; wins = 0
    equity_curve = []; year_pnl = defaultdict(float)
    for t_i, t in enumerate(cal):
        # exits
        still = []
        for p in open_pos:
            if p["exit_cal"] <= t_i:
                px = close_on(p["sym"], cal[p["exit_cal"]]) or close_on(p["sym"], t) or p["entry_px"]
                pnl = notional * (px / p["entry_px"] - 1.0 - COST)
                realized += pnl; ntr += 1; wins += (1 if pnl > 0 else 0)
                year_pnl[t[:4]] += pnl; held_syms.discard(p["sym"])
            else:
                still.append(p)
        open_pos = still
        # entries
        for e in ev_by_date.get(t, []):
            if len(open_pos) >= K or e["sym"] in held_syms: continue
            Sd = SYM[e["sym"]]; ref = e["ref"]
            if ref + hold >= len(Sd["c"]): continue
            exit_cal = cal_idx.get(Sd["d"][ref + hold])
            if exit_cal is None: continue
            open_pos.append({"sym": e["sym"], "entry_date": t, "exit_cal": exit_cal, "entry_px": Sd["c"][ref]})
            held_syms.add(e["sym"])
        # mark-to-market equity
        unreal = sum(notional * ((close_on(p["sym"], t) or p["entry_px"]) / p["entry_px"] - 1.0) for p in open_pos)
        equity_curve.append((t, CAP + realized + unreal))
    # metrics
    eq = [v for _, v in equity_curve]
    peak = eq[0]; maxdd = 0.0
    for v in eq:
        peak = max(peak, v); maxdd = min(maxdd, v / peak - 1.0)
    yrs = (datetime.strptime(cal[-1], "%Y-%m-%d") - datetime.strptime(cal[0], "%Y-%m-%d")).days / 365.25
    cagr = (eq[-1] / eq[0]) ** (1 / yrs) - 1
    rets = [eq[i] / eq[i-1] - 1 for i in range(1, len(eq)) if eq[i-1] > 0]
    sharpe = (statistics.mean(rets) / (statistics.pstdev(rets) or 1e-9)) * (252 ** 0.5)
    return {"cagr": cagr, "maxdd": maxdd, "calmar": (cagr / abs(maxdd)) if maxdd else 0,
            "sharpe": sharpe, "ntr": ntr, "wr": wins / ntr if ntr else 0, "tryr": ntr / yrs,
            "endeq": eq[-1], "profit": eq[-1] - CAP, "year_pnl": year_pnl}

print(f"\n(3) PORTFOLIO WALK  (CAP=Rs{CAP/1e5:.0f}L, equal notional/slot, hold=20d, full 0.7% cost):", flush=True)
best = walk(20, 10)
print(f"    CAGR {best['cagr']*100:+.1f}%  maxDD {best['maxdd']*100:.1f}%  Calmar {best['calmar']:.2f}  Sharpe {best['sharpe']:.2f}", flush=True)
print(f"    profit +Rs{best['profit']/1e5:.2f}L on Rs{CAP/1e5:.0f}L  |  trades {best['ntr']} ({best['tryr']:.0f}/yr)  WR {best['wr']*100:.0f}%", flush=True)
print(f"    YEAR-WISE P&L (Rs): " + "  ".join(f"{y}:{v/1e3:+.0f}k" for y, v in sorted(best["year_pnl"].items())), flush=True)

# ============ (4) SLOT + hold sensitivity ============
print("\n(4) SENSITIVITY (CAGR / maxDD / Calmar / trades-yr):", flush=True)
for K in (5, 10, 15):
    for h in (20, 40):
        w = walk(h, K)
        print(f"    K={K:>2d} hold={h}d: CAGR {w['cagr']*100:+5.1f}%  DD {w['maxdd']*100:6.1f}%  Calmar {w['calmar']:4.2f}  {w['tryr']:.0f}tr/yr", flush=True)
print("\nREAD: additive only if overlap<~50% AND Calmar/CAGR clear the bar (~0.6 / beats ~12% Nifty on risk-adj). Then plateau + faithful-OOS.", flush=True)
