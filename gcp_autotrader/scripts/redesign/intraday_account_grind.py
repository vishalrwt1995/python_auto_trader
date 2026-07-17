"""INTRADAY god-mode slice grind on the baseline entries (fast, no pipeline re-run).

The baseline report summed all 44,240 signals (grossR -0.38, cost 94%, every cell negative).
Two questions this answers from the saved entries (which carry full gross/net/cost/exit/time, but
NO score — so exact prod 3-slot ranking isn't reproducible):
  1) 3-slot PORTFOLIO scale (first-K/day chronological proxy) — the realistic # of trades + NET.
  2) Is there ANY positive-GROSS pocket? Slice by session-window, cell×window, direction, risk_mode,
     exit_reason. If every slice is gross-negative, no ranking/selection can rescue it (you can't
     rank a uniformly-negative pool into profit). If a pocket is positive-gross AND beats cost, that's
     the lead to pursue with a score-ranked re-run.
Read-only, local, instant. Uses by_capital['100000'] for NET@₹1L."""
import os, json
from collections import defaultdict
from statistics import mean

IC = os.path.expanduser("~/.autotrader_backtest_cache/intraday_audit")
E = []
for y in (2022, 2023, 2024, 2025, 2026):
    f = f"{IC}/baseline_entries_{y}.json"
    if os.path.exists(f):
        E.extend(json.load(open(f)))
print(f"total entries: {len(E):,}\n")

def net1(x): return x["by_capital"]["100000"]["net"]
def cost1(x): return x["by_capital"]["100000"]["cost"]
def gross1(x): return x["by_capital"]["100000"]["gross"]
def hourmin(ts):
    t = ts[11:16]; return int(t[:2]) * 60 + int(t[3:5])
def window(x):
    m = hourmin(x["entry_ts"])
    if m < 600: return "pre-10:00"
    if m < 660: return "10-11"
    if m < 720: return "11-12"
    if m < 780: return "12-13"
    return "13:00+"

def stats(items, lbl):
    if len(items) < 30:
        print(f"  {lbl:34} n={len(items):>5}  (thin)"); return
    n = len(items); wr = 100*sum(1 for x in items if net1(x) > 0)/n
    gR = mean(x["r_gross"] for x in items)
    g = sum(gross1(x) for x in items); nt = sum(net1(x) for x in items); c = sum(cost1(x) for x in items)
    cs = 100*c/(abs(g) or 1)
    flag = "  <== +GROSS" if gR > 0 else ""
    print(f"  {lbl:34} n={n:>5} WR{wr:>3.0f}% grossR={gR:>+6.3f} gross{g:>+9.0f} net{nt:>+9.0f} cost{cs:>4.0f}%{flag}")

# ── 1) 3-slot portfolio scale (first-K/day chronological proxy) ──
print("=== 3-slot portfolio estimate (first-K distinct-symbol entries/day, by time) ===")
for K in (3, 6):
    by_day = defaultdict(list)
    for x in E: by_day[x["day"]].append(x)
    taken = []
    for d, lst in by_day.items():
        lst.sort(key=lambda x: x["entry_ts"]); seen = set()
        for x in lst:
            if x["sym"] in seen: continue
            seen.add(x["sym"]); taken.append(x)
            if len(seen) >= K: break
    byyr = defaultdict(float)
    for x in taken: byyr[x["day"][:4]] += net1(x)
    tot = sum(net1(x) for x in taken)
    print(f"  K={K}/day: {len(taken):>5} trades, NET@₹1L total {tot:>+9.0f} | " +
          " ".join(f"{y}:{int(v/1000)}k" for y, v in sorted(byyr.items())))

# ── 2) positive-gross pocket hunt ──
print("\n=== exit-reason mix (where does the money go?) ===")
byreason = defaultdict(list)
for x in E: byreason[x["exit_reason"]].append(x)
for r, it in sorted(byreason.items(), key=lambda kv: -len(kv[1])):
    stats(it, f"exit={r}")

print("\n=== by session window (all cells) ===")
bywin = defaultdict(list)
for x in E: bywin[window(x)].append(x)
for w in ["pre-10:00", "10-11", "11-12", "12-13", "13:00+"]:
    stats(bywin.get(w, []), f"window {w}")

print("\n=== by cell × session window (hunt any +gross pocket) ===")
bycw = defaultdict(list)
for x in E: bycw[(x["setup"], x["regime"], window(x))].append(x)
for key in sorted(bycw, key=lambda k: -mean(z["r_gross"] for z in bycw[k])):
    it = bycw[key]
    if len(it) >= 50:
        stats(it, f"{key[0][:12]}×{key[1][:8]}×{key[2]}")

print("\n=== by direction × cell ===")
bydc = defaultdict(list)
for x in E: bydc[(x["direction"], x["setup"])].append(x)
for key in sorted(bydc, key=lambda k: -mean(z["r_gross"] for z in bydc[k])):
    stats(bydc[key], f"{key[0]} {key[1]}")

pos = [k for k in bycw if len(bycw[k]) >= 50 and mean(z["r_gross"] for z in bycw[k]) > 0]
print(f"\nVERDICT: positive-GROSS cell×window pockets (n>=50): {len(pos)}")
if pos:
    for k in pos: print("   ", k, round(mean(z['r_gross'] for z in bycw[k]), 3))
else:
    print("   NONE — every slice is gross-negative. Ranking/selection cannot rescue a uniformly")
    print("   negative pool; this is a no-edge signal set, not a tuning gap (mirrors swing-RANGE).")
