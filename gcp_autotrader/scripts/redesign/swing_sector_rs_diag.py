"""SWING sector-relative-strength diagnostic — the 4th RANGE-regime idea, mechanistically
distinct from the 3 that failed (MR=buy-weakness; off-high-filter=still whole-market RS;
momentum-at-lower-b200=still whole-market RS). Tests: does a stock LEADING ITS OWN SECTOR
(sector-relative RS), independent of whole-market breadth, predict a real forward edge in
RANGE regime -- capturing rotational/narrow-market leadership the whole-market-RS formula
is blind to? Reuses the 73,296 already-computed signals (swing_range_momentum_signals.pkl,
real R-multiples via the prod scoring pipeline) -- adds sector-relative-RS as a NEW feature
via cheap price-arithmetic (no re-scoring) and re-buckets. READ-ONLY, single-process."""
import os, json, pickle, statistics
from collections import defaultdict

CACHE = os.path.expanduser("~/.autotrader_backtest_cache")
MIN_BARS_SWING, MIN_PRICE_SWING, MAX_ATR_PCT_SWING, MAX_GAP_RISK_SWING, TOPN = 180, 30.0, 0.12, 0.06, 1000

print("loading signals + bars + sector map ...", flush=True)
signals = pickle.load(open(f"{CACHE}/swing_range_momentum_signals.pkl", "rb"))
raw = pickle.load(open(f"{CACHE}/swing_adj_bars_2015.pkl", "rb"))
sect_raw = json.load(open(f"{CACHE}/sector_map.json"))
SYM2SEC = {}
for _v in sect_raw.values():
    if isinstance(_v, dict) and _v.get("sym") and _v.get("sector"):
        SYM2SEC[str(_v["sym"]).strip().upper()] = _v["sector"]
print(f"  {len(signals):,} saved signals | {len(SYM2SEC):,} symbol->sector mappings", flush=True)

def _atr_series(o,h,l,c):
    n=len(c); out=[0.0]*n; trs=[0.0]*n
    for i in range(1,n): trs[i]=max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1]))
    if n<=14: return out
    atr=sum(trs[1:15])/14.0; out[14]=atr
    for i in range(15,n): atr=(atr*13+trs[i])/14.0; out[i]=atr
    return out
SYM = {}
for s, b in raw.items():
    if not b or len(b) < MIN_BARS_SWING: continue
    d=[x[0] for x in b]; o=[x[1] for x in b]; h=[x[2] for x in b]; l=[x[3] for x in b]; c=[x[4] for x in b]; v=[x[5] for x in b]
    SYM[s] = {"d": d, "o": o, "h": h, "l": l, "c": c, "v": v, "atr": _atr_series(o,h,l,c), "idx": {dt:i for i,dt in enumerate(d)}}
del raw

def _ret(c, j, n): return (c[j]/c[j-n]-1.0) if j >= n and c[j-n] > 0 else 0.0
def eligible(S, j):
    if j+1 < MIN_BARS_SWING: return False
    if S["c"][j] < MIN_PRICE_SWING: return False
    atr_pct = S["atr"][j]/S["c"][j] if S["c"][j] > 0 else 1.0
    if atr_pct > MAX_ATR_PCT_SWING: return False
    gaps = [abs(S["o"][i]/S["c"][i-1]-1.0) for i in range(max(1,j-59), j+1) if S["c"][i-1] > 0]
    if gaps and (sum(gaps)/len(gaps)) > MAX_GAP_RISK_SWING: return False
    return True

# only need eligible-universe reconstruction (turnover top-1000) on the signal dates actually used
sig_dates = sorted({s["sig_d"] for s in signals})
print(f"reconstructing eligible universe + sector-avg ret60 for {len(sig_dates)} signal dates ...", flush=True)
sector_avg_ret60 = {}   # (date, sector) -> mean ret60 among eligible universe that day
sym_ret60_on = {}       # (date, sym) -> ret60
for di, d in enumerate(sig_dates):
    if di % 400 == 0: print(f"  {d} ({di}/{len(sig_dates)})", flush=True)
    elig = []
    for sym, S in SYM.items():
        j = S["idx"].get(d)
        if j is None or not eligible(S, j): continue
        turn = S["c"][j] * S["v"][j]
        elig.append((sym, S, j, turn))
    if not elig: continue
    elig.sort(key=lambda x: -x[3]); elig = elig[:TOPN]
    by_sec = defaultdict(list)
    for sym, S, j, _ in elig:
        r60 = _ret(S["c"], j, 60) or _ret(S["c"], j, 20)
        sym_ret60_on[(d, sym)] = r60
        sec = SYM2SEC.get(sym)
        if sec: by_sec[sec].append(r60)
    for sec, rs in by_sec.items():
        sector_avg_ret60[(d, sec)] = statistics.mean(rs)

# tag each saved signal with sector-relative RS
tagged = []
missing_sector = 0
for sg in signals:
    sym, d = sg["sym"], sg["sig_d"]
    sec = SYM2SEC.get(sym)
    r60 = sym_ret60_on.get((d, sym))
    sec_avg = sector_avg_ret60.get((d, sec)) if sec else None
    if sec is None or r60 is None or sec_avg is None:
        missing_sector += 1
        continue
    tagged.append({**sg, "sector": sec, "sector_rel_rs": r60 - sec_avg})
print(f"\ntagged {len(tagged):,} signals with sector-relative RS ({missing_sector:,} skipped, no sector match)\n", flush=True)

def bucket(pool, lo, hi, lbl):
    a = [s for s in pool if lo <= s["sector_rel_rs"] < hi and s["sig_d"] <= "2022-12-31"]
    b = [s for s in pool if lo <= s["sector_rel_rs"] < hi and s["sig_d"] >= "2023-01-01"]
    if len(a) < 10 and len(b) < 10:
        print(f"  {lbl:14} n=IS{len(a):>4}/OOS{len(b):>4}  (thin)"); return
    def m(t):
        if not t: return "  n/a  "
        avgR = statistics.mean(x["R"] for x in t); wr = 100*sum(1 for x in t if x["net"]>0)/len(t)
        return f"avgR={avgR:+.3f} WR={wr:4.1f}%"
    print(f"  {lbl:14} IS(n={len(a):>4}): {m(a)}   |   OOS(n={len(b):>4}): {m(b)}", flush=True)

print("=== ALL regimes: avg R by sector-relative-RS bucket ===", flush=True)
for lo, hi, lbl in [(-1,-0.05,"<-5% (laggard)"), (-0.05,0,"-5%..0"), (0,0.05,"0..5% (leader)"),
                    (0.05,0.15,"5-15%"), (0.15,10,">=15% (top leader)")]:
    bucket(tagged, lo, hi, lbl)

print("\n=== RANGE regime + b200<70 ONLY (the currently-dark population -- the real test) ===", flush=True)
dark = [s for s in tagged if s["regime"] == "RANGE" and s["b200"] < 70]
print(f"  pool: {len(dark):,} signals", flush=True)
for lo, hi, lbl in [(-1,-0.05,"<-5% (laggard)"), (-0.05,0,"-5%..0"), (0,0.05,"0..5% (leader)"),
                    (0.05,0.15,"5-15%"), (0.15,10,">=15% (top leader)")]:
    bucket(dark, lo, hi, lbl)

print("\n=== same dark pool: sector-leader (>=0) vs sector-laggard (<0), simple split ===", flush=True)
bucket(dark, 0, 10, "leader (>=0%)")
bucket(dark, -10, 0, "laggard (<0%)")

print("\nRead: if sector-leader buckets are robustly positive BOTH halves in the dark pool ->", flush=True)
print("sector-relative RS is a genuine, previously-untested edge for RANGE. If flat/negative", flush=True)
print("like the other 3 attempts -> RANGE regime is not capturable with the signals tried so far.", flush=True)
