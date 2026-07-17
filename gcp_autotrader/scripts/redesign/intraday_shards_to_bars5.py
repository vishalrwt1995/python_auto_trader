"""Adapter: 5m hash-bucket shards -> the harness's bars5_{year}.pkl format
(dict[sym][day] -> [[ts_ist, o,h,l,c,v]]), restricted to the top-500 turnover
universe (union over each year), for 2022-2026. Mirrors intraday_baseline.universe_for_day
(BALANCED filters) so the baseline reads exactly the prod intraday universe. Memory-safe:
one pass over the shards per year (only that year's universe rows accumulate). Local, free,
no BQ. Writes to ICACHE = ~/.autotrader_backtest_cache/intraday_audit/."""
import os, pickle, glob, time

CACHE = os.path.expanduser("~/.autotrader_backtest_cache")
ICACHE = os.path.join(CACHE, "intraday_audit"); os.makedirs(ICACHE, exist_ok=True)
SHARDS = os.path.expanduser("~/.autotrader_grind_cache/intraday_5m")
TOP_N = 500
UNI_MIN_PRICE, UNI_MAX_ATR_PCT, UNI_MAX_GAP_RISK, UNI_MIN_BARS = 30.0, 0.09, 0.06, 90

print("loading s2_universe_stats ...", flush=True)
S = pickle.load(open(f"{CACHE}/s2_universe_stats.pkl", "rb"))

def universe_for_day(day):
    ranked = []
    for sym, days in S.items():
        st = days.get(day)
        if not st:
            continue
        tv = float(st.get("turnover_med_60d") or 0)
        if tv <= 0 or float(st.get("price_last") or 0) < UNI_MIN_PRICE:
            continue
        if float(st.get("atr_pct_14d") or 1.0) > UNI_MAX_ATR_PCT:
            continue
        if float(st.get("gap_risk_60d") or 1.0) > UNI_MAX_GAP_RISK:
            continue
        if int(st.get("bars_1d") or 0) < UNI_MIN_BARS:
            continue
        ranked.append((tv, sym))
    ranked.sort(reverse=True)
    return {s for _, s in ranked[:TOP_N]}

# all trading days per year (from the calendar in S)
alldays = set()
for sym in S:
    alldays.update(S[sym].keys())

def iter_rows():
    for f in sorted(glob.glob(f"{SHARDS}/bucket_*.pkl")):
        with open(f, "rb") as fh:
            while True:
                try:
                    batch = pickle.load(fh)
                except EOFError:
                    break
                yield from batch   # (sym, ts_ist, date, o,h,l,c,v)

for year in (2022, 2023, 2024, 2025, 2026):
    ystr = str(year); t0 = time.time()
    ydays = sorted(d for d in alldays if d.startswith(ystr))
    uni = set()
    for d in ydays:
        uni |= universe_for_day(d)
    print(f"{year}: {len(ydays)} days, top-500 union = {len(uni)} symbols; scanning shards ...", flush=True)
    out = {}   # sym -> day -> [[ts,o,h,l,c,v]]
    n = 0
    for (sym, ts, d, o, h, l, c, v) in iter_rows():
        if sym not in uni or d[:4] != ystr:
            continue
        out.setdefault(sym, {}).setdefault(d, []).append([ts, o, h, l, c, v])
        n += 1
    # sort each day's bars by ts (shards were unordered)
    for sym in out:
        for d in out[sym]:
            out[sym][d].sort(key=lambda b: b[0])
    fn = os.path.join(ICACHE, f"bars5_{year}.pkl")
    pickle.dump(out, open(fn, "wb"))
    print(f"  -> {fn} : {len(out)} syms, {n:,} bars, {os.path.getsize(fn)/1e6:.0f}MB, {time.time()-t0:.0f}s", flush=True)
print("DONE all years", flush=True)
