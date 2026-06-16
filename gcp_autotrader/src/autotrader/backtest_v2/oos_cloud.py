"""Faithful 2010-2026 held-out OOS — built to run as a Cloud Run JOB (8 vCPU/32GB),
entirely off the local laptop.

Three phases:
  timeline : feed the deep pickle into the UNCHANGED prod brain -> core-4 regime per day
             (validated 97% vs the certified 2022-26 timeline). Parallelised across
             SUBPROCESS workers (each builds its own clients — avoids the grpc-after-fork
             deadlock; N x 360 MB pickle is fine on a 32 GB box).
  pool     : re-run the prod-replica swing scanner (swing_s2_shorts) over the deep data
             + deep timeline -> candidate pool. SINGLE process: its universe-stats dict
             is multi-GB, so N copies would OOM; one pass is safe and ~robust.
  walk     : drive the FROZEN deployed config (prod_replay_validate predicates + exit)
             over the deep pool -> NET per year. 2010-2021 = true held-out OOS.

All inputs/outputs live in gs://<bucket>/oos/. Reads run in-region (asia-south1) -> no
egress. NOTHING here touches a prod service.

SURVIVORSHIP CAVEAT: the deep universe is today's instrument keys projected back, so
pre-2021 over-represents survivors (upward bias) — read as directional, not exact.

Modes:
  smoke    : LOCAL sanity — 6 days via 2 subprocess workers, no GCS. Proves the mechanism.
  timeline : regimes for all 2010-2026 trading days -> GCS.
  pool     : candidate pool -> GCS (needs timeline first).
  walk     : frozen-config NET/year report -> GCS (needs pool first).
  all      : timeline -> pool -> walk.
  _tlchunk i n daysfile : INTERNAL subprocess worker (not called directly).
"""
from __future__ import annotations

import collections
import csv
import json
import os
import pickle
import subprocess
import sys
import time

PROJECT = os.environ.get("GCP_PROJECT_ID", "grow-profit-machine")
BUCKET = os.environ.get("GCS_BUCKET", "grow-profit-machine-autotrader-data")
OOS_PREFIX = "oos"
CACHE = os.path.expanduser("~/.autotrader_backtest_cache")
DEEP_PKL = os.path.join(CACHE, "candles_daily_deep.pkl")
IK_CSV = os.path.join(CACHE, "ik_symbol.csv")
TL_LOCAL = os.path.join(CACHE, "regime_timeline_deep.jsonl")
POOL_LOCAL = os.path.join(CACHE, "s2_shorts_trades_deep.json")
STATS_LOCAL = os.path.join(CACHE, "s2_universe_stats_deep.pkl")
DAYS_FILE = os.path.join(CACHE, "oos_days.json")


# ── GCS + BQ helpers ──────────────────────────────────────────────────────────
def _gcs():
    from google.cloud import storage
    return storage.Client(project=PROJECT).bucket(BUCKET)

def gcs_download(name: str, local: str) -> bool:
    blob = _gcs().blob(f"{OOS_PREFIX}/{name}")
    if not blob.exists():
        return False
    os.makedirs(os.path.dirname(local), exist_ok=True)
    blob.download_to_filename(local)
    print(f"[gcs] downloaded {name} ({os.path.getsize(local):,} B)", flush=True)
    return True

def gcs_upload(local: str, name: str) -> None:
    _gcs().blob(f"{OOS_PREFIX}/{name}").upload_from_filename(local)
    print(f"[gcs] uploaded {local} -> {OOS_PREFIX}/{name}", flush=True)

def ensure_deep_pickle() -> None:
    if not os.path.exists(DEEP_PKL) and not gcs_download("candles_daily_deep.pkl", DEEP_PKL):
        raise SystemExit("deep pickle not found locally or in GCS")

def ensure_ik_csv() -> None:
    if os.path.exists(IK_CSV):
        return
    from google.cloud import bigquery
    bq = bigquery.Client(project=PROJECT)
    sql = (f"SELECT DISTINCT symbol, instrument_key FROM `{PROJECT}.autotrader.candles_daily` "
           "WHERE instrument_key IS NOT NULL")
    os.makedirs(CACHE, exist_ok=True)
    with open(IK_CSV, "w", newline="") as fh:
        w = csv.writer(fh); w.writerow(["symbol", "instrument_key"])
        for r in bq.query(sql).result():
            w.writerow([r["symbol"], r["instrument_key"]])
    print(f"[ik] wrote {IK_CSV}", flush=True)

def ensure_sector_map() -> None:
    # swing_s2_shorts._load_sector_map() reads SECTOR_MAP_FILE and SILENTLY returns {}
    # if absent -> sector-diversification collapses the watchlist ~6x. Must ship it.
    import autotrader.backtest_v2.swing_s2_shorts as S
    if os.path.exists(S.SECTOR_MAP_FILE):
        return
    if not gcs_download("sector_mapping.json", S.SECTOR_MAP_FILE):
        raise SystemExit("FATAL: sector_mapping.json missing locally and in GCS — pool would collapse")

def trading_days() -> list[str]:
    from google.cloud import bigquery
    bq = bigquery.Client(project=PROJECT)
    sql = (f"SELECT DISTINCT CAST(trade_date AS STRING) d FROM `{PROJECT}.autotrader.candles_indices` "
           "WHERE symbol='NIFTY 50' AND trade_date >= '2010-01-01' ORDER BY d")
    return [r["d"] for r in bq.query(sql, location="asia-south1").result()]

def nproc() -> int:
    n = int(os.environ.get("OOS_NPROC", "0")) or (os.cpu_count() or 4)
    return max(1, min(8, n))


# ── brain (one per subprocess worker — fresh clients, no fork sharing) ──────────
def _ik_map() -> dict[str, str]:
    m = {}
    for row in csv.DictReader(open(IK_CSV)):
        if row.get("symbol") and row.get("instrument_key"):
            m[row["symbol"]] = row["instrument_key"]
    return m

def build_brain():
    from google.cloud import bigquery
    from autotrader.backtest_v2.brain_reconstruct import BQHistoricalGCS, _stub_live_fetches, PROJECT as BR_PROJECT
    from autotrader.container import AppContainer, get_settings

    class DeepGCS(BQHistoricalGCS):
        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            self._pcache = {}
        def _load_daily(self, daily_from):
            deep = pickle.load(open(DEEP_PKL, "rb")); ikb = _ik_map(); n = 0
            for sym, bars in deep.items():
                conv = [[f"{b[0]}T09:15:00+05:30", b[1], b[2], b[3], b[4], b[5]] for b in bars if str(b[0]) >= daily_from]
                if not conv: continue
                self._daily[sym] = conv
                ik = ikb.get(sym)
                if ik: self._by_ik[self._safe_ik(ik)] = conv
                n += len(conv)
            print(f"[DeepGCS] {n} bars / {len(self._daily)} symbols", flush=True)
        def read_candles(self, path):
            if path not in self._pcache:
                self._pcache[path] = super().read_candles(path)
            return self._pcache[path]

    settings = get_settings(); container = AppContainer(settings)
    brain = container.market_brain_service(); _stub_live_fetches(container, core4=True)
    brain.persist_market_brain_state = lambda *a, **k: None     # no BQ insert / pubsub per day
    hist = DeepGCS(container.gcs, bigquery.Client(project=BR_PROJECT), daily_from="2010-01-01")
    hist.daily_only = True
    container.universe_service().gcs = hist; brain.gcs = hist
    return brain

def regime_of(brain, d: str) -> str:
    from autotrader.backtest_v2.brain_reconstruct import core
    try:
        st = brain._build_state(asof_ts=f"{d}T14:00:00+05:30", force_phase="POST_OPEN")
        return core(str(st.regime))
    except Exception as e:
        return "ERROR:" + type(e).__name__


# ── Phase 1: regime timeline (subprocess-parallel) ──────────────────────────────
def tlchunk(ci: int, nc: int, days_file: str) -> None:
    """INTERNAL worker: regimes for the strided slice days[ci::nc] -> chunk file."""
    ensure_deep_pickle(); ensure_ik_csv()
    days = json.load(open(days_file))[ci::nc]
    brain = build_brain()
    t0 = time.time(); out = []
    for j, d in enumerate(days):
        out.append((d, regime_of(brain, d)))
        if j % 50 == 0: print(f"[w{ci}] {j}/{len(days)} {d} ({time.time()-t0:.0f}s)", flush=True)
    with open(os.path.join(CACHE, f"oos_tl_chunk_{ci}.jsonl"), "w") as fh:
        for d, r in out: fh.write(json.dumps({"date": d, "regime": r}) + "\n")
    print(f"[w{ci}] DONE {len(days)} days {time.time()-t0:.0f}s", flush=True)

def run_timeline(days: list[str], np_: int, *, upload: bool = True) -> dict[str, str]:
    os.makedirs(CACHE, exist_ok=True)
    ensure_deep_pickle(); ensure_ik_csv(); ensure_sector_map()   # download ONCE in the parent; workers then only read (no 8-way race)
    json.dump(days, open(DAYS_FILE, "w"))
    for f in os.listdir(CACHE):
        if f.startswith("oos_tl_chunk_"): os.remove(os.path.join(CACHE, f))
    print(f"[timeline] {len(days)} days across {np_} subprocess workers", flush=True)
    t0 = time.time()
    procs = [subprocess.Popen([sys.executable, "-m", "autotrader.backtest_v2.oos_cloud",
                               "_tlchunk", str(i), str(np_), DAYS_FILE], env=os.environ.copy())
             for i in range(np_)]
    fail = sum(1 for p in procs if p.wait() != 0)
    out = {}
    for f in os.listdir(CACHE):
        if f.startswith("oos_tl_chunk_"):
            for line in open(os.path.join(CACHE, f)):
                r = json.loads(line); out[r["date"]] = r["regime"]
    with open(TL_LOCAL, "w") as fh:
        for d in sorted(out): fh.write(json.dumps({"date": d, "regime": out[d]}) + "\n")
    dist = dict(collections.Counter(out.values()))
    print(f"[timeline] DONE {time.time()-t0:.0f}s merged={len(out)} fail={fail} dist={dist}", flush=True)
    if upload: gcs_upload(TL_LOCAL, "regime_timeline_deep.jsonl")
    return out


# ── Phase 2: candidate pool (single process — stats dict is multi-GB) ───────────
def run_pool(*, upload: bool = True) -> None:
    import autotrader.backtest_v2.swing_s2_shorts as S
    ensure_deep_pickle(); ensure_sector_map()
    if not os.path.exists(TL_LOCAL) and not gcs_download("regime_timeline_deep.jsonl", TL_LOCAL):
        raise SystemExit("timeline missing — run timeline phase first")
    S.CANDLES_PICKLE = DEEP_PKL; S.STATS_PICKLE = STATS_LOCAL
    S.REGIME_JSONL = TL_LOCAL; S.REGIME_ARTIFACT = TL_LOCAL
    t0 = time.time()
    candles = S.download_candles()
    regime_map = S.load_regime()
    cal = sorted(regime_map.keys())
    sym_dates = {sym: [b[0] for b in bars] for sym, bars in candles.items()}
    all_stats = S.precompute_universe_stats(candles)
    sector_map = S._load_sector_map()
    cfg = S.StrategySettings()
    print(f"[pool] {len(cal)} days (stats ready {time.time()-t0:.0f}s)", flush=True)
    all_trades = []
    for i, as_of in enumerate(cal):
        regime = regime_map[as_of]
        regime_daily = S._REGIME_DAILY.get(regime, "RANGE")
        eligible = S.universe_for_date_fast(all_stats, as_of)
        watchlist = S.score_watchlist_swing(candles, sym_dates, eligible, as_of, regime_daily, sector_map, core_regime=regime)
        all_trades.extend(S.run_entry_checks(candles, sym_dates, watchlist, as_of, regime, cfg))
        if i % 100 == 0:
            print(f"  [{as_of}] {i}/{len(cal)} total={len(all_trades):,} ({time.time()-t0:.0f}s)", flush=True)
    json.dump(all_trades, open(POOL_LOCAL, "w"), default=str)
    print(f"[pool] DONE {time.time()-t0:.0f}s {len(all_trades):,} trades", flush=True)
    if upload: gcs_upload(POOL_LOCAL, "s2_shorts_trades_deep.json")


# ── Phase 3: frozen-config walk -> NET per year ─────────────────────────────────
def run_walk(*, upload: bool = True) -> None:
    import autotrader.backtest_v2.exit_lab as EL
    import autotrader.backtest_v2.final_config as FC
    import autotrader.backtest_v2.prod_replay_validate as PRV
    ensure_deep_pickle()
    if not os.path.exists(POOL_LOCAL) and not gcs_download("s2_shorts_trades_deep.json", POOL_LOCAL):
        raise SystemExit("pool missing — run pool phase first")
    if not os.path.exists(TL_LOCAL): gcs_download("regime_timeline_deep.jsonl", TL_LOCAL)
    EL.CANDLES_PKL = DEEP_PKL; FC.CANDLES = DEEP_PKL; PRV.CANDLES = DEEP_PKL; PRV.POOL = POOL_LOCAL
    def _cal():
        reg = {}
        for line in open(TL_LOCAL):
            d = json.loads(line); reg[d["date"]] = d["regime"]
        c = sorted(reg.keys()); return c, {d: i for i, d in enumerate(c)}
    EL.load_calendar = _cal

    breadth, pos, lvls = FC.build_market()
    mean60 = PRV.build_arith_mean_ret60()
    resolved = EL.load_resolved(POOL_LOCAL)
    cal, idx_of = _cal()
    years = sorted({d[:4] for d in cal})
    caps = [100000, 200000, 300000, 500000]; capl = ["1L", "2L", "3L", "5L"]
    lines = [f"OOS walk: {cal[0]}..{cal[-1]} ({len(cal)} days), years {years[0]}..{years[-1]}"]
    for variant, vlabel in [("A", "prod-faithful RS"), ("B", "backtest-identical RS")]:
        pre = PRV.build_presim(resolved, breadth, pos, lvls, idx_of, mean60, variant)
        res = {cap: PRV.walk(pre, cal, idx_of, cap) for cap in caps}
        lines.append(f"\n{'='*70}\nVARIANT {variant} — {vlabel}\n{'='*70}")
        lines.append("  NET ₹  " + "".join(f"{c:>12}" for c in capl))
        for y in years:
            tag = "  <-- OOS" if y < "2022" else ""
            lines.append(f"  {y:6}" + "".join(f"{res[cap][0].get(y,[0,0,0])[2]:>12,.0f}" for cap in caps) + tag)
        oos = {cap: sum(res[cap][0].get(y,[0,0,0])[2] for y in years if y < "2022") for cap in caps}
        ins = {cap: sum(res[cap][0].get(y,[0,0,0])[2] for y in years if y >= "2022") for cap in caps}
        n_oos = sum(1 for y in years if y < "2022")
        lines.append("  OOSΣ  " + "".join(f"{oos[cap]:>12,.0f}" for cap in caps) + "  (held-out)")
        lines.append("  INSΣ  " + "".join(f"{ins[cap]:>12,.0f}" for cap in caps) + "  (in-sample)")
        if n_oos:
            lines.append("  OOS%/yr" + "".join(f"{100*oos[cap]/caps[i]/n_oos:>11.1f}%" for i, cap in enumerate(caps)))
        if variant == "A":
            lines.append("  per-cell NET (₹1L):")
            for cell, c in sorted(res[100000][1].items(), key=lambda kv: -kv[1][1]):
                lines.append(f"    {cell:26} n={c[0]:>5}  NET ₹{c[1]:>11,.0f}")
    report = "\n".join(lines)
    print(report, flush=True)
    rp = os.path.join(CACHE, "oos_walk_report.txt"); open(rp, "w").write(report)
    if upload: gcs_upload(rp, "oos_walk_report.txt")


# ── main ────────────────────────────────────────────────────────────────────────
def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else "smoke"
    if mode == "_tlchunk":
        tlchunk(int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]); return 0
    if mode == "smoke":
        days = ["2024-01-01", "2024-01-09", "2024-03-18", "2024-06-07", "2024-09-04", "2024-12-02"]
        out = run_timeline(days, 2, upload=False)
        ok = bool(out) and all(not v.startswith("ERROR") for v in out.values())
        print(f"SMOKE regimes: {out}\nSMOKE {'PASS' if ok else 'FAIL'}", flush=True)
        return 0 if ok else 1
    np_ = nproc()
    if mode in ("timeline", "all"): run_timeline(trading_days(), np_)
    if mode in ("pool", "all"): run_pool()
    if mode in ("walk", "all"): run_walk()
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
