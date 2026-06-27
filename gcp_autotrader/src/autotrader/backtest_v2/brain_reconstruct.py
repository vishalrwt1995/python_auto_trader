"""Phase 0a — historical market-brain (regime) reconstructor + validation.

Goal: prove we can faithfully reproduce production's REGIME for any historical
timestamp, so the multi-year backtest's regime gating is trustworthy.

Approach (ZERO reimplementation — run the real production code):
  * Construct the production services via AppContainer.
  * Stub the live Upstox candle fetches → force cache-only. Production's own
    point-in-time gates (_daily_no_lookahead, _completed_intraday_bars) then
    truncate the GCS-cached candles to the as-of timestamp, so there is no
    lookahead.
  * Stub the live regime fetch → VIX defaults to neutral 15.0 (smoke test).
    (PCR/FII/DII are computed AFTER _map_regime at line 1380, so they never
    affect the regime — verified.)
  * Call MarketBrainService._build_state(asof_ts, force_phase="POST_OPEN"),
    which returns a MarketBrainState WITHOUT persisting (no prod writes).

Validation: reconstruct at the exact asof_ts timestamps archived in
`market_brain_history` (Apr 2 - Jun 5 2026, the real production brain), then
compare reconstructed regime + scores to the archived row. Target: >=94%
regime match.

Run:
    GCP_PROJECT_ID=grow-profit-machine GCS_BUCKET=grow-profit-machine-autotrader-data \
    BQ_DATASET=autotrader FIRESTORE_DATABASE='(default)' GCP_REGION=asia-south1 \
    PYTHONPATH=src python3 -m autotrader.backtest_v2.brain_reconstruct [N_PER_REGIME]
"""
from __future__ import annotations

import collections
import re
import sys
import time
import traceback
from datetime import timedelta
from typing import Any

from google.cloud import bigquery

from autotrader.container import AppContainer, get_settings
from autotrader.time_utils import IST, now_ist, parse_any_ts

PROJECT = "grow-profit-machine"
TABLE = f"{PROJECT}.autotrader.market_brain_history"
DAILY_TABLE = f"{PROJECT}.autotrader.candles_daily"

# Stable core-4 regime: fold the recently-added (May-2026) refinement regimes
# into their long-standing base, so a 2022-2026 backtest uses the regime logic
# that was stable across the whole window.
CORE_MAP = {"RANGE_ROTATING": "RANGE"}


def core(regime: str) -> str:
    return CORE_MAP.get(str(regime), str(regime))


class BQHistoricalGCS:
    """Serve the bulk per-stock DAILY candle reads from the BQ warehouse,
    in-memory, instead of per-file cross-region GCS reads (the bottleneck).

    Composition wrapper: read_candles() intercepts per-stock daily paths and
    returns the symbol's full daily history from memory; everything else
    (index candles, 5m, non-candle reads) delegates to the real GCS store.
    Production's own _daily_no_lookahead() then truncates to as-of — so this
    stays point-in-time correct without us re-implementing truncation.
    """

    def __init__(self, real_gcs: Any, bq: bigquery.Client, daily_from: str = "2025-01-01"):
        self._real = real_gcs
        self._bq = bq
        self._daily: dict[str, list[list[Any]]] = {}     # by SYMBOL
        self._by_ik: dict[str, list[list[Any]]] = {}      # by sanitized instrument_key
        self._5m: dict[str, list[list[Any]]] = {}         # by SYMBOL (loaded per dates)
        self.daily_only = False                            # timeline mode: skip ALL 5m (fast)
        self._served = 0
        self._5m_served = 0
        self._miss: collections.Counter[str] = collections.Counter()
        self._load_daily(daily_from)

    def load_5m(self, dates: list[str]) -> None:
        """Bulk-load 5m (resampled 1m→5m) for specific trade_dates into memory,
        so leadership's per-symbol 5m reads are served from RAM, not GCS."""
        if not dates:
            return
        t0 = time.time()
        dl = ",".join(f"'{d}'" for d in sorted(set(dates)))
        q = (
            "SELECT symbol, FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S+05:30', "
            "TIMESTAMP_SECONDS(UNIX_SECONDS(candle_ts)-MOD(UNIX_SECONDS(candle_ts),300)),'Asia/Kolkata') b5, "
            "ARRAY_AGG(open ORDER BY candle_ts ASC LIMIT 1)[OFFSET(0)] o, MAX(high) h, MIN(low) l, "
            "ARRAY_AGG(close ORDER BY candle_ts DESC LIMIT 1)[OFFSET(0)] c, SUM(volume) v "
            f"FROM `{PROJECT}.autotrader.candles_1m` WHERE trade_date IN ({dl}) "
            "AND FORMAT_TIMESTAMP('%H:%M',candle_ts,'Asia/Kolkata') BETWEEN '09:15' AND '15:29' "
            "GROUP BY symbol, b5 ORDER BY symbol, b5"
        )
        n = 0
        for r in self._bq.query(q, location="asia-south1").result():
            self._5m.setdefault(r["symbol"], []).append([r["b5"], r["o"], r["h"], r["l"], r["c"], r["v"]])
            n += 1
        print(f"[BQHistoricalGCS] loaded {n} 5m bars for {len(self._5m)} symbols over {len(set(dates))} dates "
              f"in {time.time()-t0:.1f}s", flush=True)

    def __getattr__(self, name: str) -> Any:
        # Delegate everything not overridden (candle_cache_path, write_*, etc.).
        return getattr(self._real, name)

    @staticmethod
    def _safe_ik(instrument_key: str) -> str:
        # Mirror GoogleCloudStorageStore.score_cache_1d_path_by_instrument_key sanitization.
        raw = str(instrument_key or "").strip().upper()
        safe = re.sub(r"[^A-Z0-9._-]+", "_", raw)
        return re.sub(r"_+", "_", safe).strip("_")

    def _load_daily(self, daily_from: str) -> None:
        t0 = time.time()
        q = (f"SELECT symbol, instrument_key, CAST(trade_date AS STRING) d, open, high, low, close, volume "
             f"FROM `{DAILY_TABLE}` WHERE trade_date >= '{daily_from}' ORDER BY symbol, trade_date")
        n = 0
        ik_by_sym: dict[str, str] = {}
        for r in self._bq.query(q, location="asia-south1").result():
            bar = [f"{r['d']}T09:15:00+05:30", r["open"], r["high"], r["low"], r["close"], r["volume"]]
            self._daily.setdefault(r["symbol"], []).append(bar)
            if r["symbol"] not in ik_by_sym and r["instrument_key"]:
                ik_by_sym[r["symbol"]] = r["instrument_key"]
            n += 1
        for sym, ik in ik_by_sym.items():
            self._by_ik[self._safe_ik(ik)] = self._daily[sym]
        print(f"[BQHistoricalGCS] loaded {n} daily bars for {len(self._daily)} symbols "
              f"({len(self._by_ik)} by IK, from {daily_from}) in {time.time()-t0:.1f}s", flush=True)

    def _lookup(self, path: str) -> list[list[Any]] | None:
        if "NSE_INDEX" in path or "/INDEX/" in path:
            return None  # index → fall through to real GCS
        if not path.endswith(".json"):
            return None
        key = path.rsplit("/", 1)[-1][:-5]
        if "score_1d_by_instrument" in path:
            return self._by_ik.get(key)
        if "score_1d" in path or "/candles/1d/" in path:
            return self._daily.get(key)
        return None  # 5m/15m/other → fall through

    def read_candles(self, path: str) -> list[list[Any]]:
        if self.daily_only and "/candles/5m/" in path:
            return []  # timeline mode: regime from daily inputs only (fast, no 5m)
        if "/candles/5m/" in path and "NSE_INDEX" not in path and path.endswith(".json"):
            sym = path.rsplit("/", 1)[-1][:-5]
            hit5 = self._5m.get(sym)
            if hit5 is not None:
                self._5m_served += 1
                return [list(c) for c in hit5]
            # 5m not loaded for this symbol/date → fall through to real GCS
        is_stock_daily = ("score_1d" in path or "/candles/1d/" in path) and "NSE_INDEX" not in path
        if is_stock_daily:
            hit = self._lookup(path)
            if hit is not None:
                self._served += 1
                return [list(c) for c in hit]
            self._miss[path] += 1
        return self._real.read_candles(path)


def _stub_live_fetches(container: AppContainer, core4: bool = False) -> None:
    """Force cache-only + neutral VIX so reconstruction is point-in-time.

    core4=True → reconstruct the STABLE core-4 regime: dq=59 (disables the
    dq>=60-gated EARLY override) and trend_up_hi_breadth_min /
    range_rotating_breadth_min set to 999 (disables the May-2026 high-breadth-alt
    TREND_UP and RANGE_ROTATING paths). _map_regime still runs unmodified."""
    def _empty(*a: Any, **k: Any):
        return []

    up = container.upstox
    for name in dir(up):
        if name.startswith("get_historical") or name.startswith("get_intraday"):
            try:
                setattr(up, name, _empty)
            except Exception:
                pass

    brain = container.market_brain_service()
    # No live regime → _compute_volatility_stress falls back to VIX=15 (neutral).
    brain.regime_service.get_market_regime = lambda *a, **k: None  # type: ignore[assignment]
    # No prev state → no hysteresis short-circuit, deterministic recompute.
    brain.read_latest_market_brain_state = lambda *a, **k: None  # type: ignore[assignment]
    # data_quality depends on live pipeline-freshness signals (watchlist/scanner
    # last-run timestamps) that cannot be reconstructed historically → it comes
    # back low and falsely trips the `dq <= 30 → PANIC` gate. Neutralize to 60
    # (per brain-feasibility analysis) so the regime is decided by price+breadth+VIX.
    _dq_val = 59.0 if core4 else 60.0  # 59 (<60) disables the EARLY override
    brain._compute_data_quality = lambda *a, **k: (_dq_val, {"_reconstructed_neutral": True})  # type: ignore[assignment]
    if core4:
        import dataclasses
        try:
            brain.thresholds = dataclasses.replace(
                brain.thresholds, trend_up_hi_breadth_min=999.0, range_rotating_breadth_min=999.0)
        except Exception:
            for _k in ("trend_up_hi_breadth_min", "range_rotating_breadth_min"):
                try:
                    object.__setattr__(brain.thresholds, _k, 999.0)
                except Exception:
                    pass

    us = container.universe_service()
    # (1) expected_lcd must be AS-OF the reconstruction timestamp, not real "now".
    # The production resolver's primary path ignores its arg and uses now_ist();
    # use the arg-honoring fallback (prev trading day before the as-of date).
    us._expected_latest_daily_candle_date = (  # type: ignore[assignment]
        lambda now=None: us._prev_weekday((now or now_ist()).astimezone(IST).date() - timedelta(days=1))
    )
    # (2) 'fresh' gates on live-pipeline metadata recency — meaningless for
    # reconstruction (our BQ daily is complete to the as-of date). Force True so
    # breadth/leadership process the eligible+liquid universe with historical data.
    _orig_cand = us._watchlist_v2_candidates
    def _fresh_cand(lcd: str, _orig: Any = _orig_cand) -> Any:
        rows = _orig(lcd)
        for r in rows:
            r["fresh"] = True
        return rows
    us._watchlist_v2_candidates = _fresh_cand  # type: ignore[assignment]


def reconstruct_one(brain: Any, asof_ts: str) -> dict[str, Any]:
    """Run the real _build_state for a historical timestamp; return regime+scores."""
    st = brain._build_state(asof_ts=asof_ts, force_phase="POST_OPEN")
    return {
        "regime": str(getattr(st, "regime", "")),
        "trend_score": float(getattr(st, "trend_score", 0.0) or 0.0),
        "breadth_score": float(getattr(st, "breadth_score", 0.0) or 0.0),
        "leadership_score": float(getattr(st, "leadership_score", 0.0) or 0.0),
        "volatility_stress_score": float(getattr(st, "volatility_stress_score", 0.0) or 0.0),
        "data_quality_score": float(getattr(st, "data_quality_score", 0.0) or 0.0),
        "tactical_trend_score": float(getattr(st, "tactical_trend_score", 0.0) or 0.0),
    }


def fetch_validation_rows(n_per_regime: int, full: bool = False) -> list[dict[str, Any]]:
    client = bigquery.Client(project=PROJECT)
    # Mid-session snapshots only (UTC hour 5-8 ≈ IST 10:30-14:30) so the brain is
    # in steady POST_OPEN state with full intraday data — clean apples-to-apples.
    # full=True → 1 snapshot per trading day across the whole window (definitive).
    qualify = ("QUALIFY ROW_NUMBER() OVER (PARTITION BY run_date ORDER BY asof_ts) = 1"
               if full else
               f"QUALIFY ROW_NUMBER() OVER (PARTITION BY regime ORDER BY asof_ts DESC) <= {n_per_regime}")
    order = "ORDER BY run_date" if full else "ORDER BY regime, asof_ts"
    sql = f"""
    SELECT asof_ts, run_date, regime, trend_score, breadth_score,
           volatility_stress_score, data_quality_score, tactical_trend_score
    FROM `{TABLE}`
    WHERE EXTRACT(HOUR FROM TIMESTAMP(asof_ts)) BETWEEN 5 AND 8
      AND run_date < CURRENT_DATE()
    {qualify}
    {order}
    """
    out = []
    for r in client.query(sql, location="asia-south1").result():
        out.append({k: r[k] for k in r.keys()})
    return out


def main() -> int:
    argv = sys.argv[1:]
    diag_mode = "diag" in argv
    full_mode = "full" in argv
    core4_mode = "core4" in argv
    timeline_mode = "timeline" in argv
    n = next((int(a) for a in argv if a.isdigit()), 2)
    print(f"== Brain reconstruction (n={n}, full={full_mode}, core4={core4_mode}, diag={diag_mode}, timeline={timeline_mode}) ==", flush=True)
    settings = get_settings()
    container = AppContainer(settings)
    brain = container.market_brain_service()
    _stub_live_fetches(container, core4=core4_mode or timeline_mode)

    # Serve the bulk per-stock daily reads from BQ in-memory (kills the
    # cross-region GCS bottleneck); index/5m fall through to real GCS.
    # Timeline spans 2022-2026 → load daily from 2022 (validation modes only need 2025+).
    _daily_from = "2022-01-01" if timeline_mode else "2025-01-01"
    hist_gcs = BQHistoricalGCS(container.gcs, bigquery.Client(project=PROJECT), daily_from=_daily_from)
    container.universe_service().gcs = hist_gcs
    brain.gcs = hist_gcs

    if timeline_mode:
        # Build a daily faithful-regime timeline 2022-2026 (core-4, daily-only
        # inputs → fast, no 5m). Output: /tmp/regime_timeline.jsonl {date, regime}.
        hist_gcs.daily_only = True
        bqc = bigquery.Client(project=PROJECT)
        days = [r["d"] for r in bqc.query(
            f"SELECT DISTINCT CAST(trade_date AS STRING) d FROM `{PROJECT}.autotrader.candles_indices` "
            "WHERE symbol='NIFTY 50' AND trade_date >= '2022-01-01' ORDER BY d",
            location="asia-south1").result()]
        print(f"[timeline] {len(days)} trading days 2022→{days[-1] if days else '?'}", flush=True)
        import json as _json
        dist: collections.Counter[str] = collections.Counter()
        fh = open("/tmp/regime_timeline.jsonl", "w")
        t0 = time.time()
        for i, d in enumerate(days):
            if i % 50 == 0:
                print(f"  {i}/{len(days)} {d} ({time.time()-t0:.0f}s) dist={dict(dist)}", flush=True)
            try:
                st = brain._build_state(asof_ts=f"{d}T14:00:00+05:30", force_phase="POST_OPEN")
                reg = core(str(st.regime))
            except Exception:
                reg = "ERROR"
            dist[reg] += 1
            fh.write(_json.dumps({"date": d, "regime": reg}) + "\n")
            fh.flush()
        fh.close()
        print(f"\n[timeline] done {time.time()-t0:.0f}s → /tmp/regime_timeline.jsonl | dist={dict(dist)}", flush=True)
        return 0

    if diag_mode:
        # Diagnose leadership on high-breadth RANGE days that we mis-called TREND_UP:
        # print the sub-component breakdown to tell a fixable bug from inherent
        # boundary sensitivity (high breadth → high persistence → elevated leadership).
        us = container.universe_service()
        rows0 = us._watchlist_v2_candidates("2026-06-05")
        ls = brain.leadership_service
        print(f"[diag] leader_sample_size={getattr(ls,'leader_sample_size','?')} "
              f"min_daily_bars={getattr(ls,'min_daily_bars','?')} eligible_rows={sum(1 for r in rows0 if r.get('eligibleSwing') or r.get('eligibleIntraday'))}", flush=True)
        diag_dates = ["2026-04-30", "2026-05-04", "2026-05-14", "2026-05-15"]
        hist_gcs.load_5m(diag_dates)
        for d in diag_dates:
            ai = parse_any_ts(f"{d}T12:00:00+05:30")
            lcd = us._expected_latest_daily_candle_date(ai).strftime("%Y-%m-%d")
            bd = brain.compute_breadth_snapshot(expected_lcd=lcd, rows=rows0)
            ld = brain.compute_leadership_snapshot(expected_lcd=lcd, rows=rows0, now_i=ai)
            print(f"[diag] {d} lcd={lcd} breadth={bd.get('score'):.0f} LEADERSHIP={ld.get('score'):.0f} "
                  f"proc={ld.get('leadersProcessed')} | hold={ld.get('breakoutHoldRate')} "
                  f"fail={ld.get('failedBreakoutRate')} gap={ld.get('gapFollowThroughRate')} "
                  f"od={ld.get('openDriveContinuationRate')} cs={ld.get('closeStrengthLeaders')} "
                  f"pers={ld.get('leaderPersistenceRate')}", flush=True)
        return 0

    # Diagnostic: confirm the universe actually loads (empty → breadth/leadership
    # silently fall to neutral, which would mask the real regime).
    try:
        _probe = container.universe_service()._watchlist_v2_candidates("2026-06-05")
        _elig = sum(1 for r in _probe if r.get("eligibleSwing") or r.get("eligibleIntraday"))
        print(f"[probe] universe candidates: {len(_probe)} (eligible {_elig})", flush=True)
        # Decisive: does breadth actually read per-stock candles through our wrapper?
        _s0 = hist_gcs._served
        r0 = _probe[0] if _probe else {}
        _dc = container.universe_service()._watchlist_daily_candles(r0, "2026-05-09")
        print(f"[probe] _watchlist_daily_candles({r0.get('symbol')}): {len(_dc)} bars "
              f"| served Δ={hist_gcs._served - _s0} | sample path miss-keys={list(hist_gcs._miss)[:2]}", flush=True)
        c_en = sum(1 for r in _probe if r.get("enabled"))
        c_fresh = sum(1 for r in _probe if r.get("fresh"))
        c_elig = sum(1 for r in _probe if r.get("eligibleSwing") or r.get("eligibleIntraday"))
        c_liq = sum(1 for r in _probe if str(r.get("liquidityBucket") or "").upper() in {"A", "B"}
                    or (int(r.get("turnoverRank60D") or 0) > 0))
        print(f"[probe] filters: enabled={c_en} fresh={c_fresh} eligible={c_elig} liq={c_liq} (of {len(_probe)})", flush=True)
        print(f"[probe] sample row: "
              f"{ {k: r0.get(k) for k in ('symbol','enabled','fresh','eligibleSwing','eligibleIntraday','liquidityBucket','turnoverRank60D','last1DDate','bars1D')} }", flush=True)
        _bd = brain.compute_breadth_snapshot(expected_lcd="2026-05-09", rows=_probe)
        print(f"[probe] breadth@2026-05-09: score={_bd.get('score')} qualified={_bd.get('qualifiedCount')} "
              f"reason={_bd.get('reason')} | served now={hist_gcs._served}", flush=True)
    except Exception as _e:
        print(f"[probe] FAILED: {_e}", flush=True)
        traceback.print_exc()

    rows = fetch_validation_rows(n, full=full_mode)
    hist_gcs.load_5m([str(r["run_date"]) for r in rows])  # serve leadership 5m from RAM
    print(f"validation rows: {len(rows)}\n", flush=True)
    hdr = f"{'asof_ts':25} {'ACTUAL':18} {'RECON':18} {'match':5} | trend Δ breadth Δ stress Δ"
    print(hdr)
    print("-" * len(hdr))

    matched = 0
    total = 0
    for row in rows:
        # BQ returns asof_ts as a datetime; str() yields a space-separated form
        # that parse_any_ts rejects → _build_state would fall back to now_ist().
        # Use isoformat() (with 'T') so the as-of timestamp parses correctly.
        _a = row["asof_ts"]
        ts = _a.isoformat() if hasattr(_a, "isoformat") else str(_a)
        try:
            _t0 = time.time()
            rec = reconstruct_one(brain, ts)
            _dt = time.time() - _t0
        except Exception as e:
            print(f"{ts:25} ERROR: {e}")
            traceback.print_exc()
            continue
        total += 1
        # In core4 mode, compare on the stable core-4 regime (fold production's
        # recently-added refinement regimes into their base for a fair match).
        act_reg = core(str(row["regime"])) if core4_mode else str(row["regime"])
        rec_reg = core(rec["regime"]) if core4_mode else rec["regime"]
        ok = act_reg == rec_reg
        matched += 1 if ok else 0
        dtrend = rec["trend_score"] - float(row["trend_score"] or 0.0)
        dbreadth = rec["breadth_score"] - float(row["breadth_score"] or 0.0)
        dstress = rec["volatility_stress_score"] - float(row["volatility_stress_score"] or 0.0)
        at, ab, as_ = float(row["trend_score"] or 0), float(row["breadth_score"] or 0), float(row["volatility_stress_score"] or 0)
        print(f"{ts:19} | ACT {act_reg:16} t{at:.0f} b{ab:.0f} s{as_:.0f}"
              f"  | REC {rec_reg:16} t{rec['trend_score']:.0f} b{rec['breadth_score']:.0f} "
              f"l{rec['leadership_score']:.0f} s{rec['volatility_stress_score']:.0f} dq{rec['data_quality_score']:.0f}"
              f"  {'OK' if ok else 'XX'} ({_dt:.0f}s)", flush=True)

    served = getattr(hist_gcs, "_served", 0)
    miss = getattr(hist_gcs, "_miss", {})
    print(f"\n[data] daily served: {served} | 5m served: {getattr(hist_gcs, '_5m_served', 0)} "
          f"| misses: {sum(miss.values())} (distinct paths {len(miss)})")
    if total:
        print(f"REGIME MATCH: {matched}/{total} = {100*matched/total:.1f}%  (target >=94%)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
