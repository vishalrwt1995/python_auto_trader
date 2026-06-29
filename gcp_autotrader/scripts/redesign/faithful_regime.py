"""Faithful historical regime timeline — runs the REAL prod `_build_state`/`_map_regime`
with REAL inputs, fixing the three poisons in backtest_v2/brain_reconstruct.py:

  1. VIX:        inject the real per-day India VIX (was stubbed to 15.0)
  2. Hysteresis: chain prev-day state into _map_regime (was read_latest→None)
  3. Taxonomy:   keep the deployed 6-label _map_regime (no core-4 fold / 999 thresholds)
  4. Index:      serve the Nifty-50 daily proxy from candles_indices BQ (deep history)

Inherent approximations (data does not exist historically — documented, not hidden):
  * leadership_score → prod uses 5m intraday; pre-recent has none → daily/neutral
  * sector map      → none in nse_equity_master → universe-only RS in ranking (later stage)
  * max-pain        → nse_fo_pcr has PCR but not max-pain → neutral in score_signal L2 (later)

Output: ~/.autotrader_backtest_cache/regime_faithful_2015.json   {date: {regime, scores...}}

Run (validation window first, then full):
  PYTHONPATH=src python3 scripts/redesign/faithful_regime.py validate   # vs real-logged rows
  PYTHONPATH=src python3 scripts/redesign/faithful_regime.py 2015-01-01 # full timeline
"""
from __future__ import annotations

import json
import sys
import time
from datetime import timedelta
from types import SimpleNamespace
from pathlib import Path

from google.cloud import bigquery

from autotrader.container import AppContainer, get_settings
from autotrader.time_utils import IST, now_ist, parse_any_ts
from autotrader.backtest_v2.brain_reconstruct import BQHistoricalGCS
from autotrader.domain.models import RegimeSnapshot, PcrSnapshot, FiiDiiSnapshot, NiftySnapshot

PROJECT = "grow-profit-machine"
CACHE = Path.home() / ".autotrader_backtest_cache"
MARKET = json.loads((CACHE / "market_inputs_2015.json").read_text())
OUT = CACHE / "regime_faithful_2015.json"

# Fold for FAIR validation only: prod's real-logged rows (2026-04+) use the OLD
# 9-label taxonomy; our recon uses the deployed 6-label. Fold both to a common
# base so the comparison measures SCORE fidelity, not the taxonomy change.
_FOLD = {"RANGE_ROTATING": "RANGE", "EARLY_TREND_UP": "TREND_UP", "EARLY_TREND_DOWN": "TREND_DOWN"}
def _fold(r: str) -> str:
    return _FOLD.get(str(r), str(r))


class IndexAwareGCS(BQHistoricalGCS):
    """BQHistoricalGCS + serve the Nifty-50 daily index proxy from candles_indices."""

    def __init__(self, real_gcs, bq, daily_from, daily_to=None):
        self._daily_to = daily_to
        self._index_daily: list[list[object]] = []
        self._index_keys: set[str] = set()
        super().__init__(real_gcs, bq, daily_from=daily_from)  # triggers _load_daily override

    def _load_daily(self, daily_from):
        """Deep daily bars from bt_bhavcopy_adj (1994+, survivorship-free, adjusted)
        instead of candles_daily (2022+ only) — so 2015-2021 breadth/leadership use real bars."""
        import time as _t
        t0 = _t.time()
        upper = f"AND date <= '{self._daily_to}'" if self._daily_to else ""
        q = (f"SELECT symbol, isin, CAST(date AS STRING) d, open, high, low, close, volume "
             f"FROM `{PROJECT}.autotrader.bt_bhavcopy_adj` "
             f"WHERE date >= '{daily_from}' {upper} AND series='EQ' AND isin IS NOT NULL "
             f"ORDER BY symbol, date")
        ik_by_sym, n = {}, 0
        for r in self._bq.query(q, location="asia-south1").result():
            self._daily.setdefault(r["symbol"], []).append(
                [f"{r['d']}T09:15:00+05:30", r["open"], r["high"], r["low"], r["close"], r["volume"]])
            if r["symbol"] not in ik_by_sym and r["isin"]:
                ik_by_sym[r["symbol"]] = f"NSE_EQ|{r['isin']}"
            n += 1
        for sym, ik in ik_by_sym.items():
            self._by_ik[self._safe_ik(ik)] = self._daily[sym]
        print(f"[bt_bhavcopy_adj] {n} bars / {len(self._daily)} syms from {daily_from}"
              + (f"..{self._daily_to}" if self._daily_to else "") + f" in {_t.time()-t0:.0f}s", flush=True)

    def load_index(self, index_symbol: str, safe_key: str) -> None:
        q = (f"SELECT CAST(trade_date AS STRING) d, "
             f"ARRAY_AGG(open ORDER BY candle_ts ASC LIMIT 1)[OFFSET(0)] o, MAX(high) h, MIN(low) l, "
             f"ARRAY_AGG(close ORDER BY candle_ts DESC LIMIT 1)[OFFSET(0)] c, SUM(volume) v "
             f"FROM `{PROJECT}.autotrader.candles_indices` WHERE symbol='{index_symbol}' "
             f"GROUP BY d ORDER BY d")
        self._index_daily = [[f"{r['d']}T09:15:00+05:30", r['o'], r['h'], r['l'], r['c'], r['v']]
                             for r in self._bq.query(q, location="asia-south1").result()]
        self._index_keys.add(safe_key)
        print(f"[index] loaded {len(self._index_daily)} {index_symbol} daily bars (key={safe_key})", flush=True)

    def read_candles(self, path: str):
        if "index_daily" in path and path.endswith(".json"):
            key = path.rsplit("/", 1)[-1][:-5]
            # primary Nifty-50 key → serve from BQ; other index proxies → empty (skipped)
            if key in self._index_keys:
                return [list(c) for c in self._index_daily]
            return []
        return super().read_candles(path)


def _setup(daily_from: str, daily_only: bool = True, daily_to: str | None = None):
    settings = get_settings()
    container = AppContainer(settings)
    brain = container.market_brain_service()
    us = container.universe_service()

    gcs = IndexAwareGCS(container.gcs, bigquery.Client(project=PROJECT), daily_from=daily_from, daily_to=daily_to)
    gcs.daily_only = daily_only  # False → serve 5m (faithful leadership); True → daily-only
    nifty_ik = str(us.upstox.settings.nifty50_instrument_key or "").strip()
    gcs.load_index("NIFTY 50", us._safe_key_fragment(nifty_ik))
    us.gcs = gcs
    brain.gcs = gcs

    # ── Stubs ────────────────────────────────────────────────────────────────
    def _empty(*a, **k):
        return []
    up = container.upstox
    for name in dir(up):
        if name.startswith("get_historical") or name.startswith("get_intraday"):
            try:
                setattr(up, name, _empty)
            except Exception:
                pass

    # data_quality: live pipeline-freshness → can't reconstruct → neutral 60
    brain._compute_data_quality = lambda *a, **k: (60.0, {"_reconstructed_neutral": True})

    # NO PROD WRITES: _build_state persists (BQ insert + pubsub publish) at line ~1502.
    # Stub it to a no-op so reconstruction never touches prod state.
    brain.persist_market_brain_state = lambda *a, **k: None
    brain.pubsub = None

    # FIX 1 — inject REAL VIX (+ neutral nifty_structure) keyed on the as-of date.
    brain._recon_date = None
    def _injected_regime(*a, **k):
        mi = MARKET.get(brain._recon_date or "", {})
        # Real per-day inputs into a fully-formed prod RegimeSnapshot (all other
        # fields keep their dataclass defaults — neutral). VIX is the load-bearing fix.
        return RegimeSnapshot(
            vix=float(mi.get("vix") or 15.0),
            pcr=PcrSnapshot(pcr=float(mi.get("pcr") or 1.0),
                            oi_change_pcr=float(mi.get("oi_change_pcr") or 1.0)),
            fii=FiiDiiSnapshot(fii=float(mi.get("fii") or 0.0)),
            nifty=NiftySnapshot(change_pct=float(mi.get("nifty_pct") or 0.0)),
        )
    brain.regime_service.get_market_regime = _injected_regime

    # FIX 2 — hysteresis: read_latest returns the chained prev-day state.
    _prev = {"state": None}
    brain.read_latest_market_brain_state = lambda *a, **k: _prev["state"]

    # universe: expected_lcd as-of the reconstruction date + force fresh
    us._expected_latest_daily_candle_date = (
        lambda now=None: us._prev_weekday((now or now_ist()).astimezone(IST).date() - timedelta(days=1))
    )
    # NOTE (2026-06-29): no sector-map wiring is needed. The candidate builder already
    # populates `sector` via sectorSource — the qualified liquid universe (the only rows
    # breadth uses) is 100% sector-covered. (`sectorCoveragePct` ≈ 2-4% is distinct-
    # sectors ÷ stocks, i.e. ~22 sectors over ~900 names — NOT a coverage gap.) A one-hour
    # v2 re-run that force-applied the Firestore map was byte-identical to v1, confirming it.
    _orig_cand = us._watchlist_v2_candidates
    def _fresh_cand(lcd, _orig=_orig_cand):
        rows = _orig(lcd)
        for r in rows:
            r["fresh"] = True
        return rows
    us._watchlist_v2_candidates = _fresh_cand

    # ── FAITHFUL SPEEDUP (profiled: 96% of the 138s/day) ────────────────────────
    # Hotspot: _daily_no_lookahead re-sorts + re-parses every symbol's full candle
    # history on EVERY day, in BOTH breadth (69%) and leadership (27%). The candles
    # and their IST dates are STATIC across days — only the cutoff moves forward.
    # Memoize each symbol's (sorted_unique candles, ascending date array) ONCE, then
    # do an O(log n) bisect slice per day. Output is BYTE-IDENTICAL to
    # _daily_no_lookahead → breadth/leadership/regime unchanged; this only stops
    # ~1.8M redundant timestamp re-parses per day. Disable with FR_NO_SPEEDUP=1.
    import os as _os, bisect as _bisect
    if not _os.environ.get("FR_NO_SPEEDUP"):
        _dno: dict[str, tuple[list, list]] = {}
        def _fast_daily_fetch(row, expected_lcd, _us=us):
            sym = str(row.get("symbol") or "")
            ent = _dno.get(sym)
            if ent is None:
                _, candles = _us._read_score_cache_with_migration(
                    sym, str(row.get("exchange") or "NSE"),
                    str(row.get("segment") or "CASH"), str(row.get("instrumentKey") or ""))
                cand, dates = [], []
                for c in _us._candles_sorted_unique(candles):
                    ts = parse_any_ts(c[0])
                    if ts is None:
                        continue
                    cand.append(c)
                    dates.append(ts.astimezone(IST).date())
                ent = (cand, dates)
                _dno[sym] = ent
            cand, dates = ent
            exp = _us._parse_iso_date(expected_lcd)
            if exp is None:
                return list(cand)
            return cand[: _bisect.bisect_right(dates, exp)]
        brain._daily_fetch = _fast_daily_fetch

    # FIX 3 — no core-4 fold, default 6-label thresholds (deployed _map_regime as-is).
    return container, brain, gcs, _prev


def _scores(st) -> dict:
    g = lambda k: float(getattr(st, k, 0.0) or 0.0)
    return {
        "regime": str(getattr(st, "regime", "")),
        "trend_score": round(g("trend_score"), 1),
        "tactical_trend_score": round(g("tactical_trend_score"), 1),
        "breadth_score": round(g("breadth_score"), 1),
        "leadership_score": round(g("leadership_score"), 1),
        "volatility_stress_score": round(g("volatility_stress_score"), 1),
        "risk_mode": str(getattr(st, "risk_mode", "")),
    }


def run_day(brain, _prev, d: str) -> dict:
    brain._recon_date = d
    st = brain._build_state(asof_ts=f"{d}T14:00:00+05:30", force_phase="POST_OPEN")
    _prev["state"] = st          # chain hysteresis to next day
    return _scores(st)


def main() -> int:
    argv = sys.argv[1:]
    if "--daemon" in argv:
        # Detach via double-fork so the ~10-min harness/process-group kill can't reap
        # this long (~75 min) run: the grandchild gets a new session (os.setsid) and is
        # reparented to launchd. stdout/err are already redirected to the logfile by the
        # caller. RESUME (above) makes any restart safe regardless.
        import os as _os
        argv = [a for a in argv if a != "--daemon"]
        if _os.fork() > 0:
            _os._exit(0)
        _os.setsid()
        if _os.fork() > 0:
            _os._exit(0)
    validate = "validate" in argv
    start = next((a for a in argv if a[:4].isdigit() and "-" in a), "2015-01-01")
    # T2: real 5m leadership for dates >= five_m_from.
    # Usage: python3 faithful_regime.py 2015-01-01 5mfrom=2022-01-03
    # Saves to regime_faithful_2015_5m.json (separate from daily-only baseline).
    _5marg = next((a for a in argv if a.startswith("5mfrom=")), None)
    five_m_from: str | None = _5marg.split("=", 1)[1] if _5marg else None

    if validate:
        # Small, fast check vs REAL-logged market_brain_history rows (2026-04+),
        # where prod ran live (real VIX). Confirms the machinery before the full run.
        bq = bigquery.Client(project=PROJECT)
        rows = list(bq.query(
            "SELECT CAST(run_date AS STRING) d, regime, ROUND(volatility_stress_score,1) vs "
            f"FROM `{PROJECT}.autotrader.market_brain_history` "
            "WHERE run_date >= '2026-04-10' AND EXTRACT(HOUR FROM TIMESTAMP(asof_ts)) BETWEEN 5 AND 8 "
            "QUALIFY ROW_NUMBER() OVER (PARTITION BY run_date ORDER BY asof_ts) = 1 "
            "ORDER BY run_date LIMIT 12", location="asia-south1").result())
        _, brain, _gcs, _prev = _setup("2026-01-01", daily_only=False)
        _gcs.load_5m([r["d"] for r in rows])  # serve faithful 5m leadership from candles_1m
        print(f"\n{'date':12} {'PROD(folded)':16} {'RECON(folded)':16} {'prodVS':7} {'reconVS':7} match")
        print("-" * 72)
        m = t = 0
        for r in rows:
            sc = run_day(brain, _prev, r["d"])
            ok = _fold(r["regime"]) == _fold(sc["regime"])
            m += ok; t += 1
            print(f"{r['d']:12} {_fold(r['regime']):16} {_fold(sc['regime']):16} "
                  f"{float(r['vs'] or 0):>6.1f} {sc['volatility_stress_score']:>7.1f}  {'OK' if ok else 'XX'}", flush=True)
        print(f"\nREGIME MATCH (real-logged window, folded): {m}/{t} = {100*m/t:.0f}%  (target ≥90%)" if t else "no rows")
        return 0

    # Full timeline from `start`. Bars loaded ~1.5yr earlier for SMA-200/ATR-252 lookback.
    from datetime import date as _dd, timedelta as _td
    nums = [a for a in argv if a.isdigit()]
    limit = int(nums[0]) if nums else None                       # test: only N output days
    date_args = [a for a in argv if a[:4].isdigit() and "-" in a]
    daily_to = date_args[1] if len(date_args) > 1 else None      # test: bound the bar load
    bars_from = (_dd.fromisoformat(start) - _td(days=550)).isoformat()
    bq = bigquery.Client(project=PROJECT)
    days = [r["d"] for r in bq.query(
        f"SELECT DISTINCT CAST(trade_date AS STRING) d FROM `{PROJECT}.autotrader.candles_indices` "
        f"WHERE symbol='NIFTY 50' AND trade_date >= '{start}'"
        + (f" AND trade_date <= '{daily_to}'" if daily_to else "")
        + " ORDER BY d", location="asia-south1").result()]
    if limit:
        days = days[:limit]
    print(f"[full] {len(days)} output days {days[0]}→{days[-1]}; bars from {bars_from}"
          + (f"..{daily_to}" if daily_to else ""), flush=True)
    out_path = CACHE / ("regime_faithful_2015_5m.json" if five_m_from else "regime_faithful_2015.json")
    _, brain, _gcs, _prev = _setup(bars_from, daily_only=True, daily_to=daily_to)
    import collections
    # RESUME (insurance for restart-after-kill): load prior OUT, skip done days, and
    # WARM the hysteresis chain over the WARMUP days before the first new day (no write)
    # so _map_regime's holds are rebuilt faithfully. A fresh run (no OUT) skips this and
    # runs fully continuous → exact hysteresis. WARMUP ≥ any _map_regime hold window.
    def _apply_5m(d: str) -> None:
        """Load one day's 5m from candles_1m if d >= five_m_from; else daily-only."""
        if five_m_from and d >= five_m_from:
            _gcs.daily_only = False
            _gcs._5m.clear()     # keep memory flat: one day at a time
            _gcs.load_5m([d])
        else:
            _gcs.daily_only = True

    out: dict[str, dict] = {}
    if out_path.exists():
        try:
            out = {d: v for d, v in json.loads(out_path.read_text()).items()
                   if str((v or {}).get("regime") or "") not in ("", "ERROR")}
        except Exception:
            out = {}
    resume_idx = next((i for i, d in enumerate(days) if d not in out), len(days))
    if resume_idx and out:
        WARMUP = 45
        w0 = max(0, resume_idx - WARMUP)
        print(f"[resume] {len(out)} done; warm hysteresis {days[w0]}→{days[resume_idx-1]}; "
              f"compute from {days[resume_idx] if resume_idx < len(days) else 'DONE'}", flush=True)
        for d in days[w0:resume_idx]:
            try:
                _apply_5m(d)
                run_day(brain, _prev, d)
            except Exception:
                _prev["state"] = None
    dist: collections.Counter = collections.Counter(
        v["regime"] for v in out.values() if v.get("regime"))
    t0 = time.time()
    for i in range(resume_idx, len(days)):
        d = days[i]
        if (i - resume_idx) % 5 == 0:
            print(f"  {i}/{len(days)} {d} ({time.time()-t0:.0f}s) dist={dict(dist)}", flush=True)
        try:
            _apply_5m(d)
            sc = run_day(brain, _prev, d)
        except Exception as e:
            sc = {"regime": "ERROR", "error": str(e)[:120]}
            _prev["state"] = None
        out[d] = sc
        dist[sc["regime"]] += 1
        if (i - resume_idx) % 50 == 0:
            out_path.write_text(json.dumps(out))
    out_path.write_text(json.dumps(out))
    print(f"\n[full] done {time.time()-t0:.0f}s → {out_path} | dist={dict(dist)} | total={len(out)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
