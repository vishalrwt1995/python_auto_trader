"""Phase E (multi-year) — intraday setup-family net-expectancy screen on the
full 2022-2026 BigQuery warehouse.

Reuses the VALIDATED Phase-E logic (production check_strategy_entry +
simulate_exit + Upstox cost model from phase_e_morning_fade_probe /
phase_e_leverage_test) but feeds it 5m candles from `candles_5m_full`
(verified 1m->5m resample) instead of the 5.5-month GCS cache.

Goal: does any intraday setup have a net-of-cost edge that is ROBUST across
all four market cycles (2022 bear / 2023 recovery / 2024 bull / 2025
correction / 2026)?  Reports trades, win-rate, gross-R, net-R, net-total
per setup PER YEAR.

Run:
    GCP_PROJECT_ID=... GCS_BUCKET=... PYTHONPATH=src python3 -m \
        autotrader.backtest_v2.phase_e_multiyear [N_SYMBOLS]
"""
from __future__ import annotations

import os
import sys
import time
from collections import defaultdict
from typing import Any

import dataclasses
import json

from google.cloud import bigquery

from autotrader.settings import StrategySettings
from autotrader.domain.indicators import compute_indicators
from autotrader.domain.scoring import check_strategy_entry, _session_open_price
from autotrader.backtest_v2.phase_e_morning_fade_probe import (
    resample_5m_to_15m, ist_min, day_of, simulate_exit, WINDOW_LO, WINDOW_HI, MIN_WARMUP_15M_BARS)
from autotrader.backtest_v2.phase_e_leverage_test import evaluate, SL_MULT, RR

PROJECT = "grow-profit-machine"
# Rolling indicator window (15m bars). Production fetches the last ~80 bars; a
# 120-bar window is faithful to that AND keeps the multi-year replay O(n) per
# symbol instead of O(n^2) from recomputing over the full growing series.
WINDOW_BARS = 120
FIVE_M_TABLE = f"{PROJECT}.autotrader.candles_5m_full"
DAILY_TABLE = f"{PROJECT}.autotrader.candles_daily"


class BQIntradayDataset:
    """Minimal dataset adapter exposing the surface phase_e logic needs:
    .intraday_candles(symbol, timeframe='5m') -> [[ist_iso_ts,o,h,l,c,v], ...]
    served from candles_5m_full (IST-ISO timestamps so ist_min/day_of parse)."""

    def __init__(self, bq: bigquery.Client):
        self._bq = bq
        self._cache: dict[str, list[list[Any]]] = {}

    def intraday_candles(self, symbol: str, end_date: Any = None,
                         end_ts: Any = None, timeframe: str = "5m") -> list[list[Any]]:
        if timeframe != "5m":
            return []
        if symbol in self._cache:
            return self._cache[symbol]
        q = (
            "SELECT FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S+05:30', bar_ts, 'Asia/Kolkata') ts, "
            "open, high, low, close, volume "
            f"FROM `{FIVE_M_TABLE}` WHERE symbol=@s ORDER BY bar_ts"
        )
        job = self._bq.query(q, location="asia-south1", job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("s", "STRING", symbol)]))
        rows = [[r["ts"], r["open"], r["high"], r["low"], r["close"], r["volume"]] for r in job.result()]
        self._cache[symbol] = rows
        return rows


def liquid_universe(bq: bigquery.Client, n: int) -> list[str]:
    """Top-N symbols by avg daily turnover (close*volume) over the last 60 trading days."""
    q = (
        f"WITH recent AS (SELECT symbol, close*volume turnover FROM `{DAILY_TABLE}` "
        f"WHERE trade_date >= DATE_SUB((SELECT MAX(trade_date) FROM `{DAILY_TABLE}`), INTERVAL 90 DAY)) "
        f"SELECT symbol FROM recent GROUP BY symbol ORDER BY AVG(turnover) DESC LIMIT {int(n)}"
    )
    return [r["symbol"] for r in bq.query(q, location="asia-south1").result()]


def year_of(day: str) -> str:
    return str(day)[:4]


def collect_one_windowed(sym: str, all5: list, base_pop: float) -> list[dict]:
    """MORNING_FADE signals for one symbol — same production gates as the
    validated phase_e _collect_one, but indicators are computed on a rolling
    WINDOW_BARS window (faithful to production's ~80-bar fetch, O(n) not O(n^2))."""
    cfg = dataclasses.replace(StrategySettings(), risk_per_trade=250.0,
                              capital=200000.0, capital_intraday=100000.0)
    out: list[dict] = []
    all15 = resample_5m_to_15m(all5)
    five_by_day: dict[str, list] = defaultdict(list)
    for c in all5:
        five_by_day[day_of(c[0])].append(c)
    cand_by_day: dict[str, list] = defaultdict(list)
    for i, c in enumerate(all15):
        if WINDOW_LO <= ist_min(c[0]) <= WINDOW_HI:
            cand_by_day[day_of(c[0])].append(i)
    for day, idxs in cand_by_day.items():
        done = False
        for idx in idxs:
            if done:
                continue
            series = all15[max(0, idx + 1 - WINDOW_BARS): idx + 1]
            if len(series) < MIN_WARMUP_15M_BARS:
                continue
            ind = compute_indicators(series, cfg)
            if ind is None:
                continue
            passed, _ = check_strategy_entry("MORNING_FADE", "SELL", ind)
            if not passed:
                continue
            so = _session_open_price(ind)
            if so <= 0:
                continue
            pct_up = (ind.close - so) / so * 100.0
            if pct_up < base_pop:
                continue
            entry, atr = float(ind.close), float(ind.atr)
            if atr <= 0 or entry <= 0:
                continue
            sl_dist = max(atr * SL_MULT, entry * 0.005)
            bar_ts = all15[idx][0]
            bars_after = [c for c in five_by_day.get(day, []) if str(c[0]) > str(bar_ts)]
            ex_price, ex_reason, _ = simulate_exit(
                bars_after, entry, entry + sl_dist, entry - sl_dist * RR, atr, ist_min(bar_ts))
            out.append({"sym": sym, "day": day, "entry": entry, "exit": ex_price,
                        "sl_dist": sl_dist, "reason": ex_reason, "pct_up": pct_up})
            done = True
    return out


def collect_windowed(symbols: list[str], ds: Any, base_pop: float, ckpt: str) -> list[dict]:
    """Resumable per-symbol collection using the rolling-window MORNING_FADE replay."""
    done_path = ckpt + ".done"
    done: set[str] = set()
    if os.path.exists(done_path):
        with open(done_path) as fh:
            done = {l.strip() for l in fh if l.strip()}
    todo = [s for s in symbols if s not in done]
    print(f"resume: {len(done)} done, {len(todo)} to go", flush=True)
    fsig, fdone = open(ckpt, "a"), open(done_path, "a")
    for si, sym in enumerate(todo):
        if si % 10 == 0:
            print(f"  {si}/{len(todo)} ({sym})", flush=True)
        try:
            all5 = ds.intraday_candles(sym, timeframe="5m") or []
        except Exception:
            all5 = []
        if all5:
            for s in collect_one_windowed(sym, all5, base_pop):
                fsig.write(json.dumps(s) + "\n")
            fsig.flush()
        fdone.write(sym + "\n")
        fdone.flush()
    fsig.close()
    fdone.close()
    sigs: list[dict] = []
    with open(ckpt) as fh:
        for line in fh:
            try:
                sigs.append(json.loads(line))
            except Exception:
                pass
    return sigs


def main() -> int:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 150
    pop = float(os.environ.get("MF_POP", "1.5"))
    bq = bigquery.Client(project=PROJECT)
    print(f"== Phase E multi-year MORNING_FADE screen (top {n} liquid, pop>={pop}%) ==", flush=True)

    universe = liquid_universe(bq, n)
    print(f"universe: {len(universe)} liquid symbols (e.g. {universe[:6]})", flush=True)

    ds = BQIntradayDataset(bq)
    ckpt = f"/tmp/mf_multiyear_{n}.jsonl"
    # Rolling-window MORNING_FADE replay (production gates), resumable via ckpt.
    t0 = time.time()
    sigs = collect_windowed(universe, ds, pop, ckpt)
    print(f"\ncollected {len(sigs)} MORNING_FADE signals in {time.time()-t0:.0f}s", flush=True)

    # Per-year net expectancy at the production intraday config (₹15k cap proxy
    # via notional, 10bp slippage = production paper SL slippage) + a leveraged
    # variant (₹100k notional, pop>=3.5, risk-cap ₹2k) for comparison.
    by_year: dict[str, list] = defaultdict(list)
    for s in sigs:
        by_year[year_of(s["day"])].append(s)

    def show(label: str, notional: int, slip: int, pop_t: float, cap):
        print(f"\n{'='*92}\n{label}\n{'='*92}")
        print(f"{'year':6} {'trades':>7} {'wr%':>6} {'grossR':>8} {'netR':>8} {'net/tr₹':>9} {'netTot₹':>11}")
        print("-" * 92)
        for yr in sorted(by_year):
            r = evaluate(by_year[yr], notional, slip, pop_thresh=pop_t, risk_cap=cap)
            if not r:
                print(f"{yr:6} {'(no trades)':>7}")
                continue
            print(f"{yr:6} {r['n']:>7} {r['wr']:>6.1f} {r['avg_gross_r']:>+8.3f} {r['avg_net_r']:>+8.3f} "
                  f"{r['net_total']/r['n']:>+9.1f} {r['net_total']:>+11,.0f}")
        allr = evaluate(sigs, notional, slip, pop_thresh=pop_t, risk_cap=cap)
        if allr:
            print("-" * 92)
            print(f"{'ALL':6} {allr['n']:>7} {allr['wr']:>6.1f} {allr['avg_gross_r']:>+8.3f} {allr['avg_net_r']:>+8.3f} "
                  f"{allr['net_total']/allr['n']:>+9.1f} {allr['net_total']:>+11,.0f}")

    show("A) Small/unleveraged (₹15k notional, 10bp slip, pop>=1.5) — production-like", 15000, 10, pop, None)
    show("B) Leveraged+selective (₹100k notional, 10bp slip, pop>=3.5, risk-cap ₹2k)", 100000, 10, 3.5, 2000)
    print("\nnetR > 0 across ALL years = robust edge. Single-year edges = likely noise.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
