"""Swing Alpha Finder — simulate EVERY swing scan to find alpha pockets.

Production currently fires 6/6,368 swing scans (0.09%) and loses ₹856.
This module simulates the trade outcome of EVERY swing scan in BQ (whether
production qualified it or not), then aggregates by filter dimensions to
identify combinations with positive expectancy.

If a subset is profitable, we know production should be qualifying those
trades. We can then recommend specific gate changes.

Run:
    python -m autotrader.backtest_v2.swing_alpha_finder 2026-04-10 2026-05-21
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from autotrader.backtest_v2.brain_loader import BrainSnapshotLoader
from autotrader.backtest_v2.data import HistoricalDataset
from autotrader.backtest_v2.phase5_trade_sim import find_entry_idx, simulate_swing_trade
from autotrader.domain.daily_bias import compute_daily_bias
from autotrader.settings import StrategySettings


def _bq_query(query: str) -> list[dict]:
    gcloud = os.environ.get("GCLOUD", "gcloud")
    token = subprocess.check_output(
        [gcloud, "auth", "application-default", "print-access-token"],
        text=True,
    ).strip()
    body = json.dumps({"query": query, "useLegacySql": False, "location": "asia-south1"}).encode()
    req = urllib.request.Request(
        "https://bigquery.googleapis.com/bigquery/v2/projects/grow-profit-machine/queries",
        data=body,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=300) as resp:
        r = json.loads(resp.read())
    if "error" in r:
        raise RuntimeError(f"BQ error: {r['error']}")
    schema = r.get("schema", {}).get("fields", [])
    out = []
    for row in r.get("rows", []):
        d = {}
        for fld, cell in zip(schema, row["f"]):
            v = cell.get("v")
            if v is None:
                d[fld["name"]] = None
                continue
            if fld["type"] == "TIMESTAMP":
                try:
                    d[fld["name"]] = datetime.fromtimestamp(float(v), tz=timezone.utc).isoformat()
                except Exception:
                    d[fld["name"]] = v
            else:
                d[fld["name"]] = v
        out.append(d)
    return out


@dataclass
class SimResult:
    symbol: str
    setup: str
    direction: str
    scan_date: str
    regime: str
    raw_score: int
    adjusted_score: int
    blocked_reason: str
    rsi: float
    adx: float
    vol_ratio: float
    ema_state: str
    supertrend: str
    daily_trend: str
    # Outcome
    net_pnl: float = 0.0
    r_realized: float = 0.0
    exit_reason: str = ""
    holding_days: int = 0
    qty: int = 0


def report(label: str, results: list[SimResult]) -> str:
    if not results:
        return f"  {label:45s} N=0"
    n = len(results)
    wins = sum(1 for r in results if r.net_pnl > 0)
    wr = wins / n * 100
    avg_r = sum(r.r_realized for r in results) / n
    net = sum(r.net_pnl for r in results)
    return f"  {label:45s} N={n:>4d} WR={wr:>5.1f}% AvgR={avg_r:>+5.2f} Net=₹{net:>+9,.0f}"


def main() -> int:
    start_date = sys.argv[1] if len(sys.argv) > 1 else "2026-04-10"
    end_date = sys.argv[2] if len(sys.argv) > 2 else "2026-05-21"

    print("=" * 95)
    print(f"SWING ALPHA FINDER — simulate every swing scan ({start_date} → {end_date})")
    print("=" * 95)

    # Pull every swing scan with full indicator context.
    # Filter out AUTO/SKIP (production correctly blocks 100% of these) and
    # HOLD (no direction to trade). These would be junk in trade simulation.
    print(f"\n[1] Pulling all swing scans from BQ...")
    rows = _bq_query(f"""
        SELECT scan_ts, run_date, symbol, setup, direction, raw_score, adjusted_score,
               qualified, blocked_reason, regime, rsi, adx, vol_ratio, atr, ltp,
               ema_state, supertrend, daily_trend
        FROM `grow-profit-machine.autotrader.scan_decisions`
        WHERE wl_type='swing'
          AND run_date >= '{start_date}' AND run_date <= '{end_date}'
          AND direction NOT IN ('HOLD', 'SKIP')
          AND setup NOT IN ('AUTO', 'DEFAULT', '')
        ORDER BY scan_ts
    """)
    print(f"    Got {len(rows)} swing scans (excl. AUTO/SKIP/HOLD)")

    # Dedup by (symbol, setup, date) — first scan/day wins
    seen: set[tuple[str, str, str]] = set()
    deduped = []
    for r in rows:
        key = (r["symbol"], r["setup"], (r["scan_ts"] or "")[:10])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)
    print(f"    Deduplicated to {len(deduped)} unique (symbol, setup, date)")

    # Simulate every scan
    print(f"\n[2] Simulating trade outcomes...")
    cfg = StrategySettings()
    ds = HistoricalDataset()
    results: list[SimResult] = []

    for i, r in enumerate(deduped):
        if i % 500 == 0 and i > 0:
            print(f"    ... {i}/{len(deduped)}")
        scan_date = (r["scan_ts"] or "")[:10]
        symbol = r["symbol"]; setup = r["setup"]; direction = r["direction"]
        daily_all = ds.daily_candles(symbol)
        daily_truncated = [c for c in daily_all if str(c[0])[:10] <= scan_date]
        if len(daily_truncated) < 60:
            continue
        try:
            db = compute_daily_bias(daily_truncated)
        except Exception:
            continue
        if db is None or not db.atr_daily:
            continue
        entry_idx = find_entry_idx(daily_all, scan_date)
        if entry_idx is None or entry_idx >= len(daily_all):
            continue
        trade = simulate_swing_trade(symbol, entry_idx, daily_all, direction, cfg, float(db.atr_daily))
        if trade.get("status") != "OK":
            continue
        results.append(SimResult(
            symbol=symbol, setup=setup, direction=direction, scan_date=scan_date,
            regime=r.get("regime") or "UNKNOWN",
            raw_score=int(r.get("raw_score") or 0),
            adjusted_score=int(r.get("adjusted_score") or 0),
            blocked_reason=r.get("blocked_reason") or "",
            rsi=float(r.get("rsi") or 0),
            adx=float(r.get("adx") or 0),
            vol_ratio=float(r.get("vol_ratio") or 0),
            ema_state=r.get("ema_state") or "",
            supertrend=r.get("supertrend") or "",
            daily_trend=r.get("daily_trend") or "",
            net_pnl=trade["net_pnl"], r_realized=trade["r_realized"],
            exit_reason=trade["exit_reason"], holding_days=trade["holding_days"],
            qty=trade["qty"],
        ))

    print(f"\n[3] Simulated {len(results)} swing trades")

    # ===== Overall =====
    n = len(results)
    total_net = sum(r.net_pnl for r in results)
    wins = sum(1 for r in results if r.net_pnl > 0)
    wr = wins / n * 100 if n else 0
    avg_r = sum(r.r_realized for r in results) / n if n else 0

    print(f"\n{'='*95}")
    print(f"BASELINE — if every swing scan had qualified ({n} trades)")
    print(f"{'='*95}")
    print(f"  Trades: {n}  Wins: {wins} ({wr:.1f}%)  Avg R: {avg_r:+.3f}  Net P&L: ₹{total_net:+,.0f}")

    # ===== Per dimension =====
    def by(key_fn):
        d: dict[str, list[SimResult]] = defaultdict(list)
        for r in results: d[key_fn(r)].append(r)
        return d

    def sorted_by_net(d):
        return sorted(d.items(), key=lambda x: -sum(t.net_pnl for t in x[1]))

    print(f"\n[ Per setup ]")
    for k, ts in sorted_by_net(by(lambda r: r.setup)): print(report(k, ts))

    print(f"\n[ Per setup × regime ]")
    for k, ts in sorted_by_net(by(lambda r: f"{r.setup}/{r.regime}")):
        if len(ts) >= 5:  # min sample
            print(report(k, ts))

    print(f"\n[ Per direction ]")
    for k, ts in sorted_by_net(by(lambda r: r.direction)): print(report(k, ts))

    print(f"\n[ Per RSI band (intraday RSI from BQ) ]")
    def rsi_bucket(r):
        if r.rsi == 0: return "no_data"
        if r.rsi < 30: return "30-_oversold"
        if r.rsi < 45: return "30-44_low"
        if r.rsi < 55: return "45-54_neutral"
        if r.rsi < 70: return "55-69_high"
        return "70+_overbought"
    for k, ts in sorted_by_net(by(rsi_bucket)): print(report(k, ts))

    print(f"\n[ Per ADX band ]")
    def adx_bucket(r):
        if r.adx == 0: return "no_data"
        if r.adx < 15: return "0-14_no_trend"
        if r.adx < 25: return "15-24_weak"
        if r.adx < 35: return "25-34_strong"
        return "35+_very_strong"
    for k, ts in sorted_by_net(by(adx_bucket)): print(report(k, ts))

    print(f"\n[ Per vol_ratio band ]")
    def vol_bucket(r):
        if r.vol_ratio == 0: return "no_data"
        if r.vol_ratio < 0.8: return "0.0-0.8_below_avg"
        if r.vol_ratio < 1.2: return "0.8-1.2_normal"
        if r.vol_ratio < 1.8: return "1.2-1.8_elevated"
        if r.vol_ratio < 3.0: return "1.8-3.0_high"
        return "3.0+_extreme"
    for k, ts in sorted_by_net(by(vol_bucket)): print(report(k, ts))

    print(f"\n[ Per raw_score band ]")
    def score_bucket(r):
        if r.raw_score < 40: return "<40"
        if r.raw_score < 50: return "40-49"
        if r.raw_score < 60: return "50-59"
        if r.raw_score < 70: return "60-69"
        if r.raw_score < 80: return "70-79"
        return "80+"
    for k, ts in sorted_by_net(by(score_bucket)): print(report(k, ts))

    print(f"\n[ Per ema_state ]")
    for k, ts in sorted_by_net(by(lambda r: r.ema_state or "unknown")): print(report(k, ts))

    print(f"\n[ Per supertrend ]")
    for k, ts in sorted_by_net(by(lambda r: r.supertrend or "unknown")): print(report(k, ts))

    print(f"\n[ Per daily_trend ]")
    for k, ts in sorted_by_net(by(lambda r: r.daily_trend or "unknown")): print(report(k, ts))

    print(f"\n[ Per exit reason ]")
    for k, ts in sorted_by_net(by(lambda r: r.exit_reason)): print(report(k, ts))

    # ===== ALPHA POCKETS — combinations with strong positive edge =====
    print(f"\n{'='*95}")
    print("ALPHA POCKETS — combinations with positive expectancy + meaningful N")
    print("=" * 95)

    # All pairwise combos with N >= 10 and positive net
    pairs = defaultdict(list)
    for r in results:
        pairs[f"setup={r.setup} regime={r.regime}"].append(r)
        pairs[f"setup={r.setup} ema={r.ema_state}"].append(r)
        pairs[f"setup={r.setup} super={r.supertrend}"].append(r)
        pairs[f"setup={r.setup} daily={r.daily_trend}"].append(r)
        pairs[f"regime={r.regime} ema={r.ema_state}"].append(r)
        pairs[f"regime={r.regime} daily={r.daily_trend}"].append(r)
        pairs[f"setup={r.setup} dir={r.direction}"].append(r)

    profitable = [
        (k, ts) for k, ts in pairs.items()
        if len(ts) >= 10 and sum(t.net_pnl for t in ts) > 0
    ]
    profitable.sort(key=lambda x: -sum(t.net_pnl for t in x[1]))
    print(f"\n  Top profitable pairs (N>=10):")
    for k, ts in profitable[:20]:
        print(report(k, ts))

    # ===== Triple combos =====
    print(f"\n  Top profitable triples (N>=10):")
    triples = defaultdict(list)
    for r in results:
        triples[f"{r.setup}/{r.regime}/{r.daily_trend}"].append(r)
        triples[f"{r.setup}/{r.direction}/{r.daily_trend}"].append(r)
    prof_triples = [(k, ts) for k, ts in triples.items() if len(ts) >= 10 and sum(t.net_pnl for t in ts) > 0]
    prof_triples.sort(key=lambda x: -sum(t.net_pnl for t in x[1]))
    for k, ts in prof_triples[:15]:
        print(report(k, ts))

    # ===== LOSING POCKETS =====
    print(f"\n  Top LOSING pairs (N>=10, to identify what to disable):")
    losing = [(k, ts) for k, ts in pairs.items() if len(ts) >= 10 and sum(t.net_pnl for t in ts) < -500]
    losing.sort(key=lambda x: sum(t.net_pnl for t in x[1]))
    for k, ts in losing[:20]:
        print(report(k, ts))

    # ===== SMART RULES — test combined filters =====
    print(f"\n{'='*95}")
    print("SMART RULE HYPOTHESES — multi-filter combinations")
    print("=" * 95)

    def filter_report(name, fn):
        f = [r for r in results if fn(r)]
        return report(name, f)

    smart_rules = [
        ("BASELINE (all scans)",                                            lambda r: True),
        ("DROP MOMENTUM (it's bad)",                                        lambda r: r.setup != "MOMENTUM"),
        ("Setup=MR + regime=RANGE",                                          lambda r: r.setup=="MEAN_REVERSION" and r.regime=="RANGE"),
        ("Setup=MR + regime=RANGE/CHOP",                                     lambda r: r.setup=="MEAN_REVERSION" and r.regime in ("RANGE","CHOP")),
        ("Setup=BREAKOUT + daily_trend=UP + supertrend=UP",                  lambda r: r.setup=="BREAKOUT" and r.daily_trend=="UP" and r.supertrend=="UP"),
        ("BREAKOUT + BULL_STACK + supertrend=UP",                            lambda r: r.setup=="BREAKOUT" and r.ema_state=="BULL_STACK" and r.supertrend=="UP"),
        ("BREAKOUT + ADX>=25",                                                lambda r: r.setup=="BREAKOUT" and r.adx >= 25),
        ("BREAKOUT + vol_ratio>=1.5",                                         lambda r: r.setup=="BREAKOUT" and r.vol_ratio >= 1.5),
        ("BREAKOUT + ADX>=25 + vol_ratio>=1.5",                              lambda r: r.setup=="BREAKOUT" and r.adx >= 25 and r.vol_ratio >= 1.5),
        ("MR + RSI<=35 daily-OB-rejection",                                  lambda r: r.setup=="MEAN_REVERSION" and r.rsi <= 40 and r.direction=="BUY"),
        ("MR + RSI>=65 + direction=SELL",                                    lambda r: r.setup=="MEAN_REVERSION" and r.rsi >= 60 and r.direction=="SELL"),
        ("Only BULL_STACK + supertrend=UP + ADX>=25",                        lambda r: r.ema_state=="BULL_STACK" and r.supertrend=="UP" and r.adx >= 25),
        ("Drop BREAKOUT + Setup=MOMENTUM (keep MR only)",                    lambda r: r.setup == "MEAN_REVERSION"),
        ("Only setups in matched regime (curated)",                          lambda r: ((r.setup=="MEAN_REVERSION" and r.regime in ("RANGE","CHOP")) or (r.setup=="BREAKOUT" and r.regime=="TREND_UP" and r.supertrend=="UP" and r.ema_state=="BULL_STACK"))),
        ("Score>=50 (lower floor)",                                           lambda r: r.raw_score >= 50),
        ("Score>=55",                                                          lambda r: r.raw_score >= 55),
        ("Score>=60",                                                          lambda r: r.raw_score >= 60),
        ("vol_ratio>=2.0 (high volume)",                                      lambda r: r.vol_ratio >= 2.0),
        ("vol_ratio>=2.0 + ADX>=25",                                          lambda r: r.vol_ratio >= 2.0 and r.adx >= 25),
        ("BREAKOUT direction-matches daily",                                  lambda r: r.setup=="BREAKOUT" and ((r.direction=="BUY" and r.daily_trend=="UP") or (r.direction=="SELL" and r.daily_trend=="DOWN"))),
    ]

    print(f"\n  {'Hypothesis':56s} {'N':>5s} {'WR':>6s} {'AvgR':>7s} {'Net':>12s}")
    print("  " + "-" * 95)
    for name, fn in smart_rules:
        print(filter_report(name, fn))

    # ===== Save details =====
    out_dir = Path.home() / ".autotrader_backtest_cache"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"swing_alpha_{start_date}_{end_date}.json"
    serializable = [r.__dict__ for r in results]
    try:
        with open(out_path, "w") as fh:
            json.dump(serializable, fh, default=str)
        print(f"\n📦 {len(results)} simulated swing trades saved to {out_path}")
    except Exception as e:
        print(f"\n⚠️ save failed: {e}")

    print(f"\n{'='*95}")
    print("✅ Swing alpha finder complete")
    print("=" * 95)
    return 0


if __name__ == "__main__":
    sys.exit(main())
