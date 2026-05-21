"""Deep System Analysis — comprehensive backtest using validated swing replica.

For the validated 10-week window (Apr 10 - May 21, 2026):
  1. Pull ALL swing scans from BQ (~5000 rows)
  2. For each qualified scan (status='qualified' in BQ), simulate the trade
  3. Compute baseline equity curve
  4. Multi-dimensional attribution:
       - Per regime
       - Per setup
       - Per time-of-day
       - Per VIX band
       - Per ADX band (stock strength)
       - Per direction
       - Per score bucket
       - Per holding-days
  5. Identify LOSING patterns: which combinations leak P&L?
  6. Identify WINNING patterns: what characteristics correlate with wins?
  7. Test 15+ hypothesis variants (disable, raise floor, add filters)
  8. Compare to actual live trades from BQ

Output: actionable insights ranked by P&L impact.

Run:
    python -m autotrader.backtest_v2.deep_analysis 2026-04-10 2026-05-21
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from autotrader.backtest_v2.brain_loader import BrainSnapshotLoader
from autotrader.backtest_v2.data import HistoricalDataset
from autotrader.backtest_v2.phase5_trade_sim import find_entry_idx, simulate_swing_trade
from autotrader.backtest_v2.prod_replica_v2 import ProdReplicaV2
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


def categorize_time(scan_ts: str) -> str:
    """Categorize scan_ts (UTC) into IST time band."""
    try:
        utc = datetime.fromisoformat(scan_ts.replace("Z", "+00:00"))
        ist_hour = (utc.astimezone().hour)
        # IST market: 09:15 - 15:30
        if 9 <= ist_hour < 10: return "MORNING_OPEN"
        if 10 <= ist_hour < 12: return "MORNING"
        if 12 <= ist_hour < 14: return "MIDDAY"
        if 14 <= ist_hour < 15: return "AFTERNOON"
        if 15 <= ist_hour < 16: return "CLOSE"
        return "OFF_HOURS"
    except Exception:
        return "UNKNOWN"


def vix_band(vix: float | None) -> str:
    if vix is None or vix <= 0: return "UNKNOWN"
    if vix < 14: return "LOW"
    if vix < 18: return "MEDIUM"
    if vix < 22: return "HIGH"
    return "VERY_HIGH"


def adx_band(adx: float | None) -> str:
    if adx is None or adx <= 0: return "UNKNOWN"
    if adx < 15: return "NO_TREND"
    if adx < 25: return "WEAK_TREND"
    if adx < 35: return "STRONG_TREND"
    return "VERY_STRONG"


def score_band(score: int) -> str:
    if score < 60: return "<60"
    if score < 65: return "60-64"
    if score < 70: return "65-69"
    if score < 75: return "70-74"
    if score < 80: return "75-79"
    return "80+"


@dataclass
class TradeOutcome:
    symbol: str
    setup: str
    scan_ts: str
    scan_date: str
    direction: str
    regime: str
    raw_score: int
    adjusted_score: int
    vix: float
    adx: float
    rsi: float
    time_band: str = ""
    # Trade outcome
    entry_price: float = 0.0
    exit_price: float = 0.0
    qty: int = 0
    exit_reason: str = ""
    holding_days: int = 0
    gross_pnl: float = 0.0
    net_pnl: float = 0.0
    r_realized: float = 0.0


def _report_bucket(label: str, trades: list[TradeOutcome]) -> str:
    if not trades:
        return f"  {label:30s} N=0"
    n = len(trades)
    wins = sum(1 for t in trades if t.net_pnl > 0)
    wr = wins / n * 100
    avg_r = sum(t.r_realized for t in trades) / n
    net = sum(t.net_pnl for t in trades)
    avg_pnl = net / n
    return f"  {label:30s} N={n:>4d} WR={wr:>5.1f}% AvgR={avg_r:>+5.2f} Net=₹{net:>+8.0f} Avg=₹{avg_pnl:>+5.0f}"


def main() -> int:
    start_date = sys.argv[1] if len(sys.argv) > 1 else "2026-04-10"
    end_date = sys.argv[2] if len(sys.argv) > 2 else "2026-05-21"
    print("=" * 90)
    print(f"Deep Analysis — Swing System ({start_date} → {end_date})")
    print("=" * 90)

    # 1. Pull all PRODUCTION-QUALIFIED swing scans
    print(f"\n[1] Pulling production-qualified swing scans from BQ...")
    qualified_rows = _bq_query(f"""
        SELECT scan_ts, run_date, symbol, setup, direction, raw_score, adjusted_score,
               regime, rsi, adx, vol_ratio, atr, ltp
        FROM `grow-profit-machine.autotrader.scan_decisions`
        WHERE wl_type='swing'
          AND qualified=true
          AND run_date >= '{start_date}' AND run_date <= '{end_date}'
        ORDER BY scan_ts
    """)
    print(f"    Got {len(qualified_rows)} qualified swing scans")
    if not qualified_rows:
        print("    ⚠️  No qualified swing trades in this window")
        print("    Pulling ALL swing scans (including blocked) to see what production rejected...")

    # 2. Also pull ALL swing scans to get total volume
    all_swing_rows = _bq_query(f"""
        SELECT scan_ts, run_date, symbol, setup, direction, raw_score, adjusted_score,
               regime, qualified, blocked_reason, rsi, adx
        FROM `grow-profit-machine.autotrader.scan_decisions`
        WHERE wl_type='swing'
          AND run_date >= '{start_date}' AND run_date <= '{end_date}'
        ORDER BY scan_ts
    """)
    print(f"    Total swing scans (all): {len(all_swing_rows)}")

    # Block reason histogram
    by_reason: dict[str, int] = defaultdict(int)
    by_setup_reason: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for r in all_swing_rows:
        br = r.get("blocked_reason") or ("QUALIFIED" if str(r.get("qualified","")).lower()=="true" else "EMPTY")
        by_reason[br] += 1
        by_setup_reason[r.get("setup","?")][br] += 1
    print(f"\n    Block-reason histogram (swing only):")
    for reason, n in sorted(by_reason.items(), key=lambda x: -x[1])[:15]:
        print(f"      {reason:55s} {n:>5d}")

    # 3. For each qualified scan, simulate the trade outcome
    print(f"\n[2] Simulating trade outcomes for {len(qualified_rows)} qualified scans...")
    cfg = StrategySettings()
    ds = HistoricalDataset()
    brain_loader = BrainSnapshotLoader()

    trades: list[TradeOutcome] = []
    seen_keys: set[tuple[str, str, str]] = set()  # (symbol, setup, date) — dedupe multi-scans/day
    for i, row in enumerate(qualified_rows):
        if i % 200 == 0 and i > 0:
            print(f"    ... {i}/{len(qualified_rows)}")
        scan_ts = row["scan_ts"]
        scan_date = (scan_ts or "")[:10]
        symbol = row["symbol"]; setup = row["setup"]; direction = row["direction"]
        key = (symbol, setup, scan_date)
        if key in seen_keys:
            continue  # Only take the FIRST qualified scan per day for this symbol/setup
        seen_keys.add(key)

        # Simulate the trade
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
        trade = simulate_swing_trade(
            symbol, entry_idx, daily_all, direction, cfg, float(db.atr_daily),
        )
        if trade.get("status") != "OK":
            continue

        out = TradeOutcome(
            symbol=symbol, setup=setup, scan_ts=scan_ts, scan_date=scan_date,
            direction=direction, regime=row.get("regime","UNKNOWN"),
            raw_score=int(row.get("raw_score") or 0),
            adjusted_score=int(row.get("adjusted_score") or 0),
            vix=0.0,  # populate from brain snapshot
            adx=float(row.get("adx") or 0),
            rsi=float(row.get("rsi") or 0),
            time_band=categorize_time(scan_ts),
            entry_price=trade["entry_price"], exit_price=trade["exit_price"],
            qty=trade["qty"], exit_reason=trade["exit_reason"],
            holding_days=trade["holding_days"],
            gross_pnl=trade["gross_pnl"], net_pnl=trade["net_pnl"],
            r_realized=trade["r_realized"],
        )
        # Pull VIX from brain snapshot
        try:
            snap = brain_loader.find_snapshot_before(scan_ts)
            if snap:
                out.vix = float(snap.raw_context.get("stressSnapshot", {}).get("vix") or 0)
        except Exception:
            pass

        trades.append(out)

    print(f"\n[3] Simulated {len(trades)} trades")

    if not trades:
        print("    ⚠️  No trades simulated")
        return 1

    # ===== Aggregate =====
    n = len(trades)
    total_net = sum(t.net_pnl for t in trades)
    total_wins = sum(1 for t in trades if t.net_pnl > 0)
    total_wr = total_wins / n * 100
    total_avg_r = sum(t.r_realized for t in trades) / n
    total_avg_pnl = total_net / n

    print(f"\n{'='*90}")
    print(f"OVERALL — Simulated swing P&L over {start_date} to {end_date}")
    print(f"{'='*90}")
    print(f"  Trades:        {n}")
    print(f"  Win rate:      {total_wr:.1f}%")
    print(f"  Average R:     {total_avg_r:+.3f}R")
    print(f"  Average P&L:   ₹{total_avg_pnl:+.0f}")
    print(f"  TOTAL P&L:     ₹{total_net:+,.0f}")

    # Compare to actual live trades
    print(f"\n  Compare to actual live BQ trades for same period:")
    try:
        live_trades = _bq_query(f"""
            SELECT setup, COUNT(*) n, ROUND(SUM(IFNULL(net_pnl,0)),0) net
            FROM `grow-profit-machine.autotrader.trades`
            WHERE trade_date >= '{start_date}' AND trade_date <= '{end_date}'
              AND strategy IN ('BREAKOUT', 'MEAN_REVERSION')
            GROUP BY setup
        """)
        for row in live_trades:
            print(f"    Live {row.get('setup','?'):16s}: N={row['n']}  Net=₹{row['net']}")
    except Exception as e:
        print(f"    (live trade fetch failed: {e})")

    # ===== Multi-dimensional attribution =====
    print(f"\n{'='*90}")
    print("ATTRIBUTION — per dimension")
    print("=" * 90)

    print("\n[ Per setup ]")
    by_setup: dict[str, list[TradeOutcome]] = defaultdict(list)
    for t in trades: by_setup[t.setup].append(t)
    for k, ts in sorted(by_setup.items(), key=lambda x: -sum(t.net_pnl for t in x[1])):
        print(_report_bucket(k, ts))

    print("\n[ Per regime ]")
    by_regime: dict[str, list[TradeOutcome]] = defaultdict(list)
    for t in trades: by_regime[t.regime].append(t)
    for k, ts in sorted(by_regime.items(), key=lambda x: -sum(t.net_pnl for t in x[1])):
        print(_report_bucket(k, ts))

    print("\n[ Per direction ]")
    by_dir: dict[str, list[TradeOutcome]] = defaultdict(list)
    for t in trades: by_dir[t.direction].append(t)
    for k, ts in sorted(by_dir.items(), key=lambda x: -sum(t.net_pnl for t in x[1])):
        print(_report_bucket(k, ts))

    print("\n[ Per setup × regime ]")
    by_sr: dict[str, list[TradeOutcome]] = defaultdict(list)
    for t in trades: by_sr[f"{t.setup}/{t.regime}"].append(t)
    for k, ts in sorted(by_sr.items(), key=lambda x: -sum(t.net_pnl for t in x[1])):
        print(_report_bucket(k, ts))

    print("\n[ Per time-of-day ]")
    by_t: dict[str, list[TradeOutcome]] = defaultdict(list)
    for t in trades: by_t[t.time_band].append(t)
    for k, ts in sorted(by_t.items(), key=lambda x: -sum(t.net_pnl for t in x[1])):
        print(_report_bucket(k, ts))

    print("\n[ Per VIX band ]")
    by_v: dict[str, list[TradeOutcome]] = defaultdict(list)
    for t in trades: by_v[vix_band(t.vix)].append(t)
    for k, ts in sorted(by_v.items(), key=lambda x: -sum(t.net_pnl for t in x[1])):
        print(_report_bucket(k, ts))

    print("\n[ Per ADX band (stock trend strength) ]")
    by_a: dict[str, list[TradeOutcome]] = defaultdict(list)
    for t in trades: by_a[adx_band(t.adx)].append(t)
    for k, ts in sorted(by_a.items(), key=lambda x: -sum(t.net_pnl for t in x[1])):
        print(_report_bucket(k, ts))

    print("\n[ Per raw_score band ]")
    by_s: dict[str, list[TradeOutcome]] = defaultdict(list)
    for t in trades: by_s[score_band(t.raw_score)].append(t)
    for k, ts in sorted(by_s.items(), key=lambda x: -sum(t.net_pnl for t in x[1])):
        print(_report_bucket(k, ts))

    print("\n[ Per exit reason ]")
    by_e: dict[str, list[TradeOutcome]] = defaultdict(list)
    for t in trades: by_e[t.exit_reason].append(t)
    for k, ts in sorted(by_e.items(), key=lambda x: -sum(t.net_pnl for t in x[1])):
        print(_report_bucket(k, ts))

    print("\n[ Per holding days ]")
    by_h: dict[str, list[TradeOutcome]] = defaultdict(list)
    for t in trades:
        if t.holding_days <= 1: b = "1"
        elif t.holding_days <= 3: b = "2-3"
        elif t.holding_days <= 5: b = "4-5"
        elif t.holding_days <= 10: b = "6-10"
        else: b = "11+"
        by_h[b].append(t)
    for b in ["1","2-3","4-5","6-10","11+"]:
        if b in by_h:
            print(_report_bucket(f"hold_{b}_days", by_h[b]))

    # ===== Identify the LEAK =====
    print(f"\n{'='*90}")
    print("LEAK ATTRIBUTION — combinations costing the most")
    print("=" * 90)
    # Drill down: setup × regime × direction
    combo: dict[str, list[TradeOutcome]] = defaultdict(list)
    for t in trades: combo[f"{t.setup}/{t.regime}/{t.direction}"].append(t)
    losses = [(k, ts) for k, ts in combo.items() if sum(t.net_pnl for t in ts) < 0]
    losses.sort(key=lambda x: sum(t.net_pnl for t in x[1]))
    print("\n[ Top losing combinations ]")
    for k, ts in losses[:10]:
        print(_report_bucket(k, ts))

    wins_combo = [(k, ts) for k, ts in combo.items() if sum(t.net_pnl for t in ts) > 0]
    wins_combo.sort(key=lambda x: -sum(t.net_pnl for t in x[1]))
    print("\n[ Top winning combinations ]")
    for k, ts in wins_combo[:10]:
        print(_report_bucket(k, ts))

    # ===== HYPOTHESIS TESTS =====
    print(f"\n{'='*90}")
    print("HYPOTHESES — what-if rule changes (P&L if applied)")
    print("=" * 90)

    def filter_pnl(filter_fn) -> tuple[int, float, float]:
        """Return (n, net_pnl, wr) for trades passing filter."""
        f = [t for t in trades if filter_fn(t)]
        if not f: return 0, 0.0, 0.0
        net = sum(t.net_pnl for t in f)
        wins = sum(1 for t in f if t.net_pnl > 0)
        return len(f), net, wins / len(f) * 100

    hypotheses = [
        ("BASELINE (current production)", lambda t: True),
        ("Drop BREAKOUT entirely", lambda t: t.setup != "BREAKOUT"),
        ("Drop MEAN_REVERSION entirely", lambda t: t.setup != "MEAN_REVERSION"),
        ("Drop all RANGE regime trades", lambda t: t.regime != "RANGE"),
        ("Drop all CHOP regime trades", lambda t: t.regime != "CHOP"),
        ("Drop BREAKOUT in RANGE", lambda t: not (t.setup == "BREAKOUT" and t.regime == "RANGE")),
        ("Drop BREAKOUT in TREND_DOWN", lambda t: not (t.setup == "BREAKOUT" and t.regime == "TREND_DOWN")),
        ("Drop SELL direction trades", lambda t: t.direction != "SELL"),
        ("Drop BUY direction trades", lambda t: t.direction != "BUY"),
        ("Score >= 65 (raise floor)", lambda t: t.raw_score >= 65),
        ("Score >= 70 (raise floor)", lambda t: t.raw_score >= 70),
        ("Score >= 75 (raise floor)", lambda t: t.raw_score >= 75),
        ("ADX >= 25 (strong trend stocks)", lambda t: t.adx >= 25),
        ("ADX >= 30", lambda t: t.adx >= 30),
        ("VIX < 18 (calmer markets)", lambda t: t.vix < 18 and t.vix > 0),
        ("VIX < 16", lambda t: t.vix < 16 and t.vix > 0),
        ("Morning only (9-12 IST)", lambda t: t.time_band in ("MORNING_OPEN","MORNING")),
        ("Afternoon only (12-15 IST)", lambda t: t.time_band in ("MIDDAY","AFTERNOON")),
        ("Drop holding 1-3 days (fast exits)", lambda t: t.holding_days > 3),
        ("Combo: drop BR+TREND_DOWN+SELL", lambda t: not (t.setup=="BREAKOUT" and t.regime=="TREND_DOWN" and t.direction=="SELL")),
    ]

    print(f"\n  {'Hypothesis':45s} {'N':>5s} {'Net P&L':>12s} {'WR':>6s} {'vs Base':>10s}")
    print("  " + "-" * 85)
    baseline_pnl = total_net
    for name, fn in hypotheses:
        n_h, pnl, wr = filter_pnl(fn)
        delta = pnl - baseline_pnl
        marker = " 📈" if delta > 1000 else (" 📉" if delta < -1000 else "")
        print(f"  {name:45s} {n_h:>5d} ₹{pnl:>+10,.0f} {wr:>5.1f}% ₹{delta:>+8,.0f}{marker}")

    # ===== Save details =====
    out_dir = Path.home() / ".autotrader_backtest_cache"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"deep_analysis_{start_date}_{end_date}.json"
    serializable = [{
        "symbol": t.symbol, "setup": t.setup, "scan_ts": t.scan_ts, "scan_date": t.scan_date,
        "direction": t.direction, "regime": t.regime, "raw_score": t.raw_score,
        "adjusted_score": t.adjusted_score, "vix": t.vix, "adx": t.adx, "rsi": t.rsi,
        "time_band": t.time_band, "entry_price": t.entry_price, "exit_price": t.exit_price,
        "exit_reason": t.exit_reason, "holding_days": t.holding_days,
        "gross_pnl": t.gross_pnl, "net_pnl": t.net_pnl, "r_realized": t.r_realized,
    } for t in trades]
    try:
        with open(out_path, "w") as fh:
            json.dump(serializable, fh, default=str)
        print(f"\n📦 {len(trades)} simulated trades saved to {out_path}")
    except Exception as e:
        print(f"\n⚠️ save failed: {e}")

    print(f"\n{'='*90}")
    print("✅ Deep analysis complete")
    print("=" * 90)
    return 0


if __name__ == "__main__":
    sys.exit(main())
