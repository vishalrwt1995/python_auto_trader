"""Phase D — Cross-validate backtest predictions vs live trades.

For each live closed trade (BigQuery `trades` table, last 60-90 days):
  1. Pull the original entry_ts, symbol, strategy, direction, signal_score, regime
  2. Run our backtest's simulate_swing_trade_with_mfe for that trade's setup/date
  3. Compare: predicted exit_reason vs actual, predicted pnl vs actual

Output: cross-validation table + accuracy stats.

Run:
    PYTHONPATH=src python -m autotrader.backtest_v2.phase_d_cross_validate
"""
from __future__ import annotations

import json
import sys
import urllib.request
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from autotrader.backtest_v2.brain_loader import BrainSnapshotLoader
from autotrader.backtest_v2.data import HistoricalDataset
from autotrader.backtest_v2.phase_a_comprehensive import simulate_swing_trade_with_mfe
from autotrader.backtest_v2.phase5_trade_sim import find_entry_idx
from autotrader.domain.daily_bias import compute_daily_bias
from autotrader.settings import StrategySettings

PROJECT = "grow-profit-machine"


def _get_token() -> str:
    return Path("/tmp/adc_access_token.txt").read_text().strip()


def _query_bq(sql: str) -> tuple[list[str], list[list]]:
    token = _get_token()
    url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/queries"
    body = json.dumps({"query": sql, "useLegacySql": False, "timeoutMs": 60000}).encode()
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json",
                 "x-goog-user-project": PROJECT},
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        j = json.loads(r.read().decode())
    fields = [f["name"] for f in j.get("schema", {}).get("fields", [])]
    rows = [[c.get("v", "") for c in row.get("f", [])] for row in j.get("rows", [])]
    return fields, rows


def main() -> int:
    print("=" * 80)
    print("PHASE D — Cross-validate backtest vs live trades")
    print("=" * 80)

    print("\n## Pulling closed swing trades from BQ (last 90 days)")
    sql = """
SELECT trade_date, position_tag, symbol, side, qty, entry_price, exit_price,
       sl_price, target, pnl, net_pnl, exit_reason, strategy, hold_minutes,
       regime, signal_score
FROM `grow-profit-machine.autotrader.trades`
WHERE trade_date >= DATE_SUB(CURRENT_DATE('Asia/Kolkata'), INTERVAL 90 DAY)
  AND exit_reason IS NOT NULL AND exit_reason != ''
  AND hold_minutes > 360  -- > 6 hours = likely swing (intraday closes EOD)
ORDER BY trade_date ASC
"""
    try:
        fields, rows = _query_bq(sql)
    except Exception as e:
        print(f"❌ BQ query failed: {e}")
        return 1

    if not rows:
        print("No swing trades in last 90 days. Check intraday-only filter.")
        # Fallback: pull all trades regardless of hold time
        sql2 = """
SELECT trade_date, position_tag, symbol, side, qty, entry_price, exit_price,
       sl_price, target, pnl, net_pnl, exit_reason, strategy, hold_minutes,
       regime, signal_score
FROM `grow-profit-machine.autotrader.trades`
WHERE trade_date >= DATE_SUB(CURRENT_DATE('Asia/Kolkata'), INTERVAL 90 DAY)
  AND exit_reason IS NOT NULL AND exit_reason != ''
  AND strategy IN ('BREAKOUT', 'PULLBACK', 'MOMENTUM', 'MEAN_REVERSION')
ORDER BY trade_date ASC
"""
        fields, rows = _query_bq(sql2)

    print(f"  Got {len(rows)} closed trades")
    if not rows:
        print("Still no trades. Aborting.")
        return 1

    # Setup backtest infrastructure
    cfg = StrategySettings()
    ds = HistoricalDataset()
    brain_loader = BrainSnapshotLoader()

    matches = 0
    mismatches = 0
    predicted_vs_actual_exits = defaultdict(int)
    sym_misses = []

    print(f"\n## Per-trade cross-validation")
    print(f"  {'date':12s} {'sym':12s} {'strat':14s} {'side':5s} {'actual':>10s} {'predicted':>10s} {'a_pnl':>8s} {'p_pnl':>8s}")

    for row in rows:
        d = dict(zip(fields, row))
        trade_date = d.get("trade_date", "")
        symbol = d.get("symbol", "")
        side = d.get("side", "BUY")
        strategy = d.get("strategy", "")
        actual_exit = d.get("exit_reason", "")
        actual_pnl = float(d.get("net_pnl", d.get("pnl", 0)) or 0)

        # Run backtest simulator
        try:
            daily_all = ds.daily_candles(symbol)
            entry_idx = find_entry_idx(daily_all, trade_date)
            if entry_idx is None or entry_idx >= len(daily_all):
                sym_misses.append((symbol, trade_date, "no_entry_idx"))
                continue
            daily_before = [c for c in daily_all if str(c[0])[:10] < trade_date][-120:]
            if len(daily_before) < 60:
                continue
            db = compute_daily_bias(daily_before)
            if db is None or not db.atr_daily:
                continue
            pred = simulate_swing_trade_with_mfe(
                symbol, entry_idx, daily_all, side, cfg, float(db.atr_daily or 0),
            )
            if pred.get("status") != "OK":
                continue
            pred_exit = pred.get("exit_reason", "")
            pred_pnl = pred.get("net_pnl", 0)
            # Normalize exit reason names
            actual_norm = actual_exit.replace("_HIT", "").replace("_CLOSE", "")
            pred_norm = pred_exit
            exit_match = (
                ("SL" in actual_norm and pred_norm == "SL") or
                ("TARGET" in actual_norm and pred_norm == "TARGET") or
                ("MAX_HOLD" in actual_norm and pred_norm == "MAX_HOLD") or
                ("EOD" in actual_norm and pred_norm in ("MAX_HOLD",))
            )
            predicted_vs_actual_exits[f"{actual_exit}→{pred_exit}"] += 1
            if exit_match:
                matches += 1
            else:
                mismatches += 1
            print(f"  {trade_date:12s} {symbol[:12]:12s} {strategy[:14]:14s} {side:5s} {actual_exit:>10s} {pred_exit:>10s} {actual_pnl:>+8.2f} {pred_pnl:>+8.2f}")
        except Exception as e:
            sym_misses.append((symbol, trade_date, str(e)[:30]))

    print(f"\n## Summary")
    print(f"  Matches: {matches}  Mismatches: {mismatches}  Total: {matches+mismatches}")
    if matches + mismatches > 0:
        print(f"  Exit reason accuracy: {matches/(matches+mismatches)*100:.1f}%")
    if sym_misses:
        print(f"  Skipped (data issues): {len(sym_misses)}")

    print(f"\n## Exit reason transitions (actual → predicted)")
    for k, v in sorted(predicted_vs_actual_exits.items(), key=lambda x: -x[1]):
        print(f"  {k:30s} {v:>4d}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
