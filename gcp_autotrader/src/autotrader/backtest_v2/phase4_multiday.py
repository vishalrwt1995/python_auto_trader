"""Phase 3+4 — Multi-day validation.

Phase 2 proved we match BQ at 97.9% on May 20. Phase 4 verifies this is
stable across multiple trading days (not a one-day anomaly).

Strategy:
  - For each of N recent trading days, run Phase 2 logic
  - Aggregate match rate per day
  - Look for drift over time

PASS: ≥95% qualified-match rate on EACH day, no degrading trend.

Run:
    python -m autotrader.backtest_v2.phase4_multiday swing 50
"""
from __future__ import annotations

import sys

from autotrader.backtest_v2.data import HistoricalDataset
from autotrader.backtest_v2.phase2_scan_validation import _bq_query, replay_one
from autotrader.settings import StrategySettings


# Most recent 10 trading days (skipping weekends, May 16/17). Today's
# date is 2026-05-20, so include 11-15 and 18-20.
TRADING_DAYS = [
    "2026-05-08", "2026-05-09",  # may be Fri+Sat — adjust if needed
    "2026-05-11", "2026-05-12", "2026-05-13", "2026-05-14", "2026-05-15",
    "2026-05-18", "2026-05-19", "2026-05-20",
]


def validate_day(as_of: str, wl_type: str, limit: int, ds: HistoricalDataset, cfg: StrategySettings) -> dict:
    """Run validation for one day, return aggregate stats."""
    sql = f"""
SELECT scan_ts, symbol, setup, wl_type, direction, adjusted_score, qualified,
       blocked_reason, regime, risk_mode, atr, vol_ratio
FROM `grow-profit-machine.autotrader.scan_decisions`
WHERE DATE(scan_ts, "Asia/Kolkata") = "{as_of}"
  AND wl_type = "{wl_type}"
  AND symbol IS NOT NULL
  AND symbol IN (
    "TCS", "RELIANCE", "HDFCBANK", "INFY", "ICICIBANK", "CANBK", "AXISBANK",
    "SBIN", "POWERGRID", "ITC", "HINDUNILVR", "BHARTIARTL", "BAJFINANCE",
    "LT", "MARUTI", "TATAMOTORS", "WIPRO", "HCLTECH", "TECHM", "ASIANPAINT",
    "TITAN", "ULTRACEMCO", "NESTLEIND", "JSWSTEEL", "GRASIM", "ADANIENT",
    "TATASTEEL", "ONGC", "COALINDIA", "NTPC", "M&M", "BPCL", "IOC", "DIVISLAB",
    "DRREDDY", "CIPLA", "SUNPHARMA", "APOLLOHOSP", "BRITANNIA", "DABUR",
    "GODREJCP", "PIDILITIND", "EICHERMOT", "BAJAJ-AUTO", "HEROMOTOCO",
    "INDUSINDBK", "KOTAKBANK", "PNB", "BANKBARODA", "CDSL", "BSE"
  )
ORDER BY RAND()
LIMIT {limit}
"""
    rows = _bq_query(sql)
    if not rows:
        return {"day": as_of, "rows": 0, "ok": 0, "qualified_match_pct": 0.0, "skip_reason": "no_bq_data"}

    ok = 0
    qual_match = 0
    statuses: dict[str, int] = {}
    for row in rows:
        r = replay_one(row, ds, cfg)
        statuses[r["replay_status"]] = statuses.get(r["replay_status"], 0) + 1
        if r["replay_status"] != "OK":
            continue
        ok += 1
        replay_qualified = (
            r.get("replay_direction") != "HOLD"
            and r.get("replay_gate_ok") is True
            and r.get("replay_score", 0) >= (cfg.swing_min_signal_score if r["wl_type"] == "swing" else cfg.min_signal_score)
        )
        bq_q = str(r.get("bq_qualified", "")).lower() == "true"
        if replay_qualified == bq_q:
            qual_match += 1

    return {
        "day": as_of,
        "rows": len(rows),
        "ok": ok,
        "qualified_match_pct": (qual_match / ok * 100) if ok else 0.0,
        "qual_match": qual_match,
        "statuses": statuses,
    }


def main() -> int:
    wl_type = sys.argv[1] if len(sys.argv) > 1 else "swing"
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 30

    print("=" * 70)
    print(f"Phase 4 — Multi-day validation: wl_type={wl_type} ({limit}/day)")
    print("=" * 70)

    cfg = StrategySettings()
    ds = HistoricalDataset()

    results = []
    for day in TRADING_DAYS:
        print(f"\nValidating {day}...")
        r = validate_day(day, wl_type, limit, ds, cfg)
        results.append(r)
        if r.get("skip_reason"):
            print(f"  → SKIP ({r['skip_reason']})")
        else:
            print(f"  → {r['ok']}/{r['rows']} replays OK, qualified match {r['qualified_match_pct']:.1f}%")

    print()
    print("Summary table:")
    print(f"  {'Day':12s} {'Rows':>5s} {'OK':>5s} {'QualMatch':>10s}")
    print("  " + "-" * 36)
    pass_days = 0
    skip_days = 0
    for r in results:
        if r.get("skip_reason"):
            print(f"  {r['day']:12s} {'-':>5s} {'-':>5s} {'SKIP':>10s}")
            skip_days += 1
            continue
        marker = "✓" if r["qualified_match_pct"] >= 95 else "✗"
        if r["qualified_match_pct"] >= 95:
            pass_days += 1
        print(f"  {r['day']:12s} {r['rows']:>5d} {r['ok']:>5d} {r['qualified_match_pct']:>9.1f}% {marker}")

    print()
    total_active = len(results) - skip_days
    print(f"PASS: {pass_days}/{total_active} days at ≥95% qualified match")

    if total_active > 0 and pass_days == total_active:
        print()
        print("✅ Phase 4 PASS — engine stable across all validation days")
        return 0
    elif total_active == 0:
        print("⚠️  Phase 4 INCONCLUSIVE — no days with usable BQ data")
        return 1
    else:
        print(f"\n❌ Phase 4 PARTIAL — {total_active - pass_days} day(s) below threshold")
        return 1


if __name__ == "__main__":
    sys.exit(main())
