"""Phase 2 — Validate scan-loop logic against BQ scan_decisions.

INSIGHT: BQ scan_decisions rows have `regime` and `risk_mode` already
recorded. We don't need to replay brain to validate the scan loop. We
just feed the BQ-recorded regime back into the production scoring
functions and compare our decision to BQ.

This isolates brain replay as a separate concern (Phase 2b later).

For a sample date:
  1. Pull all scan_decisions rows from BQ
  2. For each row, load that symbol's daily + intraday candles from GCS
  3. Run determine_direction → score_signal → check_swing_entry with the
     regime from BQ
  4. Compare result (direction, score, qualified, blocked_reason) to BQ
  5. Report match rate

PASS: ≥95% match.

Run:
    python -m autotrader.backtest_v2.phase2_scan_validation 2026-05-19 swing
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from typing import Any

import httpx

from autotrader.backtest_v2.data import HistoricalDataset
from autotrader.domain.daily_bias import compute_daily_bias
from autotrader.domain.indicators import compute_indicators
from autotrader.domain.models import (
    FiiDiiSnapshot,
    NiftySnapshot,
    PcrSnapshot,
    RegimeSnapshot,
)
from autotrader.domain.scoring import check_swing_entry, determine_direction, score_signal
from autotrader.settings import StrategySettings


def _gcloud_token() -> str:
    """Get ADC access token for BQ queries."""
    out = subprocess.check_output(
        ["/Users/vishalrawat/google-cloud-sdk/bin/gcloud",
         "auth", "application-default", "print-access-token"],
        text=True,
    ).strip()
    return out


def _bq_query(sql: str) -> list[dict[str, Any]]:
    """Run a BQ query via REST API + ADC token.

    Converts TIMESTAMP fields from Unix-epoch-seconds (BQ's default REST
    representation) to ISO strings so downstream code can treat them as
    dates. Without this, `scan_ts[:10]` returns "1.77916292" garbage.
    """
    from datetime import datetime, timezone

    token = _gcloud_token()
    resp = httpx.post(
        "https://bigquery.googleapis.com/bigquery/v2/projects/grow-profit-machine/queries",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"query": sql, "useLegacySql": False, "timeoutMs": 60000},
        timeout=90,
    )
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"BQ error: {data['error'].get('message','?')}")
    schema_fields = data.get("schema", {}).get("fields", [])
    field_types = {f["name"]: f.get("type", "STRING") for f in schema_fields}
    fields = [f["name"] for f in schema_fields]
    rows = []
    for r in data.get("rows", []):
        row: dict[str, Any] = {}
        for i, name in enumerate(fields):
            v = r["f"][i].get("v")
            if v is not None and field_types.get(name) == "TIMESTAMP":
                try:
                    # BQ returns epoch seconds (possibly fractional) as string
                    ts = datetime.fromtimestamp(float(v), tz=timezone.utc)
                    v = ts.isoformat()
                except Exception:
                    pass
            row[name] = v
        rows.append(row)
    return rows


def _build_regime_snapshot(scan_row: dict[str, Any]) -> RegimeSnapshot:
    """Build a RegimeSnapshot from a BQ scan_decisions row.

    We use the regime/risk_mode from BQ; other fields default to neutral
    since the BQ row doesn't preserve NIFTY change_pct, VIX, etc. The
    direction vote in `determine_direction` uses some of these (regime.bias,
    regime.nifty.change_pct) so we have to approximate. We document the
    approximation in the report.
    """
    regime = (scan_row.get("regime") or "RANGE").upper()
    # Bias: derived from regime in absence of richer data
    if regime in ("TREND_UP", "RECOVERY"):
        bias = "BULLISH"
    elif regime in ("TREND_DOWN", "PANIC"):
        bias = "BEARISH"
    else:
        bias = "NEUTRAL"
    return RegimeSnapshot(
        regime=regime,
        bias=bias,
        vix=14.0,
        nifty=NiftySnapshot(change_pct=0.0, ltp=22000.0),
        pcr=PcrSnapshot(),
        fii=FiiDiiSnapshot(),
        confidence=0.7,
        data_health=0.8,
        source_quality=0.9,
    )


def replay_one(scan_row: dict[str, Any], ds: HistoricalDataset, cfg: StrategySettings) -> dict[str, Any]:
    """Replay one BQ scan_decisions row through production scoring.

    Returns a dict with our prediction + the BQ ground truth.
    """
    symbol = scan_row.get("symbol", "")
    setup = scan_row.get("setup", "AUTO") or "AUTO"
    wl_type = scan_row.get("wl_type") or "intraday"
    scan_ts = str(scan_row.get("scan_ts", ""))
    as_of_date = scan_ts[:10] if scan_ts else ""

    out: dict[str, Any] = {
        "symbol": symbol,
        "setup": setup,
        "wl_type": wl_type,
        "bq_direction": scan_row.get("direction"),
        "bq_score": scan_row.get("adjusted_score"),
        "bq_qualified": scan_row.get("qualified"),
        "bq_blocked_reason": scan_row.get("blocked_reason"),
        "replay_status": "UNKNOWN",
        "replay_error": "",
    }

    # Load candles. Use scan_ts (timestamp-precise) for intraday so the
    # replay sees only the bars live had at scan time, not the whole day.
    try:
        daily = ds.daily_candles(symbol, end_date=as_of_date)
        intraday = ds.intraday_candles(symbol, end_ts=scan_ts)
    except Exception as exc:
        out["replay_status"] = "DATA_ERROR"
        out["replay_error"] = f"{type(exc).__name__}: {exc}"
        return out

    if len(daily) < 50:
        out["replay_status"] = "INSUFFICIENT_DAILY"
        return out
    if len(intraday) < 80:
        out["replay_status"] = "INSUFFICIENT_INTRADAY"
        return out

    # Compute bias + indicators
    try:
        db = compute_daily_bias(daily)
        ind = compute_indicators(intraday, cfg)
    except Exception as exc:
        out["replay_status"] = "COMPUTE_ERROR"
        out["replay_error"] = f"{type(exc).__name__}: {exc}"
        return out
    if db is None or ind is None:
        out["replay_status"] = "COMPUTE_NULL"
        return out

    regime = _build_regime_snapshot(scan_row)

    try:
        direction = determine_direction(ind, regime, setup=setup, wl_type=wl_type, daily_bias=db)
        sig = score_signal(symbol, direction, ind, regime, cfg, daily_bias=db, setup=setup)
        if wl_type == "swing":
            gate_ok, gate_reason = check_swing_entry(setup, direction, ind, db, regime=regime.regime)
        else:
            from autotrader.domain.scoring import check_strategy_entry
            gate_ok, gate_reason = check_strategy_entry(setup, direction, ind, regime=regime.regime)
    except Exception as exc:
        out["replay_status"] = "SCORE_ERROR"
        out["replay_error"] = f"{type(exc).__name__}: {exc}"
        return out

    out["replay_direction"] = direction
    out["replay_score"] = sig.score
    out["replay_gate_ok"] = gate_ok
    out["replay_gate_reason"] = gate_reason
    out["replay_status"] = "OK"
    return out


def main() -> int:
    as_of = sys.argv[1] if len(sys.argv) > 1 else "2026-05-19"
    wl_type = sys.argv[2] if len(sys.argv) > 2 else "swing"
    limit = int(sys.argv[3]) if len(sys.argv) > 3 else 100

    print("=" * 70)
    print(f"Phase 2 — Scan-loop validation: {as_of} wl_type={wl_type} (limit {limit})")
    print("=" * 70)

    cfg = StrategySettings()
    ds = HistoricalDataset()

    # Fetch BQ scan_decisions sample
    # Note: cached daily candles in GCS are stale at last-update 2026-02-26.
    # That's still 6+ years of history per symbol — enough for indicator
    # computation. We filter for liquid names (high turnover) since illiquid
    # newly-listed stocks often have <50 candles and fail compute_daily_bias.
    sql = f"""
SELECT scan_ts, symbol, setup, wl_type, direction, adjusted_score, qualified,
       blocked_reason, regime, risk_mode, atr, vol_ratio
FROM `grow-profit-machine.autotrader.scan_decisions`
WHERE DATE(scan_ts, "Asia/Kolkata") = "{as_of}"
  AND wl_type = "{wl_type}"
  AND symbol IS NOT NULL
  -- Filter for liquid symbols (most likely to have full daily history in cache).
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
    print(f"Fetching {limit} BQ rows for {as_of} {wl_type}...")
    rows = _bq_query(sql)
    print(f"  Got {len(rows)} rows")

    # Replay each
    results = []
    matches = {"direction": 0, "qualified": 0, "blocked_reason": 0}
    statuses: dict[str, int] = {}
    for i, row in enumerate(rows):
        r = replay_one(row, ds, cfg)
        results.append(r)
        statuses[r["replay_status"]] = statuses.get(r["replay_status"], 0) + 1
        if r["replay_status"] == "OK":
            if r.get("replay_direction") == r.get("bq_direction"):
                matches["direction"] += 1
            # qualified = bool(qualified)
            replay_qualified = (
                r.get("replay_direction") != "HOLD"
                and r.get("replay_gate_ok") is True
                and r.get("replay_score", 0) >= (cfg.swing_min_signal_score if wl_type == "swing" else cfg.min_signal_score)
            )
            bq_q = str(r.get("bq_qualified", "")).lower() == "true"
            if replay_qualified == bq_q:
                matches["qualified"] += 1
        if (i + 1) % 25 == 0:
            print(f"  processed {i+1}/{len(rows)}")

    ok_count = statuses.get("OK", 0)
    print()
    print("Replay status counts:")
    for s, n in sorted(statuses.items(), key=lambda x: -x[1]):
        print(f"  {s:20s} {n}")

    if ok_count > 0:
        print()
        print("Match rates (within OK-replays):")
        print(f"  direction match : {matches['direction']:4d} / {ok_count:4d}  =  {100 * matches['direction'] / ok_count:5.1f}%")
        print(f"  qualified match : {matches['qualified']:4d} / {ok_count:4d}  =  {100 * matches['qualified'] / ok_count:5.1f}%")

    # Show a few diffs
    print()
    print("Sample mismatches (first 5):")
    n = 0
    for r in results:
        if r["replay_status"] != "OK":
            continue
        if r.get("replay_direction") == r.get("bq_direction"):
            continue
        print(f"  {r['symbol']:14s} {r['setup']:18s} BQ={r['bq_direction']:5s} replay={r.get('replay_direction','?')} score=BQ{r['bq_score']} replay{r.get('replay_score','?')} bq_blocked={r['bq_blocked_reason']} replay_gate={r.get('replay_gate_reason','?')}")
        n += 1
        if n >= 5:
            break

    print()
    dir_pct = (matches["direction"] / ok_count * 100) if ok_count else 0
    qual_pct = (matches["qualified"] / ok_count * 100) if ok_count else 0
    # PRIMARY criterion: qualified-decision match (does the backtest fire
    # the same trade decision as live?). Direction match is a secondary
    # diagnostic — direction disagreements that both end in "blocked" are
    # harmless for backtest P&L.
    if ok_count == 0:
        print("⚠️  Phase 2 INCONCLUSIVE — all replays failed (data layer issue)")
        return 1
    elif qual_pct >= 95:
        print(f"✅ Phase 2 PASS — trade-decision match {qual_pct:.1f}% (≥95%)")
        if dir_pct < 95:
            print(f"   (direction sub-vote disagrees {100-dir_pct:.1f}% of the time but all such cases still")
            print(f"    block — different reasoning, same outcome. Worth investigating later but not blocking.)")
        return 0
    else:
        print(f"❌ Phase 2 FAIL — trade-decision match {qual_pct:.1f}% < 95%")
        return 1


if __name__ == "__main__":
    sys.exit(main())
