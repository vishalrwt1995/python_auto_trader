"""Swing portfolio simulator — converts signal-level trades into the ACCOUNT-level
result the live ₹1L swing book would actually produce.

Covers the caveats: portfolio limits (max 5 concurrent positions, ₹1L capital,
₹3k daily-loss halt / ₹6k profit target), faithful-regime affinity gating +
score-ranked selection (take the best signals when >5 compete for a slot),
live ₹1,500 sizing, full Upstox CNC costs.

Input: the phase9 prod_replica trades JSON (one record per qualified signal,
with as_of/holding_days/entry/sl/exit/setup/raw_score) + the faithful regime
timeline. We walk the trading calendar day-by-day maintaining the book.

Run:
    PYTHONPATH=src python3 -m autotrader.backtest_v2.swing_portfolio_sim
"""
from __future__ import annotations

import collections
import json
import os

from autotrader.backtest.costs import compute_round_trip_cost, CostConfig
from autotrader.domain.regime_affinity import regime_hard_blocks_strategy, regime_strategy_multiplier

U = CostConfig.upstox()
RISK = 1500.0
CAP_NOTIONAL = 20000.0       # 20% of ₹1L per position
MAX_POSITIONS = 5
DAILY_LOSS_HALT = -3000.0
DAILY_PROFIT_HALT = 6000.0
TRADES = os.path.expanduser("~/.autotrader_backtest_cache/phase9_trades_2022-01-01_2026-06-04.json")


def main() -> int:
    reg = {}
    for l in open("/tmp/regime_timeline.jsonl"):
        d = json.loads(l)
        reg[d["date"]] = d["regime"]
    cal = sorted(reg.keys())                      # trading calendar
    idx_of = {d: i for i, d in enumerate(cal)}

    trades = json.load(open(TRADES))
    pr = [t for t in trades if "prod_replica" in (t.get("config_tags") or [])]

    # Build per-day candidate signals (faithful-regime gated), with sizing/cost/exit precomputed.
    by_day: dict[str, list] = collections.defaultdict(list)
    gated = 0
    for t in pr:
        ao = str(t.get("as_of", ""))[:10]
        if ao not in idx_of:
            continue
        s = t.get("setup", ""); d = t.get("direction", "BUY")
        fr = reg.get(ao, "RANGE")
        if regime_hard_blocks_strategy(fr, s):    # faithful-regime hard block
            gated += 1
            continue
        e = float(t.get("entry_price") or 0); sl = float(t.get("sl") or 0); x = float(t.get("exit_price") or 0)
        sld = abs(e - sl)
        if e <= 0 or sld <= 0:
            continue
        qty = max(1, min(int(RISK / sld), int(CAP_NOTIONAL / e)))
        gross = (x - e) * qty if d == "BUY" else (e - x) * qty
        net = gross - compute_round_trip_cost(qty=qty, entry_price=e, exit_price=x, is_swing=True, cfg=U)
        # affinity-adjusted score for ranking when >5 signals compete
        mult = regime_strategy_multiplier(fr, s, d)
        adj = float(t.get("raw_score") or t.get("adjusted_score") or 50.0) * float(mult or 1.0)
        hold = int(t.get("holding_days") or 0)
        exit_idx = min(idx_of[ao] + max(hold, 1), len(cal) - 1)
        by_day[ao].append({"setup": s, "net": net, "adj": adj, "exit_idx": exit_idx, "year": ao[:4]})

    # Portfolio walk
    open_pos: list = []                            # each: {exit_idx, net, setup, year}
    taken = collections.defaultdict(lambda: [0, 0.0, 0])   # year -> n, net, wins
    skipped_full = 0; skipped_halt = 0; max_concurrent = 0
    for i, day in enumerate(cal):
        # close positions exiting today
        day_realized = 0.0
        still = []
        for p in open_pos:
            if p["exit_idx"] <= i:
                taken[p["year"]][0] += 1; taken[p["year"]][1] += p["net"]; taken[p["year"]][2] += 1 if p["net"] > 0 else 0
                day_realized += p["net"]
            else:
                still.append(p)
        open_pos = still
        # consider new signals (best score first)
        sigs = sorted(by_day.get(day, []), key=lambda z: -z["adj"])
        for sig in sigs:
            if day_realized <= DAILY_LOSS_HALT or day_realized >= DAILY_PROFIT_HALT:
                skipped_halt += 1; continue
            if len(open_pos) >= MAX_POSITIONS:
                skipped_full += 1; continue
            open_pos.append(sig)
        max_concurrent = max(max_concurrent, len(open_pos))

    # flush any still-open at end (mark to last close)
    for p in open_pos:
        taken[p["year"]][0] += 1; taken[p["year"]][1] += p["net"]; taken[p["year"]][2] += 1 if p["net"] > 0 else 0

    N = sum(v[0] for v in taken.values()); NET = sum(v[1] for v in taken.values()); W = sum(v[2] for v in taken.values())
    print("=== ACCOUNT-LEVEL swing (₹1L, 5 positions, ₹3k halt, faithful regime, full cost) ===")
    print(f"{'year':6}{'taken':>7}{'WR%':>6}{'NET₹':>13}")
    for yr in sorted(taken):
        n, net, w = taken[yr]
        print(f"{yr:6}{n:>7}{(100*w/n if n else 0):>6.1f}{net:>13,.0f}")
    print(f"{'ALL':6}{N:>7}{(100*W/N if N else 0):>6.1f}{NET:>13,.0f}")
    print(f"\nsignals hard-block-gated: {gated:,} | skipped(book full): {skipped_full:,} | "
          f"skipped(daily halt): {skipped_halt:,} | max concurrent: {max_concurrent}")
    print(f"account took {N:,} trades vs {len(pr):,} raw signals "
          f"(~{100*N/len(pr):.1f}% — the rest couldn't fit the ₹1L book)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
