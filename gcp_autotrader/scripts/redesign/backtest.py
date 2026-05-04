#!/usr/bin/env python3
"""CLI driver for the autotrader backtest harness.

Two modes (today):
  * `live-replay` — replay `scan_decisions` rows through the sim engine.
                    Fast, no decoupling required, validates exits + costs.
  * `compare`     — run live-replay AND fetch live trades, print side-by-side.

Examples
--------

    # Smoke run, post-redesign window, default settings (5m timeframe).
    python scripts/redesign/backtest.py live-replay \\
        --project grow-profit-machine --dataset autotrader \\
        --since 2026-04-16 --until 2026-05-04 \\
        --out-dir backtests/

    # Validate against live trades (apples-to-apples-ish).
    python scripts/redesign/backtest.py compare \\
        --project grow-profit-machine --dataset autotrader \\
        --since 2026-04-16 --until 2026-05-04 \\
        --out-dir backtests/

    # Counterfactual: unblock VWAP_REVERSAL signals to see what they'd earn.
    python scripts/redesign/backtest.py live-replay \\
        --project grow-profit-machine --dataset autotrader \\
        --since 2026-04-16 --until 2026-05-04 \\
        --unblock policy_strategy_blocked --label vwap_unblock \\
        --out-dir backtests/

    # Restrict to two setups, swap to 15m timeframe.
    python scripts/redesign/backtest.py live-replay \\
        --project grow-profit-machine --dataset autotrader \\
        --since 2026-04-16 --until 2026-05-04 \\
        --setups BREAKOUT,MEAN_REVERSION --timeframe 15m

Output
------
With `--out-dir DIR`, writes to DIR/<label>/:
    trades.csv, equity.csv, summary.json, summary.txt,
    per_setup.csv, per_regime.csv, per_setup_regime.csv

Always prints the human-readable summary to stdout.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path


def _setup_path() -> None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))


def _build_spec(args: argparse.Namespace):
    from autotrader.backtest.runner import RunSpec
    return RunSpec(
        project=args.project,
        dataset=args.dataset,
        since=args.since,
        until=args.until,
        timeframe=args.timeframe,
        symbols=[s.strip().upper() for s in args.symbols.split(",")] if args.symbols else None,
        setups=[s.strip().upper() for s in args.setups.split(",")] if args.setups else None,
        qualified_only=args.qualified_only,
        unblock_reasons=tuple(r.strip() for r in args.unblock.split(",") if r.strip()) if args.unblock else (),
        starting_cash=args.starting_cash,
        per_trade_risk_inr=args.per_trade_risk,
        max_concurrent=args.max_concurrent,
        direction_filter=args.direction or None,
        out_dir=args.out_dir,
        label=args.label,
    )


def cmd_live_replay(args: argparse.Namespace) -> int:
    from autotrader.backtest.runner import run_live_replay
    from autotrader.backtest.reports import print_summary

    spec = _build_spec(args)
    result = run_live_replay(spec)
    print_summary(result)
    return 0


def cmd_compare(args: argparse.Namespace) -> int:
    from autotrader.backtest.reports import print_summary
    from autotrader.backtest.runner import compare_to_truth, run_live_replay

    spec = _build_spec(args)
    result = run_live_replay(spec)
    print_summary(result)

    cmp = compare_to_truth(
        sim_result=result,
        project=args.project, dataset=args.dataset,
        since=args.since, until=args.until,
    )
    print()
    print("─" * 64)
    print("  SIM vs LIVE (same window)")
    print("─" * 64)
    print(f"  {'metric':24s} {'sim':>14s} {'live':>14s}")
    print(f"  {'n_trades':24s} {cmp.sim_n:>14d} {cmp.live_n:>14d}")
    print(f"  {'net_pnl (₹)':24s} {cmp.sim_net_pnl:>14,.0f} {cmp.live_net_pnl:>14,.0f}")
    print(f"  {'win_rate (%)':24s} {cmp.sim_win_rate:>14.2f} {cmp.live_win_rate:>14.2f}")
    if cmp.notes:
        print()
        print("  NOTES")
        for n in cmp.notes:
            print(f"    • {n}")
    print("─" * 64)
    return 0


def cmd_walk_forward(args: argparse.Namespace) -> int:
    """Walk-forward harness skeleton — parameter sweeps land here later."""
    from autotrader.backtest.reports import print_summary
    from autotrader.backtest.runner import run_live_replay
    from autotrader.backtest.walkforward import (
        WalkForwardConfig,
        run_walk_forward,
    )

    cfg = WalkForwardConfig(
        train_days=args.train_days,
        test_days=args.test_days,
        holdout_days=args.holdout_days,
    )

    base_spec = _build_spec(args)

    def _train(t_since: str, t_until: str) -> dict:
        # Live-replay has no tunable params today — return identity dict.
        # Pure-replay will plug here once it's wired.
        return {"per_trade_risk_inr": base_spec.per_trade_risk_inr}

    def _test(t_since: str, t_until: str, params: dict) -> dict:
        spec = _build_spec(args)
        spec.since = t_since
        spec.until = t_until
        spec.label = f"wf_test_{t_since}_{t_until}"
        spec.out_dir = None  # don't write per-fold reports unless asked
        spec.per_trade_risk_inr = params.get("per_trade_risk_inr", base_spec.per_trade_risk_inr)
        result = run_live_replay(spec)
        return result.metrics

    def _holdout(h_since: str, h_until: str, params: dict) -> dict:
        spec = _build_spec(args)
        spec.since = h_since
        spec.until = h_until
        spec.label = f"holdout_{h_since}_{h_until}"
        spec.per_trade_risk_inr = params.get("per_trade_risk_inr", base_spec.per_trade_risk_inr)
        result = run_live_replay(spec)
        return result.metrics

    out = run_walk_forward(
        since=args.since, until=args.until, cfg=cfg,
        train_eval=_train, test_eval=_test,
        holdout_eval=_holdout if args.holdout_days > 0 else None,
    )

    print(json.dumps(out, indent=2, default=str, sort_keys=True))
    return 0


# ── Argparse ───────────────────────────────────────────────────────────────


def _add_common_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--project", default="grow-profit-machine")
    p.add_argument("--dataset", default="autotrader")
    p.add_argument("--since", required=True, help="YYYY-MM-DD inclusive")
    p.add_argument("--until", required=True, help="YYYY-MM-DD inclusive")
    p.add_argument("--timeframe", default="5m", choices=("5m", "15m", "1d"))
    p.add_argument("--symbols", default="", help="comma-separated; default = all from scan_decisions")
    p.add_argument("--setups", default="", help="comma-separated; default = all")
    p.add_argument("--qualified-only", action="store_true",
                   help="only fire on qualified scan rows (skip blocked)")
    p.add_argument("--unblock", default="",
                   help="comma-separated blocked_reason values to unblock (counterfactual)")
    p.add_argument("--direction", default="", choices=("", "BUY", "SELL"),
                   help="restrict trades to this direction")
    p.add_argument("--starting-cash", type=float, default=1_000_000.0)
    p.add_argument("--per-trade-risk", type=float, default=5_000.0,
                   help="₹ risk per trade (sets qty)")
    p.add_argument("--max-concurrent", type=int, default=5,
                   help="max simultaneous open positions; 0 = unlimited")
    p.add_argument("--out-dir", default=None,
                   help="if set, write report bundle to this directory")
    p.add_argument("--label", default="live_replay",
                   help="label for the run; report subdir name")


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    ap = argparse.ArgumentParser(prog="backtest", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_live = sub.add_parser("live-replay",
                            help="replay scan_decisions through sim engine")
    _add_common_args(p_live)
    p_live.set_defaults(func=cmd_live_replay)

    p_cmp = sub.add_parser("compare",
                           help="live-replay + diff vs live trades table")
    _add_common_args(p_cmp)
    p_cmp.set_defaults(func=cmd_compare)

    p_wf = sub.add_parser("walk-forward",
                          help="rolling train/test fold harness")
    _add_common_args(p_wf)
    p_wf.add_argument("--train-days", type=int, default=15)
    p_wf.add_argument("--test-days", type=int, default=5)
    p_wf.add_argument("--holdout-days", type=int, default=0)
    p_wf.set_defaults(func=cmd_walk_forward)

    args = ap.parse_args(argv)
    _setup_path()
    return int(args.func(args) or 0)


if __name__ == "__main__":
    sys.exit(main())
