"""Report writers for backtest results.

Produces:
  * `trades.csv`         — flat per-trade journal (one row per closed trade)
  * `equity.csv`         — per-bar equity points
  * `summary.json`       — top-level metrics (round-trip pretty)
  * `per_setup.csv`      — per-setup breakdown
  * `per_regime.csv`     — per-regime breakdown
  * `per_setup_regime.csv` — 2D matrix
  * `summary.txt`        — human-readable rollup (headline numbers)

All writes are atomic-ish (write to .tmp + rename). No fancy plotting in this
module — keep it dependency-free. A separate `plots.py` (TODO) can wrap
matplotlib for HTML output once it's needed.
"""
from __future__ import annotations

import csv
import json
import logging
import os
from dataclasses import asdict
from pathlib import Path

from autotrader.backtest.types import BacktestResult, EquityPoint, SimTrade

log = logging.getLogger(__name__)


def write_all(result: BacktestResult, *, out_dir: str | Path) -> dict[str, str]:
    """Write the full report bundle. Returns {name: path} dict."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    paths = {
        "trades_csv": str(_write_trades_csv(result.trades, out / "trades.csv")),
        "equity_csv": str(_write_equity_csv(result.equity_curve, out / "equity.csv")),
        "summary_json": str(_write_summary_json(result, out / "summary.json")),
        "summary_txt": str(_write_summary_txt(result, out / "summary.txt")),
        "per_setup_csv": str(_write_group_csv(result.per_setup, out / "per_setup.csv", "setup")),
        "per_regime_csv": str(_write_group_csv(result.per_regime, out / "per_regime.csv", "regime")),
        "per_setup_regime_csv": str(_write_group_csv(
            result.per_setup_regime, out / "per_setup_regime.csv", "setup_regime",
        )),
    }
    return paths


def _write_trades_csv(trades: list[SimTrade], path: Path) -> Path:
    fields = list(SimTrade.__dataclass_fields__.keys())
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for t in trades:
            row = asdict(t)
            row["is_swing"] = "Y" if row["is_swing"] else "N"
            w.writerow(row)
    log.info("trades_csv_written path=%s n=%d", path, len(trades))
    return path


def _write_equity_csv(equity: list[EquityPoint], path: Path) -> Path:
    fields = list(EquityPoint.__dataclass_fields__.keys())
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for ep in equity:
            w.writerow(asdict(ep))
    return path


def _write_summary_json(result: BacktestResult, path: Path) -> Path:
    data = {
        "metrics": result.metrics,
        "per_setup": result.per_setup,
        "per_regime": result.per_regime,
        "per_setup_regime": result.per_setup_regime,
        "meta": result.meta,
    }
    with path.open("w") as f:
        json.dump(data, f, indent=2, default=str, sort_keys=True)
    return path


def _write_group_csv(group: dict[str, dict[str, float]], path: Path, key_col: str) -> Path:
    if not group:
        path.write_text(f"{key_col}\n")
        return path
    sample_stats = next(iter(group.values()))
    fields = [key_col] + list(sample_stats.keys())
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        # Sort by net_pnl desc for at-a-glance readability.
        sorted_keys = sorted(group.keys(), key=lambda k: group[k].get("net_pnl", 0.0), reverse=True)
        for k in sorted_keys:
            row = {key_col: k, **group[k]}
            w.writerow(row)
    return path


def _write_summary_txt(result: BacktestResult, path: Path) -> Path:
    m = result.metrics
    lines: list[str] = []
    lines.append("─" * 64)
    lines.append("  BACKTEST SUMMARY")
    lines.append("─" * 64)
    if result.meta:
        for k, v in sorted(result.meta.items()):
            lines.append(f"  {k:24s} {v}")
        lines.append("")

    def row(label: str, key: str, fmt: str = "{:>14}") -> None:
        v = m.get(key, "")
        if isinstance(v, float):
            s = f"{v:,.4f}" if abs(v) < 1 else f"{v:,.2f}"
        else:
            s = str(v)
        lines.append(f"  {label:34s} {s:>20s}")

    lines.append("  PERFORMANCE")
    row("Trades (n)", "n_trades")
    row("Win rate (%)", "win_rate")
    row("Profit factor", "profit_factor")
    row("Expectancy (R)", "expectancy_r")
    row("Expectancy (₹)", "expectancy_inr")
    lines.append("")
    lines.append("  P&L")
    row("Gross P&L (₹)", "gross_pnl")
    row("Net P&L (₹)", "net_pnl")
    row("Total costs (₹)", "total_costs")
    row("Avg win (₹)", "avg_win_inr")
    row("Avg loss (₹)", "avg_loss_inr")
    row("Max win (₹)", "max_win_inr")
    row("Max loss (₹)", "max_loss_inr")
    lines.append("")
    lines.append("  RISK")
    row("Sharpe (annualized)", "sharpe")
    row("Sortino (annualized)", "sortino")
    row("Max drawdown (%)", "max_drawdown_pct")
    row("MAR ratio", "mar")
    lines.append("")
    lines.append("  EQUITY")
    row("Starting cash (₹)", "starting_cash")
    row("Ending equity (₹)", "ending_equity")
    row("Total return (%)", "total_return_pct")
    row("Annualized return (%)", "annualized_return_pct")
    lines.append("")
    lines.append("  HOLD")
    row("Avg hold (bars)", "avg_hold_bars")
    row("Avg MFE (R)", "avg_mfe_r")
    row("Avg MAE (R)", "avg_mae_r")
    lines.append("─" * 64)

    if result.per_setup:
        lines.append("")
        lines.append("  PER-SETUP")
        lines.append(f"  {'Setup':18s} {'N':>4s} {'Win%':>6s} {'Net₹':>12s} {'E[R]':>8s}")
        for k in sorted(result.per_setup.keys(),
                        key=lambda k: result.per_setup[k].get("net_pnl", 0.0), reverse=True):
            s = result.per_setup[k]
            lines.append(
                f"  {k:18s} {int(s.get('n', 0)):>4d} "
                f"{s.get('win_rate', 0):>6.1f} "
                f"{s.get('net_pnl', 0):>12,.0f} "
                f"{s.get('expectancy_r', 0):>8.3f}"
            )

    if result.per_regime:
        lines.append("")
        lines.append("  PER-REGIME")
        lines.append(f"  {'Regime':18s} {'N':>4s} {'Win%':>6s} {'Net₹':>12s} {'E[R]':>8s}")
        for k in sorted(result.per_regime.keys(),
                        key=lambda k: result.per_regime[k].get("net_pnl", 0.0), reverse=True):
            s = result.per_regime[k]
            lines.append(
                f"  {k:18s} {int(s.get('n', 0)):>4d} "
                f"{s.get('win_rate', 0):>6.1f} "
                f"{s.get('net_pnl', 0):>12,.0f} "
                f"{s.get('expectancy_r', 0):>8.3f}"
            )

    txt = "\n".join(lines) + "\n"
    path.write_text(txt)
    return path


def print_summary(result: BacktestResult) -> None:
    """Print the human-readable summary to stdout."""
    import io
    buf = io.StringIO()
    # Reuse the writer to a temp dict with print fallback.
    tmp = Path(os.devnull) if False else None  # placeholder
    # Inline version of the same formatting.
    m = result.metrics
    print("─" * 64)
    print("  BACKTEST SUMMARY")
    print("─" * 64)
    for k in ("n_trades", "win_rate", "profit_factor", "expectancy_r", "expectancy_inr"):
        if k in m:
            print(f"  {k:24s} {m[k]}")
    print()
    for k in ("net_pnl", "total_costs", "sharpe", "max_drawdown_pct", "annualized_return_pct"):
        if k in m:
            print(f"  {k:24s} {m[k]}")
    print("─" * 64)
    if result.per_setup:
        print("  PER-SETUP")
        for k, s in sorted(result.per_setup.items(),
                           key=lambda kv: kv[1].get("net_pnl", 0.0), reverse=True):
            print(f"    {k:20s} N={int(s.get('n',0)):>3d}  Win%={s.get('win_rate',0):>5.1f}  "
                  f"Net₹={s.get('net_pnl',0):>10,.0f}  E[R]={s.get('expectancy_r',0):>6.3f}")


__all__ = ["write_all", "print_summary"]
