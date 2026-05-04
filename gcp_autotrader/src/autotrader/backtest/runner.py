"""End-to-end backtest orchestration.

Wires the moving parts together so the CLI / a test / a notebook can run a
full backtest with one call:

    result = run_live_replay(RunSpec(
        project="my-gcp-project", dataset="autotrader",
        since="2026-04-16", until="2026-05-04",
        timeframe="5m", starting_cash=1_000_000.0,
    ))

What this module does NOT do:
  * Define new strategies (see `replay_live.py` / future `replay_pure.py`)
  * Define new metrics (see `metrics.py`)
  * Define new cost / slippage models (see `costs.py`, `slippage.py`)

It only wires the data → engine → metrics → reports flow.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from autotrader.backtest.account import SimAccount, SimAccountConfig
from autotrader.backtest.costs import CostConfig
from autotrader.backtest.data import (
    ScanDecisionRow,
    load_candles_bulk_bq,
    load_scan_decisions,
)
from autotrader.backtest.engine import BacktestEngine, EngineConfig
from autotrader.backtest.metrics import (
    per_regime_stats,
    per_setup_regime_stats,
    per_setup_stats,
    summarize,
)
from autotrader.backtest.replay_live import LiveDecisionStrategy, LiveReplayConfig
from autotrader.backtest.reports import write_all
from autotrader.backtest.slippage import SlippageModel, default_model
from autotrader.backtest.types import Bar, BacktestResult

log = logging.getLogger(__name__)


# ── Top-level run spec ─────────────────────────────────────────────────────


@dataclass
class RunSpec:
    """Everything the runner needs to execute a single backtest run.

    Defaults are tuned for a "smoke" replay over the post-redesign window:
        since=2026-04-16 (first day of redesigned algo), until=today.
    """
    # ── Required ─────────────────────────────────────────────────────
    project: str
    dataset: str
    since: str                           # YYYY-MM-DD inclusive
    until: str                           # YYYY-MM-DD inclusive

    # ── Data ─────────────────────────────────────────────────────────
    timeframe: str = "5m"                # "5m" | "15m" | "1d"
    symbols: list[str] | None = None     # None = derive from scan_decisions
    setups: list[str] | None = None      # None = all
    qualified_only: bool = False         # only fire on qualified rows
    unblock_reasons: tuple[str, ...] = ()  # counterfactual: include blocked-for-this-reason

    # ── Account ──────────────────────────────────────────────────────
    starting_cash: float = 1_000_000.0
    per_trade_risk_inr: float = 5_000.0
    max_concurrent: int = 5              # engine-side cap; 0 = unlimited
    direction_filter: str | None = None  # "BUY" / "SELL" / None

    # ── Output ───────────────────────────────────────────────────────
    out_dir: str | None = None           # if set, write CSV/JSON/TXT reports here
    label: str = "live_replay"           # tag for meta + report dir-naming

    # ── Override hooks (None = defaults) ─────────────────────────────
    cost_cfg: CostConfig | None = None
    slippage: SlippageModel | None = None


# ── Live-replay run ────────────────────────────────────────────────────────


def run_live_replay(spec: RunSpec) -> BacktestResult:
    """Run a live-decision replay end to end.

    Steps:
      1. Pull `scan_decisions` over [since, until].
      2. Derive the symbol list (from spec or from decisions).
      3. Bulk-load candles for those symbols at the chosen timeframe.
      4. Construct SimAccount + LiveDecisionStrategy + Engine.
      5. Run engine; compute summary + per-bucket breakdowns.
      6. Optionally write reports.
    """
    log.info("backtest_run_starting label=%s since=%s until=%s tf=%s",
             spec.label, spec.since, spec.until, spec.timeframe)

    decisions = load_scan_decisions(
        project=spec.project, dataset=spec.dataset,
        since=spec.since, until=spec.until,
        qualified_only=spec.qualified_only,
        setups=spec.setups,
        symbols=spec.symbols,
    )
    log.info("decisions_loaded n=%d", len(decisions))
    if not decisions:
        return _empty_result(spec, "no scan_decisions in window")

    syms = _derive_symbols(spec.symbols, decisions)
    log.info("symbols_resolved n=%d", len(syms))

    bars_by_sym = load_candles_bulk_bq(
        project=spec.project, dataset=spec.dataset,
        symbols=syms, timeframe=spec.timeframe,
        since=spec.since, until=spec.until,
    )
    bars: list[Bar] = []
    for sym in syms:
        bars.extend(bars_by_sym.get(sym, []))
    log.info("candles_loaded total_bars=%d symbols_with_bars=%d",
             len(bars), len(bars_by_sym))
    if not bars:
        return _empty_result(spec, "no candles in window for resolved symbols")

    account_cfg = SimAccountConfig(
        starting_cash=spec.starting_cash,
        cost_cfg=spec.cost_cfg or CostConfig(),
    )
    account = SimAccount(cfg=account_cfg, slippage=spec.slippage or default_model())

    strat_cfg = LiveReplayConfig(
        per_trade_risk_inr=spec.per_trade_risk_inr,
        unblock_reasons=tuple(spec.unblock_reasons),
        setups_filter=tuple(spec.setups) if spec.setups else None,
        direction_filter=spec.direction_filter,
        max_concurrent=spec.max_concurrent,
    )
    strategy = LiveDecisionStrategy(decisions=decisions, cfg=strat_cfg)

    engine = BacktestEngine(
        account=account,
        strategy=strategy,
        cfg=EngineConfig(max_open_positions=spec.max_concurrent),
    )
    result = engine.run(bars)

    # Hydrate metrics + per-bucket breakdowns + meta.
    result.metrics = summarize(
        trades=result.trades,
        equity_curve=result.equity_curve,
        starting_cash=spec.starting_cash,
    )
    result.per_setup = per_setup_stats(result.trades)
    result.per_regime = per_regime_stats(result.trades)
    result.per_setup_regime = per_setup_regime_stats(result.trades)
    result.meta.update({
        "label": spec.label,
        "since": spec.since,
        "until": spec.until,
        "timeframe": spec.timeframe,
        "n_decisions": str(len(decisions)),
        "n_symbols": str(len(syms)),
        "qualified_only": str(spec.qualified_only),
        "unblock_reasons": ",".join(spec.unblock_reasons) or "(none)",
        "max_concurrent": str(spec.max_concurrent),
        "per_trade_risk_inr": str(spec.per_trade_risk_inr),
    })

    if spec.out_dir:
        out = Path(spec.out_dir) / spec.label
        paths = write_all(result, out_dir=out)
        log.info("reports_written dir=%s files=%d", out, len(paths))
        result.meta["report_dir"] = str(out)

    log.info(
        "backtest_run_done label=%s n_trades=%d net_pnl=%s sharpe=%s max_dd=%s",
        spec.label,
        result.metrics.get("n_trades", 0),
        result.metrics.get("net_pnl", 0),
        result.metrics.get("sharpe", 0),
        result.metrics.get("max_drawdown_pct", 0),
    )
    return result


# ── Helpers ────────────────────────────────────────────────────────────────


def _derive_symbols(
    explicit: list[str] | None,
    decisions: list[ScanDecisionRow],
) -> list[str]:
    """Return the symbol list to load candles for. Prefer the explicit user
    list; otherwise pull from the decisions stream (deduped, sorted)."""
    if explicit:
        return sorted({s.upper().strip() for s in explicit})
    return sorted({d.symbol.upper().strip() for d in decisions})


def _empty_result(spec: RunSpec, reason: str) -> BacktestResult:
    log.warning("backtest_run_empty reason=%s", reason)
    res = BacktestResult(trades=[], equity_curve=[])
    res.meta = {
        "label": spec.label, "since": spec.since, "until": spec.until,
        "reason_empty": reason,
    }
    res.metrics = summarize(
        trades=[], equity_curve=[], starting_cash=spec.starting_cash,
    )
    return res


# ── Validation against ground-truth (live trades) ─────────────────────────


@dataclass
class TruthCompare:
    """Side-by-side rollup of sim vs live trades over the same window. Used
    to sanity-check the replay — if sim trade count diverges from live by 5×,
    something is structurally off (bar timestamps, scan_ts mismatch, etc.).
    """
    sim_n: int
    live_n: int
    sim_net_pnl: float
    live_net_pnl: float
    sim_win_rate: float
    live_win_rate: float
    notes: list[str] = field(default_factory=list)


def compare_to_truth(
    *,
    sim_result: BacktestResult,
    project: str,
    dataset: str,
    since: str,
    until: str,
) -> TruthCompare:
    """Pull live trades and compare top-line counts/PnL to sim.

    NOT a strict equality check — costs differ, fill prices differ, exits
    differ. Use this to confirm the replay is roughly tracking reality.
    """
    from autotrader.backtest.data import load_trades_truth

    live = load_trades_truth(
        project=project, dataset=dataset, since=since, until=until,
    )
    live_n = len(live)
    live_net = round(sum(float(r.get("net_pnl") or 0.0) for r in live), 2)
    live_wins = sum(1 for r in live if float(r.get("net_pnl") or 0.0) > 0)
    live_wr = round(live_wins / live_n * 100, 2) if live_n else 0.0

    sim_n = len(sim_result.trades)
    sim_net = float(sim_result.metrics.get("net_pnl", 0.0))
    sim_wr = float(sim_result.metrics.get("win_rate", 0.0))

    notes: list[str] = []
    if live_n and sim_n / max(live_n, 1) < 0.5:
        notes.append(f"sim n={sim_n} < 50% of live n={live_n} — replay may be filtering too aggressively")
    if live_n and sim_n / max(live_n, 1) > 2.0:
        notes.append(f"sim n={sim_n} > 2× live n={live_n} — strategy may be over-firing (check max_concurrent / pyramid guard)")
    if live_n and abs(sim_wr - live_wr) > 25:
        notes.append(
            f"win-rate gap {abs(sim_wr - live_wr):.1f}pp — "
            f"sim {sim_wr:.1f}% vs live {live_wr:.1f}% — exit FSM behavior diverges"
        )

    return TruthCompare(
        sim_n=sim_n, live_n=live_n,
        sim_net_pnl=sim_net, live_net_pnl=live_net,
        sim_win_rate=sim_wr, live_win_rate=live_wr,
        notes=notes,
    )


__all__ = ["RunSpec", "run_live_replay", "compare_to_truth", "TruthCompare"]
