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

from datetime import date, datetime, timedelta

from autotrader.backtest.account import SimAccount, SimAccountConfig
from autotrader.backtest.costs import CostConfig
from autotrader.backtest.data import (
    ScanDecisionRow,
    brain_lookup,
    load_candles_bulk_bq,
    load_candles_bulk_gcs,
    load_market_brain,
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
from autotrader.backtest.replay_pure import PureReplayConfig, PureReplayStrategy
from autotrader.backtest.reports import write_all
from autotrader.backtest.slippage import SlippageModel, default_model
from autotrader.backtest.types import Bar, BacktestResult
from autotrader.settings import StrategySettings

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

    # ── Candle source ────────────────────────────────────────────────
    # "gcs" reads from `cache/score_1d/...` and `cache/candles/{tf}/...`,
    # the same canonical store the live system reads at scan time. This
    # is the default — backtests are faithful to live by construction
    # and don't depend on the best-effort BQ dual-write coverage.
    # "bq" falls back to the BigQuery `candles_1d` / `candles_5m` tables
    # (useful for SQL-driven analytics or when the GCS bucket is unreachable).
    candle_source: str = "gcs"           # "gcs" | "bq"
    gcs_bucket: str = "grow-profit-machine-autotrader-data"
    gcs_exchange: str = "NSE"
    gcs_segment: str = "CASH"

    # ── Pure-replay specific (only honoured by run_pure_replay) ──────
    # Calendar days of history loaded BEFORE `since` so indicators are
    # warm by the first in-window bar. compute_indicators needs ≥80
    # bars; at 5m that's ~1 trading day, but volatile leaders see EMA
    # stack stability only after 5+ days. Default 7 calendar days = ~5
    # trading days = ~375 bars at 5m, comfortable for all indicators.
    warmup_days: int = 7
    # Override the static StrategySettings.min_signal_score. None = use
    # whatever is in StrategySettings (today: 72).
    min_signal_score: int | None = None
    # If set, restrict pure-replay symbol universe to scan_decisions
    # symbols seen in [since, until]. Falls through to all symbols in
    # spec.symbols if scan_decisions is empty.
    pure_universe_from_scan: bool = True
    # When True, load 1d candles for the symbol set and pass them to the
    # strategy so `score_signal` Layer-5 (daily-trend alignment, ±15 pts)
    # and `check_swing_entry` can fire. compute_daily_bias needs ≥50 daily
    # bars; the runner pulls 90 calendar days back from `since` to be safe
    # (≈63 trading days, well over the threshold even after gaps).
    load_daily_bars: bool = True
    daily_warmup_days: int = 90
    # Pure-replay scoring/threshold flags — each maps 1:1 to a
    # PureReplayConfig flag with the same name. Defaults match live; set
    # False for diagnostic A/B runs that isolate a single live behaviour.
    apply_brain_haircut: bool = True
    apply_dynamic_min_score: bool = True
    apply_swing_entry_gate: bool = True
    apply_daily_bias: bool = True
    # Strategy-specific entry gate (live's `check_strategy_entry`). Default
    # True to match live; disable only for diagnostic A/B comparisons of
    # what the system would have done without per-setup hard gates.
    apply_strategy_entry_gate: bool = True


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

    bars_by_sym = _load_candles(
        spec, symbols=syms, timeframe=spec.timeframe,
        since=spec.since, until=spec.until,
    )
    bars: list[Bar] = []
    for sym in syms:
        bars.extend(bars_by_sym.get(sym, []))
    log.info("candles_loaded source=%s total_bars=%d symbols_with_bars=%d",
             spec.candle_source, len(bars), len(bars_by_sym))
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
        "candle_source": spec.candle_source,
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


# ── Pure-replay run ────────────────────────────────────────────────────────


def run_pure_replay(spec: RunSpec) -> BacktestResult:
    """Run a pure-replay backtest — recomputes scoring + direction from raw candles.

    Where `run_live_replay` reuses the live system's already-computed
    `scan_decisions` rows, this path throws those away and reruns
    `compute_indicators` + `determine_direction` + `score_signal` per
    bar. That's the only way to answer "would a different scoring weight
    or threshold have made money over this window?" without re-running
    the live algo.

    Steps:
      1. Resolve symbol universe (from spec or scan_decisions).
      2. Compute warmup window: warmup_since = since - warmup_days.
      3. Bulk-load bars over [warmup_since, until].
      4. Per-symbol split into (warmup_bars, in_window_bars).
      5. Load brain history over the same window → BrainTimeline.
      6. Construct SimAccount + PureReplayStrategy + Engine.
      7. Run engine over the in-window bar stream only.
      8. Hydrate metrics + per-bucket breakdowns.
    """
    log.info("pure_replay_starting label=%s since=%s until=%s tf=%s warmup_days=%d",
             spec.label, spec.since, spec.until, spec.timeframe, spec.warmup_days)

    # 1. Resolve symbol universe.
    syms: list[str]
    if spec.symbols:
        syms = sorted({s.upper().strip() for s in spec.symbols})
    elif spec.pure_universe_from_scan:
        decisions = load_scan_decisions(
            project=spec.project, dataset=spec.dataset,
            since=spec.since, until=spec.until,
            qualified_only=False, setups=None, symbols=None,
        )
        syms = sorted({d.symbol.upper().strip() for d in decisions})
        log.info("pure_replay_symbols_from_scan n=%d", len(syms))
    else:
        return _empty_result(spec, "no symbols supplied and pure_universe_from_scan=False")

    if not syms:
        return _empty_result(spec, "empty symbol universe")

    # 2. Compute warmup window.
    warmup_since = _shift_iso_date(spec.since, -spec.warmup_days)

    # 3. Bulk-load bars over the extended window.
    bars_by_sym = _load_candles(
        spec, symbols=syms, timeframe=spec.timeframe,
        since=warmup_since, until=spec.until,
    )
    total_bars = sum(len(v) for v in bars_by_sym.values())
    log.info("pure_replay_candles_loaded source=%s total_bars=%d symbols_with_bars=%d warmup_since=%s",
             spec.candle_source, total_bars, len(bars_by_sym), warmup_since)
    if total_bars == 0:
        return _empty_result(spec, "no candles in extended window")

    # 4. Per-symbol split into (warmup_bars, in_window_bars).
    since_marker = spec.since  # bar.ts < since_marker → warmup; ≥ → in-window
    warmup_bars: dict[str, list[Bar]] = {}
    in_window_bars: list[Bar] = []
    for sym, bars in bars_by_sym.items():
        warm: list[Bar] = []
        live: list[Bar] = []
        for b in bars:
            # ISO-8601 dates compare lexically; bar.ts begins with YYYY-MM-DD.
            if b.ts[:10] < since_marker:
                warm.append(b)
            else:
                live.append(b)
        if warm:
            warmup_bars[sym] = warm
        in_window_bars.extend(live)

    if not in_window_bars:
        return _empty_result(spec, "no in-window candles after warmup split")
    log.info("pure_replay_split warmup_bars=%d in_window_bars=%d",
             sum(len(v) for v in warmup_bars.values()), len(in_window_bars))

    # 5. Load brain history.
    brain_snaps = load_market_brain(
        project=spec.project, dataset=spec.dataset,
        since=warmup_since, until=spec.until,
    )
    brain = brain_lookup(brain_snaps)
    log.info("pure_replay_brain_loaded n=%d", len(brain_snaps))

    # 5b. Load 1d candles for daily-bias scoring + swing-entry gate.
    # Pulled separately because the 5m table only retains 3 months while
    # the 1d table goes back ~10 years; daily bias needs ≥50 bars and a
    # 90-calendar-day lookback comfortably covers that even on holiday-
    # heavy windows (≈63 trading days).
    daily_bars: dict[str, list[Bar]] = {}
    if spec.load_daily_bars:
        try:
            daily_since = _shift_iso_date(spec.since, -spec.daily_warmup_days)
            daily_bars = _load_candles(
                spec, symbols=syms, timeframe="1d",
                since=daily_since, until=spec.until,
            )
            log.info(
                "pure_replay_daily_loaded source=%s symbols_with_bars=%d total_bars=%d since=%s",
                spec.candle_source, len(daily_bars),
                sum(len(v) for v in daily_bars.values()), daily_since,
            )
        except Exception as e:
            log.warning("pure_replay_daily_load_failed err=%s — daily-bias path will no-op", e)
            daily_bars = {}

    # 6. Build account + strategy + engine.
    account_cfg = SimAccountConfig(
        starting_cash=spec.starting_cash,
        cost_cfg=spec.cost_cfg or CostConfig(),
    )
    account = SimAccount(cfg=account_cfg, slippage=spec.slippage or default_model())

    pr_cfg = PureReplayConfig(
        per_trade_risk_inr=spec.per_trade_risk_inr,
        min_signal_score=spec.min_signal_score,
        setups=tuple(s.upper() for s in spec.setups) if spec.setups else (
            # Mirror the universe of setups live's scanner can assign at
            # watchlist build time. Pure-replay evaluates all of them and
            # fires the highest-scoring one per bar (see
            # `PureReplayStrategy._maybe_signal_best`).
            "BREAKOUT", "VWAP_TREND", "VWAP_REVERSAL",
            "MOMENTUM", "OPEN_DRIVE",
        ),
        direction_filter=spec.direction_filter,
        max_concurrent=spec.max_concurrent,
        apply_brain_haircut=spec.apply_brain_haircut,
        apply_dynamic_min_score=spec.apply_dynamic_min_score,
        apply_swing_entry_gate=spec.apply_swing_entry_gate,
        apply_daily_bias=spec.apply_daily_bias,
        apply_strategy_entry_gate=spec.apply_strategy_entry_gate,
    )
    strategy = PureReplayStrategy(
        cfg=pr_cfg,
        strategy_settings=StrategySettings(),
        brain=brain,
        warmup_bars=warmup_bars,
        daily_bars=daily_bars,
    )

    engine = BacktestEngine(
        account=account,
        strategy=strategy,
        cfg=EngineConfig(max_open_positions=spec.max_concurrent),
    )
    result = engine.run(in_window_bars)

    # 7. Hydrate metrics + meta.
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
        "mode": "pure_replay",
        "since": spec.since,
        "until": spec.until,
        "warmup_since": warmup_since,
        "timeframe": spec.timeframe,
        "candle_source": spec.candle_source,
        "n_symbols": str(len(syms)),
        "n_warmup_bars": str(sum(len(v) for v in warmup_bars.values())),
        "n_in_window_bars": str(len(in_window_bars)),
        "n_brain_snaps": str(len(brain_snaps)),
        "min_signal_score": str(spec.min_signal_score) if spec.min_signal_score is not None else "(default)",
        "max_concurrent": str(spec.max_concurrent),
        "per_trade_risk_inr": str(spec.per_trade_risk_inr),
    })

    if spec.out_dir:
        out = Path(spec.out_dir) / spec.label
        paths = write_all(result, out_dir=out)
        log.info("pure_replay_reports_written dir=%s files=%d", out, len(paths))
        result.meta["report_dir"] = str(out)

    log.info(
        "pure_replay_done label=%s n_trades=%d net_pnl=%s sharpe=%s",
        spec.label,
        result.metrics.get("n_trades", 0),
        result.metrics.get("net_pnl", 0),
        result.metrics.get("sharpe", 0),
    )
    return result


def _load_candles(
    spec: RunSpec,
    *,
    symbols: list[str],
    timeframe: str,
    since: str,
    until: str,
) -> dict[str, list[Bar]]:
    """Dispatch to the configured candle source. Default GCS — same JSON
    files the live system reads → backtests are faithful by construction."""
    src = (spec.candle_source or "gcs").lower()
    if src == "bq":
        return load_candles_bulk_bq(
            project=spec.project, dataset=spec.dataset,
            symbols=symbols, timeframe=timeframe,
            since=since, until=until,
        )
    if src == "gcs":
        return load_candles_bulk_gcs(
            symbols=symbols, timeframe=timeframe,
            since=since, until=until,
            bucket=spec.gcs_bucket,
            exchange=spec.gcs_exchange,
            segment=spec.gcs_segment,
        )
    raise ValueError(f"unsupported candle_source: {spec.candle_source!r} (expected 'gcs' or 'bq')")


def _shift_iso_date(d: str, days: int) -> str:
    """Shift a YYYY-MM-DD date by ±N calendar days. Stays in UTC math; we only
    care about the date label, not wall-clock TZ semantics."""
    parsed = datetime.strptime(d, "%Y-%m-%d").date()
    shifted = parsed + timedelta(days=days)
    return shifted.isoformat()


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


__all__ = [
    "RunSpec",
    "run_live_replay",
    "run_pure_replay",
    "compare_to_truth",
    "TruthCompare",
]
