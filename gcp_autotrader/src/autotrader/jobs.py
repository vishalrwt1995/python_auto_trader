from __future__ import annotations

import json
import uuid

import typer

from autotrader.container import get_container
from autotrader.services.log_sink import LogSink
from autotrader.time_utils import now_ist

app = typer.Typer(add_completion=False, no_args_is_help=True, rich_markup_mode=None)


def _print(obj):
    typer.echo(json.dumps(obj, indent=2, default=str))


@app.command()
def health() -> None:
    c = get_container()
    out = {"ok": True, "project": c.settings.gcp.project_id, "region": c.settings.gcp.region, "paperTrade": c.settings.runtime.paper_trade}
    _print(out)


@app.command("bootstrap-sheets")
def bootstrap_sheets() -> None:
    c = get_container()
    _print({"ok": True, "spreadsheetId": c.settings.gcp.spreadsheet_id})


@app.command("universe-sync")
def universe_sync(limit: int = typer.Option(0)) -> None:
    c = get_container()
    sink = LogSink()
    sink.action("Universe", "sync", "START", "", {"limit": limit})
    rows = c.universe_service().sync_universe_from_upstox_instruments(limit=limit)
    sink.action("Universe", "sync", "DONE", "universe synced", {"rows": rows})
    sink.flush_all()
    _print({"rows": rows})


@app.command("raw-universe-refresh")
def raw_universe_refresh() -> None:
    c = get_container()
    sink = LogSink()
    sink.action("Universe", "raw_universe_refresh", "START")
    out = c.universe_service().refresh_raw_universe_from_upstox()
    sink.action("Universe", "raw_universe_refresh", "DONE", "upstox raw universe snapshot stored", out)
    sink.flush_all()
    _print(out)


@app.command("universe-build")
def universe_build(limit: int = typer.Option(0), replace: bool = typer.Option(False)) -> None:
    c = get_container()
    sink = LogSink()
    sink.action("Universe", "build_from_raw", "START", "", {"limit": limit, "replace": replace})
    out = c.universe_service().build_trading_universe_from_upstox_raw(limit=limit, replace=replace)
    sink.action("Universe", "build_from_raw", "DONE", "trading universe built/appended", out)
    sink.flush_all()
    _print(out)


@app.command("universe-v2-refresh")
def universe_v2_refresh(
    build_limit: int = typer.Option(0),
    replace: bool = typer.Option(False),
    candle_api_cap: int = typer.Option(600),
    run_full_backfill: bool = typer.Option(True),
    write_v2_eligibility: bool = typer.Option(False),
) -> None:
    c = get_container()
    sink = LogSink()
    sink.action(
        "Universe",
        "universe_v2_refresh",
        "START",
        "",
        {
            "buildLimit": build_limit,
            "replace": replace,
            "candleApiCap": candle_api_cap,
            "runFullBackfill": run_full_backfill,
            "writeV2Eligibility": write_v2_eligibility,
        },
    )
    out = c.universe_service().run_universe_v2_pipeline(
        build_limit=max(0, build_limit),
        replace=replace,
        candle_api_cap=max(0, candle_api_cap),
        run_full_backfill=run_full_backfill,
        write_v2_eligibility=write_v2_eligibility,
    )
    sink.action("Universe", "universe_v2_refresh", "DONE", "universe v2 pipeline complete", out)
    sink.flush_all()
    _print(out)


@app.command("universe-v2-audit")
def universe_v2_audit() -> None:
    c = get_container()
    sink = LogSink()
    sink.action("Universe", "universe_v2_audit", "START")
    out = c.universe_service().audit_universe_v2_integrity()
    sink.action("Universe", "universe_v2_audit", "DONE", "universe integrity audit complete", out)
    sink.flush_all()
    _print(out)


@app.command("upstox-token-request")
def upstox_token_request() -> None:
    c = get_container()
    out = c.upstox.request_access_token_v3()
    _print(out)


@app.command("premarket-precompute")
def premarket_precompute(
    target_size: int = typer.Option(300),
    api_cap: int = typer.Option(120),
    lookback_days: int = typer.Option(700),
    min_bars: int = typer.Option(320),
    fresh_hours: int = typer.Option(12),
    cache_only: bool = typer.Option(False),
    require_fresh_cache: bool = typer.Option(False),
    require_full_coverage: bool = typer.Option(False),
    require_today_scored: bool = typer.Option(False),
    min_watchlist_score: int = typer.Option(1),
) -> None:
    c = get_container()
    sink = LogSink()
    market_state = c.market_brain_service().build_premarket_market_brain(now_ist().isoformat())
    market_policy = c.market_brain_service().derive_market_policy(market_state)
    sink.action("Universe", "premarket_precompute", "START", "", {"targetSize": target_size})
    v2_out = c.universe_service().recompute_universe_v2_from_cache()
    wl_out = c.universe_service().build_watchlist(
        market_state,
        target_size=target_size,
        min_score=max(1, min_watchlist_score),
        require_today_scored=require_today_scored,
        require_full_coverage=require_full_coverage,
    )
    sink.action("Universe", "premarket_precompute", "DONE", "watchlist ready", {"universeV2": v2_out, "watchlist": wl_out})
    sink.flush_all()
    _print(
        {
            "regime": c.market_brain_service().watchlist_regime_payload(market_state),
            "regimeV2": c.market_brain_service().watchlist_regime_payload(market_state),
            "marketBrainState": market_state.__dict__,
            "marketPolicy": market_policy.__dict__,
            "universeV2": v2_out,
            "watchlist": wl_out,
        }
    )


@app.command("score-cache-prefetch")
def score_cache_prefetch(
    api_cap: int = typer.Option(300),
    lookback_days: int = typer.Option(700),
    min_bars: int = typer.Option(320),
) -> None:
    c = get_container()
    sink = LogSink()
    sink.action("Universe", "score_cache_prefetch", "START", "", {"apiCap": api_cap, "lookbackDays": lookback_days, "minBars": min_bars})
    out = c.universe_service().prefetch_score_cache_batch(api_cap=max(0, api_cap), lookback_days=lookback_days, min_bars=min_bars)
    sink.action("Universe", "score_cache_prefetch", "DONE", "score cache prefetch complete", out)
    sink.flush_all()
    _print(out)


@app.command("score-cache-backfill-full")
def score_cache_backfill_full(
    api_cap: int = typer.Option(600),
    lookback_days: int = typer.Option(9500),
    min_bars: int = typer.Option(320),
) -> None:
    c = get_container()
    sink = LogSink()
    sink.action("Universe", "score_cache_backfill_full", "START", "", {"apiCap": api_cap, "lookbackDays": lookback_days, "minBars": min_bars})
    out = c.universe_service().prefetch_score_cache_batch(api_cap=max(0, api_cap), lookback_days=max(3650, lookback_days), min_bars=min_bars)
    sink.action("Universe", "score_cache_backfill_full", "DONE", "full score-cache backfill batch complete", out)
    sink.flush_all()
    _print(out)


@app.command("score-cache-update-close")
def score_cache_update_close(
    api_cap: int = typer.Option(600),
    lookback_days: int = typer.Option(700),
    min_bars: int = typer.Option(320),
    retry_stale_terminal_today: bool = typer.Option(False),
) -> None:
    c = get_container()
    sink = LogSink()
    sink.action("Universe", "score_cache_update_close", "START", "", {"apiCap": api_cap, "lookbackDays": lookback_days, "minBars": min_bars, "retryStaleTerminalToday": retry_stale_terminal_today})
    out = c.universe_service().prefetch_score_cache_batch(
        api_cap=max(0, api_cap),
        lookback_days=lookback_days,
        min_bars=min_bars,
        retry_stale_terminal_today=retry_stale_terminal_today,
    )
    sink.action("Universe", "score_cache_update_close", "DONE", "daily score-cache update batch complete", out)
    sink.flush_all()
    _print(out)


@app.command("scan-once")
def scan_once(
    force: bool = typer.Option(False),
    allow_live_orders: bool = typer.Option(False, help="Unsafe unless paper mode is disabled and broker mapping validated."),
) -> None:
    c = get_container()
    out = c.trading_service().run_scan_once(allow_live_orders=allow_live_orders, force=force)
    _print(out)


@app.command("bq-backfill-candles-1d")
def bq_backfill_candles_1d() -> None:
    """Backfill candles_1d BQ table from existing GCS score_1d cache files."""
    c = get_container()
    sink = LogSink()
    sink.action("Universe", "bq_backfill_candles_1d", "START")
    out = c.universe_service().backfill_candles_1d_to_bq()
    sink.action("Universe", "bq_backfill_candles_1d", "DONE", "BQ candles backfill complete", out)
    sink.flush_all()
    _print(out)


@app.command("reset-runtime")
def reset_runtime() -> None:
    c = get_container()
    cleared = c.state.delete_runtime_prefix(("runtime:", "entry:", "exit:", "fired:", "pending_"))
    _print({"cleared": cleared})


@app.command("version")
def version() -> None:
    _print({"version": "0.1.0", "build": uuid.uuid4().hex[:8]})


if __name__ == "__main__":
    # Do not eagerly resolve runtime settings here. This allows commands like
    # `--help` and `version` to work without requiring all cloud env vars.
    app()
