from __future__ import annotations

import logging
import secrets
import time
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Query, Request

from autotrader.container import get_container
from autotrader.services.log_sink import LogSink
from autotrader.time_utils import now_utc, parse_any_ts

app = FastAPI(title="GCP AutoTrader", version="0.1.0")
logger = logging.getLogger(__name__)


def _auth(expected: str, supplied: str | None) -> None:
    if not supplied or not secrets.compare_digest(expected, supplied):
        raise HTTPException(status_code=401, detail="Unauthorized")


def _scheduler_ctx(job_name: str | None, schedule_time: str | None) -> dict[str, Any]:
    ctx: dict[str, Any] = {"triggerSource": "manual_or_unknown"}
    if job_name:
        ctx["triggerSource"] = "cloud_scheduler"
        ctx["schedulerJob"] = job_name
    if schedule_time:
        ctx["schedulerScheduleTime"] = schedule_time
        ts = parse_any_ts(schedule_time)
        if ts is not None:
            lag = (now_utc().astimezone(ts.tzinfo) - ts).total_seconds()
            ctx["schedulerLagSec"] = round(lag, 3)
    return ctx


def _duration_ctx(started_perf: float) -> dict[str, Any]:
    return {"durationSec": round(time.perf_counter() - started_perf, 3)}


def _write_market_brain_best_effort(c, regime: Any) -> None:
    try:
        c.sheets.write_market_brain(regime)
    except Exception:
        logger.exception("market_brain_write_failed")


def _acquire_named_locks(state, names: list[str], *, ttl_seconds: int) -> tuple[list[Any], str | None]:
    leases: list[Any] = []
    for name in names:
        lease = state.try_acquire_lock(name, ttl_seconds=ttl_seconds)
        if lease is None:
            for held in reversed(leases):
                state.release_lock(held)
            return [], name
        leases.append(lease)
    return leases, None


def _release_named_locks(state, leases: list[Any]) -> None:
    for lease in reversed(leases):
        try:
            state.release_lock(lease)
        except Exception:
            logger.exception("lock_release_failed name=%s", getattr(lease, "name", "unknown"))


@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    started = time.perf_counter()
    sched_job = request.headers.get("X-CloudScheduler-JobName", "")
    sched_time = request.headers.get("X-CloudScheduler-ScheduleTime", "")
    try:
        resp = await call_next(request)
        logger.info(
            "http_request method=%s path=%s status=%s durationSec=%.3f schedulerJob=%s schedulerScheduleTime=%s",
            request.method,
            request.url.path,
            getattr(resp, "status_code", 0),
            time.perf_counter() - started,
            sched_job or "-",
            sched_time or "-",
        )
        return resp
    except Exception:
        logger.exception(
            "http_request_error method=%s path=%s durationSec=%.3f schedulerJob=%s schedulerScheduleTime=%s",
            request.method,
            request.url.path,
            time.perf_counter() - started,
            sched_job or "-",
            sched_time or "-",
        )
        raise


@app.get("/healthz")
def healthz() -> dict[str, Any]:
    c = get_container()
    return {
        "ok": True,
        "project": c.settings.gcp.project_id,
        "region": c.settings.gcp.region,
        "paperTrade": c.settings.runtime.paper_trade,
    }


@app.post("/jobs/bootstrap-sheets")
def run_bootstrap_sheets(
    x_job_token: str | None = Header(default=None),
    x_cloudscheduler_jobname: str | None = Header(default=None, alias="X-CloudScheduler-JobName"),
    x_cloudscheduler_scheduletime: str | None = Header(default=None, alias="X-CloudScheduler-ScheduleTime"),
) -> dict[str, Any]:
    c = get_container()
    _auth(c.settings.runtime.job_trigger_token, x_job_token)
    sink = LogSink(c.sheets)
    sched_ctx = _scheduler_ctx(x_cloudscheduler_jobname, x_cloudscheduler_scheduletime)
    started_perf = time.perf_counter()
    try:
        sink.action("System", "bootstrap_sheets", "START", "", sched_ctx)
        c.sheets.ensure_core_sheets()
        out = {"ok": True}
        sink.action("System", "bootstrap_sheets", "DONE", "core sheets ensured", {**sched_ctx, **_duration_ctx(started_perf), **out})
        sink.flush_all()
        return out
    except Exception as e:
        sink.action("System", "bootstrap_sheets", "ERROR", f"{type(e).__name__}: {e}", {**sched_ctx, **_duration_ctx(started_perf), "errorType": type(e).__name__})
        sink.flush_all()
        raise


@app.post("/jobs/universe-sync")
def run_universe_sync(
    x_job_token: str | None = Header(default=None),
    limit: int = 0,
    x_cloudscheduler_jobname: str | None = Header(default=None, alias="X-CloudScheduler-JobName"),
    x_cloudscheduler_scheduletime: str | None = Header(default=None, alias="X-CloudScheduler-ScheduleTime"),
) -> dict[str, Any]:
    c = get_container()
    _auth(c.settings.runtime.job_trigger_token, x_job_token)
    c.sheets.ensure_core_sheets()
    sink = LogSink(c.sheets)
    sched_ctx = _scheduler_ctx(x_cloudscheduler_jobname, x_cloudscheduler_scheduletime)
    started_perf = time.perf_counter()
    try:
        sink.action("Universe", "sync", "START", "", {**sched_ctx, "limit": limit})
        rows = c.universe_service().sync_universe_from_groww_instruments(limit=limit)
        out = {"rows": rows}
        sink.action("Universe", "sync", "DONE", "universe synced", {**sched_ctx, **_duration_ctx(started_perf), **out})
        sink.flush_all()
        return out
    except Exception as e:
        sink.action("Universe", "sync", "ERROR", f"{type(e).__name__}: {e}", {**sched_ctx, **_duration_ctx(started_perf), "errorType": type(e).__name__, "limit": limit})
        sink.flush_all()
        raise


@app.post("/jobs/raw-universe-refresh")
def run_raw_universe_refresh(
    x_job_token: str | None = Header(default=None),
    x_cloudscheduler_jobname: str | None = Header(default=None, alias="X-CloudScheduler-JobName"),
    x_cloudscheduler_scheduletime: str | None = Header(default=None, alias="X-CloudScheduler-ScheduleTime"),
) -> dict[str, Any]:
    c = get_container()
    _auth(c.settings.runtime.job_trigger_token, x_job_token)
    c.sheets.ensure_core_sheets()
    sink = LogSink(c.sheets)
    sched_ctx = _scheduler_ctx(x_cloudscheduler_jobname, x_cloudscheduler_scheduletime)
    lease = c.state.try_acquire_lock("raw_universe_refresh", ttl_seconds=1800)
    if lease is None:
        sink.action("Universe", "raw_universe_refresh", "LOCK_BUSY", "skipped: lock busy", sched_ctx)
        sink.flush_all()
        return {"skipped": "lock_busy"}
    started_perf = time.perf_counter()
    try:
        sink.action("Universe", "raw_universe_refresh", "START", "", sched_ctx)
        out = c.universe_service().refresh_raw_universe_from_upstox()
        sink.action("Universe", "raw_universe_refresh", "DONE", "upstox raw universe snapshot stored", {**sched_ctx, **_duration_ctx(started_perf), **out})
        sink.flush_all()
        return out
    except Exception as e:
        sink.action(
            "Universe",
            "raw_universe_refresh",
            "ERROR",
            f"{type(e).__name__}: {e}",
            {**sched_ctx, **_duration_ctx(started_perf), "errorType": type(e).__name__},
        )
        sink.flush_all()
        raise
    finally:
        c.state.release_lock(lease)


@app.post("/jobs/universe-build")
def run_universe_build(
    x_job_token: str | None = Header(default=None),
    limit: int = 0,
    replace: bool = False,
    x_cloudscheduler_jobname: str | None = Header(default=None, alias="X-CloudScheduler-JobName"),
    x_cloudscheduler_scheduletime: str | None = Header(default=None, alias="X-CloudScheduler-ScheduleTime"),
) -> dict[str, Any]:
    c = get_container()
    _auth(c.settings.runtime.job_trigger_token, x_job_token)
    c.sheets.ensure_core_sheets()
    sink = LogSink(c.sheets)
    sched_ctx = _scheduler_ctx(x_cloudscheduler_jobname, x_cloudscheduler_scheduletime)
    lease = c.state.try_acquire_lock("universe_build", ttl_seconds=1800)
    if lease is None:
        sink.action("Universe", "build_from_raw", "LOCK_BUSY", "skipped: lock busy", {**sched_ctx, "limit": limit, "replace": replace})
        sink.flush_all()
        return {"skipped": "lock_busy"}
    started_perf = time.perf_counter()
    try:
        sink.action("Universe", "build_from_raw", "START", "", {**sched_ctx, "limit": limit, "replace": replace})
        out = c.universe_service().build_trading_universe_from_upstox_raw(limit=limit, replace=replace)
        sink.action("Universe", "build_from_raw", "DONE", "trading universe built/appended", {**sched_ctx, **_duration_ctx(started_perf), **out})
        sink.flush_all()
        return out
    except Exception as e:
        sink.action(
            "Universe",
            "build_from_raw",
            "ERROR",
            f"{type(e).__name__}: {e}",
            {**sched_ctx, **_duration_ctx(started_perf), "errorType": type(e).__name__, "limit": limit, "replace": replace},
        )
        sink.flush_all()
        raise
    finally:
        c.state.release_lock(lease)


@app.post("/jobs/upstox-token-request")
def run_upstox_token_request(
    x_job_token: str | None = Header(default=None),
    x_cloudscheduler_jobname: str | None = Header(default=None, alias="X-CloudScheduler-JobName"),
    x_cloudscheduler_scheduletime: str | None = Header(default=None, alias="X-CloudScheduler-ScheduleTime"),
) -> dict[str, Any]:
    c = get_container()
    _auth(c.settings.runtime.job_trigger_token, x_job_token)
    sink = LogSink(c.sheets)
    sched_ctx = _scheduler_ctx(x_cloudscheduler_jobname, x_cloudscheduler_scheduletime)
    started_perf = time.perf_counter()
    try:
        sink.action("Auth", "upstox_token_request", "START", "", sched_ctx)
        out = c.upstox.request_access_token_v3()
        sink.action("Auth", "upstox_token_request", "DONE", "token request initiated", {**sched_ctx, **_duration_ctx(started_perf), **out})
        sink.flush_all()
        return out
    except Exception as e:
        sink.action("Auth", "upstox_token_request", "ERROR", f"{type(e).__name__}: {e}", {**sched_ctx, **_duration_ctx(started_perf), "errorType": type(e).__name__})
        sink.flush_all()
        raise


@app.post("/webhooks/upstox/access-token")
async def upstox_access_token_webhook(
    request: Request,
    shared_secret: str | None = Query(default=None),
) -> dict[str, Any]:
    c = get_container()
    expected = (c.settings.upstox.notifier_shared_secret or "").strip()
    if expected and (not shared_secret or not secrets.compare_digest(expected, shared_secret)):
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    out = c.upstox.ingest_notifier_payload(payload)
    return {"ok": True, **out}


@app.post("/jobs/premarket-precompute")
def run_premarket_precompute(
    x_job_token: str | None = Header(default=None),
    target_size: int = 300,
    api_cap: int = 120,
    fresh_hours: int = 12,
    cache_only: bool = False,
    require_fresh_cache: bool = False,
    require_full_coverage: bool = False,
    require_today_scored: bool = False,
    min_watchlist_score: int = 1,
    x_cloudscheduler_jobname: str | None = Header(default=None, alias="X-CloudScheduler-JobName"),
    x_cloudscheduler_scheduletime: str | None = Header(default=None, alias="X-CloudScheduler-ScheduleTime"),
) -> dict[str, Any]:
    c = get_container()
    _auth(c.settings.runtime.job_trigger_token, x_job_token)
    c.sheets.ensure_core_sheets()
    sink = LogSink(c.sheets)
    sched_ctx = _scheduler_ctx(x_cloudscheduler_jobname, x_cloudscheduler_scheduletime)
    lease = c.state.try_acquire_lock("premarket_precompute", ttl_seconds=3600)
    if lease is None:
        sink.action(
            "Universe",
            "premarket_precompute",
            "LOCK_BUSY",
            "skipped: lock busy",
            {
                **sched_ctx,
                "targetSize": target_size,
                "apiCap": api_cap,
                "cacheOnly": cache_only,
                "requireFreshCache": require_fresh_cache,
                "requireFullCoverage": require_full_coverage,
                "requireTodayScored": require_today_scored,
            },
        )
        sink.flush_all()
        return {"skipped": "lock_busy"}
    started_perf = time.perf_counter()
    try:
        sink.action(
            "Universe",
            "premarket_precompute",
            "START",
            "",
            {
                **sched_ctx,
                "targetSize": target_size,
                "apiCap": api_cap,
                "freshHours": fresh_hours,
                "cacheOnly": cache_only,
                "requireFreshCache": require_fresh_cache,
                "requireFullCoverage": require_full_coverage,
                "requireTodayScored": require_today_scored,
                "minWatchlistScore": min_watchlist_score,
            },
        )
        regime = c.regime_service().get_market_regime()
        _write_market_brain_best_effort(c, regime)
        score_out = c.universe_service().score_universe_batch(
            regime,
            api_cap=max(0, api_cap),
            fresh_hours=max(0, fresh_hours),
            sheet_write_batch_size=200,
            cache_only=cache_only,
            require_fresh_cache=require_fresh_cache,
        )
        wl_out = c.universe_service().build_watchlist(
            regime,
            target_size=target_size,
            min_score=max(1, min_watchlist_score),
            require_today_scored=require_today_scored,
            require_full_coverage=require_full_coverage,
        )
        done_message = "watchlist ready" if bool(wl_out.get("ready")) and int(wl_out.get("selected", 0) or 0) > 0 else "watchlist blocked"
        sink.action(
            "Universe",
            "premarket_precompute",
            "DONE",
            done_message,
            {**sched_ctx, **_duration_ctx(started_perf), "score": score_out, "watchlist": wl_out},
        )
        sink.flush_all()
        return {"regime": regime.__dict__, "score": score_out, "watchlist": wl_out}
    except Exception as e:
        sink.action(
            "Universe",
            "premarket_precompute",
            "ERROR",
            f"{type(e).__name__}: {e}",
            {
                **sched_ctx,
                **_duration_ctx(started_perf),
                "errorType": type(e).__name__,
                "targetSize": target_size,
                "apiCap": api_cap,
            },
        )
        sink.flush_all()
        raise
    finally:
        c.state.release_lock(lease)


@app.post("/jobs/watchlist-refresh")
def run_watchlist_refresh(
    x_job_token: str | None = Header(default=None),
    target_size: int = 300,
    require_full_coverage: bool = False,
    require_today_scored: bool = False,
    min_watchlist_score: int = 1,
    x_cloudscheduler_jobname: str | None = Header(default=None, alias="X-CloudScheduler-JobName"),
    x_cloudscheduler_scheduletime: str | None = Header(default=None, alias="X-CloudScheduler-ScheduleTime"),
) -> dict[str, Any]:
    c = get_container()
    _auth(c.settings.runtime.job_trigger_token, x_job_token)
    c.sheets.ensure_core_sheets()
    sink = LogSink(c.sheets)
    sched_ctx = _scheduler_ctx(x_cloudscheduler_jobname, x_cloudscheduler_scheduletime)
    lease = c.state.try_acquire_lock("watchlist_refresh", ttl_seconds=1800)
    if lease is None:
        sink.action(
            "Universe",
            "watchlist_refresh",
            "LOCK_BUSY",
            "skipped: lock busy",
            {
                **sched_ctx,
                "targetSize": target_size,
                "requireFullCoverage": require_full_coverage,
                "requireTodayScored": require_today_scored,
                "minWatchlistScore": min_watchlist_score,
            },
        )
        sink.flush_all()
        return {"skipped": "lock_busy"}
    started_perf = time.perf_counter()
    try:
        sink.action(
            "Universe",
            "watchlist_refresh",
            "START",
            "",
            {
                **sched_ctx,
                "targetSize": target_size,
                "requireFullCoverage": require_full_coverage,
                "requireTodayScored": require_today_scored,
                "minWatchlistScore": min_watchlist_score,
            },
        )
        regime = c.regime_service().get_market_regime()
        _write_market_brain_best_effort(c, regime)
        wl_out = c.universe_service().build_watchlist(
            regime,
            target_size=target_size,
            min_score=max(1, min_watchlist_score),
            require_today_scored=require_today_scored,
            require_full_coverage=require_full_coverage,
        )
        done_message = "watchlist ready" if bool(wl_out.get("ready")) and int(wl_out.get("selected", 0) or 0) > 0 else "watchlist blocked"
        sink.action(
            "Universe",
            "watchlist_refresh",
            "DONE",
            done_message,
            {**sched_ctx, **_duration_ctx(started_perf), "watchlist": wl_out, "regime": regime.__dict__},
        )
        sink.flush_all()
        return {"regime": regime.__dict__, "watchlist": wl_out}
    except Exception as e:
        sink.action(
            "Universe",
            "watchlist_refresh",
            "ERROR",
            f"{type(e).__name__}: {e}",
            {
                **sched_ctx,
                **_duration_ctx(started_perf),
                "errorType": type(e).__name__,
                "targetSize": target_size,
            },
        )
        sink.flush_all()
        raise
    finally:
        c.state.release_lock(lease)


@app.post("/jobs/score-refresh")
def run_score_refresh(
    x_job_token: str | None = Header(default=None),
    api_cap: int = 120,
    fresh_hours: int = 18,
    cache_only: bool = False,
    require_fresh_cache: bool = False,
    x_cloudscheduler_jobname: str | None = Header(default=None, alias="X-CloudScheduler-JobName"),
    x_cloudscheduler_scheduletime: str | None = Header(default=None, alias="X-CloudScheduler-ScheduleTime"),
) -> dict[str, Any]:
    c = get_container()
    _auth(c.settings.runtime.job_trigger_token, x_job_token)
    c.sheets.ensure_core_sheets()
    sink = LogSink(c.sheets)
    sched_ctx = _scheduler_ctx(x_cloudscheduler_jobname, x_cloudscheduler_scheduletime)
    lease = c.state.try_acquire_lock("score_refresh", ttl_seconds=3600)
    if lease is None:
        sink.action(
            "Universe",
            "score_refresh",
            "LOCK_BUSY",
            "skipped: lock busy",
            {
                **sched_ctx,
                "apiCap": api_cap,
                "freshHours": fresh_hours,
                "cacheOnly": cache_only,
                "requireFreshCache": require_fresh_cache,
            },
        )
        sink.flush_all()
        return {"skipped": "lock_busy"}
    started_perf = time.perf_counter()
    try:
        sink.action(
            "Universe",
            "score_refresh",
            "START",
            "",
            {
                **sched_ctx,
                "apiCap": api_cap,
                "freshHours": fresh_hours,
                "cacheOnly": cache_only,
                "requireFreshCache": require_fresh_cache,
            },
        )
        regime = c.regime_service().get_market_regime()
        _write_market_brain_best_effort(c, regime)
        score_out = c.universe_service().score_universe_batch(
            regime,
            api_cap=max(0, api_cap),
            fresh_hours=max(0, fresh_hours),
            sheet_write_batch_size=200,
            cache_only=cache_only,
            require_fresh_cache=require_fresh_cache,
        )
        sink.action(
            "Universe",
            "score_refresh",
            "DONE",
            "universe scoring complete",
            {**sched_ctx, **_duration_ctx(started_perf), "regime": regime.__dict__, "score": score_out},
        )
        sink.flush_all()
        return {"regime": regime.__dict__, "score": score_out}
    except Exception as e:
        sink.action(
            "Universe",
            "score_refresh",
            "ERROR",
            f"{type(e).__name__}: {e}",
            {
                **sched_ctx,
                **_duration_ctx(started_perf),
                "errorType": type(e).__name__,
                "apiCap": api_cap,
                "freshHours": fresh_hours,
            },
        )
        sink.flush_all()
        raise
    finally:
        c.state.release_lock(lease)


@app.post("/jobs/score-cache-prefetch")
def run_score_cache_prefetch(
    x_job_token: str | None = Header(default=None),
    api_cap: int = 300,
    lookback_days: int = 700,
    min_bars: int = 320,
    x_cloudscheduler_jobname: str | None = Header(default=None, alias="X-CloudScheduler-JobName"),
    x_cloudscheduler_scheduletime: str | None = Header(default=None, alias="X-CloudScheduler-ScheduleTime"),
) -> dict[str, Any]:
    c = get_container()
    _auth(c.settings.runtime.job_trigger_token, x_job_token)
    c.sheets.ensure_core_sheets()
    sink = LogSink(c.sheets)
    sched_ctx = _scheduler_ctx(x_cloudscheduler_jobname, x_cloudscheduler_scheduletime)
    lease = c.state.try_acquire_lock("score_cache_prefetch", ttl_seconds=3600)
    if lease is None:
        sink.action("Universe", "score_cache_prefetch", "LOCK_BUSY", "skipped: lock busy", {**sched_ctx, "apiCap": api_cap, "lookbackDays": lookback_days, "minBars": min_bars})
        sink.flush_all()
        return {"skipped": "lock_busy"}
    started_perf = time.perf_counter()
    try:
        sink.action("Universe", "score_cache_prefetch", "START", "", {**sched_ctx, "apiCap": api_cap, "lookbackDays": lookback_days, "minBars": min_bars})
        out = c.universe_service().prefetch_score_cache_batch(
            api_cap=max(0, api_cap),
            lookback_days=lookback_days,
            min_bars=min_bars,
        )
        sink.action("Universe", "score_cache_prefetch", "DONE", "score cache prefetch complete", {**sched_ctx, **_duration_ctx(started_perf), **out})
        sink.flush_all()
        return out
    except Exception as e:
        sink.action(
            "Universe",
            "score_cache_prefetch",
            "ERROR",
            f"{type(e).__name__}: {e}",
            {**sched_ctx, **_duration_ctx(started_perf), "errorType": type(e).__name__, "apiCap": api_cap, "lookbackDays": lookback_days, "minBars": min_bars},
        )
        sink.flush_all()
        raise
    finally:
        c.state.release_lock(lease)


@app.post("/jobs/score-cache-backfill-full")
def run_score_cache_backfill_full(
    x_job_token: str | None = Header(default=None),
    api_cap: int = 600,
    lookback_days: int = 9500,
    min_bars: int = 320,
    x_cloudscheduler_jobname: str | None = Header(default=None, alias="X-CloudScheduler-JobName"),
    x_cloudscheduler_scheduletime: str | None = Header(default=None, alias="X-CloudScheduler-ScheduleTime"),
) -> dict[str, Any]:
    c = get_container()
    _auth(c.settings.runtime.job_trigger_token, x_job_token)
    c.sheets.ensure_core_sheets()
    sink = LogSink(c.sheets)
    sched_ctx = _scheduler_ctx(x_cloudscheduler_jobname, x_cloudscheduler_scheduletime)
    lease = c.state.try_acquire_lock("score_cache_backfill_full", ttl_seconds=3600)
    if lease is None:
        sink.action("Universe", "score_cache_backfill_full", "LOCK_BUSY", "skipped: lock busy", {**sched_ctx, "apiCap": api_cap, "lookbackDays": lookback_days, "minBars": min_bars})
        sink.flush_all()
        return {"skipped": "lock_busy"}
    started_perf = time.perf_counter()
    try:
        sink.action("Universe", "score_cache_backfill_full", "START", "", {**sched_ctx, "apiCap": api_cap, "lookbackDays": lookback_days, "minBars": min_bars})
        out = c.universe_service().prefetch_score_cache_batch(api_cap=max(0, api_cap), lookback_days=max(3650, lookback_days), min_bars=min_bars)
        sink.action("Universe", "score_cache_backfill_full", "DONE", "full score-cache backfill batch complete", {**sched_ctx, **_duration_ctx(started_perf), **out})
        sink.flush_all()
        return out
    except Exception as e:
        sink.action(
            "Universe",
            "score_cache_backfill_full",
            "ERROR",
            f"{type(e).__name__}: {e}",
            {**sched_ctx, **_duration_ctx(started_perf), "errorType": type(e).__name__, "apiCap": api_cap, "lookbackDays": lookback_days, "minBars": min_bars},
        )
        sink.flush_all()
        raise
    finally:
        c.state.release_lock(lease)


@app.post("/jobs/score-cache-update-close")
def run_score_cache_update_close(
    x_job_token: str | None = Header(default=None),
    api_cap: int = 600,
    lookback_days: int = 700,
    min_bars: int = 320,
    x_cloudscheduler_jobname: str | None = Header(default=None, alias="X-CloudScheduler-JobName"),
    x_cloudscheduler_scheduletime: str | None = Header(default=None, alias="X-CloudScheduler-ScheduleTime"),
) -> dict[str, Any]:
    c = get_container()
    _auth(c.settings.runtime.job_trigger_token, x_job_token)
    c.sheets.ensure_core_sheets()
    sink = LogSink(c.sheets)
    sched_ctx = _scheduler_ctx(x_cloudscheduler_jobname, x_cloudscheduler_scheduletime)
    lease = c.state.try_acquire_lock("score_cache_update_close", ttl_seconds=3600)
    if lease is None:
        sink.action("Universe", "score_cache_update_close", "LOCK_BUSY", "skipped: lock busy", {**sched_ctx, "apiCap": api_cap, "lookbackDays": lookback_days, "minBars": min_bars})
        sink.flush_all()
        return {"skipped": "lock_busy"}
    started_perf = time.perf_counter()
    try:
        sink.action("Universe", "score_cache_update_close", "START", "", {**sched_ctx, "apiCap": api_cap, "lookbackDays": lookback_days, "minBars": min_bars})
        out = c.universe_service().prefetch_score_cache_batch(
            api_cap=max(0, api_cap),
            lookback_days=lookback_days,
            min_bars=min_bars,
            retry_stale_terminal_today=True,
        )
        sink.action("Universe", "score_cache_update_close", "DONE", "daily score-cache update batch complete", {**sched_ctx, **_duration_ctx(started_perf), **out})
        sink.flush_all()
        return out
    except Exception as e:
        sink.action(
            "Universe",
            "score_cache_update_close",
            "ERROR",
            f"{type(e).__name__}: {e}",
            {**sched_ctx, **_duration_ctx(started_perf), "errorType": type(e).__name__, "apiCap": api_cap, "lookbackDays": lookback_days, "minBars": min_bars},
        )
        sink.flush_all()
        raise
    finally:
        c.state.release_lock(lease)


@app.post("/jobs/universe-refresh-append-backfill")
def run_universe_refresh_append_backfill(
    x_job_token: str | None = Header(default=None),
    raw_limit: int = 0,
    build_limit: int = 0,
    replace: bool = False,
    run_backfill: bool = True,
    backfill_max_passes: int = 2,
    backfill_api_cap: int = 300,
    backfill_lookback_days: int = 9500,
    min_bars: int = 320,
    run_score_refresh: bool = False,
    score_api_cap: int = 0,
    score_fresh_hours: int = 18,
    x_cloudscheduler_jobname: str | None = Header(default=None, alias="X-CloudScheduler-JobName"),
    x_cloudscheduler_scheduletime: str | None = Header(default=None, alias="X-CloudScheduler-ScheduleTime"),
) -> dict[str, Any]:
    c = get_container()
    _auth(c.settings.runtime.job_trigger_token, x_job_token)
    c.sheets.ensure_core_sheets()
    sink = LogSink(c.sheets)
    sched_ctx = _scheduler_ctx(x_cloudscheduler_jobname, x_cloudscheduler_scheduletime)
    lock_names = ["raw_universe_refresh", "universe_build"]
    if run_backfill:
        lock_names.append("score_cache_backfill_full")
    if run_score_refresh:
        lock_names.append("score_refresh")
    leases, blocked_lock = _acquire_named_locks(c.state, lock_names, ttl_seconds=7200)
    if blocked_lock is not None:
        sink.action(
            "Universe",
            "universe_refresh_append_backfill",
            "LOCK_BUSY",
            "skipped: lock busy",
            {
                **sched_ctx,
                "blockedLock": blocked_lock,
                "rawLimit": raw_limit,
                "buildLimit": build_limit,
                "replace": replace,
                "runBackfill": run_backfill,
                "backfillMaxPasses": backfill_max_passes,
                "runScoreRefresh": run_score_refresh,
            },
        )
        sink.flush_all()
        return {"skipped": "lock_busy", "blockedLock": blocked_lock}
    started_perf = time.perf_counter()
    try:
        sink.action(
            "Universe",
            "universe_refresh_append_backfill",
            "START",
            "",
            {
                **sched_ctx,
                "rawLimit": raw_limit,
                "buildLimit": build_limit,
                "replace": replace,
                "runBackfill": run_backfill,
                "backfillMaxPasses": backfill_max_passes,
                "backfillApiCap": backfill_api_cap,
                "backfillLookbackDays": backfill_lookback_days,
                "minBars": min_bars,
                "runScoreRefresh": run_score_refresh,
                "scoreApiCap": score_api_cap,
                "scoreFreshHours": score_fresh_hours,
            },
        )
        raw_out = c.universe_service().refresh_raw_universe_from_upstox()
        build_out = c.universe_service().build_trading_universe_from_upstox_raw(limit=max(0, build_limit), replace=replace)
        appended = int(build_out.get("appended", 0) or 0)
        priority_symbols: list[str] = []
        if appended > 0 and not replace:
            # Newly appended rows are appended at the bottom of the universe sheet. Prioritize them in the kickoff backfill
            # so the 06:15 chained job updates their history immediately even when api_cap is reached.
            try:
                universe_rows = c.sheets.read_universe_rows()
                priority_symbols = [u.symbol for u in universe_rows[-appended:]] if appended <= len(universe_rows) else []
            except Exception:
                priority_symbols = []
        backfill_runs: list[dict[str, Any]] = []
        backfill_skipped_reason = ""
        if run_backfill and not replace and appended <= 0:
            backfill_skipped_reason = "no_new_instruments_appended"
        elif run_backfill:
            for i in range(max(1, backfill_max_passes)):
                out = c.universe_service().prefetch_score_cache_batch(
                    api_cap=max(0, backfill_api_cap),
                    lookback_days=max(3650, backfill_lookback_days),
                    min_bars=min_bars,
                    priority_symbols=priority_symbols if i == 0 else None,
                )
                pass_out = {"pass": i + 1, **out}
                backfill_runs.append(pass_out)
                if bool(out.get("prefillComplete")):
                    break
                if int(out.get("fetches", 0) or 0) == 0 and int(out.get("staleOrMissing", 0) or 0) == 0:
                    break
        score_out: dict[str, Any] | None = None
        regime_dict: dict[str, Any] | None = None
        if run_score_refresh:
            regime = c.regime_service().get_market_regime()
            _write_market_brain_best_effort(c, regime)
            regime_dict = regime.__dict__
            score_out = c.universe_service().score_universe_batch(
                regime,
                api_cap=max(0, score_api_cap),
                fresh_hours=max(0, score_fresh_hours),
                sheet_write_batch_size=200,
                cache_only=True,
                require_fresh_cache=True,
            )
        out: dict[str, Any] = {
            "raw": raw_out,
            "build": build_out,
            "backfill": {
                "requested": run_backfill,
                "runs": backfill_runs,
                "skippedReason": backfill_skipped_reason,
                "last": backfill_runs[-1] if backfill_runs else None,
            },
        }
        if run_score_refresh:
            out["regime"] = regime_dict or {}
            out["score"] = score_out or {}
        sink.action(
            "Universe",
            "universe_refresh_append_backfill",
            "DONE",
            "raw refresh + universe append pipeline complete",
            {**sched_ctx, **_duration_ctx(started_perf), **out},
        )
        sink.flush_all()
        return out
    except Exception as e:
        sink.action(
            "Universe",
            "universe_refresh_append_backfill",
            "ERROR",
            f"{type(e).__name__}: {e}",
            {
                **sched_ctx,
                **_duration_ctx(started_perf),
                "errorType": type(e).__name__,
                "rawLimit": raw_limit,
                "buildLimit": build_limit,
                "replace": replace,
                "runBackfill": run_backfill,
                "runScoreRefresh": run_score_refresh,
            },
        )
        sink.flush_all()
        raise
    finally:
        _release_named_locks(c.state, leases)


@app.post("/jobs/eod-close-update-score")
def run_eod_close_update_score(
    x_job_token: str | None = Header(default=None),
    close_api_cap: int = 600,
    close_lookback_days: int = 700,
    min_bars: int = 320,
    close_max_passes: int = 3,
    allow_provisional_intraday: bool = False,
    score_api_cap: int = 0,
    score_fresh_hours: int = 0,
    score_when_complete_only: bool = True,
    x_cloudscheduler_jobname: str | None = Header(default=None, alias="X-CloudScheduler-JobName"),
    x_cloudscheduler_scheduletime: str | None = Header(default=None, alias="X-CloudScheduler-ScheduleTime"),
) -> dict[str, Any]:
    c = get_container()
    _auth(c.settings.runtime.job_trigger_token, x_job_token)
    c.sheets.ensure_core_sheets()
    sink = LogSink(c.sheets)
    sched_ctx = _scheduler_ctx(x_cloudscheduler_jobname, x_cloudscheduler_scheduletime)
    leases, blocked_lock = _acquire_named_locks(
        c.state,
        ["score_cache_update_close", "score_cache_backfill_full", "score_refresh"],
        ttl_seconds=7200,
    )
    if blocked_lock is not None:
        sink.action(
            "Universe",
            "eod_close_update_score",
            "LOCK_BUSY",
            "skipped: lock busy",
            {
                **sched_ctx,
                "blockedLock": blocked_lock,
                "closeApiCap": close_api_cap,
                "closeLookbackDays": close_lookback_days,
                "minBars": min_bars,
                "closeMaxPasses": close_max_passes,
                "allowProvisionalIntraday": allow_provisional_intraday,
                "scoreWhenCompleteOnly": score_when_complete_only,
            },
        )
        sink.flush_all()
        return {"skipped": "lock_busy", "blockedLock": blocked_lock}
    started_perf = time.perf_counter()
    try:
        sink.action(
            "Universe",
            "eod_close_update_score",
            "START",
            "",
            {
                **sched_ctx,
                "closeApiCap": close_api_cap,
                "closeLookbackDays": close_lookback_days,
                "minBars": min_bars,
                "closeMaxPasses": close_max_passes,
                "allowProvisionalIntraday": allow_provisional_intraday,
                "scoreApiCap": score_api_cap,
                "scoreFreshHours": score_fresh_hours,
                "scoreWhenCompleteOnly": score_when_complete_only,
            },
        )
        close_runs: list[dict[str, Any]] = []
        close_complete = False
        for i in range(max(1, close_max_passes)):
            out = c.universe_service().prefetch_score_cache_batch(
                api_cap=max(0, close_api_cap),
                lookback_days=close_lookback_days,
                min_bars=min_bars,
                allow_provisional_intraday=allow_provisional_intraday,
            )
            pass_out = {"pass": i + 1, **out}
            close_runs.append(pass_out)
            close_complete = bool(out.get("prefillComplete")) and int(out.get("terminalStaleSkipped", 0) or 0) == 0
            if close_complete:
                break
            # If provider is not ready yet, retries should continue on later scheduled runs.
        score_triggered = close_complete or not score_when_complete_only
        score_out: dict[str, Any] | None = None
        regime_dict: dict[str, Any] | None = None
        if score_triggered:
            regime = c.regime_service().get_market_regime()
            _write_market_brain_best_effort(c, regime)
            regime_dict = regime.__dict__
            score_out = c.universe_service().score_universe_batch(
                regime,
                api_cap=max(0, score_api_cap),
                fresh_hours=max(0, score_fresh_hours),
                sheet_write_batch_size=200,
                cache_only=True,
                require_fresh_cache=True,
            )
        out: dict[str, Any] = {
            "closeUpdate": {
                "runs": close_runs,
                "complete": close_complete,
                "last": close_runs[-1] if close_runs else None,
            },
            "scoreTriggered": score_triggered,
            "scoreSkippedReason": "" if score_triggered else "latest_candle_update_incomplete",
        }
        if regime_dict is not None:
            out["regime"] = regime_dict
        if score_out is not None:
            out["score"] = score_out
        done_message = "eod latest candles complete and score generated" if score_triggered else "eod latest candles incomplete; score skipped"
        sink.action(
            "Universe",
            "eod_close_update_score",
            "DONE",
            done_message,
            {**sched_ctx, **_duration_ctx(started_perf), **out},
        )
        sink.flush_all()
        return out
    except Exception as e:
        sink.action(
            "Universe",
            "eod_close_update_score",
            "ERROR",
            f"{type(e).__name__}: {e}",
            {
                **sched_ctx,
                **_duration_ctx(started_perf),
                "errorType": type(e).__name__,
                "closeApiCap": close_api_cap,
                "closeLookbackDays": close_lookback_days,
                "minBars": min_bars,
                "closeMaxPasses": close_max_passes,
                "allowProvisionalIntraday": allow_provisional_intraday,
            },
        )
        sink.flush_all()
        raise
    finally:
        _release_named_locks(c.state, leases)


@app.post("/jobs/scan-once")
def run_scan_once(
    x_job_token: str | None = Header(default=None),
    force: bool = False,
    allow_live_orders: bool = False,
    x_cloudscheduler_jobname: str | None = Header(default=None, alias="X-CloudScheduler-JobName"),
    x_cloudscheduler_scheduletime: str | None = Header(default=None, alias="X-CloudScheduler-ScheduleTime"),
) -> dict[str, Any]:
    c = get_container()
    _auth(c.settings.runtime.job_trigger_token, x_job_token)
    c.sheets.ensure_core_sheets()
    sink = LogSink(c.sheets)
    sched_ctx = _scheduler_ctx(x_cloudscheduler_jobname, x_cloudscheduler_scheduletime)
    started_perf = time.perf_counter()
    try:
        sink.action("Trading", "scan_once", "START", "", {**sched_ctx, "force": force, "allowLiveOrders": allow_live_orders})
        out = c.trading_service().run_scan_once(allow_live_orders=allow_live_orders, force=force)
        sink.action("Trading", "scan_once", "DONE", "scan completed", {**sched_ctx, **_duration_ctx(started_perf), **(out if isinstance(out, dict) else {"result": str(out)})})
        sink.flush_all()
        return out
    except Exception as e:
        sink.action(
            "Trading",
            "scan_once",
            "ERROR",
            f"{type(e).__name__}: {e}",
            {**sched_ctx, **_duration_ctx(started_perf), "errorType": type(e).__name__, "force": force, "allowLiveOrders": allow_live_orders},
        )
        sink.flush_all()
        raise
