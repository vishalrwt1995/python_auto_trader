from __future__ import annotations

import logging
import secrets
import json
import time
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Query, Request

from autotrader.container import get_container
from autotrader.services.log_sink import LogSink
from autotrader.time_utils import now_ist, now_utc, parse_any_ts

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


def _watchlist_daily_source_enum(raw_source: str) -> str:
    src = str(raw_source or "").strip().lower()
    if src == "cache_only":
        return "cache_only"
    if "error" in src:
        return "error_fallback"
    if src.startswith("fallback_") or src == "cache_fallback":
        return "fallback_proxy"
    if "expectedlcd" in src or src == "upstox_api":
        return "expectedlcd_sync_api"
    return "cache_only"


def _watchlist_done_log_fields(wl_out: dict[str, Any], *, is_premarket: bool) -> dict[str, Any]:
    coverage = wl_out.get("coverage", {}) if isinstance(wl_out.get("coverage"), dict) else {}
    regime_v2 = wl_out.get("regimeV2", {}) if isinstance(wl_out.get("regimeV2"), dict) else {}
    source = regime_v2.get("source", {}) if isinstance(regime_v2.get("source"), dict) else {}
    phase_stats = wl_out.get("intradayPhaseStats", {}) if isinstance(wl_out.get("intradayPhaseStats"), dict) else {}
    brain = wl_out.get("marketBrainState", {}) if isinstance(wl_out.get("marketBrainState"), dict) else {}
    policy = wl_out.get("marketPolicy", {}) if isinstance(wl_out.get("marketPolicy"), dict) else {}
    rejection_summary_raw = phase_stats.get("phase2RejectionSummary", {})
    rejection_summary = rejection_summary_raw if isinstance(rejection_summary_raw, dict) else {}

    intraday_selected = int(
        phase_stats.get("intradaySelectedCount", wl_out.get("intradaySelected", wl_out.get("selected", 0))) or 0
    )
    phase2_used = int(phase_stats.get("phase2UsedCount", 0) or 0)
    phase1_fallback = int(phase_stats.get("phase1FallbackCount", max(0, intraday_selected - phase2_used)) or 0)
    phase2_eligible = int(phase_stats.get("phase2EligibleCount", coverage.get("phase2Candidates", 0)) or 0)
    phase2_eligible_pct = float(phase_stats.get("phase2EligiblePct", 0.0) or 0.0)
    phase2_quality_score = float(phase_stats.get("phase2QualityScore", 0.0) or 0.0)
    phase2_branch_entered = bool(phase_stats.get("phase2BranchEntered", False))
    phase2_branch_completed = bool(phase_stats.get("phase2BranchCompleted", False))
    phase2_candidates_seen = int(phase_stats.get("phase2CandidatesSeen", 0) or 0)
    phase2_window_open = bool(phase_stats.get("phase2WindowOpen", False))
    phase2_policy_enabled = bool(phase_stats.get("phase2PolicyEnabled", False))
    phase2_global_skip_reason = str(phase_stats.get("phase2GlobalSkipReason", "") or "")

    return {
        "expectedLCD": str(coverage.get("expectedLCD") or ""),
        "runTimeBlock": str(coverage.get("runTimeBlock") or ("PREMARKET" if is_premarket else "UNKNOWN")),
        "isPremarket": bool(is_premarket),
        "indexDailyKeyChosen": str(source.get("dailyKey") or ""),
        "indexDailySource": _watchlist_daily_source_enum(str(source.get("dailySource") or "")),
        "regimeDaily": str(regime_v2.get("regimeDaily") or "RANGE"),
        "regimeIntraday": ("NA" if bool(is_premarket) else str(regime_v2.get("regimeIntraday") or "NA")),
        "phase2_used_count": phase2_used,
        "phase1_fallback_count": phase1_fallback,
        "phase2_eligible_count": phase2_eligible,
        "phase2_eligible_pct": round(phase2_eligible_pct, 2),
        "phase2_quality_score": round(phase2_quality_score, 2),
        "intraday_selected_count": intraday_selected,
        "phase2_branch_entered": phase2_branch_entered,
        "phase2_branch_completed": phase2_branch_completed,
        "phase2_candidates_seen": phase2_candidates_seen,
        "phase2_window_open": phase2_window_open,
        "phase2_policy_enabled": phase2_policy_enabled,
        "phase2_global_skip_reason": phase2_global_skip_reason,
        "canonicalRegime": str(brain.get("regime") or regime_v2.get("canonicalRegime") or ""),
        "riskMode": str(brain.get("risk_mode") or regime_v2.get("riskMode") or ""),
        "structureState": str(brain.get("structure_state") or regime_v2.get("structureState") or ""),
        "participation": str(brain.get("participation") or regime_v2.get("participation") or ""),
        "subRegimeV2": str(brain.get("sub_regime_v2") or regime_v2.get("subRegimeV2") or ""),
        "runDegradedFlag": bool(brain.get("run_degraded_flag", regime_v2.get("runDegradedFlag", False))),
        "marketConfidence": float(brain.get("market_confidence", regime_v2.get("marketConfidence", 0.0)) or 0.0),
        "breadthConfidence": float(brain.get("breadth_confidence", regime_v2.get("breadthConfidence", 0.0)) or 0.0),
        "leadershipConfidence": float(brain.get("leadership_confidence", regime_v2.get("leadershipConfidence", 0.0)) or 0.0),
        "phase2Confidence": float(brain.get("phase2_confidence", regime_v2.get("phase2Confidence", 0.0)) or 0.0),
        "policyConfidence": float(policy.get("policy_confidence", brain.get("policy_confidence", regime_v2.get("policyConfidence", 0.0))) or 0.0),
        "runIntegrityConfidence": float(brain.get("run_integrity_confidence", regime_v2.get("runIntegrityConfidence", 0.0)) or 0.0),
        "phase2_rejection_summary": {str(k): int(v or 0) for k, v in rejection_summary.items()},
    }


def _market_brain_response_payload(c, market_state: Any, market_policy: Any) -> dict[str, Any]:
    regime_v2 = {}
    try:
        regime_v2 = c.market_brain_service().watchlist_regime_payload(market_state)
    except Exception:
        regime_v2 = {}
    return {
        "regime": regime_v2,
        "regimeV2": regime_v2,
        "marketBrainState": getattr(market_state, "__dict__", {}),
        "marketPolicy": getattr(market_policy, "__dict__", {}),
    }


def _write_market_brain_best_effort(c, market_state: Any, market_policy: Any) -> None:
    try:
        if hasattr(c.sheets, "write_market_brain_v2"):
            c.sheets.write_market_brain_v2(market_state, market_policy)
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
        rows = c.universe_service().sync_universe_from_upstox_instruments(limit=limit)
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
        market_state = c.market_brain_service().build_premarket_market_brain(now_ist().isoformat())
        market_policy = c.market_brain_service().derive_market_policy(market_state)
        _write_market_brain_best_effort(c, market_state, market_policy)
        v2_out = c.universe_service().recompute_universe_v2_from_cache()
        wl_out = c.universe_service().build_watchlist(
            market_state,
            target_size=target_size,
            min_score=max(1, min_watchlist_score),
            require_today_scored=require_today_scored,
            require_full_coverage=require_full_coverage,
            premarket=True,
            intraday_timeframe="5m",
        )
        done_message = "watchlist ready" if bool(wl_out.get("ready")) and int(wl_out.get("selected", 0) or 0) > 0 else "watchlist blocked"
        sink.action(
            "Universe",
            "premarket_precompute",
            "DONE",
            done_message,
            {**sched_ctx, **_duration_ctx(started_perf), "universeV2": v2_out, "watchlist": wl_out},
        )
        sink.flush_all()
        out = _market_brain_response_payload(c, market_state, market_policy)
        out["universeV2"] = v2_out
        out["watchlist"] = wl_out
        return out
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
    target_size: int = 150,
    require_full_coverage: bool = False,
    require_today_scored: bool = False,
    min_watchlist_score: int = 1,
    premarket: bool = False,
    intraday_timeframe: str = "5m",
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
                "premarket": bool(premarket),
                "intradayTimeframe": str(intraday_timeframe),
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
                "premarket": bool(premarket),
                "intradayTimeframe": str(intraday_timeframe),
            },
        )
        market_state = (
            c.market_brain_service().build_premarket_market_brain(now_ist().isoformat())
            if bool(premarket)
            else c.market_brain_service().build_post_open_market_brain(now_ist().isoformat())
        )
        market_policy = c.market_brain_service().derive_market_policy(market_state)
        _write_market_brain_best_effort(c, market_state, market_policy)
        wl_out = c.universe_service().build_watchlist(
            market_state,
            target_size=target_size,
            min_score=max(1, min_watchlist_score),
            require_today_scored=require_today_scored,
            require_full_coverage=require_full_coverage,
            premarket=bool(premarket),
            intraday_timeframe=str(intraday_timeframe or "5m"),
        )
        audit_ctx = _watchlist_done_log_fields(wl_out if isinstance(wl_out, dict) else {}, is_premarket=bool(premarket))
        done_message = "watchlist ready" if bool(wl_out.get("ready")) and int(wl_out.get("selected", 0) or 0) > 0 else "watchlist blocked"
        sink.action(
            "Universe",
            "watchlist_refresh",
            "DONE",
            done_message,
            {
                **sched_ctx,
                **_duration_ctx(started_perf),
                **audit_ctx,
                "watchlist": wl_out,
                "regime": wl_out.get("regimeV2", {}),
                "regimeV2": wl_out.get("regimeV2", {}),
            },
        )
        sink.flush_all()
        out = _market_brain_response_payload(c, market_state, market_policy)
        out["watchlist"] = wl_out
        return out
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
        market_state = c.market_brain_service().build_post_open_market_brain(now_ist().isoformat())
        market_policy = c.market_brain_service().derive_market_policy(market_state)
        _write_market_brain_best_effort(c, market_state, market_policy)
        v2_out = c.universe_service().recompute_universe_v2_from_cache()
        regime_payload = c.market_brain_service().watchlist_regime_payload(market_state)
        sink.action(
            "Universe",
            "score_refresh",
            "DONE",
            "universe v2 recompute complete",
            {
                **sched_ctx,
                **_duration_ctx(started_perf),
                "regime": regime_payload,
                "regimeV2": regime_payload,
                "marketBrainState": market_state.__dict__,
                "marketPolicy": market_policy.__dict__,
                "universeV2": v2_out,
            },
        )
        sink.flush_all()
        out = _market_brain_response_payload(c, market_state, market_policy)
        out["universeV2"] = v2_out
        return out
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
    retry_stale_terminal_today: bool = False,
    run_intraday_update: bool = True,
    intraday_api_cap: int = 600,
    intraday_lookback_trading_days: int = 60,
    x_cloudscheduler_jobname: str | None = Header(default=None, alias="X-CloudScheduler-JobName"),
    x_cloudscheduler_scheduletime: str | None = Header(default=None, alias="X-CloudScheduler-ScheduleTime"),
) -> dict[str, Any]:
    c = get_container()
    _auth(c.settings.runtime.job_trigger_token, x_job_token)
    c.sheets.ensure_core_sheets()
    sink = LogSink(c.sheets)
    sched_ctx = _scheduler_ctx(x_cloudscheduler_jobname, x_cloudscheduler_scheduletime)
    lock_names = ["score_cache_update_close"]
    if run_intraday_update:
        lock_names.append("intraday_cache_update_close_5m")
    leases, blocked_lock = _acquire_named_locks(c.state, lock_names, ttl_seconds=3600)
    if blocked_lock is not None:
        sink.action(
            "Universe",
            "score_cache_update_close",
            "LOCK_BUSY",
            "skipped: lock busy",
            {
                **sched_ctx,
                "blockedLock": blocked_lock,
                "apiCap": api_cap,
                "lookbackDays": lookback_days,
                "minBars": min_bars,
                "retryStaleTerminalToday": retry_stale_terminal_today,
                "runIntradayUpdate": run_intraday_update,
                "intradayApiCap": intraday_api_cap,
                "intradayLookbackTradingDays": intraday_lookback_trading_days,
            },
        )
        sink.flush_all()
        return {"skipped": "lock_busy", "blockedLock": blocked_lock}
    started_perf = time.perf_counter()
    try:
        sink.action(
            "Universe",
            "score_cache_update_close",
            "START",
            "",
            {
                **sched_ctx,
                "apiCap": api_cap,
                "lookbackDays": lookback_days,
                "minBars": min_bars,
                "retryStaleTerminalToday": retry_stale_terminal_today,
                "runIntradayUpdate": run_intraday_update,
                "intradayApiCap": intraday_api_cap,
                "intradayLookbackTradingDays": intraday_lookback_trading_days,
            },
        )
        out = c.universe_service().prefetch_score_cache_batch(
            api_cap=max(0, api_cap),
            lookback_days=lookback_days,
            min_bars=min_bars,
            retry_stale_terminal_today=bool(retry_stale_terminal_today),
        )
        if run_intraday_update:
            intraday_out = c.universe_service().prefetch_intraday_cache_5m_batch(
                api_cap=max(0, int(intraday_api_cap)),
                lookback_trading_days=max(1, int(intraday_lookback_trading_days)),
                only_symbols=None,
                refresh_last_day_only=True,
                retry_stale_terminal_today=bool(retry_stale_terminal_today),
            )
            out = {**out, "intraday5m": intraday_out}
        sink.action("Universe", "score_cache_update_close", "DONE", "daily score-cache update batch complete", {**sched_ctx, **_duration_ctx(started_perf), **out})
        sink.flush_all()
        return out
    except Exception as e:
        sink.action(
            "Universe",
            "score_cache_update_close",
            "ERROR",
            f"{type(e).__name__}: {e}",
            {
                **sched_ctx,
                **_duration_ctx(started_perf),
                "errorType": type(e).__name__,
                "apiCap": api_cap,
                "lookbackDays": lookback_days,
                "minBars": min_bars,
                "retryStaleTerminalToday": retry_stale_terminal_today,
                "runIntradayUpdate": run_intraday_update,
                "intradayApiCap": intraday_api_cap,
                "intradayLookbackTradingDays": intraday_lookback_trading_days,
            },
        )
        sink.flush_all()
        raise
    finally:
        _release_named_locks(c.state, leases)


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
        v2_out: dict[str, Any] | None = None
        regime_dict: dict[str, Any] | None = None
        market_state_dict: dict[str, Any] | None = None
        market_policy_dict: dict[str, Any] | None = None
        if run_score_refresh:
            market_state = c.market_brain_service().build_post_open_market_brain(now_ist().isoformat())
            market_policy = c.market_brain_service().derive_market_policy(market_state)
            _write_market_brain_best_effort(c, market_state, market_policy)
            regime_dict = c.market_brain_service().watchlist_regime_payload(market_state)
            market_state_dict = market_state.__dict__
            market_policy_dict = market_policy.__dict__
            v2_out = c.universe_service().recompute_universe_v2_from_cache()
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
            out["regimeV2"] = regime_dict or {}
            out["marketBrainState"] = market_state_dict or {}
            out["marketPolicy"] = market_policy_dict or {}
            out["universeV2"] = v2_out or {}
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


@app.post("/jobs/universe-v2-refresh")
def run_universe_v2_refresh(
    x_job_token: str | None = Header(default=None),
    build_limit: int = 0,
    replace: bool = False,
    candle_api_cap: int = 600,
    run_full_backfill: bool = True,
    write_v2_eligibility: bool = False,
    run_intraday_appended_backfill: bool = True,
    intraday_api_cap: int = 1200,
    intraday_lookback_trading_days: int = 60,
    run_sector_mapping_appended: bool = True,
    sector_mapping_api_cap: int = 600,
    x_cloudscheduler_jobname: str | None = Header(default=None, alias="X-CloudScheduler-JobName"),
    x_cloudscheduler_scheduletime: str | None = Header(default=None, alias="X-CloudScheduler-ScheduleTime"),
) -> dict[str, Any]:
    c = get_container()
    _auth(c.settings.runtime.job_trigger_token, x_job_token)
    c.sheets.ensure_core_sheets()
    sink = LogSink(c.sheets)
    sched_ctx = _scheduler_ctx(x_cloudscheduler_jobname, x_cloudscheduler_scheduletime)
    lock_names = [
        "universe_v2_refresh",
        "raw_universe_refresh",
        "universe_build",
        "score_cache_backfill_full",
        "score_cache_update_close",
    ]
    if run_intraday_appended_backfill:
        lock_names.append("intraday_cache_backfill_appended_5m")
    if run_sector_mapping_appended:
        lock_names.append("sector_mapping_refresh")
    leases, blocked_lock = _acquire_named_locks(c.state, lock_names, ttl_seconds=7200)
    if blocked_lock is not None:
        sink.action(
            "Universe",
            "universe_v2_refresh",
            "LOCK_BUSY",
            "skipped: lock busy",
            {
                **sched_ctx,
                "blockedLock": blocked_lock,
                "buildLimit": build_limit,
                "replace": replace,
                "candleApiCap": candle_api_cap,
                "runFullBackfill": run_full_backfill,
                "writeV2Eligibility": write_v2_eligibility,
                "runIntradayAppendedBackfill": run_intraday_appended_backfill,
                "intradayApiCap": intraday_api_cap,
                "intradayLookbackTradingDays": intraday_lookback_trading_days,
                "runSectorMappingAppended": run_sector_mapping_appended,
                "sectorMappingApiCap": sector_mapping_api_cap,
            },
        )
        sink.flush_all()
        return {"skipped": "lock_busy", "blockedLock": blocked_lock}
    started_perf = time.perf_counter()
    try:
        sink.action(
            "Universe",
            "universe_v2_refresh",
            "START",
            "",
            {
                **sched_ctx,
                "buildLimit": build_limit,
                "replace": replace,
                "candleApiCap": candle_api_cap,
                "runFullBackfill": run_full_backfill,
                "writeV2Eligibility": write_v2_eligibility,
                "runIntradayAppendedBackfill": run_intraday_appended_backfill,
                "intradayApiCap": intraday_api_cap,
                "intradayLookbackTradingDays": intraday_lookback_trading_days,
                "runSectorMappingAppended": run_sector_mapping_appended,
                "sectorMappingApiCap": sector_mapping_api_cap,
            },
        )
        out = c.universe_service().run_universe_v2_pipeline(
            build_limit=max(0, build_limit),
            replace=replace,
            candle_api_cap=max(0, candle_api_cap),
            run_full_backfill=run_full_backfill,
            write_v2_eligibility=write_v2_eligibility,
        )
        try:
            appended_symbols = (out.get("build") or {}).get("appendedSymbols") if isinstance(out, dict) else []
            if isinstance(appended_symbols, list):
                payload = {
                    "runDate": str((out.get("raw") or {}).get("runDate") or ""),
                    "symbols": [str(s).strip().upper() for s in appended_symbols if str(s).strip()],
                }
                c.state.set_runtime_prop("runtime:universe_v2_last_appended_symbols", json.dumps(payload, separators=(",", ":")))
        except Exception:
            logger.warning("failed to persist runtime:universe_v2_last_appended_symbols", exc_info=True)
        appended_symbols = (out.get("build") or {}).get("appendedSymbols") if isinstance(out, dict) else []
        appended_set = [str(s).strip().upper() for s in (appended_symbols or []) if str(s).strip()]
        if run_intraday_appended_backfill:
            if appended_set:
                intraday_appended = c.universe_service().prefetch_intraday_cache_5m_batch(
                    api_cap=max(0, int(intraday_api_cap)),
                    lookback_trading_days=max(1, int(intraday_lookback_trading_days)),
                    only_symbols=appended_set,
                    refresh_last_day_only=False,
                )
                out = {**out, "intradayAppended5m": intraday_appended}
            else:
                out = {**out, "intradayAppended5m": {"skipped": "no_appended_symbols", "symbols": 0}}
        if run_sector_mapping_appended:
            if appended_set:
                sector_appended = c.universe_service().refresh_sector_mapping(
                    api_cap=max(0, int(sector_mapping_api_cap)),
                    retry_unknown=False,
                    only_symbols=appended_set,
                    sync_universe=True,
                )
                out = {**out, "sectorAppended": sector_appended}
            else:
                out = {**out, "sectorAppended": {"skipped": "no_appended_symbols", "symbols": 0}}
        raw_ok = bool((out.get("raw") or {}).get("ok", True))
        cache_summary = out.get("cache") if isinstance(out.get("cache"), dict) else {}
        eligibility = out.get("eligibility") if isinstance(out.get("eligibility"), dict) else {}
        if not raw_ok:
            sink.action(
                "Universe",
                "universe_v2_refresh",
                "WARN",
                "raw snapshot failed; last-good retained",
                {**sched_ctx, **_duration_ctx(started_perf), **out},
            )
        else:
            if int(cache_summary.get("errors", 0) or 0) > 0:
                sink.action(
                    "Universe",
                    "universe_v2_candle_update",
                    "WARN",
                    "one or more candle refreshes failed",
                    {
                        **sched_ctx,
                        "errors": int(cache_summary.get("errors", 0) or 0),
                        "missing": int(cache_summary.get("missing", 0) or 0),
                        "stale": int(cache_summary.get("stale", 0) or 0),
                        "invalidKey": int(cache_summary.get("invalidKey", 0) or 0),
                    },
                )
            if "totalMasterCount" in eligibility:
                sink.action(
                    "Universe",
                    "universe_v2_daily_summary",
                    "DONE",
                    "daily universe v2 eligibility summary",
                    {
                        **sched_ctx,
                        "totalMasterCount": int(eligibility.get("totalMasterCount", 0) or 0),
                        "eligibleSwingCount": int(eligibility.get("eligibleSwingCount", 0) or 0),
                        "eligibleIntradayCount": int(eligibility.get("eligibleIntradayCount", 0) or 0),
                        "disabledCount": int(eligibility.get("disabledCount", 0) or 0),
                        "staleCount": int(eligibility.get("staleCount", 0) or 0),
                        "topDisableReasons": eligibility.get("topDisableReasons") or [],
                    },
                )
            else:
                sink.action(
                    "Universe",
                    "universe_v2_daily_summary",
                    "DONE",
                    "daily universe v2 eligibility deferred to score_refresh",
                    {
                        **sched_ctx,
                        "appended": int((out.get("build") or {}).get("appended", 0) or 0),
                        "rawEligible": int((out.get("build") or {}).get("rawEligible", 0) or 0),
                        "cacheFetches": int(cache_summary.get("fetches", 0) or 0),
                        "cacheUpdated": int(cache_summary.get("updated", 0) or 0),
                        "cacheMissing": int(cache_summary.get("missing", 0) or 0),
                        "cacheStale": int(cache_summary.get("stale", 0) or 0),
                    },
                )
            sink.action(
                "Universe",
                "universe_v2_refresh",
                "DONE",
                "universe v2 pipeline complete",
                {**sched_ctx, **_duration_ctx(started_perf), **out},
            )
        sink.flush_all()
        return out
    except Exception as e:
        sink.action(
            "Universe",
            "universe_v2_refresh",
            "ERROR",
            f"{type(e).__name__}: {e}",
            {
                **sched_ctx,
                **_duration_ctx(started_perf),
                "errorType": type(e).__name__,
                "buildLimit": build_limit,
                "replace": replace,
                "candleApiCap": candle_api_cap,
                "runFullBackfill": run_full_backfill,
                "writeV2Eligibility": write_v2_eligibility,
                "runIntradayAppendedBackfill": run_intraday_appended_backfill,
                "intradayApiCap": intraday_api_cap,
                "intradayLookbackTradingDays": intraday_lookback_trading_days,
                "runSectorMappingAppended": run_sector_mapping_appended,
                "sectorMappingApiCap": sector_mapping_api_cap,
            },
        )
        sink.flush_all()
        raise
    finally:
        _release_named_locks(c.state, leases)


@app.post("/jobs/universe-v2-audit")
def run_universe_v2_audit(
    x_job_token: str | None = Header(default=None),
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
        sink.action("Universe", "universe_v2_audit", "START", "", sched_ctx)
        out = c.universe_service().audit_universe_v2_integrity()
        sink.action("Universe", "universe_v2_audit", "DONE", "universe integrity audit complete", {**sched_ctx, **_duration_ctx(started_perf), **out})
        sink.flush_all()
        return out
    except Exception as e:
        sink.action(
            "Universe",
            "universe_v2_audit",
            "ERROR",
            f"{type(e).__name__}: {e}",
            {**sched_ctx, **_duration_ctx(started_perf), "errorType": type(e).__name__},
        )
        sink.flush_all()
        raise


@app.post("/jobs/intraday-cache-backfill-full")
def run_intraday_cache_backfill_full(
    x_job_token: str | None = Header(default=None),
    api_cap: int = 1200,
    lookback_trading_days: int = 60,
    x_cloudscheduler_jobname: str | None = Header(default=None, alias="X-CloudScheduler-JobName"),
    x_cloudscheduler_scheduletime: str | None = Header(default=None, alias="X-CloudScheduler-ScheduleTime"),
) -> dict[str, Any]:
    c = get_container()
    _auth(c.settings.runtime.job_trigger_token, x_job_token)
    c.sheets.ensure_core_sheets()
    sink = LogSink(c.sheets)
    sched_ctx = _scheduler_ctx(x_cloudscheduler_jobname, x_cloudscheduler_scheduletime)
    lease = c.state.try_acquire_lock("intraday_cache_backfill_full_5m", ttl_seconds=7200)
    if lease is None:
        sink.action(
            "Universe",
            "intraday_cache_backfill_full_5m",
            "LOCK_BUSY",
            "skipped: lock busy",
            {**sched_ctx, "apiCap": api_cap, "lookbackTradingDays": lookback_trading_days},
        )
        sink.flush_all()
        return {"skipped": "lock_busy"}
    started_perf = time.perf_counter()
    try:
        sink.action(
            "Universe",
            "intraday_cache_backfill_full_5m",
            "START",
            "",
            {**sched_ctx, "apiCap": api_cap, "lookbackTradingDays": lookback_trading_days},
        )
        out = c.universe_service().prefetch_intraday_cache_5m_batch(
            api_cap=max(0, int(api_cap)),
            lookback_trading_days=max(1, int(lookback_trading_days)),
            only_symbols=None,
            refresh_last_day_only=False,
        )
        sink.action(
            "Universe",
            "intraday_cache_backfill_full_5m",
            "DONE",
            "intraday 5m full backfill batch complete",
            {**sched_ctx, **_duration_ctx(started_perf), **out},
        )
        sink.flush_all()
        return out
    except Exception as e:
        sink.action(
            "Universe",
            "intraday_cache_backfill_full_5m",
            "ERROR",
            f"{type(e).__name__}: {e}",
            {**sched_ctx, **_duration_ctx(started_perf), "errorType": type(e).__name__, "apiCap": api_cap, "lookbackTradingDays": lookback_trading_days},
        )
        sink.flush_all()
        raise
    finally:
        c.state.release_lock(lease)


@app.post("/jobs/sector-mapping-refresh")
def run_sector_mapping_refresh(
    x_job_token: str | None = Header(default=None),
    api_cap: int = 600,
    retry_unknown: bool = False,
    x_cloudscheduler_jobname: str | None = Header(default=None, alias="X-CloudScheduler-JobName"),
    x_cloudscheduler_scheduletime: str | None = Header(default=None, alias="X-CloudScheduler-ScheduleTime"),
) -> dict[str, Any]:
    c = get_container()
    _auth(c.settings.runtime.job_trigger_token, x_job_token)
    c.sheets.ensure_core_sheets()
    sink = LogSink(c.sheets)
    sched_ctx = _scheduler_ctx(x_cloudscheduler_jobname, x_cloudscheduler_scheduletime)
    lease = c.state.try_acquire_lock("sector_mapping_refresh", ttl_seconds=3600)
    if lease is None:
        sink.action(
            "Universe",
            "sector_mapping_refresh",
            "LOCK_BUSY",
            "skipped: lock busy",
            {**sched_ctx, "apiCap": api_cap, "retryUnknown": bool(retry_unknown)},
        )
        sink.flush_all()
        return {"skipped": "lock_busy"}
    started_perf = time.perf_counter()
    try:
        sink.action(
            "Universe",
            "sector_mapping_refresh",
            "START",
            "",
            {**sched_ctx, "apiCap": api_cap, "retryUnknown": bool(retry_unknown)},
        )
        out = c.universe_service().refresh_sector_mapping(api_cap=api_cap, retry_unknown=bool(retry_unknown))
        sink.action(
            "Universe",
            "sector_mapping_refresh",
            "DONE",
            "sector mapping refresh complete",
            {**sched_ctx, **_duration_ctx(started_perf), **out},
        )
        sink.flush_all()
        return out
    except Exception as e:
        sink.action(
            "Universe",
            "sector_mapping_refresh",
            "ERROR",
            f"{type(e).__name__}: {e}",
            {**sched_ctx, **_duration_ctx(started_perf), "errorType": type(e).__name__, "apiCap": api_cap},
        )
        sink.flush_all()
        raise
    finally:
        c.state.release_lock(lease)


@app.post("/jobs/intraday-cache-backfill-appended")
def run_intraday_cache_backfill_appended(
    x_job_token: str | None = Header(default=None),
    api_cap: int = 1200,
    lookback_trading_days: int = 60,
    x_cloudscheduler_jobname: str | None = Header(default=None, alias="X-CloudScheduler-JobName"),
    x_cloudscheduler_scheduletime: str | None = Header(default=None, alias="X-CloudScheduler-ScheduleTime"),
) -> dict[str, Any]:
    c = get_container()
    _auth(c.settings.runtime.job_trigger_token, x_job_token)
    c.sheets.ensure_core_sheets()
    sink = LogSink(c.sheets)
    sched_ctx = _scheduler_ctx(x_cloudscheduler_jobname, x_cloudscheduler_scheduletime)
    lease = c.state.try_acquire_lock("intraday_cache_backfill_appended_5m", ttl_seconds=7200)
    if lease is None:
        sink.action(
            "Universe",
            "intraday_cache_backfill_appended_5m",
            "LOCK_BUSY",
            "skipped: lock busy",
            {**sched_ctx, "apiCap": api_cap, "lookbackTradingDays": lookback_trading_days},
        )
        sink.flush_all()
        return {"skipped": "lock_busy"}
    started_perf = time.perf_counter()
    try:
        payload_text = c.state.get_runtime_prop("runtime:universe_v2_last_appended_symbols", "")
        symbols: list[str] = []
        run_date = ""
        if payload_text:
            try:
                payload_obj = json.loads(payload_text)
                if isinstance(payload_obj, dict):
                    run_date = str(payload_obj.get("runDate") or "")
                    arr = payload_obj.get("symbols") or []
                    if isinstance(arr, list):
                        symbols = [str(s).strip().upper() for s in arr if str(s).strip()]
            except Exception:
                symbols = []
        if not symbols:
            out = {"skipped": "no_appended_symbols", "symbols": 0, "runDate": run_date}
            sink.action(
                "Universe",
                "intraday_cache_backfill_appended_5m",
                "DONE",
                "no appended symbols to backfill",
                {**sched_ctx, **_duration_ctx(started_perf), **out},
            )
            sink.flush_all()
            return out
        sink.action(
            "Universe",
            "intraday_cache_backfill_appended_5m",
            "START",
            "",
            {**sched_ctx, "apiCap": api_cap, "lookbackTradingDays": lookback_trading_days, "symbols": len(symbols), "runDate": run_date},
        )
        out = c.universe_service().prefetch_intraday_cache_5m_batch(
            api_cap=max(0, int(api_cap)),
            lookback_trading_days=max(1, int(lookback_trading_days)),
            only_symbols=symbols,
            refresh_last_day_only=False,
        )
        sink.action(
            "Universe",
            "intraday_cache_backfill_appended_5m",
            "DONE",
            "intraday 5m appended-symbol backfill complete",
            {**sched_ctx, **_duration_ctx(started_perf), "runDate": run_date, "symbols": len(symbols), **out},
        )
        sink.flush_all()
        return {**out, "runDate": run_date, "symbols": len(symbols)}
    except Exception as e:
        sink.action(
            "Universe",
            "intraday_cache_backfill_appended_5m",
            "ERROR",
            f"{type(e).__name__}: {e}",
            {**sched_ctx, **_duration_ctx(started_perf), "errorType": type(e).__name__, "apiCap": api_cap, "lookbackTradingDays": lookback_trading_days},
        )
        sink.flush_all()
        raise
    finally:
        c.state.release_lock(lease)


@app.post("/jobs/intraday-cache-update-close")
def run_intraday_cache_update_close(
    x_job_token: str | None = Header(default=None),
    api_cap: int = 1200,
    lookback_trading_days: int = 60,
    retry_stale_terminal_today: bool = False,
    x_cloudscheduler_jobname: str | None = Header(default=None, alias="X-CloudScheduler-JobName"),
    x_cloudscheduler_scheduletime: str | None = Header(default=None, alias="X-CloudScheduler-ScheduleTime"),
) -> dict[str, Any]:
    c = get_container()
    _auth(c.settings.runtime.job_trigger_token, x_job_token)
    c.sheets.ensure_core_sheets()
    sink = LogSink(c.sheets)
    sched_ctx = _scheduler_ctx(x_cloudscheduler_jobname, x_cloudscheduler_scheduletime)
    lease = c.state.try_acquire_lock("intraday_cache_update_close_5m", ttl_seconds=7200)
    if lease is None:
        sink.action(
            "Universe",
            "intraday_cache_update_close_5m",
            "LOCK_BUSY",
            "skipped: lock busy",
            {**sched_ctx, "apiCap": api_cap, "lookbackTradingDays": lookback_trading_days},
        )
        sink.flush_all()
        return {"skipped": "lock_busy"}
    started_perf = time.perf_counter()
    try:
        sink.action(
            "Universe",
            "intraday_cache_update_close_5m",
            "START",
            "",
            {**sched_ctx, "apiCap": api_cap, "lookbackTradingDays": lookback_trading_days, "retryStaleTerminalToday": retry_stale_terminal_today},
        )
        out = c.universe_service().prefetch_intraday_cache_5m_batch(
            api_cap=max(0, int(api_cap)),
            lookback_trading_days=max(1, int(lookback_trading_days)),
            only_symbols=None,
            refresh_last_day_only=True,
            retry_stale_terminal_today=bool(retry_stale_terminal_today),
        )
        sink.action(
            "Universe",
            "intraday_cache_update_close_5m",
            "DONE",
            "intraday 5m latest-session update complete",
            {**sched_ctx, **_duration_ctx(started_perf), "retryStaleTerminalToday": retry_stale_terminal_today, **out},
        )
        sink.flush_all()
        return out
    except Exception as e:
        sink.action(
            "Universe",
            "intraday_cache_update_close_5m",
            "ERROR",
            f"{type(e).__name__}: {e}",
            {**sched_ctx, **_duration_ctx(started_perf), "errorType": type(e).__name__, "apiCap": api_cap, "lookbackTradingDays": lookback_trading_days, "retryStaleTerminalToday": retry_stale_terminal_today},
        )
        sink.flush_all()
        raise
    finally:
        c.state.release_lock(lease)


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
        v2_out: dict[str, Any] | None = None
        regime_dict: dict[str, Any] | None = None
        market_state_dict: dict[str, Any] | None = None
        market_policy_dict: dict[str, Any] | None = None
        if score_triggered:
            market_state = c.market_brain_service().build_post_open_market_brain(now_ist().isoformat())
            market_policy = c.market_brain_service().derive_market_policy(market_state)
            _write_market_brain_best_effort(c, market_state, market_policy)
            regime_dict = c.market_brain_service().watchlist_regime_payload(market_state)
            market_state_dict = market_state.__dict__
            market_policy_dict = market_policy.__dict__
            v2_out = c.universe_service().recompute_universe_v2_from_cache()
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
            out["regimeV2"] = regime_dict
        if market_state_dict is not None:
            out["marketBrainState"] = market_state_dict
        if market_policy_dict is not None:
            out["marketPolicy"] = market_policy_dict
        if v2_out is not None:
            out["universeV2"] = v2_out
        done_message = "eod latest candles complete and universe v2 recomputed" if score_triggered else "eod latest candles incomplete; universe v2 recompute skipped"
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


# ---------------------------------------------------------------------------
# EOD position reconciliation (called by Cloud Scheduler at 15:10/15:20/15:30)
# ---------------------------------------------------------------------------

@app.post("/jobs/eod-position-reconcile")
def run_eod_position_reconcile(
    x_job_token: str | None = Header(default=None),
    x_cloudscheduler_jobname: str | None = Header(default=None, alias="X-CloudScheduler-JobName"),
    x_cloudscheduler_scheduletime: str | None = Header(default=None, alias="X-CloudScheduler-ScheduleTime"),
) -> dict[str, Any]:
    """Close all open positions at EOD via Upstox order status check + forced market exit.

    Called at 15:10, 15:20, 15:30 IST by Cloud Scheduler jobs:
      autotrader-eod-recon-1510
      autotrader-eod-recon-1520
      autotrader-eod-recon-1530
    """
    c = get_container()
    _auth(c.settings.runtime.job_trigger_token, x_job_token)
    sink = LogSink(c.sheets)
    sched_ctx = _scheduler_ctx(x_cloudscheduler_jobname, x_cloudscheduler_scheduletime)
    started_perf = time.perf_counter()
    try:
        sink.action("OrderService", "eod_position_reconcile", "START", "", sched_ctx)
        out = c.order_service().reconcile_open_positions()
        sink.action(
            "OrderService",
            "eod_position_reconcile",
            "DONE",
            "eod reconcile complete",
            {**sched_ctx, **_duration_ctx(started_perf), **out},
        )
        sink.flush_all()
        return out
    except Exception as e:
        sink.action(
            "OrderService",
            "eod_position_reconcile",
            "ERROR",
            f"{type(e).__name__}: {e}",
            {**sched_ctx, **_duration_ctx(started_perf), "errorType": type(e).__name__},
        )
        sink.flush_all()
        raise


@app.get("/jobs/position-status")
def get_position_status(
    x_job_token: str | None = Header(default=None),
    symbol: str | None = Query(default=None),
    status: str | None = Query(default=None, description="OPEN or CLOSED \u2014 omit for all"),
) -> dict[str, Any]:
    """Return current open (or all) positions from Firestore.

    Query params:
      symbol \u2014 filter by symbol (case-insensitive)
      status \u2014 OPEN | CLOSED | (omit for all)
    """
    c = get_container()
    _auth(c.settings.runtime.job_trigger_token, x_job_token)
    if status and status.upper() == "OPEN":
        positions = c.state.list_open_positions()
    else:
        positions = c.state.list_all_positions(limit=500)
    if symbol:
        sym_upper = symbol.strip().upper()
        positions = [p for p in positions if str(p.get("symbol", "")).upper() == sym_upper]
    if status and status.upper() not in ("OPEN", "ALL"):
        stat_upper = status.strip().upper()
        positions = [p for p in positions if str(p.get("status", "")).upper() == stat_upper]
    return {"count": len(positions), "positions": positions}
