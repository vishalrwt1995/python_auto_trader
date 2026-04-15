"use client";

import { useEffect, useState, useMemo } from "react";
import { api } from "@/lib/api";
import { DataTable, type Column } from "@/components/shared/DataTable";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { LiveDot } from "@/components/shared/LiveDot";
import { cn, formatTime, isMarketOpen } from "@/lib/utils";
import { useAuthStore } from "@/stores/authStore";
import type { AuditLogEntry } from "@/lib/types";
import {
  RefreshCw,
  CheckCircle,
  Loader2,
  Key,
  Wifi,
  Check,
  Play,
  Clock,
  X,
} from "lucide-react";

// schedulerJobHint: substring matched against entry.scheduler_job (Cloud Scheduler job name in ctx).
const PIPELINE_JOBS = [
  { name: "token_refresh",       label: "Token Refresh",    cron: "03:35 IST", desc: "Upstox OAuth token renewal",                        schedulerJobHint: "upstox-token-request" },
  { name: "universe_refresh",    label: "Universe Refresh", cron: "06:15 IST", desc: "Raw universe refresh + new-symbol backfill",         schedulerJobHint: "universe-v2-refresh" },
  { name: "candle_cache",        label: "Candle Cache",     cron: "07:05 IST", desc: "1D + 5m fetch for all symbols (api_cap=1800)",       schedulerJobHint: "cache-update-close-0705" },
  { name: "candle_finalize",     label: "Candle Finalize",  cron: "07:40 IST", desc: "Terminalize stragglers, no new API fetches",         schedulerJobHint: "cache-update-close-0740" },
  { name: "score_refresh",       label: "Score Refresh",    cron: "08:30 IST", desc: "Compute scores + universe eligibility (cache-only)", schedulerJobHint: "score-0830" },
  { name: "premarket_watchlist", label: "Watchlist Pre",    cron: "09:00 IST", desc: "Pre-market watchlist build",                         schedulerJobHint: "premarket-0900" },
  { name: "swing_recon",         label: "Swing Recon",      cron: "09:00 IST", desc: "Re-evaluate open swing positions, place AMO exits",  schedulerJobHint: "swing-recon" },
  { name: "scanner",             label: "Scanner 5m",       cron: "09:20 IST", desc: "Live signal scan loop (every 5 min)",                schedulerJobHint: "scan-market-5m" },
  { name: "watchlist_5m",        label: "Watchlist 5m",     cron: "09:30 IST", desc: "Intraday watchlist refresh",                         schedulerJobHint: "watchlist-v2-5m-0930" },
  { name: "eod_recon",           label: "EOD Recon",        cron: "15:10 IST", desc: "Force-close open positions (3 passes)",              schedulerJobHint: "eod-recon" },
];

export default function PipelinePage() {
  const [entries, setEntries] = useState<AuditLogEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [upstoxHealth, setUpstoxHealth] = useState<{ token_valid: boolean; token_expires_at?: string } | null>(null);
  const [triggerStates, setTriggerStates] = useState<Record<string, "idle" | "loading" | "ok" | "error">>({});
  const isAdmin = useAuthStore((s) => s.isAdmin);

  const fetchData = () => {
    setRefreshing(true);
    Promise.all([
      api.getPipelineStatus().then((d: any) => setEntries(d.entries ?? [])),
      api.getUpstoxHealth().then((d: any) => setUpstoxHealth(d)),
    ])
      .catch((err) => console.error("[Pipeline] fetch failed:", err))
      .finally(() => {
        setLoading(false);
        setRefreshing(false);
      });
  };

  useEffect(() => { fetchData(); }, []);

  const jobStatuses = useMemo(() => {
    return PIPELINE_JOBS.map((job) => {
      const matching = entries.filter((e) =>
        !!e.scheduler_job && e.scheduler_job.includes(job.schedulerJobHint),
      );
      const latest = matching.find((e) => e.status !== "running") ?? matching[0];
      return {
        ...job,
        status: latest?.status ?? "pending",
        lastRun: latest?.log_ts ? formatTime(new Date(latest.log_ts)) : "",
        message: latest?.message ?? "",
      };
    });
  }, [entries]);

  const statusIcon = (status: string) => {
    if (status === "success") return "checkmark" as const;
    if (status === "running") return "running" as const;
    if (status === "error" || status === "failed") return "error" as const;
    return "pending" as const;
  };

  const statusColor = (status: string) => {
    if (status === "success") return "#22c55e";
    if (status === "running") return "#f59e0b";
    if (status === "error" || status === "failed") return "#ef4444";
    if (status === "skipped") return "#6366f1";
    return "#374151";
  };

  const passedCount = jobStatuses.filter((j) => j.status === "success" || j.status === "skipped").length;
  const runningCount = jobStatuses.filter((j) => j.status === "running").length;

  const columns: Column<AuditLogEntry>[] = useMemo(
    () => [
      {
        key: "time",
        label: "Time",
        sortable: true,
        sortValue: (r) => r.log_ts,
        render: (r) => (
          <span className="font-mono text-xs">
            {r.log_ts ? formatTime(new Date(r.log_ts)) : "—"}
          </span>
        ),
      },
      {
        key: "module",
        label: "Module",
        sortable: true,
        sortValue: (r) => r.module,
        render: (r) => <span className="text-xs font-medium">{r.module}</span>,
      },
      {
        key: "action",
        label: "Action",
        render: (r) => <span className="text-xs">{r.action}</span>,
      },
      {
        key: "status",
        label: "Status",
        render: (r) => (
          <span
            className={cn(
              "px-1.5 py-0.5 rounded text-xs font-medium",
              r.status === "success"
                ? "bg-profit/20 text-profit"
                : r.status === "error" || r.status === "failed"
                  ? "bg-loss/20 text-loss"
                  : r.status === "skipped"
                    ? "bg-indigo-500/20 text-indigo-400"
                    : "bg-bg-tertiary text-text-secondary",
            )}
          >
            {r.status}
          </span>
        ),
      },
      {
        key: "message",
        label: "Message",
        render: (r) => (
          <span className="text-xs text-text-secondary max-w-[300px] truncate block" title={r.message}>
            {r.message || "—"}
          </span>
        ),
      },
      {
        key: "exec_id",
        label: "Exec ID",
        render: (r) => (
          <span className="font-mono text-[10px] text-text-secondary">
            {r.exec_id?.slice(0, 8) ?? "—"}
          </span>
        ),
      },
    ],
    [],
  );

  if (loading) return <LoadingSkeleton lines={10} />;

  const marketOpen = isMarketOpen();

  // Dot classes per status
  const dotClass = (status: string) => {
    if (status === "success")
      return "bg-profit/20 border-2 border-profit text-profit";
    if (status === "running")
      return "bg-neutral/20 border-2 border-neutral text-neutral";
    if (status === "error" || status === "failed")
      return "bg-loss/20 border-2 border-loss text-loss";
    return "bg-bg-tertiary border-2 border-bg-tertiary text-text-secondary";
  };

  const DotIcon = ({ status }: { status: string }) => {
    if (status === "ok" || status === "success") return <Check className="h-3.5 w-3.5" />;
    if (status === "running") return <Play className="h-3 w-3" />;
    if (status === "error" || status === "failed") return <X className="h-3.5 w-3.5" />;
    return <Clock className="h-3 w-3" />;
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h1 className="text-xl font-semibold">Pipeline Monitor</h1>
          <LiveDot status={marketOpen ? "online" : "offline"} />
          <span className="text-xs text-text-secondary">
            {marketOpen ? "Market Open — Scanner Active" : "Market Closed"}
          </span>
        </div>
        <button
          onClick={fetchData}
          disabled={refreshing}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded text-xs bg-bg-tertiary text-text-secondary hover:text-text-primary transition-colors disabled:opacity-50"
        >
          <RefreshCw className={cn("h-3.5 w-3.5", refreshing && "animate-spin")} />
          Refresh
        </button>
      </div>

      {/* Stats Row */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        {/* Jobs Passed */}
        <div className="bg-bg-secondary rounded-lg border-t-2 border border-profit/40 border-bg-tertiary p-4 shadow-md">
          <div className="flex items-start justify-between">
            <div className="p-2 rounded-lg bg-profit/10">
              <CheckCircle className="h-4 w-4 text-profit" />
            </div>
          </div>
          <p className="text-xl font-mono font-bold text-profit mt-2">{passedCount}/{PIPELINE_JOBS.length}</p>
          <p className="text-xs text-text-secondary mt-1">Jobs Passed</p>
        </div>

        {/* Running */}
        <div className="bg-bg-secondary rounded-lg border-t-2 border border-neutral/40 border-bg-tertiary p-4 shadow-md">
          <div className="flex items-start justify-between">
            <div className="p-2 rounded-lg bg-neutral/10">
              <Loader2 className={cn("h-4 w-4 text-neutral", runningCount > 0 && "animate-spin")} />
            </div>
          </div>
          <p className="text-xl font-mono font-bold text-neutral mt-2">{runningCount}</p>
          <p className="text-xs text-text-secondary mt-1">Running</p>
        </div>

        {/* Upstox Token */}
        <div
          className={cn(
            "bg-bg-secondary rounded-lg border-t-2 border border-bg-tertiary p-4 shadow-md",
            upstoxHealth?.token_valid ? "border-t-profit/40" : "border-t-loss/40",
          )}
        >
          <div className="flex items-start justify-between">
            <div className={cn("p-2 rounded-lg", upstoxHealth?.token_valid ? "bg-profit/10" : "bg-loss/10")}>
              <Key className={cn("h-4 w-4", upstoxHealth?.token_valid ? "text-profit" : "text-loss")} />
            </div>
          </div>
          <p className={cn("text-xl font-mono font-bold mt-2", upstoxHealth?.token_valid ? "text-profit" : "text-loss")}>
            {upstoxHealth?.token_valid ? "Valid" : "Expired"}
          </p>
          <p className="text-xs text-text-secondary mt-1">Upstox Token</p>
        </div>

        {/* WebSocket */}
        <div className="bg-bg-secondary rounded-lg border-t-2 border border-accent/40 border-bg-tertiary p-4 shadow-md">
          <div className="flex items-start justify-between">
            <div className="p-2 rounded-lg bg-accent/10">
              <Wifi className="h-4 w-4 text-accent" />
            </div>
          </div>
          <p className="text-xl font-mono font-bold text-accent mt-2">
            {marketOpen ? "Active" : "Idle"}
          </p>
          <p className="text-xs text-text-secondary mt-1">WebSocket</p>
        </div>
      </div>

      {/* Pipeline Timeline */}
      <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-5">
        <h3 className="text-sm font-medium mb-4">Pipeline Timeline — Today</h3>
        <div className="relative pl-6">
          {jobStatuses.map((job, i) => {
            const color = statusColor(job.status);
            const icon = statusIcon(job.status);
            const isRunning = icon === "running";
            const isPassed = icon === "checkmark";
            return (
              <div key={job.name} className="flex items-center gap-3.5 py-2.5 relative">
                {/* Connector line */}
                {i < jobStatuses.length - 1 && (
                  <div
                    className="absolute left-[15px] top-[36px] w-0.5"
                    style={{
                      height: "calc(100% - 12px)",
                      background: (isPassed || isRunning) ? `${color}30` : "#1e293b",
                    }}
                  />
                )}

                {/* Status dot with optional ping ring */}
                <div className="relative z-10 shrink-0">
                  {isRunning && (
                    <div
                      className="absolute inset-0 rounded-full animate-ping"
                      style={{ background: `${color}40` }}
                    />
                  )}
                  <div
                    className={cn(
                      "w-8 h-8 rounded-full flex items-center justify-center z-10 relative",
                      dotClass(job.status),
                    )}
                  >
                    <DotIcon status={job.status} />
                  </div>
                </div>

                {/* Label */}
                <div className="flex-1 min-w-0">
                  <span className="text-xs font-semibold">{job.label}</span>
                  {job.desc && (
                    <p className="text-[10px] text-text-secondary truncate">{job.desc}</p>
                  )}
                </div>

                {/* Duration / last run */}
                {job.lastRun && (
                  <span className="text-[10px] text-text-secondary font-mono shrink-0">{job.lastRun}</span>
                )}

                {/* Time */}
                <span className="font-mono text-[11px] text-text-secondary shrink-0">{job.cron}</span>

                {/* Status badge */}
                <span
                  className="text-[10px] px-2 py-0.5 rounded font-semibold uppercase shrink-0"
                  style={{ background: `${color}15`, color }}
                >
                  {job.status}
                </span>
              </div>
            );
          })}
        </div>
      </div>

      {/* Admin: Manual Trigger */}
      {isAdmin() && (
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-5">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h3 className="text-sm font-medium">Manual Trigger</h3>
              <p className="text-[11px] text-text-secondary mt-0.5">Jobs run in the background — refresh audit log to see progress</p>
            </div>
          </div>
          <div className="grid grid-cols-2 md:grid-cols-3 gap-2">
            {PIPELINE_JOBS.map((job) => {
              const state = triggerStates[job.name] ?? "idle";
              const lastRunEntry = entries.find((e) =>
                !!e.scheduler_job && e.scheduler_job.includes(job.schedulerJobHint),
              );
              const lastRunTime = lastRunEntry?.log_ts ? formatTime(new Date(lastRunEntry.log_ts)) : null;

              return (
                <button
                  key={job.name}
                  disabled={state === "loading"}
                  onClick={async () => {
                    setTriggerStates((s) => ({ ...s, [job.name]: "loading" }));
                    try {
                      await api.triggerJob(job.name);
                      setTriggerStates((s) => ({ ...s, [job.name]: "ok" }));
                      setTimeout(() => setTriggerStates((s) => ({ ...s, [job.name]: "idle" })), 4000);
                      setTimeout(fetchData, 3000);
                    } catch {
                      setTriggerStates((s) => ({ ...s, [job.name]: "error" }));
                      setTimeout(() => setTriggerStates((s) => ({ ...s, [job.name]: "idle" })), 4000);
                    }
                  }}
                  className={cn(
                    "flex items-center gap-2 px-3 py-2.5 rounded-xl text-xs text-left transition-all border",
                    state === "idle"    && "bg-bg-tertiary border-transparent text-text-secondary hover:text-text-primary hover:border-accent/30 hover:bg-gradient-to-r hover:from-bg-tertiary hover:to-accent/5",
                    state === "loading" && "bg-accent/10 border-accent/30 text-accent opacity-80 cursor-not-allowed",
                    state === "ok"      && "bg-profit/10 border-profit/30 text-profit",
                    state === "error"   && "bg-loss/10 border-loss/30 text-loss",
                  )}
                >
                  <span className="shrink-0">
                    {state === "loading" ? (
                      <Loader2 className="h-4 w-4 animate-spin text-accent" />
                    ) : state === "ok" ? (
                      <Check className="h-4 w-4 text-profit" />
                    ) : state === "error" ? (
                      <X className="h-4 w-4 text-loss" />
                    ) : (
                      <Play className="h-4 w-4" />
                    )}
                  </span>
                  <div className="min-w-0">
                    <div className="font-medium truncate">{job.label}</div>
                    <div className="text-[10px] opacity-60 truncate">{job.cron}</div>
                    {lastRunTime && (
                      <div className="text-[10px] text-text-secondary truncate mt-0.5">Last: {lastRunTime}</div>
                    )}
                  </div>
                </button>
              );
            })}
          </div>
          <p className="text-[10px] text-text-secondary mt-3">
            Dispatched to backend · jobs may take several minutes · check Audit Log below for status
          </p>
        </div>
      )}

      {/* Upstox Token Health */}
      {isAdmin() && upstoxHealth && !upstoxHealth.token_valid && (
        <div className="rounded-lg border bg-loss/5 border-loss/20 p-3 flex items-center gap-3">
          <LiveDot status="warning" />
          <div>
            <p className="text-sm font-medium">Upstox Token Expired</p>
            {upstoxHealth.token_expires_at && (
              <p className="text-xs text-text-secondary">Was: {formatTime(new Date(upstoxHealth.token_expires_at))}</p>
            )}
          </div>
          <button
            onClick={() => api.forceTokenRefresh().then(fetchData).catch(() => {})}
            className="ml-auto px-3 py-1 rounded text-xs bg-accent text-white hover:bg-accent/80"
          >
            Force Refresh
          </button>
        </div>
      )}

      {/* Audit Log Table */}
      <div>
        <h3 className="text-sm font-medium mb-2">Audit Log (Today)</h3>
        <DataTable
          columns={columns}
          data={entries}
          emptyMessage="No audit log entries today"
          rowClassName={(r) =>
            cn(
              "border-l-2",
              r.status === "success"
                ? "border-l-profit"
                : r.status === "error" || r.status === "failed"
                ? "border-l-loss"
                : r.status === "running"
                ? "border-l-neutral"
                : "border-l-transparent",
            )
          }
        />
      </div>
    </div>
  );
}
