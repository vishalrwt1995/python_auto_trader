"use client";

import { useEffect, useState, useMemo } from "react";
import { api } from "@/lib/api";
import { DataTable, type Column } from "@/components/shared/DataTable";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { LiveDot } from "@/components/shared/LiveDot";
import { cn, formatTime, isMarketOpen } from "@/lib/utils";
import { useAuthStore } from "@/stores/authStore";
import type { AuditLogEntry } from "@/lib/types";
import { RefreshCw } from "lucide-react";

const PIPELINE_JOBS = [
  { name: "token_refresh",       label: "Token Refresh",    cron: "03:35 IST", desc: "Upstox OAuth token renewal" },
  { name: "universe_refresh",    label: "Universe Refresh", cron: "06:15 IST", desc: "Raw universe refresh + new-symbol backfill" },
  { name: "candle_cache",        label: "Candle Cache",     cron: "07:05 IST", desc: "1D + 5m fetch for all symbols (api_cap=1800)" },
  { name: "candle_finalize",     label: "Candle Finalize",  cron: "07:40 IST", desc: "Terminalize stragglers, no new API fetches" },
  { name: "score_refresh",       label: "Score Refresh",    cron: "08:30 IST", desc: "Compute scores + universe eligibility (cache-only)" },
  { name: "premarket_watchlist", label: "Watchlist Pre",    cron: "09:00 IST", desc: "Pre-market watchlist build" },
  { name: "scanner",             label: "Scanner 5m",       cron: "09:20 IST", desc: "Live signal scan loop (every 5 min)" },
  { name: "watchlist_5m",        label: "Watchlist 5m",     cron: "09:30 IST", desc: "Intraday watchlist refresh" },
  { name: "eod_recon",           label: "EOD Recon",        cron: "15:10 IST", desc: "Force-close open positions (3 passes)" },
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
      .catch(() => {})
      .finally(() => {
        setLoading(false);
        setRefreshing(false);
      });
  };

  useEffect(() => { fetchData(); }, []);

  const jobStatuses = useMemo(() => {
    return PIPELINE_JOBS.map((job) => {
      const keyword = job.name.replace(/_/g, "").toLowerCase();
      const matching = entries.filter(
        (e) =>
          e.module?.toLowerCase().replace(/[_\s]/g, "").includes(keyword) ||
          e.action?.toLowerCase().replace(/[_\s]/g, "").includes(keyword),
      );
      const latest = matching[0];
      return {
        ...job,
        status: latest?.status ?? "pending",
        lastRun: latest?.log_ts ? formatTime(new Date(latest.log_ts)) : "",
        message: latest?.message ?? "",
      };
    });
  }, [entries]);

  const statusIcon = (status: string) => {
    if (status === "ok" || status === "success") return "checkmark" as const;
    if (status === "running") return "running" as const;
    if (status === "error" || status === "failed") return "error" as const;
    return "pending" as const;
  };

  const statusColor = (status: string) => {
    if (status === "ok" || status === "success") return "#22c55e";
    if (status === "running") return "#f59e0b";
    if (status === "error" || status === "failed") return "#ef4444";
    return "#374151";
  };

  const passedCount = jobStatuses.filter((j) => j.status === "ok" || j.status === "success").length;
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
              r.status === "ok" || r.status === "success"
                ? "bg-profit/20 text-profit"
                : r.status === "error" || r.status === "failed"
                  ? "bg-loss/20 text-loss"
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
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 text-center">
          <p className="text-2xl font-mono font-bold text-profit">{passedCount}/{PIPELINE_JOBS.length}</p>
          <p className="text-xs text-text-secondary mt-1">Jobs Passed</p>
        </div>
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 text-center">
          <p className="text-2xl font-mono font-bold text-neutral">{runningCount}</p>
          <p className="text-xs text-text-secondary mt-1">Running</p>
        </div>
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 text-center">
          <p className={cn("text-2xl font-mono font-bold", upstoxHealth?.token_valid ? "text-profit" : "text-loss")}>
            {upstoxHealth?.token_valid ? "Valid" : "Expired"}
          </p>
          <p className="text-xs text-text-secondary mt-1">Upstox Token</p>
        </div>
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 text-center">
          <p className="text-2xl font-mono font-bold text-accent">
            {marketOpen ? "Active" : "Idle"}
          </p>
          <p className="text-xs text-text-secondary mt-1">WebSocket</p>
        </div>
      </div>

      {/* Pipeline Timeline */}
      <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-5">
        <h3 className="text-sm font-medium mb-4">Pipeline Timeline — Today</h3>
        <div className="relative pl-5">
          {jobStatuses.map((job, i) => {
            const color = statusColor(job.status);
            const icon = statusIcon(job.status);
            return (
              <div key={job.name} className="flex items-center gap-3.5 py-2.5 relative">
                {/* Connector line */}
                {i < jobStatuses.length - 1 && (
                  <div
                    className="absolute left-[9px] top-[30px] w-0.5"
                    style={{
                      height: "calc(100% - 10px)",
                      background: (icon === "checkmark" || icon === "running") ? `${color}30` : "#1e293b",
                    }}
                  />
                )}
                {/* Status dot */}
                <div
                  className="w-5 h-5 rounded-full flex items-center justify-center text-[10px] font-bold z-10 shrink-0"
                  style={{
                    background: `${color}20`,
                    border: `2px solid ${color}`,
                    color,
                    ...(icon === "running" ? { boxShadow: `0 0 8px ${color}` } : {}),
                  }}
                >
                  {icon === "checkmark" ? "✓" : icon === "running" ? "●" : "○"}
                </div>
                {/* Label */}
                <div className="flex-1 min-w-0">
                  <span className="text-xs font-semibold">{job.label}</span>
                  {"desc" in job && job.desc && (
                    <p className="text-[10px] text-text-secondary truncate">{job.desc}</p>
                  )}
                </div>
                {/* Time */}
                <span className="font-mono text-[11px] text-text-secondary">{job.cron}</span>
                {/* Status badge */}
                <span
                  className="text-[10px] px-2 py-0.5 rounded font-semibold uppercase"
                  style={{ background: `${color}15`, color }}
                >
                  {job.status === "ok" ? "success" : job.status}
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
                    "flex items-center gap-2 px-3 py-2.5 rounded-lg text-xs text-left transition-all border",
                    state === "idle"    && "bg-bg-tertiary border-transparent text-text-secondary hover:text-text-primary hover:border-accent/30",
                    state === "loading" && "bg-accent/10 border-accent/30 text-accent opacity-80 cursor-not-allowed",
                    state === "ok"      && "bg-profit/10 border-profit/30 text-profit",
                    state === "error"   && "bg-loss/10 border-loss/30 text-loss",
                  )}
                >
                  <span className="text-base leading-none">
                    {state === "loading" ? "⏳" : state === "ok" ? "✓" : state === "error" ? "✗" : "▶"}
                  </span>
                  <div className="min-w-0">
                    <div className="font-medium truncate">{job.label}</div>
                    <div className="text-[10px] opacity-60 truncate">{job.cron}</div>
                  </div>
                </button>
              );
            })}
          </div>
          <p className="text-[10px] text-text-secondary mt-3">
            ✓ = dispatched to backend · jobs may take several minutes · check Audit Log below for status
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
              <p className="text-xs text-text-secondary">Was: {upstoxHealth.token_expires_at}</p>
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
        />
      </div>
    </div>
  );
}
