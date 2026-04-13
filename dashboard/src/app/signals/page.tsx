"use client";

import { useEffect, useState, useMemo, useRef } from "react";
import { useRouter } from "next/navigation";
import { api } from "@/lib/api";
import { DataTable, type Column } from "@/components/shared/DataTable";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { cn } from "@/lib/utils";
import type { ScanLatest, ScanRow } from "@/lib/types";
import { ScanLine, CheckCircle, Zap, TrendingUp, Filter } from "lucide-react";

const STATUS_STYLE: Record<string, string> = {
  qualified: "text-profit font-semibold",
  filtered:  "text-text-secondary",
  skip:      "text-bg-tertiary",
};

const REASON_LABEL: Record<string, string> = {
  entry_qualified:              "✓ Entry placed",
  direction_hold:               "Direction: HOLD",
  score_below_min:              "Score too low",
  policy_long_disabled:         "Longs disabled",
  policy_short_disabled:        "Shorts disabled",
  policy_strategy_blocked:      "Strategy blocked",
  policy_max_positions_reached: "Max positions",
  live_price_below_vwap:        "Price < VWAP",
  live_price_above_vwap:        "Price > VWAP",
  entry_window_closed_or_blocked: "Window closed",
  insufficient_candles:         "No candles",
};

function ScoreBar({ row }: { row: ScanRow }) {
  const { score, status, minScore, affinityMult, dailyStrength, daily_trend, reason } = row;
  const [show, setShow] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  if (status === "skip" || score === 0) {
    return <span className="text-text-secondary font-mono text-xs">—</span>;
  }

  const threshold = minScore ?? 72;
  const gap = threshold - score;
  const affinity = affinityMult ?? 1.0;

  // Estimate layer contributions from available data
  const alignmentPenalty = daily_trend === "DOWN" && row.direction === "BUY" ? -10
    : daily_trend === "UP" && row.direction === "SELL" ? -10
    : daily_trend === "NEUTRAL" ? 5 : 0;

  return (
    <div className="relative" ref={ref}>
      <div
        className="flex items-center gap-2 justify-end px-1 py-0.5 rounded cursor-pointer"
        style={{
          background:
            score >= 80 ? "rgba(34,197,94,0.08)"
            : score >= 60 ? "rgba(59,130,246,0.05)"
            : "transparent",
        }}
        onMouseEnter={() => setShow(true)}
        onMouseLeave={() => setShow(false)}
      >
        <div className="w-12 h-1.5 bg-bg-tertiary rounded-full overflow-hidden">
          <div
            className="h-full rounded-full bg-gradient-to-r from-accent/50 to-accent"
            style={{ width: `${Math.min(100, score)}%` }}
          />
        </div>
        <span
          className={cn(
            "font-mono text-xs w-6 text-right tabular-nums",
            score >= threshold ? "text-profit" : score >= 45 ? "text-neutral" : "text-loss",
          )}
        >
          {score}
        </span>
      </div>

      {show && (
        <div
          className="absolute z-50 right-0 top-6 w-56 rounded-xl border border-bg-tertiary shadow-2xl p-3 text-[11px] space-y-1.5"
          style={{ background: "#0f1623", backdropFilter: "blur(12px)" }}
        >
          {/* Header */}
          <div className="flex justify-between items-center pb-1 border-b border-bg-tertiary/60">
            <span className="text-text-secondary font-medium">Score Breakdown</span>
            <span className={cn("font-mono font-bold", score >= threshold ? "text-profit" : "text-loss")}>
              {score} / {threshold}
            </span>
          </div>

          {/* Regime conditions */}
          <div className="space-y-1">
            <div className="flex justify-between">
              <span className="text-text-secondary">Daily Trend</span>
              <span className={cn("font-mono", daily_trend === "UP" ? "text-profit" : daily_trend === "DOWN" ? "text-loss" : "text-neutral")}>
                {daily_trend ?? "—"} {dailyStrength != null ? `(${dailyStrength.toFixed(0)}%)` : ""}
              </span>
            </div>
            {alignmentPenalty !== 0 && (
              <div className="flex justify-between">
                <span className="text-text-secondary">Alignment</span>
                <span className={cn("font-mono", alignmentPenalty > 0 ? "text-profit" : "text-loss")}>
                  {alignmentPenalty > 0 ? "+" : ""}{alignmentPenalty}
                </span>
              </div>
            )}
            <div className="flex justify-between">
              <span className="text-text-secondary">Affinity Mult</span>
              <span className={cn("font-mono", affinity >= 1 ? "text-profit" : affinity >= 0.9 ? "text-neutral" : "text-loss")}>
                {affinity.toFixed(2)}×
              </span>
            </div>
          </div>

          {/* Gap to qualify */}
          <div className="pt-1 border-t border-bg-tertiary/60">
            {gap > 0 ? (
              <>
                <div className="flex justify-between mb-1">
                  <span className="text-text-secondary">Gap to qualify</span>
                  <span className="font-mono text-loss">+{gap} needed</span>
                </div>
                <div className="w-full h-1 bg-bg-tertiary rounded-full overflow-hidden">
                  <div
                    className="h-full rounded-full bg-gradient-to-r from-loss/60 to-accent"
                    style={{ width: `${Math.min(100, (score / threshold) * 100)}%` }}
                  />
                </div>
              </>
            ) : (
              <div className="flex justify-between">
                <span className="text-text-secondary">Status</span>
                <span className="font-mono text-profit">✓ Qualified</span>
              </div>
            )}
          </div>

          {/* Block reason */}
          {reason && reason !== "entry_qualified" && (
            <div className="pt-1 border-t border-bg-tertiary/60">
              <span className="text-text-secondary">Blocked: </span>
              <span className="text-neutral">{reason.replace(/_/g, " ")}</span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function RsiDisplay({ rsi }: { rsi: number | null | undefined }) {
  if (rsi == null) return <span className="text-text-secondary">—</span>;
  const color =
    rsi < 35
      ? "text-profit"
      : rsi > 70
      ? "text-loss"
      : rsi >= 45 && rsi <= 55
      ? "text-text-secondary"
      : "text-text-primary";
  const barWidth = Math.min(100, Math.max(0, rsi));
  const barColor =
    rsi < 35 ? "#22c55e" : rsi > 70 ? "#ef4444" : "#3b82f6";
  return (
    <div className="flex items-center gap-1.5 justify-end">
      <div className="w-8 h-1 bg-bg-tertiary rounded-full overflow-hidden">
        <div
          className="h-full rounded-full"
          style={{ width: `${barWidth}%`, backgroundColor: barColor }}
        />
      </div>
      <span className={cn("font-mono text-xs tabular-nums", color)}>
        {rsi.toFixed(1)}
      </span>
    </div>
  );
}

function VolRatioDisplay({ volRatio }: { volRatio: number | null | undefined }) {
  if (!volRatio) return <span className="text-text-secondary">—</span>;
  if (volRatio >= 3.0) {
    return (
      <span className="bg-loss/10 text-loss rounded px-1 font-mono text-xs tabular-nums">
        {volRatio.toFixed(2)}x
      </span>
    );
  }
  if (volRatio >= 1.5) {
    return (
      <span className="bg-neutral/10 text-neutral rounded px-1 font-mono text-xs tabular-nums">
        {volRatio.toFixed(2)}x
      </span>
    );
  }
  if (volRatio >= 1.0) {
    return (
      <span className="text-text-secondary font-mono text-xs tabular-nums">
        {volRatio.toFixed(2)}x
      </span>
    );
  }
  return (
    <span className="text-text-secondary opacity-60 font-mono text-xs tabular-nums">
      {volRatio.toFixed(2)}x
    </span>
  );
}

function TrendDisplay({ value }: { value: string | null | undefined }) {
  if (!value) return <span className="text-text-secondary text-[10px]">—</span>;
  if (value === "UP" || value === "BULL_STACK") {
    return <span className="text-profit text-[10px] font-medium">▲ {value === "BULL_STACK" ? "BULL" : "UP"}</span>;
  }
  if (value === "DOWN" || value === "BEAR_STACK") {
    return <span className="text-loss text-[10px] font-medium">▼ {value === "BEAR_STACK" ? "BEAR" : "DN"}</span>;
  }
  return <span className="text-text-secondary text-[10px]">— {value}</span>;
}

export default function SignalsPage() {
  const router = useRouter();
  const [scan, setScan] = useState<ScanLatest | null>(null);
  const [loading, setLoading] = useState(true);
  const [filterStatus, setFilterStatus] = useState<"all" | "qualified" | "filtered" | "skip">("all");

  useEffect(() => {
    api
      .getScanLatest()
      .then((d) => setScan(d))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const rows = useMemo(() => scan?.rows ?? [], [scan]);

  const filtered = useMemo(() => {
    if (filterStatus === "all") return rows;
    return rows.filter((r) => r.status === filterStatus);
  }, [rows, filterStatus]);

  const counts = useMemo(() => ({
    all:               rows.length,
    qualified:         rows.filter((r) => r.status === "qualified").length,
    qualified_swing:   rows.filter((r) => r.status === "qualified" && r.wl_type === "swing").length,
    qualified_intraday:rows.filter((r) => r.status === "qualified" && r.wl_type !== "swing").length,
    filtered:          rows.filter((r) => r.status === "filtered").length,
    skip:              rows.filter((r) => r.status === "skip").length,
  }), [rows]);

  // Reason breakdown for filtered rows
  const reasonBreakdown = useMemo(() => {
    const map: Record<string, number> = {};
    rows.filter((r) => r.status === "filtered").forEach((r) => {
      const key = r.reason || "unknown";
      map[key] = (map[key] ?? 0) + 1;
    });
    return Object.entries(map).sort((a, b) => b[1] - a[1]);
  }, [rows]);

  // Qualified reason breakdown
  const qualifiedBreakdown = useMemo(() => {
    const map: Record<string, number> = {};
    rows.filter((r) => r.status === "qualified").forEach((r) => {
      const key = r.reason || "entry_qualified";
      map[key] = (map[key] ?? 0) + 1;
    });
    return Object.entries(map).sort((a, b) => b[1] - a[1]);
  }, [rows]);

  const columns: Column<ScanRow>[] = useMemo(
    () => [
      {
        key: "symbol",
        label: "Symbol",
        sortable: true,
        sortValue: (r) => r.symbol,
        render: (r) => (
          <div className="flex items-center gap-1.5">
            <span className="font-semibold text-sm text-text-primary">{r.symbol}</span>
            {r.wl_type && (
              <span
                className={cn(
                  "text-[9px] font-semibold px-1 py-0.5 rounded",
                  r.wl_type === "swing"
                    ? "bg-indigo-500/15 text-indigo-400"
                    : "bg-cyan-500/15 text-cyan-400",
                )}
              >
                {r.wl_type === "swing" ? "SW" : "ID"}
              </span>
            )}
          </div>
        ),
      },
      {
        key: "direction",
        label: "Dir",
        sortable: true,
        sortValue: (r) => r.direction,
        render: (r) => {
          if (r.direction === "BUY") {
            return (
              <span
                className="text-[10px] font-semibold px-2 py-1 rounded border bg-profit/20 text-profit border-profit/30 inline-flex items-center gap-0.5"
                style={{ boxShadow: "0 0 6px rgba(34,197,94,0.2)" }}
              >
                ↑ BUY
              </span>
            );
          }
          if (r.direction === "SELL") {
            return (
              <span
                className="text-[10px] font-semibold px-2 py-1 rounded border bg-loss/20 text-loss border-loss/30 inline-flex items-center gap-0.5"
                style={{ boxShadow: "0 0 6px rgba(239,68,68,0.2)" }}
              >
                ↓ SELL
              </span>
            );
          }
          return (
            <span className="text-[10px] font-semibold px-2 py-1 rounded bg-bg-tertiary text-text-secondary">
              {r.direction}
            </span>
          );
        },
      },
      {
        key: "score",
        label: "Signal Score",
        tooltip: "Live intraday entry score (0–100). Computed from 5m candles with regime penalties (VIX, RANGE, daily trend alignment). Must exceed threshold (~72 in NORMAL) to qualify. Hover a row for breakdown.",
        sortable: true,
        sortValue: (r) => r.score,
        className: "text-right",
        render: (r) => <ScoreBar row={r} />,
      },
      {
        key: "ltp",
        label: "LTP",
        sortable: true,
        sortValue: (r) => r.ltp,
        className: "text-right font-mono tabular-nums text-xs",
        render: (r) =>
          r.ltp ? (
            <span>
              {r.ltp.toFixed(2)}
              {r.changePct !== 0 && (
                <span className={cn("ml-1 text-[10px]", r.changePct > 0 ? "text-profit" : "text-loss")}>
                  {r.changePct > 0 ? "+" : ""}{r.changePct.toFixed(1)}%
                </span>
              )}
            </span>
          ) : (
            <span className="text-text-secondary">—</span>
          ),
      },
      {
        key: "rsi",
        label: "RSI",
        sortable: true,
        sortValue: (r) => r.rsi,
        className: "text-right",
        render: (r) => <RsiDisplay rsi={r.rsi} />,
      },
      {
        key: "ema",
        label: "EMA",
        render: (r) => <TrendDisplay value={r.emaState} />,
      },
      {
        key: "supertrend",
        label: "ST",
        render: (r) => {
          if (!r.supertrend) return <span className="text-text-secondary text-[10px]">—</span>;
          if (r.supertrend === "UP") return <span className="text-profit text-[10px] font-medium">▲</span>;
          if (r.supertrend === "DOWN") return <span className="text-loss text-[10px] font-medium">▼</span>;
          return <span className="text-text-secondary text-[10px]">—</span>;
        },
      },
      {
        key: "dailyTrend",
        label: "D-Trend",
        render: (r) => <TrendDisplay value={r.daily_trend} />,
      },
      {
        key: "volRatio",
        label: "Vol",
        sortable: true,
        sortValue: (r) => r.volRatio,
        className: "text-right",
        render: (r) => <VolRatioDisplay volRatio={r.volRatio} />,
      },
      {
        key: "setup",
        label: "Setup",
        render: (r) => (
          <span className="text-[10px] text-text-secondary">{r.setup || "—"}</span>
        ),
      },
      {
        key: "status",
        label: "Status",
        sortable: true,
        sortValue: (r) => r.status,
        render: (r) => (
          <span className={cn("text-xs", STATUS_STYLE[r.status] ?? "text-text-secondary")}>
            {REASON_LABEL[r.reason] ?? r.reason ?? r.status}
          </span>
        ),
      },
      {
        key: "sl_target",
        label: "SL / Target",
        className: "text-right font-mono text-xs tabular-nums",
        render: (r) =>
          r.sl && r.target ? (
            <span>
              <span className="text-loss">{r.sl.toFixed(2)}</span>
              <span className="text-text-secondary mx-1">/</span>
              <span className="text-profit">{r.target.toFixed(2)}</span>
            </span>
          ) : (
            <span className="text-text-secondary">—</span>
          ),
      },
    ],
    [],
  );

  if (loading) return <LoadingSkeleton lines={10} />;

  const scanTime = scan?.scan_ts
    ? new Date(scan.scan_ts).toLocaleTimeString("en-IN", { hour: "2-digit", minute: "2-digit", timeZone: "Asia/Kolkata" })
    : null;

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2">
        <div>
          <h1 className="text-xl font-semibold">Scanner</h1>
          {scan?.scan_ts && (
            <p className="text-[11px] text-text-secondary mt-0.5">
              Last scan {scanTime} IST · {scan.regime} · {scan.risk_mode}
            </p>
          )}
        </div>
        {scan && (
          <div className="text-xs text-text-secondary">
            {filtered.length} of {scan.scanned} shown · watchlist {scan.total_watchlist}
          </div>
        )}
      </div>

      {/* Stats */}
      <div className="grid grid-cols-2 sm:grid-cols-5 gap-3">
        {/* Scanned */}
        <div className="bg-bg-secondary rounded-lg border-t-[3px] border-t-slate-500 border-x border-b border-bg-tertiary p-3 shadow-md shadow-black/20 flex items-start justify-between">
          <div>
            <p className="text-xl font-mono font-bold text-text-primary">{counts.all}</p>
            <p className="text-[10px] text-text-secondary mt-0.5">Scanned</p>
          </div>
          <ScanLine className="h-4 w-4 text-slate-500 mt-0.5" />
        </div>
        {/* Qualified */}
        <div className="bg-bg-secondary rounded-lg border-t-[3px] border-t-[#22c55e] border-x border-b border-bg-tertiary p-3 shadow-md shadow-black/20 flex items-start justify-between">
          <div>
            <p className="text-xl font-mono font-bold text-profit">{counts.qualified}</p>
            <p className="text-[10px] text-text-secondary mt-0.5">Qualified</p>
          </div>
          <CheckCircle className="h-4 w-4 text-profit mt-0.5" />
        </div>
        {/* Intraday */}
        <div className="bg-bg-secondary rounded-lg border-t-[3px] border-t-cyan-400 border-x border-b border-bg-tertiary p-3 shadow-md shadow-black/20 flex items-start justify-between">
          <div>
            <p className="text-xl font-mono font-bold text-cyan-400">{counts.qualified_intraday}</p>
            <p className="text-[10px] text-text-secondary mt-0.5">Intraday</p>
          </div>
          <Zap className="h-4 w-4 text-cyan-400 mt-0.5" />
        </div>
        {/* Swing */}
        <div className="bg-bg-secondary rounded-lg border-t-[3px] border-t-indigo-400 border-x border-b border-bg-tertiary p-3 shadow-md shadow-black/20 flex items-start justify-between">
          <div>
            <p className="text-xl font-mono font-bold text-indigo-400">{counts.qualified_swing}</p>
            <p className="text-[10px] text-text-secondary mt-0.5">Swing</p>
          </div>
          <TrendingUp className="h-4 w-4 text-indigo-400 mt-0.5" />
        </div>
        {/* Filtered */}
        <div className="bg-bg-secondary rounded-lg border-t-[3px] border-t-neutral border-x border-b border-bg-tertiary p-3 shadow-md shadow-black/20 flex items-start justify-between">
          <div>
            <p className="text-xl font-mono font-bold text-neutral">{counts.filtered}</p>
            <p className="text-[10px] text-text-secondary mt-0.5">Filtered</p>
          </div>
          <Filter className="h-4 w-4 text-neutral mt-0.5" />
        </div>
      </div>

      {/* Reason pill breakdown */}
      {(qualifiedBreakdown.length > 0 || reasonBreakdown.length > 0) && (
        <div className="flex flex-wrap gap-2">
          {qualifiedBreakdown.map(([reason, count]) => (
            <span
              key={`q-${reason}`}
              className="text-[11px] bg-profit/10 border border-profit/20 rounded-full px-2.5 py-0.5 text-profit flex items-center gap-1.5"
            >
              {REASON_LABEL[reason] ?? reason}
              <span className="bg-bg-tertiary rounded-full px-2 py-0.5 text-[10px] text-text-primary font-mono">
                {count}
              </span>
            </span>
          ))}
          {reasonBreakdown.map(([reason, count]) => (
            <span
              key={`f-${reason}`}
              className="text-[11px] bg-loss/10 border border-loss/20 rounded-full px-2.5 py-0.5 text-loss/80 flex items-center gap-1.5"
            >
              {REASON_LABEL[reason] ?? reason}
              <span className="bg-bg-tertiary rounded-full px-2 py-0.5 text-[10px] text-text-primary font-mono">
                {count}
              </span>
            </span>
          ))}
        </div>
      )}

      {/* Tabs */}
      <div className="inline-flex bg-bg-tertiary/50 rounded-xl p-0.5">
        {(["all", "qualified", "filtered", "skip"] as const).map((t) => (
          <button
            key={t}
            onClick={() => setFilterStatus(t)}
            className={cn(
              "px-3 py-1.5 rounded-[10px] text-xs font-medium transition-all",
              filterStatus === t
                ? "bg-gradient-to-r from-accent to-blue-600 text-white shadow shadow-accent/30"
                : "text-text-secondary hover:text-text-primary",
            )}
          >
            {t === "all"
              ? `All (${counts.all})`
              : t === "qualified"
              ? `✓ Qualified (${counts.qualified})`
              : t === "filtered"
              ? `Filtered (${counts.filtered})`
              : `Skipped (${counts.skip})`}
          </button>
        ))}
      </div>

      {/* No data */}
      {!scan ? (
        <div className="text-center py-12 text-sm text-text-secondary">
          No scan data yet. Scanner runs every 5 min during market hours (9:20–15:00 IST).
        </div>
      ) : (
        <DataTable
          columns={columns}
          data={filtered}
          onRowClick={(r) => router.push(`/symbol/${r.symbol}`)}
          emptyMessage={`No ${filterStatus === "all" ? "" : filterStatus} symbols`}
        />
      )}
    </div>
  );
}
