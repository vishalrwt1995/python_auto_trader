"use client";

import { useEffect, useState, useMemo } from "react";
import { useRouter } from "next/navigation";
import { api } from "@/lib/api";
import { DataTable, type Column } from "@/components/shared/DataTable";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { cn } from "@/lib/utils";
import type { ScanLatest, ScanRow } from "@/lib/types";

const DIRECTION_STYLE: Record<string, string> = {
  BUY:  "bg-profit/20 text-profit",
  SELL: "bg-loss/20 text-loss",
  HOLD: "bg-bg-tertiary text-text-secondary",
  SKIP: "bg-bg-tertiary text-text-secondary",
};

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

function ScoreBar({ score, status }: { score: number; status: string }) {
  if (status === "skip" || score === 0) {
    return <span className="text-text-secondary font-mono text-xs">—</span>;
  }
  return (
    <div className="flex items-center gap-2 justify-end">
      <div className="w-12 h-1.5 bg-bg-tertiary rounded-full overflow-hidden">
        <div
          className={cn(
            "h-full rounded-full",
            score >= 72 ? "bg-profit" : score >= 45 ? "bg-neutral" : "bg-loss",
          )}
          style={{ width: `${Math.min(100, score)}%` }}
        />
      </div>
      <span
        className={cn(
          "font-mono text-xs w-6 text-right tabular-nums",
          score >= 72 ? "text-profit" : score >= 45 ? "text-neutral" : "text-loss",
        )}
      >
        {score}
      </span>
    </div>
  );
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

  const rows = scan?.rows ?? [];

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
        render: (r) => (
          <span
            className={cn(
              "text-[10px] font-semibold px-1.5 py-0.5 rounded",
              DIRECTION_STYLE[r.direction] ?? "bg-bg-tertiary text-text-secondary",
            )}
          >
            {r.direction}
          </span>
        ),
      },
      {
        key: "score",
        label: "Score",
        sortable: true,
        sortValue: (r) => r.score,
        className: "text-right",
        render: (r) => <ScoreBar score={r.score} status={r.status} />,
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
        className: "text-right font-mono tabular-nums text-xs",
        render: (r) =>
          r.rsi ? (
            <span
              className={cn(
                r.rsi < 35 ? "text-loss" : r.rsi > 70 ? "text-profit" : "text-text-primary",
              )}
            >
              {r.rsi.toFixed(1)}
            </span>
          ) : (
            <span className="text-text-secondary">—</span>
          ),
      },
      {
        key: "ema",
        label: "EMA",
        render: (r) => (
          <span
            className={cn(
              "text-[10px]",
              r.emaState === "BULL_STACK" ? "text-profit" :
              r.emaState === "BEAR_STACK" ? "text-loss" : "text-text-secondary",
            )}
          >
            {r.emaState === "BULL_STACK" ? "BULL" : r.emaState === "BEAR_STACK" ? "BEAR" : r.emaState || "—"}
          </span>
        ),
      },
      {
        key: "supertrend",
        label: "ST",
        render: (r) => (
          <span
            className={cn(
              "text-[10px] font-medium",
              r.supertrend === "UP" ? "text-profit" :
              r.supertrend === "DOWN" ? "text-loss" : "text-text-secondary",
            )}
          >
            {r.supertrend === "UP" ? "▲" : r.supertrend === "DOWN" ? "▼" : "—"}
          </span>
        ),
      },
      {
        key: "dailyTrend",
        label: "D-Trend",
        render: (r) => {
          if (!r.daily_trend) return <span className="text-text-secondary text-xs">—</span>;
          return (
            <span
              className={cn(
                "text-[10px] font-medium",
                r.daily_trend === "UP" ? "text-profit" :
                r.daily_trend === "DOWN" ? "text-loss" : "text-text-secondary",
              )}
            >
              {r.daily_trend === "UP" ? "▲ UP" : r.daily_trend === "DOWN" ? "▼ DN" : r.daily_trend}
            </span>
          );
        },
      },
      {
        key: "volRatio",
        label: "Vol",
        sortable: true,
        sortValue: (r) => r.volRatio,
        className: "text-right font-mono text-xs tabular-nums",
        render: (r) =>
          r.volRatio ? (
            <span className={cn(r.volRatio >= 1.5 ? "text-profit" : r.volRatio >= 1.0 ? "text-text-primary" : "text-text-secondary")}>
              {r.volRatio.toFixed(2)}x
            </span>
          ) : (
            <span className="text-text-secondary">—</span>
          ),
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
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-3 text-center">
          <p className="text-xl font-mono font-bold text-text-primary">{counts.all}</p>
          <p className="text-[10px] text-text-secondary mt-0.5">Scanned</p>
        </div>
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-3 text-center">
          <p className="text-xl font-mono font-bold text-profit">{counts.qualified}</p>
          <p className="text-[10px] text-text-secondary mt-0.5">Qualified</p>
        </div>
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-3 text-center">
          <p className="text-xl font-mono font-bold text-cyan-400">{counts.qualified_intraday}</p>
          <p className="text-[10px] text-text-secondary mt-0.5">Intraday</p>
        </div>
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-3 text-center">
          <p className="text-xl font-mono font-bold text-indigo-400">{counts.qualified_swing}</p>
          <p className="text-[10px] text-text-secondary mt-0.5">Swing</p>
        </div>
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-3 text-center">
          <p className="text-xl font-mono font-bold text-neutral">{counts.filtered}</p>
          <p className="text-[10px] text-text-secondary mt-0.5">Filtered</p>
        </div>
      </div>

      {/* Filter reason breakdown */}
      {reasonBreakdown.length > 0 && (
        <div className="flex flex-wrap gap-2">
          {reasonBreakdown.map(([reason, count]) => (
            <span key={reason} className="text-[11px] bg-bg-tertiary rounded px-2 py-0.5 text-text-secondary">
              {REASON_LABEL[reason] ?? reason}: <span className="text-text-primary font-mono">{count}</span>
            </span>
          ))}
        </div>
      )}

      {/* Tabs */}
      <div className="flex gap-1">
        {(["all", "qualified", "filtered", "skip"] as const).map((t) => (
          <button
            key={t}
            onClick={() => setFilterStatus(t)}
            className={cn(
              "px-3 py-1.5 rounded-lg text-xs font-medium transition-colors",
              filterStatus === t
                ? "bg-accent text-white"
                : "bg-bg-tertiary text-text-secondary hover:text-text-primary",
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
