"use client";

import { useEffect, useState, useMemo } from "react";
import { api } from "@/lib/api";
import { DataTable, type Column } from "@/components/shared/DataTable";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { cn, formatTime } from "@/lib/utils";
import type { Signal } from "@/lib/types";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  ScatterChart,
  Scatter,
} from "recharts";

const PIE_COLORS = ["#22c55e", "#ef4444", "#f59e0b", "#3b82f6", "#8b5cf6", "#6b7280"];

export default function SignalsPage() {
  const [signals, setSignals] = useState<Signal[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api
      .getSignalsToday()
      .then((d: any) => setSignals(d.signals ?? []))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const stats = useMemo(() => {
    const total = signals.length;
    const placed = signals.filter((s) => s.entry_placed).length;
    const blocked = total - placed;
    const blockedReasons: Record<string, number> = {};
    signals.forEach((s) => {
      if (s.blocked_reason) {
        blockedReasons[s.blocked_reason] = (blockedReasons[s.blocked_reason] ?? 0) + 1;
      }
    });
    return { total, placed, blocked, blockedReasons };
  }, [signals]);

  const scoreDistribution = useMemo(() => {
    const buckets = Array.from({ length: 10 }, (_, i) => ({
      range: `${i * 10}-${i * 10 + 10}`,
      count: 0,
    }));
    signals.forEach((s) => {
      const idx = Math.min(9, Math.floor(s.score / 10));
      buckets[idx].count++;
    });
    return buckets;
  }, [signals]);

  const blockedPieData = useMemo(() => {
    return Object.entries(stats.blockedReasons).map(([name, value]) => ({
      name,
      value,
    }));
  }, [stats.blockedReasons]);

  const scatterData = useMemo(() => {
    return signals.map((s) => ({
      time: new Date(s.scan_ts).getHours() + new Date(s.scan_ts).getMinutes() / 60,
      score: s.score,
      placed: s.entry_placed,
      symbol: s.symbol,
      direction: s.direction,
    }));
  }, [signals]);

  const columns: Column<Signal>[] = useMemo(
    () => [
      {
        key: "time",
        label: "Time",
        sortable: true,
        sortValue: (r) => r.scan_ts,
        render: (r) => (
          <span className="font-mono text-xs">
            {formatTime(new Date(r.scan_ts))}
          </span>
        ),
      },
      {
        key: "symbol",
        label: "Symbol",
        sortable: true,
        sortValue: (r) => r.symbol,
        render: (r) => <span className="font-medium">{r.symbol}</span>,
      },
      {
        key: "direction",
        label: "Direction",
        render: (r) => (
          <span
            className={cn(
              "px-1.5 py-0.5 rounded text-xs font-medium",
              r.direction === "BUY"
                ? "bg-profit/20 text-profit"
                : "bg-loss/20 text-loss",
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
        className: "text-right font-mono",
        render: (r) => (
          <span
            className={cn(
              r.score >= 72 ? "text-profit" : r.score >= 50 ? "text-neutral" : "text-loss",
            )}
          >
            {r.score}
          </span>
        ),
      },
      {
        key: "ltp",
        label: "LTP",
        className: "text-right font-mono",
        render: (r) => <span>{r.ltp.toFixed(2)}</span>,
      },
      {
        key: "sl",
        label: "SL",
        className: "text-right font-mono text-loss/80",
        render: (r) => <span>{r.sl.toFixed(2)}</span>,
      },
      {
        key: "target",
        label: "Target",
        className: "text-right font-mono text-profit/80",
        render: (r) => <span>{r.target.toFixed(2)}</span>,
      },
      {
        key: "rr",
        label: "R:R",
        className: "text-right font-mono",
        render: (r) => {
          const risk = Math.abs(r.ltp - r.sl);
          const reward = Math.abs(r.target - r.ltp);
          const rr = risk > 0 ? (reward / risk).toFixed(1) : "—";
          return <span>{rr}</span>;
        },
      },
      {
        key: "placed",
        label: "Entry",
        render: (r) => (
          <span className={r.entry_placed ? "text-profit" : "text-loss"}>
            {r.entry_placed ? "Placed" : "Blocked"}
          </span>
        ),
      },
      {
        key: "blocked",
        label: "Blocked Reason",
        render: (r) => (
          <span
            className="text-xs text-text-secondary max-w-[150px] truncate block"
            title={r.blocked_reason}
          >
            {r.blocked_reason || "—"}
          </span>
        ),
      },
      {
        key: "regime",
        label: "Regime",
        render: (r) => <span className="text-xs">{r.regime}</span>,
      },
    ],
    [],
  );

  if (loading) return <LoadingSkeleton lines={10} />;

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-semibold">Signals Log</h1>

      {/* Stats Bar */}
      <div className="grid grid-cols-3 gap-4">
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 text-center">
          <p className="text-3xl font-mono font-bold">{stats.total}</p>
          <p className="text-xs text-text-secondary mt-1">Total Signals</p>
        </div>
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 text-center">
          <p className="text-3xl font-mono font-bold text-profit">{stats.placed}</p>
          <p className="text-xs text-text-secondary mt-1">
            Entries Placed ({stats.total > 0 ? Math.round((stats.placed / stats.total) * 100) : 0}%)
          </p>
        </div>
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 text-center">
          <p className="text-3xl font-mono font-bold text-loss">{stats.blocked}</p>
          <p className="text-xs text-text-secondary mt-1">Blocked</p>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Score Distribution */}
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
          <h3 className="text-sm font-medium mb-2">Score Distribution</h3>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={scoreDistribution}>
              <CartesianGrid stroke="#1f2937" strokeDasharray="3 3" />
              <XAxis dataKey="range" tick={{ fill: "#9ca3af", fontSize: 9 }} />
              <YAxis tick={{ fill: "#9ca3af", fontSize: 9 }} />
              <Tooltip
                contentStyle={{ backgroundColor: "#111827", border: "1px solid #1f2937", fontSize: 12 }}
              />
              <Bar dataKey="count" fill="#3b82f6" radius={[2, 2, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Blocked Reason Breakdown */}
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
          <h3 className="text-sm font-medium mb-2">Blocked Reasons</h3>
          {blockedPieData.length > 0 ? (
            <ResponsiveContainer width="100%" height={200}>
              <PieChart>
                <Pie
                  data={blockedPieData}
                  cx="50%"
                  cy="50%"
                  innerRadius={40}
                  outerRadius={70}
                  dataKey="value"
                  nameKey="name"
                >
                  {blockedPieData.map((_, i) => (
                    <Cell key={i} fill={PIE_COLORS[i % PIE_COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip
                  contentStyle={{ backgroundColor: "#111827", border: "1px solid #1f2937", fontSize: 11 }}
                />
              </PieChart>
            </ResponsiveContainer>
          ) : (
            <div className="h-[200px] flex items-center justify-center text-xs text-text-secondary">
              No blocked signals
            </div>
          )}
          <div className="mt-2 space-y-1">
            {blockedPieData.map((d, i) => (
              <div key={d.name} className="flex items-center gap-2 text-xs">
                <div
                  className="w-2.5 h-2.5 rounded-full shrink-0"
                  style={{ backgroundColor: PIE_COLORS[i % PIE_COLORS.length] }}
                />
                <span className="text-text-secondary truncate">{d.name}</span>
                <span className="ml-auto font-mono">{d.value}</span>
              </div>
            ))}
          </div>
        </div>

        {/* Signal Timeline Scatter */}
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
          <h3 className="text-sm font-medium mb-2">Signal Timeline</h3>
          <ResponsiveContainer width="100%" height={200}>
            <ScatterChart>
              <CartesianGrid stroke="#1f2937" strokeDasharray="3 3" />
              <XAxis
                dataKey="time"
                name="Hour"
                tick={{ fill: "#9ca3af", fontSize: 9 }}
                domain={[9, 16]}
                tickFormatter={(v) => `${Math.floor(v)}:00`}
              />
              <YAxis
                dataKey="score"
                name="Score"
                tick={{ fill: "#9ca3af", fontSize: 9 }}
                domain={[0, 100]}
              />
              <Tooltip
                contentStyle={{ backgroundColor: "#111827", border: "1px solid #1f2937", fontSize: 11 }}
                formatter={(val: number, name: string) => [
                  name === "time" ? `${Math.floor(val)}:${String(Math.round((val % 1) * 60)).padStart(2, "0")}` : val,
                  name,
                ]}
              />
              <Scatter
                data={scatterData.filter((d) => d.placed)}
                fill="#22c55e"
                shape="circle"
              />
              <Scatter
                data={scatterData.filter((d) => !d.placed)}
                fill="#ef4444"
                shape="cross"
              />
            </ScatterChart>
          </ResponsiveContainer>
          <div className="flex gap-4 mt-2 text-[10px] text-text-secondary justify-center">
            <span className="flex items-center gap-1">
              <span className="w-2 h-2 rounded-full bg-profit" /> Placed
            </span>
            <span className="flex items-center gap-1">
              <span className="w-2 h-2 rounded-full bg-loss" /> Blocked
            </span>
          </div>
        </div>
      </div>

      {/* Signals Table */}
      <DataTable
        columns={columns}
        data={signals}
        emptyMessage="No signals generated today"
      />
    </div>
  );
}
