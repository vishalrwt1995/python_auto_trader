"use client";

import React, { useEffect, useState, useMemo } from "react";
import { api } from "@/lib/api";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { EquityCurve } from "@/components/charts/EquityCurve";
import { cn, formatCurrency, formatPercent } from "@/lib/utils";
import type { Trade, TradeSummary } from "@/lib/types";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
  AreaChart,
  Area,
  ReferenceLine,
  ReferenceArea,
} from "recharts";

type DateRange = "30d" | "90d" | "180d" | "all";

function daysAgo(n: number): string {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d.toISOString().split("T")[0];
}

// Icons as inline SVGs
function IconTrendingUp({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <polyline points="23 6 13.5 15.5 8.5 10.5 1 18" />
      <polyline points="17 6 23 6 23 12" />
    </svg>
  );
}
function IconTarget({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <circle cx="12" cy="12" r="10" />
      <circle cx="12" cy="12" r="6" />
      <circle cx="12" cy="12" r="2" />
    </svg>
  );
}
function IconBarChart2({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <line x1="18" y1="20" x2="18" y2="10" />
      <line x1="12" y1="20" x2="12" y2="4" />
      <line x1="6" y1="20" x2="6" y2="14" />
    </svg>
  );
}
function IconActivity({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <polyline points="22 12 18 12 15 21 9 3 6 12 2 12" />
    </svg>
  );
}
function IconTrophy({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path d="M6 9H4.5a2.5 2.5 0 010-5H6" />
      <path d="M18 9h1.5a2.5 2.5 0 000-5H18" />
      <path d="M4 22h16" />
      <path d="M10 14.66V17c0 .55-.47.98-.97 1.21C7.85 18.75 7 20.24 7 22" />
      <path d="M14 14.66V17c0 .55.47.98.97 1.21C16.15 18.75 17 20.24 17 22" />
      <path d="M18 2H6v7a6 6 0 0012 0V2z" />
    </svg>
  );
}
function IconAlertTriangle({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z" />
      <line x1="12" y1="9" x2="12" y2="13" />
      <line x1="12" y1="17" x2="12.01" y2="17" />
    </svg>
  );
}
function IconLineChart({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <line x1="2" y1="20" x2="22" y2="20" />
      <path d="M5 14l4-4 4 4 4-8" />
    </svg>
  );
}
function IconTrendingDown({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <polyline points="23 18 13.5 8.5 8.5 13.5 1 6" />
      <polyline points="17 18 23 18 23 12" />
    </svg>
  );
}
function IconCalendar({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <rect x="3" y="4" width="18" height="18" rx="2" ry="2" />
      <line x1="16" y1="2" x2="16" y2="6" />
      <line x1="8" y1="2" x2="8" y2="6" />
      <line x1="3" y1="10" x2="21" y2="10" />
    </svg>
  );
}
function IconClock({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <circle cx="12" cy="12" r="10" />
      <polyline points="12 6 12 12 16 14" />
    </svg>
  );
}
function IconPieChart({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path d="M21.21 15.89A10 10 0 118 2.83" />
      <path d="M22 12A10 10 0 0012 2v10z" />
    </svg>
  );
}

export default function AnalyticsPage() {
  const [range, setRange] = useState<DateRange>("90d");
  const [trades, setTrades] = useState<Trade[]>([]);
  const [summary, setSummary] = useState<TradeSummary | null>(null);
  const [equityData, setEquityData] = useState<{ date: string; pnl: number }[]>([]);
  const [loading, setLoading] = useState(true);

  const fromDate = range === "all" ? "2020-01-01" : daysAgo(range === "30d" ? 30 : range === "90d" ? 90 : 180);

  useEffect(() => {
    setLoading(true);
    Promise.all([
      api.getTradeSummary(fromDate).then((d) => setSummary(d as unknown as TradeSummary)),
      api.getEquityCurve(fromDate).then((d: any) => setEquityData(d.series ?? [])),
      api.getTrades({ from: fromDate, limit: "500" }).then((d: any) => setTrades(d.trades ?? [])),
    ])
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [range, fromDate]);

  // Monthly P&L heatmap data
  const monthlyPnl = useMemo(() => {
    const map: Record<string, number> = {};
    trades.forEach((t) => {
      const month = t.trade_date.slice(0, 7); // YYYY-MM
      map[month] = (map[month] ?? 0) + t.pnl;
    });
    return Object.entries(map)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([month, pnl]) => ({ month, pnl }));
  }, [trades]);

  // Weekly P&L — use IST date and Monday-based week
  const weeklyPnl = useMemo(() => {
    const map: Record<string, number> = {};
    trades.forEach((t) => {
      // trade_date is already YYYY-MM-DD in IST from backend — anchor to IST midnight
      const d = new Date(t.trade_date + "T00:00:00+05:30");
      const day = d.getDay(); // 0=Sun … 6=Sat
      const diff = day === 0 ? -6 : 1 - day; // shift to Monday
      const weekStart = new Date(d);
      weekStart.setDate(d.getDate() + diff);
      const key = weekStart.toISOString().split("T")[0];
      map[key] = (map[key] ?? 0) + t.pnl;
    });
    return Object.entries(map)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([week, pnl]) => ({ week, pnl }));
  }, [trades]);

  // Drawdown series
  const drawdownData = useMemo(() => {
    let peak = 0;
    let cum = 0;
    return equityData.map((d) => {
      cum = d.pnl;
      if (cum > peak) peak = cum;
      const dd = peak > 0 ? ((cum - peak) / peak) * 100 : 0;
      return { date: d.date, drawdown: dd };
    });
  }, [equityData]);

  const maxDrawdown = useMemo(() => {
    if (drawdownData.length === 0) return 0;
    return Math.min(...drawdownData.map((d) => d.drawdown));
  }, [drawdownData]);

  // Win/Loss streak analysis
  const streaks = useMemo(() => {
    let maxWin = 0, maxLoss = 0, curWin = 0, curLoss = 0;
    trades.forEach((t) => {
      if (t.pnl > 0) {
        curWin++;
        curLoss = 0;
        if (curWin > maxWin) maxWin = curWin;
      } else if (t.pnl < 0) {
        curLoss++;
        curWin = 0;
        if (curLoss > maxLoss) maxLoss = curLoss;
      }
    });
    return { maxWin, maxLoss };
  }, [trades]);

  // Hourly distribution — convert entry_ts to IST before extracting hour
  const hourlyDist = useMemo(() => {
    const hours: Record<number, { count: number; pnl: number }> = {};
    trades.forEach((t) => {
      if (!t.entry_ts) return;
      const istDate = new Date(
        new Date(t.entry_ts).toLocaleString("en-US", { timeZone: "Asia/Kolkata" }),
      );
      const h = istDate.getHours();
      if (!hours[h]) hours[h] = { count: 0, pnl: 0 };
      hours[h].count++;
      hours[h].pnl += t.pnl;
    });
    return Array.from({ length: 24 }, (_, h) => ({
      hour: `${h}:00`,
      pnlHour: h,
      count: hours[h]?.count ?? 0,
      pnl: hours[h]?.pnl ?? 0,
    })); // keep all 24 hours so x-axis is continuous and ReferenceArea bands render correctly
  }, [trades]);

  // P&L distribution (bucket histogram)
  const pnlDist = useMemo(() => {
    if (trades.length === 0) return [];
    const pnls = trades.map((t) => t.pnl);
    const min = Math.min(...pnls);
    const max = Math.max(...pnls);
    const range = max - min || 1;
    const bucketSize = range / 15;
    const buckets = Array.from({ length: 15 }, (_, i) => ({
      range: `${(min + i * bucketSize).toFixed(0)}`,
      count: 0,
      mid: min + (i + 0.5) * bucketSize,
    }));
    pnls.forEach((p) => {
      const idx = Math.min(14, Math.floor((p - min) / bucketSize));
      buckets[idx].count++;
    });
    return buckets;
  }, [trades]);

  // Monthly average
  const monthlyAvg = useMemo(() => {
    if (monthlyPnl.length === 0) return 0;
    return monthlyPnl.reduce((sum, d) => sum + d.pnl, 0) / monthlyPnl.length;
  }, [monthlyPnl]);

  if (loading) return <LoadingSkeleton lines={12} />;

  const avgTradesPerDay = trades.length > 0
    ? (trades.length / Math.max(1, new Set(trades.map((t) => t.trade_date)).size)).toFixed(1)
    : "0";

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <h1 className="text-xl font-semibold">Analytics</h1>
        {/* Segmented date range control */}
        <div className="flex gap-0 bg-bg-tertiary rounded-lg p-0.5">
          {(["30d", "90d", "180d", "all"] as const).map((r) => (
            <button
              key={r}
              onClick={() => setRange(r)}
              className={cn(
                "px-3 py-1.5 rounded-md text-xs font-medium transition-all",
                range === r
                  ? "bg-gradient-to-r from-accent to-blue-600 text-white shadow-sm"
                  : "text-text-secondary hover:text-text-primary",
              )}
            >
              {r === "all" ? "All" : r}
            </button>
          ))}
        </div>
      </div>

      {/* Key Metrics */}
      {summary && (
        <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-3">
          <MetricCard
            label="Total P&L"
            value={formatCurrency(summary.total_pnl)}
            positive={summary.total_pnl >= 0}
            borderColor={summary.total_pnl >= 0 ? "#22c55e" : "#ef4444"}
            icon={<IconTrendingUp className="w-3.5 h-3.5" />}
          />
          <MetricCard
            label="Win Rate"
            value={`${summary.win_rate}%`}
            positive={summary.win_rate >= 50}
            borderColor="#3b82f6"
            icon={<IconTarget className="w-3.5 h-3.5" />}
          />
          <MetricCard
            label="Profit Factor"
            value={summary.profit_factor?.toFixed(2) ?? "--"}
            positive={summary.profit_factor >= 1.5}
            borderColor="#6366f1"
            icon={<IconBarChart2 className="w-3.5 h-3.5" />}
          />
          <MetricCard
            label="Avg Trades/Day"
            value={avgTradesPerDay}
            borderColor="#64748b"
            icon={<IconActivity className="w-3.5 h-3.5" />}
          />
          <MetricCard
            label="Max Win Streak"
            value={String(streaks.maxWin)}
            positive
            borderColor="#22c55e"
            icon={<IconTrophy className="w-3.5 h-3.5" />}
          />
          <MetricCard
            label="Max Loss Streak"
            value={String(streaks.maxLoss)}
            positive={false}
            borderColor="#ef4444"
            icon={<IconAlertTriangle className="w-3.5 h-3.5" />}
          />
        </div>
      )}

      {/* Equity Curve */}
      {equityData.length > 0 && (
        <div
          className="rounded-lg border border-bg-tertiary p-4 shadow-md shadow-black/20"
          style={{ background: "linear-gradient(180deg, rgba(59,130,246,0.04) 0%, rgba(17,24,39,1) 100%)" }}
        >
          <h3 className="text-sm font-medium mb-2 flex items-center gap-1.5">
            <IconLineChart className="w-4 h-4 text-accent" />
            Equity Curve
          </h3>
          <EquityCurve data={equityData} height={250} />
        </div>
      )}

      {/* Drawdown Chart */}
      {drawdownData.length > 0 && (
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 shadow-md shadow-black/20">
          <h3 className="text-sm font-medium mb-2 flex items-center gap-1.5">
            <IconTrendingDown className="w-4 h-4 text-loss" />
            Drawdown %
          </h3>
          <ResponsiveContainer width="100%" height={150}>
            <AreaChart data={drawdownData} margin={{ top: 5, right: 10, bottom: 5, left: 10 }}>
              <CartesianGrid stroke="#1e2433" strokeDasharray="3 3" />
              <XAxis dataKey="date" tick={{ fill: "#9ca3af", fontSize: 9 }} />
              <YAxis tick={{ fill: "#9ca3af", fontSize: 9 }} />
              <Tooltip contentStyle={{ backgroundColor: "#111827", border: "1px solid #1f2937", fontSize: 11 }} />
              <Area type="monotone" dataKey="drawdown" fill="#ef4444" fillOpacity={0.3} stroke="#ef4444" />
              {maxDrawdown < 0 && (
                <ReferenceLine
                  y={maxDrawdown}
                  stroke="#ef4444"
                  strokeDasharray="4 2"
                  strokeOpacity={0.7}
                  label={{ value: `Max DD: ${maxDrawdown.toFixed(1)}%`, fill: "#ef4444", fontSize: 10, position: "insideTopLeft" }}
                />
              )}
            </AreaChart>
          </ResponsiveContainer>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Monthly P&L */}
        {monthlyPnl.length > 0 && (
          <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 shadow-md shadow-black/20">
            <h3 className="text-sm font-medium mb-2 flex items-center gap-1.5">
              <IconCalendar className="w-4 h-4 text-accent" />
              Monthly P&L
            </h3>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={monthlyPnl} margin={{ top: 5, right: 10, bottom: 5, left: 10 }}>
                <CartesianGrid stroke="#1e2433" strokeDasharray="3 3" />
                <XAxis dataKey="month" tick={{ fill: "#9ca3af", fontSize: 9 }} />
                <YAxis tick={{ fill: "#9ca3af", fontSize: 9 }} />
                <Tooltip contentStyle={{ backgroundColor: "#111827", border: "1px solid #1f2937", fontSize: 11 }} />
                <ReferenceLine
                  y={monthlyAvg}
                  stroke="#3b82f6"
                  strokeDasharray="4 2"
                  strokeOpacity={0.7}
                  label={{ value: "Avg", fill: "#3b82f6", fontSize: 9, position: "insideTopRight" }}
                />
                <Bar dataKey="pnl" radius={[4, 4, 0, 0]}>
                  {monthlyPnl.map((d, i) => (
                    <Cell
                      key={i}
                      fill={d.pnl >= 0 ? "url(#greenGrad)" : "url(#redGrad)"}
                    />
                  ))}
                </Bar>
                <defs>
                  <linearGradient id="greenGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="#22c55e" stopOpacity={1} />
                    <stop offset="100%" stopColor="#16a34a" stopOpacity={0.8} />
                  </linearGradient>
                  <linearGradient id="redGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="#ef4444" stopOpacity={1} />
                    <stop offset="100%" stopColor="#dc2626" stopOpacity={0.8} />
                  </linearGradient>
                </defs>
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* Weekly P&L */}
        {weeklyPnl.length > 0 && (
          <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 shadow-md shadow-black/20">
            <h3 className="text-sm font-medium mb-2 flex items-center gap-1.5">
              <IconBarChart2 className="w-4 h-4 text-accent" />
              Weekly P&L
            </h3>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={weeklyPnl} margin={{ top: 5, right: 10, bottom: 5, left: 10 }}>
                <CartesianGrid stroke="#1e2433" strokeDasharray="3 3" />
                <XAxis dataKey="week" tick={{ fill: "#9ca3af", fontSize: 9 }} />
                <YAxis tick={{ fill: "#9ca3af", fontSize: 9 }} />
                <Tooltip contentStyle={{ backgroundColor: "#111827", border: "1px solid #1f2937", fontSize: 11 }} />
                <Bar dataKey="pnl" radius={[4, 4, 0, 0]}>
                  {weeklyPnl.map((d, i) => (
                    <Cell key={i} fill={d.pnl >= 0 ? "#22c55e" : "#ef4444"} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* Hourly Distribution */}
        {hourlyDist.length > 0 && (
          <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 shadow-md shadow-black/20">
            <h3 className="text-sm font-medium mb-2 flex items-center gap-1.5">
              <IconClock className="w-4 h-4 text-accent" />
              P&L by Hour of Entry
            </h3>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={hourlyDist} margin={{ top: 5, right: 10, bottom: 5, left: 10 }}>
                <CartesianGrid stroke="#1e2433" strokeDasharray="3 3" />
                {/* Market session tints: 9:15–11:00 and 14:00–15:30 */}
                <ReferenceArea x1="9:00" x2="11:00" fill="rgba(59,130,246,0.05)" />
                <ReferenceArea x1="14:00" x2="15:00" fill="rgba(59,130,246,0.05)" />
                <XAxis dataKey="hour" tick={{ fill: "#9ca3af", fontSize: 9 }} />
                <YAxis tick={{ fill: "#9ca3af", fontSize: 9 }} />
                <Tooltip contentStyle={{ backgroundColor: "#111827", border: "1px solid #1f2937", fontSize: 11 }} />
                <Bar dataKey="pnl" radius={[4, 4, 0, 0]}>
                  {hourlyDist.map((d, i) => (
                    <Cell key={i} fill={d.pnl >= 0 ? "#22c55e" : "#ef4444"} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* P&L Distribution Histogram */}
        {pnlDist.length > 0 && (
          <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 shadow-md shadow-black/20">
            <h3 className="text-sm font-medium mb-2 flex items-center gap-1.5">
              <IconPieChart className="w-4 h-4 text-accent" />
              P&L Distribution
            </h3>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={pnlDist} margin={{ top: 5, right: 10, bottom: 5, left: 10 }}>
                <CartesianGrid stroke="#1e2433" strokeDasharray="3 3" />
                <XAxis dataKey="range" tick={{ fill: "#9ca3af", fontSize: 9 }} />
                <YAxis tick={{ fill: "#9ca3af", fontSize: 9 }} />
                <Tooltip contentStyle={{ backgroundColor: "#111827", border: "1px solid #1f2937", fontSize: 11 }} />
                <Bar dataKey="count" radius={[4, 4, 0, 0]}>
                  {pnlDist.map((d, i) => (
                    <Cell key={i} fill={d.mid >= 0 ? "#22c55e" : "#ef4444"} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>
    </div>
  );
}

function MetricCard({
  label,
  value,
  positive,
  borderColor,
  icon,
}: {
  label: string;
  value: string;
  positive?: boolean;
  borderColor?: string;
  icon?: React.ReactNode;
}) {
  return (
    <div
      className="bg-bg-secondary rounded-lg border border-bg-tertiary p-3 shadow-md shadow-black/20"
      style={{ borderTop: borderColor ? `3px solid ${borderColor}` : undefined }}
    >
      <div className="flex items-center gap-1 mb-0.5">
        {icon && (
          <span className="text-text-secondary">{icon}</span>
        )}
        <p className="text-[10px] text-text-secondary">{label}</p>
      </div>
      <p
        className={cn(
          "font-mono text-xl font-bold mt-0.5",
          positive === true ? "text-profit" : positive === false ? "text-loss" : "",
        )}
      >
        {value}
      </p>
    </div>
  );
}
