"use client";

import { useEffect, useState, useMemo } from "react";
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
} from "recharts";

type DateRange = "30d" | "90d" | "180d" | "all";

function daysAgo(n: number): string {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d.toISOString().split("T")[0];
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
      // trade_date is already YYYY-MM-DD in IST from backend
      const d = new Date(t.trade_date + "T00:00:00");
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
      count: hours[h]?.count ?? 0,
      pnl: hours[h]?.pnl ?? 0,
    })).filter((d) => d.count > 0);
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

  if (loading) return <LoadingSkeleton lines={12} />;

  const avgTradesPerDay = trades.length > 0
    ? (trades.length / Math.max(1, new Set(trades.map((t) => t.trade_date)).size)).toFixed(1)
    : "0";

  return (
    <div className="space-y-6">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <h1 className="text-xl font-semibold">Analytics</h1>
        <div className="flex gap-1">
          {(["30d", "90d", "180d", "all"] as const).map((r) => (
            <button
              key={r}
              onClick={() => setRange(r)}
              className={cn(
                "px-3 py-1 rounded text-xs",
                range === r
                  ? "bg-accent text-white"
                  : "bg-bg-tertiary text-text-secondary hover:text-text-primary",
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
          <MetricCard label="Total P&L" value={formatCurrency(summary.total_pnl)} positive={summary.total_pnl >= 0} />
          <MetricCard label="Win Rate" value={`${summary.win_rate}%`} positive={summary.win_rate >= 50} />
          <MetricCard label="Profit Factor" value={String(summary.profit_factor)} positive={summary.profit_factor >= 1.5} />
          <MetricCard label="Avg Trades/Day" value={avgTradesPerDay} />
          <MetricCard label="Max Win Streak" value={String(streaks.maxWin)} positive />
          <MetricCard label="Max Loss Streak" value={String(streaks.maxLoss)} />
        </div>
      )}

      {/* Equity Curve */}
      {equityData.length > 0 && (
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
          <h3 className="text-sm font-medium mb-2">Equity Curve</h3>
          <EquityCurve data={equityData} height={250} />
        </div>
      )}

      {/* Drawdown Chart */}
      {drawdownData.length > 0 && (
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
          <h3 className="text-sm font-medium mb-2">Drawdown %</h3>
          <ResponsiveContainer width="100%" height={150}>
            <AreaChart data={drawdownData}>
              <CartesianGrid stroke="#1f2937" strokeDasharray="3 3" />
              <XAxis dataKey="date" tick={{ fill: "#9ca3af", fontSize: 9 }} />
              <YAxis tick={{ fill: "#9ca3af", fontSize: 9 }} />
              <Tooltip contentStyle={{ backgroundColor: "#111827", border: "1px solid #1f2937", fontSize: 11 }} />
              <Area type="monotone" dataKey="drawdown" fill="#ef4444" fillOpacity={0.2} stroke="#ef4444" />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Monthly P&L */}
        {monthlyPnl.length > 0 && (
          <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
            <h3 className="text-sm font-medium mb-2">Monthly P&L</h3>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={monthlyPnl}>
                <CartesianGrid stroke="#1f2937" strokeDasharray="3 3" />
                <XAxis dataKey="month" tick={{ fill: "#9ca3af", fontSize: 9 }} />
                <YAxis tick={{ fill: "#9ca3af", fontSize: 9 }} />
                <Tooltip contentStyle={{ backgroundColor: "#111827", border: "1px solid #1f2937", fontSize: 11 }} />
                <Bar dataKey="pnl" radius={[4, 4, 0, 0]}>
                  {monthlyPnl.map((d, i) => (
                    <Cell key={i} fill={d.pnl >= 0 ? "#22c55e" : "#ef4444"} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* Weekly P&L */}
        {weeklyPnl.length > 0 && (
          <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
            <h3 className="text-sm font-medium mb-2">Weekly P&L</h3>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={weeklyPnl}>
                <CartesianGrid stroke="#1f2937" strokeDasharray="3 3" />
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
          <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
            <h3 className="text-sm font-medium mb-2">P&L by Hour of Entry</h3>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={hourlyDist}>
                <CartesianGrid stroke="#1f2937" strokeDasharray="3 3" />
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
          <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
            <h3 className="text-sm font-medium mb-2">P&L Distribution</h3>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={pnlDist}>
                <CartesianGrid stroke="#1f2937" strokeDasharray="3 3" />
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
}: {
  label: string;
  value: string;
  positive?: boolean;
}) {
  return (
    <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-3">
      <p className="text-[10px] text-text-secondary">{label}</p>
      <p
        className={cn(
          "font-mono text-sm mt-0.5",
          positive === true ? "text-profit" : positive === false ? "text-loss" : "",
        )}
      >
        {value}
      </p>
    </div>
  );
}
