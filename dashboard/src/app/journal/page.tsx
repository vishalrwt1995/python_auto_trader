"use client";

import { useEffect, useState, useMemo } from "react";
import { api } from "@/lib/api";
import { DataTable, type Column } from "@/components/shared/DataTable";
import { EquityCurve } from "@/components/charts/EquityCurve";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { cn, formatCurrency, formatPercent, formatDate } from "@/lib/utils";
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
} from "recharts";

type DateRange = "7d" | "30d" | "90d" | "all";

function daysAgo(n: number): string {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d.toISOString().split("T")[0];
}

export default function JournalPage() {
  const [range, setRange] = useState<DateRange>("30d");
  const [trades, setTrades] = useState<Trade[]>([]);
  const [summary, setSummary] = useState<TradeSummary | null>(null);
  const [equityData, setEquityData] = useState<{ date: string; pnl: number }[]>([]);
  const [loading, setLoading] = useState(true);
  const [strategyFilter, setStrategyFilter] = useState("");
  const [regimeFilter, setRegimeFilter] = useState("");
  const [sideFilter, setSideFilter] = useState("");
  const [tab, setTab] = useState<"trades" | "by-strategy" | "by-regime" | "by-day">("trades");

  const fromDate = range === "all" ? "2020-01-01" : daysAgo(range === "7d" ? 7 : range === "30d" ? 30 : 90);

  useEffect(() => {
    setLoading(true);
    Promise.all([
      api.getTradeSummary(fromDate).then((d) => setSummary(d as unknown as TradeSummary)),
      api.getEquityCurve(fromDate).then((d: any) => setEquityData(d.series ?? [])),
      api.getTrades({ from: fromDate, ...(strategyFilter ? { strategy: strategyFilter } : {}) })
        .then((d: any) => setTrades(d.trades ?? [])),
    ])
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [range, fromDate, strategyFilter]);

  const strategies = useMemo(() => [...new Set(trades.map((t) => t.strategy).filter(Boolean))], [trades]);
  const regimes = useMemo(() => [...new Set(trades.map((t) => t.regime).filter(Boolean))], [trades]);

  const filteredTrades = useMemo(() => {
    let t = trades;
    if (regimeFilter) t = t.filter((r) => r.regime === regimeFilter);
    if (sideFilter) t = t.filter((r) => r.side === sideFilter);
    return t;
  }, [trades, regimeFilter, sideFilter]);

  const byStrategy = useMemo(() => {
    const map: Record<string, { trades: number; wins: number; pnl: number }> = {};
    trades.forEach((t) => {
      const s = t.strategy || "Unknown";
      if (!map[s]) map[s] = { trades: 0, wins: 0, pnl: 0 };
      map[s].trades++;
      if (t.pnl > 0) map[s].wins++;
      map[s].pnl += t.pnl;
    });
    return Object.entries(map).map(([name, v]) => ({
      name,
      ...v,
      winRate: v.trades > 0 ? Math.round((v.wins / v.trades) * 100) : 0,
    }));
  }, [trades]);

  const byRegime = useMemo(() => {
    const map: Record<string, { trades: number; wins: number; pnl: number }> = {};
    trades.forEach((t) => {
      const r = t.regime || "Unknown";
      if (!map[r]) map[r] = { trades: 0, wins: 0, pnl: 0 };
      map[r].trades++;
      if (t.pnl > 0) map[r].wins++;
      map[r].pnl += t.pnl;
    });
    return Object.entries(map).map(([name, v]) => ({
      name,
      ...v,
      winRate: v.trades > 0 ? Math.round((v.wins / v.trades) * 100) : 0,
    }));
  }, [trades]);

  const byDay = useMemo(() => {
    const days = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const map = days.map((d) => ({ name: d, pnl: 0, count: 0 }));
    trades.forEach((t) => {
      // trade_date is YYYY-MM-DD in IST; append T00:00:00 to avoid UTC date shift
      const day = new Date(t.trade_date + "T00:00:00").getDay();
      map[day].pnl += t.pnl;
      map[day].count++;
    });
    return map.filter((d) => d.count > 0);
  }, [trades]);

  const tradeColumns: Column<Trade>[] = useMemo(
    () => [
      {
        key: "date",
        label: "Date",
        sortable: true,
        sortValue: (r) => r.trade_date,
        render: (r) => <span className="font-mono text-xs">{r.trade_date}</span>,
      },
      {
        key: "symbol",
        label: "Symbol",
        sortable: true,
        sortValue: (r) => r.symbol,
        render: (r) => <span className="font-medium">{r.symbol}</span>,
      },
      {
        key: "side",
        label: "Side",
        render: (r) => (
          <span className={cn("px-1.5 py-0.5 rounded text-xs font-medium", r.side === "BUY" ? "bg-profit/20 text-profit" : "bg-loss/20 text-loss")}>
            {r.side}
          </span>
        ),
      },
      {
        key: "entry",
        label: "Entry",
        className: "text-right font-mono",
        render: (r) => <span>{r.entry_price.toFixed(2)}</span>,
      },
      {
        key: "exit",
        label: "Exit",
        className: "text-right font-mono",
        render: (r) => <span>{r.exit_price.toFixed(2)}</span>,
      },
      {
        key: "pnl",
        label: "P&L",
        sortable: true,
        sortValue: (r) => r.pnl,
        className: "text-right font-mono",
        render: (r) => (
          <span className={r.pnl >= 0 ? "text-profit" : "text-loss"}>
            {formatCurrency(r.pnl)}
          </span>
        ),
      },
      {
        key: "pnlPct",
        label: "P&L %",
        sortable: true,
        sortValue: (r) => r.pnl_pct,
        className: "text-right font-mono",
        render: (r) => (
          <span className={r.pnl_pct >= 0 ? "text-profit" : "text-loss"}>
            {formatPercent(r.pnl_pct)}
          </span>
        ),
      },
      {
        key: "hold",
        label: "Hold",
        className: "text-right",
        render: (r) => {
          const h = Math.floor(r.hold_minutes / 60);
          const m = r.hold_minutes % 60;
          return <span className="text-xs text-text-secondary">{h > 0 ? `${h}h ${m}m` : `${m}m`}</span>;
        },
      },
      {
        key: "strategy",
        label: "Strategy",
        render: (r) => <span className="text-xs text-text-secondary">{r.strategy || "—"}</span>,
      },
      {
        key: "regime",
        label: "Regime",
        render: (r) => <span className="text-xs">{r.regime || "—"}</span>,
      },
      {
        key: "exitReason",
        label: "Exit",
        render: (r) => (
          <span className={cn("text-xs", r.exit_reason === "TARGET_HIT" ? "text-profit" : r.exit_reason === "SL_HIT" ? "text-loss" : "text-text-secondary")}>
            {r.exit_reason}
          </span>
        ),
      },
      {
        key: "signalScore",
        label: "Score",
        className: "text-right font-mono",
        render: (r) => <span className="text-xs">{r.signal_score ?? "—"}</span>,
      },
    ],
    [],
  );

  if (loading) return <LoadingSkeleton lines={12} />;

  return (
    <div className="space-y-6">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <h1 className="text-xl font-semibold">Trade Journal</h1>
        <div className="flex gap-1">
          {(["7d", "30d", "90d", "all"] as const).map((r) => (
            <button
              key={r}
              onClick={() => setRange(r)}
              className={cn("px-3 py-1 rounded text-xs", range === r ? "bg-accent text-white" : "bg-bg-tertiary text-text-secondary hover:text-text-primary")}
            >
              {r === "all" ? "All" : r}
            </button>
          ))}
        </div>
      </div>

      {/* Summary Cards */}
      {summary && (
        <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-8 gap-3">
          <SummaryCard label="Total P&L" value={formatCurrency(summary.total_pnl)} positive={summary.total_pnl >= 0} />
          <SummaryCard label="Win Rate" value={`${summary.win_rate}%`} positive={summary.win_rate >= 50} />
          <SummaryCard label="Avg R:R" value={String(summary.avg_rr)} />
          <SummaryCard label="Expectancy" value={formatCurrency(summary.expectancy)} positive={summary.expectancy >= 0} />
          <SummaryCard label="Profit Factor" value={String(summary.profit_factor)} positive={summary.profit_factor >= 1.5} />
          <SummaryCard label="Trades" value={String(summary.total_trades)} />
          <SummaryCard label="Best Win" value={formatCurrency(summary.biggest_win)} positive />
          <SummaryCard label="Worst Loss" value={formatCurrency(summary.biggest_loss)} />
        </div>
      )}

      {/* Equity Curve */}
      {equityData.length > 0 && (
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
          <h3 className="text-sm font-medium mb-2">Equity Curve</h3>
          <EquityCurve data={equityData} height={250} />
        </div>
      )}

      {/* Filters */}
      <div className="flex flex-wrap gap-3">
        <select value={strategyFilter} onChange={(e) => setStrategyFilter(e.target.value)} className="px-3 py-1.5 bg-bg-tertiary rounded-lg text-xs text-text-primary">
          <option value="">All Strategies</option>
          {strategies.map((s) => <option key={s} value={s}>{s}</option>)}
        </select>
        <select value={regimeFilter} onChange={(e) => setRegimeFilter(e.target.value)} className="px-3 py-1.5 bg-bg-tertiary rounded-lg text-xs text-text-primary">
          <option value="">All Regimes</option>
          {regimes.map((r) => <option key={r} value={r}>{r}</option>)}
        </select>
        <select value={sideFilter} onChange={(e) => setSideFilter(e.target.value)} className="px-3 py-1.5 bg-bg-tertiary rounded-lg text-xs text-text-primary">
          <option value="">All Sides</option>
          <option value="BUY">BUY</option>
          <option value="SELL">SELL</option>
        </select>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 border-b border-bg-tertiary pb-1">
        {([["trades", "Trades"], ["by-strategy", "By Strategy"], ["by-regime", "By Regime"], ["by-day", "By Day"]] as const).map(([key, label]) => (
          <button
            key={key}
            onClick={() => setTab(key as any)}
            className={cn("px-3 py-1.5 rounded-t text-xs", tab === key ? "bg-bg-secondary text-accent border border-bg-tertiary border-b-0" : "text-text-secondary hover:text-text-primary")}
          >
            {label}
          </button>
        ))}
      </div>

      {/* Tab Content */}
      {tab === "trades" && (
        <DataTable columns={tradeColumns} data={filteredTrades} emptyMessage="No trades in this period" />
      )}

      {tab === "by-strategy" && (
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={byStrategy} layout="vertical">
              <CartesianGrid stroke="#1f2937" strokeDasharray="3 3" />
              <XAxis type="number" tick={{ fill: "#9ca3af", fontSize: 10 }} />
              <YAxis dataKey="name" type="category" tick={{ fill: "#9ca3af", fontSize: 10 }} width={100} />
              <Tooltip contentStyle={{ backgroundColor: "#111827", border: "1px solid #1f2937", fontSize: 11 }} />
              <Bar dataKey="pnl" radius={[0, 4, 4, 0]}>
                {byStrategy.map((entry, i) => (
                  <Cell key={i} fill={entry.pnl >= 0 ? "#22c55e" : "#ef4444"} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          <div className="mt-4 grid grid-cols-2 md:grid-cols-4 gap-3">
            {byStrategy.map((s) => (
              <div key={s.name} className="bg-bg-tertiary/50 rounded p-3">
                <p className="text-xs font-medium">{s.name}</p>
                <p className={cn("font-mono text-sm", s.pnl >= 0 ? "text-profit" : "text-loss")}>{formatCurrency(s.pnl)}</p>
                <p className="text-[10px] text-text-secondary">{s.trades} trades &middot; {s.winRate}% win</p>
              </div>
            ))}
          </div>
        </div>
      )}

      {tab === "by-regime" && (
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
          <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
            {byRegime.map((r) => (
              <div key={r.name} className="bg-bg-tertiary/50 rounded p-3">
                <p className="text-xs font-medium">{r.name}</p>
                <p className={cn("font-mono text-lg", r.pnl >= 0 ? "text-profit" : "text-loss")}>{formatCurrency(r.pnl)}</p>
                <p className="text-[10px] text-text-secondary">{r.trades} trades &middot; {r.winRate}% win rate</p>
              </div>
            ))}
          </div>
        </div>
      )}

      {tab === "by-day" && (
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={byDay}>
              <CartesianGrid stroke="#1f2937" strokeDasharray="3 3" />
              <XAxis dataKey="name" tick={{ fill: "#9ca3af", fontSize: 10 }} />
              <YAxis tick={{ fill: "#9ca3af", fontSize: 10 }} />
              <Tooltip contentStyle={{ backgroundColor: "#111827", border: "1px solid #1f2937", fontSize: 11 }} />
              <Bar dataKey="pnl" radius={[4, 4, 0, 0]}>
                {byDay.map((d, i) => (
                  <Cell key={i} fill={d.pnl >= 0 ? "#22c55e" : "#ef4444"} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
}

function SummaryCard({ label, value, positive }: { label: string; value: string; positive?: boolean }) {
  return (
    <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-3">
      <p className="text-[10px] text-text-secondary">{label}</p>
      <p className={cn("font-mono text-sm mt-0.5", positive === true ? "text-profit" : positive === false ? "text-loss" : "")}>
        {value}
      </p>
    </div>
  );
}
