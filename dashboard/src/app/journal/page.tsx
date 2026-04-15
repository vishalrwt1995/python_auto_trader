"use client";

import React, { useEffect, useState, useMemo } from "react";
import { api } from "@/lib/api";
import { EquityCurve } from "@/components/charts/EquityCurve";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
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
} from "recharts";

type DateRange = "7d" | "30d" | "90d" | "all";

function daysAgo(n: number): string {
  // Use IST date so "7 days ago" means 7 IST calendar days, not UTC days
  const ist = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));
  ist.setDate(ist.getDate() - n);
  return `${ist.getFullYear()}-${String(ist.getMonth() + 1).padStart(2, "0")}-${String(ist.getDate()).padStart(2, "0")}`;
}

const REGIME_COLORS: Record<string, string> = {
  TREND_UP: "#22c55e",
  TREND_DOWN: "#ef4444",
  RANGE: "#f59e0b",
  CHOP: "#6b7280",
  PANIC: "#ef4444",
  RECOVERY: "#3b82f6",
};

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
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");

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

  const toggleSort = (key: string) => {
    if (sortKey === key) setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    else { setSortKey(key); setSortDir("desc"); }
  };

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
      winRate: v.trades > 0 ? Math.round((v.wins / v.trades) * 1000) / 10 : 0,
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
      winRate: v.trades > 0 ? Math.round((v.wins / v.trades) * 1000) / 10 : 0,
    }));
  }, [trades]);

  const byDay = useMemo(() => {
    const days = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const map = days.map((d) => ({ name: d, pnl: 0, count: 0 }));
    trades.forEach((t) => {
      if (!t.trade_date) return;
      // trade_date is YYYY-MM-DD in IST; anchor to +05:30 so day-of-week is correct in any browser TZ
      const d = new Date(t.trade_date + "T00:00:00+05:30");
      if (isNaN(d.getTime())) return;
      const day = d.getDay();
      map[day].pnl += t.pnl;
      map[day].count++;
    });
    return map.filter((d) => d.count > 0);
  }, [trades]);

  type TradeCol = {
    key: string;
    label: string;
    sortable?: boolean;
    className?: string;
    sortValue?: (r: Trade) => string | number;
    render: (r: Trade) => React.ReactNode;
  };

  const tradeColumns: TradeCol[] = useMemo(
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
        render: (r) => <span>{r.entry_price?.toFixed(2) ?? "—"}</span>,
      },
      {
        key: "exit",
        label: "Exit",
        className: "text-right font-mono",
        render: (r) => <span>{r.exit_price?.toFixed(2) ?? "—"}</span>,
      },
      {
        key: "pnl",
        label: "P&L",
        sortable: true,
        sortValue: (r) => r.pnl,
        className: "text-right",
        render: (r) => (
          <div className="flex flex-col items-end">
            <span className={cn("font-mono font-bold text-sm", r.pnl >= 0 ? "text-profit" : "text-loss")}>
              {formatCurrency(r.pnl)}
            </span>
            <span className={cn("font-mono text-[10px]", r.pnl_pct >= 0 ? "text-profit/70" : "text-loss/70")}>
              {formatPercent(r.pnl_pct)}
            </span>
          </div>
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

  const sortedTrades = useMemo(() => {
    if (!sortKey) return filteredTrades;
    const col = tradeColumns.find((c) => c.key === sortKey);
    if (!col?.sortValue) return filteredTrades;
    const fn = col.sortValue;
    return [...filteredTrades].sort((a, b) => {
      const av = fn(a);
      const bv = fn(b);
      if (av < bv) return sortDir === "asc" ? -1 : 1;
      if (av > bv) return sortDir === "asc" ? 1 : -1;
      return 0;
    });
  }, [filteredTrades, sortKey, sortDir, tradeColumns]);

  const tabs = [
    ["trades", "Trades"],
    ["by-strategy", "By Strategy"],
    ["by-regime", "By Regime"],
    ["by-day", "By Day"],
  ] as const;

  if (loading) return <LoadingSkeleton lines={12} />;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <h1 className="text-xl font-semibold">Trade Journal</h1>
        {/* Segmented date range control */}
        <div className="flex gap-0 bg-bg-tertiary rounded-lg p-0.5">
          {(["7d", "30d", "90d", "all"] as const).map((r) => (
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

      {/* Summary Cards */}
      {summary && (
        <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-8 gap-3">
          <SummaryCard
            label="Total P&L"
            value={formatCurrency(summary.total_pnl)}
            positive={summary.total_pnl >= 0}
            borderColor={summary.total_pnl >= 0 ? "#22c55e" : "#ef4444"}
          />
          <SummaryCard
            label="Win Rate"
            value={`${summary.win_rate}%`}
            positive={summary.win_rate >= 50}
            borderColor="#3b82f6"
          />
          <SummaryCard
            label="Avg R:R"
            value={String(summary.avg_rr)}
            borderColor="#6366f1"
          />
          <SummaryCard
            label="Expectancy"
            value={formatCurrency(summary.expectancy)}
            positive={summary.expectancy >= 0}
            borderColor="#6366f1"
          />
          <SummaryCard
            label="Profit Factor"
            value={summary.profit_factor == null ? "∞" : String(summary.profit_factor)}
            positive={summary.profit_factor == null || summary.profit_factor >= 1.5}
            borderColor="#3b82f6"
          />
          <SummaryCard
            label="Trades"
            value={String(summary.total_trades)}
            borderColor="#64748b"
          />
          <SummaryCard
            label="Best Win"
            value={formatCurrency(summary.biggest_win)}
            positive
            borderColor="#22c55e"
          />
          <SummaryCard
            label="Worst Loss"
            value={formatCurrency(summary.biggest_loss)}
            borderColor="#ef4444"
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
            <svg className="w-4 h-4 text-accent" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 12l3-3 3 3 4-4M8 21l4-4 4 4M3 4h18M4 4h16v12a1 1 0 01-1 1H5a1 1 0 01-1-1V4z" />
            </svg>
            Equity Curve
          </h3>
          <EquityCurve data={equityData} height={250} />
        </div>
      )}

      {/* Filters */}
      <div className="flex flex-wrap gap-3">
        <select
          value={strategyFilter}
          onChange={(e) => setStrategyFilter(e.target.value)}
          className="bg-bg-tertiary border border-bg-tertiary hover:border-accent/30 rounded-lg px-3 py-1.5 text-xs text-text-primary transition-colors focus:outline-none focus:border-accent/50"
        >
          <option value="">All Strategies</option>
          {strategies.map((s) => <option key={s} value={s}>{s}</option>)}
        </select>
        <select
          value={regimeFilter}
          onChange={(e) => setRegimeFilter(e.target.value)}
          className="bg-bg-tertiary border border-bg-tertiary hover:border-accent/30 rounded-lg px-3 py-1.5 text-xs text-text-primary transition-colors focus:outline-none focus:border-accent/50"
        >
          <option value="">All Regimes</option>
          {regimes.map((r) => <option key={r} value={r}>{r}</option>)}
        </select>
        <select
          value={sideFilter}
          onChange={(e) => setSideFilter(e.target.value)}
          className="bg-bg-tertiary border border-bg-tertiary hover:border-accent/30 rounded-lg px-3 py-1.5 text-xs text-text-primary transition-colors focus:outline-none focus:border-accent/50"
        >
          <option value="">All Sides</option>
          <option value="BUY">BUY</option>
          <option value="SELL">SELL</option>
        </select>
      </div>

      {/* Tabs — animated underline style */}
      <div className="flex gap-0 border-b border-bg-tertiary">
        {tabs.map(([key, label]) => (
          <button
            key={key}
            onClick={() => setTab(key as any)}
            className={cn(
              "px-4 py-2.5 text-sm font-medium transition-colors relative",
              tab === key
                ? "text-accent after:absolute after:bottom-0 after:left-0 after:right-0 after:h-0.5 after:bg-accent after:rounded-full"
                : "text-text-secondary hover:text-text-primary",
            )}
          >
            {label}
          </button>
        ))}
      </div>

      {/* Tab Content */}
      {tab === "trades" && (
        <div className="overflow-hidden rounded-lg border border-bg-tertiary">
          <table className="w-full text-xs">
            <thead
              className="sticky top-0 z-10"
              style={{
                background: "rgba(17,24,39,0.92)",
                backdropFilter: "blur(12px)",
                borderBottom: "1px solid rgba(31,41,55,0.8)",
              }}
            >
              <tr>
                {tradeColumns.map((col) => (
                  <th
                    key={col.key}
                    onClick={() => col.sortable && toggleSort(col.key)}
                    className={cn(
                      "px-3 py-2.5 text-left text-[10px] font-semibold uppercase tracking-wide text-text-secondary whitespace-nowrap",
                      col.sortable && "cursor-pointer hover:text-text-primary transition-colors select-none",
                    )}
                  >
                    <span className="inline-flex items-center gap-1">
                      {col.label}
                      {col.sortable && sortKey === col.key && (
                        <span className="text-accent">{sortDir === "asc" ? "↑" : "↓"}</span>
                      )}
                    </span>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sortedTrades.length === 0 ? (
                <tr>
                  <td colSpan={tradeColumns.length} className="px-3 py-8 text-center text-text-secondary">
                    No trades in this period
                  </td>
                </tr>
              ) : (
                sortedTrades.map((trade, i) => {
                  const rowBg =
                    trade.pnl > 500
                      ? "bg-profit/[0.08]"
                      : trade.pnl > 0
                      ? "bg-profit/[0.04]"
                      : trade.pnl < 0
                      ? "bg-loss/[0.04]"
                      : "";
                  return (
                    <tr key={i} className={cn("border-t border-bg-tertiary/40 hover:bg-bg-tertiary/30 transition-colors", rowBg)}>
                      {tradeColumns.map((col) => (
                        <td key={col.key} className={cn("px-3 py-2 whitespace-nowrap", col.className)}>
                          {col.render(trade)}
                        </td>
                      ))}
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      )}

      {tab === "by-strategy" && (
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 shadow-md shadow-black/20">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={byStrategy} layout="vertical" margin={{ top: 5, right: 10, bottom: 5, left: 10 }}>
              <CartesianGrid stroke="#1e2433" strokeDasharray="3 3" />
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
              <div
                key={s.name}
                className="bg-bg-tertiary/50 rounded-lg p-3 border-l-[3px]"
                style={{ borderLeftColor: s.pnl >= 0 ? "#22c55e" : "#ef4444" }}
              >
                <p className="text-xs font-medium truncate">{s.name}</p>
                <p className={cn("font-mono font-bold text-base mt-0.5", s.pnl >= 0 ? "text-profit" : "text-loss")}>
                  {formatCurrency(s.pnl)}
                </p>
                <p className="text-[10px] text-text-secondary mt-1">{s.trades} trades &middot; {s.winRate}% win</p>
                {/* Win rate progress bar */}
                <div className="mt-1.5 h-1 bg-bg-tertiary rounded-full overflow-hidden">
                  <div
                    className="h-full rounded-full"
                    style={{
                      width: `${s.winRate}%`,
                      backgroundColor: s.winRate >= 50 ? "#22c55e" : "#ef4444",
                    }}
                  />
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {tab === "by-regime" && (
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 shadow-md shadow-black/20">
          <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
            {byRegime.map((r) => {
              const color = REGIME_COLORS[r.name] ?? "#6b7280";
              return (
                <div
                  key={r.name}
                  className="rounded-lg p-3 border-l-[3px]"
                  style={{
                    borderLeftColor: color,
                    background: `linear-gradient(135deg, ${color}10 0%, rgba(31,41,55,0.5) 100%)`,
                  }}
                >
                  <p className="text-xs font-medium" style={{ color }}>{r.name}</p>
                  <p className={cn("font-mono font-bold text-lg mt-0.5", r.pnl >= 0 ? "text-profit" : "text-loss")}>
                    {formatCurrency(r.pnl)}
                  </p>
                  <p className="text-[10px] text-text-secondary">{r.trades} trades &middot; {r.winRate}% win rate</p>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {tab === "by-day" && (
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 shadow-md shadow-black/20">
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={byDay} margin={{ top: 5, right: 10, bottom: 5, left: 10 }}>
              <CartesianGrid stroke="#1e2433" strokeDasharray="3 3" />
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

function SummaryCard({
  label,
  value,
  positive,
  borderColor,
}: {
  label: string;
  value: string;
  positive?: boolean;
  borderColor?: string;
}) {
  return (
    <div
      className="bg-bg-secondary rounded-lg border border-bg-tertiary p-3 shadow-md shadow-black/20"
      style={{ borderTop: borderColor ? `3px solid ${borderColor}` : undefined }}
    >
      <p className="text-[10px] text-text-secondary">{label}</p>
      <p className={cn("font-mono text-xl font-bold mt-0.5", positive === true ? "text-profit" : positive === false ? "text-loss" : "")}>
        {value}
      </p>
    </div>
  );
}
