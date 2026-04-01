"use client";

import { useState, useMemo, useEffect } from "react";
import { useDashboardStore } from "@/stores/dashboardStore";
import { DataTable, type Column } from "@/components/shared/DataTable";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { cn } from "@/lib/utils";
import type { WatchlistRow } from "@/lib/types";
import { X, Search } from "lucide-react";
import {
  AreaChart,
  Area,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from "recharts";
import { api } from "@/lib/api";

interface CandleData {
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;
}

const SETUP_COLORS: Record<string, string> = {
  BREAKOUT: "text-profit",
  PULLBACK: "text-accent",
  MEAN_REVERSION: "text-neutral",
  PHASE1_MOMENTUM: "text-purple-400",
  VWAP_TREND: "text-cyan-400",
  VWAP_REVERSAL: "text-orange-400",
};

export default function WatchlistPage() {
  const watchlist = useDashboardStore((s) => s.watchlist);
  const [tab, setTab] = useState<"all" | "swing" | "intraday">("all");
  const [search, setSearch] = useState("");
  const [sectorFilter, setSectorFilter] = useState("");
  const [drawerSymbol, setDrawerSymbol] = useState<string | null>(null);

  const sectors = useMemo(() => {
    const set = new Set(watchlist.map((r) => r.sector).filter(Boolean));
    return Array.from(set).sort();
  }, [watchlist]);

  const filtered = useMemo(() => {
    let rows = watchlist;
    if (tab === "swing") rows = rows.filter((r) => r.eligible_swing);
    if (tab === "intraday") rows = rows.filter((r) => r.eligible_intraday);
    if (search) {
      const q = search.toUpperCase();
      rows = rows.filter((r) => r.symbol.includes(q));
    }
    if (sectorFilter) rows = rows.filter((r) => r.sector === sectorFilter);
    return rows;
  }, [watchlist, tab, search, sectorFilter]);

  const drawerRow = useMemo(
    () => watchlist.find((r) => r.symbol === drawerSymbol),
    [watchlist, drawerSymbol],
  );

  const columns: Column<WatchlistRow>[] = useMemo(
    () => [
      {
        key: "rank",
        label: "#",
        render: (_r, i) => (
          <span className="text-text-secondary font-mono text-xs">{i + 1}</span>
        ),
      },
      {
        key: "symbol",
        label: "Symbol",
        sortable: true,
        sortValue: (r) => r.symbol,
        render: (r) => (
          <button
            className="font-medium text-text-primary hover:text-accent transition-colors"
            onClick={(e) => {
              e.stopPropagation();
              setDrawerSymbol(r.symbol);
            }}
          >
            {r.symbol}
          </button>
        ),
      },
      {
        key: "sector",
        label: "Sector",
        sortable: true,
        sortValue: (r) => r.sector ?? "",
        render: (r) => (
          <span className="px-1.5 py-0.5 rounded bg-bg-tertiary text-xs text-text-secondary">
            {r.sector || "—"}
          </span>
        ),
      },
      {
        key: "setup",
        label: "Setup",
        sortable: true,
        sortValue: (r) => r.setup ?? "",
        render: (r) => (
          <span
            className={cn(
              "text-xs font-medium",
              SETUP_COLORS[r.setup] ?? "text-text-secondary",
            )}
          >
            {r.setup || "—"}
          </span>
        ),
      },
      {
        key: "score",
        label: "Score",
        sortable: true,
        sortValue: (r) => r.score ?? 0,
        className: "text-right",
        render: (r) => {
          const score = r.score ?? 0;
          return (
            <div className="flex items-center gap-2 justify-end">
              <div className="w-16 h-1.5 bg-bg-tertiary rounded-full overflow-hidden">
                <div
                  className={cn(
                    "h-full rounded-full",
                    score >= 60 ? "bg-profit" : score >= 30 ? "bg-neutral" : "bg-loss",
                  )}
                  style={{ width: `${Math.min(100, score)}%` }}
                />
              </div>
              <span className="font-mono text-xs w-6 text-right">{score}</span>
            </div>
          );
        },
      },
      {
        key: "beta",
        label: "Beta",
        sortable: true,
        sortValue: (r) => r.beta ?? 0,
        className: "text-right",
        render: (r) => (
          <span
            className={cn(
              "font-mono text-xs",
              (r.beta ?? 0) < 1
                ? "text-profit"
                : (r.beta ?? 0) < 1.5
                  ? "text-neutral"
                  : "text-loss",
            )}
          >
            {r.beta?.toFixed(2) ?? "—"}
          </span>
        ),
      },
      {
        key: "reason",
        label: "Reason",
        render: (r) => (
          <span
            className="text-xs text-text-secondary max-w-[200px] truncate block"
            title={r.reason}
          >
            {r.reason || "—"}
          </span>
        ),
      },
    ],
    [],
  );

  return (
    <div className="space-y-4">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <h1 className="text-xl font-semibold">Watchlist</h1>
        <span className="text-xs text-text-secondary">
          Showing {filtered.length} of {watchlist.length} stocks
        </span>
      </div>

      {/* Tabs + Filters */}
      <div className="flex flex-col sm:flex-row gap-3">
        <div className="flex gap-1">
          {(["all", "swing", "intraday"] as const).map((t) => (
            <button
              key={t}
              onClick={() => setTab(t)}
              className={cn(
                "px-3 py-1.5 rounded-lg text-xs font-medium transition-colors",
                tab === t
                  ? "bg-accent text-white"
                  : "bg-bg-tertiary text-text-secondary hover:text-text-primary",
              )}
            >
              {t === "all" ? "All" : t === "swing" ? "Swing" : "Intraday"}
            </button>
          ))}
        </div>

        <div className="relative flex-1 max-w-xs">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-text-secondary" />
          <input
            type="text"
            placeholder="Search symbol..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="w-full pl-8 pr-3 py-1.5 bg-bg-tertiary rounded-lg text-xs text-text-primary focus:outline-none focus:ring-1 focus:ring-accent"
          />
        </div>

        <select
          value={sectorFilter}
          onChange={(e) => setSectorFilter(e.target.value)}
          className="px-3 py-1.5 bg-bg-tertiary rounded-lg text-xs text-text-primary focus:outline-none focus:ring-1 focus:ring-accent"
        >
          <option value="">All Sectors</option>
          {sectors.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
      </div>

      {/* Table */}
      {watchlist.length === 0 ? (
        <LoadingSkeleton lines={8} />
      ) : (
        <DataTable
          columns={columns}
          data={filtered}
          onRowClick={(r) => setDrawerSymbol(r.symbol)}
          emptyMessage="No stocks match filters"
        />
      )}

      {/* Symbol Drawer */}
      {drawerSymbol && drawerRow && (
        <SymbolDrawer row={drawerRow} onClose={() => setDrawerSymbol(null)} />
      )}
    </div>
  );
}

function SymbolDrawer({
  row,
  onClose,
}: {
  row: WatchlistRow;
  onClose: () => void;
}) {
  const [candles, setCandles] = useState<CandleData[]>([]);
  const [candleLoading, setCandleLoading] = useState(true);
  const [candleDays, setCandleDays] = useState(90);

  useEffect(() => {
    setCandleLoading(true);
    api
      .getCandles(row.symbol, "1d", candleDays)
      .then((d: any) => setCandles(d.candles ?? []))
      .catch(() => setCandles([]))
      .finally(() => setCandleLoading(false));
  }, [row.symbol, candleDays]);

  // Compute min/max for chart domain
  const priceMin = candles.length > 0 ? Math.min(...candles.map((c) => c.low)) * 0.995 : 0;
  const priceMax = candles.length > 0 ? Math.max(...candles.map((c) => c.high)) * 1.005 : 0;
  const volMax = candles.length > 0 ? Math.max(...candles.map((c) => c.volume ?? 0)) : 1;

  // Color each candle
  const chartData = candles.map((c) => ({
    ...c,
    dateLabel: c.time?.slice(5) ?? "",
    isUp: (c.close ?? 0) >= (c.open ?? 0),
    bodyTop: Math.max(c.open ?? 0, c.close ?? 0),
    bodyBottom: Math.min(c.open ?? 0, c.close ?? 0),
  }));

  // Compute price change
  const firstClose = candles[0]?.close ?? 0;
  const lastClose = candles[candles.length - 1]?.close ?? 0;
  const pctChange = firstClose > 0 ? ((lastClose - firstClose) / firstClose) * 100 : 0;

  return (
    <>
      <div className="fixed inset-0 bg-black/50 z-40" onClick={onClose} />
      <div className="fixed right-0 top-0 bottom-0 w-full sm:w-[520px] bg-bg-secondary border-l border-bg-tertiary z-50 overflow-y-auto scrollbar-thin">
        <div className="p-4 space-y-4">
          {/* Header */}
          <div className="flex items-center justify-between">
            <div>
              <h2 className="text-lg font-semibold">{row.symbol}</h2>
              <p className="text-xs text-text-secondary">
                {row.sector} &middot; {row.setup} &middot; Beta {row.beta?.toFixed(2)}
              </p>
            </div>
            <button
              onClick={onClose}
              className="p-1.5 rounded hover:bg-bg-tertiary transition-colors"
            >
              <X className="h-5 w-5" />
            </button>
          </div>

          {/* Price Chart */}
          <div className="bg-bg-primary rounded-lg border border-bg-tertiary p-3">
            <div className="flex items-center justify-between mb-2">
              <div className="flex items-center gap-2">
                <span className="text-xs font-medium">Price</span>
                {lastClose > 0 && (
                  <span className="font-mono text-xs text-text-secondary">
                    ₹{lastClose.toLocaleString("en-IN", { maximumFractionDigits: 1 })}
                  </span>
                )}
                {candles.length > 1 && (
                  <span
                    className={cn(
                      "text-xs font-medium",
                      pctChange >= 0 ? "text-profit" : "text-loss",
                    )}
                  >
                    {pctChange >= 0 ? "+" : ""}
                    {pctChange.toFixed(1)}%
                  </span>
                )}
              </div>
              <div className="flex gap-1">
                {([30, 90, 180] as const).map((d) => (
                  <button
                    key={d}
                    onClick={() => setCandleDays(d)}
                    className={cn(
                      "px-1.5 py-0.5 rounded text-[10px]",
                      candleDays === d
                        ? "bg-accent text-white"
                        : "bg-bg-tertiary text-text-secondary hover:text-text-primary",
                    )}
                  >
                    {d}d
                  </button>
                ))}
              </div>
            </div>

            {candleLoading ? (
              <div className="h-[200px] flex items-center justify-center text-xs text-text-secondary">
                Loading…
              </div>
            ) : candles.length === 0 ? (
              <div className="h-[200px] flex items-center justify-center text-xs text-text-secondary">
                No candle data
              </div>
            ) : (
              <>
                {/* Price area chart */}
                <ResponsiveContainer width="100%" height={180}>
                  <AreaChart data={chartData} margin={{ top: 4, right: 4, left: 0, bottom: 0 }}>
                    <defs>
                      <linearGradient id="priceGrad" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor={pctChange >= 0 ? "#22c55e" : "#ef4444"} stopOpacity={0.3} />
                        <stop offset="95%" stopColor={pctChange >= 0 ? "#22c55e" : "#ef4444"} stopOpacity={0.02} />
                      </linearGradient>
                    </defs>
                    <XAxis
                      dataKey="dateLabel"
                      tick={{ fill: "#6b7280", fontSize: 8 }}
                      tickLine={false}
                      interval="preserveStartEnd"
                    />
                    <YAxis
                      domain={[priceMin, priceMax]}
                      tick={{ fill: "#6b7280", fontSize: 8 }}
                      tickLine={false}
                      axisLine={false}
                      tickFormatter={(v) => `₹${v >= 1000 ? (v / 1000).toFixed(1) + "k" : v.toFixed(0)}`}
                      width={42}
                    />
                    <Tooltip
                      contentStyle={{ backgroundColor: "#111827", border: "1px solid #1f2937", fontSize: 10 }}
                      formatter={(v: any) => [`₹${Number(v).toLocaleString("en-IN", { maximumFractionDigits: 1 })}`, "Close"]}
                      labelFormatter={(l) => l}
                    />
                    <Area
                      type="monotone"
                      dataKey="close"
                      stroke={pctChange >= 0 ? "#22c55e" : "#ef4444"}
                      strokeWidth={1.5}
                      fill="url(#priceGrad)"
                      dot={false}
                      activeDot={{ r: 3 }}
                    />
                  </AreaChart>
                </ResponsiveContainer>

                {/* Volume bars */}
                <ResponsiveContainer width="100%" height={48}>
                  <BarChart data={chartData} margin={{ top: 0, right: 4, left: 0, bottom: 0 }}>
                    <YAxis domain={[0, volMax]} hide />
                    <XAxis dataKey="dateLabel" hide />
                    <Bar dataKey="volume" radius={[1, 1, 0, 0]} maxBarSize={6}>
                      {chartData.map((d, i) => (
                        <Cell
                          key={i}
                          fill={d.isUp ? "#22c55e" : "#ef4444"}
                          fillOpacity={0.5}
                        />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </>
            )}
          </div>

          {/* Stats */}
          <div className="grid grid-cols-2 gap-3">
            <StatCard label="Score" value={String(row.score ?? "—")} />
            <StatCard label="Setup" value={row.setup || "—"} />
            <StatCard label="Beta" value={row.beta?.toFixed(2) ?? "—"} />
            <StatCard label="Exchange" value={row.exchange || "—"} />
          </div>

          {row.reason && (
            <div>
              <span className="text-xs text-text-secondary">Reason</span>
              <p className="text-sm text-text-primary mt-0.5">{row.reason}</p>
            </div>
          )}
        </div>
      </div>
    </>
  );
}

function StatCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="bg-bg-tertiary/50 rounded-lg p-3">
      <span className="text-[10px] text-text-secondary">{label}</span>
      <p className="font-mono text-sm mt-0.5">{value}</p>
    </div>
  );
}
