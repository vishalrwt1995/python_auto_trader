"use client";

import { useState, useMemo, useEffect } from "react";
import { useDashboardStore } from "@/stores/dashboardStore";
import { useWatchlist } from "@/hooks/useWatchlist";
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
  PHASE2_INPLAY: "text-cyan-400",
  VWAP_TREND: "text-cyan-400",
  VWAP_REVERSAL: "text-orange-400",
};

const SETUP_BG: Record<string, string> = {
  BREAKOUT: "bg-profit/10",
  PULLBACK: "bg-accent/10",
  MEAN_REVERSION: "bg-neutral/10",
  PHASE1_MOMENTUM: "bg-purple-500/10",
  PHASE2_INPLAY: "bg-cyan-500/10",
  VWAP_TREND: "bg-cyan-500/10",
  VWAP_REVERSAL: "bg-orange-500/10",
};

const LIQUIDITY_COLOR: Record<string, string> = {
  A: "text-profit",
  B: "text-neutral",
  C: "text-text-secondary",
};

const VWAP_BIAS_COLOR: Record<string, string> = {
  ABOVE: "text-profit",
  BELOW: "text-loss",
  NEAR: "text-neutral",
};

function SetupBadge({ setup }: { setup: string }) {
  const label = setup || "—";
  return (
    <span
      className={cn(
        "text-[10px] font-semibold px-1.5 py-0.5 rounded",
        SETUP_BG[label] ?? "bg-bg-tertiary",
        SETUP_COLORS[label] ?? "text-text-secondary",
      )}
    >
      {label}
    </span>
  );
}

function ScoreBar({ score }: { score: number }) {
  return (
    <div className="flex items-center gap-2 justify-end">
      <div className="w-14 h-1.5 bg-bg-tertiary rounded-full overflow-hidden">
        <div
          className={cn(
            "h-full rounded-full",
            score >= 60 ? "bg-profit" : score >= 30 ? "bg-neutral" : "bg-loss",
          )}
          style={{ width: `${Math.min(100, Math.max(0, score))}%` }}
        />
      </div>
      <span className="font-mono text-xs w-7 text-right tabular-nums">
        {score.toFixed(0)}
      </span>
    </div>
  );
}

export default function WatchlistPage() {
  const { data: wlDoc } = useWatchlist();
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
      rows = rows.filter((r) => r.symbol.includes(q) || r.sector?.toUpperCase().includes(q));
    }
    if (sectorFilter) rows = rows.filter((r) => r.sector === sectorFilter);
    return rows;
  }, [watchlist, tab, search, sectorFilter]);

  const drawerRow = useMemo(
    () => watchlist.find((r) => r.symbol === drawerSymbol),
    [watchlist, drawerSymbol],
  );

  // Summary stats
  const swingCount = watchlist.filter((r) => r.eligible_swing).length;
  const intradayCount = watchlist.filter((r) => r.eligible_intraday).length;
  const phase2Count = watchlist.filter((r) => r.phase2_eligible).length;
  const avgScore =
    watchlist.length > 0
      ? watchlist.reduce((s, r) => s + (r.score ?? 0), 0) / watchlist.length
      : 0;

  const columns: Column<WatchlistRow>[] = useMemo(
    () => [
      {
        key: "rank",
        label: "#",
        render: (_r, i) => (
          <span className="text-text-secondary font-mono text-xs tabular-nums">{i + 1}</span>
        ),
      },
      {
        key: "symbol",
        label: "Symbol",
        sortable: true,
        sortValue: (r) => r.symbol,
        render: (r) => (
          <button
            className="font-semibold text-text-primary hover:text-accent transition-colors text-sm"
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
        key: "type",
        label: "Type",
        render: (r) => (
          <span
            className={cn(
              "text-[10px] font-medium px-1.5 py-0.5 rounded",
              r.wl_type === "swing"
                ? "bg-indigo-500/10 text-indigo-400"
                : "bg-cyan-500/10 text-cyan-400",
            )}
          >
            {r.wl_type === "swing" ? "SWING" : "INTRADAY"}
          </span>
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
        render: (r) => <SetupBadge setup={r.setup} />,
      },
      {
        key: "score",
        label: "Score",
        sortable: true,
        sortValue: (r) => r.score ?? 0,
        className: "text-right",
        render: (r) => <ScoreBar score={r.score ?? 0} />,
      },
      {
        key: "liquidity",
        label: "Liq",
        sortable: true,
        sortValue: (r) => r.liquidity_bucket ?? "",
        render: (r) => (
          <span
            className={cn(
              "font-mono text-xs font-semibold",
              LIQUIDITY_COLOR[r.liquidity_bucket ?? ""] ?? "text-text-secondary",
            )}
          >
            {r.liquidity_bucket || "—"}
          </span>
        ),
      },
      {
        key: "vwap",
        label: "VWAP",
        render: (r) =>
          r.vwap_bias ? (
            <span
              className={cn(
                "text-[10px] font-medium",
                VWAP_BIAS_COLOR[r.vwap_bias.toUpperCase()] ?? "text-text-secondary",
              )}
            >
              {r.vwap_bias}
            </span>
          ) : (
            <span className="text-text-secondary text-xs">—</span>
          ),
      },
      {
        key: "p2",
        label: "P2",
        render: (r) => (
          <span
            className={cn(
              "text-[10px] font-semibold",
              r.phase2_eligible ? "text-cyan-400" : "text-bg-tertiary",
            )}
          >
            {r.phase2_eligible ? "✓" : "·"}
          </span>
        ),
      },
    ],
    [],
  );

  const regime = wlDoc?.regime ?? "";
  const riskMode = wlDoc?.risk_mode ?? "";
  const runBlock = wlDoc?.run_block ?? "";
  const generatedAt = wlDoc?.generated_at ?? "";

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <div>
          <h1 className="text-xl font-semibold">Watchlist</h1>
          {generatedAt && (
            <p className="text-[11px] text-text-secondary mt-0.5">
              Updated {new Date(generatedAt).toLocaleTimeString("en-IN", { hour: "2-digit", minute: "2-digit", timeZone: "Asia/Kolkata" })} IST
              {runBlock ? ` · ${runBlock}` : ""}
            </p>
          )}
        </div>
        <span className="text-xs text-text-secondary">
          {filtered.length} of {watchlist.length} shown
        </span>
      </div>

      {/* Regime Banner */}
      {(regime || riskMode) && (
        <div className="flex flex-wrap gap-2 text-[11px]">
          {regime && (
            <span
              className={cn(
                "px-2 py-1 rounded font-medium",
                regime === "TREND" ? "bg-profit/10 text-profit" :
                regime === "RANGE" ? "bg-neutral/10 text-neutral" :
                regime === "RISK_OFF" ? "bg-loss/10 text-loss" :
                "bg-bg-tertiary text-text-secondary",
              )}
            >
              Regime: {regime}
            </span>
          )}
          {riskMode && (
            <span
              className={cn(
                "px-2 py-1 rounded font-medium",
                riskMode === "TIGHT" ? "bg-loss/10 text-loss" :
                riskMode === "NORMAL" ? "bg-bg-tertiary text-text-secondary" :
                "bg-neutral/10 text-neutral",
              )}
            >
              Risk: {riskMode}
            </span>
          )}
        </div>
      )}

      {/* Stats Row */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-3 text-center">
          <p className="text-xl font-mono font-bold text-text-primary">{watchlist.length}</p>
          <p className="text-[10px] text-text-secondary mt-0.5">Total</p>
        </div>
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-3 text-center">
          <p className="text-xl font-mono font-bold text-indigo-400">{swingCount}</p>
          <p className="text-[10px] text-text-secondary mt-0.5">Swing</p>
        </div>
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-3 text-center">
          <p className="text-xl font-mono font-bold text-cyan-400">{intradayCount}</p>
          <p className="text-[10px] text-text-secondary mt-0.5">Intraday</p>
        </div>
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-3 text-center">
          <p className="text-xl font-mono font-bold text-profit">{avgScore.toFixed(0)}</p>
          <p className="text-[10px] text-text-secondary mt-0.5">Avg Score</p>
        </div>
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
              {t === "all"
                ? `All (${watchlist.length})`
                : t === "swing"
                  ? `Swing (${swingCount})`
                  : `Intraday (${intradayCount})`}
            </button>
          ))}
        </div>

        <div className="relative flex-1 max-w-xs">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-text-secondary" />
          <input
            type="text"
            placeholder="Search symbol or sector..."
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

      {/* Phase 2 note */}
      {phase2Count > 0 && (
        <p className="text-[11px] text-cyan-400/70">
          ✦ {phase2Count} stocks Phase 2 eligible (live VWAP signals)
        </p>
      )}

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

  const priceMin = candles.length > 0 ? Math.min(...candles.map((c) => c.low)) * 0.995 : 0;
  const priceMax = candles.length > 0 ? Math.max(...candles.map((c) => c.high)) * 1.005 : 0;
  const volMax = candles.length > 0 ? Math.max(...candles.map((c) => c.volume ?? 0)) : 1;

  const chartData = candles.map((c) => ({
    ...c,
    dateLabel: c.time?.slice(5) ?? "",
    isUp: (c.close ?? 0) >= (c.open ?? 0),
  }));

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
              <div className="flex items-center gap-2">
                <h2 className="text-lg font-semibold">{row.symbol}</h2>
                <SetupBadge setup={row.setup} />
                {row.wl_type && (
                  <span
                    className={cn(
                      "text-[10px] font-medium px-1.5 py-0.5 rounded",
                      row.wl_type === "swing"
                        ? "bg-indigo-500/10 text-indigo-400"
                        : "bg-cyan-500/10 text-cyan-400",
                    )}
                  >
                    {row.wl_type.toUpperCase()}
                  </span>
                )}
              </div>
              <p className="text-xs text-text-secondary mt-0.5">
                {row.sector}
                {row.macro_sector ? ` · ${row.macro_sector}` : ""}
                {" · "}
                {row.exchange}
              </p>
            </div>
            <button
              onClick={onClose}
              className="p-1.5 rounded hover:bg-bg-tertiary transition-colors"
            >
              <X className="h-5 w-5" />
            </button>
          </div>

          {/* Stats grid */}
          <div className="grid grid-cols-3 gap-2">
            <StatCard
              label="Score"
              value={String((row.score ?? 0).toFixed(1))}
              valueClass={
                (row.score ?? 0) >= 60
                  ? "text-profit"
                  : (row.score ?? 0) >= 30
                    ? "text-neutral"
                    : "text-loss"
              }
            />
            <StatCard label="Liquidity" value={row.liquidity_bucket || "—"} valueClass={LIQUIDITY_COLOR[row.liquidity_bucket ?? ""] ?? ""} />
            <StatCard
              label="VWAP Bias"
              value={row.vwap_bias || "—"}
              valueClass={VWAP_BIAS_COLOR[row.vwap_bias?.toUpperCase() ?? ""] ?? ""}
            />
            <StatCard label="Turnover Rank" value={row.turnover_rank != null ? String(row.turnover_rank) : "—"} />
            <StatCard label="Phase 2" value={row.phase2_eligible ? "Eligible" : "No"} valueClass={row.phase2_eligible ? "text-cyan-400" : ""} />
            <StatCard label="Exchange" value={row.exchange || "—"} />
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
        </div>
      </div>
    </>
  );
}

function StatCard({
  label,
  value,
  valueClass,
}: {
  label: string;
  value: string;
  valueClass?: string;
}) {
  return (
    <div className="bg-bg-tertiary/50 rounded-lg p-3">
      <span className="text-[10px] text-text-secondary">{label}</span>
      <p className={cn("font-mono text-sm mt-0.5", valueClass)}>{value}</p>
    </div>
  );
}
