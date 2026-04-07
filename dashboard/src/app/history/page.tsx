"use client";

import { useEffect, useState, useMemo } from "react";
import { api } from "@/lib/api";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { DataTable, type Column } from "@/components/shared/DataTable";
import { cn } from "@/lib/utils";

// ── Types ─────────────────────────────────────────────────────────────────────

interface HistorySummary {
  total: number;
  expected_lcd: string;
  last_5m_run: string;
  status_1d: { fresh: number; stale: number; missing: number; invalid: number; other: number };
  status_5m: { fresh: number; stale: number; missing: number; invalid: number; no_data: number; other: number };
  fresh_pct_1d: number;
  fresh_pct_5m: number;
  issues_1d: number;
  issues_5m: number;
}

interface HistorySymbol {
  symbol: string;
  exchange: string;
  sector: string;
  last_1d_date: string;
  bars_1d: number | null;
  status_1d: string;
  stale_days: number | null;
  last_5m_date: string;
  bars_5m: number | null;
  status_5m: string;
}

type FilterTab = "all" | "fresh" | "issues";

// ── Constants ─────────────────────────────────────────────────────────────────

const STATUS_META: Record<string, { label: string; color: string; bg: string; dot: string }> = {
  FRESH:   { label: "Fresh",      color: "#22c55e", bg: "bg-green-500/10",  dot: "bg-green-500"  },
  STALE:   { label: "Stale",      color: "#f59e0b", bg: "bg-amber-500/10",  dot: "bg-amber-500"  },
  MISSING: { label: "Missing",    color: "#ef4444", bg: "bg-red-500/10",    dot: "bg-red-500"    },
  INVALID: { label: "Invalid Key",color: "#8b5cf6", bg: "bg-purple-500/10", dot: "bg-purple-500" },
  NO_DATA: { label: "No Data",    color: "#6b7280", bg: "bg-gray-500/10",   dot: "bg-gray-500"   },
  OTHER:   { label: "Other",      color: "#6b7280", bg: "bg-gray-500/10",   dot: "bg-gray-500"   },
};

// ── StatusBadge ───────────────────────────────────────────────────────────────

function StatusBadge({ status }: { status: string }) {
  const m = STATUS_META[status] ?? STATUS_META.OTHER;
  return (
    <span className={cn("inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-[10px] font-medium", m.bg)}>
      <span className={cn("w-1.5 h-1.5 rounded-full shrink-0", m.dot)} />
      <span style={{ color: m.color }}>{m.label}</span>
    </span>
  );
}

// ── CoverageBar ───────────────────────────────────────────────────────────────

function CoverageBar({ counts, total }: { counts: Record<string, number>; total: number }) {
  if (total === 0) return null;
  const segments = [
    { key: "fresh",   color: "#22c55e" },
    { key: "stale",   color: "#f59e0b" },
    { key: "missing", color: "#ef4444" },
    { key: "invalid", color: "#8b5cf6" },
    { key: "no_data", color: "#6b7280" },
    { key: "other",   color: "#374151" },
  ];
  return (
    <div className="flex h-2 rounded-full overflow-hidden w-full gap-px mt-2">
      {segments.map(({ key, color }) => {
        const val = counts[key] ?? 0;
        const pct = (val / total) * 100;
        if (pct === 0) return null;
        return (
          <div
            key={key}
            style={{ width: `${pct}%`, backgroundColor: color }}
            title={`${STATUS_META[key.toUpperCase()]?.label ?? key}: ${val}`}
          />
        );
      })}
    </div>
  );
}

// ── StatusBreakdownPanel ──────────────────────────────────────────────────────

function StatusBreakdownPanel({
  title,
  counts,
  total,
  onFilter,
  activeFilter,
}: {
  title: string;
  counts: Record<string, number>;
  total: number;
  onFilter: (s: string) => void;
  activeFilter: string;
}) {
  const rows = [
    { key: "fresh",   label: "Fresh",       color: "#22c55e", bg: "bg-green-500/10"  },
    { key: "stale",   label: "Stale",       color: "#f59e0b", bg: "bg-amber-500/10"  },
    { key: "missing", label: "Missing",     color: "#ef4444", bg: "bg-red-500/10"    },
    { key: "invalid", label: "Invalid Key", color: "#8b5cf6", bg: "bg-purple-500/10" },
    { key: "no_data", label: "No Data",     color: "#6b7280", bg: "bg-gray-500/10"   },
    { key: "other",   label: "Other",       color: "#4b5563", bg: "bg-gray-500/10"   },
  ].filter(({ key }) => (counts[key] ?? 0) > 0);

  return (
    <div className="bg-bg-secondary rounded-xl border border-bg-tertiary p-5 flex-1">
      <p className="text-xs font-semibold text-text-secondary uppercase tracking-widest mb-4">{title}</p>
      <div className="space-y-2">
        {rows.map(({ key, label, color, bg }) => {
          const val = counts[key] ?? 0;
          const pct = total > 0 ? (val / total) * 100 : 0;
          const upperKey = key.toUpperCase();
          const active = activeFilter === upperKey;
          return (
            <button
              key={key}
              onClick={() => onFilter(active ? "" : upperKey)}
              className={cn(
                "w-full flex items-center gap-3 px-3 py-2 rounded-lg transition-all text-left",
                active ? `${bg} ring-1 ring-inset` : "hover:bg-bg-tertiary/50",
              )}
            >
              <span className="w-2.5 h-2.5 rounded-full shrink-0" style={{ backgroundColor: color }} />
              <span className="text-xs text-text-secondary flex-1">{label}</span>
              <div className="flex items-center gap-2">
                <div className="w-20 h-1 bg-bg-tertiary rounded-full overflow-hidden">
                  <div className="h-full rounded-full" style={{ width: `${pct}%`, backgroundColor: color }} />
                </div>
                <span className="text-xs font-mono text-text-primary w-10 text-right">{val.toLocaleString()}</span>
                <span className="text-[10px] text-text-secondary w-10 text-right">{pct.toFixed(1)}%</span>
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────

export default function HistoryPage() {
  const [summary, setSummary] = useState<HistorySummary | null>(null);
  const [symbols, setSymbols] = useState<HistorySymbol[]>([]);
  const [loading, setLoading] = useState(true);
  const [tab, setTab] = useState<FilterTab>("all");
  const [search, setSearch] = useState("");
  const [filter1d, setFilter1d] = useState("");
  const [filter5m, setFilter5m] = useState("");

  useEffect(() => {
    Promise.all([
      api.getHistorySummary(),
      api.getHistorySymbols(),
    ])
      .then(([s, d]: any[]) => {
        setSummary(s as HistorySummary);
        setSymbols((d.symbols ?? []) as HistorySymbol[]);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const filtered = useMemo(() => {
    let data = symbols;
    const q = search.toLowerCase();
    if (q) data = data.filter((r) => r.symbol.toLowerCase().includes(q) || r.sector.toLowerCase().includes(q));
    if (tab === "fresh")  data = data.filter((r) => r.status_1d === "FRESH" && r.status_5m === "FRESH");
    if (tab === "issues") data = data.filter((r) => r.status_1d !== "FRESH" || r.status_5m !== "FRESH");
    if (filter1d) data = data.filter((r) => r.status_1d === filter1d);
    if (filter5m) data = data.filter((r) => r.status_5m === filter5m);
    return data;
  }, [symbols, search, tab, filter1d, filter5m]);

  const tabCounts = useMemo(() => ({
    all:    symbols.length,
    fresh:  symbols.filter((r) => r.status_1d === "FRESH" && r.status_5m === "FRESH").length,
    issues: symbols.filter((r) => r.status_1d !== "FRESH" || r.status_5m !== "FRESH").length,
  }), [symbols]);

  const columns: Column<HistorySymbol>[] = useMemo(() => [
    {
      key: "symbol",
      label: "Symbol",
      sortable: true,
      sortValue: (r) => r.symbol,
      render: (r) => <span className="font-semibold text-xs">{r.symbol}</span>,
    },
    {
      key: "exchange",
      label: "Exch",
      sortable: true,
      sortValue: (r) => r.exchange,
      className: "text-xs text-text-secondary",
      render: (r) => <span>{r.exchange}</span>,
    },
    {
      key: "sector",
      label: "Sector",
      sortable: true,
      sortValue: (r) => r.sector,
      render: (r) => <span className="text-xs text-text-secondary">{r.sector || "—"}</span>,
    },
    {
      key: "status_1d",
      label: "1D Status",
      sortable: true,
      sortValue: (r) => r.status_1d,
      tooltip: "FRESH = data_quality_flag=GOOD (last candle matches expected trading day). STALE = behind expected date. MISSING = no candles in GCS. INVALID = bad instrument key.",
      render: (r) => <StatusBadge status={r.status_1d} />,
    },
    {
      key: "last_1d_date",
      label: "1D Last Date",
      sortable: true,
      sortValue: (r) => r.last_1d_date ?? "",
      className: "font-mono text-xs",
      tooltip: "Date of the most recent daily candle in the GCS cache for this symbol.",
      render: (r) => <span className="text-text-secondary">{r.last_1d_date || "—"}</span>,
    },
    {
      key: "bars_1d",
      label: "1D Bars",
      sortable: true,
      sortValue: (r) => r.bars_1d ?? 0,
      className: "text-right font-mono text-xs",
      tooltip: "Total daily candles stored in the GCS score cache for this symbol.",
      render: (r) => <span className="text-text-secondary">{r.bars_1d ?? "—"}</span>,
    },
    {
      key: "status_5m",
      label: "5M Status",
      sortable: true,
      sortValue: (r) => r.status_5m,
      tooltip: "FRESH = last 5m candle matches expected LCD. STALE/MISSING = data gaps. NO_DATA = 5m pipeline hasn't run yet for this symbol.",
      render: (r) => <StatusBadge status={r.status_5m} />,
    },
    {
      key: "last_5m_date",
      label: "5M Last Date",
      sortable: true,
      sortValue: (r) => r.last_5m_date ?? "",
      className: "font-mono text-xs",
      tooltip: "Date of the most recent 5-minute candle in the GCS intraday cache.",
      render: (r) => <span className="text-text-secondary">{r.last_5m_date || "—"}</span>,
    },
    {
      key: "bars_5m",
      label: "5M Bars",
      sortable: true,
      sortValue: (r) => r.bars_5m ?? 0,
      className: "text-right font-mono text-xs",
      tooltip: "Total 5-minute candles stored in the GCS intraday cache.",
      render: (r) => <span className="text-text-secondary">{r.bars_5m ?? "—"}</span>,
    },
  ], []);

  if (loading) return <LoadingSkeleton lines={12} />;

  const tabClass = (t: FilterTab) => cn(
    "px-3 py-1.5 rounded-lg text-xs font-medium transition-all",
    tab === t
      ? "bg-accent text-white shadow-sm"
      : "bg-bg-tertiary text-text-secondary hover:text-text-primary",
  );

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold">Data Freshness</h1>
          {summary?.expected_lcd && (
            <p className="text-xs text-text-secondary mt-0.5">
              Expected last completed trading day: <span className="font-mono text-text-primary">{summary.expected_lcd}</span>
            </p>
          )}
        </div>
      </div>

      {/* Summary Cards */}
      {summary && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {/* 1D Coverage */}
          <div className="bg-bg-secondary rounded-xl border border-bg-tertiary p-4">
            <div className="flex items-center justify-between mb-1">
              <p className="text-[10px] text-text-secondary uppercase tracking-widest font-medium">1D Coverage</p>
              <span className="text-[10px] text-text-secondary">{summary.status_1d.fresh}/{summary.total}</span>
            </div>
            <p className="text-3xl font-mono font-bold text-green-400">{summary.fresh_pct_1d}%</p>
            <CoverageBar counts={summary.status_1d} total={summary.total} />
          </div>

          {/* 5M Coverage */}
          <div className="bg-bg-secondary rounded-xl border border-bg-tertiary p-4">
            <div className="flex items-center justify-between mb-1">
              <p className="text-[10px] text-text-secondary uppercase tracking-widest font-medium">5M Coverage</p>
              <span className="text-[10px] text-text-secondary">{summary.status_5m.fresh}/{summary.total}</span>
            </div>
            <p className="text-3xl font-mono font-bold text-blue-400">{summary.fresh_pct_5m}%</p>
            <CoverageBar counts={summary.status_5m} total={summary.total} />
          </div>

          {/* Issues */}
          <div className="bg-bg-secondary rounded-xl border border-bg-tertiary p-4">
            <p className="text-[10px] text-text-secondary uppercase tracking-widest font-medium mb-1">Issues</p>
            <p className={cn(
              "text-3xl font-mono font-bold",
              summary.issues_1d + summary.issues_5m > 0 ? "text-amber-400" : "text-green-400"
            )}>
              {(summary.issues_1d + summary.issues_5m).toLocaleString()}
            </p>
            <p className="text-[10px] text-text-secondary mt-2">
              1D: {summary.issues_1d} &nbsp;·&nbsp; 5M: {summary.issues_5m}
            </p>
          </div>

          {/* Universe */}
          <div className="bg-bg-secondary rounded-xl border border-bg-tertiary p-4">
            <p className="text-[10px] text-text-secondary uppercase tracking-widest font-medium mb-1">Universe</p>
            <p className="text-3xl font-mono font-bold">{summary.total.toLocaleString()}</p>
            <p className="text-[10px] text-text-secondary mt-2">
              {summary.last_5m_run
                ? <>5M run: <span className="font-mono">{summary.last_5m_run}</span></>
                : "5M pipeline not yet run"}
            </p>
          </div>
        </div>
      )}

      {/* Status Breakdown */}
      {summary && (
        <div className="flex gap-4">
          <StatusBreakdownPanel
            title="Daily (1D) Data"
            counts={summary.status_1d}
            total={summary.total}
            onFilter={(s) => { setFilter1d(s); setFilter5m(""); }}
            activeFilter={filter1d}
          />
          <StatusBreakdownPanel
            title="Intraday (5M) Data"
            counts={summary.status_5m}
            total={summary.total}
            onFilter={(s) => { setFilter5m(s); setFilter1d(""); }}
            activeFilter={filter5m}
          />
        </div>
      )}

      {/* Table section */}
      <div className="bg-bg-secondary rounded-xl border border-bg-tertiary">
        {/* Table header / filters */}
        <div className="flex flex-wrap items-center gap-3 px-4 py-3 border-b border-bg-tertiary">
          {/* Tabs */}
          <div className="flex gap-1">
            <button className={tabClass("all")}    onClick={() => { setTab("all");    setFilter1d(""); setFilter5m(""); }}>
              All <span className="ml-1 text-[10px] opacity-70">{tabCounts.all.toLocaleString()}</span>
            </button>
            <button className={tabClass("fresh")}  onClick={() => { setTab("fresh");  setFilter1d(""); setFilter5m(""); }}>
              Fresh <span className="ml-1 text-[10px] opacity-70">{tabCounts.fresh.toLocaleString()}</span>
            </button>
            <button className={tabClass("issues")} onClick={() => { setTab("issues"); setFilter1d(""); setFilter5m(""); }}>
              Issues <span className={cn("ml-1 text-[10px]", tabCounts.issues > 0 ? "text-amber-400" : "opacity-70")}>{tabCounts.issues.toLocaleString()}</span>
            </button>
          </div>

          <div className="flex items-center gap-2 ml-auto">
            {(filter1d || filter5m) && (
              <button
                onClick={() => { setFilter1d(""); setFilter5m(""); }}
                className="text-[10px] text-accent hover:text-accent/80 transition-colors"
              >
                Clear filter ✕
              </button>
            )}
            <input
              type="text"
              placeholder="Search symbol or sector…"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="px-3 py-1.5 bg-bg-tertiary rounded-lg text-xs text-text-primary placeholder:text-text-secondary w-48 focus:outline-none focus:ring-1 focus:ring-accent/50"
            />
            <span className="text-xs text-text-secondary whitespace-nowrap">
              {filtered.length.toLocaleString()} symbols
            </span>
          </div>
        </div>

        {/* Active filter indicator */}
        {(filter1d || filter5m) && (
          <div className="px-4 py-2 border-b border-bg-tertiary flex gap-2">
            {filter1d && (
              <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-accent/15 text-accent text-[10px] border border-accent/30">
                1D: {STATUS_META[filter1d]?.label ?? filter1d}
                <button onClick={() => setFilter1d("")} className="ml-0.5 opacity-70 hover:opacity-100">✕</button>
              </span>
            )}
            {filter5m && (
              <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-accent/15 text-accent text-[10px] border border-accent/30">
                5M: {STATUS_META[filter5m]?.label ?? filter5m}
                <button onClick={() => setFilter5m("")} className="ml-0.5 opacity-70 hover:opacity-100">✕</button>
              </span>
            )}
          </div>
        )}

        <DataTable
          columns={columns}
          data={filtered}
          emptyMessage="No symbols match the current filters"
          maxHeight="calc(100vh - 420px)"
        />
      </div>
    </div>
  );
}
