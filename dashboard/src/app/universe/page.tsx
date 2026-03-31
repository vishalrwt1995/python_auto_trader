"use client";

import { useEffect, useState, useMemo } from "react";
import { api } from "@/lib/api";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { DataTable, type Column } from "@/components/shared/DataTable";
import { cn } from "@/lib/utils";
import {
  PieChart,
  Pie,
  Cell,
  ResponsiveContainer,
  Tooltip,
} from "recharts";

const PIE_COLORS = ["#22c55e", "#3b82f6", "#f59e0b", "#ef4444", "#8b5cf6", "#6b7280", "#ec4899", "#14b8a6"];

interface UniverseSymbol {
  symbol: string;
  exchange: string;
  sector: string;
  beta: number;
  eligible_swing: boolean;
  eligible_intraday: boolean;
  avg_turnover?: number;
  score?: number;
}

interface UniverseStats {
  total_symbols: number;
  eligible_swing: number;
  eligible_intraday: number;
  neither: number;
}

export default function UniversePage() {
  const [stats, setStats] = useState<UniverseStats | null>(null);
  const [symbols, setSymbols] = useState<UniverseSymbol[]>([]);
  const [loading, setLoading] = useState(true);
  const [sectorFilter, setSectorFilter] = useState("");
  const [eligFilter, setEligFilter] = useState<"" | "swing" | "intraday" | "both">("");
  const [search, setSearch] = useState("");

  useEffect(() => {
    Promise.all([
      api.getUniverseStats().then((d: any) => setStats(d as UniverseStats)),
      api.getUniverseList().then((d: any) => setSymbols(d.symbols ?? [])),
    ])
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const sectors = useMemo(
    () => [...new Set(symbols.map((s) => s.sector).filter(Boolean))].sort(),
    [symbols],
  );

  const sectorBreakdown = useMemo(() => {
    const map: Record<string, number> = {};
    symbols.forEach((s) => {
      const sec = s.sector || "Unknown";
      map[sec] = (map[sec] ?? 0) + 1;
    });
    return Object.entries(map)
      .map(([name, value]) => ({ name, value }))
      .sort((a, b) => b.value - a.value);
  }, [symbols]);

  const filtered = useMemo(() => {
    let data = symbols;
    if (search) {
      const q = search.toUpperCase();
      data = data.filter((s) => s.symbol.includes(q));
    }
    if (sectorFilter) data = data.filter((s) => s.sector === sectorFilter);
    if (eligFilter === "swing") data = data.filter((s) => s.eligible_swing);
    if (eligFilter === "intraday") data = data.filter((s) => s.eligible_intraday);
    if (eligFilter === "both") data = data.filter((s) => s.eligible_swing && s.eligible_intraday);
    return data;
  }, [symbols, search, sectorFilter, eligFilter]);

  const columns: Column<UniverseSymbol>[] = useMemo(
    () => [
      {
        key: "symbol",
        label: "Symbol",
        sortable: true,
        sortValue: (r) => r.symbol,
        render: (r) => <span className="font-medium">{r.symbol}</span>,
      },
      {
        key: "exchange",
        label: "Exchange",
        render: (r) => <span className="text-xs text-text-secondary">{r.exchange}</span>,
      },
      {
        key: "sector",
        label: "Sector",
        sortable: true,
        sortValue: (r) => r.sector,
        render: (r) => <span className="text-xs">{r.sector || "—"}</span>,
      },
      {
        key: "beta",
        label: "Beta",
        sortable: true,
        sortValue: (r) => r.beta,
        className: "text-right font-mono",
        render: (r) => (
          <span className={cn(r.beta > 1.2 ? "text-loss" : r.beta < 0.8 ? "text-profit" : "")}>
            {r.beta?.toFixed(2) ?? "—"}
          </span>
        ),
      },
      {
        key: "swing",
        label: "Swing",
        render: (r) => (
          <span className={r.eligible_swing ? "text-profit text-xs" : "text-text-secondary text-xs"}>
            {r.eligible_swing ? "Yes" : "No"}
          </span>
        ),
      },
      {
        key: "intraday",
        label: "Intraday",
        render: (r) => (
          <span className={r.eligible_intraday ? "text-profit text-xs" : "text-text-secondary text-xs"}>
            {r.eligible_intraday ? "Yes" : "No"}
          </span>
        ),
      },
      {
        key: "score",
        label: "Score",
        sortable: true,
        sortValue: (r) => r.score ?? 0,
        className: "text-right font-mono",
        render: (r) => (
          <span className={cn(
            "text-xs",
            (r.score ?? 0) >= 72 ? "text-profit" : (r.score ?? 0) >= 50 ? "text-neutral" : "text-text-secondary",
          )}>
            {r.score ?? "—"}
          </span>
        ),
      },
    ],
    [],
  );

  if (loading) return <LoadingSkeleton lines={10} />;

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-semibold">Universe Health</h1>

      {/* Stats Cards */}
      {stats && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <StatCard label="Total Symbols" value={stats.total_symbols} />
          <StatCard label="Swing Eligible" value={stats.eligible_swing} color="text-profit" />
          <StatCard label="Intraday Eligible" value={stats.eligible_intraday} color="text-accent" />
          <StatCard label="Neither" value={stats.neither} color="text-text-secondary" />
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Sector Breakdown Pie */}
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 lg:col-span-1">
          <h3 className="text-sm font-medium mb-2">Sector Breakdown</h3>
          {sectorBreakdown.length > 0 ? (
            <>
              <ResponsiveContainer width="100%" height={200}>
                <PieChart>
                  <Pie
                    data={sectorBreakdown}
                    cx="50%"
                    cy="50%"
                    innerRadius={40}
                    outerRadius={75}
                    dataKey="value"
                    nameKey="name"
                  >
                    {sectorBreakdown.map((_, i) => (
                      <Cell key={i} fill={PIE_COLORS[i % PIE_COLORS.length]} />
                    ))}
                  </Pie>
                  <Tooltip
                    contentStyle={{ backgroundColor: "#111827", border: "1px solid #1f2937", fontSize: 11 }}
                  />
                </PieChart>
              </ResponsiveContainer>
              <div className="mt-2 space-y-1 max-h-40 overflow-y-auto">
                {sectorBreakdown.map((d, i) => (
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
            </>
          ) : (
            <div className="h-[200px] flex items-center justify-center text-xs text-text-secondary">
              No universe data loaded
            </div>
          )}
        </div>

        {/* Filters + Table */}
        <div className="lg:col-span-2 space-y-4">
          <div className="flex flex-wrap gap-3">
            <input
              type="text"
              placeholder="Search symbol..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="px-3 py-1.5 bg-bg-tertiary rounded-lg text-xs text-text-primary placeholder:text-text-secondary w-40"
            />
            <select
              value={sectorFilter}
              onChange={(e) => setSectorFilter(e.target.value)}
              className="px-3 py-1.5 bg-bg-tertiary rounded-lg text-xs text-text-primary"
            >
              <option value="">All Sectors</option>
              {sectors.map((s) => (
                <option key={s} value={s}>{s}</option>
              ))}
            </select>
            <select
              value={eligFilter}
              onChange={(e) => setEligFilter(e.target.value as any)}
              className="px-3 py-1.5 bg-bg-tertiary rounded-lg text-xs text-text-primary"
            >
              <option value="">All Eligibility</option>
              <option value="swing">Swing</option>
              <option value="intraday">Intraday</option>
              <option value="both">Both</option>
            </select>
            <span className="text-xs text-text-secondary self-center ml-auto">
              {filtered.length} symbols
            </span>
          </div>

          <DataTable
            columns={columns}
            data={filtered}
            emptyMessage="No symbols match filters"
          />
        </div>
      </div>
    </div>
  );
}

function StatCard({ label, value, color }: { label: string; value: number; color?: string }) {
  return (
    <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 text-center">
      <p className={cn("text-3xl font-mono font-bold", color)}>{value}</p>
      <p className="text-xs text-text-secondary mt-1">{label}</p>
    </div>
  );
}
