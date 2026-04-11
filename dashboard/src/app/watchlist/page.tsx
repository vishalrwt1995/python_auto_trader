"use client";

import { useState, useMemo, useEffect, useRef } from "react";
import { useDashboardStore } from "@/stores/dashboardStore";
import { useWatchlist } from "@/hooks/useWatchlist";
import { DataTable, type Column } from "@/components/shared/DataTable";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { cn } from "@/lib/utils";
import type { WatchlistRow } from "@/lib/types";
import { Search, Globe, TrendingUp, Zap, Star } from "lucide-react";
import { useRouter } from "next/navigation";

const LIQUIDITY_COLOR: Record<string, string> = {
  A: "text-profit",
  B: "text-neutral",
  C: "text-text-secondary",
};

const VWAP_BIAS_COLOR: Record<string, string> = {
  ABOVE: "text-profit font-bold",
  BELOW: "text-loss font-bold",
  NEAR: "text-neutral",
};

function getSetupStyle(setup: string): { bg: string; text: string } {
  if (setup === "BREAKOUT") return { bg: "bg-profit/15", text: "text-profit" };
  if (setup === "PULLBACK") return { bg: "bg-amber-500/15", text: "text-amber-400" };
  if (setup === "VWAP_TREND" || setup === "VWAP_REVERSAL" || setup === "VWAP")
    return { bg: "bg-accent/15", text: "text-accent" };
  return { bg: "bg-bg-tertiary", text: "text-text-secondary" };
}

function getSetupIcon(setup: string): string {
  if (setup === "BREAKOUT") return "↑";
  if (setup === "PULLBACK") return "↗";
  if (setup === "MEAN_REVERSION") return "↔";
  return "";
}

function SetupBadge({ setup }: { setup: string }) {
  const label = setup || "—";
  const { bg, text } = getSetupStyle(label);
  const icon = getSetupIcon(label);
  return (
    <span
      className={cn(
        "text-[10px] font-semibold px-1.5 py-0.5 rounded inline-flex items-center gap-0.5",
        bg,
        text,
      )}
    >
      {icon && <span>{icon}</span>}
      {label}
    </span>
  );
}

function ScoreBar({ score }: { score: number }) {
  const barRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const el = barRef.current;
    if (!el) return;
    el.style.width = "0%";
    const raf = requestAnimationFrame(() => {
      el.style.transition = "width 0.6s ease-out";
      el.style.width = `${Math.min(100, Math.max(0, score))}%`;
    });
    return () => cancelAnimationFrame(raf);
  }, [score]);

  return (
    <div className="flex items-center gap-2 justify-end">
      <div className="w-14 h-1.5 bg-bg-tertiary rounded-full overflow-hidden">
        <div
          ref={barRef}
          className="h-full rounded-full bg-gradient-to-r from-accent/60 to-accent"
          style={{ width: "0%" }}
        />
      </div>
      <span className="font-mono text-xs w-7 text-right tabular-nums">
        {score.toFixed(0)}
      </span>
    </div>
  );
}

function getMinutesAgo(ts: string): string {
  if (!ts) return "";
  const diff = Math.floor((Date.now() - new Date(ts).getTime()) / 60000);
  if (diff < 1) return "just now";
  if (diff === 1) return "1 min ago";
  return `${diff} mins ago`;
}

export default function WatchlistPage() {
  const router = useRouter();
  const { data: wlDoc } = useWatchlist();
  const watchlist = useDashboardStore((s) => s.watchlist);
  const [tab, setTab] = useState<"all" | "swing" | "intraday">("all");
  const [search, setSearch] = useState("");
  const [sectorFilter, setSectorFilter] = useState("");

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
          <div className="flex items-center gap-2">
            <span
              className={cn(
                "w-2 h-2 rounded-full shrink-0",
                r.wl_type === "swing" ? "bg-indigo-400" : "bg-cyan-400",
              )}
            />
            <span className="font-semibold text-text-primary text-sm">{r.symbol}</span>
          </div>
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
                "text-[10px]",
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
  const minutesAgo = getMinutesAgo(generatedAt);

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <div>
          <h1 className="text-xl font-semibold">Watchlist</h1>
          {generatedAt && (
            <p className="text-[11px] text-text-secondary mt-0.5">
              Updated {minutesAgo}
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
        {/* Total */}
        <div className="bg-bg-secondary rounded-lg border-t-[3px] border-t-slate-500 border-x border-b border-bg-tertiary p-3 shadow-md shadow-black/20 flex items-start justify-between">
          <div>
            <p className="text-xl font-mono font-bold text-text-primary">{watchlist.length}</p>
            <p className="text-[10px] text-text-secondary mt-0.5">Total</p>
          </div>
          <Globe className="h-4 w-4 text-slate-500 mt-0.5" />
        </div>
        {/* Swing */}
        <div className="bg-bg-secondary rounded-lg border-t-[3px] border-t-indigo-400 border-x border-b border-bg-tertiary p-3 shadow-md shadow-black/20 flex items-start justify-between">
          <div>
            <p className="text-xl font-mono font-bold text-indigo-400">{swingCount}</p>
            <p className="text-[10px] text-text-secondary mt-0.5">Swing</p>
          </div>
          <TrendingUp className="h-4 w-4 text-indigo-400 mt-0.5" />
        </div>
        {/* Intraday */}
        <div className="bg-bg-secondary rounded-lg border-t-[3px] border-t-cyan-400 border-x border-b border-bg-tertiary p-3 shadow-md shadow-black/20 flex items-start justify-between">
          <div>
            <p className="text-xl font-mono font-bold text-cyan-400">{intradayCount}</p>
            <p className="text-[10px] text-text-secondary mt-0.5">Intraday</p>
          </div>
          <Zap className="h-4 w-4 text-cyan-400 mt-0.5" />
        </div>
        {/* Avg Score */}
        <div className="bg-bg-secondary rounded-lg border-t-[3px] border-t-[#22c55e] border-x border-b border-bg-tertiary p-3 shadow-md shadow-black/20 flex items-start justify-between">
          <div>
            <p className="text-xl font-mono font-bold text-profit">{avgScore.toFixed(0)}</p>
            <p className="text-[10px] text-text-secondary mt-0.5">Avg Score</p>
          </div>
          <Star className="h-4 w-4 text-profit mt-0.5" />
        </div>
      </div>

      {/* Tabs + Filters */}
      <div className="flex flex-col sm:flex-row gap-3">
        {/* Pill segmented control */}
        <div className="inline-flex bg-bg-tertiary/50 rounded-xl p-0.5">
          {(["all", "swing", "intraday"] as const).map((t) => (
            <button
              key={t}
              onClick={() => setTab(t)}
              className={cn(
                "px-3 py-1.5 rounded-[10px] text-xs font-medium transition-all",
                tab === t
                  ? "bg-gradient-to-r from-accent to-blue-600 text-white shadow shadow-accent/30"
                  : "text-text-secondary hover:text-text-primary",
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
          onRowClick={(r) => router.push(`/symbol/${r.symbol}`)}
          emptyMessage="No stocks match filters"
        />
      )}
    </div>
  );
}
