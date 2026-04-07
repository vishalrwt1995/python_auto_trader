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
import { InfoBadge, Tooltip as AppTooltip } from "@/components/shared/Tooltip";

const PIE_COLORS = [
  "#22c55e", "#3b82f6", "#f59e0b", "#ef4444", "#8b5cf6",
  "#6b7280", "#ec4899", "#14b8a6", "#f97316", "#a78bfa",
  "#34d399", "#60a5fa",
];

const BUCKET_COLOR: Record<string, string> = {
  A: "#22c55e",
  B: "#3b82f6",
  C: "#f59e0b",
  D: "#ef4444",
};

const DQ_COLOR: Record<string, string> = {
  GOOD: "#22c55e",
  STALE: "#f59e0b",
  MISSING: "#ef4444",
  INVALID_KEY: "#8b5cf6",
  UNKNOWN: "#6b7280",
  "": "#6b7280",
};

// InfoTooltip → now uses shared InfoBadge
function InfoTooltip({ text }: { text: string }) {
  return <InfoBadge text={text} />;
}

// ── Active filter pill ───────────────────────────────────────────────────────

function ActivePill({ label, value, onClear }: { label: string; value: string; onClear: () => void }) {
  return (
    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-accent/20 text-accent text-xs border border-accent/30">
      <span className="text-text-secondary text-[10px]">{label}:</span>
      <span className="font-medium">{value}</span>
      <button
        onClick={onClear}
        className="ml-0.5 text-accent/70 hover:text-accent transition-colors leading-none"
        aria-label="Remove filter"
      >
        ✕
      </button>
    </span>
  );
}

// ── Types ────────────────────────────────────────────────────────────────────

interface UniverseSymbol {
  symbol: string;
  exchange: string;
  segment?: string;
  sector: string;
  beta: number;
  eligible_swing: boolean;
  eligible_intraday: boolean;
  universe_score?: number;   // 0-100 computed indicator score (EMA/RSI/MACD/Breakout/Volume)
  score_calc?: string;       // breakdown string e.g. "E20|P10|R15|M10|B15|V15|O5|N-5|S85"
  priority?: number;         // manual priority (legacy)
  price_last?: number;
  atr_pct_14d?: number;
  atr_14?: number;
  turnover_med_60d?: number;
  turnover_rank_60d?: number;
  liquidity_bucket?: string;
  gap_risk_60d?: number;
  bars_1d?: number;
  last_1d_date?: string;
  data_quality_flag?: string;
  stale_days?: number;
  disable_reason?: string;
  allowed_product?: string;
  strategy_pref?: string;
  enabled?: boolean;
}

interface UniverseStats {
  total_symbols: number;
  eligible_swing: number;
  eligible_intraday: number;
  neither: number;
}

type ViewTab = "eligible" | "all" | "excluded";

// ── Page ─────────────────────────────────────────────────────────────────────

export default function UniversePage() {
  const [stats, setStats] = useState<UniverseStats | null>(null);
  const [symbols, setSymbols] = useState<UniverseSymbol[]>([]);
  const [loading, setLoading] = useState(true);
  const [sectorFilter, setSectorFilter] = useState("");
  const [bucketFilter, setBucketFilter] = useState("");
  const [dqFilter, setDqFilter] = useState("");
  const [tab, setTab] = useState<ViewTab>("all");
  const [search, setSearch] = useState("");

  useEffect(() => {
    Promise.all([
      api.getUniverseStats().then((d: any) => setStats(d as UniverseStats)),
      api.getUniverseList({ limit: "3000" }).then((d: any) => setSymbols(d.symbols ?? [])),
    ])
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  // ── Derived data ────────────────────────────────────────────────────────

  const sectors = useMemo(
    () => [...new Set(symbols.map((s) => s.sector).filter(Boolean))].sort(),
    [symbols],
  );

  // Eligible by sector — all sectors, no truncation
  const sectorBreakdown = useMemo(() => {
    const eligible = symbols.filter((s) => s.eligible_swing || s.eligible_intraday);
    const map: Record<string, number> = {};
    eligible.forEach((s) => {
      const sec = s.sector || "Unknown";
      map[sec] = (map[sec] ?? 0) + 1;
    });
    return Object.entries(map)
      .map(([name, value]) => ({ name, value }))
      .sort((a, b) => b.value - a.value);
  }, [symbols]);

  const totalEligible = useMemo(
    () => symbols.filter((s) => s.eligible_swing || s.eligible_intraday).length,
    [symbols],
  );

  // Liquidity buckets — denominator = symbols that actually have a bucket
  const bucketBreakdown = useMemo(() => {
    const counts: Record<string, number> = { A: 0, B: 0, C: 0, D: 0 };
    symbols.forEach((s) => {
      if (s.liquidity_bucket && counts[s.liquidity_bucket] !== undefined)
        counts[s.liquidity_bucket]++;
    });
    const withBucket = Object.values(counts).reduce((a, b) => a + b, 0);
    const noBucket = symbols.length - withBucket;
    return { counts, withBucket, noBucket };
  }, [symbols]);

  // Data quality — dynamic from actual data, empty string normalised to ""
  const dqBreakdown = useMemo(() => {
    const map: Record<string, number> = {};
    symbols.forEach((s) => {
      const flag = s.data_quality_flag ?? "";
      map[flag] = (map[flag] ?? 0) + 1;
    });
    return Object.entries(map).sort((a, b) => b[1] - a[1]);
  }, [symbols]);

  // Bucket counts per option (for filter badges)
  const bucketCounts = useMemo(() => {
    const m: Record<string, number> = { "": symbols.length, A: 0, B: 0, C: 0, D: 0 };
    symbols.forEach((s) => { if (s.liquidity_bucket && m[s.liquidity_bucket] !== undefined) m[s.liquidity_bucket]++; });
    return m;
  }, [symbols]);

  // DQ counts per option (for filter badges)
  const dqCounts = useMemo(() => {
    const m: Record<string, number> = { "": symbols.length };
    symbols.forEach((s) => { const f = s.data_quality_flag ?? ""; m[f] = (m[f] ?? 0) + 1; });
    return m;
  }, [symbols]);

  // Sector counts (for filter badge on selected sector)
  const sectorCounts = useMemo(() => {
    const m: Record<string, number> = {};
    symbols.forEach((s) => { const sec = s.sector || ""; m[sec] = (m[sec] ?? 0) + 1; });
    return m;
  }, [symbols]);

  // ── Filtered table data ─────────────────────────────────────────────────

  const filtered = useMemo(() => {
    let data = symbols;
    if (tab === "eligible") data = data.filter((s) => s.eligible_swing || s.eligible_intraday);
    if (tab === "excluded") data = data.filter((s) => !s.eligible_swing && !s.eligible_intraday);
    if (search) {
      const q = search.toUpperCase();
      data = data.filter((s) => s.symbol.includes(q));
    }
    if (sectorFilter) data = data.filter((s) => s.sector === sectorFilter);
    if (bucketFilter) data = data.filter((s) => (s.liquidity_bucket ?? "") === bucketFilter);
    if (dqFilter !== "") data = data.filter((s) => (s.data_quality_flag ?? "") === dqFilter);
    return data;
  }, [symbols, search, sectorFilter, bucketFilter, dqFilter, tab]);

  const anyFilterActive = !!(sectorFilter || bucketFilter || dqFilter !== "" || search);

  function clearAllFilters() {
    setSectorFilter("");
    setBucketFilter("");
    setDqFilter("");
    setSearch("");
  }

  // ── Columns ─────────────────────────────────────────────────────────────

  const columns: Column<UniverseSymbol>[] = useMemo(
    () => [
      {
        key: "symbol",
        label: "Symbol",
        sortable: true,
        sortValue: (r) => r.symbol,
        render: (r) => (
          <span className={cn("font-medium", r.enabled === false ? "opacity-40 line-through" : "")}>
            {r.symbol}
          </span>
        ),
      },
      {
        key: "exchange",
        label: "Exch",
        sortable: true,
        sortValue: (r) => r.exchange,
        className: "text-xs text-text-secondary",
        render: (r) => <span>{r.exchange || "—"}</span>,
      },
      {
        key: "sector",
        label: "Sector",
        sortable: true,
        sortValue: (r) => r.sector,
        render: (r) => <span className="text-xs text-text-secondary">{r.sector || "—"}</span>,
      },
      {
        key: "universe_score",
        label: "Score",
        tooltip: "0–100 indicator score computed from daily candles: EMA stack, RSI, MACD, Breakout proximity, Volume, OBV. Hover the value to see breakdown.",
        sortable: true,
        sortValue: (r) => r.universe_score ?? -1,
        className: "text-center",
        render: (r) => {
          const s = r.universe_score;
          if (s == null) return <span className="text-text-secondary text-xs">—</span>;
          const color = s >= 60 ? "#22c55e" : s >= 40 ? "#f59e0b" : "#6b7280";
          return (
            <AppTooltip text={r.score_calc || `Score: ${s}`}>
              <span
                className="font-mono text-xs font-semibold px-1.5 py-0.5 rounded cursor-default"
                style={{ color, background: `${color}18` }}
              >
                {s}
              </span>
            </AppTooltip>
          );
        },
      },
      {
        key: "price",
        label: "Price ₹",
        tooltip: "Last known daily close price in INR, from the most recent candle in the GCS cache.",
        sortable: true,
        sortValue: (r) => r.price_last ?? 0,
        className: "text-right font-mono",
        render: (r) => (
          <span>{r.price_last ? r.price_last.toLocaleString("en-IN", { maximumFractionDigits: 1 }) : "—"}</span>
        ),
      },
      {
        key: "bucket",
        label: "Liq",
        tooltip: "Liquidity bucket assigned cross-sectionally by 60d median turnover. A=top 25%, B=25-50%, C=50-75%, D=bottom 25% of the universe. Used in eligibility scoring.",
        sortable: true,
        sortValue: (r) => r.liquidity_bucket ?? "Z",
        className: "text-center",
        render: (r) =>
          r.liquidity_bucket ? (
            <span
              className="px-1.5 py-0.5 rounded text-xs font-bold"
              style={{ color: BUCKET_COLOR[r.liquidity_bucket] ?? "#9ca3af" }}
            >
              {r.liquidity_bucket}
            </span>
          ) : (
            <span className="text-text-secondary text-xs">—</span>
          ),
      },
      {
        key: "atr_pct",
        label: "ATR%",
        tooltip: "ATR% = ATR(14) / last_price. ATR(14) = Wilder smoothing of True Range over 14 days. TR = max(H-L, |H-prevC|, |L-prevC|). Green <5%, amber 5-9%, red >9%.",
        sortable: true,
        sortValue: (r) => r.atr_pct_14d ?? 0,
        className: "text-right font-mono",
        render: (r) => {
          const v = r.atr_pct_14d;
          if (v == null || v === 0) return <span className="text-text-secondary">—</span>;
          const pct = (v * 100).toFixed(1);
          return (
            <span className={cn(v > 0.09 ? "text-loss" : v > 0.05 ? "text-neutral" : "text-profit")}>
              {pct}%
            </span>
          );
        },
      },
      {
        key: "gap_risk",
        label: "Gap%",
        tooltip: "Gap risk = avg |open/prevClose - 1| over last 60 days. Measures overnight gap frequency and size. >6% = high risk (red).",
        sortable: true,
        sortValue: (r) => r.gap_risk_60d ?? 0,
        className: "text-right font-mono",
        render: (r) => {
          const v = r.gap_risk_60d;
          if (v == null || v === 0) return <span className="text-text-secondary">—</span>;
          return (
            <span className={cn(v > 0.06 ? "text-loss" : "text-text-secondary")}>
              {(v * 100).toFixed(1)}%
            </span>
          );
        },
      },
      {
        key: "turnover",
        label: "Turnover",
        tooltip: "Median daily turnover = median(close × volume) over last 60 trading days. Represents typical daily liquidity in INR.",
        sortable: true,
        sortValue: (r) => r.turnover_med_60d ?? 0,
        className: "text-right font-mono",
        render: (r) => {
          const v = r.turnover_med_60d;
          if (!v) return <span className="text-text-secondary">—</span>;
          const cr = v / 1e7;
          return (
            <span className="text-xs">
              {cr >= 100 ? `₹${Math.round(cr)}Cr` : `₹${cr.toFixed(1)}Cr`}
            </span>
          );
        },
      },
      {
        key: "rank",
        label: "Rank",
        tooltip: "Turnover rank across the full universe (1 = highest 60d median turnover). Used to assign A/B/C/D liquidity buckets in quartiles.",
        sortable: true,
        sortValue: (r) => r.turnover_rank_60d ?? 99999,
        className: "text-right font-mono text-xs",
        render: (r) => (
          <span className="text-text-secondary">{r.turnover_rank_60d || "—"}</span>
        ),
      },
      {
        key: "beta",
        label: "Beta",
        tooltip: "β = cov(stock_returns, nifty_returns) / var(nifty_returns) over 90 trading days. β>1 = amplifies index moves. β=1.0 means pipeline has not yet computed real value.",
        sortable: true,
        sortValue: (r) => r.beta ?? 0,
        className: "text-right font-mono text-xs",
        render: (r) => (
          <span className="text-text-secondary">{r.beta ? r.beta.toFixed(2) : "—"}</span>
        ),
      },
      {
        key: "bars_1d",
        label: "Bars",
        tooltip: "Total number of daily candles available in the GCS cache for this symbol. Higher = longer history. Min 30 required for swing eligibility.",
        sortable: true,
        sortValue: (r) => r.bars_1d ?? 0,
        className: "text-right font-mono text-xs",
        render: (r) => (
          <span className="text-text-secondary">{r.bars_1d || "—"}</span>
        ),
      },
      {
        key: "last_1d_date",
        label: "Last Date",
        tooltip: "Date of the most recent daily candle available in the GCS cache. Stale if this lags the expected last completed trading day.",
        sortable: true,
        sortValue: (r) => r.last_1d_date ?? "",
        className: "text-xs text-text-secondary",
        render: (r) => <span>{r.last_1d_date ? r.last_1d_date.slice(0, 10) : "—"}</span>,
      },
      {
        key: "swing",
        label: "Swing",
        tooltip: "Eligible for overnight swing trades. True if eligible_swing=Y in Sheets, or allowed_product ∈ {BOTH, SWING} and data quality / ATR / bars pass thresholds.",
        render: (r) => (
          <span className={r.eligible_swing ? "text-profit text-xs" : "text-text-secondary text-xs"}>
            {r.eligible_swing ? "✓" : "✗"}
          </span>
        ),
      },
      {
        key: "intraday",
        label: "Intra",
        tooltip: "Eligible for same-day intraday trades. Uses tighter thresholds than swing: higher min bars, lower max ATR%, stricter liquidity bucket requirement.",
        render: (r) => (
          <span className={r.eligible_intraday ? "text-profit text-xs" : "text-text-secondary text-xs"}>
            {r.eligible_intraday ? "✓" : "✗"}
          </span>
        ),
      },
      {
        key: "dq",
        label: "Quality",
        tooltip: "Data quality flag from the pipeline. GOOD = fresh data; STALE = last candle is behind expected date; MISSING = no candles found; INVALID_KEY = bad instrument key.",
        render: (r) => {
          const flag = r.data_quality_flag;
          if (!flag) return <span className="text-text-secondary text-xs">—</span>;
          return (
            <span className="text-xs font-medium" style={{ color: DQ_COLOR[flag] ?? "#9ca3af" }}>
              {flag}
            </span>
          );
        },
      },
      {
        key: "disable_reason",
        label: "Excl. Reason",
        tooltip: "Why this symbol is excluded from eligible trading. Common reasons: LOW_BARS, HIGH_ATR, LOW_PRICE, POOR_LIQUIDITY, STALE_DATA, DISABLED, SUSPENDED.",
        render: (r) => (
          <span
            className="text-xs text-text-secondary truncate block max-w-[160px]"
            title={r.disable_reason}
          >
            {r.disable_reason || "—"}
          </span>
        ),
      },
    ],
    [],
  );

  // ── Render ───────────────────────────────────────────────────────────────

  if (loading) return <LoadingSkeleton lines={10} />;

  const tabClass = (t: ViewTab) =>
    cn(
      "px-3 py-1 text-xs rounded-md transition-colors",
      tab === t
        ? "bg-accent text-white"
        : "bg-bg-tertiary text-text-secondary hover:text-text-primary",
    );

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-semibold">Universe Health</h1>

      {/* ── Stats Cards ─────────────────────────────────────────────────── */}
      {stats && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <StatCard
            label="Total Symbols"
            value={stats.total_symbols}
            tooltip="All instruments loaded into Firestore universe. Includes enabled and disabled symbols across NSE/BSE."
          />
          <StatCard
            label="Swing Eligible"
            value={stats.eligible_swing}
            color="#22c55e"
            tooltip="Symbols allowed for overnight swing trades. Derived from eligible_swing flag, or allowed_product ∈ {BOTH, SWING}."
          />
          <StatCard
            label="Intraday Eligible"
            value={stats.eligible_intraday}
            color="#3b82f6"
            tooltip="Symbols allowed for same-day intraday trades. Derived from eligible_intraday flag, or allowed_product ∈ {BOTH, INTRADAY}."
          />
          <StatCard
            label="Excluded"
            value={stats.neither}
            color="#6b7280"
            tooltip="Symbols not eligible for either swing or intraday. Formula: total − swing − intraday + both (set union avoids double-counting)."
          />
        </div>
      )}

      {/* ── Charts Row ──────────────────────────────────────────────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">

        {/* Eligible by Sector */}
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
          <div className="flex items-center mb-2">
            <h3 className="text-sm font-medium">Eligible by Sector</h3>
            <InfoTooltip text="Distribution of symbols eligible for swing or intraday across all sectors. Covers all eligible symbols — no truncation." />
          </div>
          {sectorBreakdown.length > 0 ? (
            <>
              <ResponsiveContainer width="100%" height={180}>
                <PieChart>
                  <Pie
                    data={sectorBreakdown}
                    cx="50%"
                    cy="50%"
                    innerRadius={36}
                    outerRadius={68}
                    dataKey="value"
                    nameKey="name"
                  >
                    {sectorBreakdown.map((_, i) => (
                      <Cell key={i} fill={PIE_COLORS[i % PIE_COLORS.length]} />
                    ))}
                  </Pie>
                  <Tooltip
                    contentStyle={{
                      backgroundColor: "#111827",
                      border: "1px solid #1f2937",
                      fontSize: 11,
                    }}
                  />
                </PieChart>
              </ResponsiveContainer>
              <div className="mt-1 space-y-0.5 max-h-36 overflow-y-auto">
                {sectorBreakdown.map((d, i) => (
                  <div key={d.name} className="flex items-center gap-2 text-xs">
                    <div
                      className="w-2 h-2 rounded-full shrink-0"
                      style={{ backgroundColor: PIE_COLORS[i % PIE_COLORS.length] }}
                    />
                    <span className="text-text-secondary truncate">{d.name}</span>
                    <span className="ml-auto font-mono">{d.value}</span>
                  </div>
                ))}
              </div>
              <p className="text-[10px] text-text-secondary mt-2 text-right">
                {sectorBreakdown.length} sectors · {totalEligible} total eligible
              </p>
            </>
          ) : (
            <div className="h-[180px] flex items-center justify-center text-xs text-text-secondary">
              No data
            </div>
          )}
        </div>

        {/* Liquidity Buckets */}
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
          <div className="flex items-center mb-3">
            <h3 className="text-sm font-medium">Liquidity Buckets</h3>
            <InfoTooltip text="A = top 25% by 60-day median turnover (most liquid) · B = 25–50% · C = 50–75% · D = bottom 25%. Percentages are share of symbols that have bucket data assigned." />
          </div>
          <div className="space-y-3">
            {(["A", "B", "C", "D"] as const).map((b) => {
              const count = bucketBreakdown.counts[b] ?? 0;
              const denom = bucketBreakdown.withBucket || 1;
              const pct = Math.round((count / denom) * 100);
              return (
                <div key={b}>
                  <div className="flex justify-between text-xs mb-1">
                    <span className="font-bold" style={{ color: BUCKET_COLOR[b] }}>
                      Bucket {b}
                    </span>
                    <span className="font-mono text-text-secondary">
                      {count} ({pct}%)
                    </span>
                  </div>
                  <div className="h-1.5 bg-bg-tertiary rounded-full overflow-hidden">
                    <div
                      className="h-full rounded-full"
                      style={{ width: `${pct}%`, backgroundColor: BUCKET_COLOR[b] }}
                    />
                  </div>
                </div>
              );
            })}
          </div>
          <div className="mt-4 flex justify-between text-[10px] text-text-secondary">
            <span>With bucket data: {bucketBreakdown.withBucket}</span>
            <span>No bucket: {bucketBreakdown.noBucket}</span>
          </div>
        </div>

        {/* Data Quality */}
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
          <div className="flex items-center mb-3">
            <h3 className="text-sm font-medium">Data Quality</h3>
            <InfoTooltip text="GOOD = data fresh within 1 day. STALE = data older than threshold. MISSING = no price history found. INVALID_KEY = bad or unresolved instrument key. Empty = flag not yet set." />
          </div>
          {dqBreakdown.map(([flag, count]) => (
            <div
              key={flag || "__empty__"}
              className="flex items-center justify-between py-1.5 border-b border-bg-tertiary last:border-0"
            >
              <span
                className="text-xs font-medium"
                style={{ color: DQ_COLOR[flag] ?? "#9ca3af" }}
              >
                {flag || "— (no flag)"}
              </span>
              <span className="font-mono text-xs">{count}</span>
            </div>
          ))}
          <div className="mt-3 flex justify-between text-[10px] text-text-secondary">
            <span>
              Stale avg:{" "}
              {symbols.length > 0
                ? (
                    symbols.reduce((a, s) => a + (s.stale_days ?? 0), 0) / symbols.length
                  ).toFixed(1)
                : "—"}{" "}
              days
            </span>
            <span>Total: {symbols.length}</span>
          </div>
        </div>
      </div>

      {/* ── Advanced Filter Bar ─────────────────────────────────────────── */}
      <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 space-y-3">

        {/* Row 1: Search + active pills + clear all */}
        <div className="flex flex-wrap items-center gap-2">
          <input
            type="text"
            placeholder="Search symbol…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="px-3 py-1.5 bg-bg-tertiary rounded-lg text-xs text-text-primary placeholder:text-text-secondary w-40 focus:outline-none focus:ring-1 focus:ring-accent"
          />
          {sectorFilter && (
            <ActivePill label="Sector" value={sectorFilter} onClear={() => setSectorFilter("")} />
          )}
          {bucketFilter && (
            <ActivePill label="Bucket" value={`Bucket ${bucketFilter}`} onClear={() => setBucketFilter("")} />
          )}
          {dqFilter !== "" && (
            <ActivePill
              label="Quality"
              value={dqFilter || "No flag"}
              onClear={() => setDqFilter("")}
            />
          )}
          {anyFilterActive && (
            <button
              onClick={clearAllFilters}
              className="text-xs text-text-secondary hover:text-text-primary transition-colors underline underline-offset-2 ml-1"
            >
              Clear all
            </button>
          )}
          <span className="text-xs text-text-secondary ml-auto">
            {filtered.length} of {symbols.length} symbols
          </span>
        </div>

        {/* Row 2: Bucket pills */}
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-[11px] text-text-secondary w-14 shrink-0">Bucket</span>
          {(["", "A", "B", "C", "D"] as const).map((b) => {
            const active = bucketFilter === b;
            const count = bucketCounts[b] ?? 0;
            return (
              <button
                key={b || "__all__"}
                onClick={() => setBucketFilter(b)}
                className={cn(
                  "px-2.5 py-0.5 rounded-full text-xs font-medium flex items-center gap-1.5 border transition-colors",
                  active
                    ? "bg-accent text-white border-accent"
                    : "bg-bg-tertiary text-text-secondary hover:text-text-primary border-transparent",
                )}
              >
                <span>{b ? `Bucket ${b}` : "All"}</span>
                <span
                  className={cn(
                    "text-[10px] px-1 py-0 rounded-full",
                    active ? "bg-white/20" : "bg-bg-secondary",
                  )}
                >
                  {count}
                </span>
              </button>
            );
          })}
        </div>

        {/* Row 3: Quality pills — dynamic from data */}
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-[11px] text-text-secondary w-14 shrink-0">Quality</span>
          <button
            onClick={() => setDqFilter("")}
            className={cn(
              "px-2.5 py-0.5 rounded-full text-xs font-medium flex items-center gap-1.5 border transition-colors",
              dqFilter === ""
                ? "bg-accent text-white border-accent"
                : "bg-bg-tertiary text-text-secondary hover:text-text-primary border-transparent",
            )}
          >
            <span>All</span>
            <span className={cn("text-[10px] px-1 rounded-full", dqFilter === "" ? "bg-white/20" : "bg-bg-secondary")}>
              {symbols.length}
            </span>
          </button>
          {dqBreakdown.map(([flag, count]) => {
            const active = dqFilter === flag && dqFilter !== "";
            return (
              <button
                key={flag || "__empty__"}
                onClick={() => setDqFilter(dqFilter === flag ? "" : flag)}
                className={cn(
                  "px-2.5 py-0.5 rounded-full text-xs font-medium flex items-center gap-1.5 border transition-colors",
                  active
                    ? "border-accent"
                    : "bg-bg-tertiary text-text-secondary hover:text-text-primary border-transparent",
                )}
                style={active ? { backgroundColor: (DQ_COLOR[flag] ?? "#9ca3af") + "33", color: DQ_COLOR[flag] ?? "#9ca3af", borderColor: DQ_COLOR[flag] ?? "#9ca3af" } : {}}
              >
                <span>{flag || "No flag"}</span>
                <span
                  className={cn(
                    "text-[10px] px-1 rounded-full",
                    active ? "bg-white/10" : "bg-bg-secondary",
                  )}
                >
                  {count}
                </span>
              </button>
            );
          })}
        </div>

        {/* Row 4: Sector pills */}
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-[11px] text-text-secondary w-14 shrink-0">Sector</span>
          {[{ label: "All", value: "" as string }, ...sectors.map((s) => ({ label: s, value: s }))].map(({ label, value }) => {
            const active = sectorFilter === value;
            const count = value === "" ? symbols.length : (sectorCounts[value] ?? 0);
            return (
              <button
                key={value || "__all__"}
                onClick={() => setSectorFilter(active && value !== "" ? "" : value)}
                className={cn(
                  "flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[11px] font-medium border transition-all",
                  active
                    ? "bg-accent text-white border-accent"
                    : "bg-bg-tertiary text-text-secondary border-transparent hover:text-text-primary hover:border-border",
                )}
              >
                <span>{label}</span>
                <span
                  className={cn(
                    "text-[10px] px-1 rounded-full",
                    active ? "bg-white/10" : "bg-bg-secondary",
                  )}
                >
                  {count}
                </span>
              </button>
            );
          })}
        </div>
      </div>

      {/* ── Table ───────────────────────────────────────────────────────── */}
      <div className="space-y-3">
        <div className="flex gap-1 items-center">
          <button className={tabClass("all")} onClick={() => setTab("all")}>
            All ({symbols.length})
          </button>
          <button className={tabClass("eligible")} onClick={() => setTab("eligible")}>
            Eligible ({symbols.filter((s) => s.eligible_swing || s.eligible_intraday).length})
          </button>
          <button className={tabClass("excluded")} onClick={() => setTab("excluded")}>
            Excluded ({symbols.filter((s) => !s.eligible_swing && !s.eligible_intraday).length})
          </button>
        </div>
        <DataTable
          columns={columns}
          data={filtered}
          emptyMessage="No symbols match filters"
        />
      </div>
    </div>
  );
}

// ── StatCard ─────────────────────────────────────────────────────────────────

function StatCard({
  label,
  value,
  color,
  tooltip,
}: {
  label: string;
  value: number;
  color?: string;
  tooltip: string;
}) {
  return (
    <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 text-center">
      <p className="text-3xl font-mono font-bold" style={{ color }}>
        {value}
      </p>
      <div className="flex items-center justify-center mt-1">
        <p className="text-xs text-text-secondary">{label}</p>
        <InfoTooltip text={tooltip} />
      </div>
    </div>
  );
}
