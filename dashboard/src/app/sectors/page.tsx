"use client";

import { useEffect, useState, useMemo } from "react";
import { api } from "@/lib/api";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { DataTable, type Column } from "@/components/shared/DataTable";
import { cn } from "@/lib/utils";
import { X, LayoutGrid, Building2, CheckCircle, IndianRupee } from "lucide-react";
import {
  BarChart,
  Bar,
  Cell,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { InfoBadge } from "@/components/shared/Tooltip";

function InfoTooltip({ text }: { text: string }) {
  return <InfoBadge text={text} />;
}

// ── Types ─────────────────────────────────────────────────────────────────────

interface SectorRow {
  sector: string;
  macro_sector: string;
  total: number;
  eligible_swing: number;
  eligible_intraday: number;
  both: number;
  neither: number;
  eligible_pct: number;
  avg_beta: number | null;
  avg_atr_pct: number | null;
  total_turnover: number;
  bucket_a: number;
  bucket_b: number;
  bucket_c: number;
  bucket_d: number;
  liq_score: number;
  avg_gap_risk: number | null;
  dq_good: number;
  dq_stale: number;
  dq_missing: number;
  dq_other: number;
  dq_score: number;
  industries: string[];
}

interface SectorSymbol {
  symbol: string;
  industry: string;
  basic_industry: string;
  eligible_swing: boolean;
  eligible_intraday: boolean;
  price_last: number | null;
  turnover_med_60d: number | null;
  atr_pct_14d: number | null;
  liquidity_bucket: string;
  beta: number | null;
  gap_risk_60d: number | null;
  data_quality_flag: string;
  disable_reason: string;
}

interface SectorDetail {
  sector: string;
  total: number;
  symbols: SectorSymbol[];
  industries: [string, number][];
}

// ── Helpers ──────────────────────────────────────────────────────────────────

const BUCKET_COLOR: Record<string, string> = {
  A: "#4ade80",
  B: "#60a5fa",
  C: "#fbbf24",
  D: "#f87171",
};

function fmtTurnover(v: number): string {
  const cr = v / 1e7;
  if (cr >= 10000) return `₹${(cr / 1000).toFixed(0)}kCr`;
  if (cr >= 1000) return `₹${(cr / 1000).toFixed(1)}kCr`;
  if (cr >= 100) return `₹${Math.round(cr)}Cr`;
  return `₹${cr.toFixed(1)}Cr`;
}

// eligible% → color
function eligibleColor(pct: number): string {
  if (pct >= 70) return "#22c55e";
  if (pct >= 40) return "#3b82f6";
  return "#f59e0b";
}

// ── LiqBar ────────────────────────────────────────────────────────────────────

function LiqBar({ row }: { row: SectorRow }) {
  const total = row.bucket_a + row.bucket_b + row.bucket_c + row.bucket_d;
  if (total === 0) return <span className="text-text-secondary text-xs">—</span>;
  return (
    <div className="flex h-1.5 rounded-full overflow-hidden w-16 gap-px">
      {(["a", "b", "c", "d"] as const).map((b) => {
        const count = row[`bucket_${b}` as keyof SectorRow] as number;
        const pct = (count / total) * 100;
        if (pct === 0) return null;
        return (
          <div
            key={b}
            className="h-full"
            style={{ width: `${pct}%`, backgroundColor: BUCKET_COLOR[b.toUpperCase()], opacity: 0.7 }}
            title={`${b.toUpperCase()}: ${count}`}
          />
        );
      })}
    </div>
  );
}

// ── Sector Card ───────────────────────────────────────────────────────────────

function SectorCard({ row, onClick }: { row: SectorRow; onClick: () => void }) {
  const eligible = row.eligible_swing + row.eligible_intraday - (row.both ?? 0);
  const borderColor = eligibleColor(row.eligible_pct);

  return (
    <button
      onClick={onClick}
      className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 text-left hover:border-accent/30 hover:shadow-lg transition-all group w-full"
      style={{ borderTop: `3px solid ${borderColor}` }}
    >
      {/* Header */}
      <div className="mb-3">
        <p className="text-sm font-medium text-text-primary leading-tight">{row.sector}</p>
        <p className="text-[10px] text-text-secondary mt-0.5 uppercase tracking-wide">{row.macro_sector}</p>
      </div>

      {/* Eligible bar */}
      <div className="flex items-center gap-2 mb-3">
        <div className="flex-1 h-1.5 bg-bg-tertiary rounded-full overflow-hidden">
          <div
            className="h-full rounded-full"
            style={{
              width: `${row.eligible_pct}%`,
              background: `linear-gradient(90deg, ${borderColor}99, ${borderColor})`,
            }}
          />
        </div>
        <span className="text-[10px] font-mono shrink-0" style={{ color: borderColor }}>
          {row.eligible_pct}%
        </span>
      </div>

      {/* Metrics */}
      <div className="grid grid-cols-2 gap-x-4 gap-y-2">
        <Metric label="Symbols" value={String(row.total)} />
        <Metric label="Eligible" value={`${eligible} / ${row.total}`} />
        <Metric
          label="β avg"
          value={row.avg_beta?.toFixed(2) ?? "—"}
          dim={row.avg_beta == null}
        />
        <Metric
          label="ATR%"
          value={row.avg_atr_pct ? `${(row.avg_atr_pct * 100).toFixed(1)}%` : "—"}
          dim={row.avg_atr_pct == null}
          warn={row.avg_atr_pct != null && row.avg_atr_pct > 0.08}
        />
        <Metric
          label="Turnover"
          value={fmtTurnover(row.total_turnover)}
          chip
          chipColor={borderColor}
        />
        <Metric label="Industries" value={String(row.industries.length)} dim />
      </div>

      {/* Footer */}
      <div className="mt-3 pt-3 border-t border-bg-tertiary">
        <LiqBar row={row} />
      </div>
    </button>
  );
}

function Metric({
  label,
  value,
  dim,
  warn,
  chip,
  chipColor,
}: {
  label: string;
  value: string;
  dim?: boolean;
  warn?: boolean;
  chip?: boolean;
  chipColor?: string;
}) {
  return (
    <div>
      <p className="text-[9px] text-text-secondary uppercase tracking-wide leading-none mb-0.5">{label}</p>
      {chip && chipColor ? (
        <p
          className="text-xs font-mono px-1.5 py-0.5 rounded-md inline-block"
          style={{ color: chipColor, background: `${chipColor}18` }}
        >
          {value}
        </p>
      ) : (
        <p className={cn("text-xs font-mono", warn ? "text-amber-400" : dim ? "text-text-secondary" : "text-text-primary")}>
          {value}
        </p>
      )}
    </div>
  );
}

// ── Sector Detail Drawer ──────────────────────────────────────────────────────

function SectorDetailDrawer({ sector, onClose }: { sector: string; onClose: () => void }) {
  const [detail, setDetail] = useState<SectorDetail | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    api
      .getSectorDetail(sector)
      .then((d: any) => setDetail(d as SectorDetail))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [sector]);

  const columns: Column<SectorSymbol>[] = useMemo(
    () => [
      {
        key: "symbol",
        label: "Symbol",
        sortable: true,
        sortValue: (r) => r.symbol,
        render: (r) => <span className="font-medium text-xs">{r.symbol}</span>,
      },
      {
        key: "industry",
        label: "Industry",
        render: (r) => <span className="text-[10px] text-text-secondary">{r.industry || "—"}</span>,
      },
      {
        key: "price",
        label: "Price",
        sortable: true,
        sortValue: (r) => r.price_last ?? 0,
        className: "text-right font-mono text-xs",
        render: (r) =>
          r.price_last ? (
            <span>₹{r.price_last.toLocaleString("en-IN", { maximumFractionDigits: 1 })}</span>
          ) : (
            <span className="text-text-secondary">—</span>
          ),
      },
      {
        key: "bucket",
        label: "Liq",
        className: "text-center",
        render: (r) =>
          r.liquidity_bucket ? (
            <span className="text-xs font-medium" style={{ color: BUCKET_COLOR[r.liquidity_bucket] ?? "#9ca3af" }}>
              {r.liquidity_bucket}
            </span>
          ) : (
            <span className="text-text-secondary text-xs">—</span>
          ),
      },
      {
        key: "atr",
        label: "ATR%",
        sortable: true,
        sortValue: (r) => r.atr_pct_14d ?? 0,
        className: "text-right font-mono text-xs",
        render: (r) => {
          const v = r.atr_pct_14d;
          if (!v) return <span className="text-text-secondary">—</span>;
          return <span className={v > 0.08 ? "text-amber-400" : ""}>{(v * 100).toFixed(1)}%</span>;
        },
      },
      {
        key: "swing",
        label: "Sw",
        render: (r) => (
          <span className={r.eligible_swing ? "text-profit text-xs" : "text-text-secondary text-xs"}>
            {r.eligible_swing ? "✓" : "✗"}
          </span>
        ),
      },
      {
        key: "intra",
        label: "In",
        render: (r) => (
          <span className={r.eligible_intraday ? "text-profit text-xs" : "text-text-secondary text-xs"}>
            {r.eligible_intraday ? "✓" : "✗"}
          </span>
        ),
      },
    ],
    [],
  );

  return (
    <>
      <div className="fixed inset-0 bg-black/50 z-40" onClick={onClose} />
      <div className="fixed right-0 top-0 bottom-0 w-full sm:w-[680px] bg-bg-secondary border-l border-bg-tertiary z-50 overflow-y-auto scrollbar-thin">
        <div className="p-5 space-y-5">
          <div className="flex items-start justify-between">
            <div className="flex gap-3 items-start">
              {/* gradient left-border strip */}
              <div
                className="w-1 self-stretch rounded-full shrink-0"
                style={{ background: "linear-gradient(180deg, #3b82f6, #22c55e)" }}
              />
              <div>
                <h2 className="text-base font-semibold">{sector}</h2>
                {detail && (
                  <p className="text-xs text-text-secondary mt-0.5">
                    {detail.total} symbols · {detail.industries.length} industries
                  </p>
                )}
              </div>
            </div>
            <button onClick={onClose} className="p-1.5 rounded hover:bg-bg-tertiary transition-colors">
              <X className="h-4 w-4" />
            </button>
          </div>

          {loading ? (
            <LoadingSkeleton lines={6} />
          ) : detail ? (
            <>
              {detail.industries.length > 0 && (
                <div className="bg-bg-primary rounded-lg border border-bg-tertiary p-4">
                  <p className="text-[10px] text-text-secondary uppercase tracking-wide mb-3">Industry Breakdown</p>
                  <ResponsiveContainer width="100%" height={Math.max(100, detail.industries.length * 22)}>
                    <BarChart
                      data={detail.industries.map(([name, count]) => ({ name, count }))}
                      layout="vertical"
                      margin={{ top: 0, right: 32, left: 0, bottom: 0 }}
                    >
                      <XAxis type="number" tick={{ fill: "#4b5563", fontSize: 9 }} tickLine={false} axisLine={false} />
                      <YAxis
                        type="category"
                        dataKey="name"
                        tick={{ fill: "#6b7280", fontSize: 9 }}
                        tickLine={false}
                        axisLine={false}
                        width={150}
                      />
                      <Tooltip
                        contentStyle={{ backgroundColor: "#0d1117", border: "1px solid #1e293b", fontSize: 10 }}
                      />
                      <Bar dataKey="count" radius={[0, 4, 4, 0]} maxBarSize={14}>
                        {detail.industries.map((_, i) => (
                          <Cell
                            key={i}
                            fill={i === 0 ? "#3b82f6" : "#3b82f6"}
                            fillOpacity={0.4 + (i === 0 ? 0.4 : Math.max(0, 0.3 - i * 0.03))}
                          />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              )}
              <div>
                <p className="text-[10px] text-text-secondary uppercase tracking-wide mb-2">Symbols — by turnover</p>
                <DataTable columns={columns} data={detail.symbols} emptyMessage="No symbols" />
              </div>
            </>
          ) : (
            <p className="text-xs text-text-secondary">Failed to load.</p>
          )}
        </div>
      </div>
    </>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────

export default function SectorsPage() {
  const [sectors, setSectors] = useState<SectorRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [sortBy, setSortBy] = useState<"turnover" | "total" | "eligible_pct" | "liq_score">("turnover");
  const [view, setView] = useState<"cards" | "table">("table");
  const [drawerSector, setDrawerSector] = useState<string | null>(null);
  const [search, setSearch] = useState("");

  useEffect(() => {
    api
      .getSectorsSummary()
      .then((d: any) => setSectors(d.sectors ?? []))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const filtered = useMemo(() => {
    let data = sectors;
    if (search) {
      const q = search.toLowerCase();
      data = data.filter((s) => s.sector.toLowerCase().includes(q));
    }
    return [...data].sort((a, b) => {
      if (sortBy === "turnover") return b.total_turnover - a.total_turnover;
      if (sortBy === "total") return b.total - a.total;
      if (sortBy === "eligible_pct") return b.eligible_pct - a.eligible_pct;
      if (sortBy === "liq_score") return b.liq_score - a.liq_score;
      return 0;
    });
  }, [sectors, sortBy, search]);

  const totals = useMemo(
    () => ({
      sectors: sectors.length,
      symbols: sectors.reduce((a, s) => a + s.total, 0),
      eligible: sectors.reduce((a, s) => a + Math.max(s.eligible_swing, s.eligible_intraday), 0),
      turnover: sectors.reduce((a, s) => a + s.total_turnover, 0),
    }),
    [sectors],
  );

  const tableColumns: Column<SectorRow>[] = useMemo(
    () => [
      {
        key: "sector",
        label: "Sector",
        sortable: true,
        sortValue: (r) => r.sector,
        render: (r) => (
          <div>
            <button
              onClick={() => setDrawerSector(r.sector)}
              className="text-sm font-medium hover:text-accent transition-colors text-left"
            >
              {r.sector}
            </button>
            <p className="text-[10px] text-text-secondary">{r.macro_sector}</p>
          </div>
        ),
      },
      {
        key: "total",
        label: "Symbols",
        tooltip: "Total symbols in this sector in the Firestore universe. Includes both eligible and excluded symbols.",
        sortable: true,
        sortValue: (r) => r.total,
        className: "text-right font-mono text-xs",
        render: (r) => <span>{r.total}</span>,
      },
      {
        key: "eligible_pct",
        label: "Eligible%",
        tooltip: "% eligible = max(eligible_swing, eligible_intraday) / total × 100. A symbol is eligible if its swing or intraday flag is set, or allowed_product ∈ {BOTH, SWING, INTRADAY}.",
        sortable: true,
        sortValue: (r) => r.eligible_pct,
        render: (r) => {
          const color = eligibleColor(r.eligible_pct);
          return (
            <div className="flex items-center gap-2">
              <div className="w-16 h-1.5 bg-bg-tertiary rounded-full overflow-hidden">
                <div
                  className="h-full rounded-full"
                  style={{
                    width: `${r.eligible_pct}%`,
                    background: `linear-gradient(90deg, ${color}80, ${color})`,
                  }}
                />
              </div>
              <span className="font-mono text-xs" style={{ color }}>{r.eligible_pct}%</span>
            </div>
          );
        },
      },
      {
        key: "avg_beta",
        label: "β avg",
        tooltip: "β = cov(stock_returns, nifty_returns) / var(nifty_returns) over 90 trading days. β>1 = amplifies index moves; β<1 = dampens them. β=1.0 means pipeline has not yet run.",
        sortable: true,
        sortValue: (r) => r.avg_beta ?? 0,
        className: "text-right font-mono text-xs text-text-secondary",
        render: (r) => <span>{r.avg_beta?.toFixed(2) ?? "—"}</span>,
      },
      {
        key: "avg_atr",
        label: "ATR%",
        tooltip: "ATR% = ATR(14) / last_price. ATR(14) uses Wilder's smoothing: avg of max(H-L, |H-prevC|, |L-prevC|) over 14 days. Avg across sector symbols. >8% = high volatility (amber).",
        sortable: true,
        sortValue: (r) => r.avg_atr_pct ?? 0,
        className: "text-right font-mono text-xs",
        render: (r) => {
          const v = r.avg_atr_pct;
          if (!v) return <span className="text-text-secondary">—</span>;
          return <span className={v > 0.08 ? "text-amber-400" : "text-text-secondary"}>{(v * 100).toFixed(1)}%</span>;
        },
      },
      {
        key: "turnover",
        label: "Turnover",
        tooltip: "Σ median(close × volume, 60d) across all symbols. Each symbol's turnover = daily close × volume. Median of last 60 days taken per symbol, then summed for the sector.",
        sortable: true,
        sortValue: (r) => r.total_turnover,
        className: "text-right font-mono text-xs",
        render: (r) => (
          <span className="px-1.5 py-0.5 rounded text-xs bg-accent/10 text-accent font-mono">
            {fmtTurnover(r.total_turnover)}
          </span>
        ),
      },
      {
        key: "liq",
        label: "Liquidity",
        tooltip: "score = (A×4 + B×3 + C×2 + D×1) / n, where A/B/C/D are symbols in each turnover quartile. Buckets assigned cross-sectionally: A=top 25%, B=25-50%, C=50-75%, D=bottom 25%. Max=4.0.",
        sortable: true,
        sortValue: (r) => r.liq_score,
        render: (r) => (
          <div className="flex items-center gap-2">
            <LiqBar row={r} />
            <span className="text-[10px] text-text-secondary">{r.liq_score.toFixed(1)}</span>
          </div>
        ),
      },
      {
        key: "industries",
        label: "Industries",
        tooltip: "Count of distinct industries (sub-sector groupings) within this sector, sourced from sector_mapping collection.",
        className: "text-right text-xs text-text-secondary",
        render: (r) => <span>{r.industries.length}</span>,
      },
    ],
    [],
  );

  if (loading) return <LoadingSkeleton lines={12} />;

  const viewTabClass = (v: "table" | "cards") =>
    cn(
      "px-4 py-1.5 rounded-lg text-xs font-medium transition-all",
      view === v ? "bg-accent text-white shadow-sm" : "text-text-secondary hover:text-text-primary hover:bg-bg-tertiary",
    );

  // Stats card configs
  const statCards = [
    {
      label: "Sectors",
      value: totals.sectors.toString(),
      icon: <LayoutGrid className="h-4 w-4" />,
      iconBg: "bg-slate-500/10",
      iconColor: "text-slate-400",
      borderColor: "border-t-2 border-slate-500",
      tip: "Total distinct sectors derived from the universe sector field.",
    },
    {
      label: "Symbols",
      value: totals.symbols.toLocaleString(),
      icon: <Building2 className="h-4 w-4" />,
      iconBg: "bg-indigo-500/10",
      iconColor: "text-indigo-400",
      borderColor: "border-t-2 border-indigo-500",
      tip: "Total symbols across all sectors in the Firestore universe.",
    },
    {
      label: "Eligible",
      value: totals.eligible.toLocaleString(),
      icon: <CheckCircle className="h-4 w-4" />,
      iconBg: "bg-profit/10",
      iconColor: "text-profit",
      borderColor: "border-t-2 border-profit",
      valueColor: "text-profit",
      tip: "Symbols eligible for swing or intraday trading (max of both).",
    },
    {
      label: "Daily Turnover",
      value: fmtTurnover(totals.turnover),
      sub: "sum of 60d medians",
      icon: <IndianRupee className="h-4 w-4" />,
      iconBg: "bg-accent/10",
      iconColor: "text-accent",
      borderColor: "border-t-2 border-accent",
      tip: "Sum of each symbol's 60-day median daily turnover (close × volume). Represents total daily liquidity across all sectors.",
    },
  ];

  return (
    <div className="space-y-5">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold">Sectors</h1>
        <div className="flex gap-1 bg-bg-tertiary/40 rounded-xl p-1">
          <button onClick={() => setView("table")} className={viewTabClass("table")}>
            Table
          </button>
          <button onClick={() => setView("cards")} className={viewTabClass("cards")}>
            Cards
          </button>
        </div>
      </div>

      {/* Summary row */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {statCards.map(({ label, value, sub, icon, iconBg, iconColor, borderColor, valueColor, tip }) => (
          <div
            key={label}
            className={cn("bg-bg-secondary rounded-lg border border-bg-tertiary p-4 shadow-md", borderColor)}
          >
            <div className="flex items-start justify-between">
              <div className={cn("p-2 rounded-lg", iconBg)}>
                <span className={iconColor}>{icon}</span>
              </div>
            </div>
            <p className={cn("text-xl font-mono font-bold mt-2", valueColor ?? "text-text-primary")}>{value}</p>
            <div className="flex items-center mt-1">
              <p className="text-xs text-text-secondary">{label}</p>
              <InfoTooltip text={tip} />
            </div>
            {sub && <p className="text-[9px] text-text-secondary mt-0.5">{sub}</p>}
          </div>
        ))}
      </div>

      {/* Filters */}
      <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
        <div className="flex flex-wrap items-center gap-3">
          <input
            type="text"
            placeholder="Search…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="px-2.5 py-1.5 bg-bg-tertiary rounded text-xs text-text-primary placeholder:text-text-secondary w-32 focus:outline-none focus:ring-1 focus:ring-accent/50"
          />
          <div className="flex items-center gap-1">
            <span className="text-[10px] text-text-secondary">Sort:</span>
            {(
              [
                ["turnover", "Turnover"],
                ["total", "Symbols"],
                ["eligible_pct", "Eligible%"],
                ["liq_score", "Liquidity"],
              ] as const
            ).map(([key, label]) => (
              <button
                key={key}
                onClick={() => setSortBy(key)}
                className={cn(
                  "px-2 py-0.5 rounded text-xs transition-colors",
                  sortBy === key ? "text-accent" : "text-text-secondary hover:text-text-primary",
                )}
              >
                {label}
              </button>
            ))}
          </div>
          <span className="ml-auto text-xs text-text-secondary">
            {filtered.length} sector{filtered.length !== 1 ? "s" : ""}
          </span>
        </div>
      </div>

      {/* Content */}
      {view === "cards" ? (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-3">
          {filtered.map((row) => (
            <SectorCard key={row.sector} row={row} onClick={() => setDrawerSector(row.sector)} />
          ))}
        </div>
      ) : (
        <DataTable
          columns={tableColumns}
          data={filtered}
          onRowClick={(r) => setDrawerSector(r.sector)}
          emptyMessage="No sectors"
          rowClassName={() => "hover:bg-accent/5"}
        />
      )}

      {drawerSector && (
        <SectorDetailDrawer sector={drawerSector} onClose={() => setDrawerSector(null)} />
      )}
    </div>
  );
}
