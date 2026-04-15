"use client";

import { useEffect, useState, useMemo } from "react";
import { useDashboardStore } from "@/stores/dashboardStore";
import { useAuthStore } from "@/stores/authStore";
import { RegimeBadge } from "@/components/widgets/RegimeBadge";
import { RiskModeBadge } from "@/components/widgets/RiskModeBadge";
import { PnLCard } from "@/components/widgets/PnLCard";
import { ConfidenceGauge } from "@/components/widgets/ConfidenceGauge";
import { LiveDot } from "@/components/shared/LiveDot";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { EquityCurve } from "@/components/charts/EquityCurve";
import { isMarketOpen, formatTime, formatCurrency } from "@/lib/utils";
import { api } from "@/lib/api";
import type { Regime, RiskMode, TradeSummary } from "@/lib/types";
import { useRouter } from "next/navigation";
import Link from "next/link";
import {
  ChevronRight,
  TrendingUp,
  Briefcase,
  List,
  Brain,
  Sunrise,
  Sun,
  Moon,
} from "lucide-react";
import {
  PieChart,
  Pie,
  Cell,
  ResponsiveContainer,
} from "recharts";
import { REGIME_COLORS, PARTICIPATION_COLORS } from "@/lib/constants";
import type { Participation } from "@/lib/types";

const NEXT_JOBS = [
  { name: "Universe Refresh", cron: "06:15" },
  { name: "Score Cache", cron: "07:05" },
  { name: "Score Cache 2", cron: "07:40" },
  { name: "Score Refresh", cron: "08:30" },
  { name: "Premarket Watchlist", cron: "09:00" },
  { name: "Swing Recon", cron: "09:00" },
  { name: "Scanner Start", cron: "09:20" },
  { name: "Watchlist 09:30", cron: "09:30" },
  { name: "Watchlist 10:00", cron: "10:00" },
  { name: "Watchlist 10:45", cron: "10:45" },
  { name: "Watchlist 11:00", cron: "11:00" },
  { name: "Watchlist 13:00", cron: "13:00" },
  { name: "Final Watchlist", cron: "14:45" },
  { name: "EOD Recon", cron: "15:10" },
];

/** Resolve regime left-border color */
function regimeBorderColor(regime: string): string {
  return (REGIME_COLORS as Record<string, string>)[regime] ?? "#6b7280";
}

/** Regime subtle tint rgba */
function regimeTint(regime: string): string {
  const hex = regimeBorderColor(regime);
  // Convert 6-char hex to rgba with low opacity
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  return `rgba(${r},${g},${b},0.04)`;
}

/** Phase icon */
function PhaseIcon({ phase }: { phase: string }) {
  const p = phase?.toUpperCase();
  if (p === "PREMARKET") return <Sunrise className="h-3.5 w-3.5 text-amber-400" />;
  if (p === "LIVE") return <Sun className="h-3.5 w-3.5 text-yellow-400" />;
  if (p === "EOD") return <Moon className="h-3.5 w-3.5 text-indigo-400" />;
  return null;
}

export default function CommandCenter() {
  const router = useRouter();
  const user = useAuthStore((s) => s.user);
  const loading = useAuthStore((s) => s.loading);
  const brain = useDashboardStore((s) => s.marketBrain);
  const watchlist = useDashboardStore((s) => s.watchlist);
  const positions = useDashboardStore((s) => s.positions);
  const ltpCache = useDashboardStore((s) => s.ltpCache);

  const [summary, setSummary] = useState<TradeSummary | null>(null);
  const [equityData, setEquityData] = useState<{ date: string; pnl: number }[]>([]);

  useEffect(() => {
    if (!loading && !user) router.push("/login");
  }, [loading, user, router]);

  useEffect(() => {
    if (!user) return;
    api.getTradeSummary().then((d) => setSummary(d as unknown as TradeSummary)).catch(() => {});
    api.getEquityCurve().then((d: any) => setEquityData(d.series ?? [])).catch(() => {});
  }, [user]);

  const openPositions = useMemo(
    () => positions.filter((p) => p.status === "OPEN" || p.status === "PENDING_AMO_EXIT"),
    [positions],
  );

  const unrealizedPnl = useMemo(() => {
    return openPositions.reduce((sum, p) => {
      const ltp = ltpCache[p.symbol];
      if (!ltp || p.entry_price == null) return sum;
      return sum + (p.side === "BUY"
        ? (ltp - p.entry_price) * p.qty
        : (p.entry_price - ltp) * p.qty);
    }, 0);
  }, [openPositions, ltpCache]);

  const ltpAvailable = useMemo(() =>
    openPositions.some((p) => !!ltpCache[p.symbol]),
  [openPositions, ltpCache]);

  const [nowMins, setNowMins] = useState(() => {
    const ist = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));
    return ist.getHours() * 60 + ist.getMinutes();
  });

  // Update "next job" every minute
  useEffect(() => {
    const id = setInterval(() => {
      const ist = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));
      setNowMins(ist.getHours() * 60 + ist.getMinutes());
    }, 60_000);
    return () => clearInterval(id);
  }, []);

  const nextJob = useMemo(() => {
    for (const j of NEXT_JOBS) {
      const [h, m] = j.cron.split(":").map(Number);
      if (h * 60 + m > nowMins) return j;
    }
    return NEXT_JOBS[0];
  }, [nowMins]);

  // Countdown to next job
  const nextJobCountdown = useMemo(() => {
    const [h, m] = nextJob.cron.split(":").map(Number);
    const targetMins = h * 60 + m;
    let diff = targetMins - nowMins;
    if (diff < 0) diff += 24 * 60;
    const hh = Math.floor(diff / 60);
    const mm = diff % 60;
    if (hh > 0) return `${hh}h ${mm}m`;
    return `${mm}m`;
  }, [nextJob, nowMins]);

  const swingCount = watchlist.filter((r) => r.eligible_swing).length;
  const intradayCount = watchlist.filter((r) => r.eligible_intraday).length;
  const swingPositions = openPositions.filter((p) => p.wl_type === "swing").length;
  const intradayPositions = openPositions.filter((p) => p.wl_type !== "swing").length;

  const watchlistTotal = swingCount + intradayCount;
  const swingPct = watchlistTotal > 0 ? Math.round((swingCount / watchlistTotal) * 100) : 0;
  const intradayPct = watchlistTotal > 0 ? Math.round((intradayCount / watchlistTotal) * 100) : 0;

  const watchlistPieData = useMemo(() => {
    if (swingCount === 0 && intradayCount === 0) return [];
    return [
      ...(swingCount > 0 ? [{ name: "Swing", value: swingCount }] : []),
      ...(intradayCount > 0 ? [{ name: "Intraday", value: intradayCount }] : []),
    ];
  }, [swingCount, intradayCount]);

  if (loading) return <LoadingSkeleton lines={8} className="max-w-3xl" />;
  if (!user) return null;

  const marketOpen = isMarketOpen();
  const todayPnl = (summary?.total_pnl ?? 0) + unrealizedPnl;

  const regimeColor = brain ? regimeBorderColor(brain.regime) : "#6b7280";
  const regimeTintColor = brain ? regimeTint(brain.regime) : "transparent";

  // Participation badge color
  const participationColor = brain
    ? ((PARTICIPATION_COLORS as Record<string, string>)[brain.participation] ?? "#6b7280")
    : "#6b7280";

  // Confidence gauge bar color
  const confidenceVal = brain?.market_confidence ?? 0;
  const confidenceBarColor =
    confidenceVal >= 70 ? "#22c55e" : confidenceVal >= 40 ? "#f59e0b" : "#ef4444";

  // Long/short bias bar widths (clamped 0-1)
  const longBias = Math.min(1, Math.max(0, brain?.long_bias ?? 0));
  const shortBias = Math.min(1, Math.max(0, brain?.short_bias ?? 0));
  const biasTotal = longBias + shortBias || 1;
  const longPct = (longBias / biasTotal) * 100;
  const shortPct = (shortBias / biasTotal) * 100;

  // Size/MaxPos multiplier visual (0–2 range mapped to 0–100%)
  const sizeMult = brain?.size_multiplier ?? 1;
  const maxPosMult = brain?.max_positions_multiplier ?? 1;
  const sizeMultPct = Math.min(100, (sizeMult / 2) * 100);
  const maxPosMultPct = Math.min(100, (maxPosMult / 2) * 100);

  // Quick stats glow
  const winRate = summary?.win_rate ?? 0;
  const avgRR = summary?.avg_rr ?? 0;

  return (
    <div className="space-y-5">
      {/* Regime Banner */}
      {brain && (
        <div
          className="rounded-lg border border-bg-tertiary p-4 space-y-2"
          style={{
            background: `linear-gradient(135deg, ${regimeTintColor} 0%, #111827 100%)`,
            borderLeft: `3px solid ${regimeColor}`,
          }}
        >
          <div className="flex flex-wrap items-center gap-4">
            <RegimeBadge regime={brain.regime as Regime} size="lg" />
            <RiskModeBadge mode={brain.risk_mode as RiskMode} />

            {/* Participation colored pill */}
            <span
              className="px-2 py-0.5 rounded-full text-xs font-semibold text-white"
              style={{ backgroundColor: participationColor }}
            >
              {brain.participation}
            </span>

            {/* Phase with icon */}
            <span className="flex items-center gap-1 text-sm text-text-secondary">
              <PhaseIcon phase={brain.phase} />
              <span>Phase: <strong className="text-text-primary">{brain.phase}</strong></span>
            </span>

            <div className="ml-auto flex items-center gap-2">
              <LiveDot status={marketOpen ? "online" : "offline"} />
              <span className="text-xs text-text-secondary">
                {marketOpen ? "Market Open" : "Market Closed"}
              </span>
            </div>
          </div>
          {brain.allowed_strategies && brain.allowed_strategies.length > 0 && (
            <div className="flex flex-wrap items-center gap-1.5 pt-1 border-t border-bg-tertiary">
              <span className="text-[10px] text-text-secondary mr-1">Strategies:</span>
              {brain.allowed_strategies.map((s) => (
                <span key={s} className="px-1.5 py-0.5 bg-bg-tertiary rounded text-[10px] text-text-secondary">
                  {s}
                </span>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Stats Grid */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {/* P&L Card — uses PnLCard, add shadow + dynamic border via wrapper */}
        <div
          className="rounded-lg shadow-lg"
          style={{ borderTop: `3px solid ${todayPnl >= 0 ? "#22c55e" : "#ef4444"}` }}
        >
          <PnLCard
            label="Today's P&L"
            value={todayPnl}
            subLabel={openPositions.length > 0 && !ltpAvailable ? "⚠ LTP unavailable" : "Realized"}
            subValue={openPositions.length > 0 && !ltpAvailable ? "" : formatCurrency(summary?.total_pnl ?? 0)}
          />
        </div>

        {/* Positions card */}
        <Link
          href="/positions"
          className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 hover:border-accent/50 transition-colors relative"
          style={{ borderTop: "3px solid #06b6d4" }}
        >
          <div className="absolute top-3 right-3 text-cyan-400/40">
            <Briefcase className="h-5 w-5" />
          </div>
          <p className="text-xs text-text-secondary mb-1">Active Positions</p>
          <p className="text-2xl font-mono font-bold text-text-primary">
            {openPositions.length}
          </p>
          {openPositions.length > 0 ? (
            <>
              <p className="text-xs text-text-secondary mt-1">
                <span className="text-cyan-400">{intradayPositions} intraday</span>
                {" · "}
                <span className="text-indigo-400">{swingPositions} swing</span>
              </p>
              {ltpAvailable && (
                <p
                  className={`text-xs font-mono mt-0.5 ${unrealizedPnl >= 0 ? "text-profit" : "text-loss"}`}
                >
                  Unrlzd: {formatCurrency(unrealizedPnl)}
                </p>
              )}
            </>
          ) : (
            <p className="text-xs text-text-secondary mt-1">No open positions</p>
          )}
        </Link>

        {/* Watchlist card */}
        <Link
          href="/watchlist"
          className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 hover:border-accent/50 transition-colors relative"
          style={{ borderTop: "3px solid #6366f1" }}
        >
          <div className="absolute top-3 right-3 text-indigo-400/40">
            <List className="h-5 w-5" />
          </div>
          <p className="text-xs text-text-secondary mb-1">Watchlist</p>
          <p className="text-2xl font-mono font-bold text-text-primary">
            {watchlist.length}
          </p>
          <p className="text-xs text-text-secondary mt-1">
            {swingCount} swing &middot; {intradayCount} intraday
          </p>
        </Link>

        {/* Market Confidence card */}
        <Link
          href="/market-brain"
          className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 hover:border-accent/50 transition-colors relative"
          style={{ borderTop: "3px solid #3b82f6" }}
        >
          <div className="absolute top-3 right-3 text-accent/40">
            <Brain className="h-5 w-5" />
          </div>
          <p className="text-xs text-text-secondary mb-1">Market Confidence</p>
          <p className="text-2xl font-mono font-bold text-accent">
            {brain?.market_confidence?.toFixed(0) ?? "--"}
          </p>
          <p className="text-xs text-text-secondary mt-1">
            Swing: {brain?.swing_permission ?? "--"}
          </p>
          {/* Mini gauge bar */}
          <div className="mt-2 h-1 rounded-full bg-bg-tertiary overflow-hidden">
            <div
              className="h-full rounded-full transition-all"
              style={{ width: `${confidenceVal}%`, backgroundColor: confidenceBarColor }}
            />
          </div>
        </Link>
      </div>

      {/* Equity Curve */}
      <div
        className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4"
        style={{ background: "linear-gradient(135deg, rgba(59,130,246,0.05) 0%, #111827 60%)" }}
      >
        <div className="flex items-center gap-2 mb-2">
          <h3 className="text-sm font-medium">Equity Curve</h3>
          <span className="px-1.5 py-0.5 bg-accent/10 text-accent text-[10px] rounded font-medium">30d</span>
        </div>
        {equityData.length > 0 ? (
          <EquityCurve data={equityData} height={180} />
        ) : (
          <div className="h-[180px] flex items-center justify-center text-sm text-text-secondary">
            No trades yet — curve will appear after first closed position
          </div>
        )}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* Confidence Gauges */}
        {brain && (
          <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-medium">Confidence Meters</h3>
              <Link href="/market-brain" className="text-xs text-accent flex items-center gap-0.5">
                Details <ChevronRight className="h-3 w-3" />
              </Link>
            </div>
            <div className="grid grid-cols-3 gap-3 justify-items-center">
              <ConfidenceGauge value={brain.market_confidence} label="Market" size={90} />
              <ConfidenceGauge value={brain.breadth_confidence} label="Breadth" size={90} />
              <ConfidenceGauge value={brain.trend_score} label="Trend" size={90} />
              <ConfidenceGauge value={brain.leadership_confidence} label="Leadership" size={85} />
              <ConfidenceGauge value={brain.liquidity_health_score} label="Liquidity" size={85} />
              <ConfidenceGauge value={brain.data_quality_score} label="Data Qual" size={85} />
            </div>
          </div>
        )}

        {/* Watchlist Split Pie + Pipeline */}
        <div className="space-y-4">
          {/* Watchlist Split Donut */}
          <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
            <h3 className="text-sm font-medium mb-2">Watchlist Split</h3>
            <div className="flex items-center gap-5">
              <div className="relative">
                <ResponsiveContainer width={120} height={120}>
                  <PieChart>
                    <Pie data={watchlistPieData} dataKey="value" cx="50%" cy="50%" innerRadius={30} outerRadius={50} strokeWidth={0}>
                      <Cell fill="#6366f1" />
                      <Cell fill="#22d3ee" />
                    </Pie>
                  </PieChart>
                </ResponsiveContainer>
                {/* Center label */}
                <div className="absolute inset-0 flex flex-col items-center justify-center pointer-events-none">
                  <span className="text-[9px] text-text-secondary leading-none">Watchlist</span>
                  <span className="text-sm font-mono font-bold text-text-primary leading-none mt-0.5">
                    {watchlistTotal}
                  </span>
                </div>
              </div>
              <div className="space-y-2.5">
                <div className="flex items-center gap-2">
                  <span className="w-3 h-3 rounded bg-indigo-400" />
                  <span className="text-xs text-indigo-400">
                    Swing{" "}
                    <span className="text-text-secondary">
                      — {swingCount} ({swingPct}%)
                    </span>
                  </span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="w-3 h-3 rounded bg-cyan-400" />
                  <span className="text-xs text-cyan-400">
                    Intraday{" "}
                    <span className="text-text-secondary">
                      — {intradayCount} ({intradayPct}%)
                    </span>
                  </span>
                </div>
              </div>
            </div>
          </div>

          {/* Pipeline Heartbeat */}
          <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-medium">Pipeline</h3>
              <Link href="/pipeline" className="text-xs text-accent flex items-center gap-0.5">
                Monitor <ChevronRight className="h-3 w-3" />
              </Link>
            </div>
            {/* Next job countdown */}
            <div className="bg-bg-tertiary/50 rounded-lg p-3">
              <p className="text-[10px] text-text-secondary uppercase tracking-wider">Next Scheduled Job</p>
              <div className="flex items-baseline justify-between mt-1">
                <p className="font-mono text-sm text-text-primary">{nextJob.name}</p>
                <div className="text-right">
                  <p className="font-mono text-xs font-bold text-accent">{nextJob.cron} IST</p>
                  <p className="text-[10px] text-text-secondary">in {nextJobCountdown}</p>
                </div>
              </div>
            </div>
            {brain && (
              <div className="space-y-2.5 mt-3">
                {/* Long/Short Bias balance bar */}
                <div>
                  <div className="flex justify-between text-[10px] text-text-secondary mb-0.5">
                    <span>Long Bias <span className="font-mono text-profit">{brain.long_bias?.toFixed(2) ?? "--"}</span></span>
                    <span>Short Bias <span className="font-mono text-loss">{brain.short_bias?.toFixed(2) ?? "--"}</span></span>
                  </div>
                  <div className="flex h-2 rounded-full overflow-hidden bg-bg-tertiary">
                    <div
                      className="h-full transition-all"
                      style={{ width: `${longPct}%`, background: "#22c55e" }}
                    />
                    <div
                      className="h-full transition-all"
                      style={{ width: `${shortPct}%`, background: "#ef4444" }}
                    />
                  </div>
                </div>
                {/* Size Multiplier progress */}
                <div>
                  <div className="flex justify-between text-[10px] text-text-secondary mb-0.5">
                    <span>Size Mult</span>
                    <span className="font-mono text-text-primary">{brain.size_multiplier?.toFixed(2) ?? "--"}x</span>
                  </div>
                  <div className="h-1.5 rounded-full bg-bg-tertiary overflow-hidden">
                    <div
                      className="h-full rounded-full transition-all"
                      style={{ width: `${sizeMultPct}%`, background: "#f59e0b" }}
                    />
                  </div>
                </div>
                {/* Max Pos Multiplier progress */}
                <div>
                  <div className="flex justify-between text-[10px] text-text-secondary mb-0.5">
                    <span>Max Pos Mult</span>
                    <span className="font-mono text-text-primary">{brain.max_positions_multiplier?.toFixed(2) ?? "--"}x</span>
                  </div>
                  <div className="h-1.5 rounded-full bg-bg-tertiary overflow-hidden">
                    <div
                      className="h-full rounded-full transition-all"
                      style={{ width: `${maxPosMultPct}%`, background: "#8b5cf6" }}
                    />
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Quick Stats Row */}
      {summary && summary.total_trades > 0 && (
        <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
          <MiniStat
            label="Win Rate"
            value={`${summary.win_rate}%`}
            borderColor="#3b82f6"
            glow={winRate > 60 ? "green" : winRate < 40 ? "red" : undefined}
          />
          <MiniStat
            label="Avg R:R"
            value={summary.avg_rr?.toFixed(2) ?? "--"}
            borderColor="#6366f1"
            glow={avgRR > 1.5 ? "green" : avgRR < 1 ? "red" : undefined}
          />
          <MiniStat label="Total Trades" value={String(summary.total_trades)} borderColor="#64748b" />
          <MiniStat label="Biggest Win" value={formatCurrency(summary.biggest_win)} positive borderColor="#22c55e" />
          <MiniStat label="Biggest Loss" value={formatCurrency(summary.biggest_loss)} negative borderColor="#ef4444" />
        </div>
      )}

      {/* Last Updated */}
      {brain?.asof_ts && (
        <p className="text-xs text-text-secondary text-right">
          Brain updated: {formatTime(new Date(brain.asof_ts))}
        </p>
      )}
    </div>
  );
}

function MiniStat({
  label,
  value,
  positive,
  negative,
  borderColor,
  glow,
}: {
  label: string;
  value: string;
  positive?: boolean;
  negative?: boolean;
  borderColor?: string;
  glow?: "green" | "red";
}) {
  const boxShadow =
    glow === "green"
      ? "0 0 0 1px rgba(34,197,94,0.15), 0 2px 8px rgba(34,197,94,0.08)"
      : glow === "red"
        ? "0 0 0 1px rgba(239,68,68,0.15), 0 2px 8px rgba(239,68,68,0.08)"
        : undefined;

  return (
    <div
      className="bg-bg-secondary rounded-lg border border-bg-tertiary p-3"
      style={{
        borderTop: borderColor ? `3px solid ${borderColor}` : undefined,
        boxShadow,
      }}
    >
      <p className="text-[10px] text-text-secondary">{label}</p>
      <p className={`font-mono text-sm mt-0.5 ${positive ? "text-profit" : negative ? "text-loss" : ""}`}>
        {value}
      </p>
    </div>
  );
}
