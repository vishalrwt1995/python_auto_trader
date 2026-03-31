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
import { ChevronRight } from "lucide-react";
import {
  PieChart,
  Pie,
  Cell,
  ResponsiveContainer,
} from "recharts";

const NEXT_JOBS = [
  { name: "Universe Refresh", cron: "06:15" },
  { name: "Score Cache", cron: "07:05" },
  { name: "Score Refresh", cron: "08:30" },
  { name: "Premarket Watchlist", cron: "09:00" },
  { name: "Scanner Start", cron: "09:20" },
  { name: "EOD Recon", cron: "15:10" },
];

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
    () => positions.filter((p) => p.status === "OPEN"),
    [positions],
  );

  const unrealizedPnl = useMemo(() => {
    return openPositions.reduce((sum, p) => {
      const ltp = ltpCache[p.symbol];
      if (!ltp) return sum;
      return sum + (p.side === "BUY"
        ? (ltp - p.entry_price) * p.qty
        : (p.entry_price - ltp) * p.qty);
    }, 0);
  }, [openPositions, ltpCache]);

  const nextJob = useMemo(() => {
    const now = new Date();
    const ist = new Date(now.toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));
    const nowMins = ist.getHours() * 60 + ist.getMinutes();
    for (const j of NEXT_JOBS) {
      const [h, m] = j.cron.split(":").map(Number);
      if (h * 60 + m > nowMins) return j;
    }
    return NEXT_JOBS[0];
  }, []);

  const swingCount = watchlist.filter((r) => r.eligible_swing).length;
  const intradayCount = watchlist.filter((r) => r.eligible_intraday).length;

  const watchlistPieData = useMemo(() => [
    { name: "Swing", value: swingCount || 1 },
    { name: "Intraday", value: intradayCount || 1 },
  ], [swingCount, intradayCount]);

  if (loading) return <LoadingSkeleton lines={8} className="max-w-3xl" />;
  if (!user) return null;

  const marketOpen = isMarketOpen();
  const todayPnl = (summary?.total_pnl ?? 0) + unrealizedPnl;

  return (
    <div className="space-y-6">
      {/* Regime Banner */}
      {brain && (
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 flex flex-wrap items-center gap-4">
          <RegimeBadge regime={brain.regime as Regime} size="lg" />
          <RiskModeBadge mode={brain.risk_mode as RiskMode} />
          <span className="text-sm text-text-secondary">
            {brain.participation}
          </span>
          <span className="text-sm text-text-secondary">
            Phase: <strong>{brain.phase}</strong>
          </span>
          <div className="ml-auto flex items-center gap-2">
            <LiveDot status={marketOpen ? "online" : "offline"} />
            <span className="text-xs text-text-secondary">
              {marketOpen ? "Market Open" : "Market Closed"}
            </span>
          </div>
        </div>
      )}

      {/* Stats Grid */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <PnLCard
          label="Today's P&L"
          value={todayPnl}
          subLabel="Realized"
          subValue={formatCurrency(summary?.total_pnl ?? 0)}
        />

        <Link href="/positions" className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 hover:border-accent/50 transition-colors">
          <p className="text-xs text-text-secondary mb-1">Active Positions</p>
          <p className="text-2xl font-mono font-bold text-text-primary">
            {openPositions.length}
          </p>
          {openPositions.length > 0 && (
            <p className="text-xs text-text-secondary mt-1 truncate">
              {openPositions.map((p) => p.symbol).join(", ")}
            </p>
          )}
        </Link>

        <Link href="/watchlist" className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 hover:border-accent/50 transition-colors">
          <p className="text-xs text-text-secondary mb-1">Watchlist</p>
          <p className="text-2xl font-mono font-bold text-text-primary">
            {watchlist.length}
          </p>
          <p className="text-xs text-text-secondary mt-1">
            {swingCount} swing &middot; {intradayCount} intraday
          </p>
        </Link>

        <Link href="/market-brain" className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4 hover:border-accent/50 transition-colors">
          <p className="text-xs text-text-secondary mb-1">Market Confidence</p>
          <p className="text-2xl font-mono font-bold text-accent">
            {brain?.market_confidence?.toFixed(0) ?? "--"}
          </p>
          <p className="text-xs text-text-secondary mt-1">
            Swing: {brain?.swing_permission ?? "--"}
          </p>
        </Link>
      </div>

      {/* Equity Curve */}
      {equityData.length > 0 && (
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
          <h3 className="text-sm font-medium mb-2">30-Day Equity Curve</h3>
          <EquityCurve data={equityData} height={180} />
        </div>
      )}

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
            <div className="flex justify-around flex-wrap gap-3">
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
          {/* Watchlist Split */}
          <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
            <h3 className="text-sm font-medium mb-2">Watchlist Split</h3>
            <div className="flex items-center gap-5">
              <ResponsiveContainer width={120} height={120}>
                <PieChart>
                  <Pie data={watchlistPieData} dataKey="value" cx="50%" cy="50%" innerRadius={30} outerRadius={50} strokeWidth={0}>
                    <Cell fill="#3b82f6" />
                    <Cell fill="#8b5cf6" />
                  </Pie>
                </PieChart>
              </ResponsiveContainer>
              <div className="space-y-2.5">
                <div className="flex items-center gap-2">
                  <span className="w-3 h-3 rounded bg-[#3b82f6]" />
                  <span className="text-xs">Swing <span className="text-text-secondary">— {swingCount}</span></span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="w-3 h-3 rounded bg-[#8b5cf6]" />
                  <span className="text-xs">Intraday <span className="text-text-secondary">— {intradayCount}</span></span>
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
            <div className="bg-bg-tertiary/50 rounded-lg p-3">
              <p className="text-xs text-text-secondary">Next Scheduled Job</p>
              <p className="font-mono text-sm mt-0.5">
                {nextJob.name}{" "}
                <span className="text-text-secondary">@ {nextJob.cron} IST</span>
              </p>
            </div>
            {brain && (
              <div className="grid grid-cols-2 gap-3 mt-3 text-sm">
                <div>
                  <span className="text-text-secondary text-[10px]">Long Bias</span>
                  <p className="font-mono text-xs">{brain.long_bias?.toFixed(2) ?? "--"}</p>
                </div>
                <div>
                  <span className="text-text-secondary text-[10px]">Short Bias</span>
                  <p className="font-mono text-xs">{brain.short_bias?.toFixed(2) ?? "--"}</p>
                </div>
                <div>
                  <span className="text-text-secondary text-[10px]">Size Mult</span>
                  <p className="font-mono text-xs">{brain.size_multiplier?.toFixed(2) ?? "--"}</p>
                </div>
                <div>
                  <span className="text-text-secondary text-[10px]">Max Pos Mult</span>
                  <p className="font-mono text-xs">{brain.max_positions_multiplier?.toFixed(2) ?? "--"}</p>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Quick Stats Row */}
      {summary && summary.total_trades > 0 && (
        <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
          <MiniStat label="Win Rate" value={`${summary.win_rate}%`} />
          <MiniStat label="Avg R:R" value={String(summary.avg_rr)} />
          <MiniStat label="Total Trades" value={String(summary.total_trades)} />
          <MiniStat label="Biggest Win" value={formatCurrency(summary.biggest_win)} positive />
          <MiniStat label="Biggest Loss" value={formatCurrency(summary.biggest_loss)} />
        </div>
      )}

      {/* Allowed Strategies */}
      {brain?.allowed_strategies && brain.allowed_strategies.length > 0 && (
        <div className="flex flex-wrap gap-1.5">
          <span className="text-xs text-text-secondary mr-1">Strategies:</span>
          {brain.allowed_strategies.map((s) => (
            <span key={s} className="px-1.5 py-0.5 bg-bg-tertiary rounded text-[10px] text-text-secondary">
              {s}
            </span>
          ))}
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
}: {
  label: string;
  value: string;
  positive?: boolean;
}) {
  return (
    <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-3">
      <p className="text-[10px] text-text-secondary">{label}</p>
      <p className={`font-mono text-sm mt-0.5 ${positive ? "text-profit" : ""}`}>
        {value}
      </p>
    </div>
  );
}
