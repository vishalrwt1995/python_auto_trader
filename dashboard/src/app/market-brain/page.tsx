"use client";

import { useDashboardStore } from "@/stores/dashboardStore";
import { RegimeBadge } from "@/components/widgets/RegimeBadge";
import { RiskModeBadge } from "@/components/widgets/RiskModeBadge";
import { ConfidenceGauge } from "@/components/widgets/ConfidenceGauge";
import { RadarScore } from "@/components/charts/RadarScore";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { cn, formatTime } from "@/lib/utils";
import type { Regime, RiskMode, Participation } from "@/lib/types";
import { PARTICIPATION_COLORS } from "@/lib/constants";

export default function MarketBrainPage() {
  const brain = useDashboardStore((s) => s.marketBrain);

  if (!brain) return <LoadingSkeleton lines={12} className="max-w-4xl" />;

  const radarData = [
    { label: "Trend", current: brain.trend_score },
    { label: "Breadth", current: brain.breadth_score },
    { label: "Leadership", current: brain.leadership_score },
    // Vol Stress is inverted: higher stress = worse, so display as calmness (100 - stress)
    { label: "Vol Calm", current: Math.max(0, 100 - brain.volatility_stress_score) },
    { label: "Liquidity", current: brain.liquidity_health_score },
    { label: "Data Quality", current: brain.data_quality_score },
  ];

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-semibold">Market Brain Live</h1>

      {/* A. Current State Panel */}
      <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-5">
        <div className="flex flex-wrap items-center gap-3 mb-4">
          <RegimeBadge regime={brain.regime as Regime} size="lg" />
          <RiskModeBadge mode={brain.risk_mode as RiskMode} />
          <span
            className="px-2 py-0.5 rounded text-xs font-medium text-white"
            style={{
              backgroundColor:
                PARTICIPATION_COLORS[brain.participation as Participation] ?? "#6b7280",
            }}
          >
            {brain.participation}
          </span>
          {brain.run_degraded_flag && (
            <span className="px-2 py-0.5 rounded bg-loss/20 text-loss text-xs font-medium">
              DEGRADED
            </span>
          )}
        </div>

        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
          <StateField label="Sub-Regime V2" value={brain.sub_regime_v2} />
          <StateField label="Structure State" value={brain.structure_state} />
          <StateField label="Recovery State" value={brain.recovery_state || "NONE"} />
          <StateField label="Event State" value={brain.event_state || "NONE"} />
          <StateField label="Intraday State" value={brain.intraday_state} />
          <div>
            <span className="text-text-secondary text-xs">Swing Permission</span>
            <p
              className={cn(
                "font-mono",
                brain.swing_permission === "ENABLED"
                  ? "text-profit"
                  : brain.swing_permission === "DISABLED"
                    ? "text-loss"
                    : "text-neutral",
              )}
            >
              {brain.swing_permission}
            </p>
          </div>
          <StateField label="Phase" value={brain.phase} />
          <div>
            <span className="text-text-secondary text-xs">Allowed Strategies</span>
            <p className="font-mono text-text-primary text-xs">
              {brain.allowed_strategies?.join(", ") || "—"}
            </p>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* B. Radar Chart */}
        <div className="lg:col-span-2 bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
          <h3 className="text-sm font-medium text-text-primary mb-2">
            Component Scores
          </h3>
          <RadarScore data={radarData} height={320} />
        </div>

        {/* C. Confidence Meters — Circular Gauges */}
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
          <h3 className="text-sm font-medium text-text-primary mb-4">
            Confidence Meters
          </h3>
          <div className="grid grid-cols-2 gap-4 justify-items-center">
            <ConfidenceGauge value={brain.market_confidence} label="Overall" size={100} />
            <ConfidenceGauge value={brain.breadth_confidence} label="Breadth" size={85} />
            <ConfidenceGauge value={brain.leadership_confidence} label="Leadership" size={85} />
            <ConfidenceGauge value={brain.trend_score} label="Trend" size={85} />
            <ConfidenceGauge value={brain.phase2_confidence} label="Phase 2" size={85} />
            <ConfidenceGauge value={brain.policy_confidence} label="Policy" size={85} />
            <ConfidenceGauge value={brain.run_integrity_confidence} label="Integrity" size={85} />
            <ConfidenceGauge value={brain.data_quality_score} label="Data Qual" size={85} />
          </div>
        </div>
      </div>

      {/* D. Policy Biases */}
      <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
        <h3 className="text-sm font-medium text-text-primary mb-3">Policy Biases</h3>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {([
            ["Long Bias", brain.long_bias?.toFixed(2) ?? "--", "#22c55e"],
            ["Short Bias", brain.short_bias?.toFixed(2) ?? "--", "#ef4444"],
            ["Size Mult", brain.size_multiplier != null ? `${brain.size_multiplier.toFixed(2)}x` : "--", "#f59e0b"],
            ["Max Pos Mult", brain.max_positions_multiplier != null ? `${brain.max_positions_multiplier.toFixed(2)}x` : "--", "#8b5cf6"],
          ] as const).map(([label, value, color]) => (
            <div key={label} className="bg-bg-primary rounded-lg p-3 text-center">
              <p className="text-[10px] text-text-secondary uppercase tracking-wider">{label}</p>
              <p className="text-xl font-mono font-bold mt-1" style={{ color }}>{value}</p>
            </div>
          ))}
        </div>
      </div>

      {/* E. Reasons Log */}
      {brain.reasons?.length > 0 && (
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
          <h3 className="text-sm font-medium text-text-primary mb-2">Reasons</h3>
          <div className="space-y-2 max-h-60 overflow-y-auto scrollbar-thin">
            {brain.reasons.map((reason, i) => (
              <div
                key={i}
                className="flex items-start gap-2.5 px-3 py-2 bg-bg-primary rounded-lg text-xs"
                style={{ borderLeft: `3px solid ${i < 3 ? "#22c55e" : "#3b82f6"}` }}
              >
                <span className="text-text-primary">{reason}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* F. Regime History — placeholder until BQ history endpoint is wired */}
      <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
        <h3 className="text-sm font-medium text-text-primary mb-3">Regime History</h3>
        <div className="h-32 flex items-center justify-center text-text-secondary text-xs">
          Historical regime timeline — coming soon
        </div>
      </div>

      <p className="text-xs text-text-secondary text-right">
        As of: {brain.asof_ts ? formatTime(new Date(brain.asof_ts)) : "—"}
      </p>
    </div>
  );
}

function StateField({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <span className="text-text-secondary text-xs">{label}</span>
      <p className="font-mono text-text-primary">{value || "—"}</p>
    </div>
  );
}

