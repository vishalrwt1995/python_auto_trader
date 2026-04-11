"use client";

import { useDashboardStore } from "@/stores/dashboardStore";
import { RegimeBadge } from "@/components/widgets/RegimeBadge";
import { RiskModeBadge } from "@/components/widgets/RiskModeBadge";
import { ConfidenceGauge } from "@/components/widgets/ConfidenceGauge";
import { RadarScore } from "@/components/charts/RadarScore";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { LiveDot } from "@/components/shared/LiveDot";
import { cn, formatTime } from "@/lib/utils";
import type { Regime, RiskMode, Participation } from "@/lib/types";
import { PARTICIPATION_COLORS, REGIME_COLORS, RISK_MODE_COLORS } from "@/lib/constants";

/** Classify a reason string by sentiment for border color */
function reasonSentiment(r: string): "positive" | "negative" | "warning" | "neutral" {
  const u = r.toUpperCase();
  // Negative: explicit failure/block keywords (not "below safe/threshold")
  if (/\bBLOCKED\b|\bDISABLED\b|\bDEGRADED\b|\bFAILED\b|\bMISSING\b|\bERROR\b|\bLOCKDOWN\b|\bPANIC\b|\bSUSPENDED\b/.test(u))
    return "negative";
  // "BELOW" is only negative when paired with "THRESHOLD" or "MIN" (e.g. "score below threshold"), NOT "below safe"
  if (/BELOW\s+(THRESHOLD|MIN|MINIMUM|LIMIT)\b/.test(u)) return "negative";
  if (/\bWARNING\b|\bCAUTION\b|\bREDUCED\b|\bTIGHT\b|\bELEVATED\b|\bWEAK\b|\bDELAYED\b/.test(u)) return "warning";
  if (/\bENABLED\b|\bPASSED\b|\bHEALTHY\b|\bSTRONG\b|\bVALID\b|\b\bOK\b|\bABOVE\b|\bACTIVE\b|\bCLEAR\b|\bCONFIRMED\b|\bBELOW SAFE\b/.test(u)) return "positive";
  return "neutral";
}

const SENTIMENT_BORDER: Record<ReturnType<typeof reasonSentiment>, string> = {
  positive: "#22c55e",
  negative: "#ef4444",
  warning:  "#f59e0b",
  neutral:  "#3b82f6",
};

const GAUGE_TIPS: Record<string, string> = {
  Overall:    "Composite market confidence used to gate trade entry",
  Breadth:    "% of stocks above key MAs — breadth of market participation",
  Leadership: "Quality of leading stocks; high = broad sector participation",
  Trend:      "Daily trend strength score (EMA stack + ADX)",
  "Phase 2":  "Confidence that the market is in a Stage 2 uptrend phase",
  Policy:     "How aligned current market conditions are with the risk policy",
  Integrity:  "Run integrity — signals data freshness and pipeline health",
  "Data Qual":"Data quality score — stale or incomplete data lowers this",
};

export default function MarketBrainPage() {
  const brain = useDashboardStore((s) => s.marketBrain);
  const brainHistory = useDashboardStore((s) => s.brainHistory);

  if (!brain) return <LoadingSkeleton lines={12} className="max-w-4xl" />;

  const radarData = [
    { label: "Trend",     current: brain.trend_score },
    { label: "Breadth",   current: brain.breadth_score },
    { label: "Leadership",current: brain.leadership_score },
    { label: "Vol Calm",  current: Math.max(0, 100 - brain.volatility_stress_score) },
    { label: "Liquidity", current: brain.liquidity_health_score },
    { label: "Data Qual", current: brain.data_quality_score },
  ];

  const swingColor =
    brain.swing_permission === "ENABLED"
      ? "text-profit"
      : brain.swing_permission === "DISABLED"
        ? "text-loss"
        : "text-neutral";

  const swingDesc =
    brain.swing_permission === "ENABLED"  ? "Swing entries allowed"
    : brain.swing_permission === "REDUCED" ? "Reduced sizing only"
    : "No new swing entries";

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold">Market Brain Live</h1>
        <div className="flex items-center gap-2 text-xs text-text-secondary">
          <LiveDot status="online" />
          <span>Real-time · Firestore</span>
        </div>
      </div>

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
            <span
              className="px-2 py-0.5 rounded bg-loss/20 text-loss text-xs font-medium cursor-help"
              title="Pipeline ran in degraded mode — data may be incomplete. Check Reasons log."
            >
              ⚠ DEGRADED
            </span>
          )}
        </div>

        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
          <StateField label="Sub-Regime V2"   value={brain.sub_regime_v2} />
          <StateField label="Structure State" value={brain.structure_state} />
          <StateField label="Recovery State"  value={brain.recovery_state || "NONE"} />
          <StateField label="Event State"     value={brain.event_state || "NONE"} />
          <StateField label="Intraday State"  value={brain.intraday_state} />

          {/* Swing Permission with description */}
          <div>
            <span className="text-text-secondary text-xs">Swing Permission</span>
            <p className={cn("font-mono text-sm", swingColor)}>
              {brain.swing_permission}
            </p>
            <p className="text-[10px] text-text-secondary mt-0.5">{swingDesc}</p>
          </div>

          <StateField label="Phase" value={brain.phase} />

          {/* Allowed Strategies as badges */}
          <div>
            <span className="text-text-secondary text-xs">Allowed Strategies</span>
            {brain.allowed_strategies?.length > 0 ? (
              <div className="flex flex-wrap gap-1 mt-1">
                {brain.allowed_strategies.map((s) => (
                  <span
                    key={s}
                    className="px-1.5 py-0.5 bg-bg-tertiary rounded text-[10px] text-text-secondary"
                  >
                    {s}
                  </span>
                ))}
              </div>
            ) : (
              <p className="font-mono text-sm text-text-secondary">—</p>
            )}
          </div>
        </div>

        {/* Vol Stress raw value */}
        <div className="mt-4 pt-3 border-t border-bg-tertiary flex flex-wrap gap-4 text-xs text-text-secondary">
          <span>
            Vol Stress:{" "}
            <span
              className={cn(
                "font-mono font-semibold",
                brain.volatility_stress_score >= 70
                  ? "text-loss"
                  : brain.volatility_stress_score >= 40
                    ? "text-neutral"
                    : "text-profit",
              )}
            >
              {brain.volatility_stress_score?.toFixed(0) ?? "--"}
            </span>
            <span className="ml-1 opacity-60">(higher = more stress)</span>
          </span>
          <span>
            Liquidity:{" "}
            <span className={cn("font-mono font-semibold",
              brain.liquidity_health_score >= 60 ? "text-profit" : brain.liquidity_health_score >= 30 ? "text-neutral" : "text-loss",
            )}>
              {brain.liquidity_health_score?.toFixed(0) ?? "--"}
            </span>
          </span>
          <span>
            Data Quality:{" "}
            <span className={cn("font-mono font-semibold",
              brain.data_quality_score >= 80 ? "text-profit" : brain.data_quality_score >= 50 ? "text-neutral" : "text-loss",
            )}>
              {brain.data_quality_score?.toFixed(0) ?? "--"}
            </span>
          </span>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* B. Radar Chart */}
        <div className="lg:col-span-2 bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
          <div className="flex items-center justify-between mb-1">
            <h3 className="text-sm font-medium text-text-primary">Component Scores</h3>
            <span className="text-[10px] text-text-secondary">Vol Calm = 100 − stress</span>
          </div>
          <RadarScore data={radarData} height={320} />
        </div>

        {/* C. Confidence Meters */}
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
          <h3 className="text-sm font-medium text-text-primary mb-1">Confidence Meters</h3>
          <p className="text-[10px] text-text-secondary mb-3">Hover for description</p>
          <div className="grid grid-cols-2 gap-4 justify-items-center">
            {([
              [brain.market_confidence,        "Overall",   100],
              [brain.breadth_confidence,       "Breadth",    85],
              [brain.leadership_confidence,    "Leadership", 85],
              [brain.trend_score,              "Trend",      85],
              [brain.phase2_confidence,        "Phase 2",    85],
              [brain.policy_confidence,        "Policy",     85],
              [brain.run_integrity_confidence, "Integrity",  85],
              [brain.data_quality_score,       "Data Qual",  85],
            ] as [number, string, number][]).map(([value, label, size]) => (
              <div key={label} title={GAUGE_TIPS[label]}>
                <ConfidenceGauge value={value} label={label} size={size} />
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* D. Policy Biases */}
      <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
        <h3 className="text-sm font-medium text-text-primary mb-3">Policy Biases</h3>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {([
            ["Long Bias",    brain.long_bias?.toFixed(2) ?? "--",    "#22c55e", "Probability-weighted long entry bias"],
            ["Short Bias",   brain.short_bias?.toFixed(2) ?? "--",   "#ef4444", "Probability-weighted short entry bias"],
            ["Size Mult",    brain.size_multiplier != null ? `${brain.size_multiplier.toFixed(2)}x` : "--",               "#f59e0b", "Position size multiplier applied to all trades"],
            ["Max Pos Mult", brain.max_positions_multiplier != null ? `${brain.max_positions_multiplier.toFixed(2)}x` : "--", "#8b5cf6", "Max concurrent positions multiplier"],
          ] as const).map(([label, value, color, tip]) => (
            <div
              key={label}
              className="bg-bg-primary rounded-lg p-3 text-center cursor-help"
              title={tip}
            >
              <p className="text-[10px] text-text-secondary uppercase tracking-wider">{label}</p>
              <p className="text-xl font-mono font-bold mt-1" style={{ color }}>{value}</p>
            </div>
          ))}
        </div>
      </div>

      {/* E. Reasons Log */}
      {brain.reasons?.length > 0 && (
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-sm font-medium text-text-primary">Reasons</h3>
            <div className="flex items-center gap-3 text-[10px] text-text-secondary">
              <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm bg-profit inline-block" />Positive</span>
              <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm bg-neutral inline-block" />Warning</span>
              <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm bg-loss inline-block" />Negative</span>
            </div>
          </div>
          <div className="space-y-1.5 max-h-64 overflow-y-auto scrollbar-thin">
            {brain.reasons.map((reason, i) => {
              const sentiment = reasonSentiment(reason);
              return (
                <div
                  key={i}
                  className="flex items-start gap-2.5 px-3 py-2 bg-bg-primary rounded-lg text-xs"
                  style={{ borderLeft: `3px solid ${SENTIMENT_BORDER[sentiment]}` }}
                >
                  <span className="text-text-primary">{reason}</span>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* F. Regime History */}
      <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
        <h3 className="text-sm font-medium text-text-primary mb-3">Regime History</h3>
        {brainHistory.length === 0 ? (
          <div className="h-16 flex items-center justify-center text-text-secondary text-xs">
            No history yet — populates after first brain run
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-text-secondary border-b border-bg-tertiary">
                  <th className="text-left pb-2 pr-4 font-medium">Time</th>
                  <th className="text-left pb-2 pr-4 font-medium">Regime</th>
                  <th className="text-left pb-2 pr-4 font-medium">Sub-Regime</th>
                  <th className="text-left pb-2 pr-4 font-medium">Risk Mode</th>
                  <th className="text-right pb-2 pr-4 font-medium">Conf</th>
                  <th className="text-right pb-2 pr-4 font-medium">Trend</th>
                  <th className="text-right pb-2 font-medium">Vol Stress</th>
                </tr>
              </thead>
              <tbody>
                {brainHistory.map((row, i) => (
                  <tr key={row._id ?? i} className="border-b border-bg-tertiary/50 last:border-0">
                    <td className="py-1.5 pr-4 font-mono text-text-secondary whitespace-nowrap">
                      {formatTime(new Date(row.asof_ts))}
                    </td>
                    <td className="py-1.5 pr-4">
                      <span
                        className="font-semibold"
                        style={{ color: REGIME_COLORS[row.regime] ?? "#6b7280" }}
                      >
                        {row.regime.replace("_", " ")}
                      </span>
                    </td>
                    <td className="py-1.5 pr-4 text-text-secondary font-mono">
                      {row.sub_regime_v2 || "—"}
                    </td>
                    <td className="py-1.5 pr-4">
                      <span
                        className="font-medium"
                        style={{ color: RISK_MODE_COLORS[row.risk_mode] ?? "#6b7280" }}
                      >
                        {row.risk_mode}
                      </span>
                    </td>
                    <td className="py-1.5 pr-4 text-right font-mono">
                      {row.market_confidence?.toFixed(0) ?? "—"}
                    </td>
                    <td className="py-1.5 pr-4 text-right font-mono">
                      {row.trend_score?.toFixed(0) ?? "—"}
                    </td>
                    <td className={cn(
                      "py-1.5 text-right font-mono",
                      row.volatility_stress_score >= 70 ? "text-loss"
                        : row.volatility_stress_score >= 40 ? "text-neutral"
                        : "text-profit",
                    )}>
                      {row.volatility_stress_score?.toFixed(0) ?? "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <p className="text-xs text-text-secondary text-right">
        Brain updated: {brain.asof_ts ? formatTime(new Date(brain.asof_ts)) : "—"}
      </p>
    </div>
  );
}

function StateField({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0">
      <span className="text-text-secondary text-xs">{label}</span>
      <p className="font-mono text-sm text-text-primary truncate" title={value || undefined}>
        {value || "—"}
      </p>
    </div>
  );
}
