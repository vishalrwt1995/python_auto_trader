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

/** Return badge color for multiplier values */
function multBadgeStyle(val: number | null | undefined): { bg: string; color: string; label: string } {
  if (val == null) return { bg: "#1f2937", color: "#9ca3af", label: "" };
  if (val >= 1.2) return { bg: "rgba(59,130,246,0.15)", color: "#3b82f6", label: "HIGH" };
  if (val <= 0.8) return { bg: "rgba(245,158,11,0.15)", color: "#f59e0b", label: "LOW" };
  return { bg: "rgba(156,163,175,0.1)", color: "#9ca3af", label: "NORM" };
}

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

  // Long/Short bias balance bar
  const longBias = Math.min(1, Math.max(0, brain.long_bias ?? 0));
  const shortBias = Math.min(1, Math.max(0, brain.short_bias ?? 0));
  const biasTotal = longBias + shortBias || 1;
  const longPct = (longBias / biasTotal) * 100;
  const shortPct = (shortBias / biasTotal) * 100;

  const sizeBadge = multBadgeStyle(brain.size_multiplier);
  const maxPosBadge = multBadgeStyle(brain.max_positions_multiplier);

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

        {/* Row 1: first 4 fields */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
          <StateField label="Sub-Regime V2"   value={brain.sub_regime_v2} />
          <StateField label="Structure State" value={brain.structure_state} />
          <StateField label="Recovery State"  value={brain.recovery_state || "NONE"} />
          <StateField label="Event State"     value={brain.event_state || "NONE"} />
        </div>

        {/* Separator between rows */}
        <div className="my-3 border-t border-bg-tertiary/60" />

        {/* Row 2: remaining fields */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
          <StateField label="Intraday State"  value={brain.intraday_state} />

          {/* Swing Permission with description */}
          <div>
            <span className="text-text-secondary text-xs">Swing Permission</span>
            <p className={cn("font-semibold font-mono text-sm", swingColor)}>
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
          <p className="text-[10px] text-text-secondary text-center mt-1 opacity-60">
            Vol Calm = 100 − stress, higher = calmer
          </p>
        </div>

        {/* C. Confidence Meters */}
        <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
          <h3 className="text-sm font-medium text-text-primary mb-0.5">Confidence Meters</h3>
          <p className="text-[10px] text-text-secondary mb-3">
            Hover for description &middot; <span className="text-profit/70">higher = better</span>
          </p>
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
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {/* Long / Short Bias with balance bar */}
          <div className="bg-bg-primary rounded-lg p-3" title="Probability-weighted long/short entry bias">
            <div className="flex justify-between text-[10px] text-text-secondary mb-1.5">
              <span>Long / Short Bias</span>
              <span className="font-mono">
                <span className="text-profit">{brain.long_bias?.toFixed(2) ?? "--"}</span>
                <span className="text-text-secondary mx-1">/</span>
                <span className="text-loss">{brain.short_bias?.toFixed(2) ?? "--"}</span>
              </span>
            </div>
            {/* Balance bar */}
            <div className="flex h-2.5 rounded-full overflow-hidden">
              <div
                className="h-full transition-all"
                style={{ width: `${longPct}%`, background: "#22c55e" }}
              />
              <div
                className="h-full transition-all"
                style={{ width: `${shortPct}%`, background: "#ef4444" }}
              />
            </div>
            <div className="flex justify-between text-[9px] mt-1 opacity-60">
              <span className="text-profit">Long {longPct.toFixed(0)}%</span>
              <span className="text-loss">Short {shortPct.toFixed(0)}%</span>
            </div>
          </div>

          {/* Size Mult + Max Pos Mult with badges */}
          <div className="grid grid-cols-2 gap-3">
            <div
              className="bg-bg-primary rounded-lg p-3 text-center cursor-help"
              title="Position size multiplier applied to all trades"
            >
              <p className="text-[10px] text-text-secondary uppercase tracking-wider mb-1">Size Mult</p>
              <p className="text-xl font-mono font-bold" style={{ color: "#f59e0b" }}>
                {brain.size_multiplier != null ? `${brain.size_multiplier.toFixed(2)}x` : "--"}
              </p>
              {brain.size_multiplier != null && (
                <span
                  className="mt-1.5 inline-block text-[9px] font-semibold px-1.5 py-0.5 rounded"
                  style={{ background: sizeBadge.bg, color: sizeBadge.color }}
                >
                  {sizeBadge.label}
                </span>
              )}
            </div>
            <div
              className="bg-bg-primary rounded-lg p-3 text-center cursor-help"
              title="Max concurrent positions multiplier"
            >
              <p className="text-[10px] text-text-secondary uppercase tracking-wider mb-1">Max Pos Mult</p>
              <p className="text-xl font-mono font-bold" style={{ color: "#8b5cf6" }}>
                {brain.max_positions_multiplier != null ? `${brain.max_positions_multiplier.toFixed(2)}x` : "--"}
              </p>
              {brain.max_positions_multiplier != null && (
                <span
                  className="mt-1.5 inline-block text-[9px] font-semibold px-1.5 py-0.5 rounded"
                  style={{ background: maxPosBadge.bg, color: maxPosBadge.color }}
                >
                  {maxPosBadge.label}
                </span>
              )}
            </div>
          </div>
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
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-sm font-medium text-text-primary">Regime History</h3>
          <span className="text-[10px] text-text-secondary">Last {brainHistory.length} snapshots · newest first</span>
        </div>
        {brainHistory.length === 0 ? (
          <div className="h-16 flex items-center justify-center text-text-secondary text-xs">
            No history yet — populates after first brain run
          </div>
        ) : (
          <div className="space-y-1.5">
            {brainHistory.map((row, i) => {
              const regimeColor = REGIME_COLORS[row.regime] ?? "#6b7280";
              const riskColor = RISK_MODE_COLORS[row.risk_mode] ?? "#6b7280";
              const confPct = Math.min(100, Math.max(0, row.market_confidence ?? 0));
              const trendPct = Math.min(100, Math.max(0, row.trend_score ?? 0));
              const stressPct = Math.min(100, Math.max(0, row.volatility_stress_score ?? 0));
              const stressColor = stressPct >= 70 ? "#ef4444" : stressPct >= 40 ? "#f59e0b" : "#22c55e";
              return (
                <div
                  key={row._id ?? i}
                  className="flex items-center gap-3 px-3 py-2 rounded-md bg-bg-primary border-l-2 text-xs"
                  style={{ borderLeftColor: regimeColor }}
                >
                  {/* Time */}
                  <span className="font-mono text-text-secondary w-10 shrink-0 text-[10px]">
                    {formatTime(new Date(row.asof_ts))}
                  </span>

                  {/* Regime + sub */}
                  <div className="w-28 shrink-0">
                    <div className="font-semibold truncate" style={{ color: regimeColor }}>
                      {row.regime.replace(/_/g, " ")}
                    </div>
                    <div className="text-[10px] text-text-secondary truncate">{row.sub_regime_v2 || "—"}</div>
                  </div>

                  {/* Risk mode badge */}
                  <span
                    className="text-[10px] font-medium px-1.5 py-0.5 rounded shrink-0"
                    style={{ color: riskColor, background: `${riskColor}20` }}
                  >
                    {row.risk_mode}
                  </span>

                  {/* Score bars */}
                  <div className="flex-1 flex flex-col gap-0.5 min-w-0">
                    {/* Confidence bar */}
                    <div className="flex items-center gap-1.5">
                      <span className="text-[9px] text-text-secondary w-7 shrink-0">Conf</span>
                      <div className="flex-1 h-1.5 bg-bg-tertiary rounded-full overflow-hidden">
                        <div className="h-full rounded-full" style={{ width: `${confPct}%`, background: regimeColor }} />
                      </div>
                      <span className="text-[10px] font-mono text-text-secondary w-5 text-right shrink-0">{confPct.toFixed(0)}</span>
                    </div>
                    {/* Trend bar */}
                    <div className="flex items-center gap-1.5">
                      <span className="text-[9px] text-text-secondary w-7 shrink-0">Trend</span>
                      <div className="flex-1 h-1.5 bg-bg-tertiary rounded-full overflow-hidden">
                        <div className="h-full rounded-full" style={{ width: `${trendPct}%`, background: "#3b82f6" }} />
                      </div>
                      <span className="text-[10px] font-mono text-text-secondary w-5 text-right shrink-0">{trendPct.toFixed(0)}</span>
                    </div>
                    {/* Vol Stress bar */}
                    <div className="flex items-center gap-1.5">
                      <span className="text-[9px] text-text-secondary w-7 shrink-0">Vol</span>
                      <div className="flex-1 h-1.5 bg-bg-tertiary rounded-full overflow-hidden">
                        <div className="h-full rounded-full" style={{ width: `${stressPct}%`, background: stressColor }} />
                      </div>
                      <span className="text-[10px] font-mono shrink-0 w-5 text-right" style={{ color: stressColor }}>{stressPct.toFixed(0)}</span>
                    </div>
                  </div>
                </div>
              );
            })}
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
      <p className="font-semibold font-mono text-sm text-text-primary truncate" title={value || undefined}>
        {value || "—"}
      </p>
    </div>
  );
}
