"use client";

import { useEffect, useState } from "react";
import { api } from "@/lib/api";
import type { MarketBrainExplain, ExplainComponent } from "@/lib/types";
import { cn } from "@/lib/utils";

/**
 * ExplainPanel (PR-2 Tier-1)
 *
 * Fetches `/dashboard/market-brain/explain` once on mount and renders a
 * per-component weight × score = contribution breakdown so users can see WHY
 * market_confidence landed where it did, plus the regime transition and
 * signal-age penalty surfaced from PR-1 state.
 *
 * The endpoint is composed server-side from the already-persisted state —
 * there is no live recompute — so two callers on the same page see the same
 * numbers the brain last persisted.
 */
export function ExplainPanel() {
  const [explain, setExplain] = useState<MarketBrainExplain | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    api
      .getMarketBrainExplain()
      .then((data) => {
        if (!cancelled) {
          setExplain(data);
          setError(null);
        }
      })
      .catch((e) => {
        if (!cancelled) setError(e?.message ?? "Failed to load explain");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-5">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-medium text-text-primary">Score Breakdown · How confidence was built</h3>
        <span className="text-[10px] text-text-secondary">server-composed, no recompute</span>
      </div>

      {loading && (
        <p className="text-xs text-text-secondary italic">Loading breakdown…</p>
      )}

      {error && !loading && (
        <p className="text-xs text-loss">⚠ {error}</p>
      )}

      {!loading && !error && explain && (
        <>
          {/* Component grid */}
          <div className="space-y-2 mb-4">
            {explain.components.map((c) => (
              <ComponentRow key={c.key} c={c} />
            ))}
          </div>

          {/* Summary strip */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 pt-3 border-t border-bg-tertiary text-xs">
            <SummaryField
              label="Market Confidence"
              value={explain.confidence.market_confidence?.toFixed(1) ?? "—"}
              accent="#3b82f6"
              tooltip="Composite confidence after signal-age penalty"
            />
            <SummaryField
              label="Raw Confidence"
              value={explain.confidence.market_confidence_raw?.toFixed(1) ?? "—"}
              accent="#8b5cf6"
              tooltip="Before signal-age penalty"
            />
            <SummaryField
              label="Signal Age Penalty"
              value={
                explain.confidence.signal_age_penalty != null
                  ? `-${explain.confidence.signal_age_penalty.toFixed(1)}`
                  : "—"
              }
              accent="#f59e0b"
              tooltip="Points shaved off confidence due to stale NIFTY/VIX/PCR"
            />
            <SummaryField
              label="Regime Age"
              value={
                explain.regime_transition.regime_age_seconds != null
                  ? formatDuration(explain.regime_transition.regime_age_seconds)
                  : "—"
              }
              accent="#22c55e"
              tooltip="How long the current regime has persisted"
            />
          </div>

          {/* PR-1 signals passthrough */}
          <div className="grid grid-cols-3 gap-3 mt-3 text-xs">
            <SummaryField
              label="Options Pos"
              value={explain.signals.options_positioning_score?.toFixed(0) ?? "—"}
              accent="#06b6d4"
              tooltip="Derived from PCR (contrarian): 50=neutral, high=bullish, low=bearish"
            />
            <SummaryField
              label="Flow (FII+DII)"
              value={explain.signals.flow_score?.toFixed(0) ?? "—"}
              accent="#06b6d4"
              tooltip="Combined net institutional flow; 50=neutral"
            />
            <SummaryField
              label="Breadth RoC"
              value={explain.signals.breadth_roc_score?.toFixed(0) ?? "—"}
              accent="#06b6d4"
              tooltip="Rate-of-change of breadth vs prior snapshot"
            />
          </div>

          {explain.regime_transition.prev_regime && (
            <div className="mt-3 px-3 py-2 rounded-md bg-bg-primary text-[11px] text-text-secondary border-l-2 border-blue-500/60">
              Regime transitioned from{" "}
              <span className="font-mono text-text-primary">
                {String(explain.regime_transition.prev_regime)}
              </span>{" "}
              → <span className="font-mono text-text-primary">{explain.regime}</span>
              {explain.regime_transition.regime_transitions_today != null && (
                <> · {explain.regime_transition.regime_transitions_today} transitions today</>
              )}
            </div>
          )}
        </>
      )}
    </div>
  );
}

function ComponentRow({ c }: { c: ExplainComponent }) {
  const pct = Math.max(0, Math.min(100, c.score));
  const contrib = c.contribution;
  const contribColor =
    contrib >= 0 ? (contrib >= 10 ? "#22c55e" : "#3b82f6") : "#ef4444";
  const bandColor = bandToColor(c.band, c.inverted);
  const deltaStr =
    c.delta != null && Math.abs(c.delta) >= 0.5
      ? `${c.delta > 0 ? "+" : ""}${c.delta.toFixed(1)}`
      : null;

  return (
    <div className="bg-bg-primary rounded-md p-2.5">
      <div className="flex items-center gap-3 mb-1">
        <span className="text-xs font-semibold text-text-primary w-24 shrink-0">
          {c.label}
        </span>
        <span
          className="text-[10px] font-medium px-1.5 py-0.5 rounded shrink-0 uppercase tracking-wider"
          style={{ color: bandColor, background: `${bandColor}20` }}
        >
          {c.band}
        </span>
        <span className="text-[10px] text-text-secondary font-mono shrink-0">
          w={c.weight >= 0 ? `+${c.weight.toFixed(2)}` : c.weight.toFixed(2)}
        </span>
        {deltaStr && (
          <span
            className={cn(
              "text-[10px] font-mono shrink-0",
              c.delta > 0 ? "text-profit" : "text-loss",
            )}
          >
            Δ{deltaStr}
          </span>
        )}
        <span className="text-[10px] text-text-secondary font-mono ml-auto shrink-0">
          {c.score.toFixed(1)}
        </span>
        <span
          className="text-[10px] font-mono font-semibold shrink-0 w-14 text-right"
          style={{ color: contribColor }}
        >
          {contrib >= 0 ? "+" : ""}
          {contrib.toFixed(2)}
        </span>
      </div>

      {/* Bar */}
      <div className="h-1.5 bg-bg-tertiary rounded-full overflow-hidden">
        <div
          className="h-full rounded-full transition-all"
          style={{ width: `${pct}%`, background: bandColor }}
        />
      </div>

      <p className="text-[10px] text-text-secondary mt-1 leading-snug">{c.rationale}</p>
    </div>
  );
}

function SummaryField({
  label,
  value,
  accent,
  tooltip,
}: {
  label: string;
  value: string;
  accent: string;
  tooltip?: string;
}) {
  return (
    <div
      className="bg-bg-primary rounded-md p-2 text-center cursor-help"
      title={tooltip}
      style={{ borderTop: `2px solid ${accent}` }}
    >
      <p className="text-[9px] uppercase tracking-wider text-text-secondary mb-0.5">
        {label}
      </p>
      <p className="text-sm font-mono font-semibold" style={{ color: accent }}>
        {value}
      </p>
    </div>
  );
}

function bandToColor(band: string, inverted: boolean): string {
  const b = band?.toLowerCase() ?? "";
  // For inverted metrics (stress), high bands are bad
  if (inverted) {
    if (b === "severe" || b === "elevated") return "#ef4444";
    if (b === "moderate") return "#f59e0b";
    return "#22c55e";
  }
  if (b === "strong" || b === "firm") return "#22c55e";
  if (b === "mixed") return "#f59e0b";
  if (b === "weak") return "#fb923c";
  if (b === "broken") return "#ef4444";
  return "#94a3b8";
}

function formatDuration(seconds: number): string {
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  if (seconds < 86400) return `${(seconds / 3600).toFixed(1)}h`;
  return `${(seconds / 86400).toFixed(1)}d`;
}
