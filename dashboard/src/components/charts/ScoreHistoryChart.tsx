"use client";

import { useEffect, useMemo, useState } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";
import { api } from "@/lib/api";
import type { BrainHistoryPoint } from "@/lib/types";
import { cn } from "@/lib/utils";

/**
 * ScoreHistoryChart (PR-2 Tier-1)
 *
 * Fetches `/dashboard/market-brain/history` (BQ-backed timeseries of the last
 * N days of brain snapshots) and renders a multi-series line chart. Users can
 * toggle individual series on/off and change the look-back window (1/3/7 days).
 *
 * All score series are clamped to 0..100 at the backend, so the Y axis is fixed
 * to [0,100] for consistent visual scale across refreshes and windows.
 */

type SeriesKey =
  | "market_confidence"
  | "trend_score"
  | "breadth_score"
  | "volatility_stress_score"
  | "options_positioning_score"
  | "flow_score"
  | "breadth_roc_score";

interface SeriesDef {
  key: SeriesKey;
  label: string;
  color: string;
  defaultOn: boolean;
}

const SERIES: SeriesDef[] = [
  { key: "market_confidence",        label: "Confidence", color: "#3b82f6", defaultOn: true  },
  { key: "trend_score",              label: "Trend",      color: "#22c55e", defaultOn: true  },
  { key: "breadth_score",            label: "Breadth",    color: "#8b5cf6", defaultOn: true  },
  { key: "volatility_stress_score",  label: "Vol Stress", color: "#ef4444", defaultOn: true  },
  { key: "options_positioning_score", label: "Options",   color: "#06b6d4", defaultOn: false },
  { key: "flow_score",               label: "Flow",       color: "#f59e0b", defaultOn: false },
  { key: "breadth_roc_score",        label: "Breadth RoC", color: "#ec4899", defaultOn: false },
];

const WINDOWS: { days: number; label: string }[] = [
  { days: 1, label: "1D" },
  { days: 3, label: "3D" },
  { days: 7, label: "7D" },
];

export function ScoreHistoryChart({ height = 320 }: { height?: number }) {
  const [days, setDays] = useState<number>(1);
  const [points, setPoints] = useState<BrainHistoryPoint[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [visible, setVisible] = useState<Record<SeriesKey, boolean>>(
    () => Object.fromEntries(SERIES.map((s) => [s.key, s.defaultOn])) as Record<SeriesKey, boolean>,
  );

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    api
      .getMarketBrainHistory(days, days === 1 ? 500 : days === 3 ? 1000 : 2000)
      .then((resp) => {
        if (cancelled) return;
        setPoints(Array.isArray(resp?.series) ? resp.series : []);
        if (resp?.meta?.error) setError(resp.meta.error);
      })
      .catch((e) => {
        if (!cancelled) setError(e?.message ?? "Failed to load history");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [days]);

  const chartData = useMemo(
    () =>
      points.map((p) => ({
        ts: p.asof_ts,
        tick: formatTick(p.asof_ts, days),
        market_confidence: p.market_confidence ?? null,
        trend_score: p.trend_score ?? null,
        breadth_score: p.breadth_score ?? null,
        volatility_stress_score: p.volatility_stress_score ?? null,
        options_positioning_score: p.options_positioning_score ?? null,
        flow_score: p.flow_score ?? null,
        breadth_roc_score: p.breadth_roc_score ?? null,
      })),
    [points, days],
  );

  return (
    <div className="bg-bg-secondary rounded-lg border border-bg-tertiary p-4">
      <div className="flex items-center justify-between mb-3 flex-wrap gap-2">
        <h3 className="text-sm font-medium text-text-primary">Score History</h3>
        <div className="flex items-center gap-1 flex-wrap">
          {WINDOWS.map((w) => (
            <button
              key={w.days}
              type="button"
              onClick={() => setDays(w.days)}
              className={cn(
                "text-[10px] font-semibold px-2 py-0.5 rounded transition-colors",
                days === w.days
                  ? "bg-blue-500 text-white"
                  : "bg-bg-primary text-text-secondary hover:bg-bg-tertiary",
              )}
            >
              {w.label}
            </button>
          ))}
        </div>
      </div>

      {/* Series toggles */}
      <div className="flex flex-wrap gap-1.5 mb-3">
        {SERIES.map((s) => {
          const on = visible[s.key];
          return (
            <button
              key={s.key}
              type="button"
              onClick={() => setVisible((prev) => ({ ...prev, [s.key]: !prev[s.key] }))}
              className={cn(
                "text-[10px] font-medium px-2 py-0.5 rounded flex items-center gap-1.5 transition-opacity",
                on ? "opacity-100" : "opacity-40",
              )}
              style={{ background: `${s.color}20`, color: s.color }}
            >
              <span
                className="w-2 h-2 rounded-full"
                style={{ background: s.color }}
              />
              {s.label}
            </button>
          );
        })}
      </div>

      {loading && (
        <div className="h-80 flex items-center justify-center text-xs text-text-secondary">
          Loading history…
        </div>
      )}

      {error && !loading && (
        <div className="h-80 flex items-center justify-center text-xs text-loss">
          ⚠ {error}
        </div>
      )}

      {!loading && !error && chartData.length === 0 && (
        <div className="h-80 flex items-center justify-center text-xs text-text-secondary">
          No history yet — populates after brain runs accumulate.
        </div>
      )}

      {!loading && !error && chartData.length > 0 && (
        <ResponsiveContainer width="100%" height={height}>
          <LineChart data={chartData} margin={{ top: 5, right: 12, bottom: 5, left: -10 }}>
            <CartesianGrid stroke="#1f2937" strokeDasharray="3 3" />
            <XAxis
              dataKey="tick"
              tick={{ fill: "#9ca3af", fontSize: 10 }}
              tickLine={false}
              axisLine={{ stroke: "#1f2937" }}
              minTickGap={20}
            />
            <YAxis
              domain={[0, 100]}
              tick={{ fill: "#9ca3af", fontSize: 10 }}
              tickLine={false}
              axisLine={{ stroke: "#1f2937" }}
              ticks={[0, 25, 50, 75, 100]}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: "#111827",
                border: "1px solid #1f2937",
                borderRadius: 8,
                fontSize: 11,
              }}
              labelStyle={{ color: "#9ca3af" }}
              formatter={(v, name) => {
                const num = typeof v === "number" ? v : Number(v);
                if (v == null || Number.isNaN(num)) return ["—", String(name)];
                return [num.toFixed(1), String(name)];
              }}
            />
            <Legend wrapperStyle={{ display: "none" }} />
            {SERIES.filter((s) => visible[s.key]).map((s) => (
              <Line
                key={s.key}
                type="monotone"
                dataKey={s.key}
                name={s.label}
                stroke={s.color}
                strokeWidth={1.75}
                dot={false}
                connectNulls
                isAnimationActive={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      )}

      <p className="text-[10px] text-text-secondary text-right mt-2 opacity-70">
        {chartData.length} points · BQ · last {days}d
      </p>
    </div>
  );
}

function formatTick(ts: string, days: number): string {
  try {
    const d = new Date(ts);
    if (Number.isNaN(d.getTime())) return ts;
    if (days <= 1) {
      return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", hour12: false });
    }
    return `${d.toLocaleDateString([], { month: "short", day: "2-digit" })} ${d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", hour12: false })}`;
  } catch {
    return ts;
  }
}
