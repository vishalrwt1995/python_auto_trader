"use client";

import type { Regime } from "@/lib/types";

const regimeColors: Record<string, string> = {
  TREND_UP: "#22c55e",
  TREND_DOWN: "#ef4444",
  RANGE: "#f59e0b",
  CHOP: "#6b7280",
  PANIC: "#dc2626",
  RECOVERY: "#3b82f6",
};

interface Props {
  regime: Regime;
  size?: "sm" | "md" | "lg";
}

export function RegimeBadge({ regime, size = "md" }: Props) {
  const color = regimeColors[regime] ?? "#6b7280";
  const text = regime.replace(/_/g, " ");
  const fontSize = size === "sm" ? 10 : size === "lg" ? 14 : 12;
  const padding = size === "sm" ? "3px 8px" : size === "lg" ? "5px 14px" : "4px 12px";

  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 6,
        padding,
        borderRadius: 6,
        fontSize,
        fontWeight: 700,
        letterSpacing: 1,
        textTransform: "uppercase",
        background: `${color}18`,
        color,
        border: `1px solid ${color}40`,
        boxShadow: `0 0 12px ${color}30`,
      }}
    >
      {/* LiveDot */}
      <span
        style={{
          display: "inline-block",
          width: 8,
          height: 8,
          borderRadius: "50%",
          background: color,
          boxShadow: `0 0 6px ${color}`,
          animation: "pulse 2s infinite",
        }}
      />
      {text}
    </span>
  );
}
