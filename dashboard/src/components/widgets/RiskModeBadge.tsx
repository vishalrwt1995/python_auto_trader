"use client";

import type { RiskMode } from "@/lib/types";

const riskColors: Record<string, string> = {
  NORMAL: "#22c55e",
  AGGRESSIVE: "#f59e0b",
  DEFENSIVE: "#f97316",
  LOCKDOWN: "#ef4444",
};

interface Props {
  mode: RiskMode;
}

export function RiskModeBadge({ mode }: Props) {
  if (!mode) return null;
  const color = riskColors[mode] ?? "#6b7280";

  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 6,
        padding: "4px 12px",
        borderRadius: 6,
        fontSize: 12,
        fontWeight: 700,
        letterSpacing: 1,
        textTransform: "uppercase",
        background: `${color}18`,
        color,
        border: `1px solid ${color}40`,
      }}
    >
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
      {mode}
    </span>
  );
}
