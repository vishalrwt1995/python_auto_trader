"use client";

import { cn } from "@/lib/utils";

interface ConfidenceGaugeProps {
  value: number; // 0-100
  label: string;
  size?: number;
}

export function ConfidenceGauge({ value, label, size = 90 }: ConfidenceGaugeProps) {
  const pct = Math.round(Math.max(0, Math.min(100, value)));
  const color = pct > 65 ? "#22c55e" : pct > 40 ? "#f59e0b" : "#ef4444";
  const r = 38;
  const circumference = 2 * Math.PI * r;
  const offset = circumference - (pct / 100) * circumference;

  return (
    <div className="text-center">
      <svg width={size} height={size} viewBox="0 0 100 100">
        <circle cx="50" cy="50" r={r} fill="none" stroke="#1e293b" strokeWidth="8" />
        <circle
          cx="50"
          cy="50"
          r={r}
          fill="none"
          stroke={color}
          strokeWidth="8"
          strokeDasharray={circumference}
          strokeDashoffset={offset}
          strokeLinecap="round"
          transform="rotate(-90 50 50)"
          className="transition-all duration-1000 ease-out"
        />
        <text
          x="50"
          y="46"
          textAnchor="middle"
          fill="#f1f5f9"
          fontSize="22"
          fontWeight="700"
          fontFamily="'JetBrains Mono', monospace"
        >
          {pct}
        </text>
        <text
          x="50"
          y="62"
          textAnchor="middle"
          fill="#64748b"
          fontSize="9"
          fontFamily="sans-serif"
        >
          / 100
        </text>
      </svg>
      <p className="text-[11px] text-text-secondary mt-0.5">{label}</p>
    </div>
  );
}
