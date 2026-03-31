"use client";

import { cn } from "@/lib/utils";

interface Props {
  label: string;
  value: number;
  max?: number;
  size?: "sm" | "md";
}

export function ScoreGauge({ label, value, max = 100, size = "md" }: Props) {
  const pct = Math.min(100, Math.max(0, (value / max) * 100));
  const color =
    pct >= 60 ? "bg-profit" : pct >= 30 ? "bg-neutral" : "bg-loss";

  return (
    <div className={cn(size === "sm" ? "space-y-0.5" : "space-y-1")}>
      <div className="flex justify-between items-center">
        <span
          className={cn(
            "text-text-secondary",
            size === "sm" ? "text-[10px]" : "text-xs",
          )}
        >
          {label}
        </span>
        <span
          className={cn(
            "font-mono",
            size === "sm" ? "text-[10px]" : "text-xs",
          )}
        >
          {value.toFixed(0)}
        </span>
      </div>
      <div
        className={cn(
          "w-full bg-bg-tertiary rounded-full overflow-hidden",
          size === "sm" ? "h-1" : "h-1.5",
        )}
      >
        <div
          className={cn("h-full rounded-full transition-all duration-500", color)}
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  );
}
