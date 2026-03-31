"use client";

import { cn } from "@/lib/utils";

interface Props {
  className?: string;
  lines?: number;
}

export function LoadingSkeleton({ className, lines = 3 }: Props) {
  return (
    <div className={cn("animate-pulse space-y-3", className)}>
      {Array.from({ length: lines }).map((_, i) => (
        <div
          key={i}
          className={cn(
            "h-4 bg-bg-tertiary rounded",
            i === lines - 1 && "w-3/4",
          )}
        />
      ))}
    </div>
  );
}
