"use client";

import { cn } from "@/lib/utils";

interface Props {
  status: "online" | "offline" | "warning";
  className?: string;
}

export function LiveDot({ status, className }: Props) {
  return (
    <span className={cn("relative flex h-2.5 w-2.5", className)}>
      {status === "online" && (
        <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-profit opacity-75" />
      )}
      <span
        className={cn(
          "relative inline-flex rounded-full h-2.5 w-2.5",
          status === "online" && "bg-profit",
          status === "offline" && "bg-loss",
          status === "warning" && "bg-neutral",
        )}
      />
    </span>
  );
}
