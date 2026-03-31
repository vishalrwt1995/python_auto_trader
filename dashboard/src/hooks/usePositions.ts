"use client";

import { useEffect, useRef } from "react";
import { useFirestoreCollection } from "./useFirestore";
import { useDashboardStore } from "@/stores/dashboardStore";
import { voiceEngine } from "@/lib/voice";
import { useSettingsStore } from "@/stores/settingsStore";
import type { Position } from "@/lib/types";

export function usePositions(statusFilter?: "OPEN" | "CLOSED") {
  const filters = statusFilter
    ? [{ field: "status", op: "==" as const, value: statusFilter }]
    : undefined;

  const { data, loading, error } = useFirestoreCollection<Position>(
    "positions",
    filters,
  );
  const setPositions = useDashboardStore((s) => s.setPositions);
  const voiceEnabled = useSettingsStore((s) => s.voiceEnabled);
  const prevTags = useRef<Set<string>>(new Set());

  useEffect(() => {
    setPositions(data);

    if (voiceEnabled && voiceEngine) {
      const currentTags = new Set(data.map((p) => p.position_tag));

      // Detect new OPEN positions
      for (const p of data) {
        if (p.status === "OPEN" && !prevTags.current.has(p.position_tag)) {
          voiceEngine.positionOpened(
            p.side,
            p.symbol,
            p.entry_price,
            p.sl_price,
            p.target,
          );
        }
      }

      // Detect closed positions (tag disappeared or status changed)
      for (const p of data) {
        if (p.status === "CLOSED" && prevTags.current.has(p.position_tag)) {
          voiceEngine.positionClosed(
            p.symbol,
            p.exit_reason ?? "CLOSED",
            p.exit_price && p.entry_price
              ? ((p.exit_price - p.entry_price) / p.entry_price) * 100 * (p.side === "BUY" ? 1 : -1)
              : 0,
          );
        }
      }

      prevTags.current = currentTags;
    }
  }, [data, setPositions, voiceEnabled]);

  return { data, loading, error };
}
