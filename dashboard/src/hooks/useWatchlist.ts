"use client";

import { useEffect } from "react";
import { useFirestoreDoc } from "./useFirestore";
import { useDashboardStore } from "@/stores/dashboardStore";
import type { WatchlistDoc, WatchlistRow } from "@/lib/types";

/**
 * Firestore watchlist rows use lowercase keys (setuplabel, confidence, pricelast, etc.)
 * while the dashboard WatchlistRow interface uses snake_case (setup, score, beta, etc.).
 * This mapper bridges the two.
 */
function mapRow(raw: Record<string, unknown>): WatchlistRow {
  return {
    symbol: String(raw.symbol ?? ""),
    exchange: String(raw.exchange ?? ""),
    enabled: String(raw.enabled ?? ""),
    setup: String(raw.setuplabel ?? raw.setup ?? ""),
    sector: String(raw.sector ?? ""),
    beta: 0, // not available in current pipeline output
    reason: String(raw.reason ?? ""),
    score: Number(raw.confidence ?? raw.score ?? 0),
    eligible_swing: String(raw.source ?? "").includes("DAILY") || String(raw.phase2eligibility ?? "") === "Y",
    eligible_intraday: String(raw.intradayscorev2 ?? "0") !== "0" && Number(raw.intradayscorev2 ?? 0) > 50,
  };
}

export function useWatchlist() {
  const { data, loading, error } = useFirestoreDoc<WatchlistDoc>(
    "watchlist",
    "latest",
  );
  const setWatchlist = useDashboardStore((s) => s.setWatchlist);

  useEffect(() => {
    const rawRows = (data?.rows ?? []) as unknown as Record<string, unknown>[];
    setWatchlist(rawRows.map(mapRow));
  }, [data, setWatchlist]);

  return { data, loading, error };
}
