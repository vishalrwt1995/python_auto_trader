"use client";

import { useEffect } from "react";
import { useFirestoreDoc } from "./useFirestore";
import { useDashboardStore } from "@/stores/dashboardStore";
import type { WatchlistDoc, WatchlistRow } from "@/lib/types";

/**
 * Firestore watchlist rows use lowercase keys (setuplabel, confidence, wlType, etc.)
 * while the dashboard WatchlistRow interface uses snake_case (setup, score, beta, etc.).
 * wlType = "swing" | "intraday" — written by build_watchlist() Firestore path.
 */
function mapRow(raw: Record<string, unknown>): WatchlistRow {
  const wlType = String(raw.wlType ?? "");
  const source = String(raw.source ?? "");
  const setupLabel = String(raw.setuplabel ?? raw.setup ?? raw.setupLabel ?? "");
  return {
    symbol: String(raw.symbol ?? ""),
    exchange: String(raw.exchange ?? ""),
    enabled: String(raw.enabled ?? ""),
    setup: setupLabel,
    sector: String(raw.sector ?? ""),
    macro_sector: String(raw.macroSector ?? raw.macro_sector ?? ""),
    beta: 0, // not available in current pipeline output
    reason: String(raw.reason ?? setupLabel),
    score: Number(raw.confidence ?? raw.score ?? 0),
    wl_type: wlType || (source.includes("SWING") ? "swing" : "intraday"),
    vwap_bias: raw.vwapBias != null ? String(raw.vwapBias) : undefined,
    liquidity_bucket: raw.liquidityBucket != null ? String(raw.liquidityBucket) : undefined,
    turnover_rank: raw.turnoverRank60D != null ? Number(raw.turnoverRank60D) : null,
    phase2_eligible: String(raw.phase2eligibility ?? "") === "Y",
    eligible_swing: wlType === "swing" || source.includes("SWING"),
    eligible_intraday: wlType === "intraday" || (wlType === "" && !source.includes("SWING")),
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
