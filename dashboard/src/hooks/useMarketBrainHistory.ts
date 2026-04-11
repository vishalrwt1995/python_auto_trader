"use client";

import { useEffect } from "react";
import { useFirestoreDoc } from "./useFirestore";
import { useDashboardStore } from "@/stores/dashboardStore";
import type { BrainHistoryRow } from "@/lib/types";

interface HistoryDoc {
  snapshots?: BrainHistoryRow[];
}

export function useMarketBrainHistory() {
  const { data } = useFirestoreDoc<HistoryDoc>("market_brain", "history");
  const setBrainHistory = useDashboardStore((s) => s.setBrainHistory);

  useEffect(() => {
    const snaps = data?.snapshots;
    if (snaps?.length) {
      // newest first for the table
      setBrainHistory([...snaps].reverse());
    }
  }, [data, setBrainHistory]);
}
