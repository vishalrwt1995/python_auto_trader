"use client";

import { useEffect } from "react";
import { useFirestoreCollection } from "./useFirestore";
import { useDashboardStore } from "@/stores/dashboardStore";
import type { BrainHistoryRow } from "@/lib/types";

export function useMarketBrainHistory() {
  const { data } = useFirestoreCollection<BrainHistoryRow>("market_brain_history", {
    orderByField: "asof_ts",
    orderByDir: "desc",
    limitCount: 30,
  });
  const setBrainHistory = useDashboardStore((s) => s.setBrainHistory);

  useEffect(() => {
    if (data?.length) setBrainHistory(data);
  }, [data, setBrainHistory]);
}
