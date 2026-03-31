"use client";

import { useEffect } from "react";
import { useFirestoreDoc } from "./useFirestore";
import { useDashboardStore } from "@/stores/dashboardStore";
import type { WatchlistDoc } from "@/lib/types";

export function useWatchlist() {
  const { data, loading, error } = useFirestoreDoc<WatchlistDoc>(
    "watchlist",
    "latest",
  );
  const setWatchlist = useDashboardStore((s) => s.setWatchlist);

  useEffect(() => {
    setWatchlist(data?.rows ?? []);
  }, [data, setWatchlist]);

  return { data, loading, error };
}
