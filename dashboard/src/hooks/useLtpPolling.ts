"use client";

import { useEffect, useRef } from "react";
import { useDashboardStore } from "@/stores/dashboardStore";
import { api } from "@/lib/api";
import { isMarketOpen } from "@/lib/utils";
import { LTP_POLL_INTERVAL_MS } from "@/lib/constants";

/**
 * Polls LTP for all open position symbols during market hours.
 * Updates the ltpCache in the dashboard store.
 */
export function useLtpPolling() {
  const positions = useDashboardStore((s) => s.positions);
  const updateLtp = useDashboardStore((s) => s.updateLtp);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    const poll = async () => {
      if (!isMarketOpen()) return;

      const symbols = positions
        .filter((p) => p.status === "OPEN")
        .map((p) => p.symbol);

      if (symbols.length === 0) return;

      try {
        const resp = await api.getLtp(symbols);
        // Backend returns { prices: { SYMBOL: ltp } } — unwrap the prices map
        const prices = (resp as unknown as { prices: Record<string, number> }).prices ?? resp;
        if (prices && typeof prices === "object") {
          updateLtp(prices as Record<string, number>);
        }
      } catch {
        // Silently ignore — will retry next interval
      }
    };

    poll();
    timerRef.current = setInterval(poll, LTP_POLL_INTERVAL_MS);

    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
    };
  }, [positions, updateLtp]);
}
