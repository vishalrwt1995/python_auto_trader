"use client";

import { useEffect, useRef } from "react";
import { useFirestoreDoc } from "./useFirestore";
import { useDashboardStore } from "@/stores/dashboardStore";
import { voiceEngine } from "@/lib/voice";
import { useSettingsStore } from "@/stores/settingsStore";
import type { MarketBrainState } from "@/lib/types";

export function useMarketBrain() {
  const { data, loading, error } = useFirestoreDoc<MarketBrainState>(
    "market_brain",
    "latest",
  );
  const setMarketBrain = useDashboardStore((s) => s.setMarketBrain);
  const voiceEnabled = useSettingsStore((s) => s.voiceEnabled);
  const prevRegime = useRef<string | null>(null);

  useEffect(() => {
    setMarketBrain(data);

    if (data && voiceEnabled && voiceEngine) {
      if (prevRegime.current !== null && prevRegime.current !== data.regime) {
        voiceEngine.regimeChange(data.regime, data.risk_mode);
      }
      prevRegime.current = data.regime;
    }
  }, [data, setMarketBrain, voiceEnabled]);

  return { data, loading, error };
}
