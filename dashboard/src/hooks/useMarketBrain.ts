"use client";

import { useEffect, useRef } from "react";
import { useFirestoreDoc } from "./useFirestore";
import { useDashboardStore } from "@/stores/dashboardStore";
import { voiceEngine } from "@/lib/voice";
import { useSettingsStore } from "@/stores/settingsStore";
import type { MarketBrainState, MarketBrainNarrative } from "@/lib/types";

/**
 * The Firestore doc `market_brain/latest` stores data in nested maps:
 *   { state: { regime, risk_mode, ... }, policy: { ... }, context: { ... }, narrative: { ... } }
 * The dashboard expects a flat MarketBrainState, so we unwrap `state`.
 * PR-2: `narrative` (if present) is pushed into the store for NarrativeCard.
 */
interface MarketBrainDoc {
  state?: MarketBrainState;
  policy?: Record<string, unknown>;
  context?: Record<string, unknown>;
  narrative?: MarketBrainNarrative;
  updated_at?: unknown;
}

export function useMarketBrain() {
  const { data: rawDoc, loading, error } = useFirestoreDoc<MarketBrainDoc>(
    "market_brain",
    "latest",
  );
  const setMarketBrain = useDashboardStore((s) => s.setMarketBrain);
  const setBrainNarrative = useDashboardStore((s) => s.setBrainNarrative);
  const voiceEnabled = useSettingsStore((s) => s.voiceEnabled);
  const prevRegime = useRef<string | null>(null);

  // Unwrap: if doc has a `state` sub-map, use that; otherwise treat the doc itself as flat
  const data: MarketBrainState | null = rawDoc?.state
    ? (rawDoc.state as MarketBrainState)
    : (rawDoc as unknown as Record<string, unknown>)?.regime
      ? (rawDoc as unknown as MarketBrainState)
      : null;

  const narrative: MarketBrainNarrative | null = rawDoc?.narrative ?? null;

  useEffect(() => {
    setMarketBrain(data);
    setBrainNarrative(narrative);

    if (data && voiceEnabled && voiceEngine) {
      if (prevRegime.current !== null && prevRegime.current !== data.regime) {
        voiceEngine.regimeChange(data.regime, data.risk_mode);
      }
      prevRegime.current = data.regime;
    }
  }, [data, narrative, setMarketBrain, setBrainNarrative, voiceEnabled]);

  return { data, narrative, loading, error };
}
