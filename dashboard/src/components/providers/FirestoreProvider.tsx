"use client";

import type { ReactNode } from "react";
import { useMarketBrain } from "@/hooks/useMarketBrain";
import { useWatchlist } from "@/hooks/useWatchlist";
import { usePositions } from "@/hooks/usePositions";
import { useVoiceAlert } from "@/hooks/useVoiceAlert";
import { useLtpPolling } from "@/hooks/useLtpPolling";

/**
 * Subscribes to core Firestore collections and syncs to Zustand.
 * Mount once in the root layout so all pages share the same listeners.
 */
export function FirestoreProvider({ children }: { children: ReactNode }) {
  useMarketBrain();
  useWatchlist();
  usePositions("OPEN");
  useVoiceAlert();
  useLtpPolling();

  return <>{children}</>;
}
