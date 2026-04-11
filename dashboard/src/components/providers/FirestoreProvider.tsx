"use client";

import { useEffect, type ReactNode } from "react";
import { useMarketBrain } from "@/hooks/useMarketBrain";
import { useMarketBrainHistory } from "@/hooks/useMarketBrainHistory";
import { useWatchlist } from "@/hooks/useWatchlist";
import { usePositions } from "@/hooks/usePositions";
import { useVoiceAlert } from "@/hooks/useVoiceAlert";
import { useLtpPolling } from "@/hooks/useLtpPolling";
import { useDashboardStore } from "@/stores/dashboardStore";
import type { MarketBrainState, WatchlistRow, BrainHistoryRow } from "@/lib/types";

const DEV_BRAIN: MarketBrainState = {
  asof_ts: new Date().toISOString(),
  phase: "LIVE",
  regime: "TREND_UP",
  sub_regime_v2: "STRONG_TREND",
  structure_state: "BREAKOUT_CONFIRMED",
  recovery_state: "NONE",
  event_state: "NONE",
  participation: "STRONG",
  risk_mode: "NORMAL",
  intraday_state: "TRENDING",
  run_degraded_flag: false,
  long_bias: 0.72,
  short_bias: 0.28,
  size_multiplier: 1.15,
  max_positions_multiplier: 1.2,
  swing_permission: "ENABLED",
  allowed_strategies: ["BREAKOUT", "PULLBACK", "VWAP_TREND"],
  reasons: [
    "NIFTY above 20/50 EMA — bull stack confirmed",
    "Breadth STRONG: 68% stocks above 20 EMA",
    "ADX=32 — ENABLED trend confirmed",
    "VIX=13.2 below safe threshold of 18",
    "PCR=0.82 — moderately bullish",
    "MEAN_REVERSION DISABLED in trending regime",
    "WARNING: FII data delayed by 1 session",
  ],
  trend_score: 78,
  breadth_score: 65,
  leadership_score: 71,
  volatility_stress_score: 22,
  liquidity_health_score: 83,
  data_quality_score: 91,
  market_confidence: 74,
  breadth_confidence: 68,
  leadership_confidence: 71,
  phase2_confidence: 60,
  policy_confidence: 82,
  run_integrity_confidence: 94,
};

const DEV_WATCHLIST: WatchlistRow[] = [
  { symbol: "RELIANCE", exchange: "NSE", enabled: "true", setup: "BREAKOUT", sector: "Energy", beta: 0.9, reason: "", score: 82, eligible_swing: true, eligible_intraday: false, wl_type: "swing", liquidity_bucket: "A", vwap_bias: "ABOVE", phase2_eligible: false },
  { symbol: "HDFCBANK", exchange: "NSE", enabled: "true", setup: "PULLBACK", sector: "Banking", beta: 0.8, reason: "", score: 74, eligible_swing: false, eligible_intraday: true, wl_type: "intraday", liquidity_bucket: "A", vwap_bias: "ABOVE", phase2_eligible: true },
  { symbol: "INFY",     exchange: "NSE", enabled: "true", setup: "VWAP_TREND", sector: "IT", beta: 0.7, reason: "", score: 68, eligible_swing: false, eligible_intraday: true, wl_type: "intraday", liquidity_bucket: "A", vwap_bias: "NEAR", phase2_eligible: false },
  { symbol: "TCS",      exchange: "NSE", enabled: "true", setup: "BREAKOUT", sector: "IT", beta: 0.75, reason: "", score: 79, eligible_swing: true, eligible_intraday: false, wl_type: "swing", liquidity_bucket: "A", vwap_bias: "ABOVE", phase2_eligible: false },
  { symbol: "ICICIBANK",exchange: "NSE", enabled: "true", setup: "PULLBACK", sector: "Banking", beta: 0.85, reason: "", score: 71, eligible_swing: false, eligible_intraday: true, wl_type: "intraday", liquidity_bucket: "A", vwap_bias: "ABOVE", phase2_eligible: true },
];

const DEV_HISTORY: BrainHistoryRow[] = [
  { asof_ts: new Date(Date.now() - 0).toISOString(),         regime: "TREND_UP",   sub_regime_v2: "STRONG_TREND",    risk_mode: "NORMAL",    participation: "STRONG",   market_confidence: 74, trend_score: 78, breadth_score: 65, volatility_stress_score: 22 },
  { asof_ts: new Date(Date.now() - 3600000).toISOString(),   regime: "TREND_UP",   sub_regime_v2: "STRONG_TREND",    risk_mode: "NORMAL",    participation: "STRONG",   market_confidence: 71, trend_score: 75, breadth_score: 62, volatility_stress_score: 25 },
  { asof_ts: new Date(Date.now() - 86400000).toISOString(),  regime: "RANGE",      sub_regime_v2: "RANGE_BOUND",     risk_mode: "DEFENSIVE", participation: "MODERATE", market_confidence: 52, trend_score: 45, breadth_score: 48, volatility_stress_score: 41 },
  { asof_ts: new Date(Date.now() - 172800000).toISOString(), regime: "RANGE",      sub_regime_v2: "RANGE_BOUND",     risk_mode: "DEFENSIVE", participation: "WEAK",     market_confidence: 44, trend_score: 38, breadth_score: 41, volatility_stress_score: 55 },
  { asof_ts: new Date(Date.now() - 259200000).toISOString(), regime: "CHOP",       sub_regime_v2: "CHOP",            risk_mode: "LOCKDOWN",  participation: "WEAK",     market_confidence: 30, trend_score: 22, breadth_score: 30, volatility_stress_score: 72 },
  { asof_ts: new Date(Date.now() - 345600000).toISOString(), regime: "RECOVERY",   sub_regime_v2: "EARLY_RECOVERY",  risk_mode: "NORMAL",    participation: "MODERATE", market_confidence: 55, trend_score: 50, breadth_score: 52, volatility_stress_score: 35 },
  { asof_ts: new Date(Date.now() - 432000000).toISOString(), regime: "TREND_UP",   sub_regime_v2: "MODERATE_TREND",  risk_mode: "NORMAL",    participation: "STRONG",   market_confidence: 68, trend_score: 70, breadth_score: 60, volatility_stress_score: 28 },
];

function DevDataSeeder() {
  const setMarketBrain = useDashboardStore((s) => s.setMarketBrain);
  const setWatchlist = useDashboardStore((s) => s.setWatchlist);
  const setBrainHistory = useDashboardStore((s) => s.setBrainHistory);
  useEffect(() => {
    setMarketBrain(DEV_BRAIN);
    setWatchlist(DEV_WATCHLIST);
    setBrainHistory(DEV_HISTORY);
  }, [setMarketBrain, setWatchlist, setBrainHistory]);
  return null;
}

/**
 * Subscribes to core Firestore collections and syncs to Zustand.
 * Mount once in the root layout so all pages share the same listeners.
 */
function LiveFirestoreHooks() {
  useMarketBrain();
  useMarketBrainHistory();
  useWatchlist();
  usePositions("OPEN");
  useVoiceAlert();
  useLtpPolling();
  return null;
}

export function FirestoreProvider({ children }: { children: ReactNode }) {
  const skipAuth = process.env.NEXT_PUBLIC_SKIP_AUTH === "true";

  return (
    <>
      {skipAuth ? <DevDataSeeder /> : <LiveFirestoreHooks />}
      {children}
    </>
  );
}
