import type { Regime, RiskMode, Participation } from "./types";

export const REGIME_COLORS: Record<Regime, string> = {
  TREND_UP: "#22c55e",
  TREND_DOWN: "#ef4444",
  RANGE: "#f59e0b",
  CHOP: "#6b7280",
  PANIC: "#dc2626",
  RECOVERY: "#3b82f6",
};

export const REGIME_BG: Record<Regime, string> = {
  TREND_UP: "bg-regime-trend-up",
  TREND_DOWN: "bg-regime-trend-down",
  RANGE: "bg-regime-range",
  CHOP: "bg-regime-chop",
  PANIC: "bg-regime-panic regime-panic",
  RECOVERY: "bg-regime-recovery",
};

export const REGIME_LABELS: Record<Regime, string> = {
  TREND_UP: "Trend Up",
  TREND_DOWN: "Trend Down",
  RANGE: "Range",
  CHOP: "Chop",
  PANIC: "Panic",
  RECOVERY: "Recovery",
};

export const RISK_MODE_COLORS: Record<RiskMode, string> = {
  NORMAL: "#22c55e",
  AGGRESSIVE: "#f59e0b",
  DEFENSIVE: "#f97316",
  LOCKDOWN: "#ef4444",
};

export const RISK_MODE_BG: Record<RiskMode, string> = {
  NORMAL: "bg-risk-normal",
  AGGRESSIVE: "bg-risk-aggressive",
  DEFENSIVE: "bg-risk-defensive",
  LOCKDOWN: "bg-risk-lockdown",
};

export const PARTICIPATION_COLORS: Record<Participation, string> = {
  STRONG: "#22c55e",
  MODERATE: "#f59e0b",
  WEAK: "#ef4444",
};

export const NAV_ITEMS = [
  { label: "Command Center", href: "/", icon: "LayoutDashboard" },
  { label: "Market Brain", href: "/market-brain", icon: "Brain" },
  { label: "Watchlist", href: "/watchlist", icon: "List" },
  { label: "Positions", href: "/positions", icon: "TrendingUp" },
  { label: "Signals", href: "/signals", icon: "Zap" },
  { label: "Journal", href: "/journal", icon: "BookOpen" },
  { label: "Universe", href: "/universe", icon: "Globe" },
  { label: "Pipeline", href: "/pipeline", icon: "Activity" },
  { label: "Analytics", href: "/analytics", icon: "BarChart3" },
  { label: "Settings", href: "/settings", icon: "Settings" },
] as const;

export const MOBILE_NAV_ITEMS = [
  { label: "Home", href: "/", icon: "LayoutDashboard" },
  { label: "Brain", href: "/market-brain", icon: "Brain" },
  { label: "Watchlist", href: "/watchlist", icon: "List" },
  { label: "Positions", href: "/positions", icon: "TrendingUp" },
  { label: "More", href: "#more", icon: "Menu" },
] as const;

export const MARKET_OPEN_HOUR = 9;
export const MARKET_OPEN_MINUTE = 15;
export const MARKET_CLOSE_HOUR = 15;
export const MARKET_CLOSE_MINUTE = 30;
export const LTP_POLL_INTERVAL_MS = 5000;
