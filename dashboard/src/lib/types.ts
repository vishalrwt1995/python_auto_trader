/* ── Market Brain ── */

export type Regime = "TREND_UP" | "TREND_DOWN" | "RANGE" | "CHOP" | "PANIC" | "RECOVERY";
export type RiskMode = "NORMAL" | "AGGRESSIVE" | "DEFENSIVE" | "LOCKDOWN";
export type Participation = "STRONG" | "MODERATE" | "WEAK";
export type SwingPermission = "ENABLED" | "REDUCED" | "DISABLED";
export type MarketPhase = "PREMARKET" | "POST_OPEN" | "LIVE" | "EOD";

export interface MarketBrainState {
  asof_ts: string;
  phase: MarketPhase;
  regime: Regime;
  sub_regime_v2: string;
  structure_state: string;
  recovery_state: string;
  event_state: string;
  participation: Participation;
  risk_mode: RiskMode;
  intraday_state: string;
  run_degraded_flag: boolean;
  long_bias: number;
  short_bias: number;
  size_multiplier: number;
  max_positions_multiplier: number;
  swing_permission: SwingPermission;
  allowed_strategies: string[];
  reasons: string[];
  trend_score: number;
  breadth_score: number;
  leadership_score: number;
  volatility_stress_score: number;
  liquidity_health_score: number;
  data_quality_score: number;
  market_confidence: number;
  breadth_confidence: number;
  leadership_confidence: number;
  phase2_confidence: number;
  policy_confidence: number;
  run_integrity_confidence: number;
  updated_at?: { seconds: number; nanoseconds: number };
}

export interface BrainHistoryRow {
  _id?: string;
  asof_ts: string;
  regime: Regime;
  sub_regime_v2: string;
  risk_mode: RiskMode;
  participation: Participation;
  market_confidence: number;
  trend_score: number;
  breadth_score: number;
  volatility_stress_score: number;
}

/* ── Watchlist ── */

export interface WatchlistRow {
  symbol: string;
  exchange: string;
  enabled: string;
  setup: string;
  sector: string;
  macro_sector?: string;
  beta: number;
  reason: string;
  score?: number;
  eligible_swing?: boolean;
  eligible_intraday?: boolean;
  wl_type?: "swing" | "intraday" | string;
  vwap_bias?: string;
  liquidity_bucket?: string;
  turnover_rank?: number | null;
  phase2_eligible?: boolean;
}

export interface WatchlistDoc {
  rows: WatchlistRow[];
  regime?: string;
  risk_mode?: string;
  run_block?: string;
  generated_at?: string;
  run_date?: string;
  selected?: number;
  symbols?: string[];
  updated_at?: { seconds: number; nanoseconds: number };
}

/* ── Positions ── */

export interface Position {
  position_tag: string;
  symbol: string;
  exchange: string;
  segment: string;
  side: "BUY" | "SELL";
  qty: number;
  entry_price: number;
  sl_price: number;
  target: number;
  atr: number;
  strategy?: string;
  order_id?: string;
  regime?: string;
  risk_mode?: string;
  signal_score?: number;
  status: "OPEN" | "CLOSED" | "PENDING_AMO_EXIT";
  wl_type?: "swing" | "intraday" | string;
  product?: string;
  gtt_sl_id?: string;
  exit_price?: number;
  exit_reason?: string;
  entry_ts?: string;
  exit_ts?: string;
  updated_at?: { seconds: number; nanoseconds: number };
}

/* ── Orders ── */

export interface Order {
  ref_id: string;
  status: string;
  filled_qty?: number;
  avg_fill_price?: number;
  order_id?: string;
  updated_at?: { seconds: number; nanoseconds: number };
}

export interface PendingOrder {
  kind: string;
  ref_id: string;
  symbol?: string;
  side?: string;
  qty?: number;
  updated_at?: { seconds: number; nanoseconds: number };
}

/* ── Signals (from BQ via API) ── */

export interface Signal {
  scan_ts: string;
  run_date: string;
  symbol: string;
  direction: string;
  score: number;
  ltp: number;
  sl: number;
  target: number;
  qty: number;
  regime: string;
  risk_mode: string;
  entry_placed: boolean;
  blocked_reason: string;
  scanner_run_id: string;
}

/* ── Scan audit row (from Firestore scan_results/latest) ── */
export interface ScanRow {
  symbol: string;
  ltp: number;
  changePct: number;
  volRatio: number;
  direction: string;    // BUY | SELL | HOLD | SKIP
  score: number;
  emaState: string;
  rsi: number;
  macdView: string;
  supertrend: string;
  setup: string;
  vwap?: number;
  sl?: number;
  target?: number;
  qty?: number;
  status: string;       // qualified | filtered | skip
  reason: string;
  wl_type?: "swing" | "intraday" | string;
  daily_trend?: string; // UP | DOWN | NEUTRAL
  affinity_mult?: number;
  score_alignment?: number;
  // Score breakdown fields (written by trading_service)
  minScore?: number;
  affinityMult?: number;
  atrMult?: number;
  dailyStrength?: number;
}

export interface ScanLatest {
  scan_ts: string;
  run_date: string;
  scanner_run_id: string;
  regime: string;
  risk_mode: string;
  total_watchlist: number;
  scanned: number;
  qualified: number;
  rows: ScanRow[];
}

/* ── Trades (from BQ via API) ── */

export interface Trade {
  trade_date: string;
  position_tag: string;
  symbol: string;
  side: string;
  qty: number;
  entry_price: number;
  exit_price: number;
  sl_price: number;
  target: number;
  pnl: number;
  pnl_pct: number;
  exit_reason: string;
  strategy: string;
  entry_ts: string;
  exit_ts: string;
  hold_minutes: number;
  regime: string;
  risk_mode: string;
  market_confidence: number;
  signal_score: number;
}

/* ── Audit Log ── */

export interface AuditLogEntry {
  log_ts: string;
  run_date: string;
  module: string;
  action: string;
  status: string;
  message: string;
  context: Record<string, unknown>;
  exec_id: string;
  scheduler_job?: string;
}

/* ── Trade Summary (from API) ── */

export interface TradeSummary {
  total_pnl: number;
  realized_pnl: number;
  unrealized_pnl: number;
  win_rate: number;
  total_trades: number;
  avg_rr: number;
  biggest_win: number;
  biggest_loss: number;
  profit_factor: number | null;  // null = no losing trades (render as ∞)
  max_drawdown: number;
  max_drawdown_pct: number;
  expectancy: number;
}

/* ── User / Auth ── */

export type UserRole = "admin" | "viewer";

export interface AppUser {
  uid: string;
  email: string;
  displayName: string;
  role: UserRole;
  photoURL?: string;
}
