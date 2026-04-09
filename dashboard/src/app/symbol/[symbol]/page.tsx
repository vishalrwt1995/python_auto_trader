"use client";

import { useEffect, useState, useMemo, useRef, useCallback } from "react";
import { useParams, useRouter } from "next/navigation";
import { api } from "@/lib/api";
import { cn } from "@/lib/utils";
import { CandlestickChart } from "@/components/charts/CandlestickChart";
import {
  BarChart, Bar, XAxis, YAxis, ResponsiveContainer, Cell,
  LineChart, Line, Tooltip, ReferenceLine,
} from "recharts";
import { ArrowLeft, TrendingUp, TrendingDown, Minus, RefreshCw, AlertCircle } from "lucide-react";
import type { LineData } from "lightweight-charts";

// ── Types ─────────────────────────────────────────────────────────────────────
interface Candle { time: string; open: number; high: number; low: number; close: number; volume?: number }
interface UniverseData {
  exchange?: string; sector?: string; segment?: string; beta?: number;
  universe_score?: number; score_calc?: string; eligible_swing?: boolean;
  eligible_intraday?: boolean; liquidity_bucket?: string; atr_pct_14d?: number;
  atr_14?: number; turnover_med_60d?: number; turnover_rank_60d?: number;
  gap_risk_60d?: number; bars_1d?: number; last_1d_date?: string;
  data_quality_flag?: string; stale_days?: number; price_last?: number; disable_reason?: string; enabled?: boolean;
}
interface WatchlistData {
  setup?: string; vwap_bias?: string; phase2_eligible?: boolean;
  wl_type?: string; score?: number; reason?: string;
}
interface PositionData {
  side?: string; qty?: number; entry_price?: number; sl_price?: number;
  target?: number; atr?: number; strategy?: string; entry_ts?: string; status?: string;
}
interface SignalData {
  scan_ts?: string; direction?: string; score?: number; ltp?: number;
  sl?: number; target?: number; qty?: number; regime?: string; blocked_reason?: string;
}
interface TradeData {
  trade_date?: string; side?: string; qty?: number; entry_price?: number;
  exit_price?: number; pnl?: number; pnl_pct?: number; exit_reason?: string;
  strategy?: string; hold_minutes?: number; entry_ts?: string;
}
interface SymbolDetail {
  symbol: string;
  universe: UniverseData;
  watchlist: WatchlistData | null;
  position: PositionData | null;
  signals_today: SignalData[];
  recent_trades: TradeData[];
}

// ── Indicator helpers (client-side) ──────────────────────────────────────────
function ema(candles: Candle[], period: number): LineData[] {
  if (candles.length < period) return [];
  const k = 2 / (period + 1);
  let val = candles.slice(0, period).reduce((s, c) => s + c.close, 0) / period;
  return candles.map((c, i) => {
    if (i < period - 1) return null;
    if (i === period - 1) { return { time: c.time as any, value: +val.toFixed(2) }; }
    val = c.close * k + val * (1 - k);
    return { time: c.time as any, value: +val.toFixed(2) };
  }).filter(Boolean) as LineData[];
}

function vwap(candles: Candle[]): LineData[] {
  let cumPV = 0, cumV = 0;
  return candles.map((c) => {
    const tp = (c.high + c.low + c.close) / 3;
    const v = c.volume ?? 0;
    cumPV += tp * v; cumV += v;
    return { time: c.time as any, value: +(cumV > 0 ? cumPV / cumV : tp).toFixed(2) };
  });
}

function rsi14(candles: Candle[]): number {
  const period = 14;
  if (candles.length < period + 1) return 50;
  const closes = candles.map(c => c.close);
  const gains: number[] = [], losses: number[] = [];
  for (let i = 1; i < closes.length; i++) {
    const d = closes[i] - closes[i - 1];
    gains.push(Math.max(0, d)); losses.push(Math.max(0, -d));
  }
  let ag = gains.slice(0, period).reduce((a, b) => a + b) / period;
  let al = losses.slice(0, period).reduce((a, b) => a + b) / period;
  for (let i = period; i < gains.length; i++) {
    ag = (ag * (period - 1) + gains[i]) / period;
    al = (al * (period - 1) + losses[i]) / period;
  }
  return al === 0 ? 100 : +(100 - 100 / (1 + ag / al)).toFixed(1);
}

function computeReturns(candles: Candle[]): Record<string, number | null> {
  const last = candles[candles.length - 1]?.close ?? 0;
  const today = new Date();
  const cutoff = (days: number) => {
    const d = new Date(today); d.setDate(d.getDate() - days); return d.toISOString().slice(0, 10);
  };
  const findClose = (days: number) => {
    const cut = cutoff(days);
    const c = candles.filter(c => c.time <= cut).pop();
    return c?.close ?? null;
  };
  const ret = (base: number | null) => (base && last ? +((last - base) / base * 100).toFixed(2) : null);
  return {
    "1M": ret(findClose(30)), "3M": ret(findClose(90)),
    "6M": ret(findClose(180)), "1Y": ret(findClose(365)),
  };
}

function high52w(candles: Candle[]): number {
  return candles.slice(-252).reduce((m, c) => Math.max(m, c.high), 0);
}
function low52w(candles: Candle[]): number {
  return candles.slice(-252).reduce((m, c) => Math.min(m, c.low), Infinity);
}

// ── Score breakdown ───────────────────────────────────────────────────────────
const SCORE_LABELS: Record<string, string> = {
  E: "EMA Stack", P: "Price/RSI", R: "RSI", M: "MACD",
  B: "Breakout", V: "Volume", O: "OBV", N: "Penalty", U: "Priority", S: "Total",
};
function parseScore(calc: string) {
  return calc.split("|").map(p => ({ key: p[0], label: SCORE_LABELS[p[0]] ?? p[0], value: +p.slice(1) }));
}

// ── Small helpers ─────────────────────────────────────────────────────────────
const fmt = (n: number | undefined | null, digits = 1) =>
  n == null ? "—" : n.toLocaleString("en-IN", { maximumFractionDigits: digits });
const fmtPct = (n: number | undefined | null) =>
  n == null ? "—" : `${n >= 0 ? "+" : ""}${(n * 100).toFixed(1)}%`;
const fmtCr = (n: number | undefined | null) => {
  if (!n) return "—";
  const cr = n / 1e7;
  return cr >= 100 ? `₹${Math.round(cr)}Cr` : `₹${cr.toFixed(1)}Cr`;
};

const SETUP_COLOR: Record<string, string> = {
  BREAKOUT: "#22c55e", PULLBACK: "#3b82f6", MEAN_REVERSION: "#9ca3af",
  PHASE1_MOMENTUM: "#a78bfa", PHASE2_INPLAY: "#06b6d4", VWAP_TREND: "#06b6d4",
  VWAP_REVERSAL: "#f97316",
};
const LIQ_COLOR: Record<string, string> = { A: "#22c55e", B: "#3b82f6", C: "#f59e0b", D: "#ef4444" };

// ── RSI Gauge ─────────────────────────────────────────────────────────────────
function RsiGauge({ value }: { value: number }) {
  const color = value >= 70 ? "#ef4444" : value >= 60 ? "#22c55e" : value >= 45 ? "#3b82f6" : value >= 35 ? "#9ca3af" : "#ef4444";
  const label = value >= 70 ? "Overbought" : value >= 60 ? "Bullish" : value >= 45 ? "Neutral" : value >= 35 ? "Weak" : "Oversold";
  const angle = ((value - 0) / 100) * 180 - 90;
  return (
    <div className="flex flex-col items-center">
      <svg width={80} height={44} viewBox="0 0 80 44">
        <path d="M 5 40 A 35 35 0 0 1 75 40" fill="none" stroke="#1f2937" strokeWidth={6} />
        <path d="M 5 40 A 35 35 0 0 1 75 40" fill="none" stroke={color} strokeWidth={6}
          strokeDasharray={`${(value / 100) * 110} 110`} />
        <line x1="40" y1="40" x2={40 + 28 * Math.cos((angle * Math.PI) / 180)}
          y2={40 + 28 * Math.sin((angle * Math.PI) / 180)} stroke={color} strokeWidth={2} strokeLinecap="round" />
        <circle cx="40" cy="40" r="3" fill={color} />
      </svg>
      <p className="font-mono text-lg font-bold -mt-1" style={{ color }}>{value}</p>
      <p className="text-[10px] text-text-secondary">{label}</p>
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────
export default function SymbolPage() {
  const params = useParams();
  const router = useRouter();
  const symbol = (params?.symbol as string ?? "").toUpperCase();

  const [detail, setDetail] = useState<SymbolDetail | null>(null);
  const [candles, setCandles] = useState<Candle[]>([]);
  const [candles5m, setCandles5m] = useState<Candle[]>([]);
  const [ltp, setLtp] = useState<number | null>(null);
  const [prevClose, setPrevClose] = useState<number | null>(null);
  const [loading, setLoading] = useState(true);
  const [view, setView] = useState<"daily" | "5m">("daily");
  const [candleDays, setCandleDays] = useState(180);
  const [activeTab, setActiveTab] = useState<"overview" | "trades" | "signals">("overview");
  const ltpTimer = useRef<ReturnType<typeof setInterval> | null>(null);

  // Fetch all data in parallel
  useEffect(() => {
    if (!symbol) return;
    setLoading(true);
    Promise.all([
      api.getSymbolDetail(symbol),
      api.getCandles(symbol, "1d", 365),
      api.getCandles(symbol, "5m", 3),
    ]).then(([det, c1d, c5m]) => {
      setDetail(det as unknown as SymbolDetail);
      const daily = ((c1d as any).candles ?? []) as Candle[];
      const intra = ((c5m as any).candles ?? []) as Candle[];
      setCandles(daily);
      setCandles5m(intra);
      setPrevClose(daily.length >= 2 ? daily[daily.length - 2].close : null);
    }).catch(() => {}).finally(() => setLoading(false));
  }, [symbol]);

  // LTP polling every 5s
  const pollLtp = useCallback(() => {
    api.getLtp([symbol]).then((r) => {
      const p = (r as any).prices?.[symbol] ?? (r as any)[symbol];
      if (p > 0) setLtp(p);
    }).catch(() => {});
  }, [symbol]);

  useEffect(() => {
    pollLtp();
    ltpTimer.current = setInterval(pollLtp, 5000);
    return () => { if (ltpTimer.current) clearInterval(ltpTimer.current); };
  }, [pollLtp]);

  // Derived values
  const price = ltp ?? detail?.universe?.price_last ?? candles[candles.length - 1]?.close ?? 0;
  const chg = prevClose ? price - prevClose : null;
  const chgPct = prevClose && chg != null ? (chg / prevClose) * 100 : null;
  const isUp = chg == null ? null : chg >= 0;

  const displayCandles = useMemo(() => {
    if (view === "5m") return candles5m;
    const n = candleDays === 30 ? 30 : candleDays === 90 ? 90 : candleDays === 180 ? 180 : 365;
    return candles.slice(-n);
  }, [view, candles, candles5m, candleDays]);

  const ema20 = useMemo(() => ema(displayCandles, 20), [displayCandles]);
  const ema50 = useMemo(() => ema(displayCandles, 50), [displayCandles]);
  const vwapLine = useMemo(() => view === "5m" ? vwap(candles5m) : [], [view, candles5m]);
  const rsiValue = useMemo(() => rsi14(candles), [candles]);
  const returns = useMemo(() => computeReturns(candles), [candles]);
  const w52h = useMemo(() => high52w(candles), [candles]);
  const w52l = useMemo(() => low52w(candles), [candles]);
  const volMax = useMemo(() => Math.max(...displayCandles.map(c => c.volume ?? 0), 1), [displayCandles]);

  const overlays = view === "daily"
    ? [{ label: "EMA20", color: "#3b82f6", data: ema20 }, { label: "EMA50", color: "#f59e0b", data: ema50 }]
    : [{ label: "EMA20", color: "#3b82f6", data: ema20 }, { label: "VWAP", color: "#a78bfa", data: vwapLine }];

  const chartData = displayCandles.map(c => ({
    ...c, isUp: c.close >= c.open,
    dateLabel: view === "5m" ? c.time?.slice(11, 16) : c.time?.slice(5),
  }));

  const scoreComponents = useMemo(() => {
    const calc = detail?.universe?.score_calc;
    return calc ? parseScore(calc).filter(c => c.key !== "S") : [];
  }, [detail]);
  const totalScore = detail?.universe?.universe_score;

  const un = detail?.universe ?? {};
  const wl = detail?.watchlist;
  const pos = detail?.position;

  // EMA alignment signals
  const lastEma20 = ema20[ema20.length - 1]?.value ?? 0;
  const lastEma50 = ema50[ema50.length - 1]?.value ?? 0;
  const emaAligned = lastEma20 > 0 && lastEma50 > 0 && price > lastEma20 && lastEma20 > lastEma50;
  const aboveEma20 = price > lastEma20;
  const aboveEma50 = price > lastEma50;

  if (loading) {
    return (
      <div className="space-y-4">
        <div className="h-8 bg-bg-tertiary rounded animate-pulse w-48" />
        <div className="h-64 bg-bg-tertiary rounded animate-pulse" />
        <div className="grid grid-cols-4 gap-3">
          {[...Array(4)].map((_, i) => <div key={i} className="h-24 bg-bg-tertiary rounded animate-pulse" />)}
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-4 pb-8">
      {/* ── Back button ───────────────────────────────────────────────── */}
      <button onClick={() => router.back()} className="flex items-center gap-1.5 text-xs text-text-secondary hover:text-text-primary transition-colors">
        <ArrowLeft className="h-3.5 w-3.5" /> Back
      </button>

      {/* ── Hero: Price + identity ─────────────────────────────────────── */}
      <div className="bg-bg-secondary rounded-xl border border-bg-tertiary p-4">
        <div className="flex flex-wrap items-start justify-between gap-4">
          {/* Left: symbol + price */}
          <div>
            <div className="flex items-center gap-2 flex-wrap">
              <h1 className="text-2xl font-bold">{symbol}</h1>
              {un.exchange && <span className="text-xs bg-bg-tertiary px-2 py-0.5 rounded text-text-secondary">{un.exchange}</span>}
              {un.sector && <span className="text-xs bg-bg-tertiary px-2 py-0.5 rounded text-text-secondary">{un.sector}</span>}
              {un.liquidity_bucket && (
                <span className="text-xs font-bold px-2 py-0.5 rounded" style={{ color: LIQ_COLOR[un.liquidity_bucket], background: LIQ_COLOR[un.liquidity_bucket] + "22" }}>
                  Liq {un.liquidity_bucket}
                </span>
              )}
              {wl?.setup && (
                <span className="text-[10px] font-semibold px-2 py-0.5 rounded" style={{ color: SETUP_COLOR[wl.setup] ?? "#9ca3af", background: (SETUP_COLOR[wl.setup] ?? "#9ca3af") + "22" }}>
                  {wl.setup}
                </span>
              )}
            </div>
            <div className="flex items-baseline gap-3 mt-2">
              <span className="text-3xl font-mono font-bold">
                ₹{price > 0 ? price.toLocaleString("en-IN", { maximumFractionDigits: 1 }) : "—"}
              </span>
              {chg != null && (
                <span className={cn("text-sm font-medium flex items-center gap-1", isUp ? "text-profit" : "text-loss")}>
                  {isUp ? <TrendingUp className="h-3.5 w-3.5" /> : <TrendingDown className="h-3.5 w-3.5" />}
                  {chg >= 0 ? "+" : ""}{chg.toFixed(1)} ({chgPct?.toFixed(2)}%)
                </span>
              )}
              <span className="text-[10px] text-text-secondary flex items-center gap-1">
                <RefreshCw className="h-2.5 w-2.5" /> Live
              </span>
            </div>
          </div>

          {/* Right: day stats + 52w */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-x-6 gap-y-2 text-right">
            {[
              { label: "Day High", value: candles.length ? `₹${fmt(candles[candles.length - 1]?.high)}` : "—" },
              { label: "Day Low", value: candles.length ? `₹${fmt(candles[candles.length - 1]?.low)}` : "—" },
              { label: "52W High", value: w52h > 0 ? `₹${fmt(w52h)}` : "—" },
              { label: "52W Low", value: w52l < Infinity ? `₹${fmt(w52l)}` : "—" },
            ].map(s => (
              <div key={s.label}>
                <p className="text-[10px] text-text-secondary">{s.label}</p>
                <p className="text-sm font-mono font-medium">{s.value}</p>
              </div>
            ))}
          </div>
        </div>

        {/* 52w range bar */}
        {w52h > 0 && w52l < Infinity && (
          <div className="mt-3">
            <div className="flex justify-between text-[10px] text-text-secondary mb-1">
              <span>52W Low ₹{fmt(w52l)}</span>
              <span>{w52h > 0 ? `${(((price - w52l) / (w52h - w52l)) * 100).toFixed(0)}% of range` : ""}</span>
              <span>52W High ₹{fmt(w52h)}</span>
            </div>
            <div className="h-1.5 bg-bg-tertiary rounded-full overflow-hidden relative">
              <div className="h-full bg-gradient-to-r from-loss via-neutral to-profit rounded-full" style={{ width: "100%" }} />
              <div
                className="absolute top-1/2 -translate-y-1/2 w-2 h-2 rounded-full bg-white border-2 border-accent shadow"
                style={{ left: `${Math.min(98, Math.max(2, ((price - w52l) / (w52h - w52l)) * 100))}%`, transform: "translate(-50%, -50%)" }}
              />
            </div>
          </div>
        )}
      </div>

      {/* ── Eligibility + position banner ─────────────────────────────── */}
      <div className="flex flex-wrap gap-2">
        <span className={cn("text-xs px-2.5 py-1 rounded-full font-medium border", un.eligible_swing ? "bg-indigo-500/10 text-indigo-400 border-indigo-500/20" : "bg-bg-tertiary text-text-secondary border-transparent")}>
          {un.eligible_swing ? "✓" : "✗"} Swing
        </span>
        <span className={cn("text-xs px-2.5 py-1 rounded-full font-medium border", un.eligible_intraday ? "bg-cyan-500/10 text-cyan-400 border-cyan-500/20" : "bg-bg-tertiary text-text-secondary border-transparent")}>
          {un.eligible_intraday ? "✓" : "✗"} Intraday
        </span>
        {wl?.phase2_eligible && (
          <span className="text-xs px-2.5 py-1 rounded-full font-medium border bg-cyan-500/10 text-cyan-400 border-cyan-500/20">
            ✦ Phase 2
          </span>
        )}
        {wl?.vwap_bias && (
          <span className={cn("text-xs px-2.5 py-1 rounded-full font-medium border",
            wl.vwap_bias === "ABOVE" ? "bg-profit/10 text-profit border-profit/20" :
            wl.vwap_bias === "BELOW" ? "bg-loss/10 text-loss border-loss/20" :
            "bg-bg-tertiary text-text-secondary border-transparent")}>
            VWAP {wl.vwap_bias}
          </span>
        )}
        {pos && (
          <span className={cn("text-xs px-2.5 py-1 rounded-full font-medium border animate-pulse",
            pos.side === "BUY" ? "bg-profit/10 text-profit border-profit/20" : "bg-loss/10 text-loss border-loss/20")}>
            ● Open {pos.side} @ ₹{fmt(pos.entry_price)} · {pos.qty} qty
          </span>
        )}
      </div>

      {/* ── Main: Chart + Score ───────────────────────────────────────── */}
      <div className="grid grid-cols-1 xl:grid-cols-5 gap-4">
        {/* Chart (3/5) */}
        <div className="xl:col-span-3 bg-bg-secondary rounded-xl border border-bg-tertiary p-4">
          {/* Toolbar */}
          <div className="flex items-center justify-between flex-wrap gap-2 mb-3">
            <div className="flex items-center gap-2">
              <div className="flex rounded overflow-hidden border border-bg-tertiary text-[10px]">
                {(["daily", "5m"] as const).map(v => (
                  <button key={v} onClick={() => setView(v)}
                    className={cn("px-3 py-1.5 font-medium transition-colors", view === v ? "bg-accent text-white" : "bg-bg-tertiary text-text-secondary hover:text-text-primary")}>
                    {v === "daily" ? "Daily" : "5m Intraday"}
                  </button>
                ))}
              </div>
            </div>
            {view === "daily" && (
              <div className="flex gap-1">
                {([30, 90, 180, 365] as const).map(d => (
                  <button key={d} onClick={() => setCandleDays(d)}
                    className={cn("px-1.5 py-0.5 rounded text-[10px]", candleDays === d ? "bg-accent text-white" : "bg-bg-tertiary text-text-secondary hover:text-text-primary")}>
                    {d === 365 ? "1Y" : `${d}d`}
                  </button>
                ))}
              </div>
            )}
          </div>
          {/* Overlay legend */}
          <div className="flex gap-3 mb-2">
            {overlays.map(o => (
              <div key={o.label} className="flex items-center gap-1">
                <div className="w-4 h-0.5 rounded" style={{ backgroundColor: o.color }} />
                <span className="text-[10px] text-text-secondary">{o.label}</span>
              </div>
            ))}
          </div>
          {displayCandles.length === 0 ? (
            <div className="h-[280px] flex items-center justify-center text-xs text-text-secondary">No candle data</div>
          ) : (
            <>
              <CandlestickChart candles={displayCandles} overlays={overlays} height={280} />
              <div className="mt-1">
                <ResponsiveContainer width="100%" height={40}>
                  <BarChart data={chartData} margin={{ top: 0, right: 0, left: 0, bottom: 0 }}>
                    <YAxis domain={[0, volMax]} hide />
                    <XAxis dataKey="dateLabel" hide />
                    <Bar dataKey="volume" radius={[1, 1, 0, 0]} maxBarSize={6}>
                      {chartData.map((d, i) => <Cell key={i} fill={d.isUp ? "#22c55e" : "#ef4444"} fillOpacity={0.5} />)}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
                <p className="text-[9px] text-text-secondary text-right">Volume</p>
              </div>
            </>
          )}
        </div>

        {/* Score panel (2/5) */}
        <div className="xl:col-span-2 space-y-3">
          {/* Score */}
          <div className="bg-bg-secondary rounded-xl border border-bg-tertiary p-4">
            <div className="flex items-center justify-between mb-3">
              <span className="text-sm font-medium">Technical Score</span>
              {totalScore != null && (
                <span className="font-mono text-2xl font-bold px-3 py-1 rounded-lg"
                  style={{ color: totalScore >= 60 ? "#22c55e" : totalScore >= 40 ? "#f59e0b" : "#6b7280", background: (totalScore >= 60 ? "#22c55e" : totalScore >= 40 ? "#f59e0b" : "#6b7280") + "22" }}>
                  {totalScore}
                </span>
              )}
            </div>
            {scoreComponents.length > 0 ? (
              <div className="space-y-2">
                {scoreComponents.map(c => {
                  const neg = c.value < 0;
                  const bar = Math.min(100, (Math.abs(c.value) / 25) * 100);
                  const col = neg ? "#ef4444" : c.value >= 15 ? "#22c55e" : c.value >= 8 ? "#3b82f6" : "#6b7280";
                  return (
                    <div key={c.key} className="flex items-center gap-2">
                      <span className="text-[10px] text-text-secondary w-20 shrink-0">{c.label}</span>
                      <div className="flex-1 h-1.5 bg-bg-tertiary rounded-full overflow-hidden">
                        <div className="h-full rounded-full" style={{ width: `${bar}%`, backgroundColor: col }} />
                      </div>
                      <span className={cn("text-[10px] font-mono w-8 text-right tabular-nums", neg ? "text-loss" : "")}>
                        {neg ? c.value : `+${c.value}`}
                      </span>
                    </div>
                  );
                })}
              </div>
            ) : (
              <p className="text-xs text-text-secondary">No score data</p>
            )}
          </div>

          {/* Key metrics */}
          <div className="bg-bg-secondary rounded-xl border border-bg-tertiary p-4">
            <h3 className="text-sm font-medium mb-3">Key Metrics</h3>
            <div className="grid grid-cols-2 gap-2">
              {[
                { label: "Beta", value: un.beta ? un.beta.toFixed(2) : "—" },
                { label: "ATR%", value: un.atr_pct_14d ? `${(un.atr_pct_14d * 100).toFixed(1)}%` : "—",
                  valueClass: un.atr_pct_14d ? (un.atr_pct_14d > 0.09 ? "text-loss" : un.atr_pct_14d > 0.05 ? "text-neutral" : "text-profit") : "" },
                { label: "Gap Risk", value: un.gap_risk_60d ? `${(un.gap_risk_60d * 100).toFixed(1)}%` : "—",
                  valueClass: un.gap_risk_60d && un.gap_risk_60d > 0.06 ? "text-loss" : "" },
                { label: "Turnover", value: fmtCr(un.turnover_med_60d) },
                { label: "T.Rank", value: un.turnover_rank_60d ? `#${un.turnover_rank_60d}` : "—" },
                { label: "Bars", value: un.bars_1d ? String(un.bars_1d) : "—" },
              ].map(m => (
                <div key={m.label} className="bg-bg-tertiary/50 rounded-lg p-2.5">
                  <p className="text-[10px] text-text-secondary">{m.label}</p>
                  <p className={cn("font-mono text-sm font-medium mt-0.5", (m as any).valueClass)}>{m.value}</p>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* ── Technical indicators row ──────────────────────────────────── */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        {/* EMA Alignment */}
        <div className="bg-bg-secondary rounded-xl border border-bg-tertiary p-4 text-center">
          <p className="text-[10px] text-text-secondary mb-2">EMA Alignment</p>
          <div className={cn("text-2xl mb-1", emaAligned ? "text-profit" : "text-neutral")}>
            {emaAligned ? "▲" : aboveEma20 ? "~" : "▼"}
          </div>
          <p className={cn("text-xs font-semibold", emaAligned ? "text-profit" : aboveEma20 ? "text-neutral" : "text-loss")}>
            {emaAligned ? "Fully Aligned" : aboveEma50 ? "Above EMA50" : aboveEma20 ? "Above EMA20" : "Below EMAs"}
          </p>
          <div className="mt-2 space-y-0.5 text-[10px] text-text-secondary">
            <p>EMA20: ₹{fmt(lastEma20)}</p>
            <p>EMA50: ₹{fmt(lastEma50)}</p>
          </div>
        </div>

        {/* RSI */}
        <div className="bg-bg-secondary rounded-xl border border-bg-tertiary p-4 text-center">
          <p className="text-[10px] text-text-secondary mb-1">RSI (14)</p>
          <RsiGauge value={rsiValue} />
        </div>

        {/* MACD state — approximated from EMA crossover */}
        <div className="bg-bg-secondary rounded-xl border border-bg-tertiary p-4 text-center">
          <p className="text-[10px] text-text-secondary mb-2">Momentum</p>
          {(() => {
            const bullish = lastEma20 > lastEma50;
            const score = (scoreComponents.find(c => c.key === "M")?.value ?? 0);
            const col = score > 0 ? "#22c55e" : score < 0 ? "#ef4444" : "#9ca3af";
            return (
              <>
                <div className="text-2xl mb-1" style={{ color: col }}>{bullish ? "↑" : "↓"}</div>
                <p className="text-xs font-semibold" style={{ color: col }}>{bullish ? "Bullish" : "Bearish"}</p>
                <p className="text-[10px] text-text-secondary mt-1">MACD Score: {score > 0 ? `+${score}` : score}</p>
              </>
            );
          })()}
        </div>

        {/* Volume */}
        <div className="bg-bg-secondary rounded-xl border border-bg-tertiary p-4 text-center">
          <p className="text-[10px] text-text-secondary mb-2">Volume</p>
          {(() => {
            const vScore = scoreComponents.find(c => c.key === "V")?.value ?? 0;
            const col = vScore >= 10 ? "#22c55e" : vScore >= 5 ? "#3b82f6" : "#9ca3af";
            const label = vScore >= 10 ? "High" : vScore >= 5 ? "Above Avg" : "Average";
            return (
              <>
                <div className="text-2xl mb-1" style={{ color: col }}>⬡</div>
                <p className="text-xs font-semibold" style={{ color: col }}>{label}</p>
                <p className="text-[10px] text-text-secondary mt-1">Score: {vScore > 0 ? `+${vScore}` : vScore}</p>
              </>
            );
          })()}
        </div>
      </div>

      {/* ── Returns row ───────────────────────────────────────────────── */}
      <div className="bg-bg-secondary rounded-xl border border-bg-tertiary p-4">
        <h3 className="text-sm font-medium mb-3">Historical Returns</h3>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {Object.entries(returns).map(([label, val]) => (
            <div key={label} className="bg-bg-tertiary/50 rounded-lg p-3 text-center">
              <p className="text-[10px] text-text-secondary">{label}</p>
              <p className={cn("font-mono text-xl font-bold mt-1", val == null ? "text-text-secondary" : val >= 0 ? "text-profit" : "text-loss")}>
                {val == null ? "—" : `${val >= 0 ? "+" : ""}${val}%`}
              </p>
            </div>
          ))}
        </div>
      </div>

      {/* ── Tabs: Position / Signals / Trade History ─────────────────── */}
      <div className="bg-bg-secondary rounded-xl border border-bg-tertiary">
        {/* Tab header */}
        <div className="flex border-b border-bg-tertiary">
          {(["overview", "signals", "trades"] as const).map(t => (
            <button key={t} onClick={() => setActiveTab(t)}
              className={cn("px-4 py-2.5 text-xs font-medium capitalize transition-colors border-b-2 -mb-px",
                activeTab === t ? "border-accent text-accent" : "border-transparent text-text-secondary hover:text-text-primary")}>
              {t === "overview" ? "Position & Risk" : t === "signals" ? `Signals Today (${detail?.signals_today.length ?? 0})` : `Trade History (${detail?.recent_trades.length ?? 0})`}
            </button>
          ))}
        </div>

        <div className="p-4">
          {/* Overview tab */}
          {activeTab === "overview" && (
            <div className="space-y-4">
              {/* Open position */}
              {pos ? (
                <div className="rounded-lg border border-accent/30 bg-accent/5 p-4">
                  <p className="text-xs font-semibold text-accent mb-3">Open Position</p>
                  <div className="grid grid-cols-3 md:grid-cols-6 gap-3">
                    {[
                      { label: "Side", value: pos.side ?? "—", valueClass: pos.side === "BUY" ? "text-profit" : "text-loss" },
                      { label: "Qty", value: String(pos.qty ?? "—") },
                      { label: "Entry", value: `₹${fmt(pos.entry_price)}` },
                      { label: "Stop Loss", value: `₹${fmt(pos.sl_price)}`, valueClass: "text-loss" },
                      { label: "Target", value: `₹${fmt(pos.target)}`, valueClass: "text-profit" },
                      { label: "Unreal P&L", value: pos.entry_price && price ? `${((price - pos.entry_price) / pos.entry_price * 100 * (pos.side === "SELL" ? -1 : 1)).toFixed(2)}%` : "—",
                        valueClass: pos.entry_price && price && ((price - pos.entry_price) * (pos.side === "SELL" ? -1 : 1)) >= 0 ? "text-profit" : "text-loss" },
                    ].map(m => (
                      <div key={m.label} className="bg-bg-tertiary/50 rounded-lg p-2.5">
                        <p className="text-[10px] text-text-secondary">{m.label}</p>
                        <p className={cn("font-mono text-sm font-medium mt-0.5", (m as any).valueClass)}>{m.value}</p>
                      </div>
                    ))}
                  </div>
                </div>
              ) : (
                <p className="text-xs text-text-secondary text-center py-4">No open position</p>
              )}

              {/* Risk profile */}
              <div>
                <p className="text-xs font-medium mb-2">Risk Profile</p>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                  {[
                    { label: "ATR%", value: un.atr_pct_14d ? `${(un.atr_pct_14d * 100).toFixed(1)}%` : "—",
                      sub: un.atr_pct_14d ? (un.atr_pct_14d < 0.05 ? "Low Volatility" : un.atr_pct_14d < 0.09 ? "Medium" : "High Volatility") : "",
                      col: un.atr_pct_14d ? (un.atr_pct_14d < 0.05 ? "#22c55e" : un.atr_pct_14d < 0.09 ? "#f59e0b" : "#ef4444") : "#9ca3af" },
                    { label: "Beta", value: un.beta ? un.beta.toFixed(2) : "—",
                      sub: un.beta ? (un.beta < 0.8 ? "Defensive" : un.beta < 1.2 ? "Market-like" : "Aggressive") : "",
                      col: un.beta ? (un.beta < 0.8 ? "#22c55e" : un.beta < 1.2 ? "#9ca3af" : "#f59e0b") : "#9ca3af" },
                    { label: "Gap Risk", value: un.gap_risk_60d ? `${(un.gap_risk_60d * 100).toFixed(1)}%` : "—",
                      sub: un.gap_risk_60d ? (un.gap_risk_60d < 0.03 ? "Low" : un.gap_risk_60d < 0.06 ? "Medium" : "High Overnight Risk") : "",
                      col: un.gap_risk_60d ? (un.gap_risk_60d < 0.03 ? "#22c55e" : un.gap_risk_60d < 0.06 ? "#f59e0b" : "#ef4444") : "#9ca3af" },
                    { label: "52W Position",
                      value: w52h > 0 && w52l < Infinity ? `${(((price - w52l) / (w52h - w52l)) * 100).toFixed(0)}%` : "—",
                      sub: w52h > 0 ? (((price - w52l) / (w52h - w52l)) > 0.8 ? "Near 52W High" : ((price - w52l) / (w52h - w52l)) < 0.2 ? "Near 52W Low" : "Mid Range") : "",
                      col: w52h > 0 ? (((price - w52l) / (w52h - w52l)) > 0.8 ? "#22c55e" : ((price - w52l) / (w52h - w52l)) < 0.2 ? "#ef4444" : "#9ca3af") : "#9ca3af" },
                  ].map(m => (
                    <div key={m.label} className="bg-bg-tertiary/40 rounded-lg p-3">
                      <p className="text-[10px] text-text-secondary">{m.label}</p>
                      <p className="font-mono text-lg font-bold mt-0.5" style={{ color: m.col }}>{m.value}</p>
                      <p className="text-[10px] mt-0.5" style={{ color: m.col }}>{m.sub}</p>
                    </div>
                  ))}
                </div>
              </div>

              {/* Data quality */}
              {un.data_quality_flag && un.data_quality_flag !== "GOOD" && (
                <div className="flex items-center gap-2 text-xs text-neutral bg-neutral/10 border border-neutral/20 rounded-lg px-3 py-2">
                  <AlertCircle className="h-3.5 w-3.5 shrink-0" />
                  Data quality: <strong>{un.data_quality_flag}</strong>
                  {un.stale_days ? ` · ${un.stale_days} days stale` : ""}
                </div>
              )}
            </div>
          )}

          {/* Signals tab */}
          {activeTab === "signals" && (
            <div>
              {detail?.signals_today.length === 0 ? (
                <p className="text-xs text-text-secondary text-center py-8">No signals today for {symbol}</p>
              ) : (
                <div className="space-y-2">
                  {detail?.signals_today.map((s, i) => (
                    <div key={i} className={cn("rounded-lg border p-3 grid grid-cols-2 md:grid-cols-5 gap-2",
                      s.direction === "BUY" ? "border-profit/20 bg-profit/5" : "border-loss/20 bg-loss/5")}>
                      <div>
                        <p className="text-[10px] text-text-secondary">Direction</p>
                        <p className={cn("text-sm font-bold", s.direction === "BUY" ? "text-profit" : "text-loss")}>{s.direction}</p>
                      </div>
                      <div>
                        <p className="text-[10px] text-text-secondary">Score</p>
                        <p className="text-sm font-mono font-bold">{s.score?.toFixed(0)}</p>
                      </div>
                      <div>
                        <p className="text-[10px] text-text-secondary">LTP</p>
                        <p className="text-sm font-mono">₹{fmt(s.ltp)}</p>
                      </div>
                      <div>
                        <p className="text-[10px] text-text-secondary">SL / Target</p>
                        <p className="text-sm font-mono"><span className="text-loss">₹{fmt(s.sl)}</span> / <span className="text-profit">₹{fmt(s.target)}</span></p>
                      </div>
                      <div>
                        <p className="text-[10px] text-text-secondary">Status</p>
                        <p className={cn("text-xs font-medium", s.blocked_reason ? "text-neutral" : "text-profit")}>
                          {s.blocked_reason ? `Blocked: ${s.blocked_reason}` : "Active"}
                        </p>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          {/* Trades tab */}
          {activeTab === "trades" && (
            <div>
              {detail?.recent_trades.length === 0 ? (
                <p className="text-xs text-text-secondary text-center py-8">No trade history for {symbol}</p>
              ) : (
                <>
                  {/* P&L mini sparkline */}
                  {(detail?.recent_trades.length ?? 0) > 1 && (
                    <div className="mb-3">
                      <ResponsiveContainer width="100%" height={50}>
                        <LineChart data={[...detail!.recent_trades].reverse().map((t, i) => ({ i, pnl: t.pnl ?? 0 }))}>
                          <ReferenceLine y={0} stroke="#374151" strokeDasharray="3 3" />
                          <Line type="monotone" dataKey="pnl" stroke="#3b82f6" strokeWidth={1.5} dot={false} />
                          <Tooltip contentStyle={{ backgroundColor: "#111827", border: "1px solid #1f2937", fontSize: 10 }}
                            formatter={(v: any) => [`₹${Number(v).toFixed(0)}`, "P&L"]} />
                        </LineChart>
                      </ResponsiveContainer>
                    </div>
                  )}
                  <div className="overflow-x-auto">
                    <table className="w-full text-xs">
                      <thead>
                        <tr className="text-text-secondary border-b border-bg-tertiary">
                          {["Date", "Side", "Qty", "Entry", "Exit", "P&L", "P&L%", "Hold", "Exit Reason"].map(h => (
                            <th key={h} className="text-left pb-2 pr-4 font-medium">{h}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {detail?.recent_trades.map((t, i) => (
                          <tr key={i} className="border-b border-bg-tertiary/50 hover:bg-bg-tertiary/30 transition-colors">
                            <td className="py-2 pr-4 text-text-secondary">{t.trade_date?.slice(0, 10)}</td>
                            <td className={cn("py-2 pr-4 font-semibold", t.side === "BUY" ? "text-profit" : "text-loss")}>{t.side}</td>
                            <td className="py-2 pr-4 font-mono">{t.qty}</td>
                            <td className="py-2 pr-4 font-mono">₹{fmt(t.entry_price)}</td>
                            <td className="py-2 pr-4 font-mono">₹{fmt(t.exit_price)}</td>
                            <td className={cn("py-2 pr-4 font-mono font-semibold", (t.pnl ?? 0) >= 0 ? "text-profit" : "text-loss")}>
                              {(t.pnl ?? 0) >= 0 ? "+" : ""}₹{fmt(t.pnl, 0)}
                            </td>
                            <td className={cn("py-2 pr-4 font-mono", (t.pnl_pct ?? 0) >= 0 ? "text-profit" : "text-loss")}>
                              {(t.pnl_pct ?? 0) >= 0 ? "+" : ""}{((t.pnl_pct ?? 0) * 100).toFixed(2)}%
                            </td>
                            <td className="py-2 pr-4 text-text-secondary">{t.hold_minutes ? `${t.hold_minutes}m` : "—"}</td>
                            <td className="py-2 text-text-secondary">{t.exit_reason ?? "—"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </>
              )}
            </div>
          )}
        </div>
      </div>

      {/* ── Exclusion warning ─────────────────────────────────────────── */}
      {un.disable_reason && (
        <div className="flex items-center gap-2 text-xs text-loss bg-loss/10 border border-loss/20 rounded-lg px-3 py-2">
          <AlertCircle className="h-3.5 w-3.5 shrink-0" />
          Excluded from trading: <strong>{un.disable_reason}</strong>
        </div>
      )}
    </div>
  );
}
