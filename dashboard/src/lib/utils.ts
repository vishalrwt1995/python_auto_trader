import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";
import type { Channel } from "./types";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function formatCurrency(value: number): string {
  const abs = Math.abs(value);
  const sign = value < 0 ? "-" : "";
  if (abs >= 10_000_000) return `${sign}${(abs / 10_000_000).toFixed(2)}Cr`;
  if (abs >= 100_000) return `${sign}${(abs / 100_000).toFixed(2)}L`;
  return `${sign}${abs.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

export function formatPercent(value: number): string {
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

export function formatTime(date: Date): string {
  return date.toLocaleTimeString("en-IN", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
    timeZone: "Asia/Kolkata",
  });
}

export function formatDate(date: Date): string {
  return date.toLocaleDateString("en-IN", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    timeZone: "Asia/Kolkata",
  });
}

export function isMarketOpen(): boolean {
  const now = new Date();
  const ist = new Date(now.toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));
  const day = ist.getDay();
  if (day === 0 || day === 6) return false;
  const hour = ist.getHours();
  const minute = ist.getMinutes();
  const timeMinutes = hour * 60 + minute;
  return timeMinutes >= 9 * 60 + 15 && timeMinutes <= 15 * 60 + 30;
}

/**
 * Infer the channel (intraday vs swing) for a Trade row.
 *
 * The BQ `trades` table does NOT yet carry the `wl_type` column — the writer
 * was added before the dual-channel split. Until that schema migration lands
 * we classify each historic trade structurally:
 *
 *   1. If the row carries an explicit `wl_type` (forward-compatible), use it.
 *   2. Else if `hold_minutes` is reliable and > 360 min (>6 h), call it swing
 *      — the regular session is 9:15-15:30 IST = 375 min, so anything that
 *      brackets two distinct sessions must have held overnight.
 *   3. Else compare the IST date portion of `entry_ts` and `exit_ts` —
 *      different calendar dates ⇒ swing.
 *   4. Default → intraday.
 *
 * Once the BQ writer persists `wl_type`, branches 2-4 become dead code; until
 * then they keep historic rows usable in the channel filters.
 */
const _KNOWN_CHANNELS: Channel[] = ["swing", "intraday", "pead", "corp_action", "gap_fade", "core"];

export function inferTradeChannel(t: {
  wl_type?: string;
  channel?: string;
  strategy?: string;
  hold_minutes?: number;
  entry_ts?: string;
  exit_ts?: string;
}): Channel {
  // 1. Explicit channel / wl_type wins (forward-compatible).
  const explicit = (t.channel || t.wl_type || "").toString().trim().toLowerCase();
  if ((_KNOWN_CHANNELS as string[]).includes(explicit)) return explicit as Channel;

  // 2. Map the channel-specific strategies — the BQ `trades` table carries
  //    `strategy` (not yet `channel`), so PEAD / GAP_FADE / CORE / corp route
  //    directly instead of being mis-bucketed into swing/intraday.
  const strat = (t.strategy || "").toString().trim().toUpperCase();
  if (strat === "PEAD") return "pead";
  if (strat === "GAP_FADE") return "gap_fade";
  if (strat === "CORE") return "core";
  if (strat.startsWith("CORP")) return "corp_action";

  // 3. Legacy swing/intraday heuristic (rows with no channel tag): the regular
  //    session is 9:15-15:30 IST = 375 min, so a >6h hold (or a cross-date
  //    entry/exit) must have held overnight ⇒ swing.
  const hold = typeof t.hold_minutes === "number" ? t.hold_minutes : null;
  if (hold != null && hold > 360) return "swing";
  if (t.entry_ts && t.exit_ts) {
    const ed = isoDateIst(t.entry_ts);
    const xd = isoDateIst(t.exit_ts);
    if (ed && xd && ed !== xd) return "swing";
  }
  return "intraday";
}

// Helper: extract YYYY-MM-DD in Asia/Kolkata from an ISO timestamp.
// Returns null if parse fails. Kept private so callers can't accidentally
// use it for display.
function isoDateIst(ts: string): string | null {
  const t = Date.parse(ts);
  if (Number.isNaN(t)) return null;
  // Format the parsed instant in IST and slice the date.
  const d = new Date(t);
  // toLocaleDateString with en-CA gives YYYY-MM-DD ordering, IST tz.
  return d.toLocaleDateString("en-CA", { timeZone: "Asia/Kolkata" });
}
