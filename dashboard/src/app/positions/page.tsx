"use client";

import { useMemo, useState, useCallback, useEffect } from "react";
import { useDashboardStore } from "@/stores/dashboardStore";
import { useAuthStore } from "@/stores/authStore";
import { usePendingOrders } from "@/hooks/usePendingOrders";
import { DataTable, type Column } from "@/components/shared/DataTable";
import { EmptyState } from "@/components/shared/EmptyState";
import { cn, formatCurrency, formatPercent } from "@/lib/utils";
import { api } from "@/lib/api";
import type { Channel, Position, PendingOrder } from "@/lib/types";
import { CHANNEL_META, CHANNEL_ORDER } from "@/lib/constants";
import { AlertTriangle } from "lucide-react";

const LTP_STALE_MS = 5 * 60 * 1000; // 5 minutes

// Positions carry a `channel` field at runtime (not yet in the TS type);
// fall back to wl_type, then intraday, so every open position maps to a tab.
const posChannel = (p: Position) =>
  String((p as { channel?: string }).channel ?? p.wl_type ?? "intraday").toLowerCase();

export default function PositionsPage() {
  const positions = useDashboardStore((s) => s.positions);
  const ltpCache = useDashboardStore((s) => s.ltpCache);
  const ltpUpdatedAt = useDashboardStore((s) => s.ltpUpdatedAt);
  const isAdmin = useAuthStore((s) => s.isAdmin);

  const isLtpStale = ltpUpdatedAt > 0 && Date.now() - ltpUpdatedAt > LTP_STALE_MS;
  const { data: pendingOrders } = usePendingOrders();

  const [paperMode, setPaperMode] = useState(true);
  const [paperLoading, setPaperLoading] = useState(false);
  const [showToggleConfirm, setShowToggleConfirm] = useState(false);
  const [exitingTag, setExitingTag] = useState<string | null>(null);
  const [showExitConfirm, setShowExitConfirm] = useState<Position | null>(null);

  // Fetch current paper mode on mount
  useEffect(() => {
    api.getPaperMode().then((d) => setPaperMode(d.paper_trade)).catch(() => {});
  }, []);

  const handleTogglePaperMode = useCallback(async () => {
    setPaperLoading(true);
    try {
      const res = await api.togglePaperMode(!paperMode);
      setPaperMode(res.paper_trade);
    } catch {
      // failed — stays as-is
    } finally {
      setPaperLoading(false);
      setShowToggleConfirm(false);
    }
  }, [paperMode]);

  const handleExitPosition = useCallback(async (position: Position) => {
    setExitingTag(position.position_tag);
    try {
      await api.exitPosition(position.position_tag);
    } catch {
      // error — position stays open
    } finally {
      setExitingTag(null);
      setShowExitConfirm(null);
    }
  }, []);

  const [channelFilter, setChannelFilter] = useState<"all" | Channel>("all");

  const openPositions = useMemo(
    () => positions.filter((p) => p.status === "OPEN" || p.status === "PENDING_AMO_EXIT"),
    [positions],
  );

  // Channel tabs derived from whatever channels are actually open, in the
  // canonical cockpit order. Keeps the tab row honest as positions change.
  const channelTabs = useMemo<("all" | Channel)[]>(() => {
    const present = new Set(openPositions.map(posChannel));
    return ["all", ...CHANNEL_ORDER.filter((c) => present.has(c))];
  }, [openPositions]);

  const filteredPositions = useMemo(() => {
    if (channelFilter === "all") return openPositions;
    return openPositions.filter((p) => posChannel(p) === channelFilter);
  }, [channelFilter, openPositions]);

  // Compute days held for a swing position. Returns null for intraday or
  // when entry_ts is missing/unparseable.
  const computeDaysHeld = useCallback((entry_ts: string | undefined): number | null => {
    if (!entry_ts) return null;
    const t = Date.parse(entry_ts);
    if (Number.isNaN(t)) return null;
    const ms = Date.now() - t;
    return Math.max(0, Math.floor(ms / (1000 * 60 * 60 * 24)));
  }, []);

  const posColumns: Column<Position>[] = useMemo(
    () => [
      {
        key: "symbol",
        label: "Symbol",
        sortable: true,
        sortValue: (r) => r.symbol,
        render: (r) => (
          <div className="flex items-center gap-1.5">
            <span className="font-medium">{r.symbol}</span>
            <span
              className={cn(
                "text-[9px] font-semibold px-1 py-0.5 rounded",
                r.wl_type === "swing" || posChannel(r) === "core"
                  ? "bg-indigo-500/15 text-indigo-400"
                  : "bg-cyan-500/15 text-cyan-400",
              )}
            >
              {r.wl_type === "swing" || posChannel(r) === "core" ? "CNC" : "MIS"}
            </span>
            {r.status === "PENDING_AMO_EXIT" && (
              <span className="text-[9px] font-semibold px-1 py-0.5 rounded bg-neutral/20 text-neutral">
                AMO
              </span>
            )}
          </div>
        ),
      },
      {
        key: "side",
        label: "Side",
        render: (r) => (
          <span
            className={cn(
              "px-2 py-1 rounded text-xs font-semibold",
              r.side === "BUY"
                ? "bg-profit/20 text-profit"
                : "bg-loss/20 text-loss",
            )}
            style={{
              boxShadow:
                r.side === "BUY"
                  ? "0 0 8px rgba(34,197,94,0.3)"
                  : "0 0 8px rgba(239,68,68,0.3)",
            }}
          >
            {r.side}
          </span>
        ),
      },
      {
        key: "qty",
        label: "Qty",
        className: "text-right font-mono",
        render: (r) => <span>{r.qty}</span>,
      },
      {
        key: "entry",
        label: "Entry",
        className: "text-right font-mono",
        render: (r) => <span>{r.entry_price?.toFixed(2) ?? "—"}</span>,
      },
      {
        key: "ltp",
        label: "LTP",
        className: "text-right font-mono",
        render: (r) => {
          const ltp = ltpCache[r.symbol];
          if (!ltp || !r.entry_price) return <span>—</span>;
          const changePct = ((ltp - r.entry_price) / r.entry_price) * 100;
          return (
            <div className="text-right">
              <div>{ltp.toFixed(2)}</div>
              <div
                className={cn(
                  "text-[10px]",
                  changePct >= 0 ? "text-profit" : "text-loss",
                )}
              >
                {changePct >= 0 ? "+" : ""}{changePct.toFixed(2)}%
              </div>
            </div>
          );
        },
      },
      {
        key: "unrealizedPnl",
        label: "Unrealized P&L",
        sortable: true,
        sortValue: (r) => {
          const ltp = ltpCache[r.symbol];
          if (!ltp) return 0;
          return r.side === "BUY"
            ? (ltp - r.entry_price) * r.qty
            : (r.entry_price - ltp) * r.qty;
        },
        className: "text-right font-mono",
        render: (r) => {
          const ltp = ltpCache[r.symbol];
          if (!ltp) return <span className="text-text-secondary">—</span>;
          const pnl =
            r.side === "BUY"
              ? (ltp - r.entry_price) * r.qty
              : (r.entry_price - ltp) * r.qty;
          const pnlPct =
            r.side === "BUY"
              ? ((ltp - r.entry_price) / r.entry_price) * 100
              : ((r.entry_price - ltp) / r.entry_price) * 100;
          return (
            <div
              className="px-1.5 py-0.5 rounded text-right"
              style={{
                background:
                  pnl >= 0
                    ? "rgba(34,197,94,0.08)"
                    : "rgba(239,68,68,0.08)",
              }}
            >
              <div className={cn("font-bold text-sm", pnl >= 0 ? "text-profit" : "text-loss")}>
                {pnl >= 0 ? "↑" : "↓"} {formatCurrency(pnl)}
              </div>
              <div
                className={cn(
                  "text-[10px]",
                  pnl >= 0 ? "text-profit/70" : "text-loss/70",
                )}
              >
                {formatPercent(pnlPct)}
              </div>
            </div>
          );
        },
      },
      {
        key: "sl",
        label: "SL",
        className: "text-right font-mono text-loss/80",
        render: (r) => {
          if (posChannel(r) === "core") {
            return <span className="text-[10px] text-neutral/60 font-sans">CATASTROPHE</span>;
          }
          return <span>{r.sl_price != null ? r.sl_price.toFixed(2) : "—"}</span>;
        },
      },
      {
        key: "target",
        label: "Target",
        className: "text-right font-mono text-profit/80",
        render: (r) => {
          if (posChannel(r) === "core") {
            return <span className="text-[10px] text-text-secondary font-sans">HOLD · Jul 1</span>;
          }
          return <span>{r.target != null ? r.target.toFixed(2) : "—"}</span>;
        },
      },
      {
        key: "rr",
        label: "R:R",
        render: (r) => {
          if (posChannel(r) === "core") return <span className="text-text-secondary/40">—</span>;
          const ltp = ltpCache[r.symbol] ?? r.entry_price;
          if (r.target == null || r.sl_price == null) return <span>—</span>;
          const totalRange = Math.abs(r.target - r.sl_price);
          if (totalRange === 0) return <span>—</span>;
          const progress =
            r.side === "BUY"
              ? ((ltp - r.sl_price) / totalRange) * 100
              : ((r.sl_price - ltp) / totalRange) * 100;
          const clamped = Math.max(0, Math.min(100, progress));
          // Threshold positions for 0.5, 1.0, 1.5 R:R (as % of total range assuming SL=1R, target=2R)
          // 0.5 RR = 33.3%, 1.0 RR = 66.7% (breakeven at 50%)
          return (
            <div
              className="relative w-20"
              title={`R:R progress: ${clamped.toFixed(0)}%`}
            >
              <div className="w-20 h-2 bg-bg-tertiary rounded-full overflow-hidden">
                <div
                  className="h-full bg-gradient-to-r from-loss via-neutral to-profit rounded-full"
                  style={{ width: `${clamped}%` }}
                />
              </div>
              {/* Threshold markers */}
              {[33, 50, 67].map((pct) => (
                <div
                  key={pct}
                  className="absolute top-0 w-px h-2 bg-bg-secondary/80"
                  style={{ left: `${pct}%` }}
                />
              ))}
            </div>
          );
        },
      },
      {
        key: "strategy",
        label: "Strategy",
        render: (r) => (
          <span className="text-xs text-text-secondary">
            {r.strategy || "—"}
          </span>
        ),
      },
      {
        key: "daysHeld",
        label: "Days",
        className: "text-right font-mono text-xs",
        sortable: true,
        sortValue: (r) => computeDaysHeld(r.entry_ts) ?? -1,
        render: (r) => {
          // Days-held is only meaningful for swing positions; intraday
          // closes same day so the value would always be 0 and adds noise.
          if (r.wl_type !== "swing" && posChannel(r) !== "core") return <span className="text-text-secondary/40">—</span>;
          const days = computeDaysHeld(r.entry_ts);
          if (days == null) return <span className="text-text-secondary/40">—</span>;
          // Highlight long-hold swings (≥7 days) so user notices stale positions.
          return (
            <span
              className={cn(
                days >= 7 ? "text-neutral font-semibold" : "text-text-secondary",
              )}
              title={r.entry_ts}
            >
              {days}d
            </span>
          );
        },
      },
      ...(isAdmin()
        ? [
            {
              key: "actions",
              label: "",
              render: (r: Position) => (
                <button
                  className="w-7 h-7 rounded-full border border-loss/40 text-loss hover:bg-loss/10 transition-colors disabled:opacity-50 flex items-center justify-center text-xs font-bold"
                  disabled={exitingTag === r.position_tag}
                  onClick={(e) => {
                    e.stopPropagation();
                    setShowExitConfirm(r);
                  }}
                  title="Exit position"
                >
                  {exitingTag === r.position_tag ? "…" : "✕"}
                </button>
              ),
            } as Column<Position>,
          ]
        : []),
    ],
    [ltpCache, isAdmin, exitingTag, computeDaysHeld],
  );

  const pendingColumns: Column<PendingOrder>[] = useMemo(
    () => [
      {
        key: "ref",
        label: "Ref ID",
        render: (r) => <span className="font-mono text-xs">{r.ref_id}</span>,
      },
      {
        key: "kind",
        label: "Kind",
        render: (r) => <span className="text-xs">{r.kind}</span>,
      },
      {
        key: "symbol",
        label: "Symbol",
        render: (r) => <span className="font-medium">{r.symbol ?? "—"}</span>,
      },
      {
        key: "side",
        label: "Side",
        render: (r) => (
          <span
            className={cn(
              "px-1.5 py-0.5 rounded text-xs",
              r.side === "BUY"
                ? "bg-profit/20 text-profit"
                : r.side === "SELL"
                  ? "bg-loss/20 text-loss"
                  : "bg-bg-tertiary text-text-secondary",
            )}
          >
            {r.side ?? "—"}
          </span>
        ),
      },
      {
        key: "qty",
        label: "Qty",
        className: "text-right font-mono",
        render: (r) => <span>{r.qty ?? "—"}</span>,
      },
    ],
    [],
  );

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h1 className="text-xl font-semibold">Positions & Orders</h1>
          <span
            className={cn(
              "inline-flex items-center justify-center min-w-[2rem] px-2 py-0.5 rounded-full text-xs font-bold",
              openPositions.length > 0
                ? "bg-accent/20 text-accent"
                : "bg-bg-tertiary text-text-secondary",
            )}
          >
            {openPositions.length}
          </span>
          {/* Per-channel breakdown indicator (visible when ≥2 channels have positions) */}
          {openPositions.length > 0 && channelTabs.length > 2 && (
            <span className="text-[10px] text-text-secondary/60 font-mono">
              {channelTabs
                .filter((t): t is Channel => t !== "all")
                .map((c) => `${openPositions.filter((p) => posChannel(p) === c).length} ${CHANNEL_META[c].label.toLowerCase()}`)
                .join(" · ")}
            </span>
          )}
        </div>
        <div className="flex items-center gap-3">
          {isAdmin() ? (
            <button
              onClick={() => setShowToggleConfirm(true)}
              disabled={paperLoading}
              className={cn(
                "px-4 py-1.5 rounded-full text-xs font-semibold transition-all border",
                paperMode
                  ? "bg-profit/10 text-profit border-profit/30 hover:bg-profit/20"
                  : "bg-loss/10 text-loss border-loss/30 hover:bg-loss/20",
              )}
            >
              {paperMode ? "PAPER MODE" : "LIVE MODE"}
            </button>
          ) : (
            <span
              className={cn(
                "px-4 py-1.5 rounded-full text-xs font-semibold border",
                paperMode
                  ? "bg-profit/10 text-profit border-profit/30"
                  : "bg-loss/10 text-loss border-loss/30",
              )}
            >
              {paperMode ? "PAPER MODE" : "LIVE MODE"}
            </span>
          )}
        </div>
      </div>

      {/* LTP Staleness Warning */}
      {isLtpStale && (
        <div className="flex items-center gap-2 text-xs text-neutral bg-neutral/10 border border-neutral/20 rounded px-3 py-2">
          <AlertTriangle className="h-4 w-4 shrink-0" />
          LTP data is stale (last updated over 5 minutes ago). Unrealized P&amp;L may be inaccurate.
        </div>
      )}

      {/* Active Positions */}
      <section>
        <div className="flex items-center justify-between mb-2">
          <h2 className="text-sm font-medium text-text-secondary">
            Active Positions
          </h2>
          {/* Channel filter tabs — only show when there's something to filter */}
          {openPositions.length > 0 && (
            <div className="flex items-center gap-1 text-xs">
              {channelTabs.map((tab) => {
                const count =
                  tab === "all"
                    ? openPositions.length
                    : openPositions.filter((p) => posChannel(p) === tab).length;
                const active = channelFilter === tab;
                const color = tab === "all" ? "#94a3b8" : CHANNEL_META[tab].color;
                return (
                  <button
                    key={tab}
                    onClick={() => setChannelFilter(tab)}
                    className={cn(
                      "px-3 py-1 rounded-full font-medium transition-colors",
                      active
                        ? "border"
                        : "text-text-secondary hover:text-text-primary border border-transparent",
                    )}
                    style={active ? { background: `${color}22`, color, borderColor: `${color}66` } : undefined}
                    title={tab === "all" ? "All channels combined" : CHANNEL_META[tab].blurb}
                  >
                    {tab === "all" ? "All" : CHANNEL_META[tab].label}
                    <span className="ml-1.5 text-[10px] opacity-70">{count}</span>
                  </button>
                );
              })}
            </div>
          )}
        </div>
        {filteredPositions.length === 0 ? (
          <EmptyState
            title={
              channelFilter === "all"
                ? "No open positions"
                : `No open ${CHANNEL_META[channelFilter as Channel]?.label ?? channelFilter} positions`
            }
            description="Positions appear here when the scanner places trades."
          />
        ) : (
          <DataTable
            columns={posColumns}
            data={filteredPositions}
            emptyMessage="No open positions"
          />
        )}
      </section>

      {/* Pending Orders */}
      {pendingOrders.length > 0 && (
        <section>
          <h2 className="text-sm font-medium text-text-secondary mb-2 flex items-center gap-2">
            <AlertTriangle className="h-4 w-4 text-neutral" />
            Pending Orders ({pendingOrders.length})
          </h2>
          <DataTable
            columns={pendingColumns}
            data={pendingOrders}
            emptyMessage="No pending orders"
          />
        </section>
      )}

      <p className="text-[10px] text-text-secondary/50 text-right">
        LTP updates via /dashboard/ltp endpoint (every 5s during market hours)
      </p>

      {/* Paper/Live Toggle Confirmation Dialog */}
      {showToggleConfirm && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm">
          <div className="bg-bg-secondary border border-bg-tertiary rounded-2xl shadow-2xl shadow-black/60 p-6 max-w-sm w-full mx-4 space-y-4">
            <h3 className="text-lg font-semibold">
              Switch to {paperMode ? "LIVE" : "PAPER"} Mode?
            </h3>
            <p className="text-sm text-text-secondary">
              {paperMode
                ? "Live mode will place real orders through Upstox. Ensure your account has sufficient funds and you understand the risks."
                : "Paper mode will simulate trades without placing real orders. Existing open positions will not be affected."}
            </p>
            {!paperMode && (
              <div className="flex items-center gap-2 text-xs text-profit bg-profit/10 rounded-xl p-3">
                Safe — switching to paper mode does not affect existing positions.
              </div>
            )}
            {paperMode && (
              <div className="flex items-center gap-2 text-xs text-loss bg-loss/10 rounded-xl p-3">
                <AlertTriangle className="h-4 w-4 shrink-0" />
                Warning — real money will be at risk in live mode.
              </div>
            )}
            <div className="flex gap-3 justify-end">
              <button
                onClick={() => setShowToggleConfirm(false)}
                className="px-5 py-2 rounded-xl text-sm text-text-secondary hover:text-text-primary hover:bg-bg-tertiary transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={handleTogglePaperMode}
                disabled={paperLoading}
                className={cn(
                  "px-5 py-2 rounded-xl text-sm font-semibold transition-colors disabled:opacity-50",
                  paperMode
                    ? "bg-loss text-white hover:bg-loss/80"
                    : "bg-profit text-white hover:bg-profit/80",
                )}
              >
                {paperLoading
                  ? "Switching..."
                  : `Switch to ${paperMode ? "LIVE" : "PAPER"}`}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Exit Position Confirmation Dialog */}
      {showExitConfirm && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm">
          <div className="bg-bg-secondary border border-bg-tertiary rounded-2xl shadow-2xl shadow-black/60 p-6 max-w-sm w-full mx-4 space-y-4">
            <h3 className="text-lg font-semibold">Exit Position?</h3>
            <div className="space-y-2">
              <div className="flex justify-between text-sm">
                <span className="text-text-secondary">Symbol</span>
                <span className="font-medium">{showExitConfirm.symbol}</span>
              </div>
              <div className="flex justify-between text-sm">
                <span className="text-text-secondary">Side</span>
                <span className={showExitConfirm.side === "BUY" ? "text-profit" : "text-loss"}>
                  {showExitConfirm.side}
                </span>
              </div>
              <div className="flex justify-between text-sm">
                <span className="text-text-secondary">Qty</span>
                <span className="font-mono">{showExitConfirm.qty}</span>
              </div>
              <div className="flex justify-between text-sm">
                <span className="text-text-secondary">Entry</span>
                <span className="font-mono">
                  {showExitConfirm.entry_price != null ? showExitConfirm.entry_price.toFixed(2) : "—"}
                </span>
              </div>
              {ltpCache[showExitConfirm.symbol] && (
                <div className="flex justify-between text-sm">
                  <span className="text-text-secondary">Current LTP</span>
                  <span className="font-mono">{ltpCache[showExitConfirm.symbol].toFixed(2)}</span>
                </div>
              )}
            </div>
            <p className="text-xs text-text-secondary">
              {paperMode
                ? "This will close the paper position at current LTP."
                : "This will place a MARKET order to exit this position immediately."}
            </p>
            <div className="flex gap-3 justify-end">
              <button
                onClick={() => setShowExitConfirm(null)}
                className="px-5 py-2 rounded-xl text-sm text-text-secondary hover:text-text-primary hover:bg-bg-tertiary transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={() => handleExitPosition(showExitConfirm)}
                disabled={exitingTag === showExitConfirm.position_tag}
                className="px-5 py-2 rounded-xl text-sm font-semibold bg-loss text-white hover:bg-loss/80 transition-colors disabled:opacity-50"
              >
                {exitingTag === showExitConfirm.position_tag ? "Exiting..." : "Confirm Exit"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
