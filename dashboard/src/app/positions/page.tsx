"use client";

import { useMemo, useState, useCallback, useEffect } from "react";
import { useDashboardStore } from "@/stores/dashboardStore";
import { useAuthStore } from "@/stores/authStore";
import { usePendingOrders } from "@/hooks/usePendingOrders";
import { DataTable, type Column } from "@/components/shared/DataTable";
import { EmptyState } from "@/components/shared/EmptyState";
import { cn, formatCurrency, formatPercent } from "@/lib/utils";
import { api } from "@/lib/api";
import type { Position, PendingOrder } from "@/lib/types";
import { AlertTriangle } from "lucide-react";

const LTP_STALE_MS = 5 * 60 * 1000; // 5 minutes

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

  const openPositions = useMemo(
    () => positions.filter((p) => p.status === "OPEN" || p.status === "PENDING_AMO_EXIT"),
    [positions],
  );

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
                r.wl_type === "swing"
                  ? "bg-indigo-500/15 text-indigo-400"
                  : "bg-cyan-500/15 text-cyan-400",
              )}
            >
              {r.wl_type === "swing" ? "CNC" : "MIS"}
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
              "px-1.5 py-0.5 rounded text-xs font-medium",
              r.side === "BUY"
                ? "bg-profit/20 text-profit"
                : "bg-loss/20 text-loss",
            )}
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
        render: (r) => <span>{r.entry_price.toFixed(2)}</span>,
      },
      {
        key: "ltp",
        label: "LTP",
        className: "text-right font-mono",
        render: (r) => {
          const ltp = ltpCache[r.symbol];
          return <span>{ltp ? ltp.toFixed(2) : "—"}</span>;
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
            <div>
              <span className={pnl >= 0 ? "text-profit" : "text-loss"}>
                {formatCurrency(pnl)}
              </span>
              <span
                className={cn(
                  "text-[10px] ml-1",
                  pnl >= 0 ? "text-profit/70" : "text-loss/70",
                )}
              >
                {formatPercent(pnlPct)}
              </span>
            </div>
          );
        },
      },
      {
        key: "sl",
        label: "SL",
        className: "text-right font-mono text-loss/80",
        render: (r) => <span>{r.sl_price.toFixed(2)}</span>,
      },
      {
        key: "target",
        label: "Target",
        className: "text-right font-mono text-profit/80",
        render: (r) => <span>{r.target.toFixed(2)}</span>,
      },
      {
        key: "rr",
        label: "R:R",
        render: (r) => {
          const ltp = ltpCache[r.symbol] ?? r.entry_price;
          const totalRange = Math.abs(r.target - r.sl_price);
          if (totalRange === 0) return <span>—</span>;
          const progress =
            r.side === "BUY"
              ? ((ltp - r.sl_price) / totalRange) * 100
              : ((r.sl_price - ltp) / totalRange) * 100;
          const clamped = Math.max(0, Math.min(100, progress));
          return (
            <div className="w-20 h-2 bg-bg-tertiary rounded-full overflow-hidden">
              <div
                className="h-full bg-gradient-to-r from-loss via-neutral to-profit rounded-full"
                style={{ width: `${clamped}%` }}
              />
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
      ...(isAdmin()
        ? [
            {
              key: "actions",
              label: "",
              render: (r: Position) => (
                <button
                  className="px-2 py-1 rounded bg-loss/20 text-loss text-xs hover:bg-loss/30 transition-colors disabled:opacity-50"
                  disabled={exitingTag === r.position_tag}
                  onClick={(e) => {
                    e.stopPropagation();
                    setShowExitConfirm(r);
                  }}
                >
                  {exitingTag === r.position_tag ? "Exiting..." : "Exit"}
                </button>
              ),
            } as Column<Position>,
          ]
        : []),
    ],
    [ltpCache, isAdmin, exitingTag],
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
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold">Positions & Orders</h1>
        <div className="flex items-center gap-3">
          <span className="text-xs text-text-secondary">
            {openPositions.length} open
          </span>
          {isAdmin() ? (
            <button
              onClick={() => setShowToggleConfirm(true)}
              disabled={paperLoading}
              className={cn(
                "px-2.5 py-1 rounded text-xs font-medium transition-colors",
                paperMode
                  ? "bg-profit/20 text-profit hover:bg-profit/30"
                  : "bg-loss/20 text-loss hover:bg-loss/30",
              )}
            >
              {paperMode ? "PAPER" : "LIVE"}
            </button>
          ) : (
            <span
              className={cn(
                "px-2 py-0.5 rounded text-xs font-medium",
                paperMode
                  ? "bg-profit/20 text-profit"
                  : "bg-loss/20 text-loss",
              )}
            >
              {paperMode ? "PAPER" : "LIVE"}
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
        <h2 className="text-sm font-medium text-text-secondary mb-2">
          Active Positions
        </h2>
        {openPositions.length === 0 ? (
          <EmptyState
            title="No open positions"
            description="Positions will appear here when the scanner places trades"
          />
        ) : (
          <DataTable
            columns={posColumns}
            data={openPositions}
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
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60">
          <div className="bg-bg-secondary border border-bg-tertiary rounded-lg p-6 max-w-sm w-full mx-4 space-y-4">
            <h3 className="text-lg font-semibold">
              Switch to {paperMode ? "LIVE" : "PAPER"} Mode?
            </h3>
            <p className="text-sm text-text-secondary">
              {paperMode
                ? "Live mode will place real orders through Upstox. Ensure your account has sufficient funds and you understand the risks."
                : "Paper mode will simulate trades without placing real orders. Existing open positions will not be affected."}
            </p>
            {!paperMode && (
              <div className="flex items-center gap-2 text-xs text-profit bg-profit/10 rounded p-2">
                Safe — switching to paper mode does not affect existing positions.
              </div>
            )}
            {paperMode && (
              <div className="flex items-center gap-2 text-xs text-loss bg-loss/10 rounded p-2">
                <AlertTriangle className="h-4 w-4 shrink-0" />
                Warning — real money will be at risk in live mode.
              </div>
            )}
            <div className="flex gap-3 justify-end">
              <button
                onClick={() => setShowToggleConfirm(false)}
                className="px-4 py-2 rounded text-sm text-text-secondary hover:text-text-primary transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={handleTogglePaperMode}
                disabled={paperLoading}
                className={cn(
                  "px-4 py-2 rounded text-sm font-medium transition-colors disabled:opacity-50",
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
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60">
          <div className="bg-bg-secondary border border-bg-tertiary rounded-lg p-6 max-w-sm w-full mx-4 space-y-4">
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
                <span className="font-mono">{showExitConfirm.entry_price.toFixed(2)}</span>
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
                className="px-4 py-2 rounded text-sm text-text-secondary hover:text-text-primary transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={() => handleExitPosition(showExitConfirm)}
                disabled={exitingTag === showExitConfirm.position_tag}
                className="px-4 py-2 rounded text-sm font-medium bg-loss text-white hover:bg-loss/80 transition-colors disabled:opacity-50"
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
