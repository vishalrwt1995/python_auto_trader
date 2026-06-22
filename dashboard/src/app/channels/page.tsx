"use client";

import { useEffect, useState, useCallback } from "react";
import { api } from "@/lib/api";
import { formatCurrency } from "@/lib/utils";
import { CHANNEL_META, CHANNEL_ORDER } from "@/lib/constants";
import type { ChannelOverview, ChannelOverviewRow } from "@/lib/types";
import { Layers, RefreshCw } from "lucide-react";

const CARD_BG = "#0e1420";
const BORDER = "#1e2433";
const TXT = "#e2e8f0";
const TXT2 = "#94a3b8";
const TXT3 = "#64748b";
const GREEN = "#22c55e";
const RED = "#ef4444";
const MONO = "ui-monospace, SFMono-Regular, monospace";

function pnlColor(v: number): string {
  return v > 0 ? GREEN : v < 0 ? RED : TXT2;
}

// formatCurrency returns "5.00L" / "200.00" with no currency sign; we add it.
function rupee(v: number): string {
  return `₹${formatCurrency(Math.abs(v))}`;
}
function signedRupee(v: number): string {
  const s = v > 0 ? "+" : v < 0 ? "−" : "";
  return `${s}${rupee(v)}`;
}

function StatTile({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div style={{ flex: 1, minWidth: 110 }}>
      <div style={{ fontSize: 11, color: TXT3, marginBottom: 4 }}>{label}</div>
      <div style={{ fontSize: 19, fontWeight: 700, color: color ?? TXT, fontFamily: MONO }}>{value}</div>
    </div>
  );
}

function channelStatus(row: ChannelOverviewRow): { text: string; color: string } {
  if (!row.enabled) return { text: "DORMANT", color: TXT3 };
  if (row.breaker_tripped) {
    return row.breaker_reason === "daily_profit_target_hit"
      ? { text: "PROFIT HALT", color: GREEN }
      : { text: "LOSS HALT", color: RED };
  }
  return { text: "ACTIVE", color: GREEN };
}

function ChannelCard({ row }: { row: ChannelOverviewRow }) {
  const meta = CHANNEL_META[row.channel] ?? { label: row.channel, color: "#6b7280", blurb: "" };
  const status = channelStatus(row);
  const slot = row.max_positions != null ? `${row.open_positions} / ${row.max_positions}` : `${row.open_positions}`;
  return (
    <div style={{
      background: CARD_BG, border: `1px solid ${BORDER}`, borderTop: `2px solid ${meta.color}`,
      borderRadius: 10, padding: 16, display: "flex", flexDirection: "column", gap: 12,
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 8 }}>
        <div>
          <div style={{ fontSize: 15, fontWeight: 700, color: TXT }}>{meta.label}</div>
          <div style={{ fontSize: 11, color: TXT3, marginTop: 2 }}>{meta.blurb}</div>
        </div>
        <span style={{
          fontSize: 10, fontWeight: 700, letterSpacing: 0.4, color: status.color,
          border: `1px solid ${status.color}55`, background: `${status.color}14`,
          padding: "2px 7px", borderRadius: 999, whiteSpace: "nowrap",
        }}>{status.text}</span>
      </div>

      <div style={{ display: "flex", gap: 14 }}>
        <StatTile label="Capital" value={row.capital > 0 ? rupee(row.capital) : "—"} />
        <StatTile label="Today P&L" value={row.enabled ? signedRupee(row.today_pnl) : "—"}
          color={row.enabled ? pnlColor(row.today_pnl) : TXT3} />
      </div>
      <div style={{ display: "flex", gap: 14 }}>
        <StatTile label="Positions" value={slot} />
        <StatTile label="Risk at stake" value={row.open_risk > 0 ? rupee(row.open_risk) : "—"} />
      </div>

      {row.enabled && row.daily_loss_limit < 0 && (
        <div style={{ fontSize: 10, color: TXT3, borderTop: `1px solid ${BORDER}`, paddingTop: 8 }}>
          Breaker {rupee(row.daily_loss_limit)} loss · +{rupee(row.daily_profit_limit)} profit
        </div>
      )}
      {row.open_symbols?.length > 0 && (
        <div style={{ fontSize: 10.5, color: TXT2, lineHeight: 1.5 }}>
          {row.open_symbols.slice(0, 8).join(" · ")}
          {row.open_symbols.length > 8 ? ` +${row.open_symbols.length - 8}` : ""}
        </div>
      )}
    </div>
  );
}

export default function ChannelsPage() {
  const [data, setData] = useState<ChannelOverview | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const load = useCallback(() => {
    api.getChannelsOverview()
      .then((d) => { setData(d); setErr(null); })
      .catch((e) => setErr(e?.message || "Failed to load"))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    load();
    const id = setInterval(load, 15000);
    return () => clearInterval(id);
  }, [load]);

  const ordered = [...(data?.channels ?? [])].sort(
    (a, b) => CHANNEL_ORDER.indexOf(a.channel) - CHANNEL_ORDER.indexOf(b.channel),
  );
  const totals = data?.totals;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <Layers size={20} style={{ color: "#60a5fa" }} />
        <h1 style={{ fontSize: 20, fontWeight: 800, color: TXT, margin: 0 }}>Channels</h1>
        <span style={{ fontSize: 12, color: TXT3 }}>5 funded books · paper</span>
        <button onClick={load} title="Refresh" style={{
          marginLeft: "auto", background: "transparent", border: `1px solid ${BORDER}`,
          color: TXT2, borderRadius: 8, padding: "5px 9px", cursor: "pointer",
          display: "flex", alignItems: "center", gap: 6, fontSize: 12, fontFamily: MONO,
        }}>
          <RefreshCw size={13} /> {data?.asof ?? "refresh"}
        </button>
      </div>

      {totals && (
        <div style={{
          background: CARD_BG, border: `1px solid ${BORDER}`, borderRadius: 10,
          padding: 16, display: "flex", gap: 24, flexWrap: "wrap",
        }}>
          <StatTile label="Total Capital" value={rupee(totals.capital)} />
          <StatTile label="Today P&L" value={signedRupee(totals.today_pnl)} color={pnlColor(totals.today_pnl)} />
          <StatTile label="Open Positions" value={String(totals.open_positions)} />
          <StatTile label="Risk at stake" value={rupee(totals.open_risk)} />
        </div>
      )}

      {loading && !data && <div style={{ color: TXT3, fontSize: 13 }}>Loading channels…</div>}
      {err && <div style={{ color: RED, fontSize: 13 }}>Error: {err}</div>}

      <div style={{
        display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))", gap: 14,
      }}>
        {ordered.map((r) => <ChannelCard key={r.channel} row={r} />)}
      </div>
    </div>
  );
}
