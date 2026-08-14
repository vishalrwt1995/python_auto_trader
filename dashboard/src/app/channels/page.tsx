"use client";

import { useEffect, useState, useCallback } from "react";
import { api } from "@/lib/api";
import { formatCurrency } from "@/lib/utils";
import { CHANNEL_META, CHANNEL_ORDER } from "@/lib/constants";
import type { Channel, ChannelOverview, ChannelOverviewRow } from "@/lib/types";
import { Layers, RefreshCw, ChevronDown } from "lucide-react";

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

function StatTile({ label, value, color, sub }: { label: string; value: string; color?: string; sub?: string }) {
  return (
    <div style={{ flex: 1, minWidth: 110 }}>
      <div style={{ fontSize: 11, color: TXT3, marginBottom: 4 }}>{label}</div>
      <div style={{ fontSize: 19, fontWeight: 700, color: color ?? TXT, fontFamily: MONO }}>{value}</div>
      {sub ? <div style={{ fontSize: 10, color: TXT3, marginTop: 3, fontFamily: MONO }}>{sub}</div> : null}
    </div>
  );
}

function channelStatus(row: ChannelOverviewRow): { text: string; color: string } {
  // A killed/halted channel must never read ACTIVE. `enabled` is only `capital > 0`,
  // so it cannot distinguish "funded but switched off" (intraday, halted 07-09) from
  // live — the backend now sends an explicit `halted` flag for that.
  if (row.halted) return { text: "HALTED", color: "#f59e0b" };
  if (!row.enabled) return { text: "DORMANT", color: TXT3 };
  if (row.breaker_tripped) {
    return row.breaker_reason === "daily_profit_target_hit"
      ? { text: "PROFIT HALT", color: GREEN }
      : { text: "LOSS HALT", color: RED };
  }
  return { text: "ACTIVE", color: GREEN };
}

function ChannelCard({ row, expanded, onClick }: {
  row: ChannelOverviewRow; expanded: boolean; onClick: () => void;
}) {
  const meta = CHANNEL_META[row.channel] ?? { label: row.channel, color: "#6b7280", blurb: "" };
  const status = channelStatus(row);
  const slot = row.max_positions != null ? `${row.open_positions} / ${row.max_positions}` : `${row.open_positions}`;
  const hasActivity = row.closed_trades > 0 || row.open_positions > 0;
  return (
    <div onClick={onClick} role="button" tabIndex={0}
      style={{
        background: CARD_BG, border: `1px solid ${expanded ? meta.color : BORDER}`,
        borderTop: `2px solid ${meta.color}`, borderRadius: 10, padding: 16,
        display: "flex", flexDirection: "column", gap: 12, cursor: "pointer",
        transition: "border-color 0.15s",
      }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 8 }}>
        <div>
          <div style={{ fontSize: 15, fontWeight: 700, color: TXT }}>{meta.label}</div>
          <div style={{ fontSize: 11, color: TXT3, marginTop: 2 }}>{meta.blurb}</div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <span style={{
            fontSize: 10, fontWeight: 700, letterSpacing: 0.4, color: status.color,
            border: `1px solid ${status.color}55`, background: `${status.color}14`,
            padding: "2px 7px", borderRadius: 999, whiteSpace: "nowrap",
          }}>{status.text}</span>
          <ChevronDown size={14} style={{
            color: TXT3, transition: "transform 0.15s",
            transform: expanded ? "rotate(180deg)" : "none",
          }} />
        </div>
      </div>
      <div style={{ display: "flex", gap: 14 }}>
        <StatTile label="Overall P&L"
          value={hasActivity ? signedRupee(row.overall_pnl) : "—"}
          color={hasActivity ? pnlColor(row.overall_pnl) : TXT3}
          sub={hasActivity ? `realized ${signedRupee(row.realized_pnl)} · unreal ${signedRupee(row.unrealized_pnl)}` : undefined} />
        <StatTile label="Today P&L"
          value={row.enabled && row.open_positions > 0 ? signedRupee(row.today_move) : "—"}
          color={row.enabled && row.open_positions > 0 ? pnlColor(row.today_move) : TXT3}
          sub={row.today_pnl !== 0 ? `${signedRupee(row.today_pnl)} realized today`
               : (row.enabled && row.open_positions > 0 ? "mark-to-market" : undefined)} />
      </div>
      <div style={{ display: "flex", gap: 14 }}>
        <StatTile label="Capital" value={row.capital > 0 ? rupee(row.capital) : "—"} />
        <StatTile label="Positions" value={slot}
          sub={row.open_value > 0 ? `${rupee(row.open_value)} deployed` : undefined} />
      </div>
      <div style={{ display: "flex", gap: 14 }}>
        <StatTile label="Trades" value={row.closed_trades > 0 ? String(row.closed_trades) : "—"}
          sub={[row.win_rate != null ? `${row.win_rate}% win` : null,
                `fwd ${signedRupee(row.fwd_realized_pnl)} · ${row.fwd_closed_trades}tr`]
                .filter(Boolean).join(" · ")} />
        <StatTile label="Risk at stake" value={row.open_risk > 0 ? rupee(row.open_risk) : "—"} />
      </div>
      {row.enabled && row.daily_loss_limit < 0 && (
        <div style={{ fontSize: 10, color: TXT3, borderTop: `1px solid ${BORDER}`, paddingTop: 8 }}>
          Breaker {rupee(row.daily_loss_limit)} loss · +{rupee(row.daily_profit_limit)} profit
        </div>
      )}
    </div>
  );
}

type DetailRow = Record<string, string | number>;

function DetailPanel({ channel }: { channel: Channel }) {
  const meta = CHANNEL_META[channel] ?? { label: channel, color: "#6b7280", blurb: "" };
  const [rows, setRows] = useState<DetailRow[] | null>(null);
  const [note, setNote] = useState<string>("");
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setRows(null); setErr(null); setNote("");
    const ok = (r: DetailRow[], n: string) => { if (!cancelled) { setRows(r); setNote(n); } };
    const fail = (e: { message?: string }) => { if (!cancelled) setErr(e?.message || "Failed to load"); };

    if (channel === "core") {
      api.getCoreBasket().then((d) => ok(
        d.holdings.map((h) => ({
          Symbol: h.symbol, Qty: h.qty, Entry: rupee(h.entry_price),
          Notional: rupee(h.notional), Weight: `${h.weight_pct}%`,
        })),
        d.count ? `${d.count} holdings · ${rupee(d.deployed_notional)} deployed` : "no holdings",
      )).catch(fail);
    } else if (channel === "momentum") {
      api.getMomentumBasket().then((d) => ok(
        d.holdings.map((h) => ({
          Symbol: h.symbol, Qty: h.qty, Entry: rupee(h.entry_price),
          Notional: rupee(h.notional), Weight: `${h.weight_pct}%`,
        })),
        d.count ? `${d.count} holdings · ${rupee(d.deployed_notional)} deployed`
                : (d.enabled ? "cash (Nifty < 100DMA) · no holdings" : "no holdings"),
      )).catch(fail);
    } else if (channel === "gap_fade") {
      api.getGapFadeShorts().then((d) => ok(
        d.shorts.map((s) => ({
          Symbol: s.symbol, Qty: s.qty ?? "", "Short @": s.entry_price != null ? rupee(s.entry_price) : "—",
          "Buy-stop": s.sl_price != null ? rupee(s.sl_price) : "—", Since: (s.entry_ts || "").slice(11, 16),
        })),
        d.count ? `${d.count} open · cover ${d.squareoff_ist} IST` : "no open shorts today",
      )).catch(fail);
    } else if (channel === "insider") {
      api.getInsiderWatchlist().then((d) => ok(
        (d.rows || []).map((r) => ({
          Symbol: r.symbol, Buyers: r.n_buyers ?? "", Category: r.category ?? "",
          "Val(cr)": r.total_val_cr != null ? r.total_val_cr.toFixed(2) : "—",
          "Turnover(cr)": r.turnover_cr != null ? r.turnover_cr.toFixed(1) : "—",
          Status: r.status ?? "",
        })),
        (d.macro_gate_ok
          ? `GATE OPEN · b200 ${d.macro?.b200 ?? "?"} · Nifty > 100DMA`
          : `GATE OFF · b200 ${d.macro?.b200 ?? "?"} · Nifty ${d.macro?.nifty_above_100dma ? ">" : "<"} 100DMA`)
          + ` · ${d.clusters ?? 0} cluster${(d.clusters ?? 0) === 1 ? "" : "s"} today`,
      )).catch(fail);
    } else if (channel === "pledge") {
      api.getPledgeWatchlist().then((d) => ok(
        (d.rows || []).map((r) => ({
          Symbol: r.symbol, Revokes: r.n_revokes ?? "", Category: r.category ?? "",
          "Turnover(cr)": r.turnover_cr != null ? r.turnover_cr.toFixed(1) : "—",
          "Close": r.reaction_close != null ? rupee(r.reaction_close) : "—",
          Status: r.status ?? "",
        })),
        (d.macro_gate_ok
          ? `GATE OPEN · b200 ${d.macro?.b200 ?? "?"} · Nifty > 100DMA`
          : `GATE OFF · b200 ${d.macro?.b200 ?? "?"} · Nifty ${d.macro?.nifty_above_100dma ? ">" : "<"} 100DMA`)
          + ` · ${d.revoke_symbols ?? 0} revoke${(d.revoke_symbols ?? 0) === 1 ? "" : "s"} today`,
      )).catch(fail);
    } else {
      api.getPositionsByChannel().then((d) => {
        const list = (d.channels?.[channel] as Record<string, unknown>[]) || [];
        ok(
          list.map((p) => ({
            Symbol: String(p.symbol ?? ""), Side: String(p.side ?? ""), Qty: Number(p.qty ?? 0),
            Entry: p.entry_price != null ? rupee(Number(p.entry_price)) : "—",
            SL: p.sl_price != null ? rupee(Number(p.sl_price)) : "—",
            Setup: String(p.strategy ?? p.wl_type ?? ""),
          })),
          list.length ? `${list.length} open position${list.length > 1 ? "s" : ""}` : "no open positions",
        );
      }).catch(fail);
    }
    return () => { cancelled = true; };
  }, [channel]);

  const cols = rows && rows.length ? Object.keys(rows[0]) : [];
  return (
    <div style={{ background: CARD_BG, border: `1px solid ${BORDER}`, borderTop: `2px solid ${meta.color}`, borderRadius: 10, padding: 16 }}>
      <div style={{ display: "flex", alignItems: "baseline", gap: 10, marginBottom: 10 }}>
        <span style={{ fontSize: 14, fontWeight: 700, color: TXT }}>{meta.label} detail</span>
        <span style={{ fontSize: 12, color: TXT3 }}>{note}</span>
      </div>
      {err && <div style={{ color: RED, fontSize: 12 }}>Error: {err}</div>}
      {!err && rows == null && <div style={{ color: TXT3, fontSize: 12 }}>Loading…</div>}
      {!err && rows != null && rows.length === 0 && <div style={{ color: TXT3, fontSize: 12 }}>Nothing open.</div>}
      {rows != null && rows.length > 0 && (
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontFamily: MONO, fontSize: 12 }}>
            <thead>
              <tr>{cols.map((c) => (
                <th key={c} style={{ textAlign: c === "Symbol" || c === "Side" || c === "Setup" ? "left" : "right", color: TXT3, fontWeight: 600, padding: "6px 10px", borderBottom: `1px solid ${BORDER}`, whiteSpace: "nowrap" }}>{c}</th>
              ))}</tr>
            </thead>
            <tbody>
              {rows.map((r, i) => (
                <tr key={i}>{cols.map((c) => (
                  <td key={c} style={{ textAlign: c === "Symbol" || c === "Side" || c === "Setup" ? "left" : "right", color: c === "Symbol" ? TXT : TXT2, padding: "6px 10px", borderBottom: `1px solid #151b27`, whiteSpace: "nowrap" }}>{r[c]}</td>
                ))}</tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

export default function ChannelsPage() {
  const [data, setData] = useState<ChannelOverview | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [expanded, setExpanded] = useState<Channel | null>(null);

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
        <span style={{ fontSize: 12, color: TXT3 }}>
          {/* computed, not hardcoded — "5 funded books" was stale and disagreed with the cards */}
          {data ? `${data.channels.filter((c) => c.enabled && !c.halted).length} active · ${data.channels.filter((c) => c.halted).length} halted · paper` : "paper"}
        </span>
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
          <StatTile label="Overall P&L" value={signedRupee(totals.overall_pnl)} color={pnlColor(totals.overall_pnl)}
            sub={`realized ${signedRupee(totals.realized_pnl)} · unreal ${signedRupee(totals.unrealized_pnl)}`} />
          <StatTile label="Today P&L" value={signedRupee(totals.today_move)} color={pnlColor(totals.today_move)}
            sub={totals.today_pnl !== 0 ? `${signedRupee(totals.today_pnl)} realized today` : "mark-to-market"} />
          <StatTile label="Open Positions" value={String(totals.open_positions)}
            sub={totals.open_value > 0 ? `${rupee(totals.open_value)} deployed` : undefined} />
          <StatTile label="Risk at stake" value={rupee(totals.open_risk)} />
          <StatTile label="Forward test"
            value={totals.fwd_closed_trades > 0 ? signedRupee(totals.fwd_realized_pnl) : "no closed trades"}
            color={totals.fwd_closed_trades > 0 ? pnlColor(totals.fwd_realized_pnl) : TXT3}
            sub={data?.forward_test?.start ? `since ${data.forward_test.start} · ${totals.fwd_closed_trades} tr` : undefined} />
        </div>
      )}

      {loading && !data && <div style={{ color: TXT3, fontSize: 13 }}>Loading channels…</div>}
      {err && <div style={{ color: RED, fontSize: 13 }}>Error: {err}</div>}

      <div style={{
        display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))", gap: 14,
      }}>
        {ordered.map((r) => (
          <ChannelCard key={r.channel} row={r} expanded={expanded === r.channel}
            onClick={() => setExpanded(expanded === r.channel ? null : r.channel)} />
        ))}
      </div>

      {expanded && <DetailPanel channel={expanded} />}
    </div>
  );
}
