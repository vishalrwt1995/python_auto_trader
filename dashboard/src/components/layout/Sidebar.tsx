"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useSettingsStore } from "@/stores/settingsStore";

const NAV = [
  { id: "/", icon: "⚡", label: "Command Center" },
  { id: "/market-brain", icon: "🧠", label: "Market Brain" },
  { id: "/watchlist", icon: "📋", label: "Watchlist" },
  { id: "/positions", icon: "💼", label: "Positions" },
  { id: "/signals", icon: "📡", label: "Signals" },
  { id: "/journal", icon: "📊", label: "Trade Journal" },
  { id: "/universe", icon: "🌐", label: "Universe" },
  { id: "/sectors", icon: "🏭", label: "Sectors" },
  { id: "/history", icon: "🗄️", label: "Data History" },
  { id: "/pipeline", icon: "🔧", label: "Pipeline" },
  { id: "/analytics", icon: "🔬", label: "Analytics" },
  { id: "/settings", icon: "⚙️", label: "Settings" },
];

export function Sidebar() {
  const pathname = usePathname();
  const voiceEnabled = useSettingsStore((s) => s.voiceEnabled);
  const toggleVoice = useSettingsStore((s) => s.toggleVoice);

  return (
    <aside
      className="hidden md:flex flex-col fixed left-0 top-0 h-screen z-30"
      style={{ width: 220, background: "#0d1117", borderRight: "1px solid #1e293b", flexShrink: 0 }}
    >
      {/* Logo */}
      <div style={{ padding: "20px 18px 16px", borderBottom: "1px solid #1e293b" }}>
        <div style={{ fontSize: 18, fontWeight: 800, letterSpacing: -0.5 }}>
          <span style={{ color: "#3b82f6" }}>Smart</span>
          <span style={{ color: "#e2e8f0" }}>Trader</span>
        </div>
      </div>

      {/* Nav */}
      <div style={{ flex: 1, padding: "8px 8px", overflowY: "auto", display: "flex", flexDirection: "column", gap: 2 }} className="scrollbar-thin">
        {NAV.map((n) => {
          const active = n.id === "/" ? pathname === "/" : pathname.startsWith(n.id);
          return (
            <Link
              key={n.id}
              href={n.id}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 10,
                padding: "10px 12px",
                borderRadius: 8,
                background: active ? "#1e293b" : "transparent",
                color: active ? "#e2e8f0" : "#64748b",
                fontSize: 13,
                fontWeight: active ? 600 : 400,
                textDecoration: "none",
                transition: "all 0.15s",
                width: "100%",
              }}
            >
              <span style={{ fontSize: 15 }}>{n.icon}</span>
              {n.label}
            </Link>
          );
        })}
      </div>

      {/* Voice toggle */}
      <div style={{ padding: "12px 16px", borderTop: "1px solid #1e293b", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <span style={{ fontSize: 11, color: "#64748b" }}>🔊 Voice Alerts</span>
        <div
          onClick={toggleVoice}
          style={{
            width: 36,
            height: 20,
            borderRadius: 10,
            background: voiceEnabled ? "#22c55e" : "#374151",
            padding: 2,
            cursor: "pointer",
            transition: "background 0.2s",
          }}
        >
          <div
            style={{
              width: 16,
              height: 16,
              borderRadius: "50%",
              background: "#fff",
              transition: "transform 0.2s",
              transform: voiceEnabled ? "translateX(16px)" : "translateX(0)",
            }}
          />
        </div>
      </div>

    </aside>
  );
}
