"use client";

import { usePathname } from "next/navigation";
import { useDashboardStore } from "@/stores/dashboardStore";
import { useAuthStore } from "@/stores/authStore";
import { isMarketOpen } from "@/lib/utils";

const PAGE_MAP: Record<string, { icon: string; label: string }> = {
  "/": { icon: "⚡", label: "Command Center" },
  "/market-brain": { icon: "🧠", label: "Market Brain" },
  "/watchlist": { icon: "📋", label: "Watchlist" },
  "/positions": { icon: "💼", label: "Positions" },
  "/signals": { icon: "📡", label: "Signals" },
  "/journal": { icon: "📊", label: "Trade Journal" },
  "/universe": { icon: "🌐", label: "Universe" },
  "/pipeline": { icon: "🔧", label: "Pipeline" },
  "/analytics": { icon: "🔬", label: "Analytics" },
  "/settings": { icon: "⚙️", label: "Settings" },
  "/login": { icon: "🔑", label: "Login" },
};

export function Topbar() {
  const pathname = usePathname();
  const user = useAuthStore((s) => s.user);
  const brain = useDashboardStore((s) => s.marketBrain);
  const marketOpen = isMarketOpen();
  const page = PAGE_MAP[pathname] ?? { icon: "📄", label: "Page" };

  return (
    <header
      className="fixed top-0 left-0 right-0 z-20 md:ml-[220px]"
      style={{
        height: 52,
        borderBottom: "1px solid #1e293b",
        padding: "0 24px",
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        background: "#0d1117",
      }}
    >
      {/* Left: Page icon + title */}
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <span style={{ fontSize: 16 }}>{page.icon}</span>
        <span style={{ fontSize: 15, fontWeight: 700, color: "#e2e8f0" }}>{page.label}</span>
      </div>

      {/* Right: Market status + time + user */}
      <div style={{ display: "flex", alignItems: "center", gap: 14 }}>
        <span style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11, color: "#64748b" }}>
          <span
            style={{
              display: "inline-block",
              width: 8,
              height: 8,
              borderRadius: "50%",
              background: marketOpen ? "#22c55e" : "#ef4444",
              boxShadow: marketOpen ? "0 0 6px #22c55e" : "0 0 6px #ef4444",
              animation: "pulse 2s infinite",
            }}
          />
          {marketOpen ? "Market Open" : "Market Closed"}
        </span>
        <span style={{ fontSize: 12, color: "#94a3b8", fontFamily: "'JetBrains Mono', monospace" }}>
          {new Date().toLocaleTimeString("en-IN", { hour: "2-digit", minute: "2-digit", timeZone: "Asia/Kolkata" })} IST
        </span>
        <div
          style={{
            width: 32,
            height: 32,
            borderRadius: "50%",
            background: "#1e293b",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            fontSize: 13,
          }}
        >
          👤
        </div>
      </div>
    </header>
  );
}
