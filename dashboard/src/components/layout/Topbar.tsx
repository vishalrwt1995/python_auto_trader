"use client";

import { usePathname, useRouter } from "next/navigation";
import { useState, useRef, useEffect } from "react";
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
  const router = useRouter();
  const user = useAuthStore((s) => s.user);
  const logout = useAuthStore((s) => s.logout);
  const brain = useDashboardStore((s) => s.marketBrain);
  const marketOpen = isMarketOpen();
  const page = PAGE_MAP[pathname] ?? { icon: "📄", label: "Page" };

  const [open, setOpen] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);

  // Close on outside click
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    if (open) document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [open]);

  const initials = user?.displayName
    ? user.displayName.split(" ").map((w) => w[0]).join("").toUpperCase().slice(0, 2)
    : user?.email?.slice(0, 2).toUpperCase() ?? "ST";

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

      {/* Right: Market status + time + profile */}
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

        {/* Profile avatar + dropdown */}
        <div ref={dropdownRef} style={{ position: "relative" }}>
          <button
            onClick={() => setOpen((v) => !v)}
            style={{
              width: 32,
              height: 32,
              borderRadius: "50%",
              background: open ? "#3b82f6" : "#1e293b",
              border: open ? "2px solid #3b82f6" : "2px solid #334155",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              fontSize: 11,
              fontWeight: 700,
              color: open ? "#fff" : "#94a3b8",
              cursor: "pointer",
              transition: "all 0.15s",
              letterSpacing: 0.5,
            }}
          >
            {initials}
          </button>

          {open && (
            <div
              style={{
                position: "absolute",
                top: 40,
                right: 0,
                minWidth: 220,
                background: "#0d1117",
                border: "1px solid #1e293b",
                borderRadius: 10,
                boxShadow: "0 8px 32px rgba(0,0,0,0.5)",
                overflow: "hidden",
                zIndex: 50,
              }}
            >
              {/* User info */}
              <div style={{ padding: "14px 16px", borderBottom: "1px solid #1e293b" }}>
                <div style={{ fontSize: 13, fontWeight: 600, color: "#e2e8f0" }}>
                  {user?.displayName || "Trader"}
                </div>
                <div style={{ fontSize: 11, color: "#64748b", marginTop: 2 }}>
                  {user?.email || ""}
                </div>
              </div>

              {/* Menu items */}
              <div style={{ padding: "6px 0" }}>
                <button
                  onClick={() => { setOpen(false); router.push("/settings"); }}
                  style={{
                    width: "100%",
                    padding: "9px 16px",
                    background: "transparent",
                    border: "none",
                    color: "#94a3b8",
                    fontSize: 13,
                    textAlign: "left",
                    cursor: "pointer",
                    display: "flex",
                    alignItems: "center",
                    gap: 10,
                    transition: "background 0.1s",
                  }}
                  onMouseEnter={(e) => (e.currentTarget.style.background = "#1e293b")}
                  onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}
                >
                  ⚙️ Settings
                </button>
              </div>

              {/* Sign out */}
              <div style={{ padding: "6px 0 8px", borderTop: "1px solid #1e293b" }}>
                <button
                  onClick={() => { setOpen(false); logout(); router.push("/login"); }}
                  style={{
                    width: "100%",
                    padding: "9px 16px",
                    background: "transparent",
                    border: "none",
                    color: "#ef4444",
                    fontSize: 13,
                    fontWeight: 600,
                    textAlign: "left",
                    cursor: "pointer",
                    display: "flex",
                    alignItems: "center",
                    gap: 10,
                    transition: "background 0.1s",
                  }}
                  onMouseEnter={(e) => (e.currentTarget.style.background = "#ef444412")}
                  onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}
                >
                  🚪 Sign Out
                </button>
              </div>
            </div>
          )}
        </div>
      </div>
    </header>
  );
}
