"use client";

import { usePathname, useRouter } from "next/navigation";
import { useState, useRef, useEffect } from "react";
import { useDashboardStore } from "@/stores/dashboardStore";
import { useAuthStore } from "@/stores/authStore";
import { isMarketOpen } from "@/lib/utils";
import { Settings, LogOut, ChevronDown } from "lucide-react";

const PAGE_LABELS: Record<string, string> = {
  "/":             "Command Center",
  "/market-brain": "Market Brain",
  "/watchlist":    "Watchlist",
  "/positions":    "Positions",
  "/signals":      "Signals",
  "/journal":      "Trade Journal",
  "/universe":     "Universe",
  "/sectors":      "Sectors",
  "/history":      "Data History",
  "/pipeline":     "Pipeline",
  "/analytics":    "Analytics",
  "/settings":     "Settings",
  "/login":        "Login",
};

function useISTClock() {
  const [time, setTime] = useState(() =>
    new Date().toLocaleTimeString("en-IN", { hour: "2-digit", minute: "2-digit", second: "2-digit", timeZone: "Asia/Kolkata", hour12: false })
  );
  useEffect(() => {
    const id = setInterval(() => {
      setTime(new Date().toLocaleTimeString("en-IN", { hour: "2-digit", minute: "2-digit", second: "2-digit", timeZone: "Asia/Kolkata", hour12: false }));
    }, 1000);
    return () => clearInterval(id);
  }, []);
  return time;
}

export function Topbar() {
  const pathname = usePathname();
  const router   = useRouter();
  const user     = useAuthStore((s) => s.user);
  const logout   = useAuthStore((s) => s.logout);
  const brain    = useDashboardStore((s) => s.marketBrain);
  const marketOpen = isMarketOpen();
  const clock    = useISTClock();

  const label = PAGE_LABELS[pathname] ?? "Page";

  const [open, setOpen] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) setOpen(false);
    }
    if (open) document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [open]);

  const initials = user?.displayName
    ? user.displayName.split(" ").map((w) => w[0]).join("").toUpperCase().slice(0, 2)
    : user?.email?.slice(0, 2).toUpperCase() ?? "ST";

  // Market status config
  const statusConfig = marketOpen
    ? { label: "Market Open",   dot: "#22c55e", glow: "0 0 8px #22c55e88", bg: "#052e16", border: "#16a34a" }
    : { label: "Market Closed", dot: "#ef4444", glow: "0 0 8px #ef444488", bg: "#2d0a0a", border: "#dc2626" };

  return (
    <header
      className="fixed top-0 left-0 right-0 z-20 md:ml-[220px]"
      style={{
        height: 52,
        borderBottom: "1px solid #1e2433",
        padding: "0 20px",
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        background: "rgba(10,14,23,0.95)",
        backdropFilter: "blur(12px)",
        WebkitBackdropFilter: "blur(12px)",
      }}
    >
      {/* Left: Page title */}
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <span style={{ fontSize: 14, fontWeight: 700, color: "#e2e8f0", letterSpacing: -0.3 }}>
          {label}
        </span>
        {brain?.run_degraded_flag && (
          <span style={{
            fontSize: 10, fontWeight: 700, color: "#f59e0b",
            background: "#451a03", border: "1px solid #78350f",
            borderRadius: 4, padding: "1px 6px", letterSpacing: 0.5,
          }}>
            ⚠ DEGRADED
          </span>
        )}
      </div>

      {/* Right */}
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>

        {/* Market status pill */}
        <div style={{
          display: "flex", alignItems: "center", gap: 5,
          padding: "3px 10px", borderRadius: 20,
          background: statusConfig.bg,
          border: `1px solid ${statusConfig.border}`,
          fontSize: 11, color: statusConfig.dot,
          fontWeight: 600,
        }}>
          <span style={{
            display: "inline-block", width: 7, height: 7, borderRadius: "50%",
            background: statusConfig.dot, boxShadow: statusConfig.glow,
            animation: marketOpen ? "pulse 2s infinite" : "none",
          }} />
          {statusConfig.label}
        </div>

        {/* Live clock — suppressHydrationWarning avoids SSR/client time mismatch */}
        <span
          suppressHydrationWarning
          style={{
            fontSize: 12, color: "#64748b",
            fontFamily: "'JetBrains Mono', monospace",
            letterSpacing: 0.5, minWidth: 72, textAlign: "right",
          }}
        >
          {clock} IST
        </span>

        {/* Profile avatar + dropdown */}
        <div ref={dropdownRef} style={{ position: "relative" }}>
          <button
            onClick={() => setOpen((v) => !v)}
            style={{
              display: "flex", alignItems: "center", gap: 5,
              padding: "3px 6px 3px 3px",
              borderRadius: 20,
              background: open ? "rgba(59,130,246,0.15)" : "rgba(30,36,51,0.8)",
              border: `1px solid ${open ? "#3b82f6" : "#334155"}`,
              cursor: "pointer", transition: "all 0.15s",
            }}
          >
            <div style={{
              width: 26, height: 26, borderRadius: "50%",
              background: "linear-gradient(135deg, #3b82f6, #6366f1)",
              display: "flex", alignItems: "center", justifyContent: "center",
              fontSize: 10, fontWeight: 700, color: "#fff",
            }}>
              {initials}
            </div>
            <ChevronDown size={12} style={{ color: "#64748b", transform: open ? "rotate(180deg)" : "rotate(0)", transition: "transform 0.15s" }} />
          </button>

          {open && (
            <div style={{
              position: "absolute", top: 40, right: 0, minWidth: 210,
              background: "#0a0e17",
              border: "1px solid #1e2433",
              borderRadius: 12,
              boxShadow: "0 16px 48px rgba(0,0,0,0.6), 0 0 0 1px rgba(59,130,246,0.1)",
              overflow: "hidden", zIndex: 50,
            }}>
              {/* User info */}
              <div style={{ padding: "14px 16px 12px", borderBottom: "1px solid #1e2433" }}>
                <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                  <div style={{
                    width: 34, height: 34, borderRadius: "50%",
                    background: "linear-gradient(135deg, #3b82f6, #6366f1)",
                    display: "flex", alignItems: "center", justifyContent: "center",
                    fontSize: 12, fontWeight: 700, color: "#fff", flexShrink: 0,
                  }}>
                    {initials}
                  </div>
                  <div>
                    <div style={{ fontSize: 13, fontWeight: 600, color: "#e2e8f0" }}>{user?.displayName || "Trader"}</div>
                    <div style={{ fontSize: 11, color: "#475569" }}>{user?.email || ""}</div>
                  </div>
                </div>
                {brain && (
                  <div style={{ marginTop: 10, display: "flex", gap: 6 }}>
                    <span style={{
                      fontSize: 10, fontWeight: 700, padding: "2px 7px", borderRadius: 4,
                      background: "rgba(59,130,246,0.15)", border: "1px solid rgba(59,130,246,0.3)",
                      color: "#93c5fd",
                    }}>
                      {brain.regime}
                    </span>
                    <span style={{
                      fontSize: 10, fontWeight: 700, padding: "2px 7px", borderRadius: 4,
                      background: "rgba(34,197,94,0.1)", border: "1px solid rgba(34,197,94,0.2)",
                      color: "#4ade80",
                    }}>
                      Conf {brain.market_confidence?.toFixed(0)}
                    </span>
                  </div>
                )}
              </div>

              {/* Menu items */}
              <div style={{ padding: "6px 8px" }}>
                <DropdownItem icon={<Settings size={13} />} label="Settings" onClick={() => { setOpen(false); router.push("/settings"); }} />
              </div>

              {/* Sign out */}
              <div style={{ padding: "4px 8px 8px", borderTop: "1px solid #1e2433" }}>
                <DropdownItem
                  icon={<LogOut size={13} />}
                  label="Sign Out"
                  danger
                  onClick={() => { setOpen(false); logout(); router.push("/login"); }}
                />
              </div>
            </div>
          )}
        </div>
      </div>
    </header>
  );
}

function DropdownItem({
  icon, label, danger, onClick,
}: {
  icon: React.ReactNode;
  label: string;
  danger?: boolean;
  onClick: () => void;
}) {
  const [hovered, setHovered] = useState(false);
  return (
    <button
      onClick={onClick}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      style={{
        width: "100%", padding: "8px 10px",
        background: hovered ? (danger ? "rgba(239,68,68,0.08)" : "#1e2433") : "transparent",
        border: "none", borderRadius: 8,
        color: danger ? "#ef4444" : "#94a3b8",
        fontSize: 13, textAlign: "left", cursor: "pointer",
        display: "flex", alignItems: "center", gap: 8,
        transition: "background 0.1s",
      }}
    >
      {icon}
      {label}
    </button>
  );
}
