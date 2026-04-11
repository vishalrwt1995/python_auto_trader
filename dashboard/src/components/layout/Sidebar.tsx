"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useSettingsStore } from "@/stores/settingsStore";
import { useAuthStore } from "@/stores/authStore";
import {
  LayoutDashboard,
  Brain,
  List,
  Briefcase,
  Zap,
  BookOpen,
  Globe,
  Factory,
  Database,
  Wrench,
  BarChart3,
  Settings,
  Volume2,
  VolumeX,
} from "lucide-react";

const NAV = [
  { id: "/",             icon: LayoutDashboard, label: "Command Center" },
  { id: "/market-brain", icon: Brain,           label: "Market Brain"   },
  { id: "/watchlist",    icon: List,            label: "Watchlist"       },
  { id: "/positions",    icon: Briefcase,       label: "Positions"       },
  { id: "/signals",      icon: Zap,             label: "Signals"         },
  { id: "/journal",      icon: BookOpen,        label: "Trade Journal"   },
  { id: "/universe",     icon: Globe,           label: "Universe"        },
  { id: "/sectors",      icon: Factory,         label: "Sectors"         },
  { id: "/history",      icon: Database,        label: "Data History"    },
  { id: "/pipeline",     icon: Wrench,          label: "Pipeline"        },
  { id: "/analytics",    icon: BarChart3,       label: "Analytics"       },
  { id: "/settings",     icon: Settings,        label: "Settings"        },
];

export function Sidebar() {
  const pathname  = usePathname();
  const voiceEnabled = useSettingsStore((s) => s.voiceEnabled);
  const toggleVoice  = useSettingsStore((s) => s.toggleVoice);
  const user = useAuthStore((s) => s.user);

  const initials = user?.displayName
    ? user.displayName.split(" ").map((w) => w[0]).join("").toUpperCase().slice(0, 2)
    : user?.email?.slice(0, 2).toUpperCase() ?? "ST";

  return (
    <aside
      className="hidden md:flex flex-col fixed left-0 top-0 h-screen z-30"
      style={{ width: 220, background: "#0a0e17", borderRight: "1px solid #1e2433", flexShrink: 0 }}
    >
      {/* Logo */}
      <div style={{ padding: "18px 18px 14px", borderBottom: "1px solid #1e2433" }}>
        <div style={{ fontSize: 17, fontWeight: 800, letterSpacing: -0.5, display: "flex", alignItems: "center", gap: 8 }}>
          <div style={{
            width: 26, height: 26, borderRadius: 6,
            background: "linear-gradient(135deg, #3b82f6, #06b6d4)",
            display: "flex", alignItems: "center", justifyContent: "center",
            fontSize: 13, fontWeight: 900, color: "#fff",
          }}>S</div>
          <span>
            <span style={{ background: "linear-gradient(90deg, #60a5fa, #22d3ee)", WebkitBackgroundClip: "text", WebkitTextFillColor: "transparent" }}>Smart</span>
            <span style={{ color: "#e2e8f0" }}>Trader</span>
          </span>
        </div>
      </div>

      {/* Nav */}
      <div
        style={{ flex: 1, padding: "6px 8px", overflowY: "auto", display: "flex", flexDirection: "column", gap: 1 }}
        className="scrollbar-thin"
      >
        {NAV.map((n) => {
          const active = n.id === "/" ? pathname === "/" : pathname.startsWith(n.id);
          const Icon = n.icon;
          return (
            <Link
              key={n.id}
              href={n.id}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 10,
                padding: "9px 10px",
                borderRadius: 8,
                position: "relative",
                background: active
                  ? "linear-gradient(90deg, rgba(59,130,246,0.15), rgba(59,130,246,0.04))"
                  : "transparent",
                color: active ? "#93c5fd" : "#64748b",
                fontSize: 13,
                fontWeight: active ? 600 : 400,
                textDecoration: "none",
                transition: "all 0.15s",
                width: "100%",
                borderLeft: active ? "2px solid #3b82f6" : "2px solid transparent",
                marginLeft: 0,
              }}
              onMouseEnter={(e) => {
                if (!active) {
                  e.currentTarget.style.background = "linear-gradient(90deg, rgba(59,130,246,0.07), transparent)";
                  e.currentTarget.style.color = "#94a3b8";
                }
              }}
              onMouseLeave={(e) => {
                if (!active) {
                  e.currentTarget.style.background = "transparent";
                  e.currentTarget.style.color = "#64748b";
                }
              }}
            >
              <Icon
                size={15}
                style={{ flexShrink: 0, opacity: active ? 1 : 0.7 }}
              />
              <span style={{ fontSize: 12.5 }}>{n.label}</span>
              {active && (
                <div style={{
                  position: "absolute",
                  right: 10,
                  width: 5,
                  height: 5,
                  borderRadius: "50%",
                  background: "#3b82f6",
                  boxShadow: "0 0 6px #3b82f6",
                }} />
              )}
            </Link>
          );
        })}
      </div>

      {/* Footer: user chip + voice toggle */}
      <div style={{ borderTop: "1px solid #1e2433" }}>
        {/* User chip */}
        {user && (
          <div style={{ padding: "10px 14px 8px", display: "flex", alignItems: "center", gap: 8 }}>
            <div style={{
              width: 26, height: 26, borderRadius: "50%",
              background: "linear-gradient(135deg, #3b82f6, #6366f1)",
              display: "flex", alignItems: "center", justifyContent: "center",
              fontSize: 10, fontWeight: 700, color: "#fff", flexShrink: 0,
            }}>
              {initials}
            </div>
            <div style={{ minWidth: 0 }}>
              <div style={{ fontSize: 11, fontWeight: 600, color: "#cbd5e1", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                {user.displayName || "Trader"}
              </div>
              <div style={{ fontSize: 10, color: "#475569", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                {user.email || ""}
              </div>
            </div>
          </div>
        )}

        {/* Voice toggle */}
        <div style={{ padding: "8px 14px 14px", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
            {voiceEnabled
              ? <Volume2 size={12} style={{ color: "#22c55e" }} />
              : <VolumeX size={12} style={{ color: "#475569" }} />
            }
            <span style={{ fontSize: 11, color: voiceEnabled ? "#4ade80" : "#475569" }}>
              Voice Alerts
            </span>
          </div>
          <div
            onClick={toggleVoice}
            style={{
              width: 34, height: 18, borderRadius: 9,
              background: voiceEnabled
                ? "linear-gradient(90deg, #16a34a, #22c55e)"
                : "#1e293b",
              border: `1px solid ${voiceEnabled ? "#22c55e" : "#334155"}`,
              padding: 2, cursor: "pointer", transition: "all 0.2s",
              position: "relative",
            }}
          >
            <div style={{
              width: 13, height: 13, borderRadius: "50%",
              background: voiceEnabled ? "#fff" : "#64748b",
              transition: "transform 0.2s",
              transform: voiceEnabled ? "translateX(16px)" : "translateX(0)",
              boxShadow: voiceEnabled ? "0 1px 4px rgba(0,0,0,0.4)" : "none",
            }} />
          </div>
        </div>
      </div>
    </aside>
  );
}
