"use client";

import { useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  LayoutDashboard,
  Brain,
  List,
  TrendingUp,
  Menu,
  X,
  Zap,
  BookOpen,
  Globe,
  Activity,
  BarChart3,
  Settings,
  PieChart,
  Database,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { MOBILE_NAV_ITEMS, NAV_ITEMS } from "@/lib/constants";

const ICON_MAP: Record<string, React.ComponentType<{ className?: string }>> = {
  LayoutDashboard,
  Brain,
  List,
  TrendingUp,
  Menu,
  Zap,
  BookOpen,
  Globe,
  Activity,
  BarChart3,
  Settings,
  PieChart,
  Database,
};

export function MobileNav() {
  const pathname = usePathname();
  const [showMore, setShowMore] = useState(false);

  return (
    <>
      {/* More drawer */}
      {showMore && (
        <div className="fixed inset-0 z-50 md:hidden">
          <div
            className="absolute inset-0 bg-black/60"
            onClick={() => setShowMore(false)}
          />
          <div className="absolute bottom-0 left-0 right-0 bg-bg-secondary rounded-t-2xl p-4 max-h-[70vh] overflow-y-auto">
            <div className="flex justify-between items-center mb-4">
              <span className="font-semibold text-sm">All Pages</span>
              <button onClick={() => setShowMore(false)}>
                <X className="h-5 w-5" />
              </button>
            </div>
            <nav className="grid grid-cols-3 gap-3">
              {NAV_ITEMS.map((item) => {
                const Icon = ICON_MAP[item.icon];
                const active =
                  item.href === "/"
                    ? pathname === "/"
                    : pathname.startsWith(item.href);
                return (
                  <Link
                    key={item.href}
                    href={item.href}
                    onClick={() => setShowMore(false)}
                    className={cn(
                      "flex flex-col items-center gap-1 p-3 rounded-lg text-xs",
                      active
                        ? "bg-accent/10 text-accent"
                        : "text-text-secondary hover:bg-bg-tertiary",
                    )}
                  >
                    {Icon && <Icon className="h-5 w-5" />}
                    {item.label}
                  </Link>
                );
              })}
            </nav>
          </div>
        </div>
      )}

      {/* Bottom bar */}
      <nav className="md:hidden fixed bottom-0 left-0 right-0 z-40 bg-bg-secondary border-t border-bg-tertiary flex justify-around items-center h-14 safe-area-inset-bottom">
        {MOBILE_NAV_ITEMS.map((item) => {
          const Icon = ICON_MAP[item.icon];
          const isMore = item.href === "#more";
          const active =
            !isMore &&
            (item.href === "/"
              ? pathname === "/"
              : pathname.startsWith(item.href));

          if (isMore) {
            return (
              <button
                key="more"
                onClick={() => setShowMore(true)}
                className="flex flex-col items-center gap-0.5 text-text-secondary min-w-[44px] min-h-[44px] justify-center"
              >
                {Icon && <Icon className="h-5 w-5" />}
                <span className="text-[10px]">{item.label}</span>
              </button>
            );
          }

          return (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                "flex flex-col items-center gap-0.5 min-w-[44px] min-h-[44px] justify-center",
                active ? "text-accent" : "text-text-secondary",
              )}
            >
              {Icon && <Icon className="h-5 w-5" />}
              <span className="text-[10px]">{item.label}</span>
            </Link>
          );
        })}
      </nav>
    </>
  );
}
