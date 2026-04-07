"use client";

import { useState, useRef, useCallback } from "react";
import { cn } from "@/lib/utils";

const TOOLTIP_WIDTH = 224; // w-56 = 14rem

// ── Smart positioned tooltip ─────────────────────────────────────────────────
// Uses position:fixed so it always renders inside the viewport regardless of
// overflow, sticky headers, or stacking contexts.

interface TooltipProps {
  text: string;
  children: React.ReactNode;
  className?: string;
}

export function Tooltip({ text, children, className }: TooltipProps) {
  const ref = useRef<HTMLSpanElement>(null);
  const [pos, setPos] = useState<React.CSSProperties | null>(null);

  const show = useCallback(() => {
    if (!ref.current) return;
    const r = ref.current.getBoundingClientRect();
    const vw = window.innerWidth;
    const vh = window.innerHeight;
    const style: React.CSSProperties = { position: "fixed" };

    // Horizontal: left-align to anchor, flip right if it would overflow viewport
    if (r.left + TOOLTIP_WIDTH > vw - 8) {
      style.right = vw - r.right;
    } else {
      style.left = r.left;
    }

    // Vertical: prefer below, flip above if near the bottom
    if (r.bottom + 120 > vh) {
      style.bottom = vh - r.top + 4;
    } else {
      style.top = r.bottom + 4;
    }

    setPos(style);
  }, []);

  return (
    <span
      ref={ref}
      className={cn("inline-flex", className)}
      onMouseEnter={show}
      onMouseLeave={() => setPos(null)}
    >
      {children}
      {pos && (
        <span
          style={pos}
          className="z-[600] w-56 p-2.5 bg-gray-950 border border-gray-700 rounded-lg text-[11px] text-text-secondary leading-relaxed shadow-xl whitespace-normal font-normal pointer-events-none"
        >
          {text}
        </span>
      )}
    </span>
  );
}

// ── InfoBadge — the "?" circle used in table headers and stat cards ───────────
// No border on the badge itself to avoid the "extra side" visual in tables.

export function InfoBadge({ text }: { text: string }) {
  return (
    <Tooltip text={text} className="ml-0.5 align-middle">
      <span className="w-3 h-3 rounded-full bg-bg-tertiary text-[8px] font-bold inline-flex items-center justify-center cursor-help text-text-secondary leading-none select-none">
        ?
      </span>
    </Tooltip>
  );
}
