"use client";

import { formatCurrency } from "@/lib/utils";

interface Props {
  label: string;
  value: number;
  subLabel?: string;
  subValue?: string;
}

export function PnLCard({ label, value, subLabel, subValue }: Props) {
  const isPositive = value >= 0;
  const color = isPositive ? "#22c55e" : "#ef4444";

  return (
    <div style={{ background: "#111827", borderRadius: 12, padding: "16px 18px", border: "1px solid #1e293b", flex: 1, minWidth: 140 }}>
      <div style={{ fontSize: 11, color: "#64748b", textTransform: "uppercase", letterSpacing: 1, marginBottom: 6, display: "flex", alignItems: "center", gap: 6 }}>
        <span>₹</span> {label}
      </div>
      <div style={{ fontSize: 24, fontWeight: 700, color, fontFamily: "'JetBrains Mono', monospace" }}>
        {isPositive ? "+" : ""}₹{Math.abs(value).toLocaleString("en-IN")}
      </div>
      {subLabel && (
        <div style={{ fontSize: 11, color: "#64748b", marginTop: 4 }}>
          {subLabel}: {subValue}
        </div>
      )}
    </div>
  );
}
