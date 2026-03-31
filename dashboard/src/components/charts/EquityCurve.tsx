"use client";

import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { formatCurrency } from "@/lib/utils";

interface DataPoint {
  date: string;
  pnl: number;
}

interface Props {
  data: DataPoint[];
  height?: number;
}

export function EquityCurve({ data, height = 300 }: Props) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <AreaChart data={data} margin={{ top: 5, right: 5, bottom: 5, left: 5 }}>
        <defs>
          <linearGradient id="pnlGrad" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor="#22c55e" stopOpacity={0.3} />
            <stop offset="95%" stopColor="#22c55e" stopOpacity={0} />
          </linearGradient>
        </defs>
        <CartesianGrid stroke="#1f2937" strokeDasharray="3 3" />
        <XAxis
          dataKey="date"
          tick={{ fill: "#9ca3af", fontSize: 10 }}
          tickLine={false}
          axisLine={{ stroke: "#1f2937" }}
        />
        <YAxis
          tick={{ fill: "#9ca3af", fontSize: 10 }}
          tickLine={false}
          axisLine={{ stroke: "#1f2937" }}
          tickFormatter={(v) => formatCurrency(v)}
        />
        <Tooltip
          contentStyle={{
            backgroundColor: "#111827",
            border: "1px solid #1f2937",
            borderRadius: 8,
            fontSize: 12,
          }}
          labelStyle={{ color: "#9ca3af" }}
          formatter={(value: number) => [formatCurrency(value), "P&L"]}
        />
        <Area
          type="monotone"
          dataKey="pnl"
          stroke="#22c55e"
          fill="url(#pnlGrad)"
          strokeWidth={2}
        />
      </AreaChart>
    </ResponsiveContainer>
  );
}
