"use client";

import {
  Radar,
  RadarChart,
  PolarGrid,
  PolarAngleAxis,
  PolarRadiusAxis,
  ResponsiveContainer,
  Legend,
} from "recharts";

interface DataPoint {
  label: string;
  current: number;
  previous?: number;
}

interface Props {
  data: DataPoint[];
  height?: number;
}

export function RadarScore({ data, height = 300 }: Props) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <RadarChart data={data} cx="50%" cy="50%" outerRadius="75%">
        <PolarGrid stroke="#1f2937" />
        <PolarAngleAxis
          dataKey="label"
          tick={{ fill: "#9ca3af", fontSize: 11 }}
        />
        <PolarRadiusAxis
          angle={90}
          domain={[0, 100]}
          tick={{ fill: "#6b7280", fontSize: 9 }}
          tickCount={5}
        />
        {data.some((d) => d.previous != null) && (
          <Radar
            name="Previous"
            dataKey="previous"
            stroke="#6b7280"
            fill="transparent"
            strokeDasharray="4 4"
            strokeWidth={1}
          />
        )}
        <Radar
          name="Current"
          dataKey="current"
          stroke="#3b82f6"
          fill="#3b82f6"
          fillOpacity={0.2}
          strokeWidth={2}
        />
        <Legend
          wrapperStyle={{ fontSize: 11, color: "#9ca3af" }}
        />
      </RadarChart>
    </ResponsiveContainer>
  );
}
