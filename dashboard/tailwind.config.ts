import type { Config } from "tailwindcss";

const config: Config = {
  darkMode: "class",
  content: ["./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        bg: {
          primary: "#0a0e17",
          secondary: "#111827",
          tertiary: "#1f2937",
        },
        text: {
          primary: "#f9fafb",
          secondary: "#9ca3af",
        },
        accent: "#3b82f6",
        profit: "#22c55e",
        loss: "#ef4444",
        neutral: "#f59e0b",
        regime: {
          "trend-up": "#22c55e",
          "trend-down": "#ef4444",
          range: "#f59e0b",
          chop: "#6b7280",
          panic: "#dc2626",
          recovery: "#3b82f6",
        },
        risk: {
          normal: "#22c55e",
          aggressive: "#f59e0b",
          defensive: "#f97316",
          lockdown: "#ef4444",
        },
      },
      fontFamily: {
        mono: ["JetBrains Mono", "SF Mono", "ui-monospace", "monospace"],
        sans: ["Inter", "system-ui", "sans-serif"],
      },
    },
  },
  plugins: [],
};

export default config;
