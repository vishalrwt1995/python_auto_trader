"use client";

import { useState, useEffect } from "react";
import { useSettingsStore } from "@/stores/settingsStore";
import { useAuthStore } from "@/stores/authStore";
import { api } from "@/lib/api";
import { cn } from "@/lib/utils";

type LiveSettings = Record<string, number | boolean>;

const INR = (v: number) =>
  v >= 100_000
    ? `₹${(v / 100_000).toFixed(1)}L`
    : v >= 1_000
    ? `₹${(v / 1_000).toFixed(1)}K`
    : `₹${v}`;

const PCT = (v: number) => `${v}%`;
const RAW = (v: number | boolean) => String(v);

type Formatter = (v: number) => string;

const CONFIG_GROUPS: {
  group: string;
  items: { key: string; label: string; fmt: Formatter; description?: string }[];
}[] = [
  {
    group: "Capital & Risk",
    items: [
      { key: "capital", label: "Capital", fmt: INR, description: "Total trading capital" },
      { key: "risk_per_trade", label: "Risk / Trade", fmt: INR, description: "Max loss per trade in ₹" },
      { key: "max_daily_loss", label: "Daily Loss Limit", fmt: INR, description: "Stop trading for the day if hit" },
      { key: "daily_profit_target", label: "Daily Target", fmt: INR, description: "Profit goal for the day" },
    ],
  },
  {
    group: "Position Limits",
    items: [
      { key: "max_trades_day", label: "Max Trades/Day", fmt: RAW },
      { key: "max_positions", label: "Max Positions", fmt: RAW },
      { key: "min_signal_score", label: "Min Signal Score", fmt: RAW, description: "Signal must score ≥ this to qualify" },
    ],
  },
  {
    group: "SL & Target",
    items: [
      { key: "atr_sl_mult", label: "ATR SL Mult", fmt: RAW, description: "Stop loss = ATR × this multiplier" },
      { key: "rr_intraday", label: "R:R Ratio", fmt: RAW, description: "Target = SL distance × this" },
      { key: "vol_mult", label: "Volume Filter", fmt: RAW, description: "Min volume ratio for signal" },
    ],
  },
  {
    group: "Indicators",
    items: [
      { key: "ema_fast", label: "EMA Fast", fmt: RAW },
      { key: "ema_med", label: "EMA Med", fmt: RAW },
      { key: "ema_slow", label: "EMA Slow", fmt: RAW },
      { key: "rsi_period", label: "RSI Period", fmt: RAW },
      { key: "rsi_buy_min", label: "RSI Buy Min", fmt: RAW },
      { key: "rsi_buy_max", label: "RSI Buy Max", fmt: RAW },
    ],
  },
  {
    group: "Swing Trading",
    items: [
      { key: "swing_atr_sl_mult", label: "Swing SL Mult", fmt: RAW, description: "ATR multiplier for swing stop loss (wider than intraday)" },
      { key: "swing_rr", label: "Swing R:R", fmt: RAW, description: "R:R target for swing trades" },
      { key: "swing_risk_per_trade", label: "Swing Risk/Trade", fmt: INR, description: "Max loss per swing trade in ₹" },
      { key: "swing_max_positions", label: "Swing Max Pos", fmt: RAW },
      { key: "swing_min_signal_score", label: "Swing Min Score", fmt: RAW, description: "Minimum score to qualify swing entry" },
    ],
  },
  {
    group: "Market Filters",
    items: [
      { key: "vix_safe_max", label: "VIX Safe Max", fmt: RAW, description: "VIX above this = cautious mode" },
      { key: "pcr_bull_min", label: "PCR Bull Min", fmt: RAW },
      { key: "pcr_bear_max", label: "PCR Bear Max", fmt: RAW },
    ],
  },
];

function SettingCard({
  label,
  value,
  description,
}: {
  label: string;
  value: string;
  description?: string;
}) {
  return (
    <div
      className="flex flex-col gap-1 px-3 py-2.5 bg-bg-primary rounded-lg"
      title={description}
    >
      <span className="font-mono text-[10px] text-text-secondary uppercase tracking-wide">{label}</span>
      <span className="font-mono text-base font-bold text-text-primary">{value}</span>
    </div>
  );
}

export default function SettingsPage() {
  const voiceEnabled = useSettingsStore((s) => s.voiceEnabled);
  const voiceVolume = useSettingsStore((s) => s.voiceVolume);
  const toggleVoice = useSettingsStore((s) => s.toggleVoice);
  const setVoiceVolume = useSettingsStore((s) => s.setVoiceVolume);

  const user = useAuthStore((s) => s.user);
  const isAdmin = useAuthStore((s) => s.isAdmin);

  const [paperMode, setPaperMode] = useState(true);
  const [paperLoading, setPaperLoading] = useState(false);
  const [liveSettings, setLiveSettings] = useState<LiveSettings | null>(null);
  const [settingsLoading, setSettingsLoading] = useState(true);

  useEffect(() => {
    api
      .getSettings()
      .then((d) => {
        setLiveSettings(d);
        if (typeof d.paper_trade === "boolean") setPaperMode(d.paper_trade);
      })
      .catch(() => api.getPaperMode().then((d) => setPaperMode(d.paper_trade)).catch(() => {}))
      .finally(() => setSettingsLoading(false));
  }, []);

  const handleTogglePaper = async () => {
    if (!isAdmin()) return;
    setPaperLoading(true);
    try {
      const res = await api.togglePaperMode(!paperMode);
      setPaperMode(res.paper_trade);
    } catch {}
    finally {
      setPaperLoading(false);
    }
  };

  const getValue = (key: string, fmt: Formatter): string => {
    if (!liveSettings) return "…";
    const v = liveSettings[key];
    if (v === undefined || v === null) return "—";
    if (typeof v === "boolean") return RAW(v);
    return fmt(v as number);
  };

  return (
    <div className="space-y-4">
      <h1 className="text-xl font-semibold">Settings</h1>

      {/* Paper / Live Toggle */}
      <div
        className={cn(
          "rounded-xl border p-5 flex items-center justify-between",
          paperMode
            ? "bg-profit/5 border-profit/30"
            : "bg-loss/5 border-loss/40",
        )}
      >
        <div>
          <p className="text-sm font-bold">Trading Mode</p>
          <p className={cn("text-xs mt-1", paperMode ? "text-profit" : "text-loss")}>
            {paperMode
              ? "✓ PAPER mode — positions are simulated, no real orders sent to Upstox"
              : "⚡ LIVE mode — real bracket orders are being sent to Upstox"}
          </p>
        </div>
        <div className="flex items-center gap-3">
          <span className={cn("text-xs font-bold", paperMode ? "text-profit" : "text-text-secondary")}>
            PAPER
          </span>
          <button
            onClick={isAdmin() ? handleTogglePaper : undefined}
            disabled={paperLoading || !isAdmin()}
            title={isAdmin() ? "Toggle paper/live mode" : "Admin only"}
            className={cn(
              "w-12 h-[26px] rounded-full p-0.5 transition-colors",
              paperMode ? "bg-profit/30 border border-profit" : "bg-loss/30 border border-loss",
              !isAdmin() && "opacity-50 cursor-not-allowed",
            )}
          >
            <div
              className={cn(
                "w-[22px] h-[22px] rounded-full transition-transform",
                paperMode ? "bg-profit translate-x-0" : "bg-loss translate-x-[22px]",
              )}
            />
          </button>
          <span className={cn("text-xs font-bold", !paperMode ? "text-loss" : "text-text-secondary")}>
            LIVE
          </span>
        </div>
      </div>

      {/* Live Config Groups */}
      {settingsLoading ? (
        <div className="text-xs text-text-secondary px-1">Loading settings…</div>
      ) : (
        CONFIG_GROUPS.map((g) => (
          <div key={g.group} className="bg-bg-secondary rounded-xl border border-bg-tertiary p-5">
            <p className="text-xs text-text-secondary font-semibold uppercase tracking-wider mb-3">
              {g.group}
            </p>
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-2.5">
              {g.items.map((item) => (
                <SettingCard
                  key={item.key}
                  label={item.label}
                  value={getValue(item.key, item.fmt)}
                  description={item.description}
                />
              ))}
            </div>
          </div>
        ))
      )}

      {/* Voice Alerts */}
      <div className="bg-bg-secondary rounded-xl border border-bg-tertiary p-5">
        <div className="flex items-center justify-between">
          <div>
            <p className="text-sm font-bold">Voice Alerts</p>
            <p className="text-xs text-text-secondary mt-1">
              Announce regime changes, new positions, exits
            </p>
          </div>
          <div className="flex items-center gap-3">
            <span className="text-xs text-text-secondary">🔊</span>
            <button
              onClick={toggleVoice}
              className={cn(
                "w-9 h-5 rounded-full p-0.5 transition-colors",
                voiceEnabled ? "bg-accent" : "bg-bg-tertiary",
              )}
            >
              <div
                className={cn(
                  "w-4 h-4 rounded-full bg-white transition-transform",
                  voiceEnabled ? "translate-x-4" : "translate-x-0",
                )}
              />
            </button>
          </div>
        </div>
        {voiceEnabled && (
          <div className="mt-4 flex items-center gap-3">
            <span className="text-xs text-text-secondary">Volume</span>
            <input
              type="range"
              min={0}
              max={100}
              step={5}
              value={Math.round(voiceVolume * 100)}
              onChange={(e) => setVoiceVolume(Number(e.target.value) / 100)}
              className="flex-1 accent-accent h-1"
            />
            <span className="font-mono text-xs w-10 text-right">
              {Math.round(voiceVolume * 100)}%
            </span>
            <button
              onClick={() => {
                if (typeof window !== "undefined") {
                  const u = new SpeechSynthesisUtterance("Voice alerts are working.");
                  u.volume = voiceVolume;
                  u.rate = 1.1;
                  window.speechSynthesis.speak(u);
                }
              }}
              className="px-3 py-1 rounded text-xs bg-bg-tertiary text-text-secondary hover:text-text-primary transition-colors"
            >
              Test
            </button>
          </div>
        )}
      </div>

      {/* Admin Config Editor */}
      {isAdmin() && (
        <div className="bg-bg-secondary rounded-xl border border-bg-tertiary p-5">
          <p className="text-xs text-text-secondary font-semibold uppercase tracking-wider mb-3">
            Admin — Config Editor
          </p>
          <AdminConfigPanel />
        </div>
      )}

      {/* Account */}
      <div className="bg-bg-secondary rounded-xl border border-bg-tertiary p-5">
        <p className="text-xs text-text-secondary font-semibold uppercase tracking-wider mb-3">
          Account
        </p>
        <div className="flex items-center gap-4">
          <div className="w-10 h-10 rounded-full bg-accent/20 flex items-center justify-center text-accent font-bold">
            {user?.email?.charAt(0).toUpperCase() ?? "?"}
          </div>
          <div>
            <p className="text-sm font-medium">{user?.email ?? "Not logged in"}</p>
            <p className="text-xs text-text-secondary">Role: {user?.role ?? "viewer"}</p>
          </div>
        </div>
      </div>
    </div>
  );
}

function AdminConfigPanel() {
  const [configKey, setConfigKey] = useState("");
  const [configValue, setConfigValue] = useState("");
  const [saving, setSaving] = useState(false);
  const [result, setResult] = useState<string | null>(null);

  const handleSave = async () => {
    if (!configKey.trim()) return;
    setSaving(true);
    setResult(null);
    try {
      await api.updateConfig(configKey.trim(), configValue.trim());
      setResult("✓ Saved");
      setConfigKey("");
      setConfigValue("");
    } catch (err: any) {
      setResult(`Error: ${err.message}`);
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="space-y-3">
      <p className="text-[11px] text-text-secondary">
        Override a strategy setting in Firestore (takes effect next scan run).
      </p>
      <div className="grid grid-cols-2 gap-3">
        <input
          type="text"
          placeholder="Config key (e.g. min_signal_score)"
          value={configKey}
          onChange={(e) => setConfigKey(e.target.value)}
          className="px-3 py-2 bg-bg-primary rounded-lg text-xs text-text-primary placeholder:text-text-secondary font-mono"
        />
        <input
          type="text"
          placeholder="Value"
          value={configValue}
          onChange={(e) => setConfigValue(e.target.value)}
          className="px-3 py-2 bg-bg-primary rounded-lg text-xs text-text-primary placeholder:text-text-secondary font-mono"
        />
      </div>
      <div className="flex items-center gap-3">
        <button
          onClick={handleSave}
          disabled={saving || !configKey.trim()}
          className="px-4 py-1.5 rounded text-xs bg-accent text-white hover:bg-accent/80 disabled:opacity-50 transition-colors"
        >
          {saving ? "Saving…" : "Save"}
        </button>
        <button
          onClick={() => api.forceTokenRefresh().catch(() => {})}
          className="px-3 py-1.5 rounded text-xs bg-bg-tertiary text-text-secondary hover:text-text-primary transition-colors"
        >
          Force Token Refresh
        </button>
        {result && (
          <span
            className={cn(
              "text-xs",
              result.startsWith("Error") ? "text-loss" : "text-profit",
            )}
          >
            {result}
          </span>
        )}
      </div>
    </div>
  );
}
