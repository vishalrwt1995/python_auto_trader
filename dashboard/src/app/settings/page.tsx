"use client";

import { useState, useEffect } from "react";
import { useSettingsStore } from "@/stores/settingsStore";
import { useAuthStore } from "@/stores/authStore";
import { api } from "@/lib/api";
import { cn } from "@/lib/utils";

const CONFIG_GROUPS = [
  {
    group: "Capital & Risk",
    items: [
      ["CAPITAL", "₹5,00,000"],
      ["RISK_PER_TRADE", "1%"],
      ["MAX_DAILY_LOSS", "₹15,000"],
      ["DAILY_PROFIT_TARGET", "₹25,000"],
    ],
  },
  {
    group: "Position Limits",
    items: [
      ["MAX_TRADES_DAY", "8"],
      ["MAX_POSITIONS", "5"],
      ["MIN_SIGNAL_SCORE", "65"],
    ],
  },
  {
    group: "SL / Target",
    items: [
      ["ATR_SL_MULT", "1.5"],
      ["RR_INTRADAY", "2.0"],
    ],
  },
  {
    group: "Indicators",
    items: [
      ["EMA_FAST", "9"],
      ["EMA_MED", "21"],
      ["EMA_SLOW", "50"],
      ["RSI_PERIOD", "14"],
    ],
  },
];

export default function SettingsPage() {
  const voiceEnabled = useSettingsStore((s) => s.voiceEnabled);
  const voiceVolume = useSettingsStore((s) => s.voiceVolume);
  const toggleVoice = useSettingsStore((s) => s.toggleVoice);
  const setVoiceVolume = useSettingsStore((s) => s.setVoiceVolume);

  const user = useAuthStore((s) => s.user);
  const isAdmin = useAuthStore((s) => s.isAdmin);

  const [paperMode, setPaperMode] = useState(true);
  const [paperLoading, setPaperLoading] = useState(false);

  useEffect(() => {
    api.getPaperMode().then((d) => setPaperMode(d.paper_trade)).catch(() => {});
  }, []);

  const handleTogglePaper = async () => {
    setPaperLoading(true);
    try {
      const res = await api.togglePaperMode(!paperMode);
      setPaperMode(res.paper_trade);
    } catch {}
    finally { setPaperLoading(false); }
  };

  return (
    <div className="space-y-4">
      <h1 className="text-xl font-semibold">Settings</h1>

      {/* Paper/Live Toggle */}
      <div className="bg-bg-secondary rounded-xl border border-bg-tertiary p-5 flex items-center justify-between">
        <div>
          <p className="text-sm font-bold">Trading Mode</p>
          <p className="text-xs text-text-secondary mt-1">
            {paperMode
              ? "Currently in paper trading mode. No real orders will be placed."
              : "LIVE mode active. Real orders are being placed through Upstox."}
          </p>
        </div>
        <div className="flex items-center gap-3">
          <span className={cn("text-xs font-bold", paperMode ? "text-profit" : "text-text-secondary")}>PAPER</span>
          <button
            onClick={isAdmin() ? handleTogglePaper : undefined}
            disabled={paperLoading || !isAdmin()}
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
          <span className={cn("text-xs font-bold", !paperMode ? "text-loss" : "text-text-secondary")}>LIVE</span>
        </div>
      </div>

      {/* Voice Alerts */}
      <div className="bg-bg-secondary rounded-xl border border-bg-tertiary p-5">
        <div className="flex items-center justify-between">
          <div>
            <p className="text-sm font-bold">Voice Alerts</p>
            <p className="text-xs text-text-secondary mt-1">Announce regime changes, new positions, exits</p>
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
              <div className={cn(
                "w-4 h-4 rounded-full bg-white transition-transform",
                voiceEnabled ? "translate-x-4" : "translate-x-0",
              )} />
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
            <span className="font-mono text-xs w-10 text-right">{Math.round(voiceVolume * 100)}%</span>
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

      {/* Config Groups */}
      {CONFIG_GROUPS.map((g) => (
        <div key={g.group} className="bg-bg-secondary rounded-xl border border-bg-tertiary p-5">
          <p className="text-xs text-text-secondary font-semibold uppercase tracking-wider mb-3">{g.group}</p>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-2.5">
            {g.items.map(([k, v]) => (
              <div key={k} className="flex items-center justify-between px-3 py-2 bg-bg-primary rounded-lg">
                <span className="font-mono text-[11px] text-text-secondary">{k}</span>
                <span className="font-mono text-sm font-bold">{v}</span>
              </div>
            ))}
          </div>
        </div>
      ))}

      {/* Admin Config Editor */}
      {isAdmin() && (
        <div className="bg-bg-secondary rounded-xl border border-bg-tertiary p-5">
          <p className="text-xs text-text-secondary font-semibold uppercase tracking-wider mb-3">Admin — Config Editor</p>
          <AdminConfigPanel />
        </div>
      )}

      {/* Account */}
      <div className="bg-bg-secondary rounded-xl border border-bg-tertiary p-5">
        <p className="text-xs text-text-secondary font-semibold uppercase tracking-wider mb-3">Account</p>
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
      setResult("Saved");
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
      <div className="grid grid-cols-2 gap-3">
        <input
          type="text"
          placeholder="Config key"
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
          {saving ? "Saving..." : "Save"}
        </button>
        <button
          onClick={() => api.forceTokenRefresh().catch(() => {})}
          className="px-3 py-1.5 rounded text-xs bg-loss/20 text-loss hover:bg-loss/30 transition-colors"
        >
          Force Token Refresh
        </button>
        {result && (
          <span className={cn("text-xs", result.startsWith("Error") ? "text-loss" : "text-profit")}>
            {result}
          </span>
        )}
      </div>
    </div>
  );
}
