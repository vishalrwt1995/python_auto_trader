import { create } from "zustand";
import { persist } from "zustand/middleware";

interface SettingsState {
  voiceEnabled: boolean;
  voiceVolume: number;
  pushEnabled: boolean;
  theme: "dark" | "light";
  toggleVoice: () => void;
  setVoiceVolume: (vol: number) => void;
  togglePush: () => void;
  toggleTheme: () => void;
}

export const useSettingsStore = create<SettingsState>()(
  persist(
    (set) => ({
      voiceEnabled: true,
      voiceVolume: 0.8,
      pushEnabled: false,
      theme: "dark",
      toggleVoice: () => set((s) => ({ voiceEnabled: !s.voiceEnabled })),
      setVoiceVolume: (voiceVolume) => set({ voiceVolume }),
      togglePush: () => set((s) => ({ pushEnabled: !s.pushEnabled })),
      toggleTheme: () =>
        set((s) => ({ theme: s.theme === "dark" ? "light" : "dark" })),
    }),
    { name: "autotrader-settings" },
  ),
);
