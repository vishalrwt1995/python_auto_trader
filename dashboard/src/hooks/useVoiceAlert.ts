"use client";

import { useEffect } from "react";
import { voiceEngine } from "@/lib/voice";
import { useSettingsStore } from "@/stores/settingsStore";

export function useVoiceAlert() {
  const enabled = useSettingsStore((s) => s.voiceEnabled);
  const volume = useSettingsStore((s) => s.voiceVolume);

  useEffect(() => {
    if (voiceEngine) {
      voiceEngine.enabled = enabled;
      voiceEngine.volume = volume;
    }
  }, [enabled, volume]);

  return voiceEngine;
}
