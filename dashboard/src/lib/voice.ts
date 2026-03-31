type Priority = "high" | "normal";

class VoiceAlertEngine {
  private synth: SpeechSynthesis | null = null;
  private _enabled = true;
  private _volume = 0.8;

  constructor() {
    if (typeof window !== "undefined") {
      this.synth = window.speechSynthesis;
    }
  }

  get enabled() {
    return this._enabled;
  }

  set enabled(val: boolean) {
    this._enabled = val;
    if (!val) this.synth?.cancel();
  }

  set volume(val: number) {
    this._volume = Math.max(0, Math.min(1, val));
  }

  speak(message: string, priority: Priority = "normal") {
    if (!this._enabled || !this.synth) return;

    const utterance = new SpeechSynthesisUtterance(message);
    utterance.rate = 1.1;
    utterance.pitch = priority === "high" ? 1.2 : 1.0;
    utterance.volume = this._volume;

    const voices = this.synth.getVoices();
    const indianVoice = voices.find((v) => v.lang === "en-IN");
    if (indianVoice) utterance.voice = indianVoice;

    if (priority === "high") {
      this.synth.cancel();
    }

    this.synth.speak(utterance);
  }

  regimeChange(regime: string, riskMode: string) {
    this.speak(
      `Alert. Market regime changed to ${regime.replace(/_/g, " ")}. Risk mode ${riskMode.replace(/_/g, " ")}.`,
      "high",
    );
  }

  positionOpened(side: string, symbol: string, price: number, sl: number, target: number) {
    this.speak(
      `${side} ${symbol} at ${price}. Stop loss ${sl}. Target ${target}.`,
      "high",
    );
  }

  positionClosed(symbol: string, reason: string, pnlPct: number) {
    const pnlText = `${Math.abs(pnlPct).toFixed(1)} percent`;
    if (reason === "SL_HIT") {
      this.speak(`Stop loss hit on ${symbol}. Loss ${pnlText}.`, "high");
    } else if (reason === "TARGET_HIT") {
      this.speak(`Target hit on ${symbol}. Profit ${pnlText}.`, "normal");
    } else {
      this.speak(`Position closed on ${symbol}. ${reason}.`, "normal");
    }
  }

  newSignal(direction: string, symbol: string, score: number) {
    this.speak(`${direction} signal on ${symbol}. Score ${score}.`, "normal");
  }

  pipelineFailure(jobName: string) {
    this.speak(`Warning. Pipeline job ${jobName} has failed.`, "high");
  }

  tokenExpiry(hours: number) {
    this.speak(`Warning. Upstox token expires in ${hours} hours.`, "high");
  }
}

export const voiceEngine = typeof window !== "undefined" ? new VoiceAlertEngine() : null;
