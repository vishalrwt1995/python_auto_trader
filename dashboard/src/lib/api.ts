const API_URL = process.env.NEXT_PUBLIC_API_URL ?? "";

async function getAuthToken(): Promise<string> {
  const { auth } = await import("./firebase");
  const user = auth.currentUser;
  if (!user) throw new Error("Not authenticated");
  return user.getIdToken();
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const token = await getAuthToken();
  const res = await fetch(`${API_URL}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
      ...init?.headers,
    },
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`API ${res.status}: ${body}`);
  }
  return res.json();
}

export const api = {
  get: <T>(path: string) => request<T>(path),

  post: <T>(path: string, body?: unknown) =>
    request<T>(path, {
      method: "POST",
      body: body ? JSON.stringify(body) : undefined,
    }),

  /* ── Dashboard endpoints ── */

  getTradeSummary: (from?: string, to?: string) => {
    const params = new URLSearchParams();
    if (from) params.set("from", from);
    if (to) params.set("to", to);
    return api.get<Record<string, unknown>>(`/dashboard/trades/summary?${params}`);
  },

  getEquityCurve: (from?: string, to?: string) => {
    const params = new URLSearchParams();
    if (from) params.set("from", from);
    if (to) params.set("to", to);
    return api.get<unknown[]>(`/dashboard/trades/equity-curve?${params}`);
  },

  getTrades: (params: Record<string, string>) => {
    const qs = new URLSearchParams(params);
    return api.get<unknown[]>(`/dashboard/trades/list?${qs}`);
  },

  getSignalsToday: () => api.get<unknown[]>("/dashboard/signals/today"),

  getUniverseStats: () => api.get<Record<string, unknown>>("/dashboard/universe/stats"),

  getUniverseList: (params?: Record<string, string>) => {
    const qs = params ? new URLSearchParams(params) : "";
    return api.get<unknown[]>(`/dashboard/universe/list?${qs}`);
  },

  getPipelineStatus: () => api.get<unknown[]>("/dashboard/pipeline/status"),

  getCandles: (symbol: string, interval = "1d", days = 90) =>
    api.get<unknown[]>(`/dashboard/candles/${symbol}?interval=${interval}&days=${days}`),

  getLtp: (symbols: string[]) =>
    api.get<Record<string, number>>(`/dashboard/ltp?symbols=${symbols.join(",")}`),

  getUpstoxHealth: () => api.get<Record<string, unknown>>("/dashboard/health/upstox"),

  updateConfig: (key: string, value: string) =>
    api.post("/dashboard/config/update", { key, value }),

  triggerJob: (jobName: string) =>
    api.post("/dashboard/admin/trigger-job", { job: jobName }),

  forceTokenRefresh: () => api.post("/dashboard/admin/force-token-refresh"),

  exitPosition: (positionTag: string) =>
    api.post<{ status: string }>("/dashboard/admin/exit-position", { position_tag: positionTag }),

  getPaperMode: () => api.get<{ paper_trade: boolean }>("/dashboard/config/paper-mode"),

  togglePaperMode: (paperTrade: boolean) =>
    api.post<{ status: string; paper_trade: boolean }>("/dashboard/admin/toggle-paper-mode", { paper_trade: paperTrade }),

  exportTrades: (from?: string, to?: string) => {
    const params = new URLSearchParams();
    if (from) params.set("from", from);
    if (to) params.set("to", to);
    return api.post<{ url: string }>(`/dashboard/trades/export?${params}`);
  },
};
