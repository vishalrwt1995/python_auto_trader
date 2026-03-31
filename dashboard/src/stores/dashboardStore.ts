import { create } from "zustand";
import type { MarketBrainState, WatchlistRow, Position } from "@/lib/types";

interface DashboardState {
  /* Market Brain */
  marketBrain: MarketBrainState | null;
  setMarketBrain: (state: MarketBrainState | null) => void;

  /* Watchlist */
  watchlist: WatchlistRow[];
  setWatchlist: (rows: WatchlistRow[]) => void;

  /* Positions */
  positions: Position[];
  setPositions: (positions: Position[]) => void;

  /* LTP cache */
  ltpCache: Record<string, number>;
  updateLtp: (updates: Record<string, number>) => void;

  /* Selected symbol (for drawers) */
  selectedSymbol: string | null;
  setSelectedSymbol: (symbol: string | null) => void;

  /* Filters */
  watchlistTab: "all" | "swing" | "intraday";
  setWatchlistTab: (tab: "all" | "swing" | "intraday") => void;
}

export const useDashboardStore = create<DashboardState>((set) => ({
  marketBrain: null,
  setMarketBrain: (marketBrain) => set({ marketBrain }),

  watchlist: [],
  setWatchlist: (watchlist) => set({ watchlist }),

  positions: [],
  setPositions: (positions) => set({ positions }),

  ltpCache: {},
  updateLtp: (updates) =>
    set((state) => ({ ltpCache: { ...state.ltpCache, ...updates } })),

  selectedSymbol: null,
  setSelectedSymbol: (selectedSymbol) => set({ selectedSymbol }),

  watchlistTab: "all",
  setWatchlistTab: (watchlistTab) => set({ watchlistTab }),
}));
