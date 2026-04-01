import { create } from "zustand";
import { signOut } from "firebase/auth";
import type { AppUser, UserRole } from "@/lib/types";

interface AuthState {
  user: AppUser | null;
  loading: boolean;
  setUser: (user: AppUser | null) => void;
  setLoading: (loading: boolean) => void;
  isAdmin: () => boolean;
  hasRole: (role: UserRole) => boolean;
  logout: () => Promise<void>;
}

export const useAuthStore = create<AuthState>((set, get) => ({
  user: null,
  loading: true,
  setUser: (user) => set({ user, loading: false }),
  setLoading: (loading) => set({ loading }),
  isAdmin: () => get().user?.role === "admin",
  hasRole: (role) => get().user?.role === role,
  logout: async () => {
    const { auth } = await import("@/lib/firebase");
    await signOut(auth);
    set({ user: null });
  },
}));
