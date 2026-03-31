"use client";

import { useEffect, type ReactNode } from "react";
import { onAuthStateChanged } from "firebase/auth";
import { auth } from "@/lib/firebase";
import { useAuthStore } from "@/stores/authStore";
import type { AppUser } from "@/lib/types";

export function AuthProvider({ children }: { children: ReactNode }) {
  const setUser = useAuthStore((s) => s.setUser);
  const setLoading = useAuthStore((s) => s.setLoading);

  useEffect(() => {
    if (!auth) {
      setLoading(false);
      return;
    }

    const unsub = onAuthStateChanged(auth, async (firebaseUser) => {
      if (!firebaseUser) {
        setUser(null);
        return;
      }

      const tokenResult = await firebaseUser.getIdTokenResult();
      const role = (tokenResult.claims.role as string) || "viewer";

      const appUser: AppUser = {
        uid: firebaseUser.uid,
        email: firebaseUser.email ?? "",
        displayName: firebaseUser.displayName ?? "",
        role: role as AppUser["role"],
        photoURL: firebaseUser.photoURL ?? undefined,
      };

      setUser(appUser);
    });

    return unsub;
  }, [setUser, setLoading]);

  return <>{children}</>;
}
