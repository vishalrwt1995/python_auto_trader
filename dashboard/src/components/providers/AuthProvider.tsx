"use client";

import { useEffect, type ReactNode } from "react";
import { onAuthStateChanged, signInAnonymously } from "firebase/auth";
import { auth } from "@/lib/firebase";
import { useAuthStore } from "@/stores/authStore";
import type { AppUser } from "@/lib/types";

const DEV_USER: AppUser = {
  uid: "dev-bypass",
  email: "dev@local",
  displayName: "Dev (bypass)",
  role: "admin",
};

export function AuthProvider({ children }: { children: ReactNode }) {
  const setUser = useAuthStore((s) => s.setUser);
  const setLoading = useAuthStore((s) => s.setLoading);

  useEffect(() => {
    // Dev bypass: NEXT_PUBLIC_SKIP_AUTH=true signs in anonymously for Firestore
    // access while injecting a fake admin user into the app state.
    if (process.env.NEXT_PUBLIC_SKIP_AUTH === "true" && auth) {
      signInAnonymously(auth)
        .then(() => setUser(DEV_USER))
        .catch(() => setUser(DEV_USER)); // still set user even if anon fails
      return;
    }

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
