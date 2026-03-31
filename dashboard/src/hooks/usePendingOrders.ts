"use client";

import { useFirestoreCollection } from "./useFirestore";
import type { PendingOrder } from "@/lib/types";

export function usePendingOrders() {
  return useFirestoreCollection<PendingOrder>("pending_orders");
}
