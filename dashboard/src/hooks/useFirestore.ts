"use client";

import { useEffect, useState } from "react";
import {
  doc,
  collection,
  onSnapshot,
  query,
  where,
  orderBy,
  limit,
  type DocumentData,
  type Query,
} from "firebase/firestore";
import { db } from "@/lib/firebase";

/** Subscribe to a single Firestore document. */
export function useFirestoreDoc<T = DocumentData>(
  collectionName: string,
  docId: string,
) {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    if (!db) return;
    const ref = doc(db, collectionName, docId);
    const unsub = onSnapshot(
      ref,
      (snap) => {
        setData(snap.exists() ? (snap.data() as T) : null);
        setLoading(false);
      },
      (err) => {
        setError(err);
        setLoading(false);
      },
    );
    return unsub;
  }, [collectionName, docId]);

  return { data, loading, error };
}

/** Subscribe to a Firestore collection (optionally filtered, ordered, limited). */
export function useFirestoreCollection<T = DocumentData>(
  collectionName: string,
  options?: {
    filters?: { field: string; op: "==" | "!=" | ">" | "<"; value: unknown }[];
    orderByField?: string;
    orderByDir?: "asc" | "desc";
    limitCount?: number;
  },
) {
  const [data, setData] = useState<T[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    if (!db) return;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const constraints: any[] = [];
    if (options?.filters?.length) {
      constraints.push(...options.filters.map((f) => where(f.field, f.op, f.value)));
    }
    if (options?.orderByField) {
      constraints.push(orderBy(options.orderByField, options.orderByDir ?? "asc"));
    }
    if (options?.limitCount) {
      constraints.push(limit(options.limitCount));
    }
    const q: Query = constraints.length
      ? query(collection(db, collectionName), ...constraints)
      : collection(db, collectionName);
    const unsub = onSnapshot(
      q,
      (snap) => {
        const docs = snap.docs.map((d) => ({ ...d.data(), _id: d.id }) as T);
        setData(docs);
        setLoading(false);
      },
      (err) => {
        setError(err);
        setLoading(false);
      },
    );
    return unsub;
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [collectionName]);

  return { data, loading, error };
}
