"use client";

import { useEffect, useState } from "react";
import {
  doc,
  collection,
  onSnapshot,
  query,
  where,
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

/** Subscribe to a Firestore collection (optionally filtered). */
export function useFirestoreCollection<T = DocumentData>(
  collectionName: string,
  filters?: { field: string; op: "==" | "!=" | ">" | "<"; value: unknown }[],
) {
  const [data, setData] = useState<T[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    if (!db) return;
    let q: Query = collection(db, collectionName);
    if (filters?.length) {
      const constraints = filters.map((f) => where(f.field, f.op, f.value));
      q = query(q, ...constraints);
    }
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
  }, [collectionName, filters]);

  return { data, loading, error };
}
