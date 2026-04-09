from __future__ import annotations

import socket
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from autotrader.time_utils import now_utc, today_ist


@dataclass
class LockLease:
    name: str
    owner: str


class FirestoreStateStore:
    def __init__(self, project_id: str, database: str = "(default)"):
        self.project_id = project_id
        self.database = database
        self._client = None
        self.owner_id = f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"

    def _db(self):
        if self._client is not None:
            return self._client
        from google.cloud import firestore

        self._client = firestore.Client(project=self.project_id, database=self.database)
        return self._client

    def _doc(self, collection: str, key: str):
        return self._db().collection(collection).document(key)

    def get_json(self, collection: str, key: str) -> dict[str, Any] | None:
        snap = self._doc(collection, key).get()
        return snap.to_dict() if snap.exists else None

    def set_json(self, collection: str, key: str, payload: dict[str, Any], merge: bool = True) -> None:
        data = dict(payload)
        data["updated_at"] = now_utc()
        self._doc(collection, key).set(data, merge=merge)

    def delete(self, collection: str, key: str) -> None:
        self._doc(collection, key).delete()

    def list_by_prefix(self, collection: str, prefix: str, limit: int = 200) -> list[dict[str, Any]]:
        # Firestore has no startswith query on doc id without a dedicated field.
        docs = self._db().collection(collection).limit(limit).stream()
        out = []
        for d in docs:
            if d.id.startswith(prefix):
                row = d.to_dict() or {}
                row["_id"] = d.id
                out.append(row)
        return out

    def get_runtime_prop(self, key: str, default: str = "") -> str:
        row = self.get_json("runtime_props", key)
        if not row:
            return default
        val = row.get("value")
        return str(val) if val is not None else default

    def set_runtime_prop(self, key: str, value: str) -> None:
        self.set_json("runtime_props", key, {"value": value})

    def delete_runtime_prefix(self, prefixes: tuple[str, ...]) -> int:
        count = 0
        for d in self._db().collection("runtime_props").stream():
            if d.id.startswith(prefixes):
                d.reference.delete()
                count += 1
        return count

    def try_acquire_lock(self, name: str, ttl_seconds: int = 30) -> LockLease | None:
        from google.cloud import firestore

        lease_ref = self._doc("locks", name)
        tx = self._db().transaction()
        now = now_utc()
        expires_at = now + timedelta(seconds=ttl_seconds)

        @firestore.transactional
        def _txn(transaction):
            snap = lease_ref.get(transaction=transaction)
            if snap.exists:
                row = snap.to_dict() or {}
                existing_exp = row.get("expires_at")
                if existing_exp and existing_exp > now and row.get("owner") != self.owner_id:
                    return False
            transaction.set(lease_ref, {"owner": self.owner_id, "acquired_at": now, "expires_at": expires_at})
            return True

        ok = _txn(tx)
        return LockLease(name=name, owner=self.owner_id) if ok else None

    def release_lock(self, lease: LockLease | None) -> None:
        if lease is None:
            return
        ref = self._doc("locks", lease.name)
        snap = ref.get()
        if snap.exists and (snap.to_dict() or {}).get("owner") == lease.owner:
            ref.delete()

    def fired_key(self, symbol: str, side: str, day: str | None = None) -> str:
        return f"{day or today_ist()}|{symbol.upper()}|{side.upper()}"

    def mark_fired_today(self, symbol: str, side: str) -> None:
        self.set_json("fired_signals", self.fired_key(symbol, side), {"symbol": symbol.upper(), "side": side.upper()})

    def already_fired_today(self, symbol: str, side: str) -> bool:
        return self.get_json("fired_signals", self.fired_key(symbol, side)) is not None

    def clear_fired_today(self, symbol: str, side: str) -> None:
        self.delete("fired_signals", self.fired_key(symbol, side))

    def save_pending_order(self, ref_id: str, payload: dict[str, Any], kind: str = "entry") -> None:
        self.set_json("pending_orders", f"{kind}:{ref_id}", {"kind": kind, **payload})

    def delete_pending_order(self, ref_id: str, kind: str = "entry") -> None:
        self.delete("pending_orders", f"{kind}:{ref_id}")

    def list_pending_orders(self, kind: str, limit: int = 200) -> list[dict[str, Any]]:
        rows = []
        for d in self._db().collection("pending_orders").stream():
            if not d.id.startswith(f"{kind}:"):
                continue
            row = d.to_dict() or {}
            row["_id"] = d.id
            rows.append(row)
            if len(rows) >= limit:
                break
        return rows

    # ------------------------------------------------------------------ #
    # Positions
    # ------------------------------------------------------------------ #

    def save_position(self, position_tag: str, payload: dict[str, Any]) -> None:
        self.set_json("positions", position_tag, payload)

    def get_position(self, position_tag: str) -> dict[str, Any] | None:
        return self.get_json("positions", position_tag)

    def update_position(self, position_tag: str, updates: dict[str, Any]) -> None:
        self.set_json("positions", position_tag, updates, merge=True)

    def list_open_positions(self) -> list[dict[str, Any]]:
        rows = []
        for d in self._db().collection("positions").stream():
            row = d.to_dict() or {}
            if str(row.get("status", "")).upper() == "OPEN":
                row["_id"] = d.id
                rows.append(row)
        return rows

    def list_all_positions(self, limit: int = 500) -> list[dict[str, Any]]:
        rows = []
        for d in self._db().collection("positions").limit(limit).stream():
            row = d.to_dict() or {}
            row["_id"] = d.id
            rows.append(row)
        return rows

    # ------------------------------------------------------------------ #
    # Orders log
    # ------------------------------------------------------------------ #

    def save_order(self, ref_id: str, payload: dict[str, Any]) -> None:
        self.set_json("orders", ref_id, payload)

    def get_order(self, ref_id: str) -> dict[str, Any] | None:
        return self.get_json("orders", ref_id)

    def list_orders(self, limit: int = 200) -> list[dict[str, Any]]:
        return self.list_by_prefix("orders", prefix="", limit=limit)

    # ------------------------------------------------------------------ #
    # Universe
    # ------------------------------------------------------------------ #

    def save_universe_row(self, symbol: str, payload: dict[str, Any]) -> None:
        self.set_json("universe", symbol.upper(), payload)

    def update_universe_row(self, symbol: str, fields: dict[str, Any]) -> None:
        """Partial merge — updates only the provided fields without overwriting others."""
        self.set_json("universe", symbol.upper(), fields, merge=True)

    def get_universe_row(self, symbol: str) -> dict[str, Any] | None:
        return self.get_json("universe", symbol.upper())

    def list_universe(self, limit: int = 3000) -> list[dict[str, Any]]:
        return self.list_by_prefix("universe", prefix="", limit=limit)

    # ------------------------------------------------------------------ #
    # Watchlist
    # ------------------------------------------------------------------ #

    def save_watchlist(self, payload: dict[str, Any]) -> None:
        self.set_json("watchlist", "latest", payload)

    def get_watchlist(self) -> dict[str, Any] | None:
        return self.get_json("watchlist", "latest")

    # ------------------------------------------------------------------ #
    # Market Brain
    # ------------------------------------------------------------------ #

    def save_market_brain(self, payload: dict[str, Any]) -> None:
        self.set_json("market_brain", "latest", payload)

    def get_market_brain(self) -> dict[str, Any] | None:
        return self.get_json("market_brain", "latest")

    # ------------------------------------------------------------------ #
    # Sector Mapping
    # ------------------------------------------------------------------ #

    def save_sector_mapping(self, symbol: str, payload: dict[str, Any]) -> None:
        self.set_json("sector_mapping", symbol.upper(), payload)

    def get_sector_mapping(self, symbol: str) -> dict[str, Any] | None:
        return self.get_json("sector_mapping", symbol.upper())

    def list_sector_mapping(self, limit: int = 3000) -> list[dict[str, Any]]:
        return self.list_by_prefix("sector_mapping", prefix="", limit=limit)

    # ------------------------------------------------------------------ #
    # Config key-value store
    # ------------------------------------------------------------------ #

    def get_config(self, key: str, default: str = "") -> str:
        row = self.get_json("config", key)
        if not row:
            return default
        val = row.get("value")
        return str(val) if val is not None else default

    def set_config(self, key: str, value: str) -> None:
        self.set_json("config", key, {"key": key, "value": value})

    def list_config(self) -> dict[str, str]:
        """Return all config key-value pairs."""
        out: dict[str, str] = {}
        for d in self._db().collection("config").stream():
            row = d.to_dict() or {}
            val = row.get("value")
            if val is not None:
                out[d.id] = str(val)
        return out

